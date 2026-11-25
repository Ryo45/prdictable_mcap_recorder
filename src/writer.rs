use std::fs::File;
use std::io::{Seek, Write};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mcap::records::MessageHeader;
use serde::Serialize;

use crate::config::Config;
use crate::fault::Stalls;
use crate::loss::{Layer, Losses, Reason};
use crate::metrics::{Histogram, Latency, ns};
use crate::pool::Buffer;
use crate::queue::{Pop, Queue};
use crate::source::{ENCODING, SCHEMA, SCHEMA_ENCODING, SCHEMA_NAME, TOPIC, channel_metadata};

pub enum Payload {
    Borrowed(Buffer),
    Owned(Buffer),
}

pub struct Frame {
    pub sequence: u64,
    pub capture_time_ns: u64,
    pub scheduled_time_ns: u64,
    pub payload: Payload,
    pub enqueued: Instant,
}

impl Frame {
    pub fn bytes(&self) -> &[u8] {
        match &self.payload {
            Payload::Borrowed(lease) | Payload::Owned(lease) => lease.bytes(),
        }
    }
}

pub struct McapSink<W: Write + Seek> {
    writer: mcap::Writer<W>,
    channel: u16,
}

impl<W: Write + Seek> McapSink<W> {
    pub fn new(output: W, config: &Config, start_unix_ns: u64) -> Result<Self> {
        let mut writer = mcap::WriteOptions::new()
            .library(concat!("predictable_recorder/", env!("CARGO_PKG_VERSION")))
            .use_chunks(false)
            .compression(None)
            .emit_summary_records(false)
            .emit_summary_offsets(false)
            .calculate_data_section_crc(true)
            .calculate_summary_section_crc(true)
            .create(output)?;
        let registration = (|| -> Result<_> {
            let schema = writer.add_schema(SCHEMA_NAME, SCHEMA_ENCODING, SCHEMA)?;
            let channel = writer.add_channel(
                schema,
                TOPIC,
                ENCODING,
                &channel_metadata(config.seed, config.frame_bytes, start_unix_ns),
            )?;
            Ok(channel)
        })();
        match registration {
            Ok(channel) => Ok(Self { writer, channel }),
            Err(error) => {
                // into_inner prevents implicit finalize-on-Drop after a partial write.
                let _ = writer.into_inner();
                Err(error)
            }
        }
    }

    pub fn write(&mut self, frame: &Frame) -> Result<()> {
        self.writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id: self.channel,
                    sequence: frame.sequence as u32,
                    log_time: frame.capture_time_ns,
                    publish_time: frame.scheduled_time_ns,
                },
                frame.bytes(),
            )
            .with_context(|| format!("write MCAP frame {}", frame.sequence))
    }

    fn abort(self) -> W {
        self.writer.into_inner()
    }

    fn finish(mut self) -> (Result<()>, W) {
        let result = self.writer.finish().map(|_| ()).context("finalize MCAP");
        (result, self.writer.into_inner())
    }
}

#[derive(Serialize)]
pub struct WriterReport {
    pub queue_wait_ns: Latency,
    pub write_call_ns: Latency,
    pub stall_wait_ns: u64,
    pub stall_wait_segments: u64,
    pub pacing_and_stall_while_owned_ns: u64,
    pub capture_to_empty_ns: u64,
    pub drain_ns: u64,
    pub finalize_ns: Option<u64>,
    pub flush_ns: Option<u64>,
    pub sync_ns: Option<u64>,
    pub capture_to_sync_ns: Option<u64>,
    pub finalized: bool,
    pub synced: bool,
    pub error: Option<String>,
}

pub struct Worker<'a> {
    pub queue: &'a Queue<Frame>,
    pub config: &'a Config,
    pub interval: Option<Duration>,
    pub aborted: &'a AtomicBool,
    pub stop_requested: &'a AtomicBool,
    pub start: Instant,
    pub start_unix_ns: u64,
}

impl Worker<'_> {
    fn consume(
        &self,
        losses: &mut Losses<'_>,
        mut write: impl FnMut(&Frame) -> Result<()>,
    ) -> WriterReport {
        let mut stalls = Stalls::new(self.config, self.start);
        let mut queue_wait = Histogram::default();
        let mut write_call = Histogram::default();
        let mut pacing_ns = 0_u64;
        let mut error = None;
        let progress = losses.progress;
        let skip_faults = || {
            self.stop_requested.load(Relaxed)
                || self.aborted.load(Relaxed)
                || progress.loss_record_failures.load(Relaxed) != 0
        };
        let closed_at = loop {
            let deadline = if skip_faults() { None } else { stalls.next() };
            match self.queue.pop_until(deadline) {
                Pop::Timeout => stalls.wait(skip_faults),
                Pop::Closed(at) => break at,
                Pop::Item(frame) => {
                    let dequeued = Instant::now();
                    queue_wait.record(ns(dequeued - frame.enqueued));
                    let sequence = frame.sequence;
                    if error.is_some() {
                        drop(frame);
                        losses.record(
                            sequence,
                            sequence + 1,
                            self.timestamp(),
                            Layer::Writer,
                            Reason::Unknown,
                        );
                        continue;
                    }
                    if let Some(interval) = self.interval {
                        stalls.pace(dequeued + interval, skip_faults);
                        pacing_ns = pacing_ns.saturating_add(ns(dequeued.elapsed()));
                    }
                    let time = Instant::now();
                    let result = write(&frame);
                    let elapsed = ns(time.elapsed());
                    drop(frame);
                    write_call.record(elapsed);
                    match result {
                        Ok(()) => {
                            progress.written.fetch_add(1, Relaxed);
                        }
                        Err(failure) => {
                            self.aborted.store(true, Relaxed);
                            error = Some(format!("{failure:#}"));
                            losses.record(
                                sequence,
                                sequence + 1,
                                self.timestamp(),
                                Layer::Writer,
                                Reason::WriterError,
                            );
                        }
                    }
                }
            }
        };
        WriterReport {
            queue_wait_ns: queue_wait.summary(),
            write_call_ns: write_call.summary(),
            stall_wait_ns: stalls.paused_ns,
            stall_wait_segments: stalls.pauses,
            pacing_and_stall_while_owned_ns: pacing_ns,
            capture_to_empty_ns: ns(self.start.elapsed()),
            drain_ns: ns(closed_at.elapsed()),
            finalize_ns: None,
            flush_ns: None,
            sync_ns: None,
            capture_to_sync_ns: None,
            finalized: false,
            synced: false,
            error,
        }
    }

    pub fn run(&self, mut losses: Losses<'_>, sink: Option<McapSink<File>>) -> WriterReport {
        let Some(mut sink) = sink else {
            return self.consume(&mut losses, |_| Ok(()));
        };
        let mut report = self.consume(&mut losses, |frame| sink.write(frame));
        if report.error.is_some() {
            drop(sink.abort());
            return report;
        }

        let result = (|| -> Result<()> {
            let timer = Instant::now();
            let (finished, mut file) = sink.finish();
            report.finalize_ns = Some(ns(timer.elapsed()));
            finished?;
            report.finalized = true;

            let timer = Instant::now();
            let flushed = file.flush();
            report.flush_ns = Some(ns(timer.elapsed()));
            flushed.context("flush MCAP")?;

            let timer = Instant::now();
            let synced = file.sync_data();
            report.sync_ns = Some(ns(timer.elapsed()));
            synced.context("sync MCAP")?;
            report.synced = true;
            report.capture_to_sync_ns = Some(ns(self.start.elapsed()));
            Ok(())
        })();
        report.error = result.err().map(|error| format!("{error:#}"));
        report
    }

    fn timestamp(&self) -> u64 {
        self.start_unix_ns.saturating_add(ns(self.start.elapsed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor, SeekFrom};
    use std::sync::mpsc;

    use clap::Parser;

    use crate::config::{Cli, Command};
    use crate::metrics::Progress;
    use crate::pool::Pool;
    use crate::source::fill;

    fn config() -> Config {
        let Cli {
            command: Command::Record(config),
        } = Cli::try_parse_from(["recorder", "record", "--frame-bytes", "128"]).unwrap()
        else {
            panic!("expected record")
        };
        config
    }

    fn frame(pool: &Pool, sequence: u64) -> Frame {
        let (mut buffer, _) = pool.acquire().unwrap();
        fill(buffer.bytes_mut(), 1, sequence);
        Frame {
            sequence,
            capture_time_ns: 101,
            scheduled_time_ns: 100,
            payload: Payload::Borrowed(buffer),
            enqueued: Instant::now(),
        }
    }

    struct FaultFile {
        file: Cursor<Vec<u8>>,
        max_per_call: usize,
        hard_limit: u64,
    }

    impl Write for FaultFile {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.file.position() >= self.hard_limit {
                return Err(io::Error::other("injected I/O error"));
            }
            let left = (self.hard_limit - self.file.position()).min(usize::MAX as u64) as usize;
            let count = bytes.len().min(self.max_per_call).min(left);
            self.file.write(&bytes[..count])
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Seek for FaultFile {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.file.seek(position)
        }
    }

    #[test]
    fn the_library_completes_short_writes_and_does_not_retry_a_failed_message() {
        let config = config();
        let pool = Pool::new(1, config.frame_bytes, None).unwrap();
        let frame = frame(&pool, (1 << 32) + 7);
        let output = FaultFile {
            file: Cursor::new(vec![]),
            max_per_call: 3,
            hard_limit: u64::MAX,
        };
        let mut sink = McapSink::new(output, &config, 100).unwrap();
        sink.write(&frame).unwrap();
        let (finished, output) = sink.finish();
        finished.unwrap();
        let bytes = output.file.into_inner();
        let messages = mcap::MessageStream::new(&bytes)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].sequence, 7);
        assert_eq!(messages[0].data.as_ref(), frame.bytes());

        let output = FaultFile {
            file: Cursor::new(vec![]),
            max_per_call: 3,
            hard_limit: u64::MAX,
        };
        let sink = McapSink::new(output, &config, 100).unwrap();
        let mut output = sink.writer.into_inner();
        // Fail after four bytes of the next record, then abort without finalizing.
        let header_bytes = output.file.position();
        output.hard_limit = header_bytes + 4;
        output.file.seek(SeekFrom::Start(0)).unwrap();
        let mut sink = McapSink::new(output, &config, 100).unwrap();
        assert!(sink.write(&frame).is_err());
        let output = sink.abort().file.into_inner();
        assert_eq!(output.len() as u64, header_bytes + 4);
        assert!(!output.ends_with(mcap::MAGIC));
    }

    #[test]
    fn writer_error_keeps_the_live_lease_and_releases_the_drained_queue() {
        let config = config();
        let pool = Pool::new(2, config.frame_bytes, Some(Duration::from_secs(1))).unwrap();
        let queue = Queue::new(3).unwrap();
        let progress = Progress::default();
        let aborted = AtomicBool::new(false);
        queue.try_push(frame(&pool, 4)).ok().unwrap();
        queue.try_push(frame(&pool, 7)).ok().unwrap();
        queue.close();
        let worker = Worker {
            queue: &queue,
            config: &config,
            interval: None,
            aborted: &aborted,
            stop_requested: &AtomicBool::new(false),
            start: Instant::now(),
            start_unix_ns: 100,
        };
        let (tx, rx) = mpsc::sync_channel(4);
        let mut losses = Losses::new(&progress, tx);
        let (owned_tx, owned_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let report = std::thread::scope(|scope| {
            let losses = &mut losses;
            let job = scope.spawn(move || {
                worker.consume(losses, |frame| {
                    assert_eq!(frame.sequence, 4);
                    owned_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    anyhow::bail!("partial file write");
                })
            });
            owned_rx.recv().unwrap();
            let unavailable = pool.acquire().is_none();
            release_tx.send(()).unwrap();
            assert!(unavailable);
            job.join().unwrap()
        });
        drop(losses);
        assert!(aborted.load(Relaxed));
        assert!(report.error.unwrap().contains("partial file write"));
        assert!(!report.finalized);
        assert_eq!(queue.levels().0, 0);
        assert_eq!(pool.levels(Instant::now()).free, 2);
        let events: Vec<_> = rx.into_iter().collect();
        assert_eq!(events.len(), 2);
        assert_eq!(
            (events[0].start_sequence, events[0].reason),
            (4, Reason::WriterError)
        );
        assert_eq!(
            (events[1].start_sequence, events[1].reason),
            (7, Reason::Unknown)
        );
    }
}
