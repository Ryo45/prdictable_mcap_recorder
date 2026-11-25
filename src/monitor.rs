use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anyhow::Result;
use rustix::time::ClockId;
use serde::Serialize;

use crate::config::Config;
use crate::loss::Event;
use crate::metrics::{Progress, cpu_ns, ns, rate, rss_bytes};
use crate::pool::Pool;
use crate::queue::Queue;

pub struct Monitor<'a, T> {
    pub config: &'a Config,
    pub start: Instant,
    pub progress: &'a Progress,
    pub dma: &'a Pool,
    pub spill: &'a Pool,
    pub queue: &'a Queue<T>,
}

#[derive(Default, Serialize)]
pub struct MonitorReport {
    pub samples: u64,
    pub metrics_sampling_wall_ns: u64,
    pub journal_io_wall_ns: u64,
    pub thread_cpu_ns: u64,
    pub loss_log_synced: bool,
    pub metrics_flushed: bool,
    pub warning_dma_reserve_samples: u64,
    pub warning_spill_low_samples: u64,
    pub warning_deadline_near_samples: u64,
    pub error: Option<String>,
}

#[derive(Default)]
struct Sample {
    elapsed_ns: u64,
    generated: u64,
    written: u64,
}

const CSV_HEADER: &str = concat!(
    "elapsed_ns,generated,received,written,input_bytes_per_sec,writer_bytes_per_sec,",
    "dma_total,dma_free,dma_held,dma_min_free,dma_earliest_deadline_in_ns,",
    "spill_total,spill_free,spill_held,spill_peak_bytes,queue_depth,queue_peak,",
    "copy_count,copy_bytes,source_deadline_miss,dma_pool_exhausted,spill_pool_exhausted,",
    "queue_full,writer_error,unknown,loss_record_failures,",
    "warn_dma_reserve,warn_spill_low,warn_dma_deadline_near,rss_bytes,peak_rss_bytes"
);

impl<T> Monitor<'_, T> {
    pub fn run(
        &self,
        stream: Receiver<Event>,
        loss_file: File,
        metrics_file: File,
    ) -> MonitorReport {
        let start_cpu = cpu_ns(ClockId::ThreadCPUTime);
        let mut loss = BufWriter::new(loss_file);
        let mut metrics = BufWriter::new(metrics_file);
        let mut report = MonitorReport::default();
        let mut previous = Sample::default();
        let interval = Duration::from_millis(self.config.metrics_interval_ms);
        let mut next = self.start + interval;
        let mut metrics_ok = writeln!(metrics, "{CSV_HEADER}")
            .map_err(|error| {
                report.error = Some(format!("metrics header: {error}"));
                self.progress.loss_record_failures.fetch_add(1, Relaxed);
            })
            .is_ok();
        let mut loss_ok = true;
        loop {
            let now = Instant::now();
            if now >= next {
                if metrics_ok {
                    metrics_ok = self.sample(&mut metrics, &mut report, &mut previous, now);
                }
                let elapsed_ns = ns(now.saturating_duration_since(self.start));
                let interval_ns = ns(interval);
                let until_next = interval_ns - (elapsed_ns % interval_ns);
                next = now + Duration::from_nanos(until_next);
            }
            match stream.recv_timeout(next.saturating_duration_since(Instant::now())) {
                Ok(event) if loss_ok => {
                    let timer = Instant::now();
                    let result = (|| -> Result<()> {
                        serde_json::to_writer(&mut loss, &event)?;
                        loss.write_all(b"\n")?;
                        Ok(())
                    })();
                    report.journal_io_wall_ns += ns(timer.elapsed());
                    if let Err(error) = result {
                        loss_ok = false;
                        self.loss_error(&mut report, format!("loss journal write: {error:#}"));
                    }
                }
                Ok(_) => {
                    self.progress.loss_record_failures.fetch_add(1, Relaxed);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        if metrics_ok && self.sample(&mut metrics, &mut report, &mut previous, Instant::now()) {
            match metrics.flush() {
                Ok(()) => report.metrics_flushed = true,
                Err(error) => {
                    self.progress.loss_record_failures.fetch_add(1, Relaxed);
                    report
                        .error
                        .get_or_insert_with(|| format!("metrics flush: {error}"));
                }
            }
        }
        if loss_ok {
            let timer = Instant::now();
            match loss.flush().and_then(|()| loss.get_ref().sync_data()) {
                Ok(()) => report.loss_log_synced = true,
                Err(error) => {
                    self.loss_error(&mut report, format!("loss journal flush/sync: {error}"))
                }
            }
            report.journal_io_wall_ns += ns(timer.elapsed());
        }
        // Do not retry a failed sidecar flush on Drop.
        let _ = metrics.into_parts();
        let _ = loss.into_parts();
        report.thread_cpu_ns = cpu_ns(ClockId::ThreadCPUTime) - start_cpu;
        report
    }

    fn loss_error(&self, report: &mut MonitorReport, error: String) {
        self.progress.loss_record_failures.fetch_add(1, Relaxed);
        report.error.get_or_insert(error);
    }

    fn sample(
        &self,
        metrics: &mut BufWriter<File>,
        report: &mut MonitorReport,
        previous: &mut Sample,
        now: Instant,
    ) -> bool {
        let timer = Instant::now();
        let sample = Sample {
            elapsed_ns: ns(now.saturating_duration_since(self.start)),
            generated: self.progress.generated.load(Relaxed),
            written: self.progress.written.load(Relaxed),
        };
        let size = self.config.frame_bytes as u64;
        let interval_ns = sample.elapsed_ns - previous.elapsed_ns;
        let input_rate = rate((sample.generated - previous.generated) * size, interval_ns);
        let output_rate = rate((sample.written - previous.written) * size, interval_ns);
        let dma = self.dma.levels(now);
        let spill = self.spill.levels(now);
        let (depth, peak) = self.queue.levels();
        let received = self.progress.received.load(Relaxed);
        let copied = self.progress.copied.load(Relaxed);
        let [source, dma_loss, spill_loss, queue, writer, unknown] = self.progress.loss_counts();
        let failures = self.progress.loss_record_failures.load(Relaxed);
        let dma_low = dma.free <= self.config.capture_reserve;
        let spill_low = spill.free <= 1;
        let deadline_near = dma
            .earliest_deadline_in_ns
            .is_some_and(|ns| ns <= (self.config.max_hold_ms * 1_000_000 / 5) as i64);
        let (rss, hwm) = rss_bytes();
        let optional = |value: Option<i64>| value.map(|n| n.to_string()).unwrap_or_default();
        let result = writeln!(
            metrics,
            "{},{},{},{},{input_rate:.0},{output_rate:.0},{},{},{},{},{},{},{},{},{},{depth},{peak},{copied},{},{source},{dma_loss},{spill_loss},{queue},{writer},{unknown},{failures},{},{},{},{},{}",
            sample.elapsed_ns, sample.generated, received, sample.written,
            dma.total, dma.free, dma.held, dma.min_free, optional(dma.earliest_deadline_in_ns),
            spill.total, spill.free, spill.held, (spill.total - spill.min_free) as u64 * size,
            copied * size, u8::from(dma_low), u8::from(spill_low), u8::from(deadline_near),
            optional(rss.map(|n| n as i64)), optional(hwm.map(|n| n as i64)),
        ).and_then(|()| metrics.flush());
        *previous = sample;
        report.samples += 1;
        report.warning_dma_reserve_samples += u64::from(dma_low);
        report.warning_spill_low_samples += u64::from(spill_low);
        report.warning_deadline_near_samples += u64::from(deadline_near);
        report.metrics_sampling_wall_ns += ns(timer.elapsed());
        if let Err(error) = result {
            self.progress.loss_record_failures.fetch_add(1, Relaxed);
            report
                .error
                .get_or_insert_with(|| format!("metrics sample: {error}"));
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::sync::mpsc;

    use crate::config::{Cli, Command};
    use crate::loss::{Layer, Reason};

    #[test]
    fn journal_flush_failure_is_observable() {
        let Cli {
            command: Command::Record(config),
        } = Cli::try_parse_from(["recorder", "record"]).unwrap()
        else {
            panic!("expected record")
        };
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let read_only = File::open(tempfile.path()).unwrap();
        let progress = Progress::default();
        let monitor = Monitor {
            config: &config,
            start: Instant::now(),
            progress: &progress,
            dma: &Pool::new(1, 10, None).unwrap(),
            spill: &Pool::new(1, 10, None).unwrap(),
            queue: &Queue::<()>::new(1).unwrap(),
        };
        let (tx, rx) = mpsc::sync_channel(2);
        tx.send(Event {
            stream_id: 0,
            start_sequence: 1,
            end_sequence_exclusive: 2,
            timestamp_unix_ns: 9,
            layer: Layer::Acquisition,
            reason: Reason::QueueFull,
        })
        .unwrap();
        drop(tx);
        let report = monitor.run(rx, read_only, tempfile::tempfile().unwrap());
        assert!(report.metrics_flushed);
        assert!(!report.loss_log_synced);
        assert_eq!(progress.loss_record_failures.load(Relaxed), 1);
        assert!(report.error.unwrap().contains("loss journal"));
    }
}
