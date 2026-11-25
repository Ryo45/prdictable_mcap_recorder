use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use rustix::time::ClockId;
use serde::Serialize;
use serde_json::json;

use crate::config::{Config, Plan, Sink};
use crate::fault::sleep_until;
use crate::loss::{Layer, LossCounts, Losses, Reason};
use crate::metrics::{Histogram, Latency, Progress, cpu_ns, ns, rate, rss_bytes};
use crate::monitor::Monitor;
use crate::pool::{Buffer, Pool};
use crate::queue::Queue;
use crate::report::{CoreReport, Outputs, REPORT_VERSION, Status, Uncertain, create_new};
use crate::source::{current_slot, fill, slot_time_ns};
use crate::stop;
use crate::writer::{Frame, McapSink, Payload, Worker};

const START_DELAY: Duration = Duration::from_millis(30);

#[derive(Serialize)]
struct CaptureReport {
    wall_ns: u64,
    generation_delay_ns: Latency,
    generation_time_ns: Latency,
    copy_time_ns: Latency,
    first_unattempted: Option<u64>,
}

#[derive(Default)]
struct CaptureTiming {
    generation_delay: Histogram,
    generation_time: Histogram,
    copy_time: Histogram,
}

struct Capture<'a> {
    config: &'a Config,
    plan: &'a Plan,
    start: Instant,
    start_unix_ns: u64,
    dma: &'a Pool,
    spill: &'a Pool,
    queue: &'a Queue<Frame>,
    aborted: &'a AtomicBool,
    stop_requested: &'a AtomicBool,
}

impl Capture<'_> {
    fn run(&self, mut losses: Losses<'_>) -> CaptureReport {
        let progress = losses.progress;
        let mut timing = CaptureTiming::default();
        let expected = self.plan.expected;
        let mut sequence = 0;
        let should_stop = || {
            self.stop_requested.load(Relaxed)
                || self.aborted.load(Relaxed)
                || progress.loss_record_failures.load(Relaxed) != 0
        };
        let mut first_unattempted = None;
        while sequence < expected {
            let due_ns = slot_time_ns(sequence, self.config.fps);
            sleep_until(self.start + Duration::from_nanos(due_ns), should_stop);
            let elapsed_ns = ns(self.start.elapsed());
            let timestamp = self.start_unix_ns.saturating_add(elapsed_ns);
            if should_stop() {
                first_unattempted = Some(sequence);
                losses.record(
                    sequence,
                    expected,
                    timestamp,
                    Layer::Source,
                    Reason::Unknown,
                );
                break;
            }
            let latest = if elapsed_ns >= self.plan.duration_ns {
                expected
            } else {
                current_slot(elapsed_ns, self.config.fps)
            };
            if latest > sequence {
                losses.record(
                    sequence,
                    latest,
                    timestamp,
                    Layer::Source,
                    Reason::SourceDeadlineMiss,
                );
                sequence = latest;
                if sequence == expected {
                    break;
                }
            }

            if let Err(reason) = self.receive(sequence, elapsed_ns, progress, &mut timing) {
                losses.record(
                    sequence,
                    sequence + 1,
                    timestamp,
                    Layer::Acquisition,
                    reason,
                );
            }
            sequence += 1;
        }
        if first_unattempted.is_none() {
            sleep_until(self.start + self.config.duration_sec, should_stop);
        }
        self.queue.close();
        CaptureReport {
            wall_ns: ns(self.start.elapsed()),
            generation_delay_ns: timing.generation_delay.summary(),
            generation_time_ns: timing.generation_time.summary(),
            copy_time_ns: timing.copy_time.summary(),
            first_unattempted,
        }
    }

    fn receive(
        &self,
        sequence: u64,
        elapsed_ns: u64,
        progress: &Progress,
        timing: &mut CaptureTiming,
    ) -> std::result::Result<(), Reason> {
        let (mut lease, free) = self.dma.acquire().ok_or(Reason::DmaPoolExhausted)?;
        let due_ns = slot_time_ns(sequence, self.config.fps);
        timing.generation_delay.record(elapsed_ns - due_ns);
        let timer = Instant::now();
        fill(lease.bytes_mut(), self.config.seed, sequence);
        timing.generation_time.record(ns(timer.elapsed()));
        progress.generated.fetch_add(1, Relaxed);

        let payload = select_payload(
            self.config,
            lease,
            free,
            self.spill,
            progress,
            &mut timing.copy_time,
        )?;
        self.queue
            .try_push(Frame {
                sequence,
                capture_time_ns: self.start_unix_ns + elapsed_ns,
                scheduled_time_ns: self.start_unix_ns + due_ns,
                payload,
                enqueued: Instant::now(),
            })
            .map_err(|_| Reason::QueueFull)?;
        progress.received.fetch_add(1, Relaxed);
        Ok(())
    }
}

fn select_payload(
    config: &Config,
    lease: Buffer,
    free_after_acquire: usize,
    spill: &Pool,
    progress: &Progress,
    copy_time: &mut Histogram,
) -> std::result::Result<Payload, Reason> {
    use crate::config::Policy::{Adaptive, Borrow, Copy};
    let copy = match config.policy {
        Copy => true,
        Borrow => false,
        Adaptive => free_after_acquire <= config.capture_reserve,
    };
    if !copy {
        return Ok(Payload::Borrowed(lease));
    }
    let Some((mut owned, _)) = spill.acquire() else {
        return Err(Reason::SpillPoolExhausted);
    };
    let timer = Instant::now();
    owned.bytes_mut().copy_from_slice(lease.bytes());
    copy_time.record(ns(timer.elapsed()));
    drop(lease);
    progress.copied.fetch_add(1, Relaxed);
    Ok(Payload::Owned(owned))
}

pub fn run(config: Config) -> Result<()> {
    let wall_start = Instant::now();
    let cpu_start = cpu_ns(ClockId::ProcessCPUTime);
    let plan = config.plan()?;
    let outputs = Outputs::new(&config.output)?;
    config.check_output_space(&plan)?;
    outputs.check_new()?;
    eprintln!(
        "Planned {} frames, {} payload bytes at {} B/s, {} preallocated pool bytes{}",
        plan.expected,
        plan.payload_bytes,
        plan.input_bytes_per_sec,
        plan.pool_bytes,
        if config.sink == Sink::Null {
            " (NullSink)"
        } else {
            ""
        },
    );

    stop::install().context("install stop handlers")?;
    let dma = Pool::new(
        config.dma_buffers,
        config.frame_bytes,
        Some(Duration::from_millis(config.max_hold_ms)),
    )
    .context("preallocate DMA pool")?;
    let spill = Pool::new(config.spill_buffers, config.frame_bytes, None)
        .context("preallocate spill pool")?;
    let queue = Queue::new(config.queue_capacity).context("preallocate writer queue")?;
    let progress = Progress::default();
    // Reserve names exclusively, including after the preflight collision check.
    let mut summary_file = create_new(&outputs.summary)?;
    let losses_file = create_new(&outputs.losses)?;
    let metrics_file = create_new(&outputs.metrics)?;
    let data = if config.sink == Sink::Mcap {
        Some(create_new(&outputs.data)?)
    } else {
        None
    };

    let start = Instant::now() + START_DELAY;
    let start_unix_ns =
        u64::try_from((SystemTime::now().duration_since(UNIX_EPOCH)? + START_DELAY).as_nanos())?;
    ensure!(
        start_unix_ns.checked_add(plan.duration_ns).is_some(),
        "capture timestamp range overflow"
    );
    let setup = data
        .map(|file| McapSink::new(file, &config, start_unix_ns))
        .transpose();
    let (sink, init_error) = match setup {
        Ok(sink) => (sink, None),
        Err(error) => (None, Some(format!("initialize MCAP: {error:#}"))),
    };
    let aborted = AtomicBool::new(init_error.is_some());
    let stop_requested = &stop::REQUESTED;
    let capture = Capture {
        config: &config,
        plan: &plan,
        start,
        start_unix_ns,
        dma: &dma,
        spill: &spill,
        queue: &queue,
        aborted: &aborted,
        stop_requested,
    };
    let worker = Worker {
        config: &config,
        interval: plan.writer_interval,
        queue: &queue,
        aborted: &aborted,
        stop_requested,
        start,
        start_unix_ns,
    };
    let monitor = Monitor {
        config: &config,
        start,
        progress: &progress,
        dma: &dma,
        spill: &spill,
        queue: &queue,
    };
    let (loss_tx, loss_rx) = mpsc::sync_channel(config.loss_queue_capacity);
    let writer_losses = Losses::new(&progress, loss_tx.clone());
    let (capture_report, writer_report, monitor_report) = thread::scope(|scope| {
        let monitoring = scope.spawn(move || monitor.run(loss_rx, losses_file, metrics_file));
        let writing = scope.spawn(move || worker.run(writer_losses, sink));
        let capture_report = capture.run(Losses::new(&progress, loss_tx));
        (
            capture_report,
            writing.join().unwrap(),
            monitoring.join().unwrap(),
        )
    });

    let finished = Instant::now();
    let dma_report = dma.report(finished);
    let spill_report = spill.report(finished);
    let peak_dma_bytes =
        (dma_report.levels.total - dma_report.levels.min_free) * config.frame_bytes;
    let peak_spill_bytes =
        (spill_report.levels.total - spill_report.levels.min_free) * config.frame_bytes;
    let (queue_depth, queue_peak) = queue.levels();
    let generated = progress.generated.load(Relaxed);
    let received = progress.received.load(Relaxed);
    let written = progress.written.load(Relaxed);
    let copies = progress.copied.load(Relaxed);
    let loss_record_failures = progress.loss_record_failures.load(Relaxed);
    let losses = LossCounts::from(progress.loss_counts());
    let mut errors: Vec<String> = [
        init_error,
        writer_report.error.clone(),
        monitor_report.error.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();
    if loss_record_failures != 0 {
        errors.push(format!(
            "{loss_record_failures} loss or monitoring record failures"
        ));
    }
    if dma_report.levels.held != 0 || spill_report.levels.held != 0 || queue_depth != 0 {
        errors.push("buffers or queue were not fully returned".into());
    }
    let stop_signal = stop_requested.load(Relaxed);
    if errors.is_empty()
        && !stop_signal
        && (written != received
            || plan
                .expected
                .checked_sub(losses.source_deadline_miss + losses.dma_pool_exhausted)
                != Some(generated)
            || generated.checked_sub(losses.spill_pool_exhausted + losses.queue_full)
                != Some(received)
            || losses.writer_error != 0
            || losses.unknown != 0)
    {
        errors.push("recording counts do not reconcile".into());
    }
    let status = if !errors.is_empty() {
        Status::Error
    } else if stop_signal {
        Status::Cancelled
    } else {
        Status::Ok
    };
    let (uncertain_start, uncertain_reason) = if config.sink == Sink::Mcap && !writer_report.synced
    {
        (Some(0), "mcap_durability_unconfirmed")
    } else if loss_record_failures != 0 {
        (Some(0), "loss_journal_incomplete")
    } else {
        (capture_report.first_unattempted, "capture_aborted")
    };
    let uncertain_ranges = uncertain_start
        .into_iter()
        .map(|start_sequence| Uncertain {
            start_sequence,
            end_sequence_exclusive: plan.expected,
            reason: uncertain_reason.into(),
        })
        .collect();
    let core = CoreReport {
        report_version: REPORT_VERSION,
        status,
        config,
        start_unix_ns,
        expected: plan.expected,
        generated,
        received,
        written,
        losses,
        loss_record_failures,
        mcap_finalized: writer_report.finalized,
        mcap_synced: writer_report.synced,
        loss_log_synced: monitor_report.loss_log_synced,
        uncertain_ranges,
    };
    let size = core.config.frame_bytes as u64;
    let output_bytes = written * size;
    let (rss, peak_rss) = rss_bytes();
    let wall_ns = ns(wall_start.elapsed());
    let cpu_time_ns = cpu_ns(ClockId::ProcessCPUTime) - cpu_start;
    let mut report = serde_json::to_value(&core)?;
    let metrics = json!({
        "planned_payload_bytes": plan.payload_bytes,
        "planned_input_bytes_per_sec": plan.input_bytes_per_sec,
        "estimated_full_mcap_bytes": (core.config.sink == Sink::Mcap).then_some(plan.estimated_mcap_bytes),
        "input_bytes_per_sec": rate(generated * size, capture_report.wall_ns),
        "writer_payload_bytes": output_bytes,
        "writer_bytes_per_sec": rate(output_bytes, writer_report.capture_to_empty_ns),
        "synced_payload_bytes_per_sec": writer_report.capture_to_sync_ns.map(|elapsed| rate(output_bytes, elapsed)),
        "pool_allocated_bytes": plan.pool_bytes,
        "dma_peak_bytes": peak_dma_bytes,
        "spill_peak_bytes": peak_spill_bytes,
        "copy_count": copies,
        "copy_bytes": copies * size,
        "queue_depth": queue_depth,
        "queue_peak": queue_peak,
        "capture": capture_report,
        "dma": dma_report,
        "spill": spill_report,
        "writer": writer_report,
        "monitor": monitor_report,
        "process": {"cpu_time_ns": cpu_time_ns, "rss_bytes": rss, "peak_rss_bytes": peak_rss, "wall_ns": wall_ns},
        "errors": errors,
    });
    let serde_json::Value::Object(metrics) = metrics else {
        unreachable!()
    };
    report.as_object_mut().unwrap().extend(metrics);
    serde_json::to_writer_pretty(&mut summary_file, &report).context("write summary")?;
    summary_file
        .write_all(b"\n")
        .context("write summary newline")?;
    summary_file.sync_data().context("sync summary")?;

    if monitor_report.warning_dma_reserve_samples != 0
        || monitor_report.warning_spill_low_samples != 0
        || monitor_report.warning_deadline_near_samples != 0
    {
        eprintln!(
            "Warning samples: DMA reserve {}, spill low {}, DMA deadline near or exceeded {}",
            monitor_report.warning_dma_reserve_samples,
            monitor_report.warning_spill_low_samples,
            monitor_report.warning_deadline_near_samples
        );
    }
    eprintln!(
        "{status:?}: {} planned, {generated} generated, {received} received, {written} written, {copies} copied. Summary {}",
        plan.expected,
        outputs.summary.display()
    );
    match status {
        Status::Ok => Ok(()),
        Status::Cancelled => anyhow::bail!("stop requested during recording"),
        Status::Error => anyhow::bail!("recording failed: {}", errors.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Cli, Command, Policy};
    use crate::queue::Pop;
    use crate::source::frame_id;
    use clap::Parser;

    #[test]
    fn adaptive_uses_the_reserve_boundary_and_releases_rejected_buffers() {
        let Cli {
            command: Command::Record(mut config),
        } = Cli::try_parse_from(["recorder", "record"]).unwrap()
        else {
            panic!("expected record")
        };
        config.policy = Policy::Adaptive;
        config.frame_bytes = 30;
        config.dma_buffers = 3;
        config.capture_reserve = 1;
        config.spill_buffers = 2;
        config.queue_capacity = 2;
        let plan = config.plan().unwrap();
        let dma = Pool::new(3, 30, Some(Duration::from_secs(1))).unwrap();
        let spill = Pool::new(2, 30, None).unwrap();
        let queue = Queue::new(2).unwrap();
        let capture = Capture {
            config: &config,
            plan: &plan,
            start: Instant::now(),
            start_unix_ns: 1,
            dma: &dma,
            spill: &spill,
            queue: &queue,
            aborted: &AtomicBool::new(false),
            stop_requested: &AtomicBool::new(false),
        };
        let progress = Progress::default();
        let mut timing = CaptureTiming::default();
        let attempt = |id, timing: &mut CaptureTiming| {
            capture.receive(id, slot_time_ns(id, config.fps), &progress, timing)
        };
        attempt(0, &mut timing).unwrap(); // Two free DMA slots, so borrow.
        assert_eq!(dma.levels(Instant::now()).free, 2);
        attempt(1, &mut timing).unwrap(); // One free DMA slot, so copy.
        assert_eq!(dma.levels(Instant::now()).free, 2);
        assert_eq!(progress.copied.load(Relaxed), 1);
        let held_spill = spill.acquire().unwrap().0;
        assert_eq!(attempt(2, &mut timing), Err(Reason::SpillPoolExhausted));
        assert_eq!(dma.levels(Instant::now()).free, 2);
        drop(held_spill);
        assert_eq!(attempt(3, &mut timing), Err(Reason::QueueFull));
        assert_eq!(dma.levels(Instant::now()).free, 2);
        assert_eq!(spill.levels(Instant::now()).free, 1);

        let spare_one = dma.acquire().unwrap().0;
        let spare_two = dma.acquire().unwrap().0;
        assert_eq!(attempt(4, &mut timing), Err(Reason::DmaPoolExhausted));
        drop((spare_one, spare_two));

        queue.close();
        let Pop::Item(first) = queue.pop_until(None) else {
            panic!("expected frame")
        };
        let Pop::Item(second) = queue.pop_until(None) else {
            panic!("expected frame")
        };
        assert!(matches!(first.payload, Payload::Borrowed(_)));
        assert!(matches!(second.payload, Payload::Owned(_)));
        assert_eq!(frame_id(second.bytes()), Some((1, 0)));
        drop((first, second));
        assert!(matches!(queue.pop_until(None), Pop::Closed(_)));
        assert_eq!(dma.levels(Instant::now()).free, 3);
        assert_eq!(spill.levels(Instant::now()).free, 2);
    }

    #[test]
    fn past_schedule_and_stop_consume_the_correct_id_ranges() {
        let Cli {
            command: Command::Record(config),
        } = Cli::try_parse_from(["recorder", "record", "--fps", "10"]).unwrap()
        else {
            panic!("expected record")
        };
        let plan = config.plan().unwrap();
        let dma = Pool::new(4, 10, None).unwrap();
        let spill = Pool::new(4, 10, None).unwrap();
        let progress = Progress::default();
        for (requested, reason) in [(false, Reason::SourceDeadlineMiss), (true, Reason::Unknown)] {
            let queue = Queue::new(4).unwrap();
            let capture = Capture {
                config: &config,
                plan: &plan,
                start: Instant::now() - Duration::from_secs(5),
                start_unix_ns: 1,
                dma: &dma,
                spill: &spill,
                queue: &queue,
                aborted: &AtomicBool::new(false),
                stop_requested: &AtomicBool::new(requested),
            };
            let (tx, rx) = mpsc::sync_channel(2);
            let report = capture.run(Losses::new(&progress, tx));
            let event = rx.recv().unwrap();
            assert_eq!(
                (event.start_sequence, event.end_sequence_exclusive),
                (0, 10)
            );
            assert_eq!(event.reason, reason);
            assert_eq!(report.first_unattempted, requested.then_some(0));
            assert!(matches!(queue.pop_until(None), Pop::Closed(_)));
        }
        assert_eq!(progress.generated.load(Relaxed), 0);
        assert_eq!(dma.levels(Instant::now()).held, 0);
        assert_eq!(spill.levels(Instant::now()).held, 0);
    }
}
