use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use anyhow::Result;
use rustix::time::ClockId;
use serde::{Deserialize, Serialize};

use super::adapter::{Device, FrameId};
use super::config::{MS, Settings, US};
use super::metrics::{Progress, SourceProgress, Warning};
use super::pool::Pool;
use super::queue::Queue;
use crate::metrics::{cpu_ns, ns, rate, rss_bytes};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layer {
    Source,
    Acquisition,
    Scheduler,
    Writer,
    Commit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    SourceDeadlineMiss,
    SourceReportedDrop,
    DmaPoolExhausted,
    QueueFull,
    SpillPoolExhausted,
    CopyError,
    WriterNotSubmitted,
    WriterError,
    SyncFailure,
    Unknown,
}

impl Reason {
    pub fn valid(self, layer: Layer, accepted: bool) -> bool {
        match self {
            Self::SourceDeadlineMiss | Self::SourceReportedDrop => {
                layer == Layer::Source && !accepted
            }
            Self::DmaPoolExhausted | Self::QueueFull => layer == Layer::Acquisition && !accepted,
            Self::SpillPoolExhausted | Self::CopyError => layer == Layer::Scheduler && accepted,
            Self::WriterNotSubmitted | Self::WriterError => layer == Layer::Writer && accepted,
            Self::SyncFailure => layer == Layer::Commit && accepted,
            Self::Unknown => {
                (layer == Layer::Writer && accepted) || (layer == Layer::Source && !accepted)
            }
        }
    }

    pub fn uncertain(self) -> bool {
        matches!(self, Self::WriterError | Self::SyncFailure | Self::Unknown)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Span {
    pub stream: u16,
    pub start: u64,
    pub end: u64,
}

impl Span {
    pub fn frame(id: FrameId) -> Self {
        Self {
            stream: id.source,
            start: id.sequence,
            end: id.sequence + 1,
        }
    }
    pub fn count(self) -> u64 {
        self.end - self.start
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Loss {
    #[serde(flatten)]
    pub span: Span,
    pub timestamp_ns: u64,
    pub layer: Layer,
    pub reason: Reason,
    pub accepted: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommitLine {
    Group {
        group_id: u64,
        start_offset: u64,
        end_offset: u64,
        message_count: u64,
        synced_at_ns: u64,
        finalized: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix_crc32: Option<u32>,
    },
    Frames {
        group_id: u64,
        #[serde(flatten)]
        span: Span,
    },
}

struct CommitJournal {
    output: BufWriter<File>,
    remaining: Option<u64>,
}

impl CommitJournal {
    fn new(file: File) -> Self {
        Self {
            output: BufWriter::new(file),
            remaining: None,
        }
    }

    fn push(&mut self, event: CommitLine) -> Result<()> {
        line(&mut self.output, &event)?;
        match event {
            CommitLine::Group {
                prefix_crc32: Some(_),
                message_count,
                ..
            } => {
                self.remaining = Some(message_count);
            }
            CommitLine::Frames { span, .. } => {
                if let Some(remaining) = &mut self.remaining {
                    *remaining = remaining.saturating_sub(span.count());
                }
            }
            _ => {}
        }
        if self.remaining == Some(0) {
            self.sync()?;
            self.remaining = None;
        }
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.output.flush()?;
        self.output.get_ref().sync_data()
    }
}

pub enum Event {
    Loss(Loss),
    Commit(CommitLine),
}

pub struct Ledger<'a> {
    sender: SyncSender<Event>,
    progress: &'a Progress,
    sources: &'a [SourceProgress],
    // One coalescing slot per source, so unrelated streams do not fragment
    // a burst of consecutive device losses.
    pending: Vec<Option<Loss>>,
}

impl<'a> Ledger<'a> {
    pub fn new(
        sender: SyncSender<Event>,
        progress: &'a Progress,
        sources: &'a [SourceProgress],
    ) -> Self {
        Self {
            sender,
            progress,
            sources,
            pending: vec![None; sources.len()],
        }
    }

    pub fn loss(&mut self, span: Span, now: u64, layer: Layer, reason: Reason, accepted: bool) {
        assert!(span.start < span.end && reason.valid(layer, accepted));
        let source = &self.sources[usize::from(span.stream)];
        let count = span.count();
        if reason.uncertain() {
            source.unknown.fetch_add(count, Relaxed);
        } else {
            source.confirmed_loss.fetch_add(count, Relaxed);
            let stamp = now.saturating_add(1);
            let _ = source
                .first_loss_ns_plus_one
                .fetch_update(Relaxed, Relaxed, |previous| {
                    (previous == 0 || previous > stamp).then_some(stamp)
                });
            self.progress.warn(Warning::LossDetected, now);
            if accepted {
                source.after_accept_loss.fetch_add(count, Relaxed);
            } else {
                source.before_accept_loss.fetch_add(count, Relaxed);
            }
        }
        let index = usize::from(span.stream);
        let pending = &mut self.pending[index];
        if let Some(loss) = pending
            && loss.span.end == span.start
            && loss.layer == layer
            && loss.reason == reason
            && loss.accepted == accepted
        {
            loss.span.end = span.end;
        } else {
            let previous = pending.replace(Loss {
                span,
                timestamp_ns: now,
                layer,
                reason,
                accepted,
            });
            if let Some(previous) = previous {
                self.send(Event::Loss(previous));
            }
        }
    }

    pub fn commit(&self, entry: CommitLine) {
        self.send(Event::Commit(entry));
    }

    fn send(&self, event: Event) {
        if self.sender.try_send(event).is_err() {
            self.progress.journal_failures.fetch_add(1, Relaxed);
        }
    }
}

impl Drop for Ledger<'_> {
    fn drop(&mut self) {
        for index in 0..self.pending.len() {
            if let Some(loss) = self.pending[index].take() {
                self.send(Event::Loss(loss));
            }
        }
    }
}

pub struct Monitor<'a> {
    pub start: Instant,
    pub settings: &'a Settings,
    pub devices: &'a [Device],
    pub spill: &'a [Pool],
    pub sources: &'a [SourceProgress],
    pub progress: &'a Progress,
    pub queue: &'a Queue,
}

pub struct MonitorFiles {
    pub loss: File,
    pub commits: File,
    pub csv: File,
}

#[derive(Default, Serialize)]
pub struct MonitorReport {
    pub samples: u64,
    pub monitor_cpu_ns: u64,
    pub loss_synced: bool,
    pub commit_log_synced: bool,
    pub csv_flushed: bool,
    pub error: Option<String>,
}

#[derive(Default)]
struct Sample {
    elapsed: u64,
    written: u64,
    committed: u64,
    backlog_since: Option<(u64, u64)>,
}

impl Monitor<'_> {
    pub fn run(&self, stream: Receiver<Event>, files: MonitorFiles) -> MonitorReport {
        let cpu_start = cpu_ns(ClockId::ThreadCPUTime);
        let mut loss = BufWriter::new(files.loss);
        let mut commits = CommitJournal::new(files.commits);
        let mut csv = BufWriter::new(files.csv);
        let mut report = MonitorReport::default();
        let interval = Duration::from_millis(self.settings.record.metrics_interval_ms);
        let mut next = self.start + interval;
        let mut prev = Sample::default();
        let mut healthy = self.observe(writeln!(csv,
            "elapsed_ns,source,generated,accepted,written,committed,before_accept_loss,after_accept_loss,unknown,dma_available,dma_held,dma_returning,dma_reserve,dma_deadline_exceeded,pool_slack_ns,required_returns,spill_bytes_held,queue_depth,copying,copy_bytes,in_flight,in_flight_bytes,borrowed_in_flight,write_bytes_per_sec,commit_bytes_per_sec,written_minus_committed_bytes,at_risk,contract_violation,loss_detected,durability_unknown,rss_bytes"
        ).map_err(Into::into), &mut report);
        loop {
            if Instant::now() >= next {
                if healthy {
                    healthy = self.observe(self.sample(&mut csv, &mut prev), &mut report);
                    report.samples += u64::from(healthy);
                }
                let elapsed = ns(self.start.elapsed());
                next = Instant::now() + Duration::from_nanos(ns(interval) - elapsed % ns(interval));
            }
            match stream.recv_timeout(next.saturating_duration_since(Instant::now())) {
                Ok(event) if healthy => {
                    let result = match event {
                        Event::Loss(event) => line(&mut loss, &event),
                        Event::Commit(event) => commits.push(event),
                    };
                    healthy = self.observe(result, &mut report);
                }
                Ok(_) => {
                    self.progress.journal_failures.fetch_add(1, Relaxed);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        if healthy {
            report.csv_flushed = self.observe(self.sample(&mut csv, &mut prev), &mut report);
            report.samples += u64::from(report.csv_flushed);
        }
        // Sync both sidecars even if one fails. A partially written sidecar is
        // never sufficient evidence of success.
        report.loss_synced = self.observe(
            loss.flush()
                .and_then(|()| loss.get_ref().sync_data())
                .map_err(Into::into),
            &mut report,
        );
        report.commit_log_synced = self.observe(commits.sync().map_err(Into::into), &mut report);
        let _ = loss.into_parts();
        let _ = commits.output.into_parts();
        let _ = csv.into_parts();
        report.monitor_cpu_ns = cpu_ns(ClockId::ThreadCPUTime) - cpu_start;
        report
    }

    fn observe(&self, result: Result<()>, report: &mut MonitorReport) -> bool {
        if let Err(error) = result {
            report
                .error
                .get_or_insert_with(|| format!("monitor sidecar: {error:#}"));
            self.progress.journal_failures.fetch_add(1, Relaxed);
            false
        } else {
            true
        }
    }

    fn sample(&self, csv: &mut BufWriter<File>, prev: &mut Sample) -> Result<()> {
        let now = ns(self.start.elapsed());
        let p = self.progress;
        let written = p.written_bytes.load(Relaxed);
        let committed = p.committed_bytes.load(Relaxed);
        let wire = p.written_wire_bytes.load(Relaxed);
        let write_rate = rate(written - prev.written, now - prev.elapsed);
        let commit_rate = rate(committed - prev.committed, now - prev.elapsed);
        let queue = self.queue.snapshot();
        let last_empty = p.queue_last_empty_ns.load(Relaxed);
        if queue.depth == 0 {
            prev.backlog_since = None;
        } else if prev
            .backlog_since
            .is_none_or(|(elapsed, _)| last_empty > elapsed)
        {
            prev.backlog_since = Some((now, wire));
        }
        if let Some((backlog_started, first_wire)) = prev.backlog_since {
            // Runtime checks the conservative rate-latency envelope only when
            // work stayed available for an entire observation window.
            let profile = &self.settings.storage_profile;
            if let (Some(raw), Some(stall), Some(io), Some(sync)) = (
                profile.sustained_bytes_per_sec,
                profile.max_stall_ms,
                profile.max_io_us,
                profile.max_sync_ms,
            ) {
                let effective = profile
                    .min_stall_period_ms
                    .filter(|&period| period > stall)
                    .map_or(raw, |period| {
                        (u128::from(raw) * u128::from(period - stall) / u128::from(period)) as u64
                    });
                let available = (now - backlog_started).saturating_sub(
                    (stall * MS)
                        .saturating_add(io * US)
                        .saturating_add(sync * MS),
                );
                if u128::from(wire - first_wire) * 1_000_000_000
                    < u128::from(effective) * u128::from(available)
                {
                    p.warn(Warning::ContractViolation, now);
                }
            }
        }
        prev.elapsed = now;
        prev.written = written;
        prev.committed = committed;
        let (rss, _) = rss_bytes();
        let spill_held_bytes: usize = self
            .spill
            .iter()
            .map(|pool| pool.levels(now).held * pool.size())
            .sum();
        for (source, device) in self.sources.iter().zip(self.devices) {
            let counters = source.report();
            let pool = device.pool.levels(now);
            writeln!(
                csv,
                "{now},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{spill_held_bytes},{},{},{},{},{},{},{write_rate:.0},{commit_rate:.0},{},{},{},{},{},{}",
                device.source.id,
                counters.generated,
                counters.accepted,
                counters.written,
                counters.committed,
                counters.before_accept_loss,
                counters.after_accept_loss,
                counters.unknown,
                pool.available,
                pool.held,
                pool.returning,
                pool.reserve,
                pool.deadline_exceeded,
                source.last_slack_ns.load(Relaxed),
                source.required_returns.load(Relaxed),
                queue.depth,
                queue.copying,
                p.copy_bytes.load(Relaxed),
                p.in_flight.load(Relaxed),
                p.in_flight_bytes.load(Relaxed),
                p.borrowed_in_flight.load(Relaxed),
                written.saturating_sub(committed),
                p.warning_count(Warning::AtRisk),
                p.warning_count(Warning::ContractViolation),
                p.warning_count(Warning::LossDetected),
                p.warning_count(Warning::DurabilityUnknown),
                rss.unwrap_or(0),
            )?;
        }
        csv.flush()?;
        Ok(())
    }
}

fn line(output: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}
