mod group;

use std::io;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::Serialize;

use self::group::Group;
use super::adapter::Device;
use super::config::{CopyScheduling, Faults, MS, Settings, US, WriterMode, transfer_time};
use super::integrity::{Layer, Ledger, Reason, Span};
use super::layout::Layout;
use super::metrics::{Progress, SourceProgress, Warning};
use super::queue::Queue;
use super::storage::{Backend, Completion};
use crate::fault::sleep_until;
use crate::metrics::ns;

const ASYNC_POLL_NS: u64 = 100_000;
const IDLE_POLL_NS: u64 = 1_000_000;

#[derive(Default, Serialize)]
pub struct WriterReport {
    pub groups: u64,
    pub finalized: bool,
    pub final_sync: bool,
    pub data_end_offset: u64,
    pub end_elapsed_ns: u64,
    pub bytes_accepted_by_storage: u64,
    pub peak_completion_reorder: u64,
    pub error: Option<String>,
}

pub struct Writer<'a> {
    pub trace: Option<&'a super::copy_trace::CopyTrace>,
    pub start: Instant,
    pub settings: &'a Settings,
    pub devices: &'a [Device],
    pub sources: &'a [SourceProgress],
    pub progress: &'a Progress,
    pub queue: &'a Queue,
}

impl Writer<'_> {
    pub fn run(
        &self,
        mut ledger: Ledger<'_>,
        mut backend: Backend,
        mut layout: Layout,
        preamble: &[u8],
    ) -> WriterReport {
        let mut report = WriterReport::default();
        let mut group = Group::new(self.settings);
        let mut broken_ring = false;
        let mut next_stall = Stalls::new(&self.settings.faults);
        let mut next_submit_ns = 0;
        let mut completion_high_water = 0_u64;
        let mut submit_count = 0_u64;
        if let Err(error) = backend.write_control(0, preamble) {
            self.abort(&mut report, format!("MCAP preamble: {error}"));
        }
        let p = self.progress;
        loop {
            let queue = self.queue.snapshot();
            let now = self.now();
            let draining_error = report.error.is_some() || p.stopped();
            let stopping = crate::stop::REQUESTED.load(Relaxed);
            if queue.depth == 0 {
                p.queue_last_empty_ns.store(now, Relaxed);
            }

            if !(draining_error || stopping || queue.closed && queue.depth == 0) {
                next_stall.pause(self, now);
            }

            if !broken_ring {
                loop {
                    let poll_at =
                        if stopping && self.settings.record.writer == WriterMode::MockAsync {
                            u64::MAX
                        } else {
                            self.now()
                        };
                    match backend.completion(poll_at) {
                        Ok(Some(completion)) => {
                            let id = completion.request.id;
                            let order = completion_high_water.saturating_sub(id);
                            completion_high_water = completion_high_water.max(id);
                            report.peak_completion_reorder =
                                report.peak_completion_reorder.max(order);
                            self.completed(completion, &mut group, &mut report, &mut ledger);
                        }
                        Ok(None) => break,
                        Err(error) => {
                            broken_ring = true;
                            self.abort(
                                &mut report,
                                format!("backend completion transport: {error}"),
                            );
                            break;
                        }
                    }
                }
            }

            let now = self.now();
            let inflight = p.in_flight.load(Relaxed) as usize;
            let queue = self.queue.snapshot();
            if report.error.is_some() || p.stopped() {
                if let Some(job) = self.queue.claim_write(true, true, |_| false) {
                    ledger.loss(
                        Span::frame(job.frame.id),
                        now,
                        Layer::Writer,
                        Reason::WriterNotSubmitted,
                        true,
                    );
                    drop(job);
                    continue;
                }
                if queue.closed && queue.depth == 0 && (inflight == 0 || broken_ring) {
                    group.unknown(&mut ledger, now, Reason::Unknown);
                    if !group.is_empty() {
                        p.warn(Warning::DurabilityUnknown, now);
                    }
                    if report.error.is_none() {
                        report.error = Some("runtime monitor or journal failed".into());
                    }
                    break;
                }
            } else {
                let barrier = group.due(now, layout.offset, self.settings);
                if queue.closed && queue.depth == 0 && inflight == 0 {
                    let tail = layout.finish();
                    if let Err(error) = backend.write_control(layout.offset, &tail) {
                        self.abort(&mut report, format!("MCAP footer: {error}"));
                        group.unknown(&mut ledger, self.now(), Reason::Unknown);
                        p.warn(Warning::DurabilityUnknown, self.now());
                        break;
                    }
                    layout.offset += tail.len() as u64;
                    report.finalized = true;
                    report.final_sync = self.sync(
                        &backend,
                        &mut group,
                        (layout.offset, layout.prefix_crc()),
                        true,
                        &mut report,
                        &mut ledger,
                    );
                    break;
                }
                if barrier && inflight == 0 {
                    self.sync(
                        &backend,
                        &mut group,
                        (layout.offset, layout.prefix_crc()),
                        false,
                        &mut report,
                        &mut ledger,
                    );
                    continue;
                }
                let paced = !stopping && now < next_submit_ns;
                if paced {
                    p.writer_blocked_until_ns.store(next_submit_ns, Relaxed);
                } else if !barrier && inflight < self.settings.depth() {
                    let may_borrow = p.borrowed_in_flight.load(Relaxed)
                        < self.settings.record.max_borrowed_in_flight as u64;
                    let io_bound = self.settings.storage_profile.max_io_us.unwrap_or(1_000) * US;
                    if let Some(job) = self.queue.claim_write(may_borrow, false, |job| {
                        let device = &self.devices[usize::from(job.frame.id.source)];
                        let defer = self.settings.record.policy.adaptive()
                            && job
                                .frame
                                .borrowed()
                                .and_then(|lease| lease.deadline_ns)
                                .is_some_and(|deadline| {
                                    deadline
                                        <= now
                                            .saturating_add(io_bound)
                                            .saturating_add(device.source.return_delay_us * US)
                                });
                        if defer && self.settings.record.copy_scheduling == CopyScheduling::Global {
                            p.global_copy
                                .borrow_admission_deferrals
                                .fetch_add(1, Relaxed);
                        }
                        defer
                    }) {
                        let frame = job.frame;
                        if let Some(trace) = self.trace {
                            trace.frame(
                                "writer_submit",
                                now,
                                &frame,
                                serde_json::json!({
                                    "request": submit_count, "borrowed": frame.borrowed().is_some(),
                                }),
                            );
                        }
                        let source = &self.sources[usize::from(frame.id.source)];
                        let size = frame.size as u64;
                        p.queue_wait
                            .lock()
                            .unwrap()
                            .record(now.saturating_sub(frame.queued_ns));
                        let id = group.submit(frame.id, size, now, frame.queued_ns);
                        debug_assert_eq!(id, submit_count);
                        let borrowed = frame.borrowed().is_some();
                        let request = layout.message(id, frame, now);
                        source.submitted.fetch_add(1, Relaxed);
                        submit_count += 1;
                        bump(&p.in_flight, &p.peak_in_flight, 1);
                        bump(&p.in_flight_bytes, &p.peak_in_flight_bytes, size);
                        if borrowed {
                            bump(&p.borrowed_in_flight, &p.peak_borrowed_in_flight, 1);
                        }
                        let timer = Instant::now();
                        let submitted = backend.submit(request, self.now());
                        p.write_busy_ns.fetch_add(ns(timer.elapsed()), Relaxed);
                        if let Err(error) = submitted {
                            // Ring transport failure cannot prove its requests are quiescent.
                            broken_ring = true;
                            self.abort(&mut report, format!("submit request {id}: {error}"));
                        }
                        if let Some(limit) = self.settings.faults.writer_bytes_per_sec {
                            next_submit_ns = now.saturating_add(transfer_time(size, limit));
                        }
                        continue;
                    }
                }
            }
            let poll = if inflight == 0 {
                IDLE_POLL_NS
            } else {
                ASYNC_POLL_NS
            };
            self.queue
                .wait_change(queue.version, Duration::from_nanos(poll));
        }
        report.groups = group.id();
        report.end_elapsed_ns = self.now();
        report.data_end_offset = layout.offset;
        report.bytes_accepted_by_storage = p.written_bytes.load(Relaxed);
        report
    }

    fn completed(
        &self,
        completion: Completion,
        group: &mut Group,
        report: &mut WriterReport,
        ledger: &mut Ledger<'_>,
    ) {
        let now = self.now();
        let frame = completion.request.frame;
        let size = frame.size as u64;
        let borrowed = frame.borrowed().is_some();
        let id = frame.id;
        let request_id = completion.request.id;
        let submitted_ns = completion.request.submitted_ns;
        // Original completion is the point where we can release the lease.
        if let Some(trace) = self.trace {
            let reusable = borrowed.then(|| {
                now.saturating_add(self.devices[usize::from(id.source)].source.return_delay_us * US)
            });
            trace.frame(
                "writer_completion",
                now,
                &frame,
                serde_json::json!({
                    "request": request_id, "borrowed": borrowed, "submitted_ns": submitted_ns,
                    "ack_estimate_ns": reusable, "success": completion.result.is_ok(),
                }),
            );
        }
        drop(frame);
        let p = self.progress;
        p.in_flight.fetch_sub(1, Relaxed);
        p.in_flight_bytes.fetch_sub(size, Relaxed);
        if borrowed {
            p.borrowed_in_flight.fetch_sub(1, Relaxed);
        }
        let service = now.saturating_sub(submitted_ns);
        p.submit_to_complete.lock().unwrap().record(service);
        if self
            .settings
            .storage_profile
            .max_io_us
            .is_some_and(|bound| service > bound * US)
        {
            p.warn(Warning::ContractViolation, now);
        }
        group.complete(request_id, id, completion.result.is_ok());
        match completion.result {
            Ok(()) => {
                self.sources[usize::from(id.source)]
                    .written
                    .fetch_add(1, Relaxed);
                p.written_bytes.fetch_add(size, Relaxed);
                p.written_wire_bytes
                    .fetch_add(size + super::layout::MESSAGE_HEADER_BYTES as u64, Relaxed);
            }
            Err(error) => {
                ledger.loss(
                    Span::frame(id),
                    now,
                    Layer::Writer,
                    Reason::WriterError,
                    true,
                );
                p.warn(Warning::DurabilityUnknown, now);
                self.abort(report, format!("write request {request_id}: {error}"));
            }
        }
    }

    fn sync(
        &self,
        backend: &Backend,
        group: &mut Group,
        boundary: (u64, Option<u32>),
        final_group: bool,
        report: &mut WriterReport,
        ledger: &mut Ledger<'_>,
    ) -> bool {
        let timer = Instant::now();
        self.progress.writer_blocked_until_ns.store(
            self.now()
                .saturating_add(self.settings.storage_profile.max_sync_ms.unwrap_or(0) * MS),
            Relaxed,
        );
        let result: io::Result<()> = backend.sync(group.id());
        let latency = ns(timer.elapsed());
        let now = self.now();
        self.progress.writer_blocked_until_ns.store(now, Relaxed);
        self.progress.sync_time.lock().unwrap().record(latency);
        if self
            .settings
            .storage_profile
            .max_sync_ms
            .is_some_and(|bound| latency > bound * MS)
        {
            self.progress.warn(Warning::ContractViolation, now);
        }
        match result {
            Ok(()) => {
                group.commit(
                    boundary,
                    final_group,
                    now,
                    self.progress,
                    self.sources,
                    ledger,
                );
                self.progress.last_sync_ns.store(now, Relaxed);
                true
            }
            Err(error) => {
                group.unknown(ledger, now, Reason::SyncFailure);
                self.progress.warn(Warning::DurabilityUnknown, now);
                self.abort(report, format!("sync group {}: {error}", group.id()));
                false
            }
        }
    }

    fn abort(&self, report: &mut WriterReport, error: String) {
        report.error.get_or_insert(error);
        self.progress.abort.store(true, Relaxed);
    }

    fn now(&self) -> u64 {
        ns(self.start.elapsed())
    }
}

fn bump(counter: &std::sync::atomic::AtomicU64, peak: &std::sync::atomic::AtomicU64, amount: u64) {
    let after = counter.fetch_add(amount, Relaxed) + amount;
    peak.fetch_max(after, Relaxed);
}

struct Stalls {
    single: Option<u64>,
    period_ns: Option<u64>,
    periodic: Option<u64>,
    duration_ns: u64,
}

impl Stalls {
    fn new(faults: &Faults) -> Self {
        let period_ns = faults.stall_period_ms.map(|ms| ms * MS);
        Self {
            single: faults.stall_at_ms.map(|ms| ms * MS),
            period_ns,
            periodic: period_ns,
            duration_ns: faults.stall_duration_ms.unwrap_or(0) * MS,
        }
    }

    fn pause(&mut self, writer: &Writer<'_>, now: u64) {
        let p = writer.progress;
        let mut end = now;
        for _ in 0..2 {
            // Either kind of window can extend the other at its boundary.
            if let Some(single) = self.single
                && single <= end
            {
                self.single = None;
                end = end.max(single.saturating_add(self.duration_ns));
            }
            if let Some(next) = self.periodic
                && next <= end
            {
                let period = self.period_ns.unwrap();
                let latest = end - end % period;
                self.periodic = latest.checked_add(period);
                end = end.max(latest.saturating_add(self.duration_ns));
                if writer
                    .settings
                    .storage_profile
                    .min_stall_period_ms
                    .is_none_or(|bound| period < bound * MS)
                {
                    p.warn(Warning::ContractViolation, now);
                }
            }
        }
        if end > now {
            let deadline = writer.start + Duration::from_nanos(end);
            p.writer_blocked_until_ns.store(end, Relaxed);
            sleep_until(deadline, || {
                p.stopped() || crate::stop::REQUESTED.load(Relaxed)
            });
            let actual = writer.now().saturating_sub(now);
            p.stall_windows.fetch_add(1, Relaxed);
            p.stall_wait_ns.fetch_add(actual, Relaxed);
            if writer
                .settings
                .storage_profile
                .max_stall_ms
                .is_some_and(|bound| self.duration_ns > bound * MS || actual > bound * MS)
            {
                p.warn(Warning::ContractViolation, now);
            }
            p.writer_blocked_until_ns.store(writer.now(), Relaxed);
        }
    }
}

pub fn backend(settings: &Settings, file: Option<std::fs::File>) -> Result<Backend> {
    Backend::new(
        settings.record.writer,
        settings.depth(),
        file,
        settings.faults.clone(),
    )
}
