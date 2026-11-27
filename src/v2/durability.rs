mod group;
mod journal;
mod worker;

use std::collections::VecDeque;
use std::fs::File;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex, mpsc::TryRecvError};
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;

use self::group::Group;
use self::journal::{Journal, JournalReport};
use super::adapter::FrameId;
use super::config::{DurabilityMode, MS, Settings};
use super::integrity::{Ledger, Reason};
use super::metrics::{Progress, SourceProgress, Warning};
use super::storage::Backend;
use crate::metrics::ns;

pub struct Submitted {
    pub request_id: u64,
    pub frame_id: FrameId,
    pub offset: u64,
    pub wire_len: u64,
    pub size: u64,
    pub now: u64,
    pub queued_ns: u64,
}

#[derive(Default, Serialize)]
pub struct DurabilityReport {
    pub journal_synced: bool,
    pub final_data_synced: bool,
    pub data_syncs: u64,
    pub journal_syncs: u64,
    pub journal_groups_written: u64,
    pub worker_cpu_ns: u64,
    pub sync_failures: u64,
    pub barrier_ns: u64,
    pub journal: JournalReport,
    pub pending_peak: usize,
    pub max_uncertain_bytes: u64,
    pub backpressure_ns: u64,
    pub data_sync_overlap_ns: u64,
    pub completions_during_sync: u64,
    pub worker_failures: u64,
    pub journal_wait_ns: u64,
    pub final_drain_ns: u64,
    pub group_confirmation: Option<crate::metrics::Latency>,
}

pub struct Coordinator<'a> {
    start: Instant,
    settings: &'a Settings,
    progress: &'a Progress,
    sources: &'a [SourceProgress],
    group: Group,
    journal: Option<Journal>,
    worker: Option<worker::Worker>,
    pending: VecDeque<Arc<Mutex<Group>>>,
    pending_bytes: u64,
    max_wire_frame: u64,
    spares: Vec<Group>,
    confirmed_groups: u64,
    confirmation: crate::metrics::Histogram,
    report: DurabilityReport,
}

impl<'a> Coordinator<'a> {
    pub fn new(
        start: Instant,
        settings: &'a Settings,
        progress: &'a Progress,
        sources: &'a [SourceProgress],
        commits: File,
        layout: (u64, u64),
        backend: &Backend,
    ) -> Result<Self> {
        let mut journal = Some(Journal::new(commits));
        let limit = settings.durability.sealed_limit();
        let worker = if settings.durability.mode == DurabilityMode::Overlap {
            Some(worker::Worker::new(
                backend.clone_data_file()?,
                journal.take().unwrap(),
                settings.faults.clone(),
                limit,
                start,
            )?)
        } else {
            None
        };
        Ok(Self {
            start,
            settings,
            progress,
            sources,
            group: Group::new(settings, layout.0, sources.len()),
            journal,
            worker,
            pending: VecDeque::with_capacity(limit),
            pending_bytes: 0,
            max_wire_frame: layout.1,
            spares: Vec::with_capacity(limit),
            confirmed_groups: 0,
            confirmation: crate::metrics::Histogram::default(),
            report: DurabilityReport::default(),
        })
    }
    pub fn submitted(&mut self, event: Submitted) -> Result<()> {
        self.group.submit(event)?;
        self.report.max_uncertain_bytes = self
            .report
            .max_uncertain_bytes
            .max(self.pending_bytes + self.group.wire_bytes());
        Ok(())
    }
    pub fn completed(&mut self, request: u64, frame: FrameId, success: bool) -> Result<()> {
        self.group.complete(request, frame, success)?;
        if self.syncing() != 0 {
            self.report.completions_during_sync += 1;
        }
        Ok(())
    }
    pub fn due(&self, now: u64, offset: u64) -> bool {
        self.group.due(now, offset, self.settings) || (!self.group.is_empty() && !self.can_submit())
    }
    pub fn is_empty(&self) -> bool {
        self.group.is_empty()
    }
    pub fn groups(&self) -> u64 {
        self.confirmed_groups
    }
    pub fn unknown(&mut self, ledger: &mut Ledger<'_>, now: u64, reason: Reason) {
        self.group.unknown(ledger, now, reason);
    }
    pub fn report(mut self) -> DurabilityReport {
        if let Some(journal) = self.journal.as_mut() {
            self.report.journal = journal.report;
        }
        self.report.group_confirmation = Some(self.confirmation.summary());
        self.report
    }

    pub fn seal(
        &mut self,
        backend: &Backend,
        boundary: (u64, Option<u32>),
        final_group: bool,
        ledger: &mut Ledger<'_>,
    ) -> Result<()> {
        self.group.validate_boundary(boundary.0, final_group)?;
        if self.worker.is_some() {
            return self.send(boundary, final_group);
        }
        let timer = Instant::now();
        self.report.pending_peak = self.report.pending_peak.max(1);
        self.progress.writer_blocked_until_ns.store(
            ns(self.start.elapsed())
                .saturating_add(self.settings.storage_profile.max_sync_ms.unwrap_or(0) * MS),
            Relaxed,
        );
        let result = self.sync(backend, boundary, final_group);
        let now = ns(self.start.elapsed());
        self.report.barrier_ns += ns(timer.elapsed());
        self.progress.writer_blocked_until_ns.store(now, Relaxed);
        if result.is_err() {
            self.unknown(ledger, now, Reason::SyncFailure);
            self.progress.warn(Warning::DurabilityUnknown, now);
        }
        result
    }

    fn sync(
        &mut self,
        backend: &Backend,
        boundary: (u64, Option<u32>),
        final_group: bool,
    ) -> Result<()> {
        let timer = Instant::now();
        if let Some(delay) = self.settings.faults.durability_sync_delay_ms {
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }
        let result = backend.sync(self.group.id());
        let latency = ns(timer.elapsed());
        let data_synced_at = ns(self.start.elapsed());
        self.progress.sync_time.lock().unwrap().record(latency);
        if self
            .settings
            .storage_profile
            .max_sync_ms
            .is_some_and(|bound| latency > bound * MS)
        {
            self.progress
                .warn(Warning::ContractViolation, data_synced_at);
        }
        if result.is_err() {
            self.report.sync_failures += 1;
        }
        result.with_context(|| format!("sync group {}", self.group.id()))?;
        self.report.data_syncs += 1;
        self.report.final_data_synced = final_group;
        let journal = self.journal.as_mut().unwrap();
        let journal_start = Instant::now();
        let result = self
            .group
            .write_evidence(journal, boundary, final_group, data_synced_at)
            .and_then(|()| {
                self.report.journal_groups_written += 1;
                if self.settings.faults.journal_fail_group == Some(self.group.id()) {
                    bail!("injected journal failure");
                }
                journal.sync()
            });
        self.report.journal_wait_ns += ns(journal_start.elapsed());
        if result.is_err() {
            journal.report.failures += 1;
            journal.discard_unconfirmed();
            self.progress.journal_failures.fetch_add(1, Relaxed);
        }
        result.with_context(|| format!("commit journal group {}", self.group.id()))?;
        self.report.journal_syncs += 1;
        self.confirmed_groups += 1;
        self.confirmation.record(ns(timer.elapsed()));
        self.report.journal_synced = final_group;
        self.group.publish(
            boundary.0,
            ns(self.start.elapsed()),
            self.progress,
            self.sources,
            boundary.1.is_some(),
        );
        self.progress.last_sync_ns.store(data_synced_at, Relaxed);
        Ok(())
    }

    pub fn syncing(&self) -> u64 {
        self.worker
            .as_ref()
            .map_or(0, |worker| worker.syncing.load(Relaxed))
    }

    pub fn observe_submit_overlap(&mut self, syncing: u64, elapsed: u64) {
        if syncing != 0 && syncing == self.syncing() {
            self.report.data_sync_overlap_ns += elapsed;
        }
    }

    pub fn can_submit(&self) -> bool {
        self.worker.is_none()
            || (self
                .pending_bytes
                .saturating_add(self.group.wire_bytes())
                .saturating_add(self.max_wire_frame)
                <= self.settings.durability.max_pending_bytes)
    }

    pub fn can_seal(&self) -> bool {
        self.worker.is_none() || self.pending.len() < self.settings.durability.sealed_limit()
    }

    pub fn waiting(&mut self, elapsed: u64) {
        self.report.backpressure_ns += elapsed;
    }

    fn send(&mut self, boundary: (u64, Option<u32>), final_group: bool) -> Result<()> {
        ensure!(self.can_seal(), "durability queue limit");
        let mut next = self
            .spares
            .pop()
            .unwrap_or_else(|| Group::new(self.settings, boundary.0, self.sources.len()));
        next.reset(self.group.id() + 1, self.group.next_request(), boundary.0);
        let sealed = Arc::new(Mutex::new(std::mem::replace(&mut self.group, next)));
        let job = worker::Job {
            group: sealed.clone(),
            boundary,
            final_group,
            sealed_at: ns(self.start.elapsed()),
        };
        let worker = self.worker.as_ref().unwrap();
        let bytes = sealed.lock().unwrap().wire_bytes();
        if worker.tx.as_ref().unwrap().try_send(job).is_err() {
            self.group = Arc::try_unwrap(sealed).ok().unwrap().into_inner().unwrap();
            self.report.worker_failures += 1;
            bail!("durability worker rejected sealed group");
        }
        self.pending_bytes += bytes;
        self.pending.push_back(sealed);
        self.report.pending_peak = self.report.pending_peak.max(self.pending.len());
        Ok(())
    }

    pub fn poll(&mut self, ledger: &mut Ledger<'_>) -> Result<()> {
        self.receive(false, ledger)
    }

    pub fn drain(&mut self, ledger: &mut Ledger<'_>) -> Result<()> {
        let start = Instant::now();
        let mut result = Ok(());
        while !self.pending.is_empty() {
            if let Err(error) = self.receive(true, ledger) {
                result = Err(error);
            }
        }
        self.report.final_drain_ns += ns(start.elapsed());
        result
    }

    fn receive(&mut self, wait: bool, ledger: &mut Ledger<'_>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let worker = self.worker.as_ref().unwrap();
        let result = if wait {
            worker.rx.recv().map_err(|_| TryRecvError::Disconnected)
        } else {
            worker.rx.try_recv()
        };
        let result = match result {
            Ok(result) => result,
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                let now = ns(self.start.elapsed());
                self.report.worker_failures += 1;
                self.progress.warn(Warning::DurabilityUnknown, now);
                for sealed in self.pending.drain(..) {
                    sealed
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .unknown(ledger, now, Reason::SyncFailure);
                }
                self.pending_bytes = 0;
                bail!("durability worker stopped with unconfirmed groups");
            }
        };
        let mut group = Arc::try_unwrap(self.pending.pop_front().unwrap())
            .ok()
            .unwrap()
            .into_inner()
            .unwrap();
        self.pending_bytes -= group.wire_bytes();
        if result.id != group.id() {
            group.unknown(ledger, result.confirmed_at, Reason::SyncFailure);
            self.report.worker_failures += 1;
            bail!("durability worker reported unexpected group");
        }
        if let Some(latency) = result.sync_elapsed {
            self.progress.sync_time.lock().unwrap().record(latency);
            if self
                .settings
                .storage_profile
                .max_sync_ms
                .is_some_and(|bound| latency > bound * MS)
            {
                self.progress
                    .warn(Warning::ContractViolation, result.confirmed_at);
            }
            if let Some(now) = result.data_synced_at {
                self.report.data_syncs += 1;
                self.report.final_data_synced = result.final_group;
                self.progress.last_sync_ns.store(now, Relaxed);
            } else {
                self.report.sync_failures += 1;
            }
        }
        if self.report.journal.failures < result.journal.failures {
            self.progress.journal_failures.fetch_add(1, Relaxed);
        }
        self.report.journal = result.journal;
        self.report.journal_groups_written += u64::from(result.journal_written);
        self.report.worker_cpu_ns += result.cpu_ns;
        if let Some(error) = result.error {
            group.unknown(ledger, result.confirmed_at, Reason::SyncFailure);
            self.progress
                .warn(Warning::DurabilityUnknown, result.confirmed_at);
            bail!(error);
        }
        self.report.journal_syncs += 1;
        self.report.journal_synced = result.final_group;
        self.confirmed_groups += 1;
        self.confirmation
            .record(result.confirmed_at.saturating_sub(result.sealed_at));
        group.publish(
            result.boundary.0,
            result.confirmed_at,
            self.progress,
            self.sources,
            result.boundary.1.is_some(),
        );
        self.spares.push(group);
        Ok(())
    }
}
