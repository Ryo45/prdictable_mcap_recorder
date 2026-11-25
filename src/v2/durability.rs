mod group;
mod journal;

use std::fs::File;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use anyhow::{Context, Result};
use serde::Serialize;

use self::group::Group;
use self::journal::{Journal, JournalReport};
use super::adapter::FrameId;
use super::config::{MS, Settings};
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
    pub sync_failures: u64,
    pub barrier_ns: u64,
    pub journal: JournalReport,
}

pub struct Coordinator<'a> {
    start: Instant,
    settings: &'a Settings,
    progress: &'a Progress,
    sources: &'a [SourceProgress],
    group: Group,
    journal: Journal,
    report: DurabilityReport,
}

impl<'a> Coordinator<'a> {
    pub fn new(
        start: Instant,
        settings: &'a Settings,
        progress: &'a Progress,
        sources: &'a [SourceProgress],
        commits: File,
        first_offset: u64,
    ) -> Self {
        Self {
            start,
            settings,
            progress,
            sources,
            group: Group::new(settings, first_offset),
            journal: Journal::new(commits),
            report: DurabilityReport::default(),
        }
    }
    pub fn submitted(&mut self, event: Submitted) -> Result<()> {
        self.group.submit(event)
    }
    pub fn completed(&mut self, request: u64, frame: FrameId, success: bool) -> Result<()> {
        self.group.complete(request, frame, success)
    }
    pub fn due(&self, now: u64, offset: u64) -> bool {
        self.group.due(now, offset, self.settings)
    }
    pub fn is_empty(&self) -> bool {
        self.group.is_empty()
    }
    pub fn groups(&self) -> u64 {
        self.group.id()
    }
    pub fn unknown(&mut self, ledger: &mut Ledger<'_>, now: u64, reason: Reason) {
        self.group.unknown(ledger, now, reason);
    }
    pub fn report(mut self) -> DurabilityReport {
        self.report.journal = std::mem::take(&mut self.journal.report);
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
        let timer = Instant::now();
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
        let result = self
            .group
            .write_evidence(&mut self.journal, boundary, final_group, data_synced_at)
            .and_then(|()| self.journal.sync());
        if result.is_err() {
            self.journal.report.failures += 1;
            self.journal.discard_unconfirmed();
            self.progress.journal_failures.fetch_add(1, Relaxed);
        }
        result.with_context(|| format!("commit journal group {}", self.group.id()))?;
        self.report.journal_syncs += 1;
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
}
