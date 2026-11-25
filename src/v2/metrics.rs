use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};

use serde::Serialize;

use crate::metrics::{Histogram, Latency};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(usize)]
pub enum Warning {
    AtRisk,
    ContractViolation,
    LossDetected,
    DurabilityUnknown,
}

#[derive(Default)]
struct WarningCounter {
    count: AtomicU64,
    first_ns_plus_one: AtomicU64,
    last_ns: AtomicU64,
}

#[derive(Serialize)]
pub struct WarningReport {
    kind: Warning,
    count: u64,
    first_ns: Option<u64>,
    last_ns: Option<u64>,
}

#[derive(Default)]
pub struct Progress {
    pub abort: AtomicBool,
    pub journal_failures: AtomicU64,
    pub written_bytes: AtomicU64,
    pub written_wire_bytes: AtomicU64,
    pub committed_bytes: AtomicU64,
    pub in_flight: AtomicU64,
    pub in_flight_bytes: AtomicU64,
    pub borrowed_in_flight: AtomicU64,
    pub peak_in_flight: AtomicU64,
    pub peak_in_flight_bytes: AtomicU64,
    pub peak_borrowed_in_flight: AtomicU64,
    pub copy_count: AtomicU64,
    pub copy_active_ns: AtomicU64,
    pub copy_bytes: AtomicU64,
    pub copy_attempts: AtomicU64,
    pub copy_claims: AtomicU64,
    pub copy_claim_contention: AtomicU64,
    pub copy_spill_pressure: AtomicU64,
    pub reserve_actions: AtomicU64,
    pub fixed_deadline_copies: AtomicU64,
    pub pool_deadline_copies: AtomicU64,
    pub forced_copies: AtomicU64,
    pub risk_episodes_without_loss: AtomicU64,
    pub false_risk_episodes: AtomicU64,
    pub stall_windows: AtomicU64,
    pub stall_wait_ns: AtomicU64,
    pub queue_last_empty_ns: AtomicU64,
    pub last_sync_ns: AtomicU64,
    pub writer_blocked_until_ns: AtomicU64,
    pub write_busy_ns: AtomicU64,
    pub fill_time: Mutex<Histogram>,
    pub queue_wait: Mutex<Histogram>,
    pub submit_to_complete: Mutex<Histogram>,
    pub copy_time: Mutex<Histogram>,
    pub sync_time: Mutex<Histogram>,
    pub accepted_to_committed: Mutex<Histogram>,
    warnings: [WarningCounter; 4],
}

impl Progress {
    pub fn stopped(&self) -> bool {
        self.abort.load(Relaxed) || self.journal_failures.load(Relaxed) != 0
    }

    pub fn warn(&self, warning: Warning, now: u64) {
        let item = &self.warnings[warning as usize];
        item.count.fetch_add(1, Relaxed);
        let stamped = now.saturating_add(1);
        let _ = item
            .first_ns_plus_one
            .fetch_update(Relaxed, Relaxed, |first| {
                (first == 0 || stamped < first).then_some(stamped)
            });
        item.last_ns.fetch_max(now, Relaxed);
    }

    pub fn warning_count(&self, warning: Warning) -> u64 {
        self.warnings[warning as usize].count.load(Relaxed)
    }

    pub fn warnings(&self) -> Vec<WarningReport> {
        [
            Warning::AtRisk,
            Warning::ContractViolation,
            Warning::LossDetected,
            Warning::DurabilityUnknown,
        ]
        .into_iter()
        .map(|kind| {
            let item = &self.warnings[kind as usize];
            let first_ns = item.first_ns_plus_one.load(Relaxed).checked_sub(1);
            WarningReport {
                kind,
                count: item.count.load(Relaxed),
                first_ns,
                last_ns: first_ns.map(|_| item.last_ns.load(Relaxed)),
            }
        })
        .collect()
    }

    pub fn warning_lead_ns(&self) -> Option<i64> {
        let first = |w: Warning| {
            self.warnings[w as usize]
                .first_ns_plus_one
                .load(Relaxed)
                .checked_sub(1)
        };
        Some(first(Warning::LossDetected)? as i64 - first(Warning::AtRisk)? as i64)
    }
}

#[derive(Default)]
pub struct SourceProgress {
    pub generated: AtomicU64,
    pub accepted: AtomicU64,
    pub submitted: AtomicU64,
    pub written: AtomicU64,
    pub committed: AtomicU64,
    pub before_accept_loss: AtomicU64,
    pub after_accept_loss: AtomicU64,
    pub unknown: AtomicU64,
    pub confirmed_loss: AtomicU64,
    pub envelope_exceeded: AtomicU64,
    pub max_burst: AtomicU64,
    pub last_slack_ns: AtomicI64,
    pub required_returns: AtomicU64,
    pub predicted_risk: AtomicU64,
    pub ready_displacements: AtomicU64,
    pub mitigations: AtomicU64,
    pub lateness: Mutex<Histogram>,
}

impl SourceProgress {
    pub fn report(&self) -> SourceCounts {
        SourceCounts {
            generated: self.generated.load(Relaxed),
            accepted: self.accepted.load(Relaxed),
            submitted: self.submitted.load(Relaxed),
            written: self.written.load(Relaxed),
            committed: self.committed.load(Relaxed),
            before_accept_loss: self.before_accept_loss.load(Relaxed),
            after_accept_loss: self.after_accept_loss.load(Relaxed),
            unknown: self.unknown.load(Relaxed),
        }
    }

    pub fn lateness(&self) -> Latency {
        self.lateness.lock().unwrap().summary()
    }
}

#[derive(Serialize, serde::Deserialize)]
pub struct SourceCounts {
    pub generated: u64,
    pub accepted: u64,
    pub submitted: u64,
    pub written: u64,
    pub committed: u64,
    pub before_accept_loss: u64,
    pub after_accept_loss: u64,
    pub unknown: u64,
}
