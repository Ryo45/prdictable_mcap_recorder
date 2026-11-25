use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::thread;
use std::time::{Duration, Instant};

use super::adapter::{Device, Payload};
use super::arrival::Requirement;
use super::config::{CopyScheduling, Model, Policy, Settings, US, transfer_time};
use super::integrity::{Layer, Ledger, Reason, Span};
use super::metrics::{Progress, SourceProgress, Warning};
use super::pool::Pool;
use super::queue::{CopyClaim, CopyDemand, CopyPriority, CopySelection, Job, Queue};
use crate::metrics::ns;

const IDLE_SAMPLE_NS: u64 = 1_000_000;

#[derive(Default)]
struct Need {
    pool: Option<Requirement>,
    copy_and_return_ns: u64,
    copy_bound_ns: u64,
    risk_episode: Option<RiskEpisode>,
    reported_deadline_overruns: u64,
}

struct RiskEpisode {
    loss_count: u64,
    mitigations: u64,
    observed_breach: bool,
}

pub struct Scheduler<'a> {
    pub trace: Option<&'a super::copy_trace::CopyTrace>,
    pub start: Instant,
    pub settings: &'a Settings,
    pub devices: &'a [Device],
    pub spill: &'a [Pool],
    pub sources: &'a [SourceProgress],
    pub queue: &'a Queue,
    pub progress: &'a Progress,
}

impl Scheduler<'_> {
    pub fn run(&self, mut ledger: Ledger<'_>) {
        let mut needs: Vec<_> = self
            .devices
            .iter()
            .map(|device| {
                let profile = &self.settings.copy_profile;
                let copy_bound_ns = transfer_time(
                    device.source.frame_bytes as u64,
                    profile.assumed_min_bytes_per_sec.unwrap_or(1_000_000_000),
                )
                .saturating_add(profile.scheduling_margin_us.unwrap_or(200) * US);
                Need {
                    copy_bound_ns,
                    copy_and_return_ns: copy_bound_ns
                        .saturating_add(profile.return_margin_us.unwrap_or(200) * US)
                        .saturating_add(device.source.return_delay_us * US),
                    ..Need::default()
                }
            })
            .collect();
        let global = self.settings.record.copy_scheduling == CopyScheduling::Global;
        let mut demands = global.then(|| {
            (0..self.devices.len())
                .map(|_| CopyDemand::default())
                .collect::<Vec<_>>()
        });
        let mut tasks = Vec::with_capacity(if global {
            self.settings.record.queue_capacity
        } else {
            0
        });
        let mut infeasible = false;
        loop {
            let before = self.queue.snapshot();
            if before.closed && before.depth == 0 {
                break;
            }
            let now = ns(self.start.elapsed());
            self.observe(now, before.depth, &mut needs);
            if self.progress.stopped() || before.depth == 0 {
                infeasible = false;
                self.queue
                    .wait_change(before.version, Duration::from_nanos(IDLE_SAMPLE_NS));
                continue;
            }
            let decision = if let Some(demands) = &mut demands {
                for (demand, need) in demands.iter_mut().zip(&needs) {
                    demand.set(
                        need.pool,
                        need.copy_bound_ns,
                        need.copy_and_return_ns - need.copy_bound_ns,
                    );
                }
                let (claim, plan) = self.queue.claim_global_copy(now, demands, &mut tasks);
                let metrics = &self.progress.global_copy;
                if let Some(trace) = self.trace
                    && let Some(claim) = &claim
                {
                    trace.frame("plan_claim", now, &claim.job.frame, serde_json::json!({
                        "pending_tasks": tasks.len(), "pending_work_ns": plan.work_ns,
                        "earliest_due_ns": plan.earliest_due_ns, "wake_ns": plan.wake_ns,
                        "infeasible": plan.infeasible,
                        "writer_blocked_until_ns": self.progress.writer_blocked_until_ns.load(Relaxed),
                        "queue_depth": before.depth,
                    }));
                }
                metrics.peak_work_ns.fetch_max(plan.work_ns, Relaxed);
                if let Some(due) = plan.earliest_due_ns {
                    metrics.earliest_due_ns.fetch_min(due, Relaxed);
                }
                for (source, demand) in self.sources.iter().zip(demands.iter()) {
                    source
                        .peak_pending_borrowed
                        .fetch_max(demand.pending_borrowed, Relaxed);
                }
                if plan.infeasible && !infeasible {
                    metrics.infeasible_events.fetch_add(1, Relaxed);
                    self.progress.warn(Warning::AtRisk, now);
                }
                infeasible = plan.infeasible;
                CopySelection {
                    claim,
                    next_ns: plan.wake_ns,
                }
            } else {
                self.queue
                    .claim_copy(now, |job| self.priority(job, now, &needs))
            };
            if let Some(claim) = decision.claim {
                self.progress.copy_claims.fetch_add(1, Relaxed);
                self.execute(claim, &mut ledger, &needs);
            } else {
                let after = self.queue.snapshot();
                if after.write_claims > before.write_claims {
                    self.progress.copy_claim_contention.fetch_add(1, Relaxed);
                }
                let wait = decision.next_ns.map_or(IDLE_SAMPLE_NS, |at| {
                    at.saturating_sub(ns(self.start.elapsed()))
                        .min(IDLE_SAMPLE_NS)
                });
                self.queue
                    .wait_change(before.version, Duration::from_nanos(wait));
            }
        }
        for (source, need) in self.sources.iter().zip(needs) {
            self.end_risk(source, need.risk_episode);
        }
    }

    fn observe(&self, now: u64, queued: usize, needs: &mut [Need]) {
        let service_ns = self.settings.storage_profile.max_io_us.unwrap_or(1_000) * US;
        let busy_until = self.progress.writer_blocked_until_ns.load(Relaxed).max(now);
        let predicted_done = busy_until.saturating_add((queued as u64).saturating_mul(service_ns));
        for ((device, progress), need) in self.devices.iter().zip(self.sources).zip(needs) {
            // Acquisition removes a pool slot before advancing next. Reading
            // next first can overestimate arrivals, never available capacity.
            let next = device.next.load(Acquire);
            let levels = device.pool.levels(now);
            if self.settings.record.copy_scheduling == CopyScheduling::Global
                && levels.deadline_exceeded > need.reported_deadline_overruns
            {
                need.reported_deadline_overruns = levels.deadline_exceeded;
                self.progress.warn(Warning::ContractViolation, now);
            }
            let requirement = if device.source.model == Model::Owned {
                None
            } else {
                device.schedule.requirement(
                    next,
                    levels.available,
                    device.source.capture_reserve,
                    now,
                )
            };
            // Returning leases remain outside F(t). For this simulated adapter,
            // an acknowledged return has a known reuse time. Avoid starting
            // another copy for debt that will be settled before the deadline.
            need.pool = requirement.and_then(|mut r| {
                if levels.returning > 0 {
                    r.count = r
                        .count
                        .saturating_sub(device.pool.scheduled_returns_by(r.deadline_ns));
                }
                (r.count != 0).then_some(r)
            });
            let deadline = levels
                .earliest_deadline_ns
                .into_iter()
                .chain(need.pool.map(|r| r.deadline_ns))
                .min();
            let slack = deadline.map_or(i64::MAX, |d| {
                (i128::from(d) - i128::from(now)).clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                    as i64
            });
            progress.last_slack_ns.store(slack, Relaxed);
            progress
                .required_returns
                .store(requirement.map_or(0, |r| r.count as u64), Relaxed);
            let risk = device.source.model != Model::Owned
                && deadline.is_some_and(|deadline| {
                    deadline <= predicted_done.saturating_add(need.copy_and_return_ns)
                        && (levels.held > 0
                            || levels.returning > 0
                            || levels.available <= device.source.capture_reserve)
                });
            let breach = levels.available < device.source.capture_reserve
                || levels
                    .earliest_deadline_ns
                    .is_some_and(|deadline| deadline < now);
            if let Some(episode) = &mut need.risk_episode {
                episode.observed_breach |= breach;
            }
            if risk && need.risk_episode.is_none() {
                progress.predicted_risk.fetch_add(1, Relaxed);
                let _ = progress.first_risk_ns_plus_one.compare_exchange(
                    0,
                    now.saturating_add(1),
                    Relaxed,
                    Relaxed,
                );
                self.progress.warn(Warning::AtRisk, now);
                need.risk_episode = Some(RiskEpisode {
                    loss_count: progress.confirmed_loss.load(Relaxed),
                    mitigations: progress.mitigations.load(Relaxed),
                    observed_breach: breach,
                });
            } else if !risk {
                self.end_risk(progress, need.risk_episode.take());
            }
        }
    }

    fn end_risk(&self, source: &SourceProgress, episode: Option<RiskEpisode>) {
        if let Some(episode) = episode
            && episode.loss_count == source.confirmed_loss.load(Relaxed)
        {
            self.progress
                .risk_episodes_without_loss
                .fetch_add(1, Relaxed);
            if !episode.observed_breach && episode.mitigations == source.mitigations.load(Relaxed) {
                self.progress.false_risk_episodes.fetch_add(1, Relaxed);
            }
        }
    }

    fn priority(&self, job: &Job, now: u64, needs: &[Need]) -> Option<CopyPriority> {
        if job.force_copy {
            return Some(CopyPriority {
                ready_ns: now,
                deadline_ns: 0,
            });
        }
        if !self.settings.record.policy.adaptive() {
            return None;
        }
        let lease = job.frame.borrowed()?;
        let need = &needs[usize::from(job.frame.id.source)];
        let pool_deadline = need.pool.map(|r| r.deadline_ns);
        let deadline_ns = lease.deadline_ns.into_iter().chain(pool_deadline).min()?;
        Some(CopyPriority {
            ready_ns: deadline_ns.saturating_sub(need.copy_and_return_ns),
            deadline_ns,
        })
    }

    fn execute(&self, claim: CopyClaim, ledger: &mut Ledger<'_>, needs: &[Need]) {
        let CopyClaim {
            slot,
            mut job,
            deadline_ns,
        } = claim;
        let stream = usize::from(job.frame.id.source);
        let now = ns(self.start.elapsed());
        let device = &self.devices[stream];
        let borrowed = job.frame.borrowed().unwrap();
        let forced = job.force_copy;
        let source = &self.sources[stream];
        let global = self.settings.record.copy_scheduling == CopyScheduling::Global;
        if !forced
            && (!global || borrowed.deadline_ns.is_none())
            && self.settings.record.policy == Policy::DeadlineAwareReserve
            && needs[stream]
                .pool
                .is_some_and(|r| r.deadline_ns <= deadline_ns)
            && device.pool.activate(borrowed.id, now)
        {
            self.progress.reserve_actions.fetch_add(1, Relaxed);
            self.sources[stream].mitigations.fetch_add(1, Relaxed);
            self.sources[stream]
                .ready_displacements
                .fetch_add(1, Relaxed);
            self.queue.finish_copy(slot, Some(job));
            return;
        }

        let spill = self.spill.iter().find(|pool| pool.size() >= job.frame.size);
        if global && spill.is_none() {
            self.progress
                .global_copy
                .spill_size_class_rejections
                .fetch_add(1, Relaxed);
        }
        let Some((mut target, _)) = spill.and_then(|pool| pool.acquire(now)) else {
            if let Some(trace) = self.trace {
                trace.frame(
                    "spill_unavailable",
                    now,
                    &job.frame,
                    serde_json::json!({
                        "class_bytes": spill.map(Pool::size), "forced": forced,
                        "copy_deadline_ns": deadline_ns,
                    }),
                );
            }
            self.progress.copy_spill_pressure.fetch_add(1, Relaxed);
            self.progress.warn(Warning::AtRisk, now);
            if forced {
                ledger.loss(
                    Span::frame(job.frame.id),
                    now,
                    Layer::Scheduler,
                    Reason::SpillPoolExhausted,
                    true,
                );
                drop(job);
                self.queue.finish_copy(slot, None);
            } else {
                job.retry_copy_ns = now.saturating_add(IDLE_SAMPLE_NS);
                self.queue.finish_copy(slot, Some(job));
            }
            return;
        };

        let attempt = self.progress.copy_attempts.fetch_add(1, Relaxed);
        if global {
            self.progress
                .global_copy
                .queue_wait
                .lock()
                .unwrap()
                .record(now.saturating_sub(job.frame.queued_ns));
        }
        let timer = Instant::now();
        let delay = self.settings.faults.copy_delay_us;
        if delay > 0 {
            let until = timer + Duration::from_micros(delay);
            while Instant::now() < until
                && !self.progress.stopped()
                && !crate::stop::REQUESTED.load(Relaxed)
            {
                thread::sleep(
                    until
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(2)),
                );
            }
        }
        let failed = self.settings.faults.copy_fail_attempt == Some(attempt);
        if !failed {
            target.bytes_mut()[..job.frame.size].copy_from_slice(job.frame.bytes());
        }
        let elapsed = ns(timer.elapsed());
        let complete = ns(self.start.elapsed());
        if let Some(trace) = self.trace {
            trace.frame(
                "copy_completion",
                complete,
                &job.frame,
                serde_json::json!({
                    "forced": forced, "copy_deadline_ns": deadline_ns, "elapsed_ns": elapsed,
                    "copy_bound_ns": needs[stream].copy_bound_ns, "success": !failed,
                    "ack_estimate_ns": complete.saturating_add(device.source.return_delay_us * US),
                    "spill_class_bytes": spill.map(Pool::size),
                }),
            );
        }
        self.progress.copy_time.lock().unwrap().record(elapsed);
        self.progress.copy_active_ns.fetch_add(elapsed, Relaxed);
        if elapsed > needs[stream].copy_bound_ns {
            self.progress.warn(Warning::ContractViolation, complete);
        }
        if failed {
            ledger.loss(
                Span::frame(job.frame.id),
                complete,
                Layer::Scheduler,
                Reason::CopyError,
                true,
            );
            drop((target, job));
            self.queue.finish_copy(slot, None);
            return;
        }
        if global {
            source.copy_bytes.fetch_add(job.frame.size as u64, Relaxed);
            source.copy_count.fetch_add(1, Relaxed);
            if !forced {
                let count = if borrowed.deadline_ns == Some(deadline_ns) {
                    &source.fixed_copies
                } else {
                    &source.pool_copies
                };
                count.fetch_add(1, Relaxed);
            }
        }
        if forced {
            self.progress.forced_copies.fetch_add(1, Relaxed);
        } else if borrowed.deadline_ns == Some(deadline_ns) {
            self.progress.fixed_deadline_copies.fetch_add(1, Relaxed);
        } else {
            self.progress.pool_deadline_copies.fetch_add(1, Relaxed);
        }
        self.progress.copy_count.fetch_add(1, Relaxed);
        self.sources[stream].mitigations.fetch_add(1, Relaxed);
        self.progress
            .copy_bytes
            .fetch_add(job.frame.size as u64, Relaxed);
        let old = std::mem::replace(&mut job.frame.payload, Payload::Owned(target));
        if let Payload::Borrowed(lease) = old {
            lease.release(complete);
        }
        job.force_copy = false;
        self.queue.finish_copy(slot, Some(job));
    }
}
