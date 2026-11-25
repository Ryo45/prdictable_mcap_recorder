use std::cmp::Reverse;
use std::collections::BinaryHeap;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::arrival::{Schedule, schedules};
use super::config::{Budget, CopyScheduling, MS, Model, NS, Policy, Settings, US, transfer_time};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Accepted,
    Rejected,
    Unknown,
}

#[derive(Debug, Serialize)]
pub struct Plan {
    pub verdict: Verdict,
    pub reasons: Vec<String>,
    pub assumptions_missing: Vec<&'static str>,
    pub input_bytes_per_sec: u64,
    pub copy_input_bytes_per_sec: u64,
    pub copy_input_basis: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy_worker_demand_ns_per_second: Option<u64>,
    pub storage_effective_bytes_per_sec: Option<u64>,
    pub storage_latency_ns: Option<u64>,
    pub backlog_bound_bytes: Option<u64>,
    pub queue_frames_recommended: Option<u64>,
    pub stall_tolerance_ns: Option<u64>,
    pub copy_busy_bound_ns: Option<u64>,
    pub memory_allocated_payload_bytes: u64,
    pub memory_estimated_metadata_bytes: u64,
    pub memory_estimated_total_bytes: u64,
    pub memory_budget_bytes: u64,
    pub estimate_mcap_bytes: u64,
    pub sources: Vec<SourcePlan>,
    pub simulation: Option<Simulation>,
}

#[derive(Debug, Serialize)]
pub struct SourcePlan {
    pub id: String,
    pub planned_frames: u64,
    pub input_wire_bytes_per_sec: u64,
    pub burst_wire_bytes: u64,
    pub recommended_dma_buffers: Option<u64>,
    pub recommended_spill_buffers: Option<u64>,
    pub delay_budget_ns: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Simulation {
    pub simulated_events: u64,
    pub truncated: bool,
    pub peak_backlog_bytes: u64,
    pub peak_simultaneous_arrivals: u64,
}

pub fn analyze(settings: &Settings, duration_ns: u64) -> Result<(Plan, Vec<Schedule>)> {
    let budget = settings.validate()?;
    let schedules = schedules(settings, duration_ns)?;
    let record = &settings.record;
    let profiles = &settings.storage_profile;
    let copy = &settings.copy_profile;
    let mut reasons = Vec::new();
    let mut unknown = Vec::new();
    let mut source_plans = Vec::new();
    let mut payload_volume = 0_u128;
    let mut input_rate = 0_u128;
    let mut frame_rate = 0_u128;
    let mut input_burst = 0_u128;
    let mut copy_rate = 0_u128;
    let mut copy_burst = 0_u128;
    let mut max_copy = 0_u64;
    let global = record.copy_scheduling == CopyScheduling::Global;
    let mut worker_rate = 0_u128;
    let mut worker_burst = 0_u128;
    let mut worker_blocking = 0_u64;
    if budget.total_bytes > record.memory_budget_bytes {
        reasons.push(format!(
            "estimated memory exceeds budget by {} bytes",
            budget.total_bytes - record.memory_budget_bytes
        ));
    }
    for (source, schedule) in settings.sources.iter().zip(&schedules) {
        if copy
            .max_capture_delay_us
            .is_some_and(|limit| source.fence_delay_us > limit)
        {
            reasons.push(format!(
                "{}: fence delay exceeds the capture bound",
                source.id
            ));
        }
        let width = source.frame_bytes as u128 + 31;
        let fps = u128::from(source.fps) * u128::from(source.burst);
        let frames = frames_bound(source, 0, copy.max_capture_delay_us.unwrap_or(0));
        let rate = width
            .checked_mul(fps)
            .context("aggregate input rate overflow")?;
        let burst = width * frames;
        ensure!(
            rate <= u128::from(u64::MAX) && burst <= u128::from(u64::MAX),
            "arrival bound too large for {}",
            source.id
        );
        input_rate += rate;
        input_burst += burst;
        frame_rate += fps;
        payload_volume += width * u128::from(schedule.expected);
        if !source.arrival_bound_known {
            unknown.push("source arrival envelope");
        }
        if source.model != Model::Owned && record.policy != Policy::AlwaysBorrow {
            copy_rate += source.frame_bytes as u128 * fps;
            copy_burst += source.frame_bytes as u128 * frames;
            max_copy = max_copy.max(source.frame_bytes as u64);
            if global
                && let Some((speed, margin)) = copy
                    .assumed_min_bytes_per_sec
                    .zip(copy.scheduling_margin_us)
            {
                let work =
                    transfer_time(source.frame_bytes as u64, speed).saturating_add(margin * US);
                worker_rate = worker_rate.saturating_add(fps.saturating_mul(u128::from(work)));
                worker_burst = worker_burst.saturating_add(frames.saturating_mul(u128::from(work)));
                worker_blocking = worker_blocking.max(work);
                if settings.faults.copy_delay_us * US >= work && settings.faults.copy_delay_us > 0 {
                    reasons.push(format!(
                        "{}: injected copy delay exhausts the per-job copy bound",
                        source.id
                    ));
                }
            }
        }
        source_plans.push(SourcePlan {
            id: source.id.clone(),
            planned_frames: schedule.expected,
            input_wire_bytes_per_sec: rate as u64,
            burst_wire_bytes: burst as u64,
            recommended_dma_buffers: None,
            recommended_spill_buffers: None,
            delay_budget_ns: None,
        });
    }
    let estimate = u64::try_from(payload_volume + 4096 + settings.sources.len() as u128 * 1024)
        .context("MCAP volume overflow")?;
    let input_rate = u64::try_from(input_rate).context("aggregate input rate overflow")?;
    let input_burst = u64::try_from(input_burst).context("aggregate burst overflow")?;
    let copy_rate = u64::try_from(copy_rate).context("copy input rate overflow")?;
    let copy_burst = u64::try_from(copy_burst).context("copy burst overflow")?;
    let needed = |value: Option<u64>, key, unknown: &mut Vec<&'static str>| {
        if value.is_none() {
            unknown.push(key);
        }
        value
    };
    let out = needed(
        profiles.sustained_bytes_per_sec,
        "storage sustained rate",
        &mut unknown,
    );
    let stall = needed(profiles.max_stall_ms, "storage maximum stall", &mut unknown);
    let io = needed(profiles.max_io_us, "storage I/O bound", &mut unknown);
    let sync = needed(profiles.max_sync_ms, "storage sync bound", &mut unknown);
    let schedule_margin = needed(copy.scheduling_margin_us, "scheduler margin", &mut unknown);
    needed(
        copy.max_capture_delay_us,
        "capture delay bound",
        &mut unknown,
    );
    let return_margin = if copy_rate > 0 {
        needed(copy.return_margin_us, "return margin", &mut unknown)
    } else {
        Some(0)
    };
    let copy_speed = if copy_rate > 0 {
        needed(
            copy.assumed_min_bytes_per_sec,
            "minimum copy rate",
            &mut unknown,
        )
    } else {
        None
    };

    if let Some(period) = settings.faults.stall_period_ms {
        match profiles.min_stall_period_ms {
            None => unknown.push("repeated-stall interval bound"),
            Some(minimum) if period < minimum => reasons.push(format!(
                "configured stall period {period} ms is shorter than the profile minimum {minimum} ms"
            )),
            Some(_) => {}
        }
    }
    if let Some((actual, bound)) = settings.faults.stall_duration_ms.zip(stall)
        && actual > bound
    {
        reasons.push(format!(
            "configured stall {actual} ms exceeds the profile bound {bound} ms"
        ));
    }

    // Sustained service is measured with the selected sync workload. For
    // repeated stalls, further discount capacity by the declared stall duty cycle.
    let mut adjusted = out;
    if let Some(period) = profiles.min_stall_period_ms {
        match stall {
            Some(stall) if period > stall => {
                adjusted = out.map(|rate| {
                    (u128::from(rate) * u128::from(period - stall) / u128::from(period)) as u64
                });
            }
            Some(_) => {
                reasons.push("repeated stalls leave no service time".into());
                adjusted = Some(0);
            }
            None => adjusted = None,
        }
    }
    if let Some(rate) = adjusted
        && input_rate >= rate
    {
        reasons.push(format!(
            "input {input_rate} B/s must be below effective storage {rate} B/s"
        ));
    }
    let latency = stall.zip(io).zip(sync).map(|((stall, io), sync)| {
        (stall * MS)
            .saturating_add(io * US)
            .saturating_add(sync * MS)
    });
    let backlog = latency.map(|latency| {
        saturating_u64(
            u128::from(input_burst)
                + (u128::from(input_rate) * u128::from(latency)).div_ceil(u128::from(NS)),
        )
    });
    let copy_busy = if copy_rate == 0 {
        Some(0)
    } else {
        copy_speed.and_then(|speed| {
            if speed <= copy_rate {
                reasons.push(format!(
                    "copy demand {copy_rate} B/s must be below minimum copy rate {speed} B/s"
                ));
                return None;
            }
            if global {
                schedule_margin?;
                if worker_rate >= u128::from(NS) {
                    reasons.push(format!(
                        "conservative single Copy Worker demand {worker_rate} ns/s exhausts its service budget"
                    ));
                    return None;
                }
                let backlog = worker_burst.saturating_add(u128::from(worker_blocking));
                return Some(saturating_u64(backlog.saturating_mul(u128::from(NS))
                    .div_ceil(u128::from(NS) - worker_rate)));
            }
            // Non-preemptive blocking by one maximum-sized copy plus
            // competing bursts serviced from the residual copy bandwidth.
            Some(transfer_time(
                copy_burst.saturating_add(max_copy),
                speed - copy_rate,
            ))
        })
    };

    let mut spill_demand = Vec::new();
    let mut backlog_frames = 0_u64;
    let mut frame_burst = 0_u64;
    for (index, source) in settings.sources.iter().enumerate() {
        frame_burst = frame_burst.saturating_add(saturating_u64(frames_bound(
            source,
            0,
            copy.max_capture_delay_us.unwrap_or(0),
        )));
        let output_backlog = latency.map(|h| {
            saturating_u64(frames_bound(
                source,
                h,
                copy.max_capture_delay_us.unwrap_or(0),
            ))
        });
        if let Some(frames) = output_backlog {
            backlog_frames = backlog_frames.saturating_add(frames);
        }
        let reserve = source.capture_reserve as u64;
        let base = source.dma_buffers as u64;
        let gates = record.policy.adaptive();
        let borrowed = settings.depth().min(record.max_borrowed_in_flight) as u64;
        let recommend = if source.model == Model::Owned || record.policy == Policy::AlwaysBorrow {
            output_backlog.map(|frames| {
                frames
                    .saturating_add(reserve)
                    .saturating_add(settings.depth() as u64)
            })
        } else {
            copy_busy.zip(schedule_margin).zip(return_margin).map(
                |((copy_ns, margin), return_margin)| {
                    let h = copy_ns
                        .saturating_add((margin + return_margin).saturating_mul(US))
                        .saturating_add(source.return_delay_us * US);
                    source_plans[index].delay_budget_ns = Some(h);
                    let deadline_count = saturating_u64(frames_bound(
                        source,
                        h,
                        copy.max_capture_delay_us.unwrap_or(0),
                    ));
                    // Only the deadline policies account for already submitted leases.
                    let borrow_slots = if record.policy == Policy::AlwaysCopy {
                        0
                    } else {
                        borrowed
                    };
                    deadline_count
                        .saturating_add(reserve)
                        .saturating_add(borrow_slots)
                },
            )
        };
        source_plans[index].recommended_dma_buffers = recommend;
        if let Some(recommend) = recommend
            && base < recommend
        {
            reasons.push(format!("{} needs {recommend} base buffers under the conservative return model (has {base})", source.id));
        }
        if let Some(hold) = source.max_hold_ms {
            let hold_ns = hold * MS;
            if source_plans[index]
                .delay_budget_ns
                .is_some_and(|ns| ns >= hold_ns)
                || (!gates
                    && record.policy != Policy::AlwaysCopy
                    && latency.is_some_and(|ns| {
                        ns.saturating_add(source.return_delay_us * US) >= hold_ns
                    }))
            {
                reasons.push(format!(
                    "{} fixed deadline is shorter than its conservative return path",
                    source.id
                ));
            }
        }
        if source.model != Model::Owned
            && record.policy != Policy::AlwaysBorrow
            && let Some(count) = output_backlog
        {
            source_plans[index].recommended_spill_buffers = Some(count);
            spill_demand.push((source.frame_bytes, count));
        }
    }
    let recommend_queue = latency.map(|_| backlog_frames.saturating_add(settings.depth() as u64));
    if let Some(count) = recommend_queue
        && (record.queue_capacity as u64) < count
    {
        reasons.push(format!(
            "queue needs {count} slots (has {})",
            record.queue_capacity
        ));
    }

    let mut classes: Vec<_> = settings
        .spill
        .iter()
        .map(|class| (class.frame_bytes, class.buffers as u64))
        .collect();
    spill_demand.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    for (size, count) in spill_demand {
        let missing = match classes.iter_mut().find(|(class, _)| *class >= size) {
            Some((_, available)) => {
                let missing = count.saturating_sub(*available);
                *available = available.saturating_sub(count);
                missing
            }
            None => count,
        };
        if missing > 0 {
            reasons.push(format!(
                "spill size class for {size} bytes is short by {missing} buffers"
            ));
        }
    }
    let effective_slots = (record.queue_capacity as u64)
        .saturating_sub(frame_burst)
        .saturating_sub(settings.depth() as u64);
    let stall_tolerance = if frame_rate == 0 {
        None
    } else {
        let queue_horizon =
            saturating_u64(u128::from(effective_slots) * u128::from(NS) / frame_rate);
        Some(
            queue_horizon
                .saturating_sub(io.unwrap_or(0) * US)
                .saturating_sub(sync.unwrap_or(0) * MS),
        )
    };
    unknown.sort_unstable();
    unknown.dedup();
    let verdict = if !reasons.is_empty() {
        Verdict::Rejected
    } else if !unknown.is_empty() {
        Verdict::Unknown
    } else {
        Verdict::Accepted
    };
    let simulation = adjusted
        .zip(latency)
        .map(|(rate, latency)| simulate(settings, &schedules, rate, latency));
    let Budget {
        payload_bytes,
        metadata_bytes,
        total_bytes,
    } = budget;
    Ok((
        Plan {
            verdict,
            reasons,
            assumptions_missing: unknown,
            input_bytes_per_sec: input_rate,
            copy_input_bytes_per_sec: copy_rate,
            copy_input_basis: "conservative upper bound assuming all eligible frames need CopyOut",
            copy_worker_demand_ns_per_second: (global
                && copy_speed.is_some()
                && schedule_margin.is_some())
            .then_some(saturating_u64(worker_rate)),
            storage_effective_bytes_per_sec: adjusted,
            storage_latency_ns: latency,
            backlog_bound_bytes: backlog,
            queue_frames_recommended: recommend_queue,
            stall_tolerance_ns: stall_tolerance,
            copy_busy_bound_ns: copy_busy,
            memory_allocated_payload_bytes: payload_bytes,
            memory_estimated_metadata_bytes: metadata_bytes,
            memory_estimated_total_bytes: total_bytes,
            memory_budget_bytes: record.memory_budget_bytes,
            estimate_mcap_bytes: estimate,
            sources: source_plans,
            simulation,
        },
        schedules,
    ))
}

fn frames_bound(source: &super::config::Source, horizon: u64, capture_delay_us: u64) -> u128 {
    let width = u128::from(horizon)
        + (u128::from(source.jitter_us) + u128::from(capture_delay_us)) * u128::from(US);
    (1 + (width * u128::from(source.fps)).div_ceil(u128::from(NS))) * u128::from(source.burst)
}

fn saturating_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn simulate(settings: &Settings, schedules: &[Schedule], rate: u64, latency: u64) -> Simulation {
    const LIMIT: u64 = 100_000;
    let mut heap = BinaryHeap::new();
    for (source, schedule) in schedules.iter().enumerate() {
        if let Some(time) = schedule.time(0) {
            heap.push(Reverse((time, source, 0)));
        }
    }
    let mut output = Simulation {
        simulated_events: 0,
        truncated: false,
        peak_backlog_bytes: 0,
        peak_simultaneous_arrivals: 0,
    };
    let mut previous = 0_u64;
    let mut concurrent = 0;
    let mut queued = 0_u128; // scaled by NS to avoid truncation at every event
    while let Some(Reverse((time, stream, seq))) = heap.pop() {
        if output.simulated_events == LIMIT {
            output.truncated = true;
            break;
        }
        let service = time
            .saturating_sub(latency)
            .saturating_sub(previous.saturating_sub(latency));
        queued = queued.saturating_sub(u128::from(service) * u128::from(rate));
        queued += (settings.sources[stream].frame_bytes as u128 + 31) * u128::from(NS);
        output.peak_backlog_bytes = output
            .peak_backlog_bytes
            .max(saturating_u64(queued.div_ceil(u128::from(NS))));
        concurrent = if time == previous { concurrent + 1 } else { 1 };
        output.peak_simultaneous_arrivals = output.peak_simultaneous_arrivals.max(concurrent);
        previous = time;
        output.simulated_events += 1;
        if let Some(next) = schedules[stream].time(seq + 1) {
            heap.push(Reverse((next, stream, seq + 1)));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> Settings {
        toml::from_str(include_str!("../../configs/v2-smoke.toml")).unwrap()
    }

    #[test]
    fn bounded_model_accepts_and_virtual_schedule_accounts_for_every_burst() {
        let cfg = settings();
        let (plan, schedules) = analyze(&cfg, NS).unwrap();
        assert_eq!(plan.verdict, Verdict::Accepted);
        assert_eq!(
            plan.simulation.as_ref().unwrap().simulated_events,
            schedules.iter().map(|s| s.expected).sum::<u64>()
        );
        assert!(plan.simulation.unwrap().peak_simultaneous_arrivals >= 2);
        assert!(plan.queue_frames_recommended.unwrap() < cfg.record.queue_capacity as u64);
    }

    #[test]
    fn rejects_storage_copy_memory_pool_deadline_and_size_class_shortfalls() {
        let check = |mut change: Box<dyn FnMut(&mut Settings)>, cause| {
            let mut cfg = settings();
            change(&mut cfg);
            let (plan, _) = analyze(&cfg, NS).unwrap();
            assert_eq!(plan.verdict, Verdict::Rejected);
            assert!(
                plan.reasons.iter().any(|reason| reason.contains(cause)),
                "{:?}",
                plan.reasons
            );
        };
        check(
            Box::new(|cfg| cfg.storage_profile.sustained_bytes_per_sec = Some(500)),
            "storage",
        );
        check(
            Box::new(|cfg| cfg.copy_profile.assumed_min_bytes_per_sec = Some(500)),
            "copy demand",
        );
        check(
            Box::new(|cfg| cfg.record.memory_budget_bytes = 100),
            "memory",
        );
        check(Box::new(|cfg| cfg.record.queue_capacity = 1), "queue");
        check(
            Box::new(|cfg| cfg.sources[0].dma_buffers = 3),
            "base buffers",
        );
        check(
            Box::new(|cfg| cfg.sources[0].max_hold_ms = Some(1)),
            "fixed deadline",
        );
        check(Box::new(|cfg| cfg.spill[2].buffers = 1), "size class");
    }

    #[test]
    fn input_or_service_uncertainty_never_becomes_accepted() {
        let mut cfg = settings();
        cfg.sources[0].arrival_bound_known = false;
        cfg.storage_profile.max_stall_ms = None;
        let (plan, _) = analyze(&cfg, NS).unwrap();
        assert_eq!(plan.verdict, Verdict::Unknown);
        assert!(
            plan.assumptions_missing
                .contains(&"source arrival envelope")
        );
        assert!(plan.assumptions_missing.contains(&"storage maximum stall"));
    }

    #[test]
    fn m8_global_worker_load_and_missing_bounds_are_distinct() {
        let mut cfg = settings();
        cfg.record.copy_scheduling = CopyScheduling::Global;
        cfg.copy_profile.scheduling_margin_us = Some(10_000);
        let plan = analyze(&cfg, NS).unwrap().0;
        assert_eq!(plan.verdict, Verdict::Rejected);
        assert!(plan.copy_worker_demand_ns_per_second.unwrap() > NS);
        assert!(
            plan.reasons
                .iter()
                .any(|reason| reason.contains("Copy Worker"))
        );
        cfg.copy_profile.scheduling_margin_us = None;
        let plan = analyze(&cfg, NS).unwrap().0;
        assert_eq!(plan.verdict, Verdict::Unknown);
        assert!(plan.copy_worker_demand_ns_per_second.is_none());
    }
}
