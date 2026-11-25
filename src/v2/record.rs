use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use rustix::time::ClockId;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::Clock;
use super::adapter::{Device, ScheduledSource, SourceAdapter};
use super::capture::Capture;
use super::config::{CopyScheduling, MIB, Settings, US, WriterMode};
use super::integrity::{Ledger, Monitor, MonitorFiles};
use super::layout::Layout;
use super::metrics::{Progress, SourceCounts, SourceProgress, Warning};
use super::planner::{Plan, Verdict, analyze};
use super::pool::{Pool, ReturnPolicy};
use super::queue::Queue;
use super::scheduler::Scheduler;
use super::storage::probe;
use super::writer::{Writer, WriterReport, backend};
use crate::metrics::{cpu_ns, ns, rate, rss_bytes};
use crate::report::{Outputs, Status, create_new};

pub const VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Core {
    pub report_version: u32,
    pub status: Status,
    pub settings: Settings,
    pub duration_ns: u64,
    pub session_epoch: u64,
    pub sources: Vec<SourceResult>,
    pub planner_verdict: Verdict,
    pub within_assumed_contract: bool,
    pub finalized: bool,
    pub final_sync: bool,
    pub loss_log_synced: bool,
    pub commit_log_synced: bool,
    pub journal_failures: u64,
    pub group_count: u64,
    pub end_offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct SourceResult {
    pub id: String,
    pub planned: u64,
    #[serde(flatten)]
    pub counts: SourceCounts,
}

pub fn commit_path(data: &Path) -> Result<PathBuf> {
    let mut stem = data
        .file_stem()
        .context("output needs a file name")?
        .to_os_string();
    stem.push("-commits.jsonl");
    Ok(data.with_file_name(stem))
}

pub fn run_cli(cli: &crate::config::Config) -> Result<()> {
    let config = cli.config.as_deref().unwrap();
    let settings = Settings::load(config, cli.profile.as_deref())?;
    let duration = Settings::duration_ns(cli.duration_sec)?;
    if let Some(dir) = &cli.output_dir {
        ensure!(
            settings.record.writer != WriterMode::Null,
            "--output-dir requires a file backend"
        );
        std::fs::create_dir(dir)
            .with_context(|| format!("create new run directory {}", dir.display()))?;
        let parent = dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
        let partial = dir.join("run.partial");
        run(
            settings,
            duration,
            &partial,
            cli.allow_unverified,
            cli.allow_large_output,
        )?;
        std::fs::hard_link(&partial, dir.join("run.mcap"))?;
        let directory = std::fs::File::open(dir)?;
        directory.sync_all()?;
        std::fs::remove_file(partial)?;
        directory.sync_all()?;
        return Ok(());
    }
    run(
        settings,
        duration,
        &cli.output,
        cli.allow_unverified,
        cli.allow_large_output,
    )
}

fn run(
    settings: Settings,
    duration: u64,
    path: &Path,
    allow_unverified: bool,
    allow_large: bool,
) -> Result<()> {
    let main_start = Instant::now();
    let cpu_start = cpu_ns(ClockId::ProcessCPUTime);
    let (plan, schedules) = analyze(&settings, duration)?;
    preflight(&settings, &plan, path, allow_unverified, allow_large)?;
    let outputs = Outputs::new(path)?;
    let commit_path = commit_path(path)?;
    outputs.check_new()?;
    ensure!(
        !commit_path.exists() && commit_path.symlink_metadata().is_err(),
        "commit ledger output already exists"
    );
    probe(settings.record.writer)?;
    crate::stop::install()?;

    let clock = Clock::default();
    let devices = settings
        .sources
        .iter()
        .zip(schedules)
        .enumerate()
        .map(|(stream, (source, schedule))| {
            Device::new(source, stream as u16, schedule, clock.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    let spill = settings
        .spill
        .iter()
        .enumerate()
        .map(|(index, class)| {
            Pool::new(
                devices.len() + index,
                class.buffers,
                0,
                class.frame_bytes,
                clock.clone(),
                ReturnPolicy::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let progress = Progress::default();
    let sources: Vec<_> = devices.iter().map(|_| SourceProgress::default()).collect();
    let queue = Queue::new(settings.record.queue_capacity, devices.len());
    let trace = super::copy_trace::CopyTrace::open(path)?;
    let mut summary = create_new(&outputs.summary)?;
    let files = MonitorFiles {
        loss: create_new(&outputs.losses)?,
        csv: create_new(&outputs.metrics)?,
    };
    let commits = create_new(&commit_path)?;
    let file = if settings.record.writer == WriterMode::Null {
        None
    } else {
        Some(create_new(&outputs.data)?)
    };

    if path.extension().is_some_and(|ext| ext == "partial")
        || settings
            .sources
            .iter()
            .any(|source| !source.kind.is_synthetic())
    {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
    }
    let lead = Duration::from_millis(80);
    let start = Instant::now() + lead;
    let epoch = u64::try_from((SystemTime::now().duration_since(UNIX_EPOCH)? + lead).as_nanos())?;
    ensure!(
        epoch.checked_add(duration).is_some(),
        "absolute timestamp overflow"
    );
    let (layout, preamble) = Layout::new(&settings, epoch)?;
    clock.start_at(start);
    let capture = Capture {
        start,
        duration,
        settings: &settings,
        devices: &devices,
        sources: &sources,
        queue: &queue,
        progress: &progress,
    };
    let monitor = Monitor {
        start,
        settings: &settings,
        devices: &devices,
        spill: &spill,
        sources: &sources,
        progress: &progress,
        queue: &queue,
    };
    let scheduler = Scheduler {
        trace: trace.as_ref(),
        start,
        settings: &settings,
        devices: &devices,
        spill: &spill,
        sources: &sources,
        progress: &progress,
        queue: &queue,
    };
    let writer = Writer {
        trace: trace.as_ref(),
        start,
        settings: &settings,
        devices: &devices,
        sources: &sources,
        progress: &progress,
        queue: &queue,
    };
    let (sender, receiver) = mpsc::sync_channel(settings.record.loss_queue_capacity);
    let writer_ledger = Ledger::new(sender.clone(), &progress, &sources);
    let copy_ledger = Ledger::new(sender.clone(), &progress, &sources);
    let (writer_report, monitor_report, capture_end) = thread::scope(|scope| {
        let monitor = scope.spawn(move || monitor.run(receiver, files));
        let copier = scope.spawn(move || scheduler.run(copy_ledger));
        let (ready, enabled) = mpsc::sync_channel(0);
        let settings = &settings;
        let progress = &progress;
        let writer = scope.spawn(move || match backend(settings, file) {
            Ok(backend) => {
                let _ = ready.send(());
                writer.run(writer_ledger, backend, layout, &preamble, commits)
            }
            Err(error) => {
                progress.abort.store(true, Relaxed);
                let _ = ready.send(());
                WriterReport {
                    error: Some(format!("initialize backend: {error:#}")),
                    ..WriterReport::default()
                }
            }
        });
        let _ = enabled.recv();
        let adapters = devices
            .iter()
            .map(|device| ScheduledSource::new(device, epoch, settings.record.seed))
            .collect();
        capture.run(adapters, Ledger::new(sender, progress, &sources));
        let capture_end = ns(start.elapsed());
        let writer_report = writer.join().unwrap();
        copier.join().unwrap();
        (writer_report, monitor.join().unwrap(), capture_end)
    });

    if let Some(trace) = trace {
        trace.finish();
    }
    let elapsed = ns(start.elapsed());
    // The pseudo device has no real interrupt to wait for at shutdown. All
    // producers are stopped, so advance its delayed-return clock to settle.
    let return_limit = devices
        .iter()
        .map(|device| device.source.return_delay_us * US)
        .max()
        .unwrap_or(0);
    let settled = elapsed.saturating_add(return_limit);
    let pool_reports: Vec<_> = devices
        .iter()
        .map(|device| device.pool.report(settled))
        .collect();
    let spill_reports: Vec<_> = spill.iter().map(|pool| pool.report(settled)).collect();
    let queue_status = queue.snapshot();
    let mut errors: Vec<_> = [
        writer_report.error.as_deref(),
        monitor_report.error.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::to_owned)
    .collect();
    let journal_failures = progress.journal_failures.load(Relaxed);
    if journal_failures > 0 {
        errors.push(format!("{journal_failures} sidecar events failed"));
    }
    let returned = pool_reports
        .iter()
        .chain(&spill_reports)
        .all(|report| report.levels.held == 0 && report.levels.returning == 0)
        && queue_status.depth == 0
        && progress.in_flight.load(Relaxed) == 0;
    if !returned {
        errors.push("resources are still owned at shutdown".into());
    }
    let source_results: Vec<_> = devices
        .iter()
        .zip(&sources)
        .map(|(device, progress)| SourceResult {
            id: device.source.id.clone(),
            planned: device.schedule.expected,
            counts: progress.report(),
        })
        .collect();
    let stop = crate::stop::REQUESTED.load(Relaxed);
    if !stop
        && errors.is_empty()
        && source_results.iter().any(|result| {
            result.planned != result.counts.accepted + result.counts.before_accept_loss
                || result.counts.accepted
                    != result.counts.committed + result.counts.after_accept_loss
                || result.counts.committed != result.counts.written
                || result.counts.unknown != 0
        })
    {
        errors.push("frame states do not reconcile".into());
    }
    let status = if !errors.is_empty() {
        Status::Error
    } else if stop {
        Status::Cancelled
    } else {
        Status::Ok
    };
    let contract = plan.verdict == Verdict::Accepted
        && status == Status::Ok
        && settings.record.writer != WriterMode::Null
        && progress.warning_count(Warning::ContractViolation) == 0
        && progress.warning_count(Warning::LossDetected) == 0
        && pool_reports
            .iter()
            .all(|pool| pool.levels.deadline_exceeded == 0);
    let core = Core {
        report_version: VERSION,
        status,
        settings: settings.clone(),
        duration_ns: duration,
        session_epoch: epoch,
        sources: source_results,
        planner_verdict: plan.verdict,
        within_assumed_contract: contract,
        finalized: writer_report.finalized,
        final_sync: writer_report.final_sync,
        loss_log_synced: monitor_report.loss_synced,
        commit_log_synced: writer_report.durability.journal_synced,
        journal_failures,
        group_count: writer_report.groups,
        end_offset: writer_report.data_end_offset,
    };
    let committed_bytes = progress.committed_bytes.load(Relaxed);
    let (rss, peak_rss) = rss_bytes();
    let generated_bytes: u64 = core
        .sources
        .iter()
        .zip(&settings.sources)
        .map(|(source, config)| source.counts.generated * config.frame_bytes as u64)
        .sum();
    let device_metrics: Vec<_> = devices.iter().zip(&sources).zip(&pool_reports).map(|((device, source), pool)| {
        let adapter = ScheduledSource::new(device, epoch, settings.record.seed);
        let levels = adapter.pool_status(settled);
        json!({"id":device.source.id,"capabilities":adapter.capabilities(),"pool":pool,"settled_pool":levels,
            "arrival_lateness_ns":source.lateness(),"envelope_exceeded":source.envelope_exceeded.load(Relaxed),
            "largest_observed_burst":source.max_burst.load(Relaxed),"predicted_risk":source.predicted_risk.load(Relaxed),
            "reserve_displacements":source.ready_displacements.load(Relaxed)})
    }).collect();
    let mut result = serde_json::to_value(&core)?;
    let mut monitor_metrics = serde_json::to_value(&monitor_report)?;
    monitor_metrics["commit_log_synced"] = json!(writer_report.durability.journal_synced);
    let mut metrics = json!({
        "plan":plan, "writer":writer_report, "monitor":monitor_metrics, "durability":writer_report.durability, "devices":device_metrics,
        "spill":spill_reports, "all_resources_returned":returned, "queue_peak":queue_status.peak,
        "capture_wall_ns":capture_end, "generated_bytes_per_sec":rate(generated_bytes,capture_end),
        "written_payload_bytes":progress.written_bytes.load(Relaxed),"committed_payload_bytes":committed_bytes,
        "committed_payload_bytes_per_sec": (settings.record.writer != WriterMode::Null && writer_report.final_sync)
            .then(||rate(committed_bytes,writer_report.end_elapsed_ns)),
        "copy_count":progress.copy_count.load(Relaxed),"copy_bytes":progress.copy_bytes.load(Relaxed),
        "copy_active_ns":progress.copy_active_ns.load(Relaxed),
        "copy_bytes_per_active_second":rate(progress.copy_bytes.load(Relaxed), progress.copy_active_ns.load(Relaxed)),
        "copy_claims":progress.copy_claims.load(Relaxed),"copy_claim_contention":progress.copy_claim_contention.load(Relaxed),
        "spill_pressure":progress.copy_spill_pressure.load(Relaxed),"fixed_deadline_copies":progress.fixed_deadline_copies.load(Relaxed),
        "pool_deadline_copies":progress.pool_deadline_copies.load(Relaxed),"forced_copies":progress.forced_copies.load(Relaxed),
        "reserve_actions":progress.reserve_actions.load(Relaxed),"writing_peak_in_flight":progress.peak_in_flight.load(Relaxed),
        "writing_peak_borrowed":progress.peak_borrowed_in_flight.load(Relaxed),
        "writing_peak_in_flight_bytes":progress.peak_in_flight_bytes.load(Relaxed),
        "stall_windows":progress.stall_windows.load(Relaxed),"stall_wait_ns":progress.stall_wait_ns.load(Relaxed),
        "writer_submit_busy_ns":progress.write_busy_ns.load(Relaxed),"last_sync_elapsed_ns":progress.last_sync_ns.load(Relaxed),
        "timings_ns":{"generation":progress.fill_time.lock().unwrap().summary(),
            "queue_wait":progress.queue_wait.lock().unwrap().summary(),
            "copy":progress.copy_time.lock().unwrap().summary(),
            "submit_to_complete":progress.submit_to_complete.lock().unwrap().summary(),
            "sync":progress.sync_time.lock().unwrap().summary()},
        "warnings":progress.warnings(),"risk_episodes_without_loss":progress.risk_episodes_without_loss.load(Relaxed),
        "false_risk_episodes":progress.false_risk_episodes.load(Relaxed),
        "first_warning_to_loss_ns":progress.warning_lead_ns(),
        "process":{"cpu_time_ns":cpu_ns(ClockId::ProcessCPUTime)-cpu_start,"rss_bytes":rss,"peak_rss_bytes":peak_rss,
            "wall_ns":ns(main_start.elapsed())},
        "uncertain_range_ledger":outputs.losses,
        "sidecar_coverage_uncertain": journal_failures > 0 || !monitor_report.loss_synced || !writer_report.durability.journal_synced,
        "errors":errors,
    });
    metrics["durability_policy"] = serde_json::to_value(&settings.durability)?;
    if settings
        .sources
        .iter()
        .any(|source| !source.kind.is_synthetic())
    {
        metrics["timings_ns"]["accepted_to_committed"] =
            serde_json::to_value(progress.accepted_to_committed.lock().unwrap().summary())?;
    }
    if settings.record.copy_scheduling == CopyScheduling::Global {
        let global = &progress.global_copy;
        let earliest = global.earliest_due_ns.load(Relaxed);
        metrics["global_copy"] = json!({
            "peak_pending_copy_work_ns": global.peak_work_ns.load(Relaxed),
            "earliest_unmet_deadline_ns": (earliest != u64::MAX).then_some(earliest),
            "unfeasible_demand_events": global.infeasible_events.load(Relaxed),
            "spill_size_class_rejections": global.spill_size_class_rejections.load(Relaxed),
            "borrow_admission_deferrals": global.borrow_admission_deferrals.load(Relaxed),
            "copy_worker_busy_ns": progress.copy_active_ns.load(Relaxed),
            "copy_queue_wait_ns": global.queue_wait.lock().unwrap().summary(),
            "sources": sources.iter().zip(&settings.sources).map(|(source, cfg)| json!({
                "id": cfg.id,
                "peak_pending_borrowed": source.peak_pending_borrowed.load(Relaxed),
                "pool_exhausted_losses": source.pool_exhausted_losses.load(Relaxed),
                "risk_warning_lead_ns": source.warning_lead_ns(),
                "copy_bytes": source.copy_bytes.load(Relaxed),
                "copy_count": source.copy_count.load(Relaxed),
                "copy_reason_fixed": source.fixed_copies.load(Relaxed),
                "copy_reason_pool": source.pool_copies.load(Relaxed),
            })).collect::<Vec<_>>(),
        });
    }
    result
        .as_object_mut()
        .unwrap()
        .extend(metrics.as_object().unwrap().clone());
    serde_json::to_writer_pretty(&mut summary, &result)?;
    writeln!(summary)?;
    summary.sync_data()?;
    eprintln!(
        "{status:?}, Planner {:?}, committed payload {committed_bytes} B. {}",
        plan.verdict,
        outputs.summary.display()
    );
    ensure!(
        status == Status::Ok,
        "V2 recording ended with {status:?}. {}",
        errors.join(", ")
    );
    Ok(())
}

fn preflight(
    settings: &Settings,
    plan: &Plan,
    output: &Path,
    allow_unverified: bool,
    allow_large: bool,
) -> Result<()> {
    eprintln!(
        "Planner {:?}, input {} B/s, estimated MCAP {} B, memory {} B (payload {} B)",
        plan.verdict,
        plan.input_bytes_per_sec,
        plan.estimate_mcap_bytes,
        plan.memory_estimated_total_bytes,
        plan.memory_allocated_payload_bytes
    );
    ensure!(
        plan.memory_estimated_total_bytes <= plan.memory_budget_bytes,
        "recording exceeds the configured memory budget"
    );
    if let Some(available) = available_memory() {
        ensure!(
            plan.memory_estimated_total_bytes <= available,
            "recording exceeds available Linux memory"
        );
    }
    ensure!(
        allow_unverified || plan.verdict == Verdict::Accepted,
        "Planner {:?}. Use plan for details or --allow-unverified for an explicit experiment",
        plan.verdict
    );
    if settings.record.writer != WriterMode::Null {
        ensure!(
            plan.estimate_mcap_bytes <= settings.record.max_output_bytes,
            "recording exceeds record.max_output_bytes"
        );
        ensure!(
            plan.estimate_mcap_bytes <= 256 * MIB || allow_large,
            "check the output disk and pass --allow-large-output for more than 256 MiB"
        );
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stat = rustix::fs::statvfs(parent)?;
        let available = u128::from(stat.f_bavail) * u128::from(stat.f_frsize);
        ensure!(
            available >= u128::from(plan.estimate_mcap_bytes),
            "insufficient space on the output filesystem"
        );
    }
    Ok(())
}

fn available_memory() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?;
    line.split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}
