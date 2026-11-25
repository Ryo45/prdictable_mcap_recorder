mod adapter;
mod arrival;
mod capture;
mod config;
mod copy_plan;
mod copy_trace;
mod integrity;
mod layout;
mod metrics;
mod mock;
mod planner;
mod pool;
mod queue;
pub mod record;
pub mod recovery;
mod scheduler;
mod storage;
pub mod verify;
mod writer;

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use clap::Args;

#[derive(Clone, Default)]
struct Clock(Arc<OnceLock<Instant>>);

impl Clock {
    fn start_at(&self, start: Instant) {
        self.0.set(start).expect("clock initialized once");
    }
    fn now(&self) -> u64 {
        self.0
            .get()
            .map_or(0, |start| crate::metrics::ns(start.elapsed()))
    }
}

impl From<Instant> for Clock {
    fn from(start: Instant) -> Self {
        let clock = Self::default();
        clock.start_at(start);
        clock
    }
}

#[derive(Args)]
pub struct InspectSourceArgs {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    source: String,
}

pub fn list_sources() -> Result<()> {
    let kinds: Vec<_> = mock::Kind::ALL
        .iter()
        .map(|kind| {
            serde_json::json!({
                "kind": kind.name(), "payload_format": kind.payload_format(),
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&kinds)?);
    Ok(())
}

pub fn inspect_source(args: InspectSourceArgs) -> Result<()> {
    let settings = config::Settings::load(&args.config, None)?;
    let (index, source) = settings
        .sources
        .iter()
        .enumerate()
        .find(|(_, source)| source.id == args.source)
        .ok_or_else(|| anyhow::anyhow!("source {} is not in the configuration", args.source))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "capabilities": adapter::Capabilities::configured(source, index as u16), "configuration": source,
            "stream_metadata": layout::metadata(index as u16, &settings, 0),
        }))?
    );
    Ok(())
}

#[derive(Args)]
pub struct PlanArgs {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    profile: Option<PathBuf>,
    #[arg(long, default_value = "1", value_parser = crate::config::parse_duration)]
    duration_sec: Duration,
}

pub fn plan(args: PlanArgs) -> Result<()> {
    let settings = config::Settings::load(&args.config, args.profile.as_deref())?;
    let (plan, _) = planner::analyze(&settings, config::Settings::duration_ns(args.duration_sec)?)?;
    eprintln!(
        "{:?}, input {} B/s, estimated memory {} B",
        plan.verdict, plan.input_bytes_per_sec, plan.memory_estimated_total_bytes
    );
    for reason in &plan.reasons {
        eprintln!("{reason}");
    }
    for assumption in &plan.assumptions_missing {
        eprintln!("missing assumption: {assumption}");
    }
    let mut stdout = io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &plan)?;
    writeln!(stdout)?;
    Ok(())
}

#[derive(Args)]
pub struct BenchmarkArgs {
    #[arg(long, default_value = "v2-smoke", value_parser = ["v2-smoke", "v3-smoke", "ssd"])]
    suite: String,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    allow_large_output: bool,
    #[arg(long)]
    include_io_uring: bool,
    #[arg(long, default_value = "0.2")]
    high_seconds: String,
    #[arg(long, default_value_t = 30)]
    high_fps_per_source: u64,
}

pub fn benchmark(args: BenchmarkArgs) -> Result<()> {
    let mut command = Command::new("python3");
    if args.suite == "v3-smoke" {
        ensure!(
            command
                .arg("-c")
                .arg(include_str!("../../tools/bench_v3.py"))
                .arg("--recorder")
                .arg(std::env::current_exe()?)
                .arg("--out")
                .arg(args.output_dir)
                .arg("--preset-toml")
                .arg(include_str!("../../configs/v3-smoke.toml"))
                .status()?
                .success(),
            "V3 benchmark did not pass"
        );
        return Ok(());
    }
    command
        .arg("-c")
        .arg(include_str!("../../tools/bench_v2.py"))
        .arg("--recorder")
        .arg(std::env::current_exe()?)
        .arg("--suite")
        .arg(args.suite)
        .arg("--out")
        .arg(args.output_dir)
        .arg("--preset-toml")
        .arg(include_str!("../../configs/v2-smoke.toml"))
        .arg("--high-seconds")
        .arg(args.high_seconds)
        .arg("--high-fps-per-source")
        .arg(args.high_fps_per_source.to_string());
    if args.allow_large_output {
        command.arg("--allow-large-output");
    }
    if args.include_io_uring {
        command.arg("--include-io-uring");
    }
    ensure!(command.status()?.success(), "V2 benchmark did not pass");
    Ok(())
}
