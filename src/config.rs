use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::source::FRAME_HEADER_BYTES;

pub const NS_PER_SEC: u64 = 1_000_000_000;
const LARGE_OUTPUT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Parser)]
#[command(
    version,
    about = "Record and verify an uncompressed synthetic DMA stream"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Record one synthetic stream and write its sidecars.
    Record(Config),
    /// Record statically configured live sources.
    RecordLive(crate::v2::live::Args),
    /// Verify a live recording independently.
    VerifyLive(crate::v2::live::VerifyArgs),
    /// Show live resource bounds.
    PlanLive(crate::v2::live::PlanArgs),
    /// Read an MCAP and independently reconcile it against the sidecars.
    Verify(VerifyCli),
    /// Analyze a V2 recording configuration against its supplied resource bounds.
    Plan(crate::v2::PlanArgs),
    /// Run comparable V2 recorder experiments.
    Benchmark(crate::v2::BenchmarkArgs),
    /// Inspect an unchunked V2/V3 MCAP, optionally rebuilding a copy with indexes.
    Recover(crate::v2::recovery::Args),
    /// List built-in source fixtures.
    ListSources,
    /// Inspect one source configured for recording.
    InspectSource(crate::v2::InspectSourceArgs),
}

#[derive(Args)]
pub struct VerifyCli {
    #[arg(long, required_unless_present = "run_dir", conflicts_with = "run_dir")]
    pub input: Option<PathBuf>,
    #[arg(long, required_unless_present = "run_dir", conflicts_with = "run_dir")]
    pub report: Option<PathBuf>,
    #[arg(long)]
    pub run_dir: Option<PathBuf>,
    #[arg(long)]
    pub loss_log: Option<PathBuf>,
    #[arg(long)]
    pub commit_log: Option<PathBuf>,
}

pub struct VerifyArgs {
    pub input: PathBuf,
    pub report: PathBuf,
    pub loss_log: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    Copy,
    Borrow,
    Adaptive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sink {
    Mcap,
    Null,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct Config {
    /// Select the V2 multi-source recorder using a TOML configuration.
    #[arg(long, conflicts_with_all = [
        "policy", "sink", "frame_bytes", "fps", "dma_buffers", "capture_reserve",
        "spill_buffers", "queue_capacity", "max_hold_ms", "metrics_interval_ms",
        "loss_queue_capacity", "seed", "stall_at_ms", "stall_period_ms",
        "stall_duration_ms", "writer_bytes_per_sec"
    ])]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<PathBuf>,
    /// Override the V2 resource assumptions with a JSON profile.
    #[arg(long, requires = "config")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<PathBuf>,
    /// Permit a V2 experiment outside an accepted Planner contract.
    #[arg(long, requires = "config")]
    #[serde(default, skip_serializing)]
    pub allow_unverified: bool,
    #[arg(long, value_enum, default_value = "adaptive")]
    pub policy: Policy,
    #[arg(long, value_enum, default_value = "mcap")]
    pub sink: Sink,
    #[arg(long, default_value = "run.mcap")]
    pub output: PathBuf,
    #[arg(long, requires = "config", conflicts_with = "output")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 65_536)]
    pub frame_bytes: usize,
    #[arg(long, default_value_t = 60)]
    pub fps: u64,
    #[arg(long, default_value = "1", value_parser = parse_duration)]
    pub duration_sec: Duration,
    #[arg(long, default_value_t = 8)]
    pub dma_buffers: usize,
    #[arg(long, default_value_t = 2)]
    pub capture_reserve: usize,
    #[arg(long, default_value_t = 16)]
    pub spill_buffers: usize,
    #[arg(long, default_value_t = 24)]
    pub queue_capacity: usize,
    #[arg(long, default_value_t = 100)]
    pub max_hold_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub metrics_interval_ms: u64,
    #[arg(long, default_value_t = 1024)]
    pub loss_queue_capacity: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Start of a single stall, relative to capture start.
    #[arg(long, requires = "stall_duration_ms")]
    pub stall_at_ms: Option<u64>,
    /// First periodic stall starts one period after capture start.
    #[arg(long, requires = "stall_duration_ms")]
    pub stall_period_ms: Option<u64>,
    /// Shared length of single and periodic stalls.
    #[arg(long)]
    pub stall_duration_ms: Option<u64>,
    /// Maximum payload bytes per second. Pacing can hold a frame in the writer.
    #[arg(long)]
    pub writer_bytes_per_sec: Option<u64>,
    /// Explicit opt-in for MCAP output above 256 MiB. Check the destination first.
    #[arg(long)]
    pub allow_large_output: bool,
}

pub(crate) fn parse_duration(value: &str) -> Result<Duration, String> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("expected decimal seconds with up to 9 fractional digits".into());
    }
    let seconds = whole.parse().map_err(|_| "seconds out of range")?;
    let fraction_ns = fraction
        .bytes()
        .fold(0_u32, |n, byte| n * 10 + u32::from(byte - b'0'))
        * 10_u32.pow(9 - fraction.len() as u32);
    Ok(Duration::new(seconds, fraction_ns))
}

pub struct Plan {
    pub duration_ns: u64,
    pub expected: u64,
    pub input_bytes_per_sec: u64,
    pub payload_bytes: u64,
    pub pool_bytes: usize,
    pub estimated_mcap_bytes: u64,
    pub writer_interval: Option<Duration>,
}

impl Config {
    pub fn plan(&self) -> Result<Plan> {
        ensure!(
            self.frame_bytes >= FRAME_HEADER_BYTES,
            "frame-bytes must be at least {FRAME_HEADER_BYTES} for the frame ID"
        );
        ensure!(
            (1..=NS_PER_SEC).contains(&self.fps),
            "fps must be in 1..={NS_PER_SEC}"
        );
        let duration_ns =
            u64::try_from(self.duration_sec.as_nanos()).context("duration too large")?;
        ensure!(duration_ns > 0, "duration must be positive");
        ensure!(
            self.capture_reserve < self.dma_buffers,
            "capture-reserve must be less than dma-buffers"
        );
        ensure!(self.spill_buffers > 0, "spill-buffers must be positive");
        ensure!(
            self.queue_capacity > 0 && self.loss_queue_capacity > 0,
            "queue capacities must be positive"
        );
        ensure!(
            self.loss_queue_capacity
                .checked_add(1)
                .and_then(
                    |capacity| std::alloc::Layout::array::<(crate::loss::Event, usize)>(capacity)
                        .ok()
                )
                .is_some(),
            "loss queue allocation size overflow"
        );
        ensure!(
            self.max_hold_ms > 0 && self.metrics_interval_ms > 0,
            "hold and metrics intervals must be positive"
        );
        ensure!(
            self.stall_duration_ms.is_some()
                == (self.stall_at_ms.is_some() || self.stall_period_ms.is_some()),
            "stall-duration-ms requires a single or periodic stall, and vice versa"
        );
        if let Some(length) = self.stall_duration_ms {
            ensure!(length > 0, "stall duration must be positive");
            if let Some(period) = self.stall_period_ms {
                ensure!(
                    length < period,
                    "periodic stall duration must be shorter than its period"
                );
            }
        }
        for ms in [
            Some(self.max_hold_ms),
            Some(self.metrics_interval_ms),
            self.stall_at_ms,
            self.stall_period_ms,
            self.stall_duration_ms,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                ms.checked_mul(1_000_000).is_some(),
                "millisecond setting too large"
            );
        }

        let expected = u64::try_from(
            (u128::from(duration_ns) * u128::from(self.fps)).div_ceil(u128::from(NS_PER_SEC)),
        )?;
        let size = u64::try_from(self.frame_bytes)?;
        let input_bytes_per_sec = size.checked_mul(self.fps).context("input rate overflow")?;
        let payload_bytes = size
            .checked_mul(expected)
            .context("payload size overflow")?;
        let estimated_mcap_bytes = size
            .checked_add(31)
            .and_then(|size| size.checked_mul(expected))
            .and_then(|size| size.checked_add(4096))
            .context("MCAP size overflow")?;
        let pool_bytes = self
            .dma_buffers
            .checked_add(self.spill_buffers)
            .and_then(|count| count.checked_mul(self.frame_bytes))
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .context("pool size overflow")?;

        let writer_interval = self
            .writer_bytes_per_sec
            .map(|bytes_per_sec| {
                ensure!(bytes_per_sec > 0, "writer-bytes-per-sec must be positive");
                let ns =
                    (u128::from(size) * u128::from(NS_PER_SEC)).div_ceil(u128::from(bytes_per_sec));
                Ok(Duration::from_nanos(
                    u64::try_from(ns).context("writer interval too large")?,
                ))
            })
            .transpose()?;
        let now = Instant::now();
        for duration in [
            self.duration_sec,
            Duration::from_millis(self.max_hold_ms),
            writer_interval.unwrap_or_default(),
        ] {
            ensure!(
                now.checked_add(duration).is_some(),
                "monotonic time range overflow"
            );
        }

        Ok(Plan {
            duration_ns,
            expected,
            input_bytes_per_sec,
            payload_bytes,
            pool_bytes,
            estimated_mcap_bytes,
            writer_interval,
        })
    }

    pub fn check_output_space(&self, plan: &Plan) -> Result<()> {
        if self.sink == Sink::Mcap {
            ensure!(
                self.allow_large_output || plan.estimated_mcap_bytes <= LARGE_OUTPUT_BYTES,
                "MCAP estimate exceeds 256 MiB. Check the destination and pass --allow-large-output."
            );
        }
        let parent = self
            .output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let fs = rustix::fs::statvfs(parent)
            .with_context(|| format!("check available space in {}", parent.display()))?;
        let available = u128::from(fs.f_bavail) * u128::from(fs.f_frsize);
        if self.sink == Sink::Mcap {
            ensure!(
                available >= u128::from(plan.estimated_mcap_bytes),
                "estimated MCAP requires {} bytes, destination has {available} available",
                plan.estimated_mcap_bytes
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_duration_and_slot_count_are_exact() {
        let Cli {
            command: Command::Record(config),
        } = Cli::try_parse_from([
            "recorder",
            "record",
            "--duration-sec",
            "0.100000001",
            "--fps",
            "30",
        ])
        .unwrap()
        else {
            panic!("expected record command")
        };
        let plan = config.plan().unwrap();
        assert_eq!(plan.expected, 4);
        assert_eq!(plan.duration_ns, 100_000_001);
        assert!(parse_duration("0.0000000001").is_err());
    }
}
