use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

pub const NS: u64 = 1_000_000_000;
pub const MS: u64 = 1_000_000;
pub const US: u64 = 1_000;
pub const MIB: u64 = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub record: Record,
    #[serde(default)]
    pub storage_profile: StorageProfile,
    #[serde(default)]
    pub copy_profile: CopyProfile,
    #[serde(default)]
    pub faults: Faults,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub spill: Vec<Spill>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Record {
    pub policy: Policy,
    pub writer: WriterMode,
    pub queue_capacity: usize,
    pub memory_budget_bytes: u64,
    pub in_flight: usize,
    pub max_borrowed_in_flight: usize,
    pub commit_interval_ms: u64,
    pub commit_bytes: u64,
    pub commit_messages: usize,
    pub metrics_interval_ms: u64,
    pub loss_queue_capacity: usize,
    pub seed: u64,
    pub max_output_bytes: u64,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            policy: Policy::DeadlineAware,
            writer: WriterMode::Sync,
            queue_capacity: 128,
            memory_budget_bytes: 128 * MIB,
            in_flight: 4,
            max_borrowed_in_flight: 1,
            commit_interval_ms: 500,
            commit_bytes: 64 * MIB,
            commit_messages: 4096,
            metrics_interval_ms: 50,
            loss_queue_capacity: 1024,
            seed: 42,
            max_output_bytes: 256 * MIB,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Policy {
    AlwaysCopy,
    AlwaysBorrow,
    V1Threshold,
    DeadlineAware,
    DeadlineAwareReserve,
}

impl Policy {
    pub fn adaptive(self) -> bool {
        matches!(self, Self::DeadlineAware | Self::DeadlineAwareReserve)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WriterMode {
    Sync,
    MockAsync,
    IoUring,
    Null,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageProfile {
    pub sustained_bytes_per_sec: Option<u64>,
    pub max_stall_ms: Option<u64>,
    pub max_io_us: Option<u64>,
    pub max_sync_ms: Option<u64>,
    pub min_stall_period_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CopyProfile {
    pub assumed_min_bytes_per_sec: Option<u64>,
    pub return_margin_us: Option<u64>,
    pub scheduling_margin_us: Option<u64>,
    pub max_capture_delay_us: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Faults {
    pub stall_at_ms: Option<u64>,
    pub stall_period_ms: Option<u64>,
    pub stall_duration_ms: Option<u64>,
    pub writer_bytes_per_sec: Option<u64>,
    pub copy_delay_us: u64,
    pub copy_fail_attempt: Option<u64>,
    pub io_short_bytes: Option<usize>,
    pub io_error_request: Option<u64>,
    pub io_error: Option<IoFault>,
    pub sync_fail_group: Option<u64>,
    pub mock_delay_us: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IoFault {
    Eio,
    Enospc,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub id: String,
    #[serde(default, skip_serializing_if = "super::mock::Kind::is_synthetic")]
    pub kind: super::mock::Kind,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub fence_delay_us: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_every: Option<u64>,
    pub model: Model,
    pub frame_bytes: usize,
    pub fps: u64,
    #[serde(default = "one")]
    pub burst: u64,
    #[serde(default)]
    pub phase_us: u64,
    #[serde(default)]
    pub jitter_us: u64,
    #[serde(default = "default_dma")]
    pub dma_buffers: usize,
    #[serde(default)]
    pub capture_reserve: usize,
    #[serde(default)]
    pub reserve_buffers: usize,
    #[serde(default)]
    pub replacement: Option<ReplacementMode>,
    pub max_hold_ms: Option<u64>,
    #[serde(default)]
    pub return_delay_us: u64,
    #[serde(default = "yes")]
    pub arrival_bound_known: bool,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn one() -> u64 {
    1
}
fn default_dma() -> usize {
    8
}
fn yes() -> bool {
    true
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Model {
    FixedDeadline,
    Pool,
    Replaceable,
    Owned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplacementMode {
    Unsupported,
    PreRegisteredPool,
    ExplicitReplacement,
}

impl Source {
    pub fn replacement(&self) -> ReplacementMode {
        self.replacement
            .unwrap_or(if self.model == Model::Replaceable {
                ReplacementMode::ExplicitReplacement
            } else {
                ReplacementMode::Unsupported
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spill {
    pub frame_bytes: usize,
    pub buffers: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Budget {
    pub payload_bytes: u64,
    pub metadata_bytes: u64,
    pub total_bytes: u64,
}

impl Settings {
    pub fn load(path: &Path, profile: Option<&Path>) -> Result<Self> {
        let mut settings: Self = toml::from_str(
            &fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        if let Some(path) = profile {
            let override_profile: Profile = serde_json::from_slice(&fs::read(path)?)?;
            settings.storage_profile = override_profile.storage_profile;
            settings.copy_profile = override_profile.copy_profile;
        }
        settings.validate()?;
        Ok(settings)
    }

    pub fn depth(&self) -> usize {
        if self.record.writer == WriterMode::MockAsync || self.record.writer == WriterMode::IoUring
        {
            self.record.in_flight
        } else {
            1
        }
    }

    pub fn validate(&self) -> Result<Budget> {
        let record = &self.record;
        ensure!(
            (1..u16::MAX as usize).contains(&self.sources.len()),
            "expected 1 to 65534 sources"
        );
        ensure!(
            record.queue_capacity > 0
                && record.in_flight > 0
                && record.max_borrowed_in_flight > 0
                && record.loss_queue_capacity > 0,
            "queue, loss, and in-flight capacities must be positive"
        );
        ensure!(
            record.in_flight <= u32::MAX as usize
                && record.commit_messages > 0
                && record.commit_bytes > 0
                && record.commit_interval_ms > 0
                && record.metrics_interval_ms > 0
                && record.max_output_bytes > 0,
            "invalid I/O, commit, metrics or output limit"
        );
        ensure!(
            record.memory_budget_bytes > 0,
            "memory budget must be positive"
        );
        let mut ids = HashSet::with_capacity(self.sources.len());
        let mut payload = 0_u128;
        let mut slots = 0_u128;
        for source in &self.sources {
            ensure!(
                !source.id.is_empty()
                    && source.id.len() <= 80
                    && source
                        .id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
                    && ids.insert(&source.id),
                "invalid or repeated source ID {}",
                source.id
            );
            ensure!(
                source.frame_bytes >= crate::source::FRAME_HEADER_BYTES
                    && source.frame_bytes <= isize::MAX as usize - 31
                    && (1..=NS).contains(&source.fps)
                    && source.burst > 0,
                "invalid frame size or arrival rate for {}",
                source.id
            );
            ensure!(
                source.dma_buffers > 0 && source.capture_reserve < source.dma_buffers,
                "invalid DMA capacity for {}",
                source.id
            );
            source.kind.validate(source)?;
            ensure!(
                source.fence_delay_us <= 100_000,
                "mock fence delay exceeds drain limit"
            );
            micros(source.phase_us)?;
            micros(source.jitter_us)?;
            micros(source.return_delay_us)?;
            ensure!(
                source.jitter_us * US < NS / source.fps,
                "jitter must be shorter than the source period"
            );
            ensure!(
                (source.model == Model::FixedDeadline) == source.max_hold_ms.is_some(),
                "max_hold_ms is required exactly for fixed-deadline sources"
            );
            if let Some(hold) = source.max_hold_ms {
                ensure!(hold > 0, "max_hold_ms must be positive");
                millis(hold)?;
            }
            ensure!(
                (source.reserve_buffers == 0)
                    == (source.replacement() == ReplacementMode::Unsupported),
                "reserve_buffers must match the replacement capability for {}",
                source.id
            );
            ensure!(
                (source.replacement() != ReplacementMode::ExplicitReplacement
                    || source.model == Model::Replaceable)
                    && (source.replacement() == ReplacementMode::Unsupported
                        || matches!(source.model, Model::Pool | Model::Replaceable)),
                "unsupported replacement model for {}",
                source.id
            );
            if source.model == Model::Owned {
                ensure!(
                    source.capture_reserve == 0 && source.return_delay_us == 0,
                    "owned source has no device reserve or return delay"
                );
            }
            let count = source
                .dma_buffers
                .checked_add(source.reserve_buffers)
                .context("device pool size overflow")?;
            payload = payload
                .checked_add(count as u128 * source.frame_bytes as u128)
                .context("payload budget overflow")?;
            slots += count as u128;
        }
        let mut last_size = 0;
        for class in &self.spill {
            ensure!(
                class.frame_bytes >= crate::source::FRAME_HEADER_BYTES
                    && class.frame_bytes <= isize::MAX as usize - 31
                    && class.frame_bytes > last_size
                    && class.buffers > 0,
                "spill classes must have positive counts and strictly increasing frame sizes"
            );
            last_size = class.frame_bytes;
            payload = payload
                .checked_add(class.buffers as u128 * class.frame_bytes as u128)
                .context("spill budget overflow")?;
            slots += class.buffers as u128;
        }
        let faults = &self.faults;
        ensure!(
            faults.stall_duration_ms.is_some()
                == (faults.stall_at_ms.is_some() || faults.stall_period_ms.is_some()),
            "stall duration requires a single or periodic start"
        );
        if let Some(length) = faults.stall_duration_ms {
            ensure!(
                length > 0 && faults.stall_period_ms.is_none_or(|p| p > length),
                "invalid stall period/duration"
            );
        }
        ensure!(
            faults.io_error_request.is_some() == faults.io_error.is_some(),
            "provide both I/O fault type and request ID"
        );
        ensure!(
            faults.io_short_bytes != Some(0)
                && faults.writer_bytes_per_sec != Some(0)
                && self.storage_profile.sustained_bytes_per_sec != Some(0)
                && self.copy_profile.assumed_min_bytes_per_sec != Some(0),
            "rates and short-write size must be positive"
        );
        for value in [
            Some(record.commit_interval_ms),
            Some(record.metrics_interval_ms),
            faults.stall_duration_ms,
            faults.stall_at_ms,
            faults.stall_period_ms,
            self.storage_profile.max_stall_ms,
            self.storage_profile.max_sync_ms,
            self.storage_profile.min_stall_period_ms,
        ]
        .into_iter()
        .flatten()
        {
            millis(value)?;
        }
        for value in [
            Some(faults.copy_delay_us),
            Some(faults.mock_delay_us),
            self.storage_profile.max_io_us,
            self.copy_profile.return_margin_us,
            self.copy_profile.scheduling_margin_us,
            self.copy_profile.max_capture_delay_us,
        ]
        .into_iter()
        .flatten()
        {
            micros(value)?;
        }
        // Upper estimates for fixed metadata, task stacks, histograms, sidecar staging,
        // the completion registry and MCAP preamble. Payload pools are exact.
        let metadata = 10_u128 * MIB as u128
            + self.sources.len() as u128 * 65_536
            + self.spill.len() as u128 * 32_768
            + slots * 192
            + (record.queue_capacity as u128 + record.in_flight as u128) * 512
            + record.commit_messages as u128 * 256
            + record.loss_queue_capacity as u128 * 256;
        let total = payload
            .checked_add(metadata)
            .context("memory budget overflow")?;
        ensure!(
            total <= isize::MAX as u128,
            "memory budget address space overflow"
        );
        Ok(Budget {
            payload_bytes: payload as u64,
            metadata_bytes: metadata as u64,
            total_bytes: total as u64,
        })
    }

    pub fn duration_ns(duration: Duration) -> Result<u64> {
        let ns = u64::try_from(duration.as_nanos()).context("duration too large")?;
        ensure!(
            ns > 0 && ns <= i64::MAX as u64,
            "duration must be positive and fit the timestamp range"
        );
        Ok(ns)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    storage_profile: StorageProfile,
    copy_profile: CopyProfile,
}

fn micros(value: u64) -> Result<u64> {
    value
        .checked_mul(US)
        .filter(|&v| v <= i64::MAX as u64)
        .context("microsecond value overflow")
}

fn millis(value: u64) -> Result<u64> {
    value
        .checked_mul(MS)
        .filter(|&v| v <= i64::MAX as u64)
        .context("millisecond value overflow")
}

pub fn transfer_time(bytes: u64, rate: u64) -> u64 {
    u64::try_from((u128::from(bytes) * u128::from(NS)).div_ceil(u128::from(rate)))
        .unwrap_or(u64::MAX)
}
