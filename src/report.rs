use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::loss::LossCounts;

pub const REPORT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Cancelled,
    Error,
}

#[derive(Serialize, Deserialize)]
pub struct Uncertain {
    pub start_sequence: u64,
    pub end_sequence_exclusive: u64,
    pub reason: String,
}

#[derive(Serialize, Deserialize)]
pub struct CoreReport {
    pub report_version: u32,
    pub status: Status,
    pub config: Config,
    pub start_unix_ns: u64,
    pub expected: u64,
    pub generated: u64,
    pub received: u64,
    pub written: u64,
    pub losses: LossCounts,
    pub loss_record_failures: u64,
    pub mcap_finalized: bool,
    pub mcap_synced: bool,
    pub loss_log_synced: bool,
    pub uncertain_ranges: Vec<Uncertain>,
}

pub struct Outputs {
    pub data: PathBuf,
    pub summary: PathBuf,
    pub metrics: PathBuf,
    pub losses: PathBuf,
}

impl Outputs {
    pub fn new(data: &Path) -> Result<Self> {
        let stem = data.file_stem().context("output needs a file name")?;
        let sidecar = |suffix: &str| {
            let mut name = stem.to_os_string();
            name.push(suffix);
            data.with_file_name(name)
        };
        Ok(Self {
            data: data.to_path_buf(),
            summary: sidecar("-summary.json"),
            metrics: sidecar("-metrics.csv"),
            losses: sidecar("-loss.jsonl"),
        })
    }

    pub fn check_new(&self) -> Result<()> {
        for path in [&self.data, &self.summary, &self.metrics, &self.losses] {
            match path.symlink_metadata() {
                Ok(_) => anyhow::bail!("output already exists: {}", path.display()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| format!("check {}", path.display()));
                }
            }
        }
        ensure!(
            self.data != self.summary && self.data != self.metrics && self.data != self.losses,
            "output name conflicts with a sidecar"
        );
        Ok(())
    }
}

pub fn create_new(path: &Path) -> Result<File> {
    File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}
