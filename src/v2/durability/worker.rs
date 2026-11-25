use std::fs::File;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering::Relaxed},
    mpsc::{self, Receiver, SyncSender},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::metrics::ns;
use anyhow::{Context, Result, bail};

use super::{
    group::Group,
    journal::{Journal, JournalReport},
};
use crate::v2::config::Faults;

pub(super) struct Job {
    pub group: Arc<Mutex<Group>>,
    pub sealed_at: u64,
    pub boundary: (u64, Option<u32>),
    pub final_group: bool,
}

pub(super) struct Outcome {
    pub id: u64,
    pub sealed_at: u64,
    pub boundary: (u64, Option<u32>),
    pub final_group: bool,
    pub sync_elapsed: Option<u64>,
    pub data_synced_at: Option<u64>,
    pub confirmed_at: u64,
    pub journal: JournalReport,
    pub journal_written: bool,
    pub cpu_ns: u64,
    pub error: Option<String>,
}

pub(super) struct Worker {
    pub tx: Option<SyncSender<Job>>,
    pub rx: Receiver<Outcome>,
    pub syncing: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(
        file: Option<File>,
        mut journal: Journal,
        faults: Faults,
        size: usize,
        start: Instant,
    ) -> Result<Self> {
        let (tx, jobs) = mpsc::sync_channel::<Job>(size);
        let (results, rx) = mpsc::sync_channel(size);
        let syncing = Arc::new(AtomicU64::new(0));
        let activity = syncing.clone();
        let handle = thread::Builder::new()
            .name("durability".into())
            .spawn(move || {
                let mut failed = false;
                while let Ok(job) = jobs.recv() {
                    let cpu_start = crate::metrics::cpu_ns(rustix::time::ClockId::ThreadCPUTime);
                    let mut group = job.group.lock().unwrap();
                    let id = group.id();
                    if faults.durability_stop_group == Some(id) {
                        return;
                    }
                    let mut sync_elapsed = None;
                    let mut data_synced_at = None;
                    let mut journal_written = false;
                    let result: Result<()> = (|| {
                        if failed {
                            bail!("earlier durability group failed");
                        }
                        let timer = Instant::now();
                        activity.store(id + 1, Relaxed);
                        if let Some(delay) = faults.durability_sync_delay_ms {
                            thread::sleep(Duration::from_millis(delay));
                        }
                        let sync = if faults.sync_fail_group == Some(id) {
                            Err(std::io::Error::from_raw_os_error(libc::EIO))
                        } else {
                            file.as_ref().map_or(Ok(()), File::sync_data)
                        };
                        activity.store(0, Relaxed);
                        sync_elapsed = Some(ns(timer.elapsed()));
                        sync.with_context(|| format!("sync group {id}"))?;
                        let now = ns(start.elapsed());
                        data_synced_at = Some(now);
                        let journal_result = group
                            .write_evidence(&mut journal, job.boundary, job.final_group, now)
                            .and_then(|()| {
                                journal_written = true;
                                if faults.journal_fail_group == Some(id) {
                                    bail!("injected journal failure");
                                }
                                journal.sync()
                            });
                        if journal_result.is_err() {
                            journal.report.failures += 1;
                            journal.discard_unconfirmed();
                        }
                        journal_result.with_context(|| format!("commit journal group {id}"))
                    })();
                    failed |= result.is_err();
                    drop(group);
                    drop(job.group);
                    if results
                        .send(Outcome {
                            id,
                            sealed_at: job.sealed_at,
                            boundary: job.boundary,
                            final_group: job.final_group,
                            sync_elapsed,
                            data_synced_at,
                            confirmed_at: ns(start.elapsed()),
                            journal: journal.report,
                            journal_written,
                            cpu_ns: crate::metrics::cpu_ns(rustix::time::ClockId::ThreadCPUTime)
                                - cpu_start,
                            error: result.err().map(|error| format!("{error:#}")),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })?;
        Ok(Self {
            tx: Some(tx),
            rx,
            syncing,
            handle: Some(handle),
        })
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.tx.take();
        // A worker can produce at most as many results as the bounded pending queue.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
