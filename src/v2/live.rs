mod codec;
mod network;
mod ros;
mod transform;

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    mpsc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;

use self::codec::Metadata;
use self::transform::{Pipeline, Transform};
use super::{
    Clock,
    adapter::{Frame, FrameId, Payload},
    config::{Durability, Record, Settings, WriterMode},
    integrity::{Layer, Ledger, Monitor, MonitorFiles, Reason, Span},
    layout::{Layout, MESSAGE_HEADER_BYTES, TAIL_BYTES},
    metrics::{Progress, SourceProgress},
    pool::{Pool, ReturnPolicy},
    queue::{Job, Queue},
    writer::{Writer, backend},
};
use crate::{
    metrics::ns,
    report::{Outputs, create_new},
};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 10)]
    duration_sec: u64,
    #[arg(long)]
    allow_large_output: bool,
}

#[derive(clap::Args)]
pub struct PlanArgs {
    #[arg(long)]
    config: PathBuf,
}

#[derive(clap::Args)]
pub struct VerifyArgs {
    #[arg(long)]
    input: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    record: Record,
    #[serde(default)]
    durability: Durability,
    sources: Vec<Source>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    id: String,
    kind: Kind,
    bind: Option<SocketAddr>,
    interface: Option<String>,
    #[serde(default)]
    enable_fd: bool,
    #[serde(default)]
    error_frames: bool,
    topic: Option<String>,
    message_type: Option<String>,
    qos: Option<String>,
    bridge: Option<PathBuf>,
    max_payload_bytes: Option<usize>,
    #[serde(default = "buffers")]
    buffers: usize,
    #[serde(default)]
    transform: Transform,
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Udp,
    Socketcan,
    Ros2,
}

fn buffers() -> usize {
    16
}

impl Source {
    fn max(&self) -> usize {
        match self.kind {
            Kind::Udp => self.max_payload_bytes.unwrap_or(65535),
            Kind::Socketcan => 64,
            Kind::Ros2 => self.max_payload_bytes.unwrap_or(1024 * 1024),
        }
    }

    fn capacity(&self) -> usize {
        codec::capacity(self.max(), self.transform, self.kind == Kind::Ros2)
    }

    fn label(&self) -> &'static str {
        match self.kind {
            Kind::Udp => "udp",
            Kind::Socketcan => "socketcan",
            Kind::Ros2 => "ros2",
        }
    }
}

pub(super) struct Registration {
    pub topic: String,
    pub schema_name: String,
    pub schema_encoding: String,
    pub schema: Vec<u8>,
    pub encoding: String,
    pub metadata: BTreeMap<String, String>,
}

pub(super) trait Backend: Send {
    fn descriptor(&self) -> std::os::fd::RawFd;
    fn receive(&mut self, bytes: &mut [u8]) -> Result<Option<Receipt>>;
}

pub(super) struct Receipt {
    metadata: Metadata,
    length: usize,
    truncated: bool,
    drops: Option<u32>,
    source_unix_ns: Option<u64>,
}

fn load(path: &std::path::Path) -> Result<(Settings, Vec<Source>, u64)> {
    let config: Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    let mut ids = HashSet::new();
    ensure!(
        (1..=32).contains(&config.sources.len()),
        "configure 1–32 sources"
    );
    for s in &config.sources {
        ensure!(
            !s.id.is_empty()
                && s.id.len() <= 64
                && s.id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
                && ids.insert(&s.id),
            "invalid or duplicate source ID"
        );
        ensure!(
            (1..=4096).contains(&s.buffers) && (1..=16 * 1024 * 1024).contains(&s.max()),
            "invalid source bounds"
        );
        match s.kind {
            Kind::Udp => ensure!(
                s.bind.is_some() && s.max() <= 65535,
                "UDP requires bind and max payload <= 65535"
            ),
            Kind::Socketcan => ensure!(
                s.interface.as_ref().is_some_and(|s| !s.is_empty()),
                "SocketCAN requires interface"
            ),
            Kind::Ros2 => {
                ensure!(
                    s.transform == Transform::Identity && s.max() >= 4,
                    "ROS 2 CDR requires identity and a valid bound"
                );
                ensure!(
                    s.topic.as_ref().is_some_and(|v| v.starts_with('/'))
                        && s.message_type
                            .as_ref()
                            .is_some_and(|v| v.split('/').count() == 3),
                    "ROS 2 requires an absolute topic and message_type"
                );
                ensure!(
                    matches!(s.qos.as_deref(), None | Some("sensor-data" | "reliable")),
                    "invalid ROS QoS"
                );
            }
        }
    }
    let mut settings = Settings {
        record: config.record,
        durability: config.durability,
        faults: Default::default(),
        storage_profile: Default::default(),
        copy_profile: Default::default(),
        sources: vec![],
        spill: vec![],
    };
    settings.apply_durability();
    let r = &settings.record;
    let d = &settings.durability;
    ensure!(
        matches!(r.writer, WriterMode::Sync | WriterMode::IoUring)
            && (1..=256).contains(&r.in_flight)
            && (1..=65536).contains(&r.queue_capacity)
            && (1..=65536).contains(&r.commit_messages)
            && r.commit_bytes > 0
            && (1..=3_600_000).contains(&r.commit_interval_ms)
            && (1..=60_000).contains(&r.metrics_interval_ms)
            && (1..=65536).contains(&r.loss_queue_capacity)
            && r.max_output_bytes > 0
            && (1..=64).contains(&d.max_pending_groups)
            && (1..=64).contains(&d.event_queue_capacity)
            && d.max_pending_bytes >= max_wire(&config.sources),
        "invalid recording or durability bounds"
    );
    let memory = config
        .sources
        .iter()
        .map(|s| s.capacity() as u64 * s.buffers as u64 + s.max() as u64 + 3 * 1024 * 1024)
        .sum::<u64>()
        + ((d.sealed_limit() + 1) * r.commit_messages * 256
            + (r.queue_capacity + r.loss_queue_capacity) * 256) as u64;
    ensure!(
        memory <= r.memory_budget_bytes,
        "live buffer estimate exceeds memory budget"
    );
    Ok((settings, config.sources, memory))
}

fn max_wire(sources: &[Source]) -> u64 {
    (sources.iter().map(Source::capacity).max().unwrap_or(0) + MESSAGE_HEADER_BYTES) as u64
}

pub fn plan(args: PlanArgs) -> Result<()> {
    let (settings, sources, memory) = load(&args.config)?;
    println!(
        "{}",
        json!({"verdict":"Unknown","reason":"external arrival and storage bounds are unqualified",
        "max_wire_message_bytes":max_wire(&sources),"estimated_local_bytes":memory,
        "output_limit_bytes":settings.record.max_output_bytes})
    );
    Ok(())
}

fn open(source: &Source, stream: usize, epoch: u64) -> Result<(Box<dyn Backend>, Registration)> {
    let mut metadata = BTreeMap::from([
        ("stream_id".into(), stream.to_string()),
        ("source_id".into(), source.id.clone()),
        ("session_epoch".into(), epoch.to_string()),
        ("source_kind".into(), source.label().into()),
        ("max_payload_bytes".into(), source.max().to_string()),
        (
            "clock_domain".into(),
            "recorder_monotonic_anchored_to_unix_ns".into(),
        ),
        ("publish_time_origin".into(), "receive_time_fallback".into()),
        ("sequence_origin".into(), "recorder_observed".into()),
        (
            "transform".into(),
            if source.transform == Transform::Zstd {
                "zstd:1:level=1"
            } else {
                "identity:1"
            }
            .into(),
        ),
    ]);
    let (backend, topic, name, schema_encoding, schema, encoding): (
        Box<dyn Backend>,
        _,
        _,
        _,
        _,
        _,
    ) = match source.kind {
        Kind::Udp => (
            Box::new(network::Udp::open(source.bind.unwrap())?),
            format!("/udp/{}", source.id),
            "recorder.UdpDatagramV1".into(),
            "recorder-binary-v1",
            b"recorder-udp-v1; see real-source-architecture.md".to_vec(),
            codec::UDP_ENCODING,
        ),
        Kind::Socketcan => {
            let interface = source.interface.as_deref().unwrap();
            metadata.insert("interface".into(), interface.into());
            (
                Box::new(network::Can::open(
                    interface,
                    source.enable_fd,
                    source.error_frames,
                )?),
                format!("/socketcan/{}", source.id),
                "recorder.SocketCanV1".into(),
                "recorder-binary-v1",
                b"recorder-socketcan-v1; see real-source-architecture.md".to_vec(),
                codec::CAN_ENCODING,
            )
        }
        Kind::Ros2 => {
            let qos = source.qos.as_deref().unwrap_or("sensor-data");
            let topic = source.topic.as_deref().unwrap();
            let kind = source.message_type.as_deref().unwrap();
            let helper = source
                .bridge
                .clone()
                .unwrap_or_else(|| "tools/ros2_raw_bridge.py".into());
            let (ros, schema) = ros::Ros::open(&helper, topic, kind, qos, source.max())?;
            metadata.insert("qos".into(), qos.into());
            metadata.insert(
                "publish_time_origin".into(),
                "dds_source_timestamp_or_receive_time".into(),
            );
            metadata.insert(
                "schema_origin".into(),
                "installed_ros2_msg_definitions".into(),
            );
            (
                Box::new(ros),
                topic.into(),
                kind.into(),
                "ros2msg",
                schema,
                "cdr",
            )
        }
    };
    Ok((
        backend,
        Registration {
            topic,
            schema_name: name,
            schema_encoding: schema_encoding.into(),
            schema,
            encoding: encoding.into(),
            metadata,
        },
    ))
}

#[derive(Default, Serialize)]
struct ReceiveReport {
    kernel_drops_observed: Option<u64>,
    receive_error: Option<String>,
}

struct Ingress<'a> {
    queue: &'a Queue,
    progress: &'a Progress,
    stop: &'a AtomicBool,
    wire_remaining: &'a AtomicU64,
    start: Instant,
    duration: Duration,
    epoch: u64,
}

impl Ingress<'_> {
    fn receive(
        &self,
        mut input: Box<dyn Backend>,
        source: &Source,
        stream: usize,
        pool: &Pool,
        progress: &SourceProgress,
        mut ledger: Ledger<'_>,
    ) -> ReceiveReport {
        let mut report = ReceiveReport::default();
        let mut scratch = vec![0; source.max()];
        let mut transform = match Pipeline::new(source.transform) {
            Ok(value) => value,
            Err(error) => {
                report.receive_error = Some(error.to_string());
                return report;
            }
        };
        let mut previous_drops = 0_u32;
        let mut sequence = 0_u64;
        while self.start.elapsed() < self.duration
            && !self.stop.load(Relaxed)
            && !self.progress.stopped()
            && !crate::stop::REQUESTED.load(Relaxed)
        {
            let receipt = match input.receive(&mut scratch) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    let mut descriptor = libc::pollfd {
                        fd: input.descriptor(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // poll does not take ownership of the descriptor.
                    unsafe { libc::poll(&mut descriptor, 1, 10) };
                    continue;
                }
                Err(error) => {
                    report.receive_error = Some(format!("{error:#}"));
                    break;
                }
            };
            let now = ns(self.start.elapsed());
            if let Some(drops) = receipt.drops {
                *report.kernel_drops_observed.get_or_insert(0) +=
                    u64::from(drops.wrapping_sub(previous_drops));
                previous_drops = drops;
            }
            if sequence > u64::from(u32::MAX) {
                report.receive_error = Some("MCAP sequence space exhausted".into());
                break;
            }
            let id = FrameId {
                source: stream as u16,
                session_epoch: self.epoch,
                sequence,
            };
            sequence += 1;
            progress.generated.fetch_add(1, Relaxed);
            if receipt.truncated {
                ledger.loss(
                    Span::frame(id),
                    now,
                    Layer::Source,
                    if source.kind == Kind::Udp {
                        Reason::DatagramTruncated
                    } else {
                        Reason::SourcePayloadTooLarge
                    },
                    false,
                );
                continue;
            }
            let Some((mut lease, _)) = pool.acquire(now) else {
                ledger.loss(
                    Span::frame(id),
                    now,
                    Layer::Acquisition,
                    Reason::DmaPoolExhausted,
                    false,
                );
                continue;
            };
            let record = codec::Record {
                metadata: receipt.metadata,
                payload: &scratch[..receipt.length],
            };
            let size =
                match codec::encode(record, source.transform, &mut transform, lease.bytes_mut()) {
                    Ok(size) => size,
                    Err(_) => {
                        ledger.loss(
                            Span::frame(id),
                            now,
                            Layer::Source,
                            Reason::TransformFailure,
                            false,
                        );
                        continue;
                    }
                };
            let wire = (size + MESSAGE_HEADER_BYTES) as u64;
            if self
                .wire_remaining
                .fetch_update(Relaxed, Relaxed, |remaining| remaining.checked_sub(wire))
                .is_err()
            {
                ledger.loss(
                    Span::frame(id),
                    now,
                    Layer::Source,
                    Reason::OutputLimit,
                    false,
                );
                self.stop.store(true, Relaxed);
                break;
            }
            if self.queue.push(Job {
                frame: Frame {
                    id,
                    planned_ns: receipt.source_unix_ns.unwrap_or(self.epoch + now),
                    captured_ns: now,
                    queued_ns: ns(self.start.elapsed()),
                    payload: Payload::Owned(lease),
                    size,
                },
                force_copy: false,
                retry_copy_ns: 0,
            }) {
                progress.accepted.fetch_add(1, Relaxed);
            } else {
                self.wire_remaining.fetch_add(wire, Relaxed);
                ledger.loss(
                    Span::frame(id),
                    now,
                    Layer::Acquisition,
                    Reason::QueueFull,
                    false,
                );
            }
        }
        report
    }
}

pub fn run(args: Args) -> Result<()> {
    let (settings, config, memory) = load(&args.config)?;
    let cpu_start = crate::metrics::cpu_ns(rustix::time::ClockId::ProcessCPUTime);
    ensure!(
        (1..=3600).contains(&args.duration_sec),
        "duration outside 1–3600 seconds"
    );
    let output = Outputs::new(&args.output)?;
    output.check_new()?;
    let commits_path = super::record::commit_path(&args.output)?;
    ensure!(
        commits_path.symlink_metadata().is_err(),
        "commit output exists"
    );
    ensure!(
        args.allow_large_output || settings.record.max_output_bytes <= 256 * 1024 * 1024,
        "explicit large output opt-in required"
    );
    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let fs = rustix::fs::statvfs(parent)?;
    ensure!(
        u128::from(fs.f_bavail) * u128::from(fs.f_frsize)
            >= u128::from(settings.record.max_output_bytes),
        "insufficient space for MCAP budget"
    );
    super::storage::probe(settings.record.writer)?;
    crate::stop::install()?;
    let start = Instant::now();
    let epoch = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let (backends, registrations): (Vec<_>, Vec<_>) = config
        .iter()
        .enumerate()
        .map(|(i, source)| {
            open(source, i, epoch).with_context(|| format!("open source {}", source.id))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    let (layout, preamble) = Layout::live(&registrations, epoch)?;
    let wire_remaining = AtomicU64::new(
        settings
            .record
            .max_output_bytes
            .checked_sub((preamble.len() + TAIL_BYTES) as u64)
            .context("output budget does not fit MCAP metadata")?,
    );
    let clock = Clock::default();
    let pools = config
        .iter()
        .enumerate()
        .map(|(i, s)| {
            Pool::new(
                i,
                s.buffers,
                0,
                s.capacity(),
                clock.clone(),
                ReturnPolicy::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let file = create_new(&output.data)?;
    let commits = create_new(&commits_path)?;
    let files = MonitorFiles {
        loss: create_new(&output.losses)?,
        csv: create_new(&output.metrics)?,
    };
    let mut summary = create_new(&output.summary)?;
    File::open(parent)?.sync_all()?;
    clock.start_at(start);
    let queue = Queue::new(settings.record.queue_capacity, config.len());
    let progress = Progress::default();
    let sources: Vec<_> = config.iter().map(|_| SourceProgress::default()).collect();
    let stop = AtomicBool::new(false);
    let writer = Writer {
        trace: None,
        start,
        settings: &settings,
        devices: &[],
        sources: &sources,
        progress: &progress,
        queue: &queue,
        max_wire_frame: max_wire(&config),
    };
    let live_pools: Vec<_> = config
        .iter()
        .zip(&pools)
        .map(|(source, pool)| (source.id.as_str(), pool))
        .collect();
    let monitor = Monitor {
        live_pools: &live_pools,
        start,
        settings: &settings,
        devices: &[],
        spill: &[],
        sources: &sources,
        progress: &progress,
        queue: &queue,
    };
    let ingress = Ingress {
        queue: &queue,
        progress: &progress,
        stop: &stop,
        wire_remaining: &wire_remaining,
        duration: start.elapsed() + Duration::from_secs(args.duration_sec),
        start,
        epoch,
    };
    let (sender, receiver) = mpsc::sync_channel(settings.record.loss_queue_capacity);
    let (writer_report, monitor_report, captures) = std::thread::scope(|scope| {
        let monitor = scope.spawn(|| monitor.run(receiver, files));
        let writer_ledger = Ledger::new(sender.clone(), &progress, &sources);
        let writer = scope.spawn(|| match backend(&settings, Some(file)) {
            Ok(backend) => writer.run(writer_ledger, backend, layout, &preamble, commits),
            Err(error) => {
                progress.abort.store(true, Relaxed);
                super::writer::WriterReport {
                    error: Some(format!("initialize backend: {error:#}")),
                    ..Default::default()
                }
            }
        });
        let mut handles = Vec::new();
        for (i, input) in backends.into_iter().enumerate() {
            let ledger = Ledger::new(sender.clone(), &progress, &sources);
            let (ingress, cfg, pool, source) = (&ingress, &config[i], &pools[i], &sources[i]);
            handles.push(scope.spawn(move || ingress.receive(input, cfg, i, pool, source, ledger)));
        }
        drop(sender);
        let captures = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        queue.close();
        (writer.join().unwrap(), monitor.join().unwrap(), captures)
    });
    let mut rows = Vec::new();
    let mut consistent = true;
    for ((config, source), receive) in config.iter().zip(&sources).zip(captures) {
        let counts = source.report();
        consistent &= receive.receive_error.is_none()
            && counts.unknown == 0
            && counts.generated == counts.accepted + counts.before_accept_loss
            && counts.accepted == counts.committed + counts.after_accept_loss;
        rows.push(
            json!({"id":config.id,"source_kind":config.label(),"counts":counts,"receive":receive}),
        );
    }
    let returned = progress.in_flight.load(Relaxed) == 0
        && queue.snapshot().depth == 0
        && pools
            .iter()
            .all(|pool| pool.levels(ns(start.elapsed())).held == 0);
    let ok = consistent
        && returned
        && writer_report.error.is_none()
        && monitor_report.error.is_none()
        && writer_report.final_sync
        && writer_report.durability.journal_synced
        && monitor_report.loss_synced
        && progress.journal_failures.load(Relaxed) == 0;
    let result = json!({"live_version":1,"ok":ok,"session_epoch":epoch,"sources":rows,"writer":writer_report,
        "durability":writer_report.durability,"loss_log_synced":monitor_report.loss_synced,"all_resources_returned":returned,
        "journal_failures":progress.journal_failures.load(Relaxed),
        "queue_peak":queue.snapshot().peak,"memory_estimate":memory,
        "peak_rss_bytes":crate::metrics::rss_bytes().1,"elapsed_ns":ns(start.elapsed()),
        "committed_payload_bytes":progress.committed_bytes.load(Relaxed),
        "cpu_ns":crate::metrics::cpu_ns(rustix::time::ClockId::ProcessCPUTime)-cpu_start,
        "output_limit_reached":stop.load(Relaxed)});
    serde_json::to_writer_pretty(&mut summary, &result)?;
    summary.sync_data()?;
    ensure!(
        ok,
        "live recording incomplete; inspect {}",
        output.summary.display()
    );
    Ok(())
}

pub fn verify(args: VerifyArgs) -> Result<()> {
    super::recovery::verify_live(&args.input)
}

pub(super) fn validate_message(encoding: &str, data: &[u8], max: usize) -> Result<()> {
    codec::decode(encoding, data, max).map(drop)
}
