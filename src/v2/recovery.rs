use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use mcap::records::{Channel, MessageHeader, Record, SchemaHeader, op};
use serde::Serialize;

use super::integrity::CommitLine;
use crate::report::create_new;

const MAX_INPUT: u64 = 128 * 1024 * 1024;
const MAX_MESSAGES: usize = 262_144;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long, required_unless_present = "run_dir", conflicts_with = "run_dir")]
    input: Option<PathBuf>,
    #[arg(long)]
    run_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "apply")]
    dry_run: bool,
    #[arg(long, requires = "output")]
    apply: bool,
    #[arg(long, requires = "apply")]
    output: Option<PathBuf>,
}

#[derive(Serialize)]
struct Report {
    original: PathBuf,
    finalized: bool,
    recovery_needed: bool,
    scanned_bytes: usize,
    readable_messages: usize,
    committed_with_evidence: usize,
    recovered_but_unconfirmed: usize,
    duplicate_identical: usize,
    duplicate_conflicting: usize,
    scan_error: Option<String>,
    journal_error: Option<String>,
    output: Option<PathBuf>,
}

struct Message<'a> {
    header: MessageHeader,
    data: &'a [u8],
    end: usize,
    stream: u16,
    sequence: u64,
}

#[derive(Default)]
struct Scan<'a> {
    schemas: BTreeMap<u16, (SchemaHeader, &'a [u8])>,
    channels: BTreeMap<u16, Channel>,
    messages: Vec<Message<'a>>,
    scanned: usize,
    finalized: bool,
    live: bool,
    duplicate_identical: usize,
    duplicate_conflicting: usize,
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    ensure!(file.metadata()?.is_file(), "expected regular file");
    ensure!(
        file.metadata()?.len() <= MAX_INPUT,
        "recovery input exceeds 128 MiB"
    );
    let mut bytes = Vec::new();
    file.take(MAX_INPUT + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_INPUT,
        "recovery input exceeds 128 MiB"
    );
    Ok(bytes)
}

impl<'a> Scan<'a> {
    fn read(&mut self, bytes: &'a [u8]) -> Result<()> {
        ensure!(bytes.starts_with(mcap::MAGIC), "missing leading MCAP magic");
        self.scanned = mcap::MAGIC.len();
        let mut ids: HashMap<(_, _, _), usize> = HashMap::new();
        let mut header_seen = false;
        let mut data_end = false;
        while self.scanned < bytes.len() {
            let start = self.scanned;
            let prefix = bytes
                .get(start..start + 9)
                .context("truncated record header")?;
            let len = usize::try_from(u64::from_le_bytes(prefix[1..].try_into()?))?;
            let end = (start + 9)
                .checked_add(len)
                .context("record length overflow")?;
            let body = bytes.get(start + 9..end).context("truncated record body")?;
            ensure!(
                matches!(
                    prefix[0],
                    op::HEADER | op::SCHEMA | op::CHANNEL | op::MESSAGE | op::DATA_END | op::FOOTER
                ),
                "unsupported opcode {:#04x} at offset {start}",
                prefix[0]
            );
            match mcap::parse_record(prefix[0], body)? {
                Record::Header(header) if start == mcap::MAGIC.len() => {
                    self.live = header.profile == "predictable-recorder-live-v1";
                    ensure!(
                        self.live || header.profile == "predictable-recorder-v2",
                        "unsupported recovery profile"
                    );
                    header_seen = true;
                }
                Record::Schema { header, data }
                    if header_seen && !data_end && self.messages.is_empty() =>
                {
                    let std::borrow::Cow::Borrowed(data) = data else {
                        bail!("unexpected owned schema")
                    };
                    ensure!(
                        header.id != 0 && !self.schemas.contains_key(&header.id),
                        "duplicate or invalid schema ID"
                    );
                    self.schemas.insert(header.id, (header, data));
                }
                Record::Channel(channel)
                    if header_seen && !data_end && self.messages.is_empty() =>
                {
                    ensure!(
                        self.schemas.contains_key(&channel.schema_id)
                            && !self.channels.contains_key(&channel.id),
                        "invalid channel"
                    );
                    self.channels.insert(channel.id, channel);
                }
                Record::Message { header, data } if !data_end => {
                    let std::borrow::Cow::Borrowed(data) = data else {
                        bail!("unexpected owned message")
                    };
                    let channel = self
                        .channels
                        .get(&header.channel_id)
                        .context("unknown channel")?;
                    let stream = channel
                        .metadata
                        .get("stream_id")
                        .context("missing stream")?
                        .parse()?;
                    let epoch: u64 = channel
                        .metadata
                        .get("session_epoch")
                        .context("missing epoch")?
                        .parse()?;
                    let sequence = if self.live {
                        let max: usize = channel
                            .metadata
                            .get("max_payload_bytes")
                            .context("missing payload bound")?
                            .parse()?;
                        ensure!(
                            max <= 16 * 1024 * 1024
                                && (channel.message_encoding == "cdr"
                                    || header.log_time == header.publish_time)
                                && header.log_time >= epoch,
                            "invalid live channel limits or timestamp"
                        );
                        super::live::validate_message(&channel.message_encoding, data, max)?;
                        u64::from(header.sequence)
                    } else {
                        let seed = channel
                            .metadata
                            .get("seed")
                            .context("missing seed")?
                            .parse()?;
                        let size: usize = channel
                            .metadata
                            .get("frame_bytes")
                            .context("missing frame size")?
                            .parse()?;
                        let kind = if channel.message_encoding == "cdr" {
                            super::mock::Kind::MockRos2
                        } else {
                            super::mock::Kind::Synthetic
                        };
                        let (sequence, payload_stream) =
                            kind.sequence(data, stream).context("invalid fixture")?;
                        ensure!(
                            data.len() == size
                                && payload_stream == stream
                                && header.sequence == sequence as u32
                                && header.log_time >= header.publish_time
                                && header.publish_time >= epoch
                                && kind.matches(data, seed, sequence, stream),
                            "corrupt message at offset {start}"
                        );
                        sequence
                    };
                    ensure!(
                        self.messages.len() < MAX_MESSAGES,
                        "recovery message limit reached"
                    );
                    if let Some(index) = ids.insert((epoch, stream, sequence), self.messages.len())
                    {
                        let old = &self.messages[index];
                        if old.data == data && old.header == header {
                            self.duplicate_identical += 1;
                        } else {
                            self.duplicate_conflicting += 1;
                        }
                    }
                    self.messages.push(Message {
                        header,
                        data,
                        end,
                        stream,
                        sequence,
                    });
                }
                Record::DataEnd(footer) if header_seen && !data_end => {
                    ensure!(
                        len == 4 && footer.data_section_crc == crc32fast::hash(&bytes[..start]),
                        "invalid data section CRC"
                    );
                    data_end = true;
                }
                Record::Footer(footer) if data_end => {
                    ensure!(
                        len == 20
                            && footer.summary_start == 0
                            && footer.summary_offset_start == 0
                            && footer.summary_crc == crc32fast::hash(&bytes[start..end - 4])
                            && &bytes[end..] == mcap::MAGIC,
                        "invalid footer or trailing bytes"
                    );
                    self.scanned = bytes.len();
                    self.finalized = true;
                    return Ok(());
                }
                _ => bail!(
                    "unexpected MCAP opcode {:#04x} at offset {start}",
                    prefix[0]
                ),
            }
            self.scanned = end;
        }
        bail!("missing MCAP footer")
    }

    fn verify_journal(&self, bytes: &[u8], journal: &[u8], committed: &mut usize) -> Result<()> {
        let mut lines = journal.split_inclusive(|b| *b == b'\n');
        let mut previous = 0;
        let mut crc = crc32fast::Hasher::new();
        let mut finalized = false;
        for id in 0.. {
            let Some(line) = lines.next() else { break };
            ensure!(
                !finalized && line.ends_with(b"\n") && line.len() <= 512,
                "invalid commit header line"
            );
            let CommitLine::Group {
                group_id,
                start_offset,
                end_offset,
                message_count,
                finalized: final_group,
                prefix_crc32,
                ..
            } = serde_json::from_slice(line)?
            else {
                bail!("expected commit group")
            };
            let end = usize::try_from(end_offset)?;
            ensure!(
                group_id == id
                    && start_offset == previous as u64
                    && end > previous
                    && end <= self.scanned,
                "invalid commit boundary"
            );
            ensure!(
                !final_group || self.finalized && end == self.scanned,
                "invalid final commit boundary"
            );
            crc.update(&bytes[previous..end]);
            ensure!(
                prefix_crc32 == Some(crc.clone().finalize()),
                "missing or invalid commit CRC"
            );
            let count = self.messages.partition_point(|msg| msg.end <= end);
            let group = &self.messages[*committed..count];
            ensure!(
                group.len() as u64 == message_count
                    && (final_group || group.last().is_some_and(|msg| msg.end == end)),
                "commit boundary does not match message records"
            );
            let mut members: std::collections::HashSet<_> =
                group.iter().map(|msg| (msg.stream, msg.sequence)).collect();
            ensure!(
                members.len() == group.len(),
                "duplicate message in commit group"
            );
            while !members.is_empty() {
                let line = lines.next().context("truncated commit membership")?;
                ensure!(
                    line.ends_with(b"\n") && line.len() <= 512,
                    "invalid commit membership line"
                );
                let CommitLine::Frames { group_id, span } = serde_json::from_slice(line)? else {
                    bail!("expected commit membership")
                };
                ensure!(
                    group_id == id && span.start < span.end && span.count() <= members.len() as u64,
                    "invalid commit span"
                );
                for sequence in span.start..span.end {
                    ensure!(
                        members.remove(&(span.stream, sequence)),
                        "commit member absent from MCAP group"
                    );
                }
            }
            *committed = count;
            finalized = final_group;
            previous = end;
        }
        ensure!(finalized || !self.finalized, "missing finalized commit");
        Ok(())
    }

    fn rebuild(&self, path: &Path) -> Result<()> {
        ensure!(!self.channels.is_empty(), "no usable channel registration");
        ensure!(
            self.duplicate_identical == 0 && self.duplicate_conflicting == 0,
            "repair requires duplicate resolution"
        );
        ensure!(
            matches!(path.symlink_metadata(), Err(error) if error.kind() == std::io::ErrorKind::NotFound),
            "recovery output already exists or cannot be checked"
        );
        let mut partial = path.as_os_str().to_os_string();
        partial.push(".partial");
        let partial = PathBuf::from(partial);
        let mut writer = mcap::WriteOptions::new()
            .compression(None)
            .chunk_size(Some(1024 * 1024))
            .profile("predictable-recorder-recovered")
            .create(std::io::BufWriter::new(create_new(&partial)?))?;
        let mut schemas = BTreeMap::new();
        let mut channels = BTreeMap::new();
        for (&id, (schema, data)) in &self.schemas {
            schemas.insert(id, writer.add_schema(&schema.name, &schema.encoding, data)?);
        }
        for channel in self.channels.values() {
            channels.insert(
                channel.id,
                writer.add_channel(
                    schemas[&channel.schema_id],
                    &channel.topic,
                    &channel.message_encoding,
                    &channel.metadata,
                )?,
            );
        }
        for message in &self.messages {
            let mut header = message.header;
            header.channel_id = channels[&header.channel_id];
            writer.write_to_known_channel(&header, message.data)?;
        }
        writer.finish()?;
        let mut output = writer.into_inner();
        output.flush()?;
        output.get_ref().sync_all()?;
        let bytes = read_bounded(&partial)?;
        let mut read = mcap::MessageStream::new(&bytes)?;
        for expected in &self.messages {
            let message = read
                .next()
                .transpose()?
                .context("missing repaired message")?;
            let channel = &self.channels[&expected.header.channel_id];
            ensure!(
                message.channel.topic == channel.topic
                    && message.channel.message_encoding == channel.message_encoding
                    && message.channel.metadata == channel.metadata
                    && message.sequence == expected.header.sequence
                    && message.log_time == expected.header.log_time
                    && message.publish_time == expected.header.publish_time
                    && message.data.as_ref() == expected.data,
                "repaired message differs"
            );
        }
        ensure!(read.next().is_none(), "unexpected extra repaired messages");
        mcap::Summary::read(&bytes)?.context("missing rebuilt MCAP summary")?;
        std::fs::hard_link(&partial, path)
            .context("publish repaired MCAP without replacing existing files")?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        std::fs::remove_file(partial)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

pub(super) fn data_path(dir: &Path) -> PathBuf {
    let finalized = dir.join("run.mcap");
    if finalized.exists() {
        finalized
    } else {
        dir.join("run.partial")
    }
}

pub fn run(args: Args) -> Result<()> {
    let input = args
        .run_dir
        .as_deref()
        .map(data_path)
        .or(args.input)
        .context("missing recovery input")?;
    let bytes = read_bounded(&input)?;
    let mut scan = Scan::default();
    let scan_error = scan.read(&bytes).err().map(|error| format!("{error:#}"));
    ensure!(
        scan.scanned > mcap::MAGIC.len(),
        "unusable recording: {scan_error:?}"
    );
    let mut committed = 0;
    let summary = crate::report::Outputs::new(&input)?.summary;
    let rollback_failed = read_bounded(&summary)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .is_some_and(|report| report["durability"]["journal"]["rollback_failed"] == true);
    let journal_error = if rollback_failed {
        Some("journal rollback failed; commit evidence is uncertain".into())
    } else {
        read_bounded(&super::record::commit_path(&input)?)
            .and_then(|journal| scan.verify_journal(&bytes, &journal, &mut committed))
            .err()
            .map(|error| format!("{error:#}"))
    };
    let duplicates = scan.duplicate_identical + scan.duplicate_conflicting;
    if duplicates > 0 {
        committed = 0;
    }
    let recovery_needed = !scan.finalized || journal_error.is_some() || duplicates > 0;
    if let Some(path) = &args.output {
        ensure!(
            recovery_needed,
            "recording is finalized and has valid commit evidence"
        );
        scan.rebuild(path)?;
    }
    let report = Report {
        original: input,
        finalized: scan.finalized,
        recovery_needed,
        scanned_bytes: scan.scanned,
        readable_messages: scan.messages.len(),
        committed_with_evidence: committed,
        recovered_but_unconfirmed: scan.messages.len() - committed,
        duplicate_identical: scan.duplicate_identical,
        duplicate_conflicting: scan.duplicate_conflicting,
        scan_error,
        journal_error,
        output: args.output,
    };
    if let Some(path) = &report.output {
        let mut name = path.as_os_str().to_os_string();
        name.push(".recovery.json");
        let mut file = create_new(Path::new(&name))?;
        serde_json::to_writer_pretty(&mut file, &report)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

pub(super) fn verify_live(input: &Path) -> Result<()> {
    let outputs = crate::report::Outputs::new(input)?;
    let bytes = read_bounded(input)?;
    let mut scan = Scan::default();
    scan.read(&bytes)?;
    ensure!(
        scan.live && scan.finalized,
        "expected finalized live recording"
    );
    ensure!(
        scan.duplicate_identical == 0 && scan.duplicate_conflicting == 0,
        "duplicate live sequence"
    );
    let mut committed = 0;
    scan.verify_journal(
        &bytes,
        &read_bounded(&super::record::commit_path(input)?)?,
        &mut committed,
    )?;
    ensure!(
        committed == scan.messages.len(),
        "missing committed messages"
    );
    let report: serde_json::Value = serde_json::from_slice(&read_bounded(&outputs.summary)?)?;
    ensure!(
        report["live_version"] == 1
            && report["ok"] == true
            && report["loss_log_synced"] == true
            && report["writer"]["final_sync"] == true
            && report["durability"]["journal_synced"] == true,
        "live summary does not certify clean shutdown"
    );
    let sources = report["sources"]
        .as_array()
        .context("missing live source list")?;
    ensure!(
        sources.len() == scan.channels.len() && sources.len() <= 32,
        "live channel count mismatch"
    );
    let epoch = report["session_epoch"]
        .as_u64()
        .context("missing session epoch")?;
    let mut seen = std::collections::HashSet::new();
    for channel in scan.channels.values() {
        let stream: usize = channel
            .metadata
            .get("stream_id")
            .context("missing stream")?
            .parse()?;
        let expected = sources.get(stream).context("bad channel stream")?;
        ensure!(
            seen.insert(stream)
                && expected["id"].as_str() == channel.metadata.get("source_id").map(String::as_str)
                && expected["source_kind"].as_str()
                    == channel.metadata.get("source_kind").map(String::as_str),
            "live channel mismatch"
        );
        ensure!(
            channel.metadata.get("session_epoch") == Some(&epoch.to_string()),
            "session mismatch"
        );
    }
    let mut intervals = vec![Vec::new(); sources.len()];
    let mut written = vec![0_u64; sources.len()];
    let mut previous = vec![None; sources.len()];
    for message in &scan.messages {
        let stream = usize::from(message.stream);
        let slot = previous
            .get_mut(stream)
            .context("message stream out of range")?;
        ensure!(
            slot.is_none_or(
                |(sequence, time)| message.sequence > sequence && message.header.log_time >= time
            ),
            "live per-source sequence/timestamp order mismatch"
        );
        *slot = Some((message.sequence, message.header.log_time));
        intervals[stream].push((message.sequence, message.sequence + 1));
        written[stream] += 1;
    }
    let mut before = vec![0_u64; sources.len()];
    let mut after = before.clone();
    for line in read_bounded(&outputs.losses)?.split_inclusive(|b| *b == b'\n') {
        ensure!(
            line.ends_with(b"\n") && line.len() <= 512,
            "incomplete loss record"
        );
        let loss: super::integrity::Loss = serde_json::from_slice(line)?;
        ensure!(
            loss.span.start < loss.span.end
                && loss.span.end <= u64::from(u32::MAX) + 1
                && loss.reason.valid(loss.layer, loss.accepted)
                && !loss.reason.uncertain(),
            "invalid live loss"
        );
        let stream = usize::from(loss.span.stream);
        intervals
            .get_mut(stream)
            .context("loss stream out of range")?
            .push((loss.span.start, loss.span.end));
        let count = if loss.accepted {
            &mut after
        } else {
            &mut before
        };
        count[stream] = count[stream]
            .checked_add(loss.span.count())
            .context("loss count overflow")?;
    }
    for (i, ranges) in intervals.iter_mut().enumerate() {
        ranges.sort_unstable();
        let mut end = 0;
        for &(start, next) in ranges.iter() {
            ensure!(
                start == end,
                "overlap or gap in live sequence coverage on stream {i}"
            );
            end = next;
        }
        let counts: super::metrics::SourceCounts =
            serde_json::from_value(sources[i]["counts"].clone())?;
        ensure!(
            counts.generated == end
                && counts.committed == written[i]
                && counts.written == written[i]
                && counts.unknown == 0
                && counts.before_accept_loss == before[i]
                && counts.after_accept_loss == after[i]
                && counts.accepted == written[i] + after[i],
            "live counters do not reconcile"
        );
    }
    println!(
        "{}",
        serde_json::json!({"ok":true,"committed":committed,"known_loss":before.iter().chain(&after).sum::<u64>(),
        "source_count":sources.len()})
    );
    Ok(())
}
