use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use mcap::records::{MessageHeader, Record, op};
use mcap::sans_io::linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};
use serde::{Deserialize, Serialize};

use super::arrival::{Schedule, schedules};
use super::config::WriterMode;
use super::integrity::{CommitLine, Layer, Loss, Reason, Span};
use super::layout::{encoding, metadata, schema, topic};
use super::record::{Core, VERSION, commit_path};
use crate::config::{VerifyArgs, VerifyCli};
use crate::report::{Outputs, Status};

use crate::verify::Ranges;

#[derive(Deserialize)]
struct Version {
    report_version: u32,
}

pub fn dispatch(args: VerifyCli) -> Result<()> {
    let (input, report) = if let Some(dir) = args.run_dir {
        (
            super::recovery::data_path(&dir),
            dir.join("run-summary.json"),
        )
    } else {
        (
            args.input.context("missing verification input")?,
            args.report.context("missing recording summary")?,
        )
    };
    let version = File::open(&report)
        .ok()
        .and_then(|file| serde_json::from_reader::<_, Version>(BufReader::new(file)).ok());
    let outputs = Outputs::new(&input)?;
    let v2 = version.is_some_and(|version| version.report_version == VERSION);
    let args_v1 = VerifyArgs {
        input,
        report,
        loss_log: args.loss_log.unwrap_or(outputs.losses),
    };
    if !v2 {
        return crate::verify::run(args_v1);
    }
    let commits = args
        .commit_log
        .map_or_else(|| commit_path(&args_v1.input), Ok)?;
    let result = match check(&args_v1, &commits) {
        Ok(result) => result,
        Err(error) => Report {
            errors: vec![format!("{error:#}")],
            ..Report::default()
        },
    };
    let mut stdout = io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &result)?;
    writeln!(stdout)?;
    ensure!(result.ok, "V2 verification failed");
    Ok(())
}

#[derive(Default, Serialize)]
struct Report {
    ok: bool,
    finalized: bool,
    commit_groups_read: u64,
    verified: u64,
    committed_and_verified: u64,
    unknown: u64,
    unreported_missing: u64,
    false_loss_claims: u64,
    duplicate_loss_claims: u64,
    duplicates: u64,
    out_of_range: u64,
    out_of_order: u64,
    corrupt_frames: u64,
    sources: Vec<CheckedSource>,
    errors: Vec<String>,
}

#[derive(Default, Serialize)]
struct CheckedSource {
    id: String,
    planned: u64,
    accepted_reported: u64,
    written_reported: u64,
    committed_reported: u64,
    messages: u64,
    verified: u64,
    committed_and_verified: u64,
    before_accept_loss: u64,
    after_accept_loss: u64,
    unknown: u64,
    unreported_missing: u64,
    false_loss_claims: u64,
}

#[derive(Default)]
struct Sets {
    present: Ranges,
    declared: Ranges,
    known_loss: Ranges,
    before_generate: u64,
    previous: Option<(u64, u64)>,
}

fn check(args: &VerifyArgs, commit_path: &Path) -> Result<Report> {
    let core: Core = serde_json::from_reader(open(&args.report)?)?;
    ensure!(core.report_version == VERSION, "unexpected report version");
    ensure!(
        core.settings.record.writer != WriterMode::Null,
        "Null has no MCAP to verify"
    );
    core.settings.validate()?;
    ensure!(
        core.duration_ns > 0
            && core.duration_ns <= i64::MAX as u64
            && core.session_epoch > 0
            && core.session_epoch.checked_add(core.duration_ns).is_some(),
        "invalid report time range"
    );
    let schedules = schedules(&core.settings, core.duration_ns)?;
    ensure!(
        core.sources.len() == schedules.len(),
        "source count mismatch"
    );
    let mut sources = Vec::with_capacity(schedules.len());
    for ((report, cfg), schedule) in core
        .sources
        .iter()
        .zip(&core.settings.sources)
        .zip(&schedules)
    {
        ensure!(
            report.id == cfg.id && report.planned == schedule.expected,
            "invalid source settings or planned range"
        );
        sources.push(CheckedSource {
            id: cfg.id.clone(),
            planned: schedule.expected,
            accepted_reported: report.counts.accepted,
            written_reported: report.counts.written,
            committed_reported: report.counts.committed,
            ..CheckedSource::default()
        });
    }
    let mut result = Report {
        sources,
        ..Report::default()
    };
    let mut sets: Vec<_> = schedules.iter().map(|_| Sets::default()).collect();
    match CommitScan::new(commit_path, &core, &schedules) {
        Ok(mut commits) => {
            if let Err(error) = read_mcap(
                &args.input,
                &core,
                &schedules,
                &mut sets,
                &mut result,
                &mut commits,
            ) {
                result
                    .errors
                    .push(format!("MCAP or commit membership: {error:#}"));
            }
            result.commit_groups_read = commits.groups;
            if commits.groups != core.group_count {
                result.errors.push("commit group count mismatch".into());
            }
        }
        Err(error) => result.errors.push(format!("commit ledger: {error:#}")),
    }
    if let Err(error) = read_losses(&args.loss_log, &schedules, &mut sets, &mut result) {
        result.errors.push(format!("loss journal: {error:#}"));
    }

    let mut counter_mismatch = false;
    for ((observed, states), recorded) in result.sources.iter_mut().zip(sets).zip(&core.sources) {
        let known_conflict = states.present.intersection(&states.known_loss);
        observed.false_loss_claims = known_conflict;
        let coverage_overlap = states.present.intersection(&states.declared);
        observed.unreported_missing =
            observed.planned - states.present.count - (states.declared.count - coverage_overlap);
        result.verified += observed.verified;
        result.committed_and_verified += observed.committed_and_verified;
        result.unknown += observed.unknown;
        result.false_loss_claims += known_conflict;
        result.unreported_missing += observed.unreported_missing;
        let counts = &recorded.counts;
        counter_mismatch |= counts.committed != observed.committed_and_verified
            || counts.before_accept_loss != observed.before_accept_loss
            || counts.after_accept_loss != observed.after_accept_loss
            || counts.unknown != observed.unknown
            || !(counts.committed <= counts.written
                && counts.written <= counts.submitted
                && counts.submitted <= counts.accepted
                && counts.accepted <= counts.generated
                && counts.generated <= recorded.planned);
        if core.status == Status::Ok {
            counter_mismatch |= observed.verified != counts.written
                || observed.verified != observed.committed_and_verified
                || counts.submitted != counts.written
                || counts.unknown != 0
                || counts.generated != recorded.planned - states.before_generate
                || counts.accepted.checked_add(observed.before_accept_loss)
                    != Some(recorded.planned)
                || counts.committed.checked_add(observed.after_accept_loss)
                    != Some(counts.accepted);
        }
    }
    if counter_mismatch {
        result
            .errors
            .push("per-source counts or state sets do not reconcile".into());
    }
    if [
        result.unreported_missing,
        result.false_loss_claims,
        result.duplicate_loss_claims,
        result.duplicates,
        result.out_of_range,
        result.out_of_order,
        result.corrupt_frames,
    ]
    .into_iter()
    .any(|count| count != 0)
    {
        result
            .errors
            .push("invalid messages or loss/commit membership".into());
    }
    if !result.finalized
        || !core.finalized
        || !core.final_sync
        || !core.loss_log_synced
        || !core.commit_log_synced
        || core.journal_failures != 0
        || core.status != Status::Ok
        || result.unknown != 0
    {
        result
            .errors
            .push("recording is incomplete, not synced or contains unknown ranges".into());
    }
    result.ok = result.errors.is_empty();
    Ok(result)
}

fn open(path: &Path) -> Result<BufReader<File>> {
    Ok(BufReader::with_capacity(
        64 * 1024,
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    ))
}

fn read_line<T: for<'a> Deserialize<'a>>(
    input: &mut impl BufRead,
    buffer: &mut Vec<u8>,
) -> Result<Option<T>> {
    buffer.clear();
    if input.take(4097).read_until(b'\n', buffer)? == 0 {
        return Ok(None);
    }
    ensure!(
        buffer.len() <= 4096 && buffer.ends_with(b"\n"),
        "invalid or truncated JSONL line"
    );
    Ok(Some(serde_json::from_slice(buffer)?))
}

struct DeclaredGroup {
    end: u64,
    count: u64,
    matched: u64,
    prefix_crc32: Option<u32>,
    ranges: Vec<Ranges>,
}

struct CommitScan<'a> {
    file: BufReader<File>,
    buffer: Vec<u8>,
    lookahead: Option<CommitLine>,
    current: Option<DeclaredGroup>,
    schedules: &'a [Schedule],
    max_messages: u64,
    require_prefix_crc: bool,
    groups: u64,
    last_end: u64,
    last_sync: u64,
    final_group: bool,
}

impl<'a> CommitScan<'a> {
    fn new(path: &Path, core: &Core, schedules: &'a [Schedule]) -> Result<Self> {
        let mut scan = Self {
            file: open(path)?,
            buffer: Vec::with_capacity(512),
            lookahead: None,
            current: None,
            schedules,
            max_messages: core.settings.record.commit_messages as u64,
            require_prefix_crc: core.settings.sources.iter().any(|s| !s.kind.is_synthetic()),
            groups: 0,
            last_end: 0,
            last_sync: 0,
            final_group: false,
        };
        scan.load()?;
        Ok(scan)
    }

    fn start_record(&mut self, start: u64, end: u64, crc: &crc32fast::Hasher) -> Result<()> {
        while let Some(group) = &self.current {
            if start < group.end {
                break;
            }
            ensure!(
                group
                    .prefix_crc32
                    .is_none_or(|expected| expected == crc.clone().finalize()),
                "commit prefix CRC mismatch"
            );
            self.load()?;
        }
        if let Some(group) = &self.current {
            ensure!(end <= group.end, "commit offset splits an MCAP record");
        }
        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        if let Some(group) = &self.current {
            ensure!(
                group.matched == group.count,
                "commit group member count disagrees with MCAP"
            );
        }
        let header = match self.lookahead.take() {
            Some(header) => Some(header),
            None => read_line(&mut self.file, &mut self.buffer)?,
        };
        let Some(header) = header else {
            self.current = None;
            return Ok(());
        };
        let CommitLine::Group {
            group_id,
            start_offset,
            end_offset,
            message_count,
            synced_at_ns,
            finalized,
            prefix_crc32,
        } = header
        else {
            bail!("commit group header is missing");
        };
        ensure!(
            !self.require_prefix_crc || prefix_crc32.is_some(),
            "missing commit prefix CRC"
        );
        ensure!(
            !self.final_group
                && group_id == self.groups
                && start_offset == self.last_end
                && end_offset > start_offset
                && synced_at_ns >= self.last_sync
                && message_count <= self.max_messages
                && (message_count != 0 || finalized),
            "invalid commit group ID, offset, count or timing"
        );
        let mut ranges: Vec<_> = self.schedules.iter().map(|_| Ranges::default()).collect();
        let mut declared = 0_u64;
        loop {
            let entry: Option<CommitLine> = read_line(&mut self.file, &mut self.buffer)?;
            match entry {
                Some(CommitLine::Frames {
                    group_id: entry_id,
                    span,
                }) => {
                    ensure!(
                        entry_id == group_id,
                        "frame span references a different commit group"
                    );
                    let index = check_span(span, self.schedules)?;
                    ensure!(
                        ranges[index].insert(span.start, span.end) == 0,
                        "duplicate commit membership in group"
                    );
                    declared = declared
                        .checked_add(span.count())
                        .context("commit frame count overflow")?;
                    ensure!(
                        declared <= message_count,
                        "commit spans exceed the group message count"
                    );
                }
                other => {
                    self.lookahead = other;
                    break;
                }
            }
        }
        ensure!(
            declared == message_count,
            "commit span count is inconsistent"
        );
        self.current = Some(DeclaredGroup {
            end: end_offset,
            count: message_count,
            matched: 0,
            prefix_crc32,
            ranges,
        });
        self.groups += 1;
        self.last_end = end_offset;
        self.last_sync = synced_at_ns;
        self.final_group = finalized;
        Ok(())
    }

    fn frame(&mut self, stream: usize, sequence: u64) -> Result<bool> {
        if let Some(group) = &mut self.current {
            ensure!(
                group.ranges[stream].contains(sequence),
                "MCAP message is absent from its committed group"
            );
            group.matched += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn finish(&mut self, position: u64, crc: &crc32fast::Hasher) -> Result<()> {
        self.start_record(position, position, crc)?;
        ensure!(
            self.current.is_none() && self.final_group && self.last_end == position,
            "commit ledger does not cover the finalized MCAP file"
        );
        Ok(())
    }
}

fn read_mcap(
    path: &Path,
    core: &Core,
    schedules: &[Schedule],
    sets: &mut [Sets],
    result: &mut Report,
    commits: &mut CommitScan<'_>,
) -> Result<()> {
    let mut input = open(path)?;
    let max_size = core
        .settings
        .sources
        .iter()
        .map(|source| source.frame_bytes)
        .max()
        .unwrap();
    let options = LinearReaderOptions::default()
        .with_emit_chunks(true)
        .with_validate_data_section_crc(true)
        .with_validate_summary_section_crc(true)
        .with_record_length_limit((max_size + 22).max(4096));
    let mut reader = LinearReader::new_with_options(options);
    let mut offset = mcap::MAGIC.len() as u64;
    let mut header = false;
    let mut crc = crc32fast::Hasher::new();
    crc.update(mcap::MAGIC);
    let mut schemas = std::collections::BTreeMap::new();
    let mut channels = vec![None; u16::MAX as usize + 1];
    let mut registered = vec![false; schedules.len()];
    let mut channel_count = 0;
    let mut data_end = false;
    let mut footer = false;
    let mut messages_started = false;

    while let Some(event) = reader.next_event() {
        match event? {
            LinearReadEvent::ReadRequest(size) => {
                let len = size.min(1024 * 1024);
                input
                    .read_exact(reader.insert(len))
                    .context("incomplete MCAP record")?;
                reader.notify_read(len);
            }
            LinearReadEvent::Record { opcode, data } => {
                let end = offset + 9 + data.len() as u64;
                commits.start_record(offset, end, &crc)?;
                crc.update(&[opcode]);
                crc.update(&(data.len() as u64).to_le_bytes());
                crc.update(data);
                offset = end;
                ensure!(
                    matches!(
                        opcode,
                        op::HEADER
                            | op::SCHEMA
                            | op::CHANNEL
                            | op::MESSAGE
                            | op::DATA_END
                            | op::FOOTER
                    ),
                    "unexpected opcode {opcode:#04x}"
                );
                match mcap::parse_record(opcode, data)? {
                    Record::Header(record)
                        if !header && record.profile == "predictable-recorder-v2" =>
                    {
                        header = true
                    }
                    Record::Schema {
                        header: entry,
                        data,
                    } if header && !data_end && !messages_started => {
                        ensure!(
                            entry.id != 0
                                && schemas
                                    .insert(
                                        entry.id,
                                        (entry.name, entry.encoding, data.into_owned())
                                    )
                                    .is_none(),
                            "invalid schema"
                        );
                    }
                    Record::Channel(channel) if header && !data_end && !messages_started => {
                        let stream = channel
                            .metadata
                            .get("stream_id")
                            .context("channel stream ID")?
                            .parse::<u16>()?;
                        let index = usize::from(stream);
                        let source = core.settings.sources.get(index).context("unknown stream")?;
                        let expected = schema(source);
                        let actual = schemas.get(&channel.schema_id).context("unknown schema")?;
                        ensure!(
                            actual.0 == expected.0
                                && actual.1 == expected.1
                                && actual.2 == expected.2
                                && index < schedules.len()
                                && !registered[index]
                                && channel.id != 0
                                && channels[usize::from(channel.id)].is_none()
                                && channel.message_encoding == encoding(source)
                                && channel.topic == topic(source)
                                && channel.metadata
                                    == metadata(stream, &core.settings, core.session_epoch),
                            "invalid channel registration"
                        );
                        channels[usize::from(channel.id)] = Some(index);
                        registered[index] = true;
                        channel_count += 1;
                    }
                    Record::Message { header, data }
                        if channel_count == schedules.len() && !data_end =>
                    {
                        messages_started = true;
                        let stream = channels[usize::from(header.channel_id)]
                            .context("unexpected channel ID")?;
                        let schedule = &schedules[stream];
                        check_message(
                            core,
                            stream,
                            schedule,
                            &mut sets[stream],
                            result,
                            commits,
                            header,
                            &data,
                        )?;
                    }
                    Record::DataEnd(_)
                        if channel_count == schedules.len() && !data_end && data.len() == 4 =>
                    {
                        data_end = true
                    }
                    Record::Footer(f)
                        if data_end
                            && !footer
                            && data.len() == 20
                            && f.summary_start == 0
                            && f.summary_offset_start == 0 =>
                    {
                        footer = true
                    }
                    _ => bail!("unexpected or malformed MCAP record {opcode:#04x}"),
                }
            }
        }
    }
    let end = offset + mcap::MAGIC.len() as u64;
    ensure!(
        footer && input.fill_buf()?.is_empty(),
        "invalid footer or trailing bytes"
    );
    result.finalized = true;
    ensure!(
        core.end_offset == end,
        "final file offset does not match summary"
    );
    crc.update(mcap::MAGIC);
    commits.finish(end, &crc)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn check_message(
    core: &Core,
    stream: usize,
    schedule: &Schedule,
    sets: &mut Sets,
    report: &mut Report,
    commits: &mut CommitScan<'_>,
    header: MessageHeader,
    data: &[u8],
) -> Result<()> {
    let sample = &mut report.sources[stream];
    sample.messages += 1;
    let source = &core.settings.sources[stream];
    let Some((sequence, stream_id)) = source.kind.sequence(data, stream as u16) else {
        report.corrupt_frames += 1;
        return Ok(());
    };
    if stream_id as usize != stream || sequence >= schedule.expected {
        report.out_of_range += 1;
        return Ok(());
    }
    let duplicate = sets.present.insert(sequence, sequence + 1) != 0;
    report.duplicates += u64::from(duplicate);
    if sets
        .previous
        .is_some_and(|(previous, time)| sequence < previous || header.log_time < time)
    {
        report.out_of_order += 1;
    }
    sets.previous = Some((sequence, header.log_time));
    let due = schedule.time(sequence).unwrap();
    let timestamp = header.log_time.checked_sub(core.session_epoch);
    let matches = data.len() == core.settings.sources[stream].frame_bytes
        && header.sequence == sequence as u32
        && header.publish_time == core.session_epoch + due
        && timestamp.is_some_and(|now| {
            now >= due
                && now < core.duration_ns
                && schedule
                    .latest_group_start(now)
                    .and_then(|group| schedule.time(group))
                    == Some(due)
        })
        && source
            .kind
            .matches(data, core.settings.record.seed, sequence, stream_id);
    let committed = commits.frame(stream, sequence)?;
    if !matches {
        report.corrupt_frames += 1;
    } else if !duplicate {
        sample.verified += 1;
        if committed {
            sample.committed_and_verified += 1;
        }
    }
    Ok(())
}

fn check_span(span: Span, schedules: &[Schedule]) -> Result<usize> {
    let index = usize::from(span.stream);
    ensure!(
        span.start < span.end
            && schedules
                .get(index)
                .is_some_and(|schedule| span.end <= schedule.expected),
        "span outside expected IDs"
    );
    Ok(index)
}

fn read_losses(
    path: &Path,
    schedules: &[Schedule],
    sets: &mut [Sets],
    report: &mut Report,
) -> Result<()> {
    let mut input = open(path)?;
    let mut line = Vec::with_capacity(512);
    while let Some(loss) = read_line::<Loss>(&mut input, &mut line)? {
        let index = check_span(loss.span, schedules)?;
        ensure!(
            loss.reason.valid(loss.layer, loss.accepted)
                && (loss.reason == Reason::Unknown && loss.layer == Layer::Source
                    || loss.timestamp_ns >= schedules[index].time(loss.span.start).unwrap()),
            "invalid loss cause/layer or timestamp"
        );
        let states = &mut sets[index];
        let overlap = states.declared.insert(loss.span.start, loss.span.end);
        report.duplicate_loss_claims = report.duplicate_loss_claims.saturating_add(overlap);
        let count = loss.span.count() - overlap;
        let source = &mut report.sources[index];
        if loss.reason.uncertain() {
            source.unknown += count;
        } else {
            states.known_loss.insert(loss.span.start, loss.span.end);
            if loss.accepted {
                source.after_accept_loss += count;
            } else {
                source.before_accept_loss += count;
            }
        }
        if matches!(
            loss.reason,
            Reason::DmaPoolExhausted | Reason::SourceDeadlineMiss | Reason::SourceReportedDrop
        ) || (loss.reason == Reason::Unknown && !loss.accepted)
        {
            states.before_generate += count;
        }
    }
    Ok(())
}
