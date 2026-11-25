use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use mcap::records::{MessageHeader, Record, op};
use mcap::sans_io::linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};
use serde::Serialize;

use crate::config::{Sink, VerifyArgs};
use crate::loss::{Event, Layer, LossCounts, Reason};
use crate::report::{CoreReport, REPORT_VERSION, Status};
use crate::source::{
    ENCODING, SCHEMA, SCHEMA_ENCODING, SCHEMA_NAME, STREAM_ID, TOPIC, channel_metadata,
    current_slot, frame_id, matches_pattern, slot_time_ns,
};

#[derive(Default, Serialize)]
struct Verification {
    ok: bool,
    expected: u64,
    generated_reported: u64,
    received_reported: u64,
    written_reported: u64,
    mcap_messages: u64,
    verified: u64,
    absent_from_mcap: u64,
    journaled_losses: u64,
    unreported_missing: u64,
    false_loss_claims: u64,
    duplicate_losses: u64,
    duplicates: u64,
    out_of_range: u64,
    out_of_order: u64,
    corrupt_frames: u64,
    finalized: bool,
    errors: Vec<String>,
}

pub fn run(args: VerifyArgs) -> Result<()> {
    let result = match check(&args) {
        Ok(result) => result,
        Err(error) => Verification {
            errors: vec![format!("{error:#}")],
            ..Verification::default()
        },
    };
    let mut output = io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &result)?;
    writeln!(output)?;
    ensure!(result.ok, "verification failed");
    Ok(())
}

fn check(args: &VerifyArgs) -> Result<Verification> {
    let summary: CoreReport =
        serde_json::from_reader(open(&args.report)?).context("parse run summary")?;
    ensure!(
        summary.report_version == REPORT_VERSION,
        "unsupported report version"
    );
    ensure!(
        summary.config.sink == Sink::Mcap,
        "NullSink has no MCAP to verify"
    );
    let plan = summary
        .config
        .plan()
        .context("invalid recorded configuration")?;
    ensure!(
        summary.expected == plan.expected,
        "expected count disagrees with scheduled slots"
    );
    ensure!(
        summary
            .start_unix_ns
            .checked_add(plan.duration_ns)
            .is_some(),
        "capture timestamp range overflow"
    );

    let mut verified = Verification {
        expected: summary.expected,
        generated_reported: summary.generated,
        received_reported: summary.received,
        written_reported: summary.written,
        ..Verification::default()
    };
    let mut present = Ranges::default();
    let mut losses = Ranges::default();
    if let Err(error) = read_mcap(&args.input, &summary, &mut verified, &mut present) {
        verified.errors.push(format!("MCAP: {error:#}"));
    }
    match read_losses(&args.loss_log, &summary, &mut verified, &mut losses) {
        Ok(audit) => {
            if summary.losses != audit.counts {
                verified
                    .errors
                    .push("loss reasons/counts disagree with the summary".into());
            }
            let generated = summary.expected.checked_sub(audit.before_generation);
            let received = present.count.checked_add(audit.after_acceptance);
            if generated != Some(summary.generated) || received != Some(summary.received) {
                verified
                    .errors
                    .push("generated/received counts do not match MCAP and journal sets".into());
            }
            if summary.status == Status::Ok && audit.uncertain != 0 {
                verified
                    .errors
                    .push("successful report contains uncertain or writer-error events".into());
            }
        }
        Err(error) => verified.errors.push(format!("loss journal: {error:#}")),
    }
    verified.absent_from_mcap = summary.expected - present.count;
    verified.journaled_losses = losses.count;
    verified.false_loss_claims = present.intersection(&losses);
    verified.unreported_missing =
        verified.absent_from_mcap - (losses.count - verified.false_loss_claims);
    if verified.unreported_missing != 0
        || verified.false_loss_claims != 0
        || verified.duplicate_losses != 0
    {
        verified
            .errors
            .push("loss journal does not exactly match absent frame IDs".into());
    }
    if verified.duplicates != 0
        || verified.out_of_range != 0
        || verified.out_of_order != 0
        || verified.corrupt_frames != 0
    {
        verified
            .errors
            .push("invalid frame IDs, order, timestamps or payloads".into());
    }
    if summary.written != verified.mcap_messages
        || summary.written != verified.verified
        || !(summary.written <= summary.received
            && summary.received <= summary.generated
            && summary.generated <= summary.expected)
    {
        verified
            .errors
            .push("expected/generated/received/written/verified counts do not reconcile".into());
    }
    if !verified.finalized
        || !summary.mcap_finalized
        || !summary.mcap_synced
        || !summary.loss_log_synced
        || summary.loss_record_failures != 0
        || summary.status != Status::Ok
        || !summary.uncertain_ranges.is_empty()
    {
        verified
            .errors
            .push("run is incomplete, unsynced or has uncertain ranges".into());
    }
    verified.ok = verified.errors.is_empty();
    Ok(verified)
}

fn open(path: &Path) -> Result<BufReader<File>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(BufReader::with_capacity(64 * 1024, file))
}

fn read_mcap(
    path: &Path,
    summary: &CoreReport,
    verified: &mut Verification,
    present: &mut Ranges,
) -> Result<()> {
    enum Section {
        Header,
        Schema,
        Channel(u16),
        Messages(u16),
        Footer,
        End,
    }

    let mut input = open(path)?;
    let limit = (summary.config.frame_bytes + 22).max(4096);
    let options = LinearReaderOptions::default()
        .with_emit_chunks(true)
        .with_record_length_limit(limit)
        .with_validate_data_section_crc(true)
        .with_validate_summary_section_crc(true);
    let mut reader = LinearReader::new_with_options(options);
    let mut section = Section::Header;
    let mut previous = None;
    let metadata = channel_metadata(
        summary.config.seed,
        summary.config.frame_bytes,
        summary.start_unix_ns,
    );
    while let Some(event) = reader.next_event() {
        match event? {
            LinearReadEvent::ReadRequest(len) => {
                // A corrupt record length must not immediately allocate its claimed size.
                let len = len.min(1024 * 1024);
                input
                    .read_exact(reader.insert(len))
                    .context("incomplete MCAP record or end magic")?;
                reader.notify_read(len);
            }
            LinearReadEvent::Record { opcode, data } => {
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
                    "unexpected opcode {opcode:#04x} for the V1 unchunked recording"
                );
                section = match (section, mcap::parse_record(opcode, data)?) {
                    (Section::Header, Record::Header(header)) if header.profile.is_empty() => {
                        Section::Schema
                    }
                    (Section::Schema, Record::Schema { header, data }) => {
                        ensure!(
                            header.id != 0
                                && header.name == SCHEMA_NAME
                                && header.encoding == SCHEMA_ENCODING
                                && data.as_ref() == SCHEMA,
                            "schema differs from the V1 encoding"
                        );
                        Section::Channel(header.id)
                    }
                    (Section::Channel(schema_id), Record::Channel(channel)) => {
                        ensure!(
                            channel.schema_id == schema_id
                                && channel.topic == TOPIC
                                && channel.message_encoding == ENCODING
                                && channel.metadata == metadata,
                            "channel metadata does not match the run summary"
                        );
                        Section::Messages(channel.id)
                    }
                    (Section::Messages(channel), Record::Message { header, data }) => {
                        check_frame(
                            summary,
                            verified,
                            present,
                            &mut previous,
                            channel,
                            header,
                            &data,
                        );
                        Section::Messages(channel)
                    }
                    (Section::Messages(_), Record::DataEnd(_)) if data.len() == 4 => {
                        Section::Footer
                    }
                    (Section::Footer, Record::Footer(footer))
                        if data.len() == 20
                            && footer.summary_start == 0
                            && footer.summary_offset_start == 0 =>
                    {
                        Section::End
                    }
                    _ => bail!("unexpected MCAP record order or invalid record {opcode:#04x}"),
                };
            }
        }
    }
    ensure!(
        matches!(section, Section::End) && input.fill_buf()?.is_empty(),
        "missing footer or trailing bytes"
    );
    verified.finalized = true;
    Ok(())
}

fn check_frame(
    summary: &CoreReport,
    verified: &mut Verification,
    present: &mut Ranges,
    previous: &mut Option<(u64, u64)>,
    channel: u16,
    header: MessageHeader,
    data: &[u8],
) {
    verified.mcap_messages += 1;
    let Some((sequence, stream)) = frame_id(data) else {
        verified.corrupt_frames += 1;
        return;
    };
    if sequence >= summary.expected || stream != STREAM_ID {
        verified.out_of_range += 1;
        return;
    }
    let overlap = present.insert(sequence, sequence + 1);
    verified.duplicates += overlap;
    if previous.is_some_and(|(id, time)| sequence < id || header.log_time < time) {
        verified.out_of_order += 1;
    }
    *previous = Some((sequence, header.log_time));

    let valid_time = header
        .log_time
        .checked_sub(summary.start_unix_ns)
        .is_some_and(|elapsed| {
            elapsed < summary.config.duration_sec.as_nanos() as u64
                && current_slot(elapsed, summary.config.fps) == sequence
        });
    let valid = data.len() == summary.config.frame_bytes
        && header.channel_id == channel
        && header.sequence == sequence as u32
        && header.publish_time
            == summary.start_unix_ns + slot_time_ns(sequence, summary.config.fps)
        && valid_time
        && matches_pattern(data, summary.config.seed, sequence, stream);
    if !valid {
        verified.corrupt_frames += 1;
    } else if overlap == 0 {
        verified.verified += 1;
    }
}

struct LossAudit {
    counts: LossCounts,
    before_generation: u64,
    after_acceptance: u64,
    uncertain: u64,
}

fn read_losses(
    path: &Path,
    summary: &CoreReport,
    verified: &mut Verification,
    losses: &mut Ranges,
) -> Result<LossAudit> {
    let mut input = open(path)?;
    let mut line = Vec::with_capacity(512);
    let mut counts = [0_u64; 6];
    let mut before_generation = 0_u64;
    let mut after_acceptance = 0_u64;
    let mut uncertain = 0_u64;
    let mut number = 0_u64;
    loop {
        line.clear();
        if input.by_ref().take(4097).read_until(b'\n', &mut line)? == 0 {
            break;
        }
        number += 1;
        ensure!(
            line.len() <= 4096 && line.ends_with(b"\n"),
            "invalid line length at {number}"
        );
        let event: Event =
            serde_json::from_slice(&line).with_context(|| format!("parse line {number}"))?;
        ensure!(
            event.stream_id == STREAM_ID
                && event.start_sequence < event.end_sequence_exclusive
                && event.end_sequence_exclusive <= summary.expected
                && event.reason.valid_layer(event.layer),
            "invalid event at line {number}"
        );
        let earliest = if event.reason == Reason::Unknown && event.layer == Layer::Source {
            summary.start_unix_ns
        } else {
            summary.start_unix_ns + slot_time_ns(event.start_sequence, summary.config.fps)
        };
        ensure!(
            event.timestamp_unix_ns >= earliest,
            "invalid timestamp at line {number}"
        );
        let len = event.end_sequence_exclusive - event.start_sequence;
        let overlap = losses.insert(event.start_sequence, event.end_sequence_exclusive);
        verified.duplicate_losses = verified.duplicate_losses.saturating_add(overlap);
        let unique = len - overlap;
        counts[event.reason as usize] += unique;
        if matches!(
            event.reason,
            Reason::SourceDeadlineMiss | Reason::DmaPoolExhausted
        ) || (event.reason == Reason::Unknown && event.layer == Layer::Source)
        {
            before_generation += unique;
        }
        if event.layer == Layer::Writer {
            after_acceptance += unique;
        }
        if matches!(event.reason, Reason::Unknown | Reason::WriterError) {
            uncertain += unique;
        }
    }
    Ok(LossAudit {
        counts: LossCounts::from(counts),
        before_generation,
        after_acceptance,
        uncertain,
    })
}

// Coalescing intervals keep exact duplicate/overlap accounting even for
// out-of-order input, without retaining any of the message payloads.
#[derive(Default)]
pub(crate) struct Ranges {
    intervals: BTreeMap<u64, u64>,
    pub(crate) count: u64,
}

impl Ranges {
    pub(crate) fn insert(&mut self, start: u64, end: u64) -> u64 {
        let mut merged_start = self
            .intervals
            .range(..=start)
            .next_back()
            .filter(|&(_, &right)| right >= start)
            .map_or(start, |(&left, _)| left);
        let mut merged_end = end;
        let mut overlap = 0;
        while let Some((&left, &right)) = self.intervals.range(merged_start..=merged_end).next() {
            overlap += end.min(right).saturating_sub(start.max(left));
            merged_start = merged_start.min(left);
            merged_end = merged_end.max(right);
            self.intervals.remove(&left);
        }
        self.intervals.insert(merged_start, merged_end);
        self.count += end - start - overlap;
        overlap
    }

    pub(crate) fn contains(&self, item: u64) -> bool {
        self.intervals
            .range(..=item)
            .next_back()
            .is_some_and(|(_, &end)| item < end)
    }

    pub(crate) fn intersection(&self, other: &Self) -> u64 {
        let mut left = self.intervals.iter().peekable();
        let mut right = other.intervals.iter().peekable();
        let mut count = 0;
        while let (Some(&(&a, &b)), Some(&(&c, &d))) = (left.peek(), right.peek()) {
            count += b.min(d).saturating_sub(a.max(c));
            if b <= d {
                left.next();
            } else {
                right.next();
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use clap::Parser;

    use crate::config::{Cli, Command};
    use crate::source::fill;

    struct Fixture {
        _directory: tempfile::TempDir,
        args: VerifyArgs,
        summary: CoreReport,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let args = VerifyArgs {
                input: directory.path().join("run.mcap"),
                report: directory.path().join("run-summary.json"),
                loss_log: directory.path().join("run-loss.jsonl"),
            };
            let Cli {
                command: Command::Record(config),
            } = Cli::try_parse_from(["recorder", "record", "--frame-bytes", "128", "--fps", "4"])
                .unwrap()
            else {
                panic!("expected record")
            };
            let summary = CoreReport {
                report_version: REPORT_VERSION,
                status: Status::Ok,
                config,
                start_unix_ns: 500,
                expected: 4,
                generated: 3,
                received: 3,
                written: 3,
                losses: LossCounts {
                    dma_pool_exhausted: 1,
                    ..LossCounts::default()
                },
                loss_record_failures: 0,
                mcap_finalized: true,
                mcap_synced: true,
                loss_log_synced: true,
                uncertain_ranges: vec![],
            };
            serde_json::to_writer(File::create(&args.report).unwrap(), &summary).unwrap();
            let fixture = Self {
                _directory: directory,
                args,
                summary,
            };
            fixture.write_losses(&[1]);
            fixture
        }

        fn write_losses(&self, ids: &[u64]) {
            let mut losses = File::create(&self.args.loss_log).unwrap();
            for &id in ids {
                let event = Event {
                    stream_id: STREAM_ID,
                    start_sequence: id,
                    end_sequence_exclusive: id + 1,
                    timestamp_unix_ns: self.summary.start_unix_ns
                        + slot_time_ns(id, self.summary.config.fps),
                    layer: Layer::Acquisition,
                    reason: Reason::DmaPoolExhausted,
                };
                serde_json::to_writer(&mut losses, &event).unwrap();
                writeln!(losses).unwrap();
            }
        }

        fn write_mcap(&self, ids: &[u64], damaged: Option<u64>) {
            let config = &self.summary.config;
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .compression(None)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .calculate_data_section_crc(true)
                .create(File::create(&self.args.input).unwrap())
                .unwrap();
            let schema = writer
                .add_schema(SCHEMA_NAME, SCHEMA_ENCODING, SCHEMA)
                .unwrap();
            let channel = writer
                .add_channel(
                    schema,
                    TOPIC,
                    ENCODING,
                    &channel_metadata(config.seed, 128, self.summary.start_unix_ns),
                )
                .unwrap();
            let mut bytes = [0; 128];
            for &sequence in ids {
                fill(&mut bytes, config.seed, sequence);
                if damaged == Some(sequence) {
                    bytes[127] ^= 0x80;
                }
                let time = self.summary.start_unix_ns + slot_time_ns(sequence, config.fps);
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id: channel,
                            sequence: sequence as u32,
                            log_time: time,
                            publish_time: time,
                        },
                        &bytes,
                    )
                    .unwrap();
            }
            writer.finish().unwrap();
        }
    }

    #[test]
    fn detects_missing_duplicate_out_of_order_out_of_range_and_corrupt_messages() {
        let fixture = Fixture::new();
        fixture.write_mcap(&[0, 2, 3], None);
        assert!(check(&fixture.args).unwrap().ok);

        fixture.write_mcap(&[0, 3], None);
        let missing = check(&fixture.args).unwrap();
        assert!(!missing.ok);
        assert_eq!(missing.unreported_missing, 1);

        fixture.write_mcap(&[0, 2, 0, 3, 4], None);
        let duplicate = check(&fixture.args).unwrap();
        assert!(!duplicate.ok);
        assert_eq!(duplicate.duplicates, 1);
        assert_eq!(duplicate.out_of_order, 1);
        assert_eq!(duplicate.out_of_range, 1);

        fixture.write_mcap(&[0, 2, 3], Some(3));
        let corrupt = check(&fixture.args).unwrap();
        assert!(!corrupt.ok);
        assert_eq!(corrupt.corrupt_frames, 1);
        assert_eq!(corrupt.verified, 2);
        assert!(corrupt.finalized);
    }

    #[test]
    fn detects_false_duplicate_and_unreported_loss_journal_entries() {
        let fixture = Fixture::new();
        fixture.write_mcap(&[0, 2, 3], None);
        fixture.write_losses(&[0, 1, 1]);
        let false_claim = check(&fixture.args).unwrap();
        assert!(!false_claim.ok);
        assert_eq!(false_claim.false_loss_claims, 1);
        assert_eq!(false_claim.duplicate_losses, 1);
        fixture.write_losses(&[]);
        let missing_loss = check(&fixture.args).unwrap();
        assert!(!missing_loss.ok);
        assert_eq!(missing_loss.unreported_missing, 1);
    }

    #[test]
    fn verifies_ids_across_the_mcap_u32_sequence_wrap() {
        let mut fixture = Fixture::new();
        let wrap = 1_u64 << 32;
        fixture.summary.config.fps = 1_000_000_000;
        fixture.summary.config.duration_sec = std::time::Duration::from_nanos(wrap + 1);
        fixture.summary.expected = wrap + 1;
        fixture.summary.generated = 2;
        fixture.summary.received = 2;
        fixture.summary.written = 2;
        fixture.summary.losses.dma_pool_exhausted = wrap - 1;
        serde_json::to_writer(
            File::create(&fixture.args.report).unwrap(),
            &fixture.summary,
        )
        .unwrap();
        let mut loss = File::create(&fixture.args.loss_log).unwrap();
        serde_json::to_writer(
            &mut loss,
            &Event {
                stream_id: STREAM_ID,
                start_sequence: 0,
                end_sequence_exclusive: wrap - 1,
                timestamp_unix_ns: fixture.summary.start_unix_ns,
                layer: Layer::Acquisition,
                reason: Reason::DmaPoolExhausted,
            },
        )
        .unwrap();
        writeln!(loss).unwrap();
        fixture.write_mcap(&[wrap - 1, wrap], None);
        let result = check(&fixture.args).unwrap();
        assert!(result.ok);
        assert_eq!(result.verified, 2);
        assert_eq!(result.journaled_losses, wrap - 1);
    }

    #[test]
    fn detects_incomplete_footer_crc_and_trailing_bytes() {
        let fixture = Fixture::new();
        fixture.write_mcap(&[0, 2, 3], None);
        let content = std::fs::read(&fixture.args.input).unwrap();
        std::fs::write(&fixture.args.input, &content[..content.len() - 1]).unwrap();
        let truncated = check(&fixture.args).unwrap();
        assert!(!truncated.ok && !truncated.finalized);

        let mut corrupt = content.clone();
        let offset = corrupt.len() - mcap::MAGIC.len() - 1;
        corrupt[offset] ^= 1;
        std::fs::write(&fixture.args.input, corrupt).unwrap();
        let bad_crc = check(&fixture.args).unwrap();
        assert!(!bad_crc.ok && !bad_crc.finalized);

        let mut trailing = content;
        trailing.push(1);
        std::fs::write(&fixture.args.input, trailing).unwrap();
        let trailing = check(&fixture.args).unwrap();
        assert!(!trailing.ok && !trailing.finalized);
    }

    #[test]
    fn intervals_count_duplicates_and_intersection_in_any_order() {
        let mut a = Ranges::default();
        assert_eq!(a.insert(5, 10), 0);
        assert_eq!(a.insert(0, 3), 0);
        assert_eq!(a.insert(3, 7), 2);
        assert_eq!(a.insert(1, 4), 3);
        assert_eq!(a.count, 10);
        let mut b = Ranges::default();
        b.insert(1, 2);
        b.insert(7, 12);
        assert_eq!(a.intersection(&b), 4);
    }
}
