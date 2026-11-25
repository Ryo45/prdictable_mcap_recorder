use std::collections::BTreeMap;
use std::io::Cursor;

use anyhow::Result;
use crc32fast::Hasher;
use mcap::records::op;

use super::adapter::Frame;
use super::config::{Settings, Source, WriterMode};
use super::mock::{Kind, ros_schema};
use super::storage::Request;
use crate::source::{ENCODING, SCHEMA, SCHEMA_ENCODING, SCHEMA_NAME};

pub const MESSAGE_HEADER_BYTES: usize = 31;

pub struct Layout {
    epoch: u64,
    channels: Vec<u16>,
    data_crc: Hasher,
    pub offset: u64,
    recovery_crc: bool,
}

pub fn topic(source: &Source) -> String {
    if source.kind == Kind::MockRos2 {
        return format!("/{}", source.id);
    }
    format!("/{}/{}", source.kind.name(), source.id)
}

pub fn schema(source: &Source) -> (&'static str, &'static str, &'static [u8]) {
    if source.kind == Kind::MockRos2 {
        return ros_schema(source.frame_bytes);
    }
    (SCHEMA_NAME, SCHEMA_ENCODING, SCHEMA)
}

pub fn encoding(source: &Source) -> &'static str {
    if source.kind == Kind::MockRos2 {
        "cdr"
    } else {
        ENCODING
    }
}

pub fn metadata(stream: u16, settings: &Settings, epoch: u64) -> BTreeMap<String, String> {
    let source = &settings.sources[usize::from(stream)];
    let mut metadata = BTreeMap::from([
        ("stream_id".into(), stream.to_string()),
        ("source_id".into(), source.id.clone()),
        ("seed".into(), settings.record.seed.to_string()),
        ("frame_bytes".into(), source.frame_bytes.to_string()),
        ("session_epoch".into(), epoch.to_string()),
    ]);
    if !source.kind.is_synthetic() {
        metadata.insert("source_kind".into(), source.kind.name().into());
        metadata.insert("payload_format".into(), source.kind.payload_format().into());
        if source.model != super::config::Model::Owned {
            metadata.insert("plane_count".into(), "1".into());
            metadata.insert("layout".into(), "pitch-linear".into());
            metadata.insert("height".into(), "1".into());
            metadata.insert("stride_bytes".into(), source.frame_bytes.to_string());
            metadata.insert("bytes_used".into(), source.frame_bytes.to_string());
        }
        metadata.insert(
            "clock_domain".into(),
            "synthetic_monotonic_mapped_to_unix_ns".into(),
        );
        metadata.insert("sequence_origin".into(), "mock_source".into());
        if source.kind == Kind::MockRos2 {
            metadata.insert("schema_origin".into(), "recorder_fixture".into());
            metadata.insert("publisher_gid".into(), format!("mock-{stream}"));
            metadata.insert("ros_distro".into(), "none (mock)".into());
            metadata.insert("qos".into(), "best_effort; keep_last; depth=1".into());
        }
    }
    metadata
}

impl Layout {
    pub fn new(settings: &Settings, epoch: u64) -> Result<(Self, Vec<u8>)> {
        // Use the official writer for all variable-size metadata. Message
        // ranges are laid out here for the positional storage backends.
        let mut writer = mcap::WriteOptions::new()
            .use_chunks(false)
            .compression(None)
            .profile("predictable-recorder-v2")
            .library(concat!("predictable_recorder/", env!("CARGO_PKG_VERSION")))
            .create(Cursor::new(Vec::new()))?;

        let mut channels = Vec::with_capacity(settings.sources.len());
        for (index, source) in settings.sources.iter().enumerate() {
            let (name, schema_encoding, data) = schema(source);
            let schema_id = writer.add_schema(name, schema_encoding, data)?;
            channels.push(writer.add_channel(
                schema_id,
                &topic(source),
                encoding(source),
                &metadata(index as u16, settings, epoch),
            )?);
        }
        let preamble = writer.into_inner().into_inner();
        let mut data_crc = Hasher::new();
        data_crc.update(&preamble);
        Ok((
            Self {
                epoch,
                channels,
                data_crc,
                offset: preamble.len() as u64,
                recovery_crc: settings.record.writer != WriterMode::Null
                    && settings
                        .sources
                        .iter()
                        .any(|source| !source.kind.is_synthetic()),
            },
            preamble,
        ))
    }

    pub fn message(&mut self, id: u64, frame: Frame, submitted_ns: u64) -> Request {
        let mut header = [0; MESSAGE_HEADER_BYTES];
        header[0] = op::MESSAGE;
        header[1..9].copy_from_slice(&(22 + frame.size as u64).to_le_bytes());
        header[9..11].copy_from_slice(&self.channels[usize::from(frame.id.source)].to_le_bytes());
        header[11..15].copy_from_slice(&(frame.id.sequence as u32).to_le_bytes());
        header[15..23].copy_from_slice(&(self.epoch + frame.captured_ns).to_le_bytes());
        header[23..31].copy_from_slice(&(self.epoch + frame.planned_ns).to_le_bytes());
        self.data_crc.update(&header);
        self.data_crc.update(frame.bytes());
        let offset = self.offset;
        self.offset += header.len() as u64 + frame.size as u64;
        Request {
            id,
            offset,
            header,
            frame,
            submitted_ns,
        }
    }

    pub fn prefix_crc(&self) -> Option<u32> {
        self.recovery_crc.then(|| self.data_crc.clone().finalize())
    }

    pub fn finish(&mut self) -> [u8; 50] {
        let mut tail = [0; 50];
        tail[0] = op::DATA_END;
        tail[1..9].copy_from_slice(&4_u64.to_le_bytes());
        tail[9..13].copy_from_slice(&self.data_crc.clone().finalize().to_le_bytes());
        tail[13] = op::FOOTER;
        tail[14..22].copy_from_slice(&20_u64.to_le_bytes());
        // Empty Summary and Summary Offset sections.
        let crc = crc32fast::hash(&tail[13..38]);
        tail[38..42].copy_from_slice(&crc.to_le_bytes());
        tail[42..].copy_from_slice(mcap::MAGIC);
        if self.recovery_crc {
            self.data_crc.update(&tail);
        }
        tail
    }
}
