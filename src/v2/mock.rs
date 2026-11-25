use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use super::config::{Model, ReplacementMode, Source};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Synthetic,
    MockRos2,
    MockUdp,
    MockSocketcan,
    MockV4l2,
    MockProframe,
    MockDrive,
}

impl Kind {
    pub const ALL: [Self; 7] = [
        Self::Synthetic,
        Self::MockRos2,
        Self::MockUdp,
        Self::MockSocketcan,
        Self::MockV4l2,
        Self::MockProframe,
        Self::MockDrive,
    ];

    pub fn is_synthetic(&self) -> bool {
        *self == Self::Synthetic
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Synthetic => "synthetic",
            Self::MockRos2 => "mock_ros2",
            Self::MockUdp => "mock_udp",
            Self::MockSocketcan => "mock_socketcan",
            Self::MockV4l2 => "mock_v4l2",
            Self::MockProframe => "mock_proframe",
            Self::MockDrive => "mock_drive",
        }
    }

    pub fn payload_format(self) -> &'static str {
        match self {
            Self::Synthetic => "synthetic-series",
            Self::MockRos2 => "cdr",
            Self::MockUdp => "mock-datagram",
            Self::MockSocketcan => "mock-can-fd-frame",
            Self::MockV4l2 | Self::MockProframe | Self::MockDrive => "mock-camera-byte-plane",
        }
    }

    pub fn validate(self, source: &Source) -> Result<()> {
        ensure!(source.drop_every != Some(0), "drop_every must be positive");
        let valid = match self {
            Self::Synthetic => source.fence_delay_us == 0 && source.drop_every.is_none(),
            Self::MockRos2 | Self::MockUdp => {
                source.model == Model::Owned && source.fence_delay_us == 0
            }
            Self::MockSocketcan => {
                source.model == Model::Owned
                    && source.fence_delay_us == 0
                    && [24, 32, 48, 64].contains(&source.frame_bytes)
            }
            Self::MockV4l2 => {
                source.model != Model::Owned && source.replacement() == ReplacementMode::Unsupported
            }
            Self::MockProframe | Self::MockDrive => {
                source.model == Model::Pool
                    && source.replacement() != ReplacementMode::ExplicitReplacement
            }
        };
        ensure!(
            valid,
            "invalid {} fixture contract for {}",
            self.name(),
            source.id
        );
        ensure!(
            self != Self::MockRos2 || (20..=1048576).contains(&source.frame_bytes),
            "invalid CDR fixture size"
        );
        Ok(())
    }

    pub fn fill(self, bytes: &mut [u8], seed: u64, stream: u16, sequence: u64) {
        crate::source::fill_for(bytes, seed, stream, sequence);
        if self == Self::MockRos2 {
            let size = bytes.len() as u32;
            bytes[..4].copy_from_slice(&[0, 1, 0, 0]); // Little-endian CDR.
            bytes[4..8].copy_from_slice(&((sequence >> 32) as u32).to_le_bytes());
            bytes[8..12].copy_from_slice(&(sequence as u32).to_le_bytes());
            bytes[12..16].copy_from_slice(&(size - 16).to_le_bytes());
        }
    }

    pub fn matches(self, bytes: &[u8], seed: u64, sequence: u64, stream: u16) -> bool {
        if self == Self::MockRos2 {
            if bytes.len() < 16
                || bytes[..4] != [0, 1, 0, 0]
                || bytes[12..16] != ((bytes.len() - 16) as u32).to_le_bytes()
            {
                return false;
            }
            let pattern = crate::source::tile(seed, stream, sequence);
            return bytes[16..]
                .iter()
                .enumerate()
                .all(|(i, byte)| *byte == pattern[(i + 6) % pattern.len()]);
        }
        crate::source::matches_pattern(bytes, seed, sequence, stream)
    }

    pub fn sequence(self, bytes: &[u8], stream: u16) -> Option<(u64, u16)> {
        if self == Self::MockRos2 {
            let upper = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?);
            let lower = u32::from_le_bytes(bytes.get(8..12)?.try_into().ok()?);
            Some(((u64::from(upper) << 32) | u64::from(lower), stream))
        } else {
            crate::source::frame_id(bytes)
        }
    }
}

pub const ROS_SCHEMA: (&str, &str, &[u8]) = (
    "predictable_recorder_msgs/msg/MockFrame",
    "ros2msg",
    b"uint32 sequence_high\nuint32 sequence_low\nuint8[] payload\n",
);

pub fn ros_schema(size: usize) -> (&'static str, &'static str, &'static [u8]) {
    if size == 20 {
        (
            "predictable_recorder_msgs/msg/MockScalar",
            "ros2msg",
            b"uint32 sequence_high\nuint32 sequence_low\nuint32 length\nuint32 value\n",
        )
    } else {
        ROS_SCHEMA
    }
}
