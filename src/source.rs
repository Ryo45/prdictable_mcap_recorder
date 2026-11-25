use std::collections::BTreeMap;

use crate::config::NS_PER_SEC;

pub const STREAM_ID: u16 = 0;
pub const FRAME_HEADER_BYTES: usize = 10;
pub const TOPIC: &str = "/synthetic/dma";
pub const ENCODING: &str = "recorder-dma-v1";
pub const SCHEMA_NAME: &str = "recorder.SyntheticFrameV1";
pub const SCHEMA_ENCODING: &str = "text/plain";
pub const SCHEMA: &[u8] = b"u64le sequence; u16le stream_id; remaining bytes repeat the recorder-dma-v1 tile (seed, stream_id, sequence)";

pub fn channel_metadata(
    seed: u64,
    frame_bytes: usize,
    start_unix_ns: u64,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("stream_id".into(), STREAM_ID.to_string()),
        ("seed".into(), seed.to_string()),
        ("frame_bytes".into(), frame_bytes.to_string()),
        ("run_start_unix_ns".into(), start_unix_ns.to_string()),
    ])
}

pub(crate) fn tile(seed: u64, stream: u16, sequence: u64) -> [u8; 256] {
    let mut bytes = [0; 256];
    let mut state = seed ^ sequence.rotate_left(21) ^ (u64::from(stream) << 48);
    for word in bytes.chunks_exact_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word.copy_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    bytes
}

pub fn fill(bytes: &mut [u8], seed: u64, sequence: u64) {
    fill_for(bytes, seed, STREAM_ID, sequence);
}

pub fn fill_for(bytes: &mut [u8], seed: u64, stream: u16, sequence: u64) {
    bytes[..8].copy_from_slice(&sequence.to_le_bytes());
    bytes[8..FRAME_HEADER_BYTES].copy_from_slice(&stream.to_le_bytes());
    let pattern = tile(seed, stream, sequence);
    for chunk in bytes[FRAME_HEADER_BYTES..].chunks_mut(pattern.len()) {
        chunk.copy_from_slice(&pattern[..chunk.len()]);
    }
}

pub fn frame_id(bytes: &[u8]) -> Option<(u64, u16)> {
    let header = bytes.get(..FRAME_HEADER_BYTES)?;
    Some((
        u64::from_le_bytes(header[..8].try_into().unwrap()),
        u16::from_le_bytes(header[8..].try_into().unwrap()),
    ))
}

pub fn matches_pattern(bytes: &[u8], seed: u64, sequence: u64, stream: u16) -> bool {
    let pattern = tile(seed, stream, sequence);
    bytes[FRAME_HEADER_BYTES..]
        .chunks(pattern.len())
        .all(|chunk| chunk == &pattern[..chunk.len()])
}

pub fn slot_time_ns(sequence: u64, fps: u64) -> u64 {
    // Callers bound the schedule to u64 nanoseconds. This saturates for the
    // end boundary of the last slot when it extends past that range.
    u64::try_from((u128::from(sequence) * u128::from(NS_PER_SEC)).div_ceil(u128::from(fps)))
        .unwrap_or(u64::MAX)
}

pub fn current_slot(elapsed_ns: u64, fps: u64) -> u64 {
    ((u128::from(elapsed_ns) * u128::from(fps)) / u128::from(NS_PER_SEC)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_is_absolute_and_has_no_rounding_drift() {
        assert_eq!(slot_time_ns(1, 60), 16_666_667);
        assert_eq!(current_slot(16_666_666, 60), 0);
        assert_eq!(current_slot(16_666_667, 60), 1);
        assert_eq!(current_slot(49_000_000, 60), 2);
        assert_eq!(slot_time_ns(600, 60), 10_000_000_000);
    }

    #[test]
    fn full_id_and_tail_bytes_are_checked() {
        let mut bytes = [0; 555];
        let id = (1 << 32) + 8;
        fill(&mut bytes, 42, id);
        assert_eq!(frame_id(&bytes), Some((id, STREAM_ID)));
        assert!(matches_pattern(&bytes, 42, id, STREAM_ID));
        bytes[554] ^= 1;
        assert!(!matches_pattern(&bytes, 42, id, STREAM_ID));
    }
}
