use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::SyncSender;

use serde::{Deserialize, Serialize};

use crate::metrics::Progress;
use crate::source::STREAM_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layer {
    Source,
    Acquisition,
    Writer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum Reason {
    SourceDeadlineMiss,
    DmaPoolExhausted,
    SpillPoolExhausted,
    QueueFull,
    WriterError,
    Unknown,
}

impl Reason {
    pub fn valid_layer(self, layer: Layer) -> bool {
        match self {
            Self::SourceDeadlineMiss => layer == Layer::Source,
            Self::DmaPoolExhausted | Self::SpillPoolExhausted | Self::QueueFull => {
                layer == Layer::Acquisition
            }
            Self::WriterError => layer == Layer::Writer,
            Self::Unknown => matches!(layer, Layer::Source | Layer::Writer),
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LossCounts {
    pub source_deadline_miss: u64,
    pub dma_pool_exhausted: u64,
    pub spill_pool_exhausted: u64,
    pub queue_full: u64,
    pub writer_error: u64,
    pub unknown: u64,
}

impl From<[u64; 6]> for LossCounts {
    fn from(
        [
            source_deadline_miss,
            dma_pool_exhausted,
            spill_pool_exhausted,
            queue_full,
            writer_error,
            unknown,
        ]: [u64; 6],
    ) -> Self {
        Self {
            source_deadline_miss,
            dma_pool_exhausted,
            spill_pool_exhausted,
            queue_full,
            writer_error,
            unknown,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub stream_id: u16,
    pub start_sequence: u64,
    pub end_sequence_exclusive: u64,
    pub timestamp_unix_ns: u64,
    pub layer: Layer,
    pub reason: Reason,
}

pub struct Losses<'a> {
    pub progress: &'a Progress,
    sender: SyncSender<Event>,
    pending: Option<Event>,
}

impl<'a> Losses<'a> {
    pub fn new(progress: &'a Progress, sender: SyncSender<Event>) -> Self {
        Self {
            progress,
            sender,
            pending: None,
        }
    }

    pub fn record(&mut self, start: u64, end: u64, timestamp: u64, layer: Layer, reason: Reason) {
        debug_assert!(start < end && reason.valid_layer(layer));
        self.progress.losses[reason as usize].fetch_add(end - start, Relaxed);
        if let Some(event) = &mut self.pending
            && event.end_sequence_exclusive == start
            && event.layer == layer
            && event.reason == reason
        {
            event.end_sequence_exclusive = end;
        } else {
            self.flush();
            self.pending = Some(Event {
                stream_id: STREAM_ID,
                start_sequence: start,
                end_sequence_exclusive: end,
                timestamp_unix_ns: timestamp,
                layer,
                reason,
            });
        }
    }

    fn flush(&mut self) {
        if let Some(event) = self.pending.take()
            && self.sender.try_send(event).is_err()
        {
            self.progress.loss_record_failures.fetch_add(1, Relaxed);
        }
    }
}

impl Drop for Losses<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn contiguous_events_coalesce_and_full_channel_is_reported() {
        let progress = Progress::default();
        let (tx, rx) = mpsc::sync_channel(1);
        let mut losses = Losses::new(&progress, tx);
        losses.record(0, 1, 0, Layer::Acquisition, Reason::DmaPoolExhausted);
        losses.record(1, 5, 1, Layer::Acquisition, Reason::DmaPoolExhausted);
        losses.record(5, 6, 2, Layer::Acquisition, Reason::QueueFull);
        drop(losses);
        let event = rx.recv().unwrap();
        assert_eq!((event.start_sequence, event.end_sequence_exclusive), (0, 5));
        assert_eq!(event.reason, Reason::DmaPoolExhausted);
        assert_eq!(progress.loss_record_failures.load(Relaxed), 1);
        assert_eq!(progress.loss_counts(), [0, 5, 0, 1, 0, 0]);
    }
}
