use std::sync::atomic::Ordering::Relaxed;

use crate::v2::adapter::FrameId;
use crate::v2::config::{MS, Settings};
use crate::v2::integrity::{CommitLine, Layer, Ledger, Reason, Span};
use crate::v2::metrics::{Progress, SourceProgress};

#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteState {
    Submitted,
    Written,
    Unknown,
}

struct Entry {
    id: FrameId,
    state: WriteState,
    size: u64,
    queued_ns: u64,
}

pub(super) struct Group {
    id: u64,
    first_request: u64,
    start_offset: u64,
    first_submit_ns: u64,
    entries: Vec<Entry>,
    spans: Vec<Option<Span>>,
}

impl Group {
    pub(super) fn new(settings: &Settings) -> Self {
        Self {
            id: 0,
            first_request: 0,
            start_offset: 0,
            first_submit_ns: 0,
            entries: Vec::with_capacity(settings.record.commit_messages),
            spans: vec![None; settings.sources.len()],
        }
    }

    pub(super) fn submit(&mut self, id: FrameId, size: u64, now: u64, queued_ns: u64) -> u64 {
        if self.entries.is_empty() {
            self.first_submit_ns = now;
        }
        let next = self.first_request + self.entries.len() as u64;
        self.entries.push(Entry {
            id,
            state: WriteState::Submitted,
            size,
            queued_ns,
        });
        next
    }

    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn complete(&mut self, request: u64, id: FrameId, written: bool) {
        let entry = &mut self.entries[(request - self.first_request) as usize];
        assert_eq!(entry.id, id);
        assert!(entry.state == WriteState::Submitted);
        entry.state = if written {
            WriteState::Written
        } else {
            WriteState::Unknown
        };
    }

    pub(super) fn due(&self, now: u64, offset: u64, settings: &Settings) -> bool {
        !self.entries.is_empty()
            && (self.entries.len() >= settings.record.commit_messages
                || offset.saturating_sub(self.start_offset) >= settings.record.commit_bytes
                || now.saturating_sub(self.first_submit_ns)
                    >= settings.record.commit_interval_ms * MS)
    }

    pub(super) fn commit(
        &mut self,
        boundary: (u64, Option<u32>),
        finalized: bool,
        now: u64,
        progress: &Progress,
        sources: &[SourceProgress],
        ledger: &Ledger<'_>,
    ) {
        let (end_offset, prefix_crc32) = boundary;
        ledger.commit(CommitLine::Group {
            group_id: self.id,
            start_offset: self.start_offset,
            end_offset,
            message_count: self.entries.len() as u64,
            synced_at_ns: now,
            finalized,
            prefix_crc32,
        });
        let mut latency = prefix_crc32.map(|_| progress.accepted_to_committed.lock().unwrap());
        for entry in &self.entries {
            if let Some(histogram) = &mut latency {
                histogram.record(now.saturating_sub(entry.queued_ns));
            }
            assert!(entry.state == WriteState::Written);
            sources[usize::from(entry.id.source)]
                .committed
                .fetch_add(1, Relaxed);
            progress.committed_bytes.fetch_add(entry.size, Relaxed);
            let span = Span::frame(entry.id);
            let pending = &mut self.spans[usize::from(span.stream)];
            if let Some(previous) = pending
                && previous.end == span.start
            {
                previous.end = span.end;
            } else if let Some(previous) = pending.replace(span) {
                ledger.commit(CommitLine::Frames {
                    group_id: self.id,
                    span: previous,
                });
            }
        }
        for span in self.spans.iter_mut().filter_map(Option::take) {
            ledger.commit(CommitLine::Frames {
                group_id: self.id,
                span,
            });
        }
        self.first_request += self.entries.len() as u64;
        self.entries.clear();
        self.start_offset = end_offset;
        self.id += 1;
    }

    pub(super) fn unknown(&mut self, ledger: &mut Ledger<'_>, now: u64, reason: Reason) {
        for entry in &mut self.entries {
            if entry.state != WriteState::Unknown {
                let layer = if reason == Reason::SyncFailure {
                    Layer::Commit
                } else {
                    Layer::Writer
                };
                ledger.loss(Span::frame(entry.id), now, layer, reason, true);
                entry.state = WriteState::Unknown;
            }
        }
    }
}
