use std::sync::atomic::Ordering::Relaxed;

use super::journal::Journal;
use crate::v2::adapter::FrameId;
use crate::v2::config::{MS, Settings};
use crate::v2::integrity::{CommitLine, Layer, Ledger, Reason, Span};
use crate::v2::metrics::{Progress, SourceProgress};
use anyhow::{Result, ensure};

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
    next_offset: u64,
    first_message_offset: u64,
    entries: Vec<Entry>,
    spans: Vec<Option<Span>>,
}

impl Group {
    pub(super) fn new(settings: &Settings, first_offset: u64) -> Self {
        Self {
            id: 0,
            first_request: 0,
            start_offset: 0,
            first_submit_ns: 0,
            next_offset: first_offset,
            first_message_offset: first_offset,
            entries: Vec::with_capacity(settings.record.commit_messages),
            spans: vec![None; settings.sources.len()],
        }
    }

    pub(super) fn submit(&mut self, event: super::Submitted) -> Result<()> {
        let super::Submitted {
            request_id,
            frame_id: id,
            offset,
            size,
            wire_len,
            now,
            queued_ns,
        } = event;
        ensure!(
            request_id == self.first_request + self.entries.len() as u64,
            "noncontiguous request ID"
        );
        ensure!(self.next_offset == offset, "noncontiguous file offset");
        self.next_offset = offset
            .checked_add(wire_len)
            .ok_or_else(|| anyhow::anyhow!("file offset overflow"))?;
        if self.entries.is_empty() {
            self.first_submit_ns = now;
        }
        self.entries.push(Entry {
            id,
            state: WriteState::Submitted,
            size,
            queued_ns,
        });
        Ok(())
    }

    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn wire_bytes(&self) -> u64 {
        self.next_offset - self.first_message_offset
    }

    pub(super) fn next_request(&self) -> u64 {
        self.first_request + self.entries.len() as u64
    }

    pub(super) fn reset(&mut self, group: u64, request: u64, offset: u64) {
        self.id = group;
        self.first_request = request;
        self.start_offset = offset;
        self.next_offset = offset;
        self.first_message_offset = offset;
        self.entries.clear();
        self.spans.fill(None);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn complete(&mut self, request: u64, id: FrameId, written: bool) -> Result<()> {
        let entry = request
            .checked_sub(self.first_request)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.entries.get_mut(index))
            .ok_or_else(|| anyhow::anyhow!("completion for unknown request {request}"))?;
        ensure!(
            entry.id == id && entry.state == WriteState::Submitted,
            "invalid or duplicate completion {request}"
        );
        entry.state = if written {
            WriteState::Written
        } else {
            WriteState::Unknown
        };
        Ok(())
    }

    pub(super) fn due(&self, now: u64, offset: u64, settings: &Settings) -> bool {
        !self.entries.is_empty()
            && (self.entries.len() >= settings.record.commit_messages
                || offset.saturating_sub(self.start_offset) >= settings.record.commit_bytes
                || now.saturating_sub(self.first_submit_ns)
                    >= settings.record.commit_interval_ms * MS)
    }

    pub(super) fn validate_boundary(&self, end: u64, final_group: bool) -> Result<()> {
        let expected = self.next_offset
            + if final_group {
                super::super::layout::TAIL_BYTES as u64
            } else {
                0
            };
        ensure!(
            end == expected && end > self.start_offset,
            "invalid commit boundary"
        );
        ensure!(
            self.entries
                .iter()
                .all(|entry| entry.state == WriteState::Written),
            "commit contains pending or failed I/O"
        );
        Ok(())
    }

    pub(super) fn write_evidence(
        &mut self,
        journal: &mut Journal,
        boundary: (u64, Option<u32>),
        finalized: bool,
        now: u64,
    ) -> Result<()> {
        let (end_offset, prefix_crc32) = boundary;
        journal.append(&CommitLine::Group {
            group_id: self.id,
            start_offset: self.start_offset,
            end_offset,
            message_count: self.entries.len() as u64,
            synced_at_ns: now,
            finalized,
            prefix_crc32,
        })?;
        for entry in &self.entries {
            let span = Span::frame(entry.id);
            let pending = &mut self.spans[usize::from(span.stream)];
            if let Some(previous) = pending
                && previous.end == span.start
            {
                previous.end = span.end;
            } else if let Some(previous) = pending.replace(span) {
                journal.append(&CommitLine::Frames {
                    group_id: self.id,
                    span: previous,
                })?;
            }
        }
        for span in self.spans.iter_mut().filter_map(Option::take) {
            journal.append(&CommitLine::Frames {
                group_id: self.id,
                span,
            })?;
        }
        Ok(())
    }

    pub(super) fn publish(
        &mut self,
        end_offset: u64,
        now: u64,
        progress: &Progress,
        sources: &[SourceProgress],
        measure: bool,
    ) {
        let mut latency = measure.then(|| progress.accepted_to_committed.lock().unwrap());
        for entry in &self.entries {
            if let Some(histogram) = &mut latency {
                histogram.record(now.saturating_sub(entry.queued_ns));
            }
            sources[usize::from(entry.id.source)]
                .committed
                .fetch_add(1, Relaxed);
            progress.committed_bytes.fetch_add(entry.size, Relaxed);
        }
        self.first_request += self.entries.len() as u64;
        self.entries.clear();
        self.start_offset = end_offset;
        self.next_offset = end_offset;
        self.first_message_offset = end_offset;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::durability::Submitted;

    #[test]
    fn contiguous_boundary_requires_every_unique_completion() {
        let settings =
            toml::from_str::<Settings>(include_str!("../../../configs/v2-smoke.toml")).unwrap();
        let mut group = Group::new(&settings, 100);
        let mut ids = Vec::new();
        for request_id in 0..3 {
            let frame_id = FrameId {
                source: 0,
                sequence: request_id,
                session_epoch: 1,
            };
            ids.push(frame_id);
            group
                .submit(Submitted {
                    request_id,
                    frame_id,
                    offset: 100 + request_id * 40,
                    size: 9,
                    wire_len: 40,
                    now: 2,
                    queued_ns: 1,
                })
                .unwrap();
        }
        assert!(group.complete(3, ids[0], true).is_err());
        group.complete(2, ids[2], true).unwrap();
        group.complete(0, ids[0], true).unwrap();
        assert!(group.complete(2, ids[2], true).is_err());
        assert!(group.validate_boundary(220, false).is_err());
        group.complete(1, ids[1], true).unwrap();
        assert!(group.validate_boundary(219, false).is_err());
        assert!(group.validate_boundary(220, false).is_ok());
        assert!(group.validate_boundary(270, true).is_ok());
    }
}
