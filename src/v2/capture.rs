use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use super::adapter::{Acquisition, Device, SourceAdapter};
use super::config::{Policy, Settings, US};
use super::integrity::{Layer, Ledger, Reason, Span};
use super::metrics::{Progress, SourceProgress, Warning};
use super::queue::{Job, Queue};
use crate::fault::sleep_until;
use crate::metrics::ns;

pub(super) struct Capture<'a> {
    pub(super) start: Instant,
    pub(super) duration: u64,
    pub(super) settings: &'a Settings,
    pub(super) devices: &'a [Device],
    pub(super) sources: &'a [SourceProgress],
    pub(super) queue: &'a Queue,
    pub(super) progress: &'a Progress,
}

impl Capture<'_> {
    pub(super) fn run(&self, mut adapters: Vec<impl SourceAdapter>, mut ledger: Ledger<'_>) {
        let mut heap = BinaryHeap::with_capacity(adapters.len());
        let mut turn = 0_u64;
        let mut last_group = vec![(u64::MAX, 0_u64); adapters.len()];
        for (i, adapter) in adapters.iter().enumerate() {
            if let Some(at) = adapter.next_event_ns() {
                heap.push(Reverse((at, turn, i)));
                turn += 1;
            }
        }
        let stop = || self.progress.stopped() || crate::stop::REQUESTED.load(Relaxed);
        while let Some(Reverse((at, _, index))) = heap.pop() {
            sleep_until(
                self.start + Duration::from_nanos(at.min(self.duration)),
                stop,
            );
            let now = ns(self.start.elapsed());
            if stop() || now >= self.duration {
                break;
            }
            let adapter = &mut adapters[index];
            let device = &self.devices[index];
            let source = &self.sources[index];
            let event = adapter.try_acquire(now);
            match event {
                Some(Acquisition::Ready {
                    mut frame,
                    free_after_acquire,
                    generation_ns,
                }) => {
                    source.generated.fetch_add(1, Relaxed);
                    self.progress
                        .fill_time
                        .lock()
                        .unwrap()
                        .record(generation_ns);
                    let delay = now - frame.planned_ns;
                    source.lateness.lock().unwrap().record(delay);
                    if self
                        .settings
                        .copy_profile
                        .max_capture_delay_us
                        .is_some_and(|bound| delay > bound * US)
                    {
                        source.envelope_exceeded.fetch_add(1, Relaxed);
                        self.progress.warn(Warning::ContractViolation, now);
                    }
                    let group = &mut last_group[index];
                    if group.0 != frame.planned_ns {
                        *group = (frame.planned_ns, 0);
                    }
                    group.1 += 1;
                    source.max_burst.fetch_max(group.1, Relaxed);
                    let force_copy = frame.borrowed().is_some()
                        && match self.settings.record.policy {
                            Policy::AlwaysCopy => true,
                            Policy::V1Threshold => {
                                free_after_acquire <= device.source.capture_reserve
                            }
                            _ => false,
                        };
                    let span = Span::frame(frame.id);
                    frame.queued_ns = ns(self.start.elapsed());
                    if self.queue.push(Job {
                        frame,
                        force_copy,
                        retry_copy_ns: 0,
                    }) {
                        source.accepted.fetch_add(1, Relaxed);
                    } else {
                        ledger.loss(span, now, Layer::Acquisition, Reason::QueueFull, false);
                    }
                }
                Some(Acquisition::Missed {
                    start,
                    end,
                    device_starved,
                }) => {
                    let (layer, reason) = if device_starved {
                        (Layer::Acquisition, Reason::DmaPoolExhausted)
                    } else {
                        source.envelope_exceeded.fetch_add(end - start, Relaxed);
                        self.progress.warn(Warning::ContractViolation, now);
                        (Layer::Source, Reason::SourceDeadlineMiss)
                    };
                    ledger.loss(
                        Span {
                            stream: device.stream,
                            start,
                            end,
                        },
                        now,
                        layer,
                        reason,
                        false,
                    );
                }
                Some(Acquisition::SourceDrop { sequence }) => ledger.loss(
                    Span {
                        stream: device.stream,
                        start: sequence,
                        end: sequence + 1,
                    },
                    now,
                    Layer::Source,
                    Reason::SourceReportedDrop,
                    false,
                ),
                None => {}
            }
            if let Some(next) = adapter.next_event_ns() {
                heap.push(Reverse((next, turn, index)));
                turn = turn.wrapping_add(1);
            }
        }
        sleep_until(self.start + Duration::from_nanos(self.duration), stop);
        for adapter in &mut adapters {
            adapter.drain(&mut |at| sleep_until(self.start + Duration::from_nanos(at), || false));
        }
        let now = ns(self.start.elapsed());
        for device in self.devices {
            let next = device.next.load(Relaxed);
            if next < device.schedule.expected {
                if stop() {
                    ledger.loss(
                        Span {
                            stream: device.stream,
                            start: next,
                            end: device.schedule.expected,
                        },
                        now,
                        Layer::Source,
                        Reason::Unknown,
                        false,
                    );
                } else {
                    self.progress.warn(Warning::ContractViolation, now);
                    ledger.loss(
                        Span {
                            stream: device.stream,
                            start: next,
                            end: device.schedule.expected,
                        },
                        now,
                        Layer::Source,
                        Reason::SourceDeadlineMiss,
                        false,
                    );
                }
            }
        }
        self.queue.close();
    }
}
