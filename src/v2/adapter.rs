use std::sync::atomic::{
    AtomicU64,
    Ordering::{Acquire, Release},
};
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

use super::config::{MS, Model, ReplacementMode, Source, US};
use super::pool::{Lease, Levels, Pool, ReturnPolicy};
use super::{Clock, arrival::Schedule};
use crate::metrics::ns;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct FrameId {
    pub source: u16,
    pub session_epoch: u64,
    pub sequence: u64,
}

pub struct Frame {
    pub id: FrameId,
    pub planned_ns: u64,
    pub captured_ns: u64,
    pub queued_ns: u64,
    pub payload: Payload,
    pub size: usize,
}

pub enum Payload {
    Borrowed(Lease),
    Owned(Lease),
}

impl Frame {
    pub fn bytes(&self) -> &[u8] {
        let lease = match &self.payload {
            Payload::Borrowed(lease) | Payload::Owned(lease) => lease,
        };
        &lease.bytes()[..self.size]
    }

    pub fn borrowed(&self) -> Option<&Lease> {
        match &self.payload {
            Payload::Borrowed(lease) => Some(lease),
            Payload::Owned(_) => None,
        }
    }
}

#[derive(Serialize)]
pub struct Capabilities {
    pub stream: u16,
    pub pool_id: usize,
    pub reclaim_model: Model,
    pub replacement: ReplacementMode,
    pub sequence_observable: bool,
    pub max_payload_bytes: usize,
    #[serde(skip_serializing_if = "super::mock::Kind::is_synthetic")]
    pub kind: super::mock::Kind,
}

impl Capabilities {
    pub fn configured(source: &Source, stream: u16) -> Self {
        Self {
            stream,
            pool_id: usize::from(stream),
            reclaim_model: source.model,
            replacement: source.replacement(),
            sequence_observable: true,
            max_payload_bytes: source.frame_bytes,
            kind: source.kind,
        }
    }
}

pub enum Acquisition {
    SourceDrop {
        sequence: u64,
    },
    Ready {
        frame: Frame,
        free_after_acquire: usize,
        generation_ns: u64,
    },
    Missed {
        start: u64,
        end: u64,
        device_starved: bool,
    },
}

pub trait SourceAdapter {
    fn capabilities(&self) -> Capabilities;
    fn next_event_ns(&self) -> Option<u64>;
    fn drain(&mut self, wait_until: &mut dyn FnMut(u64));
    fn try_acquire(&mut self, now_ns: u64) -> Option<Acquisition>;
    fn pool_status(&self, now_ns: u64) -> Option<Levels>;
}

pub struct Device {
    pub source: Source,
    pub stream: u16,
    pub schedule: Schedule,
    pub pool: Pool,
    pub next: AtomicU64,
}

impl Device {
    pub fn new(source: &Source, stream: u16, schedule: Schedule, clock: Clock) -> Result<Self> {
        let pool = Pool::new(
            usize::from(stream),
            source.dma_buffers,
            source.reserve_buffers,
            source.frame_bytes,
            clock,
            ReturnPolicy {
                delay_ns: source.return_delay_us * US,
                deadline_ns: source.max_hold_ms.map(|ms| ms * MS),
                replacement: source.replacement(),
            },
        )?;
        Ok(Self {
            source: source.clone(),
            stream,
            schedule,
            pool,
            next: AtomicU64::new(0),
        })
    }
}

// The synthetic adapter reports ready only after fill_for has initialized the
// entire payload. A future device adapter would complete its producer fence here.
pub struct ScheduledSource<'a> {
    pub device: &'a Device,
    pub epoch: u64,
    pub seed: u64,
    pending: Option<PendingFence>,
}

struct PendingFence {
    ready_ns: u64,
    captured_ns: u64,
    lease: Lease,
    free: usize,
}

impl<'a> ScheduledSource<'a> {
    pub fn new(device: &'a Device, epoch: u64, seed: u64) -> Self {
        Self {
            device,
            epoch,
            seed,
            pending: None,
        }
    }
}

impl SourceAdapter for ScheduledSource<'_> {
    fn next_event_ns(&self) -> Option<u64> {
        self.pending
            .as_ref()
            .map(|pending| pending.ready_ns)
            .or_else(|| self.device.schedule.time(self.device.next.load(Acquire)))
    }

    fn drain(&mut self, wait_until: &mut dyn FnMut(u64)) {
        if let Some(pending) = self.pending.take() {
            wait_until(pending.ready_ns);
            drop(pending);
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::configured(&self.device.source, self.device.stream)
    }

    fn try_acquire(&mut self, now_ns: u64) -> Option<Acquisition> {
        let device = self.device;
        let sequence = device.next.load(Acquire);
        let planned_ns = device.schedule.time(sequence)?;
        if planned_ns > now_ns {
            return None;
        }
        let (mut lease, free_after_acquire, captured_ns) = if let Some(pending) = &self.pending {
            if pending.ready_ns > now_ns {
                return None;
            }
            let pending = self.pending.take().unwrap();
            (pending.lease, pending.free, pending.captured_ns)
        } else {
            let latest = device.schedule.latest_group_start(now_ns).unwrap();
            if latest > sequence {
                device.next.store(latest, Release);
                return Some(Acquisition::Missed {
                    start: sequence,
                    end: latest,
                    device_starved: false,
                });
            }
            if device
                .source
                .drop_every
                .is_some_and(|n| sequence % n == n - 1)
            {
                device.next.store(sequence + 1, Release);
                return Some(Acquisition::SourceDrop { sequence });
            }
            let Some((lease, free)) = device.pool.acquire(now_ns) else {
                device.next.store(sequence + 1, Release);
                return Some(Acquisition::Missed {
                    start: sequence,
                    end: sequence + 1,
                    device_starved: true,
                });
            };
            if device.source.fence_delay_us != 0 {
                self.pending = Some(PendingFence {
                    ready_ns: now_ns.saturating_add(device.source.fence_delay_us * US),
                    captured_ns: now_ns,
                    lease,
                    free,
                });
                return None;
            }
            (lease, free, now_ns)
        };
        device.next.store(sequence + 1, Release);
        let generation = Instant::now();
        device
            .source
            .kind
            .fill(lease.bytes_mut(), self.seed, device.stream, sequence);
        let generation_ns = ns(generation.elapsed());
        let payload = if device.source.model == Model::Owned {
            Payload::Owned(lease)
        } else {
            Payload::Borrowed(lease)
        };
        Some(Acquisition::Ready {
            frame: Frame {
                id: FrameId {
                    source: device.stream,
                    session_epoch: self.epoch,
                    sequence,
                },
                planned_ns,
                captured_ns,
                queued_ns: now_ns,
                payload,
                size: device.source.frame_bytes,
            },
            free_after_acquire,
            generation_ns,
        })
    }

    fn pool_status(&self, now_ns: u64) -> Option<Levels> {
        Some(self.device.pool.levels(now_ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::arrival::schedules;
    use crate::v2::config::Settings;

    #[test]
    fn virtual_fence_holds_the_device_buffer_until_ready_then_awaits_return() {
        let settings: Settings =
            toml::from_str(include_str!("../../configs/v3-smoke.toml")).unwrap();
        for (index, source) in settings
            .sources
            .iter()
            .enumerate()
            .filter(|(_, s)| s.fence_delay_us > 0)
        {
            let schedule = schedules(&settings, 100 * MS).unwrap().swap_remove(index);
            let device = Device::new(source, index as u16, schedule, Clock::default()).unwrap();
            let mut adapter = ScheduledSource::new(&device, 1000, 1);
            let adapter: &mut dyn SourceAdapter = &mut adapter;
            let due = adapter.next_event_ns().unwrap();
            assert!(adapter.try_acquire(due).is_none());
            let ready = adapter.next_event_ns().unwrap();
            assert_eq!(ready - due, source.fence_delay_us * US);
            assert_eq!(adapter.pool_status(due).unwrap().held, 1);
            assert!(adapter.try_acquire(ready - 1).is_none());
            let Some(Acquisition::Ready { frame, .. }) = adapter.try_acquire(ready) else {
                panic!("missing completed frame");
            };
            assert_eq!(frame.captured_ns, due);
            assert!(frame.borrowed().is_some());
            assert_eq!(frame.bytes().len(), source.frame_bytes);
            drop(frame);
            let levels = adapter.pool_status(0).unwrap();
            assert_eq!(levels.held, 0);
            assert_eq!(levels.returning, 1);
            assert_eq!(adapter.pool_status(ready).unwrap().returning, 0);

            let due = adapter.next_event_ns().unwrap();
            assert!(adapter.try_acquire(due).is_none());
            adapter.drain(&mut |at| {
                assert_eq!(at, due + source.fence_delay_us * US);
            });
            assert_eq!(adapter.pool_status(due + MS).unwrap().held, 0);
        }
    }

    #[test]
    fn exhausted_mock_pool_does_not_reuse_a_live_lease() {
        let mut settings: Settings =
            toml::from_str(include_str!("../../configs/v3-smoke.toml")).unwrap();
        let source = settings
            .sources
            .iter_mut()
            .find(|s| s.id == "v4l2")
            .unwrap();
        source.dma_buffers = 1;
        source.capture_reserve = 0;
        let schedule = Schedule::new(source, 100 * MS, 1).unwrap();
        let device = Device::new(source, 0, schedule, Clock::default()).unwrap();
        let mut adapter = ScheduledSource::new(&device, 1000, 1);
        let due = adapter.next_event_ns().unwrap();
        assert!(adapter.try_acquire(due).is_none());
        let Some(Acquisition::Ready { frame, .. }) =
            adapter.try_acquire(adapter.next_event_ns().unwrap())
        else {
            panic!("first frame must be ready");
        };
        let first_id = frame.borrowed().unwrap().id;
        assert!(matches!(
            adapter.try_acquire(adapter.next_event_ns().unwrap()),
            Some(Acquisition::Missed {
                start: 1,
                end: 2,
                device_starved: true
            })
        ));
        assert_eq!(frame.borrowed().unwrap().id, first_id);
        drop(frame);

        assert!(
            adapter
                .try_acquire(adapter.next_event_ns().unwrap())
                .is_none()
        );
        let Some(Acquisition::Ready { frame, .. }) =
            adapter.try_acquire(adapter.next_event_ns().unwrap())
        else {
            panic!("released slot should become available");
        };
        let id = frame.borrowed().unwrap().id;
        assert_eq!(id.buffer, first_id.buffer);
        assert!(id.generation > first_id.generation);
    }
}
