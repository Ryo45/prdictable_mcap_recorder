use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::Serialize;

use crate::metrics::{Histogram, Latency, ns};

#[derive(Clone)]
pub struct Pool(Arc<Inner>);

struct Inner {
    state: Mutex<State>,
    max_hold: Option<Duration>,
}

struct State {
    free: Vec<Slot>,
    deadlines: Vec<Option<Instant>>,
    min_free: usize,
    hold_ns: Histogram,
    deadline_exceeded: u64,
    max_overrun_ns: u64,
}

struct Slot {
    id: usize,
    bytes: Box<[u8]>,
}

// Ownership of the slot moves with the handle. Only Drop can return it.
pub struct Buffer {
    slot: Option<Slot>,
    pool: Pool,
    acquired_at: Instant,
    return_deadline: Option<Instant>,
}

#[derive(Serialize)]
pub struct Levels {
    pub total: usize,
    pub free: usize,
    pub held: usize,
    pub min_free: usize,
    pub earliest_deadline_in_ns: Option<i64>,
}

#[derive(Serialize)]
pub struct PoolReport {
    #[serde(flatten)]
    pub levels: Levels,
    pub hold_ns: Latency,
    pub deadline_exceeded: u64,
    pub max_overrun_ns: u64,
}

impl Pool {
    pub fn new(count: usize, size: usize, max_hold: Option<Duration>) -> Result<Self> {
        let mut free = Vec::new();
        free.try_reserve_exact(count)?;
        let mut deadlines = Vec::new();
        deadlines.try_reserve_exact(count)?;
        deadlines.resize(count, None);
        for id in 0..count {
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(size)?;
            // Nonzero initialization writes all pages, including the final partial page.
            bytes.resize(size, 0xa5);
            free.push(Slot {
                id,
                bytes: bytes.into_boxed_slice(),
            });
        }
        Ok(Self(Arc::new(Inner {
            state: Mutex::new(State {
                free,
                deadlines,
                min_free: count,
                hold_ns: Histogram::default(),
                deadline_exceeded: 0,
                max_overrun_ns: 0,
            }),
            max_hold,
        })))
    }

    pub fn acquire(&self) -> Option<(Buffer, usize)> {
        let mut state = self.0.state.lock().unwrap();
        let slot = state.free.pop()?;
        let acquired_at = Instant::now();
        let return_deadline = self.0.max_hold.map(|hold| acquired_at + hold);
        state.deadlines[slot.id] = return_deadline;
        let free = state.free.len();
        state.min_free = state.min_free.min(free);
        Some((
            Buffer {
                slot: Some(slot),
                pool: self.clone(),
                acquired_at,
                return_deadline,
            },
            free,
        ))
    }

    pub fn levels(&self, now: Instant) -> Levels {
        self.0.state.lock().unwrap().levels(now)
    }

    pub fn report(&self, now: Instant) -> PoolReport {
        let state = self.0.state.lock().unwrap();
        PoolReport {
            levels: state.levels(now),
            hold_ns: state.hold_ns.summary(),
            deadline_exceeded: state.deadline_exceeded,
            max_overrun_ns: state.max_overrun_ns,
        }
    }
}

impl State {
    fn levels(&self, now: Instant) -> Levels {
        let free = self.free.len();
        let total = self.deadlines.len();
        let deadline = self.deadlines.iter().flatten().min().map(|&instant| {
            if instant >= now {
                ns(instant - now).min(i64::MAX as u64) as i64
            } else {
                -(ns(now - instant).min(i64::MAX as u64) as i64)
            }
        });
        Levels {
            total,
            free,
            held: total - free,
            min_free: self.min_free,
            earliest_deadline_in_ns: deadline,
        }
    }
}

impl Buffer {
    pub fn bytes(&self) -> &[u8] {
        &self.slot.as_ref().unwrap().bytes
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.slot.as_mut().unwrap().bytes
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let slot = self.slot.take().unwrap();
        let mut state = self.pool.0.state.lock().unwrap();
        let now = Instant::now();
        state.hold_ns.record(ns(now - self.acquired_at));
        if let Some(deadline) = self.return_deadline
            && now > deadline
        {
            state.deadline_exceeded += 1;
            state.max_overrun_ns = state.max_overrun_ns.max(ns(now - deadline));
        }
        state.deadlines[slot.id] = None;
        state.free.push(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moved_lease_returns_same_slot_once() {
        let pool = Pool::new(1, 20, Some(Duration::from_secs(1))).unwrap();
        let (mut lease, free) = pool.acquire().unwrap();
        lease.bytes_mut()[0] = 91;
        assert_eq!(free, 0);
        let moved = lease;
        assert!(pool.acquire().is_none());
        assert_eq!(pool.levels(Instant::now()).held, 1);
        drop(moved);
        let (reacquired, free) = pool.acquire().unwrap();
        assert_eq!(free, 0);
        assert_eq!(reacquired.bytes()[0], 91);
        drop(reacquired);
        let report = pool.report(Instant::now());
        assert_eq!(report.levels.free, report.levels.total);
        assert_eq!(report.hold_ns.count, 2);
    }

    #[test]
    fn expiry_is_recorded_without_revoking_the_lease() {
        let pool = Pool::new(1, 20, Some(Duration::from_secs(1))).unwrap();
        let (mut lease, _) = pool.acquire().unwrap();
        lease.return_deadline = Some(lease.acquired_at - Duration::from_nanos(1));
        assert!(pool.acquire().is_none());
        drop(lease);
        let report = pool.report(Instant::now());
        assert_eq!(report.deadline_exceeded, 1);
        assert!(report.max_overrun_ns > 0);
        assert_eq!(report.levels.free, 1);
    }
}
