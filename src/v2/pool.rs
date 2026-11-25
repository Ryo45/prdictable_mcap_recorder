use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

use super::Clock;
use super::config::ReplacementMode;
use crate::metrics::{Histogram, Latency};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferId {
    pub pool: usize,
    pub buffer: usize,
    pub generation: u64,
}

struct Slot {
    id: usize,
    bytes: Box<[u8]>,
}

#[derive(Clone, Copy)]
enum Phase {
    Available,
    Reserve,
    Held { swapped: bool },
    Returning,
}

struct Mark {
    generation: u64,
    phase: Phase,
    deadline_ns: Option<u64>,
}

struct Return {
    slot: Slot,
    ready_ns: u64,
    acquired_ns: u64,
    deadline_ns: Option<u64>,
    swapped: bool,
}

struct State {
    available: Vec<Slot>,
    reserve: Vec<Slot>,
    returns: Vec<Return>,
    marks: Vec<Mark>,
    held: usize,
    peak_held: usize,
    min_available: usize,
    empty_since_ns: Option<u64>,
    max_empty_ns: u64,
    activations: u64,
    deadline_exceeded: u64,
    max_overrun_ns: u64,
    hold: Histogram,
    return_latency: Histogram,
}

struct Inner {
    state: Mutex<State>,
    id: usize,
    size: usize,
    clock: Clock,
    return_delay_ns: u64,
    hold_ns: Option<u64>,
    replacement: ReplacementMode,
}

#[derive(Clone)]
pub struct Pool(Arc<Inner>);

pub struct Lease {
    slot: Option<Slot>,
    pool: Pool,
    pub id: BufferId,
    acquired_ns: u64,
    pub deadline_ns: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Levels {
    pub total: usize,
    pub available: usize,
    pub held: usize,
    pub returning: usize,
    pub reserve: usize,
    pub peak_held: usize,
    pub min_available: usize,
    pub max_empty_ns: u64,
    pub activations: u64,
    pub deadline_exceeded: u64,
    pub max_overrun_ns: u64,
    pub earliest_deadline_ns: Option<u64>,
}

#[derive(Serialize)]
pub struct Report {
    #[serde(flatten)]
    pub levels: Levels,
    pub hold_ns: Latency,
    pub return_latency_ns: Latency,
}

pub struct ReturnPolicy {
    pub delay_ns: u64,
    pub deadline_ns: Option<u64>,
    pub replacement: ReplacementMode,
}

impl Default for ReturnPolicy {
    fn default() -> Self {
        Self {
            delay_ns: 0,
            deadline_ns: None,
            replacement: ReplacementMode::Unsupported,
        }
    }
}

impl Pool {
    pub fn new(
        id: usize,
        base: usize,
        reserve: usize,
        size: usize,
        clock: impl Into<Clock>,
        policy: ReturnPolicy,
    ) -> Result<Self> {
        let total = base + reserve;
        let mut available = Vec::new();
        let mut spare = Vec::new();
        let mut marks = Vec::new();
        let mut returns = Vec::new();
        available.try_reserve_exact(total)?;
        spare.try_reserve_exact(reserve)?;
        marks.try_reserve_exact(total)?;
        returns.try_reserve_exact(total)?;
        for id in 0..total {
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(size)?;
            bytes.resize(size, 0xa5);
            let slot = Slot {
                id,
                bytes: bytes.into_boxed_slice(),
            };
            let phase = if id < base {
                available.push(slot);
                Phase::Available
            } else {
                spare.push(slot);
                Phase::Reserve
            };
            marks.push(Mark {
                generation: 0,
                phase,
                deadline_ns: None,
            });
        }
        Ok(Self(Arc::new(Inner {
            state: Mutex::new(State {
                available,
                reserve: spare,
                returns,
                marks,
                held: 0,
                peak_held: 0,
                min_available: base,
                empty_since_ns: None,
                max_empty_ns: 0,
                activations: 0,
                deadline_exceeded: 0,
                max_overrun_ns: 0,
                hold: Histogram::default(),
                return_latency: Histogram::default(),
            }),
            id,
            size,
            clock: clock.into(),
            return_delay_ns: policy.delay_ns,
            hold_ns: policy.deadline_ns,
            replacement: policy.replacement,
        })))
    }

    pub fn size(&self) -> usize {
        self.0.size
    }

    pub fn acquire(&self, now: u64) -> Option<(Lease, usize)> {
        let mut state = self.0.state.lock().unwrap();
        state.reap(now);
        let slot = state.available.pop()?;
        let mark = &mut state.marks[slot.id];
        assert!(matches!(mark.phase, Phase::Available));
        mark.generation = mark
            .generation
            .checked_add(1)
            .expect("buffer generation exhausted");
        mark.phase = Phase::Held { swapped: false };
        let deadline_ns = self.0.hold_ns.map(|hold| now.saturating_add(hold));
        mark.deadline_ns = deadline_ns;
        let id = BufferId {
            pool: self.0.id,
            buffer: slot.id,
            generation: mark.generation,
        };
        state.held += 1;
        state.peak_held = state.peak_held.max(state.held);
        let available = state.available.len();
        state.min_available = state.min_available.min(available);
        if available == 0 {
            state.empty_since_ns = Some(now);
        }
        let lease = Lease {
            slot: Some(slot),
            pool: self.clone(),
            id,
            acquired_ns: now,
            deadline_ns,
        };
        Some((lease, available))
    }

    pub fn activate(&self, candidate: BufferId, now: u64) -> bool {
        let mut state = self.0.state.lock().unwrap();
        state.reap(now);
        if state.reserve.is_empty() || candidate.pool != self.0.id {
            return false;
        }
        let Some(mark) = state.marks.get_mut(candidate.buffer) else {
            return false;
        };
        if mark.generation != candidate.generation
            || !matches!(mark.phase, Phase::Held { swapped: false })
        {
            return false;
        }
        match self.0.replacement {
            ReplacementMode::Unsupported => return false,
            ReplacementMode::PreRegisteredPool => {}
            ReplacementMode::ExplicitReplacement => mark.phase = Phase::Held { swapped: true },
        }
        let slot = state.reserve.pop().unwrap();
        state.marks[slot.id].phase = Phase::Available;
        state.available.push(slot);
        state.finish_empty(now);
        state.activations += 1;
        true
    }

    pub fn levels(&self, now: u64) -> Levels {
        let mut state = self.0.state.lock().unwrap();
        state.reap(now);
        state.levels(now)
    }

    pub fn scheduled_returns_by(&self, deadline_ns: u64) -> usize {
        let state = self.0.state.lock().unwrap();
        state
            .returns
            .iter()
            .filter(|entry| !entry.swapped && entry.ready_ns <= deadline_ns)
            .count()
    }

    pub fn report(&self, now: u64) -> Report {
        let mut state = self.0.state.lock().unwrap();
        state.reap(now);
        Report {
            levels: state.levels(now),
            hold_ns: state.hold.summary(),
            return_latency_ns: state.return_latency.summary(),
        }
    }
}

impl State {
    fn levels(&self, now: u64) -> Levels {
        let levels = Levels {
            total: self.marks.len(),
            available: self.available.len(),
            held: self.held,
            returning: self.returns.len(),
            reserve: self.reserve.len(),
            peak_held: self.peak_held,
            min_available: self.min_available,
            max_empty_ns: self.max_empty_ns.max(
                self.empty_since_ns
                    .map_or(0, |start| now.saturating_sub(start)),
            ),
            activations: self.activations,
            deadline_exceeded: self.deadline_exceeded,
            max_overrun_ns: self.max_overrun_ns,
            earliest_deadline_ns: self.marks.iter().filter_map(|mark| mark.deadline_ns).min(),
        };
        assert_eq!(
            levels.total,
            levels.available + levels.held + levels.returning + levels.reserve
        );
        levels
    }

    fn finish_empty(&mut self, now: u64) {
        if let Some(start) = self.empty_since_ns.take() {
            self.max_empty_ns = self.max_empty_ns.max(now.saturating_sub(start));
        }
    }

    fn reap(&mut self, now: u64) {
        if self.empty_since_ns.is_some()
            && let Some(ready) = self
                .returns
                .iter()
                .filter(|entry| !entry.swapped && entry.ready_ns <= now)
                .map(|entry| entry.ready_ns)
                .min()
        {
            self.finish_empty(ready);
        }
        let mut index = 0;
        while index < self.returns.len() {
            if self.returns[index].ready_ns <= now {
                let entry = self.returns.swap_remove(index);
                self.complete(entry);
            } else {
                index += 1;
            }
        }
    }

    fn complete(&mut self, entry: Return) {
        self.hold
            .record(entry.ready_ns.saturating_sub(entry.acquired_ns));
        if let Some(deadline) = entry.deadline_ns
            && entry.ready_ns > deadline
        {
            self.deadline_exceeded += 1;
            self.max_overrun_ns = self.max_overrun_ns.max(entry.ready_ns - deadline);
        }
        self.marks[entry.slot.id].deadline_ns = None;
        if entry.swapped {
            self.marks[entry.slot.id].phase = Phase::Reserve;
            self.reserve.push(entry.slot);
        } else {
            self.marks[entry.slot.id].phase = Phase::Available;
            self.available.push(entry.slot);
            self.finish_empty(entry.ready_ns);
        }
    }
}

impl Lease {
    pub fn bytes(&self) -> &[u8] {
        &self.slot.as_ref().unwrap().bytes
    }
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.slot.as_mut().unwrap().bytes
    }

    pub fn release(mut self, now: u64) {
        self.request_return(now);
    }

    fn request_return(&mut self, now: u64) {
        let Some(slot) = self.slot.take() else { return };
        let mut state = self.pool.0.state.lock().unwrap();
        let mark = &mut state.marks[slot.id];
        assert_eq!(mark.generation, self.id.generation);
        let Phase::Held { swapped } = mark.phase else {
            panic!("lease returned twice")
        };
        mark.phase = Phase::Returning;
        state.held -= 1;
        let delay = self.pool.0.return_delay_ns;
        state.return_latency.record(delay);
        let entry = Return {
            slot,
            ready_ns: now.saturating_add(delay),
            acquired_ns: self.acquired_ns,
            deadline_ns: self.deadline_ns,
            swapped,
        };
        if delay == 0 {
            state.complete(entry);
        } else {
            state.returns.push(entry);
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.request_return(self.pool.0.clock.now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postponed_returns_and_generation_are_distinct_from_replacement() {
        let pool = Pool::new(
            2,
            1,
            1,
            128,
            Instant::now(),
            ReturnPolicy {
                delay_ns: 10,
                deadline_ns: Some(30),
                replacement: ReplacementMode::ExplicitReplacement,
            },
        )
        .unwrap();
        let (original, _) = pool.acquire(1).unwrap();
        let old_id = original.id;
        assert!(pool.acquire(2).is_none());
        assert!(pool.activate(original.id, 3));
        let (substitute, _) = pool.acquire(4).unwrap();
        assert_ne!(substitute.id.buffer, original.id.buffer);
        let moved = original;
        moved.release(5);
        let levels = pool.levels(14);
        assert_eq!(
            (
                levels.available,
                levels.held,
                levels.returning,
                levels.reserve
            ),
            (0, 1, 1, 0)
        );
        assert_eq!(pool.levels(15).reserve, 1);
        assert!(!pool.activate(old_id, 15));
        substitute.release(40);
        assert_eq!(pool.levels(49).returning, 1);
        let (reused, _) = pool.acquire(50).unwrap();
        assert_eq!((reused.id.buffer, reused.id.generation), (1, 2));
        reused.release(50);
        assert_eq!(pool.levels(60).held, 0);
        assert_eq!(pool.levels(60).deadline_exceeded, 1);
        assert_eq!(pool.levels(60).max_empty_ns, 46);
    }
}
