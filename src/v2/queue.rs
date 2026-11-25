use std::sync::{Condvar, Mutex};
use std::time::Duration;

use super::adapter::Frame;
use super::arrival::Requirement;
use super::copy_plan::{Plan, Task, plan};

pub struct Job {
    pub frame: Frame,
    pub force_copy: bool,
    pub retry_copy_ns: u64,
}

#[derive(Default)]
struct Ends {
    head: Option<usize>,
    tail: Option<usize>,
}

struct Slot {
    prev: Option<usize>,
    next: Option<usize>,
    stream: usize,
    job: Option<Job>,
    copying: bool,
}

struct State {
    streams: Vec<Ends>,
    slots: Vec<Slot>,
    free: Vec<usize>,
    closed: bool,
    version: u64,
    peak: usize,
    copying: usize,
    round_robin: usize,
    write_claims: u64,
}

#[derive(Clone, Copy)]
pub struct Snapshot {
    pub closed: bool,
    pub version: u64,
    pub depth: usize,
    pub peak: usize,
    pub copying: usize,
    pub write_claims: u64,
}

pub struct Queue {
    state: Mutex<State>,
    changed: Condvar,
}

pub struct CopyClaim {
    pub slot: usize,
    pub job: Job,
    pub deadline_ns: u64,
}

pub struct CopySelection {
    pub claim: Option<CopyClaim>,
    pub next_ns: Option<u64>,
}

pub struct CopyPriority {
    pub ready_ns: u64,
    pub deadline_ns: u64,
}

#[derive(Default)]
pub struct CopyDemand {
    pub pool: Option<Requirement>,
    pub work_ns: u64,
    pub return_ns: u64,
    pool_remaining: usize,
    pub pending_borrowed: u64,
}

impl CopyDemand {
    pub fn set(&mut self, pool: Option<Requirement>, work_ns: u64, return_ns: u64) {
        self.pool = pool;
        self.work_ns = work_ns;
        self.return_ns = return_ns;
    }
}

impl Queue {
    pub fn new(capacity: usize, streams: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| Slot {
                prev: None,
                next: None,
                stream: 0,
                job: None,
                copying: false,
            })
            .collect();
        Self {
            state: Mutex::new(State {
                streams: (0..streams).map(|_| Ends::default()).collect(),
                slots,
                free: (0..capacity).rev().collect(),
                closed: false,
                version: 0,
                peak: 0,
                copying: 0,
                round_robin: 0,
                write_claims: 0,
            }),
            changed: Condvar::new(),
        }
    }

    pub fn push(&self, job: Job) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        let Some(index) = state.free.pop() else {
            return false;
        };
        let stream = usize::from(job.frame.id.source);
        let prev = state.streams[stream].tail;
        state.slots[index] = Slot {
            prev,
            next: None,
            stream,
            job: Some(job),
            copying: false,
        };
        if let Some(prev) = prev {
            state.slots[prev].next = Some(index);
        }
        let ends = &mut state.streams[stream];
        ends.tail = Some(index);
        if prev.is_none() {
            ends.head = Some(index);
        }
        state.peak = state.peak.max(state.depth());
        self.change(&mut state);
        true
    }

    pub fn claim_write(
        &self,
        permit_borrow: bool,
        abort: bool,
        force_copy: impl Fn(&Job) -> bool,
    ) -> Option<Job> {
        let mut state = self.state.lock().unwrap();
        let streams = state.streams.len();
        for turn in 0..streams {
            let stream = (state.round_robin + turn) % streams;
            let Some(index) = state.streams[stream].head else {
                continue;
            };
            let Some(job) = &mut state.slots[index].job else {
                continue;
            };
            if !abort && job.frame.borrowed().is_some() {
                if !job.force_copy && force_copy(job) {
                    job.force_copy = true;
                    self.change(&mut state);
                    continue;
                }
                if job.force_copy || !permit_borrow {
                    continue;
                }
            }
            let result = state.slots[index].job.take();
            state.remove(index);
            state.round_robin = (stream + 1) % streams;
            state.write_claims += 1;
            self.change(&mut state);
            return result;
        }
        None
    }

    pub fn claim_copy(
        &self,
        now: u64,
        priority: impl Fn(&Job) -> Option<CopyPriority>,
    ) -> CopySelection {
        let mut state = self.state.lock().unwrap();
        if state.copying != 0 {
            return CopySelection {
                claim: None,
                next_ns: None,
            };
        }
        let mut next_ns = None;
        let mut best = None;
        for stream in &state.streams {
            let mut next = stream.head;
            while let Some(index) = next {
                let slot = &state.slots[index];
                next = slot.next;
                let Some(job) = &slot.job else { continue };
                if job.frame.borrowed().is_none() {
                    continue;
                }
                let Some(p) = priority(job) else { continue };
                let ready = p.ready_ns.max(job.retry_copy_ns);
                if ready > now {
                    next_ns = Some(next_ns.map_or(ready, |previous: u64| previous.min(ready)));
                } else {
                    let rank = (p.deadline_ns, job.frame.queued_ns, index);
                    if best.is_none_or(|previous| rank < previous) {
                        best = Some(rank);
                    }
                }
            }
        }
        let claim = best.map(|(deadline_ns, _, slot)| {
            let job = state.slots[slot].job.take().unwrap();
            state.slots[slot].copying = true;
            state.copying += 1;
            // A writer can claim another stream while the copy owns this job.
            self.change(&mut state);
            CopyClaim {
                slot,
                job,
                deadline_ns,
            }
        });
        CopySelection { claim, next_ns }
    }

    pub fn claim_global_copy(
        &self,
        now: u64,
        demands: &mut [CopyDemand],
        tasks: &mut Vec<Task>,
    ) -> (Option<CopyClaim>, Plan) {
        let mut state = self.state.lock().unwrap();
        debug_assert_eq!(state.copying, 0, "the caller owns the single copy worker");
        debug_assert!(tasks.capacity() >= state.slots.len());
        tasks.clear();
        for demand in demands.iter_mut() {
            demand.pending_borrowed = 0;
            demand.pool_remaining = demand.pool.map_or(0, |pool| pool.count);
        }
        // A fixed return due by the pool deadline can also satisfy one unit
        // of the pool's demand. Credit it only if the same plan copies it.
        for slot in &state.slots {
            let Some(job) = &slot.job else { continue };
            let Some(lease) = job.frame.borrowed() else {
                continue;
            };
            let demand = &mut demands[slot.stream];
            demand.pending_borrowed += 1;
            if let Some(pool) = demand.pool
                && lease.deadline_ns.is_some_and(|due| due <= pool.deadline_ns)
            {
                demand.pool_remaining = demand.pool_remaining.saturating_sub(1);
            }
        }
        for (stream, demand) in state.streams.iter().zip(demands.iter_mut()) {
            let mut next = stream.head;
            while let Some(index) = next {
                let slot = &state.slots[index];
                next = slot.next;
                let Some(job) = &slot.job else { continue };
                let Some(lease) = job.frame.borrowed() else {
                    continue;
                };
                let mut due_ns = lease.deadline_ns.unwrap_or(u64::MAX);
                if let Some(pool) = demand.pool
                    && demand.pool_remaining != 0
                    && pool.deadline_ns < due_ns
                {
                    demand.pool_remaining -= 1;
                    due_ns = pool.deadline_ns;
                }
                if due_ns != u64::MAX || job.force_copy {
                    tasks.push(Task {
                        slot: index,
                        queued_ns: job.frame.queued_ns,
                        retry_ns: job.retry_copy_ns,
                        due_ns,
                        work_ns: demand.work_ns,
                        return_ns: demand.return_ns,
                        forced: job.force_copy,
                    });
                }
            }
        }
        let mut decision = plan(tasks, now, now);
        for demand in demands.iter() {
            if let Some(pool) = demand.pool
                && demand.pool_remaining > 0
                && pool.deadline_ns <= now
            {
                decision.infeasible = true;
                decision.earliest_due_ns = Some(
                    decision
                        .earliest_due_ns
                        .map_or(pool.deadline_ns, |due| due.min(pool.deadline_ns)),
                );
            }
        }
        let claim = decision.next.map(|task| {
            let slot = &mut state.slots[task.slot];
            let job = slot.job.take().unwrap();
            slot.copying = true;
            state.copying += 1;
            self.change(&mut state);
            CopyClaim {
                slot: task.slot,
                job,
                deadline_ns: task.due_ns,
            }
        });
        (claim, decision)
    }

    pub fn finish_copy(&self, slot: usize, job: Option<Job>) {
        let mut state = self.state.lock().unwrap();
        assert!(state.slots[slot].copying && state.slots[slot].job.is_none());
        state.copying -= 1;
        state.slots[slot].copying = false;
        if let Some(job) = job {
            assert_eq!(usize::from(job.frame.id.source), state.slots[slot].stream);
            state.slots[slot].job = Some(job);
        } else {
            state.remove(slot);
        }
        self.change(&mut state);
    }

    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.change(&mut state);
    }

    pub fn snapshot(&self) -> Snapshot {
        let state = self.state.lock().unwrap();
        Snapshot {
            closed: state.closed,
            version: state.version,
            depth: state.depth(),
            peak: state.peak,
            copying: state.copying,
            write_claims: state.write_claims,
        }
    }

    pub fn wait_change(&self, version: u64, timeout: Duration) {
        let state = self.state.lock().unwrap();
        let _guard = self
            .changed
            .wait_timeout_while(state, timeout, |s| s.version == version)
            .unwrap();
    }

    fn change(&self, state: &mut State) {
        state.version = state.version.wrapping_add(1);
        self.changed.notify_all();
    }
}

impl State {
    fn depth(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    fn remove(&mut self, index: usize) {
        let Slot {
            prev,
            next,
            stream,
            ref job,
            copying,
        } = self.slots[index];
        assert!(job.is_none() && !copying);
        if let Some(prev) = prev {
            self.slots[prev].next = next;
        } else {
            self.streams[stream].head = next;
        }
        if let Some(next) = next {
            self.slots[next].prev = prev;
        } else {
            self.streams[stream].tail = prev;
        }
        self.free.push(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::adapter::{FrameId, Payload};
    use crate::v2::pool::{Pool, ReturnPolicy};
    use std::sync::Barrier;
    use std::time::Instant;

    #[test]
    fn only_one_worker_can_claim_a_pending_payload() {
        let queue = Queue::new(2, 1);
        let pool = Pool::new(0, 1, 0, 32, Instant::now(), ReturnPolicy::default()).unwrap();
        let (lease, _) = pool.acquire(0).unwrap();
        assert!(queue.push(Job {
            frame: Frame {
                id: FrameId {
                    source: 0,
                    session_epoch: 1,
                    sequence: 0
                },
                planned_ns: 0,
                captured_ns: 0,
                queued_ns: 0,
                size: 32,
                payload: Payload::Borrowed(lease)
            },
            force_copy: false,
            retry_copy_ns: 0
        }));
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            let copier = scope.spawn(|| {
                start.wait();
                queue
                    .claim_copy(0, |_| {
                        Some(CopyPriority {
                            ready_ns: 0,
                            deadline_ns: 1,
                        })
                    })
                    .claim
            });
            start.wait();
            let writer = queue.claim_write(true, false, |_| false);
            let copy = copier.join().unwrap();
            assert_ne!(writer.is_some(), copy.is_some());
            assert!(pool.acquire(0).is_none());
            if let Some(copy) = copy {
                queue.finish_copy(copy.slot, Some(copy.job));
                assert!(queue.claim_write(true, false, |_| false).is_some());
            }
            drop(writer);
            assert_eq!(queue.snapshot().depth, 0);
        });
        assert_eq!(pool.levels(u64::MAX).available, 1);
    }
}

#[cfg(test)]
mod copy_budget_tests {
    use super::*;
    use crate::v2::adapter::{FrameId, Payload};
    use crate::v2::pool::{Pool, ReturnPolicy};
    use std::time::Instant;

    #[test]
    fn a_running_copy_is_nonpreemptible_and_does_not_reorder_its_stream() {
        let queue = Queue::new(3, 2);
        let huge = Pool::new(0, 2, 0, 4096, Instant::now(), ReturnPolicy::default()).unwrap();
        let small = Pool::new(1, 1, 0, 64, Instant::now(), ReturnPolicy::default()).unwrap();
        let job = |pool: &Pool, stream, sequence, force_copy| Job {
            frame: Frame {
                id: FrameId {
                    source: stream,
                    session_epoch: 1,
                    sequence,
                },
                planned_ns: 0,
                captured_ns: 0,
                queued_ns: sequence,
                size: pool.size(),
                payload: Payload::Borrowed(pool.acquire(0).unwrap().0),
            },
            force_copy,
            retry_copy_ns: 0,
        };
        assert!(queue.push(job(&huge, 0, 0, false)));
        let priority = |job: &Job| {
            Some(CopyPriority {
                ready_ns: 0,
                deadline_ns: if job.frame.id.source == 1 { 5 } else { 20 },
            })
        };
        let large_copy = queue.claim_copy(0, priority).claim.unwrap();
        assert!(queue.push(job(&huge, 0, 1, false)));
        assert!(queue.push(job(&small, 1, 0, true)));
        assert!(queue.claim_copy(5, priority).claim.is_none());
        assert!(queue.claim_write(true, false, |_| false).is_none());
        assert!(huge.acquire(5).is_none());
        // Copy completion is explicitly handed back in the same FIFO position.
        queue.finish_copy(large_copy.slot, Some(large_copy.job));
        let small_copy = queue.claim_copy(5, priority).claim.unwrap();
        assert_eq!(small_copy.job.frame.id.source, 1);
        for expected in 0..2 {
            let job = queue.claim_write(true, false, |_| false).unwrap();
            assert_eq!(job.frame.id.sequence, expected);
        }
        queue.finish_copy(small_copy.slot, Some(small_copy.job));
        drop(queue.claim_write(true, true, |_| false));
        assert_eq!(huge.levels(u64::MAX).available, 2);
        assert_eq!(small.levels(u64::MAX).available, 1);
    }
}

#[cfg(test)]
mod global_tests {
    use super::*;
    use crate::v2::adapter::{FrameId, Payload};
    use crate::v2::pool::{Pool, ReturnPolicy};
    use std::time::Instant;

    #[test]
    fn m4_m9_pool_demand_is_counted_once_and_writer_owned_lease_is_excluded() {
        let queue = Queue::new(5, 2);
        let fixed = Pool::new(
            0,
            2,
            0,
            32,
            Instant::now(),
            ReturnPolicy {
                deadline_ns: Some(10),
                ..ReturnPolicy::default()
            },
        )
        .unwrap();
        let shared = Pool::new(1, 3, 0, 32, Instant::now(), ReturnPolicy::default()).unwrap();
        for (stream, pool, count) in [(0, &fixed, 2), (1, &shared, 3)] {
            for sequence in 0..count {
                assert!(queue.push(Job {
                    frame: Frame {
                        id: FrameId {
                            source: stream,
                            session_epoch: 0,
                            sequence
                        },
                        planned_ns: 0,
                        captured_ns: 0,
                        queued_ns: 0,
                        size: 32,
                        payload: Payload::Borrowed(pool.acquire(0).unwrap().0),
                    },
                    force_copy: false,
                    retry_copy_ns: 0,
                }));
            }
        }
        let writer = queue.claim_write(true, false, |_| false).unwrap();
        assert_eq!(writer.frame.id.source, 0);
        let mut demands = [CopyDemand::default(), CopyDemand::default()];
        for demand in &mut demands {
            demand.set(
                Some(Requirement {
                    deadline_ns: 12,
                    count: 2,
                }),
                2,
                1,
            );
        }
        let mut tasks = Vec::with_capacity(5);
        let (claim, plan) = queue.claim_global_copy(6, &mut demands, &mut tasks);
        let claim = claim.unwrap();
        assert_eq!(plan.work_ns, 6); // one fixed + two shared, without double-crediting fixed
        assert_eq!(claim.job.frame.id.sequence, 1);
        assert_eq!(demands[0].pending_borrowed, 1);
        assert_eq!(demands[1].pending_borrowed, 3);
        queue.finish_copy(claim.slot, Some(claim.job));
        drop(writer);
        while queue.claim_write(true, true, |_| false).is_some() {}
        assert_eq!(fixed.levels(u64::MAX).available, 2);
        assert_eq!(shared.levels(u64::MAX).available, 3);
    }
}
