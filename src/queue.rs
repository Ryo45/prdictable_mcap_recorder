use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use anyhow::Result;

pub struct Queue<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
    capacity: usize,
}

struct State<T> {
    items: VecDeque<T>,
    closed_at: Option<Instant>,
    peak: usize,
}

pub enum Pop<T> {
    Item(T),
    Closed(Instant),
    Timeout,
}

impl<T> Queue<T> {
    pub fn new(capacity: usize) -> Result<Self> {
        let mut items = VecDeque::new();
        items.try_reserve_exact(capacity)?;
        Ok(Self {
            state: Mutex::new(State {
                items,
                closed_at: None,
                peak: 0,
            }),
            ready: Condvar::new(),
            capacity,
        })
    }

    pub fn try_push(&self, item: T) -> std::result::Result<(), T> {
        let mut state = self.state.lock().unwrap();
        if state.closed_at.is_some() || state.items.len() == self.capacity {
            return Err(item);
        }
        state.items.push_back(item);
        state.peak = state.peak.max(state.items.len());
        self.ready.notify_one();
        Ok(())
    }

    pub fn pop_until(&self, deadline: Option<Instant>) -> Pop<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if state.items.is_empty()
                && let Some(closed_at) = state.closed_at
            {
                return Pop::Closed(closed_at);
            }
            if let Some(deadline) = deadline
                && Instant::now() >= deadline
            {
                return Pop::Timeout;
            }
            if let Some(item) = state.items.pop_front() {
                return Pop::Item(item);
            }
            state = match deadline {
                Some(deadline) => {
                    self.ready
                        .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                        .unwrap()
                        .0
                }
                None => self.ready.wait(state).unwrap(),
            };
        }
    }

    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed_at.get_or_insert_with(Instant::now);
        self.ready.notify_all();
    }

    pub fn levels(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.items.len(), state.peak)
    }
}
