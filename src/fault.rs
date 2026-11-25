use std::thread;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::metrics::ns;

const STOP_POLL: Duration = Duration::from_millis(20);

pub fn sleep_until(until: Instant, stopped: impl Fn() -> bool) {
    while !stopped() {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        thread::sleep(left.min(STOP_POLL));
    }
}

// Windows are relative to the same absolute start as the input schedule.
pub struct Stalls {
    start: Instant,
    length_ns: u64,
    single_ns: Option<u64>,
    period_ns: Option<u64>,
    next_periodic_ns: Option<u64>,
    pub pauses: u64,
    pub paused_ns: u64,
}

impl Stalls {
    pub fn new(config: &Config, start: Instant) -> Self {
        let period_ns = config.stall_period_ms.map(|ms| ms * 1_000_000);
        Self {
            start,
            length_ns: config.stall_duration_ms.unwrap_or_default() * 1_000_000,
            single_ns: config.stall_at_ms.map(|ms| ms * 1_000_000),
            period_ns,
            next_periodic_ns: period_ns,
            pauses: 0,
            paused_ns: 0,
        }
    }

    pub fn next(&self) -> Option<Instant> {
        self.single_ns
            .into_iter()
            .chain(self.next_periodic_ns)
            .min()
            .map(|offset| self.start + Duration::from_nanos(offset))
    }

    fn active_end(&mut self, now_ns: u64) -> u64 {
        let mut end = now_ns;
        if let Some(single) = self.single_ns
            && single <= now_ns
        {
            end = end.max(single.saturating_add(self.length_ns));
            self.single_ns = None;
        }
        if let Some(next) = self.next_periodic_ns
            && next <= now_ns
        {
            let period = self.period_ns.unwrap();
            // Skip stale windows if a synchronous write already occupied the thread.
            let latest = now_ns - (now_ns % period);
            end = end.max(latest.saturating_add(self.length_ns));
            self.next_periodic_ns = latest.checked_add(period);
        }
        end
    }

    pub fn wait(&mut self, stopped: impl Fn() -> bool) {
        let mut end = 0;
        while !stopped() {
            let now = Instant::now();
            let elapsed_ns = ns(now.saturating_duration_since(self.start));
            end = end.max(self.active_end(elapsed_ns));
            if end <= elapsed_ns {
                break;
            }
            self.pauses += 1;
            // Check the next window boundary too, so overlapping windows are honored.
            let until = self.start + Duration::from_nanos(end);
            let until = self.next().map_or(until, |next| until.min(next));
            sleep_until(until, &stopped);
            self.paused_ns += ns(now.elapsed());
        }
    }

    pub fn pace(&mut self, until: Instant, stopped: impl Fn() -> bool) {
        while !stopped() && Instant::now() < until {
            let next = self.next().map_or(until, |next| until.min(next));
            sleep_until(next, &stopped);
            self.wait(&stopped);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_windows_skip_stale_intervals_and_keep_the_current_one() {
        let mut stalls = Stalls {
            start: Instant::now(),
            length_ns: 25,
            single_ns: Some(180),
            period_ns: Some(100),
            next_periodic_ns: Some(100),
            pauses: 0,
            paused_ns: 0,
        };
        assert_eq!(stalls.active_end(99), 99);
        assert_eq!(stalls.active_end(198), 205);
        assert_eq!(stalls.active_end(205), 225);
        assert_eq!(stalls.active_end(351), 351);
        assert_eq!(stalls.next_periodic_ns, Some(400));
    }
}
