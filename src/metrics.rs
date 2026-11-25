use std::fs;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use rustix::time::{ClockId, clock_gettime};
use serde::Serialize;

const SUB_BUCKETS: usize = 16;

pub fn ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub fn cpu_ns(clock: ClockId) -> u64 {
    let time = clock_gettime(clock);
    (time.tv_sec as u64) * 1_000_000_000 + time.tv_nsec as u64
}

pub fn rss_bytes() -> (Option<u64>, Option<u64>) {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    let value = |key: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(key)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
    };
    (value("VmRSS:"), value("VmHWM:"))
}

pub fn rate(bytes: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        0.0
    } else {
        bytes as f64 * 1e9 / elapsed_ns as f64
    }
}

pub struct Histogram {
    buckets: [u64; 64 * SUB_BUCKETS],
    count: u64,
    sum: u128,
    min: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; 64 * SUB_BUCKETS],
            count: 0,
            sum: 0,
            min: u64::MAX,
            max: 0,
        }
    }
}

impl Histogram {
    pub fn record(&mut self, value: u64) {
        let exponent = value.checked_ilog2().unwrap_or(0) as usize;
        let shift = exponent.saturating_sub(4);
        let bucket = (value >> shift) as usize % SUB_BUCKETS;
        // At values below 32, the exponent already identifies the leading bit.
        self.buckets[exponent * SUB_BUCKETS + bucket] += 1;
        self.count += 1;
        self.sum += u128::from(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn percentile(&self, per_mille: u128) -> u64 {
        let target = (u128::from(self.count) * per_mille).div_ceil(1000);
        let mut seen = 0;
        for (index, &count) in self.buckets.iter().enumerate() {
            seen += u128::from(count);
            if seen >= target {
                let exponent = index / SUB_BUCKETS;
                let bucket = index % SUB_BUCKETS;
                let shift = exponent.saturating_sub(4);
                let upper = if exponent < 4 {
                    bucket as u64
                } else {
                    (1_u64 << exponent) | (((bucket as u64) << shift) + ((1 << shift) - 1))
                };
                return upper.clamp(self.min, self.max);
            }
        }
        self.max
    }

    pub fn summary(&self) -> Latency {
        let values = (self.count != 0).then(|| LatencyValues {
            min: self.min,
            mean: self.sum as f64 / self.count as f64,
            p50: self.percentile(500),
            p95: self.percentile(950),
            p99: self.percentile(990),
            p99_9: self.percentile(999),
            max: self.max,
        });
        Latency {
            count: self.count,
            values,
        }
    }
}

#[derive(Serialize)]
pub struct Latency {
    pub count: u64,
    #[serde(flatten)]
    values: Option<LatencyValues>,
}

#[derive(Serialize)]
struct LatencyValues {
    min: u64,
    mean: f64,
    p50: u64,
    p95: u64,
    p99: u64,
    p99_9: u64,
    max: u64,
}

#[derive(Default)]
pub struct Progress {
    pub generated: AtomicU64,
    pub received: AtomicU64,
    pub copied: AtomicU64,
    pub written: AtomicU64,
    pub loss_record_failures: AtomicU64,
    pub losses: [AtomicU64; 6],
}

impl Progress {
    pub fn loss_counts(&self) -> [u64; 6] {
        self.losses.each_ref().map(|counter| counter.load(Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_buckets_are_monotone_and_saturate() {
        let mut hist = Histogram::default();
        for value in 0..100 {
            hist.record(value);
        }
        hist.record(u64::MAX);
        let stats = hist.summary().values.unwrap();
        assert!((50..=51).contains(&stats.p50));
        assert!(stats.p50 < stats.p95);
        assert!(stats.p95 <= stats.p99);
        assert_eq!(stats.p99_9, u64::MAX);
    }
}
