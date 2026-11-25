use anyhow::{Context, Result};

use super::config::{NS, Source, US};

#[derive(Clone, Debug)]
pub struct Schedule {
    fps: u64,
    burst: u64,
    phase_ns: u64,
    jitter_ns: u64,
    salt: u64,
    pub expected: u64,
}

impl Schedule {
    pub fn new(source: &Source, duration_ns: u64, salt: u64) -> Result<Self> {
        let mut schedule = Self {
            fps: source.fps,
            burst: source.burst,
            phase_ns: source.phase_us * US,
            jitter_ns: source.jitter_us * US,
            salt,
            expected: 0,
        };
        let remaining = duration_ns.saturating_sub(schedule.phase_ns);
        let mut groups = u64::try_from(
            (u128::from(remaining) * u128::from(schedule.fps)).div_ceil(u128::from(NS)),
        )?;
        if groups != 0 && schedule.group_time(groups - 1) >= duration_ns {
            groups -= 1;
        }
        schedule.expected = groups
            .checked_mul(schedule.burst)
            .context("scheduled frame count overflow")?;
        Ok(schedule)
    }

    fn group_time(&self, group: u64) -> u64 {
        let jitter = if self.jitter_ns == 0 {
            0
        } else {
            let mut z = group
                .wrapping_add(self.salt)
                .wrapping_add(0x9e37_79b9_7f4a_7c15);
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) % (self.jitter_ns + 1)
        };
        self.phase_ns
            .saturating_add(crate::source::slot_time_ns(group, self.fps))
            .saturating_add(jitter)
    }

    pub fn time(&self, sequence: u64) -> Option<u64> {
        (sequence < self.expected).then(|| self.group_time(sequence / self.burst))
    }

    pub fn latest_group_start(&self, now: u64) -> Option<u64> {
        if self.expected == 0 || now < self.group_time(0) {
            return None;
        }
        let max_group = self.expected / self.burst - 1;
        let mut group = crate::source::current_slot(now - self.phase_ns, self.fps).min(max_group);
        if self.group_time(group) > now {
            group -= 1;
        }
        Some(group * self.burst)
    }

    pub fn requirement(
        &self,
        next: u64,
        available: usize,
        reserve: usize,
        now: u64,
    ) -> Option<Requirement> {
        let allowed = available.saturating_sub(reserve);
        if next >= self.expected {
            return None;
        }
        let arrived = self
            .latest_group_start(now)
            .map_or(next, |group| {
                group
                    .saturating_add(self.burst)
                    .min(self.expected)
                    .max(next)
            })
            .saturating_sub(next);
        let deficit = reserve.saturating_sub(available);
        if deficit != 0 {
            return Some(Requirement {
                deadline_ns: now,
                count: deficit.saturating_add(arrived as usize),
            });
        }
        let target = next.saturating_add(allowed as u64);
        let deadline_ns = self.time(target)?;
        let deadline_group_end = (target / self.burst + 1).saturating_mul(self.burst);
        let count = deadline_group_end
            .saturating_sub(target)
            .max(arrived.saturating_sub(allowed as u64));
        Some(Requirement {
            deadline_ns,
            count: count as usize,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Requirement {
    pub deadline_ns: u64,
    pub count: usize,
}

pub fn schedules(settings: &super::config::Settings, duration_ns: u64) -> Result<Vec<Schedule>> {
    settings
        .sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            Schedule::new(source, duration_ns, settings.record.seed ^ index as u64)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_requirement_counts_every_arrival_at_the_deadline() {
        let source: Source = toml::from_str(
            r#"
id = "a"
model = "pool"
frame_bytes = 128
fps = 100
burst = 3
phase_us = 1000
"#,
        )
        .unwrap();
        let schedule = Schedule::new(&source, 31_000_000, 0).unwrap();
        assert_eq!(schedule.expected, 9);
        assert_eq!(
            schedule.requirement(0, 5, 1, 0),
            Some(Requirement {
                deadline_ns: 11_000_000,
                count: 2
            })
        );
        assert_eq!(
            schedule.requirement(1, 1, 1, 1_000_000),
            Some(Requirement {
                deadline_ns: 1_000_000,
                count: 2
            })
        );
        assert_eq!(
            schedule.requirement(8, 0, 1, 30_000_000),
            Some(Requirement {
                deadline_ns: 30_000_000,
                count: 2
            })
        );
        assert_eq!(schedule.requirement(9, 0, 1, 30_000_000), None);
    }
}
