//! Virtual-time examples for the single non-preemptive Copy Worker.

#[derive(Clone, Copy)]
pub struct Task {
    pub slot: usize,
    pub queued_ns: u64,
    pub retry_ns: u64,
    pub due_ns: u64,
    pub work_ns: u64,
    pub return_ns: u64,
    pub forced: bool,
}

impl Task {
    fn completion_due(self) -> u64 {
        self.due_ns.saturating_sub(self.return_ns)
    }
}

pub struct Plan {
    pub next: Option<Task>,
    pub wake_ns: Option<u64>,
    pub work_ns: u64,
    pub earliest_due_ns: Option<u64>,
    pub infeasible: bool,
}

/// EDF on copy-completion limits. The minimum prefix slack gives the last
/// start of the first job in this order for the currently visible batch.
pub fn plan(tasks: &mut [Task], now: u64, blocked_until: u64) -> Plan {
    // Smaller work breaks ties, then older arrivals. Writer FIFO is separate.
    tasks.sort_unstable_by_key(|task| {
        (
            task.completion_due(),
            task.work_ns,
            task.queued_ns,
            task.slot,
        )
    });
    let mut work_ns = 0_u64;
    let mut latest = u64::MAX;
    let mut completion = now.max(blocked_until);
    let mut infeasible = false;
    let mut earliest_due_ns = None;
    for task in tasks.iter() {
        let due = task.completion_due();
        work_ns = work_ns.saturating_add(task.work_ns);
        latest = latest.min(due.saturating_sub(work_ns));
        completion = completion.max(task.retry_ns).saturating_add(task.work_ns);
        if task.due_ns != u64::MAX {
            infeasible |= completion.saturating_add(task.return_ns) > task.due_ns;
            earliest_due_ns = Some(
                earliest_due_ns.map_or(task.due_ns, |previous: u64| previous.min(task.due_ns)),
            );
        }
    }
    let first = tasks.first().copied();
    let forced = tasks.iter().any(|task| task.forced && task.retry_ns <= now);
    let wake_ns = first.map(|task| {
        (if forced { now } else { latest })
            .max(task.retry_ns)
            .max(blocked_until)
    });
    Plan {
        next: first.filter(|_| wake_ns.is_some_and(|wake| wake <= now)),
        wake_ns,
        work_ns,
        earliest_due_ns,
        infeasible,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn individual_latest_start_misses_the_overlapping_deadline() {
        let due = 7_000_000;
        let copy = 2_000_000;
        let ack = 500_000;
        let legacy_start = due - copy - ack;
        assert_eq!(legacy_start, 4_500_000);
        assert_eq!(legacy_start + 3 * copy + ack, 11_000_000);
        assert!(3 * copy + ack <= due);
        assert!(4 * copy + ack > due);
    }
}

#[cfg(test)]
mod global_tests {
    use super::*;

    fn task(slot: usize, due_ns: u64, work_ns: u64, return_ns: u64) -> Task {
        Task {
            slot,
            queued_ns: slot as u64,
            retry_ns: 0,
            due_ns,
            work_ns,
            return_ns,
            forced: false,
        }
    }

    #[test]
    fn m1_common_deadline_advances_the_first_copy_and_detects_impossible_work() {
        let mut tasks = [task(0, 7_000_000, 2_000_000, 500_000); 3];
        tasks[1].slot = 1; // Two streams can contribute three pending frames.
        tasks[2].slot = 2;
        let start = plan(&mut tasks, 0, 0).wake_ns.unwrap();
        assert_eq!(start, 500_000);
        assert!(!plan(&mut tasks, start, 0).infeasible);
        assert_eq!(plan(&mut tasks, start, 0).next.unwrap().slot, 0);
        assert!(plan(&mut tasks, 4_500_000, 0).infeasible);
        assert!(plan(&mut [tasks[0]; 4], 0, 0).infeasible);
    }

    #[test]
    fn m2_eight_equal_deadlines_have_stable_order_and_complete() {
        let mut tasks: Vec<_> = (0..8).rev().map(|id| task(id, 100, 10, 5)).collect();
        let mut now = 15;
        for id in 0..8 {
            let decision = plan(&mut tasks, now, now);
            assert!(!decision.infeasible);
            assert_eq!(decision.next.unwrap().slot, id);
            tasks.remove(0);
            now += 10;
        }
        assert!(plan(&mut tasks, now, now).next.is_none());
    }

    #[test]
    fn m3_staggered_bursts_and_ack_delays_change_the_completion_order() {
        let mut tasks = [task(0, 26, 4, 1), task(1, 30, 8, 12)];
        assert_eq!(plan(&mut tasks, 10, 0).next.unwrap().slot, 1);
        let decision = plan(&mut tasks, 10, 16);
        assert!(decision.infeasible);
        assert!(decision.next.is_none());
        assert_eq!(decision.wake_ns, Some(16));
        assert_eq!(decision.work_ns, 12);
    }

    #[test]
    fn m5_a_long_running_copy_blocks_the_short_job_and_edf_with_release_times_is_not_optimal() {
        let short = task(0, 11, 2, 0);
        assert!(plan(&mut [short], 5, 10).infeasible);

        let mut short = short;
        short.retry_ns = 8;
        let long = task(1, 12, 7, 0);
        // EDF waits for short at 8, then finishes long at 17. Starting long
        // at zero instead completes long at 7 and short at 10.
        assert!(plan(&mut [long, short], 0, 0).infeasible);
        assert!(long.work_ns <= short.retry_ns);
        assert!(short.retry_ns + short.work_ns <= short.due_ns);
    }

    #[test]
    fn m10_equal_deadline_ties_use_oldest_after_equal_cost() {
        let mut older = task(6, 30, 10, 0);
        older.queued_ns = 0;
        let newer = task(1, 30, 10, 0);
        assert_eq!(plan(&mut [newer, older], 10, 0).next.unwrap().slot, 6);
    }
}
