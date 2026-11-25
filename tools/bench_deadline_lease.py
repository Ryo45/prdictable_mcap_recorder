#!/usr/bin/env python3
"""Matched Always Copy, Always Borrow, and Deadline-aware experiments."""
import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tomllib
from collections import Counter
from itertools import permutations
from pathlib import Path

from bench_v2 import read, run, save

ROOT = Path(__file__).resolve().parents[1]
POLICIES = ("always-copy", "always-borrow", "deadline-aware")
FIXED_SCENARIOS = ("no-stall", "deadline", "pool-pressure")
MIB = 1 << 20


def config(scenario, policy, seed):
    with (ROOT / "configs/v2-smoke.toml").open("rb") as file:
        settings = tomllib.load(file)
    slots = 32 if scenario == "deadline" else 10
    settings["record"].update(policy=policy, seed=seed, commit_bytes=16 * MIB,
                              commit_interval_ms=200, queue_capacity=48, max_output_bytes=64 * MIB)
    settings["sources"] = [{
        "id": "camera", "kind": "mock_v4l2", "model": "fixed-deadline",
        "frame_bytes": MIB // 4, "fps": 100, "phase_us": 1000,
        "dma_buffers": slots, "capture_reserve": 1, "max_hold_ms": 60,
        "return_delay_us": 250, "fence_delay_us": 100,
    }]
    if scenario == "shared-pool":
        settings["sources"][0]["model"] = "pool"
        del settings["sources"][0]["max_hold_ms"]
    settings["spill"] = [{"frame_bytes": MIB // 4, "buffers": 32}]
    settings["storage_profile"].update(sustained_bytes_per_sec=400_000_000,
                                       max_stall_ms=180, max_sync_ms=60, max_io_us=5000)
    if scenario != "no-stall":
        settings["faults"] = {"stall_at_ms": 115, "stall_duration_ms": 170}
    if scenario == "slow-copy":
        settings["faults"]["copy_delay_us"] = 72_000
    return settings


def run_case(recorder, output, scenario, policy, seed):
    name = f"{scenario}-{seed:02d}-{policy}"
    row = run(str(recorder), output, name, config(scenario, policy, seed),
              duration="0.8", expected="droppable")
    work = output / name
    reasons = Counter()
    with (work / "run-loss.jsonl").open() as journal:
        for line in journal:
            loss = json.loads(line)
            reasons[loss["reason"]] += loss["end"] - loss["start"]
    summary = read(work / "run-summary.json")
    row.update(seed=seed, scenario=scenario, loss_reasons=dict(reasons),
               accepted_to_committed_p99_ns=summary["timings_ns"]["accepted_to_committed"].get("p99"),
               accepted_loss=sum(item["after_accept_loss"] for item in summary["sources"]))
    return row


def evaluate(trials, scenarios):
    by_case = {(row["scenario"], row["seed"], row["policy"]): row for row in trials}
    pairs = []
    for scenario in scenarios:
        seeds = sorted({row["seed"] for row in trials if row["scenario"] == scenario})
        for seed in seeds:
            copy, borrow, adaptive = (by_case[scenario, seed, policy] for policy in POLICIES)
            clean = all(x["committed"] == x["planned"] and x["losses"] == x["deadline_exceeded"] == 0
                        for x in (copy, adaptive))
            if scenario == "no-stall":
                safety = borrow["committed"] == borrow["planned"] and borrow["deadline_exceeded"] == 0
            elif scenario == "deadline":
                safety = borrow["deadline_exceeded"] > 0 and borrow["losses"] == 0
            else:
                expected_deadline = (borrow["deadline_exceeded"] == 0 if scenario == "shared-pool"
                                     else borrow["deadline_exceeded"] > 0)
                safety = expected_deadline and borrow["losses"] > 0 and (
                    borrow["loss_reasons"] == {"dma_pool_exhausted": borrow["losses"]}
                )
            pairs.append({
                "scenario": scenario, "seed": seed,
                "same_payload_budget": len({x["pool_bytes"] for x in (copy, borrow, adaptive)}) == 1,
                "safe_and_complete_with_less_copy": clean and adaptive["copy_bytes"] < copy["copy_bytes"],
                "expected_borrow_outcome": safety,
                "copy_savings_fraction": 1 - adaptive["copy_bytes"] / copy["copy_bytes"],
                "cpu_savings_fraction": 1 - adaptive["cpu_ns"] / copy["cpu_ns"],
            })
    result = {
        "all_independently_verified_and_resources_returned": all(
            x["passed"] and x["committed"] + x["losses"] == x["planned"] for x in trials
        ),
        "matched_checks_pass": all(
            p["same_payload_budget"] and p["safe_and_complete_with_less_copy"]
            and p["expected_borrow_outcome"] for p in pairs
        ),
        "paired_results": pairs,
        "median_cpu_savings_fraction": {
            case: statistics.median(p["cpu_savings_fraction"] for p in pairs if p["scenario"] == case)
            for case in scenarios
        },
    }

    slow = [x for x in trials if x["scenario"] == "slow-copy"]
    if slow:
        result["slow_copy_exposes_limit"] = all(
            x["deadline_exceeded"] > 0 and x["warnings"]["CONTRACT_VIOLATION"]["count"] > 0
            for x in slow
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, default=ROOT / "target/release/recorder")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=12)
    parser.add_argument("--suite", choices=("fixed-deadline", "shared-pool"), default="fixed-deadline")
    args = parser.parse_args()
    if args.repeats not in range(6, 25, 6):
        parser.error("--repeats must be 6, 12, 18, or 24 for balanced policy ordering")
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "suite": args.suite, "duration_seconds": 0.8, "repeats": args.repeats,
        "kernel": platform.release(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    }
    save(output / "environment.json", metadata)
    orders = list(permutations(POLICIES))
    trials = []
    scenarios = FIXED_SCENARIOS if args.suite == "fixed-deadline" else ("shared-pool",)
    for i in range(args.repeats):
        for scenario in scenarios[i % len(scenarios):] + scenarios[:i % len(scenarios)]:
            for policy in orders[i % len(orders)]:
                trials.append(run_case(args.recorder.resolve(), output, scenario, policy, i + 1))
    if args.suite == "fixed-deadline":
        for seed in range(1, 4):
            trials.append(run_case(args.recorder.resolve(), output, "slow-copy", "deadline-aware", seed))
    result = evaluate(trials, scenarios)
    save(output / "comparison.json", {"environment": metadata, "runs": trials, **result})
    ok = (result["all_independently_verified_and_resources_returned"]
          and result["matched_checks_pass"] and result.get("slow_copy_exposes_limit", True))
    print(f"Matched experiment: {'PASS' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
