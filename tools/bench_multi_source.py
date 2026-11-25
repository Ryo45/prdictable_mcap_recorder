#!/usr/bin/env python3
"""Compare the normal recorder and independent verifier with bounded output."""
import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tomllib
from collections import Counter
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from bench_v2 import read, run, save

ROOT = Path(__file__).resolve().parents[1]
MIB = 1 << 20
MODES = ("legacy", "global", "always-copy", "always-borrow")
SSD = tuple(f"H{i}" for i in range(7))


def source_pool(source, **settings):
    source.pop("max_hold_ms", None)
    source.update(model="pool", kind="mock_drive", **settings)


def extend_sources(cfg, count):
    cfg["sources"] = [
        dict(cfg["sources"][i % 2], id=f"camera_{i}") for i in range(count)
    ]


def small_cases(seed):
    def base():
        with (ROOT / "configs/multi-source-ci.toml").open("rb") as file:
            return tomllib.load(file)

    cases = {"M1": base()}
    for case in ("M2", "M3"):
        cfg = cases[case] = base()
        extend_sources(cfg, 8)
        cfg["record"]["queue_capacity"] = 128
        cfg["spill"][0]["buffers"] = 96
        for i, source in enumerate(cfg["sources"]):
            source.update(max_hold_ms=52 if case == "M2" else 80, burst=1)
            if case == "M3":
                source.update(phase_us=1000 + (i * 1731 + seed * 211) % 12000,
                              jitter_us=350 + i * 70, burst=1 + i % 3)
        if case == "M2":
            cfg["faults"]["copy_delay_us"] = 1800

    cfg = cases["M4"] = base()
    source_pool(cfg["sources"][1], dma_buffers=5, capture_reserve=1, fps=60,
                return_delay_us=800, burst=1)
    cfg["sources"][0]["max_hold_ms"] = 30

    cfg = cases["M5"] = base()
    cfg["record"].update(memory_budget_bytes=256 * MIB, max_output_bytes=128 * MIB)
    cfg["sources"][0].update(frame_bytes=4 * MIB, fps=10, max_hold_ms=60, burst=1)
    cfg["sources"][1].update(frame_bytes=MIB // 4, max_hold_ms=35, phase_us=7000)
    cfg["spill"] = [{"frame_bytes": MIB // 4, "buffers": 32},
                    {"frame_bytes": 4 * MIB, "buffers": 8}]

    cfg = cases["M6"] = base()
    for source in cfg["sources"]:
        source_pool(source, dma_buffers=3, fps=100, burst=1, capture_reserve=1,
                    reserve_buffers=2, replacement="pre-registered-pool")

    cfg = cases["M7-full"] = base()
    cfg["spill"][0]["buffers"] = 1
    cfg = cases["M7-class"] = base()
    cfg["spill"][0]["frame_bytes"] = 2048

    cfg = cases["M8"] = base()
    cfg["faults"]["copy_delay_us"] = 25000

    cfg = cases["M9"] = base()
    cfg["record"].update(writer="mock-async", in_flight=4, max_borrowed_in_flight=2)
    cfg["faults"] = {"mock_delay_us": 13000}
    cfg["storage_profile"]["max_io_us"] = 8000
    for source in cfg["sources"]:
        source["max_hold_ms"] = 16

    cfg = cases["M10"] = base()
    cfg["sources"][1].update(fps=5, burst=1, max_hold_ms=110)
    return cases


def high_case(case, arrival):
    with (ROOT / "configs/multi-source-ssd.toml").open("rb") as file:
        cfg = tomllib.load(file)
    if case == "H0":
        del cfg["faults"]
    if case == "H1":
        cfg["faults"]["stall_duration_ms"] = 100
    if case == "H3":
        cfg["faults"]["stall_duration_ms"] = 500
        cfg["record"]["queue_capacity"] = 64
        cfg["spill"][0]["buffers"] = 32
    if case == "H4":
        for i, source in enumerate(cfg["sources"]):
            source.update(frame_bytes=(4 if i < 4 else 1) * MIB,
                          fps=30 if i < 4 else 120,
                          phase_us=i * 800)
        cfg["spill"] = [{"frame_bytes": MIB, "buffers": 80},
                        {"frame_bytes": 4 * MIB, "buffers": 60}]
    if case == "H6":
        cfg["faults"]["copy_delay_us"] = 10000
    if case == "H2":
        arrival = "aligned"
    if arrival != "spread":
        for i, source in enumerate(cfg["sources"]):
            source["phase_us"] = 5000 if arrival == "aligned" else i * 600
            if arrival == "jitter":
                source["jitter_us"] = 2500 + 100 * i
            if arrival == "burst":
                source["fps"] //= 2
                source["burst"] = 2
    return cfg


def estimate(cfg, seconds):
    # Upper bound on all possible offered messages, irrespective of losses.
    return sum((int(seconds * src["fps"]) + 1) * src.get("burst", 1)
               * (src["frame_bytes"] + 31) for src in cfg["sources"]) + 1024 * 1024


def telemetry():
    def sample(path):
        try:
            return path.read_text().strip()
        except OSError:
            return None

    fields = ("MemAvailable", "Cached", "Dirty", "Writeback")
    meminfo = Path("/proc/meminfo").read_text().splitlines()
    return {
        "memory": {key: next((line.split(":", 1)[1].strip() for line in meminfo
                              if line.startswith(f"{key}:")), None) for key in fields},
        "nvme_temperatures_millidegrees": {
            str(path): sample(path) for path in Path("/sys/class/nvme").glob("*/hwmon*/temp1_input")
        },
    }


def trial(binary, output, case, cfg, mode, seed, seconds, high):
    cfg = deepcopy(cfg)
    cfg["record"].update(policy="deadline-aware" if mode in ("legacy", "global") else mode,
                         copy_scheduling="global" if mode in ("global", "deadline-aware-reserve")
                         else "legacy", seed=seed)
    name = f"{case}-{seed}-{mode}"
    if high:
        before = telemetry()
    row = run(str(binary), output, name, cfg, duration=str(seconds),
              allow_large=high, expected="droppable")
    work = output / name
    summary = read(work / "run-summary.json")
    if high:
        save(work / "host-telemetry.json", {"before": before, "after": telemetry()})
    losses = Counter()
    with (work / "run-loss.jsonl").open() as file:
        for line in file:
            item = json.loads(line)
            losses[item["reason"]] += item["end"] - item["start"]
    row.update(case=case, seed=seed, mode=mode, loss_reasons=dict(losses),
               accepted_losses=sum(s["after_accept_loss"] for s in summary["sources"]),
               accepted_to_data_sync_p99_ns=summary["timings_ns"].get("accepted_to_committed", {}).get("p99"),
               sync_p99_ns=summary["timings_ns"]["sync"].get("p99"),
               global_copy=summary.get("global_copy"))
    row["passed"] &= row["committed"] + row["losses"] == row["planned"]
    return row


def checks(rows, high):
    if high:
        return {"ledger_and_resource_reconciliation": all(r["passed"] for r in rows)}
    def find(case, seed, mode="global"):
        return next(r for r in rows if (r["case"], r["seed"], r["mode"]) == (case, seed, mode))

    seeds = sorted({r["seed"] for r in rows})
    results = {"ledger_and_resource_reconciliation": all(r["passed"] for r in rows)}
    for seed in seeds:
        legacy, global_, copy, borrow = [find("M1", seed, mode) for mode in MODES]
        results[f"M1-{seed}"] = (
            len({r["pool_bytes"] for r in (legacy, global_, copy, borrow)}) == 1
            and global_["deadline_exceeded"] < legacy["deadline_exceeded"]
            and global_["losses"] == 0 and global_["committed"] == copy["committed"]
        )
        for case in ("M2", "M3", "M4", "M5", "M10"):
            result = find(case, seed)
            results[f"{case}-{seed}"] = result["losses"] == 0 and result["committed"] == result["planned"]
        inflight = find("M9", seed)
        results[f"M9-{seed}"] = (
            inflight["peak_in_flight"] > 1 and inflight["peak_in_flight_bytes"] > 4096
            and inflight["global_copy"]["borrow_admission_deferrals"] > 0
        )
        reserve = find("M6", seed, "deadline-aware-reserve")
        results[f"M6-{seed}"] = reserve["reserve_actions"] == 4 and reserve["copy_bytes"] > 0
        full, size = (find(case, seed) for case in ("M7-full", "M7-class"))
        results[f"M7-{seed}"] = (full["warnings"]["AT_RISK"]["count"] > 0
                                 and size["global_copy"]["spill_size_class_rejections"] > 0
                                 and full["losses"] > 0 and size["losses"] > 0)
        slow = find("M8", seed)
        results[f"M8-{seed}"] = (
            slow["plan"] == "rejected" and slow["warnings"]["CONTRACT_VIOLATION"]["count"] > 0
            and slow["global_copy"]["unfeasible_demand_events"] > 0
        )
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, default=ROOT / "target/release/recorder")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--ssd", choices=SSD, help="opt in to one SSD case per invocation")
    parser.add_argument("--seconds", type=Decimal, help="SSD input duration")
    parser.add_argument("--arrival", choices=("spread", "aligned", "jitter", "burst"), default="spread")
    args = parser.parse_args()
    if not 1 <= args.repeats <= 4:
        parser.error("--repeats must be 1–4")
    if bool(args.ssd) != (args.seconds is not None):
        parser.error("--ssd and --seconds must be specified together")
    high = args.ssd is not None
    seconds = args.seconds if high else Decimal("0.35")
    if not seconds.is_finite() or not Decimal("0.5" if high else "0.35") <= seconds <= 15:
        parser.error("SSD runs must specify 0.5–15 seconds")
    if args.ssd == "H3" and seconds < Decimal("0.8"):
        parser.error("H3 needs at least 0.8 seconds to include the full stall")
    output = args.out.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    cases = {args.ssd: high_case(args.ssd, args.arrival)} if high else small_cases(1)
    modes = {case: (MODES if high or case == "M1" else
                    ("global", "deadline-aware-reserve") if case == "M6" else ("global",))
             for case in cases}
    required = sum(estimate(cfg, seconds) * len(modes[case]) for case, cfg in cases.items()) * args.repeats
    if high:
        dev = output.parent.stat().st_dev
        sysfs = Path(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}")
        if not sysfs.exists() or "/nvme/" not in str(sysfs.resolve()):
            parser.error("SSD suite requires an output directory on a directly observable NVMe filesystem")
    free = shutil.disk_usage(output.parent).free
    print(f"Estimated upper output: {required:,} B; available: {free:,} B", flush=True)
    if required > 80 * (1 << 30) or free < required * 2:
        parser.error("comparison exceeds 80 GiB or leaves less than the estimated output in free space")
    binary = args.recorder.resolve()
    output.mkdir(exist_ok=False)
    environment = {
        "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "recorder_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "worktree_dirty": bool(subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()),
        "kernel": platform.release(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "estimated_max_bytes": required, "available_bytes": free, "duration_sec": str(seconds),
        "arrival": args.arrival, "opt_in_ssd": args.ssd,
    }
    save(output / "environment.json", environment)
    rows = []
    for seed in range(1, args.repeats + 1):
        if not high:
            cases = small_cases(seed)
        for case, cfg in cases.items():
            order = modes[case]
            case_offset = SSD.index(case) if high else 0
            rotation = (seed - 1 + case_offset) % len(order)
            for mode in order[rotation:] + order[:rotation]:
                result = trial(binary, output, case, cfg, mode, seed, seconds, high)
                result["git_sha"] = environment["git_sha"]
                rows.append(result)

    result = checks(rows, high)
    save(output / "comparison.json", {"environment": environment, "checks": result, "runs": rows})
    columns = ("case", "seed", "mode", "git_sha", "plan", "planned", "committed", "losses",
               "accepted_losses", "unknown", "unreported_missing", "duplicates", "corrupt_frames",
               "deadline_exceeded", "copy_bytes", "cpu_ns", "peak_queue", "committed_bytes_per_sec",
               "p99_service_ns", "accepted_to_data_sync_p99_ns", "sync_p99_ns",
               "first_warning_to_loss_ns", "reserve_actions", "passed")
    with (output / "comparison.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    for name, passed in result.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    return 0 if all(result.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
