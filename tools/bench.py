#!/usr/bin/env python3
"""Run the recorder's E0–E7 experiments and keep individual artifacts."""

import argparse
import csv
from decimal import Decimal, ROUND_CEILING
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

MIB = 1024 * 1024
POLICIES = ("copy", "borrow", "adaptive")
LOW = {
    "frame-bytes": 65536,
    "fps": 120,
    "duration-sec": "1.2",
    "dma-buffers": 8,
    "capture-reserve": 2,
    "spill-buffers": 32,
    "queue-capacity": 40,
    "max-hold-ms": 120,
    "metrics-interval-ms": 10,
    "seed": 42,
}
HIGH = {
    **LOW,
    "frame-bytes": 4 * MIB,
    "fps": 240,
    "dma-buffers": 32,
    "capture-reserve": 4,
    "spill-buffers": 128,
    "queue-capacity": 160,
    "max-hold-ms": 1000,
    "metrics-interval-ms": 25,
}
SCENARIOS = {
    "E1": {},
    "E2": {"stall-at-ms": 320, "stall-duration-ms": 30},
    "E3": {"stall-at-ms": 320, "stall-duration-ms": 170},
    "E4": {"stall-at-ms": 320, "stall-duration-ms": 450},
    "E5": {"stall-period-ms": 350, "stall-duration-ms": 170},
    "E6": {"writer-bytes-per-sec": 3 * MIB},
}


def json_file(path, value):
    with path.open("x") as file:
        json.dump(value, file, indent=2, sort_keys=True)
        file.write("\n")


def device_sectors(path):
    """Device-wide sectors written, including unrelated processes."""
    dev = path.stat().st_dev
    stat = Path(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}/stat")
    try:
        return int(stat.read_text().split()[6])
    except (OSError, ValueError, IndexError):
        return None


def delta_bytes(before, after):
    if before is not None and after is not None:
        return (after - before) * 512
    return None


def measure_disk(output, mib):
    destination = output / "ssd-baseline.bin"
    command = [
        "dd", "if=/dev/zero", f"of={destination}", "bs=1M", f"count={mib}",
        "oflag=direct", "conv=excl,fdatasync", "status=none",
    ]
    before = device_sectors(output)
    time_ns = time.perf_counter_ns()
    completed = subprocess.run(command, capture_output=True, text=True)
    elapsed_ns = time.perf_counter_ns() - time_ns
    device_delta = delta_bytes(before, device_sectors(output))
    result = {
        "command": command,
        "elapsed_ns": elapsed_ns,
        "exit_code": completed.returncode,
        "requested_bytes": mib * MIB,
        "device_bytes_written": device_delta,
        "bytes_per_sec_including_fdatasync": (
            mib * MIB * 1e9 / elapsed_ns if completed.returncode == 0 else None
        ),
        "stderr": completed.stderr,
    }
    json_file(output / "ssd-baseline.json", result)
    return result


def record(binary, directory, test, policy, settings, allow_large, sink="mcap"):
    directory.mkdir()
    output = directory / "run.mcap"
    command = [str(binary), "record", "--output", str(output), "--policy", policy, "--sink", sink]
    for key, value in settings.items():
        command.extend((f"--{key}", str(value)))
    if allow_large:
        command.append("--allow-large-output")
    verification = [
        str(binary), "verify", "--input", str(output),
        "--report", str(directory / "run-summary.json"),
        "--loss-log", str(directory / "run-loss.jsonl"),
    ]
    json_file(directory / "commands.json", {
        "record": command,
        "verify": verification if sink == "mcap" else None,
    })

    before = device_sectors(directory) if sink == "mcap" else None
    with (directory / "record.stderr").open("x") as stderr:
        capture = subprocess.run(command, stderr=stderr)
    device_delta = delta_bytes(before, device_sectors(directory)) if sink == "mcap" else None
    summary = directory / "run-summary.json"
    if capture.returncode != 0 and not summary.stat().st_size:
        raise RuntimeError(f"{test}/{policy} capture failed; see {directory / 'record.stderr'}")
    report = json.loads(summary.read_text())
    result = {
        "test": test,
        "policy": policy,
        "directory": str(directory),
        "record_exit": capture.returncode,
        "verify_exit": None,
        "device_bytes_written_during_record": device_delta,
        "expected": report["expected"],
        "written": report["written"],
        "dropped": sum(report["losses"].values()),
        "losses": report["losses"],
        "copy_bytes": report["copy_bytes"],
        "generated_bytes_per_sec": report["input_bytes_per_sec"],
        "written_bytes_per_sec": report["writer_bytes_per_sec"],
        "synced_payload_bytes_per_sec": report["synced_payload_bytes_per_sec"],
        "generation_p99_ns": report["capture"]["generation_time_ns"].get("p99"),
        "write_call_p99_ns": report["writer"]["write_call_ns"].get("p99"),
        "dma_hold_p99_ns": report["dma"]["hold_ns"].get("p99"),
        "copy_p99_ns": report["capture"]["copy_time_ns"].get("p99"),
        "dma_deadline_exceeded": report["dma"]["deadline_exceeded"],
        "stall_wait_segments": report["writer"]["stall_wait_segments"],
        "stall_wait_ns": report["writer"]["stall_wait_ns"],
        "drain_ns": report["writer"]["drain_ns"],
        "sync_ns": report["writer"]["sync_ns"],
        "cpu_ns": report["process"]["cpu_time_ns"],
        "peak_rss_bytes": report["process"]["peak_rss_bytes"],
        "pool_allocated_bytes": report["pool_allocated_bytes"],
        "peak_dma_bytes": report["dma_peak_bytes"],
        "peak_spill_bytes": report["spill_peak_bytes"],
        "queue_peak": report["queue_peak"],
        "bounds_ok": (
            report["dma"]["free"] == report["dma"]["total"]
            and report["spill"]["free"] == report["spill"]["total"]
            and report["queue_depth"] == 0
            and report["queue_peak"] <= settings["queue-capacity"]
        ),
        "monitor_deadline_warnings": report["monitor"]["warning_deadline_near_samples"],
        "recovered_between_stalls": False,
    }
    if test == "E5":
        with (directory / "run-metrics.csv").open() as file:
            rows = list(csv.DictReader(file))
        for period in range(1, 4):
            start = period * settings["stall-period-ms"] * 1_000_000
            boundary = start + settings["stall-period-ms"] * 1_000_000
            active = [row for row in rows if start <= int(row["elapsed_ns"]) < boundary]
            depths = [int(row["queue_depth"]) for row in active]
            if not depths or max(depths) == 0 or 0 not in depths[depths.index(max(depths)):]:
                break
        else:
            result["recovered_between_stalls"] = True

    if sink == "mcap":
        verification_result = subprocess.run(verification, capture_output=True, text=True)
        (directory / "verify.stderr").write_text(verification_result.stderr)
        parsed = json.loads(verification_result.stdout)
        json_file(directory / "verify.json", parsed)
        result["verify_exit"] = verification_result.returncode
        result["verified"] = parsed["verified"]
    return result


def judge(scans):
    by_policy = {result["policy"]: result for result in scans}
    test = scans[0]["test"]
    checks = {
        "successful_recording": all(result["record_exit"] == 0 for result in scans),
        "returned_all_buffers": all(result["bounds_ok"] for result in scans),
        "whole_frame_count": all(result["written"] + result["dropped"] == result["expected"] for result in scans),
    }
    if test != "E0":
        checks["mcap_integrity"] = all(result["verify_exit"] == 0 for result in scans)
    if test in ("E1", "E2"):
        checks["no_losses"] = all(result["dropped"] == 0 for result in scans)
    if test in ("E1", "E3"):
        checks["copy_reduction"] = (
            by_policy["borrow"]["copy_bytes"] == 0
            and by_policy["adaptive"]["copy_bytes"] < by_policy["copy"]["copy_bytes"]
        )
    if test == "E3":
        checks["adaptive_absorbs_stall"] = (
            by_policy["borrow"]["losses"]["dma_pool_exhausted"] > 0
            and by_policy["adaptive"]["dropped"] == by_policy["copy"]["dropped"] == 0
            and by_policy["adaptive"]["copy_bytes"] > 0
        )
    if test == "E4":
        checks["finite_pools_exhausted"] = all(result["dropped"] > 0 for result in scans)
    if test == "E5":
        adaptive = by_policy["adaptive"]
        checks["periodic_stalls_observed"] = all(result["stall_wait_segments"] >= 3 for result in scans)
        checks["adaptive_catch_up"] = adaptive["recovered_between_stalls"] and adaptive["dropped"] == 0
        checks["deadline_warnings_observed"] = adaptive["monitor_deadline_warnings"] > 0
    if test == "E6":
        checks["overload_causes_loss"] = all(result["dropped"] > 0 for result in scans)
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="new directory for all artifacts")
    parser.add_argument("--recorder", type=Path, default=Path("target/release/recorder"))
    parser.add_argument("--suite", choices=("all", "policy", "ssd"), default="all")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--high-seconds", default="0.2")
    parser.add_argument("--disk-baseline-mib", type=int, default=192)
    parser.add_argument("--allow-large-output", action="store_true")
    args = parser.parse_args()
    args.out = args.out.resolve()
    binary = args.recorder.resolve()
    if not binary.is_file():
        parser.error(f"build the release recorder first: {binary}")
    high_seconds = Decimal(args.high_seconds)
    if not high_seconds.is_finite() or high_seconds <= 0 or args.repeats <= 0 or args.disk_baseline_mib <= 0:
        parser.error("times, repeat count, and baseline size must be positive")
    include_ssd = args.suite != "policy"
    include_policy = args.suite != "ssd"
    high_frames = (240 * high_seconds).to_integral_value(rounding=ROUND_CEILING)
    high_bytes = int(high_frames) * (4 * MIB + 31) + 4096
    baseline_bytes = args.disk_baseline_mib * MIB if include_ssd else 0
    if include_ssd and max(high_bytes, baseline_bytes) > 256 * MIB and not args.allow_large_output:
        parser.error("check the destination and explicitly pass --allow-large-output")
    estimated = baseline_bytes + args.repeats * (
        (200 * MIB if include_policy else 0) + (3 * high_bytes if include_ssd else 0)
    )
    free = shutil.disk_usage(args.out.parent).free
    print(f"Artifacts: {args.out} (estimated {estimated:,} bytes, free {free:,} bytes)", flush=True)
    if estimated > free:
        parser.error("insufficient space for retained artifacts")
    args.out.mkdir()

    # Run the physical storage baseline first, independently of recorder reports.
    baseline = measure_disk(args.out, args.disk_baseline_mib) if include_ssd else None
    experiments = []
    for repeat in range(1, args.repeats + 1):
        root = args.out / f"trial-{repeat:02d}"
        root.mkdir()
        groups = []
        if include_policy:
            for fps in (240, 480, 960, 1920, 3840):
                settings = {**HIGH, "duration-sec": "0.3", "fps": fps,
                            "dma-buffers": 8, "capture-reserve": 2, "spill-buffers": 16,
                            "queue-capacity": 24}
                groups.append([record(binary, root / f"E0-{fps}", "E0", "adaptive", settings, False, "null")])
            for test, settings in SCENARIOS.items():
                groups.append([record(binary, root / f"{test}-{policy}", test, policy,
                                      {**LOW, **settings}, False) for policy in POLICIES])
        if include_ssd:
            groups.append([record(binary, root / f"E7-{policy}", "E7", policy,
                                  {**HIGH, "duration-sec": args.high_seconds},
                                  args.allow_large_output) for policy in POLICIES])

        for results in groups:
            checks = judge(results)
            experiments.append({"trial": repeat, "test": results[0]["test"], "checks": checks, "runs": results})
            for row in results:
                speed = row["synced_payload_bytes_per_sec"]
                display = f"{speed / 1e6:7.1f}" if speed is not None else "   null"
                print(f"{row['test']:>2}  {row['policy']:8} {row['written']:5}/{row['expected']:<5}"
                      f" lost {row['dropped']:5}  copied {row['copy_bytes'] / MIB:6.2f} MiB"
                      f"  synced {display} MB/s  {'PASS' if all(checks.values()) else 'FAIL'}")
    json_file(args.out / "comparison.json", {"baseline": baseline, "experiments": experiments})
    passed = (not baseline or baseline["exit_code"] == 0) and all(
        all(group["checks"].values()) for group in experiments
    )
    if not passed:
        print(f"See failed checks in {args.out / 'comparison.json'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
