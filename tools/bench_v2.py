#!/usr/bin/env python3
"""Measure V2 policies, completion backends, faults, and an opt-in SSD profile."""

import argparse
import copy
import csv
from decimal import Decimal, ROUND_CEILING
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import tomllib

MIB = 1024**2
POLICIES = ("always-copy", "always-borrow", "v1-threshold", "deadline-aware", "deadline-aware-reserve")


def save(path, data):
    with path.open("x") as file:
        json.dump(data, file, indent=2, sort_keys=True)
        file.write("\n")


def read(path):
    return json.loads(path.read_text())


def save_config(path, value):
    def write_section(file, name, items, array=False):
        brackets = ("[[", "]]") if array else ("[", "]")
        file.write(f"{brackets[0]}{name}{brackets[1]}\n")
        for key, value in items.items():
            file.write(f"{key} = {json.dumps(value)}\n")
        file.write("\n")

    with path.open("x") as file:
        for key, items in value.items():
            if isinstance(items, list):
                for item in items:
                    write_section(file, key, item, array=True)
            else:
                write_section(file, key, items)


def probe_device(path):
    device = path.stat().st_dev
    stat = Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}/stat")
    try:
        return int(stat.read_text().split()[6])
    except (ValueError, OSError, IndexError):
        return None


def device_delta(path, before):
    after = probe_device(path)
    return None if after is None or before is None else (after - before) * 512


def capture(command, work, name):
    completed = subprocess.run(command, capture_output=True, text=True)
    (work / f"{name}.stderr").write_text(completed.stderr)
    if completed.stdout:
        (work / f"{name}.json").write_text(completed.stdout)
    return completed.returncode


def sum_counts(items, key):
    return sum(item[key] for item in items)


def warnings(report, key):
    return next(warning["count"] for warning in report["warnings"] if warning["kind"] == key)


def run(binary, output, name, cfg, duration="0.6", allow_large=False, expected="clean"):
    work = output / name
    work.mkdir()
    config = work / "run.toml"
    save_config(config, cfg)
    part = ["--config", str(config), "--duration-sec", str(duration)]
    plan = [binary, "plan", *part]
    record = [binary, "record", *part, "--output", str(work / "run.mcap")]
    if expected != "clean":
        record.append("--allow-unverified")
    if allow_large:
        record.append("--allow-large-output")
    verify = [binary, "verify", "--input", str(work / "run.mcap"), "--report", str(work / "run-summary.json")]
    save(work / "commands.json", {"plan": plan, "record": record, "verify": verify})
    if capture(plan, work, "plan") != 0:
        raise RuntimeError(f"invalid configuration at {config}")
    before = probe_device(work)
    exit_code = capture(record, work, "record-output")
    disk_bytes = device_delta(work, before)
    summary = work / "run-summary.json"
    if not summary.exists() or not summary.stat().st_size:
        raise RuntimeError(f"unable to record {name}; see {work / 'record-output.stderr'}")
    summary = read(summary)
    verified = capture(verify, work, "verify")
    check = read(work / "verify.json")
    timings = summary["timings_ns"]
    losses = sum_counts(summary["sources"], "before_accept_loss") + sum_counts(summary["sources"], "after_accept_loss")
    row = {
        "name": name, "directory": str(work), "policy": cfg["record"]["policy"], "writer": cfg["record"]["writer"],
        "plan": summary["planner_verdict"], "in_contract": summary["within_assumed_contract"],
        "record_exit": exit_code, "verify_exit": verified,
        "planned": sum_counts(summary["sources"], "planned"), "written": sum_counts(summary["sources"], "written"),
        "committed": check["committed_and_verified"], "losses": losses, "unknown": check["unknown"],
        "unreported_missing": check["unreported_missing"], "duplicates": check["duplicates"],
        "corrupt_frames": check["corrupt_frames"], "false_loss_claims": check["false_loss_claims"],
        "copy_bytes": summary["copy_bytes"], "fixed_copies": summary["fixed_deadline_copies"],
        "pool_copies": summary["pool_deadline_copies"], "reserve_actions": summary["reserve_actions"],
        "reserve_actions_by_source": {d["id"]: d["reserve_displacements"] for d in summary["devices"]},
        "deadline_exceeded": sum(d["pool"]["deadline_exceeded"] for d in summary["devices"]),
        "max_empty_ns": max(d["pool"]["max_empty_ns"] for d in summary["devices"]),
        "peak_queue": summary["queue_peak"], "stall_windows": summary["stall_windows"],
        "completion_reorder": summary["writer"]["peak_completion_reorder"],
        "peak_in_flight": summary["writing_peak_in_flight"], "peak_in_flight_bytes": summary["writing_peak_in_flight_bytes"],
        "p99_service_ns": timings["submit_to_complete"].get("p99"), "p99_9_service_ns": timings["submit_to_complete"].get("p99_9"),
        "p99_copy_ns": timings["copy"].get("p99"), "sync_mean_ns": timings["sync"].get("mean"),
        "groups": summary["group_count"], "warnings": {w["kind"]: w for w in summary["warnings"]},
        "first_warning_to_loss_ns": summary["first_warning_to_loss_ns"],
        "risk_episodes_without_loss": summary["risk_episodes_without_loss"],
        "false_risk_episodes": summary["false_risk_episodes"],
        "cpu_ns": summary["process"]["cpu_time_ns"], "peak_rss_bytes": summary["process"]["peak_rss_bytes"],
        "pool_bytes": summary["plan"]["memory_allocated_payload_bytes"], "disk_bytes_written": disk_bytes,
        "committed_bytes_per_sec": summary["committed_payload_bytes_per_sec"],
        "all_resources_returned": summary["all_resources_returned"],
        "recovery_ns": [],
    }
    if cfg.get("faults", {}).get("stall_period_ms"):
        period = cfg["faults"]["stall_period_ms"] * 1_000_000
        length = cfg["faults"]["stall_duration_ms"] * 1_000_000
        with (work / "run-metrics.csv").open() as file:
            samples = [(int(row["elapsed_ns"]), int(row["queue_depth"])) for row in csv.DictReader(file)
                       if row["source"] == cfg["sources"][0]["id"]]
        for k in range(1, (int(Decimal(str(duration)) * 1_000_000_000) - 1) // period + 1):
            end = k * period + length
            recovered = next((at - end for at, q in samples if end <= at < end + period - length and q == 0), None)
            row["recovery_ns"].append(recovered)
    intact = not any(check[key] for key in (
        "unreported_missing", "false_loss_claims", "duplicate_loss_claims",
        "duplicates", "out_of_range", "out_of_order", "corrupt_frames",
    ))
    if expected == "error":
        passed = (exit_code != 0 and verified != 0 and intact and row["unknown"] > 0
                  and warnings(summary, "DURABILITY_UNKNOWN") > 0 and row["committed"] > 0)
    else:
        passed = exit_code == 0 and verified == 0 and intact and row["unknown"] == 0
        if expected == "clean":
            passed = passed and row["losses"] == 0
    passed = passed and summary["all_resources_returned"] and summary["journal_failures"] == 0
    row["passed"] = passed
    print(f"{name:26} {row['committed']:5}/{row['planned']:<5} lost={losses:<4} "
          f"unknown={row['unknown']:<4} copied={row['copy_bytes']:<10} groups={row['groups']:<4} "
          f"{'PASS' if passed else 'FAIL'}", flush=True)
    return row


def under_cpu_memory_load(measure):
    # Restrict only this harness and its children, then restore the original
    # affinity. The competitor has fixed memory and a two-second time limit.
    cpus = os.sched_getaffinity(0)
    os.sched_setaffinity(0, {min(cpus)})
    load = None
    try:
        code = """
import ctypes, time
size = 16 * 1024 * 1024
source = ctypes.create_string_buffer(size)
target = ctypes.create_string_buffer(size)
ctypes.memset(source, 1, size)
ctypes.memset(target, 2, size)
print("ready", flush=True)
end = time.monotonic() + 2
while time.monotonic() < end:
    ctypes.memmove(target, source, size)
"""
        load = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
        if load.stdout.readline().strip() != "ready":
            raise RuntimeError("CPU/memory competitor could not initialize")
        return measure()
    finally:
        if load is not None:
            if load.poll() is None:
                load.terminate()
            load.wait()
            load.stdout.close()
        os.sched_setaffinity(0, cpus)


def smoke(binary, output, base, include_io):
    results = []
    checks = {}

    def trial(name, config, **kwargs):
        result = run(binary, output, name, config, **kwargs)
        results.append(result)
        return result

    mix = trial("P1-mixed", base)
    checks["P1_accepted_mixed_sources"] = mix["in_contract"]

    fixed_pool = copy.deepcopy(base)
    fixed_pool["faults"] = {"stall_at_ms": 100, "stall_duration_ms": 58}
    run_p2 = trial("P2-fixed-and-pool", fixed_pool)
    checks["P2_different_return_models"] = run_p2["in_contract"] and run_p2["stall_windows"] == 1

    fixed = copy.deepcopy(base)
    fixed["sources"] = [fixed["sources"][0]]
    fixed["sources"][0]["dma_buffers"] = 32
    fixed["spill"] = [{"frame_bytes": 4096, "buffers": 64}]
    fixed["storage_profile"]["max_stall_ms"] = 180
    fixed["faults"] = {"stall_at_ms": 115, "stall_duration_ms": 170}
    comparison = {}
    for policy in POLICIES:
        case = copy.deepcopy(fixed)
        case["record"]["policy"] = policy
        expected = "clean" if policy in ("always-copy", "deadline-aware", "deadline-aware-reserve") else "droppable"
        comparison[policy] = trial(f"P3-{policy}", case, expected=expected)
    threshold = comparison["v1-threshold"]
    adaptive = comparison["deadline-aware"]
    checks["P3_old_pending_leases_improve_fixed_deadlines"] = (
        threshold["deadline_exceeded"] > adaptive["deadline_exceeded"]
        and adaptive["fixed_copies"] > 0 and adaptive["copy_bytes"] < comparison["always-copy"]["copy_bytes"]
        and all(r["losses"] == 0 for r in comparison.values())
        and len({r["pool_bytes"] for r in comparison.values()}) == 1
    )

    periodic = copy.deepcopy(base)
    periodic["storage_profile"]["min_stall_period_ms"] = 160
    periodic["faults"] = {"stall_period_ms": 160, "stall_duration_ms": 60}
    repeated = trial("P4-periodic", periodic, duration="0.75")
    checks["P4_recovers_before_next_stall"] = (
        repeated["stall_windows"] >= 3 and len(repeated["recovery_ns"]) >= 3
        and all(value is not None for value in repeated["recovery_ns"])
    )

    reserve = copy.deepcopy(base)
    reserve["sources"] = reserve["sources"][1:3]
    for src in reserve["sources"]:
        src.update(fps=80, burst=1, dma_buffers=7, jitter_us=0, reserve_buffers=6)
    reserve["storage_profile"]["max_stall_ms"] = 125
    reserve["faults"] = {"stall_at_ms": 100, "stall_duration_ms": 115}
    no_reserve = trial("P5-pending-copy", reserve)
    reserve["record"]["policy"] = "deadline-aware-reserve"
    with_reserve = trial("P5-with-reserve", reserve)
    checks["P5_reserve_at_same_payload_budget"] = (
        no_reserve["pool_bytes"] == with_reserve["pool_bytes"]
        and with_reserve["copy_bytes"] < no_reserve["copy_bytes"]
        and all(n > 0 for n in with_reserve["reserve_actions_by_source"].values())
    )

    slow_copy = copy.deepcopy(fixed)
    slow_copy["faults"]["copy_delay_us"] = 72_000
    slow = trial("P6-copy-bandwidth-fault", slow_copy, expected="droppable")
    checks["P6_slow_copy_detected"] = (
        slow["deadline_exceeded"] > adaptive["deadline_exceeded"] and slow["warnings"]["CONTRACT_VIOLATION"]["count"] > 0
        and slow["p99_copy_ns"] >= 70_000_000
    )
    loaded = under_cpu_memory_load(lambda: trial("P6-cpu-memory-contention", fixed, expected="droppable"))
    checks["P6_bounded_contention_preserves_integrity"] = loaded["passed"]
    copy_error = copy.deepcopy(fixed)
    copy_error["record"]["policy"] = "always-copy"
    copy_error["faults"] = {"copy_fail_attempt": 2}
    dropped_copy = trial("P6-copy-failure", copy_error, expected="droppable")
    checks["P6_known_copy_failure"] = dropped_copy["losses"] == 1

    overload = copy.deepcopy(base)
    overload["storage_profile"]["sustained_bytes_per_sec"] = 200_000
    overload["storage_profile"]["max_stall_ms"] = 20
    overload["faults"] = {"writer_bytes_per_sec": 350_000, "stall_at_ms": 100, "stall_duration_ms": 240}
    before = output / "P7-preflight"
    before.mkdir()
    config = before / "overload.toml"
    save_config(config, overload)
    exit_code = capture([binary, "record", "--config", str(config), "--output", str(before / "must-not-exist.mcap")], before, "record")
    overloaded = trial("P7-overload", overload, expected="droppable")
    checks["P7_rejected_and_detected_runtime_violations"] = (
        exit_code != 0 and not (before / "must-not-exist.mcap").exists()
        and overloaded["plan"] == "rejected" and overloaded["losses"] > 0
        and overloaded["warnings"]["CONTRACT_VIOLATION"]["count"] > 0
        and overloaded["warnings"]["LOSS_DETECTED"]["count"] > 0
    )

    mock = copy.deepcopy(base)
    mock["record"]["writer"] = "mock-async"
    mock["faults"] = {"mock_delay_us": 2500}
    mocked = trial("P8-mock-async", mock)
    checks["P8_equal_verified_data_with_reordering"] = (
        mocked["committed"] == mix["committed"] and mocked["completion_reorder"] > 0
        and 1 < mocked["peak_in_flight"] <= mock["record"]["in_flight"]
    )
    if include_io:
        ring = copy.deepcopy(base)
        ring["record"]["writer"] = "io-uring"
        real = trial("P8-io-uring", ring)
        checks["P8_real_io_uring_data"] = real["committed"] == mix["committed"] and real["peak_in_flight"] <= ring["record"]["in_flight"]

    for name, faults, expected in [
        ("short-write", {"io_short_bytes": 73}, "clean"),
        ("eio", {"io_error_request": 5, "io_error": "eio"}, "error"),
        ("enospc", {"io_error_request": 5, "io_error": "enospc"}, "error"),
        ("sync-failure", {"sync_fail_group": 1}, "error"),
    ]:
        case = copy.deepcopy(base)
        case["faults"] = faults
        case["record"]["commit_messages"] = 3 if expected == "error" else 128
        row = trial(f"P9-{name}", case, expected=expected)
        checks[f"P9_{name}"] = row["passed"]
    if include_io:
        ring["faults"] = {"io_short_bytes": 900}
        short = trial("P9-io-uring-short", ring)
        checks["P9_ring_short_completion"] = short["committed"] == mix["committed"]
    return results, checks


def ssd(binary, output, base, seconds, fps, include_io, allow_large):
    frames = int((Decimal(seconds) * (8 * fps)).to_integral_value(rounding=ROUND_CEILING)) + 8
    record_bytes = frames * (4 * MIB + 31) + 1024**2
    if record_bytes > 256 * MIB and not allow_large:
        raise RuntimeError("Check the destination and specify --allow-large-output for longer SSD runs")
    modes = ["sync", "io-uring"] if include_io else ["sync"]
    baseline_mib = max(192, int((Decimal(seconds) * 639_000_000 / MIB).to_integral_value(rounding=ROUND_CEILING))) if allow_large else 192
    steady_bytes = 192 * MIB + MIB
    estimate = record_bytes * len(modes) + steady_bytes + baseline_mib * MIB
    if shutil.disk_usage(output).free < estimate:
        raise RuntimeError(f"not enough disk space for {estimate:,} estimated bytes")
    baseline_file = output / "ssd-baseline.bin"
    baseline = ["dd", "if=/dev/zero", f"of={baseline_file}", "bs=1M", f"count={baseline_mib}",
                "oflag=direct", "conv=excl,fdatasync", "status=none"]
    before = probe_device(output)
    started = time.perf_counter_ns()
    completed = capture(baseline, output, "ssd-baseline")
    elapsed = time.perf_counter_ns() - started
    rate = baseline_mib * MIB * 1e9 / elapsed if completed == 0 else None
    save(output / "ssd-baseline.json", {
        "command": baseline, "bytes_per_sec_including_sync": rate, "device_bytes_written": device_delta(output, before),
        "elapsed_ns": elapsed, "exit": completed,
    })
    results = []
    scenarios = [(f"P10-{mode}", mode, fps, seconds) for mode in modes]
    scenarios.append(("P10-steady-2s", "sync", 3, "2"))
    for name, mode, case_fps, case_duration in scenarios:
        settings = copy.deepcopy(base)
        settings["record"].update(writer=mode, policy="deadline-aware", queue_capacity=200,
                                  memory_budget_bytes=610 * MIB, max_output_bytes=max(record_bytes, steady_bytes),
                                  commit_bytes=64 * MIB, commit_interval_ms=250, commit_messages=256,
                                  metrics_interval_ms=10, max_borrowed_in_flight=4)
        settings["storage_profile"].update(sustained_bytes_per_sec=int(rate or 600_000_000),
                                            max_stall_ms=30, max_io_us=15_000, max_sync_ms=250)
        settings["copy_profile"]["max_capture_delay_us"] = 3000
        settings["spill"] = [{"frame_bytes": 4 * MIB, "buffers": 48}]
        settings["sources"] = [
            {"id": f"camera{i}", "model": "pool", "frame_bytes": 4 * MIB, "fps": case_fps,
             "phase_us": (i * 1_000_000) // (8 * case_fps), "dma_buffers": 8, "capture_reserve": 1}
            for i in range(8)
        ]
        results.append(run(binary, output, name, settings, duration=case_duration,
                           allow_large=allow_large, expected="droppable"))
    return results, {"P10_direct_disk_baseline": completed == 0,
                     "P10_commit_and_offline_verification": all(r["groups"] > 1 and r["passed"] for r in results)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", default="target/release/recorder")
    parser.add_argument("--preset-toml")
    parser.add_argument("--suite", choices=("v2-smoke", "ssd"), default="v2-smoke")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--include-io-uring", action="store_true")
    parser.add_argument("--high-seconds", default="0.2")
    parser.add_argument("--high-fps-per-source", type=int, default=30)
    parser.add_argument("--allow-large-output", action="store_true")
    args = parser.parse_args()
    high = Decimal(args.high_seconds)
    if not high.is_finite() or high <= 0 or args.high_fps_per_source <= 0:
        parser.error("high-seconds and high-fps-per-source must be positive")
    source = args.preset_toml or Path("configs/v2-smoke.toml").read_text()
    binary = str(Path(args.recorder).resolve(strict=True))
    output = args.out.resolve()
    output.mkdir()
    if args.suite == "ssd":
        results, checks = ssd(binary, output, tomllib.loads(source), args.high_seconds,
                              args.high_fps_per_source, args.include_io_uring, args.allow_large_output)
    else:
        results, checks = smoke(binary, output, tomllib.loads(source), args.include_io_uring)
    save(output / "comparison.json", {"suite": args.suite, "checks": checks, "runs": results})
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    return 0 if all(checks.values()) and all(r["passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
