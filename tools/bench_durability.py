"""Compare Writer and Durability variants on a bounded, temperature-checked NVMe."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tomllib

from bench_bounded_ssd import device_for, sample
from bench_writer_ablation import invoke, preset

MIB = 1 << 20
CAP = 6 * (1 << 30)


def run_trial(out, binary, before_binary, label, trial, rate, disk, sensor):
    case = out / f"{rate}-{trial}-{label}"
    case.mkdir()
    high = rate == "fast"
    sync = "small" if label.endswith("small") else "time" if label.endswith("time") else "current"
    cfg = preset("C0-fast" if high else "C0", sync)
    if label.startswith("overlap"):
        cfg += '\n[durability]\nmode = "overlap"\n'
    config = case / "config.toml"
    config.write_text(cfg)
    data = case / "run.mcap"
    before = sample(disk, sensor)
    if not 0 < before["temperature_c"] < 65:
        raise RuntimeError("NVMe temperature exceeds test range")
    args = [str(binary), "record", "--config", str(config), "--output", str(data),
            "--duration-sec", "0.09" if high else "0.18", "--allow-unverified"]
    env = os.environ | {"RECORDER_COPY_TRACE": "0"}
    if invoke(args, case / "record.log", env):
        raise RuntimeError(f"record failed for {case}")
    after = sample(disk, sensor)
    if not 0 < after["temperature_c"] < 65:
        raise RuntimeError("NVMe temperature exceeds test range")
    report = case / "run-summary.json"
    verify = case / "verify.json"
    if invoke([str(binary), "verify", "--input", str(data), "--report", str(report)], verify):
        raise RuntimeError(f"verifier failed for {case}")
    legacy_verified = None
    if binary != before_binary:
        legacy_report = case / "verify-before.json"
        legacy_verified = invoke([str(before_binary), "verify", "--input", str(data),
                                  "--report", str(report)], legacy_report) == 0
        if not legacy_verified:
            raise RuntimeError(f"original binary could not verify {case}")
    recovery_verified = None
    if data.stat().st_size <= 128 * MIB:
        recovery = case / "recover.json"
        if invoke([str(binary), "recover", "--input", str(data), "--dry-run"], recovery):
            raise RuntimeError(f"recovery failed for {case}")
        recovery_verified = not json.loads(recovery.read_text())["recovery_needed"]
        if not recovery_verified:
            raise RuntimeError(f"recovery evidence incomplete for {case}")
    result = json.loads(report.read_text())
    checked = json.loads(verify.read_text())
    sources = result["sources"]
    payload = result["committed_payload_bytes"]
    writer_cpu = result["writer"].get("cpu_ns")
    durability = result.get("durability", {})
    # Pool return_latency is configured release→eligibility, hold is acquire→eligibility.
    return dict(mode=label, rate=rate, trial=trial, command=args,
                config=tomllib.loads(cfg),
                input_Bps=result["generated_bytes_per_sec"],
                durable_Bps=result["committed_payload_bytes_per_sec"],
                data_sync_only_Bps=payload * 1e9 / result["last_sync_elapsed_ns"],
                session_cpu_s_per_gib=result["process"]["cpu_time_ns"] / 1e9 * (1 << 30) / payload,
                writer_cpu_s_per_gib=writer_cpu / 1e9 * (1 << 30) / payload if writer_cpu else None,
                committed_frames=checked["committed_and_verified"], verified=checked["ok"],
                recovery_verified=recovery_verified, legacy_verified=legacy_verified,
                loss=sum(s["before_accept_loss"]+s["after_accept_loss"] for s in sources),
                unknown=checked["unknown"], corrupt=checked["corrupt_frames"],
                copy_bytes=result["copy_bytes"], queue_peak=result["queue_peak"],
                inflight_peak=result["writing_peak_in_flight"],
                deadlines=sum(d["pool"]["deadline_exceeded"] for d in result["devices"]),
                ack_delay=[d["pool"]["return_latency_ns"] for d in result["devices"]],
                acquire_to_ack=[d["pool"]["hold_ns"] for d in result["devices"]],
                lease_drop_ns=result["writer"].get("lease_drop_ns"),
                finalized=result["finalized"], journal_synced=result["commit_log_synced"],
                loss_synced=result["loss_log_synced"],
                data_sync=result["timings_ns"]["sync"], durability=durability,
                max_uncertain_bytes=durability.get("max_uncertain_bytes"),
                barrier_ns=durability.get("barrier_ns"),
                pending_peak=durability.get("pending_peak"),
                backpressure_ns=durability.get("backpressure_ns"),
                overlap_ns=durability.get("data_sync_overlap_ns"),
                journal_sync_ns=durability.get("journal", {}).get("sync_ns"),
                ssd_before=before, ssd_after=after,
                disk_written_bytes=after["disk_written_bytes"] - before["disk_written_bytes"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--recorder", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--tune", action="store_true")
    args = parser.parse_args()
    out = args.out.resolve()
    disk, sensor, controller = device_for(out.parent)
    free = shutil.disk_usage(out.parent).free
    if free < 2 * CAP:
        parser.error("need 12 GiB free for the 6 GiB output cap")
    order = ["before", "barrier"]
    if args.tune:
        order.extend(("overlap", "overlap-small", "overlap-time"))
    assert len(order) * 6 * 182 * MIB < CAP
    out.mkdir()
    cpus = sorted(os.sched_getaffinity(0))[:8]
    os.sched_setaffinity(0, cpus)
    before, recorder = args.before.resolve(strict=True), args.recorder.resolve(strict=True)
    print(f"Bound {CAP} bytes, {free} bytes free, output {out}", flush=True)
    env = dict(affinity=cpus, free_bytes=free, output_cap=CAP,
               nvme=(controller / "model").read_text().strip(),
               firmware=(controller / "firmware_rev").read_text().strip(),
               mount=subprocess.check_output(["findmnt", "-T", str(out)], text=True))
    for name, binary in (("before", before), ("recorder", recorder)):
        with binary.open("rb") as stream:
            env[name + "_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    rows = []
    for rate in ("paced", "fast"):
        for trial in range(1, 4):
            rotation = (trial - 1 + (rate == "fast")) % len(order)
            for label in order[rotation:] + order[:rotation]:
                print(f"{rate} {trial} {label}", flush=True)
                rows.append(run_trial(out, before if label == "before" else recorder, before,
                                      label, trial, rate, disk, sensor))
    (out / "results.json").write_text(json.dumps(dict(environment=env, rows=rows), indent=2) + "\n")
    with (out / "results.csv").open("x", newline="") as stream:
        columns = [k for k, val in rows[0].items() if not isinstance(val, (list, dict))]
        writer = csv.DictWriter(stream, columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
