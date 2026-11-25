"""Bounded buffered-file and frozen recorder qualification (Linux)."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import struct
import subprocess
import sys
import time

MIB = 1 << 20
FRAME = MIB
GROUP = 8
CAP = 512 * MIB


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def command(args, log):
    with log.open("x") as stream:
        subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=30)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def distribution(values):
    values = sorted(values)
    return {name: values[(len(values) * p + 99) // 100 - 1]
            for name, p in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))}


def backend(case, layer):
    payload = bytes(range(256)) * (FRAME // 256)
    output = case / "buffered.bin"
    count = 32
    headers = [struct.pack("<BIHQIQ", 5, FRAME + 22, 1, i, i, i) for i in range(count)]
    before = time.process_time_ns()
    wall = time.monotonic_ns()
    io, sync, durable = [], [], []
    offset = 0
    with output.open("xb", buffering=0) as stream:
        for i in range(count):
            start = time.monotonic_ns()
            buffers = [payload] if layer == "B1" else [headers[i], payload]
            expected = sum(map(len, buffers))
            actual = os.pwritev(stream.fileno(), buffers, offset)
            if actual != expected:
                raise IOError("short pwritev")
            offset += actual
            io.append(time.monotonic_ns() - start)
            durable.append(start)
            if (i + 1) % GROUP == 0:
                start = time.monotonic_ns()
                os.fdatasync(stream.fileno())
                end = time.monotonic_ns()
                sync.append(end - start)
                durable[-GROUP:] = [end - stamp for stamp in durable[-GROUP:]]
    elapsed = time.monotonic_ns() - wall
    cpu = time.process_time_ns() - before
    expected_hash = hashlib.sha256()
    for i in range(count):
        if layer == "B2":
            expected_hash.update(headers[i])
        expected_hash.update(payload)
    actual_hash = digest(output)
    if output.stat().st_size != offset or actual_hash != expected_hash.hexdigest():
        raise ValueError("backend verification failed")
    return dict(layer=layer, payload_bytes=count * FRAME, offered_bytes=count * FRAME,
                accepted_bytes=count * FRAME, submitted_bytes=count * FRAME,
                completed_bytes=count * FRAME, committed_bytes=count * FRAME,
                durable_Bps=count * FRAME * 10**9 / elapsed, wall_ns=elapsed,
                cpu_ns=cpu, peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                io=distribution(io), sync=distribution(sync),
                accepted_to_data_sync=distribution(durable), sha256=actual_hash,
                io_samples_ns=io, sync_samples_ns=sync)


def recorder(case, binary, layer, fps, writer="sync", allow_unverified=False):
    copy = layer == "B4"
    config = case / "config.toml"
    faults = "[faults]\nstall_at_ms = 1\nstall_duration_ms = 10\n" if copy else ""
    config.write_text(f"""[record]
policy = "deadline-aware"
copy_scheduling = "global"
writer = "{writer}"
queue_capacity = 40
memory_budget_bytes = {128 * MIB}
max_output_bytes = {52 * MIB}
commit_bytes = {GROUP * FRAME}
commit_messages = {GROUP}
commit_interval_ms = 200
metrics_interval_ms = 5
seed = 1

{faults}
[[sources]]
id = "camera"
kind = "{'mock_v4l2' if copy else 'synthetic'}"
model = "{'fixed-deadline' if copy else 'owned'}"
frame_bytes = {FRAME}
fps = {fps}
dma_buffers = 40
{'max_hold_ms = 5' if copy else ''}

[[spill]]
frame_bytes = {FRAME}
buffers = 40
""")
    output = case / "run.mcap"
    summary = case / "run-summary.json"
    argv = [str(binary), "record", "--config", str(config), "--output", str(output),
            "--duration-sec", f"{32 / fps:.6f}"]
    if allow_unverified:
        argv.append("--allow-unverified")
    save(case / "command.json", argv)
    command(argv, case / "record.log")
    report = json.loads(summary.read_text())
    if bool(report["copy_bytes"]) != copy:
        raise ValueError("copy path was not exercised as intended")
    timings = report["timings_ns"]
    result = dict(layer=layer, fps=fps, writer=writer, copy_bytes=report["copy_bytes"],
                  queue_peak=report["queue_peak"], io=timings["submit_to_complete"],
                  sync=timings["sync"], generation=timings["generation"],
                  queue_wait=timings["queue_wait"], submitted_bytes=None,
                  accepted_to_data_sync=timings.get("accepted_to_committed"),
                  completed_bytes=report["written_payload_bytes"],
                  committed_bytes=report["committed_payload_bytes"],
                  durable_Bps=report["committed_payload_bytes_per_sec"],
                  generated_Bps=report["generated_bytes_per_sec"],
                  offered_bytes=sum(s["generated"] for s in report["sources"]) * FRAME,
                  accepted_bytes=sum(s["accepted"] for s in report["sources"]) * FRAME,
                  wall_ns=report["process"]["wall_ns"], cpu_ns=report["process"]["cpu_time_ns"],
                  peak_rss=report["process"]["peak_rss_bytes"],
                  inflight_peak=report["writing_peak_in_flight"],
                  drain_ns=max(0, report["writer"]["end_elapsed_ns"] - report["capture_wall_ns"]))
    if writer != "null":
        command([str(binary), "verify", "--input", str(output), "--report", str(summary)],
                case / "verify.json")
        result["sha256"] = digest(output)
    return result


def snapshot(path):
    mount = subprocess.check_output(["findmnt", "-J", "-T", str(path)], text=True)
    dev = path.stat().st_dev
    block = Path(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}")
    nvme = {}
    for device in Path("/sys/class/nvme").glob("nvme*"):
        nvme[device.name] = {
            key: (device / key).read_text().strip()
            for key in ("model", "firmware_rev") if (device / key).exists()
        }
    return dict(mount=json.loads(mount), block=str(block.resolve()) if block.exists() else None,
                nvme_inventory=nvme, nvme_temperature=None, physical_device_io=None,
                free_bytes=shutil.disk_usage(path).free, load=os.getloadavg(),
                dirty_writeback=[line for line in Path("/proc/meminfo").read_text().splitlines()
                                 if line.startswith(("Dirty:", "Writeback:"))])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--fps", type=int, nargs=2, default=(500, 1600))
    parser.add_argument("--allow-unverified", action="store_true")
    args = parser.parse_args()
    if any(not 100 <= fps <= 2000 for fps in args.fps):
        parser.error("fps must be between 100 and 2000")
    path = args.out.resolve()
    binary = args.recorder.resolve(strict=True)
    if shutil.disk_usage(path.parent).free < 2 * CAP:
        parser.error(f"need {2 * CAP:,} bytes free")
    print(f"Output budget {CAP:,} bytes at {path}", flush=True)
    path.mkdir(exist_ok=False)
    toolchain = subprocess.check_output(["rustc", "--version"], text=True).strip()
    save(path / "environment.json", dict(
        before=snapshot(path), argv=sys.argv, output_cap=CAP, base_sha=args.base_sha,
        binary_sha256=digest(binary), kernel=platform.release(), toolchain=toolchain,
        affinity=sorted(os.sched_getaffinity(0)), cpu=next(
            line for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name"))))
    rows = []
    for trial, fps in enumerate(args.fps, 1):
        order = ("B1", "B2", "B3", "B4") if trial == 1 else ("B4", "B3", "B2", "B1")
        for layer in order:
            case = path / f"{trial}-{layer}"
            case.mkdir()
            row = backend(case, layer) if layer in ("B1", "B2") else recorder(case, binary, layer, fps, allow_unverified=args.allow_unverified)
            row["trial"] = trial
            row["phase2_profile_eligible"] = layer == "B3"
            rows.append(row)
    for writer in ("null", "io-uring"):
        case = path / writer
        case.mkdir()
        try:
            rows.append(recorder(case, binary, "B3", args.fps[1], writer, args.allow_unverified))
        except subprocess.CalledProcessError as error:
            if writer != "io-uring":
                raise
            rows.append(dict(layer="B3", writer=writer, status="failed",
                             returncode=error.returncode, durable_Bps=None))
    save(path / "capacity.json", rows)
    columns = sorted(set().union(*(row.keys() for row in rows)))
    with (path / "capacity.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    save(path / "after.json", snapshot(path))


if __name__ == "__main__":
    main()
