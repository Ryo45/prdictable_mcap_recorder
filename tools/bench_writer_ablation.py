"""Bounded, rotated Writer experiments on an isolated diagnostic build."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

from bench_bounded_ssd import device_for, run as ssd_run, sample
from bench_writer_capacity import backend

MIB = 1 << 20
LIMIT = 10 * (1 << 30)
CASES = {
    "C0": ("baseline", "split", "current"),
    "C0-profile": ("baseline", "split", "current"),
    "C1": ("loss-only", "split", "current"),
    "C2": ("loss-only-no-crc", "split", "current"),
    "C0-pwritev": ("baseline", "pwritev", "current"),
    "C2-pwritev": ("loss-only-no-crc", "pwritev", "current"),
    "small": ("baseline", "split", "small"),
    "large": ("baseline", "split", "large"),
    "time": ("baseline", "split", "time"),
    "cache-only": ("cache-only", "split", "current"),
    "W2-owned": ("baseline", "split", "current"),
    "W3-copy": ("baseline", "split", "current"),
    "C0-fast": ("baseline", "split", "current"),
    "C0-pwritev-fast": ("baseline", "pwritev", "current"),
    "C2-pwritev-fast": ("loss-only-no-crc", "pwritev", "current"),
}
GROUPS = dict(current=(16, 16, 80), small=(4, 4, 80), large=(64, 64, 250), time=(256, 256, 30))


def preset(name, sync, short=False, fault=""):
    size, count, interval = GROUPS[sync]
    owned = name == "W2-owned"
    force = name == "W3-copy"
    config = f"""[record]
policy = "{'always-copy' if force else 'deadline-aware'}"
copy_scheduling = "{'legacy' if force else 'global'}"
writer = "{'io-uring' if name == 'uring' else 'sync'}"
queue_capacity = 64
memory_budget_bytes = {256 * MIB}
max_output_bytes = {200 * MIB}
commit_interval_ms = {interval}
commit_bytes = {size * MIB}
commit_messages = {count}
metrics_interval_ms = 5
seed = 101

"""
    if short:
        config += "[faults]\nio_short_bytes = 16381\n"
    if fault:
        config += "[faults]\n" + fault + "\n"
    for i in range(2):
        config += f"""
[[sources]]
id = "camera_{i}"
kind = "{'synthetic' if owned else 'mock_v4l2'}"
model = "{'owned' if owned else 'fixed-deadline'}"
frame_bytes = {MIB if not short else 65536}
fps = {1000 if name.endswith("-fast") else 500 if not short else 40}
phase_us = {i * 1000}
dma_buffers = 64
"""
        if not owned:
            config += "max_hold_ms = 100\nreturn_delay_us = 200\n"
    config += f"\n[[spill]]\nframe_bytes = {MIB if not short else 65536}\nbuffers = 32\n"
    return config


def invoke(args, logfile, env=None):
    with logfile.open("x") as log:
        return subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, env=env,
                              timeout=35, check=False).returncode


def run_case(root, binary, name, trial, mode, io, sync, device, extended=False):
    case = root / f"{trial}-{name}"
    case.mkdir()
    config, data = case / "config.toml", case / "run.mcap"
    summary, proof = case / "run-summary.json", case / "verify.json"
    config_text = preset(name, sync)
    if extended:
        config_text = config_text.replace("max_output_bytes = 209715200", f"max_output_bytes = {910 * MIB}")
    config.write_text(config_text)
    env = os.environ | dict(RECORDER_ABLATION=mode, RECORDER_ABLATION_IO=io,
                            RECORDER_WRITER_PROFILE="1" if name == "C0-profile" else "0",
                            RECORDER_COPY_TRACE="0")
    argv = [str(binary), "record", "--config", str(config), "--output", str(data),
            "--duration-sec", "0.90" if extended else "0.09" if name.endswith("-fast") else "0.18",
            "--allow-unverified"]
    if extended:
        argv.append("--allow-large-output")
    before = sample(*device)
    if not 0 < before["temperature_c"] < 65:
        raise ValueError("NVMe temperature outside test range")
    code = invoke(argv, case / "record.log", env)
    after = sample(*device)
    if not 0 < after["temperature_c"] < 65:
        raise ValueError("NVMe temperature outside test range")
    row = dict(case=name, trial=trial, mode=mode, io=io, sync=sync, command=argv,
               record_exit=code, ssd_before=before, ssd_after=after)
    if name == "uring" and "Operation not permitted" in (case / "record.log").read_text():
        return row | dict(status="EPERM", durable_Bps=None)
    result = json.loads(summary.read_text())
    verify_code = invoke([str(binary), "verify", "--input", str(data), "--report", str(summary)], proof)
    verified, _ = json.JSONDecoder().raw_decode(proof.read_text())
    writer = result["writer"]
    sources = result["sources"]
    sum_field = lambda field: sum(s[field] for s in sources)
    if mode == "cache-only":
        if (result["final_sync"] or sum_field("committed") or not code or not verify_code
                or sum_field("unknown") != sum_field("accepted") or verified["verified"] != sum_field("written")):
            raise ValueError("invalid cache-only evidentiary state")
    elif code or verify_code or not verified["ok"]:
        raise ValueError(f"record or verifier failed in {case}")
    if not result["all_resources_returned"]:
        raise ValueError(f"not all resources returned in {case}")
    committed = result["committed_payload_bytes"]
    written = result["written_payload_bytes"]
    accepted = sum_field("accepted") * MIB
    if (name == "W3-copy" and result["copy_bytes"] == 0
            or name != "W3-copy" and result["copy_bytes"] != 0):
        raise ValueError(f"unexpected copy path in {case}")
    cpu = result["process"]["cpu_time_ns"]
    return row | dict(status="diagnostic" if mode == "cache-only" else "verified",
        planned=sum_field("planned"), accepted=accepted // MIB, written=sum_field("written"),
        committed=sum_field("committed"), missing=verified["unreported_missing"],
        loss=sum_field("before_accept_loss") + sum_field("after_accept_loss"),
        late=sum(d["pool"]["deadline_exceeded"] for d in result["devices"]),
        unknown=sum_field("unknown"), corrupt=verified["corrupt_frames"], duplicate=verified["duplicates"],
        input_Bps=result["generated_bytes_per_sec"], completed_Bps=written * 10**9 / writer["last_completion_ns"],
        durable_Bps=result["committed_payload_bytes_per_sec"], writer_profile=writer["profile"],
        data_sync=result["timings_ns"]["sync"] if mode != "cache-only" else None,
        io=result["timings_ns"]["submit_to_complete"], journal=result["monitor"]["commit_profile"],
        cpu_ns=cpu, cpu_s_per_gib=cpu / 10**9 * (1 << 30) / written,
        peak_queue=result["queue_peak"], peak_inflight=result["writing_peak_in_flight"],
        copy_bytes=result["copy_bytes"], peak_rss=result["process"]["peak_rss_bytes"],
        max_unsynced_offered_bytes=writer["max_unsynced_payload"],
        disk_written_bytes=after["disk_written_bytes"] - before["disk_written_bytes"],
        commit_journal_bytes=(case / "run-commits.jsonl").stat().st_size,
        durable_bytes=committed, completed_bytes=written)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--smoke", action="store_true", help="one recorder iteration with no W0/W1")
    group.add_argument("--extended", action="store_true", help="9 paced runs, 900 MiB payload each")
    args = parser.parse_args()
    root = args.out.resolve()
    binary = args.recorder.resolve(strict=True)
    free = shutil.disk_usage(root.parent).free
    if free < 2 * LIMIT:
        parser.error("requires at least twice the 10 GiB cap free")
    disk, sensor, controller = device_for(root.parent)
    # All recorder cases are at most 180 one-MiB frames. Allow more than 2 MiB
    # per run for MCAP metadata, sidecars, and evidence.
    extended_cases = ["C0", "C0-pwritev", "C2-pwritev"]
    upper = (910 * 9 * MIB if args.extended
             else (len(CASES) * 3 + 1) * 182 * MIB + 3 * (512 + 32) * MIB)
    if upper > LIMIT:
        parser.error("matrix exceeds output cap")
    root.mkdir(exist_ok=False)
    cpus = sorted(os.sched_getaffinity(0))[:8]
    os.sched_setaffinity(0, cpus)
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    print(f"Output cap {LIMIT:,} bytes, {free:,} bytes free, dir {root}", flush=True)
    rows = []
    for trial in range(1, 2 if args.smoke else 4):
        labels = extended_cases if args.extended else list(CASES)
        offset = (trial - 1) * (1 if args.extended else 4)
        order = labels[offset:] + labels[:offset]
        if not (args.smoke or args.extended):
            if trial == 2:
                order.insert(0, "W0")
                order.append("W1")
            else:
                order.insert(0, "W1")
                order.append("W0")
        for name in order:
            print(f"{trial} {name}", flush=True)
            if name == "W0":
                case = root / f"{trial}-W0"
                cpu_begin = time.process_time_ns()
                ssd_run(case, 512, 8)
                cpu = time.process_time_ns() - cpu_begin
                result = json.loads((case / "result.json").read_text())
                first, last = result["samples"][0], result["samples"][-1]
                rows.append(dict(case=name, trial=trial, status=result["status"],
                                 durable_Bps=last["committed_bytes"] * 10**9 / (last["time_ns"] - first["time_ns"]),
                                 cpu_s_per_gib=cpu / 10**9 * (1 << 30) / last["committed_bytes"],
                                 disk_written_bytes=last["disk_written_bytes"] - first["disk_written_bytes"],
                                 data_sync=[s["sync_ns"] for s in result["samples"][1:]],
                                 ssd_before=first, ssd_after=last))
            elif name == "W1":
                case = root / f"{trial}-W1"
                case.mkdir()
                before = sample(disk, sensor)
                row = backend(case, "B2")
                after = sample(disk, sensor)
                row.update(ssd_before=before, ssd_after=after,
                           disk_written_bytes=after["disk_written_bytes"] - before["disk_written_bytes"],
                           completed_Bps=None, input_Bps=None)
                row.update(case=name, trial=trial, status="verified",
                           cpu_s_per_gib=row["cpu_ns"] / 10**9 * (1 << 30) / row["payload_bytes"])
                rows.append(row)
            else:
                rows.append(run_case(root, binary, name, trial, *CASES[name], (disk, sensor), args.extended))
    if not args.extended:
        rows.append(run_case(root, binary, "uring", 1, "baseline", "split", "current", (disk, sensor)))
    meta = dict(base_sha="060f5b8a25a790560ab32abc5ffea3181967be0e", binary_sha256=digest,
                cpu_affinity=cpus, kernel=platform.release(), free_bytes=free, byte_limit=LIMIT,
                output=str(root), output_upper_estimate=upper,
                filesystem=subprocess.check_output(["findmnt", "-T", str(root)], text=True),
                nvme=(controller / "model").read_text().strip(),
                firmware=(controller / "firmware_rev").read_text().strip())
    (root / "results.json").write_text(json.dumps(dict(environment=meta, rows=rows), indent=2) + "\n")
    columns = sorted(set().union(*(row.keys() for row in rows)) - {"command", "writer_profile"})
    with (root / "results.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows({key: json.dumps(value) if isinstance(value, (dict, list)) else value
                          for key, value in row.items()} for row in rows)


if __name__ == "__main__":
    main()
