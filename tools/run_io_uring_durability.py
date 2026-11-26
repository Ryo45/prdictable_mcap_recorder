#!/usr/bin/env python3
"""Linux/NVMe Recorder smoke test and opt-in io_uring durability comparison."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from bench_bounded_ssd import device_for
from bench_durability import MIB, run_trial


def save(out, environment, rows):
    temporary = out / "results.json.tmp"
    temporary.write_text(json.dumps(dict(environment=environment, rows=rows), indent=2) + "\n")
    temporary.replace(out / "results.json")
    columns = [key for key, value in rows[0].items() if not isinstance(value, (dict, list))]
    temporary = out / "results.csv.tmp"
    with temporary.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(out / "results.csv")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, default=Path("target/release/recorder"))
    parser.add_argument("--out", type=Path, required=True, help="new directory on the target NVMe")
    parser.add_argument("--compare", action="store_true", help="run 24 comparisons after the smoke test")
    parser.add_argument("--depth", type=int, choices=(2, 4, 8, 16), default=4)
    args = parser.parse_args()
    binary = args.recorder.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("recorder must be an executable file")
    out = args.out.resolve()
    disk, sensor, controller = device_for(out.parent)
    # Eight 1 MiB frames in smoke, up to 180 frames per comparison.
    cap = (16 + (24 * 182 if args.compare else 0)) * MIB
    free = shutil.disk_usage(out.parent).free
    if free < 2 * cap:
        parser.error(f"requires at least {2 * cap:,} bytes free")
    out.mkdir(exist_ok=False)
    cpus = sorted(os.sched_getaffinity(0))[:8]
    os.sched_setaffinity(0, cpus)
    with binary.open("rb") as stream:
        binary_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    environment = dict(binary_sha256=binary_sha256, kernel=platform.release(), cpus=cpus,
                       depth=args.depth, planned_output_limit_bytes=cap, free_bytes=free,
                       nvme=disk.name, firmware=(controller / "firmware_rev").read_text().strip(),
                       mount=subprocess.check_output(["findmnt", "-T", str(out)], text=True))
    print(f"Output cap {cap:,} bytes, available {free:,} bytes, destination {out}", flush=True)
    (out / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    try:
        smoke = run_trial(out, binary, binary, "overlap-io-uring", 1, "smoke",
                          disk, sensor, backend="io-uring", depth=args.depth)
    except RuntimeError as error:
        log = out / "smoke-1-overlap-io-uring" / "record.log"
        if log.exists() and ("Operation not permitted" in log.read_text()
                             or "Permission denied" in log.read_text()):
            parser.exit(2, f"io_uring recording was denied. See {log}\n")
        raise error
    save(out, environment, [smoke])
    if smoke["loss"] or smoke["deadlines"] or smoke["unknown"]:
        parser.exit(2, "Smoke test recorded loss, deadline overruns, or Unknown. See results.json.\n")
    if not args.compare:
        return
    modes = [(mode, backend) for mode in ("barrier", "overlap")
             for backend in ("sync", "io-uring")]
    rows = []
    for rate in ("paced", "fast"):
        for trial in range(1, 4):
            shift = (trial - 1 + (rate == "fast")) % len(modes)
            for mode, backend in modes[shift:] + modes[:shift]:
                label = f"{mode}-{backend}"
                print(rate, trial, label, flush=True)
                rows.append(run_trial(out, binary, binary, label, trial, rate, disk, sensor,
                                      backend=backend, depth=args.depth))
                save(out, environment, [smoke, *rows])


if __name__ == "__main__":
    main()
