"""Run opt-in diagnostic archive on two bounded recorder traces and a Writer load."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

MIB = 1 << 20


def preset(name):
    if name == "aligned":
        return Path("configs/multi-source-ci.toml").read_text(), "0.35"
    mixed = name == "mixed"
    stall = "[faults]\nstall_at_ms = 1\nstall_duration_ms = 55\n" if mixed else ""
    config = f"""[record]
policy = "deadline-aware"
copy_scheduling = "global"
writer = "sync"
queue_capacity = {12 if mixed else 64}
memory_budget_bytes = {160 * MIB}
max_output_bytes = {250 * MIB}
commit_interval_ms = 80
commit_messages = 16
commit_bytes = {16 * MIB}
metrics_interval_ms = 2
seed = 1

[storage_profile]
max_io_us = 2500

[copy_profile]
assumed_min_bytes_per_sec = 1000000000
scheduling_margin_us = 400
return_margin_us = 100

{stall}
"""
    pairs = [(MIB, 45), (MIB // 4, 180)] * 2 if mixed else [(MIB, 400)] * 2
    for i, (size, fps) in enumerate(pairs):
        config += f"""
[[sources]]
id = "camera_{i}"
kind = "mock_v4l2"
model = "fixed-deadline"
frame_bytes = {size}
fps = {fps}
phase_us = {1500 if mixed else i * 1250}
dma_buffers = {16 if mixed else 48}
max_hold_ms = {24 if mixed else 120}
return_delay_us = 300
"""
    for size, buffers in (((MIB // 4, 3), (MIB, 2)) if mixed else ((MIB, 12),)):
        config += f"\n[[spill]]\nframe_bytes = {size}\nbuffers = {buffers}\n"
    return config, "0.20" if mixed else "0.27"


def command(argv, path, env=None):
    with path.open("x") as log:
        subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, env=env, check=True, timeout=45)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    binary, root = args.recorder.resolve(strict=True), args.out.resolve()
    if shutil.disk_usage(root.parent).free < 2 * (1 << 30):
        parser.error("requires 2 GiB free")
    root.mkdir(exist_ok=False)
    trials = (("aligned", "profile-trace"), ("mixed", "profile-trace"),
              ("writer", "plain"), ("writer", "profile"),
              ("writer", "profile"), ("writer", "plain"))
    with binary.open("rb") as stream:
        binary_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    manifest = dict(binary_sha256=binary_sha256, trials=[])
    for i, (name, mode) in enumerate(trials):
        trial = root / f"{name}-{i + 1}-{mode}"
        trial.mkdir()
        cfg, duration = preset(name)
        config = trial / "config.toml"
        config.write_text(cfg)
        data, summary = trial / "run.mcap", trial / "run-summary.json"
        argv = [str(binary), "record", "--config", str(config), "--output", str(data),
                "--duration-sec", duration, "--allow-unverified"]
        env = os.environ | {"RECORDER_WRITER_PROFILE": "0", "RECORDER_COPY_TRACE": "0"}
        if mode.startswith("profile"):
            env["RECORDER_WRITER_PROFILE"] = "1"
        if mode.endswith("trace"):
            env["RECORDER_COPY_TRACE"] = "1"
        command(argv, trial / "record.log", env)
        command([str(binary), "verify", "--input", str(data), "--report", str(summary)],
                trial / "verify.json")
        row = json.loads(summary.read_text())
        proof = json.loads((trial / "verify.json").read_text())
        if not proof["ok"]:
            raise ValueError(f"independent verification failed for {trial}")
        manifest["trials"].append(dict(case=trial.name, command=argv,
            profile=row["writer"]["profile"], journal=row["monitor"]["commit_profile"],
            verifier=proof["ok"], planned=sum(s["planned"] for s in row["sources"]),
            verified=proof["committed_and_verified"], copy_bytes=row["copy_bytes"],
            queue_peak=row["queue_peak"], timings=row["timings_ns"],
            process=row["process"], durable_Bps=row["committed_payload_bytes_per_sec"]))
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
