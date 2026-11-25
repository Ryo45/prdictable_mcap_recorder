"""Fault and post-write verification for the isolated Writer ablation build."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from bench_writer_ablation import invoke, preset


def file_records(data):
    """Walk unchunked records while preserving body offsets for the mutations."""
    magic = b"\x89MCAP0\r\n"
    if data[:8] != magic or data[-8:] != magic:
        raise ValueError("bad file magic")
    offset = 8
    while offset < len(data) - 8:
        size = int.from_bytes(data[offset + 1:offset + 9], "little")
        end = offset + 9 + size
        if end > len(data) - 8:
            raise ValueError("truncated record")
        yield data[offset], offset + 9, end
        offset = end


def decoded(path):
    return json.JSONDecoder().raw_decode(path.read_text())[0]


def trial(binary, root, mode, vector):
    case = root / (mode + "-" + vector)
    case.mkdir()
    env = os.environ | dict(RECORDER_ABLATION=mode, RECORDER_ABLATION_IO=vector,
                            RECORDER_WRITER_PROFILE="0", RECORDER_COPY_TRACE="0")

    def fixture(name, fault="", queue_full=False, pool_exhausted=False):
        directory = case / name
        directory.mkdir()
        config = directory / "config.toml"
        config_text = preset("C0", "current", short=not fault, fault=fault)
        if fault:
            config_text = config_text.replace("frame_bytes = 1048576", "frame_bytes = 65536")
            config_text = config_text.replace("fps = 500", "fps = 40")
        if pool_exhausted:
            config_text = config_text.replace("dma_buffers = 64", "dma_buffers = 1")
            config_text = config_text.replace('policy = "deadline-aware"', 'policy = "always-borrow"')
            config_text = config_text.replace('copy_scheduling = "global"', 'copy_scheduling = "legacy"')
        if queue_full:
            config_text = config_text.replace("queue_capacity = 64", "queue_capacity = 2")
        config.write_text(config_text)
        data = directory / "run.mcap"
        argv = [str(binary), "record", "--config", str(config), "--output", str(data),
                "--duration-sec", "0.11" if not (queue_full or pool_exhausted) else "0.22", "--allow-unverified"]
        return argv, directory, data

    def record(arguments, directory, success):
        code = invoke(arguments, directory / "record.log", env)
        assert (code == 0) == success, directory
        return decoded(directory / "run-summary.json")

    argv, directory, data = fixture("short")
    report = record(argv, directory, True)
    assert report["all_resources_returned"]
    verifier = [str(binary), "verify", "--input", str(data),
                "--report", str(directory / "run-summary.json")]
    assert invoke(verifier, directory / "verify.json") == 0
    assert decoded(directory / "verify.json")["verified"] == sum(s["planned"] for s in report["sources"])

    changed = bytearray(data.read_bytes())
    records = list(file_records(changed))
    _, start, end = next(r for r in records if r[0] == 5)
    _, crc_pos, _ = next(r for r in records if r[0] == 15)
    crc_available = int.from_bytes(changed[crc_pos:crc_pos + 4], "little") != 0
    assert crc_available == (mode != "loss-only-no-crc")
    changed[end - 1] ^= 1
    altered = directory / "altered.mcap"
    altered.write_bytes(changed)
    # Derive paths explicitly so both files are checked against the same evidence.
    assert invoke([*verifier[:2], "--input", str(altered), "--report", verifier[-1],
                   "--loss-log", str(directory / "run-loss.jsonl"),
                   "--commit-log", str(directory / "run-commits.jsonl")],
                  directory / "altered.json") != 0
    checked = decoded(directory / "altered.json")
    if not crc_available:
        assert checked["corrupt_frames"] == 1
    else:
        assert any("crc" in error.lower() for error in checked["errors"]), checked
    assert start < end

    for name, fault in (("eio", 'io_error_request = 0\nio_error = "eio"'),
                        ("sync-fail", "sync_fail_group = 0")):
        argv, directory, data = fixture(name, fault)
        report = record(argv, directory, False)
        assert report["all_resources_returned"] and not report["final_sync"]
        assert sum(s["committed"] for s in report["sources"]) == 0
        assert sum(s["unknown"] + s["after_accept_loss"] for s in report["sources"]) > 0

    argv, directory, data = fixture("queue-full", "stall_at_ms = 1\nstall_duration_ms = 80", queue_full=True)
    report = record(argv, directory, True)
    assert sum(s["before_accept_loss"] for s in report["sources"]) > 0
    assert "queue_full" in (directory / "run-loss.jsonl").read_text()
    assert invoke([str(binary), "verify", "--input", str(data), "--report",
                   str(directory / "run-summary.json")], directory / "verify.json") == 0

    argv, directory, data = fixture("pool-exhausted", "stall_at_ms = 1\nstall_duration_ms = 80",
                                    pool_exhausted=True)
    report = record(argv, directory, True)
    assert "dma_pool_exhausted" in (directory / "run-loss.jsonl").read_text()
    assert invoke([str(binary), "verify", "--input", str(data), "--report",
                   str(directory / "run-summary.json")], directory / "verify.json") == 0

    argv, directory, data = fixture("kill")
    argv[argv.index("--duration-sec") + 1] = "4"
    with (directory / "record.log").open("x") as log:
        with subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env) as process:
            try:
                cutoff = time.monotonic() + 3
                journal = directory / "run-commits.jsonl"
                while time.monotonic() < cutoff and process.poll() is None:
                    if data.exists() and data.stat().st_size > 512 * 1024 and (
                        mode != "baseline" or journal.exists() and journal.stat().st_size
                    ):
                        break
                    time.sleep(0.01)
                else:
                    raise AssertionError("writer did not reach crash checkpoint")
            finally:
                if process.poll() is None:
                    process.kill()
    assert invoke([str(binary), "recover", "--input", str(data), "--dry-run"],
                  directory / "recovery.json") == 0
    recovery = decoded(directory / "recovery.json")
    assert recovery["recovery_needed"] and recovery["readable_messages"]
    if mode != "baseline":
        assert recovery["committed_with_evidence"] == 0
        assert recovery["recovered_but_unconfirmed"] == recovery["readable_messages"]
    return dict(mode=mode, io=vector, short_write=True, corruption=True, crc_available=crc_available,
                eio=True, sync_fail=True, queue_full=True, pool_exhausted=True, crash_readable=recovery["readable_messages"],
                crash_committed=recovery["committed_with_evidence"],
                crash_unknown=recovery["recovered_but_unconfirmed"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(exist_ok=False)
    binary = args.recorder.resolve(strict=True)
    rows = [trial(binary, args.out, mode, io)
            for mode in ("baseline", "loss-only", "loss-only-no-crc")
            for io in ("split", "pwritev")]
    (args.out / "result.json").write_text(json.dumps(rows, indent=2) + "\n")


if __name__ == "__main__":
    main()
