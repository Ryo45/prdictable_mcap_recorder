"""Small, input-paced V3 benchmark. The null case only measures the pipeline."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorder", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--preset-toml")
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--policy", choices=("always-copy", "always-borrow", "deadline-aware",
                                            "deadline-aware-reserve"), default="deadline-aware")
    args = parser.parse_args()
    if not 0 < args.seconds <= 60 or not 0 < args.repeats <= 10:
        parser.error("use 0 < seconds <= 60 and 1 <= repeats <= 10")
    args.out.mkdir(parents=True, exist_ok=False)
    preset = args.preset_toml or Path("configs/v3-smoke.toml").read_text()
    preset = preset.replace('policy = "deadline-aware"', f'policy = "{args.policy}"', 1)
    comparison = []
    for mode in ("sync", "null"):
        for trial in range(1, args.repeats + 1):
            case = args.out / f"{mode}-{trial}"
            case.mkdir()
            config = case / "config.toml"
            config.write_text(preset.replace('writer = "sync"', f'writer = "{mode}"', 1))
            data = case / "run.mcap"
            invoke = [str(args.recorder), "record", "--config", str(config), "--output",
                      str(data), "--duration-sec", str(args.seconds)]
            with (case / "record.log").open("w") as log:
                subprocess.run(invoke, stdout=log, stderr=subprocess.STDOUT, check=True)
            summary = case / "run-summary.json"
            report = json.loads(summary.read_text())
            if mode == "sync":
                for action in (
                    ["verify", "--input", str(data), "--report", str(summary)],
                    ["recover", "--input", str(data), "--dry-run"],
                ):
                    with (case / f"{action[0]}.json").open("w") as log:
                        subprocess.run([str(args.recorder), *action], stdout=log, check=True)
                recovery = json.loads((case / "recover.json").read_text())
                if recovery["recovery_needed"]:
                    raise RuntimeError(f"invalid recovery evidence in {case}")
            process = report["process"]
            committed = report["committed_payload_bytes"]
            comparison.append({
                "mode": mode,
                "trial": trial,
                "seconds": args.seconds,
                "policy": args.policy,
                "input_bytes_per_sec": report["plan"]["input_bytes_per_sec"],
                "committed_payload_bytes_per_sec": report["committed_payload_bytes_per_sec"],
                "written_payload_bytes": report["written_payload_bytes"],
                "copy_bytes": report["copy_bytes"],
                "copy_bytes_per_committed_gib": report["copy_bytes"] * (1 << 30) / committed
                if mode == "sync" and committed else None,
                "queue_peak": report["queue_peak"],
                "peak_rss_bytes": process["peak_rss_bytes"],
                "process_cpu_pct": process["cpu_time_ns"] / process["wall_ns"] * 100,
                "known_loss": sum(s["before_accept_loss"] + s["after_accept_loss"] for s in report["sources"]),
                "unknown": sum(s["unknown"] for s in report["sources"]),
                "timings_ns": report["timings_ns"],
            })
    (args.out / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")


if __name__ == "__main__":
    main()
