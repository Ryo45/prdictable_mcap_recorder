"""Paired offline/online controller comparison; no recordings are created."""
import argparse
import csv
from dataclasses import asdict, replace
import json
from pathlib import Path
from time import perf_counter_ns, process_time_ns

from .controllers import Settings, controller
from .model import Engine, MS, Service, run
from .oracle import solve
from .policies import baseline
from .profiles import observed_profiles
from .scenarios import fixed, paced

BASE = "3ce438cb3c61f563a531e75f63bfdef4838b80b8"
MODES = ("global", "legacy", "always-borrow", "always-copy", "safe-greedy", "safe-dpp", "bounded-mpc")
TRAIN = (11, 23)
TEST = (101, 103, 107)


def p99(values):
    return sorted(values)[(len(values) * 99 + 99) // 100 - 1] if values else None


def evaluate(case, seed, profile, mode, settings, oracle=None):
    start = perf_counter_ns()
    cpu = process_time_ns()
    stats = None
    if mode == "oracle":
        state = oracle.outcome
    elif mode in MODES[:4]:
        state = run(case, baseline(mode))
    else:
        policy, stats = controller(mode, settings)
        state = run(case, policy)
    if mode == "oracle":
        wall = cpu_ns = None
    else:
        wall = perf_counter_ns() - start
        cpu_ns = process_time_ns() - cpu
    known, unknown, late, copied = state.cost()
    hard_ok = (known, unknown, late) == (0, 0, 0)
    committed = sum(value == "committed" for value in state.status.values())
    assert committed + known + unknown == len(case.frames)
    oracle_optimal = oracle is not None and oracle.status == "optimal"
    return dict(
        scenario=case.name, seed=seed, profile=profile, base_sha=BASE,
        binary_sha256=None, mode=mode, planner_verdict=None,
        arrivals=len(case.frames), accepted=len(case.frames) - known, committed=committed,
        known_preaccept_loss=known, known_postaccept_loss=0, unreported_missing=0,
        unknown_durability=unknown, duplicate=0, corruption=None, deadline_exceeded=late,
        worst_deadline_violation_ns=max(state.late.values(), default=0),
        copy_bytes=copied, per_source_copy=state.per_source_copy,
        spill_class_failures=state.spill_failures,
        queue_full=list(state.loss.values()).count("queue_full"),
        pool_exhausted=list(state.loss.values()).count("pool_exhausted"),
        contract_violations=state.contract_violations,
        reserve_activations=state.reserves, controller_time_ns_p99=p99(stats.timings) if stats else None,
        max_decision_ns=max(stats.timings, default=None) if stats else None, queue_lock_ns_p99=None,
        deadline_headroom_min_ns=min(state.headroom, default=None),
        fallback_count=stats.fallback_count if stats else None,
        writer_completion_p99=p99(state.completion),
        accepted_to_data_sync_p99=p99(state.sync_latency), peak_rss=None, cpu_ns=cpu_ns,
        wall_ns=wall, simulated_drain_end_ns=state.time, objective=list(state.cost()),
        hard_ok=hard_ok, nodes=stats.nodes if stats else oracle.nodes if mode == "oracle" else None,
        certificates=[dict(status=k[0], cause=k[1], count=v) for k, v in stats.certificates.items()]
        if stats else None, oracle_status=oracle.status if oracle else None,
        oracle_copy_gap_bytes=copied - oracle.outcome.copy_bytes if hard_ok and oracle_optimal else None,
    )


def tuning():
    cases = [(seed, case) for seed in TRAIN for case in fixed(seed)
             if case.name in ("M1-common-3", "H1-resume-model", "H4-queue-model", "phase-jitter-8")]
    selected = {}
    history = []
    for mode, variants in (
        ("safe-dpp", [Settings(v=v) for v in (0.0, 0.5, 2.0)]),
        ("bounded-mpc", [Settings(horizon_ns=h * MS, mpc_nodes=n)
                         for h, n in ((25, 32), (40, 80), (60, 128))]),
    ):
        ranking = []
        for settings in variants:
            rows = [evaluate(case, seed, "fixed-train", mode, settings) for seed, case in cases]
            score = tuple(sum(row["objective"][i] for row in rows) for i in range(4)) + (
                sum(row["nodes"] for row in rows),)
            history.append(dict(mode=mode, settings=asdict(settings), score=score))
            ranking.append((score, settings))
        selected[mode] = min(ranking, key=lambda entry: entry[0])[1]
    return selected, history


def experiments(seed):
    for case in fixed(seed):
        yield "fixed", case
    profiles = {
        "analytical-0.8GBps": Service(1_310_720, 750_000, 2 * MS, 8),
        "analytical-1.25GBps": Service(838_861, 750_000, 2 * MS, 8),
        "analytical-2GBps": Service(524_288, 750_000, 2 * MS, 8),
        **observed_profiles(),
    }
    for profile, service in profiles.items():
        for rate in (0.5, 1.0, 1.5, 2.1):
            yield profile, paced(rate, service, seed=seed)
    for stall in (100, 200, 500):
        yield "analytical-1.25GBps", paced(1.0, profiles["analytical-1.25GBps"], stall, seed)


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    settings, history = tuning()
    save(args.out / "tuning.json", dict(train=TRAIN, holdout=TEST, trials=history,
                                       selected={mode: asdict(s) for mode, s in settings.items()}))
    rows = []
    inputs = []
    for seed in TEST:
        for profile, case in experiments(seed):
            inputs.append(dict(seed=seed, profile=profile, scenario=asdict(case)))
            oracle = None
            if profile == "fixed" and len(case.frames) <= 16:
                start = perf_counter_ns()
                cpu = process_time_ns()
                limit = 2000 * MS if case.name == "impossible-common-4" else 250 * MS
                oracle = solve(Engine(case), time_cap_ns=limit, node_cap=12_000)
                oracle_row = evaluate(case, seed, profile, "oracle", None, oracle)
                oracle_row.update(wall_ns=perf_counter_ns() - start, cpu_ns=process_time_ns() - cpu)
                rows.append(oracle_row)
            rows.extend(evaluate(case, seed, profile, mode, settings.get(mode, Settings()), oracle)
                        for mode in MODES)
    save(args.out / "scenarios.json", inputs)
    save(args.out / "comparison.json", rows)
    with (args.out / "comparison.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows({key: json.dumps(value, separators=(",", ":"))
                          if isinstance(value, (dict, list)) else value for key, value in row.items()}
                         for row in rows)
    cases = [case for _, case in experiments(TEST[0])]
    for name in ("M1-common-3", "H1-resume-model", "H4-queue-model"):
        case = next(case for case in cases if case.name == name)
        save(args.out / f"{name}-scenario.json", asdict(case))
        for mode in ("global", "safe-greedy", "safe-dpp", "bounded-mpc"):
            policy = baseline(mode) if mode == "global" else controller(mode, settings.get(mode, Settings()))[0]
            save(args.out / f"{name}-{mode}-trace.json", run(case, policy, trace=True).trace)
    print(f"{len(rows)} reconciled runs in {args.out}")


if __name__ == "__main__":
    main()
