"""Replay Rust enqueue traces through the Phase 1 Global surrogate."""
import argparse
from collections import Counter
from itertools import combinations
import json
from pathlib import Path
from statistics import median
import tomllib

from .model import Engine, Frame, Pool, Scenario, Service, MIB, MS, WAIT
from .policies import baseline, demands


def parse_trace(path):
    events = [json.loads(line) for line in path.read_text().splitlines()]
    if not events:
        raise ValueError("trace is incomplete")
    footer = events.pop()
    if (footer.get("event") != "trace_end" or footer["failed"] or footer["omitted"]
            or footer["written"] != len(events)):
        raise ValueError("trace is incomplete")
    return events


def key(row):
    return row["stream"], row["sequence"]


def events_of(events, name):
    return [row for row in events if row["event"] == name]


def errors(actual, predicted):
    overlap = actual.keys() & predicted.keys()
    signed = sorted(predicted[k] - actual[k] for k in overlap)
    absolute = sorted(map(abs, signed))
    return dict(matched=len(overlap), actual_only=len(actual.keys() - predicted.keys()),
                predicted_only=len(predicted.keys() - actual.keys()),
                median_signed_ns=median(signed) if signed else None,
                median_absolute_ns=median(absolute) if absolute else None,
                max_absolute_ns=max(absolute) if absolute else None)


def occupancy(intervals):
    points = {}
    for size, begin, end in intervals:
        points.setdefault(size, Counter())[begin] += 1
        points[size][end] -= 1
    result = {}
    for size, changes in points.items():
        held = peak = slot_ns = 0
        previous = min(changes)
        for stamp, delta in sorted(changes.items()):
            slot_ns += (stamp - previous) * held
            held += delta
            peak = max(peak, held)
            previous = stamp
        if held:
            raise ValueError("unbalanced spill intervals")
        result[str(size)] = dict(peak=peak, slot_ns=slot_ns)
    return result


class Replay(Engine):
    def __init__(self, scenario, io, copies, sync, scheduling_margin, return_margin):
        self.io = io
        self.copies = copies
        self.sync = sync
        self.scheduling_margin = scheduling_margin
        self.return_margin = return_margin
        super().__init__(scenario, trace=True)

    def duration(self, i, copy, assumed=False):
        if not assumed:
            measured = self.copies if copy else self.io
            if i in measured:
                return measured[i]
        return super().duration(i, copy, assumed)

    def copy_plan(self):
        requested = demands(self)
        tasks = sorted((i for kind, i in self.actions() if kind == "copy" and i in requested),
                       key=lambda i: (requested[i] - self.frames[i].ack, i))
        work = 0
        latest = float("inf")
        for i in tasks:
            work += self.duration(i, True, assumed=True) + self.scheduling_margin
            latest = min(latest, requested[i] - self.frames[i].ack - self.return_margin - work)
        return tasks, latest

    def wake_times(self):
        wakes = super().wake_times()
        if self.scheduling_margin or self.return_margin:
            _, latest = self.copy_plan()
            if self.time < latest < float("inf"):
                wakes.append(latest)
        return wakes

    def step_budget(self):
        tasks, latest = self.copy_plan()
        self.step(("copy", tasks[0]) if tasks and latest <= self.time else WAIT)

    def start_sync(self):
        if self.sync:
            self.writer = -1
            duration = self.sync[min(self.syncs, len(self.sync) - 1)]
            self.push(self.storage_end(duration), "fdatasync")
        else:
            super().start_sync()


def run(case):
    cfg = tomllib.loads((case / "config.toml").read_text())
    events = parse_trace(case / "run-copy-trace.jsonl")
    summary = json.loads((case / "run-summary.json").read_text())
    verdict = json.loads((case / "verify.json").read_text())
    if not verdict["ok"]:
        raise ValueError("independent verifier did not pass")

    capture = sorted(events_of(events, "capture_push"), key=lambda r: (r["at_ns"], key(r)))
    index = {key(r): i for i, r in enumerate(capture)}
    if len(index) != len(capture):
        raise ValueError("capture IDs are not unique")
    sources = cfg["sources"]
    frames = tuple(Frame(
        r["stream"], r["at_ns"], r["size_bytes"],
        None if r["lease_deadline_ns"] is None else r["lease_deadline_ns"] - r["at_ns"],
        sources[r["stream"]].get("return_delay_us", 0) * 1000,
        borrowed=r["detail"]["borrowed"]) for r in capture)
    pools = tuple(Pool(s["dma_buffers"], s.get("reserve_buffers", 0)) for s in sources)
    spill = tuple(sorted((s["frame_bytes"], s["buffers"]) for s in cfg["spill"]))
    started = {index[key(r)]: r for r in events_of(events, "copy_start")}
    ended = {index[key(r)]: r for r in events_of(events, "copy_completion") if r["detail"]["success"]}
    submitted = {index[key(r)]: r["at_ns"] for r in events_of(events, "writer_submit")}
    completed = {index[key(r)]: r["at_ns"] for r in events_of(events, "writer_completion")
                 if r["detail"]["success"]}
    reaps = {tuple(r["token"]): r for r in events_of(events, "pool_reap")}
    ack = {i: reaps[tuple(r["token"])]["ready_ns"] for i, r in enumerate(capture)
           if frames[i].borrowed and tuple(r["token"]) in reaps}
    observed_reap = {i: reaps[tuple(r["token"])]["at_ns"] for i, r in enumerate(capture) if i in ack}
    real_spill = [(r["detail"]["spill_class_bytes"], r["at_ns"],
                   reaps[tuple(r["detail"]["spill_token"])]["ready_ns"]) for r in started.values()]
    fault = cfg.get("faults", {})
    stalls = ()
    if fault.get("stall_duration_ms"):
        start = fault["stall_at_ms"] * MS
        stalls = ((start, start + fault["stall_duration_ms"] * MS),)

    policy = baseline("global")
    copy_cfg = cfg.get("copy_profile", {})
    min_copy = copy_cfg.get("assumed_min_bytes_per_sec", 10**9)
    copy_prior = (MIB * 10**9 + min_copy - 1) // min_copy
    # Phase 0 B3 high p99 profile, fixed before these trace cases.
    service = Service(547_800, copy_prior, 5_125_975, cfg["record"].get("commit_messages", 4),
                      certified=False)
    scenario = Scenario(case.name, frames, pools, spill, cfg["record"]["queue_capacity"],
                        service, stalls, public_arrivals=False)
    real_copies = {i: r["at_ns"] for i, r in started.items()}
    measured_sync = [max(1, e["end_ns"] - e["at_ns"]) for e in events_of(events, "storage_sync")]
    outputs = []
    for mode in ("phase0_prior", "conditional_same_run", "conditional_with_rust_margins"):
        fit = mode != "phase0_prior"
        margins = mode == "conditional_with_rust_margins"
        io = {i: max(1, stamp - submitted[i]) for i, stamp in completed.items()} if fit else {}
        copies = {i: max(1, r["detail"]["elapsed_ns"]) for i, r in ended.items()} if fit else {}
        if not fit and fault.get("copy_delay_us"):
            # Fault delay is constant per job, not proportional to frame size.
            copies = {i: (f.size * copy_prior + MIB - 1) // MIB + fault["copy_delay_us"] * 1000
                      for i, f in enumerate(frames)}
        model = Replay(scenario, io, copies, measured_sync if fit else [],
                       copy_cfg.get("scheduling_margin_us", 200) * 1000 if margins else 0,
                       copy_cfg.get("return_margin_us", 200) * 1000 if margins else 0)
        while not model.done():
            if margins:
                model.step_budget()
            else:
                policy(model)
        model.finish()

        def stamps(name):
            return {r["frame"]: r["ns"] for r in events_of(model.trace, name)}

        claims = stamps("writer_claim")
        starts = stamps("copy_claim")
        releases = stamps("spill_release")
        position = {i: rank for rank, i in enumerate(sorted(submitted, key=submitted.get))}
        shared = [i for i in claims if i in position]
        inverted = sum(position[i] > position[j] for i, j in combinations(shared, 2))
        model_spill = [(r["spill_class"], r["ns"], releases[r["frame"]])
                       for r in events_of(model.trace, "copy_claim")]
        outputs.append(dict(mode=mode,
                            copied=errors(real_copies, starts),
                            observed_only_copies=[
                                dict(stream=capture[i]["stream"], sequence=capture[i]["sequence"],
                                     size_bytes=frames[i].size, forced=started[i]["detail"]["forced"])
                                for i in sorted(real_copies.keys() - starts.keys())
                            ],
                            predicted_only_copies=[key(capture[i]) for i in sorted(
                                starts.keys() - real_copies.keys())],
                            copy_actual_bytes=summary["copy_bytes"], copy_predicted_bytes=model.copy_bytes,
                            writer_claim=errors(submitted, claims), claim_pair_inversions=inverted,
                            claim_pairs=len(shared) * (len(shared) - 1) // 2,
                            dma_ack_eligible=errors(ack, stamps("ack_ready")),
                            dma_reap_observed=errors(observed_reap, stamps("ack_ready")),
                            spill_actual=occupancy(real_spill), spill_predicted=occupancy(model_spill),
                            loss_model=dict(Counter(model.loss.values())), late_model=len(model.late),
                            modeled_copy_attempts_without_spill=model.spill_failures))
    loss = sum(s["before_accept_loss"] + s["after_accept_loss"] for s in summary["sources"])
    return dict(case=case.name, capture_rows=len(capture), actual_lost=loss,
                outside_capture_trace=sum(s["planned"] for s in summary["sources"]) - len(capture),
                actual_copy_no_spill_attempts=len(events_of(events, "spill_unavailable")),
                actual_deadline_exceeded=sum(d["pool"]["deadline_exceeded"] for d in summary["devices"]),
                independent_verifier=verdict["ok"], results=outputs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cases", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    results = [run(case) for case in args.cases]
    with args.out.open("x") as stream:
        json.dump(results, stream, indent=2)


if __name__ == "__main__":
    main()
