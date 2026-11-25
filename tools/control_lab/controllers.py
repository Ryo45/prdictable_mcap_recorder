"""Online copy decisions. The Writer remains autonomous."""
from dataclasses import dataclass, field
from time import perf_counter_ns

from .model import MIB, MS, WAIT
from .policies import baseline_action


@dataclass
class Certificate:
    status: str
    limiting_resource: str | None = None
    critical_time_ns: int | None = None
    assumptions: tuple = ()


@dataclass
class Projection:
    action: tuple
    state: object
    risk: list


@dataclass
class Stats:
    timings: list = field(default_factory=list)
    certificates: dict = field(default_factory=dict)
    fallback_count: int = 0
    nodes: int = 0


def potential(engine):
    pool_risk = 0
    for slots in engine.work.slots:
        active = sum(slot.phase != "reserve" for slot in slots)
        held = sum(slot.phase in ("held", "returning") for slot in slots)
        if active:
            pool_risk += (held / active) ** 2
    return (pool_risk + 0.25 * sum(
        (list(engine.work.spill.values()).count(size) / count) ** 2
        for size, count in engine.scenario.spill if count)) / 2


def projection(engine, action, horizon, node_cap):
    state = engine.fork()
    state.trace = None
    risk = [(state.time, potential(state))]
    nodes = 0
    while not state.done() and state.time <= horizon and nodes < node_cap:
        state.step(action if nodes == 0 else baseline_action(state))
        risk.append((state.time, potential(state)))
        nodes += 1
    return Projection(action, state, risk), nodes


def dpp_score(candidate, wait, size, v):
    times = sorted({t for t, _ in candidate.risk + wait.risk})
    levels = [[v for stamp, v in path if stamp <= t][-1]
              for t in times for path in (candidate.risk, wait.risk)]
    return v * size / MIB + sum(a - b for a, b in zip(levels[::2], levels[1::2]))


def safe(state):
    return state.done() and state.cost()[:3] == (0, 0, 0)


def shield(engine, horizon, node_cap):
    # Announced arrivals, service and stall schedule are hard inputs only in
    # synthetic certified fixtures. Unknown timing cannot license a skip.
    scenario = engine.scenario
    if not scenario.service.certified or not scenario.public_arrivals:
        return [], None, 0, Certificate("Unknown", "unqualified_service")
    forecasts = []
    nodes = 0
    for action in engine.actions():
        forecast, spent = projection(engine, action, horizon, node_cap - nodes)
        nodes += spent
        forecasts.append(forecast)
        if nodes >= node_cap:
            break
    wait = next((item for item in forecasts if item.action == WAIT), None)
    witnesses = [item for item in forecasts if safe(item.state) and item.state.time <= horizon]
    if witnesses:
        return witnesses, wait, nodes, Certificate(
            "Feasible", assumptions=("announced trace", "deterministic bounded services",
                                     "autonomous RR writer", "whole-tail witness"))
    if engine.candidates() and all(engine.spill_class(i) is None for i in engine.candidates()):
        cause = "blocked_by_spill"
    else:
        cause = "no_bounded_witness"
    # Necessary lower bound for a still-held frame even on an idle worker,
    # without any release ACK already scheduled.
    for i in engine.candidates():
        due = engine.due(i)
        earliest = engine.time + engine.frames[i].ack + min(
            engine.duration(i, True), engine.duration(i, False))
        if due is not None and earliest > due:
            return [], wait, nodes, Certificate("Infeasible", "individual_deadline", due)
    return [], wait, nodes, Certificate("Unknown", cause)


@dataclass(frozen=True)
class Settings:
    v: float = 0.5
    horizon_ns: int = 40 * MS
    shield_nodes: int = 384
    mpc_nodes: int = 80
    mpc_depth: int = 4
    mpc_width: int = 6
    mpc_time_ns: int = 50 * MS


def greedy(engine, choices):
    return min(choices, key=lambda p: (
        p.state.copy_bytes - engine.copy_bytes,
        p.action != WAIT,
        engine.frames[p.action[1]].size if p.action[0] == "copy" else 0,
        p.action))


def mpc(engine, choices, settings, horizon):
    """Improve already certified root actions with bounded multi-step search."""
    start = perf_counter_ns()
    best = greedy(engine, choices)
    best_cost = (best.state.copy_bytes, potential(best.state))
    beam = [(engine, p.action, p.action) for p in choices]
    nodes = 0
    for _ in range(settings.mpc_depth):
        children = []
        for state, action, first in beam:
            if nodes >= settings.mpc_nodes or perf_counter_ns() - start >= settings.mpc_time_ns:
                return best.action, nodes, True
            candidate = state.fork()
            candidate.trace = None
            candidate.step(action)
            nodes += 1
            if safe(candidate):
                cost = (candidate.copy_bytes, potential(candidate))
                if cost < best_cost:
                    best_cost = cost
                    best = Projection(first, candidate, [])
            elif (not candidate.done() and candidate.time <= horizon
                  and candidate.cost()[:3] == (0, 0, 0) and candidate.copy_bytes <= best_cost[0]):
                children.extend((candidate, action, first) for action in candidate.actions())
        children.sort(key=lambda entry: (entry[0].copy_bytes, potential(entry[0])))
        beam = children[:settings.mpc_width]
    return best.action, nodes, False


def controller(mode, settings=Settings()):
    stats = Stats()

    def decide(engine):
        start = perf_counter_ns()
        horizon = engine.time + settings.horizon_ns
        choices, wait, nodes, certificate = shield(engine, horizon, settings.shield_nodes)
        key = (certificate.status, certificate.limiting_resource)
        stats.certificates[key] = stats.certificates.get(key, 0) + 1
        engine.log("certificate", status=certificate.status, resource=certificate.limiting_resource,
                   critical_ns=certificate.critical_time_ns, assumptions=certificate.assumptions)
        if not choices:
            stats.fallback_count += 1
            action = baseline_action(engine)
        elif mode == "safe-dpp" and wait is not None:
            action = min(choices, key=lambda p: dpp_score(
                p, wait, engine.frames[p.action[1]].size if p.action[0] == "copy" else 0,
                settings.v)).action
        elif mode == "bounded-mpc":
            action, spent, fallback = mpc(engine, choices, settings, horizon)
            nodes += spent
            if fallback:
                stats.fallback_count += 1
                action = greedy(engine, choices).action
        else:
            action = greedy(engine, choices).action
        stats.nodes += nodes
        stats.timings.append(perf_counter_ns() - start)
        engine.step(action)

    return decide, stats
