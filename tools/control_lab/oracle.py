"""Exact search within the simulator's finite decision epochs."""
from dataclasses import dataclass
from time import perf_counter_ns

from .policies import baseline_action


@dataclass(frozen=True)
class OracleResult:
    status: str
    outcome: object
    nodes: int


def solve(initial, node_cap=50_000, time_cap_ns=1_000_000):
    deadline = perf_counter_ns() + time_cap_ns
    best = initial.fork()
    while not best.done():
        best.step(baseline_action(best))
    nodes = 0
    stack = [initial]
    while stack and nodes < node_cap and perf_counter_ns() < deadline:
        state = stack.pop()
        nodes += 1
        if state.cost() >= best.cost():
            continue
        if state.done():
            best = state
            continue
        # A branch can only add loss, unknown, late ACKs and copied bytes.
        for action in state.actions():
            child = state.fork()
            child.step(action)
            stack.append(child)
    best.finish()
    hard_feasible = best.cost()[:3] == (0, 0, 0)
    if stack:
        status = "feasible_best_found" if hard_feasible else "unknown_timeout"
    else:
        status = "optimal" if hard_feasible else "infeasible_proven"
    return OracleResult(status, best, nodes)
