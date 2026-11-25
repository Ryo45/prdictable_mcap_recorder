from dataclasses import replace
import unittest

from tools.control_lab.controllers import Projection, Settings, controller, dpp_score, shield
from tools.control_lab.model import Engine, Frame, MIB, MS, WAIT, run
from tools.control_lab.oracle import solve
from tools.control_lab.scenarios import fixed, fixture


class ControllerTests(unittest.TestCase):
    def test_oracle_exhaustive_cross_check(self):
        case = fixture("six", [Frame(i % 2, i * MS, hold=3 * MS) for i in range(6)], stalls=())
        stack = [Engine(case)]
        scores = []
        while stack:
            state = stack.pop()
            if state.done():
                scores.append(state.cost())
            else:
                for action in state.actions():
                    branch = state.fork()
                    branch.step(action)
                    stack.append(branch)
        result = solve(Engine(case), time_cap_ns=2_000 * MS)
        self.assertEqual(result.status, "optimal")
        self.assertEqual(result.outcome.cost(), min(scores))
        self.assertEqual(result.outcome.cost(), (0, 0, 0, 0))

    def test_oracle_common_deadline_and_cutoff(self):
        three, four = fixed(1)[:2]
        result = solve(Engine(three), time_cap_ns=2_000 * MS)
        self.assertEqual((result.status, result.outcome.copy_bytes), ("optimal", 3 * MIB))
        result = solve(Engine(four), time_cap_ns=2_000 * MS)
        self.assertEqual(result.status, "infeasible_proven")
        self.assertEqual(len(result.outcome.late), 1)
        self.assertEqual(solve(Engine(four), node_cap=0).status, "unknown_timeout")
        self.assertEqual(solve(Engine(three), node_cap=0).status, "feasible_best_found")

    def test_unknown_and_horizon(self):
        case = fixed(1)[0]
        result = shield(Engine(replace(case, public_arrivals=False)), 40 * MS, 384)
        self.assertEqual(result[-1].status, "Unknown")
        result = shield(Engine(fixed(1)[8]), 20 * MS, 384)
        self.assertEqual(result[-1].status, "Unknown")

    def test_dpp_copy_size_penalty(self):
        copy = Projection(("copy", 0), None, [(0, 1), (1, 0)])
        wait = Projection(WAIT, None, [(0, 1), (1, 1)])
        self.assertLess(dpp_score(copy, wait, 4 * MIB, 0), 0)
        self.assertLess(dpp_score(copy, wait, MIB, 0.5), 0)
        self.assertGreater(dpp_score(copy, wait, 4 * MIB, 0.5), 0)

    def test_mpc_budget_fallback(self):
        policy, stats = controller("bounded-mpc", Settings(mpc_nodes=0))
        outcome = run(fixed(1)[0], policy)
        self.assertEqual(outcome.cost()[:3], (0, 0, 0))
        self.assertGreater(stats.fallback_count, 0)

    def test_unannounced_outage_does_not_change_initial_decision(self):
        case = fixed(1)[6]
        pending = []
        for stalls in (case.stalls, ()):
            engine = Engine(replace(case, stalls=stalls), trace=True)
            controller("safe-greedy")[0](engine)
            pending.append(next(e["frame"] for e in engine.trace if e["event"] == "writer_claim"))
        self.assertEqual(pending, [0, 0])

    def test_copy_contract_violation_is_separate_from_late_ack(self):
        case = fixed(1)[5]
        policy, _ = controller("safe-greedy")
        outcome = run(case, policy)
        self.assertEqual(outcome.contract_violations, 3)
        self.assertEqual(len(outcome.late), 1)
        self.assertEqual(outcome.loss, {})

    def test_resume_copy_savings_without_a_ledger_regression(self):
        case = next(case for case in fixed(101) if case.name == "H1-resume-model")
        policy, _ = controller("safe-greedy")
        outcome = run(case, policy)
        self.assertEqual(outcome.cost(), (0, 0, 0, 3 * MIB))
