import unittest

from tools.control_lab.model import Engine, Frame, Pool, WAIT, MS, run
from tools.control_lab.policies import baseline
from tools.control_lab.scenarios import fixed, fixture


class ModelTests(unittest.TestCase):
    def test_common_deadline(self):
        case = fixed(1)[0]
        legacy = run(case, baseline("legacy"), trace=True)
        global_ = run(case, baseline("global"), trace=True)
        self.assertEqual([e["ns"] for e in legacy.trace if e["event"] == "ack_ready"],
                         [7 * MS, 9 * MS, 11 * MS])
        self.assertEqual([e["ns"] for e in global_.trace if e["event"] == "ack_ready"],
                         [3 * MS, 5 * MS, 7 * MS])
        self.assertEqual(len(global_.late), 0)

    def test_ack_precedes_arrival(self):
        case = fixture("ack-tie", [Frame(0, 0, ack=MS), Frame(0, 3 * MS)],
                       pools=(Pool(1),), stalls=((0, 10 * MS),))
        outcome = run(case, baseline("always-copy"), trace=True)
        self.assertEqual(outcome.loss, {})
        self.assertEqual([e["event"] for e in outcome.trace if e["ns"] == 3 * MS][:2],
                         ["ack_ready", "arrival"])

    def test_irreversible_writer(self):
        engine = Engine(fixed(1)[6], trace=True)
        engine.step(WAIT)
        self.assertEqual(engine.writer, 0)
        self.assertNotIn(("copy", 0), engine.actions())
        self.assertEqual(engine.work.jobs[0][0], "writing")

    def test_terminal_conservation_and_determinism(self):
        for case in fixed(1):
            for mode in ("global", "legacy", "always-borrow", "always-copy"):
                with self.subTest(case=case.name, mode=mode):
                    left = run(case, baseline(mode), trace=True)
                    right = run(case, baseline(mode), trace=True)
                    self.assertEqual(left.trace, right.trace)

    def test_failures_remain_distinct(self):
        cases = fixed(1)
        self.assertGreater(len(run(cases[1], baseline("global")).late), 0)
        self.assertGreater(run(cases[4], baseline("global")).spill_failures, 0)
        self.assertEqual(run(cases[7], baseline("global")).cost()[:3], (0, 4, 0))
