import json
from pathlib import Path
import tempfile
import unittest

from tools.control_lab.replay_rust import errors, occupancy, parse_trace


class ReplayTests(unittest.TestCase):
    def test_occupancy_tracks_each_size_class(self):
        self.assertEqual(occupancy([(4, 0, 8), (4, 2, 5), (1, 0, 2), (1, 2, 3)]),
                         {"4": {"peak": 2, "slot_ns": 11}, "1": {"peak": 1, "slot_ns": 3}})

    def test_errors_keep_unmatched_frames(self):
        self.assertEqual(errors({1: 20, 2: 15}, {1: 15, 3: 30}),
                         {"matched": 1, "actual_only": 1, "predicted_only": 1,
                          "median_signed_ns": -5, "median_absolute_ns": 5, "max_absolute_ns": 5})

    def test_incomplete_trace_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            path.write_text(json.dumps(dict(event="trace_end", failed=False, omitted=1, written=0)))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                parse_trace(path)
