"""Import Phase 0 quantiles as simulations, never as service contracts."""
from dataclasses import replace
import json
from pathlib import Path

from .model import Service

EVIDENCE = Path(__file__).resolve().parents[2] / "docs/control-evidence/writer-capacity.json"


def observed_profiles(path=EVIDENCE):
    rows = [r for r in json.loads(path.read_text()) if r.get("phase2_profile_eligible")]
    if not rows:
        raise ValueError("no eligible B3 samples")
    template = Service(group=8, certified=False)
    return {
        name: replace(template,
                      writer_ns_per_mib=reduce(r["io"][quantile] for r in rows),
                      sync_ns=reduce(r["sync"][quantile] for r in rows))
        for name, quantile, reduce in (("observed-p50-low", "p50", min),
                                       ("observed-p99-high", "p99", max))
    }
