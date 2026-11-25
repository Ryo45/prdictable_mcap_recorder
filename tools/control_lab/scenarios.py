"""Small fixtures with documented clock values and deterministic jitter."""
from dataclasses import replace
import random

from .model import Frame, Pool, Scenario, Service, MIB, MS


def fixture(name, frames, **options):
    defaults = dict(pools=(Pool(4), Pool(4)), spill=((MIB, 6), (4 * MIB, 2)),
                    service=Service(MS, 2 * MS, MS), stalls=((0, 10 * MS),))
    defaults.update(options)
    return Scenario(name, tuple(frames), **defaults)


def fixed(seed):
    rng = random.Random(seed)
    common = tuple(Frame(i % 2, 0, hold=7 * MS, ack=MS // 2) for i in range(4))
    phases = tuple(Frame(i % 8, (i % 8) * 400_000 + rng.randrange(20_000), hold=9 * MS)
                   for i in range(16))
    return [
        fixture("M1-common-3", common[:3]),
        fixture("impossible-common-4", common),
        fixture("M4-mixed", [Frame(0, 0, hold=4 * MS), Frame(1, 0), Frame(1, 3 * MS)],
                pools=(Pool(2), Pool(1)), wakes=(MS,)),
        fixture("M5-release-edf", [Frame(0, 0, 4 * MIB, 11 * MS),
                                 Frame(1, MS, MIB, 4 * MS)], wakes=(2 * MS,)),
        fixture("M7-spill", [Frame(0, 0, 4 * MIB, 7 * MS), Frame(1, 0, hold=7 * MS)],
                spill=((MIB, 1),)),
        fixture("M8-slow-copy", common[:3], service=Service(MS, 3 * MS, MS, certified=False)),
        fixture("M9-held-writer", [Frame(0, 0, hold=5 * MS), Frame(1, 0, hold=6 * MS)],
                stalls=((MS, 12 * MS),), service=Service(3 * MS, 2 * MS, MS, certified=False)),
        fixture("sync-failure", [Frame(i % 2, i * MS) for i in range(6)],
                sync_fail=1, stalls=(), service=Service(MS, MS, MS, certified=False)),
        fixture("horizon", [Frame(0, 0, hold=33 * MS), Frame(1, 0, hold=33 * MS)],
                stalls=((0, 40 * MS),), service=Service(MS, 10 * MS, MS)),
        fixture("reserve-exhausted", [Frame(0, i * 700_000) for i in range(6)],
                pools=(Pool(1, 2), Pool(1)), wakes=(500_000,)),
        fixture("H1-resume-model", [Frame(i % 2, i * 900_000, hold=10 * MS) for i in range(8)],
                stalls=((MS, 6 * MS),), service=Service(600_000, 2 * MS, 2 * MS, 2)),
        fixture("H4-queue-model", [Frame(i % 2, i * 650_000, MIB if i % 2 else 4 * MIB,
                                         9 * MS, 100_000) for i in range(8)],
                spill=((MIB, 1), (4 * MIB, 1)), queue=4, stalls=((MS, 6 * MS),)),
        fixture("overlap", common[:3], stalls=((MS, 3 * MS),),
                service=Service(MS, 2 * MS, MS, overlap_penalty_ns=1200_000)),
        fixture("phase-jitter-8", phases, pools=(Pool(4),) * 8),
        fixture("burst-aligned-8", [replace(frame, arrival=(i // 8) * 400_000)
                                     for i, frame in enumerate(phases)], pools=(Pool(4),) * 8),
    ]


def paced(gbps, service, stall_ms=0, seed=1):
    period = int(2 * MIB / gbps)
    rng = random.Random(seed)
    frames = [Frame(i % 2, i // 2 * period + rng.randrange(20_000),
                    hold=8 * MS, ack=100_000) for i in range(24)]
    return fixture(f"paced-{gbps:g}-stall-{stall_ms}", frames, service=service, queue=12,
                   stalls=((2 * MS, (2 + stall_ms) * MS),) if stall_ms else (),
                   pools=(Pool(6), Pool(6)))
