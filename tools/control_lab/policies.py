"""Conceptual legacy/global baselines with a fixed autonomous writer."""
from .model import WAIT


def demands(engine):
    """Assign distinct candidates to fixed deadlines and projected slot shortfalls."""
    candidates = engine.candidates()
    demand = {i: engine.due(i) for i in candidates if engine.due(i) is not None}
    for source, slots in enumerate(engine.work.slots):
        pool = engine.scenario.pools[source]
        free = sum(slot.phase == "available" for slot in slots)
        acknowledgements = sorted(event[0] for event in engine.events if event[3] == "ack_ready"
                                  and engine.frames[event[4]].source == source)
        options = sorted((i for i in candidates if engine.frames[i].source == source),
                         key=lambda i: (demand.get(i, float("inf")), engine.frames[i].size, i))
        if not engine.scenario.public_arrivals or not options:
            continue
        arrivals = sorted(frame.arrival for i, frame in enumerate(engine.frames)
                          if i not in engine.status and frame.source == source and frame.borrowed)
        for count, at in enumerate(arrivals, 1):
            need = max(0, count + pool.minimum - 1 - free - sum(t <= at for t in acknowledgements))
            for i in options[:need]:
                demand[i] = min(demand.get(i, at), at)
    return demand


def baseline_action(engine, mode="global"):
    legal = engine.actions()
    if mode == "always-borrow":
        return WAIT
    candidates = [i for kind, i in legal if kind == "copy"]
    if mode == "always-copy":
        return ("copy", candidates[0]) if candidates else WAIT
    demand = demands(engine)
    tasks = sorted((i for i in candidates if i in demand),
                   key=lambda i: (demand[i] - engine.frames[i].ack, i))
    if not tasks:
        return WAIT
    work = 0
    latest = float("inf")
    for i in tasks:
        duration = engine.duration(i, True)
        work = work + duration if mode == "global" else duration
        latest = min(latest, demand[i] - engine.frames[i].ack - work)
    return ("copy", tasks[0]) if latest <= engine.time else WAIT


def baseline(mode):
    return lambda engine: engine.step(baseline_action(engine, mode))
