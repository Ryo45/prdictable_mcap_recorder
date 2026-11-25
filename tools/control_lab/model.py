"""Single nonpreemptive copy server and synchronous FIFO/RR writer."""
from dataclasses import dataclass, field
from heapq import heappop, heappush
from copy import deepcopy

MIB = 1 << 20
MS = 1_000_000


@dataclass(frozen=True)
class Frame:
    source: int
    arrival: int
    size: int = MIB
    hold: int | None = None
    ack: int = 0
    ready: int = 0
    borrowed: bool = True


@dataclass(frozen=True)
class Pool:
    slots: int
    reserve: int = 0
    minimum: int = 1


@dataclass(frozen=True)
class Service:
    writer_ns_per_mib: int = MS
    copy_ns_per_mib: int = MS
    sync_ns: int = MS
    group: int = 4
    certified: bool = True
    overlap_penalty_ns: int = 0
    assumed_copy_ns_per_mib: int | None = None


@dataclass(frozen=True)
class Scenario:
    name: str
    frames: tuple
    pools: tuple
    spill: tuple
    queue: int = 16
    service: Service = Service()
    stalls: tuple = ()
    sync_fail: int | None = None
    public_arrivals: bool = True
    wakes: tuple = ()


@dataclass
class Slot:
    phase: str
    generation: int = 0
    owner: int | None = None


@dataclass
class Work:
    queue: list
    slots: list
    jobs: dict = field(default_factory=dict)
    spill: dict = field(default_factory=dict)


WAIT = ("wait", -1)


class Engine:
    def __init__(self, scenario, trace=False):
        self.scenario = scenario
        self.frames = scenario.frames
        self.time = 0
        self.work = Work([[] for _ in scenario.pools], [
            [Slot("available") for _ in range(pool.slots)] +
            [Slot("reserve") for _ in range(pool.reserve)] for pool in scenario.pools])
        self.events = []
        self.serial = 0
        self.writer = self.copying = None
        self.rr = 0
        self.unsynced = []
        self.syncs = 0
        self.status = {}
        self.loss = {}
        self.late = {}
        self.copy_bytes = 0
        self.per_source_copy = [0] * len(scenario.pools)
        self.completion = []
        self.sync_latency = []
        self.headroom = []
        self.spill_failures = 0
        self.reserves = 0
        self.contract_violations = 0
        self.trace = [] if trace else None
        for i, frame in enumerate(self.frames):
            self.push(frame.arrival, "arrival", i)
            if frame.ready:
                self.push(frame.arrival + frame.ready, "wake")
        for stamp in scenario.wakes:
            self.push(stamp, "wake")
        for begin, end in scenario.stalls:
            self.push(begin, "wake")
            self.push(end, "wake")
        self.settle()

    def push(self, stamp, kind, i=-1, detail=None):
        self.serial += 1
        heappush(self.events, (stamp, int(kind == "arrival"), self.serial, kind, i, detail))

    def log(self, kind, i=-1, **details):
        if self.trace is not None:
            self.trace.append(dict(ns=self.time, event=kind, frame=i, **details))

    def duration(self, i, copy, assumed=False):
        profile = self.scenario.service
        rate = profile.copy_ns_per_mib if copy else profile.writer_ns_per_mib
        if copy and assumed and profile.assumed_copy_ns_per_mib is not None:
            rate = profile.assumed_copy_ns_per_mib
        return max(1, (self.frames[i].size * rate + MIB - 1) // MIB)

    def due(self, i):
        frame = self.frames[i]
        return frame.arrival + frame.hold if frame.hold is not None else None

    def acquire(self, i):
        frame = self.frames[i]
        self.log("arrival", i)
        queue = self.work.queue
        slots = self.work.slots[frame.source]
        available = [n for n, slot in enumerate(slots) if slot.phase == "available"]
        reason = None
        if sum(map(len, queue)) >= self.scenario.queue:
            reason = "queue_full"
        elif frame.borrowed and len(available) < self.scenario.pools[frame.source].minimum:
            reason = "pool_exhausted"
        if reason:
            self.loss[i] = reason
            self.status[i] = "lost"
            self.log("loss", i, reason=reason)
            return
        token = None
        if frame.borrowed:
            index = available[0]
            slot = slots[index]
            slot.generation += 1
            slot.phase, slot.owner = "held", i
            token = (index, slot.generation)
        self.work.jobs[i] = ("pending", token)
        queue[frame.source].append(i)
        self.status[i] = "accepted"
        self.log("acquire", i, token=token)

    def returning(self, i, token):
        index, generation = token
        slot = self.work.slots[self.frames[i].source][index]
        assert (slot.phase, slot.owner, slot.generation) == ("held", i, generation)
        slot.phase = "returning"
        self.log("return_requested", i, token=token)
        self.push(self.time + self.frames[i].ack, "ack_ready", i, token)

    def settle(self):
        while self.events and self.events[0][0] == self.time:
            _, _, _, kind, i, detail = heappop(self.events)
            if kind == "arrival":
                self.acquire(i)
            elif kind == "copy_complete":
                assert self.copying == i
                self.copying = None
                if self.scenario.service.assumed_copy_ns_per_mib is not None and (
                        self.time - detail > self.duration(i, True, assumed=True)):
                    self.contract_violations += 1
                    self.log("contract_violation", i, service="copy")
                phase, token = self.work.jobs[i]
                assert phase == "copying"
                self.work.jobs[i] = ("owned", None)
                self.log(kind, i)
                self.returning(i, token)
            elif kind == "ack_ready":
                index, generation = detail
                slot = self.work.slots[self.frames[i].source][index]
                assert (slot.phase, slot.owner, slot.generation) == ("returning", i, generation)
                slot.phase, slot.owner = "available", None
                due = self.due(i)
                if due is not None:
                    self.headroom.append(due - self.time)
                    if self.time > due:
                        self.late[i] = self.time - due
                self.log(kind, i, token=detail)
            elif kind == "storage_completion":
                assert self.writer == i
                self.writer = None
                phase, token = self.work.jobs.pop(i)
                assert phase == "writing"
                self.log(kind, i)
                self.completion.append(self.time - detail)
                if token is not None:
                    self.returning(i, token)
                if i in self.work.spill:
                    del self.work.spill[i]
                    self.log("spill_release", i)
                self.unsynced.append(i)
                if len(self.unsynced) == self.scenario.service.group:
                    self.start_sync()
            elif kind == "fdatasync":
                self.writer = None
                self.syncs += 1
                failed = self.syncs == self.scenario.sync_fail
                for item in self.unsynced:
                    self.status[item] = "unknown" if failed else "committed"
                    self.sync_latency.append(self.time - self.frames[item].arrival)
                self.log(kind, count=len(self.unsynced), success=not failed)
                self.unsynced.clear()
        self.validate()

    def start_sync(self):
        self.writer = -1
        self.push(self.storage_end(self.scenario.service.sync_ns), "fdatasync")

    def storage_end(self, duration):
        end = self.time + max(1, duration)
        for begin, stop in self.scenario.stalls:
            if begin < end and self.time < stop:
                end += stop - max(begin, self.time)
        return end

    def validate(self):
        queues = self.work.queue
        pending = {i for queue in queues for i in queue}
        assert sum(map(len, queues)) == len(pending) <= self.scenario.queue
        assert all(q == sorted(q, key=lambda i: (self.frames[i].arrival, i)) for q in queues)
        assert pending == {i for i, (phase, _) in self.work.jobs.items() if phase != "writing"}
        assert sum(phase == "copying" for phase, _ in self.work.jobs.values()) == (self.copying is not None)
        assert sum(phase == "writing" for phase, _ in self.work.jobs.values()) == (
            self.writer is not None and self.writer >= 0)
        held = set()
        for source, slots in enumerate(self.work.slots):
            pool = self.scenario.pools[source]
            assert len(slots) == pool.slots + pool.reserve
            for slot in slots:
                assert (slot.owner is not None) == (slot.phase in ("held", "returning"))
                if slot.phase == "held":
                    assert slot.owner not in held
                    held.add(slot.owner)
        assert held == {i for i, (_, token) in self.work.jobs.items() if token is not None}
        for size, count in self.scenario.spill:
            assert list(self.work.spill.values()).count(size) <= count
        assert self.work.spill.keys() <= self.work.jobs.keys()

    def candidates(self):
        if self.copying is not None:
            return []
        return [i for i, (phase, token) in self.work.jobs.items()
                if phase == "pending" and token is not None
                and self.frames[i].arrival + self.frames[i].ready <= self.time]

    def spill_class(self, i):
        return next((size for size, count in self.scenario.spill
                     if size >= self.frames[i].size and list(self.work.spill.values()).count(size) < count), None)

    def actions(self):
        actions = [("copy", i) for i in self.candidates() if self.spill_class(i) is not None]
        for source, slots in enumerate(self.work.slots):
            if any(s.phase == "reserve" for s in slots) and any(s.phase == "held" for s in slots):
                actions.append(("reserve", source))
        return actions + [WAIT]

    def writer_candidate(self):
        if self.writer is not None or any(a <= self.time < b for a, b in self.scenario.stalls):
            return None
        for offset in range(len(self.work.queue)):
            queue = self.work.queue[(offset + self.rr) % len(self.work.queue)]
            if queue:
                i = queue[0]
                if self.work.jobs[i][0] != "copying" and self.frames[i].arrival + self.frames[i].ready <= self.time:
                    return i
        return None

    def wake_times(self):
        starts = []
        work = 0
        for i in sorted(self.candidates(), key=lambda i: (self.due(i) is None, self.due(i) or 0)):
            due = self.due(i)
            if due is not None:
                work += self.duration(i, True, assumed=True)
                starts.extend((due - self.frames[i].ack - self.duration(i, True, assumed=True),
                               due - self.frames[i].ack - work))
        if self.scenario.public_arrivals:
            for i in self.candidates():
                frame = self.frames[i]
                for j, future in enumerate(self.frames):
                    if j not in self.status and future.borrowed and future.source == frame.source:
                        starts.append(future.arrival - frame.ack - self.duration(i, True, assumed=True))
        return [stamp for stamp in starts if stamp > self.time]

    def step(self, action):
        kind, i = action
        assert action in self.actions()
        if kind == "copy":
            size = self.spill_class(i)
            self.work.spill[i] = size
            self.copying = i
            _, token = self.work.jobs[i]
            self.work.jobs[i] = ("copying", token)
            frame = self.frames[i]
            self.copy_bytes += frame.size
            self.per_source_copy[frame.source] += frame.size
            self.log("copy_claim", i, spill_class=size)
            penalty = self.scenario.service.overlap_penalty_ns if self.writer is not None else 0
            self.push(self.time + self.duration(i, True) + penalty, "copy_complete", i, self.time)
        elif kind == "reserve":
            slot = next(slot for slot in self.work.slots[i] if slot.phase == "reserve")
            slot.phase = "available"
            self.reserves += 1
            self.log("reserve", source=i)
        else:
            self.spill_failures += sum(self.spill_class(i) is None for i in self.candidates())
            i = self.writer_candidate()
            if i is not None:
                source = self.frames[i].source
                self.rr = (source + 1) % len(self.work.queue)
                assert self.work.queue[source].pop(0) == i
                _, token = self.work.jobs[i]
                self.work.jobs[i] = ("writing", token)
                self.writer = i
                self.log("writer_claim", i, borrowed=token is not None)
                penalty = self.scenario.service.overlap_penalty_ns if self.copying is not None else 0
                self.push(self.storage_end(self.duration(i, False) + penalty), "storage_completion", i, self.time)
            wakes = self.wake_times()
            if not self.events and self.unsynced and self.writer is None:
                self.start_sync()
            if self.events:
                wakes.append(self.events[0][0])
            if wakes:
                self.time = min(wakes)
            self.settle()
        self.validate()

    def done(self):
        return not (self.work.jobs or self.events or self.unsynced)

    def fork(self, trace=False):
        memo = {id(self.scenario): self.scenario, id(self.frames): self.frames}
        if not trace and self.trace is not None:
            memo[id(self.trace)] = None
        return deepcopy(self, memo)

    def cost(self):
        return (len(self.loss), sum(x == "unknown" for x in self.status.values()),
                len(self.late), self.copy_bytes)

    def finish(self):
        assert self.done() and len(self.status) == len(self.frames)
        assert set(self.status.values()) <= {"committed", "lost", "unknown"}
        assert set(self.loss) == {i for i, state in self.status.items() if state == "lost"}
        assert all(slot.phase in ("available", "reserve") for slots in self.work.slots for slot in slots)
        assert not self.work.spill


def run(scenario, policy, trace=False):
    engine = Engine(scenario, trace)
    while not engine.done():
        policy(engine)
    engine.finish()
    return engine
