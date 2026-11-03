"""Synthetic ride-hailing event source.

``RideSimulator`` is a small discrete-event simulation: riders request rides
in 12 zones, a finite driver pool gets assigned, and rides either complete or
cancel. ``FaultInjector`` then makes the stream realistic by adding duplicate
deliveries, out-of-order events (inside the allowed lateness), truly late
events (past it), and malformed payloads. Both are seeded, so a run with the
same seed produces the same stream, which is what the tests and the
stream-vs-batch reconciliation rely on.
"""

from __future__ import annotations

import heapq
import itertools
import json
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .schema import RideEvent

# zone_id -> relative demand weight (zones.csv seed in dbt mirrors these ids)
ZONE_WEIGHTS = {1: 9, 2: 8, 3: 7, 4: 6, 5: 5, 6: 5, 7: 4, 8: 3, 9: 3, 10: 2, 11: 2, 12: 1}


@dataclass(order=True)
class _Scheduled:
    at: datetime
    seq: int
    action: str = field(compare=False)
    ride: dict = field(compare=False)


class RideSimulator:
    def __init__(
        self,
        seed: int,
        start: datetime,
        rides_per_sec: float = 5.0,
        n_drivers: int | None = None,
        run_id: str | None = None,
    ) -> None:
        # A ride occupies a driver for ~17 min on average (pickup + trip), so
        # ~1000 driver-seconds per ride. Default supply covers the average
        # demand but not the peaks of the demand wave, so some requests fail
        # with no_driver at rush hour, like a real marketplace.
        if n_drivers is None:
            n_drivers = max(20, int(rides_per_sec * 1000))
        self.rng = random.Random(seed)
        # deterministic UUIDs: derived from the seeded RNG, not os.urandom
        # run_id namespaces ride ids and event ids. Backfills use the seed so the
        # same seed reproduces the same stream; live runs pass a random run_id so
        # a restarted producer never reuses ids (dedupe would silently drop them).
        self.run_id = run_id or f"s{seed}"
        self._uuid_rng = random.Random(f"{seed}:{self.run_id}")
        self.now = start
        self.rides_per_sec = rides_per_sec
        self.free_drivers = [f"d{i:04d}" for i in range(1, n_drivers + 1)]
        self.rng.shuffle(self.free_drivers)
        self._heap: list[_Scheduled] = []
        self._seq = itertools.count()
        self._ride_seq = itertools.count(1)
        self._zones = list(ZONE_WEIGHTS)
        self._weights = list(ZONE_WEIGHTS.values())
        self._next_request = start
        self._start = start

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self._uuid_rng.getrandbits(128), version=4))

    def _push(self, at: datetime, action: str, ride: dict) -> None:
        heapq.heappush(self._heap, _Scheduled(at, next(self._seq), action, ride))

    def _event(self, etype: str, ride: dict, at: datetime, **extra) -> RideEvent:
        return RideEvent(
            event_id=self._uuid(),
            event_type=etype,
            ride_id=ride["ride_id"],
            event_time=at,
            zone_id=ride["zone_id"],
            rider_id=ride["rider_id"],
            driver_id=extra.get("driver_id", ride.get("driver_id")),
            fare_cents=extra.get("fare_cents"),
            distance_m=extra.get("distance_m"),
            cancel_reason=extra.get("cancel_reason"),
        )

    def _secs(self, s: float) -> timedelta:
        return timedelta(seconds=s)

    def advance(self, until: datetime) -> list[RideEvent]:
        """Run the simulation up to ``until`` and return events in time order."""
        out: list[RideEvent] = []
        rng = self.rng
        while True:
            # schedule new requests as a Poisson process
            while self._next_request <= until:
                at = self._next_request
                self._next_request += self._secs(rng.expovariate(self.rides_per_sec * 1.35))
                # thinning: demand follows a 1-hour wave between 0.65x and 1.35x
                phase = (at - self._start).total_seconds() / 3600 * 2 * math.pi
                if rng.random() > (1 + 0.35 * math.sin(phase)) / 1.35:
                    continue
                zone = rng.choices(self._zones, self._weights)[0]
                ride = {
                    "ride_id": f"{self.run_id}-{next(self._ride_seq):08d}",
                    "zone_id": zone,
                    "rider_id": f"u{rng.randint(1, 50_000):05d}",
                }
                self._push(at, "request", ride)
            if not self._heap or self._heap[0].at > until:
                break
            item = heapq.heappop(self._heap)
            at, ride = item.at, item.ride
            if item.action == "request":
                out.append(self._event("ride_requested", ride, at))
                self._push(at + self._secs(rng.expovariate(1 / 40)), "assign", ride)
            elif item.action == "assign":
                if not self.free_drivers or rng.random() < 0.04:
                    out.append(self._event("ride_cancelled", ride, at, cancel_reason="no_driver"))
                    continue
                ride["driver_id"] = self.free_drivers.pop()
                out.append(self._event("driver_assigned", ride, at))
                r = rng.random()
                if r < 0.05:
                    self._push(at + self._secs(rng.uniform(20, 180)), "rider_cancel", ride)
                elif r < 0.07:
                    self._push(at + self._secs(rng.uniform(20, 180)), "driver_cancel", ride)
                else:
                    self._push(at + self._secs(rng.uniform(90, 480)), "start", ride)
            elif item.action in ("rider_cancel", "driver_cancel"):
                reason = "rider_cancelled" if item.action == "rider_cancel" else "driver_cancelled"
                out.append(self._event("ride_cancelled", ride, at, cancel_reason=reason))
                self.free_drivers.insert(0, ride["driver_id"])
            elif item.action == "start":
                out.append(self._event("ride_started", ride, at))
                ride["trip_s"] = rng.lognormvariate(6.6, 0.5)  # median ~12 min
                self._push(at + self._secs(ride["trip_s"]), "complete", ride)
            elif item.action == "complete":
                dist = int(ride["trip_s"] * rng.uniform(5.0, 11.0))  # 18-40 km/h
                fare = int(250 + dist * 0.11 + ride["trip_s"] / 60 * 30)
                out.append(self._event("ride_completed", ride, at, fare_cents=fare, distance_m=dist))
                self.free_drivers.insert(0, ride["driver_id"])
        self.now = until
        return out


@dataclass(frozen=True, slots=True)
class Emission:
    emit_at: datetime
    key: bytes
    value: bytes
    kind: str  # normal | duplicate | out_of_order | late | malformed
    event: RideEvent | None


MALFORMED_SAMPLES = (
    lambda e: b'{"event_id": "not-json',
    lambda e: json.dumps({**json.loads(e.to_json()), "zone_id": "seven"}).encode(),
    lambda e: json.dumps({k: v for k, v in json.loads(e.to_json()).items() if k != "ride_id"}).encode(),
    lambda e: json.dumps({**json.loads(e.to_json()), "event_type": "ride_teleported"}).encode(),
    lambda e: json.dumps({**json.loads(e.to_json()), "schema_version": 99}).encode(),
)


class FaultInjector:
    """Reorders, duplicates, delays and corrupts a clean event stream."""

    def __init__(
        self,
        seed: int,
        allowed_lateness_s: float,
        p_duplicate: float = 0.01,
        p_out_of_order: float = 0.03,
        p_late: float = 0.004,
        p_malformed: float = 0.002,
    ) -> None:
        self.rng = random.Random(seed + 1)
        self.lateness = allowed_lateness_s
        self.p = (p_duplicate, p_out_of_order, p_late, p_malformed)
        self._held: list[tuple[datetime, int, Emission]] = []
        self._seq = itertools.count()
        self.counts: Counter[str] = Counter()

    def _hold(self, em: Emission) -> None:
        heapq.heappush(self._held, (em.emit_at, next(self._seq), em))

    def feed(self, events: list[RideEvent]) -> None:
        p_dup, p_ooo, p_late, p_bad = self.p
        rng = self.rng
        for e in events:
            key = e.ride_id.encode()
            r = rng.random()
            if r < p_bad:
                bad = rng.choice(MALFORMED_SAMPLES)(e)
                self._hold(Emission(e.event_time, key, bad, "malformed", None))
                continue
            r -= p_bad
            if r < p_late:
                # past window end + allowed lateness: streaming must flag it late
                delay = rng.uniform(self.lateness + 90, 600)
                self._hold(Emission(e.event_time + timedelta(seconds=delay), key, e.to_json(), "late", e))
            elif r - p_late < p_ooo:
                delay = rng.uniform(1, self.lateness * 0.5)
                self._hold(
                    Emission(e.event_time + timedelta(seconds=delay), key, e.to_json(), "out_of_order", e)
                )
            else:
                self._hold(Emission(e.event_time, key, e.to_json(), "normal", e))
            if rng.random() < p_dup:
                self._hold(
                    Emission(
                        e.event_time + timedelta(seconds=rng.uniform(0, 5)), key, e.to_json(), "duplicate", e
                    )
                )

    def release(self, until: datetime | None = None) -> list[Emission]:
        """Pop everything due by ``until`` (or everything, if None), in emit order."""
        out = []
        while self._held and (until is None or self._held[0][0] <= until):
            em = heapq.heappop(self._held)[2]
            self.counts[em.kind] += 1
            out.append(em)
        return out
