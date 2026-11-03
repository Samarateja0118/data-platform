from collections import defaultdict
from datetime import UTC, datetime, timedelta

from ridestream.generator import FaultInjector, RideSimulator
from ridestream.schema import InvalidEvent, parse_event

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
ORDER = ["ride_requested", "driver_assigned", "ride_started", "ride_completed"]


def simulate(seed=1, minutes=20, rate=3.0):
    sim = RideSimulator(seed, START, rides_per_sec=rate)
    return sim.advance(START + timedelta(minutes=minutes))


def test_deterministic_for_a_seed():
    a, b = simulate(seed=3), simulate(seed=3)
    assert [e.to_json() for e in a] == [e.to_json() for e in b]
    assert [e.to_json() for e in simulate(seed=4)] != [e.to_json() for e in a]


def test_events_are_time_ordered_and_lifecycles_valid():
    evs = simulate()
    assert all(x.event_time <= y.event_time for x, y in zip(evs, evs[1:], strict=False))
    by_ride = defaultdict(list)
    for e in evs:
        by_ride[e.ride_id].append(e.event_type)
    for types in by_ride.values():
        assert types[0] == "ride_requested"
        assert types.count("ride_cancelled") + types.count("ride_completed") <= 1
        happy = [t for t in types if t != "ride_cancelled"]
        assert happy == ORDER[: len(happy)]


def test_driver_is_never_double_booked():
    busy = {}
    for e in simulate(rate=8.0, minutes=30):
        if e.event_type == "driver_assigned":
            assert e.driver_id not in busy
            busy[e.driver_id] = e.ride_id
        elif e.event_type in ("ride_completed", "ride_cancelled") and e.driver_id:
            assert busy.pop(e.driver_id) == e.ride_id


def test_fault_injector_mix_and_ground_truth():
    evs = simulate(minutes=30, rate=5.0)
    inj = FaultInjector(seed=1, allowed_lateness_s=30)
    inj.feed(evs)
    out = inj.release(None)
    kinds = inj.counts
    assert sum(kinds.values()) == len(out)
    assert kinds["malformed"] > 0 and kinds["late"] > 0 and kinds["duplicate"] > 0
    assert all(x.emit_at <= y.emit_at for x, y in zip(out, out[1:], strict=False))
    valid_ids = set()
    for em in out:
        if em.kind == "malformed":
            try:
                parse_event(em.value)
                raise AssertionError("malformed payload parsed")
            except InvalidEvent:
                pass
        else:
            assert parse_event(em.value).event_id == em.event.event_id
            valid_ids.add(em.event.event_id)
    assert len(valid_ids) == len(evs) - kinds["malformed"]


def test_late_events_are_held_past_the_allowed_lateness():
    inj = FaultInjector(seed=2, allowed_lateness_s=30, p_late=1.0, p_duplicate=0, p_malformed=0)
    evs = simulate(minutes=2)
    inj.feed(evs)
    for em in inj.release(None):
        assert (em.emit_at - em.event.event_time).total_seconds() >= 30 + 90


def test_live_runs_never_reuse_ids_even_with_the_same_seed():
    a = RideSimulator(7, START, rides_per_sec=3.0, run_id="aaaa").advance(START + timedelta(minutes=5))
    b = RideSimulator(7, START, rides_per_sec=3.0, run_id="bbbb").advance(START + timedelta(minutes=5))
    assert not {e.event_id for e in a} & {e.event_id for e in b}
    assert not {e.ride_id for e in a} & {e.ride_id for e in b}
