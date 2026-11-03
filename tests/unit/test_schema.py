import json
from datetime import UTC, datetime

import pytest

from ridestream.generator import MALFORMED_SAMPLES
from ridestream.schema import InvalidEvent, RideEvent, parse_event

T = datetime(2026, 11, 1, 8, 0, 5, tzinfo=UTC)


def ev(**kw) -> RideEvent:
    base = dict(
        event_id="7b0a1f7e-8a6b-4c2e-9a50-1b0f6c1e2d3a",
        event_type="ride_completed",
        ride_id="r1",
        event_time=T,
        zone_id=3,
        rider_id="u1",
        driver_id="d1",
        fare_cents=1234,
        distance_m=4000,
    )
    base.update(kw)
    return RideEvent(**base)


def test_round_trip():
    e = ev()
    assert parse_event(e.to_json()) == e


def test_non_utc_offset_is_normalised():
    raw = json.loads(ev().to_json())
    raw["event_time"] = "2026-11-01T03:00:05-05:00"
    assert parse_event(json.dumps(raw)).event_time == T


@pytest.mark.parametrize(
    "mutate,msg",
    [
        (lambda d: d.pop("ride_id"), "missing required field ride_id"),
        (lambda d: d.update(event_time="2026-11-01T08:00:05"), "timezone"),
        (lambda d: d.update(zone_id="3"), "zone_id must be an integer"),
        (lambda d: d.update(zone_id=0), "zone_id must be >= 1"),
        (lambda d: d.update(fare_cents=True), "fare_cents must be an integer"),
        (lambda d: d.update(fare_cents=0), "zero fare"),
        (lambda d: d.update(driver_id=None), "requires driver_id"),
        (lambda d: d.update(event_type="ride_paused"), "unknown event_type"),
        (lambda d: d.update(event_id="abc"), "UUID"),
        (lambda d: d.update(schema_version=2), "schema_version"),
    ],
)
def test_rejects_bad_events(mutate, msg):
    d = json.loads(ev().to_json())
    mutate(d)
    with pytest.raises(InvalidEvent, match=msg):
        parse_event(json.dumps(d))


def test_rejects_non_json_and_non_object():
    with pytest.raises(InvalidEvent):
        parse_event(b"\xff\xfe")
    with pytest.raises(InvalidEvent):
        parse_event(b"[1,2]")


@pytest.mark.parametrize("make_bad", MALFORMED_SAMPLES)
def test_every_injected_malformed_sample_is_rejected(make_bad):
    with pytest.raises(InvalidEvent):
        parse_event(make_bad(ev()))
