"""Ride event contract and validation.

Every message on the ``rides.events`` topic is a JSON object that must pass
``parse_event``. Anything that fails goes to the dead-letter path with the
reason attached, so bad producers never block the stream.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from uuid import UUID

SCHEMA_VERSION = 1

EVENT_TYPES = (
    "ride_requested",
    "driver_assigned",
    "ride_started",
    "ride_completed",
    "ride_cancelled",
)

# Fields each event type must carry on top of the common ones.
REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "ride_requested": (),
    "driver_assigned": ("driver_id",),
    "ride_started": ("driver_id",),
    "ride_completed": ("driver_id", "fare_cents", "distance_m"),
    "ride_cancelled": ("cancel_reason",),
}

CANCEL_REASONS = ("rider_cancelled", "no_driver", "driver_cancelled")


class InvalidEvent(ValueError):
    """Raised when a message does not satisfy the event contract."""


@dataclass(frozen=True, slots=True)
class RideEvent:
    event_id: str
    event_type: str
    ride_id: str
    event_time: datetime
    zone_id: int
    rider_id: str
    driver_id: str | None = None
    fare_cents: int | None = None
    distance_m: int | None = None
    cancel_reason: str | None = None
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> bytes:
        d = asdict(self)
        d["event_time"] = self.event_time.isoformat()
        return json.dumps(d, separators=(",", ":")).encode()


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise InvalidEvent("event_time must be an ISO-8601 string")
    try:
        ts = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidEvent(f"event_time not ISO-8601: {value!r}") from exc
    if ts.tzinfo is None:
        raise InvalidEvent("event_time must carry a timezone")
    return ts.astimezone(UTC)


def _int(d: dict, key: str, *, minimum: int = 0) -> int | None:
    v = d.get(key)
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        raise InvalidEvent(f"{key} must be an integer")
    if v < minimum:
        raise InvalidEvent(f"{key} must be >= {minimum}")
    return v


def parse_event(raw: bytes | str) -> RideEvent:
    """Decode and validate one message. Raises ``InvalidEvent`` on any problem."""
    try:
        d = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidEvent(f"not valid JSON: {exc.msg if hasattr(exc, 'msg') else exc}") from exc
    if not isinstance(d, dict):
        raise InvalidEvent("payload must be a JSON object")

    if d.get("schema_version") != SCHEMA_VERSION:
        raise InvalidEvent(f"unsupported schema_version {d.get('schema_version')!r}")

    for key in ("event_id", "event_type", "ride_id", "event_time", "zone_id", "rider_id"):
        if d.get(key) in (None, ""):
            raise InvalidEvent(f"missing required field {key}")

    try:
        UUID(str(d["event_id"]))
    except ValueError as exc:
        raise InvalidEvent("event_id must be a UUID") from exc

    etype = d["event_type"]
    if etype not in EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type {etype!r}")
    for key in REQUIRED_BY_TYPE[etype]:
        if d.get(key) in (None, ""):
            raise InvalidEvent(f"{etype} requires {key}")

    zone_id = _int(d, "zone_id", minimum=1)
    fare = _int(d, "fare_cents")
    dist = _int(d, "distance_m")
    if etype == "ride_completed" and fare == 0:
        raise InvalidEvent("completed ride with zero fare")
    reason = d.get("cancel_reason")
    if reason is not None and reason not in CANCEL_REASONS:
        raise InvalidEvent(f"unknown cancel_reason {reason!r}")

    return RideEvent(
        event_id=str(d["event_id"]),
        event_type=etype,
        ride_id=str(d["ride_id"]),
        event_time=_parse_time(d["event_time"]),
        zone_id=zone_id,  # type: ignore[arg-type]
        rider_id=str(d["rider_id"]),
        driver_id=d.get("driver_id"),
        fare_cents=fare,
        distance_m=dist,
        cancel_reason=reason,
    )
