"""Postgres sink: one transaction per consumed batch.

The transaction does four things atomically:

1. dedupes and inserts the batch into ``raw.ride_events``,
2. folds only the *newly inserted, on-time* rows into the 1-minute window
   aggregate (so duplicates and replays never double count),
3. records dead letters,
4. stores the next Kafka offset and watermark per partition.

Because offsets live in the same transaction as the data, a crash at any
point either keeps the whole batch or none of it, and the consumer resumes
from the stored offset. That gives effectively-once results in Postgres even
though Kafka delivery itself is at-least-once.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

RAW_COLS = (
    "event_id",
    "event_type",
    "ride_id",
    "event_time",
    "zone_id",
    "rider_id",
    "driver_id",
    "fare_cents",
    "distance_m",
    "cancel_reason",
    "is_late",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
)
_COLS = ", ".join(RAW_COLS)

STAGE_DDL = """
CREATE TEMP TABLE IF NOT EXISTS stage_events
  (LIKE raw.ride_events INCLUDING DEFAULTS) ON COMMIT DELETE ROWS
"""

MERGE_SQL = f"""
WITH ins AS (
    INSERT INTO raw.ride_events ({_COLS})
    SELECT DISTINCT ON (event_id) {_COLS}
    FROM stage_events
    ORDER BY event_id, kafka_partition, kafka_offset
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_type, event_time, zone_id, fare_cents, is_late
), agg AS (
    SELECT date_bin('1 minute', event_time, timestamptz '1970-01-01 00:00:00+00') AS window_start,
           zone_id,
           count(*) FILTER (WHERE event_type = 'ride_requested')  AS requests,
           count(*) FILTER (WHERE event_type = 'driver_assigned') AS assignments,
           count(*) FILTER (WHERE event_type = 'ride_started')    AS starts,
           count(*) FILTER (WHERE event_type = 'ride_completed')  AS completions,
           count(*) FILTER (WHERE event_type = 'ride_cancelled')  AS cancellations,
           coalesce(sum(fare_cents) FILTER (WHERE event_type = 'ride_completed'), 0) AS gross_fare_cents
    FROM ins
    WHERE NOT is_late
    GROUP BY 1, 2
), up AS (
    INSERT INTO stream.zone_minute_metrics AS m
        (window_start, zone_id, requests, assignments, starts, completions, cancellations, gross_fare_cents)
    SELECT * FROM agg ORDER BY window_start, zone_id   -- stable lock order across consumers
    ON CONFLICT (window_start, zone_id) DO UPDATE SET
        requests         = m.requests + EXCLUDED.requests,
        assignments      = m.assignments + EXCLUDED.assignments,
        starts           = m.starts + EXCLUDED.starts,
        completions      = m.completions + EXCLUDED.completions,
        cancellations    = m.cancellations + EXCLUDED.cancellations,
        gross_fare_cents = m.gross_fare_cents + EXCLUDED.gross_fare_cents,
        updated_at       = clock_timestamp()
    RETURNING 1
)
SELECT (SELECT count(*) FROM ins),
       (SELECT count(*) FROM ins WHERE is_late),
       (SELECT count(*) FROM up)
"""

OFFSET_SQL = """
INSERT INTO ops.consumer_offsets (group_id, topic, partition, next_offset, watermark)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (group_id, topic, partition) DO UPDATE SET
    next_offset = EXCLUDED.next_offset,
    watermark   = GREATEST(ops.consumer_offsets.watermark, EXCLUDED.watermark),
    updated_at  = clock_timestamp()
"""

DLQ_SQL = """
INSERT INTO ops.dead_letters (kafka_partition, kafka_offset, payload, error)
VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
"""


@dataclass(frozen=True)
class BatchResult:
    received: int
    inserted: int
    duplicates: int
    late: int
    windows_touched: int
    dead_letters: int


def prepare(conn: psycopg.Connection) -> None:
    conn.execute(STAGE_DDL)
    conn.commit()


def load_offsets(
    conn: psycopg.Connection, group_id: str, topic: str
) -> dict[int, tuple[int, datetime | None]]:
    rows = conn.execute(
        "SELECT partition, next_offset, watermark FROM ops.consumer_offsets WHERE group_id=%s AND topic=%s",
        (group_id, topic),
    ).fetchall()
    conn.commit()
    return {p: (o, w) for p, o, w in rows}


def other_watermarks(
    conn: psycopg.Connection, group_id: str, topic: str, exclude: set[int]
) -> dict[int, datetime | None]:
    """Committed watermarks of this group's partitions that this consumer does not own."""
    rows = conn.execute(
        "SELECT partition, watermark FROM ops.consumer_offsets WHERE group_id=%s AND topic=%s",
        (group_id, topic),
    ).fetchall()
    return {p: w for p, w in rows if p not in exclude}


def write_batch(
    conn: psycopg.Connection,
    group_id: str,
    topic: str,
    rows: list[tuple],
    dead: list[tuple[int, int, str, str]],
    offsets: dict[int, tuple[int, datetime | None]],
) -> BatchResult:
    with conn.transaction(), conn.cursor() as cur:
        inserted = late = touched = 0
        if rows:
            with cur.copy(f"COPY stage_events ({_COLS}) FROM STDIN") as cp:
                for r in rows:
                    cp.write_row(r)
            inserted, late, touched = cur.execute(MERGE_SQL).fetchone()
        if dead:
            cur.executemany(DLQ_SQL, dead)
        cur.executemany(
            OFFSET_SQL,
            [(group_id, topic, p, nxt, wm) for p, (nxt, wm) in offsets.items()],
        )
    return BatchResult(
        received=len(rows) + len(dead),
        inserted=inserted,
        duplicates=len(rows) - inserted,
        late=late,
        windows_touched=touched,
        dead_letters=len(dead),
    )
