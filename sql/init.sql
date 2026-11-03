-- Warehouse bootstrap. The streaming consumer owns raw/stream/ops;
-- dbt owns everything under the analytics_* schemas.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS ops;

-- Landing table: every valid event exactly once (deduplicated on event_id).
CREATE TABLE IF NOT EXISTS raw.ride_events (
    event_id        uuid PRIMARY KEY,
    event_type      text        NOT NULL,
    ride_id         text        NOT NULL,
    event_time      timestamptz NOT NULL,
    zone_id         int         NOT NULL,
    rider_id        text        NOT NULL,
    driver_id       text,
    fare_cents      int,
    distance_m      int,
    cancel_reason   text,
    is_late         boolean     NOT NULL,   -- arrived after its window closed
    kafka_partition int         NOT NULL,
    kafka_offset    bigint      NOT NULL,
    kafka_ts        timestamptz NOT NULL,   -- broker/producer timestamp
    ingested_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ride_events_ingested_idx ON raw.ride_events (ingested_at);
CREATE INDEX IF NOT EXISTS ride_events_ride_idx ON raw.ride_events (ride_id);

-- Streaming aggregate: 1-minute tumbling windows per zone, updated
-- incrementally in the same transaction that stores the Kafka offsets.
CREATE TABLE IF NOT EXISTS stream.zone_minute_metrics (
    window_start     timestamptz NOT NULL,
    zone_id          int         NOT NULL,
    requests         int         NOT NULL DEFAULT 0,
    assignments      int         NOT NULL DEFAULT 0,
    starts           int         NOT NULL DEFAULT 0,
    completions      int         NOT NULL DEFAULT 0,
    cancellations    int         NOT NULL DEFAULT 0,
    gross_fare_cents bigint      NOT NULL DEFAULT 0,
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (window_start, zone_id)
);

-- Source of truth for consumer progress (Kafka commits are only for lag dashboards).
CREATE TABLE IF NOT EXISTS ops.consumer_offsets (
    group_id    text        NOT NULL,
    topic       text        NOT NULL,
    partition   int         NOT NULL,
    next_offset bigint      NOT NULL,
    watermark   timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, topic, partition)
);

CREATE TABLE IF NOT EXISTS ops.dead_letters (
    id              bigserial PRIMARY KEY,
    kafka_partition int         NOT NULL,
    kafka_offset    bigint      NOT NULL,
    payload         text        NOT NULL,
    error           text        NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (kafka_partition, kafka_offset)
);

-- Windows whose end is behind the global watermark will not change again.
CREATE OR REPLACE VIEW stream.zone_minute_metrics_closed AS
SELECT m.*
FROM stream.zone_minute_metrics m
WHERE m.window_start + interval '1 minute' <= (
    SELECT min(watermark) FROM ops.consumer_offsets WHERE topic = 'rides.events'
);

-- Audit trail written by the Airflow reconciliation task.
CREATE TABLE IF NOT EXISTS ops.reconciliation_runs (
    run_id          bigserial PRIMARY KEY,
    checked_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    windows_checked int         NOT NULL,
    windows_matched int         NOT NULL,
    late_events     int         NOT NULL,
    max_abs_diff    int         NOT NULL,
    passed          boolean     NOT NULL
);
