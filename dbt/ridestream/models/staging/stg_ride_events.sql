-- One row per unique event (the consumer already deduplicated on event_id).
select
    event_id,
    event_type,
    ride_id,
    event_time,
    date_bin('1 minute', event_time, timestamptz '1970-01-01 00:00:00+00') as event_minute,
    zone_id,
    rider_id,
    driver_id,
    fare_cents,
    distance_m,
    cancel_reason,
    is_late,
    kafka_ts,
    ingested_at
from {{ source('raw', 'ride_events') }}
