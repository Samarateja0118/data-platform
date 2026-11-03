-- Pivot each ride's events into one row. Late events land here too, which is
-- the point of the batch layer: it is complete where the stream is fast.
with ev as (
    select * from {{ ref('stg_ride_events') }}
)
select
    ride_id,
    min(zone_id)                                                     as zone_id,
    min(rider_id)                                                    as rider_id,
    max(driver_id)                                                   as driver_id,
    min(event_time) filter (where event_type = 'ride_requested')     as requested_at,
    min(event_time) filter (where event_type = 'driver_assigned')    as assigned_at,
    min(event_time) filter (where event_type = 'ride_started')       as started_at,
    min(event_time) filter (where event_type = 'ride_completed')     as completed_at,
    min(event_time) filter (where event_type = 'ride_cancelled')     as cancelled_at,
    max(cancel_reason)                                               as cancel_reason,
    max(fare_cents)                                                  as fare_cents,
    max(distance_m)                                                  as distance_m,
    count(*)                                                         as event_count,
    max(ingested_at)                                                 as last_ingested_at
from ev
group by ride_id
