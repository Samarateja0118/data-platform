-- Recompute every closed streaming window from raw and compare.
-- batch_* counts on-time events only (what the stream is supposed to have);
-- late_* counts what arrived after the window closed, which explains why the
-- stream's number differs from the complete batch number.
{{ config(materialized='table') }}
with wm as (
    select min(watermark) as global_watermark
    from {{ source('ops', 'consumer_offsets') }}
    where topic = '{{ var("stream_topic", "rides.events") }}'
),
batch as (
    select
        event_minute as window_start,
        zone_id,
        count(*) filter (where not is_late and event_type = 'ride_requested')  as batch_requests,
        count(*) filter (where not is_late and event_type = 'ride_completed')  as batch_completions,
        count(*) filter (where not is_late and event_type = 'ride_cancelled')  as batch_cancellations,
        coalesce(sum(fare_cents) filter (where not is_late and event_type = 'ride_completed'), 0) as batch_fare_cents,
        count(*) filter (where is_late)                                        as late_events,
        count(*) filter (where not is_late)                                    as on_time_events
    from {{ ref('stg_ride_events') }}
    group by 1, 2
),
stream as (
    select window_start, zone_id, requests, completions, cancellations, gross_fare_cents,
           requests + assignments + starts + completions + cancellations as stream_events
    from {{ source('stream', 'zone_minute_metrics') }}
)
select
    coalesce(b.window_start, s.window_start)          as window_start,
    coalesce(b.zone_id, s.zone_id)                    as zone_id,
    coalesce(s.stream_events, 0)                      as stream_events,
    coalesce(b.on_time_events, 0)                     as batch_on_time_events,
    coalesce(b.late_events, 0)                        as late_events,
    coalesce(s.requests, 0)      - coalesce(b.batch_requests, 0)      as diff_requests,
    coalesce(s.completions, 0)   - coalesce(b.batch_completions, 0)   as diff_completions,
    coalesce(s.cancellations, 0) - coalesce(b.batch_cancellations, 0) as diff_cancellations,
    coalesce(s.gross_fare_cents, 0) - coalesce(b.batch_fare_cents, 0) as diff_fare_cents,
    coalesce(s.stream_events, 0) - coalesce(b.on_time_events, 0)      as diff_events
from batch b
full outer join stream s using (window_start, zone_id)
cross join wm
where coalesce(b.window_start, s.window_start) + interval '1 minute' <= wm.global_watermark
