-- depends_on: {{ ref('stg_ride_events') }}
{{
  config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['requested_at']}, {'columns': ['driver_id']}]
  )
}}
-- Incremental on *ingestion* time, not event time: any ride that received a
-- new event since the last run (including a late one from hours ago) is
-- rebuilt from all of its events. That keeps late data correct without a
-- fixed lookback window.
with lc as (
    select * from {{ ref('int_ride_lifecycle') }}
    {% if is_incremental() %}
    where ride_id in (
        select distinct ride_id from {{ ref('stg_ride_events') }}
        where ingested_at > (select coalesce(max(last_ingested_at), '-infinity') from {{ this }})
    )
    {% endif %}
)
select
    ride_id,
    zone_id,
    rider_id,
    driver_id,
    requested_at,
    assigned_at,
    started_at,
    completed_at,
    cancelled_at,
    case
        when completed_at is not null then 'completed'
        when cancelled_at is not null then 'cancelled'
        when started_at   is not null then 'in_trip'
        when assigned_at  is not null then 'driver_en_route'
        when requested_at is not null then 'awaiting_driver'
        else 'incomplete'   -- request event not received yet (late or lost)
    end                                                           as status,
    cancel_reason,
    extract(epoch from assigned_at - requested_at)::numeric(10,1)  as wait_s,
    extract(epoch from started_at - assigned_at)::numeric(10,1)    as pickup_s,
    extract(epoch from completed_at - started_at)::numeric(10,1)   as trip_s,
    fare_cents,
    distance_m,
    event_count,
    last_ingested_at
from lc
