select
    (completed_at at time zone 'UTC')::date  as ride_date,
    driver_id,
    count(*)                                 as trips,
    sum(fare_cents)                          as earnings_cents,
    sum(distance_m)                          as distance_m,
    round(sum(trip_s) / 3600.0, 2)           as hours_on_trip
from {{ ref('fct_rides') }}
where status = 'completed'
group by 1, 2
