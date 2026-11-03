-- Hourly zone KPIs by request time.
select
    date_trunc('hour', r.requested_at)                                 as hour,
    r.zone_id,
    z.zone_name,
    count(*)                                                           as requests,
    count(*) filter (where status = 'completed')                       as completed,
    count(*) filter (where status = 'cancelled')                       as cancelled,
    count(*) filter (where cancel_reason = 'no_driver')                as unfulfilled_no_driver,
    round(count(*) filter (where status = 'completed')::numeric
          / nullif(count(*) filter (where status in ('completed','cancelled')), 0), 4) as completion_rate,
    percentile_cont(0.5) within group (order by wait_s)               as p50_wait_s,
    percentile_cont(0.9) within group (order by wait_s)               as p90_wait_s,
    coalesce(sum(fare_cents) filter (where status = 'completed'), 0)  as gross_fare_cents
from {{ ref('fct_rides') }} r
join {{ ref('dim_zones') }} z using (zone_id)
where r.requested_at is not null
group by 1, 2, 3
