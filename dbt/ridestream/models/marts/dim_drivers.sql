select
    driver_id,
    min(assigned_at)                                     as first_assignment_at,
    max(coalesce(completed_at, cancelled_at, assigned_at)) as last_activity_at,
    count(*)                                             as assignments,
    count(*) filter (where status = 'completed')         as completed_rides,
    count(*) filter (where cancel_reason = 'driver_cancelled') as driver_cancellations,
    coalesce(sum(fare_cents) filter (where status = 'completed'), 0) as lifetime_fare_cents
from {{ ref('fct_rides') }}
where driver_id is not null
group by driver_id
