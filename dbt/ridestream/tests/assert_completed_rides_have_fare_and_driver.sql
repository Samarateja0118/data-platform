select ride_id from {{ ref('fct_rides') }}
where status = 'completed' and (fare_cents is null or fare_cents <= 0 or driver_id is null)
