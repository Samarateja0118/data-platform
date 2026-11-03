select ride_id, requested_at, assigned_at, started_at, completed_at
from {{ ref('fct_rides') }}
where assigned_at < requested_at
   or started_at < assigned_at
   or completed_at < started_at
