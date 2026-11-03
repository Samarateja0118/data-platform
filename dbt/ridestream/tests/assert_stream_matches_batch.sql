-- The core correctness check: for every closed window, the streaming
-- aggregate must equal a batch recount of the on-time events in raw.
select *
from {{ ref('recon_stream_vs_batch') }}
where diff_requests <> 0 or diff_completions <> 0 or diff_cancellations <> 0
   or diff_fare_cents <> 0 or diff_events <> 0
