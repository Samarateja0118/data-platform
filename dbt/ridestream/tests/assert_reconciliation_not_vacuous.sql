-- Guard against a reconciliation that "passes" because it compared nothing
-- (e.g. wrong topic name, consumer never reported a watermark).
select 1 as problem
where (select count(*) from {{ ref('recon_stream_vs_batch') }}) = 0
  and (select count(*) from {{ source('raw', 'ride_events') }}) > 1000
