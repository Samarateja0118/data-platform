-- Fails the build if producers start emitting garbage at scale.
with c as (
    select (select count(*) from {{ source('ops', 'dead_letters') }})::numeric as bad,
           (select count(*) from {{ source('raw', 'ride_events') }})::numeric as good
)
select * from c where bad / nullif(bad + good, 0) > 0.01
