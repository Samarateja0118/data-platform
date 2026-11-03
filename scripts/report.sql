\echo '== Stream health'
SELECT count(*) AS partitions, max(updated_at) AS last_commit, min(watermark) AS global_watermark
FROM ops.consumer_offsets WHERE topic = 'rides.events';

\echo '== Landing zone'
SELECT count(*) AS events, count(*) FILTER (WHERE is_late) AS late,
       (SELECT count(*) FROM ops.dead_letters) AS dead_letters
FROM raw.ride_events;

\echo '== Produce -> queryable latency, last 5 minutes (ms)'
SELECT round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p99
FROM (SELECT extract(epoch FROM ingested_at - kafka_ts) * 1000 AS ms
      FROM raw.ride_events WHERE ingested_at > now() - interval '5 minutes') t;

\echo '== Latest closed streaming windows (busiest zones)'
SELECT window_start, zone_id, requests, completions, cancellations, gross_fare_cents
FROM stream.zone_minute_metrics_closed ORDER BY window_start DESC, requests DESC LIMIT 6;

\echo '== Ride status (batch, fct_rides)'
SELECT status, count(*) FROM marts.fct_rides GROUP BY 1 ORDER BY 2 DESC;

\echo '== Stream vs batch reconciliation (Airflow runs)'
SELECT run_id, checked_at, windows_checked, windows_matched, late_events, passed
FROM ops.reconciliation_runs ORDER BY run_id DESC LIMIT 5;
