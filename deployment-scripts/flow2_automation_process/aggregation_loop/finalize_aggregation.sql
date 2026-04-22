-- finalize_aggregation.sql
-- Finalize the most recent 'running' or 'partial' aggregation cycle

UPDATE pipeline.cycle_log
   SET cycle_end_ts      = CURRENT_TIMESTAMP,
       duration_seconds  = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - cycle_start_ts))::integer,
       status            = 'success'
 WHERE cycle_type     = 'aggregation'
   AND status         IN ('partial', 'running')
   AND cycle_start_ts = (
       SELECT MAX(cycle_start_ts)
       FROM pipeline.cycle_log
       WHERE cycle_type = 'aggregation'
         AND status IN ('partial', 'running')
   );

-- Log completion (visible in psql output)
\echo Aggregation cycle finalized for most recent pending entry.
\echo Check pipeline.cycle_log for details:
SELECT cycle_start_ts, status, duration_seconds
FROM pipeline.cycle_log
WHERE cycle_type = 'aggregation'
ORDER BY cycle_start_ts DESC
LIMIT 1;
