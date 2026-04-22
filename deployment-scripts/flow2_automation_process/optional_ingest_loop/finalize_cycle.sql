-- finalize_cycle.sql

UPDATE pipeline.cycle_log
   SET cycle_end_ts      = CURRENT_TIMESTAMP,
       duration_seconds  = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - :'cycle_start'::timestamptz))::integer,
       status            = CASE 
                             WHEN (:'rows_ingested' ~ '^[0-9]+$' 
                                   AND :'rows_ingested'::integer > 0)
                             THEN 'success'
                             ELSE 'partial'          -- allowed value when no rows ingested
                           END,
       rows_processed    = COALESCE(NULLIF(:'rows_ingested','')::integer, 0),
       symbols_affected  = COALESCE(NULLIF(:'symbols_ingested','')::integer, 0)
 WHERE cycle_type     = 'ingestion'
   AND cycle_start_ts = :'cycle_start'::timestamptz;

\echo Ingestion cycle completed:
\echo Rows ingested .......... : :rows_ingested
\echo Symbols affected ....... : :symbols_ingested
