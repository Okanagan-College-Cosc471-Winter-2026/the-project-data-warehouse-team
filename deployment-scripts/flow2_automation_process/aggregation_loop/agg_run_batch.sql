-- ====================================================================
-- SCRIPT: agg_run_batch.sql
-- PURPOSE: The core execution logic for the Live Heartbeat. 
--          Evaluates the staging progress against the High Watermark.
--          Triggers feature calculation safely at offset intervals.
-- ====================================================================

DO $$
DECLARE
    v_latest_ingested timestamptz;
    v_last_processed timestamptz;
    v_process_date date;
BEGIN
    -- 1. Check where the ingestion stream currently is
    SELECT MIN(last_ingested_ts) INTO v_latest_ingested
    FROM staging.ingestion_progress;

    -- 2. Check the High Watermark
    SELECT value::timestamptz INTO v_last_processed
    FROM pipeline.control
    WHERE key = 'aggregation_last_processed_ts';

    -- 3. Live Trigger: Check if we hit a safe offset minute (10 mins past the boundary)
    --    AND we haven't processed this exact timestamp yet.
    IF EXTRACT(MINUTE FROM v_latest_ingested) IN (10, 25, 40, 55)
       AND (v_last_processed IS NULL OR v_latest_ingested > v_last_processed) THEN

        v_process_date := DATE(v_latest_ingested AT TIME ZONE 'America/Los_Angeles');

        RAISE NOTICE '🔥 Live Trigger reached! Latest staging data is %. Processing %',
                     v_latest_ingested, v_process_date;

        -- 4. Execute the Machine Learning Feature Store Engines
        CALL dw.build_warehouse_live(v_process_date, v_process_date);
        CALL dw.build_15m_bounded_ffill_live(v_process_date, v_process_date);

        -- 5. Advance the High Watermark
        INSERT INTO pipeline.control (key, value, updated_at)
        VALUES ('aggregation_last_processed_ts', v_latest_ingested, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;

    ELSE
        RAISE NOTICE '⏳ Waiting for new complete window. Current staging: %, Last processed: %',
                     v_latest_ingested, COALESCE(v_last_processed::text, 'None');
    END IF;

END $$;
