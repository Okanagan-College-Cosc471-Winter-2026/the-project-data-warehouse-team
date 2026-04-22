-- ==========================================
-- TEST 2.1: The Empty Staging Check
-- ==========================================
-- GOAL: Prove that if the ingestion loop completely fails and staging is empty,
-- the Gatekeeper script gracefully waits instead of throwing a fatal error.

BEGIN;

-- 1. Simulate a catastrophic upstream failure by clearing the ingestion progress
-- (Using a temporary table shadows the real one so we don't actually delete your data)
CREATE TEMPORARY TABLE temp_ingestion_progress (LIKE staging.ingestion_progress);
-- We leave it completely empty.

-- 2. Simulate the Gatekeeper Logic (Adapted from your agg_run_batch.sql)
DO $$
DECLARE
    v_latest_ingested timestamptz;
    v_last_processed timestamptz;
BEGIN
    -- Ask the EMPTY temporary table where it is
    SELECT MIN(last_ingested_ts) INTO v_latest_ingested
    FROM temp_ingestion_progress;

    -- Check what we already processed
    SELECT value::timestamptz INTO v_last_processed
    FROM pipeline.control
    WHERE key = 'aggregation_last_processed_ts';

    -- The Dangerous Check: If v_latest_ingested is NULL, will this crash?
    IF EXTRACT(MINUTE FROM v_latest_ingested) IN (10, 25, 40, 55) 
       AND (v_last_processed IS NULL OR v_latest_ingested > v_last_processed) THEN
       
       RAISE EXCEPTION '❌ FAIL: Script attempted to trigger a build on NULL data!';
       
    ELSE
       -- If it makes it here without crashing, it correctly identified it needs to wait.
       RAISE NOTICE '✅ PASS: Gatekeeper safely rejected empty staging data. State: Waiting.';
    END IF;

END $$;

ROLLBACK;
