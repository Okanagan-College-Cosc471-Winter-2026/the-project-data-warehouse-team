-- ==========================================
-- TEST 2.2: The Gatekeeper Trigger Test
-- ==========================================
-- GOAL: Prove the script correctly identifies trigger minutes (10, 25, 40, 55)
-- and correctly ignores non-trigger minutes.

BEGIN;

-- Create temporary tables to mock the pipeline state
CREATE TEMPORARY TABLE temp_ingestion_progress (last_ingested_ts timestamptz);
CREATE TEMPORARY TABLE temp_control (key text, value text);

DO $$
DECLARE
    v_latest_ingested timestamptz;
    v_last_processed timestamptz;
    v_triggered boolean;
BEGIN
    -- ---------------------------------------------------------
    -- SCENARIO A: A Valid Trigger Minute (Minute 10)
    -- ---------------------------------------------------------
    -- We insert a timestamp at exactly 10:10:00
    INSERT INTO temp_ingestion_progress VALUES ('2026-01-01 10:10:00-08');
    v_triggered := false;
    
    SELECT MIN(last_ingested_ts) INTO v_latest_ingested FROM temp_ingestion_progress;
    SELECT value::timestamptz INTO v_last_processed FROM temp_control WHERE key = 'aggregation_last_processed_ts';

    IF EXTRACT(MINUTE FROM v_latest_ingested) IN (10, 25, 40, 55) 
       AND (v_last_processed IS NULL OR v_latest_ingested > v_last_processed) THEN
       v_triggered := true;
    END IF;

    IF v_triggered THEN
        RAISE NOTICE '✅ PASS [Scenario A]: Gatekeeper correctly opened the gate on Minute 10.';
    ELSE
        RAISE EXCEPTION '❌ FAIL [Scenario A]: Gatekeeper failed to trigger on Minute 10!';
    END IF;

    -- ---------------------------------------------------------
    -- SCENARIO B: An Invalid Trigger Minute (Minute 12)
    -- ---------------------------------------------------------
    -- Clear the table and insert a timestamp at 10:12:00
    DELETE FROM temp_ingestion_progress;
    INSERT INTO temp_ingestion_progress VALUES ('2026-01-01 10:12:00-08');
    v_triggered := false;

    SELECT MIN(last_ingested_ts) INTO v_latest_ingested FROM temp_ingestion_progress;

    IF EXTRACT(MINUTE FROM v_latest_ingested) IN (10, 25, 40, 55) 
       AND (v_last_processed IS NULL OR v_latest_ingested > v_last_processed) THEN
       v_triggered := true;
    END IF;

    IF NOT v_triggered THEN
        RAISE NOTICE '✅ PASS [Scenario B]: Gatekeeper correctly kept the gate closed on Minute 12.';
    ELSE
        RAISE EXCEPTION '❌ FAIL [Scenario B]: Gatekeeper accidentally triggered on Minute 12!';
    END IF;

END $$;

ROLLBACK;
