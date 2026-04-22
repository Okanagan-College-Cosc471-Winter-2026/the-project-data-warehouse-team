-- trigger_forced_recovery.sql
DO $$
DECLARE
    v_current_watermark timestamptz;
BEGIN
    -- 1. Find exactly where the pipeline is right now
    SELECT value INTO v_current_watermark 
    FROM pipeline.control 
    WHERE key = 'aggregation_last_processed_ts';

    -- 2. Delete the last 2 hours of data from BOTH live tables
    DELETE FROM dw.live_15m_ffill 
    WHERE window_ts > v_current_watermark - INTERVAL '2 hours';

    DELETE FROM dw.live_15m_agg 
    WHERE window_ts > v_current_watermark - INTERVAL '2 hours';

    -- 3. Walk back the High Watermark so the loop forgets it processed them
    UPDATE pipeline.control
    SET value = value - INTERVAL '2 hours',
        updated_at = CURRENT_TIMESTAMP,
        note = 'DEMO: Forced Recovery - Rolled back 2 hours'
    WHERE key = 'aggregation_last_processed_ts';

    RAISE NOTICE '🔥 Sabotage complete. Wiped 2 hours of data and rolled watermark back to %', (v_current_watermark - INTERVAL '2 hours');
END $$;
