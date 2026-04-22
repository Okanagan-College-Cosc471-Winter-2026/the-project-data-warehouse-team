-- ====================================================================
-- SCRIPT: pipeline_control.sql
-- PURPOSE: Manually set the High Watermark for the Live Aggregation Loop.
--          Update the SET value to the timestamp of the last fully 
--          processed 15-minute window before starting the live heartbeat.
-- ====================================================================

INSERT INTO pipeline.control (key, value, updated_at, note)
VALUES (
    'aggregation_last_processed_ts', 
    '2024-01-02 06:15:00-07'::timestamptz, -- CHANGE THIS TO YOUR DESIRED START TIME
    CURRENT_TIMESTAMP, 
    'Manual initialization for live loop transition'
)
ON CONFLICT (key) DO UPDATE 
SET value = EXCLUDED.value, 
    updated_at = EXCLUDED.updated_at,
    note = EXCLUDED.note;
