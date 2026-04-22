/* ====================================================================
   SCRIPT: 02_profile_and_filter_universe.sql
   PURPOSE: Evaluates 5m data density during Core Market Hours (06:30-13:00 PT).
            Identifies symbols falling below the 70% completion threshold
            and inserts them into the exclusion log with failure metrics.
   DEPENDENCIES: Requires `01_create_exclusion_log.sql` to be executed first.
                 Reads from raw staging tables.
   ==================================================================== */

INSERT INTO pipeline.symbol_exclusion_log (
    target_year, symbol, expected_windows, actual_windows, missing_windows, completion_pct, exclusion_reason
)
WITH core_hours_data AS (
    -- 1. Filter to 2024 Core Market Hours (Pacific Time)
    SELECT symbol, window_ts
    FROM dw.market_data_15m_bulk
    WHERE window_ts >= '2024-01-01 00:00:00-08' 
      AND window_ts < '2025-01-01 00:00:00-08'
      AND (window_ts AT TIME ZONE 'America/Los_Angeles')::time >= '06:30:00'::time
      AND (window_ts AT TIME ZONE 'America/Los_Angeles')::time < '13:00:00'::time
),
global_calendar AS (
    SELECT COUNT(DISTINCT window_ts) AS expected_core_windows
    FROM core_hours_data
),
symbol_actuals AS (
    SELECT symbol, COUNT(window_ts) AS actual_core_windows
    FROM core_hours_data
    GROUP BY symbol
)
-- 2. Identify the failures (< 70%) and log them
SELECT 
    2024 AS target_year,
    s.symbol,
    g.expected_core_windows AS expected_windows,
    s.actual_core_windows AS actual_windows,
    (g.expected_core_windows - s.actual_core_windows) AS missing_windows,
    ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) AS completion_pct,
    'Failed 70% Core Hours Liquidity Threshold' AS exclusion_reason
FROM symbol_actuals s
CROSS JOIN global_calendar g
WHERE ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) < 70.00
ON CONFLICT (audit_date, target_year, symbol) DO NOTHING;
