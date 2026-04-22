-- This script will display how complete the windows for the aggregated 15m table ended up being.
-- The ranged can be specified by changing the WHERE clause in the core_hours_data section.

WITH core_hours_data AS (
    -- 1. Filter the entire dataset down to JUST standard market hours
    SELECT symbol, window_ts
    FROM dw.market_data_15m_bulk
    WHERE window_ts >= '2024-01-01 00:00:00-08' 
      AND window_ts < '2025-01-01 00:00:00-08'
      -- Isolate the time of day in Pacific Time
      AND (window_ts AT TIME ZONE 'America/Los_Angeles')::time >= '06:30:00'::time
      AND (window_ts AT TIME ZONE 'America/Los_Angeles')::time < '13:00:00'::time
),
global_calendar AS (
    -- 2. Count how many core 15m windows actually exist in the 2024 calendar
    SELECT COUNT(DISTINCT window_ts) AS expected_core_windows
    FROM core_hours_data
),
symbol_actuals AS (
    -- 3. Count how many of those core windows each symbol actually has
    SELECT 
        symbol,
        COUNT(window_ts) AS actual_core_windows
    FROM core_hours_data
    GROUP BY symbol
)
-- 4. Calculate the TRUE health of the stock
SELECT 
    s.symbol,
    s.actual_core_windows,
    g.expected_core_windows,
    g.expected_core_windows - s.actual_core_windows AS missing_core_windows,
    ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) AS core_completion_pct
FROM symbol_actuals s
CROSS JOIN global_calendar g
ORDER BY core_completion_pct DESC;
