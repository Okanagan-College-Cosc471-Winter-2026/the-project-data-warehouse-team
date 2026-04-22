/* ====================================================================
   SCRIPT: 02_profile_and_filter_universe.sql
   PURPOSE: Evaluates raw 5m data density during Core Market Hours (06:30-13:00 PT)
            across ALL years present in the staging table. Identifies symbols 
            falling below the 70% completion threshold per year and logs them.
   DEPENDENCIES: Requires `01_create_exclusion_log.sql`. Reads from `staging.market_data_5m_bulk`.
   ==================================================================== */

INSERT INTO pipeline.symbol_exclusion_log (
    target_year, symbol, expected_windows, actual_windows, missing_windows, completion_pct, exclusion_reason
)
WITH core_hours_data AS (
    -- 1. Extract Core Hours from the raw 5m staging data and group into 15m buckets
    SELECT
        EXTRACT(YEAR FROM ts AT TIME ZONE 'America/Los_Angeles')::integer AS target_year,
        symbol,
        date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::integer % 15) * INTERVAL '1 minute' AS window_ts
    FROM staging.market_data_5m_bulk
    WHERE (ts AT TIME ZONE 'America/Los_Angeles')::time >= '06:30:00'::time
      AND (ts AT TIME ZONE 'America/Los_Angeles')::time < '13:00:00'::time
),
unique_windows AS (
    -- 2. Consolidate into distinct 15m windows
    SELECT DISTINCT target_year, symbol, window_ts
    FROM core_hours_data
),
global_calendar AS (
    -- 3. Find the baseline expected windows per year across the whole market
    SELECT target_year, COUNT(DISTINCT window_ts) AS expected_core_windows
    FROM unique_windows
    GROUP BY target_year
),
symbol_actuals AS (
    -- 4. Count actual windows per symbol per year
    SELECT target_year, symbol, COUNT(window_ts) AS actual_core_windows
    FROM unique_windows
    GROUP BY target_year, symbol
)
-- 5. Identify the failures (< 70%) and log them
SELECT 
    s.target_year,
    s.symbol,
    g.expected_core_windows AS expected_windows,
    s.actual_core_windows AS actual_windows,
    (g.expected_core_windows - s.actual_core_windows) AS missing_windows,
    ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) AS completion_pct,
    'Failed 70% Core Hours Liquidity Threshold' AS exclusion_reason
FROM symbol_actuals s
JOIN global_calendar g ON s.target_year = g.target_year
WHERE ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) < 70.00
ON CONFLICT (audit_date, target_year, symbol) DO NOTHING;
