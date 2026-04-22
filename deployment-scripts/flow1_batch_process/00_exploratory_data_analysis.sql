/* ====================================================================
   SCRIPT: 00_exploratory_data_analysis.sql
   PURPOSE: Read-only diagnostic query. Allows the user to visually inspect
            the health of the raw 5m staging data during Core Market Hours
            (06:30-13:00 PT) before making any physical changes to the database.
   DEPENDENCIES: Reads from `staging.market_data_5m_bulk`.
   ==================================================================== */

WITH core_hours_data AS (
    -- 1. Grab raw 5m ticks, group by year, and squash into virtual 15m buckets
    SELECT
        EXTRACT(YEAR FROM ts AT TIME ZONE 'America/Los_Angeles')::integer AS target_year,
        symbol,
        date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::integer % 15) * INTERVAL '1 minute' AS window_ts
    FROM staging.market_data_5m_bulk
    WHERE (ts AT TIME ZONE 'America/Los_Angeles')::time >= '06:30:00'::time
      AND (ts AT TIME ZONE 'America/Los_Angeles')::time < '13:00:00'::time
),
unique_windows AS (
    -- 2. Consolidate the virtual 15m blocks
    SELECT DISTINCT target_year, symbol, window_ts
    FROM core_hours_data
),
global_calendar AS (
    -- 3. Calculate the global denominator per year (~6,552 for a full year)
    SELECT target_year, COUNT(DISTINCT window_ts) AS expected_core_windows
    FROM unique_windows
    GROUP BY target_year
),
symbol_actuals AS (
    -- 4. Count the actual virtual 15m blocks per symbol per year
    SELECT target_year, symbol, COUNT(window_ts) AS actual_core_windows
    FROM unique_windows
    GROUP BY target_year, symbol
)
-- 5. Display the worst offenders first to prove the need for the filter
SELECT 
    s.target_year,
    s.symbol,
    s.actual_core_windows,
    g.expected_core_windows,
    ROUND((s.actual_core_windows::numeric / g.expected_core_windows) * 100, 2) AS completion_pct
FROM symbol_actuals s
JOIN global_calendar g ON s.target_year = g.target_year
ORDER BY s.target_year ASC, completion_pct ASC;
