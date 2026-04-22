WITH time_spine AS (
    -- Get the master calendar of valid market timestamps for the test range
    SELECT DISTINCT window_ts 
    FROM dw.market_data_15m_bulk 
    WHERE window_ts >= '2025-10-13 00:00:00-07' 
      AND window_ts < '2025-10-18 00:00:00-07'
),
active_symbols AS (
    -- Get all symbols that traded during this range
    SELECT DISTINCT symbol 
    FROM dw.market_data_15m_bulk 
    WHERE window_ts >= '2025-10-13 00:00:00-07' 
      AND window_ts < '2025-10-18 00:00:00-07'
),
dense_grid AS (
    -- Create the perfect matrix (Expected Data)
    SELECT s.symbol, t.window_ts
    FROM active_symbols s
    CROSS JOIN time_spine t
)
-- Compare the Expected grid against the Actual data
SELECT 
    g.symbol,
    COUNT(g.window_ts) AS expected_rows,
    COUNT(a.window_ts) AS actual_rows,
    COUNT(g.window_ts) - COUNT(a.window_ts) AS missing_holes,
    ROUND(((COUNT(g.window_ts) - COUNT(a.window_ts))::numeric / COUNT(g.window_ts)) * 100, 2) AS percent_missing
FROM dense_grid g
LEFT JOIN dw.market_data_15m_bulk a 
       ON g.symbol = a.symbol 
      AND g.window_ts = a.window_ts
GROUP BY g.symbol
HAVING COUNT(g.window_ts) - COUNT(a.window_ts) > 0
ORDER BY missing_holes DESC
LIMIT 10;
