/* ====================================================================
   BI VERIFICATION QUERY
   Shows the top symbols by volume for the latest available trading day, 
   broken down by Market Session. Proves all dimensional joins work.
   ==================================================================== */

SELECT 
    s.symbol,
    d.full_date,
    t.session_type,
    COUNT(f.time_slot_key) AS windows_recorded,
    SUM(f.volume) AS session_volume,
    ROUND(AVG(f.close), 2) AS avg_price,
    MAX(f.high) AS session_high,
    MIN(f.low) AS session_low,
    -- Quick check to ensure we are looking at raw/unimputed data
    COUNT(CASE WHEN f.status = 'provisional' THEN 1 END) AS provisional_windows
FROM star.fact_market_data_15m f
JOIN star.dim_symbol s ON f.symbol_key = s.symbol_key
JOIN star.dim_date d ON f.date_key = d.date_key
JOIN star.dim_timeslot t ON f.time_slot_key = t.time_slot_key

-- Dynamically filter to only show the absolute most recent day in the warehouse
WHERE d.full_date = (
    SELECT MAX(d2.full_date) 
    FROM star.dim_date d2 
    JOIN star.fact_market_data_15m f2 ON d2.date_key = f2.date_key
)
GROUP BY 
    s.symbol, 
    d.full_date, 
    t.session_type
ORDER BY 
    session_volume DESC
LIMIT 20;
