SELECT 
    DATE(ts) AS market_date,
    MIN(ts) AS first_tick,
    MAX(ts) AS last_tick,
    COUNT(DISTINCT ts) AS total_5m_ticks
FROM staging.market_data_5m_bulk
WHERE symbol = 'aapl'  -- A stock that is guaranteed to trade every 5 minutes
  AND ts >= '2023-07-10' 
  AND ts < '2023-07-15'
GROUP BY DATE(ts)
ORDER BY market_date;
