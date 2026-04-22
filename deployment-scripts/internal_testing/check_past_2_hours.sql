-- capture_snapshot.sql
SELECT 
    window_ts, 
    close, 
    ROUND(sma_close_5, 4) AS sma_close_5, 
    ROUND(pct_change_1, 6) AS pct_change_1,
    is_imputed
FROM dw.live_15m_ffill
WHERE symbol = 'aapl'
  -- Dynamically grab the last 2 hours relative to the pipeline's current state
  AND window_ts > (SELECT value - INTERVAL '2 hours' FROM pipeline.control WHERE key = 'aggregation_last_processed_ts')
ORDER BY window_ts;
