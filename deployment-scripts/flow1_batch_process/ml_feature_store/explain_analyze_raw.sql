EXPLAIN ANALYZE
SELECT
    symbol,
    SUM(volume) AS total_volume,
    MAX(high) - MIN(low) AS max_price_swing
FROM dw.extract_15m_ffill
WHERE window_ts >= '2023-07-10 09:30:00-07'
  AND window_ts <= '2025-12-31 16:00:00-07'
GROUP BY symbol;
