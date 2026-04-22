EXPLAIN ANALYZE
SELECT
    symbol_key,
    SUM(volume) AS total_volume,
    MAX(high) - MIN(low) AS max_price_swing
FROM ml.historical_feature_store
WHERE date_key BETWEEN 20230710 AND 20251231
  -- Filtering by time_slot_key ranges instead of timestamps
  AND time_slot_key BETWEEN 930 AND 1600 
GROUP BY symbol_key;
