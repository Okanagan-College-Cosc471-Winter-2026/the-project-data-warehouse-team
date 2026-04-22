EXPLAIN ANALYZE
SELECT
    s.symbol,
    SUM(f.volume) AS total_volume,
    MAX(f.high) - MIN(f.low) AS max_price_swing
FROM star.fact_market_data_15m f
JOIN star.dim_symbol s ON f.symbol_key = s.symbol_key
JOIN star.dim_timeslot t ON f.time_slot_key = t.time_slot_key
WHERE f.date_key BETWEEN 20230710 AND 20251231
  AND t.is_market_open = true
GROUP BY s.symbol;
