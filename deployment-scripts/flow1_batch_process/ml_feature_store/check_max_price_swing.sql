SELECT
    s.symbol,
    d.day_name,
    t.session_type,
    COUNT(f.time_slot_key) AS total_15m_windows,
    SUM(f.volume) AS total_volume,
    MAX(f.high) - MIN(f.low) AS max_price_swing
FROM star.fact_market_data_15m f
JOIN star.dim_symbol s ON f.symbol_key = s.symbol_key
JOIN star.dim_date d ON f.date_key = d.date_key
JOIN star.dim_timeslot t ON f.time_slot_key = t.time_slot_key
WHERE s.symbol IN ('AAPL', 'aapl', 'AMD', 'amd')
  AND t.is_market_open = true
GROUP BY
    s.symbol,
    d.day_name,
    t.session_type
ORDER BY
    s.symbol,
    total_volume DESC;
