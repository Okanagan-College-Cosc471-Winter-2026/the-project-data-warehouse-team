SELECT
    COUNT(*) as total_training_rows,
    COUNT(DISTINCT symbol_key) as total_symbols,
    MIN(date_key) as earliest_date_key,
    MAX(date_key) as latest_date_key
FROM ml.historical_feature_store;
