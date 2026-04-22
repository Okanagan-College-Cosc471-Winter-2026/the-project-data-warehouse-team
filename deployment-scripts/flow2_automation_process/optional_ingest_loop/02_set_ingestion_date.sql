-- Point the simulator to start drip-feeding from January 2nd, 2024
INSERT INTO staging.ingestion_progress (symbol, last_ingested_ts)
SELECT DISTINCT 
    symbol, 
    '2024-01-02 06:25:00-08'::timestamptz 
FROM staging.market_data_5m_bulk;

-- Verify it worked (Should return 500+ rows)
SELECT COUNT(*) FROM staging.ingestion_progress;
