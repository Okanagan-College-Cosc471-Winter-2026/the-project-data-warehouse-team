-- ====================================================================
-- SCRIPT: ingest_simulator_data.sql
-- PURPOSE: Simulates a live feed by grabbing the NEXT 5-minute window 
--          from the historical database and dropping it into staging.
-- ====================================================================

WITH global_next AS (
    SELECT 
        MIN(last_ingested_ts) AS min_last_ts,
        MIN(last_ingested_ts) + INTERVAL '5 minutes' AS next_window_start
    FROM staging.ingestion_progress
),
to_ingest AS (
    SELECT 
        p.symbol AS ingest_symbol,
        p.last_ingested_ts AS prev_last_ts,
        h.symbol AS data_symbol,
        h.ts AS data_ts,
        h.open, h.high, h.low, h.close, h.volume,
        h.asset_type, h.source,
        g.next_window_start
    FROM staging.ingestion_progress p
    CROSS JOIN global_next g
    LEFT JOIN staging.market_data_5m_bulk h 
           ON h.symbol = p.symbol 
          AND h.ts >= g.next_window_start 
          AND h.ts < g.next_window_start + INTERVAL '5 minutes'
    ORDER BY h.symbol, h.ts
),
inserted_rows AS (
    -- IMPORTANT: Pointing to the _stream table used by our new ML pipeline
    INSERT INTO staging.market_data_5m_stream 
        (symbol, ts, open, high, low, close, volume, asset_type, source)
    SELECT data_symbol, data_ts, open, high, low, close, volume, asset_type, source
    FROM to_ingest
    WHERE data_symbol IS NOT NULL
    ON CONFLICT (symbol, ts) DO NOTHING
    RETURNING symbol
),
updated_progress AS (
    UPDATE staging.ingestion_progress p
       SET last_ingested_ts = u.prev_last_ts + INTERVAL '5 minutes',
           updated_at       = CURRENT_TIMESTAMP
      FROM to_ingest u
     WHERE p.symbol = u.ingest_symbol
)
-- Safely output the exact numbers to bash
SELECT 
    COUNT(*) AS rows_ingested, 
    COUNT(DISTINCT symbol) AS symbols_ingested 
FROM inserted_rows;
