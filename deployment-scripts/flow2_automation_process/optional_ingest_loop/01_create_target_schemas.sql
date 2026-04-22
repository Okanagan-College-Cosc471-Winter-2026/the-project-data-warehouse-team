-- 1. Create the new target table for the live stream
CREATE TABLE IF NOT EXISTS staging.market_data_5m_stream (
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
    asset_type TEXT, source TEXT,
    PRIMARY KEY (symbol, ts)
);

-- 2. Ensure it is empty (if you are re-running this)
TRUNCATE TABLE staging.market_data_5m_stream;

-- 3. Nuke the old 29-symbol progress table
TRUNCATE TABLE staging.ingestion_progress;

-- 4. Recreate the schema just in case it was dropped
CREATE TABLE IF NOT EXISTS staging.ingestion_progress (
    symbol TEXT PRIMARY KEY,
    last_ingested_ts TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);
