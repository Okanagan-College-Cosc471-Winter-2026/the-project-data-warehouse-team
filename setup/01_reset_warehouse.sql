-- Purpose: Reset the warehouse table to a clean state for demonstration
-- Warning: This permanently deletes all data in dw.market_data_15m

TRUNCATE TABLE dw.market_data_15m RESTART IDENTITY;

-- Optional: Vacuum to reclaim space (good practice after truncate)
VACUUM dw.market_data_15m;

-- Confirmation query (optional but useful for demo)
SELECT COUNT(*) AS remaining_rows FROM dw.market_data_15m;
