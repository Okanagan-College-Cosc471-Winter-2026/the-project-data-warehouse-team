-- tests/integration/test_aggregation_roundtrip_safe.sql
-- Purpose: Fully isolated aggregation test using test schema

BEGIN;

-- Set search_path so procedure reads from test.market_data_5m instead of historical
SET search_path = test, public;

-- Clear any prior test results in warehouse
DELETE FROM dw.market_data_15m
WHERE symbol = 'TESTSYM'
  AND window_ts = '2025-01-02 09:00:00-07';

-- Insert test data into isolated table
INSERT INTO test.market_data_5m (symbol, ts, open, high, low, close, volume, asset_type, source)
VALUES
    ('TESTSYM', '2025-01-02 09:00:00-07', 100.00, 101.00,  99.00, 100.50, 1000, 'stock', 'test'),
    ('TESTSYM', '2025-01-02 09:05:00-07', 100.50, 102.00, 100.00, 101.50, 1500, 'stock', 'test'),
    ('TESTSYM', '2025-01-02 09:10:00-07', 101.50, 103.00, 101.00, 102.00, 2000, 'stock', 'test');

-- Run the procedure — it now reads from test.market_data_5m
CALL dw.process_15min_window('2025-01-02 09:00:00-07');

-- Verify
DO $$
DECLARE
    v_count     integer;
    v_slot      integer;
    v_open      numeric(18,6);
    v_high      numeric(18,6);
    v_low       numeric(18,6);
    v_close     numeric(18,6);
    v_volume    bigint;
BEGIN
    SELECT
        COUNT(*), slot_count, open, high, low, close, volume
    INTO
        v_count, v_slot, v_open, v_high, v_low, v_close, v_volume
    FROM dw.market_data_15m
    WHERE symbol = 'TESTSYM'
      AND window_ts = '2025-01-02 09:00:00-07' GROUP BY slot_count, open, high, low, close, volume;

    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Expected exactly 1 aggregated row, found %', v_count;
    END IF;

    IF v_slot <> 3 THEN
        RAISE EXCEPTION 'Expected slot_count = 3, found %', v_slot;
    END IF;

    IF v_open <> 100.00 OR v_high <> 103.00 OR v_low <> 99.00 OR v_close <> 102.00 OR v_volume <> 4500 THEN
        RAISE EXCEPTION 'OHLCV or volume mismatch: expected (100.00, 103.00, 99.00, 102.00, 4500), got (%, %, %, %, %)',
            v_open, v_high, v_low, v_close, v_volume;
    END IF;
END $$;

-- Restore normal search path
RESET search_path;

ROLLBACK;

DO $$ BEGIN RAISE NOTICE 'Safe aggregation roundtrip test passed successfully.'; END $$;

