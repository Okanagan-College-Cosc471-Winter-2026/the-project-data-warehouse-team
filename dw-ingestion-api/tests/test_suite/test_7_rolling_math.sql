-- ==========================================
-- TEST 1.4: The Rolling Window Math Test
-- ==========================================
-- GOAL: Prove that rolling aggregations (like SMA) correctly look back 
-- across the specified number of preceding complete windows.

BEGIN;

-- 1. Inject 5 complete 15-minute windows (3 ticks each).
-- We ensure the LAST tick of each window hits our target closing price (10, 20, 30, 40, 50).
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source) 
VALUES 
    -- Window 1: Closes at 10
    ('TEST_SMA', '2026-01-01 10:00:00-08', 10, 10, 10, 10, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:05:00-08', 10, 10, 10, 10, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:10:00-08', 10, 10, 10, 10, 100, 'stock', 'test_suite'),

    -- Window 2: Closes at 20
    ('TEST_SMA', '2026-01-01 10:15:00-08', 20, 20, 20, 20, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:20:00-08', 20, 20, 20, 20, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:25:00-08', 20, 20, 20, 20, 100, 'stock', 'test_suite'),

    -- Window 3: Closes at 30
    ('TEST_SMA', '2026-01-01 10:30:00-08', 30, 30, 30, 30, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:35:00-08', 30, 30, 30, 30, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:40:00-08', 30, 30, 30, 30, 100, 'stock', 'test_suite'),

    -- Window 4: Closes at 40
    ('TEST_SMA', '2026-01-01 10:45:00-08', 40, 40, 40, 40, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:50:00-08', 40, 40, 40, 40, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 10:55:00-08', 40, 40, 40, 40, 100, 'stock', 'test_suite'),

    -- Window 5: Closes at 50
    ('TEST_SMA', '2026-01-01 11:00:00-08', 50, 50, 50, 50, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 11:05:00-08', 50, 50, 50, 50, 100, 'stock', 'test_suite'),
    ('TEST_SMA', '2026-01-01 11:10:00-08', 50, 50, 50, 50, 100, 'stock', 'test_suite');

-- 2. Run the Master Procedure
CALL dw.build_warehouse_data('2026-01-01', '2026-01-01');

-- 3. The Assertion: Check the SMA on the 5th window
DO $$
DECLARE
    v_sma NUMERIC;
    v_status TEXT;
BEGIN
    SELECT sma_close_5, status INTO v_sma, v_status
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_SMA' 
      AND window_ts = '2026-01-01 11:00:00-08';

    -- The average of 10, 20, 30, 40, and 50 is exactly 30.00
    IF v_sma = 30.00 AND v_status = 'complete' THEN
        RAISE NOTICE '✅ PASS: SMA-5 rolling window calculated perfectly as % on a complete window.', v_sma;
    ELSE
        RAISE EXCEPTION '❌ FAIL: Expected SMA 30.00 and status complete, but got SMA % with status %.', v_sma, v_status;
    END IF;
END $$;

-- 4. Destroy the fake data
ROLLBACK;
