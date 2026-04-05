-- ==========================================
-- TEST 1.1: Idempotency & Conflict Handling
-- ==========================================
-- GOAL: Prove that running the aggregation procedure multiple times 
-- on the exact same staging data does not duplicate rows in the DW.

BEGIN;

-- 1. Inject 3 fake 5-minute ticks into staging (Representing one 15m window)
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source) 
VALUES 
    ('TEST_AAPL', '2026-01-01 10:00:00-08', 150.00, 151.00, 149.00, 150.50, 1000, 'stock', 'test_suite'),
    ('TEST_AAPL', '2026-01-01 10:05:00-08', 150.50, 152.00, 150.00, 151.50, 1200, 'stock', 'test_suite'),
    ('TEST_AAPL', '2026-01-01 10:10:00-08', 151.50, 151.50, 148.00, 148.50, 1500, 'stock', 'test_suite');

-- 2. Run the Master Procedure for the FIRST time
CALL dw.build_warehouse_data('2026-01-01', '2026-01-01');

-- 3. Run the Master Procedure for the SECOND time (This should trigger the ON CONFLICT)
CALL dw.build_warehouse_data('2026-01-01', '2026-01-01');

-- 4. The Assertion: Check how many rows exist for TEST_AAPL. 
-- If idempotency is working, count MUST be exactly 1.
DO $$
DECLARE
    v_row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_row_count 
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_AAPL';

    IF v_row_count = 1 THEN
        RAISE NOTICE '✅ PASS: Idempotency Test. Row count is exactly %.', v_row_count;
    ELSE
        RAISE EXCEPTION '❌ FAIL: Idempotency Test. Expected 1 row, but found %.', v_row_count;
    END IF;
END $$;

-- 5. Destroy the fake data and revert the database state
ROLLBACK;
