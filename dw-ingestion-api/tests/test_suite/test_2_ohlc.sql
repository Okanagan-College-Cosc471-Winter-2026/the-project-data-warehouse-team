-- ==========================================
-- TEST 1.2: OHLC Aggregation & Mapping
-- ==========================================
-- GOAL: Prove the procedure correctly finds the absolute HIGH and LOW 
-- across multiple 5-minute ticks, and correctly maps the FIRST open and LAST close.

BEGIN;

-- 1. Inject 3 specific ticks. 
-- Watch the math: The expected 15m result should be:
-- OPEN: 100.00 | HIGH: 115.00 | LOW: 85.00 | CLOSE: 98.00
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source) 
VALUES 
    -- Tick 1: First tick sets the OPEN (100.00)
    ('TEST_MSFT', '2026-01-01 10:00:00-08', 100.00, 105.00, 95.00, 102.00, 1000, 'stock', 'test_suite'),
    -- Tick 2: Middle tick sets the Absolute HIGH (115.00)
    ('TEST_MSFT', '2026-01-01 10:05:00-08', 102.00, 115.00, 100.00, 108.00, 1000, 'stock', 'test_suite'),
    -- Tick 3: Last tick sets the Absolute LOW (85.00) and the CLOSE (98.00)
    ('TEST_MSFT', '2026-01-01 10:10:00-08', 108.00, 109.00, 85.00, 98.00, 1000, 'stock', 'test_suite');

-- 2. Run the Master Procedure
CALL dw.build_warehouse_data('2026-01-01', '2026-01-01');

-- 3. The Assertion: Check the exact column mapping
DO $$
DECLARE
    v_open NUMERIC;
    v_high NUMERIC;
    v_low NUMERIC;
    v_close NUMERIC;
BEGIN
    SELECT open, high, low, close 
    INTO v_open, v_high, v_low, v_close
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_MSFT';

    IF v_open = 100.00 AND v_high = 115.00 AND v_low = 85.00 AND v_close = 98.00 THEN
        RAISE NOTICE '✅ PASS: OHLC Aggregation mapped perfectly. O:%, H:%, L:%, C:%', v_open, v_high, v_low, v_close;
    ELSE
        RAISE EXCEPTION '❌ FAIL: OHLC Mismatch. Expected O:100, H:115, L:85, C:98. Got O:%, H:%, L:%, C:%', v_open, v_high, v_low, v_close;
    END IF;
END $$;

-- 4. Destroy the fake data
ROLLBACK;
