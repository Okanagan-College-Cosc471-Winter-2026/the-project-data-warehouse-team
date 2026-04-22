-- ==========================================
-- TEST 1.3: The Gap & Lag Test (Phase 3)
-- ==========================================
-- GOAL: Prove the procedure accurately calculates backward-looking 
-- time-series features (Lags, Previous Close, Gap Indicators) across a day boundary.

BEGIN;

-- 1. Inject two days of data with an explicit Overnight Gap Up.
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source) 
VALUES 
    -- Day 1: End of day. Sets the "Previous Close" to 100.00
    ('TEST_TSLA', '2026-01-01 15:45:00-08', 95.00, 105.00, 95.00, 100.00, 1000, 'stock', 'test_suite'),
    
    -- Day 2: Market Open. Gaps UP to 110.00, closes window at 112.00
    ('TEST_TSLA', '2026-01-02 09:30:00-08', 110.00, 115.00, 108.00, 112.00, 1000, 'stock', 'test_suite');

-- 2. Run the Master Procedure for both days
CALL dw.build_warehouse_data('2026-01-01', '2026-01-02');

-- 3. The Assertion: Check the Phase 3 Window Functions on Day 2
DO $$
DECLARE
    v_prev_close NUMERIC;
    v_lag_close_1 NUMERIC;
    v_is_gap_up SMALLINT;
    v_close_diff NUMERIC;
BEGIN
    -- We specifically check the Day 2 row to see if it correctly looked back at Day 1
    SELECT 
        previous_close, 
        lag_close_1, 
        is_gap_up,
        close_diff_1
    INTO 
        v_prev_close, 
        v_lag_close_1, 
        v_is_gap_up,
        v_close_diff
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_TSLA' 
      AND window_ts = '2026-01-02 09:30:00-08';

    -- The Expected Math:
    -- Previous Close should be Day 1's close: 100.00
    -- Lag Close 1 should be Day 1's close: 100.00
    -- Is Gap Up should be 1 (True) because Day 2 Open (110) > Day 1 Close (100)
    -- Close Diff should be 12.00 (Day 2 Close [112] - Lag Close [100])
    
    IF v_prev_close = 100.00 AND v_lag_close_1 = 100.00 AND v_is_gap_up = 1 AND v_close_diff = 12.00 THEN
        RAISE NOTICE '✅ PASS: Phase 3 Time-Series Math is perfect across day boundaries.';
    ELSE
        RAISE EXCEPTION '❌ FAIL: Phase 3 Mismatch. Got Prev:%, Lag:%, GapUp:%, Diff:%', v_prev_close, v_lag_close_1, v_is_gap_up, v_close_diff;
    END IF;
END $$;

-- 4. Destroy the fake data
ROLLBACK;
