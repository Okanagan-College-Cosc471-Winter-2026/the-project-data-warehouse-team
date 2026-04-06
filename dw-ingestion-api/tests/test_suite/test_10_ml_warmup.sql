-- ==========================================
-- TEST 1.6: The ML Feature Warmup Test
-- ==========================================
-- GOAL: Prove that long-tail rolling features (like SMA-20) output NULL 
-- until they have exactly enough historical data to calculate accurately,
-- protecting downstream ML models from premature, inaccurate math.

BEGIN;

-- 1. Dynamically inject exactly 60 sequential 5-minute ticks (20 complete 15m windows)
-- This spans 5 hours, starting at 10:00 AM and ending at 2:55 PM.
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source)
SELECT 
    'TEST_WARMUP',
    '2026-01-01 10:00:00-08'::timestamptz + (g.i * interval '5 minutes'),
    10.00, 10.00, 10.00, 10.00, 100, 
    'stock', 'test_suite'
FROM generate_series(0, 59) AS g(i);

-- 2. Run the Master Procedure for the day
CALL dw.build_warehouse_data('2026-01-01', '2026-01-01');

-- 3. The Assertion: Check the 19th and 20th windows.
DO $$
DECLARE
    v_sma_19 NUMERIC;
    v_sma_20 NUMERIC;
BEGIN
    -- Grab the 19th window (14:30:00)
    SELECT sma_close_20 INTO v_sma_19 
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_WARMUP' AND window_ts = '2026-01-01 14:30:00-08';

    -- Grab the 20th window (14:45:00)
    SELECT sma_close_20 INTO v_sma_20 
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_WARMUP' AND window_ts = '2026-01-01 14:45:00-08';

    -- The 19th window MUST be NULL. The 20th window MUST be 10.00.
    IF v_sma_19 IS NULL AND v_sma_20 = 10.00 THEN
        RAISE NOTICE '✅ PASS: ML Warmup Gate. SMA-20 safely returned NULL until window 20.';
    ELSE
        RAISE EXCEPTION '❌ FAIL: Warmup Error. Window 19 SMA: %, Window 20 SMA: %', v_sma_19, v_sma_20;
    END IF;
END $$;

-- 4. Destroy the fake data
ROLLBACK;
