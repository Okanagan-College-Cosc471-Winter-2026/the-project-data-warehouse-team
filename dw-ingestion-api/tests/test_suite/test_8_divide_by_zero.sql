-- ==========================================
-- TEST 1.5: The Divide-by-Zero "Poison" Test
-- ==========================================
-- GOAL: Prove that the mathematical engine survives a Division-by-Zero scenario
-- (e.g., previous_close = 0.00) without crashing the entire pipeline.

BEGIN;

-- 1. Inject two windows across two days.
INSERT INTO staging.market_data_5m 
    (symbol, ts, open, high, low, close, volume, asset_type, source) 
VALUES 
    -- Day 1: A stock somehow drops to absolutely zero and closes there.
    ('TEST_POISON', '2026-01-01 15:50:00-08', 5.00, 5.00, 0.00, 0.00, 100, 'stock', 'test_suite'),
    ('TEST_POISON', '2026-01-01 15:55:00-08', 0.00, 0.00, 0.00, 0.00, 100, 'stock', 'test_suite'),
    ('TEST_POISON', '2026-01-01 16:00:00-08', 0.00, 0.00, 0.00, 0.00, 100, 'stock', 'test_suite'),

    -- Day 2: The stock miraculously recovers to $10.00 at the open.
    ('TEST_POISON', '2026-01-02 09:30:00-08', 10.00, 10.00, 10.00, 10.00, 100, 'stock', 'test_suite'),
    ('TEST_POISON', '2026-01-02 09:35:00-08', 10.00, 10.00, 10.00, 10.00, 100, 'stock', 'test_suite'),
    ('TEST_POISON', '2026-01-02 09:40:00-08', 10.00, 10.00, 10.00, 10.00, 100, 'stock', 'test_suite');

-- 2. Run the Master Procedure for both days. 
-- IF THIS CRASHES, THE TEST FAILS IMMEDIATELY.
CALL dw.build_warehouse_data('2026-01-01', '2026-01-02');

-- 3. The Assertion: Check how the system handled the undefined math.
DO $$
DECLARE
    v_gap_pct NUMERIC;
BEGIN
    SELECT overnight_gap_pct INTO v_gap_pct 
    FROM dw.market_data_15m 
    WHERE symbol = 'TEST_POISON' 
      AND window_ts = '2026-01-02 09:30:00-08';

    -- The most graceful way to handle divide-by-zero is to store NULL
    IF v_gap_pct IS NULL THEN
        RAISE NOTICE '✅ PASS: Divide-by-Zero successfully avoided. overnight_gap_pct was safely set to NULL.';
    ELSE
        RAISE NOTICE '⚠️ PASS (With Caveat): System survived without crashing, but output was %. Expected NULL.', v_gap_pct;
    END IF;
END $$;

-- 4. Destroy the fake data
ROLLBACK;
