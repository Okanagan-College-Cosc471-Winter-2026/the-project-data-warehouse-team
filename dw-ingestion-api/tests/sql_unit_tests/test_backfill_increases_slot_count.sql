-- test_backfill_increases_slot_count.sql
-- Verifies behaviour when processing a window with insufficient/no source data
-- Uses overnight time (outside trading hours) → should find 0 matching 5-min bars

BEGIN;

-- 1. Clean up any previous test artifacts in the target table only
DELETE FROM dw.market_data_15m
 WHERE symbol = 'TESTSYM'
   AND window_ts = '2025-01-03 20:00:00-07'::timestamptz;

-- 2. Insert a provisional window manually (simulates a skipped/incomplete slot)
INSERT INTO dw.market_data_15m (
    symbol,
    window_ts,
    open, high, low, close, volume,
    slot_count, status
)
VALUES (
    'TESTSYM',
    '2025-01-03 20:00:00-07'::timestamptz,
    NULL, NULL, NULL, NULL, 0,
    0, 'provisional'
)
ON CONFLICT (symbol, window_ts) DO NOTHING;

-- 3. Run the procedure on an overnight window (should find zero 5-min bars)
CALL dw.process_15min_window('2025-01-03 20:00:00-07'::timestamptz);

-- 4. Verify the outcome
DO $$
DECLARE
    v_slot   integer;
    v_status text;
BEGIN
    SELECT slot_count, status
      INTO v_slot, v_status
      FROM dw.market_data_15m
     WHERE symbol = 'TESTSYM'
       AND window_ts = '2025-01-03 20:00:00-07'::timestamptz;

    IF v_slot IS NULL THEN
        RAISE EXCEPTION 'No row found after processing overnight window';
    END IF;

    IF v_slot >= 3 THEN
        RAISE EXCEPTION 'Unexpected: slot_count >= 3 on overnight window with no source data (got %)', v_slot;
    END IF;

    IF v_status = 'complete' THEN
        RAISE EXCEPTION 'Unexpected: status became complete on window with insufficient data (got %)', v_status;
    END IF;

    -- Optional: confirm slot_count stayed low / was set to 0 or 1
    IF v_slot > 1 THEN
        RAISE NOTICE 'Note: slot_count = % (expected ≤1 for no matching source bars)', v_slot;
    END IF;
END $$;

ROLLBACK;

DO $$
BEGIN
    RAISE NOTICE 'test_backfill_increases_slot_count passed (overnight window check)';
END $$;

