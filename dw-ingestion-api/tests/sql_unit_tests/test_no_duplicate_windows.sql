-- Verifies no duplicate (symbol, window_ts) after multiple calls
-- Uses a real historical window with known source data

BEGIN;

-- Clear any prior interference for this specific window
DELETE FROM dw.market_data_15m
 WHERE symbol = 'AAPL'                           -- ← replace if needed
   AND window_ts = '2023-07-03 09:30:00-07'::timestamptz;  -- ← use real window_ts

-- First call – should create or populate the row
CALL dw.process_15min_window('2023-07-03 09:30:00-07'::timestamptz);

-- Safety check: row must exist after first call
DO $$
DECLARE
    v_count_first integer;
BEGIN
    SELECT COUNT(*) INTO v_count_first
    FROM dw.market_data_15m
    WHERE symbol = 'AAPL'
      AND window_ts = '2023-07-03 09:30:00-07'::timestamptz;

    IF v_count_first <> 1 THEN
        RAISE EXCEPTION 'First call did not create the window: found % rows', v_count_first;
    END IF;
END $$;

-- Second call – should only update (ON CONFLICT path), not duplicate
CALL dw.process_15min_window('2023-07-03 09:30:00-07'::timestamptz);

-- Final check: still exactly one row
DO $$
DECLARE
    v_count integer;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM dw.market_data_15m
    WHERE symbol = 'AAPL'
      AND window_ts = '2023-07-03 09:30:00-07'::timestamptz;

    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Duplicate or missing window after second call: found % rows', v_count;
    END IF;
END $$;

ROLLBACK;

DO $$
BEGIN
    RAISE NOTICE 'test_no_duplicate_windows passed (real data window)';
END $$;

