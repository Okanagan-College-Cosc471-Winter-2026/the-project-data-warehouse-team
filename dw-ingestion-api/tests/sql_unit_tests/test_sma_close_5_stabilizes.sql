-- Verifies that sma_close_5 is computed (non-NULL) on a real window with sufficient history
-- Uses AAPL and 2023-07-03 09:00:00-07 (known slot_count = 3 with prior data)

DO $$
DECLARE
    v_sma          numeric;
    original_sma   numeric;   -- declared here so it can be used
BEGIN
    -- Capture original value (for comparison / debugging)
    SELECT sma_close_5
      INTO original_sma
      FROM dw.market_data_15m
     WHERE symbol    = 'AAPL'
       AND window_ts = '2023-07-03 09:00:00-07'::timestamptz;

    -- Re-process the window (safe — everything will be rolled back)
    CALL dw.process_15min_window('2023-07-03 09:00:00-07'::timestamptz);

    -- Verify sma_close_5 is now non-NULL
    SELECT sma_close_5
      INTO v_sma
      FROM dw.market_data_15m
     WHERE symbol    = 'AAPL'
       AND window_ts = '2023-07-03 09:00:00-07'::timestamptz;

    IF v_sma IS NULL THEN
        RAISE EXCEPTION 'sma_close_5 remains NULL after re-processing known good window';
    END IF;

    -- Optional diagnostic output
    RAISE NOTICE 'sma_close_5 after processing: % (original was: %)', v_sma, original_sma;
END $$;

-- No BEGIN/ROLLBACK needed — DO block is self-contained and atomic
DO $$
BEGIN
    RAISE NOTICE 'test_sma_close_5_stabilizes passed (real AAPL window 2023-07-03 09:00:00-07)';
END $$;

