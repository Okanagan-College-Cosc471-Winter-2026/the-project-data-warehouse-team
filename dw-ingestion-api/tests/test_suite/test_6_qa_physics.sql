-- ==========================================
-- TEST 3.1: The Physics Constraint Gate
-- ==========================================
-- GOAL: Prove that 0 rows in the Data Warehouse violate the basic laws 
-- of financial mathematics (e.g., High is lower than Low).

DO $$
DECLARE
    v_broken_rows INTEGER;
BEGIN
    -- Count every row where the OHLC math is physically impossible
    SELECT COUNT(*) INTO v_broken_rows
    FROM dw.market_data_15m
    WHERE high < low
       OR close > high
       OR close < low
       OR open > high
       OR open < low;

    IF v_broken_rows = 0 THEN
        RAISE NOTICE '✅ PASS: Physics Quality Gate. Zero corrupted rows detected.';
    ELSE
        RAISE EXCEPTION '❌ FAIL: Physics Quality Gate. Detected % mathematically impossible rows!', v_broken_rows;
    END IF;
END $$;
