-- ==========================================
-- TEST 3.2: The Star Router Orphan Check
-- ==========================================
-- GOAL: Prove that 0 rows in the Fact table are orphaned. 
-- Every fact MUST have a valid date, time_slot, and symbol dimension.

DO $$
DECLARE
    v_orphan_count INTEGER;
BEGIN
    -- Check for any fact records where the foreign key does not exist in the parent dimension table
    SELECT COUNT(*) INTO v_orphan_count
    FROM star.fact_market_data_15m f
    WHERE NOT EXISTS (SELECT 1 FROM star.dim_date d WHERE d.date_key = f.date_key)
       OR NOT EXISTS (SELECT 1 FROM star.dim_timeslot t WHERE t.time_slot_key = f.time_slot_key)
       OR NOT EXISTS (SELECT 1 FROM star.dim_symbol s WHERE s.symbol_key = f.symbol_key);

    IF v_orphan_count = 0 THEN
        RAISE NOTICE '✅ PASS: Star Integrity Check. Zero orphaned facts detected.';
    ELSE
        RAISE EXCEPTION '❌ FAIL: Data Integrity Error! Found % orphaned rows in the Fact table.', v_orphan_count;
    END IF;
END $$;
