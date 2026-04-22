/* ====================================================================
   SCRIPT: 00_teardown_star_schema.sql
   PURPOSE: Completely drops the existing Presentation Layer (Star Schema)
            so it can be cleanly rebuilt.
   ==================================================================== */

-- Option A: Targeted Teardown (Safest)
-- We drop the Fact table first to clear the foreign key dependencies, 
-- then drop the Dimensions.
DROP TABLE IF EXISTS star.fact_market_data_15m CASCADE;
DROP TABLE IF EXISTS star.dim_symbol CASCADE;
DROP TABLE IF EXISTS star.dim_date CASCADE;
DROP TABLE IF EXISTS star.dim_timeslot CASCADE;

/* -- Option B: The Nuclear Option 
DROP SCHEMA IF EXISTS star CASCADE;
*/

DO $$
BEGIN
    RAISE NOTICE 'Old Star Schema tables successfully dropped. %',
                 'Remaining objects can now be reviewed if needed.';
END $$;
