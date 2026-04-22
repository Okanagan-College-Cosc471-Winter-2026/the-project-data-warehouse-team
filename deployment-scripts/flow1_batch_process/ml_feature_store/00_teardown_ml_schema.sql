/* ====================================================================
   SCRIPT: 00_teardown_ml_schema.sql
   PURPOSE: Drops the existing Machine Learning Feature Store table
            to ensure a perfectly clean state before rebuilding the 
            numeric matrix.
   WARNING: Uses CASCADE to safely sever any attached indexes or views.
   ==================================================================== */

-- 1. Drop the specific feature store table
DROP TABLE IF EXISTS ml.historical_feature_store CASCADE;

/* -- 2. The Nuclear Option (Optional)
-- Uncomment this line if you want to completely obliterate the 
-- entire 'ml' schema and start from absolute scratch.
-- DROP SCHEMA IF EXISTS ml CASCADE; 
*/

DO $$
BEGIN
    RAISE NOTICE 'Old Star Schema tables successfully dropped.';
END $$;

