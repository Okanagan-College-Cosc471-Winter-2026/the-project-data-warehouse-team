-- This script checks how many symbols failed the complete window threshold test from the previous script: 01_log_failures.sql

SELECT COUNT(*) FROM pipeline.symbol_exclusion_log WHERE target_year = 2024;
