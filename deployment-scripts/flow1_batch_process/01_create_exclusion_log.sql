/* ====================================================================
   SCRIPT: 01_create_exclusion_log.sql
   PURPOSE: Creates the permanent audit table for the "Golden Universe" filter.
            This table acts as a graveyard for illiquid tickers.
   DEPENDENCIES: None. Must be run before any data is processed.
   ==================================================================== */

CREATE TABLE pipeline.symbol_exclusion_log (
    audit_date DATE NOT NULL DEFAULT CURRENT_DATE,
    target_year INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    expected_windows INTEGER,
    actual_windows INTEGER,
    missing_windows INTEGER,
    completion_pct NUMERIC,
    exclusion_reason TEXT,
    PRIMARY KEY (audit_date, target_year, symbol)
);
