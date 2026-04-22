/* ====================================================================
   SCRIPT: 03_create_target_schemas.sql
   PURPOSE: Defines the Data Definition Language (DDL) for Phase 1. 
            Creates the three master tables: 5m raw, 15m aggregated (with holes), 
            and 15m forward-filled (dense).
   DEPENDENCIES: None.
   ==================================================================== */

-- ==========================================
-- TABLE 1: 5m Raw + Features (No Aggregation)
-- ==========================================
CREATE TABLE dw.extract_5m_raw (
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
    
    -- Lags & SMAs (Native 5m intervals)
    lag_close_1 NUMERIC, lag_close_5 NUMERIC, lag_close_10 NUMERIC,
    sma_close_5 NUMERIC, sma_close_10 NUMERIC, sma_close_20 NUMERIC, sma_volume_5 NUMERIC,
    
    -- Math & Returns
    close_diff_1 NUMERIC, close_diff_5 NUMERIC, 
    pct_change_1 NUMERIC, pct_change_5 NUMERIC, log_return_1 NUMERIC,
    
    -- Time Features
    day_of_week SMALLINT, hour_of_day SMALLINT, month_of_year SMALLINT,
    day_monday SMALLINT, day_tuesday SMALLINT, day_wednesday SMALLINT, day_thursday SMALLINT, day_friday SMALLINT,
    quarter_1 SMALLINT, quarter_2 SMALLINT, quarter_3 SMALLINT, quarter_4 SMALLINT,
    hour_early_morning SMALLINT, hour_mid_morning SMALLINT, hour_afternoon SMALLINT, hour_late_afternoon SMALLINT,
    
    -- Overnight Gaps
    previous_close NUMERIC, overnight_gap_pct NUMERIC, overnight_log_return NUMERIC,
    is_gap_up SMALLINT, is_gap_down SMALLINT,
    
    PRIMARY KEY (symbol, ts)
);

-- ==========================================
-- TABLE 2: 15m Aggregated + Features (Has Holes)
-- ==========================================
CREATE TABLE dw.extract_15m_agg (
    symbol TEXT NOT NULL,
    window_ts TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
    
    -- Aggregation Meta
    slot_count SMALLINT, status TEXT, created_at TIMESTAMPTZ,
    
    -- Lags & SMAs (15m intervals)
    lag_close_1 NUMERIC, lag_close_5 NUMERIC, lag_close_10 NUMERIC,
    sma_close_5 NUMERIC, sma_close_10 NUMERIC, sma_close_20 NUMERIC, sma_volume_5 NUMERIC,
    
    -- Math & Returns
    close_diff_1 NUMERIC, close_diff_5 NUMERIC, 
    pct_change_1 NUMERIC, pct_change_5 NUMERIC, log_return_1 NUMERIC,
    
    -- Time Features
    day_of_week SMALLINT, hour_of_day SMALLINT, month_of_year SMALLINT,
    day_monday SMALLINT, day_tuesday SMALLINT, day_wednesday SMALLINT, day_thursday SMALLINT, day_friday SMALLINT,
    quarter_1 SMALLINT, quarter_2 SMALLINT, quarter_3 SMALLINT, quarter_4 SMALLINT,
    hour_early_morning SMALLINT, hour_mid_morning SMALLINT, hour_afternoon SMALLINT, hour_late_afternoon SMALLINT,
    
    -- Overnight Gaps
    previous_close NUMERIC, overnight_gap_pct NUMERIC, overnight_log_return NUMERIC,
    is_gap_up SMALLINT, is_gap_down SMALLINT,
    
    PRIMARY KEY (symbol, window_ts)
);

-- ==========================================
-- TABLE 3: 15m Forward-Filled + Features (No Holes)
-- ==========================================
CREATE TABLE dw.extract_15m_ffill (
    symbol TEXT NOT NULL,
    window_ts TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
    
    -- IMPUTATION TRACKING
    is_imputed BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Aggregation Meta
    slot_count SMALLINT, status TEXT, created_at TIMESTAMPTZ,
    
    -- Lags & SMAs (Calculated post-fill)
    lag_close_1 NUMERIC, lag_close_5 NUMERIC, lag_close_10 NUMERIC,
    sma_close_5 NUMERIC, sma_close_10 NUMERIC, sma_close_20 NUMERIC, sma_volume_5 NUMERIC,
    
    -- Math & Returns
    close_diff_1 NUMERIC, close_diff_5 NUMERIC, 
    pct_change_1 NUMERIC, pct_change_5 NUMERIC, log_return_1 NUMERIC,
    
    -- Time Features
    day_of_week SMALLINT, hour_of_day SMALLINT, month_of_year SMALLINT,
    day_monday SMALLINT, day_tuesday SMALLINT, day_wednesday SMALLINT, day_thursday SMALLINT, day_friday SMALLINT,
    quarter_1 SMALLINT, quarter_2 SMALLINT, quarter_3 SMALLINT, quarter_4 SMALLINT,
    hour_early_morning SMALLINT, hour_mid_morning SMALLINT, hour_afternoon SMALLINT, hour_late_afternoon SMALLINT,
    
    -- Overnight Gaps
    previous_close NUMERIC, overnight_gap_pct NUMERIC, overnight_log_return NUMERIC,
    is_gap_up SMALLINT, is_gap_down SMALLINT,
    
    PRIMARY KEY (symbol, window_ts)
);
