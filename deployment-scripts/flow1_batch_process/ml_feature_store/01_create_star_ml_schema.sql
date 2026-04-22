-- 1. Create the ml schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ml;

-- 2. Create the flattened, purely numeric Feature Store
CREATE TABLE ml.historical_feature_store AS
SELECT
    -- Integer Keys (Replaces text and timestamps for massive RAM savings)
    d.date_key,
    t.time_slot_key,
    s.symbol_key,

    -- Core OHLCV
    m.open, m.high, m.low, m.close, m.volume,
    m.is_imputed, -- The ML gap-fill flag

    -- Lag & Moving Average Features
    m.lag_close_1, m.lag_close_5, m.lag_close_10,
    m.sma_close_5, m.sma_close_10, m.sma_close_20, m.sma_volume_5,

    -- Price Action Features
    m.close_diff_1, m.close_diff_5, m.pct_change_1, m.pct_change_5, m.log_return_1,

    -- One-Hot Encoded Time Features
    m.day_monday, m.day_tuesday, m.day_wednesday, m.day_thursday, m.day_friday,
    m.quarter_1, m.quarter_2, m.quarter_3, m.quarter_4,
    m.hour_early_morning, m.hour_mid_morning, m.hour_afternoon, m.hour_late_afternoon,

    -- Overnight Gap Features
    m.previous_close, m.overnight_gap_pct, m.overnight_log_return, m.is_gap_up, m.is_gap_down

FROM dw.extract_15m_ffill m
JOIN star.dim_date d
  ON d.full_date = DATE(m.window_ts AT TIME ZONE 'America/Los_Angeles')
JOIN star.dim_timeslot t
  ON t.hour = EXTRACT(HOUR FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
 AND t.minute = EXTRACT(MINUTE FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
JOIN star.dim_symbol s
  ON s.symbol = m.symbol;

-- 3. Composite Indexes to accelerate model training lookups
CREATE INDEX idx_ml_matrix_lookup
ON ml.historical_feature_store (symbol_key, date_key, time_slot_key);
