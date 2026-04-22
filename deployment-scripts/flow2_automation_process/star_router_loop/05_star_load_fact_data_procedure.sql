/* ====================================================================
   SCRIPT: 05_compile_star_procedure.sql
   PURPOSE: Creates `star.load_fact_data()`. This is the core ETL router
            that sweeps data from the flat Feature Store into the normalized 
            Star Schema.
   FEATURES: 
      1. Auto-Injects new symbols into `dim_symbol` on the fly.
      2. Uses `ON CONFLICT DO UPDATE` for bulletproof idempotent inserts.
   ==================================================================== */

CREATE OR REPLACE PROCEDURE star.load_fact_data(p_start_date DATE, p_end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Routing LIVE DW aggregations to Star Schema from % to %', p_start_date, p_end_date;

    -- Step 1: Auto-inject any brand new symbols that appeared in the live stream
    INSERT INTO star.dim_symbol (symbol, is_active)
    SELECT DISTINCT symbol, true
    FROM dw.live_15m_agg
    WHERE window_ts >= p_start_date::timestamptz
    ON CONFLICT (symbol) DO NOTHING;

    -- Step 2: Route the pure, unimputed facts
    INSERT INTO star.fact_market_data_15m (
        date_key, time_slot_key, symbol_key,
        open, high, low, close, volume, slot_count, status,
        lag_close_1, lag_close_5, lag_close_10,
        sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        close_diff_1, close_diff_5, pct_change_1, pct_change_5, log_return_1,
        day_monday, day_tuesday, day_wednesday, day_thursday, day_friday,
        quarter_1, quarter_2, quarter_3, quarter_4,
        hour_early_morning, hour_mid_morning, hour_afternoon, hour_late_afternoon,
        previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down
    )
    SELECT
        d.date_key, t.time_slot_key, s.symbol_key,
        dw.open, dw.high, dw.low, dw.close, dw.volume, dw.slot_count, dw.status,
        dw.lag_close_1, dw.lag_close_5, dw.lag_close_10,
        dw.sma_close_5, dw.sma_close_10, dw.sma_close_20, dw.sma_volume_5,
        dw.close_diff_1, dw.close_diff_5, dw.pct_change_1, dw.pct_change_5, dw.log_return_1,
        dw.day_monday, dw.day_tuesday, dw.day_wednesday, dw.day_thursday, dw.day_friday,
        dw.quarter_1, dw.quarter_2, dw.quarter_3, dw.quarter_4,
        dw.hour_early_morning, dw.hour_mid_morning, dw.hour_afternoon, dw.hour_late_afternoon,
        dw.previous_close, dw.overnight_gap_pct, dw.overnight_log_return, dw.is_gap_up, dw.is_gap_down
    FROM dw.live_15m_agg dw
    JOIN star.dim_date d ON d.full_date = DATE(dw.window_ts AT TIME ZONE 'America/Los_Angeles')
    JOIN star.dim_timeslot t
      ON t.hour = EXTRACT(HOUR FROM dw.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
     AND t.minute = EXTRACT(MINUTE FROM dw.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
    JOIN star.dim_symbol s ON s.symbol = dw.symbol

    WHERE dw.window_ts >= p_start_date::timestamptz
      AND dw.window_ts < (p_end_date + INTERVAL '1 day')::timestamptz

    ON CONFLICT (date_key, time_slot_key, symbol_key) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume,
        slot_count = EXCLUDED.slot_count, status = EXCLUDED.status,
        lag_close_1 = EXCLUDED.lag_close_1, lag_close_5 = EXCLUDED.lag_close_5, lag_close_10 = EXCLUDED.lag_close_10,
        sma_close_5 = EXCLUDED.sma_close_5, sma_close_10 = EXCLUDED.sma_close_10, sma_close_20 = EXCLUDED.sma_close_20, sma_volume_5 = EXCLUDED.sma_volume_5,
        close_diff_1 = EXCLUDED.close_diff_1, close_diff_5 = EXCLUDED.close_diff_5, pct_change_1 = EXCLUDED.pct_change_1, pct_change_5 = EXCLUDED.pct_change_5, log_return_1 = EXCLUDED.log_return_1,
        previous_close = EXCLUDED.previous_close, overnight_gap_pct = EXCLUDED.overnight_gap_pct, overnight_log_return = EXCLUDED.overnight_log_return, is_gap_up = EXCLUDED.is_gap_up, is_gap_down = EXCLUDED.is_gap_down,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;
