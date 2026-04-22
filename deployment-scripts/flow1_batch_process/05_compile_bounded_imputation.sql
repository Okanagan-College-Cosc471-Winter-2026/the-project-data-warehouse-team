/* ====================================================================
   SCRIPT: 05_compile_bounded_imputation.sql
   PURPOSE: Creates `dw.build_15m_bounded_ffill()`. Identifies missing 15m 
            windows and applies a LOCF fill strictly capped at 30 minutes.
            Calculates rolling features post-imputation and preserves daily gaps.
   DEPENDENCIES: Reads `dw.extract_15m_agg`. Writes to `dw.extract_15m_ffill`.
   ==================================================================== */

CREATE OR REPLACE PROCEDURE dw.build_15m_bounded_ffill(p_start_date DATE, p_end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '🚀 Executing BOUNDED Imputation Engine (Max 30m Fill): % to %', p_start_date, p_end_date;

    INSERT INTO dw.extract_15m_ffill (
        symbol, window_ts, open, high, low, close, volume, is_imputed,
        lag_close_1, lag_close_5, lag_close_10, sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        close_diff_1, close_diff_5, pct_change_1, pct_change_5, log_return_1,
        day_of_week, hour_of_day, month_of_year, day_monday, day_tuesday, day_wednesday, day_thursday, day_friday,
        quarter_1, quarter_2, quarter_3, quarter_4,
        hour_early_morning, hour_mid_morning, hour_afternoon, hour_late_afternoon,
        previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down
    )
    WITH valid_symbols AS (
        SELECT DISTINCT symbol 
        FROM dw.extract_15m_agg
        WHERE symbol NOT IN (
            SELECT symbol FROM pipeline.symbol_exclusion_log 
            WHERE target_year = EXTRACT(YEAR FROM p_start_date)
        )
    ),
    time_spine AS (
        SELECT DISTINCT window_ts 
        FROM dw.extract_15m_agg 
        WHERE window_ts >= p_start_date::timestamptz 
          AND window_ts < (p_end_date + INTERVAL '1 day')::timestamptz
    ),
    dense_grid AS (
        SELECT s.symbol, t.window_ts
        FROM valid_symbols s
        CROSS JOIN time_spine t
    ),
    joined_data AS (
        SELECT 
            g.symbol, g.window_ts,
            a.open, a.high, a.low, a.close, a.volume,
            a.previous_close, a.overnight_gap_pct, a.overnight_log_return, a.is_gap_up, a.is_gap_down,
            CASE WHEN a.close IS NULL THEN TRUE ELSE FALSE END AS is_imputed,
            COUNT(a.close) OVER (PARTITION BY g.symbol ORDER BY g.window_ts) AS value_grp
        FROM dense_grid g
        LEFT JOIN dw.extract_15m_agg a 
               ON g.symbol = a.symbol 
              AND g.window_ts = a.window_ts
    ),
    gap_calculation AS (
        SELECT 
            symbol, window_ts, is_imputed, open, high, low, close, volume, value_grp,
            previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down,
            FIRST_VALUE(window_ts) OVER (PARTITION BY symbol, value_grp ORDER BY window_ts) AS last_valid_ts
        FROM joined_data
    ),
    bounded_fill AS (
        SELECT
            symbol, window_ts, is_imputed,
            previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down,
            CASE WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' THEN FIRST_VALUE(open) OVER w ELSE NULL END AS open,
            CASE WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' THEN FIRST_VALUE(high) OVER w ELSE NULL END AS high,
            CASE WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' THEN FIRST_VALUE(low) OVER w ELSE NULL END AS low,
            CASE WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' THEN FIRST_VALUE(close) OVER w ELSE NULL END AS close,
            COALESCE(volume, 0) AS volume 
        FROM gap_calculation
        WINDOW w AS (PARTITION BY symbol, value_grp ORDER BY window_ts)
    ),
    enriched AS (
        SELECT 
            f.symbol, f.window_ts, f.open, f.high, f.low, f.close, f.volume, f.is_imputed,
            f.previous_close, f.overnight_gap_pct, f.overnight_log_return, f.is_gap_up, f.is_gap_down,
            
            LAG(f.close, 1) OVER win AS lag_close_1,
            LAG(f.close, 5) OVER win AS lag_close_5,
            LAG(f.close, 10) OVER win AS lag_close_10,
            
            AVG(f.close) OVER (PARTITION BY f.symbol ORDER BY f.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_close_5,
            AVG(f.close) OVER (PARTITION BY f.symbol ORDER BY f.window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_close_10,
            AVG(f.close) OVER (PARTITION BY f.symbol ORDER BY f.window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_close_20,
            AVG(f.volume) OVER (PARTITION BY f.symbol ORDER BY f.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_volume_5,
            
            EXTRACT(DOW FROM f.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS day_of_week,
            EXTRACT(HOUR FROM f.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS hour_of_day,
            EXTRACT(MONTH FROM f.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS month_of_year
        FROM bounded_fill f
        WINDOW win AS (PARTITION BY f.symbol ORDER BY f.window_ts)
    )
    SELECT 
        symbol, window_ts, open, high, low, close, volume, is_imputed,
        lag_close_1, lag_close_5, lag_close_10, sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        (close - lag_close_1) AS close_diff_1, (close - lag_close_5) AS close_diff_5,
        CASE WHEN lag_close_1 > 0 THEN (close - lag_close_1) / lag_close_1 ELSE NULL END AS pct_change_1,
        CASE WHEN lag_close_5 > 0 THEN (close - lag_close_5) / lag_close_5 ELSE NULL END AS pct_change_5,
        CASE WHEN lag_close_1 > 0 THEN LN(close / lag_close_1) ELSE NULL END AS log_return_1,
        day_of_week, hour_of_day, month_of_year,
        CASE WHEN day_of_week = 1 THEN 1 ELSE 0 END AS day_monday,
        CASE WHEN day_of_week = 2 THEN 1 ELSE 0 END AS day_tuesday,
        CASE WHEN day_of_week = 3 THEN 1 ELSE 0 END AS day_wednesday,
        CASE WHEN day_of_week = 4 THEN 1 ELSE 0 END AS day_thursday,
        CASE WHEN day_of_week = 5 THEN 1 ELSE 0 END AS day_friday,
        CASE WHEN month_of_year BETWEEN 1 AND 3 THEN 1 ELSE 0 END AS quarter_1,
        CASE WHEN month_of_year BETWEEN 4 AND 6 THEN 1 ELSE 0 END AS quarter_2,
        CASE WHEN month_of_year BETWEEN 7 AND 9 THEN 1 ELSE 0 END AS quarter_3,
        CASE WHEN month_of_year BETWEEN 10 AND 12 THEN 1 ELSE 0 END AS quarter_4,
        CASE WHEN hour_of_day BETWEEN 6 AND 8 THEN 1 ELSE 0 END AS hour_early_morning,
        CASE WHEN hour_of_day BETWEEN 9 AND 10 THEN 1 ELSE 0 END AS hour_mid_morning,
        CASE WHEN hour_of_day BETWEEN 11 AND 12 THEN 1 ELSE 0 END AS hour_afternoon,
        CASE WHEN hour_of_day BETWEEN 13 AND 16 THEN 1 ELSE 0 END AS hour_late_afternoon,
        previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down
    FROM enriched
    WHERE close IS NOT NULL
    ON CONFLICT (symbol, window_ts) DO NOTHING;

    RAISE NOTICE '✅ Bounded Imputation complete for % to %', p_start_date, p_end_date;
END;
$$;
