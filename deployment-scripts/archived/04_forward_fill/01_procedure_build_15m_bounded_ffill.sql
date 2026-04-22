/* ====================================================================
   SCRIPT: 05_compile_bounded_imputation.sql
   PURPOSE: Creates `dw.build_15m_bounded_ffill()`. Uses a Time Spine to
            identify missing 15m windows and applies a Last Observation 
            Carried Forward (LOCF) fill. Strictly capped at 30 minutes (2 windows).
            Calculates final rolling features post-imputation.
   DEPENDENCIES: 
     - Reads from: `dw.extract_15m_agg` (must be populated first)
     - Writes to: `dw.extract_15m_ffill` (must exist)
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
        day_of_week, hour_of_day, month_of_year
    )
    WITH valid_symbols AS (
        -- 1. THE BOUNCER: Only process the "Golden Universe"
        SELECT DISTINCT symbol 
        FROM dw.extract_15m_agg
        WHERE symbol NOT IN (SELECT symbol FROM pipeline.symbol_exclusion_log)
    ),
    time_spine AS (
        -- 2. The Master Calendar
        SELECT DISTINCT window_ts 
        FROM dw.extract_15m_agg 
        WHERE window_ts >= p_start_date::timestamptz 
          AND window_ts < (p_end_date + INTERVAL '1 day')::timestamptz
    ),
    dense_grid AS (
        -- 3. Cross Join: Only the Golden Universe on the valid calendar
        SELECT s.symbol, t.window_ts
        FROM valid_symbols s
        CROSS JOIN time_spine t
    ),
    joined_data AS (
        -- 4. Left Join actual data and create Imputation Groups
        SELECT 
            g.symbol, 
            g.window_ts,
            a.open, a.high, a.low, a.close, a.volume,
            CASE WHEN a.close IS NULL THEN TRUE ELSE FALSE END AS is_imputed,
            -- Increments only when a real close price exists
            COUNT(a.close) OVER (PARTITION BY g.symbol ORDER BY g.window_ts) AS value_grp
        FROM dense_grid g
        LEFT JOIN dw.extract_15m_agg a 
               ON g.symbol = a.symbol 
              AND g.window_ts = a.window_ts
    ),
    gap_calculation AS (
        -- 5. Calculate the exact time since the last real tick
        SELECT 
            symbol, window_ts, is_imputed, open, high, low, close, volume, value_grp,
            FIRST_VALUE(window_ts) OVER (PARTITION BY symbol, value_grp ORDER BY window_ts) AS last_valid_ts
        FROM joined_data
    ),
    bounded_fill AS (
        -- 6. The 30-Minute Cap Logic
        SELECT
            symbol,
            window_ts,
            is_imputed,
            -- If the gap is 30 mins or less, fill it. Otherwise, leave it NULL.
            CASE 
                WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' 
                THEN FIRST_VALUE(open) OVER w 
                ELSE NULL 
            END AS open,
            CASE 
                WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' 
                THEN FIRST_VALUE(high) OVER w 
                ELSE NULL 
            END AS high,
            CASE 
                WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' 
                THEN FIRST_VALUE(low) OVER w 
                ELSE NULL 
            END AS low,
            CASE 
                WHEN window_ts - last_valid_ts <= INTERVAL '30 minutes' 
                THEN FIRST_VALUE(close) OVER w 
                ELSE NULL 
            END AS close,
            -- Imputed volume is always mathematically 0
            COALESCE(volume, 0) AS volume 
        FROM gap_calculation
        WINDOW w AS (PARTITION BY symbol, value_grp ORDER BY window_ts)
    ),
    enriched AS (
        -- 7. Calculate ML Features on the newly dense (but bounded) timeline
        SELECT 
            f.symbol, f.window_ts, f.open, f.high, f.low, f.close, f.volume, f.is_imputed,
            
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
    -- 8. Final Payload
    SELECT 
        symbol, window_ts, open, high, low, close, volume, is_imputed,
        lag_close_1, lag_close_5, lag_close_10, sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        (close - lag_close_1) AS close_diff_1,
        (close - lag_close_5) AS close_diff_5,
        CASE WHEN lag_close_1 > 0 THEN (close - lag_close_1) / lag_close_1 ELSE NULL END AS pct_change_1,
        CASE WHEN lag_close_5 > 0 THEN (close - lag_close_5) / lag_close_5 ELSE NULL END AS pct_change_5,
        CASE WHEN lag_close_1 > 0 THEN LN(close / lag_close_1) ELSE NULL END AS log_return_1,
        day_of_week, hour_of_day, month_of_year
    FROM enriched
    WHERE close IS NOT NULL -- Strips out the rows that exceeded the 30-min cap and stayed NULL
    ON CONFLICT (symbol, window_ts) DO NOTHING;

    RAISE NOTICE '✅ Bounded Imputation complete for % to %', p_start_date, p_end_date;
END;
$$;
