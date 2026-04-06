CREATE OR REPLACE PROCEDURE dw.build_warehouse_data(p_start_date DATE, p_end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting bulk process from % to %', p_start_date, p_end_date;

    -- =========================================================================
    -- PHASE 1: BULK AGGREGATION (Raw 5m -> 15m)
    -- =========================================================================
    RAISE NOTICE 'Phase 1: Aggregating raw data...';
    
    INSERT INTO dw.market_data_15m (
        symbol, window_ts, open, high, low, close, volume, slot_count, status, created_at
    )
    WITH raw_data AS (
        SELECT 
            symbol, 
            date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::integer % 15) * INTERVAL '1 minute' AS window_ts,
            ts, open, high, low, close, volume
        -- UPDATED FOR DRAC: Read from staging
        FROM staging.market_data_5m
        WHERE ts >= p_start_date::timestamptz 
          AND ts < (p_end_date + INTERVAL '1 day')::timestamptz
    )
    SELECT
        symbol,
        window_ts,
        (array_agg(open ORDER BY ts ASC))[1]   AS open,
        MAX(high)                              AS high,
        MIN(low)                               AS low,
        (array_agg(close ORDER BY ts DESC))[1] AS close,
        SUM(volume)                            AS volume,
        COUNT(*)                               AS slot_count,
        CASE WHEN COUNT(*) >= 3 THEN 'complete' ELSE 'provisional' END AS status,
        CURRENT_TIMESTAMP                      AS created_at
    FROM raw_data
    GROUP BY symbol, window_ts
    ON CONFLICT (symbol, window_ts) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        slot_count = EXCLUDED.slot_count,
        status     = EXCLUDED.status;

    -- =========================================================================
    -- PHASE 2: BULK IMPUTATION (Smart Daily Boundaries)
    -- =========================================================================
    /*RAISE NOTICE 'Phase 2: Imputing missing and provisional windows...';
    
    WITH daily_schedules AS (
        SELECT 
            DATE(ts) AS market_date,
            MIN(ts) AS daily_open,
            MAX(ts) AS daily_close
        -- UPDATED FOR DRAC: Read from staging
        FROM staging.market_data_5m
        WHERE ts >= p_start_date::timestamptz 
          AND ts < (p_end_date + INTERVAL '1 day')::timestamptz
        GROUP BY DATE(ts)
    ),
    expected_windows AS (
        SELECT 
            ds.market_date,
            generate_series(
                date_trunc('minute', ds.daily_open) - (EXTRACT(MINUTE FROM ds.daily_open)::integer % 15) * INTERVAL '1 minute',
                date_trunc('minute', ds.daily_close) - (EXTRACT(MINUTE FROM ds.daily_close)::integer % 15) * INTERVAL '1 minute',
                '15 minutes'::interval
            ) AS window_ts
        FROM daily_schedules ds
    ),
    missing_targets AS (
        SELECT 
            sym.symbol,
            ew.window_ts
        FROM expected_windows ew
        -- UPDATED FOR DRAC: Read from staging
        CROSS JOIN (SELECT DISTINCT symbol FROM staging.market_data_5m) sym
        LEFT JOIN dw.market_data_15m m 
               ON m.symbol = sym.symbol 
              AND m.window_ts = ew.window_ts
        WHERE m.window_ts IS NULL OR m.slot_count < 3
    ),
    imputed_data AS (
        SELECT
            mt.symbol,
            mt.window_ts,
            AVG(h.open) AS open,
            MAX(h.high) AS high,
            MIN(h.low)  AS low,
            AVG(h.close) AS close,
            SUM(h.volume) AS volume
        FROM missing_targets mt
        -- UPDATED FOR DRAC: Read from staging
        LEFT JOIN staging.market_data_5m h
               ON h.symbol = mt.symbol
              AND h.ts >= mt.window_ts - INTERVAL '30 minutes'
              AND h.ts <= mt.window_ts + INTERVAL '30 minutes'
        GROUP BY mt.symbol, mt.window_ts
        HAVING COUNT(h.ts) > 0 
    )
    INSERT INTO dw.market_data_15m (
        symbol, window_ts, open, high, low, close, volume, slot_count, status, created_at
    )
    SELECT
        symbol, window_ts, open, high, low, close, volume, 
        3 AS slot_count, 
        'imputed' AS status,
        CURRENT_TIMESTAMP AS created_at
    FROM imputed_data
    ON CONFLICT (symbol, window_ts) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        slot_count = EXCLUDED.slot_count,
        status     = EXCLUDED.status;
*/
    -- =========================================================================
    -- PHASE 3: BULK FEATURE ENGINEERING (All 30 Features)
    -- =========================================================================
    RAISE NOTICE 'Phase 3: Calculating features...';

    WITH daily_metrics AS (
        SELECT 
            symbol,
            DATE(window_ts) as market_date,
            (array_agg(open ORDER BY window_ts ASC))[1] as daily_open,
            (array_agg(close ORDER BY window_ts DESC))[1] as daily_close
        FROM dw.market_data_15m
        WHERE window_ts >= p_start_date::timestamptz - INTERVAL '10 days'
          AND window_ts < (p_end_date + INTERVAL '1 day')::timestamptz
        GROUP BY symbol, DATE(window_ts)
    ),
    daily_features AS (
        SELECT 
            symbol,
            market_date,
            daily_open,
            LAG(daily_close, 1) OVER (PARTITION BY symbol ORDER BY market_date) as previous_close
        FROM daily_metrics
    ),
    enriched AS (
        SELECT
            m.symbol, 
            m.window_ts,
            
            -- Lags
            LAG(m.close, 1) OVER w AS lag_close_1,
            LAG(m.close, 5) OVER w AS lag_close_5,
            LAG(m.close, 10) OVER w AS lag_close_10,

	    -- SMAs (Strict Windowing to prevent greedy aggregation during warmup)
            CASE
                WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5
                THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                ELSE NULL
            END AS sma_close_5,

            CASE
                WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) = 10
                THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
                ELSE NULL
            END AS sma_close_10,

            CASE
                WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20
                THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                ELSE NULL
            END AS sma_close_20,

            CASE
                WHEN COUNT(m.volume) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5
                THEN AVG(m.volume) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                ELSE NULL
            END AS sma_volume_5,
            
            -- Time/Date 
            EXTRACT(DOW FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS day_of_week,
            EXTRACT(HOUR FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS hour_of_day,
            EXTRACT(MONTH FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS month_of_year,

            -- Daily Joined Features
            df.previous_close,
            df.daily_open

        FROM dw.market_data_15m m
        LEFT JOIN daily_features df 
               ON m.symbol = df.symbol 
              AND DATE(m.window_ts) = df.market_date
        WHERE m.window_ts >= p_start_date::timestamptz - INTERVAL '5 days'
          AND m.window_ts < (p_end_date + INTERVAL '1 day')::timestamptz
        WINDOW w AS (PARTITION BY m.symbol ORDER BY m.window_ts)
    )
    UPDATE dw.market_data_15m t
    SET
        lag_close_1  = e.lag_close_1,
        lag_close_5  = e.lag_close_5,
        lag_close_10 = e.lag_close_10,
        sma_close_5  = e.sma_close_5,
        sma_close_10 = e.sma_close_10,
        sma_close_20 = e.sma_close_20,
        sma_volume_5 = e.sma_volume_5,
        
        close_diff_1 = t.close - e.lag_close_1,
        close_diff_5 = t.close - e.lag_close_5,
        pct_change_1 = CASE WHEN e.lag_close_1 > 0 THEN (t.close - e.lag_close_1) / e.lag_close_1 ELSE NULL END,
        pct_change_5 = CASE WHEN e.lag_close_5 > 0 THEN (t.close - e.lag_close_5) / e.lag_close_5 ELSE NULL END,
        log_return_1 = CASE WHEN e.lag_close_1 > 0 THEN LN(t.close / e.lag_close_1) ELSE NULL END,

	day_of_week   = e.day_of_week,
        hour_of_day   = e.hour_of_day,
        month_of_year = e.month_of_year,
        day_monday    = CASE WHEN e.day_of_week = 1 THEN 1 ELSE 0 END,
        day_tuesday   = CASE WHEN e.day_of_week = 2 THEN 1 ELSE 0 END,
        day_wednesday = CASE WHEN e.day_of_week = 3 THEN 1 ELSE 0 END,
        day_thursday  = CASE WHEN e.day_of_week = 4 THEN 1 ELSE 0 END,
        day_friday    = CASE WHEN e.day_of_week = 5 THEN 1 ELSE 0 END,
        
        quarter_1     = CASE WHEN e.month_of_year BETWEEN 1 AND 3 THEN 1 ELSE 0 END,
        quarter_2     = CASE WHEN e.month_of_year BETWEEN 4 AND 6 THEN 1 ELSE 0 END,
        quarter_3     = CASE WHEN e.month_of_year BETWEEN 7 AND 9 THEN 1 ELSE 0 END,
        quarter_4     = CASE WHEN e.month_of_year BETWEEN 10 AND 12 THEN 1 ELSE 0 END,
        
        hour_early_morning  = CASE WHEN e.hour_of_day BETWEEN 6 AND 8 THEN 1 ELSE 0 END,
        hour_mid_morning    = CASE WHEN e.hour_of_day BETWEEN 9 AND 10 THEN 1 ELSE 0 END,
        hour_afternoon      = CASE WHEN e.hour_of_day BETWEEN 11 AND 12 THEN 1 ELSE 0 END,
        hour_late_afternoon = CASE WHEN e.hour_of_day BETWEEN 13 AND 16 THEN 1 ELSE 0 END,

        previous_close       = e.previous_close,
        overnight_gap_pct    = CASE WHEN e.previous_close > 0 THEN (e.daily_open - e.previous_close) / e.previous_close ELSE NULL END,
        overnight_log_return = CASE WHEN e.previous_close > 0 THEN LN(e.daily_open / e.previous_close) ELSE NULL END,
        is_gap_up            = CASE WHEN e.daily_open > e.previous_close THEN 1 ELSE 0 END,
        is_gap_down          = CASE WHEN e.daily_open < e.previous_close THEN 1 ELSE 0 END

    FROM enriched e
    WHERE t.symbol = e.symbol
      AND t.window_ts = e.window_ts
      AND t.window_ts >= p_start_date::timestamptz 
      AND t.window_ts < (p_end_date + INTERVAL '1 day')::timestamptz;

    RAISE NOTICE 'Processing complete from % to %', p_start_date, p_end_date;
END;
$$;
