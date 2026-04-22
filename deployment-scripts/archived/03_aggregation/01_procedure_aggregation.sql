/* ====================================================================
   SCRIPT: 04_compile_filtered_aggregator.sql
   PURPOSE: Creates `dw.build_warehouse_bulk()`. Aggregates 5m ticks to 15m,
            calculates native ML features (SMAs, logs, gaps), and dynamically
            filters out any symbol present in the exclusion log.
   DEPENDENCIES: 
     - Writes to: `dw.extract_15m_agg` (must exist)
     - Reads from: `pipeline.symbol_exclusion_log` (must exist)
   ==================================================================== */

CREATE OR REPLACE PROCEDURE dw.build_warehouse_batch(p_start_date DATE, p_end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '🔥 Executing Fused Filtered Pipeline: % to %', p_start_date, p_end_date;

    INSERT INTO dw.extract_15m_agg (
        symbol, window_ts, open, high, low, close, volume, slot_count, status, created_at,
        lag_close_1, lag_close_5, lag_close_10, sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        close_diff_1, close_diff_5, pct_change_1, pct_change_5, log_return_1,
        day_of_week, hour_of_day, month_of_year, day_monday, day_tuesday, day_wednesday, day_thursday, day_friday,
        quarter_1, quarter_2, quarter_3, quarter_4,
        hour_early_morning, hour_mid_morning, hour_afternoon, hour_late_afternoon,
        previous_close, overnight_gap_pct, overnight_log_return, is_gap_up, is_gap_down
    )
    WITH raw_expanded AS (
        -- 1. Extract Raw Data (WITH 10-DAY WARMUP BUFFER)
        SELECT
            symbol,
            date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::integer % 15) * INTERVAL '1 minute' AS window_ts,
            ts, open, high, low, close, volume
        FROM staging.market_data_5m_bulk
        WHERE ts >= p_start_date::timestamptz - INTERVAL '10 days'
          AND ts < (p_end_date + INTERVAL '1 day')::timestamptz
          -- THE BOUNCER: Exclude the trash tier dynamically based on the year being processed
          AND symbol NOT IN (
              SELECT symbol 
              FROM pipeline.symbol_exclusion_log 
              WHERE target_year = EXTRACT(YEAR FROM p_start_date)
          )
    ),
    base_15m AS (
        -- 2. Phase 1: In-Memory Aggregation
        SELECT
            symbol,
            window_ts,
            (array_agg(open ORDER BY ts ASC))[1]   AS open,
            MAX(high)                              AS high,
            MIN(low)                               AS low,
            (array_agg(close ORDER BY ts DESC))[1] AS close,
            SUM(volume)                            AS volume,
            COUNT(*)                               AS slot_count,
            CASE WHEN COUNT(*) >= 3 THEN 'complete' ELSE 'provisional' END AS status
        FROM raw_expanded
        GROUP BY symbol, window_ts
    ),
    daily_metrics AS (
        -- 3. Phase 3: Daily Rollups
        SELECT
            symbol,
            DATE(window_ts) as market_date,
            (array_agg(open ORDER BY window_ts ASC))[1] as daily_open,
            (array_agg(close ORDER BY window_ts DESC))[1] as daily_close
        FROM base_15m
        GROUP BY symbol, DATE(window_ts)
    ),
    daily_features AS (
        -- 4. Phase 3: Previous Daily Close
        SELECT
            symbol,
            market_date,
            daily_open,
            LAG(daily_close, 1) OVER (PARTITION BY symbol ORDER BY market_date) as previous_close
        FROM daily_metrics
    ),
    enriched AS (
        -- 5. Phase 3: Window Functions & Merging
        SELECT
            m.symbol, m.window_ts, m.open, m.high, m.low, m.close, m.volume, m.slot_count, m.status,

            LAG(m.close, 1) OVER w AS lag_close_1,
            LAG(m.close, 5) OVER w AS lag_close_5,
            LAG(m.close, 10) OVER w AS lag_close_10,

            CASE WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5
            THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) ELSE NULL END AS sma_close_5,
            CASE WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) = 10
            THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) ELSE NULL END AS sma_close_10,
            CASE WHEN COUNT(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20
            THEN AVG(m.close) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ELSE NULL END AS sma_close_20,
            CASE WHEN COUNT(m.volume) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5
            THEN AVG(m.volume) OVER (PARTITION BY m.symbol ORDER BY m.window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) ELSE NULL END AS sma_volume_5,

            EXTRACT(DOW FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS day_of_week,
            EXTRACT(HOUR FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS hour_of_day,
            EXTRACT(MONTH FROM m.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint AS month_of_year,

            df.previous_close,
            df.daily_open
        FROM base_15m m
        LEFT JOIN daily_features df
               ON m.symbol = df.symbol
              AND DATE(m.window_ts) = df.market_date
        WINDOW w AS (PARTITION BY m.symbol ORDER BY m.window_ts)
    )
    -- 6. Final Payload Math & Safely Stripping the 10-Day Warmup Data
    SELECT
        symbol, window_ts, open, high, low, close, volume, slot_count, status, CURRENT_TIMESTAMP,
        lag_close_1, lag_close_5, lag_close_10, sma_close_5, sma_close_10, sma_close_20, sma_volume_5,
        (close - lag_close_1) AS close_diff_1,
        (close - lag_close_5) AS close_diff_5,
        CASE WHEN lag_close_1 > 0 THEN (close - lag_close_1) / lag_close_1 ELSE NULL END AS pct_change_1,
        CASE WHEN lag_close_5 > 0 THEN (close - lag_close_5) / lag_close_5 ELSE NULL END AS pct_change_5,
        CASE WHEN lag_close_1 > 0 THEN LN(close / lag_close_1) ELSE NULL END AS log_return_1, day_of_week, hour_of_day, month_of_year,
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
        CASE WHEN hour_of_day BETWEEN 13 AND 16 THEN 1 ELSE 0 END AS hour_late_afternoon, previous_close,
        CASE WHEN previous_close > 0 THEN (daily_open - previous_close) / previous_close ELSE NULL END AS overnight_gap_pct,
        CASE WHEN previous_close > 0 THEN LN(daily_open / previous_close) ELSE NULL END AS overnight_log_return,
        CASE WHEN daily_open > previous_close THEN 1 ELSE 0 END AS is_gap_up,
        CASE WHEN daily_open < previous_close THEN 1 ELSE 0 END AS is_gap_down
    FROM enriched
    WHERE window_ts >= p_start_date::timestamptz
      AND window_ts < (p_end_date + INTERVAL '1 day')::timestamptz
    ON CONFLICT (symbol, window_ts) DO NOTHING;

    RAISE NOTICE '✅ Filtered process complete from % to %', p_start_date, p_end_date;
END;
$$;
