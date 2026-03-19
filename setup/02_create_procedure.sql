-- Purpose: (Re)create the core incremental processing procedure
-- This procedure handles aggregation of one 15-minute window + feature computation

DROP PROCEDURE IF EXISTS dw.process_15min_window(timestamptz);

CREATE OR REPLACE PROCEDURE dw.process_15min_window(p_window_ts timestamptz)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Aggregate and upsert base OHLCV + status
    -- Only data strictly inside this 15-minute window
    INSERT INTO dw.market_data_15m (
        symbol, window_ts, open, high, low, close, volume, slot_count, status, created_at
    )
    WITH new_data AS (
        SELECT symbol, ts, open, high, low, close, volume
        FROM historical.market_data_5m
        WHERE symbol IN ('AAPL','AMD','AMZN','BA')
          AND ts >= p_window_ts
          AND ts < p_window_ts + INTERVAL '15 minutes'
    ),
    aggregated AS (
        SELECT
            symbol,
            p_window_ts AS window_ts,
            (array_agg(open ORDER BY ts ASC ))[1]          AS open,
            MAX(high)                                       AS high,
            MIN(low)                                        AS low,
            (array_agg(close ORDER BY ts DESC))[1]          AS close,
            SUM(volume)                                     AS volume,
            COUNT(*)                                        AS slot_count
        FROM new_data
        GROUP BY symbol
    )
    SELECT
        symbol, window_ts, open, high, low, close, volume, slot_count,
        CASE WHEN slot_count >= 3 THEN 'complete' ELSE 'provisional' END AS status,
        CURRENT_TIMESTAMP AS created_at
    FROM aggregated
    ON CONFLICT (symbol, window_ts) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        slot_count = EXCLUDED.slot_count,
        status     = EXCLUDED.status,
        created_at = CURRENT_TIMESTAMP;

    -- 2. Update features for this window (using full history for correct lags / rolling windows)
    -- IMPORTANT FIX: Do NOT restrict e.window_ts = p_window_ts in the join.
    -- That caused lag_close_1 to reference the current close instead of the previous one.
    -- We now join only on matching row identity, so lag values are correct.
    WITH enriched AS (
        SELECT
            symbol, window_ts, close, volume,
            LAG(close, 1) OVER (PARTITION BY symbol ORDER BY window_ts) AS lag_close_1,
            LAG(close, 5) OVER (PARTITION BY symbol ORDER BY window_ts) AS lag_close_5,
            LAG(close, 10) OVER (PARTITION BY symbol ORDER BY window_ts) AS lag_close_10,
            AVG(close) OVER (PARTITION BY symbol ORDER BY window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_close_5,
            AVG(close) OVER (PARTITION BY symbol ORDER BY window_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_close_10,
            AVG(close) OVER (PARTITION BY symbol ORDER BY window_ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_close_20,
            AVG(volume) OVER (PARTITION BY symbol ORDER BY window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_volume_5,
            EXTRACT(DOW FROM window_ts)::smallint AS day_of_week,
            EXTRACT(HOUR FROM window_ts)::smallint AS hour_of_day
        FROM dw.market_data_15m
        WHERE symbol IN ('AAPL','AMD','AMZN','BA')
          AND window_ts <= p_window_ts
    )
    UPDATE dw.market_data_15m t
    SET
        lag_close_1    = e.lag_close_1,
        lag_close_5    = e.lag_close_5,
        lag_close_10   = e.lag_close_10,
        close_diff_1   = t.close - e.lag_close_1,
        close_diff_5   = t.close - e.lag_close_5,
        pct_change_1   = CASE WHEN e.lag_close_1 = 0 OR e.lag_close_1 IS NULL THEN NULL ELSE (t.close - e.lag_close_1) / e.lag_close_1 END,
        pct_change_5   = CASE WHEN e.lag_close_5 = 0 OR e.lag_close_5 IS NULL THEN NULL ELSE (t.close - e.lag_close_5) / e.lag_close_5 END,
        log_return_1   = CASE WHEN e.lag_close_1 = 0 OR e.lag_close_1 IS NULL THEN NULL ELSE LN(t.close / e.lag_close_1) END,
        sma_close_5    = e.sma_close_5,
        sma_close_10   = e.sma_close_10,
        sma_close_20   = e.sma_close_20,
        sma_volume_5   = e.sma_volume_5,
        day_of_week    = e.day_of_week,
        hour_of_day    = e.hour_of_day,
        day_monday     = CASE WHEN EXTRACT(DOW FROM t.window_ts) = 1 THEN 1 ELSE 0 END,
        day_tuesday    = CASE WHEN EXTRACT(DOW FROM t.window_ts) = 2 THEN 1 ELSE 0 END,
        day_wednesday  = CASE WHEN EXTRACT(DOW FROM t.window_ts) = 3 THEN 1 ELSE 0 END,
        day_thursday   = CASE WHEN EXTRACT(DOW FROM t.window_ts) = 4 THEN 1 ELSE 0 END,
        day_friday     = CASE WHEN EXTRACT(DOW FROM t.window_ts) = 5 THEN 1 ELSE 0 END,
        quarter_1      = CASE WHEN EXTRACT(MONTH FROM t.window_ts) BETWEEN 1 AND 3  THEN 1 ELSE 0 END,
        quarter_2      = CASE WHEN EXTRACT(MONTH FROM t.window_ts) BETWEEN 4 AND 6  THEN 1 ELSE 0 END,
        quarter_3      = CASE WHEN EXTRACT(MONTH FROM t.window_ts) BETWEEN 7 AND 9  THEN 1 ELSE 0 END,
        quarter_4      = CASE WHEN EXTRACT(MONTH FROM t.window_ts) BETWEEN 10 AND 12 THEN 1 ELSE 0 END,
        hour_early_morning  = CASE WHEN EXTRACT(HOUR FROM t.window_ts) BETWEEN 6 AND 8 THEN 1 ELSE 0 END,
        hour_mid_morning    = CASE WHEN EXTRACT(HOUR FROM t.window_ts) BETWEEN 9 AND 10 THEN 1 ELSE 0 END,
        hour_afternoon      = CASE WHEN EXTRACT(HOUR FROM t.window_ts) BETWEEN 11 AND 12 THEN 1 ELSE 0 END,
        hour_late_afternoon = CASE WHEN EXTRACT(HOUR FROM t.window_ts) BETWEEN 13 AND 16 THEN 1 ELSE 0 END
    FROM enriched e
    WHERE t.symbol = e.symbol
      AND t.window_ts = e.window_ts
      AND t.window_ts = p_window_ts;  -- restrict update to current window only
END;
$$;

-- Confirmation
DO $$
BEGIN
    RAISE NOTICE 'Procedure dw.process_15min_window created or replaced successfully.';
END $$;

