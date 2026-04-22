--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3
-- Dumped by pg_dump version 16.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: dw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dw;


--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA staging;


--
-- Name: star; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA star;


--
-- Name: build_warehouse_data(date, date); Type: PROCEDURE; Schema: dw; Owner: -
--

CREATE PROCEDURE dw.build_warehouse_data(IN p_start_date date, IN p_end_date date)
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


--
-- Name: process_15min_window(timestamp with time zone, text); Type: PROCEDURE; Schema: dw; Owner: -
--

CREATE PROCEDURE dw.process_15min_window(IN p_window_ts timestamp with time zone, IN p_symbol text DEFAULT NULL::text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_start_ts timestamptz := p_window_ts;
    v_end_ts   timestamptz := p_window_ts + INTERVAL '15 minutes';
    v_record   RECORD;
BEGIN
    FOR v_record IN
        SELECT symbol
        FROM staging.ingestion_progress
        WHERE (p_symbol IS NULL OR symbol = p_symbol)
    LOOP
        INSERT INTO dw.market_data_15m (
            symbol, window_ts, open, high, low, close, volume, slot_count, status
        )
        SELECT
            v_record.symbol,
            v_start_ts,
            FIRST_VALUE(open)  OVER (ORDER BY ts)                                 AS open,
            MAX(high)          OVER ()                                            AS high,
            MIN(low)           OVER ()                                            AS low,
            LAST_VALUE(close)  OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
            SUM(volume)        OVER ()                                            AS volume,
            COUNT(*)           OVER ()                                            AS slot_count,
            CASE WHEN COUNT(*) OVER () >= 3 THEN 'complete' ELSE 'provisional' END AS status
        FROM staging.market_data_5m s
        WHERE s.symbol = v_record.symbol
          AND s.ts >= v_start_ts
          AND s.ts < v_end_ts
        ORDER BY s.ts
        LIMIT 1;   -- ← critical: we only need one row with the window aggregates

        -- Optional: only raise notice when a row was actually inserted/updated
        IF FOUND THEN
            RAISE NOTICE 'Processed window % for symbol %', v_start_ts, v_record.symbol;
        END IF;

    END LOOP;
END;
$$;


--
-- Name: process_15min_window_archived(timestamp with time zone); Type: PROCEDURE; Schema: dw; Owner: -
--

CREATE PROCEDURE dw.process_15min_window_archived(IN p_window_ts timestamp with time zone)
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


--
-- Name: load_fact_data(date, date); Type: PROCEDURE; Schema: star; Owner: -
--

CREATE PROCEDURE star.load_fact_data(IN p_start_date date, IN p_end_date date)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE NOTICE 'Routing DW data to Star Schema from % to %', p_start_date, p_end_date;

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
    FROM dw.market_data_15m dw
    JOIN star.dim_date d ON d.full_date = DATE(dw.window_ts AT TIME ZONE 'America/Los_Angeles')
    JOIN star.dim_timeslot t 
      ON t.hour = EXTRACT(HOUR FROM dw.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
     AND t.minute = EXTRACT(MINUTE FROM dw.window_ts AT TIME ZONE 'America/Los_Angeles')::smallint
    JOIN star.dim_symbol s ON s.symbol = dw.symbol
    
    -- UPDATED: Now uses the date range
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: backfill_targets; Type: TABLE; Schema: dw; Owner: -
--

CREATE TABLE dw.backfill_targets (
    symbol text NOT NULL,
    window_ts timestamp with time zone NOT NULL,
    reason text,
    detected_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    processed_at timestamp with time zone
);


--
-- Name: market_data_15m; Type: TABLE; Schema: dw; Owner: -
--

CREATE TABLE dw.market_data_15m (
    agg_id bigint NOT NULL,
    symbol text NOT NULL,
    window_ts timestamp with time zone NOT NULL,
    open numeric(18,6),
    high numeric(18,6),
    low numeric(18,6),
    close numeric(18,6),
    volume bigint,
    slot_count integer DEFAULT 0 NOT NULL,
    status text DEFAULT 'provisional'::text NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    lag_close_1 numeric(18,6),
    lag_close_5 numeric(18,6),
    lag_close_10 numeric(18,6),
    close_diff_1 numeric(18,6),
    close_diff_5 numeric(18,6),
    pct_change_1 numeric(18,6),
    pct_change_5 numeric(18,6),
    log_return_1 numeric(18,6),
    sma_close_5 numeric(18,6),
    sma_close_10 numeric(18,6),
    sma_close_20 numeric(18,6),
    sma_volume_5 numeric(18,6),
    day_of_week smallint,
    hour_of_day smallint,
    day_monday smallint DEFAULT 0,
    day_tuesday smallint DEFAULT 0,
    day_wednesday smallint DEFAULT 0,
    day_thursday smallint DEFAULT 0,
    day_friday smallint DEFAULT 0,
    quarter_1 smallint DEFAULT 0,
    quarter_2 smallint DEFAULT 0,
    quarter_3 smallint DEFAULT 0,
    quarter_4 smallint DEFAULT 0,
    hour_early_morning smallint DEFAULT 0,
    hour_mid_morning smallint DEFAULT 0,
    hour_afternoon smallint DEFAULT 0,
    hour_late_afternoon smallint DEFAULT 0,
    previous_close numeric(18,6),
    overnight_gap_pct numeric(18,6),
    overnight_log_return numeric(18,6),
    is_gap_up smallint,
    is_gap_down smallint,
    month_of_year smallint
);


--
-- Name: market_data_15m_agg_id_seq; Type: SEQUENCE; Schema: dw; Owner: -
--

ALTER TABLE dw.market_data_15m ALTER COLUMN agg_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME dw.market_data_15m_agg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: ingestion_progress; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.ingestion_progress (
    symbol text NOT NULL,
    last_ingested_ts timestamp with time zone DEFAULT '1899-12-31 16:00:00-08'::timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    notes text
);


--
-- Name: market_data_5m; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.market_data_5m (
    market_data_id bigint NOT NULL,
    symbol text NOT NULL,
    ts timestamp with time zone NOT NULL,
    open numeric(18,6) NOT NULL,
    high numeric(18,6) NOT NULL,
    low numeric(18,6) NOT NULL,
    close numeric(18,6) NOT NULL,
    volume bigint NOT NULL,
    asset_type text NOT NULL,
    source text NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingested_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: market_data_5m_market_data_id_seq; Type: SEQUENCE; Schema: staging; Owner: -
--

ALTER TABLE staging.market_data_5m ALTER COLUMN market_data_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME staging.market_data_5m_market_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dim_date; Type: TABLE; Schema: star; Owner: -
--

CREATE TABLE star.dim_date (
    date_key integer NOT NULL,
    full_date date NOT NULL,
    year smallint,
    quarter smallint,
    month smallint,
    day_of_week smallint,
    day_name text,
    is_trading_day boolean,
    holiday_flag boolean,
    pre_holiday boolean,
    post_holiday boolean
);


--
-- Name: dim_symbol; Type: TABLE; Schema: star; Owner: -
--

CREATE TABLE star.dim_symbol (
    symbol_key integer NOT NULL,
    symbol text NOT NULL,
    asset_type text,
    company_name text,
    sector text,
    industry text,
    exchange text,
    is_active boolean DEFAULT true
);


--
-- Name: dim_symbol_symbol_key_seq; Type: SEQUENCE; Schema: star; Owner: -
--

CREATE SEQUENCE star.dim_symbol_symbol_key_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_symbol_symbol_key_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: -
--

ALTER SEQUENCE star.dim_symbol_symbol_key_seq OWNED BY star.dim_symbol.symbol_key;


--
-- Name: dim_timeslot; Type: TABLE; Schema: star; Owner: -
--

CREATE TABLE star.dim_timeslot (
    time_slot_key integer NOT NULL,
    hour smallint,
    minute smallint,
    slot_label text,
    session_type text,
    is_market_open boolean
);


--
-- Name: fact_market_data_15m; Type: TABLE; Schema: star; Owner: -
--

CREATE TABLE star.fact_market_data_15m (
    date_key integer NOT NULL,
    time_slot_key integer NOT NULL,
    symbol_key integer NOT NULL,
    open numeric(18,6),
    high numeric(18,6),
    low numeric(18,6),
    close numeric(18,6),
    volume bigint,
    slot_count smallint,
    status text,
    lag_close_1 numeric(18,6),
    lag_close_5 numeric(18,6),
    lag_close_10 numeric(18,6),
    sma_close_5 numeric(18,6),
    sma_close_10 numeric(18,6),
    sma_close_20 numeric(18,6),
    sma_volume_5 numeric(18,6),
    close_diff_1 numeric(18,6),
    close_diff_5 numeric(18,6),
    pct_change_1 numeric(18,6),
    pct_change_5 numeric(18,6),
    log_return_1 numeric(18,6),
    day_monday smallint,
    day_tuesday smallint,
    day_wednesday smallint,
    day_thursday smallint,
    day_friday smallint,
    quarter_1 smallint,
    quarter_2 smallint,
    quarter_3 smallint,
    quarter_4 smallint,
    hour_early_morning smallint,
    hour_mid_morning smallint,
    hour_afternoon smallint,
    hour_late_afternoon smallint,
    previous_close numeric(18,6),
    overnight_gap_pct numeric(18,6),
    overnight_log_return numeric(18,6),
    is_gap_up smallint,
    is_gap_down smallint,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: dim_symbol symbol_key; Type: DEFAULT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_symbol ALTER COLUMN symbol_key SET DEFAULT nextval('star.dim_symbol_symbol_key_seq'::regclass);


--
-- Name: backfill_targets backfill_targets_pkey; Type: CONSTRAINT; Schema: dw; Owner: -
--

ALTER TABLE ONLY dw.backfill_targets
    ADD CONSTRAINT backfill_targets_pkey PRIMARY KEY (symbol, window_ts);


--
-- Name: market_data_15m market_data_15m_pkey; Type: CONSTRAINT; Schema: dw; Owner: -
--

ALTER TABLE ONLY dw.market_data_15m
    ADD CONSTRAINT market_data_15m_pkey PRIMARY KEY (agg_id);


--
-- Name: market_data_15m market_data_15m_symbol_window_ts_key; Type: CONSTRAINT; Schema: dw; Owner: -
--

ALTER TABLE ONLY dw.market_data_15m
    ADD CONSTRAINT market_data_15m_symbol_window_ts_key UNIQUE (symbol, window_ts);


--
-- Name: ingestion_progress ingestion_progress_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.ingestion_progress
    ADD CONSTRAINT ingestion_progress_pkey PRIMARY KEY (symbol);


--
-- Name: market_data_5m market_data_5m_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.market_data_5m
    ADD CONSTRAINT market_data_5m_pkey PRIMARY KEY (market_data_id);


--
-- Name: market_data_5m staging_market_data_5m_symbol_ts_key; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.market_data_5m
    ADD CONSTRAINT staging_market_data_5m_symbol_ts_key UNIQUE (symbol, ts);


--
-- Name: dim_date dim_date_full_date_key; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_date
    ADD CONSTRAINT dim_date_full_date_key UNIQUE (full_date);


--
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_key);


--
-- Name: dim_symbol dim_symbol_pkey; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_symbol
    ADD CONSTRAINT dim_symbol_pkey PRIMARY KEY (symbol_key);


--
-- Name: dim_symbol dim_symbol_symbol_key; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_symbol
    ADD CONSTRAINT dim_symbol_symbol_key UNIQUE (symbol);


--
-- Name: dim_timeslot dim_timeslot_pkey; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.dim_timeslot
    ADD CONSTRAINT dim_timeslot_pkey PRIMARY KEY (time_slot_key);


--
-- Name: fact_market_data_15m fact_market_data_15m_pkey; Type: CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.fact_market_data_15m
    ADD CONSTRAINT fact_market_data_15m_pkey PRIMARY KEY (date_key, time_slot_key, symbol_key);


--
-- Name: idx_market_data_15m_status_provisional; Type: INDEX; Schema: dw; Owner: -
--

CREATE INDEX idx_market_data_15m_status_provisional ON dw.market_data_15m USING btree (status) WHERE (status = 'provisional'::text);


--
-- Name: idx_market_data_15m_symbol_window_ts; Type: INDEX; Schema: dw; Owner: -
--

CREATE INDEX idx_market_data_15m_symbol_window_ts ON dw.market_data_15m USING btree (symbol, window_ts);


--
-- Name: idx_market_data_15m_window_ts; Type: INDEX; Schema: dw; Owner: -
--

CREATE INDEX idx_market_data_15m_window_ts ON dw.market_data_15m USING btree (window_ts);


--
-- Name: idx_staging_market_data_5m_symbol_ts; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_staging_market_data_5m_symbol_ts ON staging.market_data_5m USING btree (symbol, ts);


--
-- Name: idx_staging_market_data_5m_ts; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_staging_market_data_5m_ts ON staging.market_data_5m USING btree (ts);


--
-- Name: fact_market_data_15m fact_market_data_15m_date_key_fkey; Type: FK CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.fact_market_data_15m
    ADD CONSTRAINT fact_market_data_15m_date_key_fkey FOREIGN KEY (date_key) REFERENCES star.dim_date(date_key);


--
-- Name: fact_market_data_15m fact_market_data_15m_symbol_key_fkey; Type: FK CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.fact_market_data_15m
    ADD CONSTRAINT fact_market_data_15m_symbol_key_fkey FOREIGN KEY (symbol_key) REFERENCES star.dim_symbol(symbol_key);


--
-- Name: fact_market_data_15m fact_market_data_15m_time_slot_key_fkey; Type: FK CONSTRAINT; Schema: star; Owner: -
--

ALTER TABLE ONLY star.fact_market_data_15m
    ADD CONSTRAINT fact_market_data_15m_time_slot_key_fkey FOREIGN KEY (time_slot_key) REFERENCES star.dim_timeslot(time_slot_key);


--
-- PostgreSQL database dump complete
--

