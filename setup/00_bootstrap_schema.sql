-- 00_bootstrap_schema.sql
-- Idempotent: creates schema + table if missing

CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.market_data_15m CASCADE;

CREATE TABLE dw.market_data_15m (
    agg_id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol                  text                     NOT NULL,
    window_ts               timestamptz              NOT NULL,
    open                    numeric(18,6),
    high                    numeric(18,6),
    low                     numeric(18,6),
    close                   numeric(18,6),
    volume                  bigint,
    slot_count              integer                  NOT NULL DEFAULT 0,
    status                  text                     NOT NULL DEFAULT 'complete',
    created_at              timestamptz              NOT NULL DEFAULT CURRENT_TIMESTAMP,

    lag_close_1             numeric(18,6),
    lag_close_5             numeric(18,6),
    lag_close_10            numeric(18,6),

    close_diff_1            numeric(18,6),
    close_diff_5            numeric(18,6),
    pct_change_1            numeric(18,6),
    pct_change_5            numeric(18,6),
    log_return_1            numeric(18,6),

    sma_close_5             numeric(18,6),
    sma_close_10            numeric(18,6),
    sma_close_20            numeric(18,6),
    sma_volume_5            numeric(18,6),

    day_of_week             smallint,
    hour_of_day             smallint,

    day_monday              smallint DEFAULT 0,
    day_tuesday             smallint DEFAULT 0,
    day_wednesday           smallint DEFAULT 0,
    day_thursday            smallint DEFAULT 0,
    day_friday              smallint DEFAULT 0,

    quarter_1               smallint DEFAULT 0,
    quarter_2               smallint DEFAULT 0,
    quarter_3               smallint DEFAULT 0,
    quarter_4               smallint DEFAULT 0,

    hour_early_morning      smallint DEFAULT 0,
    hour_mid_morning        smallint DEFAULT 0,
    hour_afternoon          smallint DEFAULT 0,
    hour_late_afternoon     smallint DEFAULT 0,

    UNIQUE (symbol, window_ts)
);

DO $$ 
BEGIN
    RAISE NOTICE 'Schema dw and table market_data_15m bootstrapped successfully.';
END $$;
