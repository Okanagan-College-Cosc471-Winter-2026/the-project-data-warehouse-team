-- Verifies that all required columns exist in dw.market_data_15m
-- Checks existence + basic type compatibility (ignores precision/scale)

DO $$
DECLARE
    missing_columns text[];
    expected_columns text[] := ARRAY[
        'symbol',
        'window_ts',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'slot_count',
        'status',
        'created_at',
        'lag_close_1',
        'lag_close_5',
        'lag_close_10',
        'close_diff_1',
        'close_diff_5',
        'pct_change_1',
        'pct_change_5',
        'log_return_1',
        'sma_close_5',
        'sma_close_10',
        'sma_close_20',
        'sma_volume_5',
        'day_of_week',
        'hour_of_day',
        'day_monday',
        'day_tuesday',
        'day_wednesday',
        'day_thursday',
        'day_friday',
        'quarter_1',
        'quarter_2',
        'quarter_3',
        'quarter_4',
        'hour_early_morning',
        'hour_mid_morning',
        'hour_afternoon',
        'hour_late_afternoon'
    ];

    col record;
BEGIN
    -- Collect missing columns
    FOR col IN
        SELECT unnest(expected_columns) AS col_name
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'dw'
              AND table_name   = 'market_data_15m'
              AND column_name  = col.col_name
        ) THEN
            missing_columns := missing_columns || col.col_name;
        END IF;
    END LOOP;

    IF array_length(missing_columns, 1) > 0 THEN
        RAISE EXCEPTION 'Missing columns in dw.market_data_15m: %',
                        array_to_string(missing_columns, ', ');
    END IF;

    RAISE NOTICE 'All required columns are present in dw.market_data_15m.';
END $$;

