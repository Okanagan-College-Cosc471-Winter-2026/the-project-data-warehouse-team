-- tests/integration/test_time_based_one_hot_correct.sql
DO $$
BEGIN
    -- Test known Monday in July (2023-07-03)
    IF NOT EXISTS (
        SELECT 1 FROM dw.market_data_15m
        WHERE window_ts = '2023-07-03 09:30:00-07'
          AND day_monday = 1
          AND quarter_3 = 1
          AND hour_mid_morning = 1
    ) THEN
        RAISE EXCEPTION 'Time-based one-hot encoding failed for known Monday in Q3 at 09:30';
    END IF;

    -- Test non-matching values
    IF EXISTS (
        SELECT 1 FROM dw.market_data_15m
        WHERE window_ts = '2023-07-03 09:30:00-07'
          AND (day_tuesday = 1 OR quarter_1 = 1 OR hour_early_morning = 1)
    ) THEN
        RAISE EXCEPTION 'Incorrect one-hot flags activated';
    END IF;
END $$;

