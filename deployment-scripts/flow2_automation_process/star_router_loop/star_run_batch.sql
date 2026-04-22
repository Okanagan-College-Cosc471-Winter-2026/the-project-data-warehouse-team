DO $$
DECLARE
    v_start_date DATE;
    v_end_date DATE := CURRENT_DATE;
BEGIN
    SELECT COALESCE(
        TO_DATE(MAX(date_key)::text, 'YYYYMMDD') - INTERVAL '1 day',
        '2023-01-01'::date
    )
    INTO v_start_date
    FROM star.fact_market_data_15m;

    RAISE NOTICE 'High-Water Mark detected. Processing from % to %', v_start_date, v_end_date;

    CALL star.load_fact_data(v_start_date, v_end_date);
END;
$$;
