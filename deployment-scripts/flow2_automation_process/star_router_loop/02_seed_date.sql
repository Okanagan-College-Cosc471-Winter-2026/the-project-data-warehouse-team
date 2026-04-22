/* ====================================================================
   SCRIPT: 02_seed_date.sql
   PURPOSE: Statically populates the Date Dimension (`dim_date`).
            Uses PostgreSQL's native `generate_series` to build a 
            calendar spine out to the year 2030.
   ==================================================================== */

INSERT INTO star.dim_date (date_key, full_date, year, quarter, month, day_of_week, day_name, is_trading_day)
SELECT
    to_char(d, 'YYYYMMDD')::integer AS date_key,
    d::date AS full_date,
    EXTRACT(YEAR FROM d)::smallint AS year,
    EXTRACT(QUARTER FROM d)::smallint AS quarter,
    EXTRACT(MONTH FROM d)::smallint AS month,
    EXTRACT(ISODOW FROM d)::smallint AS day_of_week,
    TRIM(to_char(d, 'Day')) AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN false ELSE true END AS is_trading_day
FROM generate_series('2023-01-01'::date, '2030-12-31'::date, '1 day'::interval) AS d
ON CONFLICT (full_date) DO NOTHING;
