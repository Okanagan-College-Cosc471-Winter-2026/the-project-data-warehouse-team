/* ====================================================================
   SCRIPT: 03_seed_timeslot.sql
   PURPOSE: Statically populates the Time Dimension (`dim_timeslot`).
            Maps every possible 15-minute window in a 24-hour day to a 
            standardized integer key (e.g., 09:30 AM = 930) and flags 
            standard market sessions.
   ==================================================================== */

INSERT INTO star.dim_timeslot (time_slot_key, hour, minute, slot_label, session_type, is_market_open)
SELECT
    (EXTRACT(HOUR FROM t) * 100 + EXTRACT(MINUTE FROM t))::integer AS time_slot_key,
    EXTRACT(HOUR FROM t)::smallint AS hour,
    EXTRACT(MINUTE FROM t)::smallint AS minute,
    to_char(t, 'HH24:MI') || ' - ' || to_char(t + interval '14 minutes', 'HH24:MI') AS slot_label,
    CASE
        WHEN t::time >= '09:30'::time AND t::time < '16:00'::time THEN 'Regular'
        WHEN t::time >= '04:00'::time AND t::time < '09:30'::time THEN 'Pre-Market'
        WHEN t::time >= '16:00'::time AND t::time <= '20:00'::time THEN 'After-Hours'
        ELSE 'Closed'
    END AS session_type,
    CASE WHEN t::time >= '09:30'::time AND t::time < '16:00'::time THEN true ELSE false END AS is_market_open
FROM generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-01 23:45:00'::timestamp, '15 minutes'::interval) AS t
ON CONFLICT (time_slot_key) DO NOTHING;
