/* ====================================================================
   SCRIPT: 06_validation_suite.sql
   PURPOSE: Validation suite to verify imputation and feature math.
   ==================================================================== */

-- Proof 1: The Imputation Integrity Check
SELECT 
    is_imputed, COUNT(*) AS total_rows, SUM(volume) AS aggregate_volume, MAX(volume) AS max_volume_found
FROM dw.extract_15m_ffill
GROUP BY is_imputed;

-- Proof 2: The Feature Math Audit
SELECT 
    window_ts, close, lag_close_1,
    pct_change_1 AS pipeline_pct_change,
    ROUND(((close - lag_close_1) / lag_close_1), 6) AS manual_pct_change,
    sma_close_5 AS pipeline_sma_5,
    ROUND(AVG(close) OVER (ORDER BY window_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 6) AS manual_sma_5
FROM dw.extract_15m_ffill
WHERE symbol = 'aapl'
  AND lag_close_1 IS NOT NULL
  AND sma_close_5 IS NOT NULL
ORDER BY window_ts ASC
LIMIT 10;

-- Proof 3: The Temporal Boundary Check (Overnight Gaps)
SELECT 
    DATE(window_ts) AS trade_date, symbol, previous_close AS yesterdays_close, open AS todays_first_open,
    overnight_gap_pct AS pipeline_gap_pct,
    ROUND(((open - previous_close) / previous_close), 6) AS manual_gap_pct,
    is_gap_up, is_gap_down
FROM dw.extract_15m_ffill
WHERE symbol = 'NVDA'
  AND (window_ts AT TIME ZONE 'America/Los_Angeles')::time = '06:30:00'::time
  AND window_ts >= '2024-03-01 00:00:00-08'
  AND window_ts < '2024-03-15 00:00:00-08'
ORDER BY trade_date;

-- Proof 4A: Holiday Lag & Return Continuity (Dec 24 - Dec 26)
SELECT
    window_ts,
    close,
    lag_close_1,
    lag_close_5,
    ROUND(pct_change_1, 6) AS pct_change_1
FROM dw.extract_15m_ffill
WHERE symbol = 'aapl'
  -- Zooming in on the end of Christmas Eve and the start of Dec 26th
  AND window_ts >= '2024-12-24 09:00:00-08'
  AND window_ts <= '2024-12-26 07:30:00-08'
ORDER BY window_ts;

-- Proof 4B: Holiday Moving Average Continuity (Dec 24 - Dec 26)
SELECT
    window_ts,
    close,
    ROUND(sma_close_5, 4) AS sma_close_5,
    ROUND(sma_close_10, 4) AS sma_close_10,
    ROUND(sma_volume_5, 0) AS sma_volume_5
FROM dw.extract_15m_ffill
WHERE symbol = 'aapl'
  AND window_ts >= '2024-12-24 09:00:00-08'
  AND window_ts <= '2024-12-26 07:30:00-08'
ORDER BY window_ts;

-- Proof 4C: Time Encodings & Overnight Gaps (Dec 24 - Dec 26)
SELECT
    window_ts,
    day_of_week,
    hour_of_day,
    previous_close,
    ROUND(overnight_gap_pct, 6) AS gap_pct,
    is_gap_up,
    is_gap_down
FROM dw.extract_15m_ffill
WHERE symbol = 'aapl'
  AND window_ts >= '2024-12-24 09:00:00-08'
  AND window_ts <= '2024-12-26 07:30:00-08'
ORDER BY window_ts;
