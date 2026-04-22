    SELECT symbol, window_ts 
    FROM dw.market_data_15m_bulk 
    WHERE window_ts >= '2023-07-01 00:00:00-07' 
      AND window_ts < '2023-08-01 00:00:00-07'
      AND symbol IN ('aph', 'APH');
