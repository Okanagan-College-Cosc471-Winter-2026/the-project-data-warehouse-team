/* ====================================================================
   SCRIPT: 04_seed_symbol.sql
   PURPOSE: Initializes the Symbol Dimension (`dim_symbol`).
            Dynamically scans the live integration layer to populate 
            the initial roster of active trading tickers.
   ==================================================================== */

INSERT INTO star.dim_symbol (symbol, is_active)
SELECT DISTINCT symbol, true
FROM dw.live_15m_agg
ON CONFLICT (symbol) DO NOTHING;
