/* ====================================================================
   SCRIPT: 01_create_pipeline_control.sql
   PURPOSE: Creates the state-management table used by the live heartbeat 
            to track the High Watermark (last processed timestamp).
   ==================================================================== */

CREATE TABLE IF NOT EXISTS pipeline.control (
    key TEXT NOT NULL,
    value TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    note TEXT,
    CONSTRAINT control_pkey PRIMARY KEY (key)
);

-- TABLE 4: 15m Aggregated LIVE
CREATE TABLE dw.live_15m_agg (LIKE dw.extract_15m_agg INCLUDING ALL);

-- TABLE 5: 15m Forward-Filled LIVE
CREATE TABLE dw.live_15m_ffill (LIKE dw.extract_15m_ffill INCLUDING ALL);
