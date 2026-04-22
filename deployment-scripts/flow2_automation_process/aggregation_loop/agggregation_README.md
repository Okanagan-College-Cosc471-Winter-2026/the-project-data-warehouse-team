Loop 2: The ML Feature Aggregation Loop

=== Overview ===

Loop 2 is the core processing engine of the live Data Warehouse. It bridges the gap between historical bulk processing and real-time streaming.

This loop acts as an infinite "Heartbeat." It wakes up every 60 seconds, checks the ingestion progress, and waits for a 15-minute market window to fully close. Once a safe offset is reached (to allow late-arriving network ticks to settle), it dynamically calculates moving averages, percentage changes, and forward-fills gaps, ensuring downstream ML models have perfectly continuous intraday data.

=== Phase 1: Database Setup & Initialization ===

Before starting the live loop, it needs a state-management table and needs to know exactly where your historical bulk processing left off. 
We manage this state using a High Watermark in the pipeline.control table.

    Create the Control Table
    If you haven't already, run this script to build the state-tracking table:

SQL

-- 00_create_pipeline_control.sql
CREATE TABLE IF NOT EXISTS pipeline.control (
    key TEXT NOT NULL,
    value TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    note TEXT,
    CONSTRAINT control_pkey PRIMARY KEY (key)
);

    Set the High Watermark
    Run this query to initialize the loop. Modify the timestamp string to match the last complete 15-minute window that your bulk pipeline successfully processed. 
    If you are starting fresh from January 2nd, set this to just before the first active window.

SQL:
INSERT INTO pipeline.control (key, value, updated_at, note)
VALUES (
    'aggregation_last_processed_ts', 
    '2024-01-02 06:15:00-08'::timestamptz, 
    CURRENT_TIMESTAMP, 
    'Manual initialization for live loop transition'
)
ON CONFLICT (key) DO UPDATE 
SET value = EXCLUDED.value, 
    updated_at = EXCLUDED.updated_at,
    note = EXCLUDED.note;

=== Phase 2: Configuration Options ===

The core intelligence of Loop 2 lives inside agg_run_batch.sql and the dedicated live stored procedures it calls.

    Target Tables
    Ensure your live loop is pointing to the _live tables, isolating it from your historical _bulk and _extract tables.

    04_compile_aggregator_live.sql should read from staging.market_data_5m_stream and insert into dw.live_15m_agg.
    05_compile_imputation_live.sql should read from dw.live_15m_agg and insert into dw.live_15m_ffill.

    Adjusting the Safe Offset Triggers
    By default, the pipeline waits 10 minutes past the 15-minute boundary before aggregating (e.g., aggregating the 09:30-09:45 window at 09:55) 
    to absorb delayed vendor ticks. To process data faster (e.g., waiting only 5 minutes), open agg_run_batch.sql and adjust the trigger minutes:

SQL:
-- Inside agg_run_batch.sql
-- Change this list to (05, 20, 35, 50) for a 5-minute offset
IF EXTRACT(MINUTE FROM v_latest_ingested) IN (10, 25, 40, 55)

=== Phase 3: Execution & Operations ===

The loop runs via a robust Bash wrapper (agg_aggregate_loop.sh). It safely catches network disconnects (SSL drops), 

=== Starting the loop ===
Just like the ingestion simulator, you should run this inside a virtual terminal (screen or tmux) so it persists in the background.

    Make the script executable (first-time setup only):
    Bash

    chmod +x agg_aggregate_loop.sh

    Start a detached terminal session:
    Bash

    screen -S agg_heartbeat

    Execute the loop:
    Bash

    ./agg_aggregate_loop.sh

    Detach from the screen: Press Ctrl + A, then D.

=== The Catch-Up Mechanic ===
If your High Watermark is days or weeks behind the live staging table, you will see the script enter Catch-Up Mode. It will rapidly execute the stored procedures back-to-back until the watermark matches the present moment, at which point it will seamlessly transition into the 60-second live heartbeat.

=== Monitoring Progress ===
Watch the pipeline calculate your ML features in real-time by querying the cycle log:

SQL:
SELECT cycle_start_ts, status, duration_seconds 
FROM pipeline.cycle_log 
WHERE cycle_type = 'aggregation' 
ORDER BY cycle_start_ts DESC 
LIMIT 5;

To stop the loop gracefully, reattach to the screen session (screen -r agg_heartbeat) and press Ctrl + C. Because the stored procedures use ON CONFLICT DO NOTHING, stopping and restarting the loop mid-transaction is completely safe and will not cause duplicate rows.
