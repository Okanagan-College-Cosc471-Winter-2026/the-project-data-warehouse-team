DATA WAREHOUSE: LAMBDA ARCHITECTURE PIPELINE
OVERVIEW

This repository contains the deployment scripts and operational daemon loops for a fully automated, stateless Data Warehouse. It is built 
on a Lambda Architecture, splitting data processing into two distinct streams:

    Flow 1 (Cold / Batch Pipeline): Processes years of historical market data, handling massive aggregations, forward-fill imputations, 
    and flattening into a highly optimized Machine Learning Feature Store.

    Flow 2 (Hot / Streaming Pipeline): A real-time processing engine that ingests live 5-minute ticks, aggregates them into intraday 
    features, and routes them to a normalized Star Schema for BI reporting.

DIRECTORY STRUCTURE & NAVIGATION

All operational code is housed within the "deployment-scripts/" directory. Below is a guide to the functional areas of the repository.

[ 1. flow1_batch_process/ ] (The Historical Pipeline)
This directory contains the sequential SQL scripts to initialize the data warehouse, profile the ticker universe, and bulk-process historical data.

    Core ETL: Scripts 00 through 07 execute the batch aggregations and bounded imputations (extract and ffill tables).

    ml_feature_store/: Contains the logic to flatten the historical forward-filled data into a densely packed, integer-mapped matrix optimized for Python/XGBoost ingestion.
    -> See "ml_README.md" in this subdirectory for EXPLAIN ANALYZE benchmarks and architecture notes.

[ 2. flow2_automation_process/ ] (The Live Pipeline)
This directory houses the three persistent Bash daemons that power the real-time data warehouse.

    optional_ingest_loop/: A simulator that acts as the data vendor. It sequentially trickles historical 5-minute ticks into the live staging table to simulate a live market feed.

    aggregation_loop/: The core "Heartbeat." Wakes up every 60 seconds to detect closed 15-minute market windows, dynamically calculating 
    moving averages and percentage changes on the fly.
    -> See "agggregation_README.md" in this subdirectory for state-management and high-water mark details.

    star_router_loop/: The BI Presentation Layer. Sweeps raw, unimputed aggregations into a normalized Kimball Star Schema to provide mathematically 
    auditable realities for client-facing dashboards.
    -> See "star_README.md" in this subdirectory for initialization and BI query instructions.

[ 3. Utility & Testing Directories ]

    validation/: Contains standalone data quality checks to ensure mathematical integrity. 
    Includes scripts to detect missing windows (01_check_holes_in_data.sql) and verify symbol completion rates.

    internal_testing/: Contains the disaster recovery simulator (trigger_forced_recovery.sql), which physically 
    deletes live data and rewinds the pipeline clock to test the system's stateless, self-healing capabilities.

    archived/: Deprecated V1 flat-table scripts retained for version control history. Do not use for current deployments.

EXECUTION GUIDE

If you are deploying this environment from scratch, follow this exact sequence:

Step 1: Build the History
Navigate to "flow1_batch_process/" and execute "run_pipeline.sh". This will initialize the schemas, execute the bulk ETL, and finalize the historical tables.

Step 2: Build the ML Matrix (Optional)
Navigate to "flow1_batch_process/ml_feature_store/" and execute "01_create_star_ml_schema.sql" to generate the cold training data.

Step 3: Start the Live Pipeline
Navigate to "flow2_automation_process/" and initialize the dimensional tables inside the "star_router_loop/".

Step 4: Ignite the Daemons
Spin up screen or tmux sessions and launch the Bash loops in this order:

    star_router_loop.sh (Starts sweeping)

    agg_aggregate_loop.sh (Starts the ML feature heartbeat)

    loop1_ingestion_simulator.sh (Starts the simulated live feed)
