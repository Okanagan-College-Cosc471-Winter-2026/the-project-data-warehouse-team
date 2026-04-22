Note: For all database commands, replace penguin_db with your database string.
Loop 3: The Live Star Schema Router

=== Overview ===
Loop 3 is the Presentation Layer of the data warehouse. 
It continuously sweeps freshly aggregated 15-minute windows from the live feature store (dw.live_15m_agg) into a normalized Kimball Star Schema.

This layer is strictly client-facing and optimized for Business Intelligence (BI) tools. It intentionally reads from the raw aggregation table rather 
than the forward-filled (imputed) tables, guaranteeing that clients see a 100% mathematically auditable view of true market reality without any synthetic data.

=== File Structure ===
To run this pipeline, ensure the following scripts are present in your directory:

Initialization Scripts:
    00_teardown_star_schema.sql (Optional: for clean resets)
    01_star_create_schema.sql
    02_seed_date.sql
    03_seed_timeslot.sql
    04_seed_symbol.sql

Execution Scripts:
    05_compile_star_procedure.sql
    star_run_batch.sql
    star_router_loop.sh

=== Phase 1: Database Initialization ===

Before starting the daemon, you must build the normalized tables and populate the static dimension spines.
1. Build the Schema

Run the schema creation script to build the star schema, the three dimension tables, and the central Fact table.

Bash:
psql -d penguin_db -f 01_star_create_schema.sql

2. Seed the Dimensions

The Fact table cannot accept data until the dimensions exist. Run these three scripts sequentially to build the calendar spine out to 2030, generate the daily 15-minute time slots, and scan the live tables for the initial ticker roster.

Bash:
psql -d penguin_db -f 02_seed_date.sql
psql -d penguin_db -f 03_seed_timeslot.sql
psql -d penguin_db -f 04_seed_symbol.sql

=== Phase 2: Compiling the Router Logic ===

Compile the stored procedure that powers the ETL process. This procedure handles UNION joins, foreign key auto-injection for new tickers, and idempotent ON CONFLICT DO UPDATE insertions.
Bash

psql -d penguin_db -f 05_compile_star_procedure.sql

=== Phase 3: Execution & Operations ===

The routing engine operates via an indestructible Bash loop that wakes up every 5 minutes. It uses a self-referencing "High-Water Mark" by querying the absolute maximum date currently residing in the Fact table, allowing it to seamlessly catch up if it is ever turned off.
Starting the Daemon

Because this loop runs indefinitely, it is highly recommended to run it inside a terminal multiplexer (screen or tmux).

    Make the bash loop executable (first-time setup only):
    Bash:
    chmod +x star_router_loop.sh

    Start a detached terminal session:
    
    Bash:
    screen -S star_router

    Execute the loop:
    Bash:
    ./star_router_loop.sh

    Detach from the screen: Press Ctrl + A, then D.

(Note: On its very first run, it will detect an empty Fact table, default the high-water mark to '2023-01-01', 
and sweep all available live history into the Star Schema in one massive transaction.)

=== Verification & BI Testing ===

Once the loop runs its first successful cycle, you can verify the integrity of the Presentation Layer by running a standard BI query. 
This query dynamically finds the latest trading day in the warehouse and groups the volume by Market Session (Pre-Market, Regular, After-Hours).

SELECT 
    s.symbol,
    d.full_date,
    t.session_type,
    COUNT(f.time_slot_key) AS windows_recorded,
    SUM(f.volume) AS session_volume,
    ROUND(AVG(f.close), 2) AS avg_price,
    MAX(f.high) AS session_high,
    MIN(f.low) AS session_low
FROM star.fact_market_data_15m f
JOIN star.dim_symbol s ON f.symbol_key = s.symbol_key
JOIN star.dim_date d ON f.date_key = d.date_key
JOIN star.dim_timeslot t ON f.time_slot_key = t.time_slot_key
WHERE d.full_date = (
    SELECT MAX(d2.full_date) 
    FROM star.dim_date d2 
    JOIN star.fact_market_data_15m f2 ON d2.date_key = f2.date_key
)
GROUP BY s.symbol, d.full_date, t.session_type
ORDER BY session_volume DESC
LIMIT 20;
