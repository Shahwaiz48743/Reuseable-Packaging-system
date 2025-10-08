# Reuseable-Packaging-system
Reusable Packaging Loop &amp; Traceability DB (SQL Server): deposits, checkouts/returns, wash/inspection, incidents, sensors—prod-ready with seed data.


Reusable Packaging Loop & Traceability DB

Production-grade SQL Server schema for deposit-based reusable packaging (cups/boxes/jars). Tracks assets, deposits, checkouts/returns, wash cycles, inspections, contamination incidents, sensor telemetry, movements, and audits—with realistic seed data and a professional query pack (basic → advanced).

Why this matters

Reduces single-use waste by making packaging reuse auditable and measurable.

Traceability from instance creation → checkout → return → wash → reuse.

Operational insights (turnaround time, pass rates, temperature breaches, loss hotspots).

Designed like a production system: clean keys, cascade strategy, indexes, and analytics views.

What’s included

SQL Server schema (tables, constraints, indexes).

Seed data: ~30 realistic rows per table for instant exploration.

Query pack: curated SQL from basic to advanced (CTEs, windows, APPLY, PIVOT/ROLLUP, Haversine).

Views for dashboards and quick analysis.

Tech: Microsoft SQL Server (T-SQL). Time stored in UTC.

Data model (at a glance)

Locations (retailer / hub / dropbox)

Retailers, Hubs, Customers (actors)

Packaging Catalog & Instances (asset & instance-level state)

Deposits: accounts + transactions (earn/spend/holds/penalties)

Checkouts & Returns (loan & reverse logistics)

Wash Cycles & Inspections (quality, compliance)

Contamination Incidents (type, severity)

Sensor Readings (temperature/shock/humidity)

Movements (chain-of-custody trail)

Audit Logs (immutable, FK-free by design)

erDiagram
  LOCATIONS ||--o| RETAILERS : has
  LOCATIONS ||--o| HUBS : has
  CUSTOMERS ||--|| DEPOSIT_ACCOUNTS : owns
  DEPOSIT_ACCOUNTS ||--o{ DEPOSIT_TRANSACTIONS : logs

  PACKAGING_CATALOG ||--o{ PACKAGING_INSTANCES : defines
  PACKAGING_INSTANCES ||--o{ CHECKOUTS : loaned_as
  CHECKOUTS ||--o{ RETURNS : closed_by

  HUBS ||--o{ WASH_CYCLES : runs
  WASH_CYCLES ||--o{ INSPECTIONS : includes
  PACKAGING_INSTANCES ||--o{ INSPECTIONS : checked
  PACKAGING_INSTANCES ||--o{ CONTAMINATION_INCIDENTS : incident
  PACKAGING_INSTANCES ||--o{ SENSOR_READINGS : telemetry
  LOCATIONS ||--o{ SENSOR_READINGS : context
  PACKAGING_INSTANCES ||--o{ MOVEMENTS : moves

Key design decisions

Cascade strategy (SQL Server safe): only isolated cascades (e.g., deposits chain). Elsewhere use NO ACTION or SET NULL to avoid multiple cascade paths errors.

Enumerations via CHECK constraints for portability (state, kind, result, etc.).

Audit logs have no FKs (history is preserved even if parents are deleted).

Indexes on foreign keys and time columns to support feeds & analytics.

Time stored with SYSUTCDATETIME().

Quick start
1) Create the database & tables

Open SSMS and run the schema script (creates DB ReusablePackagingDB and all tables/constraints/indexes).

2) Load seed data

Run the seed script to insert 30 rows per table (plus 90 locations for variety). IDs are fixed via SET IDENTITY_INSERT so FKs line up.

3) Explore with the query pack

Run the professional query pack to see:

Nearest retailers (Haversine), nearby searches

Running deposit ledger balances (window SUM)

Overdue loans, turnaround percentiles, pass rates by hub

Temperature breaches, movement dwell-time (via LAG)

KPIs with ROLLUP (daily checkouts/returns, state counts)

Folder structure (suggested)
.
├─ schema/
│  └─ reusable_packaging_schema.sql
├─ seed/
│  └─ reusable_packaging_seed_inserts.sql
├─ queries/
│  └─ reusable_packaging_queries_professional.sql
└─ README.md

Highlights for recruiters

Systems design: assets, money, quality, telemetry, and logistics connected coherently.

Advanced SQL: window functions, CTEs, APPLY (Haversine), PIVOT/ROLLUP, analytic views.

Operational rigor: constraints and indexes chosen for integrity and performance.

Demonstrable: seeded data + ready-made queries enable quick evaluation.

Example outcomes (not code)

Turnaround p50/p90 for returns by item type and retailer.

On-time vs late return rates per retailer.

Pass rate after wash cycles per hub (quality signal).

Temperature breach list for last 24h (risk control).

Dwell time at last known location (rebalancing hints).

Loss/shrink hotspots via incident & movement patterns.

Roadmap

Native geospatial types & spatial indexes for accurate proximity.

Matching/alerts as stored procedures (overdue, breach, rewash).

Role-based access & row-level security patterns (reporting vs ops).

Anonymized exports for research and policy partners.
