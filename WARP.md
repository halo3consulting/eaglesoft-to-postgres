# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Sybase SQL Anywhere to PostgreSQL synchronization tool intended for production reporting workloads. It supports multiple sync strategies (full, incremental, incremental by primary key, CDC via `rw_` tables, query-based filtering, and append-only), runs tables in parallel, auto-creates target schemas/tables/indexes, and tracks detailed sync history in PostgreSQL for observability.

## Development Environment & Commands

### Python, Poetry, and Dependencies

- Project is managed with Poetry (`pyproject.toml`).
- Main entrypoint is exposed as a Poetry script: `eaglesoft-sync = sybase_postgres_sync:main`.

Install dependencies (including dev tools like pytest/black/ruff):

```bash
poetry install
```

Run the sync tool via Poetry (recommended):

```bash
poetry run eaglesoft-sync --config sync_config.yaml --job quick
poetry run eaglesoft-sync --config sync_config.yaml --job nightly
```

Run a single-table sync (bypassing jobs):

```bash
poetry run eaglesoft-sync --config sync_config.yaml --table patient
```

You can also invoke the script directly if you are in the virtualenv:

```bash
poetry run python sybase_postgres_sync.py --config sync_config.yaml --job quick
```

### Docker-based Workflow

The recommended production/deployment path is via Docker (see `Dockerfile` and `README.md`). The image expects the SQL Anywhere client binaries to be present in a `client17011` directory at build time.

Build image (after downloading/extracting the SQL Anywhere client into `client17011`):

```bash
docker build -t eaglesoft-sync .
```

Run the container using local config and environment:

```bash
docker run \
  -v "$(pwd)/sync_config.yaml:/app/sync_config.yaml" \
  -v "$(pwd)/.env:/app/.env" \
  eaglesoft-sync
```

Environment variables for DB credentials (e.g., `SYBASE_PASSWORD`, `POSTGRES_PASSWORD`) are typically provided via `.env` and consumed by `sync_config.yaml` and `python-dotenv`.

### Linting and Formatting

Configured tools (from `pyproject.toml`):

```bash
poetry run black sybase_postgres_sync.py
poetry run ruff sybase_postgres_sync.py
```

### Testing

Pytest is included as a dev dependency. Once tests exist, they should be run via:

```bash
poetry run pytest
# or a specific test
poetry run pytest tests/test_some_module.py::test_case_name
```

## Configuration Model (`sync_config.yaml`)

The sync behavior is almost entirely driven by `sync_config.yaml`:

- **source/target**: Connection details for Sybase SQL Anywhere (`source`) and PostgreSQL (`target`). Passwords are specified as `${ENV_VAR}` placeholders and resolved from the environment.
- **sync**:
  - `batch_size`: number of records per batch when reading from Sybase and upserting to Postgres.
  - `parallel_tables`: number of tables synced concurrently (used by `ThreadPoolExecutor`).
- **tables**: Single canonical list of all tables with:
  - `name`, `source_schema`, `target_schema`.
  - `sync_strategy`: one of `full`, `incremental`, `incremental_pk`, `cdc`, `query_filter`, `append` (append handled in code as a special case of no upsert conflict handling).
  - Optional fields per strategy:
    - `incremental_column` for `incremental`.
    - `primary_key` override when autodetection from Sybase is insufficient (e.g., `transactions` view).
    - `filter_query` for `query_filter` strategies (the query is in Sybase SQL and must return PKs).
  - `tags`: used for grouping (e.g., `critical`, `reference`, `financial`, `static`, `cdc`).
- **jobs**: Named logical jobs that select/modify subsets of `tables`:
  - Filters: `include_strategies`, `include_tags`, `include_tables`, `exclude_tables`.
  - `override_strategy`: forces all tables in the job to use a strategy (e.g., `full_rebuild` job forcing `full`).
  - `overrides`: per-table overrides that win over global overrides.

Key predefined jobs:

- `quick`: frequent, fast sync for time-sensitive data; includes CDC, incremental, incremental_pk, and filtered tables.
- `nightly`: default nightly run; includes all tables with their configured strategies.
- `full_rebuild`: weekly-style full rebuild that truncates and reloads all tables.
- `reference`, `critical`, `financial`: narrower jobs keyed off `tags` for specific reporting use-cases.

The Python implementation (`DataSync.get_tables_for_job`) is the source of truth for how these filters and overrides combine; consult it before changing semantics.

## Core Runtime Architecture (`sybase_postgres_sync.py`)

### High-level Flow

- Entry point is the Click CLI `main()`:
  - Global options:
    - `--config/-c`: path to YAML config (default `sync_config.yaml`).
    - `--job/-j`: named job from `jobs` in the config (e.g., `quick`, `nightly`, `full_rebuild`).
    - `--table/-t`: sync a single table by name (ignores `jobs` and runs a one-off run with `job_name = "single-table:{table}"`).
  - If `--table` is provided, the tool looks up that table in `config["tables"]` and runs `DataSync.sync_table` for just that table, recording a dedicated `sync_runs` entry.
  - Otherwise it runs `DataSync.run_sync(job_name=job)` for a full multi-table job.

### Major Components

#### `SyncState`

Responsible for all Postgres-side metadata and observability:

- Ensures the existence of schema `sync_metadata` (configurable via constructor but defaulted in code) and three core tables:
  - `sync_state`: last sync value per `(table_name, column_name)` pair for incremental syncs.
  - `sync_runs`: one row per overall sync run (job-level), including timing, counts, and status (`success`, `partial`, `failed`).
  - `sync_table_results`: one row per table per run, with strategy, timing, record counts, status (`success`, `error`, `validation_failed`), and optional JSON `stats` (e.g., CDC insert/update/delete counts).
- Provides APIs to:
  - Read/write last sync values for incremental/incremental_pk strategies.
  - Create and complete `sync_runs` rows.
  - Record per-table results.

The table schemas and their intended usage are documented in detail in `SYNC_HISTORY_QUERIES.md`, which also contains canonical analysis queries used in production (e.g., performance dashboards, failure triage, CDC statistics, cleanup policies).

#### `DataSync`

Orchestrates everything about a sync run:

- **Initialization**:
  - Loads configuration YAML and interpolates `${ENV_VAR}` placeholders for passwords.
  - Configures structured logging using `colorlog` to STDOUT and a `RotatingFileHandler` to `logs/sync.log` (log path, level, rotation settings are in `sync_config.yaml` under `logging`).
  - Instantiates a `SyncState` bound to the Postgres target described in the config.
- **Connections**:
  - `sybase_connection()`: context manager creating a `sqlanydb` connection using host/uid/pwd from `config["source"]`.
  - `postgres_connection()`: context manager creating a `psycopg2` connection using host/port/db/user/password from `config["target"]`.
- **Job/Table Selection**:
  - `get_tables_for_job(job_name)` filters `config["tables"]` according to the job definition, combining `include_strategies`, `include_tags`, `include_tables`, `exclude_tables`, `override_strategy`, and `overrides` into a final list of table configs.
- **Validation Phase** (before any data moves):
  - `validate_table_sync(source_conn, table_config)` inspects Sybase system catalogs to ensure:
    - Source table exists and has columns.
    - All data types can be mapped to PostgreSQL via `map_sybase_to_postgres_type`.
    - Strategy-specific prerequisites are met:
      - `cdc` requires a corresponding `rw_{table}` change-tracking table (missing table yields warnings, not hard errors).
      - `incremental` requires `incremental_column` in config.
      - `incremental_pk` / `cdc` / `query_filter` require a primary key (discovered or overridden).
      - `query_filter` requires a non-empty `filter_query`.
  - Validation failures are logged and written to `sync_table_results` as `validation_failed`, and the entire run is aborted with a failed `sync_runs` record.
- **Execution Phase**:
  - `run_sync(job_name)` uses a `ThreadPoolExecutor` sized by `config["sync"]["parallel_tables"]` to run `sync_table` concurrently for each selected table, while accumulating aggregate stats and per-table timing information.

#### Schema and Table Management

- `create_target_table`:
  - Checks if the target table exists in Postgres; if not, it introspects the Sybase table definition (via `SYS.SYSTABCOL` / `SYS.SYSDOMAIN`) to construct a compatible PostgreSQL `CREATE TABLE` statement, including:
    - Column names/types mapped by `map_sybase_to_postgres_type`.
    - `NOT NULL` constraints when Sybase `nulls != 'Y'`.
    - Primary key inferred from Sybase or overridden from config.
  - If the table already exists, it ensures a primary key or at least a unique index on the configured/inferred primary key columns.
  - Invokes `create_indexes` to mirror non-PK, non-FK indexes from Sybase into PostgreSQL with sanitized index names.

#### Core Sync Strategies

- **Full**:
  - Truncates the target table (`TRUNCATE TABLE {schema}.{table}`) and bulk upserts all rows from Sybase in batches.
- **Incremental**:
  - Uses `sync_state` to remember the last value seen in `incremental_column` and only selects `WHERE column > last_value` ordered by that column.
- **Incremental by Primary Key (`incremental_pk`)**:
  - Similar to incremental but uses the primary key instead of a dedicated timestamp column.
- **Query Filter (`query_filter`)**:
  - Uses a user-provided `filter_query` (Sybase SQL) as a subquery to limit which PKs are synced.
  - Handles both single-column and composite primary keys, switching between `IN` and `EXISTS`-based joins.
- **CDC (`sync_table_cdc`)**:
  - Reads from `rw_{table}` change-tracking tables in Sybase.
  - For each change row, fetches the current record from the base table and upserts into Postgres (for `N`/`U`) or deletes from Postgres (for `D`).
  - Tracks stats per type (inserts, updates, deletes, errors) and cleans up processed rows from the `rw_` table in a batched fashion (optimized for single vs composite primary keys).

All write paths ultimately funnel through `upsert_batch`, which uses `psycopg2.extras.execute_values` to bulk insert/upsert rows, constructing an `ON CONFLICT` clause when a primary key is known and the strategy is not `append`.

## Sync History & Analysis

- The PostgreSQL `sync_metadata` schema is central for observability and operates as the long-term audit log of sync behavior.
- `SYNC_HISTORY_QUERIES.md` contains a curated collection of SQL queries for:
  - Inspecting recent runs and per-table results.
  - Performance analysis and trending (slow tables, record volumes, time-series stats).
  - CDC-specific metrics (insert/update/delete/error breakdowns).
  - Data quality and anomaly detection (tables not syncing, high failure-rate tables, outlier record counts).
  - Cleanup and storage management (retention windows, table sizes).

When debugging production behavior or adding new reporting around sync performance, start with the schemas in `SyncState._ensure_state_table()` and the queries in `SYNC_HISTORY_QUERIES.md` to ensure any new logic or dashboards align with the existing model.
