# Sync History Database Queries

This document contains useful SQL queries for analyzing sync history stored in the `sync_metadata` schema.

## Table Schemas

### sync_runs
Tracks overall sync run information:
- `sync_run_id` (UUID) - Unique identifier for each sync run
- `job_name` (VARCHAR) - Which job was executed (e.g., 'quick', 'nightly', 'full_rebuild')
- `start_time` (TIMESTAMP) - When sync started
- `end_time` (TIMESTAMP) - When sync completed
- `duration_seconds` (DECIMAL) - Total duration
- `total_tables` (INTEGER) - Number of tables in this run
- `successful_tables` (INTEGER) - Tables synced successfully
- `failed_tables` (INTEGER) - Tables that failed
- `total_records` (BIGINT) - Total records synced across all tables
- `status` (VARCHAR) - 'success', 'partial', or 'failed'
- `error_message` (TEXT) - Error details if applicable

### sync_table_results
Tracks per-table sync results:
- `id` (SERIAL) - Auto-increment ID
- `sync_run_id` (UUID) - Foreign key to sync_runs
- `table_name` (VARCHAR) - Table name
- `sync_strategy` (VARCHAR) - Strategy used (full, cdc, incremental, etc.)
- `start_time` (TIMESTAMP) - When table sync started
- `end_time` (TIMESTAMP) - When table sync completed
- `duration_seconds` (DECIMAL) - Duration for this table
- `records_synced` (BIGINT) - Number of records
- `status` (VARCHAR) - 'success', 'error', 'validation_failed'
- `error_message` (TEXT) - Error details if failed
- `stats` (JSONB) - Additional details (CDC stats: inserts/updates/deletes/errors)

---

## Recent Sync Runs

### View last 10 sync runs
```sql
SELECT
    sync_run_id,
    job_name,
    start_time,
    end_time,
    duration_seconds,
    total_tables,
    successful_tables,
    failed_tables,
    total_records,
    status
FROM sync_metadata.sync_runs
ORDER BY start_time DESC
LIMIT 10;
```

### View today's sync runs
```sql
SELECT
    sync_run_id,
    job_name,
    start_time,
    duration_seconds,
    total_tables,
    successful_tables,
    failed_tables,
    total_records,
    status
FROM sync_metadata.sync_runs
WHERE start_time >= CURRENT_DATE
ORDER BY start_time DESC;
```

### View failed or partial sync runs
```sql
SELECT
    sync_run_id,
    job_name,
    start_time,
    duration_seconds,
    total_tables,
    successful_tables,
    failed_tables,
    status,
    error_message
FROM sync_metadata.sync_runs
WHERE status IN ('failed', 'partial')
ORDER BY start_time DESC
LIMIT 20;
```

---

## Table-Level Analysis

### View table results for a specific sync run
```sql
SELECT
    table_name,
    sync_strategy,
    start_time,
    end_time,
    duration_seconds,
    records_synced,
    status,
    error_message
FROM sync_metadata.sync_table_results
WHERE sync_run_id = 'YOUR-SYNC-RUN-UUID-HERE'
ORDER BY duration_seconds DESC;
```

### Find all failed tables in recent runs
```sql
SELECT
    str.sync_run_id,
    sr.job_name,
    sr.start_time as run_start_time,
    str.table_name,
    str.sync_strategy,
    str.error_message
FROM sync_metadata.sync_table_results str
JOIN sync_metadata.sync_runs sr ON str.sync_run_id = sr.sync_run_id
WHERE str.status IN ('error', 'validation_failed')
ORDER BY sr.start_time DESC
LIMIT 50;
```

### View sync history for a specific table
```sql
SELECT
    sr.job_name,
    str.start_time,
    str.duration_seconds,
    str.records_synced,
    str.status,
    str.error_message
FROM sync_metadata.sync_table_results str
JOIN sync_metadata.sync_runs sr ON str.sync_run_id = sr.sync_run_id
WHERE str.table_name = 'patient'
ORDER BY str.start_time DESC
LIMIT 20;
```

---

## Performance Analysis

### Find slowest tables (average duration)
```sql
SELECT
    table_name,
    sync_strategy,
    COUNT(*) as sync_count,
    AVG(duration_seconds) as avg_duration,
    MIN(duration_seconds) as min_duration,
    MAX(duration_seconds) as max_duration,
    SUM(records_synced) as total_records_synced
FROM sync_metadata.sync_table_results
WHERE status = 'success'
GROUP BY table_name, sync_strategy
ORDER BY avg_duration DESC
LIMIT 20;
```

### Find tables with highest record counts
```sql
SELECT
    table_name,
    sync_strategy,
    COUNT(*) as sync_count,
    AVG(records_synced) as avg_records,
    MAX(records_synced) as max_records,
    SUM(records_synced) as total_records
FROM sync_metadata.sync_table_results
WHERE status = 'success'
GROUP BY table_name, sync_strategy
ORDER BY avg_records DESC
LIMIT 20;
```

### View sync performance trends over time
```sql
SELECT
    DATE(start_time) as sync_date,
    COUNT(*) as num_runs,
    AVG(duration_seconds) as avg_duration,
    SUM(total_records) as total_records,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status IN ('failed', 'partial') THEN 1 ELSE 0 END) as failed_runs
FROM sync_metadata.sync_runs
GROUP BY DATE(start_time)
ORDER BY sync_date DESC
LIMIT 30;
```

### Find tables with increasing sync times
```sql
WITH recent_syncs AS (
    SELECT
        table_name,
        start_time,
        duration_seconds,
        ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY start_time DESC) as rn
    FROM sync_metadata.sync_table_results
    WHERE status = 'success'
)
SELECT
    curr.table_name,
    prev.duration_seconds as previous_duration,
    curr.duration_seconds as current_duration,
    ((curr.duration_seconds - prev.duration_seconds) / prev.duration_seconds * 100) as percent_increase,
    curr.start_time as latest_sync
FROM recent_syncs curr
JOIN recent_syncs prev ON curr.table_name = prev.table_name AND prev.rn = 2
WHERE curr.rn = 1
  AND curr.duration_seconds > prev.duration_seconds * 1.2  -- 20% slower
ORDER BY percent_increase DESC;
```

---

## CDC Statistics

### View CDC statistics for recent syncs
```sql
SELECT
    table_name,
    start_time,
    records_synced,
    (stats->>'inserts')::int as inserts,
    (stats->>'updates')::int as updates,
    (stats->>'deletes')::int as deletes,
    (stats->>'errors')::int as errors
FROM sync_metadata.sync_table_results
WHERE sync_strategy = 'cdc'
  AND stats IS NOT NULL
ORDER BY start_time DESC
LIMIT 50;
```

### Aggregate CDC statistics by table
```sql
SELECT
    table_name,
    COUNT(*) as sync_count,
    SUM((stats->>'inserts')::int) as total_inserts,
    SUM((stats->>'updates')::int) as total_updates,
    SUM((stats->>'deletes')::int) as total_deletes,
    SUM((stats->>'errors')::int) as total_errors,
    AVG((stats->>'inserts')::int) as avg_inserts_per_sync,
    AVG((stats->>'updates')::int) as avg_updates_per_sync
FROM sync_metadata.sync_table_results
WHERE sync_strategy = 'cdc'
  AND stats IS NOT NULL
  AND status = 'success'
GROUP BY table_name
ORDER BY total_inserts + total_updates + total_deletes DESC;
```

### Find CDC syncs with errors
```sql
SELECT
    sr.job_name,
    str.table_name,
    str.start_time,
    (str.stats->>'inserts')::int as inserts,
    (str.stats->>'updates')::int as updates,
    (str.stats->>'deletes')::int as deletes,
    (str.stats->>'errors')::int as errors,
    str.error_message
FROM sync_metadata.sync_table_results str
JOIN sync_metadata.sync_runs sr ON str.sync_run_id = sr.sync_run_id
WHERE str.sync_strategy = 'cdc'
  AND str.stats IS NOT NULL
  AND (str.stats->>'errors')::int > 0
ORDER BY str.start_time DESC;
```

---

## Job Performance

### Compare job performance
```sql
SELECT
    job_name,
    COUNT(*) as run_count,
    AVG(duration_seconds) as avg_duration,
    MIN(duration_seconds) as min_duration,
    MAX(duration_seconds) as max_duration,
    AVG(total_records) as avg_records,
    AVG(total_tables) as avg_tables,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as success_rate
FROM sync_metadata.sync_runs
WHERE job_name IS NOT NULL
GROUP BY job_name
ORDER BY job_name;
```

### View 'quick' job execution times over last 7 days
```sql
SELECT
    start_time,
    duration_seconds,
    total_tables,
    successful_tables,
    total_records,
    status
FROM sync_metadata.sync_runs
WHERE job_name = 'quick'
  AND start_time >= NOW() - INTERVAL '7 days'
ORDER BY start_time DESC;
```

---

## Data Quality Checks

### Find tables that haven't synced recently
```sql
WITH latest_syncs AS (
    SELECT
        table_name,
        MAX(start_time) as last_sync_time
    FROM sync_metadata.sync_table_results
    WHERE status = 'success'
    GROUP BY table_name
)
SELECT
    table_name,
    last_sync_time,
    NOW() - last_sync_time as time_since_last_sync
FROM latest_syncs
WHERE last_sync_time < NOW() - INTERVAL '24 hours'
ORDER BY last_sync_time;
```

### Find tables with frequent failures
```sql
SELECT
    table_name,
    COUNT(*) as total_syncs,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
    SUM(CASE WHEN status = 'validation_failed' THEN 1 ELSE 0 END) as validation_failures,
    (SUM(CASE WHEN status IN ('error', 'validation_failed') THEN 1 ELSE 0 END)::float / COUNT(*) * 100) as failure_rate
FROM sync_metadata.sync_table_results
WHERE start_time >= NOW() - INTERVAL '30 days'
GROUP BY table_name
HAVING SUM(CASE WHEN status IN ('error', 'validation_failed') THEN 1 ELSE 0 END) > 0
ORDER BY failure_rate DESC;
```

### Find anomalous sync record counts
```sql
WITH table_stats AS (
    SELECT
        table_name,
        AVG(records_synced) as avg_records,
        STDDEV(records_synced) as stddev_records
    FROM sync_metadata.sync_table_results
    WHERE status = 'success'
      AND sync_strategy IN ('full', 'incremental', 'incremental_pk')
      AND start_time >= NOW() - INTERVAL '30 days'
    GROUP BY table_name
)
SELECT
    str.table_name,
    str.start_time,
    str.records_synced,
    ts.avg_records,
    ((str.records_synced - ts.avg_records) / NULLIF(ts.stddev_records, 0)) as z_score
FROM sync_metadata.sync_table_results str
JOIN table_stats ts ON str.table_name = ts.table_name
WHERE str.status = 'success'
  AND str.start_time >= NOW() - INTERVAL '7 days'
  AND ABS((str.records_synced - ts.avg_records) / NULLIF(ts.stddev_records, 0)) > 2  -- More than 2 standard deviations
ORDER BY ABS((str.records_synced - ts.avg_records) / NULLIF(ts.stddev_records, 0)) DESC;
```

---

## Cleanup Queries

### Delete sync history older than 90 days
```sql
-- This will cascade delete from sync_table_results due to foreign key
DELETE FROM sync_metadata.sync_runs
WHERE start_time < NOW() - INTERVAL '90 days';
```

### View sync history storage size
```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables
WHERE schemaname = 'sync_metadata'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

---

## Reporting

### Daily sync summary report
```sql
SELECT
    DATE(start_time) as sync_date,
    job_name,
    COUNT(*) as num_runs,
    AVG(duration_seconds) as avg_duration_sec,
    SUM(total_records) as total_records_synced,
    SUM(successful_tables) as total_successful_tables,
    SUM(failed_tables) as total_failed_tables,
    STRING_AGG(DISTINCT status, ', ') as statuses
FROM sync_metadata.sync_runs
WHERE start_time >= NOW() - INTERVAL '7 days'
GROUP BY DATE(start_time), job_name
ORDER BY sync_date DESC, job_name;
```

### Table sync frequency report
```sql
SELECT
    table_name,
    COUNT(DISTINCT DATE(start_time)) as days_synced,
    COUNT(*) as total_syncs,
    MAX(start_time) as last_sync,
    MIN(start_time) as first_sync
FROM sync_metadata.sync_table_results
WHERE start_time >= NOW() - INTERVAL '30 days'
  AND status = 'success'
GROUP BY table_name
ORDER BY days_synced DESC, total_syncs DESC;
```

### Comprehensive sync run detail
```sql
SELECT
    sr.sync_run_id,
    sr.job_name,
    sr.start_time,
    sr.end_time,
    sr.duration_seconds as total_duration,
    sr.status as run_status,
    sr.total_tables,
    sr.successful_tables,
    sr.failed_tables,
    sr.total_records,
    -- Table details
    json_agg(
        json_build_object(
            'table', str.table_name,
            'strategy', str.sync_strategy,
            'duration', str.duration_seconds,
            'records', str.records_synced,
            'status', str.status,
            'error', str.error_message
        ) ORDER BY str.duration_seconds DESC
    ) as table_details
FROM sync_metadata.sync_runs sr
LEFT JOIN sync_metadata.sync_table_results str ON sr.sync_run_id = str.sync_run_id
WHERE sr.sync_run_id = 'YOUR-SYNC-RUN-UUID-HERE'
GROUP BY sr.sync_run_id, sr.job_name, sr.start_time, sr.end_time,
         sr.duration_seconds, sr.status, sr.total_tables,
         sr.successful_tables, sr.failed_tables, sr.total_records;
```

---

## Tips

1. **Replace UUIDs**: When using queries that reference `sync_run_id`, replace `'YOUR-SYNC-RUN-UUID-HERE'` with an actual UUID from the `sync_runs` table.

2. **Time Intervals**: Adjust `INTERVAL` values (e.g., `'7 days'`, `'30 days'`) based on your needs.

3. **Performance**: Add indexes if queries are slow:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_sync_table_results_status
   ON sync_metadata.sync_table_results (status);

   CREATE INDEX IF NOT EXISTS idx_sync_runs_job_name
   ON sync_metadata.sync_runs (job_name);
   ```

4. **Monitoring**: Set up scheduled queries for daily/weekly reports and alerts for failed syncs.

5. **Data Retention**: Implement a cleanup policy to prevent unbounded growth of history tables.
