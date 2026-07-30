# Sybase SQL Anywhere to PostgreSQL Sync

Production-ready data synchronization tool for replicating Sybase SQL Anywhere databases to PostgreSQL for reporting purposes.

## Features

- **Multiple Sync Strategies**: Full table sync, incremental updates, or append-only
- **Parallel Processing**: Sync multiple tables concurrently
- **Change Tracking**: Maintains sync state for incremental updates
- **Automatic Schema Creation**: Creates PostgreSQL tables matching Sybase structure
- **Configurable Scheduling**: Run as daemon with periodic sync
- **Comprehensive Logging**: Colored console output and rotating file logs
- **Error Recovery**: Continues sync even if individual tables fail
- **Batch Processing**: Efficient memory usage with configurable batch sizes

## Prerequisites

- Python 3.8+
- Sybase SQL Anywhere client libraries
- PostgreSQL server (or use included Docker setup)
- Network access between source and target databases

## Installation

### Docker Installation (Recommended)

1. **Download SQL Anywhere Client**:
```bash
# Download the SQL Anywhere 17 client from SAP
wget https://d5d4ifzqzkhwt.cloudfront.net/sqla17client/sqla17_client_linux_x86x64.tar.gz

# Extract the archive to a directory named client17011
tar -xzf sqla17_client_linux_x86x64.tar.gz

# Clean up the downloaded archive (optional)
rm sqla17_client_linux_x86x64.tar.gz
```

2. **Build the Docker image**:
```bash
docker build -t eaglesoft-sync .
```

3. **Configure databases**:
```bash
cp .env.example .env
# Edit .env with your credentials
```

4. **Configure sync settings**:
   - Edit `sync_config.yaml` with your tables and sync preferences

5. **Run the container**:
```bash
docker run -v $(pwd)/sync_config.yaml:/app/sync_config.yaml \
           -v $(pwd)/.env:/app/.env \
           eaglesoft-sync
```

## Helm / Kubernetes

### Upgrading the release

The chart's `syncConfig` value defaults to commented placeholder text. The real
`sync_config.yaml` is **not** stored in `my-values.yaml` — it must be injected
at upgrade time with `--set-file`. **Always include this flag, or the
ConfigMap will be reverted to the placeholder and every CronJob will crash
with `TypeError: 'NoneType' object is not subscriptable` in
`setup_logging`.**

```bash
helm upgrade eaglesoft-sync chart/eaglesoft-sync \
  -f my-values.yaml \
  --set-file syncConfig=sync_config.yaml \
  -n eaglesoft-sync
```

Verify the ConfigMap got the real config (should show `source:` / `target:` /
`tables:`, not commented `# source:`):

```bash
kubectl get configmap eaglesoft-sync-config -n eaglesoft-sync \
  -o jsonpath='{.data.sync_config\.yaml}' | head -20
```

### Full rebuilds

The chart schedules `full_rebuild` weekly (Sunday 2 AM by default). This is
**required**, not optional: `transactions_header` and `transactions_detail`
sync with `incremental_pk` (append-only), so in-place updates to
already-synced rows (balances, insurance paid amounts) are only corrected
by a full rebuild.

**Check `my-values.yaml` before assuming rebuilds are running** — an
override there with an unreachable cron like `0 0 31 2 *` (Feb 31) makes
the CronJob manual-trigger-only and silently disables the weekly repair.

To trigger a rebuild manually:

1. Open k9s, `:cj` for the CronJobs view
2. Highlight `eaglesoft-sync-full-rebuild`
3. Press `t` — creates a one-off Job from the CronJob template
