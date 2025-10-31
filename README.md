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
