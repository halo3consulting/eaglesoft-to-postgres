#!/usr/bin/env python3
"""
Sybase SQL Anywhere to PostgreSQL Data Sync Tool
Supports full and incremental sync strategies with configurable scheduling
"""

import os
import logging
import logging.handlers
import yaml
import sqlanydb
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import click
from tqdm import tqdm
from dotenv import load_dotenv
import colorlog
import json

# Load environment variables
load_dotenv()


class SyncState:
    """Manages sync state for incremental updates using PostgreSQL"""

    def __init__(self, pg_config: Dict, state_schema: str = "sync_metadata"):
        self.pg_config = pg_config
        self.state_schema = state_schema
        self.state_table = "sync_state"
        self._ensure_state_table()

    def _get_connection(self):
        """Get a PostgreSQL connection"""
        return psycopg2.connect(
            host=self.pg_config["host"],
            port=str(self.pg_config["port"]),
            database=self.pg_config["database"],
            user=self.pg_config["username"],
            password=self.pg_config["password"],
        )

    def _ensure_state_table(self):
        """Create sync state table and schema if they don't exist"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Create schema if it doesn't exist
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.state_schema}")

                # Create sync_state table
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.state_schema}.{self.state_table} (
                        table_name VARCHAR(255) NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        last_value TEXT,
                        last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (table_name, column_name)
                    )
                """
                )

                # Create index for faster lookups
                cursor.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_sync_state_table
                    ON {self.state_schema}.{self.state_table} (table_name)
                """
                )

                conn.commit()
        finally:
            conn.close()

    def get_last_sync_value(self, table: str, column: str) -> Optional[Any]:
        """Get the last synced value for incremental sync"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT last_value
                    FROM {self.state_schema}.{self.state_table}
                    WHERE table_name = %s AND column_name = %s
                """,
                    (table, column),
                )

                result = cursor.fetchone()
                return result[0] if result else None
        finally:
            conn.close()

    def update_sync_value(self, table: str, column: str, value: Any):
        """Update the last synced value"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.state_schema}.{self.state_table}
                        (table_name, column_name, last_value, last_sync_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (table_name, column_name)
                    DO UPDATE SET
                        last_value = EXCLUDED.last_value,
                        last_sync_time = EXCLUDED.last_sync_time
                """,
                    (table, column, str(value), datetime.now()),
                )

                conn.commit()
        finally:
            conn.close()

    def save_state(self):
        """No-op for backward compatibility - state is saved immediately in update_sync_value"""
        pass


class DataSync:
    """Main sync orchestrator"""

    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.sync_state = SyncState(self.config["target"])
        self.stats = {"tables_synced": 0, "total_records": 0, "errors": [], "table_timings": []}
        self._primary_key_cache = {}  # Cache for primary key lookups

    def load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Replace environment variables
        config["source"]["password"] = os.getenv(
            config["source"]["password"].replace("${", "").replace("}", ""),
            config["source"]["password"],
        )
        config["target"]["password"] = os.getenv(
            config["target"]["password"].replace("${", "").replace("}", ""),
            config["target"]["password"],
        )

        return config

    def get_tables_for_job(self, job_name: Optional[str] = None) -> List[Dict]:
        """
        Filter tables based on job configuration.

        Args:
            job_name: Name of the job to run (e.g., 'quick', 'full', 'reference')
                     If None, returns all tables (for backward compatibility)

        Returns:
            List of table configurations to sync
        """
        all_tables = self.config.get("tables", [])

        # If no job specified, return all tables (backward compatibility)
        if not job_name:
            return all_tables

        # Get job configuration
        jobs = self.config.get("jobs", {})
        if job_name not in jobs:
            raise ValueError(
                f"Job '{job_name}' not found in configuration. "
                f"Available jobs: {', '.join(jobs.keys())}"
            )

        job_config = jobs[job_name]
        self.logger.info(f"Running job: {job_name} - {job_config.get('description', 'No description')}")

        # Check if any filters are specified
        has_filters = any([
            "include_strategies" in job_config,
            "include_tags" in job_config,
            "include_tables" in job_config
        ])

        # If no filters specified, include all tables by default
        if not has_filters:
            filtered_tables = all_tables.copy()
        else:
            filtered_tables = []

            # Filter by strategies
            if "include_strategies" in job_config:
                strategies = job_config["include_strategies"]
                for table in all_tables:
                    if table.get("sync_strategy") in strategies:
                        filtered_tables.append(table)

            # Filter by tags
            if "include_tags" in job_config:
                required_tags = set(job_config["include_tags"])
                for table in all_tables:
                    table_tags = set(table.get("tags", []))
                    # Include table if it has ANY of the required tags
                    if table_tags & required_tags:
                        if table not in filtered_tables:
                            filtered_tables.append(table)

            # Filter by specific table names
            if "include_tables" in job_config:
                table_names = job_config["include_tables"]
                for table in all_tables:
                    if table["name"] in table_names:
                        if table not in filtered_tables:
                            filtered_tables.append(table)

        # Exclude specific tables (applies whether filtered or not)
        if "exclude_tables" in job_config:
            exclude_names = job_config["exclude_tables"]
            filtered_tables = [t for t in filtered_tables if t["name"] not in exclude_names]

        # Apply global strategy override if specified (applies to ALL tables in job)
        if "override_strategy" in job_config:
            override_strategy = job_config["override_strategy"]
            self.logger.info(f"Applying global strategy override: {override_strategy} to all tables")
            for table in filtered_tables:
                original_strategy = table.get("sync_strategy")
                table["sync_strategy"] = override_strategy
                if original_strategy != override_strategy:
                    self.logger.debug(
                        f"  {table['name']}: {original_strategy} -> {override_strategy}"
                    )

        # Apply per-table overrides if specified (takes precedence over global override)
        if "overrides" in job_config:
            overrides = job_config["overrides"]
            for table in filtered_tables:
                if table["name"] in overrides:
                    table_overrides = overrides[table["name"]]
                    # Create a new dict to avoid modifying the original config
                    table.update(table_overrides)
                    self.logger.info(
                        f"Applied specific override for {table['name']}: {table_overrides}"
                    )

        self.logger.info(f"Selected {len(filtered_tables)} tables for job '{job_name}'")
        return filtered_tables

    def setup_logging(self):
        """Configure colored logging"""
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red,bg_white",
                },
            )
        )

        self.logger = logging.getLogger("DataSync")
        self.logger.addHandler(handler)
        self.logger.setLevel(getattr(logging, self.config["logging"]["level"]))

        # File handler
        file_handler = logging.handlers.RotatingFileHandler(
            self.config["logging"]["file"],
            maxBytes=self.config["logging"]["max_bytes"],
            backupCount=self.config["logging"]["backup_count"],
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(file_handler)

    @contextmanager
    def sybase_connection(self):
        """Create Sybase connection context"""
        conn = None
        try:
            conn = sqlanydb.connect(
                host=self.config["source"]["host"],
                uid=self.config["source"]["username"],
                pwd=self.config["source"]["password"],
            )
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def postgres_connection(self):
        """Create PostgreSQL connection context"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config["target"]["host"],
                port=str(self.config["target"]["port"]),
                database=self.config["target"]["database"],
                user=self.config["target"]["username"],
                password=self.config["target"]["password"],
            )
            yield conn
        finally:
            if conn:
                conn.close()

    def validate_table_sync(self, source_conn, table_config: Dict) -> Dict[str, Any]:
        """Validate that a table can be synced successfully

        Returns:
            Dict with validation results: {
                'valid': bool,
                'table': str,
                'errors': List[str],
                'warnings': List[str]
            }
        """
        table_name = table_config["name"]
        strategy = table_config.get("sync_strategy", "full")

        errors = []
        warnings = []

        self.logger.info(f"Validating table {table_name} for {strategy} sync...")

        try:
            cursor = source_conn.cursor()

            # 1. Check if source table exists
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM SYS.SYSTAB
                WHERE table_name = '{table_name}'
            """
            )

            if cursor.fetchone()[0] == 0:
                errors.append(f"Table '{table_name}' not found in source database")
                return {
                    "valid": False,
                    "table": table_name,
                    "errors": errors,
                    "warnings": warnings,
                }

            # 2. Check data types are mappable
            cursor.execute(
                f"""
                SELECT c.column_name, d.domain_name, c.width, c.scale
                FROM SYS.SYSTABCOL c
                JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
                WHERE c.table_id = (
                    SELECT table_id FROM SYS.SYSTAB
                    WHERE table_name = '{table_name}'
                )
                ORDER BY c.column_id
            """
            )

            columns = cursor.fetchall()
            if not columns:
                errors.append(f"Table '{table_name}' has no columns")
                return {
                    "valid": False,
                    "table": table_name,
                    "errors": errors,
                    "warnings": warnings,
                }

            unmapped_types = []
            for col_name, domain_name, width, scale in columns:
                try:
                    self.map_sybase_to_postgres_type(domain_name, width, scale)
                except ValueError:
                    unmapped_types.append(f"{col_name} ({domain_name})")

            if unmapped_types:
                errors.append(
                    f"Unmapped data types in table '{table_name}': {', '.join(unmapped_types)}"
                )

            # 3. Validate CDC strategy requirements
            if strategy == "cdc":
                rw_table_name = f"rw_{table_name}"
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM SYS.SYSTAB
                    WHERE table_name = '{rw_table_name}'
                """
                )

                if cursor.fetchone()[0] == 0:
                    warnings.append(
                        f"CDC tracking table '{rw_table_name}' not found. "
                        f"Will perform initial full sync, but CDC won't work for subsequent syncs."
                    )

            # 4. Validate incremental strategy requirements
            if strategy == "incremental" and "incremental_column" not in table_config:
                errors.append(
                    f"Incremental sync requires 'incremental_column' to be specified for table '{table_name}'"
                )

            # 5. Check primary key for strategies that need it
            if strategy in ("cdc", "incremental_pk", "query_filter"):
                primary_key = table_config.get("primary_key") or self.get_primary_key(
                    source_conn, table_name
                )
                if not primary_key:
                    errors.append(
                        f"Table '{table_name}' has no primary key. Required for {strategy} sync strategy."
                    )

            # 6. Validate query_filter strategy requirements
            if strategy == "query_filter":
                if "filter_query" not in table_config:
                    errors.append(
                        f"Query filter sync requires 'filter_query' to be specified for table '{table_name}'"
                    )
                else:
                    # Validate that the query is not empty
                    filter_query = table_config["filter_query"].strip()
                    if not filter_query:
                        errors.append(f"'filter_query' cannot be empty for table '{table_name}'")

            is_valid = len(errors) == 0

            if is_valid:
                self.logger.info(f"✓ Validation passed for {table_name}")
                if warnings:
                    for warning in warnings:
                        self.logger.warning(f"⚠ {warning}")
            else:
                for error in errors:
                    self.logger.error(f"✗ {error}")

            return {
                "valid": is_valid,
                "table": table_name,
                "errors": errors,
                "warnings": warnings,
            }

        except Exception as e:
            errors.append(f"Validation error for table '{table_name}': {str(e)}")
            return {
                "valid": False,
                "table": table_name,
                "errors": errors,
                "warnings": warnings,
            }

    def get_table_columns(self, conn, table_name: str, schema: str) -> List[str]:
        """Get column names for a table"""
        if isinstance(conn, sqlanydb.Connection):
            # Sybase
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT column_name
                FROM SYS.SYSTABCOL
                WHERE table_id = (
                    SELECT table_id FROM SYS.SYSTAB
                    WHERE table_name = '{table_name}'
                )
                ORDER BY column_id
            """
            )
            return [row[0] for row in cursor.fetchall()]
        else:
            # PostgreSQL
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            )
            return [row[0] for row in cursor.fetchall()]

    def get_primary_key(self, source_conn, table_name: str) -> Optional[str]:
        """Get primary key column(s) from source table (with caching)"""
        # Check cache first
        if table_name in self._primary_key_cache:
            self.logger.debug(f"Using cached primary key for {table_name}")
            return self._primary_key_cache[table_name]

        # Query database
        cursor = source_conn.cursor()
        cursor.execute(
            f"""
            SELECT c.column_name
            FROM SYS.SYSIDX ix
            JOIN SYS.SYSIDXCOL ic ON ix.table_id = ic.table_id AND ix.index_id = ic.index_id
            JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
            WHERE ix.table_id = (
                SELECT table_id FROM SYS.SYSTAB WHERE table_name = '{table_name}'
            )
            AND ix.index_category = 1
            ORDER BY ic.sequence
        """
        )

        pk_columns = [row[0] for row in cursor.fetchall()]
        result = ", ".join(pk_columns) if pk_columns else None

        # Cache the result
        self._primary_key_cache[table_name] = result
        self.logger.debug(f"Cached primary key for {table_name}: {result}")

        return result

    def get_table_indexes(self, source_conn, table_name: str) -> List[Dict[str, Any]]:
        """Get all indexes (excluding primary key and foreign keys) from source table

        Returns:
            List of index dictionaries with:
            - name: index name
            - columns: list of column names
            - unique: whether index is unique
        """
        cursor = source_conn.cursor()

        # Get all indexes except primary keys (1) and foreign keys (2)
        # index_category: 1=PK, 2=FK, 3=Unique constraint, 4=Non-unique index
        cursor.execute(
            f"""
            SELECT DISTINCT
                ix.index_name,
                ix.index_id,
                CASE WHEN ix."unique" = 1 THEN 1 ELSE 0 END as is_unique
            FROM SYS.SYSIDX ix
            WHERE ix.table_id = (
                SELECT table_id FROM SYS.SYSTAB WHERE table_name = '{table_name}'
            )
            AND ix.index_category NOT IN (1, 2)
            ORDER BY ix.index_name
        """
        )

        indexes = []
        index_rows = cursor.fetchall()

        for index_name, index_id, is_unique in index_rows:
            # Get columns for this index
            cursor.execute(
                f"""
                SELECT c.column_name
                FROM SYS.SYSIDXCOL ic
                JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
                WHERE ic.table_id = (
                    SELECT table_id FROM SYS.SYSTAB WHERE table_name = '{table_name}'
                )
                AND ic.index_id = {index_id}
                ORDER BY ic.sequence
            """
            )

            columns = [row[0] for row in cursor.fetchall()]

            if columns:  # Only add if index has columns
                indexes.append({"name": index_name, "columns": columns, "unique": bool(is_unique)})

        self.logger.debug(f"Found {len(indexes)} indexes for table {table_name}")
        return indexes

    def create_indexes(self, source_conn, target_conn, table_config: Dict):
        """Create indexes on target table matching source table indexes"""
        table_name = table_config["name"]
        target_schema = table_config["target_schema"]

        # Get indexes from source
        indexes = self.get_table_indexes(source_conn, table_name)

        if not indexes:
            self.logger.debug(f"No additional indexes to create for {table_name}")
            return

        self.logger.info(f"Creating {len(indexes)} index(es) for {table_name}")

        with target_conn.cursor() as cursor:
            for idx in indexes:
                index_name = idx["name"]
                columns = idx["columns"]
                is_unique = idx["unique"]

                # Sanitize index name for PostgreSQL (remove special chars, lowercase)
                pg_index_name = f"{table_name}_{index_name}".lower().replace(" ", "_")

                # Build column list
                column_list = ", ".join(columns)

                # Create unique or regular index
                unique_clause = "UNIQUE" if is_unique else ""

                try:
                    create_index_sql = f"""
                        CREATE {unique_clause} INDEX IF NOT EXISTS {pg_index_name}
                        ON {target_schema}.{table_name} ({column_list})
                    """

                    cursor.execute(create_index_sql)
                    self.logger.info(
                        f"  ✓ Created {'unique ' if is_unique else ''}index {pg_index_name} "
                        f"on ({column_list})"
                    )

                except Exception as e:
                    self.logger.warning(f"  ⚠ Could not create index {pg_index_name}: {e}")

            target_conn.commit()

    def create_target_table(self, source_conn, target_conn, table_config: Dict) -> bool:
        """Create target table if it doesn't exist

        Returns:
            bool: True if table was newly created, False if it already existed
        """
        table_name = table_config["name"]
        target_schema = table_config["target_schema"]

        with target_conn.cursor() as cursor:
            # Check if table exists
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = '{target_schema}'
                    AND table_name = '{table_name}'
                )
            """
            )

            table_exists = cursor.fetchone()[0]

            if not table_exists:
                self.logger.info(f"Creating table {target_schema}.{table_name}")
                # Get source table structure
                source_cursor = source_conn.cursor()
                source_cursor.execute(
                    f"""
                    SELECT c.column_name, d.domain_name, c.width, c.scale, c.nulls
                    FROM SYS.SYSTABCOL c
                    JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
                    WHERE c.table_id = (
                        SELECT table_id FROM SYS.SYSTAB
                        WHERE table_name = '{table_name}'
                    )
                    ORDER BY c.column_id
                """
                )

                columns = []
                for col in source_cursor.fetchall():
                    col_name, domain_name, width, scale, nulls = col
                    pg_type = self.map_sybase_to_postgres_type(domain_name, width, scale)
                    null_clause = "" if nulls == "Y" else "NOT NULL"
                    columns.append(f"{col_name} {pg_type} {null_clause}")

                # Auto-detect primary key from source, or use config override
                primary_key = table_config.get("primary_key") or self.get_primary_key(
                    source_conn, table_name
                )
                if primary_key:
                    columns.append(f"PRIMARY KEY ({primary_key})")
                    self.logger.info(f"Primary key for {table_name}: {primary_key}")

                create_sql = f"""
                    CREATE TABLE {target_schema}.{table_name} (
                        {", ".join(columns)}
                    )
                """
                cursor.execute(create_sql)
                target_conn.commit()

                # Create indexes from source table
                self.create_indexes(source_conn, target_conn, table_config)

                return True  # Table was newly created
            else:
                # Table exists - ensure primary key constraint exists
                primary_key = table_config.get("primary_key") or self.get_primary_key(
                    source_conn, table_name
                )
                if primary_key:
                    # Check if primary key constraint exists
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM information_schema.table_constraints
                        WHERE table_schema = '{target_schema}'
                        AND table_name = '{table_name}'
                        AND constraint_type = 'PRIMARY KEY'
                    """
                    )

                    if cursor.fetchone()[0] == 0:
                        self.logger.info(
                            f"Adding primary key constraint to {target_schema}.{table_name}"
                        )
                        try:
                            cursor.execute(
                                f"""
                                ALTER TABLE {target_schema}.{table_name}
                                ADD PRIMARY KEY ({primary_key})
                            """
                            )
                            target_conn.commit()
                        except Exception as e:
                            self.logger.warning(f"Could not add primary key: {e}")
                            # Create unique index as fallback
                            cursor.execute(
                                f"""
                                CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_{primary_key}_unique_idx
                                ON {target_schema}.{table_name} ({primary_key})
                            """
                            )
                            target_conn.commit()

                return False  # Table already existed

    def map_sybase_to_postgres_type(
        self, sybase_type: str, width: int = None, scale: int = None
    ) -> str:
        """Map Sybase data types to PostgreSQL"""
        sybase_lower = sybase_type.lower()

        # Handle types with width/scale parameters
        if sybase_lower == "decimal":
            return f"DECIMAL({width},{scale})" if width else "DECIMAL"
        elif sybase_lower == "numeric":
            return f"NUMERIC({width},{scale})" if width else "NUMERIC"
        elif sybase_lower == "char":
            return f"CHAR({width})" if width else "CHAR"
        elif sybase_lower == "varchar" or sybase_lower == "long varchar":
            return f"VARCHAR({width})" if width else "VARCHAR"

        # Static type mappings
        type_mapping = {
            "integer": "INTEGER",
            "smallint": "SMALLINT",
            "bigint": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "text": "TEXT",
            "date": "DATE",
            "time": "TIME",
            "timestamp": "TIMESTAMP",
            "datetime": "TIMESTAMP",
            "tinyint": "SMALLINT",
            "long binary": "BYTEA",
            "bit": "SMALLINT",  # Map to SMALLINT to preserve 0/1 values
            "binary": "BYTEA",
            "varbinary": "BYTEA",
        }

        if sybase_lower not in type_mapping:
            raise ValueError(f"Unmapped Sybase type '{sybase_type}'")

        return type_mapping.get(sybase_lower, "TEXT")

    def sync_table(self, table_config: Dict) -> Dict:
        """Sync a single table"""
        table_name = table_config["name"]
        strategy = table_config.get("sync_strategy", "full")
        start_time = datetime.now()

        self.logger.info(f"Starting {strategy} sync for table {table_name}")

        try:
            with (
                self.sybase_connection() as source_conn,
                self.postgres_connection() as target_conn,
            ):
                # Ensure target table exists
                table_was_created = self.create_target_table(source_conn, target_conn, table_config)

                # If CDC/incremental strategy but table was just created, do initial full sync
                # query_filter doesn't need this since it always filters based on the query
                if table_was_created and strategy in (
                    "cdc",
                    "incremental",
                    "incremental_pk",
                ):
                    self.logger.info(
                        f"Table {table_name} newly created. Performing initial full sync "
                        f"before switching to {strategy} strategy for future syncs."
                    )
                    # Temporarily override strategy to full for this sync
                    effective_strategy = "full"
                    # Create a modified config for this sync
                    effective_config = table_config.copy()
                    effective_config["sync_strategy"] = "full"
                else:
                    effective_strategy = strategy
                    effective_config = table_config

                # Use CDC strategy if specified (and table exists)
                if effective_strategy == "cdc":
                    return self.sync_table_cdc(source_conn, target_conn, table_config)

                # Build source query
                source_query = self.build_source_query(source_conn, effective_config)

                # Read data from source
                self.logger.info(f"Reading data from {table_name}")
                source_cursor = source_conn.cursor()
                source_cursor.execute(source_query)

                # Get column names
                columns = [desc[0] for desc in source_cursor.description]

                # Process in batches
                batch_size = self.config["sync"]["batch_size"]
                total_records = 0
                last_value = None

                with target_conn.cursor() as target_cursor:
                    # Clear target table if full sync
                    # Don't truncate for query_filter as it's a selective sync
                    if effective_strategy == "full":
                        self.logger.info(f"Truncating target table {table_name}")
                        target_cursor.execute(
                            f"TRUNCATE TABLE {table_config['target_schema']}.{table_name}"
                        )

                    # Process batches
                    while True:
                        batch = source_cursor.fetchmany(batch_size)
                        if not batch:
                            break

                        # Upsert data to target
                        self.upsert_batch(source_conn, target_cursor, table_config, columns, batch)

                        total_records += len(batch)

                        # Track last value for incremental sync (only if not doing initial full sync)
                        if (
                            effective_strategy == "incremental"
                            and "incremental_column" in table_config
                        ):
                            col_index = columns.index(table_config["incremental_column"])
                            last_value = batch[-1][col_index]
                        elif effective_strategy == "incremental_pk":
                            # Track max primary key value
                            primary_key = table_config.get("primary_key") or self.get_primary_key(
                                source_conn, table_name
                            )
                            col_index = columns.index(primary_key)
                            last_value = batch[-1][col_index]

                        self.logger.debug(f"Processed {total_records} records from {table_name}")

                    target_conn.commit()

                # Update sync state (only if using incremental strategies, not initial full sync)
                if effective_strategy == "incremental" and last_value:
                    self.sync_state.update_sync_value(
                        table_name, table_config["incremental_column"], last_value
                    )
                elif effective_strategy == "incremental_pk" and last_value:
                    # Save the max primary key value
                    primary_key = table_config.get("primary_key") or self.get_primary_key(
                        source_conn, table_name
                    )
                    self.sync_state.update_sync_value(table_name, primary_key, last_value)
                    self.logger.info(f"Updated max {primary_key} to {last_value} for {table_name}")

                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"Completed sync for {table_name}: {total_records} records in {duration:.2f}s")

                return {
                    "table": table_name,
                    "records": total_records,
                    "status": "success",
                    "duration": duration,
                }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Error syncing table {table_name}: {str(e)}")
            return {
                "table": table_name,
                "records": 0,
                "status": "error",
                "error": str(e),
                "duration": duration,
            }

    def sync_table_cdc(self, source_conn, target_conn, table_config: Dict) -> Dict:
        """Sync table using CDC (Change Data Capture) strategy via rw_ tables"""
        table_name = table_config["name"]
        source_schema = table_config.get("source_schema", "dbo")
        target_schema = table_config["target_schema"]
        rw_table_name = f"rw_{table_name}"

        self.logger.info(f"Using CDC sync with change tracking table: {rw_table_name}")

        # Get primary key for lookups
        primary_key = table_config.get("primary_key") or self.get_primary_key(
            source_conn, table_name
        )
        if not primary_key:
            raise ValueError(f"Primary key required for CDC sync on table {table_name}")

        self.logger.info(f"Primary key for {table_name}: {primary_key}")

        # Track statistics
        stats = {"inserts": 0, "updates": 0, "deletes": 0, "errors": 0}
        processed_pks = []

        try:
            source_cursor = source_conn.cursor()

            # Parse primary key columns (handle composite keys)
            pk_columns = [col.strip() for col in primary_key.split(",")]
            num_pk_cols = len(pk_columns)

            # Query the rw_ table for all changes
            # The rw_ table has the actual PK column(s) from the source table, plus datetime_modified and change_type
            self.logger.info(f"Querying change records from {source_schema}.{rw_table_name}")
            source_cursor.execute(
                f"""
                SELECT {primary_key}, datetime_modified, change_type
                FROM {source_schema}.{rw_table_name}
                ORDER BY datetime_modified
            """
            )

            changes = source_cursor.fetchall()

            if not changes:
                self.logger.info(f"No changes found in {rw_table_name}")
                return {
                    "table": table_name,
                    "records": 0,
                    "status": "success",
                    "stats": stats,
                }

            self.logger.info(f"Found {len(changes)} change records to process")

            with target_conn.cursor() as target_cursor:
                for change in changes:
                    # Extract PK values (could be multiple columns for composite keys)
                    if num_pk_cols == 1:
                        pk_values = (change[0],)
                    else:
                        pk_values = change[0:num_pk_cols]

                    # datetime_modified and change_type are after PK columns
                    change_type = change[num_pk_cols + 1]

                    try:
                        if change_type in ("N", "U"):  # New or Updated
                            # Build WHERE clause for composite keys
                            where_clause = " AND ".join([f"{col} = ?" for col in pk_columns])

                            # Fetch the full record from the source table
                            source_cursor.execute(
                                f"""
                                SELECT * FROM {source_schema}.{table_name}
                                WHERE {where_clause}
                            """,
                                pk_values,
                            )

                            record = source_cursor.fetchone()

                            if record:
                                # Get column names
                                columns = [desc[0] for desc in source_cursor.description]

                                # Upsert the record
                                self.upsert_batch(
                                    source_conn,
                                    target_cursor,
                                    table_config,
                                    columns,
                                    [record],
                                )

                                if change_type == "N":
                                    stats["inserts"] += 1
                                else:
                                    stats["updates"] += 1
                            else:
                                pk_display = pk_values[0] if num_pk_cols == 1 else pk_values
                                self.logger.warning(
                                    f"Record with {primary_key}={pk_display} not found in source table"
                                )
                                stats["errors"] += 1

                        elif change_type == "D":  # Deleted
                            # Build WHERE clause for DELETE with composite keys
                            where_clause = " AND ".join([f"{col} = %s" for col in pk_columns])

                            # Delete from target
                            target_cursor.execute(
                                f"""
                                DELETE FROM {target_schema}.{table_name}
                                WHERE {where_clause}
                            """,
                                pk_values,
                            )
                            stats["deletes"] += 1

                        # Track processed record PK for cleanup
                        processed_pks.append(pk_values)

                    except Exception as e:
                        pk_display = pk_values[0] if num_pk_cols == 1 else pk_values
                        self.logger.error(
                            f"Error processing change {change_type} for {primary_key}={pk_display}: {e}"
                        )
                        stats["errors"] += 1

                # Commit all changes
                target_conn.commit()

            # Clean up processed records from rw_ table
            if processed_pks:
                self.logger.info(
                    f"Cleaning up {len(processed_pks)} processed records from {rw_table_name}"
                )

                if num_pk_cols == 1:
                    # Single column primary key - use simple IN clause
                    placeholders = ", ".join(["?"] * len(processed_pks))
                    # Flatten the list of tuples to a list of values
                    pk_values_flat = [pk[0] for pk in processed_pks]
                    source_cursor.execute(
                        f"""
                        DELETE FROM {source_schema}.{rw_table_name}
                        WHERE {primary_key} IN ({placeholders})
                    """,
                        pk_values_flat,
                    )
                else:
                    # Composite primary key - build WHERE clause with OR conditions
                    # WHERE (col1 = ? AND col2 = ?) OR (col1 = ? AND col2 = ?) ...
                    single_condition = " AND ".join([f"{col} = ?" for col in pk_columns])
                    or_conditions = " OR ".join([f"({single_condition})" for _ in processed_pks])

                    # Flatten pk_values tuples into a single list
                    all_values = []
                    for pk_tuple in processed_pks:
                        all_values.extend(pk_tuple)

                    source_cursor.execute(
                        f"""
                        DELETE FROM {source_schema}.{rw_table_name}
                        WHERE {or_conditions}
                    """,
                        all_values,
                    )

                source_conn.commit()
                self.logger.info(f"Cleaned up {len(processed_pks)} records from {rw_table_name}")

            total_records = stats["inserts"] + stats["updates"] + stats["deletes"]
            self.logger.info(
                f"CDC sync completed for {table_name}: "
                f"{stats['inserts']} inserts, {stats['updates']} updates, "
                f"{stats['deletes']} deletes, {stats['errors']} errors"
            )

            return {
                "table": table_name,
                "records": total_records,
                "status": "success",
                "stats": stats,
            }

        except Exception as e:
            self.logger.error(f"Error in CDC sync for {table_name}: {str(e)}")
            return {
                "table": table_name,
                "records": 0,
                "status": "error",
                "error": str(e),
                "stats": stats,
            }

    def build_source_query(self, source_conn, table_config: Dict) -> str:
        """Build source query based on sync strategy"""
        table_name = table_config["name"]
        source_schema = table_config.get("source_schema", "dbo")
        strategy = table_config.get("sync_strategy", "full")

        base_query = f"SELECT * FROM {source_schema}.{table_name}"

        if strategy == "incremental" and "incremental_column" in table_config:
            last_value = self.sync_state.get_last_sync_value(
                table_name, table_config["incremental_column"]
            )

            if last_value:
                incremental_col = table_config["incremental_column"]
                base_query += f" WHERE {incremental_col} > '{last_value}'"
                base_query += f" ORDER BY {incremental_col}"

        elif strategy == "incremental_pk":
            # Use primary key for incremental sync
            primary_key = table_config.get("primary_key") or self.get_primary_key(
                source_conn, table_name
            )
            if not primary_key:
                raise ValueError(
                    f"Primary key required for incremental_pk sync on table {table_name}"
                )

            # Get last synced primary key value
            last_pk_value = self.sync_state.get_last_sync_value(table_name, primary_key)

            if last_pk_value:
                # Assuming single-column numeric primary key for simplicity
                # For composite keys, this would need to be more complex
                base_query += f" WHERE {primary_key} > {last_pk_value}"

            base_query += f" ORDER BY {primary_key}"

        elif strategy == "query_filter":
            # Use custom query to filter which records to sync
            primary_key = table_config.get("primary_key") or self.get_primary_key(
                source_conn, table_name
            )
            if not primary_key:
                raise ValueError(
                    f"Primary key required for query_filter sync on table {table_name}"
                )

            filter_query = table_config.get("filter_query", "").strip()
            if not filter_query:
                raise ValueError(
                    f"'filter_query' is required for query_filter sync on table {table_name}"
                )

            # Build query using the filter query as a subquery
            # This allows only records matching the filter query results to be synced
            self.logger.info(f"Using filter query for {table_name}: {filter_query}")

            # Check if primary key is composite (contains comma)
            pk_columns = [col.strip() for col in primary_key.split(",")]

            if len(pk_columns) == 1:
                # Single column primary key - simple IN clause
                base_query = f"""
                    SELECT * FROM {source_schema}.{table_name}
                    WHERE {primary_key} IN ({filter_query})
                """
            else:
                # Composite primary key - use EXISTS with subquery
                # The filter_query must return all PK columns for this to work
                # Build the join condition for all PK columns
                join_conditions = " AND ".join(
                    [
                        f"{source_schema}.{table_name}.{col} = filter_results.{col}"
                        for col in pk_columns
                    ]
                )

                base_query = f"""
                    SELECT * FROM {source_schema}.{table_name}
                    WHERE EXISTS (
                        SELECT 1 FROM ({filter_query}) AS filter_results
                        WHERE {join_conditions}
                    )
                """

        return base_query

    def upsert_batch(
        self, source_conn, cursor, table_config: Dict, columns: List[str], batch: List
    ):
        """Upsert batch of records to PostgreSQL"""
        table_name = table_config["name"]
        target_schema = table_config["target_schema"]
        # Auto-detect primary key from source, or use config override
        primary_key = table_config.get("primary_key") or self.get_primary_key(
            source_conn, table_name
        )

        # Convert batch to list of tuples
        values = [tuple(row) for row in batch]

        # Build insert query
        columns_str = ", ".join(columns)

        if primary_key and table_config.get("sync_strategy") != "append":
            # Upsert with conflict resolution
            # Parse primary key columns (handle composite keys)
            pk_columns = [col.strip() for col in primary_key.split(",")]

            # Exclude primary key columns from update
            update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in pk_columns]

            # execute_values requires %s as a template placeholder
            query = f"""
                INSERT INTO {target_schema}.{table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({primary_key}) DO UPDATE SET
                {", ".join(update_cols)}
            """
        else:
            # Simple insert
            query = f"""
                INSERT INTO {target_schema}.{table_name} ({columns_str})
                VALUES %s
            """

        # Execute batch insert using execute_values
        execute_values(cursor, query, values)

    def run_sync(self, job_name: Optional[str] = None):
        """
        Run the complete sync process

        Args:
            job_name: Optional job name to filter which tables to sync
                     (e.g., 'quick', 'full', 'reference')
        """
        self.logger.info("Starting data sync process")
        start_time = datetime.now()

        # Get tables for the specified job (or all tables if no job specified)
        tables = self.get_tables_for_job(job_name)
        max_workers = self.config["sync"].get("parallel_tables", 1)

        # Validate all tables before starting sync
        self.logger.info(f"Validating {len(tables)} tables before sync...")
        validation_failed = False

        with self.sybase_connection() as source_conn:
            for table_config in tables:
                validation_result = self.validate_table_sync(source_conn, table_config)

                if not validation_result["valid"]:
                    validation_failed = True
                    self.stats["errors"].append(
                        {
                            "table": validation_result["table"],
                            "status": "validation_failed",
                            "error": "; ".join(validation_result["errors"]),
                            "records": 0,
                        }
                    )

        if validation_failed:
            self.logger.error("Validation failed for one or more tables. Aborting sync.")
            self.logger.error("Fix the validation errors and try again.")

            # Log summary of validation errors
            self.logger.error(f"\nValidation errors found in {len(self.stats['errors'])} table(s):")
            for error in self.stats["errors"]:
                self.logger.error(f"  - {error['table']}: {error['error']}")

            return self.stats

        self.logger.info("✓ All tables validated successfully. Starting sync...\n")

        # Run sync with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.sync_table, table): table for table in tables}

            with tqdm(total=len(tables), desc="Syncing tables") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)

                    if result["status"] == "success":
                        self.stats["tables_synced"] += 1
                        self.stats["total_records"] += result["records"]
                        self.stats["table_timings"].append({
                            "table": result["table"],
                            "duration": result.get("duration", 0),
                            "records": result["records"]
                        })
                    else:
                        self.stats["errors"].append(result)

        # Log summary
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(
            f"""
Sync completed in {duration:.2f} seconds:
- Tables synced: {self.stats["tables_synced"]}/{len(tables)}
- Total records: {self.stats["total_records"]:,}
- Errors: {len(self.stats["errors"])}
        """
        )

        # Log per-table timing summary
        if self.stats["table_timings"]:
            self.logger.info("\nPer-table sync times:")
            # Sort by duration (longest first)
            sorted_timings = sorted(
                self.stats["table_timings"],
                key=lambda x: x["duration"],
                reverse=True
            )
            for timing in sorted_timings:
                self.logger.info(
                    f"  - {timing['table']}: {timing['duration']:.2f}s ({timing['records']:,} records)"
                )

        if self.stats["errors"]:
            self.logger.error("\nFailed tables:")
            for error in self.stats["errors"]:
                self.logger.error(f"  - {error['table']}: {error.get('error', 'Unknown error')}")

        return self.stats


@click.command()
@click.option("--config", "-c", default="sync_config.yaml", help="Configuration file path")
@click.option("--job", "-j", help="Job name to run (e.g., 'quick', 'full', 'reference')")
@click.option("--table", "-t", help="Sync specific table only")
def main(config, job, table):
    """Sybase to PostgreSQL Data Sync Tool"""

    syncer = DataSync(config)

    if table:
        # Sync single table
        table_config = next(
            (t for t in syncer.config.get("tables", []) if t["name"] == table), None
        )
        if table_config:
            result = syncer.sync_table(table_config)
            print(json.dumps(result, indent=2))
        else:
            print(f"Table {table} not found in configuration")
    else:
        # Run sync with optional job filter
        syncer.run_sync(job_name=job)


if __name__ == "__main__":
    main()
