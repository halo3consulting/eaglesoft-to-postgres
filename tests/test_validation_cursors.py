"""Pre-sync validation must not leak prepared statements, and must fail loudly.

Background (2026-11-25): every nightly sync from 2026-11-21 to 2026-11-24 aborted in
validation with SQL Anywhere error -685, "Resource governor for 'prepared statements'
exceeded", and still exited 0, so Kubernetes reported the Jobs as Complete.

sqlanydb keeps a strong reference to every cursor on its connection, and each cursor
holds its last prepared statement until cursor.close() (or the connection closes).
Validation runs all of a job's tables on ONE connection, so one unclosed cursor per
table walks into the server's max_statement_count (default 50) once a job has enough
tables. Adding two tables (41 -> 43) crossed it.

FakeSqlAnywhere below models exactly that driver behaviour, so these tests fail against
the old code with the same error on the same two tables.
"""

import logging
from pathlib import Path

import pytest
import sqlanydb
import yaml
from click.testing import CliRunner

import sybase_postgres_sync as sps

MAX_STATEMENT_COUNT = 50  # SQL Anywhere default for the max_statement_count option
REPO_ROOT = Path(__file__).resolve().parent.parent


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.has_stmt = False
        self.rows = []

    def execute(self, sql, parameters=()):
        # sqlanydb.Cursor.new_statement: free this cursor's old statement, prepare a new one
        self.has_stmt = False
        if self.conn.open_statements() >= self.conn.limit:
            raise sqlanydb.OperationalError(
                b"Resource governor for 'prepared statements' exceeded", -685
            )
        self.has_stmt = True
        self.conn.peak = max(self.conn.peak, self.conn.open_statements())
        if "SELECT COUNT(*)" in sql:
            self.rows = [(1,)]
        elif "d.domain_name" in sql:
            self.rows = [("id", "integer", 4, 0), ("name", "varchar", 40, 0)]
        elif "index_category = 1" in sql:
            self.rows = [("id",)]
        else:
            self.rows = []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.has_stmt = False
        self.conn.cursors.discard(self)


class FakeSqlAnywhere:
    """A connection that, like sqlanydb's, keeps its cursors alive until they are closed."""

    def __init__(self, limit=MAX_STATEMENT_COUNT):
        self.limit = limit
        self.cursors = set()
        self.peak = 0

    def cursor(self):
        c = FakeCursor(self)
        self.cursors.add(c)
        return c

    def open_statements(self):
        return sum(1 for c in self.cursors if c.has_stmt)


def make_syncer(config=None):
    syncer = sps.DataSync.__new__(sps.DataSync)  # skip __init__: no config file, no Postgres
    syncer.logger = logging.getLogger("test-sync")
    syncer._primary_key_cache = {}
    syncer.config = config or {}
    return syncer


def validate_all(syncer, conn, tables):
    return [syncer.validate_table_sync(conn, t) for t in tables]


def test_validation_leaves_no_statements_open():
    syncer, conn = make_syncer(), FakeSqlAnywhere()
    # worst case per table: cdc does the extra rw_ lookup and has no configured primary key
    tables = [{"name": f"t{i}", "sync_strategy": "cdc"} for i in range(300)]

    results = validate_all(syncer, conn, tables)

    assert all(r["valid"] for r in results), [r["errors"] for r in results if not r["valid"]]
    assert conn.open_statements() == 0
    assert conn.peak <= 2  # the table's own cursor plus the primary-key lookup


def test_cursor_is_released_when_validation_returns_early_or_raises():
    syncer, conn = make_syncer(), FakeSqlAnywhere()

    class NoSuchTable(FakeCursor):
        def fetchone(self):
            return (0,)

    class Exploding(FakeCursor):
        def fetchall(self):
            raise RuntimeError("boom")

    for cursor_cls in (NoSuchTable, Exploding):
        def cursor(cls=cursor_cls):
            c = cls(conn)
            conn.cursors.add(c)
            return c

        conn.cursor = cursor
        result = syncer.validate_table_sync(conn, {"name": "t", "sync_strategy": "full"})
        assert not result["valid"]
        assert conn.open_statements() == 0


@pytest.mark.parametrize("job", ["nightly", "full_rebuild", "quick"])
def test_every_configured_job_validates_within_the_statement_limit(job):
    config = yaml.safe_load((REPO_ROOT / "sync_config.yaml").read_text())
    syncer, conn = make_syncer(config), FakeSqlAnywhere()
    conn.cursor().execute("SET TEMPORARY OPTION ...")  # the connection_init_sql statement

    tables = syncer.get_tables_for_job(job)
    results = validate_all(syncer, conn, tables)

    assert tables
    assert [r["table"] for r in results if not r["valid"]] == []


class _StubSyncer:
    def __init__(self, stats):
        self._stats = stats

    def run_sync(self, job_name=None):
        return self._stats


@pytest.mark.parametrize(
    "stats, exit_code",
    [
        ({"tables_synced": 0, "errors": [{"table": "t"}], "validation_failed": True}, 1),
        ({"tables_synced": 43, "errors": []}, 0),
    ],
)
def test_validation_failure_exits_non_zero(monkeypatch, stats, exit_code):
    monkeypatch.setattr(sps, "DataSync", lambda config: _StubSyncer(stats))

    result = CliRunner().invoke(sps.main, ["--job", "nightly"])

    assert result.exit_code == exit_code
