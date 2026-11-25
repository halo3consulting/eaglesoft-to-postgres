"""Any failed sync exits non-zero, and opted-in jobs push a Pushover alert.

Before this, a run where validation passed but some tables failed still exited 0,
and so did a failed --table run, so Kubernetes reported the Job as Complete and
nothing paged. The 2026-11-21..24 validation aborts went unnoticed for four nights
until the warehouse's 110h source-freshness gate tripped.
"""

import urllib.parse

import pytest
from click.testing import CliRunner

import sybase_postgres_sync as sps


class _SyncState:
    def __init__(self):
        self.completed = []

    def create_sync_run(self, job_name=None):
        return "run-1"

    def complete_sync_run(self, **kwargs):
        self.completed.append(kwargs)


class _StubSyncer:
    def __init__(self, stats=None, table_result=None, crash=None):
        self._stats = stats
        self._table_result = table_result
        self._crash = crash
        self.config = {"tables": [{"name": "eod", "sync_strategy": "full"}]}
        self.sync_state = _SyncState()

    def run_sync(self, job_name=None):
        if self._crash:
            raise self._crash
        return self._stats

    def sync_table(self, table_config, sync_run_id=None):
        return self._table_result


@pytest.fixture
def alerts(monkeypatch):
    sent = []
    monkeypatch.setattr(sps, "send_failure_alert", lambda title, msg: sent.append((title, msg)))
    return sent


def run_cli(monkeypatch, syncer, args):
    monkeypatch.setattr(sps, "DataSync", lambda config: syncer)
    return CliRunner().invoke(sps.main, args)


def test_partial_table_failure_exits_1_and_alerts(monkeypatch, alerts):
    stats = {
        "tables_synced": 41,
        "total_records": 10,
        "errors": [
            {"table": "eod", "status": "error", "error": "connection reset"},
            {"table": "eom", "status": "error", "error": "timeout"},
        ],
    }
    result = run_cli(monkeypatch, _StubSyncer(stats), ["--job", "nightly"])

    assert result.exit_code == 1
    [(title, msg)] = alerts
    assert title == "Eaglesoft sync [nightly] FAILED"
    assert "2 table(s) failed; 41 synced." in msg
    assert "- eod: connection reset" in msg and "- eom: timeout" in msg


def test_validation_abort_alert_says_nothing_synced(monkeypatch, alerts):
    stats = {
        "tables_synced": 0,
        "validation_failed": True,
        "errors": [{"table": "image_templates", "error": "-685"}],
    }
    result = run_cli(monkeypatch, _StubSyncer(stats), ["--job", "nightly"])

    assert result.exit_code == 1
    assert "nothing was synced" in alerts[0][1]


def test_clean_run_exits_0_without_alert(monkeypatch, alerts):
    result = run_cli(monkeypatch, _StubSyncer({"tables_synced": 43, "errors": []}), ["--job", "nightly"])

    assert result.exit_code == 0
    assert alerts == []


def test_crash_alerts_and_still_fails(monkeypatch, alerts):
    syncer = _StubSyncer(crash=RuntimeError("Database server not found (-100)"))
    result = run_cli(monkeypatch, syncer, ["--job", "nightly"])

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert alerts == [("Eaglesoft sync [nightly] CRASHED", "RuntimeError: Database server not found (-100)")]


@pytest.mark.parametrize(
    "table_result, exit_code, alerted",
    [
        ({"table": "eod", "status": "error", "error": "boom", "records": 0}, 1, True),
        ({"table": "eod", "status": "success", "records": 5}, 0, False),
    ],
)
def test_single_table_exit_code(monkeypatch, alerts, table_result, exit_code, alerted):
    syncer = _StubSyncer(table_result=table_result)
    result = run_cli(monkeypatch, syncer, ["--table", "eod"])

    assert result.exit_code == exit_code
    assert bool(alerts) is alerted
    if alerted:
        assert alerts[0] == ("Eaglesoft sync [single-table:eod] FAILED", "eod: boom")
    assert syncer.sync_state.completed[0]["failed_tables"] == (1 if alerted else 0)


def test_unknown_table_exits_2_without_alert(monkeypatch, alerts):
    result = run_cli(monkeypatch, _StubSyncer(), ["--table", "nope"])

    assert result.exit_code == 2
    assert alerts == []


def test_describe_failures_caps_the_list():
    errors = [{"table": f"t{i}", "error": "x" * 500} for i in range(13)]
    msg = sps.describe_failures({"tables_synced": 30, "errors": errors})

    lines = msg.splitlines()
    assert lines[0] == "13 table(s) failed; 30 synced."
    assert len(lines) == 1 + sps.MAX_LISTED_FAILURES + 1
    assert lines[-1] == "... and 3 more"
    assert all(len(line) <= 210 for line in lines)


# --- send_failure_alert -------------------------------------------------------


class _Resp:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b'{"status":1}'


@pytest.fixture
def posts(monkeypatch):
    calls = []

    def fake_urlopen(url, data=None, timeout=None):
        calls.append((url, dict(urllib.parse.parse_qsl(data.decode())), timeout))
        return _Resp()

    monkeypatch.setattr(sps.urllib.request, "urlopen", fake_urlopen)
    for var in (sps.ALERT_ENV, "PUSHOVER_TOKEN", "PUSHOVER_USER", "HOSTNAME"):
        monkeypatch.delenv(var, raising=False)
    return calls


def test_alert_is_off_unless_the_job_opts_in(monkeypatch, posts):
    monkeypatch.setenv("PUSHOVER_TOKEN", "tok")
    monkeypatch.setenv("PUSHOVER_USER", "usr")

    assert sps.send_failure_alert("t", "m") is False
    assert posts == []


def test_alert_warns_when_opted_in_without_credentials(monkeypatch, posts, capsys):
    monkeypatch.setenv(sps.ALERT_ENV, "true")

    assert sps.send_failure_alert("t", "m") is False
    assert posts == []
    assert "PUSHOVER_TOKEN" in capsys.readouterr().err


def test_alert_posts_to_pushover(monkeypatch, posts):
    monkeypatch.setenv(sps.ALERT_ENV, "true")
    monkeypatch.setenv("PUSHOVER_TOKEN", "tok")
    monkeypatch.setenv("PUSHOVER_USER", "usr")
    monkeypatch.setenv("HOSTNAME", "eaglesoft-sync-nightly-123-abc")

    assert sps.send_failure_alert("Eaglesoft sync [nightly] FAILED", "x" * 2000) is True
    [(url, form, timeout)] = posts
    assert url == sps.PUSHOVER_URL
    assert form["token"] == "tok" and form["user"] == "usr"
    assert form["title"] == "Eaglesoft sync [nightly] FAILED"
    assert form["priority"] == "1"
    assert len(form["message"]) == 1024
    assert timeout == 20


def test_alert_failure_never_raises(monkeypatch, posts, capsys):
    monkeypatch.setenv(sps.ALERT_ENV, "1")
    monkeypatch.setenv("PUSHOVER_TOKEN", "tok")
    monkeypatch.setenv("PUSHOVER_USER", "usr")

    def down(*a, **k):
        raise OSError("network unreachable")

    monkeypatch.setattr(sps.urllib.request, "urlopen", down)

    assert sps.send_failure_alert("t", "m") is False
    assert "network unreachable" in capsys.readouterr().err


def test_pod_name_is_appended_to_the_message(monkeypatch, posts):
    monkeypatch.setenv(sps.ALERT_ENV, "yes")
    monkeypatch.setenv("PUSHOVER_TOKEN", "tok")
    monkeypatch.setenv("PUSHOVER_USER", "usr")
    monkeypatch.setenv("HOSTNAME", "pod-1")

    sps.send_failure_alert("t", "eod: boom")
    assert posts[0][1]["message"] == "eod: boom\nPod: pod-1"
