"""Unit tests for ``sundial_airflow.backfill._audit``."""
from __future__ import annotations

import datetime as _dt
from unittest.mock import MagicMock

from sundial_airflow.backfill import _audit


def test_ensure_audit_table_issues_create_schema_and_table():
    hook = MagicMock()
    _audit.ensure_audit_table(hook, "MY_SCHEMA")
    sqls = [call.args[0] for call in hook.run.call_args_list]
    assert any("CREATE SCHEMA IF NOT EXISTS MY_SCHEMA" in s for s in sqls)
    assert any(
        f"CREATE TABLE IF NOT EXISTS MY_SCHEMA.{_audit.AUDIT_TABLE}" in s for s in sqls
    )


def test_insert_chunk_row_passes_parameters():
    hook = MagicMock()
    started = _dt.datetime(2026, 1, 15, 9, 30, tzinfo=_dt.timezone.utc)
    _audit.insert_chunk_row(
        hook,
        audit_schema="S",
        run_id="run-1",
        model_name="m",
        chunk_start="2024-01-01",
        chunk_end="2024-04-01",
        chunk_months=3,
        airflow_run_id="airflow-1",
        started_at=started,
    )
    assert hook.run.call_count == 1
    call = hook.run.call_args
    sql, params = call.args[0], call.kwargs["parameters"]
    assert "INSERT INTO S.BACKFILL_AUDIT" in sql
    assert "'chunked'" in sql
    assert params == {
        "run_id": "run-1",
        "model": "m",
        "start": "2024-01-01",
        "end": "2024-04-01",
        "months": 3,
        "airflow_run_id": "airflow-1",
        "started_at": started.isoformat(),
    }


def test_insert_full_refresh_row_passes_parameters():
    hook = MagicMock()
    started = _dt.datetime(2026, 2, 1, 0, 0, tzinfo=_dt.timezone.utc)
    _audit.insert_full_refresh_row(
        hook,
        audit_schema="S",
        run_id="r",
        model_name="m",
        airflow_run_id="a",
        started_at=started,
    )
    sql = hook.run.call_args.args[0]
    params = hook.run.call_args.kwargs["parameters"]
    assert "INSERT INTO S.BACKFILL_AUDIT" in sql
    assert "'full_refresh'" in sql
    assert params == {
        "run_id": "r",
        "model": "m",
        "airflow_run_id": "a",
        "started_at": started.isoformat(),
    }
