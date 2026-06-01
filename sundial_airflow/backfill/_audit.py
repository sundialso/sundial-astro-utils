"""Snowflake ``BACKFILL_AUDIT`` table — DDL + per-task row inserts.

Internal. Callers pass a ready-to-use ``SnowflakeHook`` so this module
stays Airflow-free and unit-testable with ``MagicMock``.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any

AUDIT_TABLE = "BACKFILL_AUDIT"


def ensure_audit_table(hook: Any, audit_schema: str) -> None:
    """Idempotently create the audit schema + table; safe to call before every insert."""
    hook.run(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {audit_schema}.{AUDIT_TABLE} (
            run_id          VARCHAR,
            model_name      VARCHAR,
            kind            VARCHAR,
            chunk_start     DATE,
            chunk_end       DATE,
            chunk_months    INT,
            status          VARCHAR,
            airflow_run_id  VARCHAR,
            started_at      TIMESTAMP_LTZ,
            executed_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def insert_chunk_row(
    hook: Any,
    *,
    audit_schema: str,
    run_id: str,
    model_name: str,
    chunk_start: str,
    chunk_end: str,
    chunk_months: int,
    airflow_run_id: str,
    started_at: _dt.datetime,
) -> None:
    """Record a successful chunk run."""
    hook.run(
        f"""
        INSERT INTO {audit_schema}.{AUDIT_TABLE}
            (run_id, model_name, kind, chunk_start, chunk_end,
             chunk_months, status, airflow_run_id, started_at)
        VALUES
            (%(run_id)s, %(model)s, 'chunked',
             %(start)s::DATE, %(end)s::DATE, %(months)s,
             'success', %(airflow_run_id)s,
             %(started_at)s::TIMESTAMP_LTZ)
        """,
        parameters={
            "run_id": run_id,
            "model": model_name,
            "start": chunk_start,
            "end": chunk_end,
            "months": chunk_months,
            "airflow_run_id": airflow_run_id,
            "started_at": started_at.isoformat(),
        },
    )


def insert_full_refresh_row(
    hook: Any,
    *,
    audit_schema: str,
    run_id: str,
    model_name: str,
    airflow_run_id: str,
    started_at: _dt.datetime,
) -> None:
    """Record a successful full-refresh run."""
    hook.run(
        f"""
        INSERT INTO {audit_schema}.{AUDIT_TABLE}
            (run_id, model_name, kind, status, airflow_run_id, started_at)
        VALUES
            (%(run_id)s, %(model)s, 'full_refresh',
             'success', %(airflow_run_id)s,
             %(started_at)s::TIMESTAMP_LTZ)
        """,
        parameters={
            "run_id": run_id,
            "model": model_name,
            "airflow_run_id": airflow_run_id,
            "started_at": started_at.isoformat(),
        },
    )
