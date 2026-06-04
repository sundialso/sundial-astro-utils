"""``AuditWriter`` base class: shared ``BACKFILL_AUDIT`` orchestration.

Subclasses supply only what differs per warehouse — how to execute a
statement (``_execute``) and the ``CREATE TABLE`` dialect (``_create_table_sql``).
Writers wrap a hook and stay Airflow-free, so they unit-test with ``MagicMock``.
"""
from __future__ import annotations

import datetime as _dt
import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar

AUDIT_TABLE = "BACKFILL_AUDIT"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BIND_RE = re.compile(r":(\w+)")


def validate_schema(audit_schema: str) -> None:
    """Reject schema names that aren't bare SQL identifiers (injection guard)."""
    if not isinstance(audit_schema, str) or not _IDENT_RE.match(audit_schema):
        raise ValueError(
            f"Invalid audit_schema {audit_schema!r}: must match {_IDENT_RE.pattern!r}."
        )


class AuditWriter(ABC):
    """Builds and writes ``BACKFILL_AUDIT`` rows for a single warehouse.

    Statements use ``:name`` placeholders and portable identifiers; each
    subclass's ``_execute`` binds the params in its native style.
    """

    #: ``:name`` replacement for this warehouse's bind style (set by subclasses).
    _bind_replacement: ClassVar[str]

    def __init__(self, hook: Any) -> None:
        self._hook = hook

    def _render(self, sql: str) -> str:
        """Translate ``:name`` placeholders into the warehouse's bind style."""
        return _BIND_RE.sub(self._bind_replacement, sql)

    def _audit_table_ref(self, audit_schema: str) -> str:
        """Warehouse-specific ``schema.table`` (or fully qualified) ref."""
        return f"{audit_schema}.{AUDIT_TABLE}"

    def ensure_audit_table(self, audit_schema: str) -> None:
        """Create the audit schema/dataset + table if absent."""
        validate_schema(audit_schema)
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")
        self._execute(self._create_table_sql(audit_schema))

    def insert_chunk_row(
        self,
        *,
        audit_schema: str,
        model_name: str,
        start_ts: str,
        end_ts: str,
        run_id: str,
        started_at: _dt.datetime,
    ) -> None:
        """Record one successful chunk run (``execution_ts`` left NULL)."""
        validate_schema(audit_schema)
        self._execute(
            f"""
            INSERT INTO {self._audit_table_ref(audit_schema)}
                (model_name, kind, start_ts, end_ts,
                 status, run_id, started_at)
            VALUES
                (:model, 'chunked', :start, :end,
                 'success', :run_id, :started_at)
            """,
            {
                "model": model_name,
                "start": _dt.date.fromisoformat(start_ts),
                "end": _dt.date.fromisoformat(end_ts),
                "run_id": run_id,
                "started_at": started_at,
            },
        )

    def insert_full_refresh_row(
        self,
        *,
        audit_schema: str,
        model_name: str,
        run_id: str,
        started_at: _dt.datetime,
    ) -> None:
        """Record one successful non-chunked run (``start_ts``/``end_ts`` left NULL)."""
        validate_schema(audit_schema)
        self._execute(
            f"""
            INSERT INTO {self._audit_table_ref(audit_schema)}
                (model_name, kind, status, run_id, started_at)
            VALUES
                (:model, 'non_chunked', 'success', :run_id, :started_at)
            """,
            {
                "model": model_name,
                "run_id": run_id,
                "started_at": started_at,
            },
        )

    @abstractmethod
    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Run one statement, binding ``:name`` params in the warehouse's style."""

    @abstractmethod
    def _create_table_sql(self, audit_schema: str) -> str:
        """Return the ``CREATE TABLE IF NOT EXISTS`` for ``BACKFILL_AUDIT``."""
