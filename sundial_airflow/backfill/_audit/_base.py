"""Shared BACKFILL_AUDIT insert orchestration."""
from __future__ import annotations

import datetime as _dt
import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BIND_RE = re.compile(r":(\w+)")


def validate_schema(audit_schema: str) -> None:
    """Reject audit schema names that are not bare SQL identifiers."""
    if not isinstance(audit_schema, str) or not _IDENT_RE.match(audit_schema):
        raise ValueError(
            f"Invalid audit_schema {audit_schema!r}: must match {_IDENT_RE.pattern!r}."
        )


class AuditWriter(ABC):
    """Writes BACKFILL_AUDIT rows using warehouse-specific SQL execution."""

    AUDIT_TABLE: ClassVar[str]
    _bind_replacement: ClassVar[str]

    def __init__(self, hook: Any) -> None:
        self._hook = hook

    def ensure_audit_table(self, audit_schema: str) -> None:
        """Create the audit schema and table when missing."""
        validate_schema(audit_schema)
        self._execute(self._create_schema_sql(audit_schema))
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
        """Insert one successful chunked run."""
        validate_schema(audit_schema)
        table = self._audit_table_ref(audit_schema)
        self._execute(
            f"""
            INSERT INTO {table}
                (model_name, kind, start_ts, end_ts, status, run_id, started_at)
            VALUES
                (:model, 'chunked', :start, :end, 'success', :run_id, :started_at)
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
        """Insert one successful full-refresh run."""
        validate_schema(audit_schema)
        table = self._audit_table_ref(audit_schema)
        self._execute(
            f"""
            INSERT INTO {table}
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

    def _render(self, sql: str) -> str:
        """Convert :name placeholders to the warehouse bind style."""
        return _BIND_RE.sub(self._bind_replacement, sql)

    @abstractmethod
    def _audit_table_ref(self, audit_schema: str) -> str:
        """Return the qualified audit table reference for DML/DDL."""

    @abstractmethod
    def _create_schema_sql(self, audit_schema: str) -> str:
        """Return CREATE SCHEMA SQL for the audit schema."""

    @abstractmethod
    def _create_table_sql(self, audit_schema: str) -> str:
        """Return CREATE TABLE SQL for BACKFILL_AUDIT."""

    @abstractmethod
    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Run one SQL statement."""
