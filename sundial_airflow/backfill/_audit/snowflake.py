"""Snowflake BACKFILL_AUDIT writer."""
from __future__ import annotations

from typing import Any

from ._base import AuditWriter


class SnowflakeAuditWriter(AuditWriter):
    """Execute BACKFILL_AUDIT DDL/DML through a SnowflakeHook."""

    AUDIT_TABLE = "BACKFILL_AUDIT"
    _bind_replacement = r"%(\1)s"

    def _audit_table_ref(self, audit_schema: str) -> str:
        return f"{audit_schema.upper()}.{self.AUDIT_TABLE}"

    def _create_schema_sql(self, audit_schema: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {audit_schema.upper()}"

    def _create_table_sql(self, audit_schema: str) -> str:
        table = self._audit_table_ref(audit_schema)
        return f"""
            CREATE TABLE IF NOT EXISTS {table} (
                model_name      VARCHAR,
                kind            VARCHAR,
                start_ts        DATE,
                end_ts          DATE,
                execution_ts    TIMESTAMP_LTZ,
                status          VARCHAR,
                run_id          VARCHAR,
                started_at      TIMESTAMP_LTZ,
                updated_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        self._hook.run(self._render(sql), parameters=params or None)
