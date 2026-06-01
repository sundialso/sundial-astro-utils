"""Snowflake ``BACKFILL_AUDIT`` writer."""
from __future__ import annotations

from typing import Any

from ._base import AUDIT_TABLE, AuditWriter


class SnowflakeAuditWriter(AuditWriter):
    """Writes ``BACKFILL_AUDIT`` via a ``SnowflakeHook``."""

    # Snowflake's connector uses pyformat and binds Python date/datetime/int
    # values natively — no inline casts needed.
    _bind_replacement = r"%(\1)s"

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        self._hook.run(self._render(sql), parameters=params or None)

    def _create_table_sql(self, audit_schema: str) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {audit_schema}.{AUDIT_TABLE} (
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
