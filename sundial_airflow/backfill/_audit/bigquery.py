"""BigQuery ``BACKFILL_AUDIT`` writer."""
from __future__ import annotations

import datetime as _dt
from typing import Any

from ._base import AUDIT_TABLE, AuditWriter


def _bq_type(value: Any) -> str:
    """Map a Python value to its BigQuery scalar-parameter type."""
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, _dt.datetime):
        return "TIMESTAMP"
    if isinstance(value, _dt.date):
        return "DATE"
    return "STRING"


class BigQueryAuditWriter(AuditWriter):
    """Writes ``BACKFILL_AUDIT`` via a ``BigQueryHook``.

    ``audit_schema`` is the dataset; its location must match the region the
    backfill job runs in.
    """

    _bind_replacement = r"@\1"

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

        query_params = [
            ScalarQueryParameter(name, _bq_type(value), value)
            for name, value in (params or {}).items()
        ]
        client = self._hook.get_client(project_id=self._hook.project_id)
        client.query(
            self._render(sql),
            job_config=QueryJobConfig(query_parameters=query_params),
        ).result()

    def _create_table_sql(self, audit_schema: str) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {audit_schema}.{AUDIT_TABLE} (
                model_name      STRING,
                kind            STRING,
                start_ts        DATE,
                end_ts          DATE,
                execution_ts    TIMESTAMP,
                status          STRING,
                run_id          STRING,
                started_at      TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """
