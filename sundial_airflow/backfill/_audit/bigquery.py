"""BigQuery BACKFILL_AUDIT writer."""
from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import Any

from ._base import AuditWriter, validate_schema

logger = logging.getLogger(__name__)

_PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _bq_param_type(value: Any) -> str:
    """Map a Python value to a BigQuery scalar parameter type."""
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
    """Execute BACKFILL_AUDIT DDL/DML through a BigQueryHook."""

    AUDIT_TABLE = "backfill_audit"
    _bind_replacement = r"@\1"

    def __init__(
        self,
        hook: Any,
        *,
        project_id: str | None = None,
        create_dataset: bool = False,
    ) -> None:
        super().__init__(hook)
        if project_id is not None and not _PROJECT_RE.match(project_id):
            raise ValueError(
                f"Invalid BigQuery audit project_id {project_id!r}: "
                f"must match {_PROJECT_RE.pattern!r}."
            )
        self._project_id = project_id
        self._create_dataset = create_dataset

    def ensure_audit_table(self, audit_schema: str) -> None:
        """Create the audit table, optionally creating the dataset first."""
        validate_schema(audit_schema)
        if self._create_dataset:
            self._execute(self._create_schema_sql(audit_schema))
        self._execute(self._create_table_sql(audit_schema))

    def _audit_table_ref(self, audit_schema: str) -> str:
        return f"`{self._dataset_ref(audit_schema)}.{self.AUDIT_TABLE}`"

    def _create_schema_sql(self, audit_schema: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS `{self._dataset_ref(audit_schema)}`"

    def _create_table_sql(self, audit_schema: str) -> str:
        table = self._audit_table_ref(audit_schema)
        return f"""
            CREATE TABLE IF NOT EXISTS {table} (
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

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

        project_id = self._resolve_project_id()
        rendered = self._render(sql)
        query_params = [
            ScalarQueryParameter(name, _bq_param_type(value), value)
            for name, value in (params or {}).items()
        ]
        logger.info(
            "BACKFILL_AUDIT BigQuery [project=%s]: %s",
            project_id,
            rendered.strip(),
        )
        client = self._hook.get_client(project_id=project_id)
        client.query(
            rendered,
            job_config=QueryJobConfig(query_parameters=query_params),
        ).result()

    def _resolve_project_id(self) -> str:
        project = self._project_id or self._hook.project_id
        if not project:
            raise ValueError(
                "BigQuery audit writer has no project_id: set bq_audit_project "
                "on make_backfill_dag or configure project on the GCP connection."
            )
        return project

    def _dataset_ref(self, audit_schema: str) -> str:
        """Return a lowercase ``project.dataset`` reference."""
        dataset = audit_schema.lower()
        return f"{self._resolve_project_id()}.{dataset}"
