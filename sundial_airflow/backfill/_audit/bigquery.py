"""BigQuery ``BACKFILL_AUDIT`` writer."""
from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import Any

from ._base import AUDIT_TABLE, AuditWriter, validate_schema

logger = logging.getLogger(__name__)

_PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


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

    ``audit_schema`` is the dataset name. ``project_id`` should match the GCP
    project where the backfill dataset lives (often the same as dbt's
    ``+database`` / profile project, not only the connection default).

    By default ``create_dataset=False`` so tenants without
    ``bigquery.datasets.create`` can still audit as long as the dataset
    already exists (typically created by the first successful dbt chunk).
    """

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

    def _resolve_project_id(self) -> str:
        project = self._project_id or self._hook.project_id
        if not project:
            raise ValueError(
                "BigQuery audit writer has no project_id: set bq_audit_project "
                "on make_backfill_dag or configure project on the GCP connection."
            )
        return project

    def _dataset_ref(self, audit_schema: str) -> str:
        """Fully qualified dataset: ``project.dataset``."""
        return f"{self._resolve_project_id()}.{audit_schema}"

    def _table_ref(self, audit_schema: str) -> str:
        """Fully qualified table for DDL/DML."""
        return f"`{self._dataset_ref(audit_schema)}.{AUDIT_TABLE}`"

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

        project_id = self._resolve_project_id()
        rendered = self._render(sql)
        location = getattr(self._hook, "location", None)
        logger.info(
            "BACKFILL_AUDIT BigQuery job [project=%s location=%s]: %s",
            project_id,
            location,
            rendered.strip(),
        )
        if params:
            logger.info("BACKFILL_AUDIT BigQuery params: %s", params)

        query_params = [
            ScalarQueryParameter(name, _bq_type(value), value)
            for name, value in (params or {}).items()
        ]
        client = self._hook.get_client(project_id=project_id)
        client.query(
            rendered,
            job_config=QueryJobConfig(query_parameters=query_params),
        ).result()

    def ensure_audit_table(self, audit_schema: str) -> None:
        """Create the audit table if absent (optionally the dataset too)."""
        validate_schema(audit_schema)
        project_id = self._resolve_project_id()
        dataset_ref = self._dataset_ref(audit_schema)
        table_ref = self._table_ref(audit_schema)

        logger.info(
            "BACKFILL_AUDIT setup: project=%s dataset=%s table=%s "
            "create_dataset=%s hook.project_id=%s",
            project_id,
            audit_schema,
            AUDIT_TABLE,
            self._create_dataset,
            getattr(self._hook, "project_id", None),
        )

        if self._create_dataset:
            ddl = f"CREATE SCHEMA IF NOT EXISTS `{dataset_ref}`"
            logger.info(
                "BACKFILL_AUDIT creating dataset if missing (requires "
                "bigquery.datasets.create on %s): %s",
                project_id,
                ddl,
            )
            self._execute(ddl)
        else:
            logger.info(
                "BACKFILL_AUDIT skipping CREATE SCHEMA; dataset must already "
                "exist in project %s: %s",
                project_id,
                dataset_ref,
            )

        ddl = self._create_table_sql(audit_schema)
        logger.info("BACKFILL_AUDIT creating table if missing: %s", table_ref)
        self._execute(ddl)

    def _create_table_sql(self, audit_schema: str) -> str:
        table_ref = self._table_ref(audit_schema)
        return f"""
            CREATE TABLE IF NOT EXISTS {table_ref} (
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

    def _audit_table_ref(self, audit_schema: str) -> str:
        return self._table_ref(audit_schema)
