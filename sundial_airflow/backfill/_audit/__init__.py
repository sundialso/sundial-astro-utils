"""BACKFILL_AUDIT writers and factory."""
from __future__ import annotations

from typing import Any

from ._base import AuditWriter, validate_schema
from .bigquery import BigQueryAuditWriter
from .snowflake import SnowflakeAuditWriter

__all__ = [
    "AuditWriter",
    "BigQueryAuditWriter",
    "SnowflakeAuditWriter",
    "create_audit_writer",
    "validate_schema",
]


def create_audit_writer(
    warehouse: str,
    *,
    warehouse_conn_id: str,
    bq_location: str | None = None,
    bq_audit_project: str | None = None,
    bq_audit_create_dataset: bool = False,
) -> AuditWriter:
    """Build the audit writer for the requested warehouse."""
    if warehouse == "bigquery":
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        return BigQueryAuditWriter(
            BigQueryHook(
                gcp_conn_id=warehouse_conn_id,
                location=bq_location,
                use_legacy_sql=False,
            ),
            project_id=bq_audit_project,
            create_dataset=bq_audit_create_dataset,
        )
    if warehouse == "snowflake":
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        return SnowflakeAuditWriter(SnowflakeHook(snowflake_conn_id=warehouse_conn_id))
    raise ValueError(f"Unsupported warehouse {warehouse!r}.")
