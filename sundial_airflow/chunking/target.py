"""Ensure the chunk backfill schema or dataset exists."""
from __future__ import annotations

import logging
import re
from typing import Any

from sundial_airflow.warehouses import get_adapter

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BQ_PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_ident(name: str, label: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(
            f"Invalid {label} {name!r}: must match {_IDENT_RE.pattern!r}."
        )
    return name


def ensure_chunk_target(
    *,
    warehouse: str,
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    target: str,
    bq_location: str | None = None,
) -> None:
    """Create the chunk target schema or dataset when it is missing."""
    if warehouse == "snowflake":
        _ensure_snowflake_schema(conn_id, dbt_vars, target)
    elif warehouse == "bigquery":
        _ensure_bigquery_dataset(conn_id, dbt_vars, target, bq_location)
    else:
        logger.warning("No chunk-target DDL for warehouse %r.", warehouse)


def _ensure_snowflake_schema(
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    schema: str,
) -> None:
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError:
        logger.warning("apache-airflow-providers-snowflake not installed.")
        return

    schema = _validate_ident(schema, "schema")
    database = dbt_vars.get("target_database") or dbt_vars.get("target_project")
    if database:
        database = _validate_ident(database, "database")
        ddl = f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}"
    else:
        ddl = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    adapter = get_adapter("snowflake")
    conn_id = conn_id or (adapter.resolve_conn_id() if adapter else "snowflake_default")
    logger.info("Ensuring chunk target schema exists: %s", ddl)
    SnowflakeHook(snowflake_conn_id=conn_id).run(ddl)


def _ensure_bigquery_dataset(
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    dataset: str,
    location: str | None,
) -> None:
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    except ImportError:
        logger.warning("apache-airflow-providers-google not installed.")
        return

    project = dbt_vars.get("target_project")
    if not project or not _BQ_PROJECT_RE.match(project):
        logger.warning("Cannot create chunk target dataset: missing target_project.")
        return

    dataset = _validate_ident(dataset.lower(), "dataset")
    ref = f"`{project}.{dataset}`"
    if location:
        ddl = (
            f"CREATE SCHEMA IF NOT EXISTS {ref} "
            f"OPTIONS(location=\"{location}\")"
        )
    else:
        ddl = f"CREATE SCHEMA IF NOT EXISTS {ref}"

    adapter = get_adapter("bigquery")
    conn_id = conn_id or (adapter.resolve_conn_id() if adapter else "google_cloud_default")
    logger.info("Ensuring chunk target dataset exists: %s", ddl)
    BigQueryHook(gcp_conn_id=conn_id, use_legacy_sql=False).run(ddl)
