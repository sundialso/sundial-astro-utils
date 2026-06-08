"""Batch watermark reads from model partition columns."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from sundial_airflow.backfill.manifest_parser import BackfillModel
from sundial_airflow.warehouses import get_adapter

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_ident(name: str, label: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(
            f"Invalid {label} {name!r}: must match {_IDENT_RE.pattern!r}."
        )
    return name


def partition_watermark_sql(table_fqn: str, partition_column: str) -> str:
    """Build a MAX(partition_column) query for one model table."""
    col = _validate_ident(partition_column, "partition column")
    return f"SELECT MAX({col}) FROM {table_fqn}"


def fetch_partition_watermarks(
    *,
    warehouse: str,
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    models: list[BackfillModel],
) -> dict[str, datetime | None]:
    """Return the latest partition value per chunked model from its target table."""
    adapter = get_adapter(warehouse)
    if adapter is None:
        logger.warning("No warehouse adapter for %r; watermarks unavailable.", warehouse)
        return {}

    conn_id = conn_id or adapter.resolve_conn_id()
    watermarks: dict[str, datetime | None] = {}
    for model in models:
        if not model.partition_column:
            logger.warning(
                "Model %r has no start_ts() partition column; treating as first run.",
                model.name,
            )
            watermarks[model.name] = None
            continue

        table_name = model.table_name or model.name
        table_fqn = adapter.build_table_fqn(dbt_vars, table_name)
        if not table_fqn:
            logger.warning(
                "Cannot resolve table FQN for %r; treating as first run.",
                model.name,
            )
            watermarks[model.name] = None
            continue

        watermark = _fetch_one(
            warehouse=warehouse,
            conn_id=conn_id,
            table_fqn=table_fqn,
            partition_column=model.partition_column,
        )
        watermarks[model.name] = watermark
        logger.info(
            "Partition watermark for %r: %s (table=%s, column=%s)",
            model.name,
            watermark,
            table_fqn,
            model.partition_column,
        )
    return watermarks


def _fetch_one(
    *,
    warehouse: str,
    conn_id: str,
    table_fqn: str,
    partition_column: str,
) -> datetime | None:
    if warehouse == "snowflake":
        return _fetch_snowflake(conn_id, table_fqn, partition_column)
    if warehouse == "bigquery":
        return _fetch_bigquery(conn_id, table_fqn, partition_column)
    return None


def _parse_watermark(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("T", " ")[:19]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.fromisoformat(str(value)[:10])


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not exist" in message
        or "not authorized" in message and "object" in message
        or "002003" in message
        or "object not found" in message
    )


def _fetch_snowflake(
    conn_id: str,
    table_fqn: str,
    partition_column: str,
) -> datetime | None:
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError:
        logger.warning("apache-airflow-providers-snowflake not installed.")
        return None

    sql = partition_watermark_sql(table_fqn, partition_column)
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    try:
        rows = hook.get_records(sql)
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.info("Partition watermark table %s not found; first run.", table_fqn)
            return None
        raise
    if not rows or rows[0][0] is None:
        return None
    return _parse_watermark(rows[0][0])


def _fetch_bigquery(
    conn_id: str,
    table_fqn: str,
    partition_column: str,
) -> datetime | None:
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    except ImportError:
        logger.warning("apache-airflow-providers-google not installed.")
        return None

    sql = partition_watermark_sql(table_fqn, partition_column)
    hook = BigQueryHook(gcp_conn_id=conn_id, use_legacy_sql=False)
    try:
        rows = list(hook.get_client().query(sql).result())
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.info("Partition watermark table %s not found; first run.", table_fqn)
            return None
        raise
    if not rows or rows[0][0] is None:
        return None
    return _parse_watermark(rows[0][0])
