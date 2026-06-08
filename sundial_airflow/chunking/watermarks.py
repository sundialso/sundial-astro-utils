"""Batch watermark reads from ``dbt_completions_raw``."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sundial_airflow.warehouses import WarehouseAdapter, get_adapter

logger = logging.getLogger(__name__)

# Keep in sync with ``sundial_dbt_shared/macros/dbt_completions.sql :: read_watermark``.
_INCOMPLETE_RUN_GROUP = """
    SELECT 1
    FROM {table} u
    WHERE u.model_name = w.model_name
      AND u.run_group_id = w.run_group_id
      AND u.status = 'started'
      AND NOT EXISTS (
        SELECT 1 FROM {table} s
        WHERE s.model_name = u.model_name
          AND s.run_group_id = u.run_group_id
          AND s.chunk_key = u.chunk_key
          AND s.status = 'succeeded'
      )
"""


def watermark_batch_sql(table_fqn: str, placeholder_style: str) -> str:
    """Build a grouped watermark query for the given bind style."""
    if placeholder_style == "bigquery":
        model_filter = "w.model_name IN UNNEST(@models)"
    else:
        model_filter = "w.model_name IN (%(models)s)"
    incomplete = _INCOMPLETE_RUN_GROUP.format(table=table_fqn)
    return f"""
    SELECT w.model_name, MAX(w.end_ts) AS watermark_end
    FROM {table_fqn} w
    WHERE {model_filter}
      AND w.status = 'started'
      AND w.end_ts IS NOT NULL
      AND NOT EXISTS ({incomplete})
    GROUP BY w.model_name
    """


def fetch_watermark_ends(
    *,
    warehouse: str,
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    model_names: list[str],
) -> dict[str, datetime | None]:
    """Return the latest complete watermark ``end_ts`` per model."""
    if not model_names:
        return {}
    adapter = get_adapter(warehouse)
    if adapter is None:
        logger.warning("No warehouse adapter for %r; watermarks unavailable.", warehouse)
        return {}
    conn_id = conn_id or adapter.resolve_conn_id()
    table_fqn = adapter.build_table_fqn(dbt_vars, "dbt_completions_raw")
    if not table_fqn:
        logger.warning("Cannot resolve dbt_completions_raw FQN; watermarks unavailable.")
        return {}

    if warehouse == "bigquery":
        return _fetch_bigquery(conn_id, table_fqn, model_names)
    if warehouse == "snowflake":
        return _fetch_snowflake(conn_id, table_fqn, model_names)
    return {}


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


def _fetch_bigquery(
    conn_id: str, table_fqn: str, model_names: list[str],
) -> dict[str, datetime | None]:
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig
    except ImportError:
        logger.warning("apache-airflow-providers-google not installed.")
        return {}

    sql = watermark_batch_sql(table_fqn, "bigquery")
    hook = BigQueryHook(gcp_conn_id=conn_id, use_legacy_sql=False)
    job = hook.get_client().query(
        sql,
        job_config=QueryJobConfig(
            query_parameters=[ArrayQueryParameter("models", "STRING", model_names)],
        ),
    )
    return {row.model_name: _parse_watermark(row.watermark_end) for row in job.result()}


def _fetch_snowflake(
    conn_id: str, table_fqn: str, model_names: list[str],
) -> dict[str, datetime | None]:
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError:
        logger.warning("apache-airflow-providers-snowflake not installed.")
        return {}

    placeholders = ", ".join(f"%(m{i})s" for i in range(len(model_names)))
    sql = watermark_batch_sql(table_fqn, "snowflake").replace(
        "w.model_name IN (%(models)s)",
        f"w.model_name IN ({placeholders})",
    )
    params = {f"m{i}": name for i, name in enumerate(model_names)}
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    rows = hook.get_records(sql, parameters=params)
    return {row[0]: _parse_watermark(row[1]) for row in rows}
