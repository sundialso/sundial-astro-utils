"""Read partition watermarks from model tables."""
from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sundial_airflow.chunking.manifest_parser import BackfillModel
from sundial_airflow.task_log import quiet_sql_hook_loggers
from sundial_airflow.warehouses import get_adapter

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_BATCH_FALLBACK_WORKERS = 32
# The table the dbt completions hooks MERGE into (what read_watermark reads).
_COMPLETIONS_TABLE = "dbt_completions_raw"


@dataclass(frozen=True)
class _WatermarkQuery:
    model_name: str
    table_fqn: str
    partition_column: str


def _validate_ident(name: str, label: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(
            f"Invalid {label} {name!r}: must match {_IDENT_RE.pattern!r}."
        )
    return name


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def partition_watermark_sql(table_fqn: str, partition_column: str) -> str:
    """Build MAX(partition_column) SQL."""
    col = _validate_ident(partition_column, "partition column")
    return f"SELECT MAX({col}) FROM {table_fqn}"


def partition_watermarks_batch_sql(queries: list[_WatermarkQuery]) -> str:
    """Build one UNION ALL query for many model watermarks."""
    if not queries:
        raise ValueError("queries must not be empty")
    parts: list[str] = []
    for query in queries:
        col = _validate_ident(query.partition_column, "partition column")
        name = _sql_string_literal(query.model_name)
        parts.append(
            f"SELECT {name} AS model_name, MAX({col}) AS watermark "
            f"FROM {query.table_fqn}"
        )
    return "\nUNION ALL\n".join(parts)


def read_watermark_sql(completions_fqn: str, model_name: str) -> str:
    """Replicate the dbt ``read_watermark`` macro: ``MAX(end_ts)`` over the
    model's COMPLETE run_groups (every 'started' chunk has a 'succeeded'
    sibling). This is the same lower-bound source the incremental ``start_ts()``
    macro uses at run time.
    """
    name = _sql_string_literal(model_name)
    return (
        f"SELECT MAX(w.end_ts) FROM {completions_fqn} w "
        f"WHERE w.model_name = {name} AND w.status = 'started' "
        f"AND w.end_ts IS NOT NULL "
        f"AND NOT EXISTS ("
        f"SELECT 1 FROM {completions_fqn} u "
        f"WHERE u.model_name = w.model_name AND u.run_group_id = w.run_group_id "
        f"AND u.status = 'started' AND NOT EXISTS ("
        f"SELECT 1 FROM {completions_fqn} s "
        f"WHERE s.model_name = u.model_name AND s.run_group_id = u.run_group_id "
        f"AND s.chunk_key = u.chunk_key AND s.status = 'succeeded'))"
    )


def completion_watermarks_batch_sql(
    completions_fqn: str, model_names: list[str]
) -> str:
    """One UNION ALL query returning each model's ``read_watermark`` value."""
    parts = [
        f"SELECT {_sql_string_literal(name)} AS model_name, "
        f"({read_watermark_sql(completions_fqn, name)}) AS watermark"
        for name in model_names
    ]
    return "\nUNION ALL\n".join(parts)


def fetch_partition_watermarks(
    *,
    warehouse: str,
    conn_id: str | None,
    dbt_vars: dict[str, Any],
    models: list[BackfillModel],
) -> dict[str, datetime | None]:
    """Fetch latest partition value per model."""
    adapter = get_adapter(warehouse)
    if adapter is None:
        logger.warning("No warehouse adapter for %r; watermarks unavailable.", warehouse)
        return {}

    conn_id = conn_id or adapter.resolve_conn_id()
    watermarks: dict[str, datetime | None] = {}
    queries: list[_WatermarkQuery] = []

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

        queries.append(
            _WatermarkQuery(
                model_name=model.name,
                table_fqn=table_fqn,
                partition_column=model.partition_column,
            )
        )

    if not queries:
        return watermarks

    logger.info("Fetching watermarks for %d model(s) via %s", len(queries), warehouse)

    if len(queries) == 1:
        fetched = _fetch_one(
            warehouse=warehouse,
            conn_id=conn_id,
            table_fqn=queries[0].table_fqn,
            partition_column=queries[0].partition_column,
        )
        watermarks[queries[0].model_name] = fetched
    else:
        try:
            watermarks.update(_fetch_batch(warehouse, conn_id, queries))
        except Exception as exc:
            if _is_missing_table_error(exc):
                logger.info(
                    "Batch watermark query failed (%s); falling back to parallel "
                    "per-model queries.",
                    exc,
                )
                watermarks.update(_fetch_parallel(warehouse, conn_id, queries))
            else:
                raise

    # Align the planning watermark with the dbt read_watermark macro:
    # COALESCE(read_watermark, MAX(partition_column)). read_watermark (MAX
    # completed end_ts in dbt_completions_raw) is the exact lower bound the
    # incremental start_ts() macro uses at run time, so chunked planning and
    # execution can't diverge when completions and table data disagree (failed
    # tests / retries / partial runs). Falls back to the partition MAX when the
    # completions table doesn't exist yet (pre-first-run).
    partition_only = dict(watermarks)
    completions: dict[str, datetime | None] = {}
    completions_fqn = adapter.build_table_fqn(dbt_vars, _COMPLETIONS_TABLE)
    if completions_fqn:
        try:
            completions = _fetch_completion_watermarks(
                warehouse=warehouse,
                conn_id=conn_id,
                completions_fqn=completions_fqn,
                model_names=[query.model_name for query in queries],
            )
        except Exception as exc:
            logger.warning(
                "Completion watermark query failed (%s); using partition MAX only.",
                exc,
            )
        for name, completed in completions.items():
            if completed is not None:
                watermarks[name] = completed
    else:
        logger.info(
            "Cannot resolve %s FQN; planning on partition MAX only.",
            _COMPLETIONS_TABLE,
        )

    for query in queries:
        name = query.model_name
        logger.debug(
            "Planning watermark for %r: %s (read_watermark=%s, partition_max=%s, "
            "table=%s, column=%s)",
            name,
            watermarks.get(name),
            completions.get(name),
            partition_only.get(name),
            query.table_fqn,
            query.partition_column,
        )
    return watermarks


def _fetch_batch(
    warehouse: str,
    conn_id: str,
    queries: list[_WatermarkQuery],
) -> dict[str, datetime | None]:
    sql = partition_watermarks_batch_sql(queries)
    if warehouse == "snowflake":
        rows = _run_snowflake_query(conn_id, sql)
    elif warehouse == "bigquery":
        rows = _run_bigquery_query(conn_id, sql)
    else:
        return {}

    out = {query.model_name: None for query in queries}
    for row in rows:
        model_name = row[0]
        out[str(model_name)] = _parse_watermark(row[1])
    return out


def _fetch_parallel(
    warehouse: str,
    conn_id: str,
    queries: list[_WatermarkQuery],
) -> dict[str, datetime | None]:
    workers = min(len(queries), _MAX_BATCH_FALLBACK_WORKERS)
    out: dict[str, datetime | None] = {}

    def _one(query: _WatermarkQuery) -> tuple[str, datetime | None]:
        value = _fetch_one(
            warehouse=warehouse,
            conn_id=conn_id,
            table_fqn=query.table_fqn,
            partition_column=query.partition_column,
        )
        return query.model_name, value

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, query): query for query in queries}
        for future in as_completed(futures):
            model_name, value = future.result()
            out[model_name] = value
    return out


def _fetch_completion_watermarks(
    *,
    warehouse: str,
    conn_id: str,
    completions_fqn: str,
    model_names: list[str],
) -> dict[str, datetime | None]:
    """Read each model's ``read_watermark`` value in one query. Returns ``{}``
    when the completions table doesn't exist yet, so callers keep the partition
    MAX (mirrors the macro's ``COALESCE(read_watermark, MAX(col))``).
    """
    if not model_names:
        return {}
    sql = completion_watermarks_batch_sql(completions_fqn, model_names)
    if warehouse == "snowflake":
        rows = _run_snowflake_query(conn_id, sql, allow_missing_table=True)
    elif warehouse == "bigquery":
        rows = _run_bigquery_query(conn_id, sql, allow_missing_table=True)
    else:
        return {}
    return {str(row[0]): _parse_watermark(row[1]) for row in rows}


def _fetch_one(
    *,
    warehouse: str,
    conn_id: str,
    table_fqn: str,
    partition_column: str,
) -> datetime | None:
    sql = partition_watermark_sql(table_fqn, partition_column)
    if warehouse == "snowflake":
        rows = _run_snowflake_query(conn_id, sql, allow_missing_table=True)
    elif warehouse == "bigquery":
        rows = _run_bigquery_query(conn_id, sql, allow_missing_table=True)
    else:
        return None
    if not rows or rows[0][0] is None:
        return None
    return _parse_watermark(rows[0][0])


def _run_snowflake_query(
    conn_id: str,
    sql: str,
    *,
    allow_missing_table: bool = False,
) -> list[tuple[Any, ...]]:
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError:
        logger.warning("apache-airflow-providers-snowflake not installed.")
        return []

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    try:
        with quiet_sql_hook_loggers():
            rows = hook.get_records(sql)
    except Exception as exc:
        if allow_missing_table and _is_missing_table_error(exc):
            logger.info("Partition watermark table not found; first run: %s", exc)
            return []
        raise
    return rows or []


def _run_bigquery_query(
    conn_id: str,
    sql: str,
    *,
    allow_missing_table: bool = False,
) -> list[tuple[Any, ...]]:
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    except ImportError:
        logger.warning("apache-airflow-providers-google not installed.")
        return []

    hook = BigQueryHook(gcp_conn_id=conn_id, use_legacy_sql=False)
    try:
        with quiet_sql_hook_loggers():
            rows = list(hook.get_client().query(sql).result())
    except Exception as exc:
        if allow_missing_table and _is_missing_table_error(exc):
            logger.info("Partition watermark table not found; first run: %s", exc)
            return []
        raise
    return [tuple(row) for row in rows]


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
