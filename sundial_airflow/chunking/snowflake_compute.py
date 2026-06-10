"""Snowflake warehouse resize for chunked DAG runs."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from airflow.exceptions import AirflowSkipException

from sundial_airflow.chunking.run_plan import run_plan_needs_chunked_compute

logger = logging.getLogger(__name__)

BOOST_SNOWFLAKE_TASK_ID = "boost_snowflake_compute"
ORIGINAL_SIZE_XCOM_KEY = "snowflake_warehouse_original_size"
_RESTORED_XCOM_KEY = "snowflake_warehouse_restored"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SIZE_RE = re.compile(r"^[A-Za-z0-9\-]+$")


@dataclass(frozen=True)
class SnowflakeComputeConfig:
    """Snowflake warehouse sizes for chunked runs."""

    warehouse_name: str
    chunked_size: str
    normal_size: str | None = None
    conn_id: str | None = None


def _validate_warehouse_name(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"Invalid Snowflake warehouse name {name!r}.")
    return name


def _validate_size(size: str, label: str) -> str:
    text = size.strip().upper()
    if not _SIZE_RE.match(text):
        raise ValueError(f"Invalid Snowflake warehouse {label} {size!r}.")
    return text


def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _run_label(context: dict[str, Any] | None) -> str:
    if not context:
        return ""
    dag_run = context.get("dag_run")
    if dag_run is None:
        return ""
    return f"dag_id={dag_run.dag_id} run_id={dag_run.run_id}"


def _log_prefix(context: dict[str, Any] | None) -> str:
    label = _run_label(context)
    return f"Snowflake compute ({label}): " if label else "Snowflake compute: "


def describe_warehouse_size(*, conn_id: str, warehouse_name: str) -> str | None:
    """Return the current warehouse size, or None if the warehouse is missing."""
    _run_snowflake(
        conn_id,
        f"SHOW WAREHOUSES LIKE {_sql_literal(warehouse_name)}",
    )
    rows = _run_snowflake(
        conn_id,
        "SELECT \"name\", \"size\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
    )
    if not rows:
        return None
    target = warehouse_name.upper()
    for row in rows:
        if str(row[0]).upper() == target:
            return str(row[1]).upper()
    return str(rows[0][1]).upper()


def set_warehouse_size(*, conn_id: str, warehouse_name: str, size: str) -> None:
    """Set warehouse size."""
    wh = _quote_ident(_validate_warehouse_name(warehouse_name))
    target = _validate_size(size, "size")
    sql = f"ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = {target}"
    logger.info("Executing Snowflake warehouse resize: %s", sql)
    _run_snowflake(conn_id, sql)


def boost_snowflake_warehouse(
    *,
    config: SnowflakeComputeConfig,
    conn_id: str,
    needs_boost: bool,
    context: dict[str, Any] | None = None,
) -> str | None:
    """Upsize the warehouse for chunked compute. Returns the pre-boost size."""
    prefix = _log_prefix(context)
    if not needs_boost:
        logger.info(
            "%sskipping warehouse scale-up for %r (no mapped chunk runs in this run).",
            prefix,
            config.warehouse_name,
        )
        raise AirflowSkipException("No mapped chunk runs in this DAG run.")

    wh_name = _validate_warehouse_name(config.warehouse_name)
    target = _validate_size(config.chunked_size, "chunked_size")
    original = describe_warehouse_size(conn_id=conn_id, warehouse_name=wh_name)
    if original is None:
        raise RuntimeError(f"Snowflake warehouse {wh_name!r} was not found.")

    if original == target:
        logger.info(
            "%swarehouse %r already at chunked size %s; no scale-up needed.",
            prefix,
            wh_name,
            target,
        )
        return original

    logger.info(
        "%sscaling warehouse %r up for chunked run: %s -> %s.",
        prefix,
        wh_name,
        original,
        target,
    )
    set_warehouse_size(conn_id=conn_id, warehouse_name=wh_name, size=target)
    current = describe_warehouse_size(conn_id=conn_id, warehouse_name=wh_name)
    logger.info(
        "%swarehouse %r scaled up to %s (was %s).",
        prefix,
        wh_name,
        current or target,
        original,
    )
    return original


def restore_snowflake_warehouse(
    context: dict[str, Any],
    *,
    config: SnowflakeComputeConfig,
    conn_id: str,
) -> None:
    """Restore warehouse size after a chunked DAG run."""
    prefix = _log_prefix(context)
    dag_run = context.get("dag_run")
    if dag_run is None:
        logger.info("%sskipping warehouse scale-down (no dag_run in callback).", prefix)
        return

    ti = dag_run.get_task_instance(BOOST_SNOWFLAKE_TASK_ID)
    if ti is None:
        logger.info(
            "%sskipping warehouse scale-down for %r (boost task not in DAG).",
            prefix,
            config.warehouse_name,
        )
        return
    if ti.xcom_pull(key=_RESTORED_XCOM_KEY):
        logger.info(
            "%swarehouse %r already restored for this run; skipping scale-down.",
            prefix,
            config.warehouse_name,
        )
        return

    original = ti.xcom_pull(key=ORIGINAL_SIZE_XCOM_KEY)
    if not original:
        logger.info(
            "%sskipping warehouse scale-down for %r (warehouse was not scaled up this run).",
            prefix,
            config.warehouse_name,
        )
        return

    target = config.normal_size or original
    target = _validate_size(target, "restore size")
    wh_name = _validate_warehouse_name(config.warehouse_name)
    run_state = getattr(dag_run, "state", None) or context.get("reason", "unknown")

    current = describe_warehouse_size(conn_id=conn_id, warehouse_name=wh_name)
    if current == target:
        logger.info(
            "%swarehouse %r already at normal size %s after dag run (%s); no scale-down needed.",
            prefix,
            wh_name,
            target,
            run_state,
        )
        ti.xcom_push(key=_RESTORED_XCOM_KEY, value=True)
        return

    logger.info(
        "%sscaling warehouse %r down after dag run (%s): %s -> %s "
        "(pre-boost size was %s).",
        prefix,
        wh_name,
        run_state,
        current,
        target,
        original,
    )
    set_warehouse_size(conn_id=conn_id, warehouse_name=wh_name, size=target)
    restored = describe_warehouse_size(conn_id=conn_id, warehouse_name=wh_name)
    logger.info(
        "%swarehouse %r scaled down to %s (was %s before restore).",
        prefix,
        wh_name,
        restored or target,
        current,
    )
    ti.xcom_push(key=_RESTORED_XCOM_KEY, value=True)


def make_snowflake_restore_callback(
    config: SnowflakeComputeConfig,
    conn_id: str,
):
    """Build a DAG callback that restores warehouse size."""

    def _restore(context: dict[str, Any]) -> None:
        prefix = _log_prefix(context)
        logger.info(
            "%sstarting warehouse scale-down for %r.",
            prefix,
            config.warehouse_name,
        )
        try:
            restore_snowflake_warehouse(context, config=config, conn_id=conn_id)
        except Exception:
            logger.exception(
                "%sfailed to scale down warehouse %r.",
                prefix,
                config.warehouse_name,
            )

    return _restore


def make_snowflake_failure_callback(
    config: SnowflakeComputeConfig,
    conn_id: str,
    on_failure: Any,
):
    """Restore warehouse size, then run the existing failure callback."""

    restore = make_snowflake_restore_callback(config, conn_id)

    def _on_failure(context: dict[str, Any]) -> None:
        restore(context)
        if on_failure is not None:
            on_failure(context)

    return _on_failure


def prepare_needs_chunked_compute(prep: dict[str, Any] | None) -> bool:
    """Read the chunked-compute flag from prepare_dbt_args XCom."""
    if not prep:
        return False
    if prep.get("chunked_compute") is True:
        return True
    return run_plan_needs_chunked_compute(prep.get("run_plan") or {})


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _run_snowflake(conn_id: str, sql: str) -> list[tuple[Any, ...]]:
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError as exc:
        raise RuntimeError(
            "apache-airflow-providers-snowflake is required for warehouse resize."
        ) from exc

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    rows = hook.get_records(sql)
    return rows or []
