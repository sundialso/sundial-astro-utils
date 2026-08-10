"""Snowflake warehouse resize/restore tasks for Sundial dbt DAGs."""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from sundial_airflow.task_log import log_block, quiet_sql_hook_loggers

logger = logging.getLogger(__name__)

RESIZE_TASK_ID = "resize_snowflake_wh"
RESTORE_TASK_ID = "restore_snowflake_wh"

_WH_SIZE_ENV = "SUNDIAL_SF_WH_SIZE"
_BACKFILL_SIZE_ENV = "SUNDIAL_SF_BACKFILL_WH_SIZE"
_DEFAULT_WH_SIZE = "Medium"
_DEFAULT_BACKFILL_SIZE = "Large"
_BACKFILL_MODES = frozenset({"full", "partial"})

_RESTORE_RETRIES = 5
_RESTORE_RETRY_DELAY = timedelta(seconds=60)

_VALID_SNOWFLAKE_SIZES = frozenset({
    "X-Small", "Small", "Medium", "Large", "X-Large",
    "2X-Large", "3X-Large", "4X-Large", "5X-Large", "6X-Large",
})


def _env_size(env_var: str, default: str) -> tuple[str, str]:
    """Return ``(size, source_label)`` from env var or default."""
    raw = (os.environ.get(env_var) or "").strip()
    return (raw, env_var) if raw else (default, "default")


def _backfill_mode(params: dict[str, Any] | None) -> str:
    """Return ``backfill_mode`` from params (default ``"none"``)."""
    return str((params or {}).get("backfill_mode", "none"))


def _quote_ident(name: str) -> str:
    """Double-quote and uppercase a Snowflake identifier."""
    stripped = name.strip()
    if not stripped:
        raise ValueError("Warehouse name must not be empty")
    return '"' + stripped.upper().replace('"', '""') + '"'


def _warehouse_from_conn(conn_id: str) -> str | None:
    """Return the warehouse name from the connection's extras, or ``None``."""
    from airflow.hooks.base import BaseHook

    extra = BaseHook.get_connection(conn_id).extra_dejson or {}
    name = extra.get("warehouse") or extra.get("extra__snowflake__warehouse") or ""
    return str(name).strip() or None


def _resolve_conn_id(conn_id: str | None) -> str | None:
    """Return the effective connection ID, or ``None`` if unresolvable."""
    if conn_id:
        return conn_id
    try:
        from sundial_airflow.warehouses import get_adapter

        adapter = get_adapter("snowflake")
        return adapter.resolve_conn_id() if adapter else None
    except (ImportError, AttributeError, LookupError):
        logger.warning("Failed to resolve Snowflake connection", exc_info=True)
        return None


def _resolve_warehouse(conn_id: str | None) -> tuple[str, str] | None:
    """Return ``(conn_id, warehouse_name)`` or ``None`` if either is missing."""
    resolved = _resolve_conn_id(conn_id)
    if not resolved:
        return None
    warehouse = _warehouse_from_conn(resolved)
    if not warehouse:
        return None
    return resolved, warehouse


def _task_context(context: dict[str, Any]) -> dict[str, Any]:
    """Pick resize/restore kwargs from an Airflow task context."""
    return {
        "params": context.get("params"),
        "dag_id": str(context["dag"].dag_id),
        "run_id": str(context["run_id"]),
    }


def alter_warehouse_size(*, conn_id: str, warehouse: str, size: str) -> None:
    """``ALTER WAREHOUSE … SET WAREHOUSE_SIZE``."""
    if size not in _VALID_SNOWFLAKE_SIZES:
        raise ValueError(
            f"Invalid warehouse size: {size!r}. "
            f"Valid: {sorted(_VALID_SNOWFLAKE_SIZES)}"
        )
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "apache-airflow-providers-snowflake is required for warehouse sizing"
        ) from exc

    sql = f"ALTER WAREHOUSE {_quote_ident(warehouse)} SET WAREHOUSE_SIZE = '{size}'"
    logger.info("Running: %s", sql)
    with quiet_sql_hook_loggers():
        SnowflakeHook(snowflake_conn_id=conn_id).run(sql)
    logger.info("ALTER WAREHOUSE succeeded")


def resize_snowflake_warehouse(
    *,
    conn_id: str | None,
    params: dict[str, Any] | None,
    dag_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Resize warehouse: backfill size or steady-state. No-ops if unresolvable."""
    mode = _backfill_mode(params)
    is_backfill = mode in _BACKFILL_MODES
    steady_size, steady_src = _env_size(_WH_SIZE_ENV, _DEFAULT_WH_SIZE)
    backfill_size, backfill_src = _env_size(_BACKFILL_SIZE_ENV, _DEFAULT_BACKFILL_SIZE)

    resolved = _resolve_warehouse(conn_id)
    if not resolved:
        logger.warning(
            "Warehouse resize NO-OP: connection or warehouse not resolvable "
            "(conn_id=%r, dag_id=%s)",
            conn_id,
            dag_id,
        )
        log_block(RESIZE_TASK_ID, [
            f"  dag_id:          {dag_id}",
            f"  run_id:          {run_id}",
            f"  backfill_mode:   {mode}",
            "  result:          NO-OP — warehouse or connection not resolvable",
        ])
        return {
            "warehouse": None, "size": None,
            "backfill": is_backfill, "conn_id": None, "skipped": True,
        }

    resolved_conn_id, warehouse_name = resolved
    target_size = backfill_size if is_backfill else steady_size
    action = "upsize for backfill" if is_backfill else "enforce steady-state size"

    log_block(RESIZE_TASK_ID, [
        f"  dag_id:          {dag_id}",
        f"  run_id:          {run_id}",
        f"  backfill_mode:   {mode}",
        f"  conn_id:         {resolved_conn_id}",
        f"  warehouse:       {warehouse_name}",
        f"  steady_size:     {steady_size} ({steady_src})",
        f"  backfill_size:   {backfill_size} ({backfill_src})",
        f"  action:          {action}",
        f"  target_size:     {target_size}",
    ])
    alter_warehouse_size(
        conn_id=resolved_conn_id, warehouse=warehouse_name, size=target_size,
    )
    return {
        "warehouse": warehouse_name,
        "size": target_size,
        "backfill": is_backfill,
        "conn_id": resolved_conn_id,
    }


def restore_snowflake_warehouse(
    *,
    conn_id: str | None,
    params: dict[str, Any] | None,
    dag_id: str,
    run_id: str,
) -> None:
    """Restore warehouse to steady-state size. Skips non-backfill runs."""
    mode = _backfill_mode(params)
    if mode not in _BACKFILL_MODES:
        log_block(RESTORE_TASK_ID, [
            f"  dag_id:          {dag_id}",
            f"  run_id:          {run_id}",
            f"  backfill_mode:   {mode}",
            "  result:          SKIP — not a backfill",
        ])
        raise AirflowSkipException("not a backfill; steady-state already set by setup")

    resolved = _resolve_warehouse(conn_id)
    if not resolved:
        raise AirflowSkipException("Warehouse or connection not resolvable for restore")

    resolved_conn_id, warehouse_name = resolved
    steady_size, steady_src = _env_size(_WH_SIZE_ENV, _DEFAULT_WH_SIZE)

    log_block(RESTORE_TASK_ID, [
        f"  dag_id:          {dag_id}",
        f"  run_id:          {run_id}",
        f"  backfill_mode:   {mode}",
        f"  conn_id:         {resolved_conn_id}",
        f"  warehouse:       {warehouse_name}",
        f"  restore_size:    {steady_size} ({steady_src})",
    ])
    alter_warehouse_size(
        conn_id=resolved_conn_id, warehouse=warehouse_name, size=steady_size,
    )


def build_warehouse_sizing_tasks(*, conn_id: str | None = None) -> tuple[Any, Any]:
    """Return ``(resize_task, restore_task)`` with a ``resize >> restore`` edge."""

    @task(task_id=RESIZE_TASK_ID)
    def resize_snowflake_wh(**context: Any) -> dict[str, Any]:
        return resize_snowflake_warehouse(conn_id=conn_id, **_task_context(context))

    @task(
        task_id=RESTORE_TASK_ID,
        retries=_RESTORE_RETRIES,
        retry_delay=_RESTORE_RETRY_DELAY,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def restore_snowflake_wh(**context: Any) -> None:
        restore_snowflake_warehouse(conn_id=conn_id, **_task_context(context))

    resize = resize_snowflake_wh()
    restore = restore_snowflake_wh()
    resize >> restore
    return resize, restore
