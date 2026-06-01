"""``make_backfill_dag`` — lineage-preserving chunked backfill DAG factory.

Each tenant creates one thin DAG file that calls
:func:`make_backfill_dag`. The factory reads ``target/manifest.json``,
classifies every eligible model as ``CHUNKED`` (per the tenant's
``chunking_config.json``) or ``FULL_REFRESH``, topologically sorts
them, and emits a DAG whose graph mirrors the dbt lineage one-for-one
— each model becomes a TaskGroup whose shape depends on its kind:

- ``FULL_REFRESH`` → single ``run`` task (``dbt run --select <name>``).
- ``CHUNKED`` → N static ``chunk_<YYYY-MM>`` tasks, one per time window,
  computed from ``first_timestamp`` (from the model's ``start_ts()``
  call) up to today at ``chunk_size``-month strides.

After every model TaskGroup reaches a terminal state, two reporting
tasks run (``trigger_rule="all_done"``): ``backfill_report`` (audit
table summary) and, if a backfill warehouse name is provided,
``backfill_warehouse_report`` (audit ⨝ ``QUERY_HISTORY``).

See ``opendoor-dbt/BACKFILL.md`` for the operator-side runbook.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from . import _audit, _dbt_runner
from .manifest_parser import (
    CHUNKED,
    FULL_REFRESH,
    compute_static_chunks,
    load_backfill_models,
    load_chunking_config,
    topological_order,
)

logger = logging.getLogger(__name__)

_DBT_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_DEFAULT_ARGS: dict[str, Any] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}


def make_backfill_dag(
    *,
    dag_id: str,
    tenant: str,
    start_date: _dt.datetime,
    dbt_project_path: str | Path,
    dbt_profile_name: str,
    venv_execution_config: Any,
    backfill_profile_config: Any,
    chunking_config_path: str | Path | None,
    snowflake_conn_id: str | None = None,
    audit_schema: str = "DBT_BACKFILLS",
    base_vars: dict[str, Any] | None = None,
    default_args: dict[str, Any] | None = None,
    extra_tags: list[str] | None = None,
    on_failure_callback: Any | None = None,
    chunk_var_keys: tuple[str, str] = ("backfill_start_ts", "backfill_end_ts"),
    max_active_tasks: int = 16,
    backfill_warehouse: str | None = None,
    enable_final_reports: bool = True,
) -> Any:
    """Build and register a lineage-preserving chunked backfill DAG.

    Parameters
    ----------
    dag_id, tenant, start_date:
        Standard DAG identity. ``schedule=None`` is hard-set — backfills
        are always manually triggered.
    dbt_project_path:
        Absolute path to the dbt project root.
    dbt_profile_name:
        Profile name in ``profiles.yml``.
    venv_execution_config:
        Cosmos ``ExecutionConfig`` carrying the dbt executable path.
    backfill_profile_config:
        Cosmos ``ProfileConfig`` for the **dedicated backfill warehouse**.
        Never reuse the production warehouse.
    chunking_config_path:
        Path to the tenant's ``chunking_config.json``. **Source of truth**
        for which models get chunked: a model becomes ``CHUNKED`` iff it
        has an enabled entry here with a positive ``chunk_size`` AND its
        SQL contains ``start_ts()``. Everything else is full-refresh.
        ``None``/missing/empty = no models get chunked.
    snowflake_conn_id:
        Airflow Snowflake connection ID for audit writes. ``None``
        disables auditing (useful for local dev).
    audit_schema:
        Snowflake schema for ``BACKFILL_AUDIT``. Created if missing
        (requires ``CREATE SCHEMA`` privilege).
    base_vars:
        dbt vars merged into every model invocation (typically just
        ``target_schema``).
    default_args, extra_tags, on_failure_callback:
        Merged on top of factory defaults / appended to standard tags /
        wired into the DAG. Pass ``sundial_airflow.dag_failure_alert``
        as ``on_failure_callback`` for Slack notifications.
    chunk_var_keys:
        ``(start_var, end_var)`` injected as dbt vars for each chunk.
        Defaults match the standard ``start_ts()`` / ``end_ts()`` macros.
        Override only if the tenant uses non-standard macro names.
    max_active_tasks:
        DAG-level concurrent task cap. Bounds total chunk + run tasks
        the scheduler will launch simultaneously. Size for your backfill
        warehouse (e.g. ``LARGE`` w/ ``MAX_CLUSTER_COUNT=2`` handles
        ~40-50 small-window concurrent dbt queries comfortably).
    backfill_warehouse:
        Warehouse name passed to ``backfill_warehouse_report`` so it
        knows what to scope ``QUERY_HISTORY_BY_WAREHOUSE`` to. When
        ``None``, the warehouse report is skipped (audit report still
        runs).
    enable_final_reports:
        When ``True`` (default), the two reporting tasks fire with
        ``trigger_rule="all_done"`` after every model TaskGroup
        completes. Non-zero exit codes are logged as warnings — reports
        never fail the DAG.

    Returns
    -------
    The registered Airflow ``DAG`` object.
    """
    start_var, end_var = chunk_var_keys
    project_path_str = str(dbt_project_path)
    dbt_executable = str(venv_execution_config.dbt_executable_path)
    manifest_path = Path(project_path_str) / "target" / "manifest.json"
    _base_vars = base_vars or {}
    tags = ["dbt", "backfill", f"tenant:{tenant}", *(extra_tags or [])]
    merged_default_args = {**_DEFAULT_ARGS, **(default_args or {})}

    # ── Parse manifest at DAG-parse time. Cheap (single JSON load + sort);
    #    re-runs every scheduler cycle so today's date and any chunking-
    #    config edits flow through automatically.
    try:
        _config = load_chunking_config(chunking_config_path)
        _models = load_backfill_models(manifest_path, _config)
        _order = topological_order(_models) if _models else []
        _static_chunks = (
            compute_static_chunks(_models, _dt.date.today()) if _models else {}
        )
    except Exception as exc:  # missing/malformed manifest, cycle, etc.
        logger.warning(
            "Backfill manifest parse failed for DAG %r: %s. "
            "Run `dbt compile` to regenerate manifest.json.",
            dag_id, exc,
        )
        _models, _order, _static_chunks = {}, [], {}

    if _order:
        chunk_total = sum(len(v) for v in _static_chunks.values())
        logger.info(
            "Backfill DAG %r: %d models in topo order, %d chunked → "
            "%d static chunk tasks.",
            dag_id, len(_order), len(_static_chunks), chunk_total,
        )
        if not _static_chunks:
            logger.info(
                "Backfill DAG %r: no models are chunked. Populate "
                "chunking_config.json to opt models in.",
                dag_id,
            )

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=None,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=max_active_tasks,
        tags=tags,
        default_args=merged_default_args,
        on_failure_callback=on_failure_callback,
        params={
            "select": Param(
                default="",
                type=["null", "string"],
                title="Model selector (dbt syntax)",
                description=(
                    "Optional dbt-style selector to scope the run.\n\n"
                    "Examples:\n"
                    "- `my_model` — that one model only\n"
                    "- `my_model+` — model + all downstream\n"
                    "- `+my_model` — model + all upstream\n"
                    "- `+my_model+` — entire lineage\n"
                    "- `model_a+ model_b+` — two selectors, space-separated\n\n"
                    "Empty = run every eligible model. Models outside the "
                    "selection are skipped at runtime; the graph still shows "
                    "them for traceability."
                ),
            ),
        },
    )
    def _build() -> None:
        if not _order:
            EmptyOperator(task_id="no_backfill_models_configured")
            return

        # ── Closure shim around the extracted dbt_invoke runner ──────────
        def _invoke(
            subcommand_args: list[str],
            log_label: str,
            *,
            extra_global_flags: list[str] | None = None,
            stream_output: bool = False,
        ):
            return _dbt_runner.dbt_invoke(
                profile_config=backfill_profile_config,
                dbt_executable=dbt_executable,
                project_dir=project_path_str,
                profile_name=dbt_profile_name,
                subcommand_args=subcommand_args,
                log_label=log_label,
                extra_global_flags=extra_global_flags,
                stream_output=stream_output,
            )

        def _dbt_run(model_name: str, extra_vars: dict[str, Any] | None = None) -> None:
            result = _invoke(
                ["run", "--select", model_name,
                 "--vars", json.dumps({**_base_vars, **(extra_vars or {})})],
                f"dbt run [{model_name}]",
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"dbt run failed for {model_name} (exit={result.returncode})"
                )

        def _dbt_run_operation(
            macro_name: str,
            macro_args: dict[str, Any] | None = None,
            *,
            stream_output: bool = False,
        ):
            args = ["run-operation", macro_name]
            if macro_args:
                args += ["--args", json.dumps(macro_args)]
            return _invoke(
                args, f"dbt run-operation [{macro_name}]", stream_output=stream_output,
            )

        def _audit_hook():
            """Lazy SnowflakeHook — imported inside the task to keep DAG parse light."""
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            return SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # ── One-time `dbt deps` install before any model task ────────────
        @task(task_id="install_deps")
        def _install_deps(**_: Any) -> None:
            """Resolve dbt package dependencies once per DAG run."""
            result = _invoke(["deps"], "dbt deps")
            if result.returncode != 0:
                raise RuntimeError(f"dbt deps failed (exit={result.returncode})")

        deps_task = _install_deps()

        # ── Central planning: resolve `select` Param + broadcast plan ────
        @task(task_id="process_chunking_config")
        def _process_chunking_config(**context: Any) -> dict[str, Any]:
            """Resolve the operator's ``select`` Param and broadcast the run plan.

            Returns ``{"selected": [...] | None, "kind": {...}, "chunk_counts": {...}}``.
            ``selected=None`` means "run every model".
            """
            select_expr = (context["params"].get("select") or "").strip()
            if not select_expr:
                logger.info("`select` param empty → running every eligible model.")
                selected: list[str] | None = None
            else:
                # --quiet suppresses the "Running with dbt=…" preamble that
                # would otherwise pollute --output name lines.
                result = _invoke(
                    ["ls", "--select", select_expr,
                     "--resource-type", "model", "--output", "name"],
                    f"dbt ls --select {select_expr!r}",
                    extra_global_flags=["--quiet"],
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"`dbt ls --select {select_expr!r}` failed "
                        f"(exit={result.returncode}). Check the selector "
                        f"syntax and that the named models exist."
                    )
                selected, rejected = [], []
                for raw in result.stdout.splitlines():
                    clean = _dbt_runner.strip_ansi(raw).strip()
                    if not clean:
                        continue
                    (selected if _DBT_NAME_RE.match(clean) else rejected).append(clean)
                if rejected:
                    logger.warning(
                        "Discarded %d non-model line(s) from `dbt ls` stdout:\n  %s",
                        len(rejected), "\n  ".join(rejected),
                    )
                if not selected:
                    raise RuntimeError(
                        f"`dbt ls --select {select_expr!r}` matched zero models. "
                        f"Refusing to run an empty DAG."
                    )
                logger.info(
                    "Selector %r resolved to %d model(s):\n  %s",
                    select_expr, len(selected), "\n  ".join(selected),
                )

            kind_map = {m.name: m.kind for m in _order}
            chunk_counts = {n: len(c) for n, c in _static_chunks.items()}

            logger.info("=== Backfill run plan ===")
            for model in _order:
                tag = "ACTIVE" if selected is None or model.name in selected else "SKIPPED"
                if model.kind == CHUNKED:
                    logger.info(
                        "  %-8s %-50s chunked (%d chunks)",
                        tag, model.name, chunk_counts.get(model.name, 0),
                    )
                else:
                    logger.info("  %-8s %-50s full_refresh", tag, model.name)

            return {
                "selected": selected,
                "kind": kind_map,
                "chunk_counts": chunk_counts,
            }

        chunking_task = _process_chunking_config()
        deps_task >> chunking_task  # type: ignore[operator]

        # ── Worker: one task per (model, chunk window) ───────────────────
        # trigger_rule="none_failed" lets selected models still run when
        # the operator's `select` excludes their upstreams (those tasks
        # raise AirflowSkipException; real failures still propagate as
        # upstream_failed and block the downstream).
        @task(trigger_rule="none_failed")
        def _run_chunk(
            model_name: str,
            chunk_start_str: str,
            chunk_end_str: str,
            chunk_months: int,
            **context: Any,
        ) -> dict[str, Any]:
            """Run one chunk window for ``model_name`` and audit it."""
            plan = context["ti"].xcom_pull(task_ids="process_chunking_config") or {}
            selected = plan.get("selected")
            if selected is not None and model_name not in selected:
                raise AirflowSkipException(
                    f"Model {model_name!r} not in `select` ({context['params'].get('select')!r})."
                )

            started_at = _dt.datetime.now(_dt.timezone.utc)
            _dbt_run(
                model_name,
                extra_vars={
                    start_var: chunk_start_str,
                    end_var: chunk_end_str,
                    "run_context": "chunked_backfill",
                    "run_context_tag": f"{tenant}_chunked_backfill",
                },
            )
            if snowflake_conn_id:
                hook = _audit_hook()
                _audit.ensure_audit_table(hook, audit_schema)
                _audit.insert_chunk_row(
                    hook,
                    audit_schema=audit_schema,
                    run_id=context["run_id"],
                    model_name=model_name,
                    chunk_start=chunk_start_str,
                    chunk_end=chunk_end_str,
                    chunk_months=chunk_months,
                    airflow_run_id=context["dag_run"].run_id,
                    started_at=started_at,
                )
            return {
                "model": model_name,
                "chunk_start": chunk_start_str,
                "chunk_end": chunk_end_str,
                "chunk_months": chunk_months,
            }

        # ── Worker: single dbt run per full-refresh model ────────────────
        @task(trigger_rule="none_failed")
        def _run_full_refresh(model_name: str, **context: Any) -> dict[str, Any]:
            """Run ``dbt run --select <model>`` once and audit it."""
            plan = context["ti"].xcom_pull(task_ids="process_chunking_config") or {}
            selected = plan.get("selected")
            if selected is not None and model_name not in selected:
                raise AirflowSkipException(
                    f"Model {model_name!r} not in `select` ({context['params'].get('select')!r})."
                )

            started_at = _dt.datetime.now(_dt.timezone.utc)
            _dbt_run(
                model_name,
                extra_vars={
                    "run_context": "backfill_upstream_full_refresh",
                    "run_context_tag": f"{tenant}_backfill_upstream",
                },
            )
            if snowflake_conn_id:
                hook = _audit_hook()
                _audit.ensure_audit_table(hook, audit_schema)
                _audit.insert_full_refresh_row(
                    hook,
                    audit_schema=audit_schema,
                    run_id=context["run_id"],
                    model_name=model_name,
                    airflow_run_id=context["dag_run"].run_id,
                    started_at=started_at,
                )
            return {"model": model_name, "kind": "full_refresh"}

        # ── Final reports (trigger_rule="all_done" — always run) ─────────
        @task(trigger_rule="all_done")
        def _audit_report(**context: Any) -> dict[str, Any]:
            """Stream ``dbt run-operation backfill_report`` into the task log."""
            result = _dbt_run_operation(
                "backfill_report",
                macro_args={
                    "airflow_run_id": context["dag_run"].run_id,
                    "audit_schema": audit_schema,
                },
                stream_output=True,
            )
            if result.returncode != 0:
                logger.warning(
                    "backfill_report exited non-zero (%d) — see streamed output. "
                    "Report is informational; not failing the task.",
                    result.returncode,
                )
            return {"macro": "backfill_report", "exit_code": result.returncode}

        @task(trigger_rule="all_done")
        def _warehouse_report(**context: Any) -> dict[str, Any]:
            """Stream ``dbt run-operation backfill_warehouse_report`` into the task log."""
            result = _dbt_run_operation(
                "backfill_warehouse_report",
                macro_args={
                    "airflow_run_id": context["dag_run"].run_id,
                    "warehouse_name": backfill_warehouse,
                    "audit_schema": audit_schema,
                },
                stream_output=True,
            )
            if result.returncode != 0:
                logger.warning(
                    "backfill_warehouse_report exited non-zero (%d) — see streamed "
                    "output. Report is informational; not failing the task.",
                    result.returncode,
                )
            return {"macro": "backfill_warehouse_report", "exit_code": result.returncode}

        # ── Build per-model TaskGroups in topological order ──────────────
        models_by_node_key = {m.node_key: m for m in _order}
        model_groups: dict[str, TaskGroup] = {}

        for model in _order:
            with TaskGroup(group_id=model.name) as tg:
                if model.kind == CHUNKED:
                    windows = _static_chunks.get(model.name, [])
                    if not windows:
                        # Defensive: a chunked model whose first_timestamp
                        # is already >= today produces zero windows.
                        EmptyOperator(
                            task_id="no_chunks_in_window",
                            trigger_rule="none_failed",
                        )
                    else:
                        for start, end, chunk_id in windows:
                            _run_chunk.override(task_id=f"chunk_{chunk_id}")(
                                model_name=model.name,
                                chunk_start_str=str(start),
                                chunk_end_str=str(end),
                                chunk_months=model.chunk_months,
                            )
                else:  # FULL_REFRESH
                    _run_full_refresh.override(task_id="run")(model_name=model.name)

            # Wire inter-model edges. Upstreams not in the backfill set
            # (sources, opt-outs) are silently dropped.
            upstreams = [
                models_by_node_key[k].name
                for k in model.depends_on
                if k in models_by_node_key
            ]
            if upstreams:
                for up in upstreams:
                    model_groups[up] >> tg
            else:
                chunking_task >> tg  # root → planning task

            model_groups[model.name] = tg

        # ── Final reports fire after every model TaskGroup completes ─────
        if enable_final_reports:
            with TaskGroup(group_id="final_reports"):
                final_sentinel = EmptyOperator(
                    task_id="ready", trigger_rule="all_done",
                )
                for tg in model_groups.values():
                    tg >> final_sentinel

                final_sentinel >> _audit_report.override(task_id="audit_report")()

                if backfill_warehouse:
                    final_sentinel >> _warehouse_report.override(
                        task_id="warehouse_report",
                    )()
                else:
                    logger.info(
                        "`backfill_warehouse` not set — skipping warehouse_report task.",
                    )

    return _build()
