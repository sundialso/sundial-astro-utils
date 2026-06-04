"""``make_backfill_dag`` — lineage-preserving chunked backfill DAG factory.

Reads ``target/manifest.json``, classifies each eligible model as
``CHUNKED`` (per ``chunking_config.json``) or ``FULL_REFRESH``,
topologically sorts them, and emits a DAG that mirrors the dbt lineage
one TaskGroup per model. ``CHUNKED`` groups expand into static
``chunk_<YYYY-MM>`` tasks; ``FULL_REFRESH`` groups are a single ``run``
task. Every successful task writes to ``<audit_schema>.BACKFILL_AUDIT``;
reports are ad-hoc (``dbt run-operation backfill_report``).

See ``<tenant>_dbt/BACKFILL.md`` for the operator runbook.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from . import _audit
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
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

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
    warehouse: str,
    warehouse_conn_id: str | None = None,
    bq_location: str | None = None,
    audit_schema: str = "DBT_BACKFILLS",
    base_vars: dict[str, Any] | None = None,
    default_args: dict[str, Any] | None = None,
    extra_tags: list[str] | None = None,
    on_failure_callback: Any | None = None,
    chunk_var_keys: tuple[str, str] = ("backfill_start_ts", "backfill_end_ts"),
    max_active_tasks: int = 16,
) -> Any:
    """Build and register a lineage-preserving chunked backfill DAG.
    """
    warehouse = warehouse.lower()
    if warehouse not in ("snowflake", "bigquery"):
        raise ValueError(
            f"Unsupported warehouse {warehouse!r}: expected 'snowflake' or 'bigquery'."
        )
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

        # ``--no-write-json`` is hard-prepended so concurrent dbt runs
        # don't race on ``target/manifest.json``.
        def _invoke(
            subcommand_args: list[str],
            log_label: str,
            *,
            extra_global_flags: list[str] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            with backfill_profile_config.ensure_profile() as (profile_path, profile_env):
                cmd = [
                    dbt_executable,
                    "--no-write-json",
                    *(extra_global_flags or []),
                    *subcommand_args,
                    "--project-dir", project_path_str,
                    "--profiles-dir", str(Path(profile_path).parent),
                    "--profile", dbt_profile_name,
                    "--target", backfill_profile_config.target_name,
                ]
                env = {**os.environ, **profile_env}
                logger.info("%s cmd: %s", log_label, " ".join(cmd))
                result = subprocess.run(
                    cmd, capture_output=True, text=True, env=env, check=False,
                )

            logger.info("%s stdout:\n%s", log_label, result.stdout)
            if result.stderr:
                logger.warning("%s stderr:\n%s", log_label, result.stderr)
            return result

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

        def _audit_writer() -> _audit.AuditWriter:
            if warehouse == "bigquery":
                from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
                # ``location`` must match the audit dataset's region; BigQuery
                # query jobs default to US otherwise. dbt runs get their region
                # from the tenant's ProfileConfig separately.
                return _audit.BigQueryAuditWriter(
                    BigQueryHook(
                        gcp_conn_id=warehouse_conn_id,
                        location=bq_location,
                        use_legacy_sql=False,
                    )
                )
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            return _audit.SnowflakeAuditWriter(
                SnowflakeHook(snowflake_conn_id=warehouse_conn_id)
            )

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
            """Resolve the ``select`` Param and broadcast the run plan via XCom.

            Returns ``{selected, kind, chunk_counts}``; ``selected=None`` means run-all.
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
                    clean = _ANSI_RE.sub("", raw).strip()
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
            if warehouse_conn_id:
                writer = _audit_writer()
                writer.ensure_audit_table(audit_schema)
                writer.insert_chunk_row(
                    audit_schema=audit_schema,
                    model_name=model_name,
                    start_ts=chunk_start_str,
                    end_ts=chunk_end_str,
                    run_id=context["dag_run"].run_id,
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
            if warehouse_conn_id:
                writer = _audit_writer()
                writer.ensure_audit_table(audit_schema)
                writer.insert_full_refresh_row(
                    audit_schema=audit_schema,
                    model_name=model_name,
                    run_id=context["dag_run"].run_id,
                    started_at=started_at,
                )
            return {"model": model_name, "kind": "full_refresh"}

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

    return _build()
