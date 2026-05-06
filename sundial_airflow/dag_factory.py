"""``make_dbt_dag`` — single entry point each tenant uses to build its DAG.

The factory absorbs every bit of plumbing that's currently duplicated across
the tenant DAGs:

- standard ``params`` set (backfill, select/exclude, skip_tests, ...)
- ``prepare_dbt_args`` task (``--vars`` blob + ``dbt ls`` selection resolution)
- per-source-table ``DbtTestLocalOperator``s
- the Cosmos ``DbtTaskGroup``
- ``source_tests_gate`` joiner
- ``report_data_processed`` task (BigQuery / Snowflake variant)
- Slack failure alert wired into ``default_args``

Tenant DAG files reduce to ~25 lines: a single ``make_dbt_dag(...)`` call
configured with their connection IDs / dataset name / schedule.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Literal

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.operators.local import DbtTestLocalOperator

from sundial_airflow.hooks import (
    PREPARE_TASK_ID,
    skip_tests_if_disabled,
    skip_unselected,
)
from sundial_airflow.params import build_standard_params
from sundial_airflow.slack_alerts import task_failure_alert
from sundial_airflow.source_discovery import discover_source_tables_with_tests

logger = logging.getLogger(__name__)

Warehouse = Literal["bigquery", "snowflake"]

DEFAULT_DEFAULT_ARGS: dict[str, Any] = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}


def _vars_field_name(warehouse: Warehouse) -> str:
    return "target_dataset" if warehouse == "bigquery" else "target_schema"


def _param_field_name(warehouse: Warehouse) -> str:
    return "dataset" if warehouse == "bigquery" else "schema"


def make_dbt_dag(
    *,
    dag_id: str,
    tenant: str,
    start_date: _dt.datetime,
    schedule: str | None,
    warehouse: Warehouse,
    dbt_project_path: str | Path,
    dbt_profile_name: str,
    venv_execution_config: Any,
    profile_config_factory: Callable[[str, str | None], Any],
    default_dataset_or_schema: str,
    bigquery: dict[str, Any] | None = None,
    snowflake: dict[str, Any] | None = None,
    default_args: dict[str, Any] | None = None,
    extra_tags: list[str] | None = None,
    pre_tasks: list[Callable[[], Any]] | None = None,
    max_active_tasks: int = 4,
    catchup: bool = False,
    target_choices: list[str] | None = None,
    sources_yml_candidates: list[Path] | None = None,
    recursive_tests: bool = True,
):
    """Build and register a fully-wired Sundial dbt DAG.

    See ``README.md`` for an end-to-end usage example. All keyword arguments
    are required unless documented otherwise.

    Parameters
    ----------
    dag_id:
        Airflow DAG id, e.g. ``"dbt_citizen"``.
    tenant:
        Short tenant slug; used in the Slack alert and the
        ``run_context_tag`` (``"<tenant>_normal"``, etc).
    warehouse:
        ``"bigquery"`` or ``"snowflake"``. Picks the report task variant and
        the ``target_dataset`` / ``target_schema`` var key.
    dbt_project_path:
        Absolute path to the dbt project (the directory containing
        ``dbt_project.yml``).
    dbt_profile_name:
        Profile name as it appears in ``profiles.yml``; used by ``dbt ls``
        when resolving model selection.
    venv_execution_config:
        Cosmos ``ExecutionConfig`` pointing at the dbt venv.
    profile_config_factory:
        Callable ``(target, dataset_or_schema) -> ProfileConfig``. Tenants
        keep this in their ``include/constants.py`` so warehouse-specific
        profile-mapping details (BigQuery vs Snowflake) stay tenant-side.
    default_dataset_or_schema:
        The default dataset (BigQuery) or schema (Snowflake) that gets used
        when no value is supplied via the DAG params.
    bigquery:
        Required when ``warehouse="bigquery"``. Dict with keys:
        ``project``, ``location``, ``gcp_conn_id``.
    snowflake:
        Required when ``warehouse="snowflake"``. Dict with keys:
        ``conn_id``, ``warehouse``.
    default_args:
        Merged on top of :data:`DEFAULT_DEFAULT_ARGS`. The factory always
        injects ``on_failure_callback=task_failure_alert``; pass your own to
        override.
    extra_tags:
        Extra tags appended after ``["dbt", f"tenant:{tenant}"]``.
    pre_tasks:
        Optional list of zero-arg callables that return TaskFlow tasks; they
        run before ``prepare_dbt_args`` (used by ``ami_dbt`` for its EMR
        ingest step).
    max_active_tasks, catchup, target_choices, sources_yml_candidates,
    recursive_tests:
        Tuning knobs with sensible defaults; see the implementation.
    """
    if warehouse == "bigquery":
        if not bigquery:
            raise ValueError("bigquery={...} is required when warehouse='bigquery'")
        for key in ("project", "location", "gcp_conn_id"):
            if key not in bigquery:
                raise ValueError(f"bigquery['{key}'] is required")
    elif warehouse == "snowflake":
        if not snowflake:
            raise ValueError("snowflake={...} is required when warehouse='snowflake'")
        for key in ("conn_id", "warehouse"):
            if key not in snowflake:
                raise ValueError(f"snowflake['{key}'] is required")
    else:  # pragma: no cover - guarded by Literal
        raise ValueError(f"Unsupported warehouse: {warehouse!r}")

    project_path_str = str(dbt_project_path)
    dbt_executable = str(venv_execution_config.dbt_executable_path)
    vars_field = _vars_field_name(warehouse)
    param_field = _param_field_name(warehouse)

    merged_default_args = {
        **DEFAULT_DEFAULT_ARGS,
        "on_failure_callback": task_failure_alert,
        **(default_args or {}),
    }
    merged_default_args.setdefault("on_failure_callback", task_failure_alert)

    tags = ["dbt", f"tenant:{tenant}", *(extra_tags or [])]

    params = build_standard_params(
        warehouse=warehouse,
        default_dataset_or_schema=default_dataset_or_schema,
        target_choices=target_choices,
    )

    source_tables = discover_source_tables_with_tests(
        project_path_str,
        sources_yml_candidates=sources_yml_candidates,
        recursive_tests=recursive_tests,
    )

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        tags=tags,
        max_active_tasks=max_active_tasks,
        render_template_as_native_obj=True,
        default_args=merged_default_args,
        params=params,
    )
    def _build():
        @task(task_id=PREPARE_TASK_ID)
        def prepare_dbt_args(**context):
            params = context["params"]
            dbt_vars: dict[str, Any] = {}

            target_value = params.get(param_field) or default_dataset_or_schema
            dbt_vars[vars_field] = target_value

            dbt_vars["execution_ts"] = (
                params.get("execution_ts")
                or _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
            )

            backfill_mode = params.get("backfill_mode", "none")
            if backfill_mode == "partial":
                start = params.get("start_ts")
                end = params.get("end_ts")
                if not start or not end:
                    raise ValueError(
                        "backfill_mode=partial requires both start_ts and end_ts"
                    )
                dbt_vars["backfill_start_ts"] = start
                dbt_vars["backfill_end_ts"] = end

            run_context = "normal"
            if backfill_mode == "full":
                run_context = "full_backfill"
            elif backfill_mode == "partial":
                run_context = "partial_backfill"
            dbt_vars["run_context"] = run_context
            run_context_tag = f"{tenant}_{run_context}"
            dbt_vars["run_context_tag"] = run_context_tag

            custom_vars = params.get("vars")
            if custom_vars:
                try:
                    dbt_vars.update(json.loads(custom_vars))
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in 'vars' param: {e}") from e

            selected_models = None
            select_param = (params.get("select") or "").strip()
            exclude_param = (params.get("exclude") or "").strip()

            if select_param or exclude_param:
                logger.info(
                    "Resolving model selection (select=%r, exclude=%r) via dbt ls",
                    select_param,
                    exclude_param,
                )
                ls_profile_config = profile_config_factory("dev", target_value)
                with ls_profile_config.ensure_profile() as (
                    profile_path,
                    profile_env,
                ):
                    cmd = [
                        dbt_executable,
                        "--quiet",
                        "ls",
                        "--resource-type",
                        "model",
                        "--project-dir",
                        project_path_str,
                        "--profiles-dir",
                        str(profile_path.parent),
                        "--profile",
                        dbt_profile_name,
                        "--target",
                        "dev",
                        "--output",
                        "name",
                    ]
                    if select_param:
                        cmd.extend(["--select", select_param])
                    if exclude_param:
                        cmd.extend(["--exclude", exclude_param])

                    env = {**os.environ, **profile_env}
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=120,
                        env=env,
                    )
                if result.returncode != 0:
                    raise RuntimeError(
                        "dbt ls failed (exit=%d):\nSTDOUT:\n%s\nSTDERR:\n%s"
                        % (result.returncode, result.stdout, result.stderr)
                    )

                selected_models = {
                    line.split(".")[-1]
                    for line in result.stdout.strip().splitlines()
                    if line.strip()
                }
                logger.info(
                    "Selection resolved to %d model(s): %s",
                    len(selected_models),
                    sorted(selected_models),
                )

            return {
                param_field: target_value,
                "vars": dbt_vars,
                "full_refresh": backfill_mode == "full",
                "selected_models": selected_models,
                "run_context": run_context,
                "run_context_tag": run_context_tag,
            }

        dbt_args = prepare_dbt_args()

        profile_config = profile_config_factory("dev", default_dataset_or_schema)

        with TaskGroup("source_tests") as source_test_group:
            if not source_tables:
                EmptyOperator(task_id="no_source_tests")
            for source_name, table_name in source_tables:
                DbtTestLocalOperator(
                    task_id=f"test_{source_name}_{table_name}",
                    profile_config=profile_config,
                    project_dir=project_path_str,
                    dbt_executable_path=dbt_executable,
                    select=[f"source:{source_name}.{table_name}"],
                    vars=(
                        "{{ ti.xcom_pull(task_ids='"
                        + PREPARE_TASK_ID
                        + "')['vars'] }}"
                    ),
                    install_deps=True,
                    pre_execute=skip_tests_if_disabled,
                )

        manifest_path = Path(project_path_str) / "target" / "manifest.json"
        dbt_models = DbtTaskGroup(
            group_id="dbt_models",
            project_config=ProjectConfig(
                dbt_project_path=project_path_str,
                manifest_path=str(manifest_path) if manifest_path.exists() else None,
            ),
            profile_config=profile_config,
            execution_config=venv_execution_config,
            render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH),
            operator_args={
                "vars": (
                    "{{ ti.xcom_pull(task_ids='"
                    + PREPARE_TASK_ID
                    + "')['vars'] }}"
                ),
                "full_refresh": (
                    "{{ ti.xcom_pull(task_ids='"
                    + PREPARE_TASK_ID
                    + "')['full_refresh'] }}"
                ),
                "install_deps": True,
                "pre_execute": skip_unselected,
                "trigger_rule": "none_failed",
            },
        )

        source_tests_gate = EmptyOperator(
            task_id="source_tests_gate",
            trigger_rule="none_failed",
        )

        if warehouse == "bigquery":
            from sundial_airflow.reports.bigquery import make_report_task

            report_task = make_report_task(
                bq_project=bigquery["project"],
                bq_location=bigquery["location"],
                gcp_conn_id=bigquery["gcp_conn_id"],
            )
        else:
            from sundial_airflow.reports.snowflake import make_report_task

            report_task = make_report_task(
                snowflake_conn_id=snowflake["conn_id"],
                warehouse=snowflake["warehouse"],
            )

        pipeline_report = report_task()

        # Optional pre-tasks (e.g. ami_dbt's S3 -> Snowflake EMR ingest) run
        # serially before ``prepare_dbt_args``. ``pre_tasks`` items are
        # zero-arg factories that build the TaskFlow task instance.
        pre_task_chain = [factory() for factory in pre_tasks or []]
        for prev, nxt in zip(pre_task_chain, pre_task_chain[1:]):
            prev >> nxt
        if pre_task_chain:
            pre_task_chain[-1] >> dbt_args

        (
            dbt_args
            >> source_test_group
            >> source_tests_gate
            >> dbt_models
            >> pipeline_report
        )

    return _build()
