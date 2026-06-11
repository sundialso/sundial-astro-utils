"""``make_dbt_dag_legacy`` — pre-chunking DAG factory frozen at f43ceabf.

Kept as a rollback path while chunking rolls out. New work belongs in
``dag_factory.make_dbt_dag``; only bugfixes that affect both factories
should be ported here intentionally.
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
    make_source_test_skip_hook,
    skip_unselected,
)
from sundial_airflow.params import build_standard_params
from sundial_airflow.slack_alerts import dag_failure_alert
from sundial_airflow.source_discovery import (
    discover_source_tables_with_tests,
    discover_source_to_models,
)

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


def _collect_run_tasks(group: Any) -> dict[str, Any]:
    """Walk a Cosmos ``DbtTaskGroup`` and return ``{model_name: run_task}``.

    Handles both rendering layouts Cosmos can produce:

    - sub-group form: ``<group>.<model>.run`` / ``<group>.<model>.test``
      (the default with ``TestBehavior.AFTER_EACH``)
    - flat form: ``<group>.<model>_run`` / ``<group>.<model>_test``

    Seeds, snapshots, and singular tests are ignored — they have leaf names
    other than ``run``/``*_run``.
    """
    out: dict[str, Any] = {}

    def _visit(node: Any) -> None:
        children = getattr(node, "children", None)
        if children is None:
            return
        for child in children.values():
            child_children = getattr(child, "children", None)
            if child_children is not None:
                _visit(child)
                continue
            task_id = getattr(child, "task_id", "")
            leaf = task_id.rsplit(".", 1)[-1]
            if leaf == "run":
                parts = task_id.split(".")
                if len(parts) >= 2:
                    out[parts[-2]] = child
            elif leaf.endswith("_run"):
                out[leaf.removesuffix("_run")] = child

    _visit(group)
    return out


def make_dbt_dag_legacy(
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
    default_project: str | None = None,
    default_args: dict[str, Any] | None = None,
    extra_tags: list[str] | None = None,
    pre_tasks: list[Callable[[], Any]] | None = None,
    max_active_tasks: int = 4,
    catchup: bool = False,
    target_choices: list[str] | None = None,
    sources_yml_candidates: list[Path] | None = None,
    recursive_tests: bool = True,
):
    """Build a pre-chunking Sundial dbt DAG (Cosmos-only, no chunk task groups).

    Signature and behaviour match ``make_dbt_dag`` as of commit
    ``f43ceabf8149c0aa4ede9094fe04f6b64086bfc0``.
    """
    if warehouse not in ("bigquery", "snowflake"):  # pragma: no cover
        raise ValueError(f"Unsupported warehouse: {warehouse!r}")

    project_path_str = str(dbt_project_path)
    dbt_executable = str(venv_execution_config.dbt_executable_path)
    vars_field = _vars_field_name(warehouse)
    param_field = _param_field_name(warehouse)

    merged_default_args = {
        **DEFAULT_DEFAULT_ARGS,
        **(default_args or {}),
    }

    tags = ["dbt", "listener_enabled", f"tenant:{tenant}", *(extra_tags or [])]

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
    source_to_models = discover_source_to_models(project_path_str)

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
        on_failure_callback=dag_failure_alert,
    )
    def _build():
        @task(task_id=PREPARE_TASK_ID)
        def prepare_dbt_args(**context):
            params = context["params"]
            dbt_vars: dict[str, Any] = {}

            target_value = params.get(param_field) or default_dataset_or_schema
            dbt_vars[vars_field] = target_value

            if default_project:
                dbt_vars["target_project"] = default_project

            dbt_vars["execution_ts"] = (
                params.get("execution_ts")
                or _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
            )

            # TODO: re-enable cross-run lock when stable.
            dbt_vars["enable_dbt_run_lock"] = False

            dag_run = context.get("dag_run")
            run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
            if run_id:
                dbt_vars["run_group_id"] = run_id

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
                "warehouse": warehouse,
                "full_refresh": backfill_mode == "full",
                "selected_models": selected_models,
                "run_context": run_context,
                "run_context_tag": run_context_tag,
            }

        dbt_args = prepare_dbt_args()

        profile_config = profile_config_factory("dev", default_dataset_or_schema)

        source_test_tasks: dict[tuple[str, str], DbtTestLocalOperator] = {}
        with TaskGroup("source_tests") as source_test_group:
            if not source_tables:
                EmptyOperator(task_id="no_source_tests")
            for source_name, table_name in source_tables:
                dependents = source_to_models.get((source_name, table_name), ())
                source_test_tasks[(source_name, table_name)] = DbtTestLocalOperator(
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
                    pre_execute=make_source_test_skip_hook(dependents),
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

        pre_task_chain = [factory() for factory in pre_tasks or []]
        for prev, nxt in zip(pre_task_chain, pre_task_chain[1:]):
            prev >> nxt
        if pre_task_chain:
            pre_task_chain[-1] >> dbt_args

        dbt_args >> source_test_group
        dbt_args >> dbt_models

        run_tasks_by_model = _collect_run_tasks(dbt_models)
        for (source_name, table_name), test_task in source_test_tasks.items():
            for model_name in source_to_models.get((source_name, table_name), ()):
                run_task = run_tasks_by_model.get(model_name)
                if run_task is None:
                    logger.warning(
                        "Model %r references source %s.%s but no matching "
                        "run task was found in the Cosmos task group; "
                        "skipping wiring.",
                        model_name,
                        source_name,
                        table_name,
                    )
                    continue
                test_task >> run_task

    return _build()
