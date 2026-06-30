"""Chunked model task groups for unified dbt DAGs."""
from __future__ import annotations

import json
import logging
import os
import subprocess
from functools import partial
from pathlib import Path
from typing import Any, Callable

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from cosmos.operators.local import DbtTestLocalOperator

from sundial_airflow.chunking.manifest_parser import CHUNKED, BackfillModel
from sundial_airflow.hooks import (
    PREPARE_TASK_ID,
    skip_chunked_incremental,
    skip_chunked_model_test,
    skip_chunked_run,
)

logger = logging.getLogger(__name__)


def build_chunked_model_graph(
    *,
    order: list[BackfillModel],
    project_path_str: str,
    dbt_executable: str,
    dbt_profile_name: str,
    profile_config: Any,
    profile_config_factory: Callable[[str, str | None], Any],
    chunk_var_keys: tuple[str, str],
    upstream_task: Any,
    parent_group: Any | None = None,
) -> tuple[dict[str, TaskGroup], dict[str, Any], dict[str, Any]]:
    """Build TaskGroups for chunked models."""
    start_var, end_var = chunk_var_keys
    model_groups: dict[str, TaskGroup] = {}
    test_tasks: dict[str, Any] = {}
    run_entry_tasks: dict[str, Any] = {}
    models_by_key = {m.node_key: m for m in order}

    def _invoke(
        extra_vars: dict[str, Any],
        model_name: str,
        *,
        full_refresh: bool = False,
    ) -> None:
        target_value = extra_vars.get("target_dataset") or extra_vars.get("target_schema")
        run_profile = profile_config_factory("dev", target_value)
        with run_profile.ensure_profile() as (profile_path, profile_env):
            cmd = [
                dbt_executable,
                "--no-write-json",
                "run",
                "--select",
                model_name,
                "--vars",
                json.dumps(extra_vars),
                "--project-dir",
                project_path_str,
                "--profiles-dir",
                str(Path(profile_path).parent),
                "--profile",
                dbt_profile_name,
                "--target",
                "dev",
            ]
            if full_refresh:
                cmd.append("--full-refresh")
            env = {**os.environ, **profile_env}
            result = subprocess.run(
                cmd, capture_output=True, text=True, env=env, check=False,
            )
        logger.info("dbt run [%s] stdout:\n%s", model_name, result.stdout)
        if result.stderr:
            logger.warning("dbt run [%s] stderr:\n%s", model_name, result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt run failed for {model_name} (exit={result.returncode})"
            )

    def _make_model_tasks(model_name: str) -> tuple[Any, Any, Any]:
        @task(task_id="chunk_units", trigger_rule="none_failed")
        def chunk_units(**context: Any) -> list[dict[str, str]]:
            """Read mapped chunk kwargs from prepare_dbt_args."""
            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            units = (prep.get("chunk_units") or {}).get(model_name, [])
            if units:
                logger.info(
                    "chunk_units %r: %d chunk(s): %s",
                    model_name,
                    len(units),
                    ", ".join(u["chunk_id"] for u in units),
                )
            else:
                plan = (prep.get("run_plan") or {}).get(model_name) or {}
                logger.info(
                    "chunk_units %r: 0 mapped tasks (disposition=%s)",
                    model_name,
                    plan.get("disposition", "missing"),
                )
            return units

        @task(
            task_id="run_chunk",
            trigger_rule="none_failed",
            map_index_template="{{ chunk_id }}",
        )
        def run_chunk(
            chunk_id: str,
            chunk_start: str | None = None,
            chunk_end: str | None = None,
            **context: Any,
        ) -> None:
            """Run one chunk window."""
            logger.info(
                "Starting run_chunk model=%s chunk=%s window=%s..%s",
                model_name,
                chunk_id,
                chunk_start,
                chunk_end,
            )
            skip_chunked_run(context, model_name=model_name)
            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            base_vars = dict(prep.get("vars") or {})
            for var, val in ((start_var, chunk_start), (end_var, chunk_end)):
                if val is not None:
                    base_vars[var] = val
                else:
                    base_vars.pop(var, None)
            base_vars["backfill_chunk_id"] = chunk_id
            base_vars["chunk_key"] = chunk_id
            base_vars["run_group_id"] = context["dag_run"].run_id
            base_vars["defer_chunk_terminal_status"] = True
            _invoke(
                base_vars,
                model_name,
                full_refresh=bool(prep.get("full_refresh")),
            )

        @task(task_id="run_incremental", trigger_rule="none_failed")
        def run_incremental(**context: Any) -> None:
            """Run one incremental pass."""
            logger.info("Starting run_incremental for model=%s", model_name)
            skip_chunked_incremental(context, model_name=model_name)
            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            base_vars = dict(prep.get("vars") or {})
            base_vars["chunk_key"] = "full"
            base_vars["run_group_id"] = context["dag_run"].run_id
            base_vars["defer_chunk_terminal_status"] = True
            _invoke(
                base_vars,
                model_name,
                full_refresh=bool(prep.get("full_refresh")),
            )

        units = chunk_units()
        mapped = run_chunk.expand_kwargs(units)
        incremental = run_incremental()
        units >> incremental
        return units, mapped, incremental

    for model in order:
        if model.kind != CHUNKED:
            continue

        with TaskGroup(group_id=model.name, parent_group=parent_group) as tg:
            units, mapped_chunks, incremental = _make_model_tasks(model.name)

            test_task = DbtTestLocalOperator(
                task_id="test",
                profile_config=profile_config,
                project_dir=project_path_str,
                dbt_executable_path=dbt_executable,
                select=[model.name],
                vars=(
                    "{{ {**ti.xcom_pull(task_ids='"
                    + PREPARE_TASK_ID
                    + "')['vars'], "
                    "'finalize_chunk_completions': True, "
                    "'run_group_id': dag_run.run_id} }}"
                ),
                install_deps=False,
                trigger_rule="none_failed_min_one_success",
                pre_execute=partial(skip_chunked_model_test, model_name=model.name),
            )
            mapped_chunks >> test_task
            incremental >> test_task

        upstreams = [
            models_by_key[k].name
            for k in model.depends_on
            if k in models_by_key and models_by_key[k].kind == CHUNKED
        ]
        if upstreams:
            for up in upstreams:
                if up in test_tasks:
                    test_tasks[up] >> units
        else:
            upstream_task >> units

        model_groups[model.name] = tg
        test_tasks[model.name] = test_task
        run_entry_tasks[model.name] = units

    return model_groups, test_tasks, run_entry_tasks
