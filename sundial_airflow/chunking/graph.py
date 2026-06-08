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
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from cosmos.operators.local import DbtTestLocalOperator

from sundial_airflow.backfill.manifest_parser import CHUNKED, BackfillModel
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
) -> tuple[dict[str, TaskGroup], dict[str, Any]]:
    """Build chunked model TaskGroups with dynamically mapped chunk tasks."""
    start_var, end_var = chunk_var_keys
    model_groups: dict[str, TaskGroup] = {}
    test_tasks: dict[str, Any] = {}
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

    @task
    def _plan_chunks(model_name: str, **context: Any) -> list[dict[str, str]]:
        """Return active chunk windows from the run plan."""
        prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
        plan = (prep.get("run_plan") or {}).get(model_name)
        if not plan or plan.get("disposition") != "chunked":
            return []
        return list(plan.get("chunks") or [])

    @task(
        trigger_rule="none_failed",
        map_index_template="{{ chunk_spec['chunk_id'] }}",
    )
    def _run_chunk(
        chunk_spec: dict[str, str],
        model_name: str,
        **context: Any,
    ) -> None:
        """Run one mapped chunk window."""
        skip_chunked_run(context, model_name=model_name)
        prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
        base_vars = dict(prep.get("vars") or {})
        ti = context["ti"]
        chunk_id = chunk_spec["chunk_id"]
        logger.info("Running chunk %s for model %s", chunk_id, model_name)
        base_vars[start_var] = chunk_spec["start"]
        base_vars[end_var] = chunk_spec["end"]
        base_vars["backfill_chunk_id"] = chunk_id
        base_vars["chunk_key"] = chunk_id
        base_vars["run_group_id"] = f"{context['dag_run'].run_id}:{ti.task_id}"
        _invoke(base_vars, model_name)

    @task(trigger_rule="none_failed")
    def _run_incremental(model_name: str, **context: Any) -> None:
        """Run one incremental pass when the run plan is single."""
        skip_chunked_incremental(context, model_name=model_name)
        prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
        base_vars = dict(prep.get("vars") or {})
        base_vars["chunk_key"] = "full"
        base_vars["run_group_id"] = context["dag_run"].run_id
        _invoke(
            base_vars,
            model_name,
            full_refresh=bool(prep.get("full_refresh")),
        )

    for model in order:
        if model.kind != CHUNKED:
            continue

        with TaskGroup(group_id=model.name) as tg:
            planned = _plan_chunks.override(task_id="plan_chunks")(
                model_name=model.name,
            )
            mapped_chunks = _run_chunk.partial(model_name=model.name).expand(
                chunk_spec=planned,
            )
            incremental = _run_incremental.override(task_id="run_incremental")(
                model_name=model.name,
            )

            test_task = DbtTestLocalOperator(
                task_id="test",
                profile_config=profile_config,
                project_dir=project_path_str,
                dbt_executable_path=dbt_executable,
                select=[model.name],
                vars=(
                    "{{ ti.xcom_pull(task_ids='"
                    + PREPARE_TASK_ID
                    + "')['vars'] }}"
                ),
                install_deps=True,
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
                    test_tasks[up] >> tg
        else:
            upstream_task >> tg

        model_groups[model.name] = tg
        test_tasks[model.name] = test_task

    return model_groups, test_tasks
