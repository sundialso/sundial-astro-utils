"""Chunked model task groups for unified dbt DAGs."""
from __future__ import annotations

import json
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Any, Callable

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from cosmos.operators.local import DbtTestLocalOperator

from sundial_airflow.backfill.manifest_parser import CHUNKED, BackfillModel
from sundial_airflow.hooks import (
    PREPARE_TASK_ID,
    skip_chunked_model_test,
)

logger = logging.getLogger(__name__)

_MAX_CHUNK_WORKERS = 32


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

    def _make_model_tasks(model_name: str) -> tuple[Any, Any]:
        @task(task_id="run", trigger_rule="none_failed")
        def run(**context: Any) -> None:
            """Run one incremental pass or parallel chunk windows."""
            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            params = context.get("params", {})
            if params.get("skip_tests") or params.get("empty"):
                from airflow.exceptions import AirflowSkipException

                raise AirflowSkipException("Skipped (skip_tests or empty mode)")

            selected_models = prep.get("selected_models")
            if selected_models is not None and model_name not in selected_models:
                from airflow.exceptions import AirflowSkipException

                raise AirflowSkipException(f"Model '{model_name}' not in selection")

            run_plan = prep.get("run_plan") or {}
            plan = run_plan.get(model_name)
            if plan is None:
                from airflow.exceptions import AirflowSkipException

                raise AirflowSkipException(f"No run plan for '{model_name}'")

            base_vars = dict(prep.get("vars") or {})
            units = (prep.get("chunk_units") or {}).get(model_name, [])
            disposition = plan.get("disposition")

            if disposition == "chunked" and units:
                logger.info(
                    "run %r: chunked path with %d chunk(s): %s",
                    model_name,
                    len(units),
                    ", ".join(u["chunk_id"] for u in units),
                )

                def _run_chunk(unit: dict[str, str]) -> None:
                    chunk_id = unit["chunk_id"]
                    chunk_vars = dict(base_vars)
                    chunk_vars[start_var] = unit["chunk_start"]
                    chunk_vars[end_var] = unit["chunk_end"]
                    chunk_vars["backfill_chunk_id"] = chunk_id
                    chunk_vars["chunk_key"] = chunk_id
                    chunk_vars["run_group_id"] = (
                        f"{context['dag_run'].run_id}:{model_name}:{chunk_id}"
                    )
                    logger.info(
                        "run %r chunk=%s window=%s..%s",
                        model_name,
                        chunk_id,
                        unit["chunk_start"],
                        unit["chunk_end"],
                    )
                    _invoke(chunk_vars, model_name)

                workers = min(len(units), _MAX_CHUNK_WORKERS)
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = [pool.submit(_run_chunk, unit) for unit in units]
                    for future in as_completed(futures):
                        future.result()
                return

            logger.info("run %r: incremental path (disposition=%s)", model_name, disposition)
            incremental_vars = dict(base_vars)
            incremental_vars["chunk_key"] = "full"
            incremental_vars["run_group_id"] = context["dag_run"].run_id
            _invoke(
                incremental_vars,
                model_name,
                full_refresh=bool(prep.get("full_refresh")),
            )

        run_task = run()
        return run_task, run_task

    for model in order:
        if model.kind != CHUNKED:
            continue

        with TaskGroup(group_id=model.name, parent_group=parent_group) as tg:
            run_task, _ = _make_model_tasks(model.name)

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
                install_deps=False,
                trigger_rule="none_failed",
                pre_execute=partial(skip_chunked_model_test, model_name=model.name),
            )
            run_task >> test_task

        upstreams = [
            models_by_key[k].name
            for k in model.depends_on
            if k in models_by_key and models_by_key[k].kind == CHUNKED
        ]
        if upstreams:
            for up in upstreams:
                if up in test_tasks:
                    test_tasks[up] >> run_task
        else:
            upstream_task >> run_task

        model_groups[model.name] = tg
        test_tasks[model.name] = test_task
        run_entry_tasks[model.name] = run_task

    return model_groups, test_tasks, run_entry_tasks
