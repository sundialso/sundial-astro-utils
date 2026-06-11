"""Pre-execute hooks shared across Sundial dbt DAGs.

These callbacks live on every Cosmos task in a tenant DAG and are responsible
for honouring the run-time DAG params (``skip_tests``, ``empty``, ``select``,
``exclude``) without us having to materialise different task graphs per run.
"""
from __future__ import annotations

from functools import partial
from typing import Iterable

from airflow.exceptions import AirflowSkipException

PREPARE_TASK_ID = "prepare_dbt_args"


def skip_tests_if_disabled(context) -> None:
    """``pre_execute`` hook for source-test tasks.

    Skips when the DAG was triggered with ``skip_tests=True`` or
    ``empty=True``.
    """
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Tests skipped (skip_tests or empty mode)")


def _skip_source_test(dependent_models: frozenset[str], context) -> None:
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Tests skipped (skip_tests or empty mode)")

    args = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID)
    if args is None:
        return
    selected_models = args.get("selected_models")
    if selected_models is None:
        return

    if not (dependent_models & set(selected_models)):
        raise AirflowSkipException(
            "No selected models depend on this source"
        )


def make_source_test_skip_hook(dependent_models: Iterable[str]):
    """Build a ``pre_execute`` hook for a source-test task.

    Skips when ``skip_tests`` / ``empty`` is set, or when a model selection
    is active and none of ``dependent_models`` were selected.
    """
    return partial(_skip_source_test, frozenset(dependent_models))


def skip_unselected(context) -> None:
    """``pre_execute`` hook for Cosmos model tasks.

    Three responsibilities:

    1. Inject ``--empty`` into ``dbt_cmd_flags`` when ``empty=True`` so dbt
       runs each model with ``LIMIT 0``.
    2. Skip test tasks when ``skip_tests`` / ``empty`` is set.
    3. Skip model / test tasks whose model is not in the selection set
       precomputed by ``prepare_dbt_args`` (so ``select`` / ``exclude`` work
       without re-rendering the task graph).
    """
    ti = context["ti"]
    params = context.get("params", {})
    task_leaf = ti.task_id.split(".")[-1]

    is_test_task = task_leaf == "test" or task_leaf.endswith("_test")
    if is_test_task and (params.get("skip_tests") or params.get("empty")):
        raise AirflowSkipException("Tests skipped (skip_tests or empty mode)")

    if params.get("empty") and hasattr(ti.task, "dbt_cmd_flags"):
        flags = list(ti.task.dbt_cmd_flags or [])
        if "--empty" not in flags:
            flags.append("--empty")
            ti.task.dbt_cmd_flags = flags

    args = ti.xcom_pull(task_ids=PREPARE_TASK_ID)
    if args is None:
        return
    selected_models = args.get("selected_models")
    if selected_models is None:
        return

    if task_leaf in ("run", "test"):
        model_name = ti.task_id.split(".")[-2]
    elif task_leaf.endswith("_run"):
        model_name = task_leaf.removesuffix("_run")
    elif task_leaf.endswith("_test"):
        model_name = task_leaf.removesuffix("_test")
    else:
        return

    if model_name not in selected_models:
        raise AirflowSkipException(f"Model '{model_name}' not in selection")
