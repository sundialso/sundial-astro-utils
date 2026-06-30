"""Pre-execute hooks that honour run-time DAG params (``skip_tests``, ``empty``,
``select``, ``exclude``) without re-rendering the task graph per run."""
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
    """``pre_execute`` hook for Cosmos model tasks: inject ``--empty`` in empty
    mode, skip tests when ``skip_tests``/``empty`` is set, and skip models
    outside the resolved ``select``/``exclude`` set.
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


def _chunked_run_plan(context, model_name: str) -> dict | None:
    """Load the run-plan entry for one chunked model."""
    prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
    return (prep.get("run_plan") or {}).get(model_name)


def skip_chunked_run(context, model_name: str) -> None:
    """Skip a mapped chunk run when tests/empty mode is on or model is unselected."""
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Skipped (skip_tests or empty mode)")

    prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
    selected_models = prep.get("selected_models")
    if selected_models is not None and model_name not in selected_models:
        raise AirflowSkipException(f"Model '{model_name}' not in selection")

    if _chunked_run_plan(context, model_name) is None:
        raise AirflowSkipException(f"No run plan for '{model_name}'")


def skip_chunked_precreate(context, model_name: str) -> None:
    """Skip the empty pre-create unless this model will fan out into chunks.

    The pre-create only protects the parallel chunk fan-out (and rebuilds the
    table on full backfill), so it is a no-op for single/disabled runs.
    """
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Skipped (skip_tests or empty mode)")

    prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
    selected_models = prep.get("selected_models")
    if selected_models is not None and model_name not in selected_models:
        raise AirflowSkipException(f"Model '{model_name}' not in selection")

    plan = _chunked_run_plan(context, model_name)
    if plan is None:
        raise AirflowSkipException(f"No run plan for '{model_name}'")
    if plan.get("disposition") != "chunked":
        raise AirflowSkipException(
            f"Pre-create skipped for '{model_name}' "
            f"(disposition={plan.get('disposition')})"
        )


def skip_chunked_incremental(context, model_name: str) -> None:
    """Skip incremental run when the plan selected chunked mode."""
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Skipped (skip_tests or empty mode)")

    prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
    selected_models = prep.get("selected_models")
    if selected_models is not None and model_name not in selected_models:
        raise AirflowSkipException(f"Model '{model_name}' not in selection")

    plan = _chunked_run_plan(context, model_name)
    if plan is None:
        raise AirflowSkipException(f"No run plan for '{model_name}'")
    if plan.get("disposition") != "single":
        raise AirflowSkipException(
            f"Incremental run skipped for '{model_name}' "
            f"(disposition={plan.get('disposition')})"
        )


def skip_chunked_model_test(context, model_name: str) -> None:
    """Skip chunked-model tests when tests are off or the model is unselected."""
    params = context.get("params", {})
    if params.get("skip_tests") or params.get("empty"):
        raise AirflowSkipException("Tests skipped (skip_tests or empty mode)")

    prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
    selected_models = prep.get("selected_models")
    if selected_models is not None and model_name not in selected_models:
        raise AirflowSkipException(f"Model '{model_name}' not in selection")

    if model_name not in (prep.get("run_plan") or {}):
        raise AirflowSkipException(f"No run plan for '{model_name}'")
