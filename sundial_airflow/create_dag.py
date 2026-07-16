"""``create_dag`` — chunking-enabled entry point each tenant uses to build its DAG.

The factory absorbs every bit of plumbing that's currently duplicated across
the tenant DAGs:

- standard ``params`` set (backfill, select/exclude, skip_tests, ...)
- ``prepare_dbt_args`` task (``--vars`` blob + ``dbt ls`` selection resolution)
- per-source-table ``DbtTestLocalOperator``s, each wired *directly* to the
  models that consume that source (no global ``source_tests_gate`` — a single
  failing source test only blocks its own subtree, not the whole pipeline)
- the Cosmos ``DbtTaskGroup``
- ``report_data_processed`` task (BigQuery / Snowflake variant)
- Slack failure alert wired into ``default_args``

Tenant DAG files reduce to ~25 lines: a single ``create_dag(...)`` call
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
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.operators.local import DbtTestLocalOperator

from sundial_airflow.chunking.manifest_parser import (
    CHUNKED,
    load_backfill_models,
    load_chunking_config,
    topological_order,
)
from sundial_airflow.chunking.chunk_spec import build_chunk_units
from sundial_airflow.chunking.graph import build_chunked_model_graph
from sundial_airflow.chunking.run_plan import _as_datetime, build_run_plan, serialize_run_plan
from sundial_airflow.chunking.watermarks import fetch_partition_watermarks
from sundial_airflow.dbt_runtime import ensure_dbt_deps
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
from sundial_airflow.task_log import log_prepare_dbt_args_summary
from sundial_airflow.warehouses import get_adapter

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


def _validate_backfill_params(
    *, backfill_mode: str, start_ts: Any, end_ts: Any, execution_ts: Any,
) -> None:
    """Validate the run-window params against the selected ``backfill_mode``."""
    has_start = bool(start_ts)
    has_end = bool(end_ts)

    if backfill_mode == "partial":
        if not (has_start and has_end):
            raise ValueError(
                "backfill_mode=partial requires both start_ts and end_ts "
                f"(got start_ts={start_ts!r}, end_ts={end_ts!r})."
            )
        if _as_datetime(start_ts) >= _as_datetime(end_ts):
            raise ValueError(
                "backfill_mode=partial requires start_ts < end_ts "
                f"(got start_ts={start_ts!r}, end_ts={end_ts!r})."
            )
        if execution_ts:
            raise ValueError(
                "backfill_mode=partial does not accept execution_ts "
                f"(got execution_ts={execution_ts!r}); the window is defined "
                "by start_ts and end_ts."
            )
        return

    if has_start or has_end:
        raise ValueError(
            f"backfill_mode={backfill_mode!r} does not accept start_ts/end_ts "
            f"(got start_ts={start_ts!r}, end_ts={end_ts!r}). "
            "Use backfill_mode=partial for an explicit window, otherwise leave "
            "start_ts/end_ts blank and rely on execution_ts."
        )


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


def create_dag(
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
    max_active_tasks: int = 8,
    catchup: bool = False,
    target_choices: list[str] | None = None,
    sources_yml_candidates: list[Path] | None = None,
    recursive_tests: bool = True,
    chunking_config_path: str | Path | None = None,
    warehouse_conn_id: str | None = None,
    chunk_var_keys: tuple[str, str] = ("backfill_start_ts", "backfill_end_ts"),
    scaling_warehouse_name: str | None = None,
):
    """Build and register a fully-wired Sundial dbt DAG.

    See ``README.md`` for an end-to-end usage example. All keyword arguments
    are required unless documented otherwise.

    Parameters
    ----------
    dag_id:
        Airflow DAG id
    tenant:
        Short tenant slug; used in the Slack alert and the
        ``run_context_tag`` (``"<tenant>_normal"``, etc).
    warehouse:
        ``"bigquery"`` or ``"snowflake"``. Controls the ``target_dataset``
        vs ``target_schema`` var key passed to dbt.
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
        The default dataset (BigQuery) or schema (Snowflake) for this tenant.
        Must match the schema/dataset in the tenant's dbt profile (the value
        passed to ``profile_config_factory``). Flows into dbt as
        ``target_dataset`` / ``target_schema`` and drives watermark queries,
        Cosmos runs, and chunked model runs.
    default_project:
        BigQuery project where dbt writes (same value dbt uses for
        ``var('target_project')``). Passed through ``dbt_vars`` in XCom so the
        DbtCompletionsListener can locate the ``dbt_completions`` table on a
        manual UI state change. Optional; the listener no-ops without it.
    default_args:
        Merged on top of :data:`DEFAULT_DEFAULT_ARGS`. The factory wires
        ``dag_failure_alert`` as the DAG-level ``on_failure_callback`` so it
        fires once per failed DAG run rather than on every failed task.
    extra_tags:
        Extra tags appended after ``["dbt", f"tenant:{tenant}"]``.
    pre_tasks:
        Optional list of zero-arg callables that return TaskFlow tasks; they
        run before ``prepare_dbt_args``.
    max_active_tasks, catchup, target_choices, sources_yml_candidates,
    recursive_tests:
        Tuning knobs with sensible defaults; see the implementation.
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

    manifest_path = Path(project_path_str) / "target" / "manifest.json"
    _chunk_models: dict = {}
    _chunk_order: list = []
    _chunked_names: list[str] = []
    if chunking_config_path is not None:
        try:
            _chunk_config = load_chunking_config(chunking_config_path)
            _chunk_models = load_backfill_models(manifest_path, _chunk_config)
            _chunk_order = topological_order(_chunk_models) if _chunk_models else []
            _chunked_names = [m.name for m in _chunk_order if m.kind == CHUNKED]
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Chunking config parse failed for DAG %r: %s", dag_id, exc,
            )
        except Exception:
            raise

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
        @task(task_id=PREPARE_TASK_ID, show_return_value_in_logs=False)
        def prepare_dbt_args(**context):
            start_var, end_var = chunk_var_keys
            params = context["params"]
            dbt_vars: dict[str, Any] = {}

            target_value = params.get(param_field) or default_dataset_or_schema
            dbt_vars[vars_field] = target_value

            if default_project:
                dbt_vars["target_project"] = default_project
                if warehouse == "snowflake":
                    dbt_vars["target_database"] = default_project

            dbt_vars["execution_ts"] = (
                params.get("execution_ts")
                or _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
            )

            # TODO: re-enable cross-run lock when stable.
            dbt_vars["enable_dbt_run_lock"] = False

            # run_group_id ties this run's tasks together for the dbt run-lock;
            # per-chunk fan-out overrides chunk_key per task.
            dag_run = context.get("dag_run")
            run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
            if run_id:
                dbt_vars["run_group_id"] = run_id

            backfill_mode = params.get("backfill_mode", "none")
            start_ts = params.get("start_ts")
            end_ts = params.get("end_ts")

            _validate_backfill_params(
                backfill_mode=backfill_mode,
                start_ts=start_ts,
                end_ts=end_ts,
                execution_ts=params.get("execution_ts"),
            )
            if backfill_mode == "partial":
                dbt_vars[start_var] = start_ts
                dbt_vars[end_var] = end_ts

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
                ls_profile_config = profile_config_factory("dev", target_value)
                with ls_profile_config.ensure_profile() as (
                    profile_path,
                    profile_env,
                ):
                    env = {**os.environ, **profile_env}
                    # `dbt ls` compiles the project, which fails when
                    # packages.yml declares packages not installed in
                    # dbt_packages/. The scheduled path skips this block
                    # entirely (Cosmos installs deps per model task via
                    # install_deps=True), so deps are only needed here, on the
                    # select/exclude path. Run `dbt deps` first to make this
                    # path self-sufficient for tenants that don't bake deps
                    # into their image. Degrade gracefully: if deps fails
                    # (e.g. a transient network error resolving a git package),
                    # fall through to `dbt ls`, which still compiles when
                    # dbt_packages/ is already populated (baked image) and
                    # otherwise fails with its own descriptive error.
                    try:
                        ensure_dbt_deps(
                            dbt_executable, project_path_str, env=env
                        )
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "dbt deps failed; proceeding to dbt ls in case "
                            "dbt_packages/ is already populated.",
                            exc_info=True,
                        )
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

            run_plan: dict = {}
            watermarks: dict[str, Any] = {}
            if _chunked_names:
                exec_raw = dbt_vars.get("execution_ts") or _dt.date.today().isoformat()
                execution_date = _dt.date.fromisoformat(str(exec_raw)[:10])
                plan_models = _chunk_models
                if selected_models is not None:
                    plan_models = {
                        k: v for k, v in _chunk_models.items()
                        if v.name in selected_models
                    }
                watermark_models = [
                    m for m in plan_models.values() if m.kind == CHUNKED
                ]
                watermarks = fetch_partition_watermarks(
                    warehouse=warehouse,
                    conn_id=warehouse_conn_id,
                    dbt_vars=dbt_vars,
                    models=watermark_models,
                )
                plans = build_run_plan(
                    models=plan_models,
                    watermarks=watermarks,
                    backfill_mode=backfill_mode,
                    execution_ts=execution_date,
                    window_start=dbt_vars.get(start_var)
                    if backfill_mode == "partial"
                    else None,
                    window_end=dbt_vars.get(end_var)
                    if backfill_mode == "partial"
                    else None,
                )
                run_plan = serialize_run_plan(plans)

            chunk_units = build_chunk_units(run_plan) if run_plan else {}

            payload = {
                param_field: target_value,
                "vars": dbt_vars,
                # Selects the warehouse adapter in the dbt_completions listener.
                "warehouse": warehouse,
                "full_refresh": backfill_mode == "full",
                "selected_models": selected_models,
                "run_context": run_context,
                "run_context_tag": run_context_tag,
                "run_plan": run_plan,
                "chunk_units": chunk_units,
            }
            log_prepare_dbt_args_summary(
                run_id=run_id,
                params=params,
                param_field=param_field,
                target_value=target_value,
                warehouse=warehouse,
                backfill_mode=backfill_mode,
                run_context=run_context,
                full_refresh=payload["full_refresh"],
                dbt_vars=dbt_vars,
                selected_models=selected_models,
                run_plan=run_plan,
                watermarks=watermarks,
                start_var=start_var,
                end_var=end_var,
            )
            return payload

        # Snowflake-only warehouse autoscaling for backfill runs.
        _scale_warehouse = warehouse == "snowflake"
        _BACKFILL_CONTEXTS = ("full_backfill", "partial_backfill")

        def _scaling_conn_id() -> str:
            return warehouse_conn_id or get_adapter("snowflake").resolve_conn_id()

        @task(task_id="upsize_wh")
        def upsize_wh(**context: Any) -> dict[str, Any]:
            """Grow the warehouse one size on partial/full backfill runs."""
            from sundial_airflow.warehouse_scaling import (
                get_warehouse_size,
                next_warehouse_size,
                resolve_warehouse_name,
                set_warehouse_size,
            )

            result: dict[str, Any] = {
                "upsized": False,
                "warehouse": None,
                "original_size": None,
                "new_size": None,
            }
            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            if prep.get("run_context") not in _BACKFILL_CONTEXTS:
                raise AirflowSkipException(
                    "Not a partial/full backfill run; skipping warehouse upsize."
                )

            try:
                conn_id = _scaling_conn_id()
                wh_name = scaling_warehouse_name or resolve_warehouse_name(conn_id)
                if not wh_name:
                    logger.warning("Could not resolve Snowflake warehouse; skipping upsize.")
                    return result
                current = get_warehouse_size(conn_id, wh_name)
                target = next_warehouse_size(current)
                result.update(warehouse=wh_name, original_size=current)
                if not target:
                    logger.info(
                        "Warehouse %s is %s; no larger size available.", wh_name, current
                    )
                    return result
                set_warehouse_size(conn_id, wh_name, target)
                result.update(upsized=True, new_size=target)
                logger.info("Upsized warehouse %s: %s → %s.", wh_name, current, target)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "Warehouse upsize failed; continuing at current size.",
                    exc_info=True,
                )
            return result

        @task(task_id="downsize_wh", trigger_rule="all_done")
        def downsize_wh(**context: Any) -> None:
            """Restore the original warehouse size on partial/full backfill runs."""
            from sundial_airflow.warehouse_scaling import set_warehouse_size

            prep = context["ti"].xcom_pull(task_ids=PREPARE_TASK_ID) or {}
            if prep.get("run_context") not in _BACKFILL_CONTEXTS:
                raise AirflowSkipException(
                    "Not a partial/full backfill run; skipping warehouse downsize."
                )

            info = context["ti"].xcom_pull(task_ids="upsize_wh") or {}
            if not info.get("upsized"):
                logger.info("Warehouse was not upsized; nothing to downsize.")
                return
            try:
                set_warehouse_size(
                    _scaling_conn_id(), info["warehouse"], info["original_size"]
                )
                logger.info(
                    "Downsized warehouse %s back to %s.",
                    info["warehouse"],
                    info["original_size"],
                )
            except Exception:  # noqa: BLE001
                logger.warning("Warehouse downsize failed.", exc_info=True)

        # ``prefix_group_id=False`` keeps the prepare task id as ``prepare_dbt_args``
        # so XCom pulls and the completions listener keep resolving it.
        with TaskGroup("preprocess", prefix_group_id=False) as preprocess:
            dbt_args = prepare_dbt_args()
            if _scale_warehouse:
                dbt_args >> upsize_wh()

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
                    # ``none_failed`` so a skipped ``upsize_wh`` (non-backfill
                    # run) does not cascade-skip source tests.
                    trigger_rule="none_failed",
                    pre_execute=make_source_test_skip_hook(dependents),
                )

        cosmos_render = RenderConfig(
            test_behavior=TestBehavior.AFTER_EACH,
            exclude=_chunked_names or None,
        )
        dbt_models = DbtTaskGroup(
            group_id="dbt_models",
            project_config=ProjectConfig(
                dbt_project_path=project_path_str,
                manifest_path=str(manifest_path) if manifest_path.exists() else None,
            ),
            profile_config=profile_config,
            execution_config=venv_execution_config,
            render_config=cosmos_render,
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
                "install_deps": False,
                "pre_execute": skip_unselected,
                # ``none_failed`` lets a model run when its upstream source
                # test was *skipped* (skip_tests / empty mode) but still
                # propagates ``upstream_failed`` if the test actually failed.
                "trigger_rule": "none_failed",
            },
        )

        # Optional pre-tasks (e.g. ami_dbt's S3 -> Snowflake EMR ingest) run
        # serially before ``prepare_dbt_args``. ``pre_tasks`` items are
        # zero-arg factories that build the TaskFlow task instance.
        pre_task_chain = [factory() for factory in pre_tasks or []]
        for prev, nxt in zip(pre_task_chain, pre_task_chain[1:]):
            prev >> nxt
        if pre_task_chain:
            pre_task_chain[-1] >> dbt_args

        # Post-processing runs after all dbt work. Add further post-process
        # tasks inside this group as needed.
        postprocess = None
        if _scale_warehouse:
            with TaskGroup("postprocess", prefix_group_id=False) as postprocess:
                downsize_wh()

        # Per-source fan-out (no global gate):
        #
        #   prepare_dbt_args ─┬─ test_s_t ──→ models that select source(s,t)
        #                     └─ <models with no tested source> (run after prepare)
        #
        # A failing ``test_s_t`` only flips ``upstream_failed`` on the models
        # that consume that source; sibling branches are unaffected. Models
        # with no source dependency (or whose sources have no tests) just run
        # after ``prepare_dbt_args``.
        preprocess >> source_test_group
        preprocess >> dbt_models

        cosmos_runs = _collect_run_tasks(dbt_models)
        run_tasks_by_model = dict(cosmos_runs)
        chunk_groups: dict[str, Any] = {}
        chunk_tests: dict[str, Any] = {}
        chunk_plans: dict[str, Any] = {}

        if _chunk_order:
            chunk_groups, chunk_tests, chunk_plans = build_chunked_model_graph(
                order=_chunk_order,
                project_path_str=project_path_str,
                dbt_executable=dbt_executable,
                dbt_profile_name=dbt_profile_name,
                profile_config=profile_config,
                profile_config_factory=profile_config_factory,
                chunk_var_keys=chunk_var_keys,
                upstream_task=preprocess,
                parent_group=dbt_models,
            )
            models_by_key = {m.node_key: m for m in _chunk_order}

            for chunked in _chunk_order:
                if chunked.kind != CHUNKED or chunked.name not in chunk_plans:
                    continue
                planned = chunk_plans[chunked.name]
                for dep_key in chunked.depends_on:
                    dep = models_by_key.get(dep_key)
                    if dep is None or dep.name in chunk_groups:
                        continue
                    cosmos_run = cosmos_runs.get(dep.name)
                    if cosmos_run is not None:
                        cosmos_run >> planned

            for model in _chunk_order:
                cosmos_run = cosmos_runs.get(model.name)
                if cosmos_run is None:
                    continue
                for dep_key in model.depends_on:
                    dep = models_by_key.get(dep_key)
                    if dep is None or dep.name not in chunk_tests:
                        continue
                    chunk_tests[dep.name] >> cosmos_run

        for (source_name, table_name), test_task in source_test_tasks.items():
            for model_name in source_to_models.get((source_name, table_name), ()):
                if model_name in chunk_groups:
                    test_task >> chunk_groups[model_name]
                    continue
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

        # Wire post-processing only after the *complete* dbt graph is built.
        # ``dbt_models >> postprocess`` binds the group's leaves at call time, so
        # this must run after ``build_chunked_model_graph`` has attached the
        # chunk groups (``parent_group=dbt_models``) and after source-test
        # wiring, so ``downsize_wh`` (``trigger_rule="all_done"``) waits for
        # chunked models and source tests before restoring the warehouse size.
        if postprocess is not None:
            dbt_models >> postprocess
            source_test_group >> postprocess

    return _build()
