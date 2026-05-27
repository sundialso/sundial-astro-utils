"""Airflow listener that reconciles ``dbt_completions`` after a **manual**
state change (UI "Mark Success" / "Mark Failed", or an API call).

Division of labour
------------------
- **Natural runs** (scheduled, manual-trigger, backfill): the dbt macros in
  ``sundial_dbt_shared`` (``pre-hook`` / ``post-hook`` / ``on-run-end``)
  write every row to ``dbt_completions`` from *inside* the dbt invocation.
  The listener stays out of the way.

- **Manual UI/API state changes**: dbt isn't re-invoked, so the macros
  can't help — the table would go stale. The listener catches this exact
  case and writes the reconciliation row.

How the split is enforced
-------------------------
``on_task_instance_success`` / ``on_task_instance_failed`` fire on **all**
state transitions in Airflow 3.x. We gate on ``previous_state``:

  - ``previous_state == "running"``        → natural completion → **skip**
  - anything else (failed / success /
    skipped / upstream_failed / …)         → manual intervention → reconcile

That value is unambiguous because the *only* way a task reaches success or
failed from ``running`` is through Airflow's executor actually running it.
Manual "Mark Success" / "Mark Failed" always come from some other prior
state (the state the task was previously stuck in).

DAG-run-level hooks are intentionally **not** implemented. They fire on
natural DAG completion too, with no ``previous_state`` to gate on — they
would re-introduce the noise we just eliminated.

Status semantics (mirror ``log_run_results``)
---------------------------------------------
Per model, both Airflow run and test states are inspected:

  - run failed / upstream_failed   OR test failed / upstream_failed → ``failed``
  - run success and (no test OR test success)                      → ``succeeded``
  - tests-only invocation, test success, no/skipped run            → ``succeeded``
  - anything still in-flight or skipped                            → no row

Same three-column MERGE key as the dbt macro
(``model_name + execution_ts + status``) — idempotent against any concurrent
or repeated writes.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

# Must match dag_factory.PREPARE_TASK_ID; duplicated here to avoid a circular import.
PREPARE_TASK_ID = "prepare_dbt_args"

# Table name is a Sundial-wide convention; the table always sits in the same
# dataset the tenant's dbt models write to, so we hardcode it here instead of
# plumbing it through every DAG.
COMPLETIONS_TABLE = "dbt_completions"

# Override at deploy time with the env var if the BQ connection isn't the
# Airflow default.
DEFAULT_BQ_CONN_ID = os.environ.get(
    "SUNDIAL_DBT_COMPLETIONS_BQ_CONN_ID", "google_cloud_default"
)

# ``make_dbt_dag`` parses ``vars.target_project`` from dbt_project.yml at
# build time and stashes it on the DAG as a ``dbt_completions_project:<value>``
# tag. Tags survive Airflow 3.x serialization — the API server sees them in
# the SerializedDAG it loads from the metadata DB. (``user_defined_macros``
# does NOT survive serialization, which is why we don't use it.)
_TARGET_PROJECT_TAG_PREFIX = "dbt_completions_project:"


def _tag_strings(dag: Any) -> list[str]:
    """Normalise tags to a list of strings — they can come back as a set or
    list of either plain strings or ``DagTag`` model objects."""
    out: list[str] = []
    for tag in getattr(dag, "tags", None) or []:
        name = getattr(tag, "name", None) or tag
        if isinstance(name, str):
            out.append(name)
    return out


def _get_target_project(dag_run: Any) -> str | None:
    dag = _load_dag(dag_run)
    for tag in _tag_strings(dag):
        if tag.startswith(_TARGET_PROJECT_TAG_PREFIX):
            return tag[len(_TARGET_PROJECT_TAG_PREFIX):]
    return None


_AIRFLOW_FAILED_STATES = {"failed", "upstream_failed"}
_AIRFLOW_SUCCESS_STATES = {"success"}


def _is_natural_completion(previous_state: Any) -> bool:
    """True only if the task transitioned from ``running`` — i.e. the Airflow
    executor actually ran it. False for manual UI/API state changes AND for
    ``None`` (we'd rather over-write idempotent rows than miss a real
    manual intervention; the MERGE is safe to repeat).
    """
    if previous_state is None:
        return False
    value = getattr(previous_state, "value", previous_state)
    return value == "running"


# ---------------------------------------------------------------------------
# Identification & target discovery
# ---------------------------------------------------------------------------
def _load_dag(dag_run: Any) -> Any | None:
    """Fetch the full deserialized DAG via ``SerializedDagModel`` — the
    canonical Airflow 3.x way to read a DAG from the metadata DB without
    parsing files. ``dag_run.dag`` inside the listener context returns a
    partially-populated DAG (``tags`` / ``user_defined_macros`` may be
    empty), so we go to the serialized table directly."""
    try:
        from airflow.models.serialized_dag import SerializedDagModel
        dag = SerializedDagModel.get_dag(dag_run.dag_id)
        if dag is not None:
            return dag
    except Exception as exc:
        log.debug("SerializedDagModel.get_dag(%s) failed: %s", dag_run.dag_id, exc)
    # Fallback: ``dag_run.dag``. Tags may be missing but it's better than nothing.
    try:
        return getattr(dag_run, "dag", None)
    except Exception as exc:
        log.debug("dag_run.dag fallback access failed: %s", exc)
        return None


def _is_dbt_dag(dag_run: Any) -> bool:
    dag = _load_dag(dag_run)
    return "dbt" in _tag_strings(dag)


def _get_completions_target(dag_run: Any) -> tuple[str, str, str] | None:
    """Return ``(project, dataset, execution_ts)`` for the row to write.

    - ``project`` comes from the DAG's ``user_defined_macros`` (the value
      ``make_dbt_dag`` stashed there from ``dbt_project.yml`` at build
      time). Static per tenant.
    - ``dataset`` and ``execution_ts`` come from the ``prepare_dbt_args``
      XCom return (per-run values, possibly overridden via DAG params).

    Returns ``None`` if any required field is missing (Snowflake tenants
    with no ``target_project``, non-dbt DAGs, or DagRuns whose XCom has
    been pruned).
    """
    project = _get_target_project(dag_run)
    if not project:
        return None

    try:
        ti = dag_run.get_task_instance(PREPARE_TASK_ID)
    except Exception as exc:
        log.debug("%s: prepare_dbt_args lookup failed: %s", dag_run.dag_id, exc)
        return None
    if ti is None:
        return None
    try:
        payload = ti.xcom_pull(task_ids=PREPARE_TASK_ID)
    except Exception as exc:
        log.debug("%s: xcom_pull failed: %s", dag_run.dag_id, exc)
        return None
    if not isinstance(payload, dict):
        return None
    dbt_vars = payload.get("vars") or {}
    dataset = dbt_vars.get("target_dataset")
    execution_ts = dbt_vars.get("execution_ts")
    if not (dataset and execution_ts):
        return None
    return project, dataset, execution_ts


def _parse_model_task(task_id: str) -> tuple[str, str] | None:
    """Return ``(model_name, role)`` where role is ``"run"`` or ``"test"``,
    or ``None`` for tasks that aren't Cosmos model run/test tasks."""
    parts = task_id.split(".")
    leaf = parts[-1]
    # Sub-group form: <group>.<model>.run / <group>.<model>.test
    if leaf in ("run", "test") and len(parts) >= 2:
        return parts[-2], leaf
    # Flat form: <group>.<model>_run / <group>.<model>_test.
    # Exclude source-test tasks ("source_tests.test_<source>_<table>" — leaf
    # starts with "test_", which a model test task never does).
    if leaf.endswith("_run"):
        return leaf[: -len("_run")], "run"
    if leaf.endswith("_test") and not leaf.startswith("test_"):
        return leaf[: -len("_test")], "test"
    return None


# ---------------------------------------------------------------------------
# Status derivation (mirrors log_run_results)
# ---------------------------------------------------------------------------
def _derive_status(states: dict[str, str]) -> str | None:
    run_state = states.get("run")
    test_state = states.get("test")

    if run_state in _AIRFLOW_FAILED_STATES or test_state in _AIRFLOW_FAILED_STATES:
        return "failed"
    if run_state in _AIRFLOW_SUCCESS_STATES and (
        test_state is None or test_state in _AIRFLOW_SUCCESS_STATES
    ):
        return "succeeded"
    if test_state in _AIRFLOW_SUCCESS_STATES and run_state in (None, "skipped"):
        return "succeeded"
    return None


def _states_for_model(dag_run: Any, model_name: str) -> dict[str, str]:
    """Collect ``{role: airflow_state}`` for one model across all TIs in the run."""
    out: dict[str, str] = {}
    for ti in dag_run.get_task_instances():
        parsed = _parse_model_task(ti.task_id)
        if parsed is None:
            continue
        m, role = parsed
        if m == model_name:
            out[role] = ti.state or ""
    return out


# ---------------------------------------------------------------------------
# BigQuery MERGE
# ---------------------------------------------------------------------------
def _upsert(
    table_fqn: str,
    execution_ts: str,
    rows: list[dict[str, str]],
    bq_conn_id: str,
) -> None:
    if not rows:
        return

    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
    except ImportError:
        log.warning(
            "apache-airflow-providers-google not installed; dbt_completions "
            "reconciliation skipped for %s",
            table_fqn,
        )
        return

    using_clauses: list[str] = []
    params: list[Any] = [ScalarQueryParameter("e", "STRING", execution_ts)]
    for i, row in enumerate(rows):
        using_clauses.append(
            f"SELECT @m{i} AS model_name, @e AS execution_ts, "
            f"@s{i} AS status, CURRENT_TIMESTAMP() AS updated_at"
        )
        params.append(ScalarQueryParameter(f"m{i}", "STRING", row["model_name"]))
        params.append(ScalarQueryParameter(f"s{i}", "STRING", row["status"]))

    sql = f"""
    MERGE `{table_fqn}` T
    USING (
        {" UNION ALL ".join(using_clauses)}
    ) S
    ON T.model_name = S.model_name
       AND T.execution_ts = S.execution_ts
       AND T.status = S.status
    WHEN MATCHED THEN UPDATE SET updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, updated_at)
      VALUES (S.model_name, S.execution_ts, S.status, S.updated_at)
    """

    hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False)
    client = hook.get_client()
    client.query(sql, job_config=QueryJobConfig(query_parameters=params)).result()


# ---------------------------------------------------------------------------
# Reconciliation entry points
# ---------------------------------------------------------------------------
def _get_dag_run(task_instance: Any) -> Any | None:
    """Best-effort DagRun lookup.

    Airflow 3.x passes a full ``TaskInstance`` (with ``.dag_run`` /
    ``.get_dagrun()``) for UI/API-triggered transitions, but a leaner
    ``RuntimeTaskInstance`` for normal task execution — and the latter may
    not expose either accessor. That's fine: natural-execution writes are
    already handled by the dbt ``on-run-end`` macro, so returning ``None``
    here just makes the task-level listener a no-op on the path that
    doesn't need it. Manual UI changes always deliver a ``TaskInstance``.
    """
    # Guard the attribute access too: ``TaskInstance.dag_run`` is a SQLAlchemy
    # relationship that can raise (e.g. DetachedInstanceError) outside a live
    # session — getattr only catches AttributeError.
    try:
        dr = getattr(task_instance, "dag_run", None)
    except Exception as exc:
        log.debug("dag_run attribute access failed for %s: %s", task_instance.task_id, exc)
        dr = None
    if dr is not None:
        return dr
    getter = getattr(task_instance, "get_dagrun", None)
    if callable(getter):
        try:
            return getter()
        except Exception as exc:
            log.debug("get_dagrun failed for %s: %s", task_instance.task_id, exc)
    return None


def reconcile_model(task_instance: Any) -> None:
    """Reconcile dbt_completions for the single model owning ``task_instance``.

    Called from the task-level listeners. Looks up the *sibling* task (the
    other of run/test) so the verdict reflects both halves of the model's
    lifecycle even when only one task just transitioned.
    """
    parsed = _parse_model_task(task_instance.task_id)
    if parsed is None:
        log.info("DbtCompletionsListener: task_id=%s is not a Cosmos model task, skipping", task_instance.task_id)
        return
    model_name, _role = parsed

    dag_run = _get_dag_run(task_instance)
    if dag_run is None:
        log.info("DbtCompletionsListener: could not resolve dag_run for %s, skipping", task_instance.task_id)
        return
    if not _is_dbt_dag(dag_run):
        log.info("DbtCompletionsListener: dag %s is not tagged 'dbt', skipping", dag_run.dag_id)
        return

    target = _get_completions_target(dag_run)
    if not target:
        log.info(
            "DbtCompletionsListener: no completions target resolved for dag=%s "
            "(missing user_defined_macros._dbt_completions_project or XCom from prepare_dbt_args)",
            dag_run.dag_id,
        )
        return
    project, dataset, execution_ts = target

    states = _states_for_model(dag_run, model_name)
    status = _derive_status(states)
    if status is None:
        log.debug(
            "%s/%s: no terminal status from states=%s; skipping",
            dag_run.dag_id,
            model_name,
            states,
        )
        return

    table_fqn = f"{project}.{dataset}.{COMPLETIONS_TABLE}"
    log.info(
        "%s/%s: writing status=%s into %s (states=%s)",
        dag_run.dag_id,
        model_name,
        status,
        table_fqn,
        states,
    )
    try:
        _upsert(
            table_fqn,
            execution_ts,
            [{"model_name": model_name, "status": status}],
            DEFAULT_BQ_CONN_ID,
        )
    except Exception:
        log.exception("%s/%s: dbt_completions upsert failed", dag_run.dag_id, model_name)


# ---------------------------------------------------------------------------
# Listener + plugin
# ---------------------------------------------------------------------------
class DbtCompletionsListener:
    """Pluggy-marked hook impls. Registered via ``SundialDbtCompletionsPlugin``.

    Both hooks gate on ``previous_state`` so we only act on manual UI/API
    state changes — natural completions are handled by the dbt macros.

    Signatures accept ``**kwargs`` so they tolerate Airflow versions that
    pass extra arguments (e.g. ``error``, ``session``) we don't need.
    """

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, **kwargs):
        log.info(
            "DbtCompletionsListener: on_task_instance_success fired "
            "(task=%s, previous_state=%r)",
            getattr(task_instance, "task_id", "?"),
            previous_state,
        )
        if _is_natural_completion(previous_state):
            log.info("DbtCompletionsListener: skipping (natural completion)")
            return
        reconcile_model(task_instance)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, **kwargs):
        log.info(
            "DbtCompletionsListener: on_task_instance_failed fired "
            "(task=%s, previous_state=%r)",
            getattr(task_instance, "task_id", "?"),
            previous_state,
        )
        if _is_natural_completion(previous_state):
            log.info("DbtCompletionsListener: skipping (natural completion)")
            return
        reconcile_model(task_instance)


class SundialDbtCompletionsPlugin(AirflowPlugin):
    name = "sundial_dbt_completions"
    listeners = [DbtCompletionsListener()]
