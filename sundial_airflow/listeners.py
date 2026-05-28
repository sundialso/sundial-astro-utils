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
from functools import lru_cache
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

# BQ connection lookup, in precedence order:
#   1. ``dbt_completions_bq_conn_id:<value>`` tag on the DAG (set per-DAG via
#      ``make_dbt_dag(..., completions_bq_conn_id=...)``).
#   2. The env var ``SUNDIAL_DBT_COMPLETIONS_BQ_CONN_ID`` (deployment-level).
#   3. Auto-discover the single BQ-typed connection in Airflow's metadata DB.
#   4. Hardcoded Airflow convention ``google_cloud_default`` (last resort).
_BQ_CONN_ID_TAG_PREFIX = "dbt_completions_bq_conn_id:"
_BQ_CONN_TYPES = ("google_cloud_platform", "gcpbigquery")

_AUTO_BQ_CONN_CACHE: str | None = None
_AUTO_BQ_CONN_LOOKED_UP: bool = False


def _autodiscover_bq_conn() -> str | None:
    """Find the single BQ-typed connection in Airflow's connection list.

    Returns the ``conn_id`` if exactly one BQ connection is configured,
    ``None`` if zero (no BQ conn at all) or multiple (ambiguous — user
    must disambiguate via tag or env var). Cached per process; restart
    Airflow if connections change.
    """
    global _AUTO_BQ_CONN_CACHE, _AUTO_BQ_CONN_LOOKED_UP
    if _AUTO_BQ_CONN_LOOKED_UP:
        return _AUTO_BQ_CONN_CACHE
    _AUTO_BQ_CONN_LOOKED_UP = True
    try:
        from airflow.models import Connection
        from airflow.utils.session import create_session

        with create_session() as session:
            conns = (
                session.query(Connection)
                .filter(Connection.conn_type.in_(_BQ_CONN_TYPES))
                .all()
            )
        names = [c.conn_id for c in conns]
        if len(names) == 1:
            log.info("DbtCompletionsListener: auto-discovered BQ conn=%s", names[0])
            _AUTO_BQ_CONN_CACHE = names[0]
        elif len(names) > 1:
            log.warning(
                "DbtCompletionsListener: multiple BQ connections (%s); set "
                "make_dbt_dag(completions_bq_conn_id=...) or "
                "SUNDIAL_DBT_COMPLETIONS_BQ_CONN_ID to disambiguate",
                names,
            )
        else:
            log.info("DbtCompletionsListener: no BQ-typed connection found")
    except Exception as exc:
        log.debug("BQ conn auto-discovery failed: %s", exc)
    return _AUTO_BQ_CONN_CACHE

# ``make_dbt_dag`` parses ``vars.target_project`` from dbt_project.yml at
# build time and stashes it on the DAG as a ``dbt_completions_project:<value>``
# tag. Tags survive Airflow 3.x serialization — the API server sees them in
# the SerializedDAG it loads from the metadata DB. (``user_defined_macros``
# does NOT survive serialization, which is why we don't use it.)
_TARGET_PROJECT_TAG_PREFIX = "dbt_completions_project:"

# Listener enablement, in precedence order:
#   1. ``SUNDIAL_DBT_LISTENER_DAG_IDS`` env var: comma-separated allowlist.
#      If set (even to ""), it is the *only* check — tag is ignored.
#   2. ``listener_enabled`` tag on the DAG (added automatically by
#      ``make_dbt_dag``).
_LISTENER_ALLOWLIST_ENV = "SUNDIAL_DBT_LISTENER_DAG_IDS"
_LISTENER_ENABLED_TAG = "listener_enabled"


def _is_listener_enabled(dag_run: Any) -> bool:
    """Return True iff this DAG opted in to listener reconciliation.

    Env var allowlist (if set) is the hard override; otherwise fall back to
    the ``listener_enabled`` tag. Env-var path avoids any DB load — a
    non-matching ``dag_id`` short-circuits before ``_load_dag``.
    """
    allowlist = os.environ.get(_LISTENER_ALLOWLIST_ENV)
    if allowlist is not None:
        ids = {x.strip() for x in allowlist.split(",") if x.strip()}
        return dag_run.dag_id in ids
    return _LISTENER_ENABLED_TAG in _tag_strings(_load_dag(dag_run))


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


def _get_bq_conn_id(dag_run: Any) -> str:
    """Resolve BQ conn_id: tag → env var → auto-discovery → hardcoded default."""
    dag = _load_dag(dag_run)
    for tag in _tag_strings(dag):
        if tag.startswith(_BQ_CONN_ID_TAG_PREFIX):
            return tag[len(_BQ_CONN_ID_TAG_PREFIX):]
    explicit_env = os.environ.get("SUNDIAL_DBT_COMPLETIONS_BQ_CONN_ID")
    if explicit_env:
        return explicit_env
    auto = _autodiscover_bq_conn()
    if auto:
        return auto
    return "google_cloud_default"


_AIRFLOW_FAILED_STATES = {"failed", "upstream_failed"}
_AIRFLOW_SUCCESS_STATES = {"success"}


_RECONCILABLE_PREVIOUS_STATES = {"success", "failed"}


def _should_reconcile(previous_state: Any, new_state_value: str) -> bool:
    """Decide whether to reconcile on this transition.

    Fire **only** when the user flips a previously-terminal verdict, i.e.
    ``previous_state in {'success', 'failed'}`` AND it differs from the new
    state. That means the dbt macros already wrote a row to ``dbt_completions``
    during a real dbt attempt, and the user is now overriding that verdict.

    Skipped:
      - ``previous == new`` → no-op transition (e.g. bulk "Mark DAG Success"
        re-flipping already-success tasks).
      - ``previous == 'running'`` → natural completion; dbt macros handle it.
      - ``previous in {None, 'skipped', 'upstream_failed', 'queued', ...}``
        → dbt never ran this model, so there's no row to update.
    """
    if previous_state is None:
        return False
    prev = getattr(previous_state, "value", previous_state)
    if prev == new_state_value:
        return False
    return prev in _RECONCILABLE_PREVIOUS_STATES


# ---------------------------------------------------------------------------
# Identification & target discovery
# ---------------------------------------------------------------------------
@lru_cache(maxsize=128)
def _load_dag_by_id(dag_id: str) -> Any | None:
    """Cached SerializedDagModel.get_dag lookup. Each ``reconcile_model``
    call hits this 3× (for the tag check, project lookup, and conn lookup),
    so the cache turns ~3 DB queries into 1. DAG structure doesn't change
    within a process; restart Airflow to invalidate if a DAG is modified.
    """
    try:
        from airflow.models.serialized_dag import SerializedDagModel
        return SerializedDagModel.get_dag(dag_id)
    except Exception as exc:
        log.debug("SerializedDagModel.get_dag(%s) failed: %s", dag_id, exc)
        return None


def _load_dag(dag_run: Any) -> Any | None:
    """Fetch the full deserialized DAG. Canonical Airflow 3.x path is
    ``SerializedDagModel`` via the metadata DB (``dag_run.dag`` is partial
    in the listener context — ``tags`` may not be populated).
    """
    dag = _load_dag_by_id(dag_run.dag_id)
    if dag is not None:
        return dag
    # Fallback: ``dag_run.dag``. Tags may be missing but it's better than nothing.
    try:
        return getattr(dag_run, "dag", None)
    except Exception as exc:
        log.debug("dag_run.dag fallback access failed: %s", exc)
        return None


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


def _states_for_model(
    dag_run: Any,
    model_name: str,
    just_changed: Any | None = None,
) -> dict[str, str]:
    """Collect ``{role: airflow_state}`` for one model across all TIs in the run.

    ``just_changed`` is the ``task_instance`` Airflow handed to the listener.
    When the listener fires from a manual UI/API state change, the DB row
    for *that* task may not yet reflect the new state — the in-memory TI
    object carries the truth. So we let it override the sibling DB read for
    its own role.
    """
    out: dict[str, str] = {}
    for ti in dag_run.get_task_instances():
        parsed = _parse_model_task(ti.task_id)
        if parsed is None:
            continue
        m, role = parsed
        if m == model_name:
            out[role] = ti.state or ""

    if just_changed is not None:
        parsed = _parse_model_task(getattr(just_changed, "task_id", ""))
        if parsed and parsed[0] == model_name:
            _, role = parsed
            new_state = getattr(just_changed, "state", None) or out.get(role, "")
            out[role] = new_state
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
    if not _is_listener_enabled(dag_run):
        log.info("DbtCompletionsListener: dag %s not enabled for listener, skipping", dag_run.dag_id)
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

    states = _states_for_model(dag_run, model_name, just_changed=task_instance)
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
            _get_bq_conn_id(dag_run),
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
        if not _should_reconcile(previous_state, "success"):
            log.info(
                "DbtCompletionsListener: skipping success on task=%s previous=%r",
                getattr(task_instance, "task_id", "?"),
                previous_state,
            )
            return
        log.info(
            "DbtCompletionsListener: reconciling success on task=%s previous=%r",
            getattr(task_instance, "task_id", "?"),
            previous_state,
        )
        reconcile_model(task_instance)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, **kwargs):
        if not _should_reconcile(previous_state, "failed"):
            log.info(
                "DbtCompletionsListener: skipping failed on task=%s previous=%r",
                getattr(task_instance, "task_id", "?"),
                previous_state,
            )
            return
        log.info(
            "DbtCompletionsListener: reconciling failed on task=%s previous=%r",
            getattr(task_instance, "task_id", "?"),
            previous_state,
        )
        reconcile_model(task_instance)


class SundialDbtCompletionsPlugin(AirflowPlugin):
    name = "sundial_dbt_completions"
    listeners = [DbtCompletionsListener()]
