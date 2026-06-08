"""Airflow listener that reconciles ``dbt_completions`` after a **manual**
state change (UI "Mark Success" / "Mark Failed", or an API call).

The listener itself is warehouse-agnostic: which transitions to reconcile and
how a model's terminal status is derived are the same everywhere. All
warehouse-specific behaviour (connection discovery, table naming, the MERGE)
lives in ``warehouses.py`` behind the :class:`~sundial_airflow.warehouses.WarehouseAdapter`
interface. The DAG's warehouse is plumbed through the ``prepare_dbt_args``
XCom by ``make_dbt_dag``; the listener picks the matching adapter per run.

Division of labour
------------------
- **Natural runs** (scheduled, manual-trigger, backfill): the dbt macros in
  ``sundial_dbt_shared`` (``pre-hook`` / ``post-hook`` / ``on-run-end``)
  write every row to ``dbt_completions_raw`` from *inside* the dbt
  invocation. The listener stays out of the way.

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

Same MERGE key as the dbt macros — ``model_name + execution_ts + status +
run_group_id + chunk_key`` — idempotent against any concurrent or repeated
writes. The ``run_group_id`` (from the run's dbt vars / Airflow run_id) ties the
reconciliation row to the run's other rows so the ``dbt_completions`` view, which
picks a winning run_group and rolls status up per chunk, reads it correctly;
``chunk_key`` is ``"full"`` for non-chunked models (all current tenants).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

from sundial_airflow.warehouses import WarehouseAdapter, get_adapter

log = logging.getLogger(__name__)

# Must match dag_factory.PREPARE_TASK_ID; duplicated here to avoid a circular import.
PREPARE_TASK_ID = "prepare_dbt_args"

# Table name is a Sundial-wide convention; the table always sits in the same
# dataset/schema the tenant's dbt models write to, so we hardcode it here
# instead of plumbing it through every DAG. This is the raw, append/MERGE
# target (one row per status transition); the dbt_completions *view* collapses
# it to each run's final state. The listener writes here, not to the view, and
# its later updated_at makes the reconciliation row win in the view.
COMPLETIONS_TABLE = "dbt_completions_raw"

# Default warehouse for runs whose XCom predates warehouse plumbing.
_DEFAULT_WAREHOUSE = "bigquery"

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


_AIRFLOW_FAILED_STATES = {"failed", "upstream_failed"}
_AIRFLOW_SUCCESS_STATES = {"success"}


_NATURAL_COMPLETION_STATES = {"running"}


def _should_reconcile(previous_state: Any, new_state_value: str) -> bool:
    """Decide whether to reconcile on this transition.

    Skip only the two cases that are *definitely* not manual interventions;
    reconcile everything else. The ``dbt_completions`` MERGE is idempotent
    (model_name + execution_ts + status is the match key), so any extra
    write on a borderline case is safe.

    Skipped:
      - ``previous == 'running'`` → natural completion; the dbt macros'
        ``on-run-end`` hook already wrote the row.
      - ``previous == new`` → no-op transition (e.g. bulk "Mark DAG Success"
        touching tasks already in the target state).

    Reconciled (everything else, including ``previous_state=None``):
      Airflow 3.x passes ``previous_state=None`` for UI / API "Mark Success
      / Mark Failed" actions, so ``None`` is exactly the manual case we
      want to catch.
    """
    prev = getattr(previous_state, "value", previous_state) if previous_state is not None else None
    if prev in _NATURAL_COMPLETION_STATES:
        return False
    if prev == new_state_value:
        return False
    return True


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


@dataclass
class _CompletionsTarget:
    """Everything needed to write the reconciliation row, warehouse-resolved."""

    adapter: WarehouseAdapter
    table_fqn: str
    execution_ts: str
    run_group_id: str


def _get_completions_target(dag_run: Any) -> _CompletionsTarget | None:
    """Resolve where (and how) to write the reconciliation row.

    Reads the ``prepare_dbt_args`` XCom return: ``payload["warehouse"]`` selects
    the adapter, and ``payload["vars"]`` (the same dbt_vars blob dbt itself
    receives) carries the identifiers each adapter needs to build its table
    name — ``target_project``/``target_dataset`` for BigQuery,
    ``target_schema`` (+ optional database) for Snowflake — plus
    ``execution_ts``.

    Returns ``None`` if the warehouse is unknown, identifiers are missing, or
    the XCom is absent (non-dbt DAGs, pruned DagRuns).
    """
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
    warehouse = payload.get("warehouse") or _DEFAULT_WAREHOUSE
    adapter = get_adapter(warehouse)
    if adapter is None:
        log.warning(
            "DbtCompletionsListener: no warehouse adapter for %r (dag=%s)",
            warehouse,
            dag_run.dag_id,
        )
        return None

    execution_ts = dbt_vars.get("execution_ts")
    if not execution_ts:
        return None
    table_fqn = adapter.build_table_fqn(dbt_vars, COMPLETIONS_TABLE)
    if not table_fqn:
        return None
    # run_group_id ties the reconciliation row to the run's other rows. dbt sets it
    # from the Airflow run_id (dag_factory), so it's in the same vars blob; fall back
    # to dag_run.run_id for runs whose XCom predates run_group_id. Required: a row
    # without it would be dropped by the view's run_group join (see _merge_sql).
    run_group_id = dbt_vars.get("run_group_id") or getattr(dag_run, "run_id", None)
    if not run_group_id:
        return None
    return _CompletionsTarget(adapter, table_fqn, execution_ts, run_group_id)


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
            "(unknown warehouse, or missing identifiers/execution_ts in "
            "prepare_dbt_args XCom)",
            dag_run.dag_id,
        )
        return

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

    log.info(
        "%s/%s: writing status=%s into %s (warehouse=%s, states=%s)",
        dag_run.dag_id,
        model_name,
        status,
        target.table_fqn,
        target.adapter.name,
        states,
    )
    try:
        target.adapter.upsert(
            target.adapter.resolve_conn_id(),
            target.table_fqn,
            target.execution_ts,
            target.run_group_id,
            [{"model_name": model_name, "status": status}],
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
