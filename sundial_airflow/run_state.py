"""Terminal task that fails the DagRun when any task failed.

Airflow derives the DagRun state from leaf tasks only. Both Slack alerts are
leaves that succeed or skip, so without this task a run with a failed source
test or model still finishes green.
"""
from __future__ import annotations

import logging
from typing import Any

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

RUN_STATE_TASK_ID = "check_run_status"
_UNSUCCESSFUL_STATES = frozenset({"failed", "upstream_failed"})


def _unsuccessful_task_ids(dag_id: str, run_id: str) -> list[str]:
    """Task ids in this run that failed, or were skipped by a failed upstream."""
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    run_states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
    per_run = (run_states or {}).get(run_id, {})
    return sorted(
        task_id
        for task_id, state in per_run.items()
        if str(getattr(state, "value", state)).lower() in _UNSUCCESSFUL_STATES
    )


def build_run_state_task(*, dag_id: str) -> Any:
    """Build the task. Wire it to the same upstreams as the Slack alerts.

    ``all_done`` so it always runs and ends green on a clean run, red otherwise
    — a skipped task would read as neither. ``retries=0`` overrides the tenant
    ``default_args``: the check has no side effect worth repeating.
    """

    @task(task_id=RUN_STATE_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def check_run_status(**context: Any) -> None:
        run_id = str(context["run_id"])
        log_prefix = f"[{RUN_STATE_TASK_ID} dag_id={dag_id} run_id={run_id}]"

        failed = _unsuccessful_task_ids(dag_id, run_id)
        if not failed:
            logger.info("%s no task failures", log_prefix)
            return

        raise AirflowException(
            f"{log_prefix} {len(failed)} unsuccessful task(s): {', '.join(failed)}"
        )

    return check_run_status()
