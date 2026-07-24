import logging
from typing import Any

from airflow.configuration import conf as airflow_conf
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.trigger_rule import TriggerRule
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

SLACK_CONN_ID = "sundial_slack_webhook"
FAILURE_ALERT_TASK_ID = "slack_failure_alert"

# Retry the webhook on any exception (network blip, 429, 5xx): 5 attempts with
# exponential backoff (~30s total), enough to ride out Slack's short rate-limit
# windows without stalling the task.
_send_with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
)


def _get_failed_task_ids(dag_id: str, run_id: str) -> list[str]:
    """Return the ids of tasks that ended in the ``failed`` state for this run.

    Uses the Task SDK ``get_task_states`` comms call (the Airflow 3 worker has no
    direct metadata-DB access). Excludes cascade skips (``upstream_failed``) and
    this alert task itself, so only the real failures are reported.
    """
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
    per_run = (states or {}).get(run_id, {})
    return sorted(
        task_id
        for task_id, state in per_run.items()
        if str(getattr(state, "value", state)).lower() == "failed"
        and task_id != FAILURE_ALERT_TASK_ID
    )


def _send_failure_alert(context: dict[str, Any], *, tenant: str, dag_id: str) -> None:
    """Post one Slack alert listing every failed task in the run.

    Runs as an ``all_done`` task, so it also fires on success; it self-skips when
    nothing failed. The send is left to raise on failure so a dropped alert shows
    as a red task, not a silent no-op.
    """
    run_id = str(context["run_id"])
    log_prefix = f"[slack_alert dag_id={dag_id} run_id={run_id}]"

    failed = _get_failed_task_ids(dag_id, run_id)
    if not failed:
        raise AirflowSkipException(f"{log_prefix} no task failures; nothing to alert")

    base_url = airflow_conf.get("webserver", "base_url", fallback="").rstrip("/")
    link = f"<{base_url}/dags/{dag_id}/|View DAG>" if base_url else f"DAG: `{dag_id}`"
    message = (
        f":red_circle: *DAG Failed*\n"
        f"*Tenant:* `{tenant}`\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Failed Tasks ({len(failed)}):*\n"
        + "\n".join(f"• `{task_id}`" for task_id in failed)
        + f"\n{link}"
    )

    logger.info("%s sending Slack alert (%d failed) via %r", log_prefix, len(failed), SLACK_CONN_ID)
    _send_with_retry(
        lambda: SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send_text(message)
    )()
    logger.info("%s alert sent", log_prefix)


def build_failure_alert_task(*, tenant: str, dag_id: str) -> Any:
    """Terminal ``all_done`` task that posts one Slack alert listing every failed
    task, or self-skips when the run succeeded.

    Runs on a worker (reliable + visible logs) rather than as a DAG-level
    ``on_failure_callback``, which Airflow 3 runs unreliably in the DAG processor
    and whose logs Astro doesn't surface. Tenant comes from the factory.
    """

    @task(task_id=FAILURE_ALERT_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def slack_failure_alert(**context: Any) -> None:
        _send_failure_alert(context, tenant=tenant, dag_id=dag_id)

    return slack_failure_alert()
