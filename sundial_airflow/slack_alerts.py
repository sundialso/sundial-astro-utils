import logging
from typing import Any

from airflow.configuration import conf as airflow_conf
from airflow.decorators import task
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
_MAX_FAILED_SHOWN = 3

# Retry the webhook on any exception (network blip, 429, 5xx): 5 attempts with
# exponential backoff (~30s total), enough to ride out Slack's short rate-limit
# windows without stalling the task.
_send_with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
)


def _format_failed_tasks(context: dict[str, Any], log_prefix: str) -> str:
    """Best-effort list of failed task ids, or a "check the UI" fallback.

    Worker-side (Airflow 3 Task SDK), so it can't rely on metadata-DB access.
    """
    fallback = "_see the Airflow UI for the failed task(s)_"
    dag_run = context.get("dag_run")
    if dag_run is None:
        return fallback
    try:
        tis = dag_run.get_task_instances()
    except Exception:
        logger.exception("%s could not enumerate task instances", log_prefix)
        return fallback

    failed = [
        ti.task_id
        for ti in tis
        if (getattr(ti, "state", None) or "").lower() == "failed"
        and getattr(ti, "task_id", None) != FAILURE_ALERT_TASK_ID
    ]
    if not failed:
        return fallback

    lines = [f"• `{task_id}`" for task_id in failed[:_MAX_FAILED_SHOWN]]
    if len(failed) > _MAX_FAILED_SHOWN:
        lines.append(f"• …and {len(failed) - _MAX_FAILED_SHOWN} more")
    return "\n" + "\n".join(lines)


def _send_failure_alert(context: dict[str, Any], *, tenant: str, dag_id: str) -> None:
    """Build and post the Slack failure alert.

    Split from the task body to stay unit-testable, and left to raise on send
    failure so a dropped alert shows as a red task, not a silent no-op.
    """
    run_id = str(context.get("run_id") or "unknown")
    log_prefix = f"[slack_alert dag_id={dag_id} run_id={run_id}]"

    base_url = airflow_conf.get("webserver", "base_url", fallback="").rstrip("/")
    link = f"<{base_url}/dags/{dag_id}/|View DAG>" if base_url else f"DAG: `{dag_id}`"
    message = (
        f":red_circle: *DAG Failed*\n"
        f"*Tenant:* `{tenant}`\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Failed Tasks:* {_format_failed_tasks(context, log_prefix)}\n"
        f"{link}"
    )

    logger.info("%s sending Slack alert via conn_id=%r", log_prefix, SLACK_CONN_ID)
    _send_with_retry(
        lambda: SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send_text(message)
    )()
    logger.info("%s alert sent", log_prefix)


def build_failure_alert_task(*, tenant: str, dag_id: str) -> Any:
    """Terminal ``one_failed`` task that posts to Slack when any task fails.

    Runs on a worker (reliable + visible logs) rather than as a DAG-level
    ``on_failure_callback``, which Airflow 3 runs unreliably in the DAG processor
    and whose logs Astro doesn't surface. Tenant comes from the factory.
    """

    @task(task_id=FAILURE_ALERT_TASK_ID, trigger_rule=TriggerRule.ONE_FAILED, retries=1)
    def slack_failure_alert(**context: Any) -> None:
        _send_failure_alert(context, tenant=tenant, dag_id=dag_id)

    return slack_failure_alert()
