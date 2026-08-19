import logging
import os
from typing import Any

from airflow.configuration import conf as airflow_conf
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.trigger_rule import TriggerRule
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

SLACK_API_CONN_ID = "astro-alerts-bot"
FAILURE_ALERT_TASK_ID = "slack_failure_alert"
CHANNEL_ENV_VAR = "SUNDIAL_SLACK_ALERT_CHANNEL"
DEFAULT_CHANNEL = "etl-alerts"

_send_with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
)


def resolve_alert_channel() -> str:
    """Channel from ``SUNDIAL_SLACK_ALERT_CHANNEL``, else ``#etl-alerts``."""
    raw = (os.environ.get(CHANNEL_ENV_VAR) or "").strip() or DEFAULT_CHANNEL
    if raw.startswith("#"):
        return raw
    if len(raw) >= 9 and raw[0] in "CGD" and raw[1:].isalnum():
        return raw
    return f"#{raw}"


def _get_failed_task_ids(dag_id: str, run_id: str) -> list[str]:
    """Task ids in ``failed`` for this run, excluding this alert task."""
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
    per_run = (states or {}).get(run_id, {})
    return sorted(
        task_id
        for task_id, state in per_run.items()
        if str(getattr(state, "value", state)).lower() == "failed"
        and task_id != FAILURE_ALERT_TASK_ID
    )


def _post_to_slack(message: str, *, channel: str) -> None:
    """Post via the ``astro-alerts-bot`` Slack API connection."""
    SlackHook(slack_conn_id=SLACK_API_CONN_ID).call(
        "chat.postMessage",
        json={"channel": channel, "text": message, "unfurl_links": False},
    )


def _send_failure_alert(context: dict[str, Any], *, tenant: str, dag_id: str) -> None:
    """Post one Slack message listing failed tasks, or skip if none failed."""
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

    channel = resolve_alert_channel()
    logger.info("%s sending Slack alert (%d failed) to %s", log_prefix, len(failed), channel)
    _send_with_retry(_post_to_slack)(message, channel=channel)
    logger.info("%s alert sent", log_prefix)


def build_failure_alert_task(*, tenant: str, dag_id: str) -> Any:
    """Terminal ``all_done`` task: one Slack alert for failed tasks, or skip."""

    @task(task_id=FAILURE_ALERT_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def slack_failure_alert(**context: Any) -> None:
        _send_failure_alert(context, tenant=tenant, dag_id=dag_id)

    return slack_failure_alert()
