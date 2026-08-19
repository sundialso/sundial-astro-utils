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
ALERT_CHANNEL = "etl-alerts"
EXTRA_CHANNELS_ENV_VAR = "SUNDIAL_SLACK_EXTRA_ALERT_CHANNELS"

_send_with_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
)


def _normalize_channel(name: str) -> str | None:
    """``#``-prefix a channel name; pass encoded Slack IDs through unchanged."""
    raw = name.strip()
    if not raw:
        return None
    if raw.startswith("#"):
        return raw
    if len(raw) >= 9 and raw[0] in "CGD" and raw[1:].isalnum():
        return raw
    return f"#{raw}"


def resolve_alert_channels() -> list[str]:
    """``#etl-alerts`` first, then any comma-separated extras from the env var."""
    raw_extras = os.environ.get(EXTRA_CHANNELS_ENV_VAR) or ""
    channels: list[str] = []
    for name in [ALERT_CHANNEL, *raw_extras.split(",")]:
        channel = _normalize_channel(name)
        if channel and channel not in channels:
            channels.append(channel)
    return channels


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
    """Post one message per target channel listing failed tasks, or skip if none failed."""
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

    # Every channel is attempted so a misconfigured extra can't suppress the
    # alert in the others; the task still goes red if any send failed.
    errors: dict[str, Exception] = {}
    for channel in resolve_alert_channels():
        logger.info("%s sending Slack alert (%d failed) to %s", log_prefix, len(failed), channel)
        try:
            _send_with_retry(_post_to_slack)(message, channel=channel)
        except Exception as exc:  # noqa: BLE001 — reported per channel below
            logger.exception("%s could not post to %s", log_prefix, channel)
            errors[channel] = exc

    if errors:
        raise RuntimeError(
            f"{log_prefix} Slack alert failed for: {', '.join(errors)}"
        ) from next(iter(errors.values()))
    logger.info("%s alert sent", log_prefix)


def build_failure_alert_task(*, tenant: str, dag_id: str) -> Any:
    """Terminal ``all_done`` task: Slack alert for failed tasks, or skip."""

    @task(task_id=FAILURE_ALERT_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def slack_failure_alert(**context: Any) -> None:
        _send_failure_alert(context, tenant=tenant, dag_id=dag_id)

    return slack_failure_alert()
