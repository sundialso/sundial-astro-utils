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

from sundial_airflow.run_input import RunInput, run_input_from_context

logger = logging.getLogger(__name__)

SLACK_API_CONN_ID = "astro-alerts-bot"
FAILURE_ALERT_TASK_ID = "slack_failure_alert"
SUCCESS_ALERT_TASK_ID = "slack_success_alert"
FAILURE_ALERT_CHANNEL = "etl-alerts"
SUCCESS_ALERT_CHANNEL = "pipeline-completion-alerts"
EXTRA_CHANNELS_ENV_VAR = "SUNDIAL_SLACK_EXTRA_ALERT_CHANNELS"
_ALERT_TASK_IDS = frozenset({FAILURE_ALERT_TASK_ID, SUCCESS_ALERT_TASK_ID})
_FAILED_STATES = frozenset({"failed"})
_UNSUCCESSFUL_STATES = frozenset({"failed", "upstream_failed"})
_DEFAULT_CHUNK_VAR_KEYS = ("backfill_start_ts", "backfill_end_ts")

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


def resolve_failure_alert_channels() -> list[str]:
    """``#etl-alerts`` first, then any comma-separated extras from the env var."""
    raw_extras = os.environ.get(EXTRA_CHANNELS_ENV_VAR) or ""
    channels: list[str] = []
    for name in [FAILURE_ALERT_CHANNEL, *raw_extras.split(",")]:
        channel = _normalize_channel(name)
        if channel and channel not in channels:
            channels.append(channel)
    return channels


def _task_ids_in_states(dag_id: str, run_id: str, states: frozenset[str]) -> list[str]:
    """Task ids in ``states`` for this run, excluding the Slack alert tasks."""
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    run_states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
    per_run = (run_states or {}).get(run_id, {})
    return sorted(
        task_id
        for task_id, state in per_run.items()
        if str(getattr(state, "value", state)).lower() in states
        and task_id not in _ALERT_TASK_IDS
    )


def _get_failed_task_ids(dag_id: str, run_id: str) -> list[str]:
    """Root-cause failures only — ``upstream_failed`` is a consequence, not listed."""
    return _task_ids_in_states(dag_id, run_id, _FAILED_STATES)


def _post_to_slack(message: str, *, channel: str) -> None:
    """Post via the ``astro-alerts-bot`` Slack API connection."""
    SlackHook(slack_conn_id=SLACK_API_CONN_ID).call(
        "chat.postMessage",
        json={"channel": channel, "text": message, "unfurl_links": False},
    )


def _dag_link(dag_id: str) -> str:
    base_url = airflow_conf.get("webserver", "base_url", fallback="").rstrip("/")
    return f"<{base_url}/dags/{dag_id}/|View DAG>" if base_url else f"DAG: `{dag_id}`"


def _log_prefix(task_id: str, dag_id: str, run_id: str) -> str:
    return f"[{task_id} dag_id={dag_id} run_id={run_id}]"


def _selection_lines(run: RunInput) -> str:
    """The ``select`` the run used, or ``all`` when the DAG ran every model."""
    lines = [f"*Models:* `{run.models_selector}`"]
    if run.exclude:
        lines.append(f"*Exclude:* `{run.exclude}`")
    return "\n".join(lines)


def _send_failure_alert(context: dict[str, Any], *, tenant: str, dag_id: str) -> None:
    """Post one message per target channel listing failed tasks, or skip if none failed."""
    run_id = str(context["run_id"])
    log_prefix = _log_prefix(FAILURE_ALERT_TASK_ID, dag_id, run_id)

    failed = _get_failed_task_ids(dag_id, run_id)
    if not failed:
        raise AirflowSkipException(f"{log_prefix} no task failures; nothing to alert")

    message = (
        f":red_circle: *DAG Failed*\n"
        f"*Tenant:* `{tenant}`\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"{_selection_lines(run_input_from_context(context))}\n"
        f"*Failed Tasks ({len(failed)}):*\n"
        + "\n".join(f"• `{task_id}`" for task_id in failed)
        + f"\n{_dag_link(dag_id)}"
    )

    # Every channel is attempted so a misconfigured extra can't suppress the
    # alert in the others; the task still goes red if any send failed.
    errors: dict[str, Exception] = {}
    for channel in resolve_failure_alert_channels():
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


def _format_success_window_lines(run: RunInput) -> str:
    lines = [f"*Run Type:* `{run.run_context}`"]
    if run.run_context == "partial_backfill":
        if run.start_ts:
            lines.append(f"*Start TS:* `{run.start_ts}`")
        if run.end_ts:
            lines.append(f"*End TS:* `{run.end_ts}`")
    elif run.execution_ts:
        lines.append(f"*Execution TS:* `{run.execution_ts}`")
    return "\n".join(lines)


def _send_success_alert(
    context: dict[str, Any],
    *,
    tenant: str,
    dag_id: str,
    start_var: str = _DEFAULT_CHUNK_VAR_KEYS[0],
    end_var: str = _DEFAULT_CHUNK_VAR_KEYS[1],
) -> None:
    """Post a completion message to ``#pipeline-completion-alerts``, or skip.

    Skips when any task failed or is ``upstream_failed``, or Slack cannot be
    reached, so a Slack outage or a missing channel invite cannot turn a
    successful dbt run red.
    """
    run_id = str(context["run_id"])
    log_prefix = _log_prefix(SUCCESS_ALERT_TASK_ID, dag_id, run_id)

    unsuccessful = _task_ids_in_states(dag_id, run_id, _UNSUCCESSFUL_STATES)
    if unsuccessful:
        raise AirflowSkipException(
            f"{log_prefix} {len(unsuccessful)} unsuccessful task(s); skipping success alert"
        )

    run = run_input_from_context(context, start_var=start_var, end_var=end_var)
    message = (
        f":large_green_circle: *DAG Succeeded*\n"
        f"*Tenant:* `{tenant}`\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"{_selection_lines(run)}\n"
        f"{_format_success_window_lines(run)}\n"
        f"{_dag_link(dag_id)}"
    )

    channel = _normalize_channel(SUCCESS_ALERT_CHANNEL) or f"#{SUCCESS_ALERT_CHANNEL}"
    logger.info("%s sending Slack success alert to %s", log_prefix, channel)
    try:
        _send_with_retry(_post_to_slack)(message, channel=channel)
    except Exception as exc:  # noqa: BLE001 — skip so a green run stays green
        logger.exception("%s could not post to %s", log_prefix, channel)
        raise AirflowSkipException(
            f"{log_prefix} Slack success alert failed for {channel}; skipping"
        ) from exc
    logger.info("%s alert sent", log_prefix)


def build_failure_alert_task(*, tenant: str, dag_id: str) -> Any:
    """Terminal ``all_done`` task: Slack alert for failed tasks, or skip."""

    @task(task_id=FAILURE_ALERT_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def slack_failure_alert(**context: Any) -> None:
        _send_failure_alert(context, tenant=tenant, dag_id=dag_id)

    return slack_failure_alert()


def build_success_alert_task(
    *,
    tenant: str,
    dag_id: str,
    chunk_var_keys: tuple[str, str] = _DEFAULT_CHUNK_VAR_KEYS,
) -> Any:
    """Terminal ``all_done`` task: Slack success ping, or skip if anything failed."""
    start_var, end_var = chunk_var_keys

    @task(task_id=SUCCESS_ALERT_TASK_ID, trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def slack_success_alert(**context: Any) -> None:
        _send_success_alert(
            context,
            tenant=tenant,
            dag_id=dag_id,
            start_var=start_var,
            end_var=end_var,
        )

    return slack_success_alert()
