import logging

from airflow.configuration import conf as airflow_conf
from airflow.models import Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

logger = logging.getLogger(__name__)

SLACK_CONN_ID = "sundial_slack_webhook"
TENANT_KEY = "tenant_slug"


def _resolve_tenant(context) -> str:
    """Resolve the tenant name. Sources, in order:

    1. Airflow Variable named ``tenant_slug``.
    2. A DAG tag of the form ``tenant:<name>``.

    Raises ``ValueError`` if neither is set.
    """
    try:
        var_value = Variable.get(TENANT_KEY, default_var=None)
    except Exception:
        logger.exception("Failed reading Airflow Variable %r", TENANT_KEY)
        var_value = None
    if var_value:
        return var_value

    dag = context.get("dag")
    if dag is not None:
        for tag in getattr(dag, "tags", []) or []:
            if isinstance(tag, str) and tag.startswith("tenant:"):
                return tag.split(":", 1)[1]

    raise ValueError(
        f"Tenant name not configured. Set the '{TENANT_KEY}' Airflow Variable "
        "or add a 'tenant:<name>' DAG tag."
    )


_MAX_FAILED_SHOWN = 3
_MAX_ERROR_LEN = 220
_ERROR_MARKERS = ("ERROR", "Exception", "Error:", "Traceback", "FAIL")


def _extract_error_line(ti) -> str:
    """Best-effort: return the last ERROR/exception line from this TI's log.

    Returns an empty string if logs can't be read or no error line is found —
    we never want log-read failure to swallow the alert itself.
    """
    try:
        from airflow.utils.log.log_reader import TaskLogReader

        reader = TaskLogReader()
        try_number = max(getattr(ti, "try_number", 1) or 1, 1)
        logs, _ = reader.read_log_chunks(ti, try_number, {})
    except Exception:
        logger.exception(
            "[slack_alert] log read failed for task_id=%r",
            getattr(ti, "task_id", "?"),
        )
        return ""

    # ``logs`` shape varies across Airflow versions: list[list[tuple[host, str]]]
    # or list[str]. Walk it permissively.
    text_parts: list[str] = []

    def _flatten(obj):
        if obj is None:
            return
        if isinstance(obj, str):
            text_parts.append(obj)
        elif isinstance(obj, tuple) and len(obj) == 2:
            text_parts.append(str(obj[1]))
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                _flatten(item)

    _flatten(logs)
    text = "\n".join(text_parts)

    last_error = ""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if any(marker in stripped for marker in _ERROR_MARKERS):
            last_error = stripped

    if not last_error:
        return ""
    if len(last_error) > _MAX_ERROR_LEN:
        last_error = last_error[-_MAX_ERROR_LEN:]
    return last_error


def _collect_failed_tis(dag_run):
    """Return failed TIs for ``dag_run`` using an explicit session so the
    lookup is independent of whatever scoped session the callback inherits.
    Filters in Python to dodge Airflow version differences in the ``state``
    parameter of ``get_task_instances``.
    """
    from airflow.utils.session import create_session

    with create_session() as session:
        tis = dag_run.get_task_instances(session=session)
        # Materialize attributes we need while the session is alive.
        return [
            (ti.task_id, ti)
            for ti in tis
            if (getattr(ti, "state", None) or "").lower() == "failed"
        ]


def _format_failed_tasks(dag_run, log_prefix: str) -> str:
    try:
        failed = _collect_failed_tis(dag_run)
    except Exception:
        logger.exception("%s failed to enumerate failed task instances", log_prefix)
        return "_unable to enumerate failed tasks — check scheduler logs_"

    if not failed:
        return "none"

    shown = failed[:_MAX_FAILED_SHOWN]
    lines = []
    for task_id, ti in shown:
        err = _extract_error_line(ti)
        if err:
            lines.append(f"• `{task_id}`: {err}")
        else:
            lines.append(f"• `{task_id}`: _no error message found in logs_")

    remaining = len(failed) - len(shown)
    if remaining > 0:
        lines.append(f"• …and {remaining} more")

    return "\n" + "\n".join(lines)


def dag_failure_alert(context):
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else context.get("task_instance").dag_id
    run_id = dag_run.run_id if dag_run else "unknown"
    log_prefix = f"[slack_alert dag_id={dag_id} run_id={run_id}]"

    logger.info("%s callback started", log_prefix)

    try:
        tenant = _resolve_tenant(context)
    except Exception:
        logger.exception("%s tenant resolution failed; aborting alert", log_prefix)
        return

    if dag_run:
        failed_tasks = _format_failed_tasks(dag_run, log_prefix)
    else:
        failed_tasks = "_dag_run missing from callback context_"

    base_url = airflow_conf.get("webserver", "base_url", fallback="").rstrip("/")
    dag_url = f"{base_url}/dags/{dag_id}/" if base_url else ""
    link = f"<{dag_url}|View DAG>" if dag_url else f"DAG: `{dag_id}`"

    message = (
        f":red_circle: *DAG Failed*\n"
        f"*Tenant:* `{tenant}`\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Failed Tasks:* {failed_tasks}\n"
        f"{link}"
    )

    try:
        logger.info(
            "%s sending Slack message via conn_id=%r for tenant=%r",
            log_prefix,
            SLACK_CONN_ID,
            tenant,
        )
        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send_text(message)
        logger.info("%s alert sent", log_prefix)
    except Exception:
        logger.exception(
            "%s failed to send Slack alert via conn_id=%r", log_prefix, SLACK_CONN_ID
        )
