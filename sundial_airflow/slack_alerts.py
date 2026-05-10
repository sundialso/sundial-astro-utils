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

    try:
        if dag_run:
            failed_tis = dag_run.get_task_instances(state="failed")
            shown = [f"`{ti.task_id}`" for ti in failed_tis[:3]]
            remaining = len(failed_tis) - len(shown)
            if remaining > 0:
                shown.append(f"and {remaining} more")
            failed_tasks = ", ".join(shown) if shown else "none"
        else:
            failed_tasks = "unknown"
    except Exception:
        logger.exception("%s failed to enumerate failed task instances", log_prefix)
        failed_tasks = "unknown"

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
