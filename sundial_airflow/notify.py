"""End-of-pipeline notification trigger.

The DAG factories (:func:`sundial_airflow.dag_factory.make_dbt_dag` and
:func:`sundial_airflow.create_dag.create_dag`) append :func:`build_notify_task`
as a terminal task, so every tenant DAG gets it for free — no per-tenant repo
changes. On pipeline completion it POSTs a pipeline-completion event to
Sundial's notification-trigger endpoint, which fans out to the tenant's enabled
notification triggers for the run date.

Enrollment is a deploy-time toggle, not code: the task self-skips
(``AirflowSkipException``) unless the ``sundial_notify_api`` Airflow connection
is configured in the deployment, so un-enrolled deployments are harmless
no-ops.

Auth mirrors ``slack_alerts.py``: the secret lives in an Airflow **Connection**,
never a committed file. The connection's ``host`` is the Sundial gateway base
URL and its ``password`` is the HMAC trigger secret (matches
``NOTIFICATION_TRIGGER_SECRET`` on the ai-service side).
"""
from __future__ import annotations

import logging
from typing import Any

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowNotFoundException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

#: Airflow connection holding the gateway URL (host) + HMAC secret (password).
NOTIFY_CONN_ID = "sundial_notify_api"
NOTIFY_TASK_ID = "notify"

# Gateway route for the ai-service trigger endpoint. The ``/ai`` prefix is added
# by the gateway in front of ai-service's ``/khruangbin`` mount.
_TRIGGER_PATH = "/ai/khruangbin/internal/notification-triggers/fire"
_SECRET_HEADER = "X-Notification-Trigger-Secret"  # noqa: S105 — header name, not a secret
_TIMEOUT_SECONDS = 30


def _resolve_base_url(host: str) -> str:
    """Normalise a connection host into an ``https`` base URL.

    Rejects plaintext ``http``: the request carries the HMAC trigger secret, so
    it must never traverse an unencrypted channel (a MITM could capture the
    secret and forge completion events).
    """
    base = host.rstrip("/")
    if base.startswith("http://"):
        raise ValueError(
            f"{NOTIFY_CONN_ID} host must use https (the request carries a secret); got {base!r}"
        )
    if not base.startswith("https://"):
        base = f"https://{base}"
    return base


def notify_end_of_pipeline(*, tenant: str, run_date: str, dag_id: str) -> None:
    """POST a pipeline-completion event to Sundial's notification-trigger API.

    Skips (``AirflowSkipException``) when the ``sundial_notify_api`` connection
    is absent or incomplete, so a deployment that hasn't enrolled is a no-op.
    Raises on transport errors or a non-2xx response so a genuine failure is
    visible in the Airflow UI (and retried per the DAG's ``default_args``).
    """
    try:
        conn = BaseHook.get_connection(NOTIFY_CONN_ID)
    except AirflowNotFoundException:
        conn = None

    if conn is None or not conn.host or not conn.password:
        raise AirflowSkipException(
            f"{NOTIFY_CONN_ID} connection not configured; skipping notification trigger "
            f"for tenant={tenant!r}"
        )
    url = f"{_resolve_base_url(conn.host)}{_TRIGGER_PATH}"
    secret = conn.password

    payload = {
        "tenant_slug": tenant,
        "run_date": run_date,
        "dag_id": dag_id,
        "source": "astro",
    }

    logger.info(
        "Firing notification trigger for tenant=%r run_date=%s via %s",
        tenant,
        run_date,
        NOTIFY_CONN_ID,
    )
    response = requests.post(
        url,
        json=payload,
        headers={_SECRET_HEADER: secret},
        timeout=_TIMEOUT_SECONDS,
    )
    if not response.ok:
        logger.error(
            "notification-trigger endpoint returned %s for tenant=%r: %s",
            response.status_code,
            tenant,
            response.text[:500],
        )
        response.raise_for_status()

    logger.info(
        "notification-trigger fired for tenant=%r run_date=%s (status=%s)",
        tenant,
        run_date,
        response.status_code,
    )


def build_notify_task(*, tenant: str, dag_id: str) -> Any:
    """Create the terminal ``notify`` task and return the operator.

    Called inside a DAG context by the factories. ``trigger_rule=ALL_DONE`` so it
    fires on pipeline completion even if some models failed (success/completion
    signal; failure alerting is handled separately by ``on_failure_callback``).
    The run date mirrors the ``execution_ts`` dbt var the pipeline uses, so the
    endpoint's age/dedup gates see the same data date the run processed. When
    ``execution_ts`` is unset it falls back to the run's ``logical_date`` (not
    today), so a backfill notifies for the data date it actually processed.
    """

    @task(task_id=NOTIFY_TASK_ID, trigger_rule=TriggerRule.ALL_DONE)
    def notify(**context: Any) -> None:
        params = context.get("params") or {}
        run_date = str(params.get("execution_ts") or context["logical_date"])[:10]
        notify_end_of_pipeline(tenant=tenant, run_date=run_date, dag_id=dag_id)

    return notify()
