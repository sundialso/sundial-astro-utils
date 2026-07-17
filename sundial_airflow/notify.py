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

import datetime as _dt
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

# ==========================================================================
# DO NOT MERGE — test scaffolding for Astro <> Sundial wiring verification.
# When _TEST_MODE is True, bypass the ``sundial_notify_api`` Airflow connection
# and post to a hardcoded codespace URL with a hardcoded secret, so no Astro
# connection setup is needed for the first end-to-end test.
#   - Replace _TEST_BASE_URL with your codespace's PUBLIC forwarded ai-service
#     URL (Ports panel → set visibility Public), e.g. https://name-7001.app.github.dev
#   - _TEST_SECRET must match ai-service's _TRIGGER_SECRET.
#   - _TEST_BASE_URL points at the Kong gateway (port 5555), so _TEST_PATH keeps
#     the ``/ai`` prefix (Kong routes ``/ai/*`` to ai-service). Drop ``/ai`` only
#     if you point straight at the ai-service port instead.
# Remove this block (and the _TEST_MODE branch below) before merging.
_TEST_MODE = True
_TEST_BASE_URL = "https://fluffy-fishstick-rjq75wxwpgph4j9-5555.app.github.dev"
_TEST_SECRET = "sundial-astro-test-secret"  # noqa: S105
_TEST_PATH = "/ai/khruangbin/internal/notification-triggers/fire"
# ==========================================================================


def _today() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")


def _resolve_base_url(host: str, schema: str | None) -> str:
    """Normalise a connection host into a scheme-qualified base URL."""
    base = host.rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = f"{schema or 'https'}://{base}"
    return base


def notify_end_of_pipeline(*, tenant: str, run_date: str, dag_id: str) -> None:
    """POST a pipeline-completion event to Sundial's notification-trigger API.

    Skips (``AirflowSkipException``) when the ``sundial_notify_api`` connection
    is absent or incomplete, so a deployment that hasn't enrolled is a no-op.
    Raises on transport errors or a non-2xx response so a genuine failure is
    visible in the Airflow UI (and retried per the DAG's ``default_args``).
    """
    if _TEST_MODE:  # DO NOT MERGE — hardcoded URL + secret; skips the connection.
        url = f"{_TEST_BASE_URL.rstrip('/')}{_TEST_PATH}"
        secret = _TEST_SECRET
    else:
        try:
            conn = BaseHook.get_connection(NOTIFY_CONN_ID)
        except AirflowNotFoundException:
            conn = None

        if conn is None or not conn.host or not conn.password:
            raise AirflowSkipException(
                f"{NOTIFY_CONN_ID} connection not configured; skipping notification trigger "
                f"for tenant={tenant!r}"
            )
        url = f"{_resolve_base_url(conn.host, conn.schema)}{_TRIGGER_PATH}"
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
    endpoint's age/dedup gates see the same data date the run processed.
    """

    @task(task_id=NOTIFY_TASK_ID, trigger_rule=TriggerRule.ALL_DONE)
    def notify(**context: Any) -> None:
        params = context.get("params") or {}
        run_date = str(params.get("execution_ts") or _today())[:10]
        notify_end_of_pipeline(tenant=tenant, run_date=run_date, dag_id=dag_id)

    return notify()
