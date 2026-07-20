"""End-of-pipeline notification trigger.

The DAG factories (:func:`sundial_airflow.dag_factory.make_dbt_dag` and
:func:`sundial_airflow.create_dag.create_dag`) append :func:`build_notify_task`
as a terminal task, so every tenant DAG gets it for free — no per-tenant repo
changes. On pipeline completion it POSTs a pipeline-completion event to
Sundial's notification-trigger endpoint, which fans out to the tenant's enabled
notification triggers for the run date.

Config mirrors the data-quality trigger's convention (see gamma-dbt's
``include/dq_email_trigger.py``) — two env vars set on the Astro deployment:

  - ``SUNDIAL_AI_SERVICE_URL``      Kong gateway base URL, shared with the
                                    data-quality + connector-sync triggers
                                    (``/ai/*`` routes to ai-service).
  - ``NOTIFICATION_TRIGGER_SECRET`` HMAC secret (flag it Secret); must match the
                                    ``NOTIFICATION_TRIGGER_SECRET`` value on the
                                    ai-service side.

Enrollment is a deploy-time toggle: the task self-skips
(``AirflowSkipException``) unless BOTH are set, so un-enrolled deployments are
harmless no-ops. (The data-quality trigger ``raise``s on a missing env because
it is opt-in per tenant; ``notify`` is added to *every* tenant DAG, so an
un-enrolled tenant must skip, not fail.)
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

NOTIFY_TASK_ID = "notify"

# Gateway route for the ai-service trigger endpoint. The ``/ai`` prefix is added
# by the gateway in front of ai-service's ``/khruangbin`` mount.
_ENDPOINT_PATH = "/ai/khruangbin/internal/notification-triggers/fire"
_SECRET_HEADER = "X-Notification-Trigger-Secret"  # noqa: S105 — header name, not a secret
_URL_ENV_VAR = "SUNDIAL_AI_SERVICE_URL"
_SECRET_ENV_VAR = "NOTIFICATION_TRIGGER_SECRET"  # noqa: S105 — env var name, not a secret
_TIMEOUT_SECONDS = 30


def notify_end_of_pipeline(*, tenant: str, run_date: str, dag_id: str) -> None:
    """POST a pipeline-completion event to Sundial's notification-trigger API.

    Self-skips (``AirflowSkipException``) when ``SUNDIAL_AI_SERVICE_URL`` or
    ``NOTIFICATION_TRIGGER_SECRET`` is unset, so a deployment that hasn't
    enrolled is a no-op. Raises on a non-https gateway, a transport error, or a
    non-2xx response so a genuine failure is visible in the Airflow UI (and
    retried per the DAG's ``default_args``).
    """
    base_url = os.environ.get(_URL_ENV_VAR, "")
    secret = os.environ.get(_SECRET_ENV_VAR, "")
    if not base_url or not secret:
        raise AirflowSkipException(
            f"{_URL_ENV_VAR}/{_SECRET_ENV_VAR} not configured; skipping notification "
            f"trigger for tenant={tenant!r}"
        )
    if not base_url.startswith("https://"):
        # The request carries the HMAC secret — refuse to send it unencrypted.
        raise ValueError(f"{_URL_ENV_VAR} must be an https URL; got {base_url!r}")

    url = base_url.rstrip("/") + _ENDPOINT_PATH
    logger.info("Firing notification trigger for tenant=%r run_date=%s", tenant, run_date)
    response = requests.post(
        url,
        headers={_SECRET_HEADER: secret, "Content-Type": "application/json"},
        json={
            "tenant_slug": tenant,
            "run_date": run_date,
            "dag_id": dag_id,
            "source": "astro",
        },
        timeout=_TIMEOUT_SECONDS,
        # Don't follow redirects: requests preserves the secret header across
        # them, so a redirect could leak it to a different/downgraded host.
        allow_redirects=False,
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
