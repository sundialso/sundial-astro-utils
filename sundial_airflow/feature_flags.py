"""Runtime feature flags read from environment variables."""
from __future__ import annotations

import os

CHUNKING_ENABLED_ENV = "SUNDIAL_CHUNKING_ENABLED"

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def is_chunking_enabled() -> bool:
    """Return True when chunking (``create_dag``) should be the scheduled DAG.

    Defaults to ``False`` when ``SUNDIAL_CHUNKING_ENABLED`` is unset, empty, or
    any value other than ``1`` / ``true`` / ``yes`` / ``on`` (case-insensitive).
    """
    raw = os.environ.get(CHUNKING_ENABLED_ENV)
    if not raw:
        return False
    return raw.strip().lower() in _TRUTHY


def resolve_dag_schedules(cron: str | None) -> tuple[str | None, str | None]:
    """Return ``(create_dag_schedule, dag_factory_schedule)`` for a tenant cron.

    When chunking is enabled, only ``create_dag`` is scheduled; otherwise only
    ``dag_factory`` (Cosmos-only) is scheduled. The unscheduled DAG stays
    available for manual triggers and rollback.
    """
    if not cron:
        return None, None
    if is_chunking_enabled():
        return cron, None
    return None, cron
