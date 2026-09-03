"""Shared DAG-run input: the params a tenant typed, plus what prepare resolved.

``parse_run_input`` is the single place that reads Airflow DAG params into a
structured object. ``prepare_dbt_args``, Slack alerts, notify, and anything
else that needs "what was this run asked to do?" should go through it instead
of re-parsing ``context["params"]``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Mapping

from sundial_airflow.hooks import PREPARE_TASK_ID

logger = logging.getLogger(__name__)

BACKFILL_MODE_TO_RUN_CONTEXT = {
    "none": "normal",
    "full": "full_backfill",
    "partial": "partial_backfill",
}

_DEFAULT_START_VAR = "backfill_start_ts"
_DEFAULT_END_VAR = "backfill_end_ts"


@dataclass(frozen=True)
class RunInput:
    """What this DAG run was asked to do.

    ``execution_ts`` / ``start_ts`` / ``end_ts`` are the raw param values
    (or the values prepare wrote to XCom after overlay). Call
    :meth:`resolved_execution_ts` when a concrete date is required (prepare,
    notify). Slack should omit the field when it is still ``None``.
    """

    backfill_mode: str
    run_context: str
    select: str | None
    exclude: str | None
    execution_ts: Any
    start_ts: Any
    end_ts: Any
    extra_vars: str | None

    @property
    def models_selector(self) -> str:
        """``select`` as typed, or ``all`` when the run covers every model."""
        return self.select or "all"

    @property
    def is_backfill(self) -> bool:
        return self.backfill_mode in {"full", "partial"}

    @property
    def full_refresh(self) -> bool:
        return self.backfill_mode == "full"

    def run_context_tag(self, tenant: str) -> str:
        return f"{tenant}_{self.run_context}"

    def resolved_execution_ts(self) -> str:
        """``execution_ts`` param, else today's UTC date — same as prepare_dbt_args."""
        if self.execution_ts:
            return str(self.execution_ts)
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def validate(self) -> None:
        """Reject inconsistent backfill-window params. Same rules in both factories."""
        validate_backfill_params(
            backfill_mode=self.backfill_mode,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            execution_ts=self.execution_ts,
        )


def parse_run_input(params: Mapping[str, Any] | None) -> RunInput:
    """Parse Airflow DAG params into a :class:`RunInput` (no XCom overlay)."""
    params = params or {}
    backfill_mode = str(params.get("backfill_mode") or "none")
    extra_vars = params.get("vars")
    extra_vars_str = str(extra_vars).strip() if extra_vars else None
    return RunInput(
        backfill_mode=backfill_mode,
        run_context=BACKFILL_MODE_TO_RUN_CONTEXT.get(backfill_mode, "normal"),
        select=_stripped(params.get("select")),
        exclude=_stripped(params.get("exclude")),
        execution_ts=params.get("execution_ts") or None,
        start_ts=params.get("start_ts") or None,
        end_ts=params.get("end_ts") or None,
        extra_vars=extra_vars_str or None,
    )


def validate_backfill_params(
    *, backfill_mode: str, start_ts: Any, end_ts: Any, execution_ts: Any,
) -> None:
    """Validate the run-window params against the selected ``backfill_mode``."""
    has_start = bool(start_ts)
    has_end = bool(end_ts)

    if backfill_mode == "partial":
        if not (has_start and has_end):
            raise ValueError(
                "backfill_mode=partial requires both start_ts and end_ts "
                f"(got start_ts={start_ts!r}, end_ts={end_ts!r})."
            )
        if _as_datetime(start_ts) >= _as_datetime(end_ts):
            raise ValueError(
                "backfill_mode=partial requires start_ts < end_ts "
                f"(got start_ts={start_ts!r}, end_ts={end_ts!r})."
            )
        if execution_ts:
            raise ValueError(
                "backfill_mode=partial does not accept execution_ts "
                f"(got execution_ts={execution_ts!r}); the window is defined "
                "by start_ts and end_ts."
            )
        return

    if has_start or has_end:
        raise ValueError(
            f"backfill_mode={backfill_mode!r} does not accept start_ts/end_ts "
            f"(got start_ts={start_ts!r}, end_ts={end_ts!r}). "
            "Use backfill_mode=partial for an explicit window, otherwise leave "
            "start_ts/end_ts blank and rely on execution_ts."
        )


def run_input_from_context(
    context: Mapping[str, Any],
    *,
    start_var: str = _DEFAULT_START_VAR,
    end_var: str = _DEFAULT_END_VAR,
) -> RunInput:
    """Params plus ``prepare_dbt_args`` XCom when present (downstream tasks)."""
    run = parse_run_input(context.get("params"))
    return _overlay_prepare_payload(
        run,
        _prepare_payload(context),
        start_var=start_var,
        end_var=end_var,
    )


def _stripped(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _prepare_payload(context: Mapping[str, Any]) -> dict[str, Any]:
    """``prepare_dbt_args`` XCom, or ``{}`` if missing / unreadable."""
    ti = context.get("ti")
    if ti is None:
        return {}
    try:
        pulled = ti.xcom_pull(task_ids=PREPARE_TASK_ID)
    except Exception:  # noqa: BLE001 — params are a sufficient fallback
        logger.warning("could not read %s XCom; falling back to DAG params", PREPARE_TASK_ID)
        return {}
    return pulled if isinstance(pulled, dict) else {}


def _overlay_prepare_payload(
    run: RunInput,
    payload: Mapping[str, Any] | None,
    *,
    start_var: str,
    end_var: str,
) -> RunInput:
    """Prefer values prepare already resolved (execution_ts default, window vars)."""
    if not payload:
        return run
    dbt_vars = payload.get("vars") if isinstance(payload.get("vars"), dict) else {}
    return replace(
        run,
        run_context=payload.get("run_context") or run.run_context,
        execution_ts=dbt_vars.get("execution_ts") or run.execution_ts,
        start_ts=dbt_vars.get(start_var) or run.start_ts,
        end_ts=dbt_vars.get(end_var) or run.end_ts,
    )


def _as_datetime(value: Any) -> datetime:
    """Coerce a date/datetime/ISO string to a UTC-aware datetime for window checks."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip().replace(" ", "T")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.fromisoformat(text[:10])
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
