"""Per-model chunk vs single-run decisions for a DAG run."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from dateutil.relativedelta import relativedelta

from sundial_airflow.chunking.manifest_parser import (
    CHUNKED,
    BackfillModel,
    chunk_windows_from_anchor,
)

RunDisposition = Literal["single", "chunked"]

_UTC = timezone.utc


@dataclass(frozen=True)
class ChunkWindow:
    """One chunk window: ``start`` at midnight, ``end`` at the ``end_ts`` instant."""

    chunk_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class ModelRunPlan:
    """Resolved run shape for one chunked model."""

    model_name: str
    disposition: RunDisposition
    chunks: tuple[ChunkWindow, ...] = ()


def build_run_plan(
    *,
    models: dict[str, BackfillModel],
    watermarks: dict[str, datetime | None],
    backfill_mode: str,
    execution_ts: date,
    window_start: date | datetime | None = None,
    window_end: date | datetime | None = None,
) -> dict[str, ModelRunPlan]:
    """Build the per-model run plan for chunked models."""
    plans: dict[str, ModelRunPlan] = {}

    for model in models.values():
        if model.kind != CHUNKED:
            continue
        if model.first_timestamp is None or model.chunk_months is None:
            continue

        plan = _plan_for_model(
            model=model,
            watermark=watermarks.get(model.name),
            backfill_mode=backfill_mode,
            execution_ts=execution_ts,
            window_start=window_start,
            window_end=window_end,
        )
        plans[model.name] = plan
        _log_plan(plan)

    return plans


def compute_window_end(execution_ts: date, lag: int = 0) -> datetime:
    """Mirror the ``end_ts()`` macro: ``execution_ts - lag days - 1s`` (inclusive)."""
    base = datetime(execution_ts.year, execution_ts.month, execution_ts.day, tzinfo=_UTC)
    return base - timedelta(days=max(lag, 0)) - timedelta(seconds=1)


def _window_bounds(execution_ts: date, lag: int) -> tuple[date, datetime]:
    """``(exclusive chunk-gen date, inclusive end_ts)``; both from one end_ts."""
    final_end = compute_window_end(execution_ts, lag)
    return final_end.date() + timedelta(days=1), final_end


def _plan_for_model(
    *,
    model: BackfillModel,
    watermark: datetime | None,
    backfill_mode: str,
    execution_ts: date,
    window_start: date | datetime | None = None,
    window_end: date | datetime | None = None,
) -> ModelRunPlan:
    """Dispatch to the per-mode processing-window planner."""
    anchor = model.first_timestamp
    chunk_months = model.chunk_months
    assert anchor is not None and chunk_months is not None

    if backfill_mode == "partial":
        return _plan_partial(
            model=model,
            anchor=anchor,
            chunk_months=chunk_months,
            window_start=window_start,
            window_end=window_end,
        )

    if backfill_mode == "full":
        return _plan_full(
            model=model,
            anchor=anchor,
            chunk_months=chunk_months,
            execution_ts=execution_ts,
            lag=model.lag,
        )

    return _plan_incremental(
        model=model,
        anchor=anchor,
        chunk_months=chunk_months,
        watermark=watermark,
        execution_ts=execution_ts,
        lag=model.lag,
        lookback=model.lookback,
    )


def _plan_full(
    *,
    model: BackfillModel,
    anchor: date,
    chunk_months: int,
    execution_ts: date,
    lag: int,
) -> ModelRunPlan:
    """Full backfill: always chunk anchor → ``execution_ts - lag - 1s``."""
    upper_date, final_end = _window_bounds(execution_ts, lag)
    chunks = _to_windows(
        chunk_windows_from_anchor(anchor, chunk_months, anchor, upper_date),
        final_end=final_end,
    )
    return ModelRunPlan(model.name, "chunked", chunks)


def _plan_incremental(
    *,
    model: BackfillModel,
    anchor: date,
    chunk_months: int,
    watermark: datetime | None,
    execution_ts: date,
    lag: int,
    lookback: int,
) -> ModelRunPlan:
    """Incremental: watermark → end_ts. No watermark or large gap chunks; a
    gap <= ``chunk_months`` runs once (dbt computes the live window).
    """
    upper_date, final_end = _window_bounds(execution_ts, lag)

    if watermark is None:
        chunks = _to_windows(
            chunk_windows_from_anchor(anchor, chunk_months, anchor, upper_date),
            final_end=final_end,
        )
        return ModelRunPlan(model.name, "chunked", chunks)

    first_start = _resume_start(watermark, anchor, lookback)
    range_start = first_start.date()
    if _month_span(range_start, upper_date) <= chunk_months:
        return ModelRunPlan(model.name, "single")

    chunks = _to_windows(
        chunk_windows_from_anchor(anchor, chunk_months, range_start, upper_date),
        first_start=first_start,
        final_end=final_end,
    )
    return ModelRunPlan(model.name, "chunked", chunks)


def _plan_partial(
    *,
    model: BackfillModel,
    anchor: date,
    chunk_months: int,
    window_start: date | datetime | None,
    window_end: date | datetime | None,
) -> ModelRunPlan:
    """Partial backfill over ``[start_ts, end_ts]`` verbatim (matches the
    non-chunking DAG); window <= ``chunk_months`` runs once.
    """
    if window_start is None or window_end is None:
        return ModelRunPlan(model.name, "single")

    range_start = max(_as_date(window_start), _as_date(anchor))
    range_end = _as_date(window_end)
    if range_end <= range_start or _month_span(range_start, range_end) <= chunk_months:
        return ModelRunPlan(model.name, "single")

    chunks = _to_windows(
        chunk_windows_from_anchor(anchor, chunk_months, range_start, range_end),
        first_start=max(_as_datetime(window_start), _as_datetime(anchor)),
        final_end=_as_datetime(window_end),
    )
    if not chunks:
        return ModelRunPlan(model.name, "single")
    return ModelRunPlan(model.name, "chunked", chunks)


def _to_windows(
    raw: list[tuple[date, date, str]],
    *,
    first_start: datetime | None = None,
    final_end: datetime | None = None,
) -> tuple[ChunkWindow, ...]:
    """Build non-overlapping ``ChunkWindow``s.

    Starts are midnight; interior ends are the next boundary minus 1s (so
    ``BETWEEN start AND end`` never double-counts the boundary instant).
    ``first_start``/``final_end`` override the first start / last end with
    precise instants.
    """
    last = len(raw) - 1
    windows: list[ChunkWindow] = []
    for i, (start, end, chunk_id) in enumerate(raw):
        win_start = first_start if (first_start is not None and i == 0) else _as_datetime(start)
        if final_end is not None and i == last:
            win_end = final_end
        else:
            win_end = _as_datetime(end) - timedelta(seconds=1)
        windows.append(ChunkWindow(chunk_id=chunk_id, start=win_start, end=win_end))
    return tuple(windows)


def _resume_start(watermark: datetime, anchor: date, lookback: int) -> datetime:
    """Incremental lower bound, mirroring start_ts(): ``max(watermark + 1s - lookback, anchor)``."""
    resume = (
        _as_datetime(watermark)
        + timedelta(seconds=1)
        - timedelta(days=max(lookback, 0))
    )
    return max(resume, _as_datetime(anchor))


def _as_date(value: datetime | date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_datetime(value: datetime | date | str) -> datetime:
    """Coerce a date/datetime/ISO string to a UTC-aware datetime."""
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
        return dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def _month_span(start: date, end: date) -> int:
    """Return the month span between two dates (partial months count as one)."""
    if end <= start:
        return 0
    delta = relativedelta(end, start)
    return delta.years * 12 + delta.months + (1 if delta.days > 0 else 0)


def _log_plan(plan: ModelRunPlan) -> None:
    import logging

    logger = logging.getLogger(__name__)
    if plan.disposition == "single":
        logger.debug("Run plan: %s → single run", plan.model_name)
        return
    logger.debug(
        "Run plan: %s → %d chunk(s): %s",
        plan.model_name,
        len(plan.chunks),
        ", ".join(c.chunk_id for c in plan.chunks),
    )


def serialize_run_plan(plans: dict[str, ModelRunPlan]) -> dict[str, dict]:
    """Convert run plans to JSON-safe dicts for XCom."""
    return {
        name: {
            "disposition": plan.disposition,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "start": c.start.isoformat(),
                    "end": c.end.isoformat(),
                }
                for c in plan.chunks
            ],
        }
        for name, plan in plans.items()
    }

