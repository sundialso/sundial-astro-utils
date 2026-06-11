"""Per-model chunk vs single-run decisions for a DAG run."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from dateutil.relativedelta import relativedelta

from sundial_airflow.chunking.manifest_parser import (
    CHUNKED,
    BackfillModel,
    chunk_windows_from_anchor,
)

RunDisposition = Literal["single", "chunked"]


@dataclass(frozen=True)
class ChunkWindow:
    """One active chunk window for this run."""

    chunk_id: str
    start: date
    end: date


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
    today: date | None = None,
    window_start: date | datetime | None = None,
    window_end: date | datetime | None = None,
) -> dict[str, ModelRunPlan]:
    """Build the per-model run plan for chunked models."""
    today = today or date.today()
    upper = min(execution_ts, today)
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
            upper=upper,
            window_start=window_start,
            window_end=window_end,
        )
        plans[model.name] = plan
        _log_plan(plan)

    return plans


def _plan_for_model(
    *,
    model: BackfillModel,
    watermark: datetime | None,
    backfill_mode: str,
    upper: date,
    window_start: date | datetime | None = None,
    window_end: date | datetime | None = None,
) -> ModelRunPlan:
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

    if backfill_mode == "full" or watermark is None:
        chunks = _to_windows(
            chunk_windows_from_anchor(anchor, chunk_months, anchor, upper),
        )
        return ModelRunPlan(model.name, "chunked", chunks)

    range_start = _as_date(watermark)
    if _month_span(range_start, upper) <= chunk_months:
        return ModelRunPlan(model.name, "single")

    chunks = _to_windows(
        chunk_windows_from_anchor(anchor, chunk_months, range_start, upper),
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
    """Plan an explicit ``[start_ts, end_ts]`` partial backfill window.

    Windows at or under ``chunk_months`` run once; larger windows fan out into
    anchor-aligned chunks so chunk_keys match the full-backfill cadence.
    """
    if window_start is None or window_end is None:
        return ModelRunPlan(model.name, "single")

    range_start = max(_as_date(window_start), _as_date(anchor))
    range_end = _as_date(window_end)
    if range_end <= range_start or _month_span(range_start, range_end) <= chunk_months:
        return ModelRunPlan(model.name, "single")

    chunks = _to_windows(
        chunk_windows_from_anchor(anchor, chunk_months, range_start, range_end),
    )
    if not chunks:
        return ModelRunPlan(model.name, "single")
    return ModelRunPlan(model.name, "chunked", chunks)


def _to_windows(
    raw: list[tuple[date, date, str]],
) -> tuple[ChunkWindow, ...]:
    return tuple(
        ChunkWindow(chunk_id=chunk_id, start=start, end=end)
        for start, end, chunk_id in raw
    )


def _as_date(value: datetime | date) -> date:
    return value.date() if isinstance(value, datetime) else value


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
        logger.info("Run plan: %s → single run", plan.model_name)
        return
    logger.info(
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

