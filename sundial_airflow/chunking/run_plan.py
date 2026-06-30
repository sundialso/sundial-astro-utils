"""Per-model chunk vs single-run decisions for a DAG run."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from dateutil.relativedelta import relativedelta

from sundial_airflow.chunking.manifest_parser import CHUNKED, BackfillModel
from sundial_airflow.chunking.window import (
    PlannedChunk,
    RunWindow,
    full_backfill_window,
    incremental_window,
    partial_backfill_window,
    subdivide_window,
)

RunDisposition = Literal["single", "chunked"]
ChunkWindow = PlannedChunk


@dataclass(frozen=True)
class ModelRunPlan:
    model_name: str
    disposition: RunDisposition
    chunks: tuple[PlannedChunk, ...] = ()


def build_run_plan(
    *,
    models: dict[str, BackfillModel],
    watermarks: dict[str, datetime | None],
    backfill_mode: str,
    execution_ts: date,
    window_start: date | datetime | str | None = None,
    window_end: date | datetime | str | None = None,
) -> dict[str, ModelRunPlan]:
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


def _plan_for_model(
    *,
    model: BackfillModel,
    watermark: datetime | None,
    backfill_mode: str,
    execution_ts: date,
    window_start: date | datetime | str | None = None,
    window_end: date | datetime | str | None = None,
) -> ModelRunPlan:
    anchor = model.first_timestamp
    chunk_months = model.chunk_months
    assert anchor is not None and chunk_months is not None

    lag = model.lag or 0
    if backfill_mode == "partial":
        return _plan_partial_backfill(
            model.name, anchor, chunk_months, window_start, window_end,
        )
    if backfill_mode == "full":
        return _plan_full_backfill(
            model.name, anchor, chunk_months, execution_ts, lag=lag,
        )
    return _plan_incremental(
        model.name, anchor, chunk_months, execution_ts, watermark, lag=lag,
    )


def _plan_partial_backfill(
    model_name: str,
    anchor: date,
    chunk_months: int,
    window_start: date | datetime | str | None,
    window_end: date | datetime | str | None,
) -> ModelRunPlan:
    if window_start is None or window_end is None:
        return ModelRunPlan(model_name, "single")

    clip_start = max(_as_date(window_start), anchor)
    if _month_span(clip_start, _as_date(window_end)) <= chunk_months:
        return ModelRunPlan(model_name, "single")

    return _chunk_or_single(
        model_name,
        anchor,
        chunk_months,
        clip_start,
        partial_backfill_window(
            partial_start=window_start,
            partial_end=window_end,
        ),
    )


def _plan_full_backfill(
    model_name: str,
    anchor: date,
    chunk_months: int,
    execution_ts: date,
    *,
    lag: int = 0,
) -> ModelRunPlan:
    return _chunk_or_single(
        model_name,
        anchor,
        chunk_months,
        anchor,
        full_backfill_window(execution_ts=execution_ts, lag=lag),
    )


def _plan_incremental(
    model_name: str,
    anchor: date,
    chunk_months: int,
    execution_ts: date,
    watermark: datetime | None,
    *,
    lag: int = 0,
) -> ModelRunPlan:
    clip_start = anchor if watermark is None else _as_date(watermark)
    if watermark is not None and _month_span(clip_start, execution_ts) <= chunk_months:
        return ModelRunPlan(model_name, "single")

    return _chunk_or_single(
        model_name,
        anchor,
        chunk_months,
        clip_start,
        incremental_window(execution_ts=execution_ts, lag=lag),
    )


def _chunk_or_single(
    model_name: str,
    anchor: date,
    chunk_months: int,
    clip_start: date,
    window: RunWindow,
) -> ModelRunPlan:
    chunks = subdivide_window(
        anchor=anchor,
        chunk_months=chunk_months,
        window=window,
        clip_start=clip_start,
    )
    if not chunks:
        return ModelRunPlan(model_name, "single")
    return ModelRunPlan(model_name, "chunked", tuple(chunks))


def _as_date(value: datetime | date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _month_span(start: date, end: date) -> int:
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
    return {
        name: {
            "disposition": plan.disposition,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "start": c.start.isoformat(),
                    "end": c.end.isoformat(),
                    "omit_start_override": c.omit_start_override,
                    "omit_end_override": c.omit_end_override,
                    "start_ts": c.start_ts,
                    "end_ts": c.end_ts,
                }
                for c in plan.chunks
            ],
        }
        for name, plan in plans.items()
    }
