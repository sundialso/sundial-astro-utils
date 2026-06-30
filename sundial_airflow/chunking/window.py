"""Single-run incremental windows and lossless anchor-aligned chunk splits."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from dateutil.relativedelta import relativedelta


@dataclass(frozen=True)
class RunWindow:
    end: datetime
    defer_start: bool = False
    defer_end: bool = False
    start_literal: str | None = None
    end_literal: str | None = None


@dataclass(frozen=True)
class PlannedChunk:
    chunk_id: str
    start: date
    end: date
    omit_start_override: bool = False
    omit_end_override: bool = False
    start_ts: str | None = None
    end_ts: str | None = None


def parse_ts(value: date | datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = str(value).strip()
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")[:19]).replace(
            tzinfo=None,
        )
    return datetime.combine(date.fromisoformat(text[:10]), time.min)


def format_ts(value: datetime) -> str:
    return value.replace(tzinfo=None).isoformat(timespec="seconds")


def end_ts_from_execution(
    execution_ts: date | datetime | str,
    *,
    lag: int = 0,
) -> datetime:
    """``end_ts(lag)``: execution_ts minus ``lag`` days minus one second."""
    end = parse_ts(execution_ts)
    if lag > 0:
        end -= timedelta(days=lag)
    return end - timedelta(seconds=1)


def raw_ts(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return format_ts(value.replace(tzinfo=None))
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def partial_backfill_window(
    *,
    partial_start: date | datetime | str,
    partial_end: date | datetime | str,
) -> RunWindow:
    return RunWindow(
        end=parse_ts(partial_end),
        start_literal=raw_ts(partial_start),
        end_literal=raw_ts(partial_end),
    )


def full_backfill_window(
    *,
    execution_ts: date | datetime | str,
    lag: int = 0,
) -> RunWindow:
    return RunWindow(
        end=end_ts_from_execution(execution_ts, lag=lag),
        defer_end=True,
    )


def incremental_window(
    *,
    execution_ts: date | datetime | str,
    lag: int = 0,
) -> RunWindow:
    return RunWindow(
        end=end_ts_from_execution(execution_ts, lag=lag),
        defer_start=True,
        defer_end=True,
    )


def subdivide_window(
    *,
    anchor: date,
    chunk_months: int,
    window: RunWindow,
    clip_start: date,
) -> list[PlannedChunk]:
    if chunk_months < 1:
        return []

    clip_dt = datetime.combine(clip_start, time.min)
    if window.end < clip_dt:
        return []

    cap = window.end + timedelta(seconds=1)
    segments: list[tuple[datetime, datetime, str]] = []
    for grid_start, grid_end, chunk_id in _anchor_periods(anchor, chunk_months, cap):
        if grid_start >= cap or grid_end <= clip_dt:
            continue
        seg_start = max(grid_start, clip_dt)
        seg_end = min(grid_end - timedelta(seconds=1), window.end)
        if seg_end >= seg_start:
            segments.append((seg_start, seg_end, chunk_id))

    chunks = [
        _planned_chunk(window, len(segments), i, seg_start, seg_end, chunk_id)
        for i, (seg_start, seg_end, chunk_id) in enumerate(segments)
    ]
    return _merge_trailing_instant(chunks, window.end)


def _planned_chunk(
    window: RunWindow,
    n_segments: int,
    index: int,
    seg_start: datetime,
    seg_end: datetime,
    chunk_id: str,
) -> PlannedChunk:
    is_first = index == 0
    is_last = index == n_segments - 1
    omit_start, start_ts = _bound_override(
        defer=window.defer_start and is_first,
        literal=window.start_literal if is_first else None,
        default=format_ts(seg_start),
    )
    omit_end, end_ts = _bound_override(
        defer=window.defer_end and is_last,
        literal=window.end_literal if is_last else None,
        default=format_ts(seg_end),
    )
    return PlannedChunk(
        chunk_id=chunk_id,
        start=seg_start.date(),
        end=seg_end.date(),
        omit_start_override=omit_start,
        omit_end_override=omit_end,
        start_ts=start_ts,
        end_ts=end_ts,
    )


def _bound_override(
    *,
    defer: bool,
    literal: str | None,
    default: str,
) -> tuple[bool, str | None]:
    if literal is not None:
        return False, literal
    if defer:
        return True, None
    return False, default


def _merge_trailing_instant(
    chunks: list[PlannedChunk],
    window_end: datetime,
) -> list[PlannedChunk]:
    if len(chunks) < 2:
        return chunks
    last = chunks[-1]
    if last.start_ts is None or last.end_ts is None:
        return chunks
    if parse_ts(last.start_ts) == parse_ts(last.end_ts) == window_end:
        prev = chunks[-2]
        chunks[-2] = PlannedChunk(
            chunk_id=prev.chunk_id,
            start=prev.start,
            end=window_end.date(),
            omit_start_override=prev.omit_start_override,
            omit_end_override=prev.omit_end_override,
            start_ts=prev.start_ts,
            end_ts=last.end_ts,
        )
        chunks.pop()
    return chunks


def _anchor_periods(
    anchor: date,
    chunk_months: int,
    cap_exclusive: datetime,
) -> list[tuple[datetime, datetime, str]]:
    out: list[tuple[datetime, datetime, str]] = []
    cursor = datetime.combine(anchor, time.min)
    while cursor < cap_exclusive:
        nxt = min(cursor + relativedelta(months=chunk_months), cap_exclusive)
        out.append((cursor, nxt, cursor.strftime("%Y-%m")))
        cursor = nxt
    return out
