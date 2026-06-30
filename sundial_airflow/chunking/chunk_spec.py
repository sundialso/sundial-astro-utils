"""Chunk window helpers for dynamic task mapping."""
from __future__ import annotations

from datetime import date, datetime, time


def _parse_ts(value: str) -> datetime:
    text = str(value).strip()
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")[:19]).replace(
            tzinfo=None,
        )
    return datetime.combine(date.fromisoformat(text[:10]), time.min)


def _format_ts(value: datetime) -> str:
    return value.replace(tzinfo=None).isoformat(timespec="seconds")


def as_timestamp_start(value: str) -> str:
    return _format_ts(_parse_ts(value))


def as_timestamp_end(value: str) -> str:
    text = str(value).strip()
    if "T" in text:
        return _format_ts(_parse_ts(text))
    return _format_ts(_parse_ts(text).replace(hour=23, minute=59, second=59))


def as_timestamp(value: str) -> str:
    return as_timestamp_start(value)


def _chunk_bound(raw: str | None, fallback: str, *, end_of_day: bool) -> str:
    if raw:
        return raw if "T" in raw else as_timestamp_start(raw)
    return as_timestamp_end(fallback) if end_of_day else as_timestamp_start(fallback)


def chunk_expand_kwargs(chunks: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for chunk in chunks:
        kw: dict[str, str] = {"chunk_id": chunk["chunk_id"]}
        if not chunk.get("omit_start_override"):
            kw["chunk_start"] = _chunk_bound(
                chunk.get("start_ts"), chunk["start"], end_of_day=False,
            )
        if not chunk.get("omit_end_override"):
            kw["chunk_end"] = _chunk_bound(
                chunk.get("end_ts"), chunk["end"], end_of_day=True,
            )
        out.append(kw)
    return out


def run_expand_kwargs(plan: dict | None) -> list[dict[str, str]]:
    if not plan or plan.get("disposition") != "chunked":
        return []
    return chunk_expand_kwargs(plan.get("chunks") or [])


def build_chunk_units(run_plan: dict[str, dict]) -> dict[str, list[dict[str, str]]]:
    return {name: run_expand_kwargs(plan) for name, plan in run_plan.items()}
