"""Chunk window helpers for dynamic task mapping."""
from __future__ import annotations


def as_timestamp(value: str) -> str:
    """Normalize a run-plan date string to a dbt-friendly timestamp literal."""
    text = str(value).strip()
    if not text:
        return text
    if "T" in text:
        return text.replace("Z", "+00:00")[:19]
    return f"{text}T00:00:00"


def chunk_expand_kwargs(chunks: list[dict[str, str]]) -> list[dict[str, str]]:
    """Shape plan chunks for ``expand_kwargs`` and readable map indexes."""
    return [
        {
            "chunk_id": c["chunk_id"],
            "chunk_start": as_timestamp(c["start"]),
            "chunk_end": as_timestamp(c["end"]),
        }
        for c in chunks
    ]


def run_expand_kwargs(plan: dict | None) -> list[dict[str, str]]:
    """Build mapped ``run_chunk`` kwargs for one model's serialized run plan."""
    if not plan:
        return []
    if plan.get("disposition") == "chunked":
        return chunk_expand_kwargs(plan.get("chunks") or [])
    return []


def build_chunk_units(run_plan: dict[str, dict]) -> dict[str, list[dict[str, str]]]:
    """Precompute per-model ``expand_kwargs`` lists for all chunked models."""
    return {name: run_expand_kwargs(plan) for name, plan in run_plan.items()}
