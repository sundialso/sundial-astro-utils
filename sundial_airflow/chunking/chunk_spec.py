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
