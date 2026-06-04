"""Parse dbt ``manifest.json`` + tenant chunking config into a backfill graph.

Models default to ``FULL_REFRESH``; a model is promoted to ``CHUNKED``
only if ``chunking_config.json`` opts it in with a positive
``chunk_size`` AND its SQL contains a parseable ``start_ts()`` call.
``config.meta.backfill_disabled: true`` opts a model out entirely.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

CHUNKED = "chunked"
FULL_REFRESH = "full_refresh"

# Matches the third positional argument of `start_ts(col, lookback, 'first_ts')`.
# Whitespace-tolerant; matches both quote styles; spans newlines.
_START_TS_RE = re.compile(
    r"""start_ts\s*\(
        \s*[^,]+,
        \s*\d+\s*,
        \s*['"]([^'"]+)['"]
        \s*\)
    """,
    re.VERBOSE | re.DOTALL,
)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


@dataclass
class BackfillModel:
    """A dbt model in the backfill DAG. ``chunk_months`` is set iff ``kind == CHUNKED``."""

    node_key: str
    name: str
    kind: str
    first_timestamp: Optional[date]
    depends_on: list[str]
    chunk_months: Optional[int] = None


@dataclass(frozen=True)
class ChunkingConfigEntry:
    """One entry in ``chunking_config.json``."""

    model_name: str
    chunking_enabled: bool
    chunk_size: Optional[int] = None


# ── Public API ────────────────────────────────────────────────────────────

def load_chunking_config(
    config_path: str | Path | None,
) -> dict[str, ChunkingConfigEntry]:
    """Load and validate ``chunking_config.json`` → ``{model_name -> entry}``.

    Fail-soft: missing file, bad JSON, or malformed entries log a
    warning and are dropped — never raised. Duplicates: last wins.
    """
    if config_path is None:
        return {}
    path = Path(config_path)
    if not path.exists():
        logger.info("No chunking config at %s.", path)
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("Chunking config %s is invalid JSON: %s — ignoring.", path, exc)
        return {}
    if not isinstance(raw, list):
        logger.warning(
            "Chunking config %s must be a JSON list, got %s — ignoring.",
            path, type(raw).__name__,
        )
        return {}

    out: dict[str, ChunkingConfigEntry] = {}
    for idx, item in enumerate(raw):
        entry = _parse_config_entry(item, idx)
        if entry is None:
            continue
        if entry.model_name in out:
            logger.warning(
                "Chunking config has duplicate entry for %r — last wins.",
                entry.model_name,
            )
        out[entry.model_name] = entry

    logger.info("Loaded %d chunking-config entries from %s.", len(out), path)
    return out


def load_backfill_models(
    manifest_path: str | Path,
    chunking_config: dict[str, ChunkingConfigEntry] | None = None,
) -> dict[str, BackfillModel]:
    """Discover eligible models from ``manifest.json`` and apply chunking config.

    Returns ``{node_key -> BackfillModel}`` where ``node_key`` is the
    full dbt key ``"model.<project>.<name>"``.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    models: dict[str, BackfillModel] = {}
    opted_out = 0

    for node_key, node in manifest.get("nodes", {}).items():
        if not _is_eligible(node):
            continue
        meta = (node.get("config") or {}).get("meta") or {}
        if meta.get("backfill_disabled") is True:
            opted_out += 1
            continue
        raw_sql = node.get("raw_code") or node.get("raw_sql") or ""
        models[node_key] = BackfillModel(
            node_key=node_key,
            name=node["name"],
            kind=FULL_REFRESH,
            first_timestamp=_extract_first_timestamp(raw_sql),
            depends_on=[
                d for d in node.get("depends_on", {}).get("nodes", [])
                if d.startswith("model.")
            ],
        )

    promoted = _apply_chunking_config(models, chunking_config or {})

    chunked = sum(1 for m in models.values() if m.kind == CHUNKED)
    logger.info(
        "Discovered %d eligible model(s) from %s "
        "(%d chunked + %d full-refresh; %d opted out; %d promoted via config).",
        len(models), manifest_path, chunked, len(models) - chunked,
        opted_out, promoted,
    )
    return models


def topological_order(
    models: dict[str, BackfillModel],
) -> list[BackfillModel]:
    """Return models in upstream-first topological order (name-sorted within layers).

    Raises ``ValueError`` on cycles.
    """
    remaining = set(models.keys())
    completed: set[str] = set()
    ordered: list[BackfillModel] = []

    while remaining:
        ready = sorted(
            (
                k for k in remaining
                if all(
                    dep in completed
                    for dep in models[k].depends_on
                    if dep in models
                )
            ),
            key=lambda k: models[k].name,
        )
        if not ready:
            raise ValueError(
                "Cycle in backfill graph. Stuck models: "
                f"{[models[k].name for k in remaining]}"
            )
        for k in ready:
            ordered.append(models[k])
            remaining.discard(k)
            completed.add(k)
    return ordered


def compute_static_chunks(
    models: dict[str, BackfillModel],
    today: date,
) -> dict[str, list[tuple[date, date, str]]]:
    """Static ``(start, end, "YYYY-MM")`` chunk list per ``CHUNKED`` model.

    Called once at DAG parse time; each tuple becomes one ``chunk_<YYYY-MM>`` task.
    """
    out: dict[str, list[tuple[date, date, str]]] = {}
    for m in models.values():
        if m.kind != CHUNKED or m.first_timestamp is None or m.chunk_months is None:
            continue
        out[m.name] = [
            (s, e, s.strftime("%Y-%m"))
            for s, e in _generate_chunks(m.first_timestamp, m.chunk_months, today)
        ]
    return out


# ── Internals ─────────────────────────────────────────────────────────────

def _is_eligible(node: dict) -> bool:
    if node.get("resource_type") != "model":
        return False
    return (node.get("config") or {}).get("materialized") != "ephemeral"


def _extract_first_timestamp(raw_sql: str) -> Optional[date]:
    """Earliest ``first_timestamp`` from any ``start_ts()`` call in ``raw_sql``.

    Comments are stripped first; when multiple calls are present the earliest wins
    so the backfill window covers every required range.
    """
    sql = _BLOCK_COMMENT_RE.sub("", raw_sql)
    sql = _LINE_COMMENT_RE.sub("", sql)
    parsed: list[date] = []
    for raw_ts in _START_TS_RE.findall(sql):
        try:
            parsed.append(date.fromisoformat(raw_ts[:10]))
        except ValueError:
            logger.warning(
                "Unparseable first_timestamp %r in start_ts() — ignoring.", raw_ts,
            )
    return min(parsed) if parsed else None


def _parse_config_entry(item: object, idx: int) -> Optional[ChunkingConfigEntry]:
    """Validate one config entry; return None (with a warning) if malformed."""
    if not isinstance(item, dict):
        logger.warning(
            "Chunking config entry #%d is not an object — skipped.", idx,
        )
        return None

    name = item.get("model_name")
    if not isinstance(name, str) or not name:
        logger.warning(
            "Chunking config entry #%d missing or empty 'model_name' — skipped.",
            idx,
        )
        return None

    enabled = item.get("chunking_enabled")
    if not isinstance(enabled, bool):
        logger.warning(
            "Chunking config entry for %r has non-bool 'chunking_enabled'=%r — skipped.",
            name, enabled,
        )
        return None

    raw_size = item.get("chunk_size")
    if raw_size is None:
        size: Optional[int] = None
    elif isinstance(raw_size, bool) or not isinstance(raw_size, int) or raw_size <= 0:
        logger.warning(
            "Chunking config entry for %r has invalid 'chunk_size'=%r "
            "(must be a positive integer) — entry will downgrade to full_refresh.",
            name, raw_size,
        )
        size = None
    else:
        size = raw_size

    return ChunkingConfigEntry(name, enabled, size)


def _apply_chunking_config(
    models: dict[str, BackfillModel],
    config: dict[str, ChunkingConfigEntry],
) -> int:
    """Promote eligible models from FULL_REFRESH → CHUNKED. Returns count promoted."""
    by_name = {m.name: m for m in models.values()}
    promoted = 0
    for cfg_name, entry in config.items():
        target = by_name.get(cfg_name)
        if target is None:
            logger.warning(
                "Chunking config references unknown model %r — skipped.", cfg_name,
            )
            continue
        if not entry.chunking_enabled:
            logger.info(
                "Chunking config explicitly disables %r — staying full_refresh.",
                target.name,
            )
            continue
        if target.first_timestamp is None:
            logger.warning(
                "Chunking config enables %r but its SQL has no start_ts() anchor "
                "— staying full_refresh.",
                target.name,
            )
            continue
        if entry.chunk_size is None:
            logger.warning(
                "Chunking config enables %r but provides no valid chunk_size "
                "— staying full_refresh.",
                target.name,
            )
            continue
        target.kind = CHUNKED
        target.chunk_months = entry.chunk_size
        promoted += 1
        logger.info(
            "Chunking config: %r → chunked, %d-month window (anchor=%s).",
            target.name, entry.chunk_size, target.first_timestamp,
        )
    return promoted


def _generate_chunks(
    first_timestamp: date, chunk_months: int, today: date,
) -> list[tuple[date, date]]:
    """Non-overlapping contiguous windows from ``first_timestamp`` to ``today``."""
    if chunk_months is None or chunk_months < 1:
        raise ValueError(
            f"chunk_months must be a positive int, got {chunk_months!r}"
        )
    chunks: list[tuple[date, date]] = []
    cursor = first_timestamp
    while cursor < today:
        end = min(cursor + relativedelta(months=chunk_months), today)
        chunks.append((cursor, end))
        cursor = end
    return chunks
