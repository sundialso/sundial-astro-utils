"""Snowflake warehouse sizing helpers for chunked backfills.

``create_dag`` upsizes the warehouse before a chunked run and restores it
afterwards. These helpers own the size ladder and the Snowflake SQL.
"""
from __future__ import annotations

import logging
import re
from contextlib import closing

logger = logging.getLogger(__name__)

# Snowflake warehouse sizes, smallest → largest. These strings are what
# ``SHOW WAREHOUSES`` returns and are also accepted by
# ``ALTER WAREHOUSE ... SET WAREHOUSE_SIZE``.
_SIZES = [
    "X-Small",
    "Small",
    "Medium",
    "Large",
    "X-Large",
    "2X-Large",
    "3X-Large",
    "4X-Large",
    "5X-Large",
    "6X-Large",
]

# Map every spelling Snowflake accepts (enum form, synonyms) to a canonical size.
_ALIASES = {
    "XSMALL": "X-Small",
    "SMALL": "Small",
    "MEDIUM": "Medium",
    "LARGE": "Large",
    "XLARGE": "X-Large",
    "XXLARGE": "2X-Large",
    "2XLARGE": "2X-Large",
    "XXXLARGE": "3X-Large",
    "3XLARGE": "3X-Large",
    "X4LARGE": "4X-Large",
    "4XLARGE": "4X-Large",
    "X5LARGE": "5X-Large",
    "5XLARGE": "5X-Large",
    "X6LARGE": "6X-Large",
    "6XLARGE": "6X-Large",
}


def _canonical_size(size: str | None) -> str | None:
    """Return the canonical size string for any Snowflake spelling, or None."""
    if not size:
        return None
    return _ALIASES.get(re.sub(r"[^A-Z0-9]", "", str(size).upper()))


def next_warehouse_size(size: str | None) -> str | None:
    """Return the next-larger size, or None if unknown or already the largest."""
    canonical = _canonical_size(size)
    if canonical is None:
        return None
    idx = _SIZES.index(canonical)
    return _SIZES[idx + 1] if idx + 1 < len(_SIZES) else None


def _esc(literal: str) -> str:
    """Escape single quotes so ``literal`` is safe inside a SQL string."""
    return str(literal).replace("'", "''")


def _hook(conn_id: str):
    """Return a SnowflakeHook for ``conn_id`` with SQL logging disabled."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    hook.log_sql = False
    return hook


def resolve_warehouse_name(conn_id: str) -> str | None:
    """Resolve the Snowflake warehouse for a connection (extra, else session)."""
    hook = _hook(conn_id)
    try:
        extra = hook.get_connection(conn_id).extra_dejson or {}
        for key in ("warehouse", "extra__snowflake__warehouse"):
            if extra.get(key):
                return str(extra[key])
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not read warehouse from connection extra: %s", exc)
    rows = hook.get_records("SELECT CURRENT_WAREHOUSE()")
    if rows and rows[0][0]:
        return str(rows[0][0])
    return None


def get_warehouse_size(conn_id: str, warehouse_name: str) -> str | None:
    """Return the current size of ``warehouse_name``, or None if not found."""
    hook = _hook(conn_id)
    with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cur:
        cur.execute(f"SHOW WAREHOUSES LIKE '{_esc(warehouse_name)}'")
        rows = cur.fetchall()
        if not rows:
            return None
        cols = [c[0].lower() for c in cur.description]
        if "size" not in cols:
            return None
        # ``SHOW ... LIKE`` treats ``%``/``_`` as wildcards (warehouse names
        # routinely contain ``_``), so the pattern can match sibling warehouses.
        # Keep only the exact name match before reading size; SHOW matching is
        # case-insensitive, so compare case-insensitively too.
        if "name" in cols:
            name_idx = cols.index("name")
            want = str(warehouse_name).upper()
            rows = [r for r in rows if str(r[name_idx]).upper() == want]
            if not rows:
                return None
        return rows[0][cols.index("size")]


def set_warehouse_size(conn_id: str, warehouse_name: str, size: str) -> None:
    """Set ``warehouse_name`` to ``size`` via ALTER WAREHOUSE."""
    hook = _hook(conn_id)
    hook.run(
        f"ALTER WAREHOUSE IDENTIFIER('{_esc(warehouse_name)}') "
        f"SET WAREHOUSE_SIZE = '{_esc(size)}'"
    )
