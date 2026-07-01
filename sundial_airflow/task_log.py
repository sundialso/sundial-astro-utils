"""Structured, low-noise logging for Airflow task execution."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_BANNER = "=" * 78


def log_block(title: str, lines: list[str]) -> None:
    """Emit a titled multi-line INFO block."""
    logger.info("\n%s\n%s\n%s", title, "\n".join(lines), _BANNER)


def _fmt(value: Any) -> str:
    if value is None or value == "":
        return "(none)"
    return str(value)


def _compact_chunk_ids(chunks: list[dict], *, max_ids: int = 5) -> str:
    if not chunks:
        return ""
    ids = [str(c.get("chunk_id", "?")) for c in chunks]
    if len(ids) <= max_ids:
        return ", ".join(ids)
    head = ", ".join(ids[:2])
    tail = ", ".join(ids[-2:])
    return f"{head}, … ({len(ids)} total), …, {tail}"


def log_prepare_dbt_args_summary(
    *,
    run_id: str | None,
    params: dict[str, Any],
    param_field: str,
    target_value: str,
    warehouse: str,
    backfill_mode: str,
    run_context: str,
    full_refresh: bool,
    dbt_vars: dict[str, Any],
    selected_models: set[str] | None,
    run_plan: dict[str, dict] | None = None,
    watermarks: dict[str, Any] | None = None,
    start_var: str | None = None,
    end_var: str | None = None,
) -> None:
    """Pretty-print prepare_dbt_args inputs and the prepared XCom payload."""
    lines = [
        "INPUT",
        f"  run_id:         {_fmt(run_id)}",
        f"  backfill_mode:  {backfill_mode}",
        f"  execution_ts:   {_fmt(dbt_vars.get('execution_ts'))}",
        f"  {param_field}:  {_fmt(target_value)}",
        f"  warehouse:      {warehouse}",
        f"  select:         {_fmt(params.get('select'))}",
        f"  exclude:        {_fmt(params.get('exclude'))}",
    ]
    if backfill_mode == "partial" and start_var and end_var:
        lines.extend([
            f"  {start_var}: {_fmt(dbt_vars.get(start_var))}",
            f"  {end_var}:   {_fmt(dbt_vars.get(end_var))}",
        ])

    lines.extend([
        "",
        "OUTPUT",
        f"  run_context:   {run_context}",
        f"  full_refresh:  {full_refresh}",
        f"  run_group_id:  {_fmt(dbt_vars.get('run_group_id'))}",
    ])
    if selected_models is not None:
        lines.append(f"  selected:      {len(selected_models)} model(s)")
    else:
        lines.append("  selected:      (all models)")

    run_plan = run_plan or {}
    watermarks = watermarks or {}
    if run_plan:
        lines.extend(["", "CHUNKED RUN PLANS"])
        for name in sorted(run_plan):
            plan = run_plan[name]
            disposition = plan.get("disposition", "?")
            chunks = plan.get("chunks") or []
            wm_str = _fmt(watermarks.get(name))
            if disposition == "single":
                action = "single → run_incremental"
            else:
                action = (
                    f"{len(chunks)} chunk(s) → run_chunk: "
                    f"{_compact_chunk_ids(chunks)}"
                )
            lines.append(f"  {name}")
            lines.append(f"    watermark:   {wm_str}")
            lines.append(f"    disposition: {action}")

    log_block("prepare_dbt_args", lines)


def log_chunk_units(
    model_name: str,
    units: list[dict],
    *,
    disposition: str | None = None,
) -> None:
    if units:
        ids = ", ".join(u["chunk_id"] for u in units)
        logger.info("[%s] chunk_units → %d chunk(s): %s", model_name, len(units), ids)
        return
    logger.info(
        "[%s] chunk_units → (none; disposition=%s)",
        model_name,
        disposition or "missing",
    )


def log_chunk_task(
    model_name: str,
    task: str,
    *,
    chunk_id: str | None = None,
    window: str | None = None,
    full_refresh: bool | None = None,
) -> None:
    parts = [f"[{model_name}] {task}"]
    if chunk_id:
        parts.append(f"chunk={chunk_id}")
    if window:
        parts.append(window)
    if full_refresh is not None:
        parts.append(f"full_refresh={full_refresh}")
    logger.info(" ".join(parts))


def log_dbt_run_result(
    model_name: str,
    *,
    returncode: int,
    stdout: str,
    stderr: str,
    chunk_id: str | None = None,
    tail_lines: int = 8,
) -> None:
    label = f"[{model_name}]"
    if chunk_id:
        label = f"{label} chunk={chunk_id}"
    if returncode == 0:
        tail = _tail(stdout, tail_lines)
        if tail:
            logger.debug("%s dbt stdout (tail):\n%s", label, tail)
        logger.info("%s dbt run OK", label)
        return
    logger.error(
        "%s dbt run FAILED (exit=%d)\n--- stdout ---\n%s\n--- stderr ---\n%s",
        label,
        returncode,
        stdout,
        stderr,
    )


def _tail(text: str, n: int) -> str:
    lines = [line for line in text.rstrip().splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-n:])
