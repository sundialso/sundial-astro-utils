"""Thin wrapper around ``subprocess`` for invoking dbt under a backfill profile.

Internal module. Two entry points:

- :func:`dbt_invoke` — run any dbt subcommand and return its
  ``CompletedProcess``.
- :func:`strip_ansi` — clean ANSI escape codes from captured output
  (used by callers parsing ``dbt ls --output name`` results).

``--no-write-json`` is prepended to every invocation so concurrent dbt
runs don't race on ``target/manifest.json`` — without it the scheduler
re-parses the DAG mid-write, gets a corrupted manifest, and the
in-flight task disappears from the graph.
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(s: str) -> str:
    """Remove ANSI color/control codes from a string."""
    return _ANSI_RE.sub("", s)


def dbt_invoke(
    *,
    profile_config: Any,
    dbt_executable: str,
    project_dir: str | Path,
    profile_name: str,
    subcommand_args: Iterable[str],
    log_label: str,
    extra_global_flags: Iterable[str] | None = None,
    stream_output: bool = False,
    extra_env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a dbt subcommand inside ``profile_config.ensure_profile()``.

    Parameters
    ----------
    profile_config:
        A Cosmos ``ProfileConfig`` whose ``ensure_profile()`` context
        manager yields ``(profile_path, profile_env)``. ``target_name``
        is taken from it directly.
    dbt_executable:
        Absolute path to the ``dbt`` binary inside the project's venv.
    project_dir:
        Absolute path to the dbt project root.
    profile_name:
        Name of the profile entry in ``profiles.yml`` to use.
    subcommand_args:
        e.g. ``["run", "--select", "my_model"]``.
    log_label:
        Prefix used in INFO logs for this invocation (e.g.
        ``"dbt run [my_model]"``).
    extra_global_flags:
        dbt global flags inserted **after** ``--no-write-json`` and
        **before** the subcommand. Use for ``--quiet`` etc.
    stream_output:
        When ``True``, line-stream dbt's stdout (merged with stderr)
        through ``print()`` so it lands directly in the Airflow task
        log. When ``False`` (default), capture both streams and log
        them once via the module logger after dbt exits.

        Streaming is used by the final reporting tasks so the rendered
        report appears inline as it's generated. ``CompletedProcess``
        returned in stream mode has empty ``stdout``/``stderr`` — only
        ``returncode`` is meaningful.
    extra_env:
        Environment variables merged into the subprocess env on top of
        ``os.environ`` + ``profile_env``.

    Returns
    -------
    The ``subprocess.CompletedProcess`` for the dbt invocation. Callers
    decide whether a non-zero ``returncode`` should raise.
    """
    with profile_config.ensure_profile() as (profile_path, profile_env):
        cmd = [
            dbt_executable,
            "--no-write-json",
            *(extra_global_flags or []),
            *subcommand_args,
            "--project-dir", str(project_dir),
            "--profiles-dir", str(Path(profile_path).parent),
            "--profile", profile_name,
            "--target", profile_config.target_name,
        ]
        env = {**os.environ, **profile_env, **(extra_env or {})}
        logger.info("%s cmd: %s", log_label, " ".join(cmd))

        if stream_output:
            # Popen + per-line print() (not capture_output=False) because
            # Airflow's SDK wraps sys.stdout at the Python level — a
            # subprocess that inherits raw fds bypasses the task-log
            # capture entirely. Routing every line through print() ensures
            # the report reaches the task log regardless of executor
            # stdio wiring.
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                print(line, end="")
            proc.wait()
            return subprocess.CompletedProcess(
                args=cmd, returncode=proc.returncode, stdout="", stderr=""
            )

        result = subprocess.run(
            cmd, capture_output=True, text=True, env=env, check=False
        )

    logger.info("%s stdout:\n%s", log_label, result.stdout)
    if result.stderr:
        logger.warning("%s stderr:\n%s", log_label, result.stderr)
    return result
