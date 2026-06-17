"""Runtime helpers for shelling out to the dbt CLI from Airflow tasks."""
from __future__ import annotations

import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def ensure_dbt_deps(
    dbt_executable: str,
    project_dir: str,
    env: dict | None = None,
    timeout: int = 120,
) -> None:
    """Install ``packages.yml`` dependencies into ``dbt_packages/``.

    ``dbt ls`` (and any dbt compile step) refuses to run when ``packages.yml``
    declares packages that are not present in ``dbt_packages/`` — it exits 2
    with ``"found N package(s) ... but only 0 installed ... Run 'dbt deps'"``.

    Tenants that bake ``dbt deps`` into their image (``RUN dbt deps`` in the
    Dockerfile) satisfy this at build time, but we cannot rely on every
    deployment doing so. Run ``dbt deps`` here before ``dbt ls`` so the
    selection-resolution path is self-sufficient. ``dbt deps`` is idempotent
    and fast (~1-2s) when ``dbt_packages/`` already matches ``packages.yml``,
    and a no-op when no ``packages.yml`` exists.
    """
    result = subprocess.run(
        [dbt_executable, "deps", "--project-dir", project_dir],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env if env is not None else {**os.environ},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "dbt deps failed (exit=%d):\nSTDOUT:\n%s\nSTDERR:\n%s"
            % (result.returncode, result.stdout, result.stderr)
        )
    logger.info("dbt deps installed packages into %s/dbt_packages", project_dir)
