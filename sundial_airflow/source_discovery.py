"""Discover dbt source tables that need source tests.

Scanned at DAG-parse time so the factory can materialise one
``DbtTestLocalOperator`` per source table, and wire each source test only to
the models that actually consume that source (no global gate).
"""
from __future__ import annotations

import re
from pathlib import Path

# Default locations we look in for the ``sources.yml`` blob, in priority order.
# BigQuery tenants usually keep theirs at ``models/sources.yml`` while Snowflake
# tenants keep theirs at ``models/sources/_sources.yml``; we accept both.
DEFAULT_SOURCES_CANDIDATES: tuple[tuple[str, ...], ...] = (
    ("models", "sources", "_sources.yml"),
    ("models", "sources.yml"),
    ("models", "marts", "sources.yml"),
    ("models", "_sources.yml"),
)

_SOURCE_REF_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)"
)


def discover_source_tables_with_tests(
    project_path: str | Path,
    *,
    sources_yml_candidates: list[Path] | None = None,
    recursive_tests: bool = True,
) -> list[tuple[str, str]]:
    """Return ``(source_name, table_name)`` tuples that have at least one test.

    A source table is considered "tested" if either:

    - A singular SQL test under ``tests/`` references it via ``source(...)``.
    - The corresponding YAML block in ``sources.yml`` declares table-level
      tests or column-level tests on any of its columns.

    Parameters
    ----------
    project_path:
        Absolute path to the dbt project root (the directory containing
        ``dbt_project.yml``).
    sources_yml_candidates:
        Optional override list of YAML files to consult. Defaults to a sensible
        set of common locations.
    recursive_tests:
        Whether to recurse into ``tests/`` (Snowflake tenants nest tests under
        ``tests/singular`` etc; BigQuery tenants generally don't). Defaults to
        ``True``.
    """
    import yaml

    project = Path(project_path)
    found: set[tuple[str, str]] = set()

    test_dir = project / "tests"
    if test_dir.exists():
        iterator = test_dir.rglob("*.sql") if recursive_tests else test_dir.glob("*.sql")
        for sql_file in iterator:
            try:
                content = sql_file.read_text()
            except OSError:
                continue
            for match in _SOURCE_REF_RE.findall(content):
                found.add(match)

    candidates: list[Path]
    if sources_yml_candidates is not None:
        candidates = list(sources_yml_candidates)
    else:
        candidates = [project.joinpath(*parts) for parts in DEFAULT_SOURCES_CANDIDATES]

    for sources_yml in candidates:
        if not sources_yml.exists():
            continue
        try:
            with open(sources_yml, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except Exception:
            config = {}
        for source in config.get("sources", []):
            source_name = source["name"]
            for table in source.get("tables", []):
                table_name = table["name"]
                has_tests = bool(table.get("tests"))
                if not has_tests:
                    has_tests = any(
                        col.get("tests") for col in table.get("columns", [])
                    )
                if has_tests:
                    found.add((source_name, table_name))

    return sorted(found)


def discover_source_to_models(
    project_path: str | Path,
    *,
    models_subdir: str = "models",
) -> dict[tuple[str, str], list[str]]:
    """Return ``{(source_name, table_name): [model_name, ...]}``.

    Walks every ``.sql`` file under ``<project>/<models_subdir>/`` and records
    which models reference each source via ``{{ source('s', 't') }}``. The
    factory uses this map to wire each per-source ``DbtTestLocalOperator``
    directly to the model run tasks that consume that source, so a single
    failing source test only blocks its own subtree instead of the whole
    pipeline.

    The model name is the SQL file's basename (without ``.sql``), which is the
    name Cosmos uses when it builds the ``DbtTaskGroup`` tasks.
    """
    project = Path(project_path)
    models_dir = project / models_subdir
    if not models_dir.exists():
        return {}

    mapping: dict[tuple[str, str], set[str]] = {}
    for sql_file in models_dir.rglob("*.sql"):
        try:
            content = sql_file.read_text()
        except OSError:
            continue
        model_name = sql_file.stem
        for source_name, table_name in _SOURCE_REF_RE.findall(content):
            mapping.setdefault((source_name, table_name), set()).add(model_name)

    return {key: sorted(models) for key, models in sorted(mapping.items())}
