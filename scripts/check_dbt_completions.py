#!/usr/bin/env python3
"""Ad-hoc audit: does every dbt model have an entry in the ``dbt_completions`` view?

For each Sundial tenant the script:

  1. Reads the tenant's connection secret from AWS Secrets Manager. The secret is
     named ``<tenant_slug>_hybrid_sync``. Its shape tells us the warehouse:
       - a ``{"bigquery": {project, dataset, location, ...}}`` block  -> BigQuery
       - the flat Snowflake fields (account/user/private_key/...)     -> Snowflake
  2. Connects to that warehouse:
       - BigQuery   : Application Default Credentials (your ``gcloud auth login``)
       - Snowflake  : key-pair auth, using the ``private_key`` from the secret
  3. Reads the ``dbt_completions`` view for the LATEST run (max execution_ts) and
     collects the distinct model names that produced an entry.
  4. Parses the tenant's dbt ``target/manifest.json`` for the authoritative set of
     models (the tenant's own package, excluding ephemeral models, which never run
     hooks and so can never have a completions entry).
  5. Reports counts + the models that are missing from the latest run.

The warehouse can only tell you which models *did* write entries; the manifest is
what lets us see which models *should* have but didn't. That diff is the point.

------------------------------------------------------------------------------
Requirements (install into a venv):

    pip install boto3 snowflake-connector-python google-cloud-bigquery cryptography

Auth assumed already configured locally:
    - AWS creds/SSO (env, ~/.aws, or SSO) with read access to Secrets Manager
    - ``gcloud auth application-default login`` for the BigQuery tenants

------------------------------------------------------------------------------
Usage:

    python scripts/check_dbt_completions.py --repos-dir ~/sundial

    # restrict to specific tenants
    python scripts/check_dbt_completions.py --repos-dir ~/sundial --tenant mirage --tenant chronicle

    # point at manifests explicitly when the repo layout differs
    python scripts/check_dbt_completions.py \
        --manifest mirage=/path/to/mirage_dbt/target/manifest.json

    # machine-readable output
    python scripts/check_dbt_completions.py --repos-dir ~/sundial --json report.json

Manifest discovery (unless overridden with --manifest <slug>=<path>): for each
slug the script looks for ``target/manifest.json`` under, in order,
``<repos-dir>/<slug>_dbt`` then ``<repos-dir>/<slug>``.

If no manifest is found but the project dir is, it FALLS BACK to scanning the
project's ``model-paths`` for ``*.sql`` files (a rough denominator — includes
ephemeral/disabled models that never write entries). Run ``dbt parse`` in the
repo to get an exact manifest. If neither a manifest nor a project dir is found,
the tenant is reported data-only (no coverage).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

VIEW_NAME = "dbt_completions"
DEFAULT_SECRET_SUFFIX = "_hybrid_sync"
DEFAULT_REGION = "us-east-2"  # implied by the SUNDIAL-AWS_US_EAST_2 account

# Non-tenant *_dbt repos that must never be audited (test/non-prod). Matched
# against the secret slug (repo intuit_dbt -> slug "intuit", etc.). Override with
# --exclude / --no-default-excludes.
DEFAULT_EXCLUDED_SLUGS = {"intuit", "yahoo_testing"}


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------
@dataclass
class TenantResult:
    slug: str
    warehouse: str = ""
    latest_execution_ts: str | None = None
    total_view_rows: int | None = None          # all-time rows in the view
    models_in_latest_run: set[str] = field(default_factory=set)
    manifest_models: set[str] | None = None      # None => no model list available
    ephemeral_count: int = 0
    manifest_path: str | None = None
    model_source: str | None = None              # "manifest" | "source-scan" | None
    error: str | None = None

    @property
    def missing(self) -> set[str]:
        """In the manifest but absent from the latest run."""
        if self.manifest_models is None:
            return set()
        return self.manifest_models - self.models_in_latest_run

    @property
    def unknown(self) -> set[str]:
        """In the latest run but not in the manifest (renamed/removed/package models)."""
        if self.manifest_models is None:
            return set()
        return self.models_in_latest_run - self.manifest_models

    @property
    def coverage(self) -> float | None:
        if not self.manifest_models:
            return None
        return len(self.manifest_models & self.models_in_latest_run) / len(self.manifest_models)


# ---------------------------------------------------------------------------
# AWS Secrets Manager
# ---------------------------------------------------------------------------
def list_tenant_secrets(region: str, suffix: str) -> dict[str, str]:
    """Return {tenant_slug: secret_name} for every secret ending in ``suffix``."""
    import boto3

    client = boto3.client("secretsmanager", region_name=region)
    paginator = client.get_paginator("list_secrets")
    out: dict[str, str] = {}
    for page in paginator.paginate():
        for entry in page.get("SecretList", []):
            name = entry["Name"]
            if name.endswith(suffix):
                slug = name[: -len(suffix)]
                out[slug] = name
    return out


def fetch_secret(region: str, secret_name: str) -> dict[str, Any]:
    import boto3

    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    raw = resp.get("SecretString")
    if raw is None:
        raise ValueError(f"{secret_name}: binary secrets are not supported")
    return json.loads(raw)


def detect_warehouse(secret: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return (warehouse, params) from the secret JSON shape.

    Handles both nested ({"bigquery": {...}} / {"snowflake": {...}}) and flat
    Snowflake secrets (account/private_key at the top level).
    """
    if isinstance(secret.get("bigquery"), dict):
        return "bigquery", secret["bigquery"]
    if isinstance(secret.get("snowflake"), dict):
        return "snowflake", secret["snowflake"]
    if "account" in secret and "private_key" in secret:
        return "snowflake", secret
    if "project" in secret and "dataset" in secret:
        return "bigquery", secret
    raise ValueError("could not determine warehouse type from secret shape")


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------
def _normalize_pem(key: str) -> bytes:
    """Coerce a possibly-mangled PEM string into valid PEM bytes.

    Secrets sometimes store the key with literal ``\\n`` escapes, or with the
    base64 body space-separated on one line. Rebuild proper 64-char-wrapped PEM
    so cryptography can parse it (works for both PKCS#1 'RSA PRIVATE KEY' and
    PKCS#8 'PRIVATE KEY').
    """
    key = key.strip()
    if "\\n" in key and "\n" not in key:
        key = key.replace("\\n", "\n")
    if "\n" in key:
        return key.encode()

    # Single line: split header/footer from the base64 body.
    import re

    m = re.match(r"-----BEGIN ([A-Z ]+)-----(.*?)-----END \1-----", key, re.DOTALL)
    if not m:
        return key.encode()  # let the parser raise a clear error
    label, body = m.group(1).strip(), m.group(2).strip()
    body = "".join(body.split())  # drop interior whitespace
    wrapped = "\n".join(body[i : i + 64] for i in range(0, len(body), 64))
    return f"-----BEGIN {label}-----\n{wrapped}\n-----END {label}-----\n".encode()


def _snowflake_private_key_der(pem: str, passphrase: str | None) -> bytes:
    from cryptography.hazmat.primitives import serialization

    private_key = serialization.load_pem_private_key(
        _normalize_pem(pem),
        password=passphrase.encode() if passphrase else None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def query_snowflake(params: dict[str, Any]) -> tuple[str | None, int, set[str]]:
    import snowflake.connector

    pkey_der = _snowflake_private_key_der(
        params["private_key"], params.get("private_key_passphrase")
    )
    database = params["database"]
    schema = params["schema"]
    view = f"{database}.{schema}.{VIEW_NAME}"

    conn = snowflake.connector.connect(
        account=params["account"],
        user=params["user"],
        private_key=pkey_der,
        role=params.get("role"),
        warehouse=params.get("warehouse"),
        database=database,
        schema=schema,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(execution_ts), COUNT(*) FROM {view}")
        latest_ts, total_rows = cur.fetchone()
        if latest_ts is None:
            return None, int(total_rows or 0), set()
        cur.execute(
            f"SELECT DISTINCT model_name FROM {view} WHERE execution_ts = %s",
            (latest_ts,),
        )
        models = {r[0] for r in cur.fetchall()}
        return str(latest_ts), int(total_rows or 0), models
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
def query_bigquery(params: dict[str, Any]) -> tuple[str | None, int, set[str]]:
    from google.cloud import bigquery

    project = params["project"]
    dataset = params["dataset"]
    location = params.get("location")
    view = f"`{project}.{dataset}.{VIEW_NAME}`"

    client = bigquery.Client(project=project, location=location)
    row = list(client.query(f"SELECT MAX(execution_ts) AS ts, COUNT(*) AS n FROM {view}").result())[0]
    latest_ts, total_rows = row["ts"], row["n"]
    if latest_ts is None:
        return None, int(total_rows or 0), set()

    job = client.query(
        f"SELECT DISTINCT model_name FROM {view} WHERE execution_ts = @ts",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ts", "STRING", str(latest_ts))]
        ),
    )
    models = {r["model_name"] for r in job.result()}
    return str(latest_ts), int(total_rows or 0), models


# ---------------------------------------------------------------------------
# dbt manifest
# ---------------------------------------------------------------------------
# Dirs to skip when hunting for dbt_project.yml — installed packages and build
# output each carry their own dbt_project.yml and would be false matches.
_SKIP_DIRS = {
    "dbt_packages", "dbt_modules", "target", "logs", "venv", ".venv",
    ".git", "node_modules", "__pycache__",
}


def _find_dbt_project(root: Path, max_depth: int = 3) -> Path | None:
    """Return the shallowest dir under ``root`` (inclusive) holding a
    dbt_project.yml, skipping package/build dirs. Handles both the Snowflake
    layout (project at repo root) and the BigQuery one (under e.g.
    ``dbt_bq_project/``) without per-tenant config.
    """
    # Breadth-first so the shallowest match wins (the real project sits above any
    # nested package copies).
    queue: list[tuple[Path, int]] = [(root, 0)]
    while queue:
        d, depth = queue.pop(0)
        if (d / "dbt_project.yml").is_file():
            return d
        if depth >= max_depth:
            continue
        try:
            children = sorted(p for p in d.iterdir() if p.is_dir() and p.name not in _SKIP_DIRS)
        except OSError:
            children = []
        queue.extend((c, depth + 1) for c in children)
    return None


def _read_project_slug(project_dir: Path) -> str | None:
    """The tenant slug a dbt project declares = its ``name:`` minus a trailing
    ``_dbt`` (e.g. mirage_dbt's project is ``captions_dbt`` -> slug ``captions``).
    """
    try:
        import yaml

        cfg = yaml.safe_load((project_dir / "dbt_project.yml").read_text()) or {}
        name = cfg.get("name")
    except Exception:
        return None
    if not name:
        return None
    return name[:-4] if name.endswith("_dbt") else name


def build_project_index(repos_dir: Path | None, repo_glob: str = "*_dbt") -> dict[str, str]:
    """Scan ``repos_dir`` for ``*_dbt`` repos and map each tenant slug (from the
    repo's dbt_project.yml ``name``) to its project dir. This is authoritative —
    it doesn't rely on the repo *folder* name matching the slug (it usually
    doesn't, e.g. captions lives in mirage_dbt).
    """
    index: dict[str, str] = {}
    if repos_dir is None or not repos_dir.is_dir():
        return index
    for repo in sorted(repos_dir.glob(repo_glob)):
        if not repo.is_dir():
            continue
        proj = _find_dbt_project(repo)
        if proj is None:
            continue
        slug = _read_project_slug(proj)
        if slug:
            index[slug] = str(proj)
    return index


def find_project_dir(
    slug: str, repos_dir: Path | None, repo_overrides: dict[str, str] | None = None
) -> Path | None:
    """Locate a tenant's dbt project dir (the one holding dbt_project.yml),
    searching each repo-root candidate recursively so a nested layout like
    ``<repo>/dbt_bq_project/`` is found as well as a root-level one.

    The repo folder name doesn't always equal the secret slug (e.g. the
    ``mirage`` secret's repo is ``captions``). ``repo_overrides`` maps
    slug -> repo folder name or path; an absolute/relative path is used as-is,
    a bare name is resolved under ``--repos-dir``.
    """
    candidates: list[Path] = []
    override = (repo_overrides or {}).get(slug)
    if override:
        p = Path(override).expanduser()
        if p.is_absolute() or p.exists():
            candidates.append(p)
        elif repos_dir is not None:
            candidates.append(repos_dir / override)
    if repos_dir is not None:
        candidates.append(repos_dir / f"{slug}_dbt")
        candidates.append(repos_dir / slug)

    for candidate in candidates:
        if candidate.is_dir():
            found = _find_dbt_project(candidate)
            if found is not None:
                return found
    return None


def find_manifest(
    slug: str,
    repos_dir: Path | None,
    overrides: dict[str, str],
    repo_overrides: dict[str, str] | None = None,
) -> Path | None:
    if slug in overrides:
        p = Path(overrides[slug]).expanduser()
        return p if p.is_file() else None
    project = find_project_dir(slug, repos_dir, repo_overrides)
    if project is None:
        return None
    manifest = project / "target" / "manifest.json"
    return manifest if manifest.is_file() else None


def scan_source_models(project_dir: Path) -> set[str] | None:
    """Fallback when no manifest exists: count model .sql files directly.

    Reads ``model-paths`` from dbt_project.yml (default ["models"]) and returns
    the set of .sql filename stems under them. The completions ``model_name`` is
    exactly this stem (the macro logs ``this.name``), so the set diffs cleanly.

    Caveat vs a manifest: this can't see materialization, so ephemeral models are
    included (they never write entries -> they'd show as false-missing), and
    disabled models are counted too. It's a rough denominator, not authoritative.
    Returns None if the project dir / dbt_project.yml can't be read.
    """
    proj_yml = project_dir / "dbt_project.yml"
    if not proj_yml.is_file():
        return None
    model_paths = ["models"]
    try:
        import yaml  # PyYAML — already a dependency of this package

        cfg = yaml.safe_load(proj_yml.read_text()) or {}
        model_paths = cfg.get("model-paths") or cfg.get("source-paths") or ["models"]
    except Exception:
        pass  # fall back to the default ["models"]

    names: set[str] = set()
    for mp in model_paths:
        root = (project_dir / mp)
        if not root.is_dir():
            continue
        for sql in root.rglob("*.sql"):
            names.add(sql.stem)
    return names or None


def parse_manifest_models(manifest_path: Path) -> tuple[set[str], int]:
    """Return (non-ephemeral model names of the root package, ephemeral count)."""
    data = json.loads(manifest_path.read_text())
    project_name = data.get("metadata", {}).get("project_name")
    models: set[str] = set()
    ephemeral = 0
    for node in data.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        # Only the tenant's own package — hooks are wired under the project key,
        # so installed-package models don't write completions entries.
        if project_name and node.get("package_name") != project_name:
            continue
        if node.get("config", {}).get("materialized") == "ephemeral":
            ephemeral += 1
            continue
        models.add(node["name"])
    return models, ephemeral


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def audit_tenant(
    slug: str,
    secret_name: str,
    region: str,
    repos_dir: Path | None,
    manifest_overrides: dict[str, str],
    model_count: str = "source-scan",
    repo_overrides: dict[str, str] | None = None,
) -> TenantResult:
    res = TenantResult(slug=slug)
    try:
        secret = fetch_secret(region, secret_name)
        warehouse, params = detect_warehouse(secret)
        res.warehouse = warehouse

        if warehouse == "snowflake":
            latest_ts, total_rows, models = query_snowflake(params)
        elif warehouse == "bigquery":
            latest_ts, total_rows, models = query_bigquery(params)
        else:
            raise ValueError(f"unsupported warehouse {warehouse!r}")

        res.latest_execution_ts = latest_ts
        res.total_view_rows = total_rows
        res.models_in_latest_run = models
    except Exception as exc:  # one tenant failing must not abort the sweep
        res.error = f"{type(exc).__name__}: {exc}"
        return res

    # An explicit --manifest override always wins (you asked for that exact file).
    # Otherwise: source-scan counts .sql files in models/ (default, no dbt needed);
    # manifest mode parses target/manifest.json for an exact, ephemeral-aware count.
    use_manifest = slug in manifest_overrides or model_count == "manifest"

    if use_manifest:
        manifest_path = find_manifest(slug, repos_dir, manifest_overrides, repo_overrides)
        if manifest_path is not None:
            try:
                res.manifest_models, res.ephemeral_count = parse_manifest_models(manifest_path)
                res.manifest_path = str(manifest_path)
                res.model_source = "manifest"
            except Exception as exc:
                res.error = f"manifest parse failed: {type(exc).__name__}: {exc}"
            return res
        # asked for manifest but none found — fall through to the .sql scan

    project_dir = find_project_dir(slug, repos_dir, repo_overrides)
    if project_dir is not None:
        scanned = scan_source_models(project_dir)
        if scanned is not None:
            res.manifest_models = scanned
            res.manifest_path = str(project_dir)
            res.model_source = "source-scan"
    return res


def print_report(results: list[TenantResult]) -> None:
    print("\n" + "=" * 78)
    print("DBT COMPLETIONS AUDIT — latest run per tenant")
    print("=" * 78)
    for r in sorted(results, key=lambda x: x.slug):
        print(f"\n● {r.slug}  [{r.warehouse or '?'}]")
        if r.error:
            print(f"    ERROR: {r.error}")
            continue
        n_latest = len(r.models_in_latest_run)
        print(f"    latest execution_ts : {r.latest_execution_ts}")
        print(f"    models in latest run : {n_latest}")
        print(f"    total view rows      : {r.total_view_rows}")
        if r.manifest_models is None:
            print("    project models       : UNAVAILABLE — no manifest.json and no "
                  "project dir found; can't compute coverage.")
            print("      → pass --repos-dir / --manifest, or run `dbt parse` in the "
                  "repo to generate target/manifest.json")
            continue
        total = len(r.manifest_models)
        cov = r.coverage
        src_note = {
            "manifest": "manifest.json",
            "source-scan": "models/*.sql scan — ROUGH: includes ephemeral/disabled, "
                           "run `dbt parse` for an exact count",
        }.get(r.model_source, r.model_source or "?")
        print(f"    model source         : {src_note}")
        print(f"    models in dbt project: {total}"
              + (f"  (+{r.ephemeral_count} ephemeral, excluded)" if r.ephemeral_count else ""))
        print(f"    coverage             : {cov * 100:.1f}%" if cov is not None else
              "    coverage             : n/a")
        if r.missing:
            print(f"    MISSING ({len(r.missing)}) — in project, not in latest run:")
            for m in sorted(r.missing):
                print(f"        - {m}")
        else:
            print("    ✓ every project model has an entry in the latest run")
        if r.unknown:
            print(f"    note: {len(r.unknown)} model(s) in the view are not in the "
                  f"project (renamed/removed?): {', '.join(sorted(r.unknown))}")

    # Summary line.
    ok = [r for r in results if not r.error and r.manifest_models is not None and not r.missing]
    incomplete = [r for r in results if not r.error and r.manifest_models is not None and r.missing]
    errored = [r for r in results if r.error]
    no_manifest = [r for r in results if not r.error and r.manifest_models is None]
    print("\n" + "-" * 78)
    print(f"SUMMARY: {len(results)} tenants — {len(ok)} fully covered, "
          f"{len(incomplete)} with missing models, {len(no_manifest)} without a manifest, "
          f"{len(errored)} errored")
    print("-" * 78)


def to_json(results: list[TenantResult]) -> list[dict[str, Any]]:
    out = []
    for r in results:
        out.append({
            "slug": r.slug,
            "warehouse": r.warehouse,
            "error": r.error,
            "latest_execution_ts": r.latest_execution_ts,
            "total_view_rows": r.total_view_rows,
            "models_in_latest_run": sorted(r.models_in_latest_run),
            "manifest_models": sorted(r.manifest_models) if r.manifest_models is not None else None,
            "model_source": r.model_source,
            "ephemeral_count": r.ephemeral_count,
            "missing": sorted(r.missing),
            "unknown": sorted(r.unknown),
            "coverage": r.coverage,
            "manifest_path": r.manifest_path,
        })
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION),
                    help=f"AWS region for Secrets Manager (default {DEFAULT_REGION})")
    ap.add_argument("--secret-suffix", default=DEFAULT_SECRET_SUFFIX,
                    help=f"secret name suffix identifying tenant secrets (default {DEFAULT_SECRET_SUFFIX})")
    ap.add_argument("--repos-dir", type=Path, default=None,
                    help="directory containing tenant repos (<slug>_dbt/...)")
    ap.add_argument("--model-count", choices=("source-scan", "manifest"), default="source-scan",
                    help="how to count project models: 'source-scan' = count .sql files in "
                         "models/ (default, no dbt needed); 'manifest' = parse "
                         "target/manifest.json (exact, excludes ephemeral, needs `dbt parse`)")
    ap.add_argument("--tenant", action="append", default=[], metavar="SLUG",
                    help="restrict to these tenant slugs (repeatable); default = all discovered")
    ap.add_argument("--exclude", action="append", default=[], metavar="SLUG",
                    help=f"skip these tenant slugs (repeatable). Added on top of the "
                         f"defaults: {', '.join(sorted(DEFAULT_EXCLUDED_SLUGS))}")
    ap.add_argument("--no-default-excludes", action="store_true",
                    help="don't apply the built-in exclude list "
                         f"({', '.join(sorted(DEFAULT_EXCLUDED_SLUGS))})")
    ap.add_argument("--manifest", action="append", default=[], metavar="SLUG=PATH",
                    help="explicit manifest.json path for a slug (repeatable)")
    ap.add_argument("--repo", action="append", default=[], metavar="SLUG=NAME_OR_PATH",
                    help="map a tenant slug to its repo folder name or path when they "
                         "differ (e.g. --repo mirage=captions). Repeatable.")
    ap.add_argument("--json", type=Path, default=None, help="also write the report as JSON to this path")
    args = ap.parse_args(argv)

    manifest_overrides: dict[str, str] = {}
    for item in args.manifest:
        if "=" not in item:
            ap.error(f"--manifest expects SLUG=PATH, got {item!r}")
        slug, path = item.split("=", 1)
        manifest_overrides[slug] = path

    explicit_repos: dict[str, str] = {}
    for item in args.repo:
        if "=" not in item:
            ap.error(f"--repo expects SLUG=NAME_OR_PATH, got {item!r}")
        slug, name = item.split("=", 1)
        explicit_repos[slug] = name

    # Auto-map slug -> project dir by reading each *_dbt repo's dbt_project.yml
    # name (the folder name rarely equals the slug). Explicit --repo wins.
    project_index = build_project_index(args.repos_dir)
    if project_index:
        print(f"Indexed {len(project_index)} dbt project(s) under {args.repos_dir}: "
              f"{', '.join(sorted(project_index))}")
    repo_overrides = {**project_index, **explicit_repos}

    print(f"Discovering tenant secrets (*{args.secret_suffix}) in {args.region} ...")
    try:
        secrets = list_tenant_secrets(args.region, args.secret_suffix)
    except Exception as exc:
        print(f"Failed to list secrets: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    if args.tenant:
        wanted = set(args.tenant)
        missing_secrets = wanted - secrets.keys()
        for s in sorted(missing_secrets):
            print(f"  warning: no secret found for tenant {s!r}", file=sys.stderr)
        secrets = {s: n for s, n in secrets.items() if s in wanted}

    excluded = set(args.exclude)
    if not args.no_default_excludes:
        excluded |= DEFAULT_EXCLUDED_SLUGS
    skipped = sorted(excluded & secrets.keys())
    if skipped:
        print(f"Excluding {len(skipped)} non-tenant slug(s): {', '.join(skipped)}")
    secrets = {s: n for s, n in secrets.items() if s not in excluded}

    if not secrets:
        print("No tenant secrets matched.", file=sys.stderr)
        return 1
    print(f"Found {len(secrets)} tenant(s): {', '.join(sorted(secrets))}\n")

    results: list[TenantResult] = []
    for slug in sorted(secrets):
        print(f"Auditing {slug} ...", flush=True)
        results.append(
            audit_tenant(slug, secrets[slug], args.region, args.repos_dir,
                         manifest_overrides, args.model_count, repo_overrides)
        )

    print_report(results)

    if args.json:
        args.json.write_text(json.dumps(to_json(results), indent=2))
        print(f"\nJSON report written to {args.json}")

    # Non-zero exit if any tenant errored or has missing models — handy in CI.
    bad = any(r.error or r.missing for r in results)
    return 2 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
