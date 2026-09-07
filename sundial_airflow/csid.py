"""Reaching the dbt subprocess's interpreter with the Snowflake CSID patch.

The ``.pth`` this distribution installs runs in the *Airflow* environment. That
covers the ``SnowflakeHook`` connections made there (partition watermarks,
``report_data_processed``), but not dbt: Cosmos invokes dbt from a separate
virtualenv, whose interpreter never processes our ``.pth``. Confirmed in
production — ``ACCOUNT_USAGE.SESSIONS`` showed every dbt session still tagged
``dbt``, with ``APPLICATION_PATH`` pointing at ``dbt_venv/bin/dbt``.

``site`` does import ``sitecustomize`` from any ``sys.path`` entry, and Cosmos
preserves ``PYTHONPATH`` into the dbt subprocess — its
``remove_dags_folder_from_pythonpath`` drops only the DAGs folder and documents
that every other entry is kept. So putting ``_csid_site`` on ``PYTHONPATH``
autoloads the patch inside the dbt venv, with no change to any tenant image.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

#: Ships with this package; holds the autoloaded ``sitecustomize.py`` and the
#: payload staged beside it. Nothing else importable lives there.
_CSID_SITE = Path(__file__).parent / "_csid_site"


def csid_site_dir() -> str:
    """Absolute path of the directory to place on a dbt subprocess's PYTHONPATH."""
    return str(_CSID_SITE)


def with_csid_pythonpath(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return ``env`` with our site directory prepended to ``PYTHONPATH``.

    Called with no argument it yields just that one variable, which is what the
    Cosmos operators want: they default to ``append_env=False``, so the dbt
    subprocess does not inherit ``os.environ`` and this is layered on top of the
    env Cosmos builds itself. Existing entries are preserved, and re-applying is
    a no-op.
    """
    result = dict(env or {})
    entry = csid_site_dir()
    parts = [p for p in (result.get("PYTHONPATH") or "").split(os.pathsep) if p]
    if entry not in parts:
        parts.insert(0, entry)
    result["PYTHONPATH"] = os.pathsep.join(parts)
    return result
