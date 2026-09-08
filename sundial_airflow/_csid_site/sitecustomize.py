"""Autoload the CSID patch in an interpreter this distribution never installs into.

Cosmos invokes dbt from a separate virtualenv (``ExecutionConfig.dbt_executable_path``),
so the ``.pth`` installed into the Airflow environment never executes there —
the sessions stay tagged ``dbt``. What we *can* reach is the dbt subprocess's
environment: ``site`` imports ``sitecustomize`` from any ``sys.path`` entry, and
Cosmos preserves ``PYTHONPATH`` into that subprocess (``remove_dags_folder_from_pythonpath``
drops only the DAGs folder). The DAG factory puts this directory on it — see
``sundial_airflow.csid``.

``sitecustomize`` and its payload are deliberately the only importable names
here, so nothing else leaks onto dbt's ``sys.path``.
"""
from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_shadowed_sitecustomize() -> None:
    """Run any ``sitecustomize`` in the target venv that this file shadows.

    ``PYTHONPATH`` precedes site-packages, so without this a ``sitecustomize``
    shipped by another package into the dbt venv would silently stop running.
    """
    rest = [p for p in sys.path if p and os.path.abspath(p) != _HERE]
    spec = importlib.machinery.PathFinder.find_spec("sitecustomize", rest)
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    sys.modules["_sundial_shadowed_sitecustomize"] = module
    spec.loader.exec_module(module)


# Importing the payload applies the patch (or arms its import hook). Staged
# alongside this file at build time, so it resolves here rather than needing
# the Airflow environment's copy.
try:
    import _sundial_csid  # noqa: F401
except Exception:  # noqa: BLE001
    pass

# A raising sitecustomize breaks every interpreter that loads it, so neither
# step may escape.
try:
    _load_shadowed_sitecustomize()
except Exception:  # noqa: BLE001
    pass
