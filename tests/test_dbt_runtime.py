"""Tests for sundial_airflow.dbt_runtime.ensure_dbt_deps."""
from __future__ import annotations

import importlib.util
import os
import subprocess
import unittest
from unittest import mock

# Load dbt_runtime.py directly by path — importing the sundial_airflow package
# runs __init__.py, which pulls in dag_factory (and airflow). A unique standalone
# module name also sidesteps the path-less ``sundial_airflow`` stub that
# test_chunk_*.py inject into sys.modules (which would otherwise shadow the real
# package for a ``from sundial_airflow.dbt_runtime import`` here). dbt_runtime.py
# imports only stdlib, so loading the file is airflow-free.
_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sundial_airflow",
    "dbt_runtime.py",
)
_spec = importlib.util.spec_from_file_location("_dbt_runtime_standalone", _PATH)
_dbt_runtime = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dbt_runtime)
ensure_dbt_deps = _dbt_runtime.ensure_dbt_deps


def _completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=["dbt", "deps"], returncode=returncode, stdout=stdout, stderr=stderr
    )


class EnsureDbtDepsTests(unittest.TestCase):
    def test_runs_dbt_deps_with_project_dir(self):
        with mock.patch.object(
            _dbt_runtime.subprocess, "run", return_value=_completed(0)
        ) as run:
            ensure_dbt_deps("/usr/bin/dbt", "/proj", env={"A": "1"})

        run.assert_called_once()
        cmd = run.call_args.args[0]
        self.assertEqual(cmd, ["/usr/bin/dbt", "deps", "--project-dir", "/proj"])
        self.assertEqual(run.call_args.kwargs["env"], {"A": "1"})

    def test_raises_on_nonzero_exit(self):
        with mock.patch.object(
            _dbt_runtime.subprocess,
            "run",
            return_value=_completed(2, stdout="boom", stderr="err"),
        ):
            with self.assertRaisesRegex(RuntimeError, "dbt deps failed"):
                ensure_dbt_deps("/usr/bin/dbt", "/proj")

    def test_defaults_env_to_os_environ(self):
        with mock.patch.object(
            _dbt_runtime.subprocess, "run", return_value=_completed(0)
        ) as run, mock.patch.dict(
            _dbt_runtime.os.environ, {"X": "y"}, clear=True
        ):
            ensure_dbt_deps("/usr/bin/dbt", "/proj")

        self.assertEqual(run.call_args.kwargs["env"], {"X": "y"})


if __name__ == "__main__":
    unittest.main()
