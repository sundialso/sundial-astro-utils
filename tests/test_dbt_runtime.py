"""Tests for sundial_airflow.dbt_runtime.ensure_dbt_deps."""
from __future__ import annotations

import subprocess
from unittest import mock

import pytest

from sundial_airflow.dbt_runtime import ensure_dbt_deps


def _completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=["dbt", "deps"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_ensure_dbt_deps_runs_dbt_deps_with_project_dir():
    with mock.patch(
        "sundial_airflow.dbt_runtime.subprocess.run",
        return_value=_completed(0),
    ) as run:
        ensure_dbt_deps("/usr/bin/dbt", "/proj", env={"A": "1"})

    run.assert_called_once()
    cmd = run.call_args.args[0]
    assert cmd == ["/usr/bin/dbt", "deps", "--project-dir", "/proj"]
    assert run.call_args.kwargs["env"] == {"A": "1"}


def test_ensure_dbt_deps_raises_on_nonzero_exit():
    with mock.patch(
        "sundial_airflow.dbt_runtime.subprocess.run",
        return_value=_completed(2, stdout="boom", stderr="err"),
    ):
        with pytest.raises(RuntimeError, match="dbt deps failed"):
            ensure_dbt_deps("/usr/bin/dbt", "/proj")


def test_ensure_dbt_deps_defaults_env_to_os_environ():
    with mock.patch(
        "sundial_airflow.dbt_runtime.subprocess.run",
        return_value=_completed(0),
    ) as run, mock.patch.dict(
        "sundial_airflow.dbt_runtime.os.environ", {"X": "y"}, clear=True
    ):
        ensure_dbt_deps("/usr/bin/dbt", "/proj")

    assert run.call_args.kwargs["env"] == {"X": "y"}
