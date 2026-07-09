"""Tests for the shared BigQuery profile-arg builder."""
from __future__ import annotations

import importlib.util
import os
import unittest
from unittest import mock

_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sundial_airflow",
    "profiles.py",
)
_spec = importlib.util.spec_from_file_location("_profiles_standalone", _PATH)
profiles = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(profiles)


class BigQueryProfileArgsTest(unittest.TestCase):
    def test_base_args_without_dataproc(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertEqual(
            args, {"project": "p", "dataset": "d", "location": "US", "threads": 4}
        )

    def test_threads_from_env(self) -> None:
        with mock.patch.dict(os.environ, {profiles.THREADS_ENV: "8"}, clear=True):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertEqual(args["threads"], 8)

    def test_threads_explicit_param_overrides_env(self) -> None:
        with mock.patch.dict(os.environ, {profiles.THREADS_ENV: "8"}, clear=True):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US", threads=2
            )
        self.assertEqual(args["threads"], 2)

    def test_dataproc_emitted_when_region_and_bucket_set(self) -> None:
        env = {
            profiles.DATAPROC_REGION_ENV: "us-central1",
            profiles.GCS_BUCKET_ENV: "gs-staging",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertEqual(args["dataproc_region"], "us-central1")
        self.assertEqual(args["gcs_bucket"], "gs-staging")
        self.assertEqual(args["submission_method"], "serverless")

    def test_submission_method_override(self) -> None:
        env = {
            profiles.DATAPROC_REGION_ENV: "us-central1",
            profiles.GCS_BUCKET_ENV: "gs-staging",
            profiles.DATAPROC_SUBMISSION_METHOD_ENV: "cluster",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertEqual(args["submission_method"], "cluster")

    def test_dataproc_not_emitted_when_only_region_set(self) -> None:
        with mock.patch.dict(
            os.environ, {profiles.DATAPROC_REGION_ENV: "us-central1"}, clear=True
        ):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertNotIn("dataproc_region", args)
        self.assertNotIn("gcs_bucket", args)
        self.assertNotIn("submission_method", args)

    def test_dataproc_not_emitted_when_only_bucket_set(self) -> None:
        with mock.patch.dict(
            os.environ, {profiles.GCS_BUCKET_ENV: "gs-staging"}, clear=True
        ):
            args = profiles.bigquery_profile_args(
                project="p", dataset="d", location="US"
            )
        self.assertNotIn("dataproc_region", args)
        self.assertNotIn("gcs_bucket", args)
        self.assertNotIn("submission_method", args)


if __name__ == "__main__":
    unittest.main()
