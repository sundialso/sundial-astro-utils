"""Unit tests for ``sundial_airflow.backfill.manifest_parser``."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from sundial_airflow.backfill.manifest_parser import (
    CHUNKED,
    FULL_REFRESH,
    BackfillModel,
    ChunkingConfigEntry,
    compute_static_chunks,
    load_backfill_models,
    load_chunking_config,
    topological_order,
)


# ── Helpers ──────────────────────────────────────────────────────────────

def _node(
    *,
    name: str,
    raw_sql: str = "select 1",
    materialized: str = "incremental",
    depends_on: list[str] | None = None,
    meta: dict | None = None,
    resource_type: str = "model",
) -> dict:
    """Build a minimal dbt manifest node."""
    return {
        "name": name,
        "resource_type": resource_type,
        "raw_code": raw_sql,
        "config": {"materialized": materialized, "meta": meta or {}},
        "depends_on": {"nodes": depends_on or []},
    }


def _write_manifest(tmp_path: Path, nodes: dict) -> Path:
    manifest = {"nodes": nodes}
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return p


def _write_config(tmp_path: Path, entries: list) -> Path:
    p = tmp_path / "chunking_config.json"
    p.write_text(json.dumps(entries))
    return p


_START_TS_SQL = "select * from t where ts >= {{ start_ts('ts', 0, '2024-01-15') }}"


# ── load_chunking_config ─────────────────────────────────────────────────

class TestLoadChunkingConfig:
    def test_none_path_returns_empty(self):
        assert load_chunking_config(None) == {}

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_chunking_config(tmp_path / "nope.json") == {}

    def test_invalid_json_returns_empty(self, tmp_path, caplog):
        p = tmp_path / "bad.json"
        p.write_text("{not json")
        with caplog.at_level("WARNING"):
            assert load_chunking_config(p) == {}
        assert any("invalid JSON" in r.message for r in caplog.records)

    def test_non_list_root_returns_empty(self, tmp_path, caplog):
        p = tmp_path / "obj.json"
        p.write_text('{"a": 1}')
        with caplog.at_level("WARNING"):
            assert load_chunking_config(p) == {}
        assert any("must be a JSON list" in r.message for r in caplog.records)

    def test_valid_entries(self, tmp_path):
        p = _write_config(tmp_path, [
            {"model_name": "m1", "chunking_enabled": True, "chunk_size": 3},
            {"model_name": "m2", "chunking_enabled": False},
        ])
        out = load_chunking_config(p)
        assert out == {
            "m1": ChunkingConfigEntry("m1", True, 3),
            "m2": ChunkingConfigEntry("m2", False, None),
        }

    def test_non_object_entry_skipped(self, tmp_path, caplog):
        p = _write_config(tmp_path, ["just a string", {"model_name": "m", "chunking_enabled": True}])
        with caplog.at_level("WARNING"):
            out = load_chunking_config(p)
        assert "m" in out
        assert any("not an object" in r.message for r in caplog.records)

    def test_missing_model_name_skipped(self, tmp_path, caplog):
        p = _write_config(tmp_path, [{"chunking_enabled": True, "chunk_size": 3}])
        with caplog.at_level("WARNING"):
            assert load_chunking_config(p) == {}
        assert any("model_name" in r.message for r in caplog.records)

    def test_non_bool_enabled_skipped(self, tmp_path, caplog):
        p = _write_config(tmp_path, [
            {"model_name": "m", "chunking_enabled": "yes", "chunk_size": 3},
        ])
        with caplog.at_level("WARNING"):
            assert load_chunking_config(p) == {}
        assert any("non-bool" in r.message for r in caplog.records)

    @pytest.mark.parametrize("bad_size", [0, -1, 3.14, True, False, "3"])
    def test_invalid_chunk_size_downgrades_to_none(self, tmp_path, bad_size, caplog):
        p = _write_config(tmp_path, [
            {"model_name": "m", "chunking_enabled": True, "chunk_size": bad_size},
        ])
        with caplog.at_level("WARNING"):
            out = load_chunking_config(p)
        assert out["m"].chunk_size is None
        assert any("chunk_size" in r.message for r in caplog.records)

    def test_missing_chunk_size_is_none(self, tmp_path):
        p = _write_config(tmp_path, [{"model_name": "m", "chunking_enabled": True}])
        out = load_chunking_config(p)
        assert out["m"].chunk_size is None

    def test_duplicate_last_wins(self, tmp_path, caplog):
        p = _write_config(tmp_path, [
            {"model_name": "m", "chunking_enabled": True, "chunk_size": 1},
            {"model_name": "m", "chunking_enabled": True, "chunk_size": 9},
        ])
        with caplog.at_level("WARNING"):
            out = load_chunking_config(p)
        assert out["m"].chunk_size == 9
        assert any("duplicate" in r.message for r in caplog.records)


# ── load_backfill_models ─────────────────────────────────────────────────

class TestLoadBackfillModels:
    def test_ephemeral_excluded(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "model.proj.eph": _node(name="eph", materialized="ephemeral"),
            "model.proj.keep": _node(name="keep"),
        })
        out = load_backfill_models(manifest)
        assert set(m.name for m in out.values()) == {"keep"}

    def test_non_model_resource_excluded(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "test.proj.t": _node(name="t", resource_type="test"),
            "snapshot.proj.s": _node(name="s", resource_type="snapshot"),
            "model.proj.m": _node(name="m"),
        })
        out = load_backfill_models(manifest)
        assert set(m.name for m in out.values()) == {"m"}

    def test_backfill_disabled_excluded(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "model.proj.skip": _node(name="skip", meta={"backfill_disabled": True}),
            "model.proj.keep": _node(name="keep"),
        })
        out = load_backfill_models(manifest)
        assert set(m.name for m in out.values()) == {"keep"}

    def test_no_config_means_all_full_refresh(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "model.proj.has_anchor": _node(name="has_anchor", raw_sql=_START_TS_SQL),
            "model.proj.no_anchor": _node(name="no_anchor"),
        })
        out = load_backfill_models(manifest)
        assert all(m.kind == FULL_REFRESH for m in out.values())
        # first_timestamp still populated where present:
        anchored = next(m for m in out.values() if m.name == "has_anchor")
        assert anchored.first_timestamp == date(2024, 1, 15)

    def test_promotion_requires_both_anchor_and_config(self, tmp_path, caplog):
        manifest = _write_manifest(tmp_path, {
            "model.proj.good": _node(name="good", raw_sql=_START_TS_SQL),
            "model.proj.no_anchor": _node(name="no_anchor"),
        })
        config = {
            "good": ChunkingConfigEntry("good", True, 2),
            "no_anchor": ChunkingConfigEntry("no_anchor", True, 2),
        }
        with caplog.at_level("WARNING"):
            out = load_backfill_models(manifest, config)
        good = next(m for m in out.values() if m.name == "good")
        nope = next(m for m in out.values() if m.name == "no_anchor")
        assert good.kind == CHUNKED
        assert good.chunk_months == 2
        assert nope.kind == FULL_REFRESH
        assert any("no start_ts() anchor" in r.message for r in caplog.records)

    def test_disabled_entry_keeps_full_refresh(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "model.proj.m": _node(name="m", raw_sql=_START_TS_SQL),
        })
        config = {"m": ChunkingConfigEntry("m", False, 6)}
        out = load_backfill_models(manifest, config)
        m = next(iter(out.values()))
        assert m.kind == FULL_REFRESH
        assert m.chunk_months is None

    def test_unknown_model_in_config_warned(self, tmp_path, caplog):
        manifest = _write_manifest(tmp_path, {"model.proj.real": _node(name="real")})
        config = {"ghost": ChunkingConfigEntry("ghost", True, 3)}
        with caplog.at_level("WARNING"):
            out = load_backfill_models(manifest, config)
        assert all(m.kind == FULL_REFRESH for m in out.values())
        assert any("unknown model" in r.message for r in caplog.records)

    def test_no_chunk_size_keeps_full_refresh(self, tmp_path, caplog):
        manifest = _write_manifest(tmp_path, {
            "model.proj.m": _node(name="m", raw_sql=_START_TS_SQL),
        })
        config = {"m": ChunkingConfigEntry("m", True, None)}
        with caplog.at_level("WARNING"):
            out = load_backfill_models(manifest, config)
        m = next(iter(out.values()))
        assert m.kind == FULL_REFRESH

    def test_depends_on_filters_to_models(self, tmp_path):
        manifest = _write_manifest(tmp_path, {
            "model.proj.m": _node(name="m", depends_on=[
                "model.proj.upstream", "source.proj.raw.t", "seed.proj.s",
            ]),
            "model.proj.upstream": _node(name="upstream"),
        })
        out = load_backfill_models(manifest)
        m = out["model.proj.m"]
        assert m.depends_on == ["model.proj.upstream"]


# ── _extract_first_timestamp (via load_backfill_models) ─────────────────

class TestFirstTimestamp:
    def _load_one(self, tmp_path, raw_sql):
        manifest = _write_manifest(tmp_path, {
            "model.proj.m": _node(name="m", raw_sql=raw_sql),
        })
        out = load_backfill_models(manifest)
        return next(iter(out.values()))

    def test_basic(self, tmp_path):
        m = self._load_one(tmp_path, "{{ start_ts('ts', 0, '2024-06-01') }}")
        assert m.first_timestamp == date(2024, 6, 1)

    def test_multi_line(self, tmp_path):
        sql = """select * from t
        where ts >= {{
            start_ts(
                'event_ts',
                7,
                '2023-03-15T12:00:00+00:00'
            )
        }}"""
        m = self._load_one(tmp_path, sql)
        assert m.first_timestamp == date(2023, 3, 15)

    def test_line_comment_ignored(self, tmp_path):
        sql = """
        -- {{ start_ts('ts', 0, '2099-01-01') }}
        {{ start_ts('ts', 0, '2024-01-01') }}
        """
        m = self._load_one(tmp_path, sql)
        assert m.first_timestamp == date(2024, 1, 1)

    def test_block_comment_ignored(self, tmp_path):
        sql = """
        /* {{ start_ts('ts', 0, '2099-01-01') }} */
        {{ start_ts('ts', 0, '2024-01-01') }}
        """
        m = self._load_one(tmp_path, sql)
        assert m.first_timestamp == date(2024, 1, 1)

    def test_multiple_calls_min_wins(self, tmp_path):
        sql = """
        {{ start_ts('a', 0, '2024-06-01') }}
        {{ start_ts('b', 0, '2023-01-01') }}
        {{ start_ts('c', 0, '2025-12-01') }}
        """
        m = self._load_one(tmp_path, sql)
        assert m.first_timestamp == date(2023, 1, 1)

    def test_unparseable_timestamp_ignored(self, tmp_path, caplog):
        sql = "{{ start_ts('ts', 0, 'not-a-date') }} {{ start_ts('ts', 0, '2024-01-01') }}"
        with caplog.at_level("WARNING"):
            m = self._load_one(tmp_path, sql)
        assert m.first_timestamp == date(2024, 1, 1)
        assert any("Unparseable" in r.message for r in caplog.records)

    def test_no_call_returns_none(self, tmp_path):
        m = self._load_one(tmp_path, "select 1")
        assert m.first_timestamp is None


# ── topological_order ───────────────────────────────────────────────────

def _bm(name: str, deps: list[str] | None = None, kind: str = FULL_REFRESH) -> BackfillModel:
    return BackfillModel(
        node_key=f"model.proj.{name}",
        name=name,
        kind=kind,
        first_timestamp=None,
        depends_on=deps or [],
    )


class TestTopologicalOrder:
    def test_linear(self):
        models = {
            "model.proj.a": _bm("a"),
            "model.proj.b": _bm("b", deps=["model.proj.a"]),
            "model.proj.c": _bm("c", deps=["model.proj.b"]),
        }
        order = [m.name for m in topological_order(models)]
        assert order == ["a", "b", "c"]

    def test_diamond(self):
        models = {
            "model.proj.a": _bm("a"),
            "model.proj.b": _bm("b", deps=["model.proj.a"]),
            "model.proj.c": _bm("c", deps=["model.proj.a"]),
            "model.proj.d": _bm("d", deps=["model.proj.b", "model.proj.c"]),
        }
        order = [m.name for m in topological_order(models)]
        assert order.index("a") < order.index("b") < order.index("d")
        assert order.index("a") < order.index("c") < order.index("d")

    def test_deterministic_within_layer(self):
        # Two roots with the same depth — name-sorted, so z comes after a.
        models = {f"model.proj.{n}": _bm(n) for n in ("z", "m", "a")}
        order = [m.name for m in topological_order(models)]
        assert order == ["a", "m", "z"]

    def test_cycle_raises(self):
        models = {
            "model.proj.a": _bm("a", deps=["model.proj.b"]),
            "model.proj.b": _bm("b", deps=["model.proj.a"]),
        }
        with pytest.raises(ValueError, match="Cycle"):
            topological_order(models)

    def test_external_upstreams_ignored(self):
        # An upstream not in the dict (e.g. a source) shouldn't block.
        models = {
            "model.proj.a": _bm("a", deps=["source.proj.raw.t"]),
        }
        order = topological_order(models)
        assert [m.name for m in order] == ["a"]


# ── compute_static_chunks ────────────────────────────────────────────────

class TestComputeStaticChunks:
    def test_exact_multiple(self):
        m = BackfillModel(
            node_key="x", name="x", kind=CHUNKED,
            first_timestamp=date(2024, 1, 1),
            depends_on=[], chunk_months=3,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 7, 1))
        assert out == {"x": [
            (date(2024, 1, 1), date(2024, 4, 1), "2024-01"),
            (date(2024, 4, 1), date(2024, 7, 1), "2024-04"),
        ]}

    def test_last_window_truncated(self):
        m = BackfillModel(
            node_key="x", name="x", kind=CHUNKED,
            first_timestamp=date(2024, 1, 1),
            depends_on=[], chunk_months=3,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 5, 15))
        assert out["x"][-1] == (date(2024, 4, 1), date(2024, 5, 15), "2024-04")

    def test_full_refresh_excluded(self):
        m = BackfillModel(
            node_key="x", name="x", kind=FULL_REFRESH,
            first_timestamp=date(2024, 1, 1),
            depends_on=[], chunk_months=3,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 7, 1))
        assert out == {}

    def test_chunked_without_chunk_months_excluded(self):
        m = BackfillModel(
            node_key="x", name="x", kind=CHUNKED,
            first_timestamp=date(2024, 1, 1),
            depends_on=[], chunk_months=None,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 7, 1))
        assert out == {}

    def test_chunked_without_first_timestamp_excluded(self):
        m = BackfillModel(
            node_key="x", name="x", kind=CHUNKED,
            first_timestamp=None,
            depends_on=[], chunk_months=3,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 7, 1))
        assert out == {}

    def test_first_timestamp_after_today_yields_empty(self):
        m = BackfillModel(
            node_key="x", name="x", kind=CHUNKED,
            first_timestamp=date(2030, 1, 1),
            depends_on=[], chunk_months=3,
        )
        out = compute_static_chunks({"x": m}, today=date(2024, 1, 1))
        assert out == {"x": []}
