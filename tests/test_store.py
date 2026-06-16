"""ArenaStore + ResidualStore behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from nirs4all_benchmarks import ARENA_SCHEMA_VERSION
from nirs4all_benchmarks.store import ArenaStore, ArenaStoreVersionError


def test_schema_is_stamped(empty_store: ArenaStore):
    assert empty_store.get_meta("arena_schema_version") == str(ARENA_SCHEMA_VERSION)
    uv = empty_store.conn.execute("PRAGMA user_version").fetchone()[0]
    assert uv == ARENA_SCHEMA_VERSION


def test_upsert_dedups_by_pk(empty_store: ArenaStore):
    row = {"operator_spec_hash": "a" * 64, "entrypoint": "x.Y", "role": "model", "created_at": "now"}
    assert empty_store.upsert("operator_specs", row) is True
    assert empty_store.upsert("operator_specs", row) is False  # dedup hit
    assert empty_store.count("operator_specs") == 1


def test_forward_incompatible_version_guard(tmp_path: Path):
    store = ArenaStore(tmp_path / "s")
    store.conn.execute(f"PRAGMA user_version = {ARENA_SCHEMA_VERSION + 1}")
    store.conn.commit()
    store.close()
    with pytest.raises(ArenaStoreVersionError):
        ArenaStore(tmp_path / "s")


def test_residual_roundtrip(empty_store: ArenaStore):
    rows = [
        {"sample_id": "s1", "scope": "cv", "fold_id": "fold0", "partition": "validation",
         "y_true": 1.0, "y_pred": 1.1, "residual": -0.1, "y_proba": None},
        {"sample_id": "s2", "scope": "cv", "fold_id": "fold0", "partition": "validation",
         "y_true": 2.0, "y_pred": 1.8, "residual": 0.2, "y_proba": None},
    ]
    path = empty_store.residuals.write("rs1", rows)
    assert path.exists()
    back = empty_store.residuals.read("rs1")
    assert len(back) == 2
    assert back[0]["sample_id"] == "s1"
    assert back[1]["residual"] == pytest.approx(0.2)


def test_residual_roundtrip_with_proba(empty_store: ArenaStore):
    rows = [{"sample_id": "s1", "scope": "cv", "partition": "validation", "y_proba": [0.1, 0.9]}]
    empty_store.residuals.write("rsp", rows)
    back = empty_store.residuals.read("rsp")
    assert back[0]["y_proba"] == [pytest.approx(0.1), pytest.approx(0.9)]
