"""Ingestion state machine: commit, idempotency, quarantine, recompute."""

from __future__ import annotations

from pathlib import Path

import pytest

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore


def _first_export():
    return generate_fixture_exports()[0]


def test_commit_and_idempotency(tmp_path: Path):
    exp = _first_export()
    with ArenaStore(tmp_path / "a") as store:
        r1 = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert r1.status == "committed"
        assert r1.run_condition_hash and r1.execution_hash
        r2 = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert r2.status == "already_ingested"
        assert store.count("executions") == 1


def test_scores_recomputed_from_residuals(tmp_path: Path):
    exp = _first_export()
    exp.scores.observations = []
    exp.arena_export_hash = None
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c", recompute_scores=True))
        assert r.clean_report["scores_recomputed"] is True
        assert store.count("metric_observations") > 0


def test_quarantine_on_unattested_leakage_for_release(tmp_path: Path):
    exp = _first_export()
    exp.leakage_attestation.oof_enforced = False
    exp.arena_export_hash = None  # Intentional metadata edit invalidates the old claim.
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="rel", collection_kind="benchmark_release"))
        assert r.status == "quarantined"
        assert r.validity_status == "quarantined"
        row = store.query_one("SELECT validity_status FROM executions LIMIT 1")
        assert row["validity_status"] == "quarantined"


def test_unattested_leakage_flagged_for_user_runs(tmp_path: Path):
    exp = _first_export()
    exp.leakage_attestation.oof_enforced = False
    exp.arena_export_hash = None  # Intentional metadata edit invalidates the old claim.
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="u", collection_kind="user_run_collection"))
        assert r.status == "committed"
        assert any(i["code"] == "leakage_unattested" for i in r.issues)


def test_dedup_across_datasets(tmp_path: Path):
    # All PLS-sweep pipelines are shared across the two datasets → deduped pipelines.
    with ArenaStore(tmp_path / "a") as store:
        for exp in generate_fixture_exports():
            ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert store.count("pipeline_dags") == 7
        assert store.count("dataset_fingerprints") == 2
        assert store.count("run_conditions") == 14


def test_pseudonymization_is_consistent_across_runs(tmp_path: Path):
    exps = generate_fixture_exports()
    with ArenaStore(tmp_path / "a") as store:
        for exp in exps[:2]:  # two pipelines on the same first dataset
            ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        rsets = store.query("SELECT residual_set_id FROM residual_sets")
        ids_a = {r["sample_id"] for r in store.residuals.read(rsets[0]["residual_set_id"])}
        ids_b = {r["sample_id"] for r in store.residuals.read(rsets[1]["residual_set_id"])}
        # Same raw sample ids -> same pseudo ids -> large overlap (cross-run join works).
        assert len(ids_a & ids_b) > 50
        assert all(s.startswith("s_") for s in ids_a)


@pytest.mark.parametrize("path", ["transaction", "direct", "atomic", "upload-export", "upload-pipeline", "cli"])
def test_f2_held_writer_has_typed_busy_and_no_partial_records(tmp_path, monkeypatch, path):
    import json
    import sqlite3
    from functools import partial

    from typer.testing import CliRunner

    import nirs4all_benchmarks.store as store_module
    from nirs4all_benchmarks.cli import app
    from nirs4all_benchmarks.ingestion import ingest_exports_atomic, upload
    from nirs4all_benchmarks.store import ArenaStoreRefusal
    from test_arena_v2_work_review import residual_record

    if path == "cli":
        monkeypatch.setattr(store_module, "ArenaStore", partial(ArenaStore, busy_timeout_ms=30))
    root = tmp_path / "store"
    source = residual_record()
    recipe = {"nodes": source["pipeline"]["nodes"]}
    manifest = tmp_path / "export.json"
    manifest.write_text(json.dumps(source))

    def act(store):
        if path == "transaction":
            with store.transaction():
                store.set_meta("committed_probe", "retained")
        elif path == "direct":
            assert ingest_export(store, source).ok
        elif path == "atomic":
            assert all(r.ok for r in ingest_exports_atomic(store, [source]))
        elif path.startswith("upload"):
            result = upload(store, recipe if path == "upload-pipeline" else source)
            assert result.status in {"registered", "ingested"}
        else:
            return CliRunner().invoke(app, ["ingest-export", str(manifest), "--store", str(root)])

    with ArenaStore(root, busy_timeout_ms=30) as store:
        writer = sqlite3.connect(root / "arena.sqlite")
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("UPDATE arena_meta SET value=value")
            if path == "cli":
                result = act(store)
                assert result.exit_code == 2
                assert "arena_store_busy" in result.output
                assert "database is locked" not in result.output
                assert not isinstance(result.exception, sqlite3.Error)
            else:
                with pytest.raises(ArenaStoreRefusal, match=r"^arena_store_busy$") as refusal:
                    act(store)
                assert refusal.value.code == "arena_store_busy"
            assert not store.conn.in_transaction
            for table in ("executions", "ingestion_batches", "metadata_receipts", "pipeline_dags", "collections"):
                assert store.count(table) == 0
            assert list(store.arrays_dir.iterdir()) == list(store.exports_dir.iterdir()) == []
        finally:
            writer.rollback()
            writer.close()
        result = act(store)
        if path == "cli":
            assert result.exit_code == 0, result.output
        elif path == "transaction":
            assert store.get_meta("committed_probe") == "retained"
        else:
            assert store.count("pipeline_dags") == 1
        assert not store.conn.in_transaction


@pytest.mark.parametrize("atomic", [False, True])
def test_f2_commit_contention_rolls_back_new_blob_and_keeps_committed_blob(tmp_path, monkeypatch, atomic):
    import sqlite3

    from nirs4all_benchmarks.ingestion import ingest_exports_atomic
    from nirs4all_benchmarks.store import ArenaStoreRefusal
    from test_arena_v2_work_review import residual_record

    with ArenaStore(tmp_path / "store", busy_timeout_ms=30) as store:
        assert ingest_export(store, residual_record()).ok
        committed = {p.name: p.read_bytes() for p in store.arrays_dir.iterdir()}
        assert committed
        assert store.conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        source = residual_record("new-execution")
        source["residuals"]["inline"][0]["y_pred"] = 2.5
        reader = sqlite3.connect(store.db_path)
        statements = []
        store.conn.set_trace_callback(statements.append)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM arena_meta").fetchall()
            with pytest.raises(ArenaStoreRefusal, match=r"^arena_store_busy$"):
                if atomic:
                    ingest_exports_atomic(store, [source])
                else:
                    ingest_export(store, source)
            assert any(s.startswith("INSERT") for s in statements)
            assert "COMMIT" in statements and "ROLLBACK" in statements
            assert not store.conn.in_transaction
            assert store.count("executions") == store.count("metadata_receipts") == 1
            assert {p.name: p.read_bytes() for p in store.arrays_dir.iterdir()} == committed
            assert list(store.exports_dir.iterdir()) == []
        finally:
            reader.rollback()
            reader.close()
            store.conn.set_trace_callback(None)
        assert ingest_export(store, source).ok
        assert store.count("executions") == store.count("metadata_receipts") == 2
