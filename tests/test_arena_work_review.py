"""Synthetic production-path regressions for Opus WORK blockers B1–B5."""

from __future__ import annotations

import json
import sqlite3
import threading
import time

import pytest

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity.hashing import export_hash
from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.site_build import _build_bundle, _public_run_document, build_site
from nirs4all_benchmarks.store import ArenaStore, Queries


def run(tag="PUBLIC", visibility="public", terminal="completed"):
    return {
        "arena_export_schema_version": 2,
        "identity_basis": {
            key: {"kind": "owner_reported", "owner": "synthetic-work-regression"}
            for key in ("dataset_fingerprint", "task_hash", "dataset_variant_hash")
        },
        "dataset": {
            "dataset_fingerprint": {"PUBLIC": "a", "PRIVATE": "b", "UNKNOWN": "c", "FAILED": "d"}[tag] * 64,
            "visibility": visibility,
            "visibility_source": "producer_reported",
            "dataset_card": {"name": tag + "-DATASET"},
        },
        "task": {
            "task_hash": {"PUBLIC": "a", "PRIVATE": "b", "UNKNOWN": "c", "FAILED": "d"}[tag] * 64,
            "task_type": "regression",
        },
        "dataset_variant": {
            "dataset_variant_hash": {"PUBLIC": "a", "PRIVATE": "b", "UNKNOWN": "c", "FAILED": "d"}[tag] * 64
        },
        "leakage_attestation": {"oof_enforced": True, "group_leakage_checked": True, "nested_cv_safe": True, "unsafe_flags": []},
        "pipeline": {
            "human_label": tag + "-LABEL",
            "nodes": [{"node_id": "n", "role": "model", "operator": tag + "-OP", "params": {tag + "-PARAM": 3}}],
        },
        "execution": {"status": "ok" if terminal == "completed" else "failed", "terminal_status": terminal},
        "scores": {
            "observations": [
                {
                    "metric_name": "rmse",
                    "metric_value": 1.0,
                    "direction": "min",
                    "direction_source": "registry",
                    "scope": "cv",
                    "partition": "validation",
                    "aggregation_level": "sample",
                    "fold_id": "0",
                    "source_fold_id": None,
                }
            ]
        },
    }


def test_site_all_tables_and_documents_are_only_explicitly_public_completed(tmp_path):
    root, out = tmp_path / "store", tmp_path / "site"
    with ArenaStore(root) as store:
        public = ingest_export(store, run())
        private = run("PRIVATE", "private")
        private["residuals"] = residuals()
        unknown = run("UNKNOWN")
        del unknown["dataset"]["visibility"]
        del unknown["dataset"]["visibility_source"]
        for source in (private, unknown, run("FAILED", "private", "failed")):
            assert ingest_export(store, source).status == "committed"
        q = Queries(store)
        bundle = _build_bundle(store, q)
        blob = json.dumps([bundle, _public_run_document(store, q, public.execution_hash, bundle)])
        assert "PUBLIC-DATASET" in blob and "PUBLIC-LABEL" in blob
        for marker in ("PRIVATE", "UNKNOWN", "FAILED", "77.25", "70.5", "parquet_path", str(root)):
            assert marker not in blob
        assert bundle["overview"]["datasets"] == bundle["overview"]["pipelines"] == 1
        assert bundle["overview"]["total_metric_observations"] == 1
        assert bundle["planned"] == []
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, out)
    assert not out.exists()


def residuals():
    return {
        "key": "sample_id",
        "publishable": {"y_true":True,"y_pred":True,"residual":False,"y_proba":True},
        "inline": [
            {
                "sample_id": "s",
                "y_true": 10.0,
                "y_pred": 7.0,
                "residual": 3.0,
                "fold_id": "0",
                "scope": "cv",
                "partition": "validation",
            }
        ],
    }


@pytest.mark.parametrize("kind", ["private", "legacy_public", "unreported", "receipt_disabled"])
def test_no_proven_public_runs_refuses_before_output(tmp_path, monkeypatch, kind):
    root, out = tmp_path / "store", tmp_path / "site"
    with ArenaStore(root) as store:
        source = run()
        if kind == "private":
            source["dataset"]["visibility"] = "private"
        elif kind == "legacy_public":
            del source["dataset"]["visibility_source"]
            source = ArenaRunExport.model_validate(source).to_manifest()
        elif kind == "unreported":
            source["dataset"].update(visibility="unreported", visibility_source="unreported")
        assert ingest_export(store, source).ok
        if kind == "receipt_disabled":
            store.conn.execute("DELETE FROM metadata_receipts")
            store.conn.commit()
        if kind == "legacy_public":
            store.conn.execute("UPDATE dataset_fingerprints SET privacy_level = 'public'")
            store.conn.commit()
        with pytest.raises(ValueError, match="zero rankable"):
            _build_bundle(store, Queries(store))
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, out)
    assert not out.exists()


def test_existing_output_with_stale_private_files_is_refused(tmp_path, monkeypatch):
    root, out = tmp_path / "store", tmp_path / "site"
    with ArenaStore(root) as store:
        ingest_export(store, run())
    out.mkdir()
    (out / "stale.json").write_text("PRIVATE-STALE")
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, out)
    assert (out / "stale.json").read_text() == "PRIVATE-STALE"


def test_marker_alone_cannot_inject_public_visibility(empty_store):
    source = run()
    del source["dataset"]["visibility"]
    assert ingest_export(empty_store, source).status == "rejected"


def test_shared_catalog_keys_do_not_publish_the_private_label_or_provenance(tmp_path, monkeypatch):
    root, out = tmp_path / "store", tmp_path / "site"
    private, public = run("PRIVATE", "private"), run()
    private["pipeline"]["nodes"] = public["pipeline"]["nodes"]
    private["pipeline"]["engine_graph_fingerprint"] = "b" * 64
    public["pipeline"]["engine_graph_fingerprint"] = "a" * 64
    with ArenaStore(root) as store:
        assert ingest_export(store, private).status == "committed"
        assert ingest_export(store, public).status == "committed"
        store.conn.execute("UPDATE collections SET name='PRIVATE-COLLECTION', description='PRIVATE-DESCRIPTION'")
        store.conn.commit()
    with ArenaStore(root) as store:
        q = Queries(store)
        bundle = _build_bundle(store, q)
        public_hash = next(
            r["execution_hash"]
            for r in q.run_explorer()
            if r["execution_hash"] == ingest_export(store, public).execution_hash
        )
        blob = json.dumps([bundle, _public_run_document(store, q, public_hash, bundle)])
    assert not out.exists()
    assert "PRIVATE" not in blob and '"' + "b" * 64 + '"' not in blob
    assert "PUBLIC-LABEL" in blob


@pytest.mark.parametrize(
    "field,value", [("oof_enforced", True), ("group_leakage_checked", True), ("nested_cv_safe", False)]
)
def test_excluded_nondefault_attestation_is_rejected(empty_store, field, value):
    source = run()
    source["leakage_attestation"] = {"reported_fields": [], field: value}
    result = ingest_export(empty_store, source)
    assert result.status == "rejected" and result.issues[0]["code"] == "schema_invalid"
    assert empty_store.count("executions") == 0


def test_normalization_never_creates_safety_or_publication_evidence(empty_store):
    source = run()
    del source["dataset"]["visibility"]
    del source["dataset"]["visibility_source"]
    source.pop("leakage_attestation")
    normalized = ArenaRunExport.model_validate(source).to_manifest()
    a, b = validate_metadata(source), validate_metadata(normalized)
    assert a.arena_export_hash == b.arena_export_hash
    assert a.reported_attestation_fields == b.reported_attestation_fields == []
    assert a.visibility == b.visibility == "unreported"
    first = ingest_export(empty_store, normalized)
    assert ingest_export(empty_store, source).status == "already_ingested"
    q = Queries(empty_store)
    for row in (q.run_detail(first.execution_hash)["execution"], q.run_explorer()[0]):
        assert all(row[name] is None for name in ("oof_enforced", "group_leakage_checked", "nested_cv_safe"))
    assert "provenance" not in q.run_detail(first.execution_hash)["metadata"]


def test_marked_attestation_values_survive_round_trip(empty_store):
    source = run()
    source["leakage_attestation"] = {"oof_enforced": False, "nested_cv_safe": True}
    result = ingest_export(empty_store, ArenaRunExport.model_validate(source).to_manifest())
    row = Queries(empty_store).run_detail(result.execution_hash)["execution"]
    assert row["oof_enforced"] is False and row["nested_cv_safe"] is True
    assert row["group_leakage_checked"] is None


def folds(ids, metric="rmse", scope="cv", partition="validation"):
    from nirs4all_benchmarks.contract.folds import is_pseudo_fold

    rows = []
    for fid in ids:
        pseudo = is_pseudo_fold(fid)
        row_scope = ("refit" if fid in {"final", "final_agg"} else "view") if pseudo else scope
        rows.append(
            {
                "metric_name": metric,
                "metric_value": 1.0,
                "scope": row_scope,
                "fold_id": None if pseudo else fid,
                "source_fold_id": fid if pseudo else None,
                "partition": partition,
                "aggregation_level": ("group" if fid.endswith("_agg") else "sample"),
                "direction": "max" if metric == "r2" else "min",
                "direction_source": "registry",
            }
        )
    return rows


@pytest.mark.parametrize(
    "extra",
    [
        folds(["final"], scope="refit"),
        folds(["_agg"]),
        folds(["t"], scope="unreported"),
        folds(["t"], partition="test"),
        folds(["_agg"], scope="cv"),
    ],
)
def test_pseudo_refit_test_and_nonfold_score_ids_cannot_fill_a_gap(empty_store, extra):
    source = run()
    source["cv"] = {"n_folds": 2}
    source["scores"]["observations"] = folds(["0"]) + extra
    assert ingest_export(empty_store, source).status == "rejected"


@pytest.mark.parametrize("expected,repeats", [(2, None), (5, 2), (7, 2)])
def test_explicit_expectations_must_agree_with_cv(expected, repeats):
    source = run()
    source["cv"] = {"n_folds": 5, "n_repeats": repeats}
    source["scores"].update(expected_fold_count=expected)
    assert validate_metadata(source).issues[0]["code"] == "fold_expectation_contradiction"


def test_more_observed_folds_than_declared_is_also_a_contradiction():
    source = run()
    source["cv"] = {"n_folds": 2}
    source["scores"]["observations"] = folds(["0", "1", "2"])
    assert validate_metadata(source).issues[0]["code"] == "fold_count_contradiction"


def test_repeated_cv_and_per_metric_per_partition_coverage():
    source = run()
    source["cv"] = {"n_folds": 3, "n_repeats": 2}
    ids = [f"r{repeat}/f{fold}" for repeat in range(2) for fold in range(3)]
    source["scores"] = {
        "expected_fold_count": 6,
        "completed_fold_count": 6,
        "observations": folds(ids) + folds(ids, "r2"),
    }
    assert validate_metadata(source).status == "metadata_valid"
    source["scores"]["observations"] = folds(ids) + folds(ids[:1], "r2")
    assert validate_metadata(source).issues[0]["code"] == "incomplete_metric_fold_coverage"
    source["execution"].update(status="failed", terminal_status="incomplete")
    assert validate_metadata(source).status == "metadata_valid"


def test_withheld_residual_is_not_reconstructible_even_in_legacy_rows(empty_store):
    source = run()
    source["residuals"] = residuals()
    result = ingest_export(empty_store, source)
    q = Queries(empty_store)
    row = q.residuals(result.execution_hash)[0]
    assert row["y_true"] == 10.0 and row["y_pred"] is None and row["residual"] is None
    rset = empty_store.query_one("SELECT residual_set_id FROM residual_sets")
    row.update(y_pred=7.0, residual=3.0)
    with pytest.raises(ValueError, match="residual_blob_conflict"):
        empty_store.residuals.write(rset["residual_set_id"], [row])
    # Inject a legacy-shaped blob in this isolated synthetic store only.
    import pyarrow as pa
    import pyarrow.parquet as pq

    from nirs4all_benchmarks.store.residual_store import RESIDUALS_ARROW_SCHEMA

    pq.write_table(
        pa.Table.from_pylist([row], schema=RESIDUALS_ARROW_SCHEMA),
        empty_store.residuals.path_for(rset["residual_set_id"]),
    )
    assert q.residuals(result.execution_hash)[0]["y_pred"] is None


def test_warm_reader_opens_while_writer_holds_lock(tmp_path):
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        ingest_export(store, run())
    writer = sqlite3.connect(root / "arena.sqlite")
    writer.execute("BEGIN IMMEDIATE")
    finished = threading.Event()
    errors = []

    def reader():
        try:
            with ArenaStore(root) as store:
                assert Queries(store).overview()["executions"] == 1
        except BaseException as error:
            errors.append(error)
        finally:
            finished.set()

    thread = threading.Thread(target=reader)
    try:
        started = time.monotonic()
        thread.start()
        assert finished.wait(3), "warm store reader blocked behind a writer"
        assert not errors
        assert time.monotonic() - started < 3
    finally:
        writer.rollback()
        writer.close()
        thread.join(35)


def test_registry_direction_and_explicit_custom_direction_without_hash_drift(empty_store):
    source = run()
    base = source["scores"]["observations"][0]
    source["scores"]["observations"] = [{**base, "metric_name": "r2", "metric_value": 0.8, "direction": "max"}]
    normalized = ArenaRunExport.model_validate(source).to_manifest()
    assert validate_metadata(source).arena_export_hash == export_hash(normalized)
    result = ingest_export(empty_store, normalized)
    assert Queries(empty_store).run_detail(result.execution_hash)["scores"][0]["direction"] == "max"
    source["scores"]["observations"] = [
        {
            **source["scores"]["observations"][0],
            "metric_name": "custom_gain",
            "metric_value": 0.9,
            "direction": "max",
            "direction_source": "producer_reported",
        }
    ]
    assert ingest_export(empty_store, source).status == "committed"
    assert Queries(empty_store).leaderboard(metric="custom_gain")["direction"] == "max"
    source["scores"]["observations"] = [
        {
            **base,
            "metric_name": "custom_unknown",
            "metric_value": 0.1,
            "direction": None,
            "direction_source": "unreported",
        }
    ]
    result = ingest_export(empty_store, source)
    assert result.status == "committed"
    assert Queries(empty_store).run_detail(result.execution_hash)["scores"][0]["direction"] is None
    assert Queries(empty_store).overview()["executions"] == 3
    assert Queries(empty_store).overview()["valid_executions"] == 2
    with pytest.raises(ValueError, match="direction_unreported"):
        Queries(empty_store).leaderboard(metric="custom_unknown")


def test_static_custom_direction_is_refused_instead_of_using_a_frontend_default(tmp_path, monkeypatch):
    root, out = tmp_path / "store", tmp_path / "site"
    source = run()
    source["scores"]["observations"] = [
        {
            **source["scores"]["observations"][0],
            "metric_name": "custom_gain",
            "metric_value": 0.9,
            "direction": "max",
            "direction_source": "producer_reported",
        }
    ]
    with ArenaStore(root) as store:
        assert ingest_export(store, source).status == "committed"
        with pytest.raises(ValueError, match="registered metric directions"):
            _build_bundle(store, Queries(store))
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, out)
    assert not out.exists()
