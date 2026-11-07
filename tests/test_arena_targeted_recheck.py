"""Synthetic, store-isolated regressions for the narrow Opus C1–C4 recheck."""

from __future__ import annotations

import json
import sqlite3

import pytest

from nirs4all_benchmarks.adapters import WorkspaceAdapterError, workspace_to_exports
from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.site_build import _build_bundle
from nirs4all_benchmarks.store import Queries
from test_adapters import _make_workspace
from test_arena_work_review import folds, run


@pytest.mark.parametrize("partition", ["train", "validation"])
@pytest.mark.parametrize("scope", ["fold", "cv"])
def test_real_fold_ids_at_both_scopes_detect_partial_and_complete_series(empty_store, scope, partition):
    source = run()
    source["cv"] = {"method": "kfold", "n_folds": 5}
    source["scores"]["observations"] = folds(["0", "1"], scope=scope, partition=partition)
    result = validate_metadata(source)
    assert result.status == "rejected" and result.fold_coverage == "reported"
    assert ingest_export(empty_store, source).status == "rejected"
    assert empty_store.count("executions") == 0
    source["scores"]["observations"] = folds(list("01234"), scope=scope, partition=partition)
    result = ingest_export(empty_store, source)
    assert result.status == "committed"
    assert result.clean_report["metadata_validation"]["fold_coverage"] == "reported"


def test_cv_fold_series_cannot_be_filled_by_aggregate_or_other_metric(empty_store):
    source = run()
    source["cv"] = {"n_folds": 5}
    source["scores"]["observations"] = folds(["0", "1"], scope="cv") + folds(list("01234"), metric="r2") + [
        {**folds(["0"])[0], "fold_id":None}]
    result = ingest_export(empty_store, source)
    assert result.status == "rejected"
    assert result.issues[0]["code"] == "incomplete_metric_fold_coverage"
    source["execution"].update(status="failed", terminal_status="incomplete")
    result = ingest_export(empty_store, source)
    assert result.status == "committed" and result.terminal_status == "incomplete"
    assert result.clean_report["metadata_validation"]["fold_coverage"] == "reported"
    assert Queries(empty_store).leaderboard()["rows"] == []


def test_workspace_score_only_cv_fold_shape_is_counted(tmp_path):
    workspace = _make_workspace(tmp_path)
    # Removing this generated fixture's arrays makes score observations the sole
    # fold evidence, matching the producer shape that bypassed the earlier gate.
    (workspace / "arrays" / "corn.parquet").unlink()
    with sqlite3.connect(workspace / "store.sqlite") as connection:
        connection.execute("ALTER TABLE pipelines ADD COLUMN expected_fold_count INTEGER")
        connection.execute("ALTER TABLE pipelines ADD COLUMN status TEXT")
        connection.execute("ALTER TABLE pipelines ADD COLUMN terminal_status TEXT")
        connection.execute("UPDATE pipelines SET expected_fold_count=5, status='ok', terminal_status='completed'")
    with pytest.raises(WorkspaceAdapterError,match="incomplete_metric_fold_coverage"):
        workspace_to_exports(workspace)
    with sqlite3.connect(workspace / "store.sqlite") as connection:
        connection.execute("UPDATE pipelines SET expected_fold_count=3")
    export = workspace_to_exports(workspace)[0]
    result = validate_metadata(export)
    assert result.status == "metadata_valid" and result.fold_coverage == "reported"


def decode(table, strings):
    return [{key: (strings[col["s"][i]] if col["s"][i] is not None else None) if "s" in col else col["v"][i]
             for key, col in table["columns"].items()} for i in range(table["n"])]


def test_static_pack_omits_string_parameter_facets_but_keeps_typed_numbers_and_booleans(empty_store):
    source = run()
    source["pipeline"]["nodes"][0]["params"] = {
        "k": 3, "gain": 12.5, "enabled": True, "disabled": False,
        "path": "/home/u/PARAM-PATH", "category": "SECRET-CATEGORY",
        "looks_boolean": "True", "looks_numeric": "12.5",
        "nested": {"path": "NESTED-SECRET"}, "sequence": ["SEQUENCE-SECRET"],
    }
    assert ingest_export(empty_store, source).status == "committed"
    # Retain local pipeline metadata; the correction belongs to public packing.
    local = empty_store.query("SELECT facet_value FROM run_facets WHERE facet_key = 'param:path'")
    assert local[0]["facet_value"] == "/home/u/PARAM-PATH"
    bundle = _build_bundle(empty_store, Queries(empty_store))  # In memory; no site rebuild or assets.
    blob = json.dumps(bundle)
    for secret in ("/home/u/PARAM-PATH", "SECRET-CATEGORY", "NESTED-SECRET", "SEQUENCE-SECRET"):
        assert secret not in blob
    facets = decode(bundle["tables"]["facets"], bundle["strings"])
    params = {(row["facet_key"], row["facet_value"]) for row in facets if row["facet_key"].startswith("param:")}
    assert params == {("param:k", "3"), ("param:gain", "12.5"), ("param:enabled", "True"), ("param:disabled", "False")}
    assert {row["name"] for row in bundle["parameters"]} == {"k", "gain", "enabled", "disabled"}
    assert any(row["facet_key"] == "model_family" for row in facets)


def test_dangling_citation_returns_typed_refusal_and_same_export_can_retry(empty_store):
    source = run()
    source["dataset"]["dataset_card"]["citation_id"] = "CITATION-NOT-YET-PRESENT"
    for _ in range(2):
        result = ingest_export(empty_store, source)
        assert result.status == "rejected" and result.issues[0]["code"] == "metadata_not_indexable"
        assert "CITATION-NOT-YET-PRESENT" not in json.dumps(result.issues)
        assert all(empty_store.count(table) == 0 for table in (
            "ingestion_batches", "collections", "dataset_cards", "dataset_fingerprints", "executions", "metric_observations"))
        assert not list(empty_store.exports_dir.iterdir())
    original_hash = result.arena_export_hash
    empty_store.upsert("citations", {"citation_id": "CITATION-NOT-YET-PRESENT", "title": "Synthetic test citation"})
    empty_store.conn.commit()
    result = ingest_export(empty_store, source)
    assert result.status == "committed" and result.arena_export_hash == original_hash
    assert ingest_export(empty_store, source).status == "already_ingested"
    assert empty_store.count("executions") == empty_store.count("ingestion_batches") == 1


def test_publication_flags_are_not_cross_run_or_single_sample_score_privacy(empty_store):
    source_a, source_c = run(), run()
    source_a["residuals"] = {"key":"sample_id", "publishable": {"y_true":True,"y_pred":False,"residual":True,"y_proba":True}, "inline": [
        {"sample_id": "same", "y_true": 10.0, "y_pred": 1.0,"scope":"cv","partition":"validation","fold_id":"0"}]}
    source_c["residuals"] = {"key":"sample_id", "publishable": {"y_true":False,"y_pred":True,"residual":True,"y_proba":True}, "inline": [
        {"sample_id": "same", "y_true": 10.0, "y_pred": 7.0,"scope":"cv","partition":"validation","fold_id":"0"}]}
    a, c = ingest_export(empty_store, source_a), ingest_export(empty_store, source_c)
    assert a.ok and c.ok
    q = Queries(empty_store)
    row_a, row_c = q.residuals(a.execution_hash)[0], q.residuals(c.execution_hash)[0]
    assert row_a["sample_id"] == row_c["sample_id"]
    assert row_c["y_true"] is row_c["residual"] is None
    assert row_a["y_true"] - row_c["y_pred"] == 3.0  # Documented cross-run inference remains possible.
    source_c["cv"] = {"method": "loo", "n_folds": 1}
    source_c["residuals"]["inline"][0]["fold_id"] = "0"
    source_c["scores"]["observations"] = [{**folds(["0"],scope="fold")[0], "metric_value":3.0,"n_samples":1}]
    single = ingest_export(empty_store, source_c)
    row = q.residuals(single.execution_hash)[0]
    score = q.run_detail(single.execution_hash)["scores"][0]["metric_value"]
    assert row["y_true"] is None and score == 3.0
    assert {row["y_pred"] - score, row["y_pred"] + score} == {4.0, 10.0}
