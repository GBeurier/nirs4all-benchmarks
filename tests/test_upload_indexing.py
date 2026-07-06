"""Unified upload state machine + role-aware faceting."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from nirs4all_benchmarks.indexing import (
    ROLE_AUGMENTATION,
    ROLE_MERGE,
    ROLE_MODEL,
    ROLE_PREPROCESSING,
    ROLE_SCALER,
    classify_role,
)
from nirs4all_benchmarks.ingestion import register_pipeline, upload
from nirs4all_benchmarks.store import ArenaStore, Queries


def test_role_classification():
    assert classify_role("nirs4all.transform.SNV", "transform") == ROLE_PREPROCESSING
    assert classify_role("sklearn.preprocessing.StandardScaler", "transform") == ROLE_SCALER
    assert classify_role("nirs4all.augmentation.RandomShift", None) == ROLE_AUGMENTATION
    assert classify_role("sklearn.cross_decomposition.PLSRegression", None) == ROLE_MODEL
    assert classify_role("anything", "model") == ROLE_MODEL
    assert classify_role("nirs4all.merge.MeanEnsemble", "mean") == ROLE_MERGE


def test_role_classification_no_module_path_collisions():
    # Regression: sklearn.ensemble.* models must be 'model' (not caught by the
    # 'ensemble' module segment as 'merge'); *Transformer preprocessors stay
    # preprocessing (not caught by a neural 'transformer' needle).
    for op in ("sklearn.ensemble.RandomForestRegressor", "sklearn.ensemble.ExtraTreesRegressor",
               "sklearn.ensemble.GradientBoostingRegressor", "sklearn.ensemble.AdaBoostRegressor"):
        assert classify_role(op, "transform") == ROLE_MODEL, op
    assert classify_role("sklearn.preprocessing.PowerTransformer", "transform") == ROLE_PREPROCESSING
    assert classify_role("sklearn.preprocessing.QuantileTransformer", "transform") == ROLE_PREPROCESSING
    assert classify_role("sklearn.ensemble.StackingRegressor", "transform") == ROLE_MERGE


def test_raw_step_list_hashes_like_role_tagged_graph():
    # Regression: a raw {"model": ...} step list must hash identically to the
    # engine/role-tagged graph for the same pipeline (else plans never fulfil and
    # the leaderboard splits one pipeline into two).
    from nirs4all_benchmarks.adapters.nirs4all_workspace import _steps_to_nodes
    from nirs4all_benchmarks.identity import compute_pipeline_dag_hash

    steps = [{"class": "nirs4all.transform.SNV"},
             {"model": "sklearn.cross_decomposition.PLSRegression", "params": {"n_components": 10}}]
    h_raw = compute_pipeline_dag_hash(steps).pipeline_dag_hash
    h_nodes = compute_pipeline_dag_hash({"nodes": _steps_to_nodes(steps)}).pipeline_dag_hash
    assert h_raw == h_nodes


def test_extensionless_file_upload_is_sniffed(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports

    p = tmp_path / "manifest"  # no extension
    p.write_text(json.dumps(generate_fixture_exports()[0].to_manifest()))
    with ArenaStore(tmp_path / "a") as store:
        res = upload(store, str(p), collection_id="up")
        assert res.kind == "arena_export" and res.status == "ingested"


def test_upload_pipeline_list_plans_on_datasets(tmp_path: Path):
    with ArenaStore(tmp_path / "a") as store:
        pipe = ["sklearn.preprocessing.StandardScaler", {"class": "nirs4all.transform.SNV"},
                {"model": "sklearn.cross_decomposition.PLSRegression", "params": {"n_components": 9}}]
        res = upload(store, pipe, collection_id="up", target_datasets=["my_dataset"])
        assert res.kind == "pipeline" and res.status == "registered"
        assert res.pipeline_dag_hash and len(res.pipeline_dag_hash) == 64
        assert res.datasets[0]["status"] == "planned"
        assert store.count("planned_runs") == 1
        assert store.count("pipeline_dags") == 1


def test_upload_yaml_pipeline(tmp_path: Path):
    yaml_text = """
pipeline:
  - class: nirs4all.transform.SNV
  - model: sklearn.cross_decomposition.PLSRegression
    params:
      n_components: 7
"""
    with ArenaStore(tmp_path / "a") as store:
        res = upload(store, yaml_text, collection_id="up")
        assert res.kind == "pipeline" and res.pipeline_dag_hash


def test_upload_n4a_strips_weights(tmp_path: Path):
    n4a = tmp_path / "model.n4a"
    with zipfile.ZipFile(n4a, "w") as zf:
        zf.writestr("manifest.json", json.dumps({"bundle_format_version": "1.0"}))
        zf.writestr("pipeline.json", json.dumps([{"class": "nirs4all.transform.SNV"},
                                                 {"model": "PLS", "params": {"n_components": 11}}]))
        zf.writestr("artifacts/step_0.joblib", b"WEIGHTS")
    with ArenaStore(tmp_path / "a") as store:
        res = upload(store, str(n4a), collection_id="up", target_datasets=["d1"])
        assert res.kind == "pipeline"
        assert res.stripped_artifacts == 1


def test_upload_arena_export_ingests(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports

    with ArenaStore(tmp_path / "a") as store:
        manifest = generate_fixture_exports()[0].to_manifest()
        res = upload(store, manifest, collection_id="up")
        assert res.kind == "arena_export" and res.status == "ingested"
        assert store.count("executions") == 1


def test_public_release_upload_requires_as_release_for_results(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports

    manifest = generate_fixture_exports()[0].to_manifest()
    with ArenaStore(tmp_path / "a") as store, pytest.raises(ValueError, match="as_release=True"):
        upload(store, manifest, collection_id="benchmark_release")


def test_release_upload_quarantines_unattested_leakage_when_as_release(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports

    exp = generate_fixture_exports()[0]
    exp.leakage_attestation.oof_enforced = False
    with ArenaStore(tmp_path / "a") as store:
        res = upload(store, exp.to_manifest(), collection_id="benchmark_release", as_release=True)

        assert res.kind == "arena_export"
        assert res.status == "ingested"
        assert res.ingestion is not None
        assert res.ingestion["status"] == "quarantined"
        assert res.ingestion["validity_status"] == "quarantined"


def test_planned_fulfilled_by_execution(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

    exp = generate_fixture_exports()[0]
    df = exp.dataset.dataset_fingerprint
    with ArenaStore(tmp_path / "a") as store:
        # register the SAME pipeline graph + plan on the dataset
        graph = exp.pipeline.graph
        register_pipeline(store, graph, collection_id="up", target_datasets=[df])
        assert store.count("planned_runs", "status = 'planned'") == 1
        # now ingest a real execution for it → the plan is fulfilled
        ingest_export(store, exp, policy=IngestionPolicy(collection_id="up"))
        assert store.count("planned_runs", "status = 'planned'") == 0
        assert store.count("planned_runs", "status = 'fulfilled'") == 1


def test_facets_and_pivot_on_demo(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import seed_store

    seed_store(tmp_path / "demo", collection_id="demo", demo=True)
    with ArenaStore(tmp_path / "demo") as store:
        q = Queries(store)
        keys = {f["facet_key"] for f in q.facets()}
        assert {"model_family", "preprocessing_op", "has_augmentation", "split_method"} <= keys
        assert "param:n_components" in keys
        # augmentation on/off both present in the demo
        aug = {r["group_value"] for r in q.pivot(group_by="has_augmentation", metric="rmse", scope="cv")["rows"]}
        assert aug == {"yes", "no"}
        # multiple models present
        models = {r["group_value"] for r in q.pivot(group_by="model_family", metric="rmse", scope="cv")["rows"]}
        assert len(models) >= 3
        # parallel coordinates rows carry the requested dimensions
        par = q.parallel(dimensions=["model_family", "param:n_components"], metric="rmse", scope="cv")
        assert par["rows"] and "metric" in par["rows"][0]


def test_pivot_numeric_param_ordering(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import seed_store

    seed_store(tmp_path / "demo", collection_id="demo", demo=True)
    with ArenaStore(tmp_path / "demo") as store:
        pv = Queries(store).pivot(group_by="param:n_components", metric="rmse", scope="cv")
        nums = [r["group_num"] for r in pv["rows"] if r["group_num"] is not None]
        assert nums == sorted(nums)


def test_graph_and_composition_on_demo(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import seed_store

    seed_store(tmp_path / "demo", collection_id="demo", demo=True)
    with ArenaStore(tmp_path / "demo") as store:
        q = Queries(store)
        pg = q.pipeline_graph(metric="rmse", scope="cv")
        assert pg["nodes"] and pg["edges"]  # a real clustered graph
        assert {n["cluster"] for n in pg["nodes"]}  # model-family clusters
        og = q.operator_graph(metric="rmse", scope="cv")
        # operator clusters are stage roles, classified correctly (no module collisions)
        clusters = {n["cluster"] for n in og["nodes"]}
        assert "model" in clusters and "preprocessing" in clusters
        assert "merge" not in clusters or all("Regressor" not in n["operator"] for n in og["nodes"]
                                              if n["cluster"] == "merge")
        comp = q.composition(metric="rmse", scope="cv")
        roles = {r["role"] for r in comp["rows"]}
        assert {"model", "preprocessing", "augmentation"} <= roles


def test_stats_on_demo(tmp_path: Path):
    from nirs4all_benchmarks.fixtures import seed_store

    seed_store(tmp_path / "demo", collection_id="demo", demo=True)
    with ArenaStore(tmp_path / "demo") as store:
        st = Queries(store).stats(metric="rmse", scope="cv")
        assert st["summary"]["n"] > 0
        assert {"mean", "median", "std", "p25", "p75"} <= set(st["summary"])
        assert len(st["by_dataset"]) == 2
        assert st["values"]  # raw values for the histogram
        # numeric facets correlate; each entry has r in [-1, 1]
        assert all(-1.0 <= c["r"] <= 1.0 for c in st["correlations"])
