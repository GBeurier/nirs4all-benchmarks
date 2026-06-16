"""Dual-compatibility adapters: nirs4all workspace, dag-ml bundle, .n4a."""

from __future__ import annotations

import json
import sqlite3
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nirs4all_benchmarks.adapters import (
    bundle_to_export,
    extract_n4a_recipe,
    n4a_pipeline_identity,
    workspace_to_exports,
)
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore


def _make_workspace(root: Path) -> Path:
    ws = root / "workspace"
    (ws / "arrays").mkdir(parents=True)
    conn = sqlite3.connect(str(ws / "store.sqlite"))
    conn.executescript(
        """
        CREATE TABLE pipelines(pipeline_id TEXT, name TEXT, expanded_config TEXT, dataset_name TEXT,
                               dataset_hash TEXT, metric TEXT);
        CREATE TABLE predictions(prediction_id TEXT, pipeline_id TEXT, dataset_name TEXT, model_name TEXT,
                                 fold_id TEXT, partition TEXT, val_score REAL, metric TEXT, task_type TEXT,
                                 n_samples INT, n_features INT, scores TEXT);
        """
    )
    steps = json.dumps([{"class": "sklearn.preprocessing.StandardScaler"},
                        {"model": {"class": "sklearn.cross_decomposition.PLSRegression"}, "params": {"n_components": 10}}])
    conn.execute("INSERT INTO pipelines VALUES (?,?,?,?,?,?)", ("p1", "PLS", steps, "corn", "dh", "rmse"))
    rows = []
    for f in range(3):
        pid = f"pred_{f}"
        conn.execute("INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                     (pid, "p1", "corn", "PLS", f"fold{f}", "validation", 0.7, "rmse", "regression", 30, 700,
                      json.dumps({"rmse": 0.7, "r2": 0.8})))
        rows.append({"prediction_id": pid, "y_true": [10.0, 11.0, 12.0], "y_pred": [10.1, 10.8, 12.3],
                     "sample_indices": [f * 3, f * 3 + 1, f * 3 + 2]})
    conn.commit()
    conn.close()
    schema = pa.schema([("prediction_id", pa.utf8()), ("y_true", pa.list_(pa.float64())),
                        ("y_pred", pa.list_(pa.float64())), ("sample_indices", pa.list_(pa.int32()))])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), ws / "arrays" / "corn.parquet")
    return ws


def test_adapter_a_workspace(tmp_path: Path):
    ws = _make_workspace(tmp_path)
    exps = workspace_to_exports(ws)
    assert len(exps) == 1
    exp = exps[0]
    assert exp.residuals.key == "positional"  # degraded keying (no stable sample id)
    assert exp.leakage_attestation.oof_enforced is False  # honest
    with ArenaStore(tmp_path / "arena") as store:
        res = ingest_export(store, exp, policy=IngestionPolicy(collection_id="ws"))
        assert res.status == "committed"
        assert store.count("metric_observations") > 0


def test_adapter_b_bundle(tmp_path: Path):
    bundle = {
        "bundle_id": "b1", "schema_version": 1, "graph_fingerprint": "a" * 64,
        "campaign_fingerprint": "c" * 64, "controller_fingerprint": "d" * 64, "unsafe_flags": [],
        "refit_artifacts": [{"id": "r1"}],
        "prediction_caches": [{"partition": "validation", "fold_id": "fold0",
                               "sample_ids": ["s1", "s2", "s3"], "values": [10.1, 10.9, 12.2],
                               "y_true": [10.0, 11.0, 12.0]}],
        "metadata": {"label": "dagml-pls"},
    }
    reports = [{"partition": "validation", "level": "macro", "metrics": {"rmse": 0.31, "r2": 0.86}}]
    exp = bundle_to_export(bundle, metric_reports=reports, n_samples=3, n_features=700)
    assert exp.residuals.key == "sample_id"  # native sample keying
    assert exp.leakage_attestation.oof_enforced is True  # dag-ml enforces OOF
    assert exp.pipeline.engine_graph_fingerprint == "a" * 64
    with ArenaStore(tmp_path / "arena") as store:
        res = ingest_export(store, exp, policy=IngestionPolicy(collection_id="dagml", collection_kind="benchmark_release"))
        assert res.status == "committed"


def test_n4a_recipe_strips_weights(tmp_path: Path):
    n4a = tmp_path / "model.n4a"
    with zipfile.ZipFile(n4a, "w") as zf:
        zf.writestr("manifest.json", json.dumps({"bundle_format_version": "1.0"}))
        zf.writestr("pipeline.json", json.dumps([{"class": "nirs4all.SNV"},
                                                 {"model": "PLS", "params": {"n_components": 7}}]))
        zf.writestr("artifacts/step_0.joblib", b"FAKE_WEIGHTS")
    recipe = extract_n4a_recipe(n4a)
    assert len(recipe["steps"]) == 2
    assert recipe["stripped_artifacts"] == ["artifacts/step_0.joblib"]
    pid = n4a_pipeline_identity(n4a)
    assert len(pid.pipeline_dag_hash) == 64
    assert pid.nirs4all_identity_hash is not None
