"""Synthetic SQLite fixtures using the read-only verified owner table/score shapes.

pipelines/predictions columns and constraints: nirs4all/pipeline/storage/store_schema.py.
Only referenced runs/chains are minimal stubs. No producer imports or execution.
"""

from __future__ import annotations

import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from conftest import declare_synthetic_safety
from nirs4all_benchmarks.adapters import workspace_to_exports
from nirs4all_benchmarks.cli import app
from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.site_build import _build_bundle
from nirs4all_benchmarks.store import ArenaStore, Queries

# Relevant owner DDL copied locally, not imported or dynamically read by tests.
OWNER_SCHEMA = """
CREATE TABLE runs(run_id TEXT PRIMARY KEY);
CREATE TABLE chains(chain_id TEXT PRIMARY KEY);
CREATE TABLE pipelines (
    pipeline_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    name TEXT NOT NULL,
    expanded_config TEXT,
    original_template TEXT,
    generator_choices TEXT,
    dataset_name TEXT NOT NULL,
    dataset_hash TEXT,
    status TEXT DEFAULT 'running',
    created_at TIMESTAMP DEFAULT current_timestamp,
    completed_at TIMESTAMP,
    best_val REAL,
    best_test REAL,
    metric TEXT,
    duration_ms INTEGER,
    error TEXT
);
CREATE TABLE predictions (
    prediction_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    chain_id TEXT REFERENCES chains(chain_id),
    dataset_name TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_class TEXT NOT NULL,
    fold_id TEXT NOT NULL,
    partition TEXT NOT NULL,
    val_score REAL,
    test_score REAL,
    train_score REAL,
    metric TEXT NOT NULL,
    task_type TEXT NOT NULL,
    n_samples INTEGER,
    n_features INTEGER,
    scores TEXT,
    best_params TEXT,
    preprocessings TEXT DEFAULT '',
    branch_id INTEGER,
    branch_name TEXT,
    exclusion_count INTEGER DEFAULT 0,
    exclusion_rate REAL DEFAULT 0.0,
    refit_context TEXT DEFAULT NULL,
    prediction_scope TEXT,
    prediction_level TEXT,
    evaluation_scope TEXT,
    reduction_role TEXT,
    reduction_id TEXT,
    physical_sample_id TEXT,
    origin_sample_id TEXT,
    derived_unit_id TEXT,
    unit_level TEXT,
    unit_id TEXT,
    row_id TEXT,
    sample_influence_weight REAL,
    created_at TIMESTAMP DEFAULT current_timestamp
);
CREATE UNIQUE INDEX idx_predictions_natural_key_v2
ON predictions(pipeline_id, chain_id, fold_id, partition, model_name, branch_id);
"""


def owner_workspace(root, *, folds=5, status="completed", final_id="final", arrays=True):
    root.mkdir()
    (root / "arrays").mkdir()
    rows = []
    with sqlite3.connect(root / "store.sqlite") as c:
        c.executescript(OWNER_SCHEMA)
        c.execute("INSERT INTO runs VALUES ('r')")
        c.execute("INSERT INTO chains VALUES ('c')")
        steps = [{"model": {"class": "sklearn.cross_decomposition.PLSRegression"}, "params": {"n_components": 3}}]
        c.execute("""INSERT INTO pipelines
            (pipeline_id,run_id,name,expanded_config,dataset_name,dataset_hash,status,metric)
            VALUES ('p','r','Synthetic owner fixture',?,'owner-fixture',?,?,'rmse')""",
                  (json.dumps(steps), "a" * 64, status))
        for fold in [*(str(i) for i in range(folds)), final_id]:
            parts = ["train", "test"] if fold == final_id else ["val", "train"]
            scores = {"val": {} if fold == final_id else {"rmse": 1.0},
                      "train": {"rmse": 0.05 if fold == final_id else 0.1}, "test": {"rmse": 0.5}}
            for partition in parts:
                prediction_id = f"{fold}-{partition}"
                c.execute("""INSERT INTO predictions
                    (prediction_id,pipeline_id,chain_id,dataset_name,model_name,model_class,fold_id,partition,
                     val_score,test_score,train_score,metric,task_type,n_samples,n_features,scores,branch_id)
                    VALUES (?,'p','c','owner-fixture','PLSRegression','sklearn.cross_decomposition.PLSRegression',
                            ?,?,?,?,?, 'rmse','regression',2,700,?,0)""",
                          (prediction_id, fold, partition, 999.0 if fold == final_id else 1.0,
                           0.5, 0.05 if fold == final_id else 0.1, json.dumps(scores)))
                rows.append({"prediction_id": prediction_id, "fold_id": fold, "partition": partition,
                             "dataset_name": "owner-fixture", "model_name": "PLSRegression", "sample_indices": [0, 1],
                             "y_true": [10.0, 11.0], "y_pred": [9.5, 10.5]})
    if arrays:
        pq.write_table(pa.Table.from_pylist(rows), root / "arrays" / "owner-fixture.parquet")
    return root


# Each extractor exercises a distinct production aggregation path.
AGGREGATIONS = [
    ("leaderboard", {}, lambda r: [x["mean"] for x in r["rows"]]),
    ("matrix", {}, lambda r: [x["value"] for x in r["cells"]]),
    ("run_explorer", {}, lambda r: [x["metric_value"] for x in r]),
    ("operator_effect", {}, lambda r: [x["mean"] for x in r["series"]]),
    ("parameter_effect", {"param_name": "n_components"}, lambda r: [x["metric_value"] for x in r["points"]]),
    ("robustness", {}, lambda r: [x["mean"] for x in r]),
    ("pivot", {"group_by": "model_family"}, lambda r: [x["value"] for x in r["rows"]]),
    ("parallel", {"dimensions": ["model_family"]}, lambda r: [x["metric"] for x in r["rows"]]),
    ("stats", {}, lambda r: r["values"]),
    ("pipeline_graph", {}, lambda r: [x["score"] for x in r["nodes"]]),
    ("operator_graph", {}, lambda r: [x["score"] for x in r["nodes"]]),
    ("composition", {}, lambda r: [x["score"] for x in r["rows"]]),
]


@pytest.mark.parametrize("method,kwargs,values", AGGREGATIONS)
def test_every_aggregation_selects_one_partition(tmp_path, method, kwargs, values):
    export, = workspace_to_exports(owner_workspace(tmp_path / "workspace"))
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed"
        aggregate = getattr(Queries(store), method)
        assert set(values(aggregate(**kwargs))) == {1.0}
        assert set(values(aggregate(**kwargs, scope="cv", partition="train"))) == {0.1}
        with pytest.raises(ValueError, match="score_partition_required_for_refit"):
            aggregate(**kwargs, scope="refit")
        for partition, expected in (("train", 0.05), ("test", 0.5)):
            assert set(values(aggregate(**kwargs, scope="refit", partition=partition))) == {expected}
        with pytest.raises(ValueError, match="invalid_score_partition"):
            aggregate(**kwargs, partition="not-a-partition")


@pytest.mark.parametrize("final_id", ["final", "final_agg"])
def test_owner_nested_scores_partitions_and_unknown_expectations_are_preserved(tmp_path, final_id):
    workspace = owner_workspace(tmp_path / "workspace", final_id=final_id)
    with sqlite3.connect(workspace / "store.sqlite") as c:
        columns = {r[1] for r in c.execute("PRAGMA table_info(pipelines)")}
        assert "expected_fold_count" not in columns and "terminal_status" not in columns
    export, = workspace_to_exports(workspace)
    assert export.cv.n_folds is None and export.scores.expected_fold_count is None
    assert export.scores.completed_fold_count is None
    assert export.dataset.n_samples is None  # Prediction n_samples=2 is not dataset cardinality.
    assert export.execution.terminal_status == "completed"  # Explicit stored owner status, not fold inference.
    assert len(export.scores.observations) == 12
    assert all(o.n_samples == 2 for o in export.scores.observations)
    assert {(o.partition, o.metric_value) for o in export.scores.observations if o.scope == "refit"} == {
        ("train", 0.05), ("test", 0.5)}
    assert validate_metadata(export).status == "metadata_valid"
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed"
        q = Queries(store)
        scores = q.run_detail(result.execution_hash)["scores"]
        assert len(scores) == 12 and not any(s["partition"] == "final" for s in scores)
        ranked, = q.leaderboard()["rows"]
        assert ranked["mean"] == 1.0 and ranked["n_obs"] == 5
        if final_id == "final":
            rows = q.residuals(result.execution_hash)
            keys = [(r["scope"], r["fold_id"], r["partition"], r["sample_id"]) for r in rows]
            assert len(rows) == len(set(keys)) == 24
            train = q.residuals(result.execution_hash, partition="train", scope="refit")
            test = q.residuals(result.execution_hash, partition="test", scope="refit")
            assert len(train) == len(test) == 2
            assert {r["sample_id"] for r in train} == {r["sample_id"] for r in test}
        assert ingest_export(store, declare_synthetic_safety(export)).status == "already_ingested"


def test_owner_expected_count_is_never_invented_from_observed_rows(tmp_path):
    export, = workspace_to_exports(owner_workspace(tmp_path / "workspace", folds=4, arrays=False))
    assert export.cv.n_folds is None and export.scores.expected_fold_count is None
    assert export.scores.completed_fold_count is None
    result = validate_metadata(export)
    assert result.status == "metadata_valid"  # No asserted five-fold expectation exists in this owner shape.
    assert result.scientific_verification == "not_performed"


def test_owner_branch_zero_and_absent_branch_do_not_merge(tmp_path):
    workspace = owner_workspace(tmp_path / "workspace", arrays=True)
    with sqlite3.connect(workspace / "store.sqlite") as c:
        c.execute("UPDATE predictions SET branch_id=NULL WHERE partition='train' AND fold_id != 'final'")
    exports = workspace_to_exports(workspace)
    assert len(exports) == 2 and sum(len(e.scores.observations) for e in exports) == 12
    assert {tuple(e.pipeline.nodes[-1].branch_path) for e in exports} == {(), ("0",)}
    with ArenaStore(tmp_path / "store") as store:
        assert all(ingest_export(store, declare_synthetic_safety(e)).status == "committed" for e in exports)
        rows = Queries(store).leaderboard()["rows"]
        assert len(rows) == 1 and rows[0]["mean"] == 1.0 and rows[0]["n_obs"] == 5


@pytest.mark.parametrize("status,terminal", [(None, "unreported"), ("ok", "unreported"), ("failed", "failed")])
def test_owner_status_is_not_inferred_from_rows(tmp_path, status, terminal):
    export, = workspace_to_exports(owner_workspace(tmp_path / "workspace", status=status))
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed" and result.terminal_status == terminal
        assert Queries(store).leaderboard()["rows"] == []


def test_owner_partition_scalar_fallback_uses_own_column(tmp_path):
    workspace = owner_workspace(tmp_path / "workspace", arrays=False)
    with sqlite3.connect(workspace / "store.sqlite") as c:
        c.execute("UPDATE predictions SET scores=NULL")
    export, = workspace_to_exports(workspace)
    assert {(o.partition, o.metric_value) for o in export.scores.observations if o.scope == "refit"} == {
        ("train", 0.05), ("test", 0.5)}
    assert {(o.partition, o.metric_value) for o in export.scores.observations if o.scope == "cv"} == {
        ("train", 0.1), ("validation", 1.0)}


@pytest.mark.parametrize("mutation,code", [
    ("UPDATE predictions SET partition='mystery' WHERE prediction_id='0-val'", "workspace_partition_missing_or_unsupported"),
    ("UPDATE predictions SET scores='malformed'", "workspace_scores_malformed"),
    ("UPDATE predictions SET scores='[]'", "workspace_scores_unsupported"),
    ('''UPDATE predictions SET scores='{"test":{"rmse":0.5}}' WHERE partition='val' ''', "workspace_score_partition_missing_or_ambiguous"),
    ('''UPDATE predictions SET scores='{"val":{"rmse":true}}' WHERE partition='val' ''', "workspace_score_value_unsupported"),
    ("UPDATE pipelines SET status='running'", "workspace_nonterminal_or_unsupported_status"),
    ("UPDATE predictions SET chain_id=NULL WHERE prediction_id='0-val'", "workspace_multiple_chains_unsupported"),
])
def test_unsupported_owner_shapes_refuse_explicitly(tmp_path, mutation, code):
    workspace = owner_workspace(tmp_path / "workspace")
    with sqlite3.connect(workspace / "store.sqlite") as c:
        c.execute(mutation)
    with pytest.raises(ValueError, match=code):
        workspace_to_exports(workspace)


def test_unsupported_residual_positions_refuse_without_fake_ids(tmp_path):
    workspace = owner_workspace(tmp_path / "workspace")
    path = workspace / "arrays" / "owner-fixture.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["sample_indices"] = None
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match="workspace_residual_positions_unsupported"):
        workspace_to_exports(workspace)


def test_disagreeing_array_partition_refuses_instead_of_relabeling(tmp_path):
    workspace = owner_workspace(tmp_path / "workspace")
    path = workspace / "arrays" / "owner-fixture.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["partition"] = "test"
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match="workspace_array_context_mismatch"):
        workspace_to_exports(workspace)


def test_cli_requires_refit_partition_and_prints_selected_context(tmp_path):
    export, = workspace_to_exports(owner_workspace(tmp_path / "workspace"))
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        assert ingest_export(store, declare_synthetic_safety(export)).status == "committed"
    runner = CliRunner()
    refused = runner.invoke(app, ["leaderboard", "--store", str(root), "--scope", "refit"])
    assert refused.exit_code == 2 and "score_partition_required_for_refit" in refused.output
    accepted = runner.invoke(app, ["leaderboard", "--store", str(root), "--scope", "refit", "--partition", "train"])
    assert accepted.exit_code == 0 and "0.0500" in accepted.output and "train" in accepted.output


def test_static_pack_refuses_contexts_its_consumer_cannot_select(tmp_path):
    export, = workspace_to_exports(owner_workspace(tmp_path / "workspace"))
    export.dataset.visibility = "public"
    export.dataset.visibility_source = "producer_reported"
    export.arena_export_hash = None
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, declare_synthetic_safety(export)).status == "committed"
        with pytest.raises(ValueError, match="static_score_partition_selection_unsupported"):
            _build_bundle(store, Queries(store))  # Pure metadata packing; never build_site.
