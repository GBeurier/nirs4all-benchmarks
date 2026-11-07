"""B1/B2 faithful owner-storage counterexamples; no producer engine execution.

Default-engine unresolved templates/empty choices and legacy resolved variants
mirror the actual Opus runs. Scores are synthetic metadata, not scientific proof.
"""

from __future__ import annotations

import json
import sqlite3

import pytest
from typer.testing import CliRunner

from conftest import declare_synthetic_safety
from nirs4all_benchmarks.adapters import (
    WorkspaceAdapter,
    WorkspaceAdapterError,
    bundle_to_export,
    workspace_to_exports,
)
from nirs4all_benchmarks.cli import app
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.schema import validate_manifest
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export, validate_metadata
from nirs4all_benchmarks.ingestion.resolve import resolve_identities
from nirs4all_benchmarks.store import ArenaStore, Queries
from test_offline_arena import manifest
from test_workspace_partitions import AGGREGATIONS, owner_workspace


def duplicate_pipeline(c, source, destination, name, config):
    pipe = dict(zip([r[1] for r in c.execute("PRAGMA table_info(pipelines)")],
                    c.execute("SELECT * FROM pipelines WHERE pipeline_id=?", (source,)).fetchone(), strict=True))
    pipe.update(pipeline_id=destination, name=name, expanded_config=json.dumps(config), generator_choices="[]")
    c.execute(f"INSERT INTO pipelines ({','.join(pipe)}) VALUES ({','.join('?' for _ in pipe)})", tuple(pipe.values()))
    columns = [r[1] for r in c.execute("PRAGMA table_info(predictions)")]
    for row in c.execute("SELECT * FROM predictions WHERE pipeline_id=?", (source,)).fetchall():
        prediction = dict(zip(columns, row, strict=True))
        prediction.update(pipeline_id=destination, prediction_id=destination + prediction["prediction_id"])
        c.execute(f"INSERT INTO predictions ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                  tuple(prediction.values()))


@pytest.mark.parametrize("generator", ["_or_", "_range_"])
def test_default_engine_collision_refuses_before_first_yield_or_ingest(tmp_path, generator):
    root = owner_workspace(tmp_path / "workspace", arrays=False)
    if generator == "_or_":
        template = [{"_or_": [{"class": "synthetic.SNV"}, {"class": "synthetic.MSC"}]}]
    else:
        template = [{"model": {"class": "sklearn.cross_decomposition.PLSRegression"},
                     "params": {"n_components": {"_range_": [1, 3, 1]}}}]
    with sqlite3.connect(root / "store.sqlite") as c:
        # An unrelated valid pipeline comes first, to catch partial-iterator ingestion.
        duplicate_pipeline(c, "p", "gen1", "gen_p0_7f3de883", template)
        duplicate_pipeline(c, "p", "gen2", "gen_p0_7b855728", template)
    with ArenaStore(tmp_path / "destination") as store:
        with pytest.raises(WorkspaceAdapterError) as refusal:
            for export in WorkspaceAdapter(root).iter_exports():
                ingest_export(store, declare_synthetic_safety(export))
        assert refusal.value.code == "workspace_unresolved_generator"
        assert all(store.count(t) == 0 for t in ("executions", "ingestion_batches", "pipeline_dags", "run_conditions"))
        assert not list(store.exports_dir.iterdir())
    cli_store = tmp_path / "cli-store"
    result = CliRunner().invoke(app, ["ingest-workspace", str(root), "--store", str(cli_store)])
    assert result.exit_code == 2 and "workspace_unresolved_generator" in result.output
    assert not cli_store.exists()


@pytest.mark.parametrize("generator", ["_or_", "_range_"])
def test_legacy_resolved_variant_shapes_keep_distinct_identities(tmp_path, generator):
    root = owner_workspace(tmp_path / "workspace", arrays=True)
    if generator == "_or_":
        a, b = [{"class": "synthetic.SNV"}], [{"class": "synthetic.MSC"}]
    else:
        a, b = [{"model": "PLS", "params": {"n_components": 1}}], [{"model": "PLS", "params": {"n_components": 3}}]
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE pipelines SET expanded_config=?,name='variant-a',generator_choices='[]'", (json.dumps(a),))
        duplicate_pipeline(c, "p", "other", "variant-b", b)
    import pyarrow as pa
    import pyarrow.parquet as pq
    path=root/"arrays/owner-fixture.parquet"
    rows=pq.read_table(path).to_pylist()
    pq.write_table(pa.Table.from_pylist([*rows,*[{**row,"prediction_id":"other"+row["prediction_id"]} for row in rows]]),path)
    exports = workspace_to_exports(root)
    assert len(exports) == len({resolve_identities(e).pipeline_dag_hash for e in exports}) == 2
    with ArenaStore(tmp_path / "store") as store:
        assert all(ingest_export(store, declare_synthetic_safety(e)).status == "committed" for e in exports)
        assert len(Queries(store).leaderboard()["rows"]) == 2


def aggregate_workspace(root):
    root = owner_workspace(root)
    with sqlite3.connect(root / "store.sqlite") as c:
        columns = [r[1] for r in c.execute("PRAGMA table_info(predictions)")]
        base = dict(zip(columns, c.execute("SELECT * FROM predictions WHERE prediction_id='final-test'").fetchone(), strict=True))
        cases = [("final_agg", "train", 0.2), ("final_agg", "test", 0.8),
                 ("avg", "val", 0.3), ("w_avg", "val", 0.7), ("all", "val", 0.9), ("avg_agg", "val", 0.4)]
        for label, partition, value in cases:
            row = {**base, "prediction_id": label + partition, "fold_id": label, "partition": partition,
                   "scores": json.dumps({partition: {"rmse": value}})}
            c.execute(f"INSERT INTO predictions ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                      tuple(row.values()))
    return root


@pytest.mark.parametrize("method,kwargs,values", AGGREGATIONS)
def test_all_query_paths_keep_source_aggregates_separate(tmp_path, method, kwargs, values):
    export, = workspace_to_exports(aggregate_workspace(tmp_path / "workspace"))
    assert {(o.source_fold_id, o.aggregation_level) for o in export.scores.observations if o.source_fold_id} == {
        ("final", "sample"), ("final_agg", "group"), ("avg", "sample"), ("w_avg", "sample"),
        ("all", "sample"), ("avg_agg", "group")}
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed"
        q = Queries(store)
        aggregate = getattr(q, method)
        assert set(values(aggregate(**kwargs))) == {1.0}
        assert set(values(aggregate(**kwargs, scope="refit", partition="test"))) == {0.5}
        assert set(values(aggregate(**kwargs, scope="refit", partition="test", aggregation_level="group",
                                    source_fold_id="final_agg"))) == {0.8}
        with pytest.raises(ValueError, match="source_fold_selection_required_for_view"):
            aggregate(**kwargs, scope="view")
        for label, value in (("avg", 0.3), ("w_avg", 0.7), ("all", 0.9)):
            assert set(values(aggregate(**kwargs, scope="view", source_fold_id=label))) == {value}
        with pytest.raises(ValueError, match="aggregate_level_selection_required"):
            aggregate(**kwargs, scope="refit", partition="test", source_fold_id="final_agg")
        scores = q.run_detail(result.execution_hash)["scores"]
        assert len(scores) == len(export.scores.observations) == 18
        assert {s["source_fold_id"] for s in scores} >= {"final", "final_agg", "avg", "w_avg", "all"}
        assert ingest_export(store, declare_synthetic_safety(export)).status == "already_ingested"


def test_aggregate_cli_and_quote_safe_selection(tmp_path):
    export, = workspace_to_exports(aggregate_workspace(tmp_path / "workspace"))
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        assert ingest_export(store, declare_synthetic_safety(export)).status == "committed"
        assert Queries(store).leaderboard(scope="view", source_fold_id="avg' OR 1=1 --")["rows"] == []
    runner = CliRunner()
    absent = runner.invoke(app, ["leaderboard", "--store", str(root), "--scope", "view"])
    assert absent.exit_code == 2 and "source_fold_selection_required_for_view" in absent.output
    selected = runner.invoke(app, ["leaderboard", "--store", str(root), "--scope", "refit", "--partition", "test",
                                    "--aggregation-level", "group", "--source-fold-id", "final_agg"])
    assert selected.exit_code == 0 and "0.8000" in selected.output


def test_owner_classification_is_typed_and_never_yields_a_prefix(tmp_path):
    root = owner_workspace(tmp_path / "workspace", arrays=False)
    with sqlite3.connect(root / "store.sqlite") as c:
        duplicate_pipeline(c, "p", "classification", "classification", [{"model": "Classifier"}])
        c.execute("UPDATE predictions SET task_type='classification' WHERE pipeline_id='classification'")
    with pytest.raises(WorkspaceAdapterError) as refusal:
        next(WorkspaceAdapter(root).iter_exports())
    assert refusal.value.code == "workspace_classification_task_unsupported"


def test_unknown_qualified_direction_stays_null_through_export_and_reads(tmp_path):
    root = owner_workspace(tmp_path / "workspace", arrays=False)
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE predictions SET scores=?", (json.dumps({"r2:y": 0.9}),))
    export, = workspace_to_exports(root)
    assert all(o.direction is None and o.direction_source == "unreported" for o in export.scores.observations)
    raw = export.to_manifest()
    assert ArenaRunExport.model_validate(raw).to_manifest() == raw
    assert validate_manifest(raw) == [] and validate_metadata(export).status == "metadata_valid"
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed"
        assert all(s["direction"] is None for s in Queries(store).run_detail(result.execution_hash)["scores"])
        with pytest.raises(ValueError, match="metric_direction_unreported_or_conflicting"):
            Queries(store).leaderboard(metric="r2:y")


def test_native_and_dagml_report_context_survives_and_recomputation_refuses(tmp_path):
    native = manifest()
    native["arena_export_schema_version"] = 2
    native["scores"]["observations"] = [{"metric_name": "rmse", "metric_value": 0.8,
                                         "scope": "refit", "partition": "test", "aggregation_level": "group",
                                         "source_fold_id": "final_agg", "fold_id":None, "direction":"min", "direction_source":"registry"}]
    native["residuals"] = {"key":"sample_id", "publishable":dict.fromkeys(("y_true","y_pred","residual","y_proba"),True),
                           "inline": [{"sample_id": "synthetic", "y_true": 1.0, "y_pred": 0.0, "scope":"refit","partition":"test"}]}
    reports = [{"variant_label":"selected", "scope": "refit", "partition": "test", "fold_id": "final_agg",
                "aggregation_level": "group", "metrics": {"rmse": 0.8}}]
    bundle = bundle_to_export({"bundle_id": "synthetic","selected_variant_id":"selected"}, owner_export=native, metric_reports=reports)
    assert bundle.scores.observations[0].source_fold_id == "final_agg"
    assert bundle.scores.observations[0].aggregation_level == "group"
    with ArenaStore(tmp_path / "store") as store:
        refused = ingest_export(store, native, policy=IngestionPolicy(recompute_scores=True))
        assert refused.status == "rejected" and refused.issues[0]["code"] == "recompute_score_context_unsupported"
        assert store.count("executions") == 0
        assert ingest_export(store, native).status == "committed"
        assert Queries(store).leaderboard(scope="refit", partition="test")["rows"] == []
        assert Queries(store).leaderboard(scope="refit", partition="test", aggregation_level="group",
                                         source_fold_id="final_agg")["rows"][0]["mean"] == 0.8
    del reports[0]["aggregation_level"]
    with pytest.raises(ValueError, match="dagml_aggregate_context_required"):
        bundle_to_export({"bundle_id": "synthetic","selected_variant_id":"selected"}, owner_export=native, metric_reports=reports)
