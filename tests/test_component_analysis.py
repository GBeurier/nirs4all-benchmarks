"""Original-observation qualification, designs and exact analysis artifacts."""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from nirs4all_benchmarks.identity.pipeline_dag import normalize_graph_spec
from nirs4all_benchmarks.ranking import ComponentAnalysisRequest, component_analysis
from nirs4all_benchmarks.service import create_app
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore
from test_quality_ranking import add


def populate(store, *, metric="nrmse"):
    for dataset in ("D1", "D2", "D3"):
        for split in ("s1", "s2"):
            for pipeline, value in (("A", 0.2), ("B", 0.3)):
                add(store, dataset, pipeline, value, split=split, metric=metric)
    for pipeline in ("A", "B"):
        graph = normalize_graph_spec([{"kind": "model", "operator": f"models.{pipeline}", "operator_version": "1"}])[0]
        store.update("pipeline_dags", "pipeline_dag_hash", pipeline, {"graph_json": json.dumps(graph)})
    store.conn.commit()


def request(**extra):
    return {"factors": [{"name": "model", "dimension": "model", "reference": "models.A@1"}], **extra}


class MixedCapture:
    def __call__(self, X, y, groups, **options):
        self.X, self.y, self.groups, self.options = X, y, groups, options
        p = len(X[0])
        return {
            "schema_version": "n4m.mixed_effects/1",
            "state": "ready",
            "reason": None,
            "beta": [0.2] * p,
            "standard_errors": [0.01] * p,
            "covariance": [[0.0001 if i == j else 0.0 for j in range(p)] for i in range(p)],
            "variances": [0.01, 0.02, 0.03],
            "diagnostics": {"converged": True, "fit_state": "fitted"},
            "warnings": [],
            "options": options,
        }


class FanovaCapture:
    def __call__(self, X, y, **options):
        self.X, self.y, self.options = X, y, options
        return {
            "schema_version": "n4m.fanova/1",
            "state": "ready",
            "reason": None,
            "feature_importance": [{"index": i, "fraction": 1.0 / len(X[0]), "tree_sd": 0.1} for i in range(len(X[0]))],
            "interactions": [],
            "diagnostics": {
                "trees": 32,
                "informative_trees": 32,
                "total_variance_mean": 0.3,
                "remaining_fraction": 0.0,
                "train_rmse": 0.1,
                "validation_rmse": 0.2,
                "validation_r2": -0.5,
            },
            "domain_measure": "uniform_product",
            "aggregation": "mean_per_tree_normalized_components",
            "uncertainty_kind": "tree_dispersion_not_confidence",
            "options": options,
            "warnings": [],
        }


def test_original_rows_and_explicit_references_reach_native(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        capture = MixedCapture()
        result = component_analysis(store, request(), estimator=capture)
        assert result["state"] == "ready", result
        assert len(capture.y) == 12 and capture.y == [0.2, 0.3] * 6
        assert capture.X == [[1.0, 0.0], [1.0, 1.0]] * 6
        assert len({row[0] for row in capture.groups}) == 3
        assert len({row[1] for row in capture.groups}) == 6
        assert result["design"]["references"] == {"model": "models.A@1"}
        assert result["design"]["columns"][1]["level"] == "models.B@1"
        assert len(result["provenance"]["observations"]) == 12
        assert all("task" in row and "block" in row for row in result["provenance"]["observations"])


def populate_level_blocks(store, layout):
    """Add two executions per dataset/split block for ``layout[dataset] = [(pipeline_id, operator, alpha), ...]``."""
    count = 0
    for dataset, pipelines in layout.items():
        for split in ("s1", "s2"):
            for pipeline, _, _ in pipelines:
                for repeat in ("e1", "e2"):
                    count += 1
                    add(
                        store,
                        dataset,
                        pipeline,
                        0.2 + 0.001 * count,
                        split=split,
                        metric="nrmse",
                        execution=f"{repeat}-{dataset}-{pipeline}-{split}",
                    )
    for pipelines in layout.values():
        for pipeline, operator, alpha in pipelines:
            graph = normalize_graph_spec(
                [{"kind": "model", "operator": operator, "operator_version": "1", "params": {"alpha": alpha}}]
            )[0]
            store.update("pipeline_dags", "pipeline_dag_hash", pipeline, {"graph_json": json.dumps(graph)})
    store.conn.commit()


def test_mixed_effects_refuses_levels_never_sharing_a_block_before_compute(tmp_path):
    with ArenaStore(tmp_path) as store:
        # A is observed only on D1/D2 and B only on D3/D4: beta_B would be a pure between-dataset contrast.
        populate_level_blocks(
            store,
            {
                "D1": [("A", "models.A", 1)],
                "D2": [("A", "models.A", 1)],
                "D3": [("B", "models.B", 1)],
                "D4": [("B", "models.B", 1)],
            },
        )
        calls = []

        def estimator(*args, **kwargs):
            calls.append(args)
            return MixedCapture()(*args, **kwargs)

        result = component_analysis(store, request(), estimator=estimator)
        assert result["state"] == "unavailable"
        assert result["reason"] == "categorical_levels_not_connected_within_blocks"
        assert result["result"] is None and calls == []
        assert result["provenance"]["accepted_observations"] == 16
        assert any("not connected through shared evaluation blocks" in warning for warning in result["warnings"])


def test_mixed_effects_accepts_levels_bridged_within_blocks_by_distinct_pipelines(tmp_path):
    with ArenaStore(tmp_path) as store:
        # Full pipelines A1/B1 and A2/B2 never share a block, yet models A and B are compared in every block.
        populate_level_blocks(
            store,
            {
                "D1": [("A1", "models.A", 1), ("B1", "models.B", 1)],
                "D2": [("A1", "models.A", 1), ("B1", "models.B", 1)],
                "D3": [("A2", "models.A", 2), ("B2", "models.B", 2)],
                "D4": [("A2", "models.A", 2), ("B2", "models.B", 2)],
            },
        )
        capture = MixedCapture()
        result = component_analysis(store, request(), estimator=capture)
        assert result["state"] == "ready", result
        assert len(capture.y) == 32 and {tuple(row) for row in capture.X} == {(1.0, 0.0), (1.0, 1.0)}
        assert {row["pipeline"] for row in result["provenance"]["observations"]} == {"A1", "A2", "B1", "B2"}
        assert len({row[0] for row in capture.groups}) == 4 and len({row[1] for row in capture.groups}) == 8


def test_preview_and_missing_reference_preserve_domain_evidence(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        preview = component_analysis(store, {"preview_only": True})
        assert preview["reason"] == "design_preview_only"
        assert preview["design"]["factors"][0]["observed_levels"] == ["models.A@1", "models.B@1"]
        assert (
            component_analysis(store, {}, estimator=MixedCapture())["reason"] == "categorical_reference_level_required"
        )
        assert (
            component_analysis(store, request(metric="rmse"), estimator=MixedCapture())["reason"]
            == "raw_metric_requires_native_baseline_normalization"
        )


def test_source_split_protocol_task_and_missing_runtime_refusals(tmp_path, monkeypatch):
    module = importlib.import_module("nirs4all_benchmarks.ranking.component_analysis")
    with ArenaStore(tmp_path) as store:
        populate(store)
        monkeypatch.setattr(module, "_native", lambda: SimpleNamespace())
        assert component_analysis(store, request())["reason"] == "native_component_analysis_runtime_unavailable"
        store.conn.execute("UPDATE split_instances SET split_indices_hash=NULL")
        # A bound fold set identifies CV validation rows without an outer-split identity.
        result = component_analysis(store, request(), estimator=MixedCapture())
        assert "missing_comparison_identity" not in result["provenance"]["excluded"]
        assert result["provenance"]["fold_granularity"] == "unfolded"
        # Fold-train rows are relation-narrowed: they still need the absent outer-split identity.
        store.conn.execute("UPDATE metric_observations SET partition='train'")
        result = component_analysis(store, request(partition="train"), estimator=MixedCapture())
        assert result["reason"] == "no_eligible_original_observations"
        assert result["provenance"]["excluded"]["missing_comparison_identity"] == 12
    with ArenaStore(tmp_path / "history") as store:
        populate(store)
        for row in store.query("SELECT * FROM pipeline_dags"):
            graph = json.loads(row["graph_json"])
            del graph["arena_normalization_version"]
            store.update(
                "pipeline_dags", "pipeline_dag_hash", row["pipeline_dag_hash"], {"graph_json": json.dumps(graph)}
            )
        result = component_analysis(store, request(), estimator=MixedCapture())
        assert result["reason"] == "no_eligible_original_observations"
        assert result["provenance"]["excluded"]["analysis_graph_source_requires_reingestion"] == 12


def test_native_baseline_ratios_require_direct_original_block_pairing(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store, metric="rmse")
        calls = []

        def normalize(values, baselines):
            calls.append((values, baselines))
            return [1.0, 1.5] * 6

        capture = MixedCapture()
        req = request(metric="rmse", outcome_policy="native_baseline_ratio", baseline_pipeline_id="A")
        result = component_analysis(store, req, estimator=capture, normalizer=normalize)
        assert result["state"] == "ready", result
        assert calls == [([0.2, 0.3] * 6, [0.2] * 12)]
        assert capture.y == [1.0, 1.5] * 6
        assert len(result["provenance"]["baseline_observations"]) == 6
        store.conn.execute("UPDATE metric_observations SET metric_value=0 WHERE score_set_id LIKE 'exec-D1-A-%'")
        assert (
            component_analysis(store, req, estimator=capture, normalizer=normalize)["reason"]
            == "baseline_requires_positive_denominator_nonnegative_loss"
        )


def test_fanova_declared_domains_disjoint_datasets_and_validation(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        capture = FanovaCapture()
        req = request(
            method="fanova",
            factors=[{"name": "model", "dimension": "model", "levels": ["models.A@1", "models.B@1"]}],
            training_datasets=["D1", "D2"],
            validation_datasets=["D3"],
            options={"seed": 9, "n_trees": 32},
        )
        result = component_analysis(store, req, estimator=capture)
        assert result["state"] == "ready", result
        assert len(capture.X) == 8 and len(capture.options["X_valid"]) == 4
        assert capture.options["feature_types"] == [2]
        assert capture.options["bounds"] == [[0.0, 1.0]]
        assert capture.options["seed"] == 9
        assert result["provenance"]["training_datasets"] == ["D1", "D2"]
        assert result["result"]["diagnostics"]["validation_r2"] == -0.5
        assert (
            component_analysis(store, {**req, "validation_datasets": []}, estimator=capture)["reason"]
            == "every_dataset_requires_training_or_validation_assignment"
        )
        assert (
            component_analysis(store, {**req, "factors": [{"name": "model", "dimension": "model"}]}, estimator=capture)[
                "reason"
            ]
            == "categorical_domain_levels_required"
        )
    with pytest.raises(ValidationError, match="disjoint"):
        ComponentAnalysisRequest(training_datasets=["D1"], validation_datasets=["D1"])


def test_parameter_factors_are_model_sliced_and_never_impute_missing(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        req = request(
            factors=[
                {
                    "name": "alpha",
                    "dimension": "parameter",
                    "operator": "models.A",
                    "parameter": "alpha",
                    "kind": "numeric",
                }
            ]
        )
        assert (
            component_analysis(store, req, estimator=MixedCapture())["reason"]
            == "parameter_analysis_requires_model_slice"
        )
        result = component_analysis(store, {**req, "model_slice": "models.A@1"}, estimator=MixedCapture())
        assert result["reason"] == "incomplete_or_conditional_design_requires_slice"
        assert result["provenance"]["excluded"]["missing_or_ambiguous_parameter"] == 1


def test_service_static_and_cli_exact_analysis_snapshots(tmp_path, monkeypatch):
    module = importlib.import_module("nirs4all_benchmarks.ranking.component_analysis")
    monkeypatch.setattr(module, "_native", lambda: SimpleNamespace(mixed_effects=MixedCapture()))
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        populate(store)
    req = request()
    response = TestClient(create_app(root)).post("/api/component-analysis", json=req)
    assert response.status_code == 200
    expected = response.json()
    assert expected["state"] == "ready", expected
    # The publication gate is closed: no static snapshot or browser lookup can be built from this request.
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, tmp_path / "site", component_analysis_requests=[req])
    assert not (tmp_path / "site").exists()
    assert (
        TestClient(create_app(root)).post("/api/component-analysis", json={**req, "formula": "arbitrary()"}).status_code
        == 422
    )


def test_cli_accepts_component_requests_and_combined_snapshot_limit(tmp_path):
    from typer.testing import CliRunner

    from nirs4all_benchmarks.cli import app

    path = tmp_path / "analyses.json"
    path.write_text(json.dumps([request()]))
    result = CliRunner().invoke(
        app,
        [
            "build-site",
            "--store",
            str(tmp_path / "empty"),
            "--out",
            str(tmp_path / "site"),
            "--domain",
            "",
            "--component-analysis-requests",
            str(path),
        ],
    )
    # The request file is valid; the closed publication gate refuses the build before any output exists.
    assert result.exit_code == 1, result.output
    assert "publication_gate_closed" in f"{result.exception} {result.output}"
    assert not (tmp_path / "site").exists() and not (tmp_path / "empty").exists()
    path.write_text(json.dumps([request()] * 65))
    result = CliRunner().invoke(
        app, ["build-site", "--out", str(tmp_path / "oversize"), "--component-analysis-requests", str(path)]
    )
    assert result.exit_code == 2 and not (tmp_path / "oversize").exists()


def test_conflicting_original_rows_unsupported_options_and_nonfinite_native_results(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        assert (
            component_analysis(store, request(options={"formula": 3}), estimator=MixedCapture())["reason"]
            == "unsupported_component_analysis_option"
        )

        def corrupt(*args, **kwargs):
            result = MixedCapture()(*args, **kwargs)
            result["diagnostics"]["condition_number"] = float("inf")
            return result

        assert (
            component_analysis(store, request(), estimator=corrupt)["reason"]
            == "native_component_analysis_nonfinite_or_unserializable_result"
        )
        original = store.query("SELECT * FROM metric_observations LIMIT 1")[0]
        store.conn.execute(
            "INSERT INTO metric_observations(score_set_id,metric_name,metric_value,direction,partition,fold_id,aggregation_level) VALUES(?,?,?,?,?,?,?)",
            (original["score_set_id"], "nrmse", 0.9, "min", "validation", original["fold_id"], "sample"),
        )
        assert (
            component_analysis(store, request(), estimator=MixedCapture())["reason"]
            == "conflicting_original_metric_observations"
        )


def test_fanova_rejects_domain_levels_absent_from_training(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        req = request(
            method="fanova",
            factors=[{"name": "model", "dimension": "model", "levels": ["models.A@1", "models.B@1", "models.C@1"]}],
            training_datasets=["D1", "D2"],
            validation_datasets=["D3"],
        )
        assert (
            component_analysis(store, req, estimator=FanovaCapture())["reason"]
            == "categorical_domain_has_unobserved_training_levels"
        )


def populate_native(store):
    import numpy as np

    rng = np.random.default_rng(19)
    for dataset in range(4):
        for block in range(4):
            block_effect = rng.normal(0, 0.04)
            for model, pipeline in enumerate(("A", "B")):
                for repeat in range(3):
                    add(
                        store,
                        f"D{dataset}",
                        pipeline,
                        0.5 + dataset * 0.1 + block_effect + model * 0.2 + rng.normal(0, 0.02),
                        split=f"s{block}",
                        metric="nrmse",
                        execution=f"e{dataset}-{block}-{pipeline}-{repeat}",
                    )
    for pipeline in ("A", "B"):
        store.update(
            "pipeline_dags",
            "pipeline_dag_hash",
            pipeline,
            {
                "graph_json": json.dumps(
                    normalize_graph_spec(
                        [{"kind": "model", "operator": f"models.{pipeline}", "operator_version": "1"}]
                    )[0]
                )
            },
        )
    store.conn.commit()


def test_real_native_mixed_and_fanova_service_static_delivery(tmp_path):
    import os

    if not os.environ.get("N4M_LIB_PATH"):
        pytest.skip("native component-analysis runtime opt-in")
    root = tmp_path / "native-store"
    with ArenaStore(root) as store:
        populate_native(store)
        mixed = component_analysis(store, request())
        if mixed["reason"] in {"native_component_analysis_runtime_unavailable", "mixed_effects_backend_not_compiled"}:
            pytest.skip("native mixed-effects backend unavailable")
        assert mixed["state"] == "ready", mixed
        assert mixed["result"]["beta"][1] == pytest.approx(0.2, abs=0.02)
        assert mixed["result"]["diagnostics"]["design_rank"] == 2
        assert mixed["provenance"]["accepted_observations"] == 96
    fanova_request = request(
        method="fanova",
        factors=[{"name": "model", "dimension": "model", "levels": ["models.A@1", "models.B@1"]}],
        training_datasets=["D0", "D1", "D2"],
        validation_datasets=["D3"],
        options={"seed": 9, "n_trees": 16},
    )
    response = TestClient(create_app(root)).post("/api/component-analysis", json=fanova_request)
    assert response.status_code == 200
    result = response.json()
    assert result["state"] == "ready", result
    assert result["result"]["feature_importance"][0]["fraction"] == pytest.approx(1)
    assert result["result"]["diagnostics"]["validation_r2"] < 0
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, tmp_path / "native-site", component_analysis_requests=[request(), fanova_request])
    assert not (tmp_path / "native-site").exists()


def test_categorical_parameter_integer_and_float_have_portable_distinct_ids(tmp_path):
    with ArenaStore(tmp_path) as store:
        populate(store)
        for pipeline, value in (("A", 1), ("B", 1.0)):
            graph = normalize_graph_spec(
                [
                    {
                        "kind": "model",
                        "operator": "models.Tree",
                        "operator_version": "1",
                        "params": {"min_samples_leaf": value},
                    }
                ]
            )[0]
            store.update("pipeline_dags", "pipeline_dag_hash", pipeline, {"graph_json": json.dumps(graph)})
        factor = {
            "name": "leaf",
            "dimension": "parameter",
            "operator": "models.Tree",
            "parameter": "min_samples_leaf",
            "kind": "categorical",
        }
        req = {"model_slice": "models.Tree@1", "factors": [factor]}
        preview = component_analysis(store, {**req, "preview_only": True})
        levels = preview["design"]["factors"][0]["observed_levels"]
        assert len(levels) == 2 and all(isinstance(level, str) for level in levels)
        assert {json.loads(level)["type"] for level in levels} == {"integer", "number"}
        result = component_analysis(
            store, {**req, "factors": [{**factor, "reference": levels[0], "levels": levels}]}, estimator=MixedCapture()
        )
        assert result["state"] == "ready", result
    for seed in (2**53 + 1, 2.0, True):
        with pytest.raises(ValidationError, match="JSON-safe integer"):
            ComponentAnalysisRequest(method="fanova", options={"seed": seed})
