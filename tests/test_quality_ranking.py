"""Controlled cohort assembly and native/service/static artifact delivery."""

from __future__ import annotations

import json
import os
from itertools import pairwise

import pytest
from fastapi.testclient import TestClient

from nirs4all_benchmarks.ranking import RankingRequest, quality_ranking
from nirs4all_benchmarks.service import create_app
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore


def add(
    store,
    dataset,
    pipeline,
    value,
    *,
    task=None,
    split="s",
    cv="cv",
    scope="cv",
    partition="validation",
    status="ok",
    validity="valid",
    protocol="protocol",
    actual=True,
    operator=None,
    params=None,
    fold=None,
    execution=None,
    direction="min",
    metric="rmse",
    variant=None,
    role="model",
):
    task = task or f"task-{dataset}"
    variant = variant or f"variant-{dataset}"
    execution = execution or f"exec-{dataset}-{pipeline}-{split}-{cv}-{scope}-{partition}-{fold}"
    stamp = "2026-11-04T00:00:00Z"
    store.upsert("dataset_fingerprints", {"dataset_fingerprint": dataset, "created_at": stamp})
    store.upsert("task_specs", {"task_hash": task, "dataset_fingerprint": dataset, "created_at": stamp})
    store.upsert(
        "dataset_variants",
        {"dataset_variant_hash": variant, "dataset_fingerprint": dataset, "task_hash": task, "created_at": stamp},
    )
    store.upsert(
        "pipeline_dags",
        {
            "pipeline_dag_hash": pipeline,
            "graph_json": "{}",
            "human_label": pipeline,
            "is_linear": 1,
            "created_at": stamp,
        },
    )
    store.upsert(
        "pipeline_nodes",
        {
            "pipeline_dag_hash": pipeline,
            "node_id": "m",
            "node_signature": pipeline,
            "operator": operator or f"models.{pipeline}",
            "operator_version": "1",
            "role": role,
            "params_json": json.dumps(params or {}),
        },
    )
    store.upsert("split_specs", {"split_spec_hash": "spec-" + split})
    store.upsert(
        "split_instances",
        {
            "split_instance_hash": split,
            "split_spec_hash": "spec-" + split,
            "split_indices_hash": "actual-" + split if actual else None,
        },
    )
    store.upsert(
        "cv_instances", {"cv_instance_hash": cv, "engine_fold_set_fingerprint": "actual-" + cv if actual else None}
    )
    store.upsert("refit_strategies", {"refit_strategy_hash": "refit"})
    store.upsert("score_computation_specs", {"score_computation_hash": protocol, "created_at": stamp})
    rc = execution + "-condition"
    store.upsert(
        "run_conditions",
        {
            "run_condition_hash": rc,
            "dataset_fingerprint": dataset,
            "task_hash": task,
            "pipeline_dag_hash": pipeline,
            "dataset_variant_hash": variant,
            "split_instance_hash": split,
            "cv_instance_hash": cv,
            "refit_strategy_hash": "refit",
            "created_at": stamp,
        },
    )
    store.upsert(
        "executions",
        {
            "execution_hash": execution,
            "run_condition_hash": rc,
            "status": status,
            "validity_status": validity,
            "created_at": stamp,
        },
    )
    store.upsert(
        "score_sets",
        {
            "score_set_id": execution + "-score",
            "execution_hash": execution,
            "score_computation_hash": protocol,
            "scope": scope,
            "created_at": stamp,
        },
    )
    store.conn.execute(
        "INSERT INTO metric_observations(score_set_id,metric_name,metric_value,direction,partition,fold_id,aggregation_level) VALUES(?,?,?,?,?,?,?)",
        (execution + "-score", metric, value, direction, partition, fold, "sample"),
    )
    store.conn.commit()


class Capture:
    def __call__(self, scores, **kwargs):
        self.scores, self.kwargs = scores, kwargs
        return {
            "schema_version": "n4m.ranking/1",
            "candidates": [
                {
                    "index": i,
                    "rank": i + 1,
                    "score": 1.0,
                    "component": 0,
                    "uncertainty": None,
                    "average_rank": 1.5,
                    "observations": 1,
                    "matches": 1,
                    "wins": 1,
                    "ties": 0,
                    "losses": 0,
                }
                for i in range(len(scores[0]))
            ],
            "coverage": {"blocks": len(scores), "connected_components": 1},
            "convergence": {"converged": True},
            "uncertainty_kind": None,
            "score_direction": "higher_is_better",
            "inference": {"friedman_p_value": 0.01},
            "warnings": [],
        }


def test_transitive_partial_cohort_and_dataset_trial_balance(tmp_path):
    with ArenaStore(tmp_path) as store:
        add(store, "D1", "A", 1)
        add(store, "D1", "B", 2)
        add(store, "D2", "B", 500)
        add(store, "D2", "C", 600)
        capture = Capture()
        result = quality_ranking(store, ranker=capture)
        assert result["state"] == "ready"
        assert capture.kwargs["present"] == [[1, 1, 0], [0, 1, 1]]
        assert capture.scores == [[1, 2, 0], [0, 500, 600]]
        for i in range(20):
            add(store, "D1", "A", 1, execution=f"repeat-{i}")
        after = quality_ranking(store, ranker=capture)
        assert capture.scores == [[1, 2, 0], [0, 500, 600]]
        assert after["provenance"]["comparison_rows"] == 2
        assert result["inference"]["friedman_p_value"] is None


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"split": "different"}, "no_matched_comparisons"),
        ({"cv": "different"}, "no_matched_comparisons"),
        ({"task": "other"}, "ambiguous_dataset_context_requires_filter"),
        ({"variant": "other"}, "ambiguous_dataset_context_requires_filter"),
        ({"protocol": "other"}, "ambiguous_protocol_requires_filter"),
        ({"actual": False, "split": "missing"}, "no_matched_comparisons"),
        ({"status": "unreported"}, "no_matched_comparisons"),
        ({"status": "failed"}, "no_matched_comparisons"),
        ({"validity": "invalidated"}, "no_matched_comparisons"),
        ({"scope": "refit", "partition": "test"}, "no_matched_comparisons"),
        ({"direction": "max"}, "no_matched_comparisons"),
    ],
)
def test_identity_refusals(tmp_path, change, reason):
    with ArenaStore(tmp_path) as store:
        add(store, "D", "A", 1)
        add(store, "D", "B", 2, **change)
        result = quality_ranking(store, ranker=Capture())
        assert result["state"] == "unavailable"
        assert result["reason"] == reason


def test_average_ranks_use_genuine_dataset_panels(tmp_path):
    with ArenaStore(tmp_path) as store:
        for ds in ("D1", "D2"):
            for candidate, value in (("A", 1), ("B", 2), ("C", 3)):
                add(store, ds, candidate, value)
        capture = Capture()
        result = quality_ranking(store, {"method": "average_rank"}, ranker=capture)
        assert capture.scores == [[1, 2, 3], [1, 2, 3]]
        assert result["config"]["aggregation"] == "dataset_panel_mean"
        assert result["inference"]["friedman_p_value"] == 0.01
        add(store, "D1", "A", 2, split="repeat")
        assert (
            quality_ranking(store, {"method": "average_rank"}, ranker=capture)["reason"]
            == "average_rank_requires_common_dataset_panel"
        )


def test_parameters_qualified_and_models_pool_configs(tmp_path):
    with ArenaStore(tmp_path) as store:
        add(store, "D", "A", 1, operator="models.PLS", params={"n_components": 5})
        add(store, "D", "B", 2, operator="models.PLS", params={"n_components": 10})
        add(store, "D", "C", 3, operator="models.PCA", params={"n_components": 5})
        capture = Capture()
        result = quality_ranking(store, {"dimension": "parameter"}, ranker=capture)
        assert result["reason"] == "operator_qualified_parameter_required"
        result = quality_ranking(
            store, {"dimension": "parameter", "operator": "models.PLS", "parameter": "n_components"}, ranker=capture
        )
        assert len(result["candidates"]) == 2
        assert all("models.PLS@1::n_components=" in c["id"] for c in result["candidates"])
        result = quality_ranking(store, {"dimension": "model"}, ranker=capture)
        assert len(result["candidates"]) == 2
        assert capture.scores == [[3.0, 1.5]]


def test_native_unavailable_rejected_deferred_and_direction(tmp_path, monkeypatch):
    import nirs4all_benchmarks.ranking.quality as quality

    with ArenaStore(tmp_path) as store:
        add(store, "D", "A", 1)
        add(store, "D", "B", 2)

        def unavailable():
            raise ImportError("not installed")

        monkeypatch.setattr(quality, "_native_rank", unavailable)
        assert quality_ranking(store)["reason"] == "native_ranking_runtime_unavailable"
        assert quality_ranking(store, {"method": "mixed_effects"})["reason"] == "mixed_effects_backend_not_qualified"
        assert quality_ranking(store, {"direction": "max"})["reason"] == "metric_direction_mismatch"
        assert quality_ranking(store, {"dimension": "split"})["reason"] == "split_requires_matched_external_error"

        def reject(*args, **kwargs):
            raise ValueError("invalid option")

        assert quality_ranking(store, ranker=reject)["reason"] == "native_ranking_rejected_input"
        assert (
            quality_ranking(store, ranker=lambda *a, **k: {"schema_version": "old"})["reason"]
            == "native_ranking_schema_unsupported"
        )


def test_service_static_native_artifact_parity(tmp_path, monkeypatch):
    import nirs4all_benchmarks.ranking.quality as quality

    root = tmp_path / "store"
    with ArenaStore(root) as store:
        add(store, "D", "A", 1, status="completed")
        add(store, "D", "B", 2)
    monkeypatch.setattr(quality, "_native_rank", lambda: Capture())
    client = TestClient(create_app(root))
    live = client.get("/api/quality-ranking").json()
    assert live["state"] == "ready", live
    assert client.post("/api/quality-ranking", json={}).json() == live
    assert client.get("/api/quality-ranking", params={"scope": "invalid"}).status_code == 422
    assert client.post("/api/quality-ranking", json={"mystery": 1}).status_code == 422
    build_site(root, tmp_path / "site", ranking_requests=[{}])
    bundle = json.loads((tmp_path / "site/data/bundle.json").read_text())
    assert bundle["quality_rankings"] == [live]
    assert RankingRequest().identity == live["request_id"]


def test_native_end_to_end_partial_and_disconnected(tmp_path):
    if not os.environ.get("N4M_LIB_PATH"):
        pytest.skip("native ranking runtime opt-in via N4M_LIB_PATH")
    with ArenaStore(tmp_path) as store:
        add(store, "D1", "A", 1)
        add(store, "D1", "B", 2)
        add(store, "D2", "B", 20)
        add(store, "D2", "C", 30)
        for method in ("elo", "bradley_terry", "trueskill", "openskill", "average_rank"):
            result = quality_ranking(store, {"method": method})
            assert result["state"] == "ready", result
            assert result["coverage"]["connected_components"] == 1
            assert all(c["rank"] is not None for c in result["candidates"])
        add(store, "D3", "X", 1)
        add(store, "D3", "Y", 2)
        result = quality_ranking(store)
        assert result["state"] == "ready", result
        assert result["coverage"]["connected_components"] == 2
        assert all(c["rank"] is None for c in result["candidates"])


def test_static_exact_config_lookup_and_explicit_unavailable(tmp_path, monkeypatch):
    import shutil
    import subprocess
    from pathlib import Path

    import nirs4all_benchmarks.ranking.quality as quality

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required")
    major = int(
        subprocess.run(
            [node, "-p", "process.versions.node.split('.')[0]"], capture_output=True, text=True, check=True
        ).stdout
    )
    if major < 18:
        pytest.skip("Node.js 18 or newer is required")
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        add(store, "D", "A", 1)
        add(store, "D", "B", 2)
    monkeypatch.setattr(quality, "_native_rank", lambda: Capture())
    build_site(root, tmp_path / "site", ranking_requests=[{}])
    engine = Path(__file__).parents[1] / "src/nirs4all_benchmarks/web/lib/static-engine.js"
    (tmp_path / "engine.mjs").write_text(engine.read_text())
    (tmp_path / "driver.mjs").write_text("""
import { readFileSync } from "node:fs";
import { createStaticApi } from "./engine.mjs";
globalThis.fetch = async () => ({ok: true, json: async () => JSON.parse(readFileSync(process.argv[2], "utf8"))});
const api = await createStaticApi();
console.log(JSON.stringify(await Promise.all([
  api.qualityRanking(), api.qualityRanking({dataset: "different"}),
  api.qualityRanking({options: {k_factor: 4}}), api.qualityRanking({method: "elo"}),
  api.qualityRanking({partition: "test"}), api.qualityRanking({scope: "test"})])));
""")
    result = subprocess.run(
        [node, str(tmp_path / "driver.mjs"), str(tmp_path / "site/data/bundle.json")],
        text=True,
        capture_output=True,
        check=True,
    )
    results = json.loads(result.stdout)
    expected = json.loads((tmp_path / "site/data/bundle.json").read_text())["quality_rankings"][0]
    assert results[0] == expected
    assert all(r["reason"] == "static_ranking_request_not_precomputed" and not r["candidates"] for r in results[1:])


def test_matched_parameter_holds_remaining_configuration_fixed(tmp_path):
    with ArenaStore(tmp_path) as store:
        for pipe, components, extra in (("A", 5, {}), ("B", 10, {}), ("C", 20, {"scale": False})):
            params = {"n_components": components, **extra}
            add(store, "D", pipe, components, operator="models.PLS", params=params)
            graph = {
                "nodes": [
                    {
                        "node_id": "m",
                        "kind": "model",
                        "operator": "models.PLS",
                        "operator_version": "1",
                        "params": params,
                    }
                ],
                "edges": [],
            }
            store.conn.execute(
                "UPDATE pipeline_dags SET graph_json=? WHERE pipeline_dag_hash=?", (json.dumps(graph), pipe)
            )
        capture = Capture()
        result = quality_ranking(
            store,
            {
                "dimension": "parameter",
                "operator": "models.PLS",
                "parameter": "n_components",
                "comparison_mode": "matched",
            },
            ranker=capture,
        )
        assert result["state"] == "ready", result
        assert result["config"]["comparison_mode"] == "matched"
        assert len(capture.scores) == 1 and sum(capture.kwargs["present"][0]) == 2
        assert len(result["provenance"]["comparisons"]) == 1
        assert all("rest_of_pipeline_fingerprint" in b for b in result["provenance"]["blocks"])
        assert "causal" in " ".join(result["warnings"])
        bad = quality_ranking(store, {"comparison_mode": "matched"}, ranker=capture)
        assert bad["reason"] == "matched_mode_requires_component_dimension"


def test_matched_operator_compares_selected_presence_with_absence(tmp_path):
    with ArenaStore(tmp_path) as store:
        for pipe, snv, msc_params in (("A", True, {}), ("B", False, {}), ("C", False, {"reference": "changed"})):
            add(store, "D", pipe, 1 if snv else 2, operator="models.PLS")
            graph_nodes = []
            if snv:
                graph_nodes.append(
                    {"node_id": "s", "kind": "transform", "operator": "prep.SNV", "operator_version": "1", "params": {}}
                )
            graph_nodes += [
                {
                    "node_id": "p",
                    "kind": "transform",
                    "operator": "prep.MSC",
                    "operator_version": "1",
                    "params": msc_params,
                },
                {"node_id": "m", "kind": "model", "operator": "models.PLS", "operator_version": "1", "params": {}},
            ]
            edges = [{"src": a["node_id"], "dst": b["node_id"]} for a, b in pairwise(graph_nodes)]
            for node in graph_nodes[:-1]:
                store.upsert(
                    "pipeline_nodes",
                    {
                        "pipeline_dag_hash": pipe,
                        "node_id": node["node_id"],
                        "node_signature": pipe + node["node_id"],
                        "role": "transform",
                        "operator": node["operator"],
                        "operator_version": "1",
                        "params_json": json.dumps(node["params"]),
                    },
                )
            for edge in edges:
                store.upsert("pipeline_edges", {"pipeline_dag_hash": pipe, **edge})
            store.conn.execute(
                "UPDATE pipeline_dags SET graph_json=? WHERE pipeline_dag_hash=?",
                (json.dumps({"nodes": graph_nodes, "edges": edges}), pipe),
            )
        capture = Capture()
        req = {"dimension": "preprocessing_operator", "operator": "prep.SNV", "comparison_mode": "matched"}
        result = quality_ranking(store, req, ranker=capture)
        assert result["state"] == "ready", result
        assert len(capture.scores) == 1
        comparison = result["provenance"]["comparisons"][0]
        source_block = next(b for b in result["provenance"]["blocks"] if b["id"] == comparison["matched_blocks"][0])
        assert source_block["rest_of_pipeline_fingerprint"]
        assert {c["label"] for c in result["candidates"]} == {"With prep.SNV@1", "Without prep.SNV"}
        assert capture.scores[0] == [2, 1]
