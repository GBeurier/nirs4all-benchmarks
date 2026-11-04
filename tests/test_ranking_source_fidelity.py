"""Scientific source semantics survive real ingestion before matched ranking."""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.identity.pipeline_dag import (
    GRAPH_NORMALIZATION_VERSION,
    compute_pipeline_dag_hash,
)
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.ranking import quality_ranking
from nirs4all_benchmarks.store import ArenaStore
from test_quality_ranking import Capture


def _exports():
    first = generate_fixture_exports()[0]
    first.split.split_indices_hash = "controlled-actual-holdout"
    second = first.model_copy(deep=True)
    second.execution.execution_id += "-different-model"
    second.pipeline.graph["nodes"][-1]["operator"] = "sklearn.linear_model.Ridge"
    second.pipeline.human_label = "Different model"
    return first, second


def _ingest(store, exports):
    for export in exports:
        result = ingest_export(store, export, policy=IngestionPolicy(collection_id="source-fidelity"))
        assert result.status == "committed"
        assert result.validity_status == "valid"


@pytest.mark.parametrize("field", ["graph", "node", "edge", "source_id"])
def test_real_ingestion_preserves_unselected_source_constraints(tmp_path, field):
    first, second = _exports()
    for export, value in ((first, "first"), (second, "second")):
        graph = export.pipeline.graph
        if field == "graph":
            graph["execution_semantics"] = value
        elif field == "node":
            graph["nodes"][0]["scientific_option"] = {"precision": value}
        elif field == "edge":
            graph["edges"][0]["sample_policy"] = value
        else:
            graph["nodes"][0]["source_id"] = value
    with ArenaStore(tmp_path) as store:
        _ingest(store, (first, second))
        graphs = [json.loads(row["graph_json"]) for row in store.query("SELECT graph_json FROM pipeline_dags")]
        assert len(graphs) == 2
        assert all(graph["arena_normalization_version"] == GRAPH_NORMALIZATION_VERSION for graph in graphs)
        if field == "graph":
            values = [graph["execution_semantics"] for graph in graphs]
        elif field == "node":
            values = [graph["nodes"][0]["scientific_option"]["precision"] for graph in graphs]
        elif field == "edge":
            values = [graph["edges"][0]["sample_policy"] for graph in graphs]
        else:
            values = [graph["nodes"][0]["source_id"] for graph in graphs]
            assert {
                row["source_id"] for row in store.query("SELECT source_id FROM pipeline_nodes") if row["source_id"]
            } == {"first", "second"}
        assert set(values) == {"first", "second"}
        artifact = quality_ranking(store, {"dimension": "model", "comparison_mode": "matched"}, ranker=Capture())
        assert artifact["state"] == "unavailable"
        assert artifact["reason"] == "no_matched_comparisons"


def test_real_ingestion_qualifies_identical_rest_and_refuses_historical_lossy_graphs(tmp_path):
    with ArenaStore(tmp_path) as store:
        _ingest(store, _exports())
        request = {"dimension": "model", "comparison_mode": "matched"}
        assert quality_ranking(store, request, ranker=Capture())["state"] == "ready"
        for row in store.query("SELECT pipeline_dag_hash, graph_json FROM pipeline_dags"):
            graph = json.loads(row["graph_json"])
            del graph["arena_normalization_version"]
            store.update(
                "pipeline_dags", "pipeline_dag_hash", row["pipeline_dag_hash"], {"graph_json": json.dumps(graph)}
            )
        artifact = quality_ranking(store, request, ranker=Capture())
        assert artifact["state"] == "unavailable"
        assert artifact["provenance"]["excluded"]["matched_graph_source_requires_reingestion"] > 0
        assert quality_ranking(store, {"dimension": "model"}, ranker=Capture())["state"] == "ready"


@pytest.mark.parametrize("field", ["graph", "node", "edge", "source_id"])
def test_semantic_extensions_change_hash_and_normalization_is_idempotent(field):
    graph = {
        "nodes": [{"id": "p", "operator": "nirs4all.SNV"}, {"id": "m", "kind": "model", "operator": "Ridge"}],
        "edges": [{"src": "p", "dst": "m"}],
    }
    changed = deepcopy(graph)
    if field == "graph":
        changed["execution_semantics"] = "different"
    elif field == "node":
        changed["nodes"][0]["scientific_option"] = {"precision": "f32"}
    elif field == "edge":
        changed["edges"][0]["sample_policy"] = "different"
    else:
        changed["nodes"][0]["source_id"] = "NIR"
    baseline = compute_pipeline_dag_hash(graph)
    result = compute_pipeline_dag_hash(changed)
    assert baseline.pipeline_dag_hash != result.pipeline_dag_hash
    assert compute_pipeline_dag_hash(result.normalized_graph).pipeline_dag_hash == result.pipeline_dag_hash


def test_existing_hash_and_equivalent_syntax_remain_stable():
    graph = {
        "nodes": [
            {"id": "p", "kind": "transform", "operator": "nirs4all.SNV"},
            {"id": "m", "kind": "model", "operator": "sklearn.linear_model.Ridge", "params": {"alpha": 1}},
        ],
        "edges": [{"src": "p", "dst": "m"}],
    }
    # Captured from the previous normalizer: no-extension identities must remain stable.
    expected = "0e32b270e09176211fcf76ba26d893d312a4dd542aa89af8b46c4d6eeec2917c"
    assert compute_pipeline_dag_hash(graph).pipeline_dag_hash == expected
    nullable_source = deepcopy(graph)
    nullable_source["nodes"][0]["source_id"] = None
    assert compute_pipeline_dag_hash(nullable_source).pipeline_dag_hash == expected
    aliased = {
        "id": "display-graph-id",
        "label": "Display title",
        "nodes": [
            {"node_id": "a", "role": "transform", "class": "nirs4all.SNV", "label": "Display stage"},
            {"node_id": "b", "type": "model", "model": {"class": "sklearn.linear_model.Ridge", "params": {"alpha": 1}}},
        ],
        "edges": [{"from_node": "a", "to_node": "b", "label": "Display edge"}],
    }
    assert compute_pipeline_dag_hash(aliased).pipeline_dag_hash == expected


def test_nested_operator_extensions_are_explicitly_refused():
    with pytest.raises(ValueError, match="unsupported nested operator fields: scientific_option"):
        compute_pipeline_dag_hash([{"model": {"class": "Ridge", "scientific_option": "different"}}])
