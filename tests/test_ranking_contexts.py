"""Matched contexts preserve the unselected scientific configuration."""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from nirs4all_benchmarks.identity.pipeline_dag import normalize_graph_spec
from nirs4all_benchmarks.ranking.contexts import build_component_contexts


def _node(operator, kind="transform", *, params=None, version="1", fit_scope="train"):
    return {
        "operator": operator,
        "kind": kind,
        "params": params or {},
        "operator_version": version,
        "fit_scope": fit_scope,
    }


def _pipe(key, steps):
    graph, _, _ = normalize_graph_spec(steps)
    return {"pipeline_dag_hash": key, "is_linear": True, "graph_json": json.dumps(graph)}


def _context(pipe, dimension="model", **kwargs):
    return build_component_contexts([pipe], dimension, **kwargs)[pipe["pipeline_dag_hash"]]


def _value(pipe, dimension="model", **kwargs):
    result = _context(pipe, dimension, **kwargs)
    assert result["excluded_reason"] is None, result
    assert result["fingerprint"] is not None
    return result["fingerprint"]


@pytest.mark.parametrize(
    ("dimension", "left", "right"),
    [
        (
            "model",
            [_node("sklearn.linear_model.Ridge", "model", params={"alpha": 1})],
            [_node("sklearn.svm.SVR", "model", params={"C": 2})],
        ),
        ("preprocessing_sequence", [_node("nirs4all.SNV"), _node("sklearn.preprocessing.StandardScaler")], []),
        (
            "feature_selection",
            [_node("sklearn.feature_selection.SelectKBest", "feature_selection", params={"k": 3})],
            [],
        ),
        ("augmentation", [_node("nirs4all.augmentation.RandomShift", "augmentation", params={"scale": 2})], []),
    ],
)
def test_selected_factor_changes_share_context(dimension, left, right):
    rest = [_node("sklearn.linear_model.Ridge", "model")] if dimension != "model" else [_node("nirs4all.SNV")]
    a, b = _pipe("a", left + rest), _pipe("b", right + rest)
    before = deepcopy([a, b])
    contexts = build_component_contexts([a, b], dimension)
    assert contexts["a"]["fingerprint"] == contexts["b"]["fingerprint"]
    assert contexts["a"]["excluded_reason"] is None
    assert [a, b] == before


@pytest.mark.parametrize("change", ["params", "version", "fit_scope", "order", "extra", "graph_extra"])
def test_nonselected_scientific_changes_split_context(change):
    a = _pipe("a", [_node("nirs4all.SNV"), _node("nirs4all.Detrend"), _node("sklearn.linear_model.Ridge", "model")])
    graph = json.loads(a["graph_json"])
    b = deepcopy(a)
    b["pipeline_dag_hash"] = "b"
    if change == "params":
        graph["nodes"][0]["params"]["axis"] = 1
    elif change == "version":
        graph["nodes"][0]["operator_version"] = "2"
    elif change == "fit_scope":
        graph["nodes"][0]["fit_scope"] = "all_data"
    elif change == "order":
        graph["nodes"][0]["operator"], graph["nodes"][1]["operator"] = (
            graph["nodes"][1]["operator"],
            graph["nodes"][0]["operator"],
        )
    elif change == "extra":
        graph["nodes"][0]["scientific_option"] = {"precision": "f32"}
    else:
        graph["execution_semantics"] = "different"
    b["graph_json"] = json.dumps(graph)
    assert _value(a) != _value(b)


def test_node_ids_merkle_signatures_and_declaration_order_are_not_context():
    a = _pipe("a", [_node("nirs4all.SNV"), _node("sklearn.linear_model.Ridge", "model")])
    graph = json.loads(a["graph_json"])
    rename = {node["id"]: f"different_{i}" for i, node in enumerate(graph["nodes"])}
    for node in graph["nodes"]:
        node["id"] = rename[node["id"]]
        node["signature"] = "different_derived_signature"
    for edge in graph["edges"]:
        edge["src"], edge["dst"] = rename[edge["src"]], rename[edge["dst"]]
    graph["nodes"].reverse()
    b = {"pipeline_dag_hash": "b", "is_linear": False, "graph_json": json.dumps(graph)}
    assert _value(a) == _value(b)


def test_parameter_removes_only_selected_value_preserves_version_scope_position_and_other_params():
    steps = [_node("nirs4all.SNV"), _node("sklearn.linear_model.Ridge", "model", params={"alpha": 1, "solver": "svd"})]
    a = _pipe("a", steps)
    options = {"parameter_operator": "sklearn.linear_model.Ridge", "parameter_name": "alpha"}
    steps[1]["params"]["alpha"] = 2
    b = _pipe("b", steps)
    assert _value(a, "parameter", **options) == _value(b, "parameter", **options)
    for field, value in [("operator_version", "2"), ("fit_scope", "all_data")]:
        changed = deepcopy(steps)
        changed[1][field] = value
        assert _value(a, "parameter", **options) != _value(_pipe("c", changed), "parameter", **options)
    changed = deepcopy(steps)
    changed[1]["params"]["solver"] = "cholesky"
    assert _value(a, "parameter", **options) != _value(_pipe("c", changed), "parameter", **options)
    assert _value(a, "parameter", **options) != _value(_pipe("c", list(reversed(steps))), "parameter", **options)


def test_parameter_missing_and_repeated_operator_are_excluded():
    options = {"parameter_operator": "nirs4all.SNV", "parameter_name": "axis"}
    model = _node("sklearn.linear_model.Ridge", "model")
    a = _pipe("a", [_node("nirs4all.SNV", params={"axis": 0}), _node("nirs4all.SNV"), model])
    assert _context(a, "parameter", **options)["excluded_reason"] == "ambiguous_parameter_operator"
    b = _pipe("b", [_node("nirs4all.SNV"), model])
    assert _context(b, "parameter", **options)["excluded_reason"] == "missing_target_parameter"
    c = _pipe("c", [model])
    assert _context(c, "parameter", **options)["excluded_reason"] == "missing_parameter_operator"


def test_target_preprocessing_operator_masks_only_it_and_allows_absence():
    model = _node("sklearn.linear_model.Ridge", "model")
    other = _node("nirs4all.Detrend", params={"order": 2})
    a = _pipe("a", [_node("nirs4all.SNV"), other, model])
    b = _pipe("b", [other, model])
    options = {"parameter_operator": "nirs4all.SNV"}
    assert _value(a, "preprocessing_operator", **options) == _value(b, "preprocessing_operator", **options)
    changed = _pipe("c", [_node("nirs4all.Detrend", params={"order": 3}), model])
    assert _value(a, "preprocessing_operator", **options) != _value(changed, "preprocessing_operator", **options)
    assert _context(a, "preprocessing_operator")["excluded_reason"] == "missing_target_operator"
    repeat = _pipe("d", [_node("nirs4all.SNV"), _node("nirs4all.SNV"), model])
    assert _context(repeat, "preprocessing_operator", **options)["excluded_reason"] == "ambiguous_target_operator"


@pytest.mark.parametrize(
    "case", ["branch", "cycle", "disconnected", "merge", "multi_model", "branch_path", "unknown_role", "duplicate_id"]
)
def test_unsupported_graphs_are_excluded_without_hash(case):
    graph = json.loads(
        _pipe("a", [_node("nirs4all.SNV"), _node("nirs4all.Detrend"), _node("sklearn.linear_model.Ridge", "model")])[
            "graph_json"
        ]
    )
    if case == "branch":
        graph["edges"].append({"src": "g0", "dst": "g2"})
    elif case == "cycle":
        graph["edges"].append({"src": "g2", "dst": "g0"})
    elif case == "disconnected":
        graph["edges"].pop()
    elif case == "merge":
        graph["nodes"][1]["kind"] = "merge"
    elif case == "multi_model":
        graph["nodes"][1].update(kind="model", operator="sklearn.svm.SVR")
    elif case == "branch_path":
        graph["nodes"][1]["branch_path"] = ["branch0"]
    elif case == "unknown_role":
        graph["nodes"][1].update(kind="unknown", operator="custom.Unclassified")
    else:
        graph["nodes"][1]["id"] = graph["nodes"][0]["id"]
    result = _context({"pipeline_dag_hash": "a", "graph_json": json.dumps(graph)})
    assert result["fingerprint"] is None
    assert result["excluded_reason"]


def test_noncontiguous_sequence_and_ambiguous_feature_chain_excluded():
    model = _node("sklearn.linear_model.Ridge", "model")
    feature = _node("sklearn.feature_selection.SelectKBest", "feature_selection")
    pp = _node("nirs4all.SNV")
    assert (
        _context(_pipe("a", [pp, feature, pp, model]), "preprocessing_sequence")["excluded_reason"]
        == "noncontiguous_preprocessing_sequence"
    )
    assert (
        _context(_pipe("a", [feature, feature, model]), "feature_selection")["excluded_reason"]
        == "ambiguous_selected_nodes"
    )


def test_materialized_projection_fallback_and_full_source_metadata():
    graph = json.loads(_pipe("a", [_node("nirs4all.SNV"), _node("sklearn.linear_model.Ridge", "model")])["graph_json"])
    rows = [
        {
            "node_id": n["id"],
            "node_signature": n["signature"],
            "role": n["kind"],
            "operator": n["operator"],
            "operator_version": n["operator_version"],
            "params_json": json.dumps(n["params"]),
            "fit_scope": n["fit_scope"],
            "branch_path_json": json.dumps(n["branch_path"]),
        }
        for n in graph["nodes"]
    ]
    pipe = {"pipeline_dag_hash": "a", "nodes": rows, "edges": graph["edges"]}
    assert _value(pipe)
    source = deepcopy(graph)
    source["nodes"][0]["scientific_extension"] = "preserved"
    assert _value({**pipe, "graph_json": json.dumps(source)}) != _value({**pipe, "graph_json": json.dumps(graph)})


def test_remaining_edge_metadata_preserves_its_position_and_masked_metadata_refuses():
    graph = json.loads(
        _pipe(
            "a",
            [
                _node("nirs4all.SNV"),
                _node("nirs4all.Detrend"),
                _node("nirs4all.Crop"),
                _node("sklearn.linear_model.Ridge", "model"),
            ],
        )["graph_json"]
    )
    a = deepcopy(graph)
    a["edges"][0]["scientific_extension"] = "x"
    b = deepcopy(graph)
    b["edges"][1]["scientific_extension"] = "x"
    assert _value({"pipeline_dag_hash": "a", "graph_json": json.dumps(a)}) != _value(
        {"pipeline_dag_hash": "b", "graph_json": json.dumps(b)}
    )
    graph["edges"][-1]["src_port"] = "special_output"
    result = _context({"pipeline_dag_hash": "a", "graph_json": json.dumps(graph)})
    assert result["excluded_reason"] == "unsupported_masked_edge_metadata"


def test_masked_model_preserves_training_scope_and_unknown_constraints():
    a = _pipe("a", [_node("nirs4all.SNV"), _node("sklearn.linear_model.Ridge", "model")])
    base = json.loads(a["graph_json"])
    for field, value in [("fit_scope", "all_data"), ("scientific_constraint", {"sample_weight": "balanced"})]:
        changed = deepcopy(base)
        changed["nodes"][-1][field] = value
        b = {"pipeline_dag_hash": "b", "graph_json": json.dumps(changed)}
        assert _value(a) != _value(b)
    changed = deepcopy(base)
    changed["nodes"][-1].update(operator="sklearn.svm.SVR", operator_version="2", params={"C": 12})
    assert _value(a) == _value({"pipeline_dag_hash": "b", "graph_json": json.dumps(changed)})
