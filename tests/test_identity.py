"""Identity spine: hashing determinism + topology-aware pipeline identity."""

from __future__ import annotations

import pytest

from nirs4all_benchmarks.identity import (
    canonical_json,
    compose_run_condition_hash,
    compute_pipeline_dag_hash,
    fingerprint,
    is_hash,
)
from nirs4all_benchmarks.identity.hashing import export_hash
from nirs4all_benchmarks.identity.pipeline_dag import nirs4all_identity_hash


def test_canonical_json_is_key_order_independent():
    assert canonical_json({"b": 1, "a": [3, 2, 1]}) == canonical_json({"a": [3, 2, 1], "b": 1})
    assert fingerprint({"x": 1, "y": 2}) == fingerprint({"y": 2, "x": 1})


def test_canonical_json_rejects_nan():
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_fingerprint_is_64_hex():
    h = fingerprint({"a": 1})
    assert is_hash(h) and len(h) == 64


def test_run_condition_hash_is_order_sensitive_and_stable():
    base = {
        "dataset_variant_hash": "a" * 64, "split_instance_hash": "b" * 64, "cv_instance_hash": "c" * 64,
        "rng_context_hash": "d" * 64, "pipeline_dag_hash": "e" * 64, "refit_strategy_hash": "f" * 64,
    }
    h1 = compose_run_condition_hash(**base)
    h2 = compose_run_condition_hash(**base)
    assert h1 == h2
    swapped = {**base, "dataset_variant_hash": "b" * 64, "split_instance_hash": "a" * 64}
    assert compose_run_condition_hash(**swapped) != h1


def test_pipeline_hash_is_node_id_invariant():
    g1 = {"nodes": [{"node_id": "a", "kind": "transform", "operator": "X"},
                    {"node_id": "b", "kind": "model", "operator": "M", "params": {"k": 1}}],
          "edges": [{"src": "a", "dst": "b"}]}
    g2 = {"nodes": [{"node_id": "zzz", "kind": "transform", "operator": "X"},
                    {"node_id": "q", "kind": "model", "operator": "M", "params": {"k": 1}}],
          "edges": [{"src": "zzz", "dst": "q"}]}
    assert compute_pipeline_dag_hash(g1).pipeline_dag_hash == compute_pipeline_dag_hash(g2).pipeline_dag_hash


def test_pipeline_hash_merge_is_order_insensitive():
    a = {"nodes": [{"node_id": "x", "operator": "A"}, {"node_id": "y", "operator": "B"},
                   {"node_id": "m", "kind": "mean"}],
         "edges": [{"src": "x", "dst": "m"}, {"src": "y", "dst": "m"}]}
    b = {"nodes": [{"node_id": "x", "operator": "B"}, {"node_id": "y", "operator": "A"},
                   {"node_id": "m", "kind": "mean"}],
         "edges": [{"src": "x", "dst": "m"}, {"src": "y", "dst": "m"}]}
    assert compute_pipeline_dag_hash(a).pipeline_dag_hash == compute_pipeline_dag_hash(b).pipeline_dag_hash


def test_pipeline_hash_stacking_is_order_sensitive_on_shared_port():
    # Regression for the adversarial-review finding: an order-sensitive merge whose
    # two inputs arrive on the same (default) port must NOT collapse when the
    # upstream operators are swapped — stack(A, B) != stack(B, A).
    def g(op_a, op_b):
        return {
            "nodes": [
                {"node_id": "in", "kind": "input", "operator": "X"},
                {"node_id": "a", "kind": "model", "operator": op_a},
                {"node_id": "b", "kind": "model", "operator": op_b},
                {"node_id": "stack", "kind": "stacking", "operator": "Ridge"},
            ],
            "edges": [
                {"src": "in", "dst": "a"}, {"src": "in", "dst": "b"},
                {"src": "a", "dst": "stack"}, {"src": "b", "dst": "stack"},
            ],
        }
    h1 = compute_pipeline_dag_hash(g("PLS", "RF")).pipeline_dag_hash
    h2 = compute_pipeline_dag_hash(g("RF", "PLS")).pipeline_dag_hash
    assert h1 != h2


def test_pipeline_hash_distinguishes_real_topology_change():
    base = {"nodes": [{"node_id": "x", "operator": "A"}, {"node_id": "m", "kind": "model", "operator": "M"}],
            "edges": [{"src": "x", "dst": "m"}]}
    changed = {"nodes": [{"node_id": "x", "operator": "A2"}, {"node_id": "m", "kind": "model", "operator": "M"}],
               "edges": [{"src": "x", "dst": "m"}]}
    assert compute_pipeline_dag_hash(base).pipeline_dag_hash != compute_pipeline_dag_hash(changed).pipeline_dag_hash


def test_linear_list_lifts_and_records_get_hash():
    steps = ["sklearn.preprocessing.StandardScaler", {"model": "PLS", "params": {"n_components": 10}}]
    pid = compute_pipeline_dag_hash(steps)
    assert pid.is_linear
    assert pid.nirs4all_identity_hash == nirs4all_identity_hash(steps)
    assert len(pid.nirs4all_identity_hash) == 16


def test_export_hash_is_fixed_point():
    manifest = {"arena_export_schema_version": 1, "dataset": {"dataset_fingerprint": "a" * 64}}
    h = export_hash(manifest)
    manifest["arena_export_hash"] = h
    assert export_hash(manifest) == h
