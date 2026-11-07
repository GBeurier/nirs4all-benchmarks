"""Versioned fixture identity pins; old v1 proof remains intact.

Sole v2 includes explicit evidence/context in identity. Native v2 pins below were
observed from the deterministic synthetic generator, never a scientific fit.
"""

from __future__ import annotations

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.ingestion.resolve import resolve_identities

# (pipeline label | dataset name) -> first 16 hex of run_condition_hash
GOLDEN_RUN_CONDITIONS = {
    "StdScaler→SNV→PLS(k=5)|Mock Corn": "18192e5006c03268",
    "StdScaler→SNV→PLS(k=8)|Mock Corn": "92c9184ddf7b5efd",
    "StdScaler→SNV→PLS(k=12)|Mock Corn": "42264bd31b9b4296",
    "StdScaler→SNV→PLS(k=16)|Mock Corn": "a1eb4f89f6f27907",
    "StdScaler→SNV→PLS(k=20)|Mock Corn": "c46941419e6e7167",
    "[SNV→PLS | SG→PLS] → mean|Mock Corn": "8ae9b55ea8d7ccb6",
    "stack(SNV→PLS, SNV→RF)|Mock Corn": "160f4d84180b9ffa",
    "StdScaler→SNV→PLS(k=5)|Mock Wheat": "02e46f0f1a65c6e5",
    "StdScaler→SNV→PLS(k=8)|Mock Wheat": "7c7bcf579f7b87e5",
    "StdScaler→SNV→PLS(k=12)|Mock Wheat": "d8fced0ec9ed2023",
    "StdScaler→SNV→PLS(k=16)|Mock Wheat": "c8e9c286e35773e6",
    "StdScaler→SNV→PLS(k=20)|Mock Wheat": "ee409b207392b148",
    "[SNV→PLS | SG→PLS] → mean|Mock Wheat": "74aec083df5b2da3",
    "stack(SNV→PLS, SNV→RF)|Mock Wheat": "719643225b4b1d20",
}


# Native v2 identity domain; do not reuse these for a v1 wire or store.
GOLDEN_V2_RUN_CONDITIONS = {
    "StdScaler→SNV→PLS(k=5)|Mock Corn": "07b18e88dbf54eb0",
    "StdScaler→SNV→PLS(k=8)|Mock Corn": "778095a0e8225bb2",
    "StdScaler→SNV→PLS(k=12)|Mock Corn": "f392d30ff964de3f",
    "StdScaler→SNV→PLS(k=16)|Mock Corn": "54edb589e9a1fcb9",
    "StdScaler→SNV→PLS(k=20)|Mock Corn": "1236286c3fc83394",
    "[SNV→PLS | SG→PLS] → mean|Mock Corn": "6496a6f7f9eb9012",
    "stack(SNV→PLS, SNV→RF)|Mock Corn": "cb925186587dfb34",
    "StdScaler→SNV→PLS(k=5)|Mock Wheat": "acdc37d55594b077",
    "StdScaler→SNV→PLS(k=8)|Mock Wheat": "03372ce1c1c21415",
    "StdScaler→SNV→PLS(k=12)|Mock Wheat": "b7c2a92e5218f052",
    "StdScaler→SNV→PLS(k=16)|Mock Wheat": "c82a3f9952a53405",
    "StdScaler→SNV→PLS(k=20)|Mock Wheat": "82a70f4a70c91400",
    "[SNV→PLS | SG→PLS] → mean|Mock Wheat": "43a897134689e1f2",
    "stack(SNV→PLS, SNV→RF)|Mock Wheat": "4f2a93485cb2d9d4",
}


def test_fixture_count_and_distinct_pipelines():
    exps = generate_fixture_exports()
    assert len(exps) == 14
    pdh = {resolve_identities(e).pipeline_dag_hash for e in exps}
    assert len(pdh) == 7  # 5 PLS-sweep + branch/merge + stacking, deduped across datasets


def test_run_condition_hashes_are_frozen():
    for exp in generate_fixture_exports():
        key = exp.pipeline.human_label + "|" + (exp.dataset.dataset_card.get("name") or "")
        got = resolve_identities(exp).run_condition_hash[:16]
        assert exp.arena_export_schema_version == 2
        assert got == GOLDEN_V2_RUN_CONDITIONS[key], f"drift for {key}: {got}"
        assert got != GOLDEN_RUN_CONDITIONS[key]  # Versioned identity change is explicit.


def test_same_pipeline_different_dataset_shares_pipeline_hash():
    exps = generate_fixture_exports()
    by_label: dict[str, set[str]] = {}
    for e in exps:
        by_label.setdefault(e.pipeline.human_label, set()).add(resolve_identities(e).pipeline_dag_hash)
    # every pipeline label maps to exactly one pipeline_dag_hash regardless of dataset
    assert all(len(v) == 1 for v in by_label.values())
