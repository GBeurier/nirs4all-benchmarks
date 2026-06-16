"""Frozen identity regression contract (DATA_MANAGEMENT.md §9 step 5).

The pipeline_dag_hash and run_condition_hash of the fixtures must not drift: a
change here means the canonical identity computation changed, which would silently
break cross-version dedup. If this test fails intentionally, bump the relevant
schema version and re-freeze the golden values.
"""

from __future__ import annotations

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.ingestion.resolve import resolve_identities

# (pipeline label | dataset name) -> first 16 hex of run_condition_hash
GOLDEN_RUN_CONDITIONS = {
    "StdScaler→SNV→PLS(k=12)|Mock Corn": "f362b60d7ad41481",
    "StdScaler→SNV→PLS(k=12)|Mock Wheat": "3705e88c526e4ed0",
    "StdScaler→SNV→PLS(k=16)|Mock Corn": "0a0c446074582815",
    "StdScaler→SNV→PLS(k=16)|Mock Wheat": "d8090e2315f4fb41",
    "StdScaler→SNV→PLS(k=20)|Mock Corn": "9c7075663a2d55c3",
    "StdScaler→SNV→PLS(k=20)|Mock Wheat": "a38d0a79f1b802f9",
    "StdScaler→SNV→PLS(k=5)|Mock Corn": "266ef92351329d34",
    "StdScaler→SNV→PLS(k=5)|Mock Wheat": "816dd60e72c18ab5",
    "StdScaler→SNV→PLS(k=8)|Mock Corn": "e692b20be411d828",
    "StdScaler→SNV→PLS(k=8)|Mock Wheat": "61272e4b093a4d06",
    "[SNV→PLS | SG→PLS] → mean|Mock Corn": "07fa3d185dfa617a",
    "[SNV→PLS | SG→PLS] → mean|Mock Wheat": "ed3f0906d86f869b",
    "stack(SNV→PLS, SNV→RF)|Mock Corn": "d12e102f9b9820dd",
    "stack(SNV→PLS, SNV→RF)|Mock Wheat": "52ff5b3b19751e58",
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
        assert got == GOLDEN_RUN_CONDITIONS[key], f"drift for {key}: {got}"


def test_same_pipeline_different_dataset_shares_pipeline_hash():
    exps = generate_fixture_exports()
    by_label: dict[str, set[str]] = {}
    for e in exps:
        by_label.setdefault(e.pipeline.human_label, set()).add(resolve_identities(e).pipeline_dag_hash)
    # every pipeline label maps to exactly one pipeline_dag_hash regardless of dataset
    assert all(len(v) == 1 for v in by_label.values())
