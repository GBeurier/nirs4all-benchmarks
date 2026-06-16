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
