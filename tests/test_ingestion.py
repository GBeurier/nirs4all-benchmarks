"""Ingestion state machine: commit, idempotency, quarantine, recompute."""

from __future__ import annotations

from pathlib import Path

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore


def _first_export():
    return generate_fixture_exports()[0]


def test_commit_and_idempotency(tmp_path: Path):
    exp = _first_export()
    with ArenaStore(tmp_path / "a") as store:
        r1 = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert r1.status == "committed"
        assert r1.run_condition_hash and r1.execution_hash
        r2 = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert r2.status == "already_ingested"
        assert store.count("executions") == 1


def test_scores_recomputed_from_residuals(tmp_path: Path):
    exp = _first_export()  # fixtures carry no observations, only residuals
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert r.clean_report["scores_recomputed"] is True
        assert store.count("metric_observations") > 0


def test_quarantine_on_unattested_leakage_for_release(tmp_path: Path):
    exp = _first_export()
    exp.leakage_attestation.oof_enforced = False
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="rel", collection_kind="benchmark_release"))
        assert r.status == "quarantined"
        assert r.validity_status == "quarantined"
        row = store.query_one("SELECT validity_status FROM executions LIMIT 1")
        assert row["validity_status"] == "quarantined"


def test_unattested_leakage_flagged_for_user_runs(tmp_path: Path):
    exp = _first_export()
    exp.leakage_attestation.oof_enforced = False
    with ArenaStore(tmp_path / "a") as store:
        r = ingest_export(store, exp, policy=IngestionPolicy(collection_id="u", collection_kind="user_run_collection"))
        assert r.status == "committed"
        assert any(i["code"] == "leakage_unattested" for i in r.issues)


def test_dedup_across_datasets(tmp_path: Path):
    # All PLS-sweep pipelines are shared across the two datasets → deduped pipelines.
    with ArenaStore(tmp_path / "a") as store:
        for exp in generate_fixture_exports():
            ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        assert store.count("pipeline_dags") == 7
        assert store.count("dataset_fingerprints") == 2
        assert store.count("run_conditions") == 14


def test_pseudonymization_is_consistent_across_runs(tmp_path: Path):
    exps = generate_fixture_exports()
    with ArenaStore(tmp_path / "a") as store:
        for exp in exps[:2]:  # two pipelines on the same first dataset
            ingest_export(store, exp, policy=IngestionPolicy(collection_id="c"))
        rsets = store.query("SELECT residual_set_id FROM residual_sets")
        ids_a = {r["sample_id"] for r in store.residuals.read(rsets[0]["residual_set_id"])}
        ids_b = {r["sample_id"] for r in store.residuals.read(rsets[1]["residual_set_id"])}
        # Same raw sample ids -> same pseudo ids -> large overlap (cross-run join works).
        assert len(ids_a & ids_b) > 50
        assert all(s.startswith("s_") for s in ids_a)
