"""Actual DAG fold-set identity: owner-API binding, contract rules and ranking scope boundaries.

The DAG outcome is an owner production-path test fixture (see
``fixtures/dagml_outcome_native_role/README.md``), used as identity and mapping
evidence only. A single-candidate result proves plumbing, not a qualified ranking.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from conftest import rankable_fixture_exports
from nirs4all_benchmarks.adapters import bundle_to_export
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export, validate_metadata
from nirs4all_benchmarks.ingestion.resolve import fold_set_cv_instance_hash
from nirs4all_benchmarks.ranking import quality_ranking
from nirs4all_benchmarks.store import ArenaStore
from test_quality_ranking import Capture

FIXTURE = Path(__file__).parent / "fixtures" / "dagml_outcome_native_role" / "training_outcome.json"
IDENTITY_REFUSALS = {
    "missing_comparison_identity",
    "missing_actual_cv_identity",
    "actual_cv_identity_requires_reingestion",
}


def _declarations() -> dict[str, Any]:
    """Explicit test declarations; none of them is derived from or claimed by the owner fixture."""
    return {
        "dataset_envelope": {"dataset_fingerprint": "a" * 64},
        "task": {"task_hash": "b" * 64, "task_type": "regression", "target_name": "protein"},
        "dataset_variant": {"dataset_variant_hash": "c" * 64},
        "identity_basis": {
            name: {"kind": "owner_reported", "owner": "test-declaration"}
            for name in ("dataset_fingerprint", "task_hash", "dataset_variant_hash")
        },
        "task_type": "regression",
        "execution": {"status": "ok", "terminal_status": "completed"},
        "leakage_attestation": {
            "oof_enforced": True,
            "group_leakage_checked": True,
            "nested_cv_safe": True,
            "unsafe_flags": [],
        },
        "score_context": {
            "score_version": "dag-ml.score_set/2",
            "metric_implementation": "dag-ml.score_set",
            "aggregation_policy": "dag-ml.native",
        },
        "visibility": "private",
        "provenance": {"test_declarations": "identities, execution, attestations and score context"},
    }


def _owner_dag_ml() -> types.ModuleType:
    if getattr(sys, "_arena_test_guards", False):
        pytest.skip("the guarded profile does not admit the owner dag_ml runtime")
    return pytest.importorskip("dag_ml")


def test_training_outcome_conflicts_and_absent_or_incompatible_owner_api(monkeypatch):
    with pytest.raises(ValueError, match="dagml_training_outcome_input_conflict"):
        bundle_to_export({}, training_outcome=FIXTURE)
    monkeypatch.setitem(sys.modules, "dag_ml", None)
    with pytest.raises(ValueError, match="dagml_runtime_required_for_fold_identity"):
        bundle_to_export(training_outcome=FIXTURE, **_declarations())
    monkeypatch.setitem(sys.modules, "dag_ml", types.ModuleType("dag_ml"))
    with pytest.raises(ValueError, match="dagml_fold_identity_api_unsupported"):
        bundle_to_export(training_outcome=FIXTURE, **_declarations())
    # The outcome path maps regression score reports only; another declared task is refused first.
    declarations = _declarations()
    declarations["task"] = {**declarations["task"], "task_type": "binary"}
    declarations["task_type"] = "binary"
    with pytest.raises(ValueError, match="dagml_prediction_task_unsupported"):
        bundle_to_export(training_outcome=FIXTURE, **declarations)


def test_generated_variant_choices_are_refused_before_mapping(tmp_path, monkeypatch):
    # Refusal-path seam only: no executed outcome is claimed. Owner validation is bypassed so the
    # guard is reached; a real owner outcome with non-empty choices is refused at the same check.
    seam = types.ModuleType("dag_ml")
    seam.DagMlError = type("DagMlError", (Exception,), {})
    seam.validate_training_outcome_json = lambda text: None
    seam.fold_set_fingerprint_json = lambda text: "e" * 64
    monkeypatch.setitem(sys.modules, "dag_ml", seam)
    outcome = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert outcome["effective_plan"]["variants"][0]["choices"] == {}
    outcome["effective_plan"]["variants"][0]["choices"] = {"model:base.n_components": 2}
    path = tmp_path / "generated_choice.json"
    path.write_text(json.dumps(outcome), encoding="utf-8")
    with pytest.raises(ValueError, match="dagml_outcome_single_unpatched_variant_required"):
        bundle_to_export(training_outcome=path, **_declarations())


def test_declared_fold_set_contract_rules_and_hash_stability(tmp_path):
    raw = rankable_fixture_exports()[0].to_manifest()
    assert "fold_set_fingerprint" not in raw["cv"]
    raw["cv"]["fold_set_fingerprint"] = fingerprint({"synthetic_fold_set": "declaration"})
    assert validate_metadata(raw).issues[0]["code"] == "cv_identity_contradiction"
    raw["cv"].pop("cv_instance_hash")
    assert validate_metadata(raw).issues[0]["code"] == "fold_set_relation_identity_required"
    raw["dataset"]["relation_fingerprint"] = fingerprint({"synthetic_relation": "declaration"})
    raw["provenance"]["synthetic_test_declarations"] += "; synthetic fold set and relation declarations"
    metadata = validate_metadata(raw)
    assert metadata.status == "metadata_valid"
    assert "cv.fold_set_fingerprint" in metadata.adopted_unverified
    assert validate_metadata({**raw, "cv": {**raw["cv"], "fold_set_fingerprint": "not-a-hash"}}).issues[0][
        "code"
    ] == "identity_invalid"
    with ArenaStore(tmp_path) as store:
        assert ingest_export(store, raw, policy=IngestionPolicy(collection_id="declared")).status == "committed"
        row = store.query_one("SELECT * FROM cv_instances WHERE engine_fold_set_fingerprint IS NOT NULL")
        assert row["engine_fold_set_fingerprint"] == raw["cv"]["fold_set_fingerprint"]
        assert json.loads(row["fold_summary_json"]) == {"relation_fingerprint": raw["dataset"]["relation_fingerprint"]}
        assert row["cv_instance_hash"] == fold_set_cv_instance_hash(
            row["cv_spec_hash"], row["engine_fold_set_fingerprint"], raw["dataset"]["relation_fingerprint"]
        )


def test_owner_outcome_binds_fold_set_and_maps_scores_and_identity_only(tmp_path):
    dag_ml = _owner_dag_ml()
    outcome = json.loads(FIXTURE.read_text(encoding="utf-8"))
    export = bundle_to_export(training_outcome=FIXTURE, **_declarations())
    expected = dag_ml.fold_set_fingerprint_json(json.dumps(outcome["effective_plan"]["fold_set"]))
    assert export.cv.fold_set_fingerprint == expected
    assert export.cv.cv_instance_hash is None
    assert export.dataset.relation_fingerprint == outcome["data_identities"][0]["relation_fingerprint"]
    observations = export.scores.observations
    assert {(o.fold_id, o.partition) for o in observations if o.scope == "cv"} == {
        ("fold:0", "validation"),
        ("fold:1", "validation"),
    }
    assert {o.source_fold_id for o in observations if o.scope == "view"} == {"avg", "w_avg"}
    assert all(o.fold_id not in {"avg", "w_avg"} for o in observations)
    unmapped = export.provenance.model_dump()["dagml_unmapped_score_reports"]
    assert {u["partition"] for u in unmapped if u["reason"] == "fold_partition_without_actual_cohort_identity"} == {
        "train",
        "train_pool",
    }
    assert any(u["reason"] == "metric_unsupported" for u in unmapped)
    # Classification metrics emitted for a regression output are never regression observations.
    incompatible = {u["metric_name"] for u in unmapped if u["reason"] == "metric_task_type_incompatible"}
    assert {"accuracy", "balanced_accuracy"} <= incompatible
    assert not {o.metric_name for o in observations} & {"accuracy", "balanced_accuracy", "f1"}
    # Valid regression scores are preserved exactly.
    owner_rmse = {
        report["fold_id"]: report["metrics"]["rmse"]
        for report in outcome["score_set"]["reports"]
        if report["partition"] == "validation" and report["fold_id"] in {"fold:0", "fold:1"}
    }
    assert {o.fold_id: o.metric_value for o in observations if o.scope == "cv" and o.metric_name == "rmse"} == owner_rmse
    node = export.pipeline.graph["nodes"][0]
    assert "models.pls.pls_regression" in node["operator"] and node["params"]["n_components"] == 1
    manifest = canonical_json(export.to_manifest())
    assert export.residuals.inline == []
    for payload in ("prediction_caches", "raw_artifact_payloads", "portable_prediction_caches", "effective_plan"):
        assert payload not in manifest
    with ArenaStore(tmp_path) as store:
        result = ingest_export(store, export, policy=IngestionPolicy(collection_id="dagml-outcome"))
        assert result.status == "committed"
        ranking = quality_ranking(store, ranker=Capture())
        provenance = ranking["provenance"]
        assert provenance["accepted_observations"] > 0
        assert not IDENTITY_REFUSALS & set(provenance["excluded"])
        assert provenance["fold_aggregation"] == "cv_fold_mean"
        # One candidate: identity plumbing is proven, no ranking is qualified.
        assert ranking["state"] == "unavailable" and ranking["reason"] == "no_matched_comparisons"
        accuracy = quality_ranking(store, {"metric": "accuracy"}, ranker=Capture())
        assert accuracy["state"] == "unavailable" and accuracy["provenance"]["source_observations"] == 0
        refit = quality_ranking(store, {"scope": "refit", "partition": "final"}, ranker=Capture())
        refit_provenance = refit["provenance"]
        assert refit_provenance["source_observations"] > 0
        assert refit_provenance["excluded"] == {"missing_comparison_identity": refit_provenance["source_observations"]}


def test_owner_validation_refuses_mutated_outcome_and_relation_conflict(tmp_path):
    _owner_dag_ml()
    text = FIXTURE.read_text(encoding="utf-8")
    mutated = text.replace('"outer"', '"outer-mutated"', 1)
    assert mutated != text
    path = tmp_path / "mutated.json"
    path.write_text(mutated, encoding="utf-8")
    with pytest.raises(ValueError, match="dagml_training_outcome_invalid"):
        bundle_to_export(training_outcome=path, **_declarations())
    declarations = _declarations()
    declarations["dataset_envelope"] = {**declarations["dataset_envelope"], "relation_fingerprint": "d" * 64}
    with pytest.raises(ValueError, match="dagml_outcome_relation_mismatch"):
        bundle_to_export(training_outcome=FIXTURE, **declarations)
