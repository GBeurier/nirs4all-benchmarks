"""Synthetic counterexamples for the accepted offline Arena review (C1–C8)."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from typer.testing import CliRunner

from nirs4all_benchmarks.adapters import bundle_to_export
from nirs4all_benchmarks.cli import app
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity.hashing import export_hash
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export, register_pipeline, validate_metadata
from nirs4all_benchmarks.ingestion.metadata_bundle import read_stored_metadata
from nirs4all_benchmarks.ingestion.resolve import resolve_identities
from nirs4all_benchmarks.site_build import _build_bundle, build_site
from nirs4all_benchmarks.store import ArenaStore, Queries


def manifest(terminal="completed", *, value=1.0, execution_id="synthetic"):
    """A test declaration, not evidence that a scientific execution occurred."""
    execution = {"execution_id": execution_id}
    if terminal is not None:
        execution.update(
            status="ok" if terminal == "completed" else "failed" if terminal == "incomplete" else terminal,
            terminal_status=terminal,
        )
    from test_arena_v2 import record

    source = record(value, execution_id=execution_id)
    source["execution"] = execution
    source["dataset"].update(visibility="private", visibility_source="producer_reported")
    return source


def residuals():
    return [
        {
            "sample_id": "SENSITIVE_SAMPLE",
            "group_id": "SENSITIVE_GROUP",
            "partition": "validation",
            "scope": "cv",
            "fold_id": "f0",
            "y_true": 27.0,
            "y_pred": 25.0,
            "residual": 2.0,
        }
    ]


def residual_bundle():
    return {
        "key": "sample_id",
        "inline": residuals(),
        "publishable": dict.fromkeys(("y_true", "y_pred", "residual", "y_proba"), True),
    }


def test_observed_v1_proof_is_preserved_but_not_adopted_by_v2():
    # Obtained independently from the HEAD v1 model, not from the modified model.
    source = {
        "arena_export_schema_version": 1,
        "dataset": {"dataset_fingerprint": "a" * 64},
        "task": {"task_hash": "b" * 64, "task_type": "regression"},
        "dataset_variant": {"dataset_variant_hash": "c" * 64},
        "pipeline": {"nodes": [{"node_id": "g0"}]},
    }
    result = validate_metadata(source)
    frozen_v1_hash = "bd3fc0350c7f42a15a29129a932806db49092ac744313dc981c9a8e68b6e947f"
    assert len(frozen_v1_hash) == 64
    assert result.status == "rejected" and result.issues[0]["code"] == "unsupported_export_version"
    assert result.arena_export_hash is None
    assert result.visibility == "unreported"
    assert result.reported_attestation_fields == []


def test_validation_is_store_free_and_metadata_only(tmp_path, monkeypatch):
    path = tmp_path / "run.json"
    path.write_text(json.dumps(manifest()))
    before = set(tmp_path.iterdir())

    def no_store(*args, **kwargs):
        raise AssertionError("offline validation opened a store")

    monkeypatch.setattr(ArenaStore, "__init__", no_store)
    result = validate_metadata(path)
    assert result.status == "metadata_valid"
    assert result.scientific_verification == "not_performed"
    response = CliRunner().invoke(app, ["validate-export", str(path)])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["scientific_verification"] == "not_performed"
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        "SECRET_INPUT",
        {"dataset": "SECRET_INPUT"},
        {"execution": {"status": "incomplete"}},
        {"scores": {"observations": [{"metric_name": "rmse", "metric_value": float("nan")}]}},
        {"scores": {"observations": [{"metric_name": "rmse", "metric_value": float("inf")}]}},
        {"pipeline": {"graph": {"nodes": "SECRET_INPUT"}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a"}], "edges": [{"src": "a", "dst": "b"}]}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a"}], "edges": [{"src": "a", "dst": "a"}]}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a"}, {"id": "a"}]}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a"}], "edges": ["SECRET_INPUT"]}}},
    ],
)
def test_malformed_inputs_reject_without_echo_or_facts(empty_store, bad):
    source = {**manifest(), **bad} if isinstance(bad, dict) else bad
    result = ingest_export(empty_store, source)
    assert result.status == "rejected"
    assert "SECRET_INPUT" not in json.dumps(result.issues)
    assert empty_store.count("executions") == empty_store.count("ingestion_batches") == 0


@pytest.mark.parametrize(
    "source",
    [
        {"pipeline": {"graph": {"nodes": [{"id": "a", "params": "SECRET_INPUT"}]}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a"}], "edges": [{"src": ["a"], "dst": "a"}]}}},
        {"residuals": {"inline": [{"sample_id": "s", "fold_id": 0}]}},
        {"residuals": {"inline": [{"sample_id": "s", "y_true": "SECRET_INPUT"}]}},
        {"residuals": {"inline": [{"sample_id": "s", "y_proba": {"SECRET_INPUT": 1.0}}]}},
        {"scores": {"observations": [{"metric_name": "rmse", "metric_value": True}]}},
        {"dataset": {"dataset_fingerprint": "a" * 64, "dataset_card": {"axis": "SECRET_INPUT"}}},
        {"dataset": {"dataset_fingerprint": "a" * 64, "dataset_card": {"axis": {"range": {"SECRET_INPUT": 1}}}}},
        {"dataset": {"dataset_fingerprint": "a" * 64, "dataset_card": {"name": {"SECRET_INPUT": 1}}}},
        {"dataset": {"dataset_fingerprint": "a" * 64, "producer": {"io_version": {"SECRET_INPUT": 1}}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a", "fit_scope": {"SECRET_INPUT": 1}}]}}},
    ],
)
def test_malformed_nested_metadata_rejects_before_store_writes(empty_store, source):
    assert validate_metadata({**manifest(), **source}).status == "rejected"
    result = ingest_export(empty_store, {**manifest(), **source})
    assert result.status == "rejected" and empty_store.count("executions") == 0
    assert "SECRET_INPUT" not in json.dumps(result.issues)


def test_unrepresentable_metadata_is_a_typed_refusal_with_no_partial_facts(empty_store):
    source = manifest()
    source["dataset"]["n_samples"] = 2**63
    result = ingest_export(empty_store, source)
    assert result.status == "rejected" and result.issues[0]["code"] == "schema_invalid"
    assert empty_store.count("executions") == empty_store.count("ingestion_batches") == 0
    source["dataset"]["n_samples"] = 5
    assert ingest_export(empty_store, source).status == "committed"


@pytest.mark.parametrize(
    "terminal,status,resolved",
    [
        (None, "ok", "unreported"),
        (None, "failed", "failed"),
        (None, "cancelled", "cancelled"),
        ("completed", "ok", "completed"),
        ("incomplete", "failed", "incomplete"),
        ("failed", "failed", "failed"),
        ("cancelled", "cancelled", "cancelled"),
    ],
)
def test_terminal_contract_and_idempotent_ingestion(empty_store, terminal, status, resolved):
    source = manifest(terminal)
    source["execution"]["status"] = status
    first, second = ingest_export(empty_store, source), ingest_export(empty_store, source)
    assert first.status == "committed" and second.status == "already_ingested"
    assert first.terminal_status == second.terminal_status == resolved
    assert first.execution_hash == second.execution_hash
    assert empty_store.get("executions", "execution_hash", first.execution_hash)["status"] == resolved
    assert len(Queries(empty_store).leaderboard()["rows"]) == int(resolved == "completed")
    assert Queries(empty_store).run_explorer()[0]["execution_status"] == resolved


@pytest.mark.parametrize(
    "terminal,status", [("completed", "failed"), ("failed", "ok"), ("incomplete", "ok"), ("cancelled", "failed")]
)
def test_contradictory_status_rejected(empty_store, terminal, status):
    source = manifest(terminal)
    source["execution"]["status"] = status
    assert ingest_export(empty_store, source).issues[0]["code"] == "status_contradiction"


def test_missing_execution_is_unreported_and_no_score_failure_stays_inspectable(empty_store):
    source = manifest(None)
    del source["execution"]
    result = ingest_export(empty_store, source)
    assert result.terminal_status == "unreported"
    source = manifest("failed")
    del source["scores"]
    result = ingest_export(empty_store, source)
    assert result.status == "committed"
    assert len(Queries(empty_store).run_explorer()) == 2
    assert Queries(empty_store).run_detail(result.execution_hash)["scores"] == []


@pytest.mark.parametrize(
    "block,key",
    [
        (None, "arena_export_hash"),
        (None, "run_condition_hash"),
        ("pipeline", "pipeline_dag_hash"),
        ("scores", "score_computation_hash"),
        ("split", "split_spec_hash"),
        ("cv", "cv_spec_hash"),
    ],
)
def test_arena_claim_mismatch_rejects_before_dedup(empty_store, block, key):
    source = manifest()
    good = ingest_export(empty_store, source)
    if block:
        source.setdefault(block, {})[key] = "0" * 64
    else:
        source[key] = good.arena_export_hash if key == "arena_export_hash" else "0" * 64
        if key == "arena_export_hash":
            source["scores"]["observations"][0]["metric_value"] = 0.00001
    result = ingest_export(empty_store, source)
    assert result.status == "rejected"
    assert empty_store.count("executions") == 1


def test_c1_adopted_claims_are_reported_unverified(empty_store):
    source = manifest()
    source["rng"] = {"rng_context_hash": "d" * 64}
    source["refit"] = {"refit_strategy_hash": "e" * 64}
    result = ingest_export(empty_store, source)
    assert result.status == "committed"
    adopted = result.clean_report["metadata_validation"]["adopted_unverified"]
    assert {
        "task.task_hash",
        "dataset_variant.dataset_variant_hash",
        "rng.rng_context_hash",
        "refit.refit_strategy_hash",
        "dataset.dataset_fingerprint",
    } <= set(adopted)


def test_only_normalized_v2_hash_is_accepted_for_dict_path_and_model(empty_store, tmp_path):
    raw = manifest()
    raw["arena_export_hash"] = export_hash(raw)
    assert validate_metadata(raw).issues[0]["code"] == "export_hash_mismatch"
    path = tmp_path / "raw.json"
    path.write_text(json.dumps(raw))
    assert validate_metadata(path).issues[0]["code"] == "export_hash_mismatch"
    model = ArenaRunExport.model_validate(raw)
    model.arena_export_hash = export_hash(model.to_manifest())
    assert validate_metadata(model).hash_profile == "v2_normalized"
    assert ingest_export(empty_store, model).status == "committed"
    path.write_text(model.model_dump_json())
    assert ingest_export(empty_store, path).status == "already_ingested"


def test_all_arena_domain_claims_accept_their_recomputation(empty_store):
    model = ArenaRunExport.model_validate(manifest())
    resolved = resolve_identities(model)
    model.run_condition_hash = resolved.run_condition_hash
    model.pipeline.pipeline_dag_hash = resolved.pipeline_dag_hash
    model.split.split_spec_hash = resolved.split_spec_hash
    model.cv.cv_spec_hash = resolved.cv_spec_hash
    model.scores.score_computation_hash = resolved.score_spec.score_computation_hash
    assert ingest_export(empty_store, model).status == "committed"


def test_reimport_returns_original_v2_execution_despite_changed_derivation(empty_store, monkeypatch):
    import nirs4all_benchmarks.ingestion.offline as offline

    raw = manifest(None)
    first = ingest_export(empty_store, raw)
    original = offline.resolve_identities

    def changed(model):
        resolved = original(model)
        resolved.execution_hash = "e" * 64
        return resolved

    monkeypatch.setattr(offline, "resolve_identities", changed)
    again = ingest_export(empty_store, raw)
    assert again.status == "already_ingested" and again.execution_hash == first.execution_hash
    assert again.terminal_status == "unreported"
    assert empty_store.count("executions") == empty_store.count("ingestion_batches") == 1


@pytest.mark.parametrize("ref", ["../../SECRET_INPUT", "/absolute/residuals.parquet", "C:\\private\\x", "missing", ""])
def test_all_refs_refused_including_inline_and_existing_claims(empty_store, tmp_path, ref):
    source = manifest()
    first = ingest_export(empty_store, source)
    source["arena_export_hash"] = first.arena_export_hash
    source["residuals"] = {"ref": ref, **residual_bundle()}
    assert ingest_export(empty_store, source).issues[0]["code"] == "residual_ref_unsupported"
    path = tmp_path / "run.json"
    path.write_text(json.dumps(source))
    assert validate_metadata(path).issues[0]["code"] == "residual_ref_unsupported"


def test_rejection_then_restored_manifest_and_legacy_rejected_slot_retry(empty_store):
    source = manifest()
    source["residuals"] = {"ref": "missing"}
    assert ingest_export(empty_store, source).status == "rejected"
    del source["residuals"]
    key = validate_metadata(source).arena_export_hash
    empty_store.upsert(
        "ingestion_batches",
        {
            "ingestion_batch_id": "legacy_rejection",
            "input_export_hash": key,
            "arena_schema_version": 1,
            "status": "rejected",
            "target_collection": "default",
            "created_at": "synthetic",
        },
    )
    empty_store.conn.commit()
    result = ingest_export(empty_store, source)
    assert result.status == "committed" and result.ingestion_batch_id != "legacy_rejection"
    assert empty_store.get("ingestion_batches", "ingestion_batch_id", "legacy_rejection")["arena_schema_version"] == 1
    assert ingest_export(empty_store, source).status == "already_ingested"


@pytest.mark.parametrize(
    "counts,folds,terminal,accepted,coverage",
    [
        ((5, 2), ["f0", "f1"], "completed", False, "reported"),
        ((5, 2), ["f0", "f1"], "incomplete", True, "reported"),
        ((5, None), [], "completed", True, "not_reported"),
        ((None, None), ["f0"], "completed", True, "reported"),
        ((2, 1), ["f0", "f1"], "completed", False, "reported"),
    ],
)
def test_fold_metadata_without_completion_inference(counts, folds, terminal, accepted, coverage):
    source = manifest(terminal)
    source["scores"].update(expected_fold_count=counts[0], completed_fold_count=counts[1])
    if folds:
        source["scores"]["observations"] = [
            {**manifest()["scores"]["observations"][0], "scope": "fold", "fold_id": f} for f in folds
        ]
    else:
        source["scores"]["observations"][0]["fold_id"] = None
    result = validate_metadata(source)
    assert (result.status == "metadata_valid") == accepted
    assert result.fold_coverage == coverage


def test_direction_and_score_provenance_are_honest(empty_store):
    source = manifest()
    source["scores"]["observations"] = [
        {**manifest()["scores"]["observations"][0], "metric_name": "r2", "metric_value": 0.8, "direction": "max"}
    ]
    inferred = validate_metadata(source)
    assert inferred.status == "metadata_valid"  # Registry definition, not an invented producer default.
    wrong = json.loads(json.dumps(source))
    wrong["scores"]["observations"][0].update(direction="min", direction_source="producer_reported")
    assert ingest_export(empty_store, wrong).issues[0]["code"] == "metric_direction_mismatch"
    source["scores"]["observations"][0]["direction"] = "max"
    source["scores"].update(metric_implementation="owner/scorer/7", aggregation_policy="owner_policy")
    source["residuals"] = {**residual_bundle()}
    result = ingest_export(empty_store, source)
    assert result.scientific_verification == "not_performed"  # does not compare scientific values
    metadata = Queries(empty_store).run_detail(result.execution_hash)["metadata"]
    assert metadata["scores"]["metric_implementation"] == "owner/scorer/7"
    assert metadata["scores"]["observations"][0]["origin"] == "producer_reported"
    assert metadata["scores"]["observations"][0]["direction"] == "max"


@pytest.mark.parametrize("observations", [[], [{"metric_name": "rmse", "metric_value": 0.1}]])
def test_recomputation_cannot_invent_the_producer_scoring_implementation(empty_store, observations):
    source = manifest()
    source["scores"] = {
        "observations": [{**manifest()["scores"]["observations"][0], **obs} for obs in observations],
        "metric_implementation": "owner/scorer/7",
    }
    source["residuals"] = {**residual_bundle()}
    result = ingest_export(empty_store, source, policy=IngestionPolicy(recompute_scores=True))
    assert result.status == "rejected"
    assert result.issues[0]["code"] == "recompute_context_unsupported"
    assert empty_store.count("executions") == 0


@pytest.mark.parametrize(
    "method,kwargs,rows",
    [
        ("leaderboard", {}, "rows"),
        ("matrix", {}, "cells"),
        ("operator_effect", {}, "series"),
        ("parameter_effect", {"param_name": "k"}, "points"),
        ("robustness", {}, None),
        ("pivot", {"group_by": "model_family"}, "rows"),
        ("parallel", {"dimensions": ["model_family"]}, "rows"),
        ("stats", {}, "values"),
        ("pipeline_graph", {}, "nodes"),
        ("operator_graph", {}, "nodes"),
        ("composition", {}, "rows"),
    ],
)
def test_failed_winner_excluded_from_every_score_aggregation(empty_store, method, kwargs, rows):
    source = manifest("failed", value=0.000001)
    source["pipeline"]["nodes"][0]["params"] = {"k": 1}
    assert ingest_export(empty_store, source).ok
    q = Queries(empty_store)
    answer = getattr(q, method)(**kwargs)
    assert (answer[rows] if rows else answer) == []
    if "include_quarantined" in __import__("inspect").signature(getattr(q, method)).parameters:
        answer = getattr(q, method)(include_quarantined=True, **kwargs)
        assert (answer[rows] if rows else answer) == []
    assert q.overview()["valid_executions"] == 0
    assert q.metrics_available() == []
    assert len(q.run_explorer()) == 1
    assert q.run_detail(q.run_explorer()[0]["execution_hash"])
    # A worse completed result must be the only contribution at every boundary.
    source["execution"] = {"status": "ok", "terminal_status": "completed"}
    source["scores"]["observations"][0]["metric_value"] = 10.0
    assert ingest_export(empty_store, source).ok
    assert Queries(empty_store).leaderboard()["rows"][0]["mean"] == 10.0
    answer = getattr(q, method)(**kwargs)
    assert answer[rows] if rows else answer


@pytest.mark.parametrize("terminal", ["failed", "incomplete", "cancelled", None])
def test_ineligible_run_never_fulfils_a_plan(empty_store, terminal):
    source = manifest(terminal)
    recipe = {"nodes": source["pipeline"]["nodes"]}
    target = source["dataset"]["dataset_fingerprint"]
    register_pipeline(empty_store, recipe, target_datasets=[target])
    assert ingest_export(empty_store, source).ok
    planned = register_pipeline(empty_store, recipe, target_datasets=[target])
    assert planned.datasets[0]["status"] == "planned"
    assert Queries(empty_store).planned()
    # Old ingestion could mark a failed run fulfilled; readers must not trust it.
    empty_store.conn.execute("UPDATE planned_runs SET status = 'fulfilled'")
    empty_store.conn.commit()
    assert Queries(empty_store).planned()[0]["status"] == "planned"
    source["execution"] = {"status": "ok", "terminal_status": "completed"}
    assert ingest_export(empty_store, source).ok
    assert not Queries(empty_store).planned()
    assert register_pipeline(empty_store, recipe, target_datasets=[target]).datasets[0]["status"] == "already_run"


def test_collection_conflict_is_explicit(empty_store):
    source = manifest()
    assert ingest_export(empty_store, source).status == "committed"
    assert (
        ingest_export(empty_store, source, policy=IngestionPolicy(collection_id="other")).issues[0]["code"]
        == "collection_conflict"
    )
    source["execution"]["execution_id"] = "distinct"
    assert (
        ingest_export(empty_store, source, policy=IngestionPolicy(collection_id="other")).issues[0]["code"]
        == "collection_conflict"
    )
    assert empty_store.count("executions") == 1


def test_unreported_v2_has_only_nonranked_unknown_view_and_no_empty_site(tmp_path):
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        source = manifest(None)
        source["dataset"].update(visibility="unreported", visibility_source="unreported")
        result = ingest_export(store, source)
        q = Queries(store)
        assert q.leaderboard()["rows"] == [] and q.stats()["summary"]["n"] == 0
        unknown = q.unknown_context_rows()
        assert unknown["ranked"] is False and unknown["rows"][0]["execution_hash"] == result.execution_hash
        with pytest.raises(TypeError):
            Queries(store, include_unreported=True)
        with pytest.raises(ValueError, match="zero rankable"):
            _build_bundle(store, q)
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(root, tmp_path / "site")
    assert not (tmp_path / "site").exists()


def test_python_site_pack_has_only_rankable_rows(empty_store):
    for terminal in ("failed", "incomplete", "cancelled", None, "completed"):
        source = manifest(terminal)
        source["dataset"].update(visibility="public", visibility_source="producer_reported")
        assert ingest_export(empty_store, source).ok
    pack = _build_bundle(empty_store, Queries(empty_store))
    col = pack["tables"]["metrics"]["columns"]["execution_status"]["s"]
    assert {pack["strings"][i] for i in col} == {"completed"}
    assert pack["overview"]["executions"] == 1


def test_privacy_receipt_allowlist_and_legacy_read_refusal(empty_store):
    source = manifest("failed")
    source["execution"]["failure_message"] = "SENSITIVE_SAMPLE /home/private/source"
    source["residuals"] = residual_bundle()
    source["residuals"]["publishable"]["y_true"] = False
    source["unknown"] = {"sample_id": "SENSITIVE_SAMPLE"}
    source["task"]["extra"] = "SENSITIVE_SAMPLE"
    source["provenance"] = {
        "prov_jsonld": "SENSITIVE_SAMPLE",
        "historical_source": {
            "path": "/home/private/source",
            "exporter_path": "/home/private/exporter",
            "sha256": "d" * 64,
            "exporter_sha256": "e" * 64,
            "experiment_key": ["SENSITIVE_SAMPLE"],
            "extra": "SENSITIVE_SAMPLE",
        },
    }
    assert ingest_export(empty_store, source).issues[0]["code"] == "schema_invalid"
    del source["unknown"]
    del source["task"]["extra"]
    result = ingest_export(empty_store, source)
    assert result.ok
    path = empty_store.exports_dir / f"arena_run_{result.arena_export_hash}.json"
    receipt_text = empty_store.query_one("SELECT receipt_json FROM metadata_receipts")["receipt_json"]
    receipt = json.loads(receipt_text)
    assert receipt["sanitized"] and receipt["source_export_hash"] == result.arena_export_hash
    assert "SENSITIVE_SAMPLE" not in receipt_text and "/home/private" not in receipt_text
    assert receipt["provenance"]["historical_source"]["sha256"] == "d" * 64
    assert ingest_export(empty_store, receipt).issues[0]["code"] == "schema_invalid"
    assert Queries(empty_store).residuals(result.execution_hash)[0]["y_true"] is None
    assert Queries(empty_store).residuals(result.execution_hash)[0]["residual"] is None
    rset = empty_store.query_one("SELECT residual_set_id FROM residual_sets")
    old_rows = empty_store.residuals.read(rset["residual_set_id"])
    old_rows[0]["residual"] = 2.0  # Reproduce the pre-fix reconstructive residual.
    with pytest.raises(ValueError, match="residual_blob_conflict"):
        empty_store.residuals.write(rset["residual_set_id"], old_rows)
    import pyarrow as pa
    import pyarrow.parquet as pq

    from nirs4all_benchmarks.store.residual_store import RESIDUALS_ARROW_SCHEMA

    pq.write_table(
        pa.Table.from_pylist(old_rows, schema=RESIDUALS_ARROW_SCHEMA),
        empty_store.residuals.path_for(rset["residual_set_id"]),
    )
    assert Queries(empty_store).residuals(result.execution_hash)[0]["residual"] is None
    assert Queries(empty_store).residual_compare(result.execution_hash, result.execution_hash)["n_paired"] == 0
    # Reproduce a raw pre-fix export and failure string, leaving bytes unchanged.
    raw = json.dumps(source)
    path.write_text(raw)
    empty_store.conn.execute("UPDATE executions SET failure_message = ?", ("SENSITIVE_SAMPLE",))
    empty_store.conn.commit()
    assert read_stored_metadata(path, result.arena_export_hash)["reason"] == "legacy_unsanitized_export_refused"
    detail = Queries(empty_store).run_detail(result.execution_hash)
    assert detail["metadata"]["sanitized"]
    assert "SENSITIVE_SAMPLE" not in json.dumps(detail)
    assert path.read_text() == raw


def test_residual_comparison_refuses_unrelated_dataset_identity(empty_store):
    first = manifest()
    first["residuals"] = {**residual_bundle()}
    a = ingest_export(empty_store, first)
    second = manifest()
    second["dataset"]["dataset_fingerprint"] = "f" * 64
    second["task"]["task_hash"] = "d" * 64
    second["dataset_variant"]["dataset_variant_hash"] = "e" * 64
    second["residuals"] = {**residual_bundle()}
    b = ingest_export(empty_store, second)
    assert a.ok and b.ok
    with pytest.raises(ValueError, match="identity_mismatch"):
        Queries(empty_store).residual_compare(a.execution_hash, b.execution_hash)


def test_legacy_raw_files_never_reach_the_python_service_or_pack(tmp_path):
    fastapi = pytest.importorskip("fastapi.testclient")
    from nirs4all_benchmarks.service import create_app

    root = tmp_path / "legacy"
    with ArenaStore(root) as store:
        source = manifest()
        source["dataset"].update(visibility="public", visibility_source="producer_reported")
        result = ingest_export(store, source)
        path = store.exports_dir / f"arena_run_{result.arena_export_hash}.json"
        legacy = {
            **source,
            "residuals": {**residual_bundle()},
            "provenance": {"historical_source": {"path": "/SECRET_LOCAL_PATH"}},
        }
        raw = json.dumps(legacy)
        path.write_text(raw)
        store.conn.execute("UPDATE executions SET failure_message = 'SENSITIVE_SAMPLE'")
        store.conn.commit()
        pack = _build_bundle(store, Queries(store))
        assert pack["tables"]["metrics"]["n"] == 1
        assert "SECRET_LOCAL_PATH" not in json.dumps(pack)
    client = fastapi.TestClient(create_app(root))
    response = client.get(f"/api/run/{result.execution_hash}")
    assert response.status_code == 200
    assert response.json()["metadata"]["sanitized"] is True
    assert "SENSITIVE_SAMPLE" not in response.text and "SECRET_LOCAL_PATH" not in response.text
    assert path.read_text() == raw


def test_store_and_residual_symlinks_are_refused(tmp_path):
    from nirs4all_benchmarks.store.residual_store import ResidualStore

    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="arena_store_unsafe_path"):
        ArenaStore(link)
    arrays = ResidualStore(real)
    with pytest.raises(ValueError, match="unsafe_residual_set_id"):
        arrays.read("../../outside")
    (real / "residuals_x.parquet").symlink_to(tmp_path / "outside.parquet")
    with pytest.raises(ValueError, match="unsafe_residual_path"):
        arrays.read("x")


def test_receipt_write_failure_rolls_back_facts_and_blobs_then_retry_succeeds(empty_store, monkeypatch):
    import nirs4all_benchmarks.ingestion.ingest as module

    original = module.sanitize_manifest

    def fail(*args, **kwargs):
        raise OSError("synthetic disk failure")

    monkeypatch.setattr(module, "sanitize_manifest", fail)
    source = manifest()
    source["residuals"] = residual_bundle()
    with pytest.raises(OSError):
        ingest_export(empty_store, source)
    assert empty_store.count("executions") == empty_store.count("ingestion_batches") == 0
    assert list(empty_store.arrays_dir.iterdir()) == []
    monkeypatch.setattr(module, "sanitize_manifest", original)
    assert ingest_export(empty_store, source).status == "committed"


@pytest.mark.parametrize("identical", [True, False])
def test_concurrent_first_open_salt_and_dedup(tmp_path, identical):
    # Independent SQLite connections, synchronized before cold opens (S9/S10/A12).
    for repetition in range(12):
        root = tmp_path / str(repetition)
        barrier = threading.Barrier(6)

        def worker(number, barrier=barrier, root=root):
            barrier.wait(timeout=15)
            source = manifest(execution_id="same" if identical else str(number))
            source["residuals"] = {**residual_bundle()}
            with ArenaStore(root) as store:
                result = ingest_export(store, source)
                rows = Queries(store).residuals(result.execution_hash)
                return result, rows[0]["sample_id"]

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(worker, range(6)))
        assert len({pseudonym for _, pseudonym in results}) == 1
        assert sum(result.status == "committed" for result, _ in results) == (1 if identical else 6)
        with ArenaStore(root) as store:
            assert store.count("executions") == store.count("ingestion_batches") == (1 if identical else 6)
            assert all(
                store.get("ingestion_batches", "ingestion_batch_id", result.ingestion_batch_id) for result, _ in results
            )


def test_dagml_refuses_fabricated_dataset_identity_and_preserves_owner_metadata():
    source = {"bundle_id": "synthetic", "graph_fingerprint": "d" * 64, "status": "ok"}
    with pytest.raises(ValueError, match="identity_basis_required"):
        bundle_to_export(source)
    with pytest.raises(ValueError, match="identity_basis_required"):
        bundle_to_export(source, dataset_envelope={"schema_fingerprint": "a" * 64})
    envelope = {"dataset_fingerprint": "a" * 64, "schema_fingerprint": "b" * 64}
    owner = manifest(None)
    export = bundle_to_export(source, dataset_envelope=envelope, owner_export=owner)
    assert export.dataset.dataset_fingerprint == "a" * 64
    assert export.dataset.visibility == "private"
    assert export.leakage_attestation.model_dump() == owner["leakage_attestation"]
    assert validate_metadata(export).terminal_status == "unreported"
    assert export.arena_export_hash == export_hash(export.to_manifest())
    reports = [
        {
            "variant_label": "selected",
            "partition": "test",
            "scope": "test",
            "aggregation_level": "sample",
            "level": "test",
            "metrics": {"r2": 0.5},
            "n_samples": 3,
        }
    ]
    export = bundle_to_export(
        {**source, "selected_variant_id": "selected"},
        dataset_envelope=envelope,
        owner_export=owner,
        metric_reports=reports,
        execution={"status": "failed", "terminal_status": "incomplete"},
        score_context={"expected_fold_count": 5, "completed_fold_count": 2},
    )
    assert validate_metadata(export).terminal_status == "incomplete"
    assert export.scores.observations[0].partition == "test"
    assert export.scores.observations[0].scope == "test"
    assert export.scores.observations[0].n_samples == 3
    assert export.scores.observations[0].direction == "max"
    assert export.scores.expected_fold_count == 5
    with pytest.raises(ValueError, match="execution_status_conflict"):
        bundle_to_export({**source, "status": "failed"}, owner_export=manifest("completed"))
    contextual = bundle_to_export(
        {**source, "selected_variant_id": "selected"},
        owner_export=owner,
        metric_reports=[
            {
                "variant_label": "selected",
                "partition": "validation",
                "scope": "view",
                "level": "macro",
                "fold_id": 0,
                "metrics": {"rmse": 2.0},
            }
        ],
    )
    assert contextual.scores.observations[0].scope == "view"
    assert contextual.scores.observations[0].fold_id == "0"
    # A task or prediction shape that cannot be represented must refuse explicitly.
    with pytest.raises(ValueError, match="multitarget"):
        bundle_to_export(
            {**source, "selected_variant_id": "selected"},
            dataset_envelope=envelope,
            owner_export=owner,
            prediction_blocks=[
                {
                    "variant_label": "selected",
                    "sample_ids": ["s"],
                    "values": [[1.0, 2.0]],
                    "y_true": [[1.0, 2.0]],
                }
            ],
        )


def test_native_owner_export_is_the_cli_context_and_no_generic_success_is_invented(tmp_path):
    bundle = tmp_path / "bundle.json"
    bundle.write_text(json.dumps({"bundle_id": "synthetic", "graph_fingerprint": "d" * 64, "status": "ok"}))
    owner = tmp_path / "owner.json"
    source = manifest("incomplete")
    source["scores"]["observations"][0].update(scope="test", partition="test", n_samples=4, origin="recomputed")
    source["scores"].update(metric_implementation="owner/metrics/7", aggregation_policy="owner/policy")
    owner.write_text(json.dumps(source))
    store_path = tmp_path / "arena"
    refused = CliRunner().invoke(app, ["ingest-bundle", str(bundle), "--store", str(store_path)])
    assert refused.exit_code == 2
    assert not store_path.exists()
    accepted = CliRunner().invoke(
        app, ["ingest-bundle", str(bundle), "--metadata", str(owner), "--store", str(store_path)]
    )
    assert accepted.exit_code == 0, accepted.output
    assert "scientific_verification=not_performed" in accepted.output
    with ArenaStore(store_path) as store:
        q = Queries(store)
        rows = q.run_explorer(scope="test")
        assert rows[0]["execution_status"] == "incomplete"
        receipt = q.run_detail(rows[0]["execution_hash"])["metadata"]
        assert receipt["scores"]["aggregation_policy"] == "owner/policy"
        assert receipt["scores"]["observations"][0]["origin"] == "recomputed"
        assert not q.leaderboard(scope="test")["rows"]
