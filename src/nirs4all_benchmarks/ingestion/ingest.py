"""``ingest_export`` — the ``ArenaRunExport`` → ``ArenaStore`` state machine."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import ARENA_SCHEMA_VERSION
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.evidence import attestation_values, observation_direction
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.ingestion.metadata_bundle import sanitize_manifest
from nirs4all_benchmarks.ingestion.offline import MetadataValidation, terminal_status, validate_metadata
from nirs4all_benchmarks.ingestion.pseudonymize import Pseudonymizer
from nirs4all_benchmarks.ingestion.report import CleanReport
from nirs4all_benchmarks.ingestion.resolve import ResolvedExport
from nirs4all_benchmarks.ingestion.validate import validate_export
from nirs4all_benchmarks.scoring import ScoreComputationSpec, recompute_observations
from nirs4all_benchmarks.store import ArenaStore
from nirs4all_benchmarks.store.arena_store import utc_now


@dataclass
class IngestionPolicy:
    """How an ingestion behaves (DATA_MANAGEMENT.md §9 open decisions)."""

    collection_id: str = "default"
    collection_kind: str = "user_run_collection"  # or "benchmark_release"
    quarantine_on_leakage: bool | None = None  # default: True for releases, False for user runs
    residual_row_cap: int = 1_000_000  # per ResidualSet; fall back to scores-only above
    store_export_bundle: bool = True
    recompute_scores: bool = False  # always recompute even when observations are present
    bind_card_citation: bool = True

    def resolved_quarantine(self) -> bool:
        if self.quarantine_on_leakage is not None:
            return self.quarantine_on_leakage
        return self.collection_kind == "benchmark_release"


@dataclass
class IngestionResult:
    """Outcome of an ingestion attempt."""

    status: str  # committed | already_ingested | rejected | quarantined
    ingestion_batch_id: str
    run_condition_hash: str | None = None
    execution_hash: str | None = None
    arena_export_hash: str | None = None
    validity_status: str = "rejected"
    metadata_status: str = "rejected"
    scientific_verification: str = "not_performed"
    terminal_status: str = "unreported"
    clean_report: dict[str, Any] = field(default_factory=dict)
    issues: list[dict[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in ("committed", "already_ingested", "quarantined")


@contextmanager
def _ingestion_transaction(store: ArenaStore) -> Iterator[None]:
    """Retain blob ownership until commit; clean up before releasing the SQL lock."""
    owns_commit = not store._atomic_pack

    def keep_committed(path: Path) -> bool:
        # A signal can arrive just after commit, or SQLite can automatically
        # roll back a failed transaction. Reacquire the write lock before checking
        # references in that case; never unlink a now-committed inode.
        if not store.conn.in_transaction:
            store.conn.execute("BEGIN IMMEDIATE")
            store.residuals._rollback_reads_committed = True
        return store.residuals._rollback_reads_committed and store.query_one(
            "SELECT 1 FROM residual_sets WHERE parquet_path=? LIMIT 1",
            (str(path.relative_to(store.root)),),
        ) is not None

    with store.transaction():
        try:
            with store.residuals.transaction(keep_committed):
                yield
                if owns_commit:
                    store.conn.commit()
        except BaseException:
            if owns_commit:
                store.conn.rollback()
            raise


def _prepare_residual_rows(
    rows: list[dict[str, Any]],
    *,
    pseudo: Pseudonymizer,
    task_type: str | None,
    publishable: dict[str, bool],
    report: CleanReport,
) -> list[dict[str, Any]]:
    publish_true = publishable.get("y_true", True)
    publish_pred = publishable.get("y_pred", True) and publishable.get("residual", True)
    publish_proba = publishable.get("y_proba", True) and publishable.get("residual", True)
    is_regression = task_type == "regression"
    prepared: list[dict[str, Any]] = []
    dropped: set[str] = set()
    for r in rows:
        y_true = r.get("y_true") if publish_true else None
        y_pred = r.get("y_pred") if publish_pred else None
        y_proba = r.get("y_proba") if publish_proba else None
        if not publish_true and "y_true" in r:
            dropped.add("y_true")
        if not publish_pred and "y_pred" in r:
            dropped.add("y_pred")
        residual = r.get("residual") if (publish_true and publish_pred and publishable.get("residual", True)) else None
        if (residual is None and is_regression and publishable.get("residual", True)
                and y_true is not None and y_pred is not None):
            residual = float(y_true) - float(y_pred)
        prepared.append(
            {
                "sample_id": pseudo.map(str(r.get("sample_id"))) if r.get("sample_id") is not None else None,
                "group_id": pseudo.map(str(r["group_id"])) if r.get("group_id") is not None else None,
                "origin_sample_id": pseudo.map(str(r["origin_sample_id"]))
                if r.get("origin_sample_id") is not None
                else None,
                "scope": r["scope"],
                "fold_id": r.get("fold_id"),
                "partition": r["partition"],
                "y_true": float(y_true) if y_true is not None else None,
                "y_pred": float(y_pred) if y_pred is not None else None,
                "y_proba": [float(x) for x in y_proba] if y_proba is not None else None,
                "residual": float(residual) if residual is not None else None,
                "weight": float(r["weight"]) if r.get("weight") is not None else None,
            }
        )
    for fld in sorted(dropped):
        report.dropped_fields.append(f"residuals.{fld} (publication policy)")
    return prepared


def _split_instance_row(model: ArenaRunExport, resolved: ResolvedExport) -> dict[str, Any]:
    return {"split_instance_hash": resolved.split_instance_hash, "split_spec_hash": resolved.split_spec_hash,
            "partition_summary_json": canonical_json(model.split.partition_summary), "split_indices_hash": None}


def _cv_instance_row(model: ArenaRunExport, resolved: ResolvedExport) -> dict[str, Any]:
    # Only a declared actual fold set is an engine fold-set identity; an opaque CV id never is.
    fold_set = model.cv.fold_set_fingerprint
    summary = {"relation_fingerprint": model.dataset.relation_fingerprint} if fold_set is not None else {}
    return {"cv_instance_hash": resolved.cv_instance_hash, "cv_spec_hash": resolved.cv_spec_hash,
            "fold_summary_json": canonical_json(summary), "engine_fold_set_fingerprint": fold_set}


def _refit_dimension_row(model: ArenaRunExport, resolved: ResolvedExport) -> dict[str, Any]:
    return {
        "refit_strategy_hash": resolved.refit_strategy_hash,
        "strategy": model.refit.strategy,
        "selection_scope": model.refit.selection_scope,
        "train_scope": model.refit.train_scope,
        "params_json": canonical_json(model.refit.params),
    }


def ingest_export(
    store: ArenaStore,
    export: ArenaRunExport | dict[str, Any] | str | Path,
    *,
    policy: IngestionPolicy | None = None,
    _validated: MetadataValidation | None = None,
) -> IngestionResult:
    """Ingest verified metadata, not verified science. Dedup under a write lock."""
    policy = policy or IngestionPolicy()
    batch_id = f"ib_{uuid.uuid4().hex}"
    metadata = _validated or validate_metadata(export)
    result = IngestionResult(
        status="rejected", ingestion_batch_id=batch_id,
        arena_export_hash=metadata.arena_export_hash,
        metadata_status=metadata.status, terminal_status=metadata.terminal_status,
        issues=list(metadata.issues), clean_report={"metadata_validation": metadata.to_json()},
    )
    if metadata.status == "rejected":
        return result
    model, resolved = metadata.model, metadata.resolved
    assert model is not None and resolved is not None and metadata.arena_export_hash is not None
    export_hash = metadata.arena_export_hash
    from nirs4all_benchmarks.datasets import build_dataset_card_row
    from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
    from nirs4all_benchmarks.ingestion.submitted_metadata import binding_consistent, make_row, valid_row

    if compute_export_hash(model.to_manifest()) != export_hash:
        result.issues = [{"level": "error", "code": "identity_dimension_conflict",
                          "message": "A declared identity conflicts with stored metadata"}]
        return result

    dataset_block = model.dataset.model_dump(mode="json", exclude_none=True)
    dataset_block["visibility"] = metadata.visibility
    card_row = build_dataset_card_row(dataset_block, utc_now())
    card_hash = card_row["dataset_card_hash"] if card_row else None
    source_row = make_row(metadata, card_hash)
    outcome = validate_export(model, resolved, quarantine_on_leakage=policy.resolved_quarantine())
    report = CleanReport(source=f"{model.producer.capsule}:ArenaRunExport")
    report.issues = outcome.issues
    report.residual_key = outcome.residual_key
    result.issues = outcome.issues
    if outcome.rejected:
        return result

    # Preparation is outside the write transaction. The salt was committed during
    # provisioning; every concurrent writer uses the same salt.
    try:
        raw_residuals = [dict(row) for row in model.residuals.inline or []]
        if len(raw_residuals) > policy.residual_row_cap:
            report.residuals_truncated = True
            report.notes.append("residual row cap exceeded; stored scores-only")
            prepared_residuals = []
        else:
            prepared_residuals = _prepare_residual_rows(
                raw_residuals, pseudo=Pseudonymizer.for_store(store), task_type=model.task.task_type,
                publishable=dict(model.residuals.publishable), report=report,
            )
        observations = [obs.model_dump(mode="json") for obs in model.scores.observations]
        for observation, source_observation in zip(observations, model.scores.observations, strict=True):
            observation["direction"], observation["direction_source"] = observation_direction(source_observation)
            observation["origin"] = observation.get("origin") or model.scores.origin or "producer_reported"
        if policy.recompute_scores and prepared_residuals:
            if model.task.task_type is None or model.producer.capsule == "nirs4all-workspace" or any(
                obs.get("source_fold_id") is not None or (obs.get("aggregation_level") or "unreported") != "sample"
                for obs in observations
            ):
                result.issues = [{"level": "error", "code": "recompute_score_context_unsupported",
                                  "message": "Arena cannot recompute the preserved source aggregation context"}]
                return result
            implemented = ScoreComputationSpec()
            if any(getattr(resolved.score_spec, name) != getattr(implemented, name)
                   for name in ("metric_implementation", "score_version", "aggregation_policy")):
                result.issues = [{"level": "error", "code": "recompute_context_unsupported",
                                  "message": "Arena cannot recompute under the declared producer scoring context"}]
                return result
            observations = recompute_observations(prepared_residuals, model.task.task_type)
            for observation in observations:
                observation["origin"] = "recomputed"
                observation["direction_source"] = "registry"
            report.scores_recomputed = True
        # Validate derived/prepared JSON too, before SQLite can accept NaN as NULL.
        canonical_json([prepared_residuals, observations])
    except (ValueError, TypeError, KeyError, OverflowError):
        result.issues = [{"level": "error", "code": "residuals_invalid", "message": "Residual metadata is malformed"}]
        return result

    def clean_report():
        return {**report.to_json(), "metadata_validation": metadata.to_json(),
                "score_origins": sorted({o["origin"] for o in observations})}

    # Rejections never occupy the input hash slot. Reuse a legacy rejected slot
    # when present, avoiding the UNIQUE(input_export_hash, schema) retry trap.
    try:
        with _ingestion_transaction(store):
            existing = store.query_one(
                "SELECT * FROM ingestion_batches WHERE input_export_hash = ? AND arena_schema_version = ? "
                "ORDER BY arena_schema_version DESC LIMIT 1",
                (export_hash, ARENA_SCHEMA_VERSION),
            )
            prior_card = store.get("dataset_cards", "dataset_card_hash", card_hash) if card_hash else None
            if prior_card is not None:
                assert card_row is not None
            if prior_card is not None and card_row is not None and prior_card["card_json"] != card_row["card_json"]:
                try:
                    code = ("identity_dimension_conflict" if fingerprint(json.loads(prior_card["card_json"]))
                            == card_hash else "stored_batch_inconsistent")
                except (ValueError, TypeError):
                    code = "stored_batch_inconsistent"
                result.issues = [{"level": "error", "code": code,
                                  "message": "A declared identity conflicts with stored metadata"}]
                return result
            receipt = store.get("metadata_receipts", "arena_export_hash", export_hash)
            retained = store.get("submitted_metadata", "arena_export_hash", export_hash)
            try:
                receipt_valid = not receipt or (fingerprint(json.loads(receipt["receipt_json"]))
                                                == receipt["receipt_sha256"]
                                                and json.loads(receipt["receipt_json"]).get("source_export_hash")
                                                == export_hash)
            except (ValueError, TypeError, KeyError):
                receipt_valid = False
            if bool(receipt) != bool(retained) or not receipt_valid or (retained and not valid_row(retained)):
                result.issues = [{"level": "error", "code": "stored_batch_inconsistent",
                                  "message": "Stored metadata binding is inconsistent"}]
                return result
            if existing and existing["status"] in ("committed", "quarantined"):
                execution = store.query_one("SELECT * FROM executions WHERE ingestion_batch_id = ? LIMIT 1",
                                            (existing["ingestion_batch_id"],))
                if execution is None:
                    result.issues = [{"level": "error", "code": "stored_batch_inconsistent",
                                      "message": "Stored batch has no execution"}]
                    return result
                if existing["target_collection"] != policy.collection_id:
                    result.issues = [{"level": "error", "code": "collection_conflict",
                                      "message": "Export already belongs to another collection"}]
                    return result
                stored_card = (store.get("dataset_cards", "dataset_card_hash", retained["dataset_card_hash"])
                               if retained and retained["dataset_card_hash"] else None)
                try:
                    card_valid = (not retained or not retained["dataset_card_hash"] or
                                  (stored_card is not None and fingerprint(json.loads(stored_card["card_json"]))
                                   == retained["dataset_card_hash"]))
                except (ValueError, TypeError):
                    card_valid = False
                if ((policy.store_export_bundle and (not receipt or not retained)) or not card_valid
                    or (retained and receipt and not binding_consistent(execution, retained, receipt))):
                    result.issues = [{"level": "error", "code": "stored_batch_inconsistent",
                                      "message": "Stored metadata binding is inconsistent"}]
                    return result
                context = json.loads(retained["source_metadata_json"]).get("arena_context") if retained else None
                if context is not None and (context["pipeline"].get("human_label") != model.pipeline.human_label
                                            or context["task"].get("target_unit") != model.task.target_unit):
                    result.issues = [{"level": "error", "code": "stored_batch_inconsistent",
                                      "message": "Stored metadata binding is inconsistent"}]
                    return result
                # Replaying the same export keeps its stored execution identity even if derivation later changes.
                if retained and any(retained[k] != source_row[k] for k in (
                    "dataset_card_hash", "normalized_provenance_sha256",
                    "normalized_dataset_card_sha256"
                )):
                    result.issues = [{"level": "error", "code": "identity_dimension_conflict",
                                      "message": "A declared identity conflicts with stored metadata"}]
                    return result
                return IngestionResult(
                    status="already_ingested", ingestion_batch_id=existing["ingestion_batch_id"],
                    arena_export_hash=export_hash, execution_hash=execution["execution_hash"],
                    run_condition_hash=execution["run_condition_hash"], validity_status=execution["validity_status"],
                    metadata_status="metadata_valid",
                    terminal_status=metadata.terminal_status,
                    clean_report=json.loads(existing["clean_report_json"]),
                )
            if receipt or retained:
                result.issues = [{"level": "error", "code": "stored_batch_inconsistent",
                                  "message": "Stored metadata binding is inconsistent"}]
                return result
            owner_name = model.execution.owner_pipeline_name
            if owner_name is not None and store.query_one(
                "SELECT execution_hash FROM executions WHERE run_condition_hash=? "
                "AND owner_pipeline_name IS NOT NULL AND owner_pipeline_name!=? LIMIT 1",
                (resolved.run_condition_hash, owner_name),
            ):
                result.issues = [{"level":"error", "code":"workspace_identity_collision",
                                  "message":"Owner pipeline identity collision"}]
                return result
            for table, key_name, identity, expected in (
                ("task_specs", "task_hash", resolved.task_hash,
                 {"dataset_fingerprint":resolved.dataset_fingerprint, "task_type":model.task.task_type,
                  "target_name":model.task.target_name, "target_hash":model.task.target_hash,
                  "target_unit":model.task.target_unit}),
                ("dataset_variants", "dataset_variant_hash", resolved.dataset_variant_hash,
                 {"dataset_fingerprint":resolved.dataset_fingerprint, "task_hash":resolved.task_hash,
                  "variant_spec_json":canonical_json(model.dataset_variant.variant_spec)}),
                ("split_instances", "split_instance_hash", resolved.split_instance_hash,
                 {key: _split_instance_row(model, resolved)[key]
                  for key in ("split_spec_hash", "partition_summary_json")}),
                ("cv_instances", "cv_instance_hash", resolved.cv_instance_hash,
                 {key: _cv_instance_row(model, resolved)[key]
                  for key in ("cv_spec_hash", "fold_summary_json", "engine_fold_set_fingerprint")
                  if key == "cv_spec_hash" or model.cv.fold_set_fingerprint is not None}),
                ("refit_strategies", "refit_strategy_hash", resolved.refit_strategy_hash,
                 _refit_dimension_row(model, resolved)),
                ("rng_contexts", "rng_context_hash", resolved.rng_context_hash,
                 {"root_seed": str(model.rng.root_seed) if model.rng.root_seed is not None else None,
                  "derivation": model.rng.derivation,
                  "framework_seeds_json": canonical_json(model.rng.framework_seeds),
                  "determinism_flags_json": canonical_json(model.rng.determinism_flags)}),
            ):
                existing_dimension = store.get(table, key_name, identity)
                if existing_dimension is not None and any(
                    existing_dimension[name] != value for name, value in expected.items()
                ):
                    result.issues = [{"level":"error", "code":"identity_dimension_conflict",
                                      "message":"A declared identity conflicts with stored metadata"}]
                    return result
            declared_units: dict[str, set[str | None]] = {}
            for observation in observations:
                declared_units.setdefault(observation["metric_name"].casefold(), set()).add(
                    observation.get("metric_unit"))
            stored_units: dict[str, set[str | None]] = {}
            for row in store.query(
                "SELECT DISTINCT mo.metric_name, mo.metric_unit FROM metric_observations mo "
                "JOIN score_sets ss ON ss.score_set_id=mo.score_set_id "
                "JOIN executions e ON e.execution_hash=ss.execution_hash "
                "JOIN run_conditions rc ON rc.run_condition_hash=e.run_condition_hash WHERE rc.task_hash=?",
                (resolved.task_hash,),
            ):
                stored_units.setdefault(row["metric_name"].casefold(), set()).add(row["metric_unit"])
            if any(len(units) != 1 or (name in stored_units and stored_units[name] != units)
                   for name, units in declared_units.items()):
                result.issues = [{"level": "error", "code": "metric_unit_conflict",
                                  "message": "A score unit conflicts with stored metadata"}]
                return result
            prior_execution = store.get("executions", "execution_hash", resolved.execution_hash)
            if prior_execution is not None:
                result.issues = [{"level": "error", "code": "execution_identity_conflict",
                                  "message": "Execution already exists with a different source export"}]
                return result
            condition = store.get("run_conditions", "run_condition_hash", resolved.run_condition_hash)
            if condition and condition["collection_id"] != policy.collection_id:
                result.issues = [{"level": "error", "code": "collection_conflict",
                                  "message": "Run condition already belongs to another collection"}]
                return result
            if existing:
                batch_id = existing["ingestion_batch_id"]
                store.update("ingestion_batches", "ingestion_batch_id", batch_id,
                             _batch_row(batch_id, export_hash, policy, "staging", report, outcome.issues))
            else:
                store.upsert("ingestion_batches", _batch_row(
                    batch_id, export_hash, policy, "staging", report, outcome.issues))
            store.ensure_collection(policy.collection_id, kind=policy.collection_kind)
            _write_dimensions(store, model, resolved, report, visibility=metadata.visibility,
                              bind_card_citation=policy.bind_card_citation)
            validity = "quarantined" if outcome.validity_status == "quarantined" else "valid"
            _write_facts(store, model, resolved, observations, prepared_residuals,
                         batch_id, policy.collection_id, export_hash, validity, report, visibility=metadata.visibility)
            if report.facts_written.get("executions") != 1:
                raise RuntimeError("ingestion wrote no execution facts")
            final_status = "quarantined" if validity == "quarantined" else "committed"
            store.update("ingestion_batches", "ingestion_batch_id", batch_id, {
                "status": final_status, "n_dimensions": sum(report.new_dimensions.values()),
                "n_facts": sum(report.facts_written.values()), "clean_report_json": canonical_json(clean_report()),
                "errors_json": canonical_json(outcome.issues),
            })
            if policy.store_export_bundle:
                # The receipt is the only preserved provenance record. Write/rename
                # BEFORE committing, so a failed write cannot leave a committed batch.
                receipt_data = model.to_manifest()
                receipt_data["scores"]["observations"] = observations
                receipt_model = ArenaRunExport.model_validate(receipt_data)
                receipt = sanitize_manifest(receipt_model, export_hash, metadata.to_json())
                store.upsert("metadata_receipts", {"arena_export_hash": export_hash,
                             "receipt_json": canonical_json(receipt), "receipt_sha256": fingerprint(receipt)})
                store.insert("submitted_metadata", source_row)
                stored_row = store.get("submitted_metadata", "arena_export_hash", export_hash)
                if stored_row is None or not valid_row(stored_row):
                    raise RuntimeError("stored metadata binding is inconsistent")

    except (sqlite3.ProgrammingError, sqlite3.InterfaceError, sqlite3.IntegrityError, OverflowError):
        result.issues = [{"level": "error", "code": "metadata_not_indexable",
                          "message": "Metadata cannot be represented by the Arena store"}]
        return result

    return IngestionResult(
        status=final_status, ingestion_batch_id=batch_id, run_condition_hash=resolved.run_condition_hash,
        execution_hash=resolved.execution_hash, arena_export_hash=export_hash, validity_status=validity,
        metadata_status="metadata_valid", terminal_status=metadata.terminal_status,
        clean_report=clean_report(), issues=outcome.issues,
    )


def _batch_row(
    batch_id: str,
    export_hash: str,
    policy: IngestionPolicy,
    status: str,
    report: CleanReport,
    issues: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "ingestion_batch_id": batch_id,
        "source": report.source,
        "input_export_hash": export_hash,
        "arena_schema_version": ARENA_SCHEMA_VERSION,
        "target_collection": policy.collection_id,
        "status": status,
        "n_dimensions": sum(report.new_dimensions.values()),
        "n_facts": sum(report.facts_written.values()),
        "clean_report_json": canonical_json(report.to_json()),
        "errors_json": canonical_json(issues),
        "created_at": utc_now(),
    }


def _write_dimensions(
    store: ArenaStore, model: ArenaRunExport, r: ResolvedExport, report: CleanReport, *, visibility: str,
    bind_card_citation: bool = True,
) -> None:
    from nirs4all_benchmarks.datasets import build_dataset_card_row, build_dataset_fingerprint_row

    now = utc_now()
    dataset_block = model.dataset.model_dump(mode="json", exclude_none=True)
    dataset_block["visibility"] = visibility

    card_row = build_dataset_card_row(dataset_block, now)
    card_hash = card_row["dataset_card_hash"] if card_row else None
    if card_row:
        if not bind_card_citation:
            card_row["citation_id"] = None
        report.record_upsert("dataset_cards", store.upsert("dataset_cards", card_row))
    report.record_upsert(
        "dataset_fingerprints",
        store.upsert("dataset_fingerprints", build_dataset_fingerprint_row(dataset_block, now, card_hash)),
    )

    report.record_upsert(
        "task_specs",
        store.upsert(
            "task_specs",
            {
                "task_hash": r.task_hash,
                "dataset_fingerprint": r.dataset_fingerprint,
                "task_type": model.task.task_type,
                "target_name": model.task.target_name,
                "target_unit": model.task.target_unit,
                "target_hash": model.task.target_hash,
                "encoding_json": canonical_json(model.task.encoding),
                "created_at": now,
            },
        ),
    )

    variant_spec = model.dataset_variant.variant_spec
    report.record_upsert(
        "dataset_variants",
        store.upsert(
            "dataset_variants",
            {
                "dataset_variant_hash": r.dataset_variant_hash,
                "dataset_fingerprint": r.dataset_fingerprint,
                "task_hash": r.task_hash,
                "variant_spec_json": canonical_json(variant_spec),
                "sample_manifest_hash": model.dataset_variant.sample_manifest_hash,
                "size_label": str(variant_spec.get("size") or "unreported"),
                "aggregation": str(variant_spec.get("aggregation") or "unreported"),
                "created_at": now,
            },
        ),
    )

    # Pipeline DAG + nodes + edges + operators + params
    norm = r.pipeline_id.normalized_graph
    terminals = _terminal_nodes(norm)
    entries = _entry_nodes(norm)
    report.record_upsert(
        "pipeline_dags",
        store.upsert(
            "pipeline_dags",
            {
                "pipeline_dag_hash": r.pipeline_dag_hash,
                "dag_schema_version": norm.get("schema", "arena.graph/1"),
                "graph_json": canonical_json(norm),
                "entry_nodes_json": canonical_json(entries),
                "terminal_nodes_json": canonical_json(terminals),
                "n_nodes": len(norm["nodes"]),
                "is_linear": int(r.pipeline_id.is_linear),
                "main_model": r.main_model,
                "human_label": model.pipeline.human_label,
                "engine_graph_fingerprint": r.pipeline_id.engine_graph_fingerprint,
                "nirs4all_identity_hash": r.pipeline_id.nirs4all_identity_hash,
                "created_at": now,
            },
        ),
    )
    for op in r.operator_rows:
        report.record_upsert("operator_specs", store.upsert("operator_specs", {**op, "created_at": now}))
    for pv in r.param_value_rows:
        report.record_upsert("parameter_values", store.upsert("parameter_values", pv))
    for node in r.node_rows:
        inserted = store.upsert("pipeline_nodes", {"pipeline_dag_hash": r.pipeline_dag_hash, **node})
        report.record_upsert("pipeline_nodes", inserted)
    for np_row in r.node_param_rows:
        store.upsert("pipeline_node_params", {"pipeline_dag_hash": r.pipeline_dag_hash, **np_row})
    for edge in r.edge_rows:
        store.upsert("pipeline_edges", {"pipeline_dag_hash": r.pipeline_dag_hash, **edge})

    # Split / CV / RNG / refit specs + instances
    report.record_upsert(
        "split_specs",
        store.upsert("split_specs", {
            "split_spec_hash": r.split_spec_hash, "method": model.split.method,
            "params_json": canonical_json(model.split.params),
            "group_policy": None,
            "stratification_policy": None,
        }),
    )
    report.record_upsert(
        "split_instances",
        store.upsert("split_instances", _split_instance_row(model, r)),
    )
    report.record_upsert(
        "cv_specs",
        store.upsert(
            "cv_specs",
            {
                "cv_spec_hash": r.cv_spec_hash,
                "method": model.cv.method,
                "n_folds": model.cv.n_folds,
                "n_repeats": model.cv.n_repeats,
                "nested": None if model.cv.nested is None else int(model.cv.nested),
                "within_train_only": None if model.cv.within_train_only is None else int(model.cv.within_train_only),
                "params_json": canonical_json({}),
            },
        ),
    )
    report.record_upsert(
        "cv_instances",
        store.upsert("cv_instances", _cv_instance_row(model, r)),
    )
    report.record_upsert(
        "rng_contexts",
        store.upsert("rng_contexts", {
            "rng_context_hash": r.rng_context_hash,
            "root_seed": str(model.rng.root_seed) if model.rng.root_seed is not None else None,
            "derivation": model.rng.derivation,
            "framework_seeds_json": canonical_json(model.rng.framework_seeds),
            "determinism_flags_json": canonical_json(model.rng.determinism_flags),
        }),
    )
    report.record_upsert(
        "refit_strategies",
        store.upsert("refit_strategies", _refit_dimension_row(model, r)),
    )
    report.record_upsert("score_computation_specs", store.upsert("score_computation_specs", r.score_spec.to_row(now)))


def _write_facts(
    store: ArenaStore,
    model: ArenaRunExport,
    r: ResolvedExport,
    observations: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
    batch_id: str,
    collection_id: str,
    export_hash: str,
    validity: str,
    report: CleanReport,
    *, visibility: str,
) -> None:
    now = utc_now()
    report.record_fact(
        "run_conditions",
        int(store.upsert("run_conditions", {
            "run_condition_hash": r.run_condition_hash,
            "collection_id": collection_id,
            "dataset_variant_hash": r.dataset_variant_hash,
            "split_instance_hash": r.split_instance_hash,
            "cv_instance_hash": r.cv_instance_hash,
            "rng_context_hash": r.rng_context_hash,
            "pipeline_dag_hash": r.pipeline_dag_hash,
            "refit_strategy_hash": r.refit_strategy_hash,
            "task_hash": r.task_hash,
            "dataset_fingerprint": r.dataset_fingerprint,
            "created_at": now,
        })),
    )
    # Role-aware facets (split / aug / pp / model / params …) for the pivot dataviz.
    from nirs4all_benchmarks.indexing import build_run_facets

    for facet in build_run_facets(r, model):
        store.upsert("run_facets", facet)
    # A planned run for this (pipeline, dataset) is now fulfilled by a real execution.
    if terminal_status(model) == "completed" and validity == "valid":
        store.conn.execute(
            "UPDATE planned_runs SET status = 'fulfilled' WHERE pipeline_dag_hash = ? AND dataset_fingerprint = ?",
            (r.pipeline_dag_hash, r.dataset_fingerprint),
        )
    att = model.leakage_attestation
    if store.upsert("executions", {
        "execution_hash": r.execution_hash,
        "execution_id": model.execution.execution_id,
        "owner_pipeline_name": model.execution.owner_pipeline_name,
        "visibility": visibility,
        "identity_basis_json": canonical_json(model.identity_basis.model_dump(mode="json")),
        "identity_rankable": int(_identity_rankable(model)),
        "run_condition_hash": r.run_condition_hash,
        "ingestion_batch_id": batch_id,
        "arena_export_hash": export_hash,
        "producer_capsule": model.producer.capsule,
        "nirs4all_version": model.producer.nirs4all_version,
        "dag_ml_version": model.producer.dag_ml_version,
        "dag_ml_data_version": model.producer.dag_ml_data_version,
        "io_version": model.producer.io_version,
        "os": model.execution.os,
        "hardware": model.execution.hardware,
        "time_ms": model.execution.time_ms,
        "peak_mem_mb": model.execution.peak_mem_mb,
        "status": terminal_status(model),
        "failure_code": model.execution.failure_code,
        "failure_message": "[redacted]" if model.execution.failure_message is not None else None,
        **{name: None if value is None else int(value) for name,value in attestation_values(att).items()},
        "unsafe_flags_json": canonical_json(sorted(att.unsafe_flags)) if att.unsafe_flags is not None else "null",
        "validity_status": validity,
        "created_at": now,
    }):
        report.record_fact("executions")

    # One ScoreSet per score scope, so v_run_metrics.score_scope is accurate and a
    # cv leaderboard never blends test/refit observations (DATA_MANAGEMENT.md §6).
    by_scope: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        by_scope.setdefault(str(obs.get("scope") or "unreported"), []).append(obs)

    score_set_ids: dict[str, str] = {}
    for scope, obs_list in sorted(by_scope.items()):
        score_set_id = fingerprint(
            {"execution": r.execution_hash, "score_spec": r.score_spec.score_computation_hash, "scope": scope}
        )
        score_set_ids[scope] = score_set_id
        # Supersede any prior *valid* score set for this execution + scope — a
        # recompute under a new score_computation_hash gets a new id and the old
        # one is marked superseded (versioned scoring; never mutated).
        prior = store.query_one(
            "SELECT score_set_id FROM score_sets WHERE execution_hash = ? AND scope = ? "
            "AND score_set_id != ? AND validity_status = 'valid'",
            (r.execution_hash, scope, score_set_id),
        )
        inserted = store.upsert("score_sets", {
            "score_set_id": score_set_id,
            "execution_hash": r.execution_hash,
            "score_computation_hash": r.score_spec.score_computation_hash,
            "scope": scope,
            "supersedes_score_set_id": prior["score_set_id"] if prior else None,
            "validity_status": "valid",
            "created_at": now,
        })
        if prior:
            store.update("score_sets", "score_set_id", prior["score_set_id"], {"validity_status": "superseded"})
        if inserted:
            report.record_fact("score_sets")
            for obs in obs_list:
                store.insert("metric_observations", {
                    "score_set_id": score_set_id,
                    "metric_name": obs["metric_name"],
                    "metric_value": obs.get("metric_value"),
                    "metric_unit": obs.get("metric_unit"),
                    "direction": obs.get("direction"),
                    "fold_id": obs.get("fold_id"),
                    "source_fold_id": obs.get("source_fold_id"),
                    "partition": obs.get("partition") or "unreported",
                    "aggregation_level": obs.get("aggregation_level") or "unreported",
                    "n_samples": obs.get("n_samples"),
                    "coverage": obs.get("coverage"),
                })
            report.record_fact("metric_observations", len(obs_list))

    if residual_rows:
        residual_set_id = fingerprint({"execution": r.execution_hash, "key": report.residual_key})
        store.residuals.write(residual_set_id, residual_rows)
        partitions = sorted({row["partition"] for row in residual_rows})
        linked_score_set = score_set_ids.get("cv") or next(iter(score_set_ids.values()), None)
        if store.upsert("residual_sets", {
            "residual_set_id": residual_set_id,
            "execution_hash": r.execution_hash,
            "score_set_id": linked_score_set,
            "key": report.residual_key,
            "partition_set_json": canonical_json(partitions),
            "parquet_path": str(store.residuals.path_for(residual_set_id).relative_to(store.root)),
            "n_rows": len(residual_rows),
            "pseudonymized": 1,
            "publishable_json": canonical_json(dict(model.residuals.publishable)),
            "validity_status": validity,
            "created_at": now,
        }):
            report.record_fact("residual_sets")
        report.residual_rows = len(residual_rows)


def _terminal_nodes(graph: dict[str, Any]) -> list[str]:
    has_out = {e["src"] for e in graph["edges"]}
    return [n["id"] for n in graph["nodes"] if n["id"] not in has_out]


def _entry_nodes(graph: dict[str, Any]) -> list[str]:
    has_in = {e["dst"] for e in graph["edges"]}
    return [n["id"] for n in graph["nodes"] if n["id"] not in has_in]


def _identity_rankable(model: ArenaRunExport) -> bool:
    if model.task.task_type is None:
        return False
    for name in ("dataset_fingerprint", "task_hash", "dataset_variant_hash"):
        basis = getattr(model.identity_basis, name)
        if basis.kind == "source_declared_unverified":
            return False
        if (basis.kind == "arena_derived" and name == "task_hash"
                and (basis.inputs.get("namespace") != "arena.task/2"
                     or not basis.inputs.get("dataset_name")
                     or not basis.inputs.get("target_context_sha256"))):
            return False
    return True


class AtomicIngestionError(ValueError):
    """A whole-pack refusal; none of that pack's facts or receipts are committed."""

    def __init__(self, code: str, ordinal: int):
        self.code, self.ordinal = code, ordinal
        super().__init__(code)


def ingest_exports_atomic(
    store: ArenaStore, exports: Sequence[ArenaRunExport | dict[str, Any]], *,
    policy: IngestionPolicy | None = None, collection_metadata: dict[str, Any] | None = None,
    progress: Any = None,
) -> list[IngestionResult]:
    policy = policy or IngestionPolicy()
    validations = [validate_metadata(export) for export in exports]
    for ordinal, validation in enumerate(validations):
        if validation.status != "metadata_valid":
            raise AtomicIngestionError(validation.issues[0]["code"], ordinal)
    results = []
    with _ingestion_transaction(store):
        store._atomic_pack = True
        try:
            if collection_metadata is not None:
                existing_collection = store.get("collections", "collection_id", policy.collection_id)
                if (existing_collection is not None and existing_collection.get("metadata_json") not in {
                    None, "{}", canonical_json(collection_metadata)
                }):
                    raise AtomicIngestionError("collection_metadata_conflict", 0)
                store.ensure_collection(policy.collection_id, kind=policy.collection_kind)
                store.update("collections", "collection_id", policy.collection_id,
                             {"metadata_json": canonical_json(collection_metadata)})
            for ordinal, (export, validation) in enumerate(zip(exports, validations, strict=True)):
                result = ingest_export(store, export, policy=policy, _validated=validation)
                if not result.ok:
                    raise AtomicIngestionError(result.issues[0]["code"], ordinal)
                results.append(result)
                if progress is not None and ((ordinal+1) % 500 == 0 or ordinal+1 == len(exports)):
                    progress(ordinal+1, len(exports))
        finally:
            store._atomic_pack = False
    return results
