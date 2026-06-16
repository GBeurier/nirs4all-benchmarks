"""``ingest_export`` — the ``ArenaRunExport`` → ``ArenaStore`` state machine."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import ARENA_EXPORT_SCHEMA_VERSION, ARENA_SCHEMA_VERSION
from nirs4all_benchmarks.contract import ArenaRunExport, validate_manifest
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
from nirs4all_benchmarks.ingestion.pseudonymize import Pseudonymizer
from nirs4all_benchmarks.ingestion.report import CleanReport
from nirs4all_benchmarks.ingestion.resolve import ResolvedExport, resolve_identities
from nirs4all_benchmarks.ingestion.validate import validate_export
from nirs4all_benchmarks.scoring import recompute_observations
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
    validity_status: str = "valid"
    clean_report: dict[str, Any] = field(default_factory=dict)
    issues: list[dict[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in ("committed", "already_ingested", "quarantined")


def _load_export(export: ArenaRunExport | dict[str, Any] | str | Path) -> ArenaRunExport:
    if isinstance(export, ArenaRunExport):
        return export
    data = json.loads(Path(export).read_text(encoding="utf-8")) if isinstance(export, (str, Path)) else export
    return ArenaRunExport.model_validate(data)


def _residual_rows(export: ArenaRunExport, base_dir: Path | None) -> list[dict[str, Any]]:
    if export.residuals.inline:
        return [dict(r) for r in export.residuals.inline]
    ref = export.residuals.ref
    if ref and base_dir is not None:
        path = base_dir / ref
        if path.exists():
            import pyarrow.parquet as pq

            return pq.read_table(path).to_pylist()
    return []


def _prepare_residual_rows(
    rows: list[dict[str, Any]],
    *,
    pseudo: Pseudonymizer,
    task_type: str,
    publishable: dict[str, bool],
    report: CleanReport,
) -> list[dict[str, Any]]:
    publish_true = publishable.get("y_true", True)
    publish_pred = publishable.get("y_pred", True)
    publish_proba = publishable.get("y_proba", True)
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
        residual = r.get("residual")
        if residual is None and is_regression and y_true is not None and y_pred is not None:
            residual = float(y_true) - float(y_pred)
        prepared.append(
            {
                "sample_id": pseudo.map(str(r.get("sample_id"))) if r.get("sample_id") is not None else None,
                "group_id": pseudo.map(str(r["group_id"])) if r.get("group_id") is not None else None,
                "origin_sample_id": pseudo.map(str(r["origin_sample_id"]))
                if r.get("origin_sample_id") is not None
                else None,
                "scope": r.get("scope") or "cv",
                "fold_id": r.get("fold_id"),
                "partition": r.get("partition") or "validation",
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


def ingest_export(
    store: ArenaStore,
    export: ArenaRunExport | dict[str, Any] | str | Path,
    *,
    policy: IngestionPolicy | None = None,
) -> IngestionResult:
    """Ingest one ``ArenaRunExport`` into the store (idempotent, leakage-honest)."""
    policy = policy or IngestionPolicy()
    base_dir = Path(export).parent if isinstance(export, (str, Path)) else None
    batch_id = f"ib_{uuid.uuid4().hex}"

    # ── 1. VERIFY ──────────────────────────────────────────────────────
    model = _load_export(export)
    if model.arena_export_schema_version != ARENA_EXPORT_SCHEMA_VERSION:
        return IngestionResult(
            status="rejected",
            ingestion_batch_id=batch_id,
            issues=[
                {
                    "level": "error",
                    "code": "unsupported_export_version",
                    "message": f"export schema v{model.arena_export_schema_version} != "
                    f"supported v{ARENA_EXPORT_SCHEMA_VERSION}",
                }
            ],
        )
    manifest = model.to_manifest()
    schema_errors = validate_manifest(manifest)
    if schema_errors:
        return IngestionResult(
            status="rejected",
            ingestion_batch_id=batch_id,
            issues=[{"level": "error", "code": "schema_invalid", "message": e} for e in schema_errors],
        )
    export_hash = compute_export_hash(manifest)

    # ── 2. DEDUP-CHECK ─────────────────────────────────────────────────
    # Idempotency is GLOBAL across collections: the execution/run-condition facts
    # are content-hashed and exclude the collection, and the store keys a run
    # condition to a single (origin) collection. So a given export is ingested
    # once; re-ingesting it (to the same or any other collection) is a truthful
    # no-op that returns the original facts, rather than falsely reporting a
    # second commit that lands nothing. (Multi-collection membership is a future
    # enhancement; see docs/INGESTION.md.)
    existing = store.query_one(
        "SELECT * FROM ingestion_batches WHERE input_export_hash = ? AND arena_schema_version = ? "
        "AND status IN ('committed', 'quarantined') LIMIT 1",
        (export_hash, ARENA_SCHEMA_VERSION),
    )
    if existing:
        existing_exec = store.query_one(
            "SELECT execution_hash, run_condition_hash, validity_status FROM executions "
            "WHERE ingestion_batch_id = ? LIMIT 1",
            (existing["ingestion_batch_id"],),
        )
        return IngestionResult(
            status="already_ingested",
            ingestion_batch_id=existing["ingestion_batch_id"],
            run_condition_hash=(existing_exec or {}).get("run_condition_hash"),
            execution_hash=(existing_exec or {}).get("execution_hash"),
            arena_export_hash=export_hash,
            validity_status=(existing_exec or {}).get("validity_status", "valid"),
            clean_report=json.loads(existing["clean_report_json"]),
        )

    # ── 3. RESOLVE IDS ─────────────────────────────────────────────────
    resolved = resolve_identities(model)

    # ── 4. VALIDATE ────────────────────────────────────────────────────
    outcome = validate_export(model, resolved, quarantine_on_leakage=policy.resolved_quarantine())
    report = CleanReport(source=f"{model.producer.capsule}:ArenaRunExport")
    report.issues = outcome.issues
    report.residual_key = outcome.residual_key
    if outcome.rejected:
        store.upsert(
            "ingestion_batches",
            _batch_row(batch_id, export_hash, policy, "rejected", report, outcome.issues),
        )
        store.conn.commit()
        return IngestionResult(
            status="rejected",
            ingestion_batch_id=batch_id,
            arena_export_hash=export_hash,
            issues=outcome.issues,
            clean_report=report.to_json(),
        )

    # ── 5/6. PSEUDONYMIZE + STRIP residuals ────────────────────────────
    pseudo = Pseudonymizer.for_store(store)
    raw_residuals = _residual_rows(model, base_dir)
    residuals_truncated = len(raw_residuals) > policy.residual_row_cap
    if residuals_truncated:
        report.residuals_truncated = True
        report.notes.append(
            f"residuals ({len(raw_residuals)} rows) exceed cap {policy.residual_row_cap}; stored scores-only"
        )
        prepared_residuals: list[dict[str, Any]] = []
    else:
        prepared_residuals = _prepare_residual_rows(
            raw_residuals,
            pseudo=pseudo,
            task_type=model.task.task_type,
            publishable=dict(model.residuals.publishable),
            report=report,
        )

    # Scores: provided observations, or recompute from residuals.
    observations = [obs.model_dump(mode="json") for obs in model.scores.observations]
    if (policy.recompute_scores or not observations) and prepared_residuals:
        observations = recompute_observations(prepared_residuals, model.task.task_type)
        report.scores_recomputed = True

    # ── 7. STAGE→COMMIT ────────────────────────────────────────────────
    with store.transaction():
        store.ensure_collection(policy.collection_id, kind=policy.collection_kind)
        # Stage the batch first so fact rows can reference it (FK ordering).
        store.upsert("ingestion_batches", _batch_row(batch_id, export_hash, policy, "staging", report, outcome.issues))
        _write_dimensions(store, model, resolved, report)
        validity = "quarantined" if outcome.validity_status == "quarantined" else "valid"
        _write_facts(
            store, model, resolved, observations, prepared_residuals,
            batch_id, policy.collection_id, export_hash, validity, report,
        )
        final_status = "quarantined" if validity == "quarantined" else "committed"
        store.update(
            "ingestion_batches", "ingestion_batch_id", batch_id,
            {
                "status": final_status,
                "n_dimensions": sum(report.new_dimensions.values()),
                "n_facts": sum(report.facts_written.values()),
                "clean_report_json": canonical_json(report.to_json()),
                "errors_json": canonical_json(outcome.issues),
            },
        )
        if policy.store_export_bundle:
            bundle_path = store.exports_dir / f"arena_run_{export_hash}.json"
            bundle_path.write_text(canonical_json(manifest), encoding="utf-8")

    status = "quarantined" if validity == "quarantined" else "committed"
    return IngestionResult(
        status=status,
        ingestion_batch_id=batch_id,
        run_condition_hash=resolved.run_condition_hash,
        execution_hash=resolved.execution_hash,
        arena_export_hash=export_hash,
        validity_status=validity,
        clean_report=report.to_json(),
        issues=outcome.issues,
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


def _write_dimensions(store: ArenaStore, model: ArenaRunExport, r: ResolvedExport, report: CleanReport) -> None:
    from nirs4all_benchmarks.datasets import build_dataset_card_row, build_dataset_fingerprint_row

    now = utc_now()
    dataset_block = model.dataset.model_dump(mode="json", exclude_none=True)

    card_row = build_dataset_card_row(dataset_block, now)
    card_hash = card_row["dataset_card_hash"] if card_row else None
    if card_row:
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
                "size_label": str(variant_spec.get("size", "all")),
                "aggregation": str(variant_spec.get("aggregation", "none")),
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
    split_extra = model.split.model_extra or {}
    report.record_upsert(
        "split_specs",
        store.upsert("split_specs", {
            "split_spec_hash": r.split_spec_hash, "method": model.split.method,
            "params_json": canonical_json(model.split.params),
            "group_policy": split_extra.get("group_policy"),
            "stratification_policy": split_extra.get("stratification_policy"),
        }),
    )
    report.record_upsert(
        "split_instances",
        store.upsert("split_instances", {
            "split_instance_hash": r.split_instance_hash, "split_spec_hash": r.split_spec_hash,
            "rng_context_hash": r.rng_context_hash,
            "partition_summary_json": canonical_json(model.split.partition_summary),
            "split_indices_hash": split_extra.get("split_indices_hash"),
        }),
    )
    report.record_upsert(
        "cv_specs",
        store.upsert("cv_specs", {
            "cv_spec_hash": r.cv_spec_hash, "method": model.cv.method, "n_folds": model.cv.n_folds,
            "n_repeats": model.cv.n_repeats, "nested": int(model.cv.nested),
            "within_train_only": int(model.cv.within_train_only), "params_json": canonical_json({}),
        }),
    )
    report.record_upsert(
        "cv_instances",
        store.upsert("cv_instances", {
            "cv_instance_hash": r.cv_instance_hash, "cv_spec_hash": r.cv_spec_hash,
            "rng_context_hash": r.rng_context_hash,
            "fold_summary_json": canonical_json((model.cv.model_extra or {}).get("fold_summary", {})),
            "engine_fold_set_fingerprint": model.cv.cv_instance_hash,
        }),
    )
    report.record_upsert(
        "rng_contexts",
        store.upsert("rng_contexts", {
            "rng_context_hash": r.rng_context_hash, "root_seed": model.rng.root_seed,
            "derivation": model.rng.derivation,
            "framework_seeds_json": canonical_json(model.rng.framework_seeds),
            "determinism_flags_json": canonical_json(model.rng.determinism_flags),
        }),
    )
    report.record_upsert(
        "refit_strategies",
        store.upsert("refit_strategies", {
            "refit_strategy_hash": r.refit_strategy_hash, "strategy": model.refit.strategy,
            "selection_scope": model.refit.selection_scope, "train_scope": model.refit.train_scope,
            "params_json": canonical_json(model.refit.params),
        }),
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
    store.conn.execute(
        "UPDATE planned_runs SET status = 'fulfilled' WHERE pipeline_dag_hash = ? AND dataset_fingerprint = ?",
        (r.pipeline_dag_hash, r.dataset_fingerprint),
    )
    att = model.leakage_attestation
    if store.upsert("executions", {
        "execution_hash": r.execution_hash,
        "execution_id": model.execution.execution_id,
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
        "status": model.execution.status,
        "failure_code": model.execution.failure_code,
        "failure_message": model.execution.failure_message,
        "oof_enforced": int(att.oof_enforced),
        "unsafe_flags_json": canonical_json(sorted(att.unsafe_flags)),
        "validity_status": validity,
        "created_at": now,
    }):
        report.record_fact("executions")

    # One ScoreSet per score scope, so v_run_metrics.score_scope is accurate and a
    # cv leaderboard never blends test/refit observations (DATA_MANAGEMENT.md §6).
    by_scope: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        by_scope.setdefault(str(obs.get("scope") or r.score_spec.score_level), []).append(obs)

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
                    "partition": obs.get("partition"),
                    "aggregation_level": obs.get("aggregation_level", "sample"),
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
