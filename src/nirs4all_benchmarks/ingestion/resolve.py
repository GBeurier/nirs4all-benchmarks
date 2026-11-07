"""Resolve Arena indexing identities without claiming scientific verification.

Dataset/schema/relation/plan, target, engine/controller, split/CV instance and
sample-manifest identities remain producer-scoped and unverified. RNG/refit
claims remain engine-scoped; absent claims use Arena metadata/spec identities.
Arena computes pipeline/spec/score/condition hashes and preserves declared task
and variant domains with their explicit identity basis. Execution identity always uses the computed normalized export
hash; a submitted export-hash claim never determines execution identity.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity import (
    canonical_json,
    compose_run_condition_hash,
    compute_pipeline_dag_hash,
    fingerprint,
)
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
from nirs4all_benchmarks.identity.pipeline_dag import PipelineDagIdentity
from nirs4all_benchmarks.scoring import ScoreComputationSpec


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except OverflowError:
            return None
    return None


def _param_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "numeric"
    if isinstance(value, str):
        return "categorical"
    if isinstance(value, (list, tuple)):
        return "sequence"
    return "object"


def _library_of(entrypoint: str | None) -> str | None:
    if not entrypoint:
        return None
    return entrypoint.split(".", 1)[0]


@dataclass
class ResolvedExport:
    """The fully-resolved identity set + extracted dimension rows for one export."""

    export: ArenaRunExport
    dataset_fingerprint: str
    task_hash: str
    dataset_variant_hash: str
    split_spec_hash: str
    split_instance_hash: str
    cv_spec_hash: str
    cv_instance_hash: str
    rng_context_hash: str
    refit_strategy_hash: str
    pipeline_id: PipelineDagIdentity
    run_condition_hash: str
    score_spec: ScoreComputationSpec
    execution_hash: str
    operator_rows: list[dict[str, Any]] = field(default_factory=list)
    param_value_rows: list[dict[str, Any]] = field(default_factory=list)
    node_rows: list[dict[str, Any]] = field(default_factory=list)
    node_param_rows: list[dict[str, Any]] = field(default_factory=list)
    edge_rows: list[dict[str, Any]] = field(default_factory=list)
    main_model: str | None = None

    @property
    def pipeline_dag_hash(self) -> str:
        return self.pipeline_id.pipeline_dag_hash


def _resolve_pipeline(export: ArenaRunExport) -> PipelineDagIdentity:
    pipe = export.pipeline
    source: Any
    if pipe.graph:
        source = pipe.graph
    elif pipe.nodes:
        source = {"nodes": [n.model_dump(mode="json", exclude_none=True) for n in pipe.nodes]}
    else:
        raise ValueError("graph_missing")
    identity = compute_pipeline_dag_hash(source, engine_graph_fingerprint=pipe.engine_graph_fingerprint)
    return replace(identity, nirs4all_identity_hash=pipe.nirs4all_identity_hash)



def _extract_pipeline_rows(
    pid: PipelineDagIdentity,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    str | None,
]:
    operators: dict[str, dict[str, Any]] = {}
    param_values: dict[str, dict[str, Any]] = {}
    nodes: list[dict[str, Any]] = []
    node_params: list[dict[str, Any]] = []
    main_model: str | None = None

    for node in pid.normalized_graph["nodes"]:
        node_id = node["id"]
        role = node.get("kind") or "transform"
        operator = node.get("operator")
        operator_version = node.get("operator_version")
        params = node.get("params") or {}
        op_hash = None
        if operator:
            op_hash = fingerprint({"entrypoint": operator, "version": operator_version, "role": role})
            operators.setdefault(
                op_hash,
                {
                    "operator_spec_hash": op_hash,
                    "library": _library_of(operator),
                    "version": operator_version,
                    "entrypoint": operator,
                    "role": role,
                    "family": role,
                    "citation_id": None,
                    "license": None,
                },
            )
        if main_model is None and "model" in role.lower() and operator:
            main_model = operator
        nodes.append(
            {
                "node_id": node_id,
                "node_signature": node["signature"],
                "role": role,
                "operator": operator,
                "operator_version": operator_version,
                "operator_spec_hash": op_hash,
                "params_hash": fingerprint(params),
                "params_json": canonical_json(params),
                "branch_path_json": canonical_json(node.get("branch_path", [])),
                "source_id": node.get("source_id"),
                "model_family": role if "model" in role.lower() else None,
                "fit_scope": node.get("fit_scope"),
            }
        )
        for pname, pval in params.items():
            pv_hash = fingerprint({"name": pname, "value": pval})
            kind = _param_kind(pval)
            param_values.setdefault(
                pv_hash,
                {
                    "param_value_hash": pv_hash,
                    "name": pname,
                    "value_json": canonical_json(pval),
                    "kind": kind,
                    "numeric_value": _numeric_value(pval),
                    "is_numeric": int(kind == "numeric"),
                    "is_ordinal": int(kind == "numeric"),
                    "is_sweepable": int(kind in ("numeric", "categorical", "bool")),
                },
            )
            node_params.append({"node_id": node_id, "param_name": pname, "param_value_hash": pv_hash})

    # The edge table is an indexing projection. Scientific extensions stay in
    # the full canonical graph_json, which is authoritative for matching.
    edges = [{key: e[key] for key in ("src", "src_port", "dst", "dst_port")} for e in pid.normalized_graph["edges"]]
    return list(operators.values()), list(param_values.values()), nodes, node_params, edges, main_model


def fold_set_cv_instance_hash(cv_spec_hash: str, fold_set_fingerprint: str, relation_fingerprint: str) -> str:
    """CV instance key of an actual DAG fold set under one relation authority.

    The canonical fold-set fingerprint binds the CV universe and the per-fold
    train/validation members; the relation fingerprint binds the exclusions that
    narrow fold-train views. Consumers recompute this key to accept a stored
    fold-set identity, so an opaque or copied value is never trusted.
    """
    return fingerprint(
        {
            "spec": cv_spec_hash,
            "fold_set_fingerprint": fold_set_fingerprint,
            "relation_fingerprint": relation_fingerprint,
        }
    )


def resolve_identities(export: ArenaRunExport) -> ResolvedExport:
    """Resolve all identity hashes + extracted dimension rows for an export."""
    dataset_fingerprint = export.dataset.dataset_fingerprint

    # Declared identity domains are retained; Arena-derived hashes are separately
    # verified against identity_basis.inputs by the v2 metadata validator.
    task_hash = export.task.task_hash
    dataset_variant_hash = export.dataset_variant.dataset_variant_hash

    split_spec_hash = fingerprint({"method": export.split.method, "params": export.split.params})
    split_instance_hash = export.split.split_instance_hash or fingerprint(
        {"spec": split_spec_hash, "summary": export.split.partition_summary}
    )

    cv_spec_hash = fingerprint(
        {
            "method": export.cv.method,
            "n_folds": export.cv.n_folds,
            "n_repeats": export.cv.n_repeats,
            "nested": export.cv.nested,
            "within_train_only": export.cv.within_train_only,
        }
    )
    if export.cv.fold_set_fingerprint is not None and export.dataset.relation_fingerprint is not None:
        cv_instance_hash = fold_set_cv_instance_hash(
            cv_spec_hash, export.cv.fold_set_fingerprint, export.dataset.relation_fingerprint
        )
    else:
        cv_instance_hash = export.cv.cv_instance_hash or fingerprint(
            {"spec": cv_spec_hash, "summary": {}}
        )

    rng_context_hash = export.rng.rng_context_hash or fingerprint(
        {
            "root_seed": export.rng.root_seed,
            "derivation": export.rng.derivation,
            "framework_seeds": export.rng.framework_seeds,
            "determinism_flags": export.rng.determinism_flags,
        }
    )

    refit_strategy_hash = export.refit.refit_strategy_hash or fingerprint(
        {
            "strategy": export.refit.strategy,
            "selection_scope": export.refit.selection_scope,
            "train_scope": export.refit.train_scope,
            "params": export.refit.params,
        }
    )

    pipeline_id = _resolve_pipeline(export)
    operator_rows, param_value_rows, node_rows, node_param_rows, edge_rows, main_model = _extract_pipeline_rows(
        pipeline_id
    )

    run_condition_hash = compose_run_condition_hash(
        dataset_variant_hash=dataset_variant_hash,
        split_instance_hash=split_instance_hash,
        cv_instance_hash=cv_instance_hash,
        rng_context_hash=rng_context_hash,
        pipeline_dag_hash=pipeline_id.pipeline_dag_hash,
        refit_strategy_hash=refit_strategy_hash,
    )

    scopes = {obs.scope or "unreported" for obs in export.scores.observations}
    score_level = "cv" if "cv" in scopes else sorted(scopes)[0] if scopes else "unreported"
    score_spec = ScoreComputationSpec(
        score_version=export.scores.score_version,
        metric_implementation=export.scores.metric_implementation or "unreported",
        score_level=score_level,
        aggregation_policy=export.scores.aggregation_policy or "unreported",
    )

    # Execution identity: content hash over the condition + the export hash, so two
    # genuinely distinct runs of the same condition get distinct executions, but a
    # re-ingest of the same export collapses.
    export_hash = compute_export_hash(export.to_manifest())
    execution_hash = fingerprint(
        {"run_condition": run_condition_hash, "export": export_hash, "execution_id": export.execution.execution_id}
    )

    return ResolvedExport(
        export=export,
        dataset_fingerprint=dataset_fingerprint,
        task_hash=task_hash,
        dataset_variant_hash=dataset_variant_hash,
        split_spec_hash=split_spec_hash,
        split_instance_hash=split_instance_hash,
        cv_spec_hash=cv_spec_hash,
        cv_instance_hash=cv_instance_hash,
        rng_context_hash=rng_context_hash,
        refit_strategy_hash=refit_strategy_hash,
        pipeline_id=pipeline_id,
        run_condition_hash=run_condition_hash,
        score_spec=score_spec,
        execution_hash=execution_hash,
        operator_rows=operator_rows,
        param_value_rows=param_value_rows,
        node_rows=node_rows,
        node_param_rows=node_param_rows,
        edge_rows=edge_rows,
        main_model=main_model,
    )
