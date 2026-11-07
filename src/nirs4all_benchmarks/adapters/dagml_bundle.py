"""Map supported DAG-ML metadata into ArenaRunExport.

Owner identities and explicit native execution metadata are required evidence.
Bundle presence, generic success and schema fingerprints do not establish
scientific dataset identity, completion, leakage safety or a refit strategy.

Bundle metadata is mapped without importing the runtime. Only the optional
``training_outcome`` path lazily imports the ``dag_ml`` owner API, to validate
the retained outcome and compute its canonical fold-set fingerprint; it refuses
explicitly when that runtime or API is absent and never reimplements either.
The outcome path emits scores and identity metadata only: no predictions,
fitted state, prediction caches or outcome blob are copied.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import ARENA_EXPORT_SCHEMA_VERSION
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.folds import is_pseudo_fold
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
from nirs4all_benchmarks.identity.hashing import is_hash
from nirs4all_benchmarks.scoring import METRIC_DIRECTION


def _load_json(value: dict[str, Any] | str | Path) -> dict[str, Any]:
    result = value if isinstance(value, dict) else strict_loads(Path(value).read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise ValueError("dagml_metadata_object_required")
    return result


def _prediction_residuals(prediction_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map dag-ml ``PredictionBlock``s (sample-keyed) → residual rows."""
    rows: list[dict[str, Any]] = []
    for block in prediction_blocks:
        if not isinstance(block, dict):
            raise ValueError("dagml_prediction_block_unsupported")
        partition = str(block.get("partition") or "unreported").lower()
        scope = block.get("scope") or "unreported"
        fold_id = str(block["fold_id"]) if block.get("fold_id") is not None else None
        sample_ids = block.get("sample_ids") or []
        values = block.get("values") or []
        y_true = block.get("y_true") or block.get("targets") or []
        group_ids = block.get("group_ids") or []
        if (len(values) != len(sample_ids) or (y_true and len(y_true) != len(sample_ids))
                or (group_ids and len(group_ids) != len(sample_ids))):
            raise ValueError("dagml_prediction_length_mismatch")
        if partition not in {"train", "validation", "test", "final", "unreported"}:
            raise ValueError("dagml_partition_unsupported")
        for i, sid in enumerate(sample_ids):
            if not isinstance(sid, str) or not sid:
                raise ValueError("dagml_sample_identity_required")
            yp = values[i] if i < len(values) else None
            if isinstance(yp, list):
                if len(yp) != 1:
                    raise ValueError("dagml_multitarget_predictions_unsupported")
                yp = yp[0]
            yt = y_true[i] if i < len(y_true) else None
            if isinstance(yt, list):
                if len(yt) != 1:
                    raise ValueError("dagml_multitarget_predictions_unsupported")
                yt = yt[0]
            residual = (yt - yp) if (yt is not None and yp is not None) else None
            rows.append(
                {
                    "sample_id": str(sid),
                    "group_id": group_ids[i] if i < len(group_ids) else None,
                    "scope": scope,
                    "fold_id": fold_id,
                    "partition": "final" if partition == "final" else partition,
                    "y_true": yt,
                    "y_pred": yp,
                    "residual": residual,
                }
            )
    return rows


def _metric_observations(metric_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map dag-ml ``RegressionMetricReport``s → score observations."""
    out: list[dict[str, Any]] = []
    for report in metric_reports:
        if not isinstance(report, dict) or not isinstance(report.get("metrics", {}), dict):
            raise ValueError("dagml_metric_report_unsupported")
        partition = str(report.get("partition") or "unreported").lower()
        scope = str(report.get("scope") or "unreported").lower()
        if scope not in {"fold", "cv", "refit", "test", "view", "unreported"}:
            raise ValueError("dagml_score_scope_unsupported")
        fold_id = str(report["fold_id"]) if report.get("fold_id") is not None else None
        label = report.get("source_fold_id")
        if fold_id is not None and is_pseudo_fold(fold_id):
            if report.get("scope") is None or report.get("aggregation_level") is None:
                raise ValueError("dagml_aggregate_context_required")
            if label is not None and label != fold_id:
                raise ValueError("dagml_aggregate_context_contradiction")
            label, fold_id = fold_id, None
        metrics = report.get("metrics", {})
        for name, value in metrics.items():
            if isinstance(value, bool) or (value is not None and not isinstance(value, (int, float))):
                raise ValueError("dagml_metric_value_unsupported")
            out.append(
                {
                    "metric_name": str(name).lower(),
                    "metric_value": float(value) if value is not None else None,
                    "origin": "producer_reported",
                    "direction": METRIC_DIRECTION.get(str(name).lower()),
                    "direction_source": "registry" if str(name).lower() in METRIC_DIRECTION else "unreported",
                    "scope": scope,
                    "fold_id": fold_id,
                    "source_fold_id": label,
                    "partition": "final" if partition == "final" else partition,
                    "aggregation_level": report.get("aggregation_level") or (
                        "target" if report.get("level") == "macro" else "unreported"),
                    **{k: report[k] for k in ("n_samples", "coverage", "metric_unit") if k in report},
                }
            )
    return out


# DAG native pooled cross-fold report labels: preserved as ``view`` aggregates, never folds.
_DAG_POOLED_FOLD_IDS = frozenset({"avg", "w_avg"})
# The scoring registry's regression metrics (METRIC_DIRECTION keys defined for continuous predictions).
# The registry has no named task split, so this adapter states it; classification keys stay unmapped.
_REGRESSION_METRICS = frozenset({"mse", "rmse", "nrmse", "mae", "medae", "bias", "r2", "rpd", "rpiq", "ccc"})
# Scalar ExecutionBundle identities read by this adapter; caches, artifacts and payloads are never copied.
_DAG_BUNDLE_IDENTITY_FIELDS = (
    "bundle_id", "schema_version", "graph_fingerprint", "controller_fingerprint", "selected_variant_id",
    "unsafe_flags",
)


@dataclass(frozen=True)
class _OutcomeFacts:
    """Score and identity metadata derived from one validated DAG ``TrainingOutcome``."""

    bundle: dict[str, Any]
    graph: dict[str, Any]
    cv: dict[str, Any]
    relation_fingerprint: str
    observations: list[dict[str, Any]]
    unmapped_reports: list[dict[str, Any]]


@dataclass(frozen=True)
class _DagOwnerApi:
    """The public ``dag_ml`` functions this adapter needs; no local fallback exists."""

    validate_training_outcome_json: Callable[[str], None]
    fold_set_fingerprint_json: Callable[[str], str]
    error: type[Exception]


def _dag_owner_api() -> _DagOwnerApi:
    try:
        dag_ml = importlib.import_module("dag_ml")
    except ImportError:
        raise ValueError("dagml_runtime_required_for_fold_identity") from None
    validate = getattr(dag_ml, "validate_training_outcome_json", None)
    fold_set_fingerprint = getattr(dag_ml, "fold_set_fingerprint_json", None)
    error = getattr(dag_ml, "DagMlError", None)
    if (not callable(validate) or not callable(fold_set_fingerprint)
            or not isinstance(error, type) or not issubclass(error, Exception)):
        raise ValueError("dagml_fold_identity_api_unsupported")
    return _DagOwnerApi(validate, fold_set_fingerprint, error)


def _dag_graph(graph: dict[str, Any]) -> dict[str, Any]:
    """Express a DAG ``GraphSpec`` in Arena graph keys without dropping any field.

    Node ``id`` becomes ``node_id``; a structured operator is kept whole as its
    canonical JSON text, so its steps and parameters stay in the hashed identity.
    Edge port references become ``src``/``dst`` ports; contracts, ports, metadata
    and graph-level fields are retained as hashed extensions.
    """
    nodes = graph.get("nodes")
    edges = graph.get("edges", [])
    if not isinstance(nodes, list) or not nodes or not isinstance(edges, list):
        raise ValueError("dagml_outcome_graph_unsupported")
    arena_nodes = []
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str) or "node_id" in node:
            raise ValueError("dagml_outcome_graph_unsupported")
        operator = node.get("operator")
        arena_nodes.append({
            **{key: value for key, value in node.items() if key not in {"id", "operator"}},
            "node_id": node["id"],
            "operator": operator if operator is None or isinstance(operator, str) else canonical_json(operator),
        })
    arena_edges = []
    for edge in edges:
        source, target = (edge.get("source"), edge.get("target")) if isinstance(edge, dict) else (None, None)
        if not isinstance(source, dict) or not isinstance(target, dict):
            raise ValueError("dagml_outcome_graph_unsupported")
        arena_edges.append({
            **{key: value for key, value in edge.items() if key not in {"source", "target"}},
            "src": source.get("node_id"), "src_port": source.get("port_name"),
            "dst": target.get("node_id"), "dst_port": target.get("port_name"),
        })
    return {**{key: value for key, value in graph.items() if key not in {"nodes", "edges"}},
            "nodes": arena_nodes, "edges": arena_edges}


def _outcome_observation(report: dict[str, Any], fold_ids: set[str]) -> tuple[dict[str, Any] | None, str | None]:
    """Classify one DAG report from owner facts only; return (context, unmapped reason)."""
    fold_id = report.get("fold_id")
    partition = report.get("partition")
    if fold_id in fold_ids:
        if partition == "validation":
            return {"scope": "cv", "fold_id": fold_id, "source_fold_id": None, "partition": partition}, None
        return None, "fold_partition_without_actual_cohort_identity"
    if fold_id in _DAG_POOLED_FOLD_IDS:
        if partition in {"train", "validation", "test", "final"}:
            return {"scope": "view", "fold_id": None, "source_fold_id": fold_id, "partition": partition}, None
        return None, "partition_unsupported"
    if fold_id is None and partition in {"test", "final"}:
        return {"scope": "test" if partition == "test" else "refit", "fold_id": None, "source_fold_id": None,
                "partition": partition}, None
    return None, "fold_unknown"


def _training_outcome_facts(source: str | Path) -> _OutcomeFacts:
    """Validate a retained DAG ``TrainingOutcome`` through the owner API and map its metadata.

    Only a single unpatched variant with no generated choices is supported, so the
    mapped graph is the executed configuration and validation scores used by SELECT
    are never presented as evidence for unselected configurations. Only regression
    point outputs are mapped, and only the registry's regression metrics; any other
    metric reported for them is listed as ``metric_task_type_incompatible``.
    """
    text = Path(source).read_text(encoding="utf-8")
    owner = _dag_owner_api()
    try:
        # Owner validation checks the strict JSON and its outcome fingerprint before any field is read.
        owner.validate_training_outcome_json(text)
    except owner.error:
        raise ValueError("dagml_training_outcome_invalid") from None
    outcome = strict_loads(text)
    if not isinstance(outcome, dict):
        raise ValueError("dagml_training_outcome_invalid")
    plan = outcome["effective_plan"]
    selected = outcome["selected_variant_id"]
    variants = plan.get("variants") or []
    if len(variants) != 1 or variants[0].get("choices") or outcome.get("parameter_patches"):
        raise ValueError("dagml_outcome_single_unpatched_variant_required")
    fold_set = plan.get("fold_set")
    if not isinstance(fold_set, dict):
        raise ValueError("dagml_outcome_fold_set_required")
    try:
        fold_set_fingerprint = owner.fold_set_fingerprint_json(json.dumps(fold_set))
    except owner.error:
        raise ValueError("dagml_outcome_fold_set_invalid") from None
    relations = {identity.get("relation_fingerprint") for identity in outcome.get("data_identities") or []}
    if len(relations) != 1 or not is_hash(next(iter(relations))):
        raise ValueError("dagml_outcome_relation_identity_required")
    relation_fingerprint = str(next(iter(relations)))
    output = next((item["binding"] for item in outcome.get("outputs") or []
                   if item["binding"]["binding_id"] == outcome["selection_output_id"]), None)
    if output is None:
        raise ValueError("dagml_outcome_selection_output_required")
    if output.get("prediction_kind") != "regression_point":
        raise ValueError("dagml_prediction_task_unsupported")
    fold_ids = {str(fold["fold_id"]) for fold in fold_set["folds"]}
    observations: list[dict[str, Any]] = []
    unmapped: list[dict[str, Any]] = []
    for report in outcome["score_set"]["reports"]:
        key = {name: report.get(name) for name in
               ("producer_node", "producer_port", "variant_id", "partition", "fold_id", "level")}
        if (report.get("producer_node"), report.get("producer_port")) != (output["node_id"], output["port_name"]):
            unmapped.append({**key, "reason": "producer_not_selection_output"})
            continue
        if report.get("variant_id") not in {None, selected}:
            unmapped.append({**key, "reason": "variant_not_selected"})
            continue
        if report.get("level") != "sample" or report.get("grouping_key") is not None:
            unmapped.append({**key, "reason": "level_unsupported"})
            continue
        context, reason = _outcome_observation(report, fold_ids)
        if context is None:
            unmapped.append({**key, "reason": reason})
            continue
        for name, value in sorted(report["metrics"].items()):
            metric = str(name).lower()
            if metric not in METRIC_DIRECTION or isinstance(value, bool) or not isinstance(value, (int, float)):
                unmapped.append({**key, "metric_name": str(name), "reason": "metric_unsupported"})
                continue
            if metric not in _REGRESSION_METRICS:
                unmapped.append({**key, "metric_name": str(name), "reason": "metric_task_type_incompatible"})
                continue
            observations.append({
                "metric_name": metric, "metric_value": float(value), "origin": "producer_reported",
                "direction": METRIC_DIRECTION[metric], "direction_source": "registry",
                "aggregation_level": "sample", "n_samples": report.get("row_count"), **context,
            })
    split = plan["campaign"].get("split_invocation") or {}
    method = (split.get("params") or {}).get("kind")
    bundle = {key: outcome["execution_bundle"][key] for key in _DAG_BUNDLE_IDENTITY_FIELDS
              if key in outcome["execution_bundle"]}
    if bundle.get("selected_variant_id") not in {None, selected}:
        raise ValueError("dagml_variant_selection_mismatch")
    bundle["selected_variant_id"] = selected
    return _OutcomeFacts(
        bundle=bundle,
        graph=_dag_graph(plan["graph_plan"]["graph"]),
        cv={"method": method if isinstance(method, str) else "unreported", "n_folds": len(fold_ids),
            "fold_set_fingerprint": fold_set_fingerprint},
        relation_fingerprint=relation_fingerprint,
        observations=observations,
        unmapped_reports=unmapped,
    )


def bundle_to_export(
    bundle: dict[str, Any] | str | Path | None = None,
    *,
    graph: dict[str, Any] | str | Path | None = None,
    dataset_envelope: dict[str, Any] | str | Path | None = None,
    dataset_card: dict[str, Any] | None = None,
    metric_reports: list[dict[str, Any]] | None = None,
    prediction_blocks: list[dict[str, Any]] | None = None,
    provenance: dict[str, Any] | None = None,
    task_type: str | None = None,
    n_samples: int | None = None,
    n_features: int | None = None,
    task: dict[str, Any] | None = None,
    dataset_variant: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    leakage_attestation: dict[str, Any] | None = None,
    visibility: str | None = None,
    score_context: dict[str, Any] | None = None,
    owner_export: ArenaRunExport | dict[str, Any] | str | Path | None = None,
    identity_basis: dict[str, Any] | None = None,
    training_outcome: str | Path | None = None,
) -> ArenaRunExport:
    """Build an ``ArenaRunExport`` from a dag-ml bundle and its companion files.

    A genuine owner dataset fingerprint and task/variant identities are required.
    A schema fingerprint or bundle id is never a scientific dataset identity.
    Generic bundle success does not prove completion. Only an explicit Arena
    execution block supplied by the caller can declare completion. Missing
    attestations are unreported; private visibility is a local sharing policy.

    ``training_outcome`` is the path of a retained DAG ``TrainingOutcome`` JSON. It
    replaces ``bundle``, ``graph``, ``metric_reports``, ``prediction_blocks`` and
    ``owner_export``: the owner API validates it and computes the declared
    ``cv.fold_set_fingerprint``; the full graph, relation identity and the
    selected output's scores are taken from it. Unmapped reports are listed in
    provenance. Caller identities, execution, attestations and score context
    remain required declarations.
    """
    outcome: _OutcomeFacts | None = None
    if training_outcome is not None:
        if any(value is not None for value in (bundle, graph, metric_reports, prediction_blocks, owner_export)):
            raise ValueError("dagml_training_outcome_input_conflict")
        # This path maps regression score reports only; the declared task decides metric compatibility.
        if (task_type if task_type is not None else (task or {}).get("task_type")) != "regression":
            raise ValueError("dagml_prediction_task_unsupported")
        outcome = _training_outcome_facts(training_outcome)
        declared = _load_json(dataset_envelope) if dataset_envelope is not None else {}
        if declared.get("relation_fingerprint") not in {None, outcome.relation_fingerprint}:
            raise ValueError("dagml_outcome_relation_mismatch")
        bundle, graph, prediction_blocks = outcome.bundle, outcome.graph, []
        dataset_envelope = {**declared, "relation_fingerprint": outcome.relation_fingerprint}
    elif bundle is None:
        raise ValueError("dagml_bundle_required")
    b = _load_json(bundle)
    if metric_reports is not None and (not isinstance(metric_reports, list)
                                       or any(not isinstance(report, dict) for report in metric_reports)):
        raise ValueError("dagml_metric_report_unsupported")
    if any(report.get("variant_label") is not None and not isinstance(report["variant_label"], str)
           for report in metric_reports or []):
        raise ValueError("dagml_variant_context_unsupported")
    if b.get("variants") is not None and not isinstance(b["variants"], (list, dict)):
        raise ValueError("dagml_variant_context_unsupported")
    variants = {
        report.get("variant_label") for report in metric_reports or [] if report.get("variant_label") is not None
    }
    if len(variants) > 1 or len(b.get("variants") or []) > 1:
        raise ValueError("dagml_variant_context_unsupported")
    graph_dict = _load_json(graph) if graph is not None else None
    envelope = _load_json(dataset_envelope) if dataset_envelope is not None else {}
    owner = None
    owner_metadata = None
    if owner_export is not None:
        from nirs4all_benchmarks.ingestion.offline import validate_metadata

        owner_metadata = validate_metadata(owner_export)
        if owner_metadata.status != "metadata_valid":
            raise ValueError("dagml_owner_metadata_rejected")
        owner = owner_metadata.model
        assert owner is not None
        identity_basis = owner.identity_basis.model_dump(mode="json")
        task_type = owner.task.task_type
        if envelope.get("dataset_fingerprint") not in (None, owner.dataset.dataset_fingerprint):
            raise ValueError("dagml_owner_dataset_mismatch")
        envelope = {**owner.dataset.model_dump(mode="json", exclude_none=True), **envelope}
        task = task if task is not None else owner.task.model_dump(mode="json", exclude_none=True)
        dataset_variant = dataset_variant if dataset_variant is not None else owner.dataset_variant.model_dump(
            mode="json", exclude_none=True
        )
        execution = execution if execution is not None else owner.execution.model_dump(mode="json", exclude_none=True)
        if leakage_attestation is None:
            leakage_attestation = owner.leakage_attestation.model_dump(mode="json")
        if score_context is None:
            score_context = owner.scores.model_dump(mode="json", exclude_none=True, exclude={"observations"})
        if graph_dict is None:
            graph_dict = owner.pipeline.graph
        dataset_card = owner.dataset.dataset_card if dataset_card is None else dataset_card
        n_samples = owner.dataset.n_samples if n_samples is None else n_samples
        n_features = owner.dataset.n_features if n_features is None else n_features

    graph_fingerprint = b.get("graph_fingerprint")
    if (owner and owner.pipeline.engine_graph_fingerprint and graph_fingerprint
            and graph_fingerprint != owner.pipeline.engine_graph_fingerprint):
        raise ValueError("dagml_owner_graph_mismatch")
    raw_flags = b.get("unsafe_flags")
    if raw_flags is not None and (not isinstance(raw_flags, list)
                                  or any(not isinstance(flag, str) for flag in raw_flags)):
        raise ValueError("dagml_unsafe_flags_unsupported")
    unsafe_flags = sorted(raw_flags or [])

    if identity_basis is None:
        raise ValueError("dagml_identity_basis_required")
    # Dataset identity from io's CoordinatorDataPlanEnvelope (three fingerprints).
    dataset_fingerprint = envelope.get("dataset_fingerprint")
    if not is_hash(dataset_fingerprint):
        raise ValueError("dagml_dataset_identity_required")
    if not isinstance(task, dict) or not is_hash(task.get("task_hash")):
        raise ValueError("dagml_task_identity_required")
    if not isinstance(dataset_variant, dict) or not is_hash(dataset_variant.get("dataset_variant_hash")):
        raise ValueError("dagml_dataset_variant_identity_required")
    if task_type not in {None, "regression"} or task.get("task_type") not in {None, "regression"}:
        raise ValueError("dagml_prediction_task_unsupported")
    if execution is None:
        execution = {"execution_id": b.get("bundle_id")}
        if b.get("status") in {"failed", "cancelled"}:
            execution["status"] = b["status"]
    else:
        execution = {"execution_id": b.get("bundle_id"), **execution}
    if b.get("status") in {"failed", "cancelled"} and execution.get("status", "ok") != b["status"]:
        raise ValueError("dagml_execution_status_conflict")
    attestation = dict(leakage_attestation) if leakage_attestation is not None else {}
    if unsafe_flags or attestation.get("unsafe_flags") is not None:
        attestation["unsafe_flags"] = sorted(set(unsafe_flags + (attestation.get("unsafe_flags") or [])))

    cached_blocks = b.get("prediction_caches", [])
    if cached_blocks is None:
        cached_blocks = []
    pred_blocks = prediction_blocks if prediction_blocks is not None else cached_blocks
    if (not isinstance(cached_blocks, list) or not isinstance(pred_blocks, list)
            or any(not isinstance(block, dict) for block in [*cached_blocks, *pred_blocks])):
        raise ValueError("dagml_prediction_block_unsupported")
    variant_items = [*(metric_reports or []), *cached_blocks, *(prediction_blocks or [])]
    labels = {item.get("variant_label") for item in variant_items
              if isinstance(item.get("variant_label"), str)}
    if (len(labels) > 1 or any(item.get("variant_label") is not None
                              and not isinstance(item["variant_label"], str) for item in variant_items)):
        raise ValueError("dagml_variant_context_unsupported")
    selected = b.get("selected_variant_id")
    owner_selected = owner.refit.selected_variant_id if owner is not None else None
    if selected is not None and owner_selected is not None and selected != owner_selected:
        raise ValueError("dagml_variant_selection_mismatch")
    selected = selected if selected is not None else owner_selected
    if variant_items and (not isinstance(selected, str) or not selected):
        raise ValueError("dagml_selected_variant_required")
    if selected is not None and variant_items and any(item.get("variant_label") != selected for item in variant_items):
        raise ValueError("dagml_variant_selection_mismatch")
    variants = labels
    residual_rows = _prediction_residuals(pred_blocks)
    observations = _metric_observations(metric_reports or [])
    if metric_reports is None and owner is not None:
        observations = [obs.model_dump(mode="json") for obs in owner.scores.observations]
    if outcome is not None:
        observations = outcome.observations

    pipeline_block: dict[str, Any] = {
        "engine_graph_fingerprint": graph_fingerprint,
        "controller_fingerprint": b.get("controller_fingerprint"),
        "human_label": (b.get("metadata") or {}).get("label"),
    }
    if owner is not None:
        pipeline_block = {**owner.pipeline.model_dump(mode="json", exclude_none=True),
                          **{key: value for key, value in pipeline_block.items() if value is not None}}
    if graph_dict:
        pipeline_block["graph"] = graph_dict
        if outcome is None:
            pipeline_block["nodes"] = graph_dict.get("nodes", [])
    elif not pipeline_block.get("nodes"):
        raise ValueError("dagml_pipeline_structure_required")

    if visibility is not None:
        declared_visibility, visibility_source = visibility, "producer_reported"
    elif owner_metadata is not None:
        declared_visibility, visibility_source = owner_metadata.visibility, owner_metadata.visibility_source
    else:
        declared_visibility = envelope.get("visibility", "unreported")
        visibility_source = envelope.get("visibility_source") or "unreported"
        if declared_visibility == "public" and visibility_source != "producer_reported":
            declared_visibility = "unreported"
    manifest: dict[str, Any] = {
        "arena_export_schema_version": ARENA_EXPORT_SCHEMA_VERSION,
        "identity_basis": identity_basis,
        "producer": {"capsule": "dag-ml", "dag_ml_version": (b.get("producer") or {}).get("dag_ml_version"),
                     "source_schema_version": b.get("schema_version")},
        "dataset": {
            "dataset_fingerprint": dataset_fingerprint,
            "schema_fingerprint": envelope.get("schema_fingerprint"),
            "relation_fingerprint": envelope.get("relation_fingerprint"),
            "plan_fingerprint": envelope.get("plan_fingerprint"),
            "dataset_card": dataset_card or {"task_type": task_type},
            "visibility": declared_visibility or "unreported",
            "visibility_source": visibility_source,
            "n_samples": n_samples,
            "n_features": n_features,
        },
        "task": task,
        "dataset_variant": dataset_variant,
        "pipeline": pipeline_block,
        "split": owner.split.model_dump(exclude_none=True) if owner else {"method": "unreported", "params": {}},
        "cv": outcome.cv if outcome else owner.cv.model_dump(exclude_none=True) if owner else {
            "method": "unreported"},
        "rng": owner.rng.model_dump(exclude_none=True) if owner else {
            "root_seed": (b.get("metadata") or {}).get("root_seed")},
        "refit": owner.refit.model_dump(exclude_none=True) if owner else {
            "selected_variant_id": b.get("selected_variant_id")},
        "execution": execution,
        "leakage_attestation": attestation,
        "scores": {"score_version": "unreported", **(score_context or {}), "observations": observations,
                   "origin": (score_context or {}).get("origin") or "producer_reported"},
        "residuals": {
            "key": "sample_id",
            "pseudonymized": False,
            "publishable": {"y_true":True, "y_pred":True, "residual":True, "y_proba":True},
            "inline": residual_rows,
        },
    }
    manifest["provenance"] = {
        **(owner.provenance.model_dump(exclude_none=True) if owner and owner.provenance else {}),
        **(provenance or {}), "dagml_bundle_sha256": fingerprint(b),
        "dagml_metric_reports_sha256": fingerprint(metric_reports),
        "dagml_variant_labels": sorted(variants),
    }
    if owner_metadata:
        manifest["provenance"]["owner_metadata_sha256"] = owner_metadata.arena_export_hash
    if outcome is not None:
        manifest["provenance"]["dagml_unmapped_score_reports"] = outcome.unmapped_reports
    try:
        model = ArenaRunExport.model_validate(manifest)
    except ValueError:
        raise ValueError("dagml_metadata_unsupported") from None
    model.arena_export_hash = compute_export_hash(model.to_manifest())
    from nirs4all_benchmarks.ingestion.offline import validate_metadata
    validation = validate_metadata(model)
    if validation.status != "metadata_valid":
        raise ValueError("dagml_metadata_refused:" + validation.issues[0]["code"])
    return model
