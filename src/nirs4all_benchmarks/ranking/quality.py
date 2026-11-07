"""Controlled observed rankings backed exclusively by the native methods library.

No raw metric values cross dataset boundaries. Repeated observations are collapsed
inside matched blocks, then each dataset contributes once per candidate pair.
These descriptive component rankings do not estimate adjusted or causal effects.
"""

from __future__ import annotations

import importlib
import itertools
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.indexing import classify_role
from nirs4all_benchmarks.scoring import direction_of
from nirs4all_benchmarks.scoring.metrics import METRIC_DIRECTION
from nirs4all_benchmarks.store import ArenaStore

from .observations import (
    CV_IDENTITY_SCOPES,
    MIXED_CV_FOLD_GRANULARITY,
    comparison_identity_reason,
    cv_fold_granularity,
    original_observations,
    pipeline_rows,
)

METHODS = ["average_rank", "elo", "bradley_terry", "trueskill", "openskill"]
DIMENSIONS = [
    "pipeline",
    "model",
    "preprocessing_sequence",
    "preprocessing_operator",
    "feature_selection",
    "augmentation",
    "parameter",
    "split",
]
CAPABILITIES: dict[str, Any] = {
    "methods": METHODS,
    "dimensions": DIMENSIONS,
    "analyses": ["ranking", "relative_gain", "mixed_effects", "fanova"],
    "deferred": {
        "adjusted_interaction": "adjusted_interaction_backend_not_qualified",
    },
}


class RankingRequest(BaseModel):
    """Complete, versioned identity of a ranking request and static cache entry."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    metric: str = "rmse"
    scope: Literal["fold", "cv", "refit", "test", "view"] = "cv"
    partition: Literal["train", "validation", "test", "final"] = "validation"
    dimension: str = "pipeline"
    method: str = "bradley_terry"
    direction: Literal["min", "max"] | None = None
    dataset: str | None = None
    task: str | None = None
    collection: str | None = None
    protocol: str | None = None
    operator: str | None = None
    parameter: str | None = None
    split_criterion: str | None = None
    tie_tolerance: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    options: dict[str, float | int] = Field(default_factory=dict)
    aggregation: Literal["method_default"] = "method_default"
    order: Literal["dataset_candidate_lexical"] = "dataset_candidate_lexical"
    comparison_mode: Literal["observed", "matched"] = "observed"

    @model_validator(mode="after")
    def resolve_direction(self) -> RankingRequest:
        if self.direction is None and self.metric.lower() in METRIC_DIRECTION:
            self.direction = "max" if direction_of(self.metric) == "max" else "min"
        return self

    @property
    def identity(self) -> str:
        return fingerprint(self.model_dump(mode="json"))


def _artifact(req: RankingRequest) -> dict[str, Any]:
    identity = req.model_dump(mode="json")
    aggregation = "dataset_panel_mean" if req.method == "average_rank" else "dataset_pair_mean"
    return {
        "schema_version": "n4a.quality-ranking/1",
        "request_id": req.identity,
        "request": identity,
        "state": "unavailable",
        "reason": None,
        **{key: identity[key] for key in ("dimension", "method", "metric", "scope", "partition", "direction")},
        "config": {
            **{key: identity[key] for key in ("tie_tolerance", "options", "order", "comparison_mode")},
            "aggregation": aggregation,
        },
        "interpretation": "observed_performance" if req.dimension == "pipeline" else "observed_component_performance",
        "candidates": [],
        "coverage": None,
        "inference": None,
        "convergence": None,
        "uncertainty_kind": None,
        "score_direction": None,
        "native_schema_version": None,
        "provenance": {
            "source_observations": 0,
            "accepted_observations": 0,
            "excluded": {},
            "datasets": [],
            "matched_blocks": 0,
            "comparison_rows": 0,
            "blocks": [],
            "comparisons": [],
            "aggregation": aggregation,
            "dataset_weighting": "one row per dataset"
            if req.method == "average_rank"
            else "one comparison per dataset and candidate pair",
        },
        "warnings": [],
        "capabilities": CAPABILITIES,
    }


def _unavailable(artifact: dict[str, Any], reason: str) -> dict[str, Any]:
    artifact["reason"] = reason
    return artifact


def _native_rank() -> Callable[..., dict[str, Any]]:
    module = importlib.import_module("n4m.ranking")
    return module.rank


def _mean_component(observations: list[tuple[float, str, str]]) -> float:
    per_pipeline: dict[str, list[float]] = defaultdict(list)
    for value, pipeline, _ in observations:
        per_pipeline[pipeline].append(value)
    means = [sum(v) / len(v) for v in per_pipeline.values()]
    return sum(means) / len(means)


def _labels(
    store: ArenaStore, req: RankingRequest, pipeline_ids: set[str]
) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
    """Extract identities from normalized nodes, preserving operator qualification."""
    nodes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in pipeline_rows(store, "pipeline_nodes", pipeline_ids):
        nodes[node["pipeline_dag_hash"]].append(node)
    edges: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in pipeline_rows(store, "pipeline_edges", pipeline_ids):
        edges[edge["pipeline_dag_hash"]].append(edge)
    result: dict[str, list[tuple[str, str]]] = {}
    ambiguous: set[str] = set()
    for pipe in pipeline_rows(store, "pipeline_dags", pipeline_ids):
        key = pipe["pipeline_dag_hash"]
        group = nodes[key]
        if req.dimension == "pipeline":
            result[key] = [(key, pipe["human_label"] or key)]
            continue
        if req.dimension == "parameter":
            selected = [n for n in group if n["operator"] == req.operator]
            if len(selected) != 1 or req.parameter not in json.loads(selected[0]["params_json"]):
                ambiguous.add(key)
                continue
            value = json.loads(selected[0]["params_json"])[req.parameter]
            operator_label = f"{req.operator}@{selected[0]['operator_version'] or 'unreported'}"
            label = f"{operator_label}::{req.parameter}={canonical_json(value)}"
            result[key] = [(label, label)]
            continue
        roles = {
            "model": {"model", "merge"},
            "preprocessing_sequence": {"preprocessing", "scaler"},
            "preprocessing_operator": {"preprocessing", "scaler"},
            "feature_selection": {"feature_selection"},
            "augmentation": {"augmentation"},
        }
        selected = [n for n in group if classify_role(n["operator"], n["role"]) in roles.get(req.dimension, set())]
        if req.dimension == "preprocessing_operator":
            if req.comparison_mode == "matched":
                selected = [n for n in selected if n["operator"] == req.operator]
            operators = sorted(
                {f"{n['operator']}@{n['operator_version'] or 'unreported'}" for n in selected if n["operator"]}
            )
            result[key] = [(op, op) for op in operators]
            if not selected:
                result[key] = [("none", "None")]
            if req.comparison_mode == "matched":
                result[key] = [
                    (candidate, f"With {label}" if candidate != "none" else f"Without {req.operator}")
                    for candidate, label in result[key]
                ]
            continue
        if req.dimension == "preprocessing_sequence":
            if not pipe["is_linear"]:
                ambiguous.add(key)
                continue
            # Order follows graph edges, never the storage order of node identifiers.
            remaining = {n["node_id"]: n for n in group}
            ordered = []
            while remaining:
                ready = sorted(
                    nid for nid in remaining if not any(e["dst"] == nid and e["src"] in remaining for e in edges[key])
                )
                if len(ready) != 1:
                    break
                ordered.append(remaining.pop(ready[0]))
            if remaining:
                ambiguous.add(key)
                continue
            selected_ids = {n["node_id"] for n in selected}
            selected = [n for n in ordered if n["node_id"] in selected_ids]
        # Sequences are configurations, including parameters; operator view is presence-only.
        signature = [
            (
                f"{n['operator']}@{n['operator_version'] or 'unreported'}",
                {} if req.dimension == "model" else json.loads(n["params_json"]),
            )
            for n in selected
        ]
        if req.dimension != "preprocessing_sequence":
            signature.sort(key=canonical_json)
        candidate = canonical_json(signature)
        label = " → ".join(f"{op} {canonical_json(params)}" if params else op for op, params in signature) or "None"
        result[key] = [(candidate, label)]
    return result, ambiguous


def _assemble_cohort(
    store: ArenaStore,
    request: RankingRequest | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build masked, transitive comparison evidence and call the native kernel.

    A single task/variant/protocol per dataset is required after filtering.
    """
    req = request if isinstance(request, RankingRequest) else RankingRequest(**(request or {}))
    artifact = _artifact(req)
    if req.method in {"mixed_effects", "fanova"}:
        return _unavailable(artifact, f"{req.method}_requires_component_analysis_request")
    if req.method in CAPABILITIES["deferred"]:
        return _unavailable(artifact, CAPABILITIES["deferred"][req.method])
    if req.method not in METHODS or req.dimension not in DIMENSIONS:
        return _unavailable(artifact, "unsupported_method_or_dimension")
    if req.direction is None:
        return _unavailable(artifact, "custom_metric_direction_required")
    if req.dimension == "parameter" and (not req.operator or not req.parameter):
        return _unavailable(artifact, "operator_qualified_parameter_required")
    if req.comparison_mode == "matched" and req.dimension in {"pipeline", "split"}:
        return _unavailable(artifact, "matched_mode_requires_component_dimension")
    if req.comparison_mode == "matched" and req.dimension == "preprocessing_operator" and not req.operator:
        return _unavailable(artifact, "matched_preprocessing_operator_requires_operator")
    if req.dimension == "split" and (
        req.split_criterion != "external_error"
        or req.scope not in {"test", "refit"}
        or req.partition not in {"test", "final"}
    ):
        return _unavailable(artifact, "split_requires_matched_external_error")
    # Known metric direction is authoritative; custom metrics retain explicit caller direction.
    expected_direction = direction_of(req.metric) if req.metric.lower() in METRIC_DIRECTION else req.direction
    if expected_direction != req.direction:
        return _unavailable(artifact, "metric_direction_mismatch")
    batch = original_observations(
        store,
        metric=req.metric,
        scope=req.scope,
        partition=req.partition,
        dataset=req.dataset,
        task=req.task,
        collection=req.collection,
        protocol=req.protocol,
    )
    artifact["provenance"]["source_observations"] = batch.source_count
    artifact["provenance"]["source_scan"] = batch.scan
    if batch.limit_exceeded:
        artifact["warnings"].append(batch.limit_warning)
        return _unavailable(artifact, "source_observation_limit_requires_filter")
    rows = batch.rows
    excluded: Counter[str] = Counter()
    accepted = []
    cv_bindings: dict[str, bool] = {}
    for row in rows:
        reason = None
        if (
            row["execution_status"] not in {"ok", "completed"}
            or row["execution_validity"] != "valid"
            or row["score_validity"] != "valid"
        ):
            reason = "execution_or_score_not_valid_completed"
        elif json.loads(row["unsafe_flags_json"] or "[]"):
            reason = "unsafe_execution"
        elif row["metric_value"] is None or not math.isfinite(row["metric_value"]):
            reason = "missing_or_nonfinite_metric"
        elif row["direction"] != req.direction:
            reason = "metric_direction_mismatch"
        else:
            reason = comparison_identity_reason(row, scope=req.scope, partition=req.partition, cv_bindings=cv_bindings)
        if reason:
            excluded[reason] += 1
        else:
            accepted.append(row)
    artifact["provenance"]["excluded"] = dict(excluded)
    artifact["provenance"]["accepted_observations"] = len(accepted)
    if not accepted:
        return _unavailable(artifact, "no_eligible_observations")
    fold_granularity = cv_fold_granularity(req.scope, accepted)
    if fold_granularity == MIXED_CV_FOLD_GRANULARITY:
        return _unavailable(artifact, MIXED_CV_FOLD_GRANULARITY)
    # Per-fold scores are averaged by the host across matched folds, never a native pooled OOF score.
    fold_aggregation = "cv_fold_mean" if fold_granularity == "per_fold" else None
    artifact["provenance"]["fold_aggregation"] = fold_aggregation
    if len({r["score_computation_hash"] for r in accepted}) > 1:
        return _unavailable(artifact, "ambiguous_protocol_requires_filter")
    if len({(r["task_type"], r["aggregation_level"]) for r in accepted}) > 1:
        return _unavailable(artifact, "incompatible_task_type_or_aggregation")
    contexts: dict[str, set[tuple[Any, ...]]] = defaultdict(set)
    for row in accepted:
        contexts[row["dataset_fingerprint"]].add(
            (row["task_hash"], row["dataset_variant_hash"], row["score_computation_hash"])
        )
    if any(len(context) > 1 for context in contexts.values()):
        return _unavailable(artifact, "ambiguous_dataset_context_requires_filter")
    pipeline_ids = {row["pipeline_dag_hash"] for row in accepted}
    labels, ambiguous = _labels(store, req, pipeline_ids)
    component_contexts = {}
    if req.comparison_mode == "matched":
        from nirs4all_benchmarks.identity.pipeline_dag import GRAPH_NORMALIZATION_VERSION

        from .contexts import build_component_contexts

        pipeline_nodes: dict[str, list[dict[str, Any]]] = defaultdict(list)
        pipeline_edges: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for node in pipeline_rows(store, "pipeline_nodes", pipeline_ids):
            pipeline_nodes[node["pipeline_dag_hash"]].append(node)
        for edge in pipeline_rows(store, "pipeline_edges", pipeline_ids):
            pipeline_edges[edge["pipeline_dag_hash"]].append(edge)
        pipelines = [
            {
                **row,
                "nodes": pipeline_nodes[row["pipeline_dag_hash"]],
                "edges": pipeline_edges[row["pipeline_dag_hash"]],
            }
            for row in pipeline_rows(store, "pipeline_dags", pipeline_ids)
        ]
        component_contexts = build_component_contexts(
            pipelines, req.dimension, parameter_operator=req.operator, parameter_name=req.parameter
        )
        for pipeline in pipelines:
            try:
                source = json.loads(pipeline["graph_json"])
            except (TypeError, ValueError):
                source = None
            if not isinstance(source, dict) or source.get("arena_normalization_version") != GRAPH_NORMALIZATION_VERSION:
                component_contexts[pipeline["pipeline_dag_hash"]] = {
                    "fingerprint": None,
                    "excluded_reason": "matched_graph_source_requires_reingestion",
                }
    if req.dimension == "split":
        # Rank CV selection strategies on an identical external holdout. Their
        # internal folds vary intentionally; the outer split indices must match.
        labels = {}
        split_names = {row["split_spec_hash"]: row["method"] for row in store.query("SELECT * FROM split_specs")}
        cv_names = {row["cv_spec_hash"]: row["method"] for row in store.query("SELECT * FROM cv_specs")}
        for row in accepted:
            sid = canonical_json([row["split_spec_hash"], row["cv_spec_hash"]])
            label = f"{split_names.get(row['split_spec_hash'], 'Unreported split')} / "
            label += cv_names.get(row["cv_spec_hash"], "Unreported CV")
            labels[row["run_condition_hash"]] = [(sid, label)]
    blocks: dict[str, dict[str, list[tuple[float, str, str]]]] = defaultdict(lambda: defaultdict(list))
    block_meta = {}
    candidate_labels = {}
    candidate_pipelines: dict[str, set[str]] = defaultdict(set)
    seen = set()
    for row in accepted:
        pipe = row["pipeline_dag_hash"]
        if req.comparison_mode == "matched" and not component_contexts.get(pipe, {}).get("fingerprint"):
            context_reason = (
                component_contexts.get(pipe, {}).get("excluded_reason") or "missing_matched_component_context"
            )
            excluded[context_reason] += 1
            continue
        if pipe in ambiguous:
            excluded["ambiguous_component_identity"] += 1
            continue
        candidate_entries = labels.get(row["run_condition_hash"] if req.dimension == "split" else pipe, [])
        meta = {
            key: row[key]
            for key in (
                "dataset_fingerprint",
                "dataset_variant_hash",
                "task_hash",
                "score_computation_hash",
                "split_indices_hash",
                "refit_strategy_hash",
                "aggregation_level",
                "fold_id",
            )
        }
        meta["cv_identity"] = row["engine_fold_set_fingerprint"] if req.scope in CV_IDENTITY_SCOPES else None
        meta["cv_instance_hash"] = row["cv_instance_hash"] if req.scope in CV_IDENTITY_SCOPES else None
        if req.comparison_mode == "matched":
            meta["rest_of_pipeline_fingerprint"] = component_contexts[pipe]["fingerprint"]
        # For splitter comparisons match the rest of the complete pipeline too.
        if req.dimension == "split":
            meta["pipeline_dag_hash"] = pipe
        block_id = fingerprint(meta)
        block_meta[block_id] = meta
        for candidate, label in candidate_entries:
            # Duplicate ingestion and repeated identical score rows never add weight.
            identity = (block_id, candidate, row["execution_hash"], row["score_set_id"], row["metric_value"])
            if identity in seen:
                excluded["duplicate_observation"] += 1
                continue
            seen.add(identity)
            candidate_labels[candidate] = label
            candidate_pipelines[candidate].add(pipe)
            blocks[block_id][candidate].append((row["metric_value"], pipe, row["execution_hash"]))
    if len(candidate_labels) > 500:
        return _unavailable(artifact, "cohort_size_limit_requires_filter")
    pair_values: dict[tuple[str, str, str], list[tuple[float, float]]] = defaultdict(list)
    pair_blocks: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for block_id, values in sorted(blocks.items()):
        for a, b in itertools.combinations(sorted(values), 2):
            if req.dimension == "preprocessing_operator" and ({v[1] for v in values[a]} & {v[1] for v in values[b]}):
                excluded["cooccurring_operator_comparison"] += 1
                continue
            # Collapse executions within pipeline first: hyperparameter trial counts
            # cannot make a heavily repeated pipeline dominate its component group.
            key = (block_meta[block_id]["dataset_fingerprint"], a, b)
            pair_values[key].append((_mean_component(values[a]), _mean_component(values[b])))
            pair_blocks[key].append(block_id)
            if len(pair_values) * len(candidate_labels) > 2_000_000:
                return _unavailable(artifact, "cohort_size_limit_requires_filter")
    provenance = artifact["provenance"]
    provenance.update(
        {
            "excluded": dict(excluded),
            "datasets": sorted(contexts),
            "matched_blocks": len(blocks),
            "blocks": [{"id": key, **block_meta[key]} for key in sorted(blocks)],
            "comparison_rows": len(pair_values),
        }
    )
    provenance["candidate_universe"] = [
        {"id": key, "label": candidate_labels[key], "pipelines": sorted(candidate_pipelines[key])}
        for key in sorted(candidate_labels)
    ]
    if not pair_values:
        return _unavailable(artifact, "no_matched_comparisons")
    candidates = sorted(candidate_labels)
    if len(candidates) > 500 or len(pair_values) * len(candidates) > 2_000_000:
        return _unavailable(artifact, "cohort_size_limit_requires_filter")
    index = {candidate: i for i, candidate in enumerate(candidates)}
    scores, present = [], []
    for (dataset, a, b), pair_observations in sorted(pair_values.items()):
        score, mask = [0.0] * len(candidates), [0] * len(candidates)
        for candidate, col in ((a, 0), (b, 1)):
            score[index[candidate]] = sum(v[col] for v in pair_observations) / len(pair_observations)
            mask[index[candidate]] = 1
        scores.append(score)
        present.append(mask)
        provenance["comparisons"].append(
            {"dataset": dataset, "candidate_ids": [a, b], "matched_blocks": pair_blocks[(dataset, a, b)]}
        )
    if req.method == "average_rank":
        dataset_blocks: dict[str, list[dict[str, list[tuple[float, str, str]]]]] = defaultdict(list)
        for block_id, values in blocks.items():
            dataset_blocks[block_meta[block_id]["dataset_fingerprint"]].append(values)
        scores, present = [], []
        for dataset in sorted(dataset_blocks):
            panels = dataset_blocks[dataset]
            if len({tuple(sorted(panel)) for panel in panels}) != 1:
                return _unavailable(artifact, "average_rank_requires_common_dataset_panel")
            if req.dimension == "preprocessing_operator" and any(
                {v[1] for v in panel[a]} & {v[1] for v in panel[b]}
                for panel in panels
                for a, b in itertools.combinations(panel, 2)
            ):
                return _unavailable(artifact, "average_rank_requires_nonoverlapping_components")
            score, mask = [0.0] * len(candidates), [0] * len(candidates)
            for candidate in panels[0]:
                score[index[candidate]] = sum(_mean_component(panel[candidate]) for panel in panels) / len(panels)
                mask[index[candidate]] = 1
            scores.append(score)
            present.append(mask)
        provenance["comparison_rows"] = len(scores)
    artifact["warnings"] = [
        "Observed component distributions are descriptive; they are not matched, adjusted or causal contributions.",
        "Executions are averaged within pipeline, pipelines within matched block, then paired blocks within dataset.",
        "Dataset domains must support a stable common rating; indirect comparisons rely on that modeling assumption.",
        "Mean ranks on different candidate subsets are descriptive and may not be comparable.",
    ]
    if fold_aggregation == "cv_fold_mean":
        artifact["warnings"].append(
            "CV values are host means of per-fold scores within matched folds; "
            "they are not the producer's pooled avg/w_avg out-of-fold scores."
        )
    if req.comparison_mode == "matched":
        artifact["warnings"].append(
            "Matched component comparisons hold the rest of the strict linear pipeline fixed; "
            "they remain descriptive and do not establish causal effects."
        )
    if req.method == "average_rank":
        artifact["warnings"] += [
            "Each dataset contributes one genuine candidate panel; all its matched blocks use that panel.",
            "Friedman uses complete dataset panels only and assumes independent datasets.",
        ]
    else:
        artifact["warnings"] += [
            "Each dataset contributes once per candidate pair; candidate-rich datasets contribute more distinct pairs.",
            "Pair rows within a dataset are dependent; uncertainty does not establish independent study effects.",
        ]
    artifact["_cohort"] = {"scores": scores, "present": present, "candidates": candidates, "labels": candidate_labels}
    return artifact


def quality_ranking(
    store: ArenaStore,
    request: RankingRequest | dict[str, Any] | None = None,
    *,
    ranker: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble controlled evidence and rank it through the native methods ABI."""
    req = request if isinstance(request, RankingRequest) else RankingRequest(**(request or {}))
    artifact = _assemble_cohort(store, req)
    evidence = artifact.pop("_cohort", None)
    if evidence is None:
        return artifact
    scores, present = evidence["scores"], evidence["present"]
    candidates, candidate_labels = evidence["candidates"], evidence["labels"]
    try:
        compute = ranker or _native_rank()
    except (ImportError, OSError, AttributeError, RuntimeError):
        return _unavailable(artifact, "native_ranking_runtime_unavailable")
    try:
        native = compute(
            scores,
            present=present,
            method=req.method,
            higher_is_better=req.direction == "max",
            tie_tolerance=req.tie_tolerance,
            **req.options,
        )
    except (ImportError, OSError, AttributeError):
        return _unavailable(artifact, "native_ranking_runtime_unavailable")
    except (ValueError, TypeError, RuntimeError) as exc:
        artifact["warnings"].append(f"Native input rejected: {type(exc).__name__}: {exc}")
        return _unavailable(artifact, "native_ranking_rejected_input")
    if native.get("schema_version") != "n4m.ranking/1":
        return _unavailable(artifact, "native_ranking_schema_unsupported")
    artifact.update(
        {
            "state": "ready",
            "reason": None,
            "native_schema_version": native["schema_version"],
            "candidates": [
                {
                    "id": candidates[row["index"]],
                    "label": candidate_labels[candidates[row["index"]]],
                    **{k: v for k, v in row.items() if k != "index"},
                }
                for row in native["candidates"]
            ],
            "coverage": native["coverage"],
            "convergence": native["convergence"],
            "uncertainty_kind": native["uncertainty_kind"],
            "score_direction": native["score_direction"],
            "inference": native["inference"]
            if req.method == "average_rank"
            else {
                "reason": "friedman_suppressed_for_dependent_pair_decomposition",
                "friedman_statistic": None,
                "friedman_p_value": None,
                "degrees_of_freedom": None,
                "p_value_method": None,
            },
            "native_variant": native.get("variant"),
            "native_options": native.get("options"),
        }
    )
    artifact["warnings"].extend(
        warning for warning in native.get("warnings", []) if req.method == "average_rank" or "Friedman" not in warning
    )
    return artifact
