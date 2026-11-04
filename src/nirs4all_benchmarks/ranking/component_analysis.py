"""Original-observation designs for native adjusted effects and surrogate importance.

This module selects evidence and constructs declared design matrices. All outcome
normalization, fitting, covariance and variance decomposition belong to methods.
"""

from __future__ import annotations

import importlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.pipeline_dag import GRAPH_NORMALIZATION_VERSION
from nirs4all_benchmarks.scoring.metrics import METRIC_DIRECTION
from nirs4all_benchmarks.store import ArenaStore

from .contexts import _chain
from .observations import original_observations
from .relative_gain import POSITIVE_LOSSES

SCALE_FREE_METRICS = {
    "nrmse",
    "r2",
    "rpd",
    "rpiq",
    "ccc",
    "accuracy",
    "balanced_accuracy",
    "f1_macro",
    "f1_micro",
    "f1_weighted",
    "precision_macro",
    "recall_macro",
    "roc_auc",
    "mcc",
}
Scalar = str | int | float | bool


class AnalysisFactor(BaseModel):
    """An explicit factor, never a user formula or executable expression."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    name: str = Field(min_length=1, max_length=120)
    dimension: Literal[
        "model", "preprocessing_sequence", "preprocessing_operator", "feature_selection", "augmentation", "parameter"
    ]
    kind: Literal["categorical", "numeric"] = "categorical"
    operator: str | None = None
    operator_version: str | None = None
    parameter: str | None = None
    reference: str | None = None
    levels: list[str] | None = None
    bounds: tuple[float, float] | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> AnalysisFactor:
        if self.dimension in {"parameter", "preprocessing_operator"} and not self.operator:
            raise ValueError("parameter and preprocessing-operator factors require an exact operator")
        if self.dimension == "parameter" and not self.parameter:
            raise ValueError("parameter factors require a parameter name")
        if self.kind == "numeric" and self.dimension != "parameter":
            raise ValueError("only an operator-qualified parameter may be numeric")
        if self.levels is not None and (
            len(self.levels) < 2 or len({canonical_json(x) for x in self.levels}) != len(self.levels)
        ):
            raise ValueError("categorical levels must contain at least two distinct JSON scalar values")
        if self.bounds is not None and not self.bounds[0] < self.bounds[1]:
            raise ValueError("numeric bounds must be finite and strictly increasing")
        if self.kind == "numeric" and (self.reference is not None or self.levels is not None):
            raise ValueError("numeric factors use a slope and bounds, not categorical references or levels")
        if self.kind == "categorical" and self.bounds is not None:
            raise ValueError("categorical factors use levels, not numeric bounds")
        if self.dimension not in {"parameter", "preprocessing_operator"} and (self.operator or self.operator_version):
            raise ValueError("operator selectors apply only to parameter or preprocessing-operator factors")
        if self.dimension != "parameter" and self.parameter:
            raise ValueError("a parameter selector requires the parameter dimension")
        return self


class ComponentAnalysisRequest(BaseModel):
    """Complete identity of an adjusted-effects or fANOVA analysis."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    method: Literal["mixed_effects", "fanova"] = "mixed_effects"
    metric: str = "nrmse"
    scope: Literal["fold", "cv", "refit", "test", "view"] = "cv"
    partition: Literal["train", "validation", "test", "final"] = "validation"
    direction: Literal["min", "max"] | None = None
    dataset: str | None = None
    task: str | None = None
    collection: str | None = None
    protocol: str | None = None
    aggregation_level: str = "sample"
    model_slice: str | None = None
    factors: list[AnalysisFactor] = Field(
        default_factory=lambda: [AnalysisFactor(name="model", dimension="model")], min_length=1, max_length=16
    )
    outcome_policy: Literal["scale_free", "native_baseline_ratio"] = "scale_free"
    baseline_pipeline_id: str | None = None
    missing_policy: Literal["reject", "exclude_complete_case"] = "reject"
    training_datasets: list[str] = Field(default_factory=list)
    validation_datasets: list[str] = Field(default_factory=list)
    options: dict[str, float | int | bool] = Field(default_factory=dict)
    preview_only: bool = False

    @model_validator(mode="after")
    def validate_identity(self) -> ComponentAnalysisRequest:
        if self.direction is None:
            expected = METRIC_DIRECTION.get(self.metric.lower())
            if expected in {"min", "max"}:
                self.direction = "min" if expected == "min" else "max"
        if len({factor.name for factor in self.factors}) != len(self.factors):
            raise ValueError("factor names must be unique")
        if len(set(self.training_datasets)) != len(self.training_datasets) or len(set(self.validation_datasets)) != len(
            self.validation_datasets
        ):
            raise ValueError("dataset selections must not contain duplicates")
        if set(self.training_datasets) & set(self.validation_datasets):
            raise ValueError("training and validation dataset identities must be disjoint")
        if self.method == "mixed_effects" and (self.training_datasets or self.validation_datasets):
            raise ValueError("training and validation assignments apply only to fANOVA")
        if self.outcome_policy == "scale_free" and self.baseline_pipeline_id:
            raise ValueError("baseline_pipeline_id requires native_baseline_ratio")
        integer_options = {
            "max_evaluations",
            "n_trees",
            "max_depth",
            "min_samples_leaf",
            "max_features",
            "seed",
            "max_order",
            "max_marginal_evaluations",
        }
        for name, value in self.options.items():
            if name in integer_options and (type(value) is not int or abs(value) > 9007199254740991):
                raise ValueError(f"{name} must be an exact JSON-safe integer")
        return self


def _base(req: ComponentAnalysisRequest) -> dict[str, Any]:
    request = req.model_dump(mode="json")
    return {
        "schema_version": "n4a.component-analysis/1",
        "state": "unavailable",
        "reason": None,
        "request_id": fingerprint(request),
        "request": request,
        "method": req.method,
        "metric": req.metric,
        "scope": req.scope,
        "partition": req.partition,
        "direction": req.direction,
        "design": {
            "factors": [],
            "columns": [],
            "references": {},
            "domains": [],
            "model_slice": req.model_slice,
            "outcome_policy": req.outcome_policy,
            "group_names": ["dataset", "dataset_evaluation_block"],
        },
        "provenance": {
            "source_observations": 0,
            "accepted_observations": 0,
            "excluded": {},
            "datasets": [],
            "observations": [],
            "training_datasets": req.training_datasets,
            "validation_datasets": req.validation_datasets,
            "baseline_observations": [],
        },
        "result": None,
        "warnings": [
            "Adjusted coefficients and surrogate importance describe observed configurations "
            "and do not establish causal effects.",
            "Original score observations are used; synthetic pairwise ranking rows are never fitted.",
            "Dataset and evaluation-block dependence is modeled only through the declared random intercepts; "
            "further dependence may remain.",
        ],
    }


def _unavailable(artifact: dict[str, Any], reason: str) -> dict[str, Any]:
    artifact["reason"] = reason
    return artifact


def _observations(store: ArenaStore, req: ComponentAnalysisRequest, artifact: dict[str, Any]) -> list[dict[str, Any]]:
    batch = original_observations(
        store,
        metric=req.metric,
        scope=req.scope,
        partition=req.partition,
        dataset=req.dataset,
        task=req.task,
        collection=req.collection,
        protocol=req.protocol,
        aggregation_level=req.aggregation_level,
    )
    artifact["provenance"]["source_observations"] = batch.source_count
    artifact["provenance"]["source_scan"] = batch.scan
    if batch.limit_exceeded:
        artifact["reason"] = "source_observation_limit_requires_filter"
        artifact["warnings"].append(batch.limit_warning)
        return []
    rows = batch.rows
    excluded: Counter[str] = Counter()
    accepted = []
    seen = set()
    chains: dict[str, tuple[Any, str | None]] = {}
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
        elif not all(
            row[key]
            for key in (
                "dataset_fingerprint",
                "dataset_variant_hash",
                "pipeline_dag_hash",
                "task_hash",
                "task_type",
                "score_computation_hash",
                "split_indices_hash",
            )
        ):
            reason = "missing_comparison_identity"
        elif req.scope in {"cv", "fold"} and not row["engine_fold_set_fingerprint"]:
            reason = "missing_actual_cv_identity"
        elif req.scope == "fold" and row["fold_id"] is None:
            reason = "missing_fold_identity"
        elif req.scope in {"refit", "test"} and not row["refit_strategy_hash"]:
            reason = "missing_refit_identity"
        if reason is None:
            pipeline_id = row["pipeline_dag_hash"]
            if pipeline_id not in chains:
                try:
                    pipeline = store.get("pipeline_dags", "pipeline_dag_hash", pipeline_id) or {}
                    raw_graph = pipeline.get("graph_json")
                    if not isinstance(raw_graph, str):
                        raise ValueError("missing serialized graph source")
                    graph = json.loads(raw_graph)
                    if graph.get("arena_normalization_version") != GRAPH_NORMALIZATION_VERSION:
                        chains[pipeline_id] = (None, "analysis_graph_source_requires_reingestion")
                    else:
                        chains[pipeline_id] = (_chain(graph)[0], None)
                except (TypeError, ValueError, KeyError, AttributeError):
                    chains[pipeline_id] = (None, "analysis_requires_qualified_linear_graph")
            chain, reason = chains[pipeline_id]
            if reason is None:
                row["_chain"] = chain
        if reason:
            excluded[reason] += 1
            continue
        identity = (row["execution_hash"], row["score_set_id"], row["fold_id"], row["metric_value"])
        if identity in seen:
            excluded["duplicate_observation"] += 1
            continue
        seen.add(identity)
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
        meta["actual_cv_identity"] = row["engine_fold_set_fingerprint"] if req.scope in {"cv", "fold"} else None
        row["_block_id"] = fingerprint(meta)
        row["_block"] = meta
        accepted.append(row)
    artifact["provenance"]["excluded"] = dict(excluded)
    return accepted


def _factor_value(row: dict[str, Any], factor: AnalysisFactor) -> Scalar:
    chain = row["_chain"]
    if factor.dimension == "model":
        node = next(node["content"] for node in chain if node["role"] == "model")
        return f"{node['operator']}@{node['operator_version'] or 'unreported'}"
    if factor.dimension == "parameter":
        selected = [node["content"] for node in chain if node["content"]["operator"] == factor.operator]
        if len(selected) != 1 or factor.parameter not in selected[0]["params"]:
            raise ValueError("missing_or_ambiguous_parameter")
        node = selected[0]
        if factor.operator_version is not None and node["operator_version"] != factor.operator_version:
            raise ValueError("parameter_operator_version_mismatch")
        value = node["params"][factor.parameter]
        if (
            value is None
            or not isinstance(value, (str, int, float, bool))
            or (isinstance(value, float) and not math.isfinite(value))
        ):
            raise ValueError("parameter_requires_finite_json_scalar")
        if factor.kind == "categorical":
            # JSON parsers erase the difference between 1 and 1.0, despite
            # operators such as min_samples_leaf giving them different meaning.
            # Type-tagged string IDs preserve category identity in static clients.
            kind = {str: "string", int: "integer", float: "number", bool: "boolean"}[type(value)]
            return canonical_json({"type": kind, "value": value})
        if type(value) is int and abs(value) > 9007199254740991:
            raise ValueError("numeric_parameter_requires_exact_f64_integer")
        return value
    roles = {
        "preprocessing_sequence": {"preprocessing", "scaler"},
        "preprocessing_operator": {"preprocessing", "scaler"},
        "feature_selection": {"feature_selection"},
        "augmentation": {"augmentation"},
    }
    selected = [node["content"] for node in chain if node["role"] in roles[factor.dimension]]
    if factor.dimension == "preprocessing_operator":
        selected = [node for node in selected if node["operator"] == factor.operator]
        if len(selected) > 1:
            raise ValueError("ambiguous_preprocessing_operator")
        if not selected:
            return "none"
        node = selected[0]
        if factor.operator_version is not None and node["operator_version"] != factor.operator_version:
            raise ValueError("preprocessing_operator_version_mismatch")
        return f"{node['operator']}@{node['operator_version'] or 'unreported'}"
    return canonical_json(selected) if selected else "none"


def _native() -> Any:
    return importlib.import_module("n4m.component_analysis")


def component_analysis(
    store: ArenaStore,
    request: ComponentAnalysisRequest | dict[str, Any],
    *,
    estimator: Callable[..., dict[str, Any]] | None = None,
    normalizer: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Select original evidence and delegate all numerical inference to methods."""
    req = request if isinstance(request, ComponentAnalysisRequest) else ComponentAnalysisRequest(**request)
    artifact = _base(req)
    allowed_options = {
        "mixed_effects": {"reml", "max_evaluations", "tolerance", "rank_tolerance", "boundary_tolerance"},
        "fanova": {
            "n_trees",
            "max_depth",
            "min_samples_leaf",
            "max_features",
            "seed",
            "max_order",
            "max_marginal_evaluations",
        },
    }
    if set(req.options) - allowed_options[req.method]:
        return _unavailable(artifact, "unsupported_component_analysis_option")
    if req.direction is None or req.direction != METRIC_DIRECTION.get(req.metric.lower()):
        return _unavailable(artifact, "known_metric_direction_required")
    if req.outcome_policy == "scale_free" and req.metric.lower() not in SCALE_FREE_METRICS:
        return _unavailable(artifact, "raw_metric_requires_native_baseline_normalization")
    if req.outcome_policy == "native_baseline_ratio" and (
        req.metric.lower() not in POSITIVE_LOSSES or req.direction != "min" or not req.baseline_pipeline_id
    ):
        return _unavailable(artifact, "baseline_normalization_requires_positive_loss_and_pipeline")
    if any(factor.dimension == "parameter" for factor in req.factors) and not req.model_slice:
        return _unavailable(artifact, "parameter_analysis_requires_model_slice")
    rows = _observations(store, req, artifact)
    if artifact["reason"] is not None:
        return artifact
    if not rows:
        return _unavailable(artifact, "no_eligible_original_observations")
    if len(rows) > 8192:
        return _unavailable(artifact, "analysis_size_limit_requires_filter")
    if (
        len({row["score_computation_hash"] for row in rows}) != 1
        or len({(row["task_type"], row["aggregation_level"]) for row in rows}) != 1
    ):
        return _unavailable(artifact, "incompatible_task_type_aggregation_or_protocol")
    dataset_contexts: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in rows:
        dataset_contexts[row["dataset_fingerprint"]].add((row["task_hash"], row["dataset_variant_hash"]))
    if any(len(contexts) > 1 for contexts in dataset_contexts.values()):
        return _unavailable(artifact, "ambiguous_dataset_context_requires_filter")
    identity_values: dict[tuple[Any, ...], set[float]] = defaultdict(set)
    for row in rows:
        identity_values[(row["execution_hash"], row["score_set_id"], row["fold_id"])].add(row["metric_value"])
    if any(len(values) > 1 for values in identity_values.values()):
        return _unavailable(artifact, "conflicting_original_metric_observations")
    rows.sort(
        key=lambda row: (row["dataset_fingerprint"], row["_block_id"], row["pipeline_dag_hash"], row["execution_hash"])
    )
    baseline_rows = [row for row in rows if row["pipeline_dag_hash"] == req.baseline_pipeline_id]
    model_factor = AnalysisFactor(name="model", dimension="model")
    if req.model_slice:
        rows = [row for row in rows if _factor_value(row, model_factor) == req.model_slice]
    if not rows:
        return _unavailable(artifact, "model_slice_has_no_observations")
    excluded = Counter(artifact["provenance"]["excluded"])
    values, selected = [], []
    for row in rows:
        try:
            point = [_factor_value(row, factor) for factor in req.factors]
            for factor, value in zip(req.factors, point, strict=True):
                if factor.kind == "numeric" and (isinstance(value, bool) or not isinstance(value, (int, float))):
                    raise ValueError("numeric_parameter_required")
        except ValueError as exc:
            excluded[str(exc)] += 1
            if req.missing_policy == "reject":
                artifact["provenance"]["excluded"] = dict(excluded)
                return _unavailable(artifact, "incomplete_or_conditional_design_requires_slice")
            continue
        selected.append(row)
        values.append(point)
    rows = selected
    artifact["provenance"]["excluded"] = dict(excluded)
    if not rows:
        return _unavailable(artifact, "no_complete_case_observations")
    datasets = sorted({row["dataset_fingerprint"] for row in rows})
    artifact["provenance"].update(
        {
            "accepted_observations": len(rows),
            "datasets": datasets,
            "observations": [
                {
                    "id": row["metric_observation_id"],
                    "execution": row["execution_hash"],
                    "pipeline": row["pipeline_dag_hash"],
                    "dataset": row["dataset_fingerprint"],
                    "task": row["task_hash"],
                    "block_id": row["_block_id"],
                    "block": row["_block"],
                }
                for row in rows
            ],
        }
    )
    artifact["provenance"]["input_order"] = "dataset_block_pipeline_execution_lexical"
    design = artifact["design"]
    design["factors"] = [
        {
            **factor.model_dump(mode="json"),
            "observed_levels": sorted(
                {canonical_json(point[i]): point[i] for point in values}.values(), key=canonical_json
            ),
        }
        for i, factor in enumerate(req.factors)
    ]
    if req.preview_only:
        return _unavailable(artifact, "design_preview_only")
    # A parameter cannot silently mix operator implementation versions.
    for factor in req.factors:
        if factor.dimension == "parameter":
            versions = {
                node["content"]["operator_version"]
                for row in rows
                for node in row["_chain"]
                if node["content"]["operator"] == factor.operator
            }
            if len(versions) > 1:
                return _unavailable(artifact, "parameter_operator_version_requires_filter")
    matrix: list[list[float]] = [[1.0] if req.method == "mixed_effects" else [] for _ in rows]
    columns = [{"name": "Intercept", "factor": None, "level": None}] if req.method == "mixed_effects" else []
    feature_types, bounds = [], []
    for i, factor in enumerate(req.factors):
        observed = design["factors"][i]["observed_levels"]
        if factor.kind == "numeric":
            if req.method == "fanova" and factor.bounds is None:
                return _unavailable(artifact, "numeric_domain_bounds_required")
            if factor.bounds and any(not factor.bounds[0] <= float(point[i]) <= factor.bounds[1] for point in values):
                return _unavailable(artifact, "observation_outside_declared_domain")
            for point, vector in zip(values, matrix, strict=True):
                vector.append(float(point[i]))
            columns.append({"name": factor.name, "factor": factor.name, "level": None})
            feature_types.append(0)
            bounds.append(list(factor.bounds) if factor.bounds else None)
            design["domains"].append(
                {
                    "factor": factor.name,
                    "kind": "numeric",
                    "bounds": list(factor.bounds) if factor.bounds else None,
                    "levels": None,
                }
            )
        else:
            levels = factor.levels or observed
            if req.method == "fanova" and factor.levels is None:
                return _unavailable(artifact, "categorical_domain_levels_required")
            keys = [canonical_json(level) for level in levels]
            if any(canonical_json(point[i]) not in keys for point in values):
                return _unavailable(artifact, "observation_outside_declared_domain")
            if len(keys) < 2:
                return _unavailable(artifact, "factor_has_fewer_than_two_levels")
            if req.method == "fanova" and len(keys) > 128:
                return _unavailable(artifact, "analysis_domain_size_limit_requires_filter")
            if req.method == "mixed_effects":
                if factor.reference is None or canonical_json(factor.reference) not in keys:
                    return _unavailable(artifact, "categorical_reference_level_required")
                design["references"][factor.name] = factor.reference
                for key, level in zip(keys, levels, strict=True):
                    if key == canonical_json(factor.reference):
                        continue
                    for point, vector in zip(values, matrix, strict=True):
                        vector.append(float(canonical_json(point[i]) == key))
                    columns.append(
                        {"name": f"{factor.name} = {canonical_json(level)}", "factor": factor.name, "level": level}
                    )
            else:
                for point, vector in zip(values, matrix, strict=True):
                    vector.append(float(keys.index(canonical_json(point[i]))))
                columns.append({"name": factor.name, "factor": factor.name, "level": None})
                feature_types.append(len(levels))
                bounds.append([0.0, float(len(levels) - 1)])
            design["domains"].append({"factor": factor.name, "kind": "categorical", "bounds": None, "levels": levels})
    design["columns"] = columns
    if len(columns) > (64 if req.method == "mixed_effects" else 32) or len(columns) * len(rows) > 262144:
        return _unavailable(artifact, "analysis_design_size_limit_requires_filter")
    outcomes = [row["metric_value"] for row in rows]
    if req.outcome_policy == "native_baseline_ratio":
        baseline: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in baseline_rows:
            baseline[row["_block_id"]].append(row)
        denominators = []
        used_baselines = {}
        for row in rows:
            paired = baseline[row["_block_id"]]
            unique = {item["metric_value"] for item in paired}
            if len(unique) != 1:
                return _unavailable(artifact, "missing_or_ambiguous_baseline_observation")
            value = next(iter(unique))
            if value <= 0 or row["metric_value"] < 0:
                return _unavailable(artifact, "baseline_requires_positive_denominator_nonnegative_loss")
            denominators.append(value)
            for item in paired:
                used_baselines[item["metric_observation_id"]] = {
                    "id": item["metric_observation_id"],
                    "block_id": item["_block_id"],
                    "pipeline": item["pipeline_dag_hash"],
                    "loss": item["metric_value"],
                }
        artifact["provenance"]["baseline_observations"] = list(used_baselines.values())
        try:
            normalize = normalizer or _native().normalize_loss
            outcomes = list(normalize(outcomes, denominators))
        except (ImportError, OSError, AttributeError):
            return _unavailable(artifact, "native_outcome_normalization_unavailable")
        except (TypeError, ValueError, RuntimeError):
            return _unavailable(artifact, "native_outcome_normalization_rejected_input")
        if len(outcomes) != len(rows) or any(not math.isfinite(value) or value < 0 for value in outcomes):
            return _unavailable(artifact, "native_outcome_normalization_invalid_result")
        artifact["warnings"].append(
            "Response is the native loss/baseline ratio on identical actual evaluation blocks; lower ratios are better."
        )
        artifact["warnings"].append(
            "Baseline losses are treated as fixed normalizers; uncertainty of the estimated baseline is not propagated."
        )
    artifact["provenance"]["input_fingerprint"] = fingerprint(
        {
            "X": matrix,
            "y": outcomes,
            "evidence": [{"execution": row["execution_hash"], "block": row["_block_id"]} for row in rows],
        }
    )
    try:
        compute = estimator or getattr(_native(), req.method)
    except (ImportError, OSError, AttributeError, RuntimeError):
        return _unavailable(artifact, "native_component_analysis_runtime_unavailable")
    try:
        if req.method == "mixed_effects":
            dataset_index = {key: i for i, key in enumerate(datasets)}
            block_index = {key: i for i, key in enumerate(sorted({row["_block_id"] for row in rows}))}
            groups = [[dataset_index[row["dataset_fingerprint"]], block_index[row["_block_id"]]] for row in rows]
            if len(datasets) < 2 or len(block_index) <= len(datasets):
                return _unavailable(artifact, "random_effect_groups_require_multiple_datasets_and_blocks")
            if len(datasets) + len(block_index) > 1024 or len(rows) * (len(datasets) + len(block_index)) > 4194304:
                return _unavailable(artifact, "analysis_group_size_limit_requires_filter")
            result = compute(matrix, outcomes, groups, **req.options)
        else:
            if not req.training_datasets:
                return _unavailable(artifact, "training_dataset_selection_required")
            if not (set(req.training_datasets) | set(req.validation_datasets)) <= set(datasets):
                return _unavailable(artifact, "selected_training_or_validation_dataset_missing")
            if set(datasets) != set(req.training_datasets) | set(req.validation_datasets):
                return _unavailable(artifact, "every_dataset_requires_training_or_validation_assignment")
            train = [i for i, row in enumerate(rows) if row["dataset_fingerprint"] in req.training_datasets]
            valid = [i for i, row in enumerate(rows) if row["dataset_fingerprint"] in req.validation_datasets]
            design["training_coverage"] = []
            for i, factor in enumerate(req.factors):
                observed_train = sorted(
                    {canonical_json(values[index][i]): values[index][i] for index in train}.values(), key=canonical_json
                )
                design["training_coverage"].append({"factor": factor.name, "observed_levels": observed_train})
                if factor.kind == "categorical" and {canonical_json(level) for level in factor.levels or []} - {
                    canonical_json(level) for level in observed_train
                }:
                    return _unavailable(artifact, "categorical_domain_has_unobserved_training_levels")
                numeric_train = [float(value) for value in observed_train] if factor.kind == "numeric" else []
                if (
                    factor.kind == "numeric"
                    and factor.bounds
                    and (factor.bounds[0] < min(numeric_train) or factor.bounds[1] > max(numeric_train))
                ):
                    artifact["warnings"].append(
                        f"Declared domain for {factor.name} extends beyond observed training values; "
                        "surrogate extrapolation is not established."
                    )
            kwargs: dict[str, Any] = {"feature_types": feature_types, "bounds": bounds, **req.options}
            if valid:
                kwargs.update(X_valid=[matrix[i] for i in valid], y_valid=[outcomes[i] for i in valid])
            else:
                artifact["warnings"].append(
                    "No held-out dataset evidence supplied: surrogate generalization is not established."
                )
            result = compute([matrix[i] for i in train], [outcomes[i] for i in train], **kwargs)
    except (TypeError, ValueError, RuntimeError) as exc:
        artifact["warnings"].append(f"Native analysis rejected input: {type(exc).__name__}: {exc}")
        return _unavailable(artifact, "native_component_analysis_rejected_input")
    expected = "n4m.mixed_effects/1" if req.method == "mixed_effects" else "n4m.fanova/1"
    if not isinstance(result, dict) or result.get("schema_version") != expected:
        return _unavailable(artifact, "native_component_analysis_schema_unsupported")
    try:
        canonical_json(result)
    except (TypeError, ValueError, OverflowError):
        return _unavailable(artifact, "native_component_analysis_nonfinite_or_unserializable_result")
    artifact["result"] = result
    artifact["warnings"].extend(result.get("warnings", []))
    if result.get("state", "ready") != "ready":
        return _unavailable(artifact, result.get("reason") or "native_fit_unavailable")
    artifact.update(state="ready", reason=None)
    return artifact
