"""Paired positive-loss effects with caller-supplied study-effect variances."""

from __future__ import annotations

import importlib
import math
from collections.abc import Callable
from typing import Any, Literal

from pydantic import Field

from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.store import ArenaStore

from .quality import CAPABILITIES, RankingRequest, _assemble_cohort

POSITIVE_LOSSES = {"rmse", "mse", "mae", "medae", "nrmse", "log_loss", "logloss"}


class RelativeGainRequest(RankingRequest):
    """Exact comparison identity; variances describe relative paired effects."""

    method: Literal["relative_gain"] = "relative_gain"
    baseline_id: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    effect_variances: dict[str, float] | None = None


def quality_relative_gain(
    store: ArenaStore,
    request: RelativeGainRequest | dict[str, Any],
    *,
    estimator: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Estimate effects only from directly paired comparable dataset evidence."""
    req = request if isinstance(request, RelativeGainRequest) else RelativeGainRequest(**request)
    normalized = req.model_dump(mode="json")
    ranking_fields = {key: normalized[key] for key in RankingRequest.model_fields}
    ranking_fields["method"] = "bradley_terry"
    assembled = _assemble_cohort(store, RankingRequest(**ranking_fields))
    evidence = assembled.pop("_cohort", None)
    artifact = {
        "schema_version": "n4a.quality-relative-gain/1",
        "state": "unavailable",
        "reason": None,
        "request_id": fingerprint(normalized),
        "request": normalized,
        **{
            key: normalized[key]
            for key in ("dimension", "metric", "scope", "partition", "direction", "baseline_id", "candidate_id")
        },
        "candidates": [],
        "studies": [],
        "result": None,
        "provenance": assembled["provenance"],
        "warnings": [
            "Relative gains require direct paired positive-loss evidence; indirect ratings do not identify ratios.",
            "Dataset effects are descriptive unless caller-supplied sampling variances and independence are justified.",
            "Effect variances must include paired covariance; fold independence and variances are never inferred.",
        ],
        "capabilities": CAPABILITIES,
    }

    def unavailable(reason: str) -> dict[str, Any]:
        artifact["reason"] = reason
        return artifact

    if req.metric.lower() not in POSITIVE_LOSSES or req.direction != "min":
        return unavailable("relative_gain_requires_positive_loss_metric")
    if req.baseline_id == req.candidate_id:
        return unavailable("distinct_baseline_and_candidate_required")
    if evidence is None:
        if assembled["reason"] == "source_observation_limit_requires_filter":
            artifact["warnings"].extend(assembled["warnings"])
        return unavailable(assembled["reason"])
    candidate_ids = evidence["candidates"]
    if req.baseline_id not in candidate_ids or req.candidate_id not in candidate_ids:
        return unavailable("comparison_candidate_not_in_cohort")
    artifact["candidates"] = [
        {"id": candidate, "label": evidence["labels"][candidate]} for candidate in (req.baseline_id, req.candidate_id)
    ]
    baseline_index, candidate_index = candidate_ids.index(req.baseline_id), candidate_ids.index(req.candidate_id)
    baseline, candidate, datasets = [], [], []
    for i, comparison in enumerate(assembled["provenance"]["comparisons"]):
        if set(comparison["candidate_ids"]) != {req.baseline_id, req.candidate_id}:
            continue
        losses = evidence["scores"][i]
        if losses[baseline_index] <= 0 or losses[candidate_index] <= 0:
            return unavailable("relative_gain_requires_strictly_positive_losses")
        dataset = comparison["dataset"]
        datasets.append(dataset)
        baseline.append(losses[baseline_index])
        candidate.append(losses[candidate_index])
        artifact["studies"].append(
            {
                "dataset": dataset,
                "baseline_loss": losses[baseline_index],
                "candidate_loss": losses[candidate_index],
                "effect_variance": None,
                "matched_blocks": comparison["matched_blocks"],
            }
        )
    if not datasets:
        return unavailable("no_direct_paired_dataset_evidence")
    variances = None
    if req.effect_variances is not None:
        if set(req.effect_variances) != set(datasets):
            return unavailable("effect_variances_must_match_paired_dataset_ids")
        if any(not math.isfinite(value) or value <= 0 for value in req.effect_variances.values()):
            return unavailable("effect_variances_must_be_finite_positive")
        variances = [req.effect_variances[dataset] for dataset in datasets]
        for study, variance in zip(artifact["studies"], variances, strict=True):
            study["effect_variance"] = variance
    try:
        compute = estimator or importlib.import_module("n4m.ranking").paired_relative_gain
    except (ImportError, OSError, AttributeError, RuntimeError):
        return unavailable("native_ranking_runtime_unavailable")
    try:
        result = compute(baseline, candidate, variances=variances)
    except (ValueError, TypeError, RuntimeError) as exc:
        artifact["warnings"].append(f"Native input rejected: {type(exc).__name__}: {exc}")
        return unavailable("native_relative_gain_rejected_input")
    if result.get("schema_version") != "n4m.relative_gain/1":
        return unavailable("native_relative_gain_schema_unsupported")
    artifact.update({"state": "ready", "result": result})
    artifact["provenance"]["study_dataset_ids"] = datasets
    artifact["provenance"]["independence_assumption"] = "caller_assessed_independent_dataset_effects"
    if variances is None:
        artifact["warnings"].append("No effect variances supplied: pooled gain and standard error are unavailable.")
    return artifact
