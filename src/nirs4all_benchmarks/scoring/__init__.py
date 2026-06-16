"""Versioned scoring — scores are facts, never bare numbers (DESIGN.md §10).

Every score carries a :class:`ScoreComputationSpec` (metric implementation, scope,
aggregation, filters, direction, date). Recomputation adds a new ``ScoreSet`` that
*supersedes* the old one; it never mutates it. Metrics are computed with NumPy only
so the Arena can re-derive scores from residuals without a producer dependency.
"""

from __future__ import annotations

from nirs4all_benchmarks.scoring.metrics import (
    METRIC_DIRECTION,
    compute_classification_metrics,
    compute_regression_metrics,
    direction_of,
    recompute_observations,
)
from nirs4all_benchmarks.scoring.score_spec import ScoreComputationSpec

__all__ = [
    "METRIC_DIRECTION",
    "ScoreComputationSpec",
    "compute_classification_metrics",
    "compute_regression_metrics",
    "direction_of",
    "recompute_observations",
]
