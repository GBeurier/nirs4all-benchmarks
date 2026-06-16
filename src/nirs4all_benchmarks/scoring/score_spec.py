"""``ScoreComputationSpec`` — the versioned identity of *how* a score was computed."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nirs4all_benchmarks.identity import fingerprint


@dataclass(frozen=True)
class ScoreComputationSpec:
    """A self-describing score-computation policy (DESIGN.md §6.15).

    Its ``score_computation_hash`` keys a ``ScoreSet``; two score sets computed by
    the same policy collapse, and a metric fix (new ``score_version`` or
    ``metric_implementation``) yields a new hash → a new, superseding score set.
    """

    score_version: str = "1.0"
    metric_implementation: str = "nirs4all_benchmarks.scoring.metrics/1"
    score_level: str = "cv"  # fold | cv | refit | test | view
    aggregation_policy: str = "macro_mean"
    direction: str | None = None
    standardization: str | None = None
    filters: dict[str, Any] = field(default_factory=dict)

    @property
    def score_computation_hash(self) -> str:
        return fingerprint(
            {
                "score_version": self.score_version,
                "metric_implementation": self.metric_implementation,
                "score_level": self.score_level,
                "aggregation_policy": self.aggregation_policy,
                "direction": self.direction,
                "standardization": self.standardization,
                "filters": self.filters,
            }
        )

    def to_row(self, created_at: str) -> dict[str, Any]:
        from nirs4all_benchmarks.identity import canonical_json

        return {
            "score_computation_hash": self.score_computation_hash,
            "score_version": self.score_version,
            "metric_implementation": self.metric_implementation,
            "score_level": self.score_level,
            "aggregation_policy": self.aggregation_policy,
            "filters_json": canonical_json(self.filters),
            "direction": self.direction,
            "standardization": self.standardization,
            "created_at": created_at,
        }
