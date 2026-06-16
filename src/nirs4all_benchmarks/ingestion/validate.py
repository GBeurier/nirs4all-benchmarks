"""VALIDATE — minimal correctness gate + leakage honesty (DESIGN.md §8.4)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from nirs4all_benchmarks.contract import ArenaRunExport

if TYPE_CHECKING:
    from nirs4all_benchmarks.ingestion.resolve import ResolvedExport


@dataclass
class ValidationOutcome:
    """The verdict of validation: issues + the resulting validity + residual keying."""

    issues: list[dict[str, str]] = field(default_factory=list)
    validity_status: str = "valid"  # valid | quarantined | rejected
    residual_key: str = "sample_id"

    def add(self, level: str, code: str, message: str) -> None:
        self.issues.append({"level": level, "code": code, "message": message})

    @property
    def rejected(self) -> bool:
        return self.validity_status == "rejected"


def validate_export(
    export: ArenaRunExport,
    resolved: ResolvedExport,
    *,
    quarantine_on_leakage: bool,
) -> ValidationOutcome:
    """Validate a resolved export; decide quarantine/degraded keying."""
    out = ValidationOutcome()

    # Dataset identity present (schema already guarantees the fingerprint string).
    if not export.dataset.dataset_fingerprint:
        out.add("error", "no_dataset_identity", "dataset_fingerprint missing")
        out.validity_status = "rejected"

    # Single-target task is the v1 indexing unit (DESIGN.md §2.5).
    if export.task.task_type not in {"regression", "binary", "multiclass", "multilabel"}:
        out.add("error", "bad_task_type", f"unknown task_type '{export.task.task_type}'")
        out.validity_status = "rejected"

    # Split / CV consistency with sample count, when known.
    n = export.dataset.n_samples
    if n is not None and export.cv.n_folds is not None and export.cv.n_folds > n:
        out.add("error", "cv_inconsistent", f"n_folds={export.cv.n_folds} > n_samples={n}")
        out.validity_status = "rejected"

    # Score version must be present (it is, by default) and there must be scores.
    if not export.scores.observations:
        out.add("warning", "no_scores", "export carries no score observations")

    # Leakage honesty (DATA_MANAGEMENT.md §1.7 / §5).
    att = export.leakage_attestation
    if not att.oof_enforced:
        if quarantine_on_leakage:
            out.add(
                "error",
                "leakage_unattested",
                "oof_enforced is false → quarantined (excluded from published views)",
            )
            if not out.rejected:
                out.validity_status = "quarantined"
        else:
            out.add("warning", "leakage_unattested", "oof_enforced is false → ingested and flagged")
    if att.unsafe_flags:
        out.add("warning", "unsafe_flags", f"engine reported unsafe flags: {sorted(att.unsafe_flags)}")

    # Residual keying: sample-keyed enables cross-run comparison; positional is degraded.
    if export.residuals.key == "positional":
        out.residual_key = "positional"
        out.add(
            "warning",
            "degraded_residual_keying",
            "residuals are positional, not sample-keyed → excluded from cross-run sample comparison",
        )

    return out


def summarize_issues(issues: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for issue in issues:
        counts[issue["level"]] = counts.get(issue["level"], 0) + 1
    return counts
