"""Workspace fold sentinels mirrored locally without importing the producer."""

# Owner references (as identified by the Opus owner-source review):
# nirs4all/nirs4all/data/reduction.py:is_real_cv_fold
# nirs4all/nirs4all/pipeline/storage/target_domain_summary.py:115
# Mirror strip/empty/sentinel handling locally; intentionally retain arbitrary
# genuine string IDs for the Arena workspace contract rather than integers only.
PSEUDO_FOLD_IDS = frozenset({
    "final", "final_agg", "avg", "w_avg", "ensemble", "all",
    "macro", "fold_avg", "fold_w_avg",
})


def is_canonical_fold_id(fold_id: str) -> bool:
    """A wire fold id is never silently stripped or aliased to another fold."""
    return bool(fold_id) and fold_id == fold_id.strip()


def is_pseudo_fold(fold_id: str | int | None) -> bool:
    """Mirror reduction/target-domain aggregate IDs; retain arbitrary real IDs."""
    if fold_id is None:
        return False  # Absent fold evidence is handled separately from a named sentinel.
    key = str(fold_id).strip()
    return not key or key in PSEUDO_FOLD_IDS or key.endswith("_agg")
