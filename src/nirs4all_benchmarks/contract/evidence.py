"""Tri-state v2 declarations; normalization cannot create evidence."""

from __future__ import annotations

from nirs4all_benchmarks.contract.arena_run_export import LeakageAttestation, ScoreObservation
from nirs4all_benchmarks.scoring import METRIC_DIRECTION

METADATA_PROFILE = "arena.metadata/2"


def attestation_values(att: LeakageAttestation) -> dict[str, bool | None]:
    """V2 is tri-state. Neither normalization nor absence can create attestation."""
    return {name: getattr(att, name) for name in ("oof_enforced", "group_leakage_checked", "nested_cv_safe")}


def observation_direction(observation: ScoreObservation) -> tuple[str | None, str]:
    """Use known registry definitions without inventing a custom metric default."""
    known = METRIC_DIRECTION.get(observation.metric_name.lower())
    if (observation.direction is None) != (observation.direction_source == "unreported"):
        raise ValueError("metric_direction_contradiction")
    if observation.direction_source == "registry" and known != observation.direction:
        raise ValueError("metric_direction_mismatch")
    if known is not None and observation.direction is not None and known != observation.direction:
        raise ValueError("metric_direction_mismatch")
    return observation.direction, observation.direction_source
