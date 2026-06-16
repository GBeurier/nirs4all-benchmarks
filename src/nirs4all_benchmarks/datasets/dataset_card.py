"""``DatasetCard`` model + store-row builders from an export's ``dataset`` block."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from nirs4all_benchmarks.identity import canonical_json, fingerprint


class DatasetCard(BaseModel):
    """Identity card of a known dataset (DESIGN.md §6.2 + nirs4all-datasets schema)."""

    model_config = ConfigDict(extra="allow")

    dataset_id: str
    dataset_version: str | None = None
    source_registry: str | None = "nirs4all-datasets"
    name: str | None = None
    description: str | None = None
    domain: str | None = None
    modality: str | None = None
    signal_type: str | None = None
    visibility: str = "public"
    axis_unit: str | None = None
    axis_min: float | None = None
    axis_max: float | None = None
    axis_resolution: float | None = None
    n_samples: int | None = None
    n_features: int | None = None
    license: str | None = None
    keywords: list[str] = Field(default_factory=list)
    sources: list[dict[str, Any]] = Field(default_factory=list)
    variables: list[dict[str, Any]] = Field(default_factory=list)
    access_policy: dict[str, Any] = Field(default_factory=dict)
    identity_stats: dict[str, Any] = Field(default_factory=dict)
    nirs_stats: dict[str, Any] = Field(default_factory=dict)
    grouping_metadata: dict[str, Any] = Field(default_factory=dict)
    citation_id: str | None = None

    @property
    def dataset_card_hash(self) -> str:
        return fingerprint(self.model_dump(mode="json", exclude_none=True))


def _f(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def build_dataset_fingerprint_row(dataset: dict[str, Any], created_at: str, card_hash: str | None) -> dict[str, Any]:
    """Build a ``dataset_fingerprints`` row from an export ``dataset`` block."""
    return {
        "dataset_fingerprint": dataset["dataset_fingerprint"],
        "privacy_level": dataset.get("visibility", "public"),
        "n_samples": dataset.get("n_samples"),
        "n_features": dataset.get("n_features"),
        "task_type": (dataset.get("dataset_card") or {}).get("task_type"),
        "schema_fingerprint": dataset.get("schema_fingerprint"),
        "relation_fingerprint": dataset.get("relation_fingerprint"),
        "plan_fingerprint": dataset.get("plan_fingerprint"),
        "x_stats_json": canonical_json(dataset.get("x_stats", {})),
        "y_stats_json": canonical_json(dataset.get("y_stats", {})),
        "group_stats_json": canonical_json(dataset.get("group_stats", {})),
        "dataset_card_hash": card_hash,
        "producer_version": (dataset.get("producer") or {}).get("io_version"),
        "created_at": created_at,
    }


def build_dataset_card_row(dataset: dict[str, Any], created_at: str) -> dict[str, Any] | None:
    """Build a ``dataset_cards`` row from an export ``dataset`` block, or ``None``.

    Returns ``None`` when the export carries no rich card (only a fingerprint),
    which is the normal case for anonymized user runs.
    """
    card = dataset.get("dataset_card") or {}
    if not card:
        return None
    axis = card.get("axis") or {}
    axis_range = axis.get("range") or [None, None]
    card_hash = fingerprint(card)
    return {
        "dataset_card_hash": card_hash,
        "dataset_id": card.get("name") or card.get("dataset_id"),
        "dataset_version": card.get("version") or card.get("dataset_version"),
        "source_registry": card.get("source_registry", "nirs4all-datasets"),
        "dataset_fingerprint": dataset.get("dataset_fingerprint"),
        "visibility": dataset.get("visibility", "public"),
        "name": card.get("name"),
        "domain": card.get("domain"),
        "modality": card.get("modality"),
        "signal_type": card.get("signal_type"),
        "axis_unit": axis.get("unit"),
        "axis_min": _f(axis_range[0] if len(axis_range) > 0 else None),
        "axis_max": _f(axis_range[1] if len(axis_range) > 1 else None),
        "axis_resolution": _f(axis.get("resolution")),
        "n_samples": dataset.get("n_samples"),
        "n_features": dataset.get("n_features") or axis.get("n"),
        "license": card.get("license"),
        "content_hash": card.get("content_hash"),
        "descriptor_hash": card.get("descriptor_hash"),
        "access_policy_json": canonical_json(card.get("access_policy", {})),
        "identity_stats_json": canonical_json(card.get("identity_stats", {})),
        "nirs_stats_json": canonical_json(card.get("nirs_stats", {})),
        "grouping_metadata_json": canonical_json(card.get("grouping_metadata", {})),
        "citation_id": card.get("citation_id"),
        "card_json": canonical_json(card),
        "created_at": created_at,
    }
