"""Optional bridge to the ``nirs4all-datasets`` catalog.

The Arena anticipates ``nirs4all-datasets`` v1 with a mock ``DatasetCard`` and
plugs in the real catalog when present (DESIGN.md §5.1). Loading is **best-effort**:
if the catalog (a sibling checkout or installed package) is unavailable, callers
fall back to :func:`mock_dataset_card`. The Arena never re-implements dataset IO.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from nirs4all_benchmarks.datasets.dataset_card import DatasetCard
from nirs4all_benchmarks.identity import fingerprint


class CatalogUnavailable(RuntimeError):
    """Raised when a requested dataset card cannot be found in any catalog."""


def _candidate_catalog_dirs(catalog_root: str | Path | None) -> list[Path]:
    if catalog_root is not None:
        return [Path(catalog_root)]
    here = Path(__file__).resolve()
    candidates: list[Path] = []
    # Sibling checkout: <ecosystem>/nirs4all-datasets/catalog/datasets
    for parent in here.parents:
        sibling = parent / "nirs4all-datasets" / "catalog" / "datasets"
        if sibling.is_dir():
            candidates.append(sibling)
    # Installed package data, if present.
    try:
        import importlib.util

        spec = importlib.util.find_spec("nirs4all_datasets")
        if spec and spec.origin:
            pkg_catalog = Path(spec.origin).parent / "catalog" / "datasets"
            if pkg_catalog.is_dir():
                candidates.append(pkg_catalog)
    except (ImportError, ValueError):
        pass
    return candidates


def load_catalog_card(dataset_id: str, catalog_root: str | Path | None = None) -> DatasetCard:
    """Load a ``DatasetCard`` for ``dataset_id`` from a ``nirs4all-datasets`` catalog.

    Reads the per-dataset YAML (``catalog/datasets/<id>.yaml``). Requires PyYAML;
    raises :class:`CatalogUnavailable` if neither the catalog nor PyYAML is present.
    """
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise CatalogUnavailable("PyYAML is required to read the dataset catalog") from exc

    for catalog_dir in _candidate_catalog_dirs(catalog_root):
        path = catalog_dir / f"{dataset_id}.yaml"
        if path.is_file():
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            return _card_from_catalog_yaml(raw)
    raise CatalogUnavailable(f"dataset '{dataset_id}' not found in any nirs4all-datasets catalog")


def _card_from_catalog_yaml(raw: dict[str, Any]) -> DatasetCard:
    sources = raw.get("sources", []) or []
    first = sources[0] if sources else {}
    targets = [v for v in raw.get("variables", []) or [] if v.get("role") == "target"]
    n_features = sum(int(s.get("n_variables", 0) or 0) for s in sources) or first.get("n_variables")
    n_samples = max((int(s.get("n_observations", 0) or 0) for s in sources), default=None) or None
    governance = raw.get("governance", {}) or {}
    return DatasetCard(
        dataset_id=raw.get("id", "unknown"),
        dataset_version=(raw.get("versions", {}) or {}).get("content"),
        name=raw.get("name"),
        description=raw.get("description"),
        domain=raw.get("domain"),
        modality=first.get("modality"),
        signal_type=first.get("signal_type"),
        visibility="public" if raw.get("tier") == "public" else "restricted",
        axis_unit=first.get("axis_unit"),
        axis_min=first.get("axis_min"),
        axis_max=first.get("axis_max"),
        axis_resolution=first.get("axis_resolution"),
        n_samples=n_samples,
        n_features=n_features,
        license=governance.get("license"),
        keywords=raw.get("keywords", []) or [],
        sources=sources,
        variables=raw.get("variables", []) or [],
        grouping_metadata={"alignment_level": raw.get("alignment_level"), "ids": raw.get("ids", {})},
        identity_stats={"n_targets": len(targets), "splits": raw.get("splits", [])},
    )


def mock_dataset_card(
    dataset_id: str,
    *,
    n_samples: int = 100,
    n_features: int = 700,
    axis_unit: str = "nm",
    axis_range: tuple[float, float] = (1100.0, 2500.0),
    domain: str = "mock",
    task_type: str = "regression",
) -> DatasetCard:
    """A deterministic mock card for fixtures and offline development."""
    return DatasetCard(
        dataset_id=dataset_id,
        dataset_version="mock-1",
        source_registry="mock",
        name=dataset_id.replace("_", " ").title(),
        domain=domain,
        modality="NIR",
        signal_type="absorbance",
        visibility="public",
        axis_unit=axis_unit,
        axis_min=axis_range[0],
        axis_max=axis_range[1],
        axis_resolution=(axis_range[1] - axis_range[0]) / max(n_features - 1, 1),
        n_samples=n_samples,
        n_features=n_features,
        license="CC-BY-4.0",
        identity_stats={"task_type": task_type},
    )


def card_to_dataset_block(
    card: DatasetCard,
    *,
    task_type: str = "regression",
    visibility: str | None = None,
) -> dict[str, Any]:
    """Project a ``DatasetCard`` into an export ``dataset`` block (for fixtures/uploads).

    ``dataset_fingerprint`` is derived deterministically from the card identity so a
    given card always maps to the same dataset identity in the store.
    """
    card_dict = card.model_dump(mode="json", exclude_none=True)
    card_dict.setdefault("task_type", task_type)
    return {
        "dataset_fingerprint": fingerprint({"dataset_card": card.dataset_id, "v": card.dataset_version}),
        "dataset_card": {
            "name": card.name or card.dataset_id,
            "modality": card.modality,
            "signal_type": card.signal_type,
            "domain": card.domain,
            "task_type": task_type,
            "license": card.license,
            "axis": {
                "unit": card.axis_unit,
                "n": card.n_features,
                "range": [card.axis_min, card.axis_max],
                "resolution": card.axis_resolution,
            },
            "sources": card.sources,
        },
        "visibility": visibility or card.visibility,
        "n_samples": card.n_samples,
        "n_features": card.n_features,
    }
