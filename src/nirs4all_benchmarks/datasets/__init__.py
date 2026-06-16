"""Dataset identity — ``DatasetCard`` (known) and ``DatasetFingerprint`` (anonymized).

``DatasetCard`` describes a known dataset (typically from ``nirs4all-datasets``);
``DatasetFingerprint`` is the always-present anonymized data-regime identity used
for user runs and non-shareable datasets (DESIGN.md §6.2/§6.3). The dataset
identity the Arena keys on is the ``dataset_fingerprint`` (io ``schema_fingerprint``
when available); the card is optional enrichment linked to it.
"""

from __future__ import annotations

from nirs4all_benchmarks.datasets.catalog import (
    CatalogUnavailable,
    card_to_dataset_block,
    load_catalog_card,
    mock_dataset_card,
)
from nirs4all_benchmarks.datasets.dataset_card import (
    DatasetCard,
    build_dataset_card_row,
    build_dataset_fingerprint_row,
)

__all__ = [
    "CatalogUnavailable",
    "DatasetCard",
    "build_dataset_card_row",
    "build_dataset_fingerprint_row",
    "card_to_dataset_block",
    "load_catalog_card",
    "mock_dataset_card",
]
