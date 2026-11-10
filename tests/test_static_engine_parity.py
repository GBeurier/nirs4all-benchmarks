"""Unsupported frozen history cannot qualify static engine publication.

The exact historical aggregation oracle, including its numerical tolerances and
multivalued-parameter checks, is preserved in the static-oracle fixtures. Those
assertions are historical; successful current browser parity is not claimed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nirs4all_benchmarks.history import ingest_history
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore

PACK = Path(__file__).resolve().parents[1] / "data" / "catalog-history-2026-09"


def test_frozen_history_refuses_static_engine_publication(tmp_path: Path) -> None:
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(ValueError, match=r"^unsupported_export_version$"):
            ingest_history(store, PACK)
        assert store.count("executions") == store.count("collections") == 0
        assert store.count("metric_observations") == store.count("metadata_receipts") == 0
    with pytest.raises(ValueError, match=r"^publication_gate_closed$"):
        build_site(tmp_path / "store", tmp_path / "site", domain=None)
    assert not (tmp_path / "site").exists()
