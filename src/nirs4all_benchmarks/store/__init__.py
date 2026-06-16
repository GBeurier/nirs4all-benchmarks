"""The Arena store — ``arena.sqlite`` (dimensions + facts) + ``arrays/*.parquet``.

``DESIGN.md`` §7 verbatim: SQLite for dimensions and facts, Parquet for residual
arrays, ``exports/`` for ingested bundles. **No ``artifacts/``.** Every identity is
a content hash, so dedup tables collapse by key on insert (``INSERT OR IGNORE``).
"""

from __future__ import annotations

from nirs4all_benchmarks.store.arena_store import ArenaStore, ArenaStoreVersionError
from nirs4all_benchmarks.store.queries import Queries
from nirs4all_benchmarks.store.residual_store import RESIDUALS_ARROW_SCHEMA, ResidualStore

__all__ = [
    "RESIDUALS_ARROW_SCHEMA",
    "ArenaStore",
    "ArenaStoreVersionError",
    "Queries",
    "ResidualStore",
]
