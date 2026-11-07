"""The Arena store — ``arena.sqlite`` (dimensions + facts) + ``arrays/*.parquet``.

``DESIGN.md`` §7 verbatim: SQLite for dimensions and facts, Parquet for residual
arrays and transactional, allowlisted metadata receipts. Raw export archives are
not stored; legacy files under ``exports/`` are ignored/refused. **No ``artifacts/``.** Every identity is
a content hash, so dedup tables collapse by key on insert (``INSERT OR IGNORE``).
"""

from __future__ import annotations

from nirs4all_benchmarks.store.arena_store import ArenaStore, ArenaStoreRefusal
from nirs4all_benchmarks.store.queries import Queries
from nirs4all_benchmarks.store.residual_store import RESIDUALS_ARROW_SCHEMA, ResidualStore

__all__ = [
    "RESIDUALS_ARROW_SCHEMA",
    "ArenaStore",
    "ArenaStoreRefusal",
    "Queries",
    "ResidualStore",
]
