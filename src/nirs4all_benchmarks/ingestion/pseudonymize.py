"""PSEUDONYMIZE — sample/group ids are pseudonymized at ingest (DATA_MANAGEMENT.md §1.8).

The engine *sanitizes* ids (ASCII-safe, collision-free) but does **not** pseudonymize
them (PERSISTENCE_FORMATS.md §4.4). The Arena therefore maps every ``sample_id`` /
``group_id`` to a salted digest at ingest. The salt is **store-global and persistent**
(stored in ``arena_meta``) so the *same* raw id maps to the *same* pseudo id across
every run of a dataset — which is exactly what makes cross-run residual comparison on
the same samples possible (DATA_MANAGEMENT.md §6).
"""

from __future__ import annotations

import hashlib
import os

from nirs4all_benchmarks.store import ArenaStore

_SALT_META_KEY = "pseudonymization_salt"


class Pseudonymizer:
    """Deterministic, salted, one-way mapping of raw ids to pseudo ids."""

    def __init__(self, salt: str, *, length: int = 16) -> None:
        self._salt = salt.encode("utf-8")
        self._length = length

    @classmethod
    def for_store(cls, store: ArenaStore) -> Pseudonymizer:
        """Get (or lazily create + persist) the store-global pseudonymization salt."""
        salt = store.get_meta(_SALT_META_KEY)
        if salt is None:
            salt = os.urandom(16).hex()
            store.set_meta(_SALT_META_KEY, salt)
        return cls(salt)

    def map(self, raw: str | None) -> str | None:
        if raw is None:
            return None
        digest = hashlib.sha256(self._salt + str(raw).encode("utf-8")).hexdigest()
        return f"s_{digest[: self._length]}"
