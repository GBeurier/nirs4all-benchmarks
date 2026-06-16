"""``ArenaStore`` — the SQLite metadata store with dedup-by-hash upserts.

A thin, explicit wrapper over ``sqlite3``: it creates/migrates the schema, stamps
``PRAGMA user_version`` with :data:`ARENA_SCHEMA_VERSION`, guards against opening a
store written by a newer library (the nirs4all forward-incompatibility pattern),
and offers content-hash upserts (``INSERT OR IGNORE`` → trivially correct dedup).

Identity is content-addressed, so writes are *idempotent at the row level*: writing
the same dimension twice is a no-op. The ingestion pipeline wraps a whole export in
a single transaction on top of these primitives.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import ARENA_SCHEMA_VERSION
from nirs4all_benchmarks.store.residual_store import ResidualStore


class ArenaStoreVersionError(RuntimeError):
    """Raised when opening a store stamped with a newer schema than this library."""


def utc_now() -> str:
    """ISO-8601 UTC timestamp with a ``Z`` suffix (stable, sortable)."""
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_SCHEMA_SQL = resources.files("nirs4all_benchmarks.store").joinpath("schema.sql").read_text(encoding="utf-8")


class ArenaStore:
    """Open (or create) an Arena store rooted at ``root``.

    Layout::

        root/
          arena.sqlite
          arrays/      # sample-keyed residual parquet (ResidualStore)
          exports/     # ingested ArenaRunExport bundles (audit/replay)
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.arrays_dir = self.root / "arrays"
        self.exports_dir = self.root / "exports"
        self.arrays_dir.mkdir(exist_ok=True)
        self.exports_dir.mkdir(exist_ok=True)
        self.db_path = self.root / "arena.sqlite"
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.residuals = ResidualStore(self.arrays_dir)
        self._init_schema()

    # ── lifecycle ──────────────────────────────────────────────────────
    def _init_schema(self) -> None:
        current = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if current == 0:
            self.conn.executescript(_SCHEMA_SQL)
            self.conn.execute(f"PRAGMA user_version = {ARENA_SCHEMA_VERSION}")
            self.set_meta("arena_schema_version", str(ARENA_SCHEMA_VERSION))
            self.conn.commit()
        elif current > ARENA_SCHEMA_VERSION:
            raise ArenaStoreVersionError(
                f"arena.sqlite is schema v{current} but this library only supports "
                f"v{ARENA_SCHEMA_VERSION}; upgrade nirs4all-benchmarks."
            )
        else:
            # Same version: ensure all CREATE IF NOT EXISTS objects are present
            # (idempotent — covers a partially-initialized DB).
            self.conn.executescript(_SCHEMA_SQL)
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> ArenaStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A single atomic transaction; rolls back on any exception."""
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # ── meta ───────────────────────────────────────────────────────────
    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO arena_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM arena_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    # ── generic writes ────────────────────────────────────────────────
    def upsert(self, table: str, row: Mapping[str, Any]) -> bool:
        """Insert ``row`` into ``table``; ignore if the primary key already exists.

        Returns ``True`` when a new row was inserted, ``False`` when it was a dedup
        hit. Because every dimension's PK is a content hash, ``INSERT OR IGNORE`` is
        exactly the dedup semantics DESIGN.md §7.4 prescribes.
        """
        cols = list(row.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_sql = ", ".join(cols)
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO {table} ({col_sql}) VALUES ({placeholders})",
            tuple(row[c] for c in cols),
        )
        return cur.rowcount > 0

    def insert(self, table: str, row: Mapping[str, Any]) -> int:
        """Insert a fact row that always appends (e.g. ``metric_observations``)."""
        cols = list(row.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_sql = ", ".join(cols)
        cur = self.conn.execute(
            f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})",
            tuple(row[c] for c in cols),
        )
        return int(cur.lastrowid or 0)

    def update(self, table: str, pk_col: str, pk_val: Any, changes: Mapping[str, Any]) -> None:
        sets = ", ".join(f"{c} = ?" for c in changes)
        self.conn.execute(
            f"UPDATE {table} SET {sets} WHERE {pk_col} = ?",
            (*changes.values(), pk_val),
        )

    # ── generic reads ──────────────────────────────────────────────────
    def exists(self, table: str, pk_col: str, pk_val: Any) -> bool:
        row = self.conn.execute(f"SELECT 1 FROM {table} WHERE {pk_col} = ? LIMIT 1", (pk_val,)).fetchone()
        return row is not None

    def get(self, table: str, pk_col: str, pk_val: Any) -> dict[str, Any] | None:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE {pk_col} = ?", (pk_val,)).fetchone()
        return dict(row) if row else None

    def query(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> list[dict[str, Any]]:
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def query_one(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> dict[str, Any] | None:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def count(self, table: str, where: str = "", params: Sequence[Any] = ()) -> int:
        clause = f" WHERE {where}" if where else ""
        return int(self.conn.execute(f"SELECT COUNT(*) FROM {table}{clause}", params).fetchone()[0])

    # ── collections ────────────────────────────────────────────────────
    def ensure_collection(
        self,
        collection_id: str,
        *,
        kind: str = "user_run_collection",
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        self.upsert(
            "collections",
            {
                "collection_id": collection_id,
                "kind": kind,
                "name": name or collection_id,
                "description": description,
                "created_at": utc_now(),
            },
        )
