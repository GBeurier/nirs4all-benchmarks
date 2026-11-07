"""Owned Arena schema 3 stores with SQLite recovery. No legacy opening or migration."""

from __future__ import annotations

import datetime as _dt
import errno
import hashlib
import os
import sqlite3
import stat
import tempfile
import threading
import time
import weakref
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from importlib import resources
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.store.residual_store import ResidualStore

ARENA_APPLICATION_ID = 0x4E344132
_SCHEMA_SQL = resources.files("nirs4all_benchmarks.store").joinpath("schema.sql").read_text(encoding="utf-8")
SCHEMA_FINGERPRINT = hashlib.sha256(_SCHEMA_SQL.encode()).hexdigest()
_STORE_LOCK = threading.RLock()
# Only managed live SQLite connections are counted; never cache a closed inode.
_CONNECTIONS: dict[tuple[int, int], int] = {}
_DEFERRED_RAW: dict[tuple[int, int], list[int]] = {}
_BUSY_TIMEOUT_MS = 30_000


class ArenaStoreRefusal(ValueError):
    """A fixed path-free refusal; claimed ownership precedes SQLite recovery."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def utc_now() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _close_raw(fd: int) -> None:
    # A regular-file swap during os.open can yield a different, already-live
    # inode. Never close that descriptor until SQLite releases its last lease.
    info = os.fstat(fd)
    key = info.st_dev, info.st_ino
    if _CONNECTIONS.get(key, 0):
        _DEFERRED_RAW.setdefault(key, []).append(fd)
    else:
        os.close(fd)


def _open_regular_nonblocking(path: Path) -> int:
    """Read a fixed header safely; caller holds the process registry lock."""
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ArenaStoreRefusal("arena_store_unsafe_file")
    if _CONNECTIONS.get((before.st_dev, before.st_ino), 0):
        raise ArenaStoreRefusal("arena_store_unsafe_file")
    flags = (os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
             | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    try:
        fd = os.open(path, flags | getattr(os, "O_NOATIME", 0))
    except OSError as exc:
        if exc.errno != errno.EPERM:
            raise
        fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ArenaStoreRefusal("arena_store_unsafe_file")
    except BaseException:
        _close_raw(fd)
        raise
    return fd


def _components(root: Path) -> bool:
    if root.is_symlink():
        raise ArenaStoreRefusal("arena_store_unsafe_path")
    if root.exists() and not root.is_dir():
        raise ArenaStoreRefusal("arena_store_unsafe_path")
    for name in ("arena.sqlite", "arrays", "exports", "arena.sqlite-wal", "arena.sqlite-shm", "arena.sqlite-journal"):
        path = root / name
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ArenaStoreRefusal("arena_store_unsafe_path")
        if name in {"arrays", "exports"}:
            if not stat.S_ISDIR(info.st_mode):
                raise ArenaStoreRefusal("arena_store_unsafe_path")
        elif not stat.S_ISREG(info.st_mode):
            raise ArenaStoreRefusal("arena_store_unsafe_file")
    if (root / "arena.sqlite").exists():
        return False
    if root.exists():
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        # Refusing a foreign directory must not itself advance its access time
        # when the platform permits a no-atime read (ctime is never changed).
        try:
            fd = os.open(root, flags | getattr(os, "O_NOATIME", 0))
        except OSError as exc:
            if exc.errno != errno.EPERM:
                raise
            fd = os.open(root, flags)
        try:
            names = os.listdir(fd)
            if "arena.sqlite" in names:
                # A peer may publish after the earlier exists() check. Treat
                # its regular inode as an existing candidate, never as empty.
                _inode(root / "arena.sqlite")
                return False
            if names:
                raise ArenaStoreRefusal("arena_store_root_not_empty")
        finally:
            os.close(fd)
    return True


def _inode(path: Path) -> tuple[int, int]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ArenaStoreRefusal("arena_store_unsafe_file")
    return info.st_dev, info.st_ino


def _claimed_header(root: Path) -> tuple[int, int]:
    """Check a claimed v2 main header without closing any active SQLite inode."""
    path = root / "arena.sqlite"
    key = _inode(path)
    if _CONNECTIONS.get(key, 0):
        return key
    fd = _open_regular_nonblocking(path)
    try:
        if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != key:
            raise ArenaStoreRefusal("arena_store_unsafe_file")
        header = os.read(fd, 100)
    finally:
        _close_raw(fd)
    if _inode(path) != key:
        raise ArenaStoreRefusal("arena_store_unsafe_file")
    if (
        len(header) != 100
        or header[:16] != b"SQLite format 3\x00"
        or int.from_bytes(header[60:64], "big") != 3
        or int.from_bytes(header[68:72], "big") != ARENA_APPLICATION_ID
    ):
        raise ArenaStoreRefusal("unsupported_arena_store_schema")
    return key


def store_header_status(root: str | Path) -> dict[str, Any]:
    """Identify the claimed header only; use the same live-inode registry as open."""
    with _STORE_LOCK:
        try:
            root = Path(root)
            if _components(root):
                return {"status": "uninitialized", "schema_version": None}
            _claimed_header(root)
            return {"status": "schema_3_header", "schema_version": 3}
        except ArenaStoreRefusal as exc:
            return {"status": "refused", "code": exc.code, "schema_version": None}
        except OSError:
            return {"status": "refused", "code": "arena_store_read_refused", "schema_version": None}


def _sqlite_refusal(exc: sqlite3.DatabaseError) -> ArenaStoreRefusal:
    code = getattr(exc, "sqlite_errorcode", 0) & 0xFF
    if code in {5, 6}:  # SQLite primary result codes SQLITE_BUSY / SQLITE_LOCKED.
        return ArenaStoreRefusal("arena_store_busy")
    return ArenaStoreRefusal("arena_store_corrupt")


def _release(connection: sqlite3.Connection, key: tuple[int, int]) -> None:
    """Close under the registry lock, before permitting any new raw header read."""
    connection.close()
    remaining = _CONNECTIONS[key] - 1
    if remaining:
        _CONNECTIONS[key] = remaining
    else:
        del _CONNECTIONS[key]
        for fd in _DEFERRED_RAW.pop(key, []):
            os.close(fd)


def _close_registered(connection: sqlite3.Connection, key: tuple[int, int]) -> None:
    with _STORE_LOCK:
        _release(connection, key)


def _set_wal(connection: sqlite3.Connection, busy_timeout_ms: int) -> None:
    # SQLite does not invoke its busy handler for every journal-mode lock
    # upgrade. Fresh concurrent openers must wait for the successful peer.
    deadline = time.monotonic() + busy_timeout_ms / 1000
    while True:
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.DatabaseError as exc:
            if _sqlite_refusal(exc).code != "arena_store_busy" or time.monotonic() >= deadline:
                raise
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))


def _connect_owned(root: Path, busy_timeout_ms: int) -> tuple[sqlite3.Connection, tuple[int, int]]:
    # Caller holds _STORE_LOCK across header read, connect and registration.
    key = _claimed_header(root)
    connection = None
    registered = False
    try:
        _components(root)
        if _inode(root / "arena.sqlite") != key:
            raise ArenaStoreRefusal("arena_store_unsafe_file")
        connection = sqlite3.connect(
            (root / "arena.sqlite").as_uri() + "?mode=rw", uri=True, timeout=busy_timeout_ms / 1000,
            # GC may release the last store reference on another thread. Close
            # is serialized by the registry lock; this grants no shared SQL API.
            check_same_thread=False,
        )
        _CONNECTIONS[key] = _CONNECTIONS.get(key, 0) + 1
        registered = True
        connection.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        if _inode(root / "arena.sqlite") != key:
            raise ArenaStoreRefusal("arena_store_unsafe_file")
        # A claimed owned header permits SQLite recovery, including main-page
        # rewrites and sidecars. No application writes precede this schema check.
        values = dict(connection.execute("SELECT key,value FROM arena_meta"))
        if values.get("schema_fingerprint") != SCHEMA_FINGERPRINT:
            raise ArenaStoreRefusal("arena_store_schema_fingerprint_mismatch")
        if values.get("arena_schema_version") != "3" or len(values.get("pseudonymization_salt", "")) != 32:
            raise ArenaStoreRefusal("arena_store_metadata_invalid")
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        _set_wal(connection, busy_timeout_ms)
        return connection, key
    except BaseException as exc:
        if connection is not None:
            if registered:
                _release(connection, key)
            else:
                connection.close()
        if isinstance(exc, sqlite3.DatabaseError):
            raise _sqlite_refusal(exc) from exc
        if isinstance(exc, OSError):
            raise ArenaStoreRefusal("arena_store_read_refused") from exc
        raise


def _provision(root: Path) -> None:
    root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".arena-provision-", dir=root.parent) as directory:
        source = Path(directory) / "arena.sqlite"
        connection = sqlite3.connect(source)
        try:
            connection.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA_SQL)
            connection.execute(f"PRAGMA application_id={ARENA_APPLICATION_ID}")
            connection.execute("PRAGMA user_version=3")
            connection.executemany("INSERT INTO arena_meta(key,value) VALUES (?,?)", (
                ("arena_schema_version", "3"), ("schema_fingerprint", SCHEMA_FINGERPRINT),
                ("pseudonymization_salt", os.urandom(16).hex()),
            ))
            connection.commit()
        finally:
            connection.close()
        with source.open("rb") as stream:
            os.fsync(stream.fileno())
        root.mkdir(exist_ok=True)
        # A peer may have published since the initial empty-root check. Validate
        # the components, then let no-clobber publication keep that candidate;
        # _connect_owned independently checks its header and schema fingerprint.
        _components(root)
        with suppress(FileExistsError):
            os.link(source, root / "arena.sqlite")
        # The winner must independently pass all identification checks.
        fd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class ArenaStore:
    """Owned v3 stores; unclaimed headers refuse before SQLite is opened."""

    def __init__(self, root: str | Path, *, create: bool = True, busy_timeout_ms: int = _BUSY_TIMEOUT_MS) -> None:
        if type(create) is not bool or type(busy_timeout_ms) is not int or not 0 <= busy_timeout_ms <= 2147483647:
            raise ValueError("invalid arena store options")
        with _STORE_LOCK:
            self._open(root, create, busy_timeout_ms)

    def _open(self, root: str | Path, create: bool, busy_timeout_ms: int) -> None:
        self.root = Path(root).absolute()
        self._closed = True
        try:
            if not create and any(not (self.root / name).exists() for name in ("arena.sqlite", "arrays", "exports")):
                raise ArenaStoreRefusal("arena_store_uninitialized")
            if _components(self.root):
                if not create:
                    raise ArenaStoreRefusal("arena_store_uninitialized")
                _provision(self.root)
            self.conn, self._inode_key = _connect_owned(self.root, busy_timeout_ms)
        except OSError as exc:
            raise ArenaStoreRefusal("arena_store_read_refused") from exc
        self._closed = False
        self._finalizer = weakref.finalize(self, _close_registered, self.conn, self._inode_key)
        self.db_path = self.root / "arena.sqlite"
        self.arrays_dir = self.root / "arrays"
        self.exports_dir = self.root / "exports"
        try:
            for directory in (self.arrays_dir, self.exports_dir):
                if create:
                    directory.mkdir(exist_ok=True)
            self.residuals = ResidualStore(self.arrays_dir)
            self._atomic_pack = False
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        with _STORE_LOCK:
            if not self._closed:
                self._finalizer()
                self._closed = True

    def __enter__(self) -> ArenaStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A single atomic transaction; rolls back on any exception."""
        with _STORE_LOCK:
            try:
                with self._transaction() as connection:
                    yield connection
            except sqlite3.DatabaseError as exc:
                # Preserve the ingestion layer's metadata/indexing refusals.
                # Map storage errors only after transaction/blob cleanup, for
                # BEGIN, body and COMMIT, including nested atomic-pack writes.
                if isinstance(exc, (sqlite3.ProgrammingError, sqlite3.IntegrityError)):
                    raise
                raise _sqlite_refusal(exc) from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        if self._atomic_pack:
            yield self.conn
            return
        try:
            if not self.conn.in_transaction:
                self.conn.execute("BEGIN IMMEDIATE")
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
