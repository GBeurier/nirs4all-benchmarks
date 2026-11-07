"""``ResidualStore`` — sample-keyed residual arrays in Parquet (Zstd).

The one schema change that matters (DATA_MANAGEMENT.md §3): the row key is the
**stable ``sample_id``**, not a positional index. One Parquet file per
``residual_set`` (named by its content hash) gives free idempotency and dedup —
re-ingesting the same residuals writes byte-identical rows to the same path.

Aggregate (``_agg``) rows are *not* mixed into per-sample residuals; they are
modelled as aggregate-level metric observations elsewhere.
"""

from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from nirs4all_benchmarks.contract.schema import RESIDUALS_PARQUET_COLUMNS

_TYPE_MAP = {
    "utf8": pa.string(),
    "f64": pa.float64(),
    "list<f64>": pa.list_(pa.float64()),
}

RESIDUALS_ARROW_SCHEMA = pa.schema(
    [pa.field(name, _TYPE_MAP[dtype], nullable=nullable) for name, dtype, nullable in RESIDUALS_PARQUET_COLUMNS]
)

_COLUMN_NAMES = [name for name, _, _ in RESIDUALS_PARQUET_COLUMNS]


class ResidualStore:
    """Read/write sample-keyed residual Parquet files under ``arrays/``."""

    def __init__(self, arrays_dir: str | Path) -> None:
        self.arrays_dir = Path(arrays_dir)
        self.arrays_dir.mkdir(parents=True, exist_ok=True)
        self._pending: list[list[tuple[Path, int, int]]] = []
        self._rollback_reads_committed = False

    @contextmanager
    def transaction(self, keep_committed: Callable[[Path], bool] | None = None) -> Iterator[None]:
        """Track only newly published inodes, while the caller holds its SQL lock."""
        owned: list[tuple[Path, int, int]] = []
        if not self._pending:
            self._rollback_reads_committed = False
        self._pending.append(owned)
        try:
            yield
        except BaseException:
            for path, device, inode in reversed(owned):
                if keep_committed is not None and keep_committed(path):
                    continue
                try:
                    current = path.lstat()
                except FileNotFoundError:
                    continue
                if (current.st_dev, current.st_ino) == (device, inode):
                    path.unlink()
            raise
        else:
            if len(self._pending) > 1:
                self._pending[-2].extend(owned)
        finally:
            self._pending.pop()

    def path_for(self, residual_set_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", residual_set_id):
            raise ValueError("unsafe_residual_set_id")
        path = self.arrays_dir / f"residuals_{residual_set_id}.parquet"
        if self.arrays_dir.is_symlink() or path.is_symlink():
            raise ValueError("unsafe_residual_path")
        return path

    def write(self, residual_set_id: str, rows: Sequence[dict[str, Any]]) -> Path:
        """Write residual ``rows`` for one ``residual_set``. Returns the file path.

        Missing columns are filled with ``None``; unknown keys are dropped. A complete
        private staging file is linked without replacing a preexisting blob. The
        caller journals new inodes for rollback; process-crash cleanup is not
        claimed by this filesystem journal.
        """
        columns: dict[str, list[Any]] = {name: [] for name in _COLUMN_NAMES}
        for row in rows:
            for name in _COLUMN_NAMES:
                columns[name].append(row.get(name))
        table = pa.table(columns, schema=RESIDUALS_ARROW_SCHEMA)
        path = self.path_for(residual_set_id)
        fd, name = tempfile.mkstemp(prefix=".residual-stage-", dir=self.arrays_dir)
        tmp = Path(name)
        try:
            with os.fdopen(fd, "wb") as stream:
                pq.write_table(table, stream, compression="zstd", compression_level=3)
                stream.flush()
                os.fsync(stream.fileno())
                info = os.fstat(stream.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError:
                if not path.is_file() or path.is_symlink():
                    raise ValueError("residual_blob_conflict") from None
                existing = self.read_table(residual_set_id)
                if existing is None or not existing.equals(table):
                    raise ValueError("residual_blob_conflict") from None
            else:
                if self._pending:
                    self._pending[-1].append((path, info.st_dev, info.st_ino))
        finally:
            tmp.unlink(missing_ok=True)
        return path

    def read(self, residual_set_id: str) -> list[dict[str, Any]]:
        table = self.read_table(residual_set_id)
        return table.to_pylist() if table is not None else []

    def read_table(self, residual_set_id: str) -> pa.Table | None:
        path = self.path_for(residual_set_id)
        if not path.exists():
            return None
        table = pq.read_table(path)
        if not table.schema.equals(RESIDUALS_ARROW_SCHEMA, check_metadata=False):
            raise ValueError("residual_schema_mismatch")
        return table

    def delete(self, residual_set_id: str) -> None:
        self.path_for(residual_set_id).unlink(missing_ok=True)
