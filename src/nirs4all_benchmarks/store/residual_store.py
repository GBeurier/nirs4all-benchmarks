"""``ResidualStore`` — sample-keyed residual arrays in Parquet (Zstd).

The one schema change that matters (DATA_MANAGEMENT.md §3): the row key is the
**stable ``sample_id``**, not a positional index. One Parquet file per
``residual_set`` (named by its content hash) gives free idempotency and dedup —
re-ingesting the same residuals writes byte-identical rows to the same path.

Aggregate (``_agg``) rows are *not* mixed into per-sample residuals; they are
modelled as aggregate-level metric observations elsewhere.
"""

from __future__ import annotations

from collections.abc import Sequence
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

    def path_for(self, residual_set_id: str) -> Path:
        return self.arrays_dir / f"residuals_{residual_set_id}.parquet"

    def write(self, residual_set_id: str, rows: Sequence[dict[str, Any]]) -> Path:
        """Write residual ``rows`` for one ``residual_set``. Returns the file path.

        Missing columns are filled with ``None``; unknown keys are dropped. The
        write is atomic (temp file + rename) so a crash never leaves a partial file.
        """
        columns: dict[str, list[Any]] = {name: [] for name in _COLUMN_NAMES}
        for row in rows:
            for name in _COLUMN_NAMES:
                columns[name].append(row.get(name))
        table = pa.table(columns, schema=RESIDUALS_ARROW_SCHEMA)
        path = self.path_for(residual_set_id)
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        tmp.replace(path)
        return path

    def read(self, residual_set_id: str) -> list[dict[str, Any]]:
        path = self.path_for(residual_set_id)
        if not path.exists():
            return []
        return pq.read_table(path).to_pylist()

    def read_table(self, residual_set_id: str) -> pa.Table | None:
        path = self.path_for(residual_set_id)
        if not path.exists():
            return None
        return pq.read_table(path)

    def delete(self, residual_set_id: str) -> None:
        self.path_for(residual_set_id).unlink(missing_ok=True)
