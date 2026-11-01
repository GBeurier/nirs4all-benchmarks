"""Load checked-in, weights-free historical exports through the normal ingestion gate."""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore


def ingest_history(
    store: ArenaStore,
    directory: str | Path,
    *,
    progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Verify a historical data pack, then ingest its exports idempotently.

    These are producer-reported observations in a user collection. Import does
    not promote them to a newly qualified benchmark release or recompute scores.
    """
    root = Path(directory)
    metadata = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    archive = root / "runs.jsonl.gz"
    if hashlib.sha256(archive.read_bytes()).hexdigest() != metadata["archive_sha256"]:
        raise ValueError("Historical data archive checksum mismatch")
    # Check the entire stream before writing, including truncation/count errors.
    with gzip.open(archive, "rt", encoding="utf-8") as stream:
        exports = [json.loads(line) for line in stream if line.strip()]
    if len(exports) != metadata["executions"]:
        raise ValueError("Historical data archive execution count mismatch")
    collection = metadata["collection_id"]
    policy = IngestionPolicy(collection_id=collection, collection_kind="user_run_collection")
    store.ensure_collection(collection, name=metadata["name"], description=metadata["description"])
    store.update("collections", "collection_id", collection, {"metadata_json": json.dumps(metadata)})
    counts: Counter[str] = Counter()
    for index, export in enumerate(exports, 1):
        result = ingest_export(store, export, policy=policy)
        if not result.ok:
            raise ValueError(f"Historical export {index} rejected: {result.issues}")
        counts[result.status] += 1
        if progress is not None and (index % 500 == 0 or index == len(exports)):
            progress(index, len(exports))
    return {"collection_id": collection, "executions": len(exports), "statuses": dict(counts)}
