"""V2-only pack intake; the pinned frozen converter is a separate explicit operation."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.frozen_history import (
    RECORDS_SHA256,
    SOURCE_ARCHIVE_SHA256,
    SOURCE_MANIFEST_SHA256,
    _read,
    strict_gzip,
    validate_pack_manifest,
)
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_exports_atomic, validate_metadata
from nirs4all_benchmarks.store import ArenaStore


def load_history_exports(directory: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = Path(directory)
    for name in ("manifest.json", "runs.jsonl.gz"):
        if (root / name).is_symlink() or not (root / name).is_file():
            raise ValueError("history_input_unsafe")
    metadata = strict_loads(_read(root / "manifest.json"))
    if type(metadata.get("schema_version")) is not int or metadata["schema_version"] != 2:
        raise ValueError("unsupported_export_version")
    validate_pack_manifest(metadata)
    if (
        metadata.get("kind") != "arena.frozen-history/2"
        or metadata.get("source", {}).get("archive_sha256") != SOURCE_ARCHIVE_SHA256
        or metadata.get("source", {}).get("manifest_sha256") != SOURCE_MANIFEST_SHA256
    ):
        raise ValueError("unsupported_history_pack")
    archive = _read(root / "runs.jsonl.gz")
    if hashlib.sha256(archive).hexdigest() != metadata["output"]["archive_sha256"]:
        raise ValueError("history_archive_hash_mismatch")
    records = strict_gzip(archive)
    digest = hashlib.sha256(records).hexdigest()
    if digest != RECORDS_SHA256 or digest != metadata["output"]["records_sha256"]:
        raise ValueError("history_records_hash_mismatch")
    exports = [strict_loads(line) for line in records.split(b"\n")[:-1]]
    if (
        len(exports) != metadata["output"]["executions"]
        or sum(len(e["scores"]["observations"]) for e in exports) != metadata["output"]["metric_observations"]
    ):
        raise ValueError("history_count_mismatch")
    for export in exports:
        validation = validate_metadata(export)
        if validation.status != "metadata_valid":
            raise ValueError(validation.issues[0]["code"])
    return metadata, exports


def ingest_history(
    store: ArenaStore,
    directory: str | Path,
    *,
    progress: Callable[[int, int], None] | None = None,
    _loaded: tuple[dict[str, Any], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    metadata, exports = _loaded or load_history_exports(directory)
    policy = IngestionPolicy(collection_id=metadata["collection_id"], collection_kind="user_run_collection")
    results = ingest_exports_atomic(store, exports, policy=policy, collection_metadata=metadata, progress=progress)
    return {
        "collection_id": policy.collection_id,
        "executions": len(exports),
        "statuses": dict(Counter(result.status for result in results)),
    }
