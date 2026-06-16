"""Ingestion — the ``ArenaRunExport`` → ``ArenaStore`` state machine (DATA_MANAGEMENT.md §5).

``VERIFY → DEDUP-CHECK → RESOLVE IDS → VALIDATE → PSEUDONYMIZE → STRIP →
STAGE→COMMIT → REPORT``. Idempotent (the ``arena_export_hash`` key), leakage-honest
(quarantine non-attested runs), privacy-first (pseudonymize at ingest), and it
never trusts producer UUIDs (every Arena-owned hash is recomputed).
"""

from __future__ import annotations

from nirs4all_benchmarks.ingestion.ingest import (
    IngestionPolicy,
    IngestionResult,
    ingest_export,
)
from nirs4all_benchmarks.ingestion.resolve import ResolvedExport, resolve_identities
from nirs4all_benchmarks.ingestion.upload import UploadResult, register_pipeline, upload
from nirs4all_benchmarks.ingestion.validate import ValidationOutcome, validate_export

__all__ = [
    "IngestionPolicy",
    "IngestionResult",
    "ResolvedExport",
    "UploadResult",
    "ValidationOutcome",
    "ingest_export",
    "register_pipeline",
    "resolve_identities",
    "upload",
    "validate_export",
]
