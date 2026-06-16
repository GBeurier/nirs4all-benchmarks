"""The identity spine — content-addressed hashes that key every Arena dimension.

Meta-analysis is only as good as its join keys (DATA_MANAGEMENT.md §2). Every
dimension is keyed by a hash the Arena can **recompute** from canonical JSON, or
that the engine guarantees stable (graph/fold/schema fingerprints). Producer
UUIDs are recorded but **never** used as join keys.
"""

from __future__ import annotations

from nirs4all_benchmarks.identity.hashing import (
    HASH_ALGO,
    HEX_LEN,
    canonical_json,
    compose_run_condition_hash,
    fingerprint,
    is_hash,
    sha256_hex,
    short,
)
from nirs4all_benchmarks.identity.pipeline_dag import (
    PipelineDagIdentity,
    compute_pipeline_dag_hash,
    nirs4all_identity_hash,
    normalize_graph_spec,
)

__all__ = [
    "HASH_ALGO",
    "HEX_LEN",
    "PipelineDagIdentity",
    "canonical_json",
    "compose_run_condition_hash",
    "compute_pipeline_dag_hash",
    "fingerprint",
    "is_hash",
    "nirs4all_identity_hash",
    "normalize_graph_spec",
    "sha256_hex",
    "short",
]
