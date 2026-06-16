"""Canonical-JSON hashing primitives — the one place the Arena computes identity.

Design rules (DATA_MANAGEMENT.md §1.3, §2):

* **One algorithm, one canonicalization.** Every Arena-owned hash is
  ``sha256(canonical_json(payload))`` rendered as lowercase 64-hex. ``canonical_json``
  is ``json.dumps`` with ``sort_keys=True`` and the most compact separators, UTF-8,
  ``ensure_ascii=False`` so two equal payloads always serialize byte-identically.
* **Composition is explicit.** ``run_condition_hash`` is a hash of the six
  component hashes in a *fixed order* — never a re-hash of nested payloads — so it
  is reproducible from the components alone.
* **Engine fingerprints are trusted, recorded, and verifiable.** When a producer
  supplies a fingerprint it guarantees stable (dag-ml ``graph_fingerprint``,
  ``fold_set_fingerprint`` …), the Arena adopts it as the dimension key and records
  which engine fingerprint it was derived from. It still *recomputes* its own
  composed hashes so dedup is correct even across producers.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

HASH_ALGO = "sha256"
HEX_LEN = 64

_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(payload: Any) -> str:
    """Serialize ``payload`` to canonical JSON (stable, compact, sorted).

    The result is byte-identical for two semantically-equal payloads, which is the
    property every content hash relies on. ``NaN``/``Infinity`` are rejected
    (``allow_nan=False``) because they are not valid JSON and would break
    cross-language reproducibility.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_hex(data: str | bytes) -> str:
    """Lowercase 64-hex SHA-256 of ``data``."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def fingerprint(payload: Any) -> str:
    """Content hash of an arbitrary JSON-serializable payload.

    ``fingerprint(x) == sha256_hex(canonical_json(x))``. This is the Arena-owned
    identity for any dimension whose canonical spec is a dict/list (operators,
    params, score-computation specs, dataset variants, …).
    """
    return sha256_hex(canonical_json(payload))


def is_hash(value: object) -> bool:
    """True when ``value`` is a lowercase 64-hex SHA-256 string."""
    return isinstance(value, str) and bool(_HEX_RE.match(value))


def short(value: str, n: int = 12) -> str:
    """First ``n`` hex chars of a hash — for human-facing labels only, never keys."""
    return value[:n]


# Fixed component order for the natural identity of a run condition
# (DESIGN.md §6.13 / DATA_MANAGEMENT.md §2). Changing this order is a breaking
# change to every stored ``run_condition_hash``.
_RUN_CONDITION_FIELDS = (
    "dataset_variant_hash",
    "split_instance_hash",
    "cv_instance_hash",
    "rng_context_hash",
    "pipeline_dag_hash",
    "refit_strategy_hash",
)


def compose_run_condition_hash(
    *,
    dataset_variant_hash: str,
    split_instance_hash: str,
    cv_instance_hash: str,
    rng_context_hash: str,
    pipeline_dag_hash: str,
    refit_strategy_hash: str,
) -> str:
    """Compose the natural identity of an experimental condition.

    ``H(dataset_variant, split, cv, rng, pipeline_dag, refit)`` over the six
    component hashes, in a fixed labelled order. The labels are included in the
    hashed payload so a future reordering or addition is detectable rather than
    silently colliding.
    """
    components = {
        "dataset_variant_hash": dataset_variant_hash,
        "split_instance_hash": split_instance_hash,
        "cv_instance_hash": cv_instance_hash,
        "rng_context_hash": rng_context_hash,
        "pipeline_dag_hash": pipeline_dag_hash,
        "refit_strategy_hash": refit_strategy_hash,
    }
    payload = {
        "kind": "run_condition",
        "v": 1,
        "components": [[field, components[field]] for field in _RUN_CONDITION_FIELDS],
    }
    return fingerprint(payload)


def export_hash(manifest: dict[str, Any]) -> str:
    """Idempotency key of an ingested export — SHA-256 of the canonical manifest.

    The ``arena_export_hash`` field itself (if present) is excluded so the hash is a
    fixed point: ``manifest["arena_export_hash"] == export_hash(manifest)``.
    """
    clean = {k: v for k, v in manifest.items() if k != "arena_export_hash"}
    return fingerprint(clean)
