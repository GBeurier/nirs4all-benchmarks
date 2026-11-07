"""Allowlisted metadata receipts; never a raw export archive or a replay input."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.evidence import METADATA_PROFILE, observation_direction
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import is_hash


def sanitize_manifest(model: ArenaRunExport, source_export_hash: str, metadata: dict[str, Any]) -> dict[str, Any]:
    """Keep identities, score context and provenance digests, with no inline data.

    Free-form graphs, params, cards, extra fields and provenance documents can
    contain sample ids, targets or local paths. Only their content digests survive
    in this receipt. This intentionally is not a scientific reproducibility bundle.
    """
    manifest = model.to_manifest()
    allow = {
        "producer": ("capsule", "nirs4all_version", "dag_ml_version", "dag_ml_data_version", "io_version"),
        "dataset": ("dataset_fingerprint", "schema_fingerprint", "relation_fingerprint", "plan_fingerprint"),
        "task": ("task_hash", "task_type", "target_hash"),
        "dataset_variant": ("dataset_variant_hash", "sample_manifest_hash"),
        "pipeline": (
            "pipeline_dag_hash", "controller_fingerprint", "engine_graph_fingerprint", "nirs4all_identity_hash"),
        "split": ("split_spec_hash", "split_instance_hash"),
        "cv": ("cv_spec_hash", "cv_instance_hash", "n_folds", "n_repeats"),
        "rng": ("rng_context_hash",), "refit": ("refit_strategy_hash",),
        "execution": ("status", "terminal_status", "time_ms", "peak_mem_mb"),
        "scores": ("score_computation_hash", "score_version", "metric_implementation", "aggregation_policy",
                   "expected_fold_count", "completed_fold_count", "origin"),
    }
    clean: dict[str, Any] = {"arena_export_schema_version": manifest["arena_export_schema_version"]}
    for block, names in allow.items():
        clean[block] = {name: manifest[block][name] for name in names if name in manifest[block]}
    clean["dataset"]["visibility"] = metadata.get("visibility", "unreported")
    clean["dataset"]["visibility_source"] = metadata.get("visibility_source", "unreported")
    if metadata.get("visibility") == "public" and metadata.get("visibility_source") == "producer_reported":
        clean["dataset"]["dataset_card"] = {
            key: value for key, value in model.dataset.dataset_card.items()
            if key in {"name", "domain", "modality", "signal_type", "axis_unit", "axis_min", "axis_max"}
            and isinstance(value, (str, int, float))
        }
        for key in ("n_samples", "n_features"):
            if getattr(model.dataset, key) is not None:
                clean["dataset"][key] = getattr(model.dataset, key)
        if model.pipeline.human_label is not None:
            clean["pipeline"]["human_label"] = model.pipeline.human_label
    clean["scores"]["observations"] = [
        observation.model_dump(mode="json", include=set(type(observation).model_fields))
        for observation in model.scores.observations
    ]
    for clean_observation, observation in zip(clean["scores"]["observations"], model.scores.observations, strict=True):
        clean_observation["direction"], clean_observation["direction_source"] = observation_direction(observation)
    clean["producer"] = model.producer.model_dump(mode="json")
    if model.execution.failure_message is not None:
        clean["execution"]["failure_message"] = "[redacted]"
    clean["leakage_attestation"] = model.leakage_attestation.model_dump(mode="json")
    if model.provenance is not None:
        clean["provenance"] = {"content_sha256": fingerprint(manifest["provenance"])}
    historical = (manifest.get("provenance") or {}).get("historical_source")
    if isinstance(historical, dict):
        clean["provenance"]["historical_source"] = {
            name: historical[name] for name in ("sha256", "exporter_sha256") if is_hash(historical.get(name))
        }
        for name in ("path", "exporter_path", "experiment_key"):
            if name in historical:
                clean["provenance"]["historical_source"][name + "_sha256"] = fingerprint(historical[name])
    clean["identity_basis"] = {
        name: {"kind": basis.kind, "owner": basis.owner,
               "inputs": {"content_sha256": fingerprint(basis.inputs)}}
        for name, basis in ((name, getattr(model.identity_basis, name)) for name in
                            ("dataset_fingerprint", "task_hash", "dataset_variant_hash"))
    }
    clean.update(sanitized=True, source_export_hash=source_export_hash, metadata_validation=metadata,
                 dataset_card_sha256=fingerprint(model.dataset.dataset_card))
    return clean


def atomic_write_metadata(path: Path, manifest: dict[str, Any]) -> None:
    """Write a private, fsynced receipt atomically. Caller commits SQLite afterwards."""
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError("metadata_path_unsafe")
    fd, temp = tempfile.mkstemp(prefix=".arena-metadata-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(canonical_json(manifest))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        if hasattr(os, "O_DIRECTORY"):
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def read_stored_metadata(
    path: Path | None, source_export_hash: str, *, stored_json: str | None = None
) -> dict[str, Any]:
    """Refuse legacy raw files without modifying them; never serve their contents.

    Even a marker is not an allowlist: re-project a marked file before returning
    it so extras cannot become a data exfiltration channel.
    """
    if not is_hash(source_export_hash) or (path is not None and (path.is_symlink() or path.parent.is_symlink())):
        return {"status": "metadata_unavailable", "reason": "unsafe_metadata_path"}
    try:
        raw = strict_loads(
            stored_json if stored_json is not None else path.read_text(encoding="utf-8") if path else "null"
        )
        if not isinstance(raw, dict) or raw.get("sanitized") is not True:
            return {"status": "metadata_unavailable", "reason": "legacy_unsanitized_export_refused"}
        if raw.get("source_export_hash") != source_export_hash:
            return {"status": "metadata_unavailable", "reason": "metadata_source_mismatch"}
        # This receipt has no replay structure. Return only our fixed receipt keys;
        # arbitrary input, including nested extras, is not exposed by this reader.
        validation = raw.get("metadata_validation") or {}
        if validation.get("profile") != METADATA_PROFILE or raw.get("arena_export_schema_version") != 2:
            return {"status":"metadata_unavailable", "reason":"unsupported_receipt"}
        allowed = {"arena_export_schema_version", "producer", "identity_basis", "dataset", "task", "dataset_variant",
                   "pipeline", "split", "cv", "rng", "refit", "execution", "scores", "leakage_attestation"}
        projected = ArenaRunExport.model_validate({key: raw[key] for key in allowed if key in raw}, strict=True)
        # Reapply the full allowlist even to marked receipts. A marker alone does
        # not authorize nested free-form fields copied from an old raw export.
        result = sanitize_manifest(projected, source_export_hash, validation)
        basis = raw["identity_basis"]
        if any(set(entry["inputs"]) != {"content_sha256"} or not is_hash(entry["inputs"]["content_sha256"])
               for entry in basis.values()):
            return {"status":"metadata_unavailable", "reason":"unsafe_receipt_identity_inputs"}
        result["identity_basis"] = basis
        if not is_hash(raw.get("dataset_card_sha256")):
            return {"status": "metadata_unavailable", "reason": "invalid_card_digest"}
        result.update(sanitized=True, source_export_hash=source_export_hash,
                      dataset_card_sha256=raw["dataset_card_sha256"])
        visibility = validation.get("visibility", "unreported")
        source = validation.get("visibility_source", "unreported")
        # Provenance digests already represent the removed original documents.
        provenance = raw.get("provenance") or {}
        result.pop("provenance", None)
        if is_hash(provenance.get("content_sha256")):
            result["provenance"] = {"content_sha256": provenance["content_sha256"]}
        historical = provenance.get("historical_source") or {}
        if isinstance(historical, dict) and "provenance" in result:
            result["provenance"]["historical_source"] = {
                k: v for k, v in historical.items()
                if k in {"sha256", "exporter_sha256", "path_sha256", "exporter_path_sha256", "experiment_key_sha256"}
                and is_hash(v)
            }
        result["metadata_validation"] = {
            k: validation[k] for k in (
                "status", "scientific_verification", "terminal_status", "fold_coverage", "hash_profile")
            if validation.get(k) in {
                "metadata_valid", "not_performed", "completed", "incomplete", "failed", "cancelled",
                "unreported", "reported", "not_reported", "v2_normalized"}
        }
        result["metadata_validation"].update(
            profile=METADATA_PROFILE, visibility=visibility, visibility_source=source)
        return result
    except (OSError, ValueError, TypeError, AttributeError, KeyError):
        return {"status": "metadata_unavailable", "reason": "metadata_receipt_unreadable"}
