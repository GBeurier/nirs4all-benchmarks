"""Private per-export source fragments and integrity checks."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from nirs4all_benchmarks.identity import canonical_json, fingerprint


def make_row(validation: Any, card_hash: str | None) -> dict[str, Any]:
    model = validation.model
    text = validation.source_metadata_json
    assert model is not None and text is not None and validation.resolved is not None
    provenance = model.provenance.model_dump(mode="json") if model.provenance is not None else None
    return {
        "arena_export_hash": validation.arena_export_hash,
        "execution_hash": validation.resolved.execution_hash,
        "source_metadata_json": text,
        "source_metadata_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "normalized_provenance_sha256": fingerprint(provenance),
        "normalized_dataset_card_sha256": fingerprint(model.dataset.dataset_card),
        "dataset_card_hash": card_hash,
        "representation_kind": validation.source_representation_kind,
        "profile_version": 1,
    }


def valid_row(row: dict[str, Any]) -> bool:
    try:
        from nirs4all_benchmarks.contract.arena_run_export import ProvenanceBlock

        text = row["source_metadata_json"]
        source = json.loads(text)
        if "arena_context" in source:
            context = source["arena_context"]
            if (not isinstance(context, dict)
                    or set(context) != {"schema_version", "arena_export_hash", "pipeline", "task"}
                    or type(context["schema_version"]) is not int or context["schema_version"] != 1
                    or context["arena_export_hash"] != row["arena_export_hash"]):
                return False
            for block, key in (("pipeline", "human_label"), ("task", "target_unit")):
                value = context[block]
                if (not isinstance(value, dict) or not set(value) <= {key}
                        or (key in value and value[key] is not None and not isinstance(value[key], str))):
                    return False
        provenance = source.get("provenance")
        normalized_provenance = (ProvenanceBlock.model_validate(provenance, strict=True).model_dump(mode="json")
                                 if provenance is not None else None)
        card = source.get("dataset", {}).get("dataset_card", {})
        expected_card = fingerprint(card) if card else None
        return (canonical_json(source) == text
                and hashlib.sha256(text.encode("utf-8")).hexdigest() == row["source_metadata_sha256"]
                and fingerprint(normalized_provenance) == row["normalized_provenance_sha256"]
                and fingerprint(card) == row["normalized_dataset_card_sha256"]
                and expected_card == row["dataset_card_hash"]
                and row["representation_kind"] in {"submitted_json", "model_normalized"}
                and row["profile_version"] == 1)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return False


def binding_consistent(execution: dict[str, Any], row: dict[str, Any], receipt: dict[str, Any]) -> bool:
    """Check partial consistency among cached facts; hashes are not authentication."""
    try:
        data = json.loads(receipt["receipt_json"])
        return (valid_row(row) and fingerprint(data) == receipt["receipt_sha256"]
                and execution["arena_export_hash"] == row["arena_export_hash"]
                == receipt["arena_export_hash"] == data["source_export_hash"]
                and execution["execution_hash"] == row["execution_hash"]
                and execution["status"] == data["metadata_validation"]["terminal_status"]
                and row["normalized_provenance_sha256"] == (
                    data.get("provenance", {}).get("content_sha256") or fingerprint(None))
                and row["normalized_dataset_card_sha256"] == data["dataset_card_sha256"])
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
