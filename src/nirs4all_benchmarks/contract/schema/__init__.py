"""Loaders + validators for the frozen Arena wire contracts."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any

import jsonschema


def _load(name: str) -> dict[str, Any]:
    text = resources.files(__package__).joinpath(name).read_text(encoding="utf-8")
    return json.loads(text)


ARENA_RUN_EXPORT_SCHEMA: dict[str, Any] = _load("arena_run_export.schema.json")

# The sample-keyed residuals.parquet column contract (DATA_MANAGEMENT.md §4).
# (column_name, arrow_logical_type, nullable)
RESIDUALS_PARQUET_COLUMNS: list[tuple[str, str, bool]] = [
    ("sample_id", "utf8", False),
    ("group_id", "utf8", True),
    ("origin_sample_id", "utf8", True),
    ("scope", "utf8", False),
    ("fold_id", "utf8", True),
    ("partition", "utf8", False),
    ("y_true", "f64", True),
    ("y_pred", "f64", True),
    ("y_proba", "list<f64>", True),
    ("residual", "f64", True),
    ("weight", "f64", True),
]

_VALIDATOR = jsonschema.Draft202012Validator(ARENA_RUN_EXPORT_SCHEMA)


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    """Validate a manifest dict against the frozen schema.

    Returns a list of human-readable error strings (empty == valid). The Arena
    ingestion pipeline treats a non-empty list as a hard rejection at VERIFY.
    """
    errors = sorted(_VALIDATOR.iter_errors(manifest), key=lambda e: list(e.absolute_path))
    return [f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}" for e in errors]


__all__ = ["ARENA_RUN_EXPORT_SCHEMA", "RESIDUALS_PARQUET_COLUMNS", "validate_manifest"]
