"""Strict JSON inputs: duplicate keys and non-finite constants are refusals."""
from __future__ import annotations

import json
from typing import Any


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _constant(_value: str) -> Any:
    raise ValueError("nonfinite_json")


def strict_loads(value: str | bytes) -> Any:
    return json.loads(value, object_pairs_hook=_pairs, parse_constant=_constant)
