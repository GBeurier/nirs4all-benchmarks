"""Executable adapter for finalized, content-addressed JSON result artifacts."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

from pydantic import ValidationError

from nirs4all_benchmarks.qualification.contract import AdapterRequest, ComponentOutput


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def adapt(request: AdapterRequest) -> ComponentOutput:
    """Validate and return the single artifact with role ``result``."""
    results = [artifact for artifact in request.artifacts if artifact.role == "result"]
    if len(results) != 1:
        raise ValueError("json result adapter requires exactly one artifact with role 'result'")
    if _sha256(results[0].path) != results[0].sha256:
        raise ValueError("result artifact SHA-256 changed after runner preflight")
    output = ComponentOutput.model_validate_json(results[0].path.read_text(encoding="utf-8"))
    if output.component != request.component:
        raise ValueError("result component does not match adapter request")
    if output.scenario_id != request.scenario.scenario_id:
        raise ValueError("result scenario does not match adapter request")
    return output


def main() -> None:
    """Read one adapter request from stdin and emit one finalized JSON document."""
    try:
        request = AdapterRequest.model_validate_json(sys.stdin.read())
        output = adapt(request)
    except (OSError, ValueError, ValidationError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    sys.stdout.write(output.model_dump_json())


if __name__ == "__main__":
    main()
