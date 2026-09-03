#!/usr/bin/python3.11
"""PERF-001 stdio adapter for the exact public Python native oracle."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PROTOCOL = "nirs4all.performance-compare.adapter.v1"
PYTHON_COMMIT = "e227244464983ea2a94ebc01b6af30d474a025df"
PYTHON_TREE = "f142b410194e3c99190fb97685724140a5599159"
CORE_COMMIT = "e0f5d485eae4279f02d58fe82fad3946202e463f"
CORE_TREE = "3fd59b96fc5728088c6d1d207e783d826f87401f"
METHODS_COMMIT = "48ad1e5a50844f68c2b99e93b02ad6a3b491c07b"
METHODS_TREE = "f2eaa3c46629c26d11913a25bff723f9a9cefbc9"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _attest(root: Path, commit: str, tree: str) -> None:
    actual = subprocess.run(
        ["/usr/bin/git", "-C", str(root), "rev-parse", "HEAD", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if actual != [commit, tree]:
        raise RuntimeError(f"source identity mismatch for {root}: {actual}")


def main() -> int:
    started = time.perf_counter()
    request: dict[str, Any] = json.load(sys.stdin)
    if request.get("protocol") != PROTOCOL or request.get("surface") != "python_oracle":
        raise ValueError("unexpected PERF-001 adapter request")
    if request["candidate"]["commit_sha"] != PYTHON_COMMIT:
        raise ValueError("Python candidate mismatch")

    repository = Path(__file__).resolve().parents[2]
    workspace = repository.parents[1]
    python_root = workspace / "_worktrees" / "PERF001-python-e227"
    core_root = workspace / "_worktrees" / "CORE-formats-aggregate"
    methods_root = workspace / "_worktrees" / "CORE001-methods-48ad1e5"
    python_site = repository / ".perf001" / "python-site"
    methods_library = methods_root / "build/dev-release/cpp/src/libn4m.so.2.5.0"
    _attest(python_root, PYTHON_COMMIT, PYTHON_TREE)
    _attest(core_root, CORE_COMMIT, CORE_TREE)
    _attest(methods_root, METHODS_COMMIT, METHODS_TREE)
    if not python_site.is_dir() or not methods_library.is_file():
        raise RuntimeError("prepared Core wheel or Methods ABI 2.5 closure is missing")
    sys.path[:0] = [str(python_root), str(python_site)]

    import nirs4all  # noqa: PLC0415

    matrix = request["matrix"]

    def predict() -> tuple[list[list[float]], dict[str, Any]]:
        result = nirs4all.predict(
            model=request["archive_v2"]["path"],
            data={"X": matrix["x"], "sample_ids": matrix["sample_ids"]},
            engine="native",
            methods_library_path=methods_library,
        )
        values = result.y_pred.tolist()
        metadata = dict(result.metadata)
        descriptors = metadata.get("native_predictor_descriptors")
        if metadata.get("engine") != "core-native" or not isinstance(descriptors, list):
            raise RuntimeError("public Python prediction did not use the closed native Core path")
        if descriptors[0].get("descriptor_fingerprint") != request["predictor_fingerprint"]:
            raise RuntimeError("Python native descriptor fingerprint mismatch")
        return values, metadata

    predictions, metadata = predict()
    startup_ms = (time.perf_counter() - started) * 1000
    steady: list[float] = []
    for _ in range(request["repeats"]):
        before = time.perf_counter()
        predictions, metadata = predict()
        steady.append((time.perf_counter() - before) * 1000)

    json.dump(
        {
            "protocol": PROTOCOL,
            "surface": "python_oracle",
            "commit_sha": PYTHON_COMMIT,
            "archive_sha256": request["archive_v2"]["sha256"],
            "matrix_sha256": request["matrix_sha256"],
            "sample_ids": matrix["sample_ids"],
            "target_names": matrix["target_names"],
            "predictor_descriptor": request["predictor_descriptor"],
            "predictor_fingerprint": request["predictor_fingerprint"],
            "fallback_used": False,
            "predictions": predictions,
            "startup_ms": startup_ms,
            "steady_state_ms": steady,
            "evidence": {
                "kind": "local_real",
                "entrypoint": "nirs4all.predict(engine='native')",
                "core_commit": CORE_COMMIT,
                "methods_commit": METHODS_COMMIT,
                "methods_library_sha256": _sha256(methods_library),
                "engine": metadata["engine"],
            },
        },
        sys.stdout,
        sort_keys=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
