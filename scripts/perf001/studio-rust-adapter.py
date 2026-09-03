#!/usr/bin/python3.11
"""PERF-001 stdio adapter for Studio's packaged Rust sidecar route."""

from __future__ import annotations

import hashlib
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

PROTOCOL = "nirs4all.performance-compare.adapter.v1"
STUDIO_COMMIT = "e254a1ebba578e5b1932d09079088d02eb51d411"
STUDIO_TREE = "d97d2faf4e9643c2bf71cd9fb242f4a1d0db35d9"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _request_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method="GET" if payload is None else "POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def main() -> int:
    started = time.perf_counter()
    request: dict[str, Any] = json.load(sys.stdin)
    if request.get("protocol") != PROTOCOL or request.get("surface") != "studio_rust":
        raise ValueError("unexpected PERF-001 adapter request")
    if request["candidate"]["commit_sha"] != STUDIO_COMMIT:
        raise ValueError("Studio candidate mismatch")

    repository = Path(__file__).resolve().parents[2]
    workspace_root = repository.parents[1]
    studio_root = workspace_root / "_worktrees" / "R3-studio-consolidated"
    backend = studio_root / "backend-dist"
    sidecar = backend / "native/studio-sidecar"
    methods = backend / "native/libn4m.so"
    contract = json.loads((backend / "native/STUDIO_RUNTIME_CONTRACT.json").read_text())
    identity = subprocess.run(
        ["/usr/bin/git", "-C", str(studio_root), "rev-parse", "HEAD", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if identity != [STUDIO_COMMIT, STUDIO_TREE]:
        raise RuntimeError(f"Studio source identity mismatch: {identity}")
    if contract["sidecar"]["sha256"] != _sha256(sidecar):
        raise RuntimeError("Studio sidecar runtime contract mismatch")
    if contract["methods_library"]["member"]["sha256"] != _sha256(methods):
        raise RuntimeError("Studio Methods runtime contract mismatch")

    matrix = request["matrix"]
    with tempfile.TemporaryDirectory(prefix="n4a-perf001-studio-") as temporary:
        root = Path(temporary)
        config = root / "config"
        model = root / "workspace/exports/models/model.n4a"
        config.mkdir(parents=True)
        model.parent.mkdir(parents=True)
        shutil.copyfile(request["archive_v2"]["path"], model)
        (config / "app_settings.json").write_text(
            json.dumps(
                {
                    "version": "3.0",
                    "linked_workspaces": [
                        {
                            "id": "workspace-perf001",
                            "path": str(root / "workspace"),
                            "name": "PERF-001",
                            "is_active": True,
                            "linked_at": "2026-09-03T00:00:00",
                            "last_scanned": None,
                            "discovered": {"runs_count": 0},
                        }
                    ],
                }
            )
        )
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        process = subprocess.Popen(
            [str(sidecar), "--host", "127.0.0.1", "--port", str(port)],
            cwd=backend,
            env={
                "NIRS4ALL_CONFIG": str(config),
                "HOME": str(root),
                "LANG": "C.UTF-8",
                "OMP_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        url = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 20
            while True:
                try:
                    health = _request_json(f"{url}/sidecar/v1/health")
                    if health.get("sidecar_ready") is True:
                        break
                except (OSError, urllib.error.URLError):
                    pass
                if process.poll() is not None or time.monotonic() >= deadline:
                    stderr = process.stderr.read() if process.stderr else ""
                    raise RuntimeError(f"Studio Rust sidecar did not become ready: {stderr}")
                time.sleep(0.02)

            payload = {
                "schema_version": 1,
                "operation": "archive_v2_predict",
                "workspace_id": "workspace-perf001",
                "archive": {"ref": "models/model.n4a", "sha256": request["archive_v2"]["sha256"]},
                "input": {
                    "kind": "array",
                    "sample_ids": matrix["sample_ids"],
                    "x": matrix["x"],
                    "expected_target_names": matrix["target_names"],
                },
                "execution": {"engine": "core_rust_methods", "allow_fallback": False},
            }

            def predict() -> dict[str, Any]:
                result = _request_json(f"{url}/api/predict/archive-v2", payload)
                if result.get("engine") != "core_rust_methods" or result.get("fallback_used") is not False:
                    raise RuntimeError("Studio selected a non-Rust or fallback execution path")
                return result

            result = predict()
            startup_ms = (time.perf_counter() - started) * 1000
            steady: list[float] = []
            for _ in range(request["repeats"]):
                before = time.perf_counter()
                result = predict()
                steady.append((time.perf_counter() - before) * 1000)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    json.dump(
        {
            "protocol": PROTOCOL,
            "surface": "studio_rust",
            "commit_sha": STUDIO_COMMIT,
            "archive_sha256": request["archive_v2"]["sha256"],
            "matrix_sha256": request["matrix_sha256"],
            "sample_ids": matrix["sample_ids"],
            "target_names": matrix["target_names"],
            "predictor_descriptor": request["predictor_descriptor"],
            "predictor_fingerprint": request["predictor_fingerprint"],
            "fallback_used": False,
            "predictions": result["values"],
            "startup_ms": startup_ms,
            "steady_state_ms": steady,
            "evidence": {
                "kind": "local_real",
                "entrypoint": "POST /api/predict/archive-v2",
                "product_backend": "rust-sidecar",
                "python_worker": False,
                "sidecar_sha256": contract["sidecar"]["sha256"],
                "methods_library_sha256": contract["methods_library"]["member"]["sha256"],
                "executor": result["provenance"]["executor"],
            },
        },
        sys.stdout,
        sort_keys=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
