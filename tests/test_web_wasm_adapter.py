"""The Web/WASM launcher is closed, content-addressed and fail-closed."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tarfile
from pathlib import Path

import pytest

from nirs4all_benchmarks.qualification import web_wasm_adapter
from nirs4all_benchmarks.qualification.contract import AdapterRequest
from nirs4all_benchmarks.qualification.web_wasm_adapter import _CHILD_SCRIPT, _FIT_SYMBOL_PATTERN, adapt

WEB_COMMIT = "6b17d09e7e6261ff245fa1dc7e12715d6423ed05"
CORE_COMMIT = "7c3ed3fdaeec7dd01ee2a99a8b72bfa378676d66"
REQUIRED_CLOSURE = {
    "aggregate_package": "node_modules/nirs4all/package.json",
    "core_provenance": "node_modules/nirs4all/PROVENANCE.md",
    "aggregate_index": "node_modules/nirs4all/src/index.js",
    "aggregate_archive_v2": "node_modules/nirs4all/src/archive-v2.js",
    "native_package": "node_modules/nirs4all/native/package.json",
    "native_loader": "node_modules/nirs4all/native/native.js",
    "core_native_wasm": "node_modules/nirs4all/native/native.wasm",
    "methods_index": "node_modules/@nirs4all/methods/index.js",
    "methods_wasm": "node_modules/@nirs4all/methods/n4m.wasm",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


@pytest.fixture(autouse=True)
def _unit_python_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(web_wasm_adapter, "_python_runtime", lambda _path, _paths: tmp_path)


@pytest.fixture
def web_artifacts(tmp_path: Path) -> dict[str, Path]:
    scenario = tmp_path / "scenario.json"
    _write_json(
        scenario,
        {
            "schema_version": "nirs4all.perf-scenario.v1",
            "scenario_id": "perf001-multitarget-archive-v2",
            "operation": "archive_v2_predict",
            "fallback_allowed": False,
            "prediction": {
                "sample_ids": ["predict.0", "predict.1"],
                "x": [[1.5, 0.5], [3.5, 1.5]],
            },
            "legacy_oracle": {},
        },
    )
    profile = tmp_path / "profile.json"
    _write_json(
        profile,
        {
            "contract": "nirs4all.web-runtime-profile.v1",
            "profile": "strict-wasm",
            "nativeWasmRequired": True,
            "jsBackendFallback": "forbid",
            "providerMatrixFallback": "forbid",
            "schedulerFallback": "forbid",
            "remoteComputeProvider": "forbid",
        },
    )
    source = tmp_path / "web-source.tar"
    archive = tmp_path / "model.n4a"
    archive.write_bytes(b"Archive V2 fixture")
    node = tmp_path / "node"
    node.write_text(
        "#!/bin/sh\n"
        "printf '%s' '{\"protocol\":\"nirs4all.web-wasm-child.v1\","
        "\"values\":[1.6363636363636365,13.272727272727273,2.4999999999999996,15.0],"
        "\"targetNames\":[\"protein\",\"moisture\"],"
        "\"sampleIds\":[\"predict.0\",\"predict.1\"],"
        f"\"archiveSha256\":\"{_sha256(archive)}\","
        "\"engine\":\"nirs4all-methods-wasm\",\"fallback\":false,"
        "\"calls\":{\"n4m_model_predict_alloc\":1},\"fitCalls\":0,\"predictionMs\":1.25}'\n",
        encoding="utf-8",
    )
    node.chmod(0o755)
    files = []
    for role, destination in REQUIRED_CLOSURE.items():
        closure_file = tmp_path / f"closure-{role}"
        if role == "core_provenance":
            closure_file.write_text(
                f"nirs4all-core` commit `{CORE_COMMIT}`\n"
                "native 9d1c173f0109db308325989052768e6492ed7dc72e14373a18583f8739a3d469\n",
                encoding="utf-8",
            )
        else:
            closure_file.write_bytes(role.encode())
        files.append(
            {
                "role": role,
                "path": str(closure_file),
                "destination": destination,
                "sha256": _sha256(closure_file),
                "snapshot_member": f"web-app/runtime/{role}",
            }
        )
    native = next(item for item in files if item["role"] == "core_native_wasm")
    provenance = next(item for item in files if item["role"] == "core_provenance")
    Path(provenance["path"]).write_text(
        f"nirs4all-core` commit `{CORE_COMMIT}`\nnative {native['sha256']}\n",
        encoding="utf-8",
    )
    provenance["sha256"] = _sha256(Path(provenance["path"]))
    with tarfile.open(source, "w") as snapshot:
        snapshot.add(archive, arcname="web-app/src/engine/fixtures/archive-v2/multitarget-pls.n4a")
        for item in files:
            snapshot.add(item["path"], arcname=item["snapshot_member"])
    closure = tmp_path / "closure.json"
    _write_json(
        closure,
        {
            "schema_version": "nirs4all.web-wasm-runtime-closure.v1",
            "web_commit_sha": WEB_COMMIT,
            "web_version": "0.1.8",
            "core_commit_sha": CORE_COMMIT,
            "aggregate_version": "0.3.22",
            "source_snapshot_sha256": _sha256(source),
            "entrypoint": "node_modules/nirs4all/src/index.js",
            "files": files,
        },
    )
    runtime_python = tmp_path / "python"
    runtime_python.write_bytes(b"python")
    runtime_python.chmod(0o755)
    python_closure = tmp_path / "python-runtime.json"
    _write_json(python_closure, {})
    return {
        "scenario": scenario,
        "runtime_python": runtime_python,
        "python_runtime_closure": python_closure,
        "runtime_node": node,
        "web_runtime_closure": closure,
        "web_source_snapshot": source,
        "strict_profile": profile,
        "archive_v2": archive,
    }


def _request(paths: dict[str, Path]) -> AdapterRequest:
    return AdapterRequest.model_validate(
        {
            "component": "web_wasm",
            "declared_version": "0.1.8",
            "declared_commit_sha": WEB_COMMIT,
            "scenario": {
                "scenario_id": "perf001-multitarget-archive-v2",
                "definition": {"path": paths["scenario"], "sha256": _sha256(paths["scenario"])},
            },
            "tolerances": {"absolute": 1e-9, "relative": 1e-9},
            "artifacts": [
                {"role": role, "path": path, "sha256": _sha256(path)}
                for role, path in paths.items()
                if role != "scenario"
            ],
        }
    )


def test_web_launcher_finalizes_one_multitarget_replay(web_artifacts: dict[str, Path]) -> None:
    output = adapt(_request(web_artifacts))

    assert output.component == "web_wasm"
    assert output.completed is True
    assert output.fallback_used is False
    assert output.observations == {
        "prediction.protein": [1.6363636363636365, 2.4999999999999996],
        "prediction.moisture": [13.272727272727273, 15.0],
    }
    assert output.metrics["runtime.fit_calls"] == 0
    assert output.metrics["runtime.predict_calls"] == 1


def test_web_launcher_refuses_a_missing_child_fit_counter(web_artifacts: dict[str, Path]) -> None:
    node = web_artifacts["runtime_node"]
    node.write_text(node.read_text(encoding="utf-8").replace(',"fitCalls":0', ""), encoding="utf-8")

    with pytest.raises(ValueError, match="reported a fit/refit call"):
        adapt(_request(web_artifacts))


def test_web_launcher_refuses_tampered_closure_file(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    closure = json.loads(web_artifacts["web_runtime_closure"].read_text(encoding="utf-8"))
    Path(closure["files"][0]["path"]).write_bytes(b"tampered after manifest")

    with pytest.raises(ValueError, match="SHA-256 changed before execution"):
        adapt(request)


def test_web_launcher_refuses_commit_identity_mismatch(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    request.declared_commit_sha = "f" * 40

    with pytest.raises(ValueError, match="declared Web commit differs"):
        adapt(request)


def test_web_launcher_refuses_core_provenance_mismatch(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    closure = json.loads(web_artifacts["web_runtime_closure"].read_text(encoding="utf-8"))
    closure["core_commit_sha"] = "f" * 40
    _write_json(web_artifacts["web_runtime_closure"], closure)
    request.artifacts = [
        artifact.model_copy(update={"sha256": _sha256(artifact.path)})
        if artifact.role == "web_runtime_closure"
        else artifact
        for artifact in request.artifacts
    ]

    with pytest.raises(ValueError, match="Core provenance does not bind"):
        adapt(request)


def test_web_launcher_refuses_non_strict_profile(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    profile = json.loads(web_artifacts["strict_profile"].read_text(encoding="utf-8"))
    profile["jsBackendFallback"] = "allow"
    _write_json(web_artifacts["strict_profile"], profile)
    request.artifacts = [
        artifact.model_copy(update={"sha256": _sha256(artifact.path)})
        if artifact.role == "strict_profile"
        else artifact
        for artifact in request.artifacts
    ]

    with pytest.raises(ValueError, match="exactly forbid every Web fallback"):
        adapt(request)


def test_web_launcher_refuses_missing_node_runtime(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    web_artifacts["runtime_node"].unlink()

    with pytest.raises(ValueError, match=r"runtime_node.*not a regular file"):
        adapt(request)


def test_web_launcher_refuses_missing_methods_runtime(web_artifacts: dict[str, Path]) -> None:
    request = _request(web_artifacts)
    closure = json.loads(web_artifacts["web_runtime_closure"].read_text(encoding="utf-8"))
    closure["files"] = [item for item in closure["files"] if item["role"] != "methods_wasm"]
    _write_json(web_artifacts["web_runtime_closure"], closure)
    request.artifacts = [
        artifact.model_copy(update={"sha256": _sha256(artifact.path)})
        if artifact.role == "web_runtime_closure"
        else artifact
        for artifact in request.artifacts
    ]

    with pytest.raises(ValueError, match=r"missing required roles.*methods_wasm"):
        adapt(request)


@pytest.mark.parametrize("symbol", ["n4m_fit", "n4m_model_refit", "n4m_refit_model"])
def test_fit_symbol_detector_covers_fit_and_refit_abi_names(symbol: str) -> None:
    assert re.search(_FIT_SYMBOL_PATTERN, symbol)


def test_node_child_refuses_a_refit_call(tmp_path: Path) -> None:
    node = os.environ.get("N4A_TEST_NODE")
    if node is None:
        pytest.skip("set N4A_TEST_NODE to exercise the JavaScript child")
    entrypoint = tmp_path / "fake-web-aggregate.mjs"
    entrypoint.write_text(
        """
const module = { ccall(_symbol) { return 0 } }
export async function loadMethodsWasm() {
  return { async loadModule() {}, getModule() { return module } }
}
export async function replayMethodsArchiveV2(_archive, dataset) {
  for (const symbol of [
    'n4m_serialization_inspect', 'n4m_model_import_from_buffer', 'n4m_model_predict_alloc',
    'n4m_model_destroy', 'n4m_array_free', 'n4m_context_create', 'n4m_context_destroy',
  ]) module.ccall(symbol)
  module.ccall('n4m_model_refit')
  return {
    schema: 'nirs4all.core.archive-v2-replay.v1', engine: 'nirs4all-methods-wasm', fallback: false,
    archiveSha256: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    sampleIds: dataset.sampleIds, targetNames: ['protein', 'moisture'],
    rows: 2, cols: 2, data: [1, 2, 3, 4],
  }
}
""",
        encoding="utf-8",
    )
    archive = tmp_path / "model.n4a"
    archive.write_bytes(b"archive")
    payload = {
        "protocol": "nirs4all.web-wasm-child.v1",
        "entrypoint": str(entrypoint),
        "archive": str(archive),
        "archiveSha256": "a" * 64,
        "fitSymbolPattern": _FIT_SYMBOL_PATTERN,
        "scenario": {
            "prediction": {
                "sample_ids": ["predict.0", "predict.1"],
                "x": [[1.5, 0.5], [3.5, 1.5]],
            }
        },
    }

    process = subprocess.run(
        [node, "--input-type=module", "--eval", _CHILD_SCRIPT],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode != 0
    assert "fit/refit call observed" in process.stderr
    assert "n4m_model_refit" in process.stderr
