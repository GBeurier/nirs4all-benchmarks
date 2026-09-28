"""Tests for the Studio native Archive V2 qualification adapter."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from nirs4all_benchmarks.qualification import studio_sidecar_adapter
from nirs4all_benchmarks.qualification.contract import AdapterRequest

DOCS = Path(__file__).parent / "fixtures"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def artifacts(tmp_path: Path) -> dict[str, Path]:
    scenario = tmp_path / "scenario.json"
    scenario.write_bytes((DOCS / "perf001-scenario.v1.json").read_bytes())
    result = {"scenario": scenario}
    for role in studio_sidecar_adapter._ROLES:
        path = tmp_path / role
        path.write_bytes(role.encode())
        if role in {"runtime_python", "runtime_sidecar"}:
            path.chmod(0o755)
        result[role] = path
    return result


def _request(paths: dict[str, Path]) -> AdapterRequest:
    return AdapterRequest.model_validate(
        {
            "component": "studio_sidecar",
            "declared_version": "0.9.1",
            "declared_commit_sha": studio_sidecar_adapter.STUDIO_COMMIT,
            "scenario": {
                "scenario_id": "perf001-multitarget-archive-v2",
                "definition": {"path": paths["scenario"], "sha256": _sha256(paths["scenario"])},
            },
            "tolerances": {"absolute": 1e-12, "relative": 1e-9},
            "artifacts": [
                {"role": role, "path": path, "sha256": _sha256(path)}
                for role, path in paths.items()
                if role != "scenario"
            ],
        }
    )


def _response() -> dict[str, object]:
    return {
        "schema_version": 1,
        "operation": "archive_v2_predict",
        "archive_id": studio_sidecar_adapter.ARCHIVE_ID,
        "archive_sha256": studio_sidecar_adapter.ARCHIVE_SHA256,
        "engine": "core_rust_methods",
        "fallback_used": False,
        "sample_ids": ["predict.0", "predict.1"],
        "target_names": ["protein", "moisture"],
        "values": studio_sidecar_adapter.EXPECTED_VALUES,
        "provenance": studio_sidecar_adapter.EXPECTED_PROVENANCE,
    }


def test_studio_adapter_returns_native_multitarget_observations(
    artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(studio_sidecar_adapter, "_adapter_closure", lambda *_args: Path("/stage"))
    monkeypatch.setattr(studio_sidecar_adapter, "_studio_closure", lambda *_args: None)
    monkeypatch.setattr(
        studio_sidecar_adapter,
        "_execute",
        lambda *_args: studio_sidecar_adapter._Evidence(response=_response(), elapsed_ms=2.5),
    )

    output = studio_sidecar_adapter.adapt(_request(artifacts))

    assert output.completed is True
    assert output.fallback_used is False
    assert output.qualification_refusal_reason is None
    assert output.observations == {
        "prediction.protein": [1.6363636363636365, 2.4999999999999996],
        "prediction.moisture": [13.272727272727273, 15.0],
    }
    assert output.metrics["runtime.python_children"] == 0
    assert output.metrics["runtime.route_scratch_entries"] == 0


def test_studio_adapter_refuses_changed_target_order(
    artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    response = _response()
    response["target_names"] = ["moisture", "protein"]
    monkeypatch.setattr(studio_sidecar_adapter, "_adapter_closure", lambda *_args: Path("/stage"))
    monkeypatch.setattr(studio_sidecar_adapter, "_studio_closure", lambda *_args: None)
    monkeypatch.setattr(
        studio_sidecar_adapter,
        "_execute",
        lambda *_args: studio_sidecar_adapter._Evidence(response=response, elapsed_ms=2.5),
    )

    with pytest.raises(ValueError, match="target order"):
        studio_sidecar_adapter.adapt(_request(artifacts))


def test_studio_adapter_refuses_changed_value(artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    response = _response()
    response["values"] = [[99.0, 13.272727272727273], [2.4999999999999996, 15.0]]
    monkeypatch.setattr(studio_sidecar_adapter, "_adapter_closure", lambda *_args: Path("/stage"))
    monkeypatch.setattr(studio_sidecar_adapter, "_studio_closure", lambda *_args: None)
    monkeypatch.setattr(
        studio_sidecar_adapter,
        "_execute",
        lambda *_args: studio_sidecar_adapter._Evidence(response=response, elapsed_ms=2.5),
    )

    with pytest.raises(ValueError, match="differs from Core witness"):
        studio_sidecar_adapter.adapt(_request(artifacts))


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("archive_id", "archive:wrong"),
        ("fallback_used", True),
        ("provenance", {"executor": "nirs4all-core"}),
    ],
)
def test_studio_adapter_refuses_changed_response_identity_or_provenance(
    artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch, field: str, changed: object
) -> None:
    response = _response()
    response[field] = changed
    monkeypatch.setattr(studio_sidecar_adapter, "_adapter_closure", lambda *_args: Path("/stage"))
    monkeypatch.setattr(studio_sidecar_adapter, "_studio_closure", lambda *_args: None)
    monkeypatch.setattr(
        studio_sidecar_adapter,
        "_execute",
        lambda *_args: studio_sidecar_adapter._Evidence(response=response, elapsed_ms=2.5),
    )

    with pytest.raises(ValueError, match="identity, provenance, engine, fallback"):
        studio_sidecar_adapter.adapt(_request(artifacts))


def test_studio_adapter_refuses_missing_sidecar_before_execution(artifacts: dict[str, Path]) -> None:
    request = _request(artifacts)
    artifacts["runtime_sidecar"].unlink()
    with pytest.raises(ValueError, match=r"runtime_sidecar.*non-symlink regular file"):
        studio_sidecar_adapter.adapt(request)


def test_studio_adapter_refuses_symlinked_sidecar_before_execution(artifacts: dict[str, Path]) -> None:
    sidecar = artifacts["runtime_sidecar"]
    target = sidecar.with_suffix(".real")
    sidecar.rename(target)
    sidecar.symlink_to(target)

    with pytest.raises(ValueError, match=r"runtime_sidecar.*non-symlink regular file"):
        studio_sidecar_adapter.adapt(_request(artifacts))


def test_studio_adapter_refuses_artifact_tamper_after_request(artifacts: dict[str, Path]) -> None:
    request = _request(artifacts)
    artifacts["methods_library"].write_bytes(b"tampered")

    with pytest.raises(ValueError, match=r"methods_library.*SHA-256 changed"):
        studio_sidecar_adapter.adapt(request)


def test_studio_adapter_refuses_changed_multitarget_identity(artifacts: dict[str, Path]) -> None:
    scenario = json.loads(artifacts["scenario"].read_text(encoding="utf-8"))
    scenario["legacy_oracle"]["training"]["target_names"] = ["protein"]
    artifacts["scenario"].write_text(json.dumps(scenario), encoding="utf-8")
    with pytest.raises(ValueError, match="scenario SHA-256"):
        studio_sidecar_adapter.adapt(_request(artifacts))


def test_studio_adapter_refuses_changed_prediction_x_even_with_declared_digest(
    artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = json.loads(artifacts["scenario"].read_text(encoding="utf-8"))
    scenario["prediction"]["x"][0][0] = 9.0
    artifacts["scenario"].write_text(json.dumps(scenario), encoding="utf-8")
    changed_sha = _sha256(artifacts["scenario"])
    monkeypatch.setattr(studio_sidecar_adapter, "SCENARIO_SHA256", changed_sha)

    with pytest.raises(ValueError, match="prediction witness identity changed"):
        studio_sidecar_adapter.adapt(_request(artifacts))


def test_studio_adapter_refuses_scenario_tamper_after_request(artifacts: dict[str, Path]) -> None:
    request = _request(artifacts)
    artifacts["scenario"].write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="scenario SHA-256"):
        studio_sidecar_adapter.adapt(request)


def test_route_isolation_detects_child_or_scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(studio_sidecar_adapter, "_descendants", lambda _pid: {123})
    with (
        pytest.raises(ValueError, match="spawned children or scratch"),
        studio_sidecar_adapter._Isolation(42, scratch, set()),
    ):
        (scratch / "worker").write_text("unexpected", encoding="utf-8")
