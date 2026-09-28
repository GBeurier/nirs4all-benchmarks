"""GATE-001 artifact-driven qualification contract and refusal behavior."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from nirs4all_benchmarks.cli import app
from nirs4all_benchmarks.qualification import QualificationManifest, run_qualification

COMPONENTS = ("python_oracle", "core_rust", "studio_sidecar", "web_wasm")
COMMITS = {
    "python_oracle": "1" * 40,
    "core_rust": "2" * 40,
    "studio_sidecar": "3" * 40,
    "web_wasm": "4" * 40,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> Path:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    return path


def _result(
    component: str,
    *,
    completed: bool = True,
    fallback_used: bool = False,
    qualification_refusal_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "protocol": "nirs4all.qualification-adapter.v1",
        "component": component,
        "scenario_id": "pls-parity",
        "version": "1.0.0",
        "commit_sha": COMMITS[component],
        "completed": completed,
        "fallback_used": fallback_used,
        "qualification_refusal_reason": qualification_refusal_reason,
        "observations": {"predictions": [1.0, 2.0, 3.0]} if completed else {},
        "metrics": {"rmse": 0.125},
        "timings_ms": {"runtime": 1.25},
    }


def _manifest(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    scenario = _write_json(
        tmp_path / "scenario.json",
        {"scenario_id": "pls-parity", "dataset": "synthetic-v1", "pipeline": "pls-v1"},
    )
    adapter = Path(sys.executable).parent / "n4a-gate-json-adapter"
    assert adapter.is_file(), "the package's executable JSON adapter must be installed"
    components = []
    for component in COMPONENTS:
        result = _write_json(tmp_path / f"{component}.json", _result(component))
        components.append(
            {
                "component": component,
                "adapter": "stdio-json-v1",
                "executable": {"path": str(adapter), "sha256": _sha256(adapter)},
                "artifacts": [{"role": "result", "path": str(result), "sha256": _sha256(result)}],
                "version": "1.0.0",
                "commit_sha": COMMITS[component],
                "timeout_seconds": 10,
            }
        )
    value = {
        "schema_version": "nirs4all.qualification-manifest.v1",
        "scenario": {
            "scenario_id": "pls-parity",
            "definition": {"path": str(scenario), "sha256": _sha256(scenario)},
        },
        "tolerances": {"absolute": 1e-12, "relative": 1e-9},
        "components": components,
    }
    return _write_json(tmp_path / "manifest.json", value), value


def _component(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(item for item in report["components"] if item["component"] == name)


def test_json_adapter_qualifies_all_four_explicit_artifacts(tmp_path: Path):
    manifest, _ = _manifest(tmp_path)
    report = run_qualification(manifest.resolve())

    assert report["overall_disposition"] == "passed"
    assert report["scenario"]["definition"]["scenario_id"] == "pls-parity"
    assert report["tolerances"] == {"absolute": 1e-12, "relative": 1e-9}
    assert [item["component"] for item in report["components"]] == list(COMPONENTS)
    for item in report["components"]:
        assert item["disposition"] == "passed"
        assert item["executed"] is True
        assert item["version"] == "1.0.0"
        assert item["commit_sha"] == COMMITS[item["component"]]
        assert item["executable"]["actual_sha256"] == item["executable"]["expected_sha256"]
        assert item["artifacts"][0]["actual_sha256"] == item["artifacts"][0]["expected_sha256"]
        assert item["metrics"]["comparison.outside_tolerance"] == 0.0
        assert item["timings_ms"]["process_wall"] >= 0


def test_cli_writes_the_terminal_json_report(tmp_path: Path):
    manifest, _ = _manifest(tmp_path)
    destination = tmp_path / "evidence" / "report.json"

    result = CliRunner().invoke(
        app,
        ["qualify-artifacts", str(manifest.resolve()), "--json-out", str(destination)],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(destination.read_text(encoding="utf-8"))
    assert report["schema_version"] == "nirs4all.qualification-report.v1"
    assert report["overall_disposition"] == "passed"


def test_wrong_artifact_digest_is_refused_without_execution(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    value["components"][1]["artifacts"][0]["sha256"] = "0" * 64
    path = _write_json(tmp_path / "bad-digest-manifest.json", value)

    report = run_qualification(path.resolve())
    core = _component(report, "core_rust")
    assert core["disposition"] == "refused"
    assert core["executed"] is False
    assert core["reason"] == "artifact 'result' SHA-256 mismatch"


def test_wrong_executable_digest_is_refused_without_execution(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    value["components"][2]["executable"]["sha256"] = "0" * 64
    path = _write_json(tmp_path / "bad-executable-digest-manifest.json", value)

    report = run_qualification(path.resolve())
    studio = _component(report, "studio_sidecar")
    assert studio["disposition"] == "refused"
    assert studio["executed"] is False
    assert studio["reason"] == "executable SHA-256 mismatch"


def test_wrong_scenario_digest_refuses_every_component_without_execution(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    value["scenario"]["definition"]["sha256"] = "0" * 64
    path = _write_json(tmp_path / "bad-scenario-manifest.json", value)

    report = run_qualification(path.resolve())
    assert report["overall_disposition"] == "refused"
    assert all(item["executed"] is False for item in report["components"])
    assert all("SHA-256 mismatch" in item["reason"] for item in report["components"])


def test_unfinished_output_is_failed(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    output_path = Path(value["components"][2]["artifacts"][0]["path"])
    _write_json(output_path, _result("studio_sidecar", completed=False))
    value["components"][2]["artifacts"][0]["sha256"] = _sha256(output_path)
    path = _write_json(tmp_path / "unfinished-manifest.json", value)

    report = run_qualification(path.resolve())
    studio = _component(report, "studio_sidecar")
    assert studio["disposition"] == "failed"
    assert studio["executed"] is True
    assert studio["reason"] == "adapter output is not finalized"


def test_executed_but_incomparable_output_is_refused(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    output_path = Path(value["components"][2]["artifacts"][0]["path"])
    _write_json(
        output_path,
        _result(
            "studio_sidecar",
            completed=False,
            qualification_refusal_reason="selected product result omits comparable observations",
        ),
    )
    value["components"][2]["artifacts"][0]["sha256"] = _sha256(output_path)
    path = _write_json(tmp_path / "incomparable-manifest.json", value)

    report = run_qualification(path.resolve())
    studio = _component(report, "studio_sidecar")
    assert studio["disposition"] == "refused"
    assert studio["executed"] is True
    assert studio["reason"] == "selected product result omits comparable observations"
    assert studio["metrics"]["rmse"] == 0.125


def test_declared_fallback_is_refused(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    output_path = Path(value["components"][3]["artifacts"][0]["path"])
    _write_json(output_path, _result("web_wasm", fallback_used=True))
    value["components"][3]["artifacts"][0]["sha256"] = _sha256(output_path)
    path = _write_json(tmp_path / "fallback-manifest.json", value)

    report = run_qualification(path.resolve())
    web = _component(report, "web_wasm")
    assert web["disposition"] == "refused"
    assert web["executed"] is True
    assert web["reason"] == "adapter declared fallback use"


def test_missing_artifact_is_refused_without_execution(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    missing = tmp_path / "not-produced.json"
    value["components"][1]["artifacts"][0] = {
        "role": "result",
        "path": str(missing),
        "sha256": "0" * 64,
    }
    path = _write_json(tmp_path / "missing-manifest.json", value)

    report = run_qualification(path.resolve())
    core = _component(report, "core_rust")
    assert core["disposition"] == "refused"
    assert core["executed"] is False
    assert core["reason"] == "artifact 'result' file is missing"


def test_required_but_unimplemented_component_is_never_skipped(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    value["components"][3] = {
        "component": "web_wasm",
        "adapter": "required",
        "refusal_reason": "selected Web/WASM launcher has not been packaged",
    }
    path = _write_json(tmp_path / "required-manifest.json", value)

    report = run_qualification(path.resolve())
    web = _component(report, "web_wasm")
    assert web == {
        "component": "web_wasm",
        "disposition": "refused",
        "executed": False,
        "reason": "required adapter unavailable: selected Web/WASM launcher has not been packaged",
        "version": None,
        "commit_sha": None,
        "executable": None,
        "artifacts": [],
        "metrics": {},
        "timings_ms": {},
        "observation_sha256": None,
        "observation_values": 0,
    }
    assert "skip" not in json.dumps(report).lower()


def test_relative_paths_and_missing_component_are_contract_errors(tmp_path: Path):
    _, value = _manifest(tmp_path)
    value["scenario"]["definition"]["path"] = "scenario.json"
    value["components"].pop()

    with pytest.raises(ValidationError):
        QualificationManifest.model_validate(value)


def test_numerical_mismatch_fails_candidate(tmp_path: Path):
    manifest, value = _manifest(tmp_path)
    del manifest
    output_path = Path(value["components"][1]["artifacts"][0]["path"])
    output = _result("core_rust")
    output["observations"]["predictions"][1] = 2.5
    _write_json(output_path, output)
    value["components"][1]["artifacts"][0]["sha256"] = _sha256(output_path)
    path = _write_json(tmp_path / "mismatch-manifest.json", value)

    report = run_qualification(path.resolve())
    core = _component(report, "core_rust")
    assert core["disposition"] == "failed"
    assert core["metrics"]["comparison.outside_tolerance"] == 1.0
