"""Targeted tests for the bounded local SOAK/PERF probe."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from nirs4all_benchmarks import soak_probe

REPO_ROOT = Path(__file__).resolve().parents[1]


def _plan(
    tmp_path: Path,
    *,
    integrity_exit: int = 0,
    add_late_command: bool = False,
    workload_code: str = "import time; time.sleep(0.05); print('ok')",
) -> Path:
    commands = [
        {
            "id": "workload",
            "role": "workload",
            "argv": [sys.executable, "-c", workload_code],
            "cwd": "{workspace_root}",
            "expected_exit_code": 0,
            "timeout_seconds": 2,
        },
        {
            "id": "integrity",
            "role": "integrity",
            "argv": [sys.executable, "-c", f"raise SystemExit({integrity_exit})"],
            "cwd": "{workspace_root}",
            "expected_exit_code": 0,
            "timeout_seconds": 2,
        },
    ]
    if add_late_command:
        commands.append(
            {
                "id": "late-integrity",
                "role": "integrity",
                "argv": [sys.executable, "-c", "print('must not run')"],
                "cwd": "{workspace_root}",
                "expected_exit_code": 0,
                "timeout_seconds": 2,
            }
        )
    payload = {
        "schema_version": soak_probe.PLAN_SCHEMA,
        "scope": "unit fixture; never release evidence",
        "sample_interval_ms": 5,
        "max_output_bytes": 4096,
        "release_holds": ["fixture_only"],
        "scenarios": [{"id": "fixture", "repetitions": 2, "commands": commands}],
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_probe_records_bounded_metrics_and_integrity(tmp_path: Path) -> None:
    report = soak_probe.run_plan(
        _plan(tmp_path), workspace_root=tmp_path, generated_at="2000-01-01T00:00:00Z"
    )

    scenario = report["scenarios"][0]
    assert report["overall_status"] == "passed"
    assert report["release_eligible"] is False
    assert report["generated_at"] == "2000-01-01T00:00:00Z"
    assert scenario["status"] == "passed"
    assert scenario["integrity_status"] == "passed"
    assert scenario["completed_repetitions"] == 2
    assert len(scenario["latency_ms"]["workload"]["values"]) == 2
    assert scenario["peak_rss_bytes"] > 0
    assert scenario["peak_fd_count"] > 0
    assert scenario["peak_process_count"] == 1
    assert scenario["resources_by_command"]["workload"]["peak_fd_count"]["values"]
    assert report["measurement_scope"]["process_group"] is True
    assert report["measurement_scope"]["direct_process_only"] is False
    result = scenario["repetitions"][0]["commands"][0]
    assert result["stdout_sha256"] == soak_probe._sha256(b"ok\n")
    assert result["failure"] is None


def test_probe_measures_workload_descendants_in_the_process_group(tmp_path: Path) -> None:
    child_code = "import time; time.sleep(0.15)"
    workload_code = (
        "import subprocess, sys, time; "
        f"child = subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
        "time.sleep(0.10); child.wait()"
    )

    report = soak_probe.run_plan(
        _plan(tmp_path, workload_code=workload_code),
        workspace_root=tmp_path,
        generated_at="2000-01-01T00:00:00Z",
    )

    result = report["scenarios"][0]["repetitions"][0]["commands"][0]
    assert result["status"] == "passed"
    assert result["peak_process_count"] >= 2
    assert result["lingering_process_count"] == 0


def test_probe_expands_repetition_in_declared_environment(tmp_path: Path) -> None:
    path = _plan(tmp_path, workload_code="import os; print(os.environ['SOAK_PASS'])")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["scenarios"][0]["commands"][0]["env"] = {"SOAK_PASS": "pass-{repetition}"}
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = soak_probe.run_plan(
        path, workspace_root=tmp_path, generated_at="2000-01-01T00:00:00Z"
    )

    commands = [item["commands"][0] for item in report["scenarios"][0]["repetitions"]]
    assert [item["stdout_sha256"] for item in commands] == [
        soak_probe._sha256(b"pass-1\n"),
        soak_probe._sha256(b"pass-2\n"),
    ]


def test_integrity_failure_stops_plan_before_later_commands(tmp_path: Path) -> None:
    report = soak_probe.run_plan(
        _plan(tmp_path, integrity_exit=7, add_late_command=True),
        workspace_root=tmp_path,
        generated_at="2000-01-01T00:00:00Z",
    )

    scenario = report["scenarios"][0]
    commands = scenario["repetitions"][0]["commands"]
    assert report["overall_status"] == "failed"
    assert scenario["integrity_status"] == "failed"
    assert [command["id"] for command in commands] == ["workload", "integrity"]
    assert commands[-1]["failure"] == "expected exit 0, observed 7"


def test_plan_refuses_missing_integrity_role(tmp_path: Path) -> None:
    path = _plan(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["scenarios"][0]["commands"] = payload["scenarios"][0]["commands"][:1]
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="workload and integrity"):
        soak_probe.load_plan(path)


def test_tracked_plan_is_bounded_and_keeps_release_holds() -> None:
    plan = soak_probe.load_plan(REPO_ROOT / "docs" / "soak-local" / "soak-plan.v1.json")

    assert plan["scenarios"][0]["repetitions"] == 3
    assert [command["role"] for command in plan["scenarios"][0]["commands"]] == ["workload", "integrity"]
    assert "representative_user_corpus_missing" in plan["release_holds"]
    assert "release_matrices_incomplete" in plan["release_holds"]


def test_r3_functional_plan_pins_python_and_retains_release_holds() -> None:
    plan = soak_probe.load_plan(REPO_ROOT / "docs" / "soak-local" / "soak-plan.r3-functional.v1.json")

    assert plan["runtime_identity"]["python_commit_sha"] == (
        "2af6cfd7f988fa400617c460a77450dbad4228c9"
    )
    assert plan["scenarios"][0]["repetitions"] == 3
    assert plan["scenarios"][1]["repetitions"] == 30
    assert plan["runtime_identity"]["python_distribution"] == "nirs4all==1.0.0rc2"
    assert "studio_final_identity_pending" in plan["release_holds"]
    assert "published_artifacts_pending" in plan["release_holds"]


def test_checked_in_current_head_probe_passes_without_closing_soak_gate() -> None:
    report = json.loads(
        (REPO_ROOT / "docs" / "soak-local" / "soak-report.current-head.v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert report["overall_status"] == "passed"
    assert report["release_eligible"] is False
    assert report["scenarios"][0]["completed_repetitions"] == 3
    assert report["scenarios"][0]["integrity_status"] == "passed"
    assert report["runtime_identity"]["commit_sha"] == (
        "86d5e5033d62240815e532038b6e769b14b25c2b"
    )
    assert report["runtime_identity"]["tree_sha"] == (
        "dc61df097434c38a8d2bdd9939d3057683fc7661"
    )
    assert "representative_user_corpus_missing" in report["release_holds"]
    assert "sustained_soak_not_run" in report["release_holds"]


def test_report_writer_is_sorted_and_stable(tmp_path: Path) -> None:
    path = tmp_path / "report.json"
    soak_probe.write_report(path, {"z": 1, "a": {"d": 2, "b": 1}})

    assert path.read_text(encoding="utf-8") == '{\n  "a": {\n    "b": 1,\n    "d": 2\n  },\n  "z": 1\n}\n'


def test_module_cli_writes_the_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"

    assert soak_probe.main(
        [
            "--plan",
            str(_plan(tmp_path)),
            "--workspace-root",
            str(tmp_path),
            "--json-out",
            str(report_path),
        ]
    ) == 0
    assert json.loads(report_path.read_text(encoding="utf-8"))["overall_status"] == "passed"
