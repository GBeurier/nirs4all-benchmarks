"""PERF-001 Archive V2 cross-surface contract tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from nirs4all_benchmarks import performance_compare as pc


def _fixture_plan(tmp_path: Path) -> tuple[Path, Path]:
    plan = pc.load_plan()
    archive = tmp_path / "fixture.n4a"
    archive.write_bytes(b"deterministic Archive V2 contract fixture")
    plan["workload"]["archive_v2"]["sha256"] = pc._sha256_file(archive)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    return path, archive


def _fixture_adapter(tmp_path: Path, *, divergent_surface: str | None = None) -> Path:
    script = tmp_path / "adapter"
    divergence = repr(divergent_surface)
    script.write_text(
        """#!/usr/bin/python3.11
import json, sys
request = json.load(sys.stdin)
predictions = [[1.6363636363636365, 13.272727272727273], [2.4999999999999996, 15.0]]
if request['surface'] == __DIVERGENCE__:
    predictions[0][0] += 0.25
json.dump({
    'protocol': request['protocol'],
    'surface': request['surface'],
    'commit_sha': request['candidate']['commit_sha'],
    'archive_sha256': request['archive_v2']['sha256'],
    'matrix_sha256': request['matrix_sha256'],
    'sample_ids': request['matrix']['sample_ids'],
    'target_names': request['matrix']['target_names'],
    'predictor_descriptor': request['predictor_descriptor'],
    'predictor_fingerprint': request['predictor_fingerprint'],
    'fallback_used': False,
    'predictions': predictions,
    'startup_ms': 4.0,
    'steady_state_ms': [1.25] * request['repeats'],
    'evidence': {'kind': 'deterministic-contract-fixture'},
}, sys.stdout, sort_keys=True)
""".replace("__DIVERGENCE__", divergence),
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _all_adapters(executable: Path) -> dict[str, Path]:
    return dict.fromkeys(pc.SURFACES, executable)


def test_frozen_plan_pins_delivered_candidates_and_native_predictor() -> None:
    plan = pc.load_plan()

    assert {name: value["commit_sha"] for name, value in plan["candidates"].items()} == {
        "methods": "9d4e2753836eb85d61b1e7712ec6a06e627d4a5f",
        "dag_ml": "ce0a1963077612b3ce2604746e77a6405d0c3002",
        "formats": "2d46285843dc366da1d38f133131b5329c886b12",
        "io": "e41bf8f94a92356e98c215d4c41e907a7dfaf6ac",
        "core": "e0f5d485eae4279f02d58fe82fad3946202e463f",
        "python": "e227244464983ea2a94ebc01b6af30d474a025df",
        "studio": "e254a1ebba578e5b1932d09079088d02eb51d411",
        "web": "59cfffbd7444a9afa9527fbaa12d078811abaf33",
    }
    assert plan["workload"]["archive_v2"]["sha256"] == (
        "994252030ff80129d0431995bae53eb473082f05825b65714379262b72af13fa"
    )
    assert plan["workload"]["sample_ids"] == ["predict.0", "predict.1"]
    assert plan["workload"]["x"] == [[1.5, 0.5], [3.5, 1.5]]
    assert plan["predictor_descriptor"]["dimensions"] == {
        "training_samples": 6,
        "n_features": 2,
        "n_targets": 2,
        "n_components": 1,
    }
    assert plan["predictor_fingerprint"] == (
        "c130231adf7468c6682747e8b1c32d960a6da8ba3b05fb388a2c396392f6ca6b"
    )
    assert "engine='native'" in plan["candidates"]["python"]["selection_note"]


def test_comparison_uses_same_archive_matrix_and_descriptor_for_all_surfaces(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)
    adapter = _fixture_adapter(tmp_path)

    report = pc.run_comparison(
        plan_path=plan,
        workspace_root=tmp_path,
        archive=archive,
        adapters=_all_adapters(adapter),
        repeats=2,
        evidence_kind="contract_fixture",
    )

    assert report["overall_disposition"] == "passed"
    assert report["release_eligible"] is False
    assert report["performance_policy"]["thresholds"] is None
    assert report["performance_policy"]["startup_and_steady_state_separate"] is True
    assert report["historical_compatibility"]["studio_python_worker"] == "excluded_from_v1"
    for surface, result in report["surfaces"].items():
        assert result["surface"] == surface
        assert result["disposition"] == "passed"
        assert result["fallback_used"] is False
        assert result["predictor_fingerprint"] == report["predictor_fingerprint"]
        assert result["timings_ms"]["startup"] == 4.0
        assert result["timings_ms"]["steady_state"] == [1.25, 1.25]
        assert result["numeric_consistency"]["outside_tolerance"] == 0
        assert result["numeric_consistency"]["passed"] is True


def test_missing_surface_artifacts_are_visible_refusals(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)

    report = pc.run_comparison(
        plan_path=plan,
        workspace_root=tmp_path,
        archive=archive,
        adapters={},
        repeats=1,
    )

    assert report["overall_disposition"] == "refused"
    assert all(value["disposition"] == "refused" for value in report["surfaces"].values())
    assert all("not supplied" in value["reason"] for value in report["surfaces"].values())


def test_numeric_divergence_fails_surface_without_a_performance_threshold(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)
    adapter = _fixture_adapter(tmp_path, divergent_surface="web_wasm")

    report = pc.run_comparison(
        plan_path=plan,
        workspace_root=tmp_path,
        archive=archive,
        adapters=_all_adapters(adapter),
        repeats=1,
        evidence_kind="contract_fixture",
    )

    assert report["overall_disposition"] == "failed"
    assert report["surfaces"]["web_wasm"]["numeric_consistency"]["outside_tolerance"] == 1
    assert report["surfaces"]["web_wasm"]["disposition"] == "failed"
    assert report["performance_policy"]["thresholds"] is None


def test_adapter_identity_drift_is_rejected(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)
    adapter = _fixture_adapter(tmp_path)
    original = adapter.read_text(encoding="utf-8")
    adapter.write_text(original.replace("request['candidate']['commit_sha']", "'0' * 40"), encoding="utf-8")

    report = pc.run_comparison(
        plan_path=plan,
        workspace_root=tmp_path,
        archive=archive,
        adapters=_all_adapters(adapter),
        repeats=1,
        evidence_kind="contract_fixture",
    )

    assert report["overall_disposition"] == "failed"
    assert all("commit_sha mismatch" in value["reason"] for value in report["surfaces"].values())


def test_web_handoff_contains_archive_matrix_predictor_and_web_candidate(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)
    adapter = _fixture_adapter(tmp_path)
    report = pc.run_comparison(
        plan_path=plan,
        workspace_root=tmp_path,
        archive=archive,
        adapters=_all_adapters(adapter),
        repeats=1,
        evidence_kind="contract_fixture",
    )

    handoff = pc.write_web_handoff(tmp_path, report)
    payload = json.loads(handoff.read_text(encoding="utf-8"))

    assert handoff.parent.name == "performance-compare"
    assert payload["schema_version"] == pc.WEB_HANDOFF_SCHEMA
    assert payload["archive_v2"]["actual_sha256"] == pc._sha256_file(archive)
    assert payload["matrix"]["sample_ids"] == ["predict.0", "predict.1"]
    assert payload["predictor_fingerprint"] == report["predictor_fingerprint"]
    assert payload["web_candidate"]["commit_sha"] == "59cfffbd7444a9afa9527fbaa12d078811abaf33"
    assert payload["release_eligible"] is False
    assert "performance_budgets_not_frozen" in payload["release_eligibility_holds"]
    assert "release_matrices_incomplete" in payload["release_eligibility_holds"]
    assert (handoff.parent / "performance-report.v1.json").is_file()


def test_historical_reports_remain_renderable_but_are_not_executable() -> None:
    historical: dict[str, Any] = {
        "suites": {
            "studio_run": {
                "engines": {
                    "legacy": {"run_s_median": 2.0},
                    "dag-ml": {"run_s_median": 1.0},
                }
            }
        }
    }

    markdown = pc.render_markdown(historical)

    assert "read-only compatibility" in markdown
    assert "studio_run" in markdown
    assert "legacy" in markdown


def test_parse_adapter_overrides_requires_explicit_absolute_paths(tmp_path: Path) -> None:
    adapter = _fixture_adapter(tmp_path)

    assert pc.parse_adapter_overrides([f"web_wasm={adapter}"]) == {"web_wasm": adapter}
    with pytest.raises(ValueError, match="absolute"):
        pc.parse_adapter_overrides(["web_wasm=relative-adapter"])
    with pytest.raises(ValueError, match="expected one of"):
        pc.parse_adapter_overrides([f"studio_python={adapter}"])


def test_module_smoke_writes_one_repeat_report_and_web_handoff(tmp_path: Path) -> None:
    plan, archive = _fixture_plan(tmp_path)
    adapter = _fixture_adapter(tmp_path)
    report_path = tmp_path / "report.json"
    args = [
        "--plan", str(plan), "--workspace-root", str(tmp_path), "--archive", str(archive),
        "--repeats", "1", "--evidence-kind", "contract_fixture", "--json-out", str(report_path),
        "--handoff-dir", str(tmp_path),
    ]
    for surface in pc.SURFACES:
        args += ["--adapter", f"{surface}={adapter}"]

    assert pc.main(args) == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["overall_disposition"] == "passed"
    assert all(len(value["timings_ms"]["steady_state"]) == 1 for value in report["surfaces"].values())
    assert (tmp_path / "performance-compare" / "archive-v2-performance-compare.v1.json").is_file()
