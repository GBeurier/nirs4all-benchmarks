"""Compare one Archive V2 across the delivered Python, Rust, Studio and Web surfaces.

This package owns orchestration and evidence only. Product code is reached by
explicit stdio adapters; no runtime checkout is imported and the retired Studio
Python worker is never used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAN_PATH = REPO_ROOT / "docs" / "performance-compare.handoff.v1.json"
REPORT_SCHEMA = "nirs4all.performance-compare.report.v1"
ADAPTER_PROTOCOL = "nirs4all.performance-compare.adapter.v1"
WEB_HANDOFF_SCHEMA = "nirs4all.performance-compare.web-handoff.v1"
SURFACES = ("python_oracle", "rust_direct", "studio_rust", "web_wasm")
MAX_ADAPTER_OUTPUT_BYTES = 2 * 1024 * 1024
THREAD_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _digest(value: Any, label: str, length: int = 64) -> str:
    if not isinstance(value, str) or len(value) != length or any(c not in "0123456789abcdef" for c in value):
        raise ValueError(f"{label} must be a lowercase {length}-character hexadecimal digest")
    return value


def _matrix(value: Any, label: str) -> list[list[float]]:
    if not isinstance(value, list) or not value or not all(isinstance(row, list) and row for row in value):
        raise ValueError(f"{label} must be a non-empty matrix")
    width = len(value[0])
    if any(len(row) != width for row in value):
        raise ValueError(f"{label} must not be ragged")
    result = [[float(item) for item in row] for row in value]
    if any(not math.isfinite(item) for row in result for item in row):
        raise ValueError(f"{label} must contain only finite values")
    return result


def load_plan(path: str | Path = DEFAULT_PLAN_PATH) -> dict[str, Any]:
    """Load the frozen PERF-001 workload and exact candidate identities."""
    plan = json.loads(Path(path).read_text(encoding="utf-8"))
    root = _object(plan, "performance plan")
    if root.get("schema_version") != "nirs4all.performance-compare.plan.v1":
        raise ValueError("unsupported performance plan schema")
    candidates = _object(root.get("candidates"), "candidates")
    required = {"methods", "dag_ml", "formats", "io", "core", "python", "studio", "web"}
    if set(candidates) != required:
        raise ValueError("performance plan must pin every delivered candidate exactly once")
    for name, candidate in candidates.items():
        item = _object(candidate, f"candidate {name}")
        _digest(item.get("commit_sha"), f"candidate {name} commit", 40)
        _digest(item.get("tree_sha"), f"candidate {name} tree", 40)
    workload = _object(root.get("workload"), "workload")
    archive = _object(workload.get("archive_v2"), "workload.archive_v2")
    _digest(archive.get("sha256"), "Archive V2 SHA-256")
    x = _matrix(workload.get("x"), "workload.x")
    sample_ids, targets = workload.get("sample_ids"), workload.get("target_names")
    if not isinstance(sample_ids, list) or len(sample_ids) != len(x) or len(set(sample_ids)) != len(sample_ids):
        raise ValueError("sample_ids must be unique and align with workload.x")
    if not isinstance(targets, list) or not targets or len(set(targets)) != len(targets):
        raise ValueError("target_names must be a non-empty unique list")
    expected = _matrix(workload.get("expected_native_predictions"), "expected predictions")
    if len(expected) != len(x) or any(len(row) != len(targets) for row in expected):
        raise ValueError("expected predictions must align with samples and targets")
    descriptor = _object(root.get("predictor_descriptor"), "predictor_descriptor")
    fingerprint = _digest(descriptor.get("descriptor_fingerprint"), "descriptor fingerprint")
    if fingerprint != root.get("predictor_fingerprint"):
        raise ValueError("predictor_fingerprint must repeat the authoritative descriptor fingerprint")
    if set(_object(root.get("surfaces"), "surfaces")) != set(SURFACES):
        raise ValueError("plan must bind every performance surface")
    return dict(plan)


def resolve_archive(
    plan: Mapping[str, Any], *, workspace_root: str | Path, archive: str | Path | None = None
) -> Path:
    """Resolve the one byte-identical Archive V2 without discovery or conversion."""
    if archive is not None:
        return Path(archive).expanduser().resolve()
    source = _object(plan["workload"]["archive_v2"].get("source"), "Archive V2 source")
    return (Path(workspace_root) / str(source["worktree"]) / str(source["relative_path"])).resolve()


def parse_adapter_overrides(values: Iterable[str]) -> dict[str, Path]:
    """Parse repeated ``SURFACE=/absolute/executable`` declarations."""
    adapters: dict[str, Path] = {}
    for raw in values:
        surface, separator, executable = raw.partition("=")
        if not separator or surface not in SURFACES or not executable:
            raise ValueError(f"expected one of {SURFACES} as SURFACE=/absolute/executable, got {raw!r}")
        path = Path(executable).expanduser()
        if not path.is_absolute():
            raise ValueError(f"adapter path for {surface} must be absolute")
        adapters[surface] = path
    return adapters


def _adapter_environment() -> dict[str, str]:
    allowed = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT", "WINDIR")
    environment = {name: os.environ[name] for name in allowed if name in os.environ}
    environment.update(dict.fromkeys(THREAD_ENV_VARS, "1"))
    environment["N4A_PERFORMANCE_PROTOCOL"] = ADAPTER_PROTOCOL
    return environment


def _base_surface(plan: Mapping[str, Any], surface: str, executable: Path | None) -> dict[str, Any]:
    candidate_name = plan["surfaces"][surface]["candidate"]
    candidate = plan["candidates"][candidate_name]
    return {
        "surface": surface,
        "candidate": candidate_name,
        "commit_sha": candidate["commit_sha"],
        "tree_sha": candidate["tree_sha"],
        "adapter_path": str(executable) if executable else None,
        "adapter_sha256": _sha256_file(executable) if executable and executable.is_file() else None,
        "disposition": "refused",
        "reason": None,
        "executed": False,
        "fallback_used": None,
        "predictor_fingerprint": None,
        "predictions": None,
        "observation_sha256": None,
        "timings_ms": {"startup": None, "steady_state": [], "process_wall": None},
        "numeric_consistency": None,
    }


def _request(plan: Mapping[str, Any], surface: str, archive: Path, repeats: int) -> dict[str, Any]:
    workload = plan["workload"]
    candidate = plan["candidates"][plan["surfaces"][surface]["candidate"]]
    matrix = {
        "sample_ids": workload["sample_ids"],
        "x": workload["x"],
        "target_names": workload["target_names"],
    }
    return {
        "protocol": ADAPTER_PROTOCOL,
        "surface": surface,
        "candidate": candidate,
        "archive_v2": {"path": str(archive), "sha256": workload["archive_v2"]["sha256"]},
        "matrix": matrix,
        "matrix_sha256": _canonical_sha256(matrix),
        "predictor_descriptor": plan["predictor_descriptor"],
        "predictor_fingerprint": plan["predictor_fingerprint"],
        "repeats": repeats,
    }


def _validated_output(output: Any, request: Mapping[str, Any]) -> dict[str, Any]:
    value = _object(output, "adapter output")
    exact = {
        "protocol": ADAPTER_PROTOCOL,
        "surface": request["surface"],
        "commit_sha": request["candidate"]["commit_sha"],
        "archive_sha256": request["archive_v2"]["sha256"],
        "matrix_sha256": request["matrix_sha256"],
        "sample_ids": request["matrix"]["sample_ids"],
        "target_names": request["matrix"]["target_names"],
        "predictor_descriptor": request["predictor_descriptor"],
        "predictor_fingerprint": request["predictor_fingerprint"],
        "fallback_used": False,
    }
    for key, expected in exact.items():
        if value.get(key) != expected:
            raise ValueError(f"adapter {key} mismatch")
    predictions = _matrix(value.get("predictions"), "adapter predictions")
    if len(predictions) != len(request["matrix"]["sample_ids"]) or any(
        len(row) != len(request["matrix"]["target_names"]) for row in predictions
    ):
        raise ValueError("adapter predictions do not align with the shared matrix")
    startup = float(value.get("startup_ms"))
    steady = value.get("steady_state_ms")
    if not math.isfinite(startup) or startup < 0:
        raise ValueError("adapter startup_ms must be finite and non-negative")
    if not isinstance(steady, list) or len(steady) != request["repeats"]:
        raise ValueError("adapter must report one steady-state timing per repeat")
    steady_values = [float(item) for item in steady]
    if any(not math.isfinite(item) or item < 0 for item in steady_values):
        raise ValueError("adapter steady-state timings must be finite and non-negative")
    return {"predictions": predictions, "startup": startup, "steady": steady_values, "evidence": value.get("evidence", {})}


def _run_adapter(
    plan: Mapping[str, Any], surface: str, executable: Path | None, archive: Path, repeats: int, timeout: float
) -> dict[str, Any]:
    report = _base_surface(plan, surface, executable)
    if executable is None:
        report["reason"] = "required surface adapter was not supplied"
        return report
    if not executable.is_file() or not os.access(executable, os.X_OK):
        report["reason"] = "surface adapter is missing or not executable"
        return report
    request = _request(plan, surface, archive, repeats)
    started = time.perf_counter()
    try:
        process = subprocess.run(
            [str(executable)], input=json.dumps(request), capture_output=True, text=True,
            env=_adapter_environment(), timeout=timeout, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        report.update(disposition="failed", reason=f"adapter could not complete: {error}")
        return report
    report["executed"] = True
    report["timings_ms"]["process_wall"] = (time.perf_counter() - started) * 1000
    if len(process.stdout.encode()) > MAX_ADAPTER_OUTPUT_BYTES:
        report.update(disposition="failed", reason="adapter output exceeded 2 MiB")
        return report
    if process.returncode != 0:
        report.update(disposition="failed", reason=f"adapter exited {process.returncode}: {process.stderr.strip()[:500]}")
        return report
    try:
        output = _validated_output(json.loads(process.stdout), request)
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        report.update(disposition="failed", reason=f"invalid adapter evidence: {error}")
        return report
    predictions = output["predictions"]
    report.update(
        disposition="passed", executed=True, fallback_used=False, reason=None,
        predictor_fingerprint=plan["predictor_fingerprint"], predictions=predictions,
        observation_sha256=_canonical_sha256(predictions), adapter_evidence=output["evidence"],
    )
    report["timings_ms"].update(
        startup=output["startup"], steady_state=output["steady"],
        steady_state_median=float(statistics.median(output["steady"])),
    )
    return report


def _compare(expected: list[list[float]], actual: list[list[float]], absolute: float, relative: float) -> dict[str, Any]:
    max_absolute = max_relative = 0.0
    outside = values = 0
    for expected_row, actual_row in zip(expected, actual, strict=True):
        for reference, candidate in zip(expected_row, actual_row, strict=True):
            error = abs(candidate - reference)
            max_absolute = max(max_absolute, error)
            max_relative = max(max_relative, error / max(abs(reference), 1e-300))
            outside += not math.isclose(candidate, reference, abs_tol=absolute, rel_tol=relative)
            values += 1
    return {
        "reference": "python_oracle", "values": values,
        "max_absolute_error": max_absolute, "max_relative_error": max_relative,
        "outside_tolerance": int(outside), "passed": outside == 0,
    }


def _is_wsl() -> bool:
    release = platform.release().lower()
    return "microsoft" in release or "wsl" in release or "WSL_INTEROP" in os.environ


def run_comparison(
    *, plan_path: str | Path = DEFAULT_PLAN_PATH, workspace_root: str | Path,
    adapters: Mapping[str, Path] | None = None, archive: str | Path | None = None,
    repeats: int = 3, timeout_seconds: float = 120.0, evidence_kind: str = "local_real",
) -> dict[str, Any]:
    """Execute available adapters and compare every result to the Python oracle."""
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    if evidence_kind not in {"local_real", "contract_fixture"}:
        raise ValueError("evidence_kind must be local_real or contract_fixture")
    plan = load_plan(plan_path)
    archive_path = resolve_archive(plan, workspace_root=workspace_root, archive=archive)
    expected_sha = plan["workload"]["archive_v2"]["sha256"]
    actual_sha = _sha256_file(archive_path) if archive_path.is_file() else None
    archive_error = None if actual_sha == expected_sha else (
        "Archive V2 witness is missing" if actual_sha is None else "Archive V2 witness SHA-256 mismatch"
    )
    paths = adapters or {}
    reports: dict[str, dict[str, Any]] = {}
    for surface in SURFACES:
        if archive_error:
            report = _base_surface(plan, surface, paths.get(surface))
            report["reason"] = archive_error
        else:
            report = _run_adapter(plan, surface, paths.get(surface), archive_path, repeats, timeout_seconds)
        reports[surface] = report
    tolerance = plan["numeric_tolerance"]
    oracle = reports["python_oracle"]
    if oracle["disposition"] == "passed":
        oracle_consistency = _compare(
            plan["workload"]["expected_native_predictions"],
            oracle["predictions"],
            float(tolerance["absolute"]),
            float(tolerance["relative"]),
        )
        oracle_consistency["reference"] = "frozen_archive_v2_witness"
        oracle["numeric_consistency"] = oracle_consistency
        if not oracle_consistency["passed"]:
            oracle.update(disposition="failed", reason="Python oracle differs from the frozen Archive V2 witness")
        for surface in SURFACES[1:]:
            report = reports[surface]
            if report["disposition"] != "passed":
                continue
            if oracle["disposition"] != "passed":
                report.update(disposition="refused", reason="Python oracle did not produce a comparison baseline")
                continue
            comparison = _compare(
                oracle["predictions"], report["predictions"],
                float(tolerance["absolute"]), float(tolerance["relative"]),
            )
            report["numeric_consistency"] = comparison
            if not comparison["passed"]:
                report.update(disposition="failed", reason="prediction values differ from the Python oracle")
    else:
        for surface in SURFACES[1:]:
            if reports[surface]["disposition"] == "passed":
                reports[surface].update(disposition="refused", reason="Python oracle did not produce a comparison baseline")
    dispositions = {item["disposition"] for item in reports.values()}
    overall = "failed" if "failed" in dispositions else "refused" if "refused" in dispositions else "passed"
    wsl = _is_wsl()
    release_holds = []
    if evidence_kind != "local_real":
        release_holds.append("non_product_fixture_evidence")
    if overall != "passed":
        release_holds.append("comparison_not_passed")
    if wsl:
        release_holds.append("wsl_measurement_host")
    release_holds.extend(("performance_budgets_not_frozen", "release_matrices_incomplete"))
    workload = plan["workload"]
    matrix = {"sample_ids": workload["sample_ids"], "x": workload["x"], "target_names": workload["target_names"]}
    return {
        "schema_version": REPORT_SCHEMA, "scenario_id": plan["scenario_id"],
        "evidence_kind": evidence_kind,
        "release_eligible": not release_holds,
        "release_eligibility_holds": release_holds,
        "overall_disposition": overall,
        "archive_v2": {"path": str(archive_path), "expected_sha256": expected_sha, "actual_sha256": actual_sha},
        "matrix": {**matrix, "sha256": _canonical_sha256(matrix)},
        "predictor_descriptor": plan["predictor_descriptor"],
        "predictor_fingerprint": plan["predictor_fingerprint"],
        "candidates": plan["candidates"], "numeric_tolerance": tolerance,
        "performance_policy": {
            "platform": platform.platform(), "wsl": wsl,
            "startup_and_steady_state_separate": True, "thresholds": None,
            "verdict": "record_only_under_wsl" if wsl else "budgets_not_frozen",
        },
        "historical_compatibility": {
            "legacy_vs_dag_ml_execution": "excluded_from_v1",
            "studio_python_worker": "excluded_from_v1",
            "historical_report_rendering": "supported",
        },
        "surfaces": reports,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render current evidence and retain read-only rendering of old reports."""
    if report.get("schema_version") != REPORT_SCHEMA and "suites" in report:
        lines = ["Historical legacy/dag-ml report (read-only compatibility)", "", "| suite | engine | run (s) |", "|---|---|---:|"]
        for suite, value in report["suites"].items():
            for engine, summary in value.get("engines", {}).items():
                run = "ERROR" if "error" in summary else f"{float(summary['run_s_median']):.4f}"
                lines.append(f"| {suite} | {engine} | {run} |")
        return "\n".join(lines)
    lines = [
        f"PERF-001 `{report['overall_disposition']}` — {report['performance_policy']['verdict']}", "",
        "| surface | disposition | startup (ms) | steady median (ms) | max abs delta | predictor |",
        "|---|---|---:|---:|---:|---|",
    ]
    for surface in SURFACES:
        item, timings = report["surfaces"][surface], report["surfaces"][surface]["timings_ms"]
        startup = "n/a" if timings["startup"] is None else f"{float(timings['startup']):.3f}"
        steady = "n/a" if timings.get("steady_state_median") is None else f"{float(timings['steady_state_median']):.3f}"
        delta = (item.get("numeric_consistency") or {}).get("max_absolute_error")
        lines.append(
            f"| {surface} | {item['disposition']} | {startup} | {steady} | "
            f"{'n/a' if delta is None else f'{float(delta):.3g}'} | {(item.get('predictor_fingerprint') or '')[:12]} |"
        )
    lines += ["", "Startup and steady-state are separate. No definitive performance threshold is applied."]
    return "\n".join(lines)


def write_web_handoff(directory: str | Path, report: Mapping[str, Any]) -> Path:
    """Write the deterministic ``performance-compare`` Web handoff."""
    root = Path(directory)
    if root.name != "performance-compare":
        root /= "performance-compare"
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": WEB_HANDOFF_SCHEMA, "scenario_id": report["scenario_id"],
        "archive_v2": report["archive_v2"], "matrix": report["matrix"],
        "predictor_descriptor": report["predictor_descriptor"],
        "predictor_fingerprint": report["predictor_fingerprint"],
        "python_oracle_predictions": report["surfaces"]["python_oracle"].get("predictions"),
        "numeric_tolerance": report["numeric_tolerance"], "web_candidate": report["candidates"]["web"],
        "web_surface": report["surfaces"]["web_wasm"],
        "performance_policy": report["performance_policy"], "release_eligible": report["release_eligible"],
        "release_eligibility_holds": report["release_eligibility_holds"],
    }
    handoff = root / "archive-v2-performance-compare.v1.json"
    handoff.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (root / "performance-report.v1.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return handoff


def _default_workspace_root() -> Path:
    return REPO_ROOT.parent.parent if REPO_ROOT.parent.name == "_worktrees" else REPO_ROOT.parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN_PATH)
    parser.add_argument("--workspace-root", type=Path, default=_default_workspace_root())
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--adapter", action="append", default=[], metavar="SURFACE=/ABSOLUTE/EXECUTABLE")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--evidence-kind", choices=("local_real", "contract_fixture"), default="local_real")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument("--handoff-dir", type=Path)
    args = parser.parse_args(argv)
    try:
        report = run_comparison(
            plan_path=args.plan, workspace_root=args.workspace_root,
            adapters=parse_adapter_overrides(args.adapter), archive=args.archive,
            repeats=args.repeats, timeout_seconds=args.timeout, evidence_kind=args.evidence_kind,
        )
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    markdown = render_markdown(report)
    print(markdown)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown + "\n", encoding="utf-8")
    if args.handoff_dir:
        write_web_handoff(args.handoff_dir, report)
    return 0 if report["overall_disposition"] != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
