"""Execute the four runtime adapters and produce an evidence-complete report."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from nirs4all_benchmarks.process_guard import OutputLimitExceeded, run_bounded
from nirs4all_benchmarks.qualification.contract import (
    COMPONENTS,
    QUALIFICATION_REPORT_SCHEMA_VERSION,
    AdapterRequest,
    ComponentOutput,
    ExecutableComponent,
    FileReference,
    QualificationManifest,
    QualificationReport,
    RequiredComponent,
)

MAX_STDIO_BYTES = 2 * 1024 * 1024


def sha256_file(path: Path) -> str:
    """Return the SHA-256 of one regular file without resolving siblings."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: str | Path) -> QualificationManifest:
    """Load a strict qualification manifest from an explicit path."""
    manifest_path = Path(path)
    if not manifest_path.is_absolute():
        raise ValueError("qualification manifest path must be absolute")
    return QualificationManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))


def _canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _file_evidence(reference: FileReference) -> tuple[dict[str, Any], str | None]:
    evidence: dict[str, Any] = {
        "path": str(reference.path),
        "expected_sha256": reference.sha256,
        "actual_sha256": None,
    }
    if not reference.path.exists():
        return evidence, "file is missing"
    if not reference.path.is_file():
        return evidence, "path is not a regular file"
    try:
        actual = sha256_file(reference.path)
    except OSError as exc:
        return evidence, f"file cannot be read: {exc}"
    evidence["actual_sha256"] = actual
    if actual != reference.sha256:
        return evidence, "SHA-256 mismatch"
    return evidence, None


def _base_component_report(spec: ExecutableComponent | RequiredComponent) -> dict[str, Any]:
    return {
        "component": spec.component,
        "disposition": "refused",
        "executed": False,
        "reason": None,
        "version": getattr(spec, "version", None),
        "commit_sha": getattr(spec, "commit_sha", None),
        "executable": None,
        "artifacts": [],
        "metrics": {},
        "timings_ms": {},
        "observation_sha256": None,
        "observation_values": 0,
    }


def _preflight(spec: ExecutableComponent) -> tuple[dict[str, Any], str | None]:
    report = _base_component_report(spec)
    errors: list[str] = []
    executable, error = _file_evidence(spec.executable)
    report["executable"] = executable
    if error is not None:
        errors.append(f"executable {error}")
    elif not os.access(spec.executable.path, os.X_OK):
        errors.append("executable path is not executable")

    for artifact in spec.artifacts:
        evidence, artifact_error = _file_evidence(artifact)
        evidence["role"] = artifact.role
        report["artifacts"].append(evidence)
        if artifact_error is not None:
            errors.append(f"artifact {artifact.role!r} {artifact_error}")
    return report, "; ".join(errors) if errors else None


def _adapter_environment() -> dict[str, str]:
    """Return a small environment with no Python/source-tree injection paths."""
    allowed = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT", "WINDIR")
    environment = {name: os.environ[name] for name in allowed if name in os.environ}
    environment["N4A_QUALIFICATION_PROTOCOL"] = "nirs4all.qualification-adapter.v1"
    return environment


def _execute(
    spec: ExecutableComponent,
    manifest: QualificationManifest,
    report: dict[str, Any],
) -> tuple[dict[str, Any], ComponentOutput | None]:
    request = AdapterRequest(
        component=spec.component,
        declared_version=spec.version,
        declared_commit_sha=spec.commit_sha,
        scenario=manifest.scenario,
        tolerances=manifest.tolerances,
        artifacts=spec.artifacts,
    )
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix="n4a-qualification-") as cwd:
            process = run_bounded(
                [str(spec.executable.path)],
                input_text=request.model_dump_json(),
                cwd=cwd,
                env=_adapter_environment(),
                timeout=spec.timeout_seconds,
                max_output_bytes=MAX_STDIO_BYTES,
            )
    except subprocess.TimeoutExpired:
        report.update(disposition="failed", executed=True, reason="adapter timed out")
        report["timings_ms"]["process_wall"] = (time.perf_counter() - started) * 1000.0
        return report, None
    except OutputLimitExceeded:
        report.update(disposition="failed", executed=True, reason="adapter output exceeded the 2 MiB limit")
        return report, None
    except OSError as exc:
        report.update(disposition="failed", executed=False, reason=f"adapter could not start: {exc}")
        return report, None

    wall_ms = (time.perf_counter() - started) * 1000.0
    report["executed"] = True
    report["timings_ms"]["process_wall"] = wall_ms
    if len(process.stdout.encode("utf-8")) > MAX_STDIO_BYTES or len(process.stderr.encode("utf-8")) > MAX_STDIO_BYTES:
        report.update(disposition="failed", reason="adapter output exceeded the 2 MiB limit")
        return report, None
    if process.returncode != 0:
        stderr = process.stderr.strip()
        report.update(
            disposition="failed",
            reason=f"adapter exited with code {process.returncode}" + (f": {stderr[:500]}" if stderr else ""),
        )
        return report, None
    try:
        output = ComponentOutput.model_validate_json(process.stdout)
    except ValidationError as exc:
        report.update(
            disposition="failed",
            reason=f"adapter emitted invalid or unfinished JSON: {exc.errors()[0]['msg']}",
        )
        return report, None

    provenance_mismatch = None
    if output.component != spec.component:
        provenance_mismatch = "component identity mismatch"
    elif output.scenario_id != manifest.scenario.scenario_id:
        provenance_mismatch = "scenario identity mismatch"
    elif output.version != spec.version:
        provenance_mismatch = "version mismatch"
    elif output.commit_sha != spec.commit_sha:
        provenance_mismatch = "commit SHA mismatch"
    if provenance_mismatch:
        report.update(disposition="refused", reason=provenance_mismatch)
        return report, None
    report["metrics"].update(output.metrics)
    report["timings_ms"].update(output.timings_ms)
    report["timings_ms"]["process_wall"] = wall_ms
    if output.observations:
        report["observation_sha256"] = _canonical_digest(output.observations)
        report["observation_values"] = sum(len(values) for values in output.observations.values())
    if output.qualification_refusal_reason is not None:
        report.update(disposition="refused", reason=output.qualification_refusal_reason)
        return report, None
    if not output.completed:
        report.update(disposition="failed", reason="adapter output is not finalized")
        return report, None
    if output.fallback_used:
        report.update(disposition="refused", reason="adapter declared fallback use")
        return report, None

    report.update(disposition="passed", reason=None)
    return report, output


def _compare(
    oracle: ComponentOutput,
    candidate: ComponentOutput,
    *,
    absolute: float,
    relative: float,
) -> tuple[dict[str, float], str | None]:
    if set(candidate.observations) != set(oracle.observations):
        return {}, "observation series differ from the Python oracle"
    max_absolute = 0.0
    max_relative = 0.0
    count = 0
    outside = 0
    for name, expected_values in oracle.observations.items():
        actual_values = candidate.observations[name]
        if len(actual_values) != len(expected_values):
            return {}, f"observation length differs for series {name!r}"
        for expected, actual in zip(expected_values, actual_values, strict=True):
            absolute_error = abs(actual - expected)
            relative_error = absolute_error / max(abs(expected), 1e-300)
            max_absolute = max(max_absolute, absolute_error)
            max_relative = max(max_relative, relative_error)
            count += 1
            if not math.isclose(actual, expected, abs_tol=absolute, rel_tol=relative):
                outside += 1
    metrics = {
        "comparison.max_absolute_error": max_absolute,
        "comparison.max_relative_error": max_relative,
        "comparison.values": float(count),
        "comparison.outside_tolerance": float(outside),
    }
    if outside:
        return metrics, f"{outside} observation value(s) exceed declared tolerances"
    return metrics, None


def run_qualification(manifest_path: str | Path) -> dict[str, Any]:
    """Run all declared adapters and return the complete JSON-ready report."""
    path = Path(manifest_path)
    manifest = load_manifest(path)
    manifest_evidence, manifest_error = _file_evidence(FileReference(path=path, sha256=sha256_file(path)))
    assert manifest_error is None
    scenario_evidence, scenario_error = _file_evidence(manifest.scenario.definition)
    scenario_definition: Any = None
    if scenario_error is None:
        try:
            scenario_definition = json.loads(manifest.scenario.definition.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            scenario_error = f"scenario definition is not valid JSON: {exc}"

    reports: dict[str, dict[str, Any]] = {}
    outputs: dict[str, ComponentOutput] = {}
    ordered_specs = {spec.component: spec for spec in manifest.components}
    for name in COMPONENTS:
        spec = ordered_specs[name]
        report = _base_component_report(spec)
        if isinstance(spec, RequiredComponent):
            report["reason"] = f"required adapter unavailable: {spec.refusal_reason}"
        else:
            report, error = _preflight(spec)
            if scenario_error is not None:
                report["reason"] = f"scenario refused: {scenario_error}" + (f"; {error}" if error else "")
            elif error is not None:
                report["reason"] = error
            else:
                report, output = _execute(spec, manifest, report)
                if output is not None:
                    outputs[name] = output
        reports[name] = report

    oracle = outputs.get("python_oracle") if reports["python_oracle"]["disposition"] == "passed" else None
    if oracle is None:
        for name in COMPONENTS[1:]:
            if reports[name]["disposition"] == "passed":
                reports[name].update(disposition="refused", reason="Python oracle did not produce a comparable result")
    else:
        reports["python_oracle"]["metrics"].update(
            {
                "comparison.max_absolute_error": 0.0,
                "comparison.max_relative_error": 0.0,
                "comparison.values": float(reports["python_oracle"]["observation_values"]),
                "comparison.outside_tolerance": 0.0,
            }
        )
        for name in COMPONENTS[1:]:
            output = outputs.get(name)
            if output is None or reports[name]["disposition"] != "passed":
                continue
            metrics, error = _compare(
                oracle,
                output,
                absolute=manifest.tolerances.absolute,
                relative=manifest.tolerances.relative,
            )
            reports[name]["metrics"].update(metrics)
            if error is not None:
                reports[name].update(disposition="failed", reason=error)

    dispositions = {report["disposition"] for report in reports.values()}
    overall = "failed" if "failed" in dispositions else "refused" if "refused" in dispositions else "passed"
    qualification_report = QualificationReport.model_validate(
        {
            "schema_version": QUALIFICATION_REPORT_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "manifest": manifest_evidence,
            "scenario": {
                "scenario_id": manifest.scenario.scenario_id,
                **scenario_evidence,
                "definition": scenario_definition,
            },
            "tolerances": manifest.tolerances.model_dump(mode="json"),
            "overall_disposition": overall,
            "components": [reports[name] for name in COMPONENTS],
        }
    )
    return qualification_report.model_dump(mode="json")
