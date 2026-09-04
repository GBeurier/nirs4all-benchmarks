"""Bounded local process probe for SOAK-001 and PERF-002 evidence.

The runner measures the complete command process group on Linux ``/proc``.  It
is a reusable local diagnostic, not a release decision or a cross-platform gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PLAN_SCHEMA = "nirs4all.soak-probe.plan.v1"
REPORT_SCHEMA = "nirs4all.soak-probe.report.v1"
ROLES = {"workload", "integrity"}
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
DEFAULT_SAMPLE_INTERVAL_MS = 10
DEFAULT_MAX_OUTPUT_BYTES = 1024 * 1024


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _positive_int(value: Any, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        raise ValueError(f"{label} must be an integer between 1 and {maximum}")
    return value


def _validate_command(value: Any, label: str) -> dict[str, Any]:
    command = dict(_object(value, label))
    command_id = command.get("id")
    if not isinstance(command_id, str) or not ID_PATTERN.fullmatch(command_id):
        raise ValueError(f"{label}.id must be a lowercase stable identifier")
    role = command.get("role")
    if role not in ROLES:
        raise ValueError(f"{label}.role must be one of {sorted(ROLES)}")
    argv = command.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
        raise ValueError(f"{label}.argv must be a non-empty string array")
    if not (argv[0].startswith("/") or argv[0].startswith("{workspace_root}/") or argv[0].startswith("{plan_dir}/")):
        raise ValueError(f"{label}.argv[0] must be absolute or use a supported root placeholder")
    cwd = command.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        raise ValueError(f"{label}.cwd must be declared")
    expected = command.get("expected_exit_code")
    if isinstance(expected, bool) or not isinstance(expected, int):
        raise ValueError(f"{label}.expected_exit_code must be an integer")
    timeout = command.get("timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 86_400:
        raise ValueError(f"{label}.timeout_seconds must be in (0, 86400]")
    declared_environment = command.get("env", {})
    if not isinstance(declared_environment, Mapping) or not all(
        isinstance(name, str) and name and isinstance(item, str)
        for name, item in declared_environment.items()
    ):
        raise ValueError(f"{label}.env must be a string-to-string object")
    command["env"] = dict(declared_environment)
    return command


def load_plan(path: str | Path) -> dict[str, Any]:
    """Load and validate a bounded soak probe plan."""
    plan_path = Path(path)
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"), parse_constant=_reject_json_constant)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not load soak plan: {error}") from error
    root = dict(_object(plan, "soak plan"))
    if root.get("schema_version") != PLAN_SCHEMA:
        raise ValueError("unsupported soak plan schema")
    scope = root.get("scope")
    if not isinstance(scope, str) or not scope.strip():
        raise ValueError("scope must explicitly describe the local evidence boundary")
    root["sample_interval_ms"] = _positive_int(
        root.get("sample_interval_ms", DEFAULT_SAMPLE_INTERVAL_MS), "sample_interval_ms", 1_000
    )
    root["max_output_bytes"] = _positive_int(
        root.get("max_output_bytes", DEFAULT_MAX_OUTPUT_BYTES), "max_output_bytes", 16 * 1024 * 1024
    )
    scenarios = root.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("scenarios must be a non-empty array")
    scenario_ids: set[str] = set()
    validated_scenarios: list[dict[str, Any]] = []
    for index, raw_scenario in enumerate(scenarios):
        scenario = dict(_object(raw_scenario, f"scenarios[{index}]"))
        scenario_id = scenario.get("id")
        if not isinstance(scenario_id, str) or not ID_PATTERN.fullmatch(scenario_id):
            raise ValueError(f"scenarios[{index}].id must be a lowercase stable identifier")
        if scenario_id in scenario_ids:
            raise ValueError(f"duplicate scenario id: {scenario_id}")
        scenario_ids.add(scenario_id)
        scenario["repetitions"] = _positive_int(
            scenario.get("repetitions"), f"scenario {scenario_id} repetitions", 1_000
        )
        raw_commands = scenario.get("commands")
        if not isinstance(raw_commands, list) or not raw_commands:
            raise ValueError(f"scenario {scenario_id} commands must be a non-empty array")
        commands = [
            _validate_command(item, f"scenario {scenario_id} command {i}")
            for i, item in enumerate(raw_commands)
        ]
        command_ids = [command["id"] for command in commands]
        if len(command_ids) != len(set(command_ids)):
            raise ValueError(f"scenario {scenario_id} command ids must be unique")
        roles = [command["role"] for command in commands]
        if set(roles) != ROLES:
            raise ValueError(f"scenario {scenario_id} must declare workload and integrity commands")
        if roles != sorted(roles, key=lambda role: role == "integrity"):
            raise ValueError(f"scenario {scenario_id} integrity commands must follow workload commands")
        scenario["commands"] = commands
        validated_scenarios.append(scenario)
    root["scenarios"] = validated_scenarios
    return root


def _expand(value: str, *, workspace_root: Path, plan_dir: Path, repetition: int) -> str:
    expanded = (
        value.replace("{workspace_root}", str(workspace_root))
        .replace("{plan_dir}", str(plan_dir))
        .replace("{repetition}", str(repetition))
    )
    if "{" in expanded or "}" in expanded:
        raise ValueError(f"unsupported placeholder in {value!r}")
    return expanded


def _sample_process(pid: int) -> tuple[int, int] | None:
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
        rss_line = next(line for line in status.splitlines() if line.startswith("VmRSS:"))
        rss_bytes = int(rss_line.split()[1]) * 1024
        fd_count = len(list(Path(f"/proc/{pid}/fd").iterdir()))
        return rss_bytes, fd_count
    except (FileNotFoundError, PermissionError, StopIteration, ValueError):
        return None


def _process_group_id(pid: int) -> int | None:
    """Read one process group id without depending on an optional package."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        closing_parenthesis = stat.rfind(")")
        if closing_parenthesis < 0:
            return None
        fields_after_name = stat[closing_parenthesis + 2 :].split()
        return int(fields_after_name[2])
    except (FileNotFoundError, PermissionError, IndexError, ValueError):
        return None


def _sample_process_group(process_group_id: int) -> tuple[int, int, int] | None:
    """Aggregate RSS, descriptors and member count for a process group."""
    rss_bytes = fd_count = process_count = 0
    try:
        entries = list(Path("/proc").iterdir())
    except (FileNotFoundError, PermissionError):
        return None
    for entry in entries:
        if not entry.name.isdigit() or _process_group_id(int(entry.name)) != process_group_id:
            continue
        sample = _sample_process(int(entry.name))
        if sample is None:
            continue
        rss, fds = sample
        rss_bytes += rss
        fd_count += fds
        process_count += 1
    if process_count == 0:
        return None
    return rss_bytes, fd_count, process_count


def _kill_process_group(process_group_id: int) -> None:
    with suppress(ProcessLookupError):
        os.killpg(process_group_id, signal.SIGKILL)


def _read_output(handle: Any, maximum: int) -> tuple[int, str]:
    handle.seek(0)
    content = handle.read(maximum + 1)
    return len(content), _sha256(content)


def _run_command(
    command: Mapping[str, Any],
    *,
    workspace_root: Path,
    plan_dir: Path,
    repetition: int,
    sample_interval_ms: int,
    max_output_bytes: int,
) -> dict[str, Any]:
    declared_argv = list(command["argv"])
    argv = [
        _expand(item, workspace_root=workspace_root, plan_dir=plan_dir, repetition=repetition)
        for item in declared_argv
    ]
    cwd = Path(
        _expand(
            str(command["cwd"]),
            workspace_root=workspace_root,
            plan_dir=plan_dir,
            repetition=repetition,
        )
    )
    base = {
        "id": command["id"],
        "role": command["role"],
        "argv": declared_argv,
        "status": "failed",
        "exit_code": None,
        "timed_out": False,
        "latency_ms": 0.0,
        "peak_rss_bytes": None,
        "peak_fd_count": None,
        "peak_process_count": None,
        "lingering_process_count": 0,
        "stdout_bytes": 0,
        "stdout_sha256": None,
        "stderr_bytes": 0,
        "stderr_sha256": None,
        "failure": None,
    }
    executable = Path(argv[0])
    if not cwd.is_dir():
        base["failure"] = "declared working directory is unavailable"
        return base
    if not executable.is_file() or not os.access(executable, os.X_OK):
        base["failure"] = "declared executable is unavailable or not executable"
        return base

    inherited_names = ("HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TZ")
    environment = {name: os.environ[name] for name in inherited_names if name in os.environ}
    environment.update(
        {
            name: _expand(value, workspace_root=workspace_root, plan_dir=plan_dir, repetition=repetition)
            for name, value in command["env"].items()
        }
    )
    started = time.perf_counter_ns()
    peak_rss = peak_fds = peak_processes = None
    timed_out = output_exceeded = False
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        except OSError as error:
            base["failure"] = f"process could not start: {error}"
            return base
        deadline = started + int(float(command["timeout_seconds"]) * 1_000_000_000)
        while process.poll() is None:
            sample = _sample_process_group(process.pid)
            if sample is not None:
                rss, fds, processes = sample
                peak_rss = rss if peak_rss is None else max(peak_rss, rss)
                peak_fds = fds if peak_fds is None else max(peak_fds, fds)
                peak_processes = processes if peak_processes is None else max(peak_processes, processes)
            stdout_size = os.fstat(stdout.fileno()).st_size
            stderr_size = os.fstat(stderr.fileno()).st_size
            if stdout_size > max_output_bytes or stderr_size > max_output_bytes:
                output_exceeded = True
                _kill_process_group(process.pid)
                break
            if time.perf_counter_ns() >= deadline:
                timed_out = True
                _kill_process_group(process.pid)
                break
            time.sleep(sample_interval_ms / 1_000)
        process.wait()
        lingering = _sample_process_group(process.pid)
        lingering_processes = lingering[2] if lingering is not None else 0
        if lingering_processes:
            _kill_process_group(process.pid)
        latency_ms = (time.perf_counter_ns() - started) / 1_000_000
        stdout_bytes, stdout_sha = _read_output(stdout, max_output_bytes)
        stderr_bytes, stderr_sha = _read_output(stderr, max_output_bytes)

    base.update(
        exit_code=process.returncode,
        timed_out=timed_out,
        latency_ms=round(latency_ms, 3),
        peak_rss_bytes=peak_rss,
        peak_fd_count=peak_fds,
        peak_process_count=peak_processes,
        lingering_process_count=lingering_processes,
        stdout_bytes=stdout_bytes,
        stdout_sha256=stdout_sha,
        stderr_bytes=stderr_bytes,
        stderr_sha256=stderr_sha,
    )
    if timed_out:
        base["failure"] = "command timed out"
    elif output_exceeded or stdout_bytes > max_output_bytes or stderr_bytes > max_output_bytes:
        base["failure"] = "command output exceeded the declared bound"
    elif lingering_processes:
        base["failure"] = f"command left {lingering_processes} process-group member(s) running"
    elif process.returncode != command["expected_exit_code"]:
        base["failure"] = f"expected exit {command['expected_exit_code']}, observed {process.returncode}"
    elif command["role"] == "workload" and (
        peak_rss is None or peak_fds is None or peak_processes is None
    ):
        base["failure"] = "process-group RSS/FD measurement was unavailable"
    else:
        base["status"] = "passed"
    return base


def _summary(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    return {"values": values, "minimum": min(values), "median": round(median, 3), "maximum": max(values)}


def _normalized_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_plan(
    plan_path: str | Path, *, workspace_root: str | Path, generated_at: str | None = None
) -> dict[str, Any]:
    """Run one bounded plan, stopping at the first failed command."""
    plan_file = Path(plan_path).resolve()
    plan = load_plan(plan_file)
    workspace = Path(workspace_root).resolve()
    if not workspace.is_dir():
        raise ValueError("workspace_root must be an existing directory")
    if not Path("/proc/self/status").is_file():
        raise ValueError("Linux procfs is required for RSS and FD measurements")
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "generated_at": generated_at or _normalized_timestamp(),
        "scope": plan["scope"],
        "evidence_kind": "bounded_local_process_probe",
        "release_eligible": False,
        "release_holds": list(plan.get("release_holds", [])),
        "plan_sha256": _sha256(_canonical_json(plan)),
        "measurement_scope": {
            "platform": "linux-procfs",
            "process_group": True,
            "direct_process_only": False,
            "wsl": "microsoft" in os.uname().release.lower() or "WSL_INTEROP" in os.environ,
            "sample_interval_ms": plan["sample_interval_ms"],
        },
        "scenarios": [],
        "overall_status": "passed",
    }
    if "runtime_identity" in plan:
        report["runtime_identity"] = dict(plan["runtime_identity"])
    plan_failed = False
    for scenario in plan["scenarios"]:
        scenario_started = time.perf_counter_ns()
        repetitions: list[dict[str, Any]] = []
        workload_results: list[dict[str, Any]] = []
        integrity_status = "passed"
        for repetition_index in range(1, scenario["repetitions"] + 1):
            command_results: list[dict[str, Any]] = []
            for command in scenario["commands"]:
                result = _run_command(
                    command,
                    workspace_root=workspace,
                    plan_dir=plan_file.parent,
                    repetition=repetition_index,
                    sample_interval_ms=plan["sample_interval_ms"],
                    max_output_bytes=plan["max_output_bytes"],
                )
                command_results.append(result)
                if result["role"] == "workload":
                    workload_results.append(result)
                elif result["status"] != "passed":
                    integrity_status = "failed"
                if result["status"] != "passed":
                    plan_failed = True
                    break
            repetitions.append({"index": repetition_index, "commands": command_results})
            if plan_failed:
                break
        latencies: dict[str, list[float]] = {}
        rss_by_command: dict[str, list[float]] = {}
        fds_by_command: dict[str, list[float]] = {}
        processes_by_command: dict[str, list[float]] = {}
        for result in workload_results:
            command_id = result["id"]
            latencies.setdefault(command_id, []).append(result["latency_ms"])
            if result["peak_rss_bytes"] is not None:
                rss_by_command.setdefault(command_id, []).append(result["peak_rss_bytes"])
            if result["peak_fd_count"] is not None:
                fds_by_command.setdefault(command_id, []).append(result["peak_fd_count"])
            if result["peak_process_count"] is not None:
                processes_by_command.setdefault(command_id, []).append(result["peak_process_count"])
        measured_rss = [result["peak_rss_bytes"] for result in workload_results if result["peak_rss_bytes"] is not None]
        measured_fds = [result["peak_fd_count"] for result in workload_results if result["peak_fd_count"] is not None]
        measured_processes = [
            result["peak_process_count"]
            for result in workload_results
            if result["peak_process_count"] is not None
        ]
        scenario_report = {
            "id": scenario["id"],
            "status": "failed" if plan_failed else "passed",
            "integrity_status": integrity_status if not plan_failed or integrity_status == "failed" else "not_reached",
            "requested_repetitions": scenario["repetitions"],
            "completed_repetitions": len(repetitions),
            "duration_ms": round((time.perf_counter_ns() - scenario_started) / 1_000_000, 3),
            "latency_ms": {command_id: _summary(values) for command_id, values in latencies.items()},
            "resources_by_command": {
                command_id: {
                    "peak_rss_bytes": _summary(rss_by_command[command_id]),
                    "peak_fd_count": _summary(fds_by_command[command_id]),
                    "peak_process_count": _summary(processes_by_command[command_id]),
                }
                for command_id in latencies
                if command_id in rss_by_command
                and command_id in fds_by_command
                and command_id in processes_by_command
            },
            "peak_rss_bytes": max(measured_rss) if measured_rss else None,
            "peak_fd_count": max(measured_fds) if measured_fds else None,
            "peak_process_count": max(measured_processes) if measured_processes else None,
            "repetitions": repetitions,
        }
        report["scenarios"].append(scenario_report)
        if plan_failed:
            report["overall_status"] = "failed"
            break
    return report


def write_report(path: str | Path, report: Mapping[str, Any]) -> None:
    """Write stable, sorted JSON with a trailing newline."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True)
    destination.write_text(serialized + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--workspace-root", required=True, type=Path)
    parser.add_argument("--json-out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        report = run_plan(args.plan, workspace_root=args.workspace_root)
    except ValueError as error:
        parser.error(str(error))
    write_report(args.json_out, report)
    return 0 if report["overall_status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
