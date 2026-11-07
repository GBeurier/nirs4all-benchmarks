"""Run focused Python Arena checks with isolated stores, one thread and a disk guard."""

from __future__ import annotations

import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DISK_FLOOR = 2_684_354_560
DISK_RESERVE = 64 * 1024 * 1024
SCRATCH_LIMIT = 1_000_000_000
TESTS = (
    "tests/test_arena_work_review.py", "tests/test_site_build.py", "tests/test_offline_arena.py", "tests/test_contract.py", "tests/test_identity.py",
    "tests/test_store.py", "tests/test_ingestion.py", "tests/test_adapters.py",
    "tests/test_queries.py", "tests/test_upload_indexing.py", "tests/test_fixtures_regression.py",
    "tests/test_scoring.py", "tests/test_repository_bridge.py", "tests/test_service_api.py",
    "tests/test_history.py",
)
TARGETED_TESTS = (
    "tests/test_workspace_evidence_context.py",
    "tests/test_workspace_partitions.py",
    "tests/test_arena_fold_coverage.py",
    "tests/test_arena_targeted_recheck.py", "tests/test_contract.py", "tests/test_identity.py",
    "tests/test_store.py", "tests/test_ingestion.py", "tests/test_adapters.py", "tests/test_queries.py",
    "tests/test_upload_indexing.py", "tests/test_fixtures_regression.py",
    "tests/test_arena_work_review.py::test_pseudo_refit_test_and_nonfold_score_ids_cannot_fill_a_gap",
    "tests/test_arena_work_review.py::test_explicit_expectations_must_agree_with_cv",
    "tests/test_arena_work_review.py::test_more_observed_folds_than_declared_is_also_a_contradiction",
    "tests/test_arena_work_review.py::test_repeated_cv_and_per_metric_per_partition_coverage",
)


def scratch_bytes(root: Path) -> int:
    """Measure only this scratch, without links or changing fixture atimes."""
    import stat
    total = 0
    pending = [root]
    while pending:
        parent = pending.pop()
        try:
            fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                         | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NOATIME", 0))
        except FileNotFoundError:
            continue
        try:
            for name in os.listdir(fd):
                try:
                    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                total += max(info.st_size, getattr(info, "st_blocks", 0) * 512)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(parent/name)
        finally:
            os.close(fd)
    return total


def main() -> int:
    if shutil.disk_usage(ROOT).free <= DISK_FLOOR + DISK_RESERVE:
        print(f"Refused: free disk is within the reserve above the {DISK_FLOOR}-byte floor.")
        return 2
    with tempfile.TemporaryDirectory(prefix="benchmarks-offline-check-") as directory:
        temp = Path(directory)
        env = {**os.environ, **dict.fromkeys((
            "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS", "OMP_THREAD_LIMIT",
        ), "1"), "PYTHONDONTWRITEBYTECODE": "1", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTHONPATH": str(ROOT / "src"), "TMPDIR": directory, "TEMP": directory, "TMP": directory,
            "XDG_CACHE_HOME": str(temp / "cache"), "MYPY_CACHE_DIR": str(temp / "mypy"),
            "RUFF_CACHE_DIR": str(temp / "ruff"), "NIRS4ALL_BENCHMARKS_STORE": str(temp / "unused-store")}
        if sys.argv[1:] == ["--v2-core"]:
            command = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp="+str(temp/"pytest"), "tests/test_arena_v2.py", "--show-capture=all"]
        elif sys.argv[1:] == ["--v2-regressions"]:
            command = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp="+str(temp/"pytest"), "tests/test_arena_v2.py", "tests/test_identity.py", "tests/test_scoring.py"]
        elif sys.argv[1:] == ["--v2-small-history"]:
            command = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp="+str(temp/"pytest"), "tests/test_arena_v2_history.py", "-k", "not full_pack"]
        elif sys.argv[1:] == ["--v2-history"]:
            command = [sys.executable, "-m", "pytest", "-s", "-p", "no:cacheprovider", "--basetemp="+str(temp/"pytest"), "tests/test_arena_v2_history.py"]
        elif sys.argv[1:] == ["--v2-history-bootstrap"]:
            command = [sys.executable, "-B", str(ROOT/"scripts/check_arena_v2_history.py")]
        elif sys.argv[1:] == ["--production-probes"]:
            command = [sys.executable, str(ROOT / "scripts" / "check_arena_work_probes.py")]
        elif sys.argv[1:] == ["--targeted-reviewer-probe"]:
            command = [sys.executable, str(ROOT / "scripts" / "check_arena_rework_probe.py")]
        elif len(sys.argv) == 3 and sys.argv[1] == "--targeted-fold-source":
            command = [sys.executable, str(ROOT / "scripts" / "check_arena_rework_probe.py"), "--folds", sys.argv[2]]
        elif len(sys.argv) == 3 and sys.argv[1] == "--workspace-fold-source":
            command = [sys.executable, str(ROOT / "scripts" / "check_arena_rework_probe.py"),
                       "--workspace-folds", sys.argv[2]]
        elif sys.argv[1:] == ["--targeted-recheck"]:
            command = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp=" + str(temp / "pytest"),
                       *TARGETED_TESTS]
        elif sys.argv[1:] == ["--fold-reviewer-probe"]:
            command = [sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider",
                       "--basetemp=" + str(temp / "pytest"), "tests/test_arena_fold_coverage.py"]
        elif sys.argv[1:] == ["--owner-partition-probe"]:
            command = [sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider",
                       "--basetemp=" + str(temp / "pytest"), "tests/test_workspace_partitions.py"]
        elif sys.argv[1:] == ["--evidence-context-probe"]:
            command = [sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider",
                       "--basetemp=" + str(temp / "pytest"), "tests/test_workspace_evidence_context.py"]
        elif sys.argv[1:] in (["--ruff"], ["--ruff", "--fix"]):
            command = [str(Path(sys.executable).parent / "ruff"), "check", "src", "tests", "--output-format=concise", *sys.argv[2:]]
        elif sys.argv[1:] == ["--mypy"]:
            command = [str(Path(sys.executable).parent / "mypy")]
        elif sys.argv[1:]:
            raise SystemExit("Supported options: --production-probes, --targeted-recheck, "
                             "--targeted-reviewer-probe, --targeted-fold-source PATH, --workspace-fold-source PATH, "
                             "--fold-reviewer-probe, --owner-partition-probe, --evidence-context-probe, --ruff, --mypy")
        else:
            command = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp=" + str(temp / "pytest"),
                       *TESTS, "-k", "not test_real_charts_recover_from_saved_demo_filters"]
        print("Command:", " ".join(command), flush=True)
        minimum_free = shutil.disk_usage(ROOT).free
        maximum_scratch = 0
        process = subprocess.Popen(command, cwd=ROOT, env=env, start_new_session=True)

        def stop():
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=10)
        try:
            while True:
                minimum_free = min(minimum_free, shutil.disk_usage(ROOT).free, shutil.disk_usage(temp).free)
                maximum_scratch = max(maximum_scratch, scratch_bytes(temp))
                if minimum_free <= DISK_FLOOR + DISK_RESERVE:
                    if process.poll() is None:
                        stop()
                    print("Stopped at disk reserve before the configured free-byte floor.")
                    return 2
                if maximum_scratch >= SCRATCH_LIMIT - DISK_RESERVE:
                    if process.poll() is None:
                        stop()
                    print("Stopped at scratch reserve before the 1000000000-byte limit.")
                    return 2
                if process.poll() is not None:
                    break
                time.sleep(0.25)
            return process.returncode
        finally:
            if process.poll() is None:
                stop()
            print(f"Minimum observed free bytes: {minimum_free}", flush=True)
            print(f"Maximum observed scratch bytes: {maximum_scratch}; limit: {SCRATCH_LIMIT}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
