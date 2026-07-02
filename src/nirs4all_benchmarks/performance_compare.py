"""Deterministic cross-engine performance comparison for RC-v1 worktrees.

The harness is intentionally small and CI-friendly:

* one seeded synthetic dataset and one supported PLS pipeline;
* fresh subprocess per (suite, engine, repeat) measurement;
* medians over a tiny repeat count;
* explicit `legacy` vs `dag-ml` selection with fallback disabled.

It compares two execution surfaces:

* `python_run`: the public `nirs4all.run()` API;
* `studio_run`: Studio's real training worker path
  (`api.runs._execute_pipeline_training`) with only the workspace-bound seams
  stubbed so the scientific execution stays real.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SUITES: tuple[str, ...] = ("python_run", "studio_run")
DEFAULT_ENGINES: tuple[str, ...] = ("legacy", "dag-ml")
THREAD_ENV_VARS: tuple[str, ...] = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)
REPORT_SENTINEL = "@@N4A_PERF@@"


def _workspace_root() -> Path:
    explicit = os.environ.get("N4A_BENCH_WORKSPACE_ROOT")
    if explicit:
        return Path(explicit).resolve()

    if REPO_ROOT.parent.name == "_worktrees":
        return REPO_ROOT.parent.parent.resolve()
    return REPO_ROOT.parent.resolve()


def _rc_paths() -> dict[str, Path]:
    workspace = _workspace_root()
    return {
        "workspace_root": workspace,
        "benchmarks_root": REPO_ROOT,
        "studio_root": Path(
            os.environ.get(
                "N4A_BENCH_STUDIO_ROOT",
                workspace / "_worktrees" / "RC-v1-studio",
            )
        ).resolve(),
        "dagml_python_root": Path(
            os.environ.get(
                "N4A_BENCH_DAGML_PY_ROOT",
                workspace / "_worktrees" / "RC-v1-dagml" / "crates" / "dag-ml-py" / "python",
            )
        ).resolve(),
        "dagml_data_python_root": Path(
            os.environ.get(
                "N4A_BENCH_DAGML_DATA_PY_ROOT",
                workspace / "_worktrees" / "RC-v1-dmd" / "crates" / "dag-ml-data-py" / "python",
            )
        ).resolve(),
    }


def _candidate_nirs4all_roots() -> list[Path]:
    workspace = _workspace_root()
    candidates = [
        os.environ.get("N4A_BENCH_NIRS4ALL_ROOT"),
        workspace / "_worktrees" / "RC-v1-nirs4all-python",
        workspace / "nirs4all",
    ]
    roots: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().absolute()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(path)
    return roots


def _interpreter_candidates() -> list[Path]:
    workspace = _workspace_root()
    candidates = [
        os.environ.get("N4A_BENCH_CHILD_PYTHON"),
        workspace / "nirs4all-studio" / ".venv" / "bin" / "python",
        workspace / "nirs4all" / ".venv" / "bin" / "python",
        workspace / "nirs4all-benchmarks" / ".venv" / "bin" / "python",
        sys.executable,
        shutil.which("python3"),
    ]
    paths: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().absolute()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        paths.append(path)
    return paths


def _probe_source(require_studio: bool) -> str:
    paths = _rc_paths()
    bootstrap = "\n".join(
        f"sys.path.insert(0, {str(path)!r})"
        for path in (
            paths["studio_root"],
            paths["dagml_data_python_root"],
            paths["dagml_python_root"],
        )
    )
    if require_studio:
        body = "import api.runs\n"
    else:
        body = "import numpy\n"
    template = """
    import sys
    __BOOTSTRAP__
    __BODY__
    """
    return (
        textwrap.dedent(template)
        .replace("__BOOTSTRAP__", bootstrap.rstrip())
        .replace("__BODY__", body.rstrip())
    )


def choose_child_python(
    explicit: str | Path | None = None,
    *,
    require_studio: bool = True,
) -> Path:
    candidates = [Path(explicit).expanduser().absolute()] if explicit else _interpreter_candidates()
    probe = _probe_source(require_studio=require_studio)

    for candidate in candidates:
        if not candidate.exists():
            continue
        proc = subprocess.run(
            [str(candidate), "-c", probe],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        if proc.returncode == 0:
            return candidate

    rendered = "\n".join(f"- {candidate}" for candidate in candidates)
    missing = "nirs4all + Studio" if require_studio else "nirs4all"
    raise RuntimeError(
        "could not find a Python interpreter that can import "
        f"{missing} from the RC worktrees.\nTried:\n{rendered}"
    )


def _nirs4all_probe_source(nirs4all_root: Path) -> str:
    paths = _rc_paths()
    bootstrap = "\n".join(
        f"sys.path.insert(0, {str(path)!r})"
        for path in (
            paths["studio_root"],
            paths["dagml_data_python_root"],
            paths["dagml_python_root"],
            nirs4all_root,
        )
    )
    template = """
    import sys
    __BOOTSTRAP__
    import numpy as np
    import nirs4all
    from sklearn.cross_decomposition import PLSRegression
    from sklearn.model_selection import ShuffleSplit
    from sklearn.preprocessing import MinMaxScaler

    rng = np.random.default_rng(2026)
    X = rng.normal(0.5, 0.1, size=(20, 10)).astype(float)
    y = X[:, :3].sum(axis=1) + rng.normal(0, 0.05, size=20)
    pipeline = [
        MinMaxScaler(),
        ShuffleSplit(n_splits=2, test_size=0.25, random_state=0),
        {"model": PLSRegression(n_components=2)},
    ]
    result = nirs4all.run(
        pipeline=pipeline,
        dataset=(X, y),
        verbose=0,
        save_artifacts=False,
        save_charts=False,
        plots_visible=False,
        engine="dag-ml",
    )
    close = getattr(result, "close", None)
    if callable(close):
        close()
    """
    return textwrap.dedent(template).replace("__BOOTSTRAP__", bootstrap.rstrip())


def choose_nirs4all_root(python: Path) -> Path:
    candidates = _candidate_nirs4all_roots()
    for candidate in candidates:
        if not candidate.exists():
            continue
        with tempfile.TemporaryDirectory(prefix="n4a-perf-probe-") as tmpdir:
            proc = subprocess.run(
                [str(python), "-c", _nirs4all_probe_source(candidate)],
                capture_output=True,
                text=True,
                cwd=tmpdir,
            )
        if proc.returncode == 0:
            return candidate

    rendered = "\n".join(f"- {candidate}" for candidate in candidates)
    raise RuntimeError(
        "could not find a usable nirs4all source root for the dag-ml comparison.\n"
        f"Tried:\n{rendered}"
    )


def _thread_env() -> dict[str, str]:
    return {name: "1" for name in THREAD_ENV_VARS}


def _child_source(nirs4all_root: Path) -> str:
    paths = _rc_paths()
    bootstrap = "\n".join(
        f"sys.path.insert(0, {str(path)!r})"
        for path in (
            paths["studio_root"],
            paths["dagml_data_python_root"],
            paths["dagml_python_root"],
            nirs4all_root,
        )
    )
    template = f"""
    import json
    import math
    import sys
    import time
    from types import SimpleNamespace

    SUITE = sys.argv[1]
    ENGINE = sys.argv[2]
    WARMUPS = int(sys.argv[3])

    __BOOTSTRAP__

    def _build_case():
        import numpy as np
        from sklearn.cross_decomposition import PLSRegression
        from sklearn.model_selection import ShuffleSplit
        from sklearn.preprocessing import MinMaxScaler

        rng = np.random.default_rng(2026)
        X = rng.normal(0.5, 0.1, size=(80, 50)).astype(float)
        y = X[:, :5].sum(axis=1) + rng.normal(0, 0.05, size=80)
        pipeline = [
            MinMaxScaler(),
            ShuffleSplit(n_splits=2, test_size=0.25, random_state=0),
            {{"model": PLSRegression(n_components=3)}},
        ]
        return pipeline, (X, y)

    def _manifest_engine(result):
        to_rt = getattr(result, "to_rt_result", None)
        if not callable(to_rt):
            return None
        try:
            rt_result = to_rt()
        except Exception:
            return None
        if isinstance(rt_result, dict):
            manifest = rt_result.get("manifest")
            return manifest.get("engine") if isinstance(manifest, dict) else None
        manifest = getattr(rt_result, "manifest", None)
        if isinstance(manifest, dict):
            return manifest.get("engine")
        return getattr(manifest, "engine", None)

    def _finite_or_none(value):
        if value is None:
            return None
        value = float(value)
        return value if math.isfinite(value) else None

    def _python_once(engine):
        import nirs4all

        pipeline, dataset = _build_case()
        result = nirs4all.run(
            pipeline=pipeline,
            dataset=dataset,
            verbose=0,
            random_state=0,
            save_artifacts=False,
            save_charts=False,
            plots_visible=False,
            engine=engine,
            allow_fallback=False,
        )
        try:
            recorded = _manifest_engine(result) or engine
            if recorded != engine:
                raise RuntimeError(f"requested {{engine}} but recorded {{recorded}}")
            best_score = getattr(result, "best_score", None)
            return {{
                "engine_recorded": recorded,
                "best_score": _finite_or_none(best_score),
                "num_predictions": int(getattr(result, "num_predictions", 0)),
            }}
        finally:
            close = getattr(result, "close", None)
            if callable(close):
                close()

    def _studio_once(engine):
        import api.pipelines as pipelines_api
        import api.spectra as spectra_api

        runtime_pipeline, dataset = _build_case()
        spectra_api._load_dataset = lambda _dataset_id: SimpleNamespace(name=_dataset_id)
        pipelines_api.prepare_pipeline_steps_with_runtime_grouping = (
            lambda steps, _dataset, _group_by: SimpleNamespace(warnings=[], steps=steps)
        )
        pipelines_api.editor_steps_to_runtime_canonical = lambda _steps: runtime_pipeline
        pipelines_api.count_runtime_variants = lambda _steps: 1

        job = SimpleNamespace(
            config={{
                "pipeline_id": "bench-pipeline",
                "pipeline_name": "bench-pipeline",
                "pipeline_steps": [{{"id": "model"}}],
                "dataset_id": "bench-dataset",
                "dataset_path": dataset,
                "workspace_path": None,
                "verbose": 0,
                "export_model": False,
                "engine": engine,
                "allow_fallback": False,
            }}
        )
        payload = pipelines_api._run_pipeline_task(job, lambda *_args, **_kwargs: True)
        recorded = payload.get("engine")
        if recorded != engine:
            raise RuntimeError(f"requested {{engine}} but recorded {{recorded}}")
        metrics = payload.get("metrics") or {{}}
        score = metrics.get("score")
        return {{
            "engine_recorded": recorded,
            "best_score": _finite_or_none(score),
            "num_predictions": None,
            "variants_tested": int(payload.get("variants_tested", 0)),
            "runtime_source": payload.get("runtime_source"),
        }}

    t0 = time.perf_counter()
    if SUITE == "python_run":
        runner = _python_once
    elif SUITE == "studio_run":
        runner = _studio_once
    else:
        raise SystemExit(f"unknown suite: {{SUITE}}")
    import_s = time.perf_counter() - t0

    for _ in range(WARMUPS):
        runner(ENGINE)

    t1 = time.perf_counter()
    payload = runner(ENGINE)
    run_s = time.perf_counter() - t1
    payload["import_s"] = import_s
    payload["run_s"] = run_s
    print({REPORT_SENTINEL!r} + json.dumps(payload, sort_keys=True))
    """
    return (
        textwrap.dedent(template)
        .replace("__BOOTSTRAP__", bootstrap.rstrip())
    )


def _run_child(
    *,
    suite: str,
    engine: str,
    python: Path,
    nirs4all_root: Path,
    warmups: int,
) -> dict[str, Any]:
    env = dict(os.environ)
    env.update(_thread_env())
    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="n4a-perf-run-") as tmpdir:
        proc = subprocess.run(
            [str(python), "-c", _child_source(nirs4all_root), suite, engine, str(warmups)],
            capture_output=True,
            text=True,
            env=env,
            cwd=tmpdir,
        )
    total_s = time.perf_counter() - t0

    if proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip() or "child failed without output"
        return {"error": stderr, "total_s": total_s}

    for line in proc.stdout.splitlines():
        if line.startswith(REPORT_SENTINEL):
            payload = json.loads(line[len(REPORT_SENTINEL):])
            payload["total_s"] = total_s
            return payload
    return {"error": "child produced no report sentinel", "total_s": total_s}


def _median(values: Iterable[float]) -> float:
    return float(statistics.median(values))


def _summarize_runs(engine: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    errors = [run["error"] for run in runs if "error" in run]
    if errors:
        return {"engine": engine, "error": errors[0], "runs": runs}

    summary = {
        "engine": engine,
        "engine_recorded": runs[0].get("engine_recorded"),
        "run_s_median": _median(float(run["run_s"]) for run in runs),
        "import_s_median": _median(float(run["import_s"]) for run in runs),
        "total_s_median": _median(float(run["total_s"]) for run in runs),
        "best_score": runs[0].get("best_score"),
        "num_predictions": runs[0].get("num_predictions"),
        "variants_tested": runs[0].get("variants_tested"),
        "runtime_source": runs[0].get("runtime_source"),
        "runs": runs,
    }
    return summary


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _suite_label(name: str) -> str:
    labels = {
        "python_run": "nirs4all.run() direct",
        "studio_run": "Studio training worker",
    }
    return labels.get(name, name)


def run_comparison(
    *,
    suites: Iterable[str] = DEFAULT_SUITES,
    repeats: int = 3,
    warmups: int = 0,
    child_python: str | Path | None = None,
    max_ratios: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    suite_list = tuple(suites)
    invalid = sorted(set(suite_list) - set(DEFAULT_SUITES))
    if invalid:
        raise ValueError(f"unknown suites: {invalid}")
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    if warmups < 0:
        raise ValueError("warmups must be >= 0")

    python = choose_child_python(child_python, require_studio="studio_run" in suite_list)
    nirs4all_root = choose_nirs4all_root(python)
    report: dict[str, Any] = {
        "case": {
            "name": "seeded_small_pls_cv",
            "seed": 2026,
            "samples": 80,
            "features": 50,
            "cv_splits": 2,
            "pipeline": "MinMaxScaler -> ShuffleSplit(2) -> PLSRegression(n_components=3)",
        },
        "environment": {
            "child_python": str(python),
            "nirs4all_root": str(nirs4all_root),
            "workspace_root": str(_workspace_root()),
            "thread_env": _thread_env(),
            "repeats": repeats,
            "warmups": warmups,
        },
        "suites": {},
    }

    for suite in suite_list:
        engines: dict[str, Any] = {}
        for engine in DEFAULT_ENGINES:
            runs = [
                _run_child(
                    suite=suite,
                    engine=engine,
                    python=python,
                    nirs4all_root=nirs4all_root,
                    warmups=warmups,
                )
                for _ in range(repeats)
            ]
            engines[engine] = _summarize_runs(engine, runs)

        ratios: dict[str, Any] = {}
        legacy = engines["legacy"]
        dagml = engines["dag-ml"]
        if "error" not in legacy and "error" not in dagml:
            ratios["run_s_dag_ml_over_legacy"] = _ratio(
                float(dagml["run_s_median"]),
                float(legacy["run_s_median"]),
            )
            ratios["total_s_dag_ml_over_legacy"] = _ratio(
                float(dagml["total_s_median"]),
                float(legacy["total_s_median"]),
            )
        report["suites"][suite] = {
            "label": _suite_label(suite),
            "engines": engines,
            "ratios": ratios,
        }

    if max_ratios:
        failures: list[str] = []
        for suite, limit in max_ratios.items():
            actual = (
                report["suites"]
                .get(suite, {})
                .get("ratios", {})
                .get("run_s_dag_ml_over_legacy")
            )
            if actual is None:
                failures.append(f"{suite}: ratio unavailable")
            elif float(actual) > float(limit):
                failures.append(f"{suite}: {actual:.3f} > {limit:.3f}")
        if failures:
            raise RuntimeError("performance ratio gate failed: " + "; ".join(failures))

    return report


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "| suite | engine | run median (s) | total median (s) | import median (s) | recorded engine | score |",
        "|---|---|---|---|---|---|---|",
    ]
    for suite_name, suite in report["suites"].items():
        label = suite.get("label", suite_name)
        engines = suite.get("engines", {})
        for engine in DEFAULT_ENGINES:
            summary = engines.get(engine, {})
            if "error" in summary:
                lines.append(f"| {label} | {engine} | ERROR | | | | {summary['error']} |")
                continue
            score = summary.get("best_score")
            score_text = "" if score is None else f"{float(score):.6f}"
            lines.append(
                "| "
                f"{label} | {engine} | {summary['run_s_median']:.4f} | "
                f"{summary['total_s_median']:.4f} | {summary['import_s_median']:.4f} | "
                f"{summary.get('engine_recorded') or ''} | {score_text} |"
            )

    lines.extend(
        [
            "",
            "| suite | dag-ml/legacy run ratio | dag-ml/legacy total ratio |",
            "|---|---|---|",
        ]
    )
    for suite_name, suite in report["suites"].items():
        label = suite.get("label", suite_name)
        ratios = suite.get("ratios", {})
        run_ratio = ratios.get("run_s_dag_ml_over_legacy")
        total_ratio = ratios.get("total_s_dag_ml_over_legacy")
        run_text = "n/a" if run_ratio is None else f"{float(run_ratio):.3f}x"
        total_text = "n/a" if total_ratio is None else f"{float(total_ratio):.3f}x"
        lines.append(f"| {label} | {run_text} | {total_text} |")

    return "\n".join(lines)


def parse_ratio_overrides(values: Iterable[str]) -> dict[str, float]:
    parsed: dict[str, float] = {}
    for raw in values:
        suite, sep, limit = raw.partition("=")
        if not sep:
            raise ValueError(f"expected SUITE=FLOAT, got {raw!r}")
        suite = suite.strip()
        if suite not in DEFAULT_SUITES:
            raise ValueError(f"unknown suite in ratio override: {suite!r}")
        parsed[suite] = float(limit)
    return parsed


def _json_dump(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--suite",
        action="append",
        dest="suites",
        choices=list(DEFAULT_SUITES),
        help="suite to run; repeat to select multiple",
    )
    parser.add_argument("--repeats", type=int, default=3, help="measured repeats per suite/engine")
    parser.add_argument("--warmups", type=int, default=0, help="discarded warmup runs per measurement child")
    parser.add_argument("--python", dest="child_python", help="override child interpreter")
    parser.add_argument("--json-out", type=Path, help="write the full report as JSON")
    parser.add_argument("--markdown-out", type=Path, help="write the rendered markdown summary")
    parser.add_argument(
        "--assert-max-ratio",
        action="append",
        default=[],
        metavar="SUITE=FLOAT",
        help="fail when dag-ml/legacy run ratio for SUITE exceeds FLOAT",
    )
    args = parser.parse_args(argv)

    try:
        report = run_comparison(
            suites=args.suites or DEFAULT_SUITES,
            repeats=args.repeats,
            warmups=args.warmups,
            child_python=args.child_python,
            max_ratios=parse_ratio_overrides(args.assert_max_ratio),
        )
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    markdown = render_markdown(report)
    print(markdown)
    if args.json_out:
        _json_dump(args.json_out, report)
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
