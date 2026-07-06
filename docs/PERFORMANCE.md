# Performance comparison harness

`nirs4all-benchmarks` ships a small comparison harness for the RC-v1 migration
worktrees. It does not edit `nirs4all` or Studio, and it does not require a
workspace dataset on disk.

## What it measures

The harness runs one seeded synthetic PLS case on two surfaces:

- `python_run`: direct `nirs4all.run(...)`
- `studio_run`: Studio's real pipeline job worker
  `api.pipelines._run_pipeline_task(...)`

Each surface is measured twice: once with `engine="legacy"` and once with
`engine="dag-ml"`. Fallback is disabled, so an unsupported dag-ml path fails
the run instead of silently timing legacy.

Because both engines in a suite run the *same* seeded case, their `best_score`
values are directly comparable. The report therefore carries, per suite, a
`scores` block (`legacy`, `dag_ml`, `abs_delta`) so the comparison shows not
just *how fast* dag-ml is relative to legacy but *whether it computed the same
answer*. This is a single-case agreement signal, not full numeric parity.

## Why it is stable enough for CI

- fixed synthetic dataset (`80 x 50`, seed `2026`)
- fixed pipeline (`MinMaxScaler -> ShuffleSplit(2) -> PLSRegression(3)`)
- fresh subprocess per repeat
- single-threaded BLAS/OpenMP environment
- median-based report

The goal is not an absolute performance claim. The gate is a reproducible
comparison report that catches "the dag-ml path got much slower than legacy on
this representative small case" without requiring full numeric parity.

## Running it

From the `RC-v1-benchmarks` worktree:

```bash
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/n4a-benchmarks perf-compare \
  --repeats 3 \
  --json-out ./perf-report.json \
  --markdown-out ./perf-report.md
```

The parent command only needs the benchmark package environment. The measured
children auto-discover a Python interpreter that can import Studio plus a usable
workspace `nirs4all` source tree. The harness prefers the RC-v1 `nirs4all`
worktree and falls back to the sibling `nirs4all/` checkout when the RC tree is
not runnable for the dag-ml case. Override the child interpreter explicitly when
needed:

```bash
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/n4a-benchmarks perf-compare \
  --python ../nirs4all-studio/.venv/bin/python
```

You can also pin the exact `nirs4all` source root:

```bash
N4A_BENCH_NIRS4ALL_ROOT=../nirs4all \
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/n4a-benchmarks perf-compare
```

Or run the module directly:

```bash
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/python -m nirs4all_benchmarks.performance_compare
```

## Optional ratio gates

The command can fail when the measured `dag-ml/legacy` run-time ratio exceeds a
suite-specific ceiling:

```bash
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/n4a-benchmarks perf-compare \
  --assert-max-ratio python_run=1.25 \
  --assert-max-ratio studio_run=1.35
```

The exact ceilings are coordinator policy, not package policy. Start by running
the report a few times on the target CI class, then set conservative bounds
above the observed medians.

## Optional score-agreement gates

Timing bounds only catch "dag-ml got slower". To also catch "dag-ml computed a
different answer than legacy on this case", gate on the absolute score delta:

```bash
PYTHONPATH=src \
  ../nirs4all-benchmarks/.venv/bin/n4a-benchmarks perf-compare \
  --assert-max-score-delta python_run=0.001 \
  --assert-max-score-delta studio_run=0.001
```

The gate fails when `|legacy_score - dag_ml_score|` for a suite exceeds the
ceiling, or when either engine did not produce a finite score (reported as
`score delta unavailable`). Like the ratio gate, the tolerance is coordinator
policy: the two engines should agree closely on the seeded PLS case, so a tight
bound is appropriate.
