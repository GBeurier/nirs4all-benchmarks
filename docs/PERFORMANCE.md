# Performance comparison harness

`nirs4all-benchmarks perf-compare` compares an explicitly selected Archive V2 on four product surfaces: `python_oracle`, `rust_direct`, `studio_rust` and `web_wasm`. It invokes explicit stdio adapter executables; it does not discover sibling Python environments or import a runtime checkout into the orchestrator.

The plan selects the archive digest, prediction matrix, sample and target identities, predictor descriptor, candidate commits and absolute numerical tolerance. Every adapter must return these same identities and finite predictions without fallback. Missing archives or adapters produce a `refused` report; invalid results or numerical divergence produce a `failed` report. Both dispositions exit nonzero. Only `passed` exits zero, and reports are written before this final exit check.

## Running the selected workload

Use a plan whose candidate identities and artifact digests describe the actual staged runtimes. The checked-in `tests/fixtures/performance-plan.historical.json` is a historical contract fixture, not a selection of the current public cohort.

```bash
n4a-benchmarks perf-compare \
  --plan /absolute/path/to/performance-plan.json \
  --workspace-root /absolute/path/to/release-workspace \
  --archive /absolute/path/to/selected-model.n4a \
  --adapter python_oracle=/absolute/path/to/python-adapter \
  --adapter rust_direct=/absolute/path/to/rust-adapter \
  --adapter studio_rust=/absolute/path/to/studio-adapter \
  --adapter web_wasm=/absolute/path/to/web-adapter \
  --repeats 3 \
  --timeout 120 \
  --evidence-kind local_real \
  --json-out ./perf-report.json \
  --markdown-out ./perf-report.md \
  --handoff-dir ./performance-compare
```

The module entry point accepts the same inputs:

```bash
python -m nirs4all_benchmarks.performance_compare --help
```

`--evidence-kind contract_fixture` identifies contract-test output; it must not be presented as execution of a real public product. The adapters and their installed runtime dependencies must be available before running this command.

## Evidence and interpretation

The report separates startup and steady-state measurements and retains each repeat. It compares predictions against the Python oracle using the absolute tolerance in the plan, preserving sample order, target order and predictor identity. The deterministic Web handoff contains the archive, matrix, descriptor, oracle predictions and selected Web candidate.

This harness reports timings without a definitive speed threshold (`performance_policy.thresholds` is null). Its numerical comparison is scoped to the selected archive and matrix, not the complete estimator catalog. `passed` does not imply `release_eligible`: the report retains explicit release eligibility holds, including unfrozen budgets and incomplete release matrices.

The earlier RC-v1 direct `python_run`/Studio Python worker workload, interpreter discovery, `--python`, `--assert-max-ratio` and `--assert-max-score-delta` options are retired. Historical reports remain renderable through read-only compatibility; those options are not executable interfaces of this harness. The separate ecosystem ALL11 performance workload and its strict speed and parity assertions are unchanged by this package delivery.
