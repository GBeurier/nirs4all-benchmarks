# PERF-001 Archive V2 comparison

The V1 harness replays one byte-identical Archive V2 and one matrix through
four product surfaces:

- the public full-Python oracle;
- Core's direct Rust entry point;
- Studio's Rust sidecar route;
- Web's Core/Methods WASM entry point.

The frozen workload, predictor descriptor, descriptor fingerprint, candidate
commits and candidate trees are in
[`performance-compare.handoff.v1.json`](performance-compare.handoff.v1.json).
The harness does not import sibling source trees, rebuild or convert the
archive, publish artifacts, or update a release lock.

## Adapter protocol

Each surface is supplied as an absolute executable path:

```bash
PYTHONPATH=src python3.11 -m nirs4all_benchmarks.performance_compare \
  --workspace-root /home/delete/nirs4all \
  --adapter python_oracle=/absolute/bin/python-oracle-adapter \
  --adapter rust_direct=/absolute/bin/core-rust-adapter \
  --adapter studio_rust=/absolute/bin/studio-rust-adapter \
  --adapter web_wasm=/absolute/bin/web-wasm-adapter \
  --repeats 3 \
  --json-out /tmp/perf001.json \
  --handoff-dir /tmp
```

The runner sends one JSON request on stdin. It contains the absolute Archive
path and SHA-256, the ordered sample IDs and matrix, ordered target names, the
exact candidate commit, the authoritative predictor descriptor/fingerprint,
and the steady-state repeat count. An adapter returns one JSON document using
protocol `nirs4all.performance-compare.adapter.v1` with:

- the exact archive, matrix, candidate and predictor identities;
- `fallback_used=false`;
- aligned predictions;
- one `startup_ms` value and exactly `repeats` `steady_state_ms` values.

Missing executables or external artifacts are explicit `refused` surfaces.
Invalid or numerically inconsistent evidence is `failed`. The old
legacy-vs-dag-ml and Studio Python-worker execution paths are no longer
reachable; historical JSON reports remain renderable read-only.

## Web handoff

`--handoff-dir /tmp` creates:

```text
/tmp/performance-compare/
  archive-v2-performance-compare.v1.json
  performance-report.v1.json
```

The first file is the Web handoff. It carries the same Archive V2 identity,
matrix, predictor descriptor/fingerprint, Python oracle values, Web candidate,
numeric tolerance and Web result.

## Current local hold

The exact source candidates exist locally, and the canonical Archive V2 is
present in the Web candidate. A single mutually attested runtime closure for
all four exact candidates is not staged: the Python native environment, direct
Rust runner, Studio packaged libn4m closure, and rebuilt Web closure listed in
the plan remain required. Web already carries the exact staged Core/Methods
WASM closure, but still needs the PERF stdio timing adapter around
`replayMethodsArchiveV2`. Contract fixtures may exercise the runner and Web
handoff, but are stamped `evidence_kind=contract_fixture` and
`release_eligible=false`; they never stand in for candidate performance.

WSL results are measurement-only. Startup and steady-state timings are kept
separate, and the harness deliberately applies no definitive performance
threshold until budgets are frozen on an approved reference host.
