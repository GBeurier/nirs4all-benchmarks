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
The harness does not import sibling source trees, convert the archive, publish
artifacts, or update a release lock. The committed adapters attest every source
identity before invoking the locally built product closure.

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

## Current candidate staging

The plan now stages the requested subset of candidate heads recorded by
Governance snapshot `8aa4540a6b97b9e6cb8facf2f3a189f0d62f1e1b`: Methods
`e0bee1ce`, DAG-ML `b08c6263`, Python R3 `3a38f589`, and Studio R3
`bb66016c`. Other plan candidates retain their prior selection, so this is not
a complete Governance-closure repin. These identities select the next campaign;
they do not alter the provenance of the checked-in four-runtime report.

No four-runtime comparison has been run on this exact staged plan. Release
readiness therefore remains **NO-GO** until that campaign, the external release
matrices, frozen performance budgets, complete closure staging, signed
artifacts, publication, and final-lock reconstruction are available.
Contract-fixture tests validate orchestration only and cannot close those holds.

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

## Current local evidence

The checked-in [`performance-compare/performance-report.v1.json`](performance-compare/performance-report.v1.json)
is a historical real local run with one startup observation and three steady-state
observations per surface. Python used the public `predict(engine="native")`
API, Rust called Core directly, Studio used its packaged Rust sidecar, and Web
used its shipped Core/Methods WASM pair. All four surfaces replayed the same
Archive V2 and matrix, passed with zero maximum numeric delta, and reported
`fallback_used=false`. The report records source trees, adapter hashes, the
Methods library/sidecar hashes, and the predictor descriptor/fingerprint. Its
recorded candidate and runtime identities are intentionally unchanged by the
new staging selection.

The companion
[`performance-compare/archive-v2-performance-compare.v1.json`](performance-compare/archive-v2-performance-compare.v1.json)
is directly consumable by Web's existing `performance-compare` handoff.
Contract fixtures remain available only for protocol failure tests and never
substitute for these product closures.

This WSL campaign is `local_real` but measurement-only. It is intentionally
`release_eligible=false` because the host is WSL, performance budgets are not
frozen, and release matrices are incomplete. Startup and steady-state timings
remain separate, and no release threshold is set or claimed as passed.
