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

The plan pins the explicitly selected final product heads: Python R3
`53a0acb9`, Core 0.3.28 `550cb8c8`, Studio R3 `86d5e503`, and Web 0.1.10
`051bf636`. Supporting Methods and DAG-ML identities remain individually
attested in the plan and runtime evidence. This local projection must still be
incorporated into the Governance ledger before a final release lock is built.

The exact four heads have been run together on the deterministic Archive V2
workload. This closes only the current-head synthetic comparison action. Release
readiness remains **NO-GO** pending a provenance-qualified representative user
corpus, sustained soak, external release matrices, frozen performance budgets,
signatures, remaining product publication, and final-lock reconstruction.

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
is a real-product, synthetic-workload local run with one startup observation and
three steady-state observations per surface. Python used the public
`predict(engine="native")` API at `53a0acb9`, Rust called Core 0.3.28 directly,
Studio used the packaged Rust sidecar at `86d5e503`, and Web used the shipped
Core/Methods WASM closure at `051bf636`. All four surfaces replayed the same
Archive V2 and matrix, passed with zero maximum numeric delta, and reported
`fallback_used=false`. The report records source trees, adapter hashes, the
Methods library/sidecar hashes, and the predictor descriptor/fingerprint.

The companion
[`performance-compare/archive-v2-performance-compare.v1.json`](performance-compare/archive-v2-performance-compare.v1.json)
is directly consumable by Web's existing `performance-compare` handoff.
Contract fixtures remain available only for protocol failure tests and never
substitute for these product closures.

This WSL campaign is `local_synthetic_current_head` and measurement-only. It is
intentionally `release_eligible=false` because the deterministic fixture is not
a representative user corpus, no sustained soak was run, the host is WSL,
performance budgets are not frozen, and release matrices are incomplete.
Startup and steady-state timings remain separate, and no release threshold is
set or claimed as passed.
