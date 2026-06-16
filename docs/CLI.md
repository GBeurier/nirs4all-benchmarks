# CLI reference — `n4a-benchmarks`

Command-line interface for the Arena: initialize a store, ingest scored runs from any producer, seed
fixtures, query the store, and run the dataviz web service.

The package exposes two identical console entry points:

| Command | Maps to |
|---|---|
| `n4a-benchmarks` | `nirs4all_benchmarks.cli:main` |
| `n4a-arena` | `nirs4all_benchmarks.cli:main` (alias) |

Both names invoke the same Typer app; the examples below use `n4a-benchmarks` interchangeably with
`n4a-arena`. Running either with no arguments prints help.

## Contents

- [Global options](#global-options)
- [Commands](#commands)
  - [`version`](#version)
  - [`init`](#init)
  - [`ingest-export`](#ingest-export)
  - [`ingest-workspace`](#ingest-workspace)
  - [`ingest-bundle`](#ingest-bundle)
  - [`inspect-n4a`](#inspect-n4a)
  - [`fixtures`](#fixtures)
  - [`stats`](#stats)
  - [`leaderboard`](#leaderboard)
  - [`serve`](#serve)
- [End-to-end session](#end-to-end-session)
- [Installation extras](#installation-extras)

## Global options

Most commands accept a single shared option for the store location:

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory. Created if it does not exist. |

The store directory holds `arena.sqlite`, an `arrays/` directory (sample-keyed residual Parquet), and
an `exports/` directory (ingested `ArenaRunExport` bundles, kept for audit/replay).

## Commands

### `version`

Print the package version.

```bash
n4a-benchmarks version
# nirs4all-benchmarks 0.1.0
```

### `init`

Create (or open) an empty Arena store. Idempotent — running it against an existing store just opens it
and reports the schema version.

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory. |

```bash
n4a-benchmarks init --store ./arena-store
# ✓ store ready at arena-store (schema v1)
```

### `ingest-export`

Ingest one or more `ArenaRunExport` manifests (the canonical producer-neutral export contract). The
`PATH` argument may be a single manifest JSON file or a directory; when it is a directory, every
`*.json` file directly inside it is ingested (sorted by name).

| Argument / Option | Default | Description |
|---|---|---|
| `PATH` (positional, required) | — | `ArenaRunExport` manifest JSON, or a directory of them. Must exist. |
| `--store`, `-s` | `./arena-store` | Arena store directory. |
| `--collection`, `-c` | `default` | Target collection id. |
| `--release` | off (flag) | Ingest as a benchmark release. Releases quarantine runs on detected leakage; without it, runs are ingested as a user-run collection. |

Each file's per-file outcome (`committed` / `already_ingested` / `quarantined` / `rejected`) and its
validity status are printed, followed by an aggregate count.

```bash
# Single manifest into the default collection.
n4a-benchmarks ingest-export ./runs/arena_run_ab12cd.json

# A whole directory, as a leakage-checked benchmark release.
n4a-benchmarks ingest-export ./runs/ \
  --store ./arena-store \
  --collection paper-2026 \
  --release
#   arena_run_ab12cd.json: committed (valid)
#   arena_run_ef34gh.json: quarantined (quarantined)
# ✓ {'committed': 1, 'quarantined': 1}
```

Ingestion is idempotent: re-ingesting the same export into the same collection yields
`already_ingested` rather than a duplicate.

### `ingest-workspace`

Adapter A. Ingest a nirs4all workspace directory, producing one `ArenaRunExport` per pipeline run and
ingesting each. The workspace must contain a `store.sqlite`; prediction residuals are read from the
sibling `arrays/<dataset>.parquet` files. Model weights (`artifacts/`) are ignored entirely.

Because the nirs4all workspace store carries no OOF attestation and only positional sample indices,
these exports are honest about it: `oof_enforced` is reported as `False`, and residuals use the
degraded `key="positional"` path (synthetic `pos_<idx>` sample ids).

| Argument / Option | Default | Description |
|---|---|---|
| `WORKSPACE` (positional, required) | — | nirs4all workspace dir (`store.sqlite` + `arrays/`). Must exist. |
| `--store`, `-s` | `./arena-store` | Arena store directory. |
| `--collection`, `-c` | `nirs4all-workspace` | Target collection id. Ingested as a user-run collection. |

```bash
n4a-benchmarks ingest-workspace ~/nirs4all/my_project/workspace \
  --store ./arena-store \
  --collection my-lab-runs
# ✓ workspace ingested: {'committed': 12, 'already_ingested': 3}
```

### `ingest-bundle`

Adapter B. Ingest a dag-ml `ExecutionBundle` JSON, optionally enriched with a canonical `GraphSpec`
and an io `CoordinatorDataPlanEnvelope`. The adapter parses the engine's serde JSON without importing
dag-ml; missing companions degrade gracefully (without a graph, pipeline identity falls back to the
engine `graph_fingerprint`). dag-ml predictions are sample-keyed, so the export uses the native,
non-degraded `key="sample_id"` residual path.

| Argument / Option | Default | Description |
|---|---|---|
| `BUNDLE` (positional, required) | — | dag-ml `ExecutionBundle` JSON. Must exist. |
| `--graph` | none | Canonical `GraphSpec` JSON (gives a full pipeline DAG identity). |
| `--envelope` | none | io `CoordinatorDataPlanEnvelope` JSON (dataset fingerprints). |
| `--store`, `-s` | `./arena-store` | Arena store directory. |
| `--collection`, `-c` | `dag-ml` | Target collection id. |
| `--release` / `--user` | `--release` | dag-ml runs are leakage-safe by construction, so they ingest as a benchmark release by default. Pass `--user` to ingest into a user-run collection instead. |

```bash
n4a-benchmarks ingest-bundle ./out/bundle.json \
  --graph ./out/graph.json \
  --envelope ./out/data_plan.json \
  --store ./arena-store \
  --collection dag-ml-nightly
# ✓ bundle ingested: committed (valid)
```

### `inspect-n4a`

Extract a `.n4a` bundle's recipe and show its canonical pipeline identity, with all model weights
stripped. A `.n4a` is a ZIP (`manifest.json` + `pipeline.json`/`chain.json` + `artifacts/*`); only the
recipe is read, never the artifact bytes — the stripped artifacts are merely counted for the audit
trail. Identity is taken from `pipeline.json` (a DSL step list), never `chain.json`.

This command does not touch a store and takes no `--store` option.

| Argument | Description |
|---|---|
| `PATH` (positional, required) | A `.n4a` bundle. Must exist. |

```bash
n4a-benchmarks inspect-n4a ./models/best_pls.n4a
# steps: 4  stripped artifacts: 3
# pipeline_dag_hash: 7f3a9c1e0b...
# nirs4all_identity_hash: 1c8d44ab02...
```

### `fixtures`

Seed the store with the synthetic fixture grid (2 mock datasets × {PLS n_components sweep,
branch/merge, stacking}). Fixtures are ingested as a benchmark release and are intended for the
dataviz demo and for trying queries without real data.

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory. |
| `--collection`, `-c` | `fixtures` | Target collection id for the seeded grid. |
| `--write-to` | none | If given, also write each fixture export as JSON to this directory. |

The printed summary is a dict of `committed` / `already` / `quarantined` / `rejected` counts plus the
number of distinct `run_conditions`.

```bash
n4a-benchmarks fixtures --store ./arena-store
# ✓ seeded: {'committed': 14, 'already': 0, 'quarantined': 0, 'rejected': 0, 'run_conditions': 14}

# Also dump the fixture manifests to disk (e.g. to re-ingest via ingest-export).
n4a-benchmarks fixtures --store ./arena-store --write-to ./fixture-exports
# ✓ wrote 14 fixture exports to fixture-exports
```

### `stats`

Print a store overview as a table: dimension/fact counts, valid vs. quarantined executions, the list
of available metric names, and the schema version.

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory. |

```bash
n4a-benchmarks stats --store ./arena-store
# Arena store overview
#   datasets                2
#   pipelines               9
#   executions              14
#   valid_executions        14
#   quarantined_executions  0
#   metric_observations     ...
#   metrics                 mae, r2, rmse
#   schema_version          1
#   ...
```

### `leaderboard`

Print a leaderboard for a given metric and scope. There is no canonical baseline imposed — the
leaderboard is fully configurable. Rows are ranked by mean metric value in the metric's natural
direction (lower-is-better for error metrics, higher-is-better for e.g. `r2`); the direction is shown
in the title. Quarantined executions are excluded.

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory. |
| `--metric`, `-m` | `rmse` | Metric name to rank on. |
| `--scope` | `cv` | Score scope: one of `fold`, `cv`, `refit`, `test`, `view`. |
| `--dataset` | none | Restrict to one `dataset_fingerprint`. |
| `--limit`, `-n` | `15` | Maximum number of rows to show. |

```bash
# Default: RMSE on the cross-validation scope.
n4a-benchmarks leaderboard --store ./arena-store
# Leaderboard — rmse (cv, min)
# #  mean    pipeline                       main model
# 1  0.5421  stack(SNV→PLS, SNV→RF)         Ridge
# 2  0.5803  [SNV→PLS | SG→PLS] → mean      MeanEnsemble
# 3  ...

# R² on the external test scope, top 5, one dataset.
n4a-benchmarks leaderboard \
  --store ./arena-store \
  --metric r2 \
  --scope test \
  --dataset 9ab3... \
  --limit 5
```

### `serve`

Run the dataviz web service (FastAPI + a no-build Plotly SPA). The service reads the same store via the
`Queries` facade. The store path is exported to the service as the `NIRS4ALL_BENCHMARKS_STORE`
environment variable (resolved to an absolute path) before the app is launched with uvicorn.

Requires the `service` extra (`fastapi`, `uvicorn`); without it the command exits with code 1.

| Option | Default | Description |
|---|---|---|
| `--store`, `-s` | `./arena-store` | Arena store directory the service queries. |
| `--host` | `127.0.0.1` | Bind host. |
| `--port`, `-p` | `8000` | Bind port. |
| `--reload` | off (flag) | Enable uvicorn auto-reload (development). |

```bash
n4a-benchmarks serve --store ./arena-store --host 0.0.0.0 --port 8080
# ✓ serving Arena at http://0.0.0.0:8080 (store: ./arena-store)
```

The SPA is served at `/`, and the JSON API under `/api/*` (`/api/healthz`, `/api/overview`,
`/api/leaderboard`, `/api/matrix`, `/api/runs`, `/api/run/{execution_hash}`, `/api/compare`,
`/api/ingest`, and others).

## End-to-end session

A minimal walkthrough: create a store, seed the fixture grid, inspect it, rank it, and serve it.

```bash
# 1. Create an empty store.
n4a-benchmarks init --store ./arena-store
# ✓ store ready at arena-store (schema v1)

# 2. Seed the synthetic fixture grid (no real data needed).
n4a-benchmarks fixtures --store ./arena-store
# ✓ seeded: {'committed': 14, 'already': 0, 'quarantined': 0, 'rejected': 0, 'run_conditions': 14}

# 3. See what's in the store.
n4a-benchmarks stats --store ./arena-store

# 4. Rank pipelines by RMSE on the CV scope.
n4a-benchmarks leaderboard --store ./arena-store --metric rmse --scope cv

# 5. Browse it in the dataviz UI (needs the 'service' extra).
n4a-benchmarks serve --store ./arena-store
# open http://127.0.0.1:8000
```

To work from real producer output instead of fixtures, replace step 2 with one of the ingestion
commands — `ingest-export`, `ingest-workspace`, or `ingest-bundle`.

## Installation extras

Some commands need optional dependency groups:

| Command | Extra | Install |
|---|---|---|
| `serve` | `service` | `pip install 'nirs4all-benchmarks[service]'` |
| `ingest-bundle` (live from engine) | `dagml` | `pip install 'nirs4all-benchmarks[dagml]'` |
| `ingest-workspace` (live from library) | `nirs4all` | `pip install 'nirs4all-benchmarks[nirs4all]'` |

The core CLI (`init`, `ingest-export`, `inspect-n4a`, `fixtures`, `stats`, `leaderboard`) works with
the base install. `pip install 'nirs4all-benchmarks[all]'` pulls the `service` and `datasets` extras.
