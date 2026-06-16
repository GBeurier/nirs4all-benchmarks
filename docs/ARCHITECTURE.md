# Architecture

System overview of `nirs4all-benchmarks` (codename **the Arena**): a reproducible, scored, **weights-free** benchmark store and dataviz service for NIRS pipelines produced by `nirs4all` and/or `dag-ml`.

## Contents

- [What the Arena is — and is not](#what-the-arena-is--and-is-not)
- [Module map](#module-map)
- [End-to-end data flow](#end-to-end-data-flow)
- [The dual-producer model](#the-dual-producer-model)
- [Standalone by contract](#standalone-by-contract)
- [The four versioned contracts](#the-four-versioned-contracts)
- [Related documents](#related-documents)

## What the Arena is — and is not

The Arena is a producer-agnostic **sink** for run data: it ingests one weights-free, content-addressed bundle per execution (the `ArenaRunExport`), normalizes it into `arena.sqlite` (dimensions + facts) plus `arrays/*.parquet` (sample-keyed residuals), and serves meta-analysis over the `dataset × task × variant × split × cv × rng × pipeline × refit` space through a REST API and a no-build Plotly single-page app.

What it stores: identity cards / fingerprints of datasets, single-target task specs, canonical pipeline DAGs (with materialized node/operator/parameter tables), split/CV/RNG/refit specs, versioned score sets and long-format metric observations, and sample-keyed residuals.

**No artifacts, ever.** The Arena never stores fitted models, transformers, feature caches, `.n4a` weights, or raw datasets. A `.n4a` upload is accepted, but its weights are stripped on read — only the recipe and its canonical identity are kept (`adapters/n4a_bundle.py`). This is the load-bearing invariant of the whole system: see [`../DESIGN.md`](../DESIGN.md) §2 and the `RESIDUALS_PARQUET_COLUMNS` contract in `contract/schema/__init__.py`.

Two correctness properties follow from content-addressed identity:

- **Idempotent ingestion.** The idempotency key is `(input_export_hash, target_collection, arena_schema_version)`, enforced by a `UNIQUE` constraint on `ingestion_batches` and a dedup check at ingest. Every dimension's primary key is a content hash, so the store uses `INSERT OR IGNORE` for trivially-correct dedup.
- **Leakage honesty.** Ingestion records the producer's OOF/leakage attestation and, for benchmark releases, quarantines runs that cannot attest within-train CV (`oof_enforced == false`) rather than dropping them.

## Module map

Source lives under `src/nirs4all_benchmarks/`. CLI entry points are `n4a-benchmarks` and `n4a-arena` (both resolve to `cli.py:main`).

- **`identity/`** — the identity spine. `hashing.py` defines the *one* hashing primitive: `fingerprint(x) == sha256_hex(canonical_json(x))`, lowercase 64-hex, with `canonical_json` = sorted-key, compact-separator, `allow_nan=False` JSON. It also composes `run_condition_hash` from six labelled component hashes in a fixed order and computes the `export_hash` idempotency key (excluding the `arena_export_hash` field so the hash is a fixed point). `pipeline_dag.py` owns `pipeline_dag_hash`: it normalizes any pipeline shape (canonical `GraphSpec`, an Arena `nodes` projection, or a linear nirs4all step list) into a Merkle-DAG hash — node ids made positional (`g0`, `g1`, …), inputs sorted only for order-insensitive reducers (`mean`/`vote`/`bagging`/…). The dag-ml `graph_fingerprint` and nirs4all `get_hash` are recorded as secondary ids, never as join keys.

- **`contract/`** — the frozen `ArenaRunExport` v1 wire contract. `arena_run_export.py` holds the typed pydantic models (blocks are `extra="allow"` so producers can add fields). `schema/arena_run_export.schema.json` is the authoritative JSON Schema; `schema/__init__.py` exposes `validate_manifest()` and `RESIDUALS_PARQUET_COLUMNS` (the sample-keyed residual column contract). The manifest mirrors [`../DATA_MANAGEMENT.md`](../DATA_MANAGEMENT.md) §4 exactly.

- **`store/`** — the persistence layer. `schema.sql` is the full `arena.sqlite` schema (dimensions, facts, indexes, and the `v_run_metrics` serving view). `arena_store.py` wraps `sqlite3`: it creates/migrates the schema, stamps `PRAGMA user_version` with `ARENA_SCHEMA_VERSION`, guards against opening a store written by a newer library, and offers `upsert` (dedup) / `insert` (append) / `query` primitives wrapped in a single transaction per ingest. `residual_store.py` is `ResidualStore` — one Zstd Parquet file per residual set (`residuals_<hash>.parquet`), keyed by stable `sample_id`, written atomically. `queries.py` is the `Queries` read facade (leaderboard, matrix, run explorer, operator/parameter effect, robustness, run detail, residual compare) used by both the service and the CLI.

- **`ingestion/`** — the `ArenaRunExport → ArenaStore` state machine. `ingest.py` runs VERIFY → DEDUP-CHECK → RESOLVE → VALIDATE → PSEUDONYMIZE/STRIP → STAGE→COMMIT → REPORT in one transaction, driven by an `IngestionPolicy` (collection, leakage quarantine, residual row cap, score recompute). `resolve.py` recomputes every Arena-owned hash and extracts operator/parameter/node/edge rows. `validate.py` is the minimal correctness gate plus leakage/keying verdict (`valid` | `quarantined` | `rejected`, and `residual_key` = `sample_id` | `positional`). `pseudonymize.py` maps every `sample_id`/`group_id` to a salted digest using a store-global persistent salt, so the same raw id maps to the same pseudo id across runs (the basis of cross-run residual comparison).

- **`adapters/`** — producer → `ArenaRunExport` converters. `nirs4all_workspace.py` (Adapter A, live) reads a nirs4all workspace (`store.sqlite` + `arrays/*.parquet`), emits one export per pipeline, ignores `artifacts/`, and marks residuals `key="positional"` (synthetic `pos_<idx>` ids) because nirs4all writes positional `sample_indices`. `dagml_bundle.py` (Adapter B, future) maps a dag-ml `ExecutionBundle` (+ optional `GraphSpec` and io envelope) onto the contract without importing dag-ml, using the native sample-keyed path (`key="sample_id"`). `n4a_bundle.py` extracts a `.n4a` recipe (from `pipeline.json`, never `chain.json`) and computes its canonical identity, listing — but never reading — the stripped artifact entries.

- **`scoring/`** — re-derivable, auditable scores. `metrics.py` is a NumPy-only registry (regression: `rmse`/`mae`/`r2`/`rpd`/`rpiq`/`ccc`/…; classification: `accuracy`/`f1_macro`/`mcc`/`roc_auc`/`log_loss`/…), a `direction_of()` map, and `recompute_observations()` that rebuilds long-format observations from residual rows grouped by scope/fold/partition. `score_spec.py` is `ScoreComputationSpec` — the versioned identity of *how* a score was computed (metric implementation, level, aggregation, version); its `score_computation_hash` keys a `ScoreSet`, and a metric fix yields a new, superseding score set rather than mutating the old one.

- **`datasets/`** — dataset identity. `dataset_card.py` is the `DatasetCard` model plus builders that project an export's `dataset` block into `dataset_cards` / `dataset_fingerprints` rows. `catalog.py` is a best-effort bridge to the `nirs4all-datasets` catalog (sibling checkout or installed package) with a deterministic `mock_dataset_card` fallback; the Arena never re-implements dataset IO.

- **`service/`** — `app.py` is the FastAPI application factory. It exposes the read API under `/api/*` (overview, collections, datasets, pipelines, operators, parameters, leaderboard, matrix, runs, operator-/parameter-effect, robustness, run detail, residuals, compare), a `POST /api/ingest` endpoint that accepts an `ArenaRunExport` manifest, and mounts the static SPA at `/`. FastAPI is an optional `[service]` extra; the factory raises a clear error if it is absent.

- **`cli.py`** — the `n4a-benchmarks` / `n4a-arena` Typer app: `init`, `ingest-export`, `ingest-workspace` (Adapter A), `ingest-bundle` (Adapter B), `inspect-n4a`, `fixtures`, `stats`, `leaderboard`, `serve`. It is a thin shell over the same `ingest_export` / `Queries` API the service uses.

- **`fixtures/`** — `generate.py` produces the synthetic fixture grid (two mock datasets × {PLS `n_components` sweep, branch/merge, stacking}) with structured residuals, plus `seed_store()` to ingest them and `write_fixture_exports()` to emit them as JSON. This powers the dataviz demo and pins `run_condition_hash`es as a regression contract.

- **`web/`** — the no-build SPA served by FastAPI. `index.html` + `app.js` (a hash-router and view registry), `lib/` (`api.js`, `dom.js`, `plot.js`), `views/*.js` (overview, leaderboard, matrix, param-/operator-effect, robustness, runs, compare, datasets, pipelines, run-detail, upload), and a vendored `plotly.min.js`. No bundler, no framework — ES modules loaded directly.

## End-to-end data flow

```
 producer run (compute happens elsewhere — the Arena never runs models)
        │
        ▼  ArenaRunExport v1   (manifest.json + sample-keyed residuals + canonical GraphSpec)
   ┌────────────────────────────────────────────────────────────────────┐
   │ INGEST (ingestion/ingest.py)                                        │
   │   1 VERIFY        schema version + JSON Schema + export_hash         │
   │   2 DEDUP-CHECK   (export_hash, collection, schema_version) seen?    │
   │   3 RESOLVE       recompute every hash; normalize GraphSpec →        │
   │                   pipeline_dag_hash; compose run_condition_hash      │
   │   4 VALIDATE      identity / task / split-cv / leakage / keying      │
   │   5 PSEUDONYMIZE  sample_id/group_id → salted pseudo ids             │
   │   6 STRIP         drop artifact bytes; drop arrays per policy        │
   │   7 STAGE→COMMIT  upsert dimensions + facts in one transaction       │
   │   8 REPORT        clean_report_json on the ingestion_batch           │
   └────────────────────────────────────────────────────────────────────┘
        │                                  │
        ▼                                  ▼
   arena.sqlite                       arrays/residuals_<hash>.parquet
   (dimensions + facts,               (sample-keyed: sample_id, scope,
    keyed by content hashes;           fold_id, partition, y_true/y_pred,
    exports/ keeps the bundle)         residual, weight)
        │                                  │
        └──────────────┬───────────────────┘
                       ▼
              Queries (store/queries.py)  ── plain SQL joins on the
              v_run_metrics view + materialized node/param tables
                       │
                       ▼
              REST API (service/app.py, /api/*)
                       │
                       ▼
              SPA (web/, Plotly, no build step)
```

Each producer-specific format reaches this pipeline through an adapter that emits the *same* `ArenaRunExport`; the dotted CLI/HTTP entry points (`ingest-export`, `ingest-workspace`, `ingest-bundle`, `POST /api/ingest`) all funnel into `ingest_export`.

## The dual-producer model

Two producers feed **one** contract, and the Arena composes a single identity over both halves of an export:

- **nirs4all workspace** (Adapter A, `adapters/nirs4all_workspace.py`) — live today. Reads the documented frozen workspace schema (`pipelines` / `predictions` tables + per-dataset prediction Parquet). It cannot attest OOF safety (the workspace store carries no attestation), so it honestly reports `oof_enforced=false` and emits residuals as `key="positional"` with `pos_<idx>` sample ids — queryable per-run, excluded from cross-run sample comparison until an io ordinal→`SampleId` sidecar exists.

- **dag-ml bundle** (Adapter B, `adapters/dagml_bundle.py`) — the future engine. Its persisted `ExecutionBundle` is already the Arena's shape: a topology-aware `graph_fingerprint`, sample-keyed OOF `PredictionBlock`s, a `fold_set_fingerprint`, leakage `unsafe_flags`, and standards-conformant provenance. Adapter B maps that JSON onto the contract using the native, non-degraded `key="sample_id"` path, with dataset identity taken from io's `CoordinatorDataPlanEnvelope` (its three fingerprints).

Both adapters produce an `ArenaRunExport`; `ingest_export` treats them identically. The Arena owns `pipeline_dag_hash` (recomputed via the Merkle normalizer so a nirs4all step list and its dag-ml DAG converge) and composes `run_condition_hash` from the dataset-side identity (from io) and the run-side identity (from the engine). Producer UUIDs are recorded but never used as join keys. See [`../PERSISTENCE_FORMATS.md`](../PERSISTENCE_FORMATS.md) §6 for the full mapping tables.

## Standalone by contract

The Arena depends on the contract, not on the sibling libraries. None of `nirs4all`, `dag-ml`, `dag-ml-data`, `nirs4all-io`, or `nirs4all-datasets` is imported at ingest or query time — the adapters parse the *serialized* output (workspace SQLite/Parquet, dag-ml bundle JSON, `.n4a` ZIP) rather than calling the producing library. Concretely:

- Identity is **self-contained**: `pipeline_dag_hash` is the Arena's own Merkle hash, requiring no producer wheel; any engine `graph_fingerprint` is recorded for verification/drift only.
- Scores are **re-derivable**: `scoring/metrics.py` reimplements metrics in NumPy so the store never depends on a producer's metric implementation (and supplies classification metrics dag-ml lacks).
- The `nirs4all-datasets` catalog is **optional**: `datasets/catalog.py` degrades to a deterministic mock card if neither a sibling checkout nor the installed package is present.
- FastAPI/uvicorn are an **optional extra**: the core store, ingestion, and CLI work without them; `service/app.py` raises a clear error only when the web service is actually requested.

The result is graceful degradation in every direction: a positional-keyed nirs4all run still ingests (flagged degraded); an export without a graph still gets a stable identity (single opaque node); a private dataset still contributes scores and a fingerprint without exposing data.

## The four versioned contracts

The Arena tracks four independent schema versions; fingerprints are comparable only within matching versions, and each ingested export records the producing versions in its `producer` block (persisted on `executions`).

| Contract | Where it lives | Constant / field |
|---|---|---|
| Arena store schema | `store/schema.sql`, stamped in `PRAGMA user_version` | `ARENA_SCHEMA_VERSION` (`__init__.py`) |
| `ArenaRunExport` wire contract | `contract/schema/arena_run_export.schema.json` + pydantic models | `ARENA_EXPORT_SCHEMA_VERSION` / `arena_export_schema_version` |
| Sample-keyed residuals Parquet | `contract/schema/__init__.py` (`RESIDUALS_PARQUET_COLUMNS`) | `RESIDUALS_SCHEMA_VERSION` |
| Producer contracts | recorded per export (`producer` block) | `nirs4all_version`, `dag_ml_version`, `dag_ml_data_version`, `io_version` |

All three Arena-owned versions are currently `1`. Bumping one signals an incompatible on-disk or wire change. Invalidation is explicit and never destructive: a metric fix supersedes a `ScoreSet`; a leakage/policy change flips a `validity_status` (`valid` → `quarantined`/`invalidated`); rows are never deleted. See [`../DATA_MANAGEMENT.md`](../DATA_MANAGEMENT.md) §8 for drift handling.

## Related documents

These docs are the authoritative "why"; this file is the "how it is built":

- [`../DESIGN.md`](../DESIGN.md) — conceptual entity model, the `RunCondition` tuple, the no-artifacts rule, SQLite+Parquet storage.
- [`../DATA_MANAGEMENT.md`](../DATA_MANAGEMENT.md) — the identity spine, the `ArenaRunExport` contract, the ingestion state machine, the serving model.
- [`../PERSISTENCE_FORMATS.md`](../PERSISTENCE_FORMATS.md) — how nirs4all / dag-ml / nirs4all-io serialize, and how the adapters consume them.
