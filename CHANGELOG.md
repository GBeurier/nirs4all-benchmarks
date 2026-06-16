# Changelog

All notable changes to `nirs4all-benchmarks` (the Arena) are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-06-16

First implementation — the Arena goes from design docs to a working v1.

### Added

- **Identity spine** (`identity/`): canonical-JSON SHA-256 hashing, the six-field
  `run_condition_hash` composition, and a topology-aware, producer-agnostic
  `pipeline_dag_hash` (Merkle hash over a normalized graph; id/order-invariant; order-insensitive
  merge handling; linear-list lifting; records `engine_graph_fingerprint` and nirs4all `get_hash`).
- **Ingestion contract** (`contract/`): the frozen `ArenaRunExport` v1 — pydantic models + an
  authoritative JSON Schema + the sample-keyed `residuals.parquet` column contract.
- **Store** (`store/`): `arena.sqlite` (full DESIGN §6/§7 dimension + fact schema, dedup-by-hash
  upserts, forward-version guard) + `ResidualStore` (sample-keyed Zstd Parquet) + a `Queries`
  serving facade (leaderboard, matrix, operator/parameter effect, robustness, run detail,
  residual compare). **No artifacts are ever stored.**
- **Ingestion pipeline** (`ingestion/`): the verify → dedup → resolve → validate → pseudonymize →
  strip → commit → report state machine; idempotent; leakage-honest (quarantine for releases);
  store-global pseudonymization; score recomputation with supersede.
- **Dual-compatibility adapters** (`adapters/`): Adapter A (nirs4all workspace), Adapter B
  (dag-ml `ExecutionBundle`), and `.n4a` recipe extraction (weights stripped).
- **Scoring** (`scoring/`): NumPy-only regression + classification metrics, versioned
  `ScoreComputationSpec`.
- **Datasets** (`datasets/`): `DatasetCard` / `DatasetFingerprint` models + an optional
  `nirs4all-datasets` catalog loader and a deterministic mock.
- **Service** (`service/`): a FastAPI app exposing the full query API + `POST /api/ingest`, serving
  the dataviz SPA.
- **Dataviz web app** (`web/`): a no-build vanilla-JS + Plotly SPA — overview, leaderboard,
  pipeline × dataset matrix, parameter/operator effect, robustness, run explorer, residual
  complementarity compare, dataset/pipeline catalogs, DAG-rendering run detail, and upload.
- **CLI** (`n4a-benchmarks` / `n4a-arena`): init, ingest-export, ingest-workspace, ingest-bundle,
  inspect-n4a, fixtures, stats, leaderboard, serve.
- **Fixtures**: a deterministic 2-dataset × {PLS sweep, branch/merge, stacking} grid with synthetic
  OOF residuals, with frozen `run_condition_hash`es as a regression contract.
- **Docs**: ARCHITECTURE, CONTRACT, IDENTITY, INGESTION, ADAPTERS, API, DATAVIZ, DEPLOYMENT, CLI.
- **Ops**: Dockerfile, docker-compose, GitHub Actions CI (green gate × Python 3.10–3.12 +
  frontend syntax check + Docker smoke test).

[0.1.0]: https://github.com/GBeurier/nirs4all-benchmarks/releases/tag/v0.1.0
