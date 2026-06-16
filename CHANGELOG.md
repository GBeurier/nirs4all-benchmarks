# Changelog

All notable changes to `nirs4all-benchmarks` (the Arena) are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Second + third iterations — richer ingestion surface, deep faceted dataviz, the
ecosystem visual identity, and graph/3D visualizations.

### Added (iteration 6 — static GitHub Pages site + roadmap)

- **Static, client-side deployment.** `n4a-benchmarks build-site` snapshots a store to JSON and copies
  the SPA; a new client-side engine (`web/lib/static-engine.js`) answers every query in the browser
  from that snapshot (verified at parity with the Python `Queries`), so the whole dataviz runs on
  **GitHub Pages with no backend**. A `pages.yml` workflow builds + deploys to
  **benchmarks.nirs4all.org** (CNAME); `config.js` flips the SPA into static mode.
- **Prototype notice + roadmap everywhere** — a dismissible prototype banner, and a documented plan
  ([docs/ROADMAP.md](ROADMAP.md), README, deployment docs): *static client-side now → a live server
  managing runs / pipelines / meta-analyses with `nirs4all-repository` & `nirs4all-datasets` next.*
- **Fixed** a real `operator_effect` bug (it counted the `input` pseudo-operator `X`); now excluded,
  matching `operator_graph`/`composition`.

### Changed (iteration 5 — meta-analysis UX overhaul)

- **Single shared lens** — a sticky context bar (Metric · Score level · Dataset) now drives **every**
  analysis view; the per-view metric/scope/dataset selectors were removed (major declutter). Set the
  lens once; the whole app follows.
- **Meta-analysis IA** — nav reorganized into a narrative (Start · Rankings · Drivers · Structure ·
  Runs · Catalog · Contribute) and every view reframed around the question it answers.
- **Overview is now a findings dashboard** — headline insights (top pipeline, best model family, best
  preprocessing, augmentation effect, split sensitivity, most robust), each linking to the relevant view.
- **Professional charts** — centralized Plotly theme (muted grid, readable hover, brand colorway),
  small consistent markers (no more oversized dots), tidy legends/colorbars.
- **Fixed Composition** — the sunburst/treemap rendered blank because the `branchvalues:"total"` root
  value was 0; the root now totals its children.

### Added (iteration 4 — stats, more 3D, polish, prototype/version)

- **Statistics view** + `/api/stats` (`Queries.stats`) — score distribution (histogram), spread per
  dataset (violin), and Pearson correlation of every numeric facet with the score, plus summary tables.
- **More 3D** — Landscape gains a **Z-axis facet** for a true 3-facet 3D scatter (x × y × z, colored
  by the metric), alongside the 2-facet surface.
- **Official logos** — nav, favicon, OG image and footer now use the canonical ecosystem brand set
  (`nirs4all-org/assets/brand/nirs4all-benchmarks/`), wired like the other ecosystem pages.
- **Prototype notice + version tracking** — a "prototype" badge in the nav + footer, a version chip
  fed from `/api/healthz` (VERSION → `version.py` → `__version__`), and a footer with version,
  ecosystem links, changelog, and license. Mirrors the `version-guard` CI guardrail.
- **Serious polish pass** — refined cards/hover, footer, skeleton shimmer, unobtrusive Plotly modebar,
  gradient hero, and consistent ecosystem theming.

### Added (iteration 3 — identity + graphs)

- **Ecosystem restyle** — the SPA now wears the **nirs4all.org** identity: teal `#0d9488`
  primary (+ cyan/green/indigo/amber), warm paper background with grid ruling + aurora glow,
  glassmorphic nav, IBM Plex Sans / Inter / JetBrains Mono, gradient-clip headings, ecosystem nav
  links, and a gradient hero on the overview.
- **3D + graph dataviz** — three new views: **Landscape (3D)** (Plotly `scatter3d`/`surface` of the
  score over two facets), **Composition** (sunburst/treemap of stage-role → operator usage, colored
  by score), and **Network** — a clustered **mega-graph** (Cytoscape, force-directed) of pipelines
  (edges = shared-operator Jaccard, clusters = model family) and operators (edges = co-occurrence,
  clusters = stage role).
- **Graph/composition analytics** — `Queries.pipeline_graph/operator_graph/composition` +
  `/api/graph` and `/api/composition`. Plotly upgraded to the full build; Cytoscape.js vendored.

### Added (iteration 2 — ingestion + faceting)

- **Unified upload + run/store/display state machine** (`ingestion/upload.py`, `POST /api/upload`,
  CLI `ingest-pipeline`): auto-detects a `.n4a` bundle (with **or** without fitted artifacts —
  weights stripped), a raw nirs4all pipeline as a Python list / JSON / **YAML**, a dag-ml
  `ExecutionBundle`, or an `ArenaRunExport`. Results-bearing inputs are ingested; bare pipelines are
  registered and **planned** against target datasets, with already-run detection per pipeline×dataset.
- **`planned_runs`** table + `/api/planned` + a Planned view — the not-yet-run state (a runner
  fulfils a plan and ingests the result; the Arena never runs compute).
- **Role-aware indexing** (`indexing.py`) — every node is classified
  (preprocessing / augmentation / scaler / feature-selection / model / merge) and materialized into a
  long **`run_facets`** table, so the benchmark is groupable by *any* dimension.
- **Faceted analytics** — `Queries.facets/facet_values/pivot/parallel` + `/api/facets`,
  `/api/pivot`, `/api/parallel`.
- **New dataviz** — a **Playground** (pivot/group-by any facet: split × aug × pp × model × param;
  bars / line / heatmap), **Parallel coordinates** across dimensions → score, a Planned view, and a
  rewritten **Upload** view driving the unified endpoint. Plotly upgraded to the full build
  (parcoords/violin).
- **Rich demo fixtures** (`generate_demo_exports`, `n4a-benchmarks fixtures` default) — a ~128-run
  factorial over datasets × seeds × splits × {preprocessing chains, augmentation on/off, models,
  parameter sweeps} so the playground has a real space to explore. The small deterministic set
  (`--basic`) remains the frozen regression contract.

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
