<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/brand/horizontal-dark.svg">
    <img alt="nirs4all-benchmarks" src="assets/brand/horizontal.svg" width="440">
  </picture>
</p>

# nirs4all-benchmarks

**Reproducible, scored, weights-free benchmarks — and best-in-class dataviz — for NIRS pipelines.**

`nirs4all-benchmarks` (codename *the Arena*) is the reference environment for storing, comparing and
exploring the performance of NIRS pipelines across the
`model × pipeline × split × cv × rng × refit × dataset` space. It ingests runs produced by
[`nirs4all`](../nirs4all) and/or [`dag-ml`](../dag-ml), keys everything by content hashes,
and serves an interactive web app to explore the results.

It stores identity cards, canonical pipeline DAGs, versioned scores, and **sample-keyed residuals** —
**never fitted artifacts**.

> **nirs4all-benchmarks** is the public package name; the project is referred to as *the Arena*
> internally (design docs, code, the engine output contract). Same project.

Part of the [nirs4all ecosystem](https://github.com/GBeurier/nirs4all-ecosystem).

---

## What it does

- **Dual producer compatibility.** One ingestion contract, two producers: a live adapter for the
  **nirs4all workspace** (`store.sqlite` + `arrays/*.parquet`) and an adapter for the **dag-ml
  `ExecutionBundle`**. `.n4a` bundles are accepted as pipeline uploads with the weights stripped.
- **Content-addressed identity.** A topology-aware `pipeline_dag_hash` (a Merkle hash over the
  canonical graph) dedups "same pipeline, different syntax" across producers; the full
  `run_condition_hash` is composed from six dimension hashes. Producer UUIDs are never join keys.
- **Versioned, re-derivable scores.** Every score carries a `ScoreComputationSpec`; metrics
  (regression + classification) are recomputed with NumPy from sample-keyed residuals, so a metric
  fix adds a *new* score set that supersedes the old one — nothing is mutated.
- **Leakage-honest ingestion.** Runs that cannot attest out-of-fold safety are *quarantined*
  (excluded from published views), never silently dropped. Idempotent: re-ingesting an export is a
  no-op.
- **Top-tier dataviz.** A no-build single-page app (Plotly) with a leaderboard, a pipeline × dataset
  matrix, operator/parameter effect explorers, a robustness view, a sample-keyed **residual
  complementarity** comparator, a DAG-rendering run detail, and an upload page.

See the design docs for the *why*: [DESIGN.md](DESIGN.md) ·
[DATA_MANAGEMENT.md](DATA_MANAGEMENT.md) · [PERSISTENCE_FORMATS.md](PERSISTENCE_FORMATS.md).

## Quickstart

```bash
# install (uv recommended)
uv venv --python 3.11 .venv
uv pip install --python .venv -e ".[service]"

# create a store, seed the demo fixtures, and explore from the terminal
.venv/bin/n4a-benchmarks init      --store ./arena-store
.venv/bin/n4a-benchmarks fixtures  --store ./arena-store
.venv/bin/n4a-benchmarks stats     --store ./arena-store
.venv/bin/n4a-benchmarks leaderboard --store ./arena-store --metric rmse

# launch the dataviz web app  ->  http://127.0.0.1:8000
.venv/bin/n4a-benchmarks serve --store ./arena-store
```

Ingest real runs:

```bash
# from a nirs4all workspace (artifacts ignored)
n4a-benchmarks ingest-workspace /path/to/workspace --store ./arena-store

# from a dag-ml ExecutionBundle (+ optional graph / io envelope)
n4a-benchmarks ingest-bundle bundle.json --graph graph.json --store ./arena-store

# an ArenaRunExport manifest (the freeze contract), or a directory of them
n4a-benchmarks ingest-export run.json --store ./arena-store --release
```

With Docker:

```bash
docker compose up --build           # serves on :8000 with a persistent volume
docker compose run --rm arena n4a-benchmarks fixtures   # seed demo data once
```

## Documentation

| Doc | What |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture, module map, data flow |
| [docs/CONTRACT.md](docs/CONTRACT.md) | The `ArenaRunExport` v1 ingestion contract (frozen) |
| [docs/IDENTITY.md](docs/IDENTITY.md) | The identity spine + `pipeline_dag_hash` |
| [docs/INGESTION.md](docs/INGESTION.md) | The ingestion state machine |
| [docs/ADAPTERS.md](docs/ADAPTERS.md) | Dual compatibility: nirs4all / dag-ml / `.n4a` |
| [docs/API.md](docs/API.md) | REST API + Python `Queries` reference |
| [docs/DATAVIZ.md](docs/DATAVIZ.md) | The dataviz web app |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Running the service online + persistently |
| [docs/CLI.md](docs/CLI.md) | `n4a-benchmarks` command reference |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Dev setup + green gate |

## Architecture at a glance

```
producer run ─► ArenaRunExport (weights-free, content-addressed)
                     │   adapters: nirs4all workspace · dag-ml bundle · .n4a
                     ▼
            INGEST  (verify · dedup · resolve ids · validate · pseudonymize · strip · commit)
                     ▼
   arena.sqlite (dimensions + facts)  +  arrays/residuals_<hash>.parquet (sample-keyed)
                     ▼
            Queries  ─►  FastAPI  /api/*  ─►  Plotly SPA
```

The Arena is **standalone by contract**: it needs only the `ArenaRunExport` bundle. The sibling
libraries (`nirs4all`, `dag-ml`, `nirs4all-io`, `nirs4all-datasets`) are optional — every path
degrades gracefully when they are absent.

## License

Pipeline **code** is dual-licensed open-source — **`CeCILL-2.1 OR AGPL-3.0-or-later`** — with an optional
**commercial license** (for any commercial use, contact <nirs4all-admin@cirad.fr>). Scored **results** and
leaderboards are content (**CC-BY-4.0**). See [`LICENSING.md`](LICENSING.md) and [`LICENSES/`](LICENSES/).
