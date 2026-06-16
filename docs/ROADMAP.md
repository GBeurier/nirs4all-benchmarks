# Roadmap & status

> **Status: early prototype.** Schemas, scores, views and the public page may still change.

`nirs4all-benchmarks` (the Arena) is the reference environment for storing, scoring and exploring
NIRS pipeline performance. It is being built in stages.

## Now — static, client-side (v0.1.x)

The public page is a **fully static, client-side snapshot** deployed on **GitHub Pages** at
**[benchmarks.nirs4all.org](https://benchmarks.nirs4all.org)** — no backend required:

- `n4a-benchmarks build-site` exports a store to a JSON snapshot (`data/bundle.json` + per-run files)
  and copies the no-build SPA next to it.
- A client-side engine (`web/lib/static-engine.js`) answers every query in the browser from that
  snapshot — it mirrors the Python `Queries` facade and is verified for parity. So the whole dataviz
  (leaderboard, matrix, playground, parallel coordinates, 3D landscape, sunburst, clustered network
  graphs, statistics, residual comparison) runs entirely in the browser, like the rest of the
  nirs4all ecosystem's client-side pages.
- The full **FastAPI service** (`n4a-benchmarks serve`) runs locally for ingestion, real stores, and
  the live `/api` + upload state machine.

What is *not* in the static demo: ingestion / upload (those need the local service), and the data is
the synthetic demo fixtures (a factorial over preprocessing × augmentation × model × params × split ×
dataset) — enough to exercise every view, not a real benchmark yet.

## Next — a live meta-analysis server (medium term)

The static page is the front door for a **persistent, hosted service** that manages **runs,
pipelines and meta-analyses** and interacts with the rest of the ecosystem:

- **`nirs4all-repository`** — the public library of pre-configured, tested nirs4all pipelines. The
  Arena pulls recipes to score, and publishes their benchmarked performance back as a browsable
  resource.
- **`nirs4all-datasets`** — the curated, DOI-pinned dataset catalog. The Arena resolves
  `DatasetCard`s / fingerprints from it and benchmarks pipelines against those datasets.
- The hosted server keeps the persistent SQLite + Parquet store, accepts uploads (`.n4a` / pipeline
  JSON-YAML / dag-ml bundle / `ArenaRunExport`), runs the run/store/display state machine, schedules
  or ingests runs, and serves the same dataviz over live data.

This turns the current static demo into a continuously-updated public benchmark, with the SPA
unchanged (it already speaks the same query API, whether backed by the static snapshot or the live
service).

## Underlying design phases

The store / ingestion / scoring / dataviz build order is detailed in the design docs:
[DESIGN.md](../DESIGN.md) (conceptual model + Arena store schema), [DATA_MANAGEMENT.md](../DATA_MANAGEMENT.md)
(the `ArenaRunExport` contract + identity spine), and [PERSISTENCE_FORMATS.md](../PERSISTENCE_FORMATS.md)
(nirs4all / dag-ml / nirs4all-io formats the Arena ingests).
