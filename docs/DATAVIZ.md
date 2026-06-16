# The Arena dataviz web app

The Arena ships a self-contained, no-build single-page app (SPA) for exploring a benchmark store
visually. It is plain ES-module JavaScript plus a vendored Plotly bundle, served as static files by
the same FastAPI process that exposes the read API. There is no transpiler, no bundler, and no
`node_modules` — the browser loads the modules directly.

## Contents

- [How to run it](#how-to-run-it)
- [Architecture](#architecture)
- [Design system](#design-system)
- [The views](#the-views)
  - [Overview](#overview)
  - [Leaderboard](#leaderboard)
  - [Pipeline × Dataset matrix](#pipeline--dataset-matrix)
  - [Parameter effect](#parameter-effect)
  - [Operator effect](#operator-effect)
  - [Robustness](#robustness)
  - [Run explorer](#run-explorer)
  - [Residual compare](#residual-compare)
  - [Datasets](#datasets)
  - [Pipelines](#pipelines)
  - [Upload](#upload)
  - [Run detail](#run-detail)
- [The REST API behind the views](#the-rest-api-behind-the-views)
- [Cross-view state](#cross-view-state)

## How to run it

The web service needs the optional `service` extra (FastAPI + uvicorn):

```bash
pip install 'nirs4all-benchmarks[service]'
```

Start the server with the CLI (`n4a-benchmarks` and `n4a-arena` are the same entry point):

```bash
# serve the SPA + API over an existing store
n4a-benchmarks serve --store ./arena-store

# defaults: host 127.0.0.1, port 8000
n4a-benchmarks serve -s ./arena-store --host 0.0.0.0 --port 8080 --reload
```

`serve` exports `NIRS4ALL_BENCHMARKS_STORE` to the resolved store path and runs
`nirs4all_benchmarks.service.app:create_app` via uvicorn (factory mode). The SPA is then at the
server root (`http://127.0.0.1:8000/`) and the JSON API under `/api/*`.

If the store is empty, seed the synthetic fixture grid first so the views have something to draw:

```bash
n4a-benchmarks fixtures --store ./arena-store
n4a-benchmarks serve   --store ./arena-store
```

You can also build the ASGI app directly (e.g. behind your own uvicorn/gunicorn):

```python
from nirs4all_benchmarks.service.app import create_app

app = create_app("./arena-store")  # or read NIRS4ALL_BENCHMARKS_STORE / default ./arena-store
```

## Architecture

The whole front end is static files under `src/nirs4all_benchmarks/web/`, mounted at `/` by
`create_app` with `StaticFiles(..., html=True)`:

```
web/
  index.html        # shell: header, side-nav slot, view root, loads Plotly + app.js
  styles.css        # the entire design system (CSS custom properties, no preprocessor)
  app.js            # router, view registry, shared state, nav rendering
  vendor/
    plotly.min.js   # vendored Plotly (~1.3 MB), loaded globally as window.Plotly
  lib/
    api.js          # fetch wrapper + one method per /api endpoint
    dom.js          # el / mount / table / controls / badge / fmt helpers (no framework)
    plot.js         # Plotly theme: brand palette, base layout, draw()/purge()
  views/
    overview.js leaderboard.js matrix.js param-effect.js operator-effect.js
    robustness.js runs.js compare.js datasets.js pipelines.js upload.js run-detail.js
```

Key facts:

- **No build step.** `index.html` loads `app.js` as a native `<script type="module">`; every view is
  a default-exported ES module imported by `app.js`. Editing a `.js` file and refreshing is the whole
  dev loop.
- **Plotly is vendored, not bundled.** `index.html` loads `/vendor/plotly.min.js` with `defer`, which
  defines `window.Plotly`. `lib/plot.js` calls `window.Plotly.react(...)`; if it is missing it writes
  `"Plotly failed to load."` into the node rather than throwing.
- **Hash router.** `app.js` reads `location.hash` (`#/<view>/<param>`), looks the view up in the
  registry, and calls `view.render(ctx)`. The context object passed to every view is
  `{ root, api, dom, plot, state, param, navigate }`.
- **Side nav is generated** from a fixed group list in `app.js`: `Explore` (overview, leaderboard,
  matrix), `Effects` (param-effect, operator-effect, robustness), `Runs` (runs, compare), `Catalog`
  (datasets, pipelines), `Contribute` (upload). The `run` (run-detail) view is `hidden` — it is only
  reachable by navigation, not listed in the nav.
- **Header pill** shows live counts: `<valid>/<total> runs · <pipelines> pipelines · <datasets>
  datasets`, refreshed from `/api/overview` on boot and after an upload.

## Design system

The design system lives entirely in `styles.css` as CSS custom properties — there is no Tailwind,
no Sass, no JS theming layer. It honors `prefers-color-scheme: dark` with a second `:root` block.

**Brand palette** (the nirs4all green + accent):

| Token | Value | Use |
|---|---|---|
| `--brand` | `#00704A` | primary green: logo mark, active nav, best bars, model nodes |
| `--brand-d` / `--brand-l` | `#00583a` / `#1b8e63` | darker / lighter green |
| `--accent` | `#E9362D` | accent red (also the low end of the diverging residual scale) |
| `--teal` `--cyan` `--indigo` `--amber` `--green` | `#0d9488` … `#10b981` | categorical series / role colors |
| `--paper` `--bg` `--surface` `--border` | warm paper neutrals | page, cards, lines |
| `--tier-*` | green / amber / red / indigo | dataset privacy tiers (public/restricted/private/anonymized) |

**Fonts** (loaded from Google Fonts in `index.html`):

- `--display` **IBM Plex Sans** — headings, stat values, the logo.
- `--font` **Inter** — body text, controls, tables.
- `--mono` **JetBrains Mono** — hashes, numeric table cells, the header pill, code.

Plotly figures reuse this theme through `lib/plot.js`: `baseLayout()` sets transparent backgrounds,
the Inter font, brand grid colors pulled from the CSS variables, and `colorway = palette`. There are
three shared color sets:

- `palette` — 8 categorical colors starting at brand green, used for per-dataset / per-role series.
- `sequential` — green → paper, for the score heatmap (green = low/better; the matrix view flips it
  when the metric direction is "max").
- `diverging` — red → paper → green, for residual-colored scatters.

The Plotly config disables the logo and trims the mode bar (`lasso2d`, `select2d`, `autoScale2d`
removed), keeping charts responsive.

## The views

Every view is an object `{ id, title, subtitle, icon, render(ctx) }`. Most follow the same reactive
pattern: a `page-head`, a `dom.controls([...])` bar wired to a `refresh()` closure, and a card holding
a `.plot` node that `plot.draw()` fills. The shared controls are `Metric` (from
`overview.metrics`), `Score level` / scope (`cv`, `test`, `refit`, `fold`), and where relevant a
`Dataset` selector and an `Include quarantined` toggle.

### Overview

**Question:** what is in this store, and what is the best pipeline on each dataset?

The landing dashboard (`#/overview`). A four-column grid of stat cards from `/api/overview`
(`datasets`, `pipelines`, `run_conditions`, `valid/total executions`, `residual_sets`, `score_sets`,
`operators`, `quarantined`). Below it: a **"Best pipeline per dataset"** table (it queries
`/api/leaderboard?limit=1` per dataset and shows the top pipeline by mean cv RMSE — clicking a row
sets the dataset filter and jumps to the leaderboard) and a **"Runs per dataset"** Plotly bar chart of
run-condition counts per dataset.

### Leaderboard

**Question:** which pipelines rank best by a chosen metric, at a chosen score level, on a chosen
dataset?

Routed at `#/leaderboard`. Controls: metric, score level, dataset, include-quarantined. The chart is
a **horizontal bar** of the top 18 pipelines (best at top), with the per-fold min–max drawn as
asymmetric error bars and the single best bar painted brand green. A ranked table below lists every
matching pipeline with model, mean, best/worst fold, and observation count; the header shows the
metric direction (`↑ better` / `↓ better`). There is no canonical baseline — ranking is whatever the
selected metric/scope says. Clicking a row opens the run explorer filtered to that
`pipeline_dag_hash`.

### Pipeline × Dataset matrix

**Question:** how does every pipeline score on every dataset at a glance, and which pairs were never
run?

Routed at `#/matrix`. A **Plotly heatmap** with pipelines on the y-axis and datasets on the x-axis;
each cell is the mean metric for that pipeline×dataset pair. Gaps (`null`) mean the pipeline was never
run on that dataset and are rendered as holes (`hoverongaps: false`). The color scale is the brand
green→paper sequential scale, reversed automatically when the metric direction is "max" so green
always means "better."

### Parameter effect

**Question:** how does one hyperparameter (e.g. PLS `n_components`) move the score?

Routed at `#/param-effect`. A `Parameter` selector is populated from `/api/parameters` (the store's
sweepable parameters; defaults to `n_components` when present). For **numeric** parameters it draws,
per dataset, a translucent scatter of every run plus a bold per-x mean line — exposing U-curves and
trends; for **categorical** parameters it draws one box per distinct value. An aggregated table lists
each distinct value with its run count and mean metric.

### Operator effect

**Question:** does the presence of a given operator in the pipeline tend to help or hurt?

Routed at `#/operator-effect`. A **horizontal box plot**, one box per operator, over every run in
which that operator appears, colored by the operator's role. Boxes are sorted with the best operator
on top (direction-aware). A companion table reports per-operator role, run count, mean, median, and
standard deviation.

### Robustness

**Question:** which pipeline×dataset pairs are both accurate and stable across folds/seeds/splits?

Routed at `#/robustness`. A **scatter** of mean metric (x) versus its standard deviation (y), one
point per pipeline×dataset, one color per dataset, with marker size scaling with the number of
observations. The **bottom-left corner is the robust corner** (low score + low variance), annotated
on the plot. The table adds a coefficient-of-variation column (`CV %`); clicking a row opens the run
explorer for that pipeline.

### Run explorer

**Question:** what does the full population of individual runs look like, and where is any single
run?

Routed at `#/runs`, or `#/runs/<pipeline_dag_hash>` to pre-filter to one pipeline (with a "Clear
filter" banner). Controls add an `Operator` free-text search (dotted path) and default
include-quarantined to on. The chart is a **histogram** of the chosen metric across all matching
runs. The table lists each run with metric value, pipeline, model, dataset, a validity badge
(valid / quarantined / invalid), the producer capsule, and wall time; clicking a row opens the run
detail.

### Residual compare

**Question:** do two models err on the *same* samples, or are they complementary?

Routed at `#/compare`. This is the showcase view for **sample-keyed residuals**. You pick Run A,
Run B, and a partition (`validation` / `test`). The service joins the two runs' residuals **by
`sample_id`** (`/api/compare`) and returns only paired samples. The chart is a square **scatter of
residual A (x) vs. residual B (y)**, with a y=x diagonal and zero-axes drawn in; each point is
colored by its observed value (diverging scale) and labeled by `sample_id`. Points off the diagonal
are samples where the two models disagree — i.e. **complementary models** worth ensembling. Stat
cards report the number of common/paired samples, the **residual correlation** (low or negative =
complementary), and each run's RMSE on the common set.

This complementarity analysis is only possible because residuals are stored keyed by sample
identity, not by row position. Without sample-keyed residuals the two runs could not be aligned and
the view falls back to "No paired sample-keyed residuals for these runs."

### Datasets

**Question:** what data regimes are benchmarked here?

Routed at `#/datasets`. A card grid from `/api/datasets`, one card per dataset fingerprint, showing a
privacy-tier badge (color-coded public/restricted/private/anonymized) and a key-value sheet: domain,
modality, signal type, sample/feature counts, spectral axis range and unit, task type, run-condition
count, and the truncated fingerprint. Clicking a card sets the dataset filter and jumps to the
leaderboard.

### Pipelines

**Question:** what distinct pipeline identities exist, regardless of how they were authored?

Routed at `#/pipelines`. A filterable, sortable table from `/api/pipelines`, one row per
`pipeline_dag_hash` — the topology-aware Merkle hash, so two equivalent pipelines collapse to a
single row. Columns: human label, main model, node count, structure (linear vs. branching), and
run-condition count. A free-text filter matches label/model/hash and a `Structure` selector filters
linear vs. branching. Clicking a row opens the run explorer for that pipeline.

### Upload

**Question:** how do I contribute a run to this store from the browser?

Routed at `#/upload`. A form that ingests a **weights-free `ArenaRunExport` manifest** (JSON): choose
a `.json` file or paste the manifest, set a collection (default `uploads`), optionally tick "Ingest as
benchmark release (quarantine on leakage)", and POST it to `/api/ingest`. The result panel shows the
ingestion `status` (committed / already_ingested / quarantined / rejected), validity, the
`run_condition_hash` / `execution_hash` / `arena_export_hash`, the clean-report counts (new
dimensions, facts written, residual rows, residual key, whether scores were recomputed), notes, and
all issues with severity badges. On success it offers a button to open the new run's detail page. The
copy makes the contract explicit: only run conditions, scores, and residual summaries are stored —
fitted models are never uploaded or reconstructed.

### Run detail

**Question:** what exactly was run for one execution, and how did it score per fold and per sample?

Routed at `#/run/<execution_hash>` (hidden from the nav; reached from leaderboard / runs /
robustness / upload). It renders, from `/api/run/<hash>` and `/api/run/<hash>/residuals`:

- a hand-drawn **SVG DAG** of the pipeline (nodes laid out by longest-path depth, colored by role:
  input / transform / model / merge / mean / stacking / pipeline);
- a **run-condition** key-value card (pipeline_dag_hash, run_condition_hash, dataset, CV instance,
  RNG seed, refit strategy, nirs4all identity, engine graph fingerprint), with a validity badge;
- a **scores** table across all metric / scope / partition / fold rows;
- a **per-fold RMSE** bar chart;
- a **predicted vs. observed** scatter on the validation partition (with a y=x reference), drawn from
  sample-keyed residuals, falling back to "No sample-keyed residuals for this run." when absent.

## The REST API behind the views

The SPA never talks to SQLite directly; every view calls a thin client in `lib/api.js` that maps to
the endpoints in `service/app.py`. All read endpoints are GET and return JSON.

| Endpoint | Used by | Notable query params |
|---|---|---|
| `GET /api/healthz` | (status) | — |
| `GET /api/overview` | overview, header pill, all views | — |
| `GET /api/datasets` | datasets, overview, filters | — |
| `GET /api/pipelines` | pipelines | — |
| `GET /api/operators` | (operator filters) | — |
| `GET /api/parameters` | param-effect | — |
| `GET /api/leaderboard` | leaderboard, overview | `metric`, `scope`, `dataset`, `task`, `collection`, `include_quarantined`, `limit` |
| `GET /api/matrix` | matrix | `metric`, `scope`, `include_quarantined` |
| `GET /api/runs` | run explorer, compare | `metric`, `scope`, `dataset`, `pipeline`, `operator`, `include_quarantined`, `limit` |
| `GET /api/operator-effect` | operator-effect | `metric`, `scope` |
| `GET /api/parameter-effect` | param-effect | `param` (required), `metric`, `scope` |
| `GET /api/robustness` | robustness | `metric`, `scope` |
| `GET /api/run/{execution_hash}` | run detail | — |
| `GET /api/run/{execution_hash}/residuals` | run detail, compare | `partition` |
| `GET /api/compare` | residual compare | `a`, `b` (execution hashes), `partition` (default `validation`) |
| `POST /api/ingest` | upload | body = `ArenaRunExport`; query `collection`, `as_release` |

Example calls the SPA makes:

```bash
# top 200 pipelines by cv RMSE on every dataset
curl 'http://127.0.0.1:8000/api/leaderboard?metric=rmse&scope=cv&limit=200'

# the score heatmap
curl 'http://127.0.0.1:8000/api/matrix?metric=rmse&scope=cv'

# sample-keyed residual comparison of two executions
curl 'http://127.0.0.1:8000/api/compare?a=<exec_a>&b=<exec_b>&partition=validation'

# upload a weights-free manifest into the "uploads" collection
curl -X POST 'http://127.0.0.1:8000/api/ingest?collection=uploads' \
     -H 'Content-Type: application/json' \
     --data-binary @run-export.json
```

## Cross-view state

`app.js` keeps a small shared state object — `metric`, `scope`, and the selected `dataset`
fingerprint — persisted to `localStorage` under `arena.state.v1`. Picking a dataset on the overview
or datasets card, or changing the metric/scope on one view, carries over to the others. The cached
overview is also reused so a freshly loaded view does not re-fetch counts it already has.
