# The Arena dataviz web app

The Arena ships a self-contained, no-build single-page app (SPA) for exploring a benchmark store
visually. It is plain ES-module JavaScript plus two vendored libraries (the **full Plotly build** and
**Cytoscape.js**), served as static files by the same FastAPI process that exposes the read API. There
is no transpiler, no bundler, and no `node_modules` — the browser loads the modules directly.

The view set is **2D + 3D + network**: classic charts (bars, lines, heatmaps, scatters, box plots,
histograms, parallel coordinates), a 3D score landscape (`scatter3d` / `surface`), a sunburst/treemap
composition view, and a clustered Cytoscape mega-graph.

## Contents

- [How to run it](#how-to-run-it)
- [Architecture](#architecture)
- [Role-aware faceting backbone](#role-aware-faceting-backbone)
- [Design system](#design-system)
- [The views](#the-views)
  - [Overview](#overview)
  - [Leaderboard](#leaderboard)
  - [Pipeline × Dataset matrix](#pipeline--dataset-matrix)
  - [Playground](#playground)
  - [Parameter effect](#parameter-effect)
  - [Operator effect](#operator-effect)
  - [Parallel coordinates](#parallel-coordinates)
  - [Robustness](#robustness)
  - [Landscape (3D)](#landscape-3d)
  - [Composition](#composition)
  - [Network](#network)
  - [Run explorer](#run-explorer)
  - [Residual compare](#residual-compare)
  - [Datasets](#datasets)
  - [Pipelines](#pipelines)
  - [Planned](#planned)
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
  index.html        # shell: header, side-nav slot, view root, loads Plotly + Cytoscape + app.js
  styles.css        # the entire design system (CSS custom properties, no preprocessor)
  app.js            # router, view registry, shared state, nav rendering
  vendor/
    plotly.min.js     # vendored full Plotly build (~4.3 MB), loaded globally as window.Plotly
    cytoscape.min.js  # vendored Cytoscape.js (~365 KB), loaded globally as window.cytoscape
  lib/
    api.js          # fetch wrapper + one method per /api endpoint
    dom.js          # el / mount / table / controls / badge / fmt helpers (no framework)
    plot.js         # Plotly theme: brand palette, base layout, draw()/purge()
    graph.js        # Cytoscape theme: clusterColors() + renderGraph() (cose/concentric/grid)
  views/
    overview.js leaderboard.js matrix.js playground.js param-effect.js operator-effect.js
    parallel.js robustness.js landscape.js composition.js network.js
    runs.js compare.js datasets.js pipelines.js planned.js upload.js run-detail.js
```

Key facts:

- **No build step.** `index.html` loads `app.js` as a native `<script type="module">`; every view is
  a default-exported ES module imported by `app.js`. Editing a `.js` file and refreshing is the whole
  dev loop.
- **Two libraries are vendored, not bundled.** `index.html` loads `/vendor/plotly.min.js` and
  `/vendor/cytoscape.min.js` with `defer`, defining `window.Plotly` and `window.cytoscape`. Plotly is
  the **full build** (it must include `parcoords`, `scatter3d`, `surface`, `sunburst`, and `treemap`
  traces, not just the basic chart types). `lib/plot.js` and `lib/graph.js` both degrade gracefully if
  their library is missing, writing `"Plotly failed to load."` / `"Cytoscape failed to load."` into the
  node rather than throwing.
- **Hash router.** `app.js` reads `location.hash` (`#/<view>/<param>`), looks the view up in the
  registry, and calls `view.render(ctx)`. The context object passed to every view is
  `{ root, api, dom, plot, state, param, navigate }`.
- **Side nav is generated** from a fixed group list in `app.js`:
  - `Explore` — overview, leaderboard, matrix, playground
  - `Effects` — param-effect, operator-effect, parallel, robustness
  - `Graphs` — landscape, composition, network
  - `Runs` — runs, compare
  - `Catalog` — datasets, pipelines, planned
  - `Contribute` — upload

  The `run` (run-detail) view is **not** in any nav group — it is only reachable by navigation, not
  listed in the nav.
- **Header pill** shows live counts: `<valid>/<total> runs · <pipelines> pipelines · <datasets>
  datasets`, refreshed from `/api/overview` on boot (and re-fetchable via `window.__arena.refreshHeader`).

## Role-aware faceting backbone

Most of the new views (playground, parallel, landscape, composition, network) are not bespoke SQL —
they all read the same **role-aware facet index**, the backbone added in iterations 2 and 3.

At ingest, `nirs4all_benchmarks.indexing` classifies each pipeline node into a stage **role** and
materializes a long `run_facets` table — one row per `(run_condition_hash, facet_key, facet_value)`,
with an optional `facet_num` for ordered/continuous facets and a `role` for stage facets. A single
`pipeline_dag_hash` cannot answer "effect of SNV" or "augmentation on vs off"; the facet table can.

- **`classify_role(operator, declared_kind)`** buckets a node into one of
  `preprocessing / augmentation / scaler / feature_selection / model / merge / split / sampler /
  input / other`. Roles declared in the canonical graph kind (`model`, `merge`, `split`, …) win;
  otherwise substring heuristics run over the **leaf** of the dotted entrypoint (the class name), so a
  module segment like `sklearn.ensemble` cannot masquerade a `RandomForestRegressor` as a merge node
  and a `PowerTransformer` is not caught by a neural "transformer" needle. Augmentation is the one role
  checked against the full module path first, since its class names often carry no token.
- **`build_run_facets(resolved, model)`** emits the facet rows for one run condition. Condition-level
  facets: `dataset`, `task_type`, `split_method`, `cv_method`, `n_folds`, `seed`, `refit_strategy`,
  `pipeline`, `is_linear`. Stage facets (presence semantics — a run contributes one row per stage
  operator it contains): `<role>_op` (e.g. `preprocessing_op`, `model_op`), a generic `operator`,
  `merge_strategy`, and `param:<name>` per hyperparameter (numeric params carry `facet_num`).
  Roll-ups: `model`, `model_family`, `n_models`, `n_preprocessing`, `n_augmentation`,
  `has_augmentation`, `has_scaler`, `has_feature_selection`, `n_stages`.

The serving side exposes this via `Queries` (`store/queries.py`): `facets()` lists facet keys with
cardinality + whether they are numeric; `facet_values(key)` lists a key's values; `pivot()` aggregates
the metric grouped by one or two facets; `parallel()` returns per-execution rows with selected facet
columns + the metric; and `composition()` / `pipeline_graph()` / `operator_graph()` reuse the role
classification for the hierarchy and network views. Because facets are presence-keyed, multi-valued
stages (several preprocessings) work correctly: a run is counted under each value of a multi-valued
facet, which is exactly what makes "effect of SNV" and "augmentation on/off" meaningful.

## Design system

The design system lives entirely in `styles.css` as CSS custom properties — there is no Tailwind,
no Sass, no JS theming layer. It is restyled to the **nirs4all.org ecosystem identity** (warm paper +
teal, glass nav, aurora glows) and honors `prefers-color-scheme: dark` with a second `:root` block.

**Palette** (the ecosystem tokens, lifted from nirs4all.org):

| Token | Value | Use |
|---|---|---|
| `--benchmarks` | `#00704a` | the benchmarks brand mark green: logo mark, active nav, best bars, model nodes |
| `--teal` / `--teal-d` / `--teal-l` | `#0d9488` / `#0f766e` / `#2dd4bf` | primary ecosystem teal |
| `--cyan` `--indigo` `--green` `--amber` | `#06b6d4` `#4f46e5` `#10b981` `#d97706` | categorical series / role colors |
| `--accent` | `#e9362d` | accent red (also the low end of the diverging residual scale) |
| `--paper` `--paper-2` `--bg` `--surface` `--border` `--border-warm` | warm paper neutrals | page, cards, lines |
| `--tier-*` | green / amber / red / indigo | dataset privacy tiers (public/restricted/private/anonymized) |

**Fonts** (loaded from Google Fonts in `index.html`):

- `--display` **IBM Plex Sans** — headings, stat values, the logo.
- `--font` **Inter** — body text, controls, tables.
- `--mono` **JetBrains Mono** — hashes, numeric table cells, the header pill, nav group labels, code.

Plotly figures reuse this theme through `lib/plot.js`: `baseLayout()` sets transparent backgrounds,
the Inter font, grid colors pulled from the CSS variables, and `colorway = palette`. There are three
shared color sets:

- `palette` — 8 categorical colors starting at the benchmarks green
  (`#00704A, #0d9488, #06b6d4, #4f46e5, #d97706, #E9362D, #10b981, #8b5cf6`), used for per-series and
  per-cluster colors (the Cytoscape graph reuses it via `clusterColors()`).
- `sequential` — green → paper, for the score heatmaps and the 3D surface (green = low/better;
  callers flip it when the metric direction is "max").
- `diverging` — red → paper → green, for residual-colored scatters.

The Plotly config disables the logo and trims the mode bar (`lasso2d`, `select2d`, `autoScale2d`
removed), keeping charts responsive. The Cytoscape theme (`lib/graph.js`) colors nodes by cluster,
sizes them by a magnitude field, draws haystack edges weighted by edge weight, dims everything but a
node's closed neighbourhood on hover, and offers `cose` / `concentric` / `grid` layouts.

## The views

Every view is an object `{ id, title, subtitle, icon, render(ctx) }`. Most follow the same reactive
pattern: a `page-head`, a `dom.controls([...])` bar wired to a `refresh()` closure, and a card holding
a `.plot` node that `plot.draw()` fills. The shared controls are `Metric` (from `overview.metrics`),
`Score level` / scope (`cv`, `test`, `refit`, `fold`), and where relevant a `Dataset` selector and an
`Include quarantined` toggle.

### Overview

**Question:** what is in this store, and what is the best pipeline on each dataset?

The landing dashboard (`#/overview`). A grid of stat cards from `/api/overview` (`datasets`,
`pipelines`, `run_conditions`, `valid/total executions`, `residual_sets`, `score_sets`, `operators`,
`quarantined`). Below it: a **"Best pipeline per dataset"** table (it queries `/api/leaderboard?limit=1`
per dataset and shows the top pipeline by mean cv RMSE — clicking a row sets the dataset filter and
jumps to the leaderboard) and a **"Runs per dataset"** Plotly bar chart of run-condition counts per
dataset.

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

### Playground

**Question:** how does the score move across *any* dimension I choose — and any second dimension to
split by?

Routed at `#/playground`, the flagship pivot view. It reads `/api/facets` and offers a `Group by` and
optional `Color by` selector over **every facet key** (`split_method`, `has_augmentation`,
`preprocessing_op`, `model_family`, `param:n_components`, …). A `Chart` selector picks **bars** (grouped
when a color facet is set, otherwise single bars with min–max error bars), **line** (one line per color
value; numeric group facets plot on a numeric x), or **heatmap** (requires a color facet; group on x,
color on y, mean metric as z). Group values are ordered numerically when the facet is numeric, else by
aggregated value. A sortable table lists each group/color cell with mean/n/min/max. This is the UI over
`Queries.pivot`, so presence semantics apply: a run contributes to each value of a multi-valued facet,
making "effect of SNV" and "augmentation on/off" answerable directly.

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

### Parallel coordinates

**Question:** what combinations of split / CV / model / parameters lead to good scores?

Routed at `#/parallel`. A Plotly **parcoords** plot that traces every execution across a curated set
of facet dimensions down to the metric. Toggles offer the dimensions that are present in `/api/facets`
(from `model_family`, `cv_method`, `split_method`, `has_augmentation`, `refit_strategy`, `seed`,
`param:n_components`), with the first four checked by default and at least two required. Numeric
facets stay numeric; categorical facets are coded to integer axes with tick labels. The final axis is
the metric itself, and lines are colored by the metric (Viridis, reversed for "min" metrics so darker
= better). It reads `/api/parallel` (`Queries.parallel`), which takes the first value per single-valued
dimension.

### Robustness

**Question:** which pipeline×dataset pairs are both accurate and stable across folds/seeds/splits?

Routed at `#/robustness`. A **scatter** of mean metric (x) versus its standard deviation (y), one
point per pipeline×dataset, one color per dataset, with marker size scaling with the number of
observations. The **bottom-left corner is the robust corner** (low score + low variance), annotated
on the plot. The table adds a coefficient-of-variation column (`CV %`); clicking a row opens the run
explorer for that pipeline.

### Landscape (3D)

**Question:** what does the score surface look like over two dimensions at once?

Routed at `#/landscape`. The **3D** view: `X axis` and `Y axis` selectors each take any facet key, and
a `Chart` selector picks **scatter3d** (one marker per run, z = metric, colored by metric) or
**surface** (rows aggregated by `(x, y)` into a mean-metric grid). It reads `/api/parallel` with the two
chosen dimensions. Numeric facets feed Plotly directly; categorical facets are coded to integers with
tick labels restored on the axes and in hover. The surface uses the brand green→paper sequential scale
(reversed for "max" metrics). It guards the degenerate cases (same facet on both axes; both axes with a
single distinct value).

### Composition

**Question:** how are pipelines built — which stage roles and operators dominate, and how do they score?

Routed at `#/composition`. A **sunburst** or **treemap** (selectable) of a two-level hierarchy — root →
stage **role** (inner ring) → **operator** (outer ring) — sized by usage (`runs` or `pipelines`,
selectable) and colored by mean score. It reads `/api/composition` (`Queries.composition`), which reuses
`classify_role` to assign each operator's role. Role-level color is the usage-weighted mean of its
operators' scores; the color range is taken from the operator leaves; the scale is the sequential brand
scale (reversed for "max"). A companion table lists role / operator / pipeline count / run count / score.

### Network

**Question:** which pipelines and operators cluster together across the whole benchmark space?

Routed at `#/network`, the clustered **Cytoscape mega-graph**. A `Graph` selector switches between:

- **Pipelines (shared operators)** — nodes are pipelines, clustered by model family, sized by run count,
  colored by mean score; an edge links two pipelines whose operator sets exceed a chosen Jaccard
  (`Min Jaccard` selector). Reads `/api/graph?kind=pipelines` (`Queries.pipeline_graph`). Clicking a
  pipeline node opens its runs.
- **Operators (co-occurrence)** — nodes are operators, clustered by stage role, sized by pipeline count,
  colored by the mean score of runs that use them; edges connect operators that appear together in a
  pipeline. Reads `/api/graph?kind=operators` (`Queries.operator_graph`).

A `Layout` selector offers `cose` (force-directed, the default — clusters spread apart), `concentric`,
and `grid`. A per-cluster color legend is rendered above the graph; hovering a node dims everything but
its closed neighbourhood.

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

### Planned

**Question:** which pipeline×dataset conditions have been registered but not yet run?

Routed at `#/planned`. A table from `/api/planned` of the `planned_runs` rows still in `status =
'planned'`: pipeline label, dataset, collection, source, and creation time. These are produced by the
upload state machine when a bare pipeline is registered against target datasets that have no valid
execution yet (see [Upload](#upload)). An info banner makes the contract explicit — **the Arena stores
results, it does not run compute**; a nirs4all/dag-ml runner fulfils a planned condition and ingests
the result back, after which it disappears here and appears across the rest of the dataviz. Clicking a
row opens the existing runs of that pipeline.

### Upload

**Question:** how do I contribute a pipeline or a run to this store from the browser?

Routed at `#/upload`, rewritten around the **unified upload state machine**
(`nirs4all_benchmarks.ingestion.upload`). The form accepts a file (`.n4a` / `.json` / `.yaml`) **or**
pasted text, plus a set of target-dataset checkboxes, a collection (default `uploads`), and an "Ingest
as benchmark release" toggle, and POSTs multipart `file|text + target_datasets + collection +
as_release` to `/api/upload`.

The backend auto-detects the payload and routes it:

- a results-bearing **`ArenaRunExport`** manifest (`arena_export_schema_version` present) → ingested;
- a **dag-ml `ExecutionBundle`** (`graph_fingerprint` / `bundle_id` present) → adapted to an export,
  then ingested;
- a **`.n4a` bundle** (with or without fitted artifacts) → weights stripped, the recipe registered;
- a **bare pipeline** as a Python list / JSON / YAML (or `{pipeline|steps|nodes}`) → registered.

For a registered pipeline plus target datasets it runs the **run / store / display** decision per
dataset: if a valid execution already exists for that pipeline×dataset it is reported as **already
run** (display); otherwise a **planned** `planned_runs` row is written (a runner fulfils it later).
Fitted artifacts are always stripped and never stored.

The result panel reflects the state-machine outcome (`UploadResult.to_json()`): the overall
`kind`/`status`, the registered `pipeline_dag_hash` (linking to its runs) and stripped-artifact count,
a per-dataset table (`already_run` / `planned` with execution counts), and — for an ingested
export/bundle — the ingestion `status` / `validity_status`, the clean-report counts (facts written,
residual rows, whether scores were recomputed), and all issues with severity badges. The same machine
is available from the CLI as `n4a-benchmarks ingest-pipeline <path> --datasets <a,b> --collection
<name>`. (`POST /api/ingest` remains as the narrower JSON-only `ArenaRunExport` ingest endpoint.)

### Run detail

**Question:** what exactly was run for one execution, and how did it score per fold and per sample?

Routed at `#/run/<execution_hash>` (hidden from the nav; reached from leaderboard / runs / robustness
/ planned / upload). It renders, from `/api/run/<hash>` and `/api/run/<hash>/residuals`:

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
the endpoints in `service/app.py`. All read endpoints are GET and return JSON; the two write endpoints
are POST.

| Endpoint | Used by | Notable query params |
|---|---|---|
| `GET /api/healthz` | (status) | — |
| `GET /api/overview` | overview, header pill, all views | — |
| `GET /api/collections` | (collections) | — |
| `GET /api/datasets` | datasets, overview, upload, filters | — |
| `GET /api/pipelines` | pipelines | — |
| `GET /api/operators` | (operator filters) | — |
| `GET /api/parameters` | param-effect | — |
| `GET /api/leaderboard` | leaderboard, overview | `metric`, `scope`, `partition`, `dataset`, `task`, `collection`, `include_quarantined`, `limit` |
| `GET /api/matrix` | matrix | `metric`, `scope`, `include_quarantined` |
| `GET /api/runs` | run explorer, compare | `metric`, `scope`, `dataset`, `pipeline`, `operator`, `include_quarantined`, `limit` |
| `GET /api/operator-effect` | operator-effect | `metric`, `scope` |
| `GET /api/parameter-effect` | param-effect | `param` (required), `metric`, `scope` |
| `GET /api/robustness` | robustness | `metric`, `scope` |
| `GET /api/facets` | playground, parallel, landscape | — |
| `GET /api/facet-values` | (facet drill-down) | `key` (required) |
| `GET /api/pivot` | playground | `group_by` (required), `color_by`, `metric`, `scope`, `partition`, `dataset`, `include_quarantined`, `agg` |
| `GET /api/parallel` | parallel, landscape | `dimensions` (required, comma-separated), `metric`, `scope`, `include_quarantined` |
| `GET /api/planned` | planned | — |
| `GET /api/graph` | network | `kind` (`pipelines`\|`operators`), `metric`, `scope`, `min_jaccard` |
| `GET /api/composition` | composition | `metric`, `scope` |
| `GET /api/run/{execution_hash}` | run detail | — |
| `GET /api/run/{execution_hash}/residuals` | run detail, compare | `partition` |
| `GET /api/compare` | residual compare | `a`, `b` (execution hashes), `partition` (default `validation`) |
| `POST /api/upload` | upload | multipart `file`\|`text` + `target_datasets`, `collection`, `as_release` |
| `POST /api/ingest` | (JSON-only ingest) | body = `ArenaRunExport`; query `collection`, `as_release` |

Example calls the SPA makes:

```bash
# top 200 pipelines by cv RMSE on every dataset
curl 'http://127.0.0.1:8000/api/leaderboard?metric=rmse&scope=cv&limit=200'

# the score heatmap
curl 'http://127.0.0.1:8000/api/matrix?metric=rmse&scope=cv'

# pivot: mean cv RMSE grouped by model family, split by augmentation on/off
curl 'http://127.0.0.1:8000/api/pivot?group_by=model_family&color_by=has_augmentation&metric=rmse&scope=cv'

# parallel coordinates over split / cv / model / n_components down to the metric
curl 'http://127.0.0.1:8000/api/parallel?dimensions=split_method,cv_method,model_family,param:n_components&metric=rmse&scope=cv'

# the clustered pipeline mega-graph (edges = shared-operator Jaccard >= 0.5)
curl 'http://127.0.0.1:8000/api/graph?kind=pipelines&metric=rmse&scope=cv&min_jaccard=0.5'

# the role -> operator composition hierarchy
curl 'http://127.0.0.1:8000/api/composition?metric=rmse&scope=cv'

# sample-keyed residual comparison of two executions
curl 'http://127.0.0.1:8000/api/compare?a=<exec_a>&b=<exec_b>&partition=validation'

# unified upload: register a pipeline recipe and PLAN it on two datasets
curl -X POST 'http://127.0.0.1:8000/api/upload' \
     -F 'file=@pipeline.json' \
     -F 'target_datasets=<fingerprint_or_name_a>,<fingerprint_or_name_b>' \
     -F 'collection=uploads'

# unified upload: ingest a results-bearing ArenaRunExport as a benchmark release
curl -X POST 'http://127.0.0.1:8000/api/upload' \
     -F 'file=@run-export.json' \
     -F 'as_release=true'
```

## Cross-view state

`app.js` keeps a small shared state object — `metric`, `scope`, and the selected `dataset`
fingerprint — persisted to `localStorage` under `arena.state.v1`. Picking a dataset on the overview
or datasets card, or changing the metric/scope on one view, carries over to the others. The cached
overview (`state._overview`) is also reused so a freshly loaded view does not re-fetch counts it
already has.
