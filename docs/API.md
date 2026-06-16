# Arena API reference

REST + in-process query reference for the **nirs4all-benchmarks** dataviz service (codename *the Arena*).

The HTTP layer is a thin shell over the `Queries` facade
(`src/nirs4all_benchmarks/store/queries.py`); every endpoint opens a short-lived
`ArenaStore`, wraps it in `Queries`, and returns the facade's result verbatim. The
Python facade exposes the same data in-process. No weights or fitted artifacts are
ever served — only identity cards, canonical pipeline DAGs, recomputed scores, and
sample-keyed residuals.

## Table of contents

- [Running the service](#running-the-service)
- [Conventions](#conventions)
  - [Scopes](#scopes)
  - [Partitions](#partitions)
  - [Metrics and direction](#metrics-and-direction)
  - [Facet keys](#facet-keys)
- [Meta endpoints](#meta-endpoints)
- [Catalog endpoints](#catalog-endpoints)
- [Dataviz query endpoints](#dataviz-query-endpoints)
- [Faceting and pivot endpoints](#faceting-and-pivot-endpoints)
- [Graph and composition endpoints](#graph-and-composition-endpoints)
- [Planned runs](#planned-runs)
- [Run detail and residuals](#run-detail-and-residuals)
- [Ingestion endpoints](#ingestion-endpoints)
- [Python Queries facade](#python-queries-facade)

## Running the service

The service requires the `service` extra (`fastapi`, `uvicorn`):

```bash
pip install 'nirs4all-benchmarks[service]'

# Serve a store (defaults: host 127.0.0.1, port 8000)
n4a-benchmarks serve --store ./arena-store --port 8000
# n4a-arena is the same CLI under a second entry point.
```

`serve` exports `NIRS4ALL_BENCHMARKS_STORE` and launches the ASGI factory
`nirs4all_benchmarks.service.app:create_app`. The store root is resolved, in order,
from the `create_app(store_root=...)` argument, the `NIRS4ALL_BENCHMARKS_STORE`
environment variable, then `./arena-store`. If a `src/nirs4all_benchmarks/web/`
directory is present it is mounted at `/` as the no-build SPA; the JSON API lives
under `/api`.

All endpoints below are prefixed with `/api`. The base URL in the examples is
`http://127.0.0.1:8000`.

### Error responses

- A `ValueError` raised by the facade (e.g. an invalid `scope`) is returned as
  HTTP `400` with body `{"detail": "<message>"}`.
- `GET /api/run/{execution_hash}` returns HTTP `404` with
  `{"detail": "execution not found"}` for an unknown hash.

## Conventions

### Scopes

The `scope` query parameter selects the score level. Valid values are validated by
`Queries._safe_scope`; anything else raises `ValueError` → HTTP `400`.

| Scope | Meaning |
|---|---|
| `fold` | per-fold scores within a CV instance |
| `cv` | cross-validation aggregate (default for every scoped endpoint) |
| `refit` | scores of the refit/selection model |
| `test` | held-out test partition scores |
| `view` | scores attached to a materialized view definition |

### Partitions

Where a `partition` parameter is accepted (`leaderboard`, `run residuals`,
`compare`), valid values are `train`, `validation`, `test`, `final`. `compare`
defaults to `validation`; residual rows ingested without an explicit partition are
stored as `validation`.

### Metrics and direction

`metric` is a metric name from the recompute registry
(`src/nirs4all_benchmarks/scoring/metrics.py`). Every scored response includes a
`direction` field — `"min"` (lower is better) or `"max"` — used to sort
leaderboards/effects. Defaults to `"min"` for unknown names.

| Direction | Metrics |
|---|---|
| `min` | `mse`, `rmse` (default), `mae`, `medae`, `bias`, `log_loss` |
| `max` | `r2`, `rpd`, `rpiq`, `ccc`, `accuracy`, `balanced_accuracy`, `f1_macro`, `f1_micro`, `precision_macro`, `recall_macro`, `roc_auc`, `mcc` |

The actual metric names available in a given store are reported by
`GET /api/overview` under `metrics` (distinct `metric_name` values in
`metric_observations`).

### Facet keys

The pivot / parallel / playground endpoints slice the benchmark space by **facet
keys**. At ingest, `build_run_facets` (`src/nirs4all_benchmarks/indexing.py`)
classifies every pipeline node into a stage *role* (`classify_role`) and
materializes a long `run_facets` table — one row per
`(run_condition_hash, facet_key, facet_value)`, with a `facet_num` for numeric
facets and a `role` for stage facets. A run contributes one row per value of a
multi-valued facet (presence semantics), so "effect of SNV" or "augmentation
on/off" become groupable.

Facet keys come in three groups:

| Group | Example keys |
|---|---|
| condition | `dataset`, `task_type`, `split_method`, `cv_method`, `n_folds`, `seed`, `refit_strategy`, `pipeline`, `is_linear` |
| stage / structure | `operator`, `model`, `model_family`, `merge_strategy`, `preprocessing_op`, `augmentation_op`, `scaler_op`, `feature_selection_op`, `n_models`, `n_preprocessing`, `n_augmentation`, `n_stages`, `has_augmentation`, `has_scaler`, `has_feature_selection` |
| swept parameters | `param:n_components`, `param:kernel`, … (one per sweepable node param) |

`GET /api/facets` lists the keys actually present in a store (those with more than
one value, plus every `param:*`); `GET /api/facet-values?key=…` enumerates the
distinct values of one key. Use a returned key as `group_by` / `color_by` for
`/api/pivot`, as a `dimensions` member for `/api/parallel`.

## Meta endpoints

| Method | Path | Query params | Returns |
|---|---|---|---|
| GET | `/api/healthz` | — | health/version object |
| GET | `/api/overview` | — | store-wide counts |

### `GET /api/healthz`

Liveness + store location. Does not open the store.

```json
{
  "status": "ok",
  "version": "0.1.0",
  "store": "/abs/path/to/arena-store",
  "store_exists": true
}
```

`store_exists` is `true` when `<store>/arena.sqlite` exists on disk.

```bash
curl -s http://127.0.0.1:8000/api/healthz
```

### `GET /api/overview`

Row counts across the normalized store plus the list of available metrics and the
schema version.

```json
{
  "datasets": 6, "dataset_cards": 4, "tasks": 6, "pipelines": 12,
  "operators": 18, "parameters": 9, "run_conditions": 60, "executions": 60,
  "valid_executions": 57, "quarantined_executions": 3,
  "score_sets": 60, "metric_observations": 480, "residual_sets": 60,
  "collections": 2,
  "metrics": ["bias", "ccc", "mae", "mse", "r2", "rmse", "rpd", "rpiq"],
  "schema_version": 1
}
```

```bash
curl -s http://127.0.0.1:8000/api/overview
```

## Catalog endpoints

These return the dimension catalogs and never take query parameters.

| Method | Path | Returns (array of) |
|---|---|---|
| GET | `/api/collections` | rows from `collections` (ordered by `created_at`) |
| GET | `/api/datasets` | dataset fingerprints joined to dataset cards |
| GET | `/api/pipelines` | pipeline DAGs with run-condition counts |
| GET | `/api/operators` | operator specs with pipeline-usage counts |
| GET | `/api/parameters` | sweepable parameter names with value counts |

### `GET /api/collections`

```bash
curl -s http://127.0.0.1:8000/api/collections
```

```json
[
  {
    "collection_id": "fixtures", "kind": "benchmark_release",
    "name": "fixtures", "description": null,
    "created_at": "2026-06-16T10:00:00.000000Z", "metadata_json": "{}"
  }
]
```

### `GET /api/datasets`

Each row carries the fingerprint identity plus card metadata (when a
`dataset_cards` row is linked) and `n_run_conditions`.

```json
[
  {
    "dataset_fingerprint": "df_...", "privacy_level": "public",
    "n_samples": 120, "n_features": 200, "task_type": "regression",
    "name": "soil-carbon", "domain": "soil", "modality": "NIR",
    "signal_type": "absorbance", "axis_unit": "nm",
    "axis_min": 1100.0, "axis_max": 2500.0, "n_run_conditions": 10
  }
]
```

```bash
curl -s http://127.0.0.1:8000/api/datasets
```

### `GET /api/pipelines`

```json
[
  {
    "pipeline_dag_hash": "pdag_...", "human_label": "SNV → PLS",
    "main_model": "sklearn.cross_decomposition.PLSRegression",
    "n_nodes": 3, "is_linear": 1,
    "nirs4all_identity_hash": "...", "engine_graph_fingerprint": "...",
    "n_run_conditions": 5
  }
]
```

```bash
curl -s http://127.0.0.1:8000/api/pipelines
```

### `GET /api/operators`

Operator specs with the number of distinct pipelines each appears in
(`n_pipelines`, descending).

```json
[
  {
    "operator_spec_hash": "op_...", "entrypoint": "sklearn.preprocessing.StandardScaler",
    "library": "sklearn", "version": "1.5.0", "role": "transform",
    "family": "scaler", "n_pipelines": 8
  }
]
```

```bash
curl -s http://127.0.0.1:8000/api/operators
```

### `GET /api/parameters`

Names of sweepable parameters (`is_sweepable = 1`), with the count of distinct
values seen and whether any value is numeric. Use a returned `name` as the `param`
argument to `/api/parameter-effect`.

```json
[
  {"name": "n_components", "n_values": 12, "numeric": 1},
  {"name": "kernel", "n_values": 3, "numeric": 0}
]
```

```bash
curl -s http://127.0.0.1:8000/api/parameters
```

## Dataviz query endpoints

### `GET /api/leaderboard`

Configurable, baseline-free ranking. Aggregates `metric_value` per
`run_condition_hash` and sorts by mean according to the metric direction.

| Param | Type | Default | Notes |
|---|---|---|---|
| `metric` | str | `rmse` | metric name |
| `scope` | str | `cv` | see [Scopes](#scopes) |
| `partition` | str? | — | filter to one partition |
| `dataset` | str? | — | `dataset_fingerprint` filter |
| `task` | str? | — | `task_hash` filter |
| `collection` | str? | — | `collection_id` filter |
| `include_quarantined` | bool | `false` | include quarantined executions |
| `limit` | int | `200` | max rows (capped at `2000`) |

Only `score_validity = 'valid'` observations are considered;
`include_quarantined=false` additionally requires `execution_validity = 'valid'`.

Response:

```json
{
  "metric": "rmse", "scope": "cv", "direction": "min",
  "rows": [
    {
      "run_condition_hash": "rc_...", "pipeline_dag_hash": "pdag_...",
      "pipeline_label": "SNV → PLS", "main_model": "...PLSRegression",
      "dataset_fingerprint": "df_...",
      "mean": 0.412, "min": 0.380, "max": 0.461, "n_obs": 5, "rank": 1
    }
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/leaderboard?metric=rmse&scope=cv&dataset=df_abc&limit=20'
```

### `GET /api/matrix`

Pipeline × dataset heatmap: mean `metric_value` per (pipeline, dataset) plus a
`coverage` count of distinct executions per cell.

| Param | Type | Default |
|---|---|---|
| `metric` | str | `rmse` |
| `scope` | str | `cv` |
| `include_quarantined` | bool | `false` |

```json
{
  "metric": "rmse", "scope": "cv", "direction": "min",
  "datasets":  [{"dataset_fingerprint": "df_...", "label": "soil-carbon"}],
  "pipelines": [{"pipeline_dag_hash": "pdag_...", "label": "SNV → PLS"}],
  "cells": [
    {"pipeline_dag_hash": "pdag_...", "dataset_fingerprint": "df_...",
     "value": 0.41, "coverage": 5}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/matrix?metric=r2&scope=test'
```

### `GET /api/runs`

Run explorer — one row per execution, mean `metric_value` over its observations,
ordered ascending by metric value.

| Param | Type | Default | Notes |
|---|---|---|---|
| `metric` | str | `rmse` | |
| `scope` | str | `cv` | |
| `dataset` | str? | — | `dataset_fingerprint` filter |
| `pipeline` | str? | — | `pipeline_dag_hash` filter |
| `operator` | str? | — | join to executions whose pipeline contains this operator (dotted import path) |
| `include_quarantined` | bool | `true` | note: default differs from leaderboard/matrix |
| `limit` | int | `500` | max rows (capped at `5000`) |

```json
[
  {
    "execution_hash": "exec_...", "run_condition_hash": "rc_...",
    "pipeline_dag_hash": "pdag_...", "pipeline_label": "SNV → PLS",
    "main_model": "...PLSRegression", "dataset_fingerprint": "df_...",
    "execution_validity": "valid", "execution_status": "ok",
    "producer_capsule": "nirs4all@0.9.3", "time_ms": 842.0,
    "metric_value": 0.41
  }
]
```

```bash
curl -s 'http://127.0.0.1:8000/api/runs?operator=sklearn.cross_decomposition.PLSRegression&limit=50'
```

### `GET /api/operator-effect`

Score distribution grouped by operator presence (valid executions only). One
series per operator, sorted by mean per the metric direction.

| Param | Type | Default |
|---|---|---|
| `metric` | str | `rmse` |
| `scope` | str | `cv` |

```json
{
  "metric": "rmse", "scope": "cv", "direction": "min",
  "series": [
    {
      "operator": "...SavitzkyGolay", "role": "transform", "n": 30,
      "mean": 0.39, "median": 0.40, "stdev": 0.05, "min": 0.30, "max": 0.50,
      "values": [0.30, 0.41, "..."]
    }
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/operator-effect?metric=rmse'
```

### `GET /api/parameter-effect`

Metric vs a single parameter (e.g. PLS `n_components`). `param` is required.

| Param | Type | Default | Notes |
|---|---|---|---|
| `param` | str | — | **required** parameter name (from `/api/parameters`) |
| `metric` | str | `rmse` | |
| `scope` | str | `cv` | |

Each point's `param` is the numeric value when numeric, else the decoded JSON
value; `numeric` is the numeric value (or `null` for categoricals).

```json
{
  "param_name": "n_components", "metric": "rmse", "scope": "cv", "direction": "min",
  "points": [
    {"param": 10, "numeric": 10.0, "metric_value": 0.41,
     "dataset_fingerprint": "df_...", "pipeline_label": "SNV → PLS"}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/parameter-effect?param=n_components&metric=rmse'
```

### `GET /api/robustness`

Per-(pipeline, dataset) score stability across executions (folds/seeds/splits).
Valid executions only; sorted by `stdev` ascending.

| Param | Type | Default |
|---|---|---|
| `metric` | str | `rmse` |
| `scope` | str | `cv` |

```json
[
  {
    "pipeline_dag_hash": "pdag_...", "dataset_fingerprint": "df_...",
    "label": "SNV → PLS", "n": 5,
    "mean": 0.41, "stdev": 0.03, "cv_pct": 7.3
  }
]
```

```bash
curl -s 'http://127.0.0.1:8000/api/robustness?metric=rmse&scope=cv'
```

## Faceting and pivot endpoints

These power the **playground** SPA view: pick a facet to group by, optionally a
second to split series, and aggregate any metric across executions. See
[Facet keys](#facet-keys) for what `key` / `group_by` / `color_by` accept.

### `GET /api/facets`

Available facet keys with cardinality, role, and whether the key is numeric. Keys
with a single value are hidden (except every `param:*`). Used to fill the
group-by / color-by selectors.

```json
[
  {"facet_key": "model_family", "n_values": 6, "role": "model", "numeric": 0},
  {"facet_key": "split_method", "n_values": 3, "role": null, "numeric": 0},
  {"facet_key": "param:n_components", "n_values": 12, "role": null, "numeric": 1}
]
```

```bash
curl -s http://127.0.0.1:8000/api/facets
```

### `GET /api/facet-values`

Distinct values of one facet key (numeric values first, then alphabetical), each
with the run count `n`. `key` is required.

| Param | Type | Default | Notes |
|---|---|---|---|
| `key` | str | — | **required** facet key from `/api/facets` |

```json
[
  {"facet_value": "5", "facet_num": 5.0, "n": 8},
  {"facet_value": "10", "facet_num": 10.0, "n": 14},
  {"facet_value": "kfold", "facet_num": null, "n": 22}
]
```

```bash
curl -s 'http://127.0.0.1:8000/api/facet-values?key=param:n_components'
```

### `GET /api/pivot`

Aggregate the metric across executions grouped by one facet (`group_by`) and
optionally split into series by a second (`color_by`). Valid score observations
only; `include_quarantined=false` additionally requires `execution_validity =
'valid'`.

| Param | Type | Default | Notes |
|---|---|---|---|
| `group_by` | str | — | **required** facet key (primary axis) |
| `color_by` | str? | — | facet key for the second series dimension |
| `metric` | str | `rmse` | metric name |
| `scope` | str | `cv` | see [Scopes](#scopes) |
| `partition` | str? | — | filter to one partition |
| `dataset` | str? | — | `dataset_fingerprint` filter |
| `include_quarantined` | bool | `false` | include quarantined executions |
| `agg` | str | `mean` | echoed back; the aggregate plotted is the per-group mean |

Each row's `value` is the per-group mean of the metric; `min`/`max` bound it and
`n` is the count of distinct executions in the cell. `group_num` / `color_num` are
the numeric value when the facet is numeric, else `null` (rows sort by `group_num`
then `group_value`).

```json
{
  "group_by": "model_family", "color_by": "has_augmentation",
  "metric": "rmse", "scope": "cv", "direction": "min", "agg": "mean",
  "rows": [
    {"group_value": "PLSRegression", "group_num": null,
     "color": "no", "color_num": null,
     "value": 0.41, "min": 0.38, "max": 0.46, "n": 12}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/pivot?group_by=model_family&color_by=has_augmentation&metric=rmse&scope=cv'
```

### `GET /api/parallel`

One row per execution with a column for each selected single-valued facet plus the
metric — feeds a parallel-coordinates plot over split / cv / model / params →
score. `dimensions` is a comma-separated list of facet keys; the response
`dimensions` array appends `"metric"`.

| Param | Type | Default | Notes |
|---|---|---|---|
| `dimensions` | str | — | **required** comma-separated facet keys |
| `metric` | str | `rmse` | metric name |
| `scope` | str | `cv` | see [Scopes](#scopes) |
| `include_quarantined` | bool | `false` | include quarantined executions |

Within each row, a numeric facet contributes its `facet_num`, a categorical one its
`facet_value`; for a multi-valued facet the first value encountered wins.

```json
{
  "dimensions": ["split_method", "model_family", "param:n_components", "metric"],
  "metric": "rmse", "scope": "cv", "direction": "min",
  "rows": [
    {"split_method": "kfold", "model_family": "PLSRegression",
     "param:n_components": 10.0, "metric": 0.41,
     "pipeline_label": "SNV → PLS", "execution_hash": "exec_..."}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/parallel?dimensions=split_method,model_family,param:n_components&metric=rmse'
```

## Graph and composition endpoints

These power the network / landscape / composition SPA views (Cytoscape mega-graph,
sunburst/treemap).

### `GET /api/graph`

A clustered network. `kind=pipelines` (default) returns the pipeline mega-graph —
nodes are pipelines (clustered by main-model family, sized by run count, colored by
mean score); an edge links two pipelines whose shared-operator **Jaccard** is at
least `min_jaccard`. `kind=operators` returns the operator co-occurrence graph —
nodes are operators (clustered by stage role via `classify_role`, sized by pipeline
count); edges connect operators that appear together in a pipeline, weighted by the
number of co-occurrences. Both cap at the 150 highest-coverage nodes.

| Param | Type | Default | Notes |
|---|---|---|---|
| `kind` | str | `pipelines` | `pipelines` or `operators` |
| `metric` | str | `rmse` | metric name |
| `scope` | str | `cv` | see [Scopes](#scopes) |
| `min_jaccard` | float | `0.34` | edge threshold (pipelines graph only) |

Pipelines graph:

```json
{
  "kind": "pipelines", "metric": "rmse", "scope": "cv", "direction": "min",
  "nodes": [
    {"id": "pdag_...", "label": "SNV → PLS", "cluster": "PLSRegression",
     "size": 5, "score": 0.41}
  ],
  "edges": [
    {"source": "pdag_a", "target": "pdag_b", "weight": 0.5}
  ]
}
```

Operators graph (`kind=operators`):

```json
{
  "kind": "operators", "metric": "rmse", "scope": "cv", "direction": "min",
  "nodes": [
    {"id": "PLSRegression", "operator": "sklearn.cross_decomposition.PLSRegression",
     "cluster": "model", "size": 8, "score": 0.41, "n_runs": 30}
  ],
  "edges": [
    {"source": "SavitzkyGolay", "target": "PLSRegression", "weight": 6}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/graph?kind=pipelines&metric=rmse&min_jaccard=0.34'
curl -s 'http://127.0.0.1:8000/api/graph?kind=operators&metric=rmse'
```

### `GET /api/composition`

Role → operator usage hierarchy for a sunburst/treemap, colored by score. One row
per operator (valid runs only) with its classified `role`, the short class name,
the number of distinct pipelines and runs it appears in, and the mean score of
those runs. Rows are grouped by role then descending run count.

| Param | Type | Default |
|---|---|---|
| `metric` | str | `rmse` |
| `scope` | str | `cv` |

```json
{
  "metric": "rmse", "scope": "cv", "direction": "min",
  "rows": [
    {"operator": "sklearn.cross_decomposition.PLSRegression",
     "operator_short": "PLSRegression", "role": "model",
     "n_pipes": 8, "n_runs": 30, "score": 0.41}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/composition?metric=rmse&scope=cv'
```

## Planned runs

### `GET /api/planned`

The not-yet-run pipeline × dataset conditions awaiting a runner. When a bare
pipeline is uploaded against target datasets that have no valid execution yet, the
Arena writes a `planned_runs` row (status `planned`); a runner later fulfils it and
ingests the result. The Arena itself never runs compute. Takes no parameters;
ordered newest first.

```json
[
  {
    "plan_id": "plan_0123456789abcdef",
    "pipeline_dag_hash": "pdag_...", "human_label": "SNV → PLS",
    "dataset_fingerprint": "df_...", "dataset_name": "soil-carbon",
    "collection_id": "uploads", "status": "planned",
    "source": "upload", "created_at": "2026-06-16T10:00:00.000000Z"
  }
]
```

```bash
curl -s http://127.0.0.1:8000/api/planned
```

## Run detail and residuals

### `GET /api/run/{execution_hash}`

Full reproducibility record for one execution: the execution row, its run
condition, the canonical pipeline (graph, nodes, edges, node params), all score
observations, the linked residual-set row, and the dataset / CV / RNG / refit
dimensions. Returns `404` if `execution_hash` is unknown.

```json
{
  "execution": {"execution_hash": "exec_...", "validity_status": "valid", "...": "..."},
  "run_condition": {"run_condition_hash": "rc_...", "...": "..."},
  "pipeline": {"pipeline_dag_hash": "pdag_...", "graph": {"nodes": [], "edges": []}, "...": "..."},
  "nodes": [], "edges": [], "node_params": [],
  "scores": [
    {"score_set_id": "ss_...", "scope": "cv", "validity_status": "valid",
     "metric_name": "rmse", "metric_value": 0.41, "fold_id": null,
     "partition": "validation", "direction": "min"}
  ],
  "residual_set": {"residual_set_id": "rs_...", "n_rows": 120, "key": "sample_id"},
  "dataset": {"dataset_fingerprint": "df_...", "...": "..."},
  "cv": {"cv_instance_hash": "...", "...": "..."},
  "rng": {"rng_context_hash": "...", "...": "..."},
  "refit": {"refit_strategy_hash": "...", "...": "..."}
}
```

```bash
curl -s http://127.0.0.1:8000/api/run/exec_0123456789abcdef
```

### `GET /api/run/{execution_hash}/residuals`

Sample-keyed residual rows for the execution, read from the Parquet residual store.
Returns `[]` when no residual set exists (e.g. scores-only ingestion). Optional
`partition` filters the rows in-process.

| Param | Type | Default |
|---|---|---|
| `partition` | str? | — |

Each row follows the residuals Parquet column contract (sample-keyed, not
positional):

| Column | Type | Nullable |
|---|---|---|
| `sample_id` | str | no |
| `group_id` | str | yes |
| `origin_sample_id` | str | yes |
| `scope` | str | no |
| `fold_id` | str | yes |
| `partition` | str | no |
| `y_true` | float | yes |
| `y_pred` | float | yes |
| `y_proba` | list[float] | yes |
| `residual` | float | yes |
| `weight` | float | yes |

```bash
curl -s 'http://127.0.0.1:8000/api/run/exec_0123.../residuals?partition=validation'
```

### `GET /api/compare`

Compare two executions' residuals **on the same samples** (joined by stable
`sample_id`). `a` and `b` are execution hashes; both are required.

| Param | Type | Default | Notes |
|---|---|---|---|
| `a` | str | — | **required** execution hash |
| `b` | str | — | **required** execution hash |
| `partition` | str | `validation` | partition to compare |

`residual_correlation` (the complementarity signal) is Pearson correlation of the
paired residuals, or `null` when fewer than two pairs exist or the correlation is
undefined.

```json
{
  "n_common": 118, "n_paired": 118, "residual_correlation": 0.62,
  "rmse_a": 0.41, "rmse_b": 0.44,
  "paired": [
    {"sample_id": "s_...", "residual_a": -0.02, "residual_b": 0.05, "y_true": 3.1}
  ]
}
```

```bash
curl -s 'http://127.0.0.1:8000/api/compare?a=exec_aaa&b=exec_bbb&partition=test'
```

## Ingestion endpoints

### `POST /api/ingest`

Upload one `ArenaRunExport` manifest as the JSON request body. The manifest is
validated against the frozen export schema, identities are resolved, leakage is
checked, residuals are pseudonymized, and scores are stored (recomputed from
residuals when no observations are present).

| Where | Param | Type | Default | Notes |
|---|---|---|---|---|
| query | `collection` | str | `uploads` | target collection id |
| query | `as_release` | bool | `false` | ingest as a `benchmark_release` (quarantine on leakage) vs `user_run_collection` |
| body | — | object | — | an `ArenaRunExport` manifest |

Response mirrors the `IngestionResult`:

```json
{
  "status": "committed",
  "validity_status": "valid",
  "run_condition_hash": "rc_...",
  "execution_hash": "exec_...",
  "arena_export_hash": "...",
  "issues": [],
  "clean_report": {}
}
```

`status` is one of `committed`, `already_ingested`, `quarantined`, or `rejected`.
A schema-invalid manifest is `rejected` with `issues` describing the failures.

```bash
curl -s -X POST \
  'http://127.0.0.1:8000/api/ingest?collection=uploads&as_release=false' \
  -H 'Content-Type: application/json' \
  --data-binary @arena_run_export.json
```

### `POST /api/upload`

The unified upload entry point: hand the Arena **anything** that identifies a
pipeline and/or a run as a `multipart/form-data` POST and it auto-detects the
payload and runs the run/store/display state machine
(`src/nirs4all_benchmarks/ingestion/upload.py`, `upload()`). The same logic backs
the rewritten **upload** SPA view and the `n4a-benchmarks ingest-pipeline` CLI.

Recognized payloads (`kind` in the response):

| Payload | `kind` | Behavior |
|---|---|---|
| `ArenaRunExport` manifest (JSON/YAML) | `arena_export` | validated + ingested (`as_release` controls release vs user collection) |
| dag-ml `ExecutionBundle` (JSON) | `dagml_bundle` | adapted via `bundle_to_export`, then ingested |
| `.n4a` bundle (zip, with **or** without fitted artifacts) | `pipeline` | weights stripped, recipe registered; `stripped_artifacts` counts removed artifacts |
| raw nirs4all pipeline — Python step list, or `{pipeline\|steps\|nodes}` — as JSON/YAML | `pipeline` | recipe registered |

For a registered (bare) pipeline plus `target_datasets`, the state machine decides
**per dataset**: if a valid execution already exists for that pipeline × dataset →
`already_run` (nothing queued); otherwise a `planned_runs` row is written (status
`planned`) for a runner to fulfil. See [`GET /api/planned`](#get-apiplanned).

Provide exactly one of `file` or `text`; omitting both is HTTP `400`.

| Where | Param | Type | Default | Notes |
|---|---|---|---|---|
| form | `file` | file | — | uploaded `.n4a` / `.json` / `.yaml` (auto-detected by content; `.n4a` is sniffed as a zip) |
| form | `text` | str | — | raw JSON/YAML recipe or manifest (used when no `file`) |
| form | `target_datasets` | str | `""` | comma-separated dataset tokens — fingerprints **or** card names / dataset ids — to plan the pipeline against |
| form | `collection` | str | `uploads` | target collection id |
| form | `as_release` | bool | `false` | ingest results as a `benchmark_release` (quarantine on leakage) vs `user_run_collection` |

Response mirrors `UploadResult.to_json()`:

```json
{
  "kind": "pipeline",
  "status": "registered",
  "pipeline_dag_hash": "pdag_...",
  "pipeline_label": "SNV → PLS",
  "stripped_artifacts": 2,
  "datasets": [
    {"dataset": "df_...", "token": "soil-carbon", "status": "planned",
     "n_executions": 0, "plan_id": "plan_0123456789abcdef"},
    {"dataset": "df_...", "token": "df_other", "status": "already_run",
     "n_executions": 5}
  ],
  "ingestion": null,
  "message": "registered pipeline pdag_01234567; stripped 2 artifact(s)"
}
```

`status` is `registered` (pipeline recipe), `ingested` (results accepted), or
`rejected`. For a results payload `datasets` is `[]` and `ingestion` carries the
`IngestionResult` fields (`status`, `validity_status`, `run_condition_hash`,
`execution_hash`, `issues`, `clean_report`); for a pipeline payload `ingestion` is
`null` and each `datasets` entry reports its per-dataset `status` (`planned` or
`already_run`), `n_executions`, and (when planned) `plan_id`.

```bash
# register a .n4a recipe and plan it on two datasets
curl -s -X POST 'http://127.0.0.1:8000/api/upload' \
  -F 'file=@model.n4a' \
  -F 'target_datasets=soil-carbon,df_abc123' \
  -F 'collection=uploads'

# upload a YAML pipeline recipe as text
curl -s -X POST 'http://127.0.0.1:8000/api/upload' \
  -F 'text=- {op: SNV}
- {op: PLSRegression, n_components: 10}' \
  -F 'target_datasets=soil-carbon'
```

The CLI provides equivalent ingestion paths without HTTP:
`n4a-benchmarks ingest-export`, `ingest-workspace`, `ingest-bundle`, and
`ingest-pipeline` (the latter is the CLI face of `/api/upload`).

## Python Queries facade

The same data is available in-process through `Queries`, which the service and CLI
both consume. Open a store, wrap it, and call the methods directly.

```python
from nirs4all_benchmarks.store import ArenaStore, Queries

store = ArenaStore("./arena-store")
try:
    q = Queries(store)

    q.overview()                       # GET /api/overview
    q.collections()                    # GET /api/collections
    q.datasets()                       # GET /api/datasets
    q.pipelines()                      # GET /api/pipelines
    q.operators()                      # GET /api/operators
    q.sweepable_parameters()           # GET /api/parameters
    q.metrics_available()              # distinct metric names

    q.leaderboard(metric="rmse", scope="cv", dataset_fingerprint="df_...", limit=20)
    q.matrix(metric="r2", scope="test")
    q.run_explorer(metric="rmse", operator="sklearn.cross_decomposition.PLSRegression")
    q.operator_effect(metric="rmse", scope="cv")
    q.parameter_effect("n_components", metric="rmse", scope="cv")
    q.robustness(metric="rmse", scope="cv")

    q.facets()                         # GET /api/facets
    q.facet_values("param:n_components")            # GET /api/facet-values
    q.pivot(group_by="model_family", color_by="has_augmentation")  # GET /api/pivot
    q.parallel(dimensions=["split_method", "model_family"])        # GET /api/parallel
    q.planned()                        # GET /api/planned
    q.pipeline_graph(metric="rmse", scope="cv", min_jaccard=0.34)  # GET /api/graph?kind=pipelines
    q.operator_graph(metric="rmse", scope="cv")     # GET /api/graph?kind=operators
    q.composition(metric="rmse", scope="cv")        # GET /api/composition

    q.run_detail("exec_...")           # None if unknown
    q.fold_scores("exec_...", metric="rmse")   # per-fold rows (facade-only)
    q.residuals("exec_...", partition="validation")
    q.residual_compare("exec_a", "exec_b", partition="validation")
finally:
    store.close()
```

Mapping of facade methods to HTTP endpoints:

| Facade method | HTTP endpoint |
|---|---|
| `overview()` | `GET /api/overview` |
| `collections()` | `GET /api/collections` |
| `datasets()` | `GET /api/datasets` |
| `pipelines()` | `GET /api/pipelines` |
| `operators()` | `GET /api/operators` |
| `sweepable_parameters()` | `GET /api/parameters` |
| `leaderboard(...)` | `GET /api/leaderboard` |
| `matrix(...)` | `GET /api/matrix` |
| `run_explorer(...)` | `GET /api/runs` |
| `operator_effect(...)` | `GET /api/operator-effect` |
| `parameter_effect(name, ...)` | `GET /api/parameter-effect` |
| `robustness(...)` | `GET /api/robustness` |
| `facets()` | `GET /api/facets` |
| `facet_values(key, ...)` | `GET /api/facet-values` |
| `pivot(...)` | `GET /api/pivot` |
| `parallel(...)` | `GET /api/parallel` |
| `planned()` | `GET /api/planned` |
| `pipeline_graph(...)` | `GET /api/graph?kind=pipelines` |
| `operator_graph(...)` | `GET /api/graph?kind=operators` |
| `composition(...)` | `GET /api/composition` |
| `run_detail(hash)` | `GET /api/run/{execution_hash}` |
| `residuals(hash, ...)` | `GET /api/run/{execution_hash}/residuals` |
| `residual_compare(a, b, ...)` | `GET /api/compare` |
| `metrics_available()` | — (folded into `overview()`) |
| `fold_scores(hash, ...)` | — (facade only, no endpoint) |

Notes:

- The HTTP `leaderboard`/`runs` `dataset`, `pipeline`, and `task` parameters map to
  the facade's `dataset_fingerprint`, `pipeline_dag_hash`, and `task_hash` keyword
  arguments.
- `residual_compare` is **sample-keyed**: it intersects the two executions'
  residual rows by stable `sample_id` (per partition) before pairing, so two
  pipelines are always compared on the *same* samples.
- All scoped methods (`leaderboard`, `matrix`, `run_explorer`, `operator_effect`,
  `parameter_effect`, `robustness`, `pivot`, `parallel`, `pipeline_graph`,
  `operator_graph`, `composition`) validate `scope` against `fold`, `cv`, `refit`,
  `test`, `view` and raise `ValueError` on anything else (HTTP `400` via the
  service's exception handler). `facets`, `facet_values`, and `planned` are not
  scoped.
- The HTTP `pivot`/`parallel` `dataset` parameter maps to the facade's
  `dataset_fingerprint`; `parallel` takes `dimensions` as a Python list, whereas
  the endpoint takes a comma-separated string.
- The upload state machine is **not** on `Queries` (it writes, not reads). Use
  `from nirs4all_benchmarks.ingestion import upload, register_pipeline` directly;
  `upload(store, payload, target_datasets=[...], collection_id=..., as_release=...)`
  returns an `UploadResult` whose `.to_json()` is exactly the `POST /api/upload`
  body. `payload` may be an `ArenaRunExport`/dict manifest, a dag-ml bundle dict, a
  pipeline list/dict, a `Path` to a `.n4a`/`.json`/`.yaml` file, or raw JSON/YAML
  text.
