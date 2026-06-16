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
- [Meta endpoints](#meta-endpoints)
- [Catalog endpoints](#catalog-endpoints)
- [Dataviz query endpoints](#dataviz-query-endpoints)
- [Run detail and residuals](#run-detail-and-residuals)
- [Ingestion endpoint](#ingestion-endpoint)
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

## Ingestion endpoint

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

The CLI provides equivalent ingestion paths without HTTP:
`n4a-benchmarks ingest-export`, `ingest-workspace`, and `ingest-bundle`.

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
  `parameter_effect`, `robustness`) validate `scope` against `fold`, `cv`, `refit`,
  `test`, `view` and raise `ValueError` on anything else (HTTP `400` via the
  service's exception handler).
