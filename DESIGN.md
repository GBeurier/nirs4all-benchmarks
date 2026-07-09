# nirs4all-benchmarks design

| Field | Value |
|---|---|
| Status | Initial design cleaned up |
| Date | 2026-06-05 |
| Audience | nirs4all maintainers, future dataset/method contributors, Arena developers |
| Scope | Storage, ingestion, querying, and data visualization of NIRS pipeline performances |
| Out of scope | Detailed compute execution, dataset hosting, retention of trained models |

## 1. Summary

`nirs4all-benchmarks`should become the reference environment for storing, comparing and exploring the performance of NIRS pipelines. Arena does not store raw datasets, trained models,`.n4a`bundles, fitted transformers or other heavy artifacts. It stores:

- identity cards of anonymized datasets or fingerprints; - complete definitions of spots, splits, CV, seeds and RNG; - nirs4all pipelines in canonical form, including complex DAGs; - versioned scores; - the residuals and, only if authorized, the predictions/targets necessary for the recalculation; - sufficient environment metadata to document the execution conditions.

The basic experimental tuple is:

```text
DATASET_CARD ou DATASET_FINGERPRINT
  x TASK
  x DATASET_VARIANT
  x METHOD_SPLIT_TEST
  x METHOD_CV
  x RNG_CONTEXT
  x PIPELINE_DAG
  x REFIT_STRATEGY
  x SCORE_VERSION
```

`PIPELINE_DAG` replaces the old linear view `preprocessing_chain x model`: a pipeline can contain branches, multi-source or multimodal processing, merges, several models, stacking, bagging, averaging, voting, or any other DAG structure produced by nirs4all/dag-ml.

The Arena must serve two purposes:

1. **Public benchmarks** on `nirs4all-datasets` reference datasets.
2. **Aggregation of user runs** in the medium term, even if the dataset is not shareable: in that case we store an anonymized statistical map and a hash/fingerprint, not the data.

## 2. Structural decisions

1. **Single target producer: nirs4all.** Runs come to completion from nirs4all, whatever the capsule: Python lib, R lib, Studio, nirs4all-web, batch/cluster. External sources are only accepted if they pass through a nirs4all-compatible export/manifest.
2. **No artifacts.** Arena does not retain trained models, fitted transformers, feature caches, bundles, or datasets. It keeps documented pipelines, scores, residuals, and identity cards.
3. **Aligned nirs4all workspace storage.** The target format is SQLite for metadata and Parquet for result/residual arrays, aligned with the current nirs4all workspace. DuckDB is not the reference format.
4. **Dataset split into three levels.** `DatasetCard` describes the data, `TaskSpec` describes the target/task, and `DatasetVariant` describes a view/subpopulation/aggregation.
5. **Multi-target: several single-target tasks by default.** A multi-output task remains possible later, but the initial benchmark indexes each target as a single-target `TaskSpec`.
6. **No canonical baseline imposed.** The Arena is agnostic. Dataviz allows you to choose an interactive reference, but storage does not assume `PLS-canon`.
7. **Configurable ranking.** A view can rank the CV, a fold, the external test, the refit, or a specific aggregation. The class level is part of the versioned definition of the view.
8. **Explicit score versioning.** Every score carries a calculation version: metric implementation, aggregation policy, filters, fold/CV/refit level, and date.
9. **Private/anonymized datasets supported.** If the data is not publishable, Arena only exposes the statistical map, scores, and residuals authorized by the publication policy.

## 3. Goals

### 3.1 Product goals

- Provide online quantitative benchmarks for the NIRS community. - Allow you to explore the results according to all dimensions: dataset, task, size, domain, split, CV, seed, parameters, operators, DAG branch, model, merge/refit strategy, metric, cost. - Show the effects of operators and parameters, for example the effect of`n_components`on PLS scores. - Make visible what works, what does not work, and under what conditions. - Allow the export of views and quotes for notebooks, articles and reports.

### 3.2 Scientific goals

- Compare pipelines under documented and, for official benchmarks, controlled conditions. - Identify patterns by dataset, statistical map, domain, size, task, instrument, preprocessing, model and parameters. - Measure robustness: folds/seeds/splits variance, parameter instability, failure rate, cost. - Allow analyzes of residuals and complementarity between models/DAG.

### 3.3 Technical goals

- Provide a clean export/import scheme from nirs4all workspaces. - Normalize and hash dimensions to deduplicate: datasets, tasks, DAG, operators, parameters, y_true when allowed, residues. - Allow rapid analytical queries without complicating the format. - Keep old scores auditable without silent mutation.

## 4. Non-objectifs

- Host raw datasets. - Host trained models or fitted artifacts. - Impose a universal baseline. - Impose a single definition of leaderboard. - Redo the nirs4all-cluster compute scheduler in the initial design. - Replace`nirs4all-datasets`or`nirs4all-methods`.

## 5. Relationship with the ecosystem

### 5.1 nirs4all-datasets

`nirs4all-datasets`will provide the versioned reference datasets. Arena must anticipate its v1 with a mock of`DatasetCard`, then plug in the real catalog when it is stable.

For each dataset reference, Arena stores:

- identity:`dataset_id`,`dataset_version`, DOI/source, citation, license; - access: URL/API/download documentation, public/restricted/private status; - hashes:`content_hash`,`descriptor_hash`,`dataset_card_hash`; - statistical map: n_samples, n_features, signal_type, axis range, missingness, target distributions, spectral statistics, available groups; - NIRS metadata: instrument, domain, modality, source(s), axis unit, spectral range, resolution if known; - splits provided if the dataset declares them.

The dataset can be private: Arena does not host the data, but keeps the statistical identity card and the access link if the user has the rights.

### 5.2 nirs4all

The current nirs4all workspace uses:

-`store.sqlite`for runs, pipelines, chains, predictions, logs, projects; -`arrays/*.parquet`for prediction arrays; -`artifacts/`for fitted artifacts.

The Arena must reuse the SQLite + Parquet spirit, but **exclude the retention of artifacts**. Two compatible options:

1. **Extension of the nirs4all workspace**: add Arena tables directly in`store.sqlite`. 2. **ArenaStore separate**: import/export from a nirs4all workspace to`arena.sqlite + arrays/`.

The MVP should start with a clean import from the nirs4all workspaces/exports, then push the useful changes into nirs4all to directly produce a clean Arena export.

### 5.3 nirs4all-methods

The Arena references the methods, it does not store them. For each operator coming from`nirs4all-methods`or an external dependency, we keep:

- library/package; - version; -entrypoint; - family; - settings; - OS/execution environment; - citation/license if available; - canonical specification hash.

## 6. Conceptual model

### 6.1 Overview

```text
BenchmarkRelease / UserRunCollection
  -> DatasetCard ou DatasetFingerprint
  -> TaskSpec
  -> DatasetVariant
  -> SplitSpec + SplitInstance
  -> CVSpec + CVInstance
  -> RNGContext
  -> PipelineDAGSpec
  -> RunCondition
  -> ExecutionSummary
  -> ScoreSet + MetricObservation
  -> ResidualSet
```

### 6.2 DatasetCard

Identity card of a known dataset, typically from`nirs4all-datasets`.

Fields:

-`dataset_card_id`; -`dataset_id`,`dataset_version`,`source_registry`; -`content_hash`,`descriptor_hash`,`dataset_card_hash`; -`visibility`:`public`,`restricted`,`private`,`anonymized`; -`access_policy`: API, documentation, contact, DOI, license; -`identity_stats_json`: stable descriptive statistics; -`nirs_stats_json`: spectral and instrumental statistics; -`grouping_metadata_json`: columns of groups, repetitions, batches, instruments; -`citation_id`.

### 6.3 DatasetFingerprint

Anonymized card for user runs or non-shareable datasets.

It must make it possible to compare data regimes without exposing the data:

-`dataset_fingerprint_hash`; -`privacy_level`; - n_samples, n_features, task_type; - X statistics: summarized means/variances, spectral range, missingness, outlier scores, SNR estimates if available; - statistics of y: anonymized distribution, quantiles, number of classes, imbalance; - group statistics: number of groups, sizes, repetition rate; - optional hash of sample manifest or target if allowed; - no mandatory gross value.

This entity is used in the medium term for aggregation of user runs: no need for an explicit dataset in`nirs4all-datasets`.

### 6.4 TaskSpec

A task corresponds to a target and a prediction definition. By default, each target is a separate`TaskSpec`.

Fields:

-`task_id`; -`dataset_card_id`or`dataset_fingerprint_hash`; -`task_type`; -`target_name`,`target_unit`,`target_columns`; -`encoding_json`; -`target_stats_json`; -`target_hash`if authorized; -`task_hash`.

### 6.5 DatasetVariant

View of a dataset for a task:

- subsampling (`size = 50`,`100`,`all`); - aggregation (`sample_mean`,`group_mean`, etc.); - grouping; - selection of source or modality; - editorial filtering of lines; - target encoding if the variant changes the effective task.

Fields:

-`dataset_variant_id`; -`dataset_card_id`or`dataset_fingerprint_hash`; -`task_id`; -`variant_spec_json`; -`sample_manifest_hash`if authorized; -`dataset_variant_hash`.

### 6.6 SplitSpec et SplitInstance

`SplitSpec`describes the method,`SplitInstance`describes the exact instance.

Fields for `SplitSpec`:

- `split_method`: random, Kennard-Stone, SPXY, predefined, group split, leave-group-out, etc.;
- `params_json`;
- `group_policy`;
- `stratification_policy`;
- `split_spec_hash`.

Fields for `SplitInstance`:

-`split_instance_id`; -`split_spec_hash`; -`rng_context_id`; -`partition_summary_json`; -`split_indices_hash`; - indices or sample ids only if authorized.

### 6.7 CVSpec et CVInstance

Same spec/instance separation:

- methode CV;
- folds/repeats;
- nested CV si besoin;
- relation stricte au train externe (`within_train_only = true`);
- `cv_spec_hash`, `cv_instance_hash`.

### 6.8 RNGContext

Seeds and RNG are experimental conditions in their own right.

Fields:

-`rng_context_id`; - user seed; - seeds derived by framework: Python, NumPy, sklearn, torch, tensorflow, jax; - RNG status or policy if available; - deterministic variables (`PYTHONHASHSEED`, CUDA/cuDNN flags, threads); -`rng_context_hash`.

### 6.9 PipelineDAGSpec

The pipeline is a canonical DAG, not a chain.

A DAG contains:

- **nodes**: preprocessing, augmentation, filter, feature selector, model, meta-model, merge, stack, bagging, mean, vote, refit, scorer; - **edges**: data flow X/y/predictions/residues/metadata between nodes; - **ports**: input/output named to manage multi-source, multimodal, multi-model; - **branches**: parallel or conditional paths; - **merge nodes**: concat, mean, weighted mean, voting, stacking, bagging, custom reducer; - **fit/transform** scopes: train_only, fold_train_only, refit_train_only, inference; - **params** per node.

Fields:

- `pipeline_dag_id`;
- `dag_schema_version`;
- `nodes_json`;
- `edges_json`;
- `entry_nodes`, `terminal_nodes`;
- `pipeline_dag_hash`;
- `human_label`;
- `nirs4all_pipeline_manifest_json`.

Invariant: two equivalent nirs4all syntaxes must produce the same canonical DAG and the same `pipeline_dag_hash`.

### 6.10 PipelineNodeSpec

Chaque node est indexable:

- `node_id`;
- `pipeline_dag_hash`;
- `role`;
- `operator_spec_hash`;
- `params_hash`;
- `branch_path`;
- `source_id`;
- `model_family`;
- `fit_scope`.

This granularity allows data viz by operator or parameter: effect of`OSC`, effect of`n_components`, effect of a branch, effect of a merge.

### 6.11 OperatorSpec et ParameterSpec

`OperatorSpec`:

- bibliotheque/package;
- version;
- entrypoint;
- role;
- citation/licence;
- `operator_spec_hash`.

`ParameterSpec`:

-`node_id`; - parameter name; - canonical value; - kind; -`param_value_hash`; - indicators for dataviz: numeric/categorical, ordinal, sweepable.

The parameters must be output in normalized tables, not just hidden in JSON, to be able to perform effect analyses.

### 6.12 RefitStrategySpec

Refit is a DAG strategy:

- `none`;
- `best_fold`;
- `global_best_params_full_train`;
- `per_fold_best`;
- `weighted_average`;
- `stacking_refit`;
- `bagging_refit`;
- `mean_ensemble`;
- autre node terminal du DAG.

Fields:

- `refit_strategy_hash`;
- `selection_scope`;
- `train_scope`;
- `params_json`.

### 6.13 RunCondition

Natural identity of an experimental condition:

- `benchmark_release_id` ou `user_run_collection_id`;
- `dataset_variant_hash`;
- `split_instance_hash`;
- `cv_instance_hash`;
- `rng_context_hash`;
- `pipeline_dag_hash`;
- `refit_strategy_hash`;
- `run_condition_hash`.

The derived columns useful for querying (main model, presence of SNV,`n_components`value, etc.) can be materialized, but the canonical source remains the DAG.

### 6.14 ExecutionSummary

Summary of a concrete execution, without fitted artifact:

-`execution_id`; -`run_condition_hash`; -`producer_capsule`: python, R, studio, nirs4all-web, cluster; -`nirs4all_version`,`dag_ml_version`if applicable; - OS, Python/R version, major dependencies; - hardware summary; - time/memory; - status: ok/failed/cancelled; - failure code/truncated message; -`execution_hash`.

### 6.15 ScoreComputationSpec et ScoreSet

Score versioning is self-explanatory.

`ScoreComputationSpec`:

-`score_version`; - metric implementation; - score level: fold, CV, refit, test, custom view; - aggregation policy; - filters; - direction; - possible standardization; -`score_computation_hash`.

`ScoreSet`:

- `score_set_id`;
- `execution_id`;
- `score_computation_hash`;
- `scope`: fold, cv, refit, test, view;
- `created_at`;
- `supersedes_score_set_id` si recalcul;
- `validity_status`.

A score recalculation adds a new`ScoreSet`; it does not silently replace the old one.

### 6.16 MetricObservation

Format long:

- `score_set_id`;
- `metric_name`;
- `metric_value`;
- `metric_unit`;
- `direction`;
- `fold_id`;
- `partition`;
- `aggregation_level`;
- `n_samples`;
- `coverage`.

### 6.17 ResidualSet

The Arena preserves the residue, not the artifacts.

Parquet minimal:

- `residual_set_id`;
- `execution_id`;
- `score_set_id`;
- `fold_id`;
- `partition`;
- `sample_pseudo_id`;
- `group_pseudo_id` si autorise;
- `residual`;
- `y_pred` optionnel;
- `y_true` optionnel;
- `weight` optionnel.

Politique:

- for public dataset:`y_true`,`y_pred`, residues can be published if compatible license; - for restricted/private dataset: publish by default aggregated scores and residuals only if the policy allows it; - IDs are pseudonymized.

### 6.18 IngestionBatch

Lot transactionnel:

-`ingestion_batch_id`; - source workspace/export nirs4all; -`input_export_hash`; - status staging/committed/rejected; - line counters; - validation errors; -`clean_report_json`.

Idempotence uses`(input_export_hash, target_collection, schema_version)`.

### 6.19 ViewDefinition

A published view is versioned:

-`view_id`; - leaderboard, matrix, pp-effect, param-effect, DAG explorer, residual explorer; - filters; - score level; - aggregation; - date of materialization; -`view_definition_hash`.

## 7. Stockage physique

### 7.1 Format cible

The target format extends the nirs4all workspace:

```text
workspace-or-arena/
  store.sqlite
  arrays/
    residuals_<partition>.parquet
    optional_predictions_<partition>.parquet
  exports/
    arena_export_<hash>.zip
```

No`artifacts/`in the Arena. If a nirs4all workspace contains artifacts for its internal needs, the Arena export ignores them.

### 7.2 SQLite

SQLite stores dimensions and facts:

- dataset cards/fingerprints/tasks/variants;
- split/CV/RNG;
- pipeline DAGs/nodes/edges/operators/params;
- run conditions;
- execution summaries;
- score computation specs;
- score sets;
- metric observations;
- ingestion batches;
- view definitions.

SQLite convient car:

- he is already in nirs4all; - it is portable; - it handles millions of indexed rows well if the schema is normalized; - it avoids the slowness and complexity observed with DuckDB in this context.

### 7.3 Parquet

Parquet only stores result arrays:

- residues; - optional y_pred/y_true; - weight; - pseudonymized ids; - minimal fold/partition metadata.

Arrays should be deduplicated as much as possible:

-`target_vector_hash`when y_true is storable; - sample ids dictionaries; - fold ids dictionaries; - Zstd compression; - periodic compaction.

### 7.4 Deduplication

Tables de dedup prioritaires:

-`operator_specs`; -`parameter_values`; -`pipeline_dag_specs`; -`dataset_cards`; -`dataset_fingerprints`; -`task_specs`; -`split_instances`; -`cv_instances`; -`rng_contexts`; -`score_computation_specs`; -`target_vectors`only if authorized; -`residual_sets`.

The dedup serves performance but also dataviz: an identical operator or parameter must be queryable everywhere.

### 7.5 Implications for the current nirs4all workspace

The current`store.sqlite + arrays/*.parquet`scheme is a good base, but it must evolve for the Arena.

Mapping initial:

| Workspace actuel | Usage Arena | Evolution necessaire |
|---|---|---|
| `runs` | batch/session source | ajouter/exporter release, collection, RNG, score version |
| `pipelines` | configuration pipeline | remplacer/augmenter par `PipelineDAGSpec` canonique |
| `chains` | chemin lineaire ou branche simple | generaliser en `pipeline_nodes` + `pipeline_edges` |
| `predictions` | metadonnees fold/partition/scores | separate score metadata, residual metadata and scope fold/CV/refit |
| `arrays/*.parquet` | y_true/y_pred actuels | produire `ResidualSet` dedup, y_true/y_pred optionnels |
| `artifacts` | cache fitted interne | ignored by Arena export |
| `logs` | diagnostic local | keep only error/cost summary if useful |

Tables nouvelles ou a materialiser:

- `dataset_cards`, `dataset_fingerprints`, `task_specs`, `dataset_variants`;
- `pipeline_dags`, `pipeline_nodes`, `pipeline_edges`;
- `operator_specs`, `parameter_values`;
- `split_specs`, `split_instances`, `cv_specs`, `cv_instances`;
- `rng_contexts`;
- `score_computation_specs`, `score_sets`, `metric_observations`;
- `residual_sets`;
- `ingestion_batches`, `view_definitions`.

The key point is not to force nirs4all to abandon its artifact storage for its local replay needs. Arena only defines an **export profile without artifacts**.

## 8. Ingestion et clean

### 8.1 Sources

- workspace nirs4all local;
- export nirs4all dedie Arena;
- runs Studio/nirs4all-web via export;
- batch cluster nirs4all;
- imports historiques convertis en format nirs4all si possible.

### 8.2 Ingestion pipeline

```text
workspace/export
  -> detect schema
  -> clean legacy fields
  -> validate dataset card/fingerprint
  -> canonicalize pipeline DAG
  -> extract operator/param tables
  -> compute hashes
  -> validate split/CV/RNG
  -> extract scores and residuals
  -> drop fitted artifacts
  -> write staging
  -> commit SQLite + Parquet
  -> refresh views
```

### 8.3 Clean attendu

The clean of a workspace/export must:

- remove any reference to unpreserved fitted artifacts; - standardize the names of models, operators, branches and sources; - extract parameters from JSON to normalized tables; - check for duplicates via hashes; - type the scales: fold/CV/refit/test; - recalculate or tag scores according to`ScoreComputationSpec`; - produce a readable report.

### 8.4 Validation minimale

- dataset card or fingerprint present; - explicit single-target task; - split/CV compatible with the number of samples; - RNGContext present; - Valid canonical PipelineDAG; - no leakage detected in split/CV; - score version present; - residues aligned with fold/partition; - publication policy compatible with exported arrays.

## 9. Dataviz et web app

### 9.1 MVP

1. **Explorer of runs**: table filterable by dataset, task, DAG, node, parameter, split, CV, seed. 2. **Configurable leaderboard**: choice of score level, metric, aggregation, filters. 3. **Pipeline x dataset/task matrix**: scores, coverage, failure. 4. **Detail run**: DAG, nodes, params, folds, scores, residuals.

### 9.2 Vues prioritaires

- Operator effect: ex. impact of`OSC`when present in a DAG. - Parameter effect: ex.`n_components`PLS vs score. - Split/CV/seed effect. - Comparison of two DAGs. - Residual explorer. - Robustness and failure rate. - Cost performance.

### 9.3 Playground

The playground must allow:

- interactive selection of a reference, without imposed canonical baseline; - arbitrary groupby: dataset card, fingerprint stats, node role, parameter, merge strategy; - notebook-ready extraction; - saving versioned views.

## 10. Versioning, invalidation et scoring

### 10.1 Score versioning

A score is never just a number. He is always attached to:

- a metric; - an implementation; - a fold/CV/refit/test level; - an aggregation; - a filter policy; - a date and a version.

If a metric is corrected, Arena adds a new score version. Old ones remain auditable and marked as superseded.

### 10.2 Invalidation

The invalidations are explicit:

- leakage detecte;
- bug metric;
- bug ingestion;
- dataset retire;
- politique de publication changee;
- run corrompu.

An invalidation does not delete rows; it only changes valid views.

### 10.3 Releases

- **Patch**: display bug or new view, no rerun. - **Minor**: new datasets/methods/views, no mandatory protocol change. - **Major**: new grid or new official scoring policy.

A release can publish several leaderboards, each with its`ViewDefinition`.

## 11. Gouvernance et confidentialite

### 11.1 Public, restricted, private

Everything that is published is visible. For private datasets, publication must be limited to authorized information:

- statistical map; - aggregated scores; - residues only if authorized; - no raw X; - no raw except explicit policy.

### 11.2 Citation

The pipeline contains all the operators and parameters; Arena must maintain citations to libraries and methods when they exist.

View exports should produce a list of citations:

- dataset or dataset card; - source method/operator; - nirs4all version; - protocol/view definition.

## 12. Roadmap parallele

### Overview

The sites are designed to progress in parallel. Blocking dependencies are limited to two common contracts:`PipelineDAGSpec`and`ArenaStore schema`.

| Stream | Goal | Can start when | Deliverable |
|---|---|---|---|
| A. Schema Arena | SQLite tables + Parquet schemas | now | `arena_schema.sql`, array schemas |
| B. Pipeline DAG | nirs4all/dag-ml DAG canonicalization | now | `PipelineDAGSpec` + hash |
| C. Dataset cards | Mock, then integration with `nirs4all-datasets` v1 | now with mock | `DatasetCard`, `DatasetFingerprint` |
| D. Ingestion workspace | Import/clean from nirs4all workspace | after partial A | `n4a-arena ingest-workspace` |
| E. Versioned scores | `ScoreComputationSpec`, recalculation, supersede | after partial A | score versioning |
| F. Residual store | Parquet residuals + publication policies | after partial A | `ResidualSet` |
| G. Dataviz MVP | UI/exports on fixture data | with A/C/B fixtures | explorer + configurable leaderboard |
| H. nirs4all export | Library evolution for a clean Arena export | after B/D prototypes | `nirs4all export-arena` |
| I. Tests/fixtures | Two datasets x DAGs x scores | now | validation fixtures |

### Phase 0 - Contrats et fixtures

Parallel tasks:

- A1. Write the minimal SQLite Arena schema. - B1. Set JSON canonical`PipelineDAGSpec`. - C1. Define`DatasetCard`mock compatible future v1 datasets. - I1. Create fixture with 2 datasets, 2 mono-target tasks, 3 DAGs including a branch/merge. - E1. Define`ScoreComputationSpec`.

Sortie: fixtures chargeables et hashes stables.

### Phase 1 - Store local

Parallel tasks:

- A2. Implement`ArenaStore`SQLite + Parquet. - D1. Import existing nirs4all workspace without artifacts. - F1. Write/read again`ResidualSet`Parquet. - E2. Ingest fold/CV/refit scores with explicit version. - B2. Extract nodes/operators/params from nirs4all pipeline.

Sortie: `n4a-arena ingest-workspace <workspace>` et requetes locales.

### Phase 2 - Clean et dedup

Parallel tasks:

- D2. Clean report: legacy fields, ignored artifacts, class scores. - A3. Tables dedup operators/params/DAG/dataset/task/RNG. - F2. Dedup y_true/residues when allowed. - E3. Recalculation or supersession of scores. - C2.`DatasetFingerprint`support for anonymized user runs.

Sortie: ingestion idempotente, compacte et auditable.

### Phase 3 - Dataviz MVP

Parallel tasks:

- G1. Explorer runs/conditions.
- G2. Leaderboard configurable sans baseline canonique.
- G3. Matrice DAG x dataset/task.
- G4. Effet operateur/parametre.
- G5. Detail DAG + folds + residuals.

Sortie: site/app interne exploitable sur fixtures et premiers workspaces.

### Phase 4 - Integration nirs4all

Parallel tasks:

- H1. Add clean Arena export to nirs4all. - H2. Add emission`PipelineDAGSpec`from nirs4all/dag-ml. - H3. Add full`RNGContext`to runs. - H4. Add Studio/nirs4all-web hooks to Arena export. - D3. Validate import from exports produced by all capsules.

Sortie: producteur unifie nirs4all.

### Phase 5 - Public benchmark

Parallel tasks:

- C3. Plug in`nirs4all-datasets`v1. - G6. Public exports with restricted/private policies. - E4. Set first public`ViewDefinition`. - I2. Non-regression tests on hashes/scores. - Contribution and citation documentation.

Sortie: premiere Arena publique utilisable.

## 13. Questions residuelles

- What exact format of`PipelineDAGSpec`will be shared with dag-ml? - What residuals can be published for each level of confidentiality? - What is the size limit for retaining the residue of large user runs? - Should we materialize certain columns derived from the DAG to speed up the views, or calculate them on demand?

## 14. Decisions a appliquer au prochain dev

1. Start with SQLite + Parquet, not DuckDB. 2. Do not store fitted artifacts. 3. Make`PipelineDAGSpec`the heart of design. 4. Index the operators and parameters to analyze their effects. 5. Support anonymized datasets via`DatasetFingerprint`. 6. Make the score versioned and auditable. 7. First build the ingestion/clean from workspace nirs4all.
