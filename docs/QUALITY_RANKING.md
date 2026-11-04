# Observed pipeline and component rankings

`quality_ranking(store, RankingRequest(...))` constructs comparison evidence in
Python and calls `n4m.ranking.rank` in the optional full `nirs4all-methods` runtime. Install the
`ranking` extra with a runtime that exports `n4m.ranking/1`. An absent or older
runtime returns `state: unavailable`, never a Python or browser approximation.
The dependency version alone does not establish runtime capability. Until a ranking-enabled release is published, install the full methods package from its ranking implementation branch with ABI 2.21; the slim `pls4all` distribution does not provide this module.

## Evidence and interpretation

Eligible observations must have a completed execution (`ok` in the committed
Arena export contract, or explicit `completed`), valid execution and score,
finite metric, reported direction and no unsafe flags. Dataset fingerprint,
variant, task, task type and score-computation identity must exist. Actual outer
split indices must be identified by `split_instances.split_indices_hash`;
`cv` and `fold` observations also need
`cv_instances.engine_fold_set_fingerprint`. A generated hash of a splitter
configuration is not evidence of the evaluated sample assignment. Existing
historical observations without these identities remain usable by other Arena
views, but cannot establish matched ranking evidence. No migration invents them.

A producer-supplied fingerprint is an identity claim recorded by the producer;
the Arena does not retrospectively verify sample membership from that claim.
The synthetic tests supply deliberately controlled identity labels. They are not
cryptographic provenance claims about measured public datasets.

Comparisons match dataset, task, variant, actual outer split, actual CV identity
where relevant, fold, refit policy, aggregation level and score protocol. Scope,
partition, metric and direction are explicit request fields. Multiple protocols,
task types or aggregation levels require filtering. A dataset with multiple task
or variant contexts is refused, rather than averaging incompatible targets or
sample sets. Different datasets may contribute different candidate subsets:
A/B on D1 and B/C on D2 remain valid input to indirect ratings. Generalization
across domains requires a stable common rating assumption.

Repeated executions are averaged within pipeline, pipelines equally within an
observed component group, and matched blocks equally within dataset. Ratings
(Elo, Bradley–Terry, TrueSkill 1 and Weng–Lin Bradley–Terry/OpenSkill) receive one
masked two-candidate row per dataset and candidate pair. Dataset trial counts do
not multiply the comparison weight. Candidate-rich datasets contribute more
distinct pairs; this limitation is explicit in artifacts. Pair rows from a
dataset are dependent. The uncertainty is conditional approximation from the
native algorithm, not an independent cross-dataset confidence interval.

Average rank uses genuine dataset candidate panels instead of pair rows. All
matched blocks within a dataset must contain the same panel; otherwise this
analysis is unavailable. Dataset panels may be incomplete across datasets, with
an explicit comparability warning for their descriptive average ranks. Friedman
uses only complete panels and assumes independent datasets. For the rating
methods the host suppresses Friedman inference from the artificial pair rows.
No raw RMSE values are pooled across datasets or interpreted as a universal loss.

Model rankings group version-qualified operators, averaging their observed
configurations. Preprocessing sequences preserve DAG order and configurations;
branching or ambiguous order cannot establish a sequence. Operator rankings
use version-qualified presence; co-occurring operators are not compared against
each other on their shared pipeline. Feature-selection and augmentation rankings
use observed configurations including absence. Parameter requests must specify
both operator and parameter; multiple occurrences in a pipeline are ambiguous.
Operator versions are part of all component identities. These distributions are
**observed performance**, not controlled, adjusted or causal contributions.

Splitter ranking requires `split_criterion: external_error`, scope `test` or
`refit`, and partition `test` or `final`. It matches complete pipeline and actual
outer holdout identity while allowing the internal CV strategy to vary. It does
not rank strategies by their own CV score or compare different holdouts as a
controlled splitter experiment. Missing external evidence is unavailable.

## Artifact contract

`n4a.quality-ranking/1` is the shared UI wire format. Every artifact, including an
unavailable one, contains:

- `state` (`ready` or `unavailable`), `reason` (nullable machine-readable string).
- `request_id`: hash of the complete normalized `request`; request defaults and
  filters, including method options, aggregation and ordering, are part of it.
- `dimension`, `method`, `metric`, `scope`, `partition`, `direction`.
- `config`: `tie_tolerance`, caller `options`, actual `aggregation`
  (`dataset_pair_mean` or `dataset_panel_mean`), and deterministic `order`
  (`dataset_candidate_lexical`). Native resolved defaults are `native_options`.
- `interpretation`: `observed_performance` or `observed_component_performance`.
- `candidates`: `{id, label, score, uncertainty, average_rank, observations,
  matches, component, wins, ties, losses, rank}`. All numerical fields come from
  methods. Global rank is null for disconnected graphs and nonconverged BT fits.
- `coverage`, `convergence`, `uncertainty_kind`, `score_direction`,
  `native_schema_version`, and (when ready) `native_variant`/`native_options`.
- `inference`: native Friedman fields for genuine panels; otherwise null fields
  with `friedman_suppressed_for_dependent_pair_decomposition`.
- `provenance`: source and accepted counts, exclusion reason counts, datasets,
  matched block identities, comparison rows with their source block IDs,
  candidate universe with supporting pipeline IDs, aggregation and weighting.
- `warnings` and `capabilities`, including the distinct component-analysis
  endpoint and the remaining unqualified adjusted-interaction analysis.

The methods package also provides paired positive-loss relative gains and
caller-variance meta-analysis. This observed-ranking endpoint does not manufacture
paired sampling variances or expose those distinct effect estimates as ranks.

## Service and static delivery

`POST /api/quality-ranking` accepts the complete `RankingRequest` JSON. The GET
form supports the same scalar filters and defaults. Known metric directions are
derived when omitted and conflicting directions are refused; custom metrics
need an explicit direction. Unknown POST fields are rejected. All requested
method options are forwarded to methods, which validates them. Seeded scientific
reordering is not silently introduced: online pair updates use lexical dataset
and candidate order and their order dependence remains visible.

`api.qualityRanking(request)` is available in both service and static API clients.
`api.qualityRankingRequests()` reports unrestricted service mode, or an index of
exact precomputed static requests with state and reason. Static builds opt in:

```python
build_site(store_root, output, ranking_requests=[
    {"metric": "rmse", "dimension": "pipeline", "method": "bradley_terry"},
    {"metric": "rmse", "dimension": "model", "method": "average_rank"},
])
```

At most 64 explicit requests are accepted per build. Each cohort is limited to
500 candidates and two million matrix cells; larger requests need narrower
filters. No ranking is precomputed by default. Artifacts, including unavailable
states, are embedded in `bundle.json`. Browser code performs exact normalized
request matching only. A different dataset, task, collection, scope, partition,
operator, parameter, protocol, method or option returns
`static_ranking_request_not_precomputed`; it never reuses a global ranking for a
filtered cohort. No independent ranking algorithm exists in JavaScript.

All ranking, relative-gain and component-analysis requests share a bounded
original-observation reader. It joins observations by their primary/foreign
identities once; identical score values never create a quadratic self-join.
A narrow ID scan refuses source cohorts above 100,000 observations before wide
records are materialized, and the wide query repeats the bound to protect
against concurrent ingestion. The refusal reason is
`source_observation_limit_requires_filter`; no truncated cohort is fitted.
`provenance.source_scan` records the limit, whether it was exceeded and whether
`source_observations` is only a lower bound. Refusals also state that lower bound
in visible warnings. This source cap precedes eligibility exclusions and is
distinct from each estimator's stricter candidate/design limits. Pipeline
metadata reads are scoped to the selected evidence; component graph structures
are parsed once per pipeline rather than repeated for every score row.

## Matched descriptive component comparisons

`comparison_mode: matched` adds a strict rest-of-pipeline fingerprint to each
comparison block. It supports model, preprocessing sequence/operator, feature
selection, augmentation and operator-qualified parameter dimensions. A parameter
context removes only the chosen parameter; other settings of the same operator
remain fixed. Preprocessing-operator matching requires `operator` and compares
its presence with absence, while preserving all other stages. Selected factor
versions remain in candidate identities. Matched comparison requests for whole
pipelines or split strategies are explicitly unsupported.

Qualification is conservative: a full, explicit, unbranched linear graph with a
single model is required; source graph metadata, operator versions, remaining
parameters, fit scope, order and edge semantics stay in the context. Ambiguous
roles, multiple target occurrences, branches, merges and unsafe edge contraction
receive explicit exclusion reasons. Preprocessing sequences must be contiguous;
feature selection and augmentation mask at most one selected node. Matched
contrasts remain descriptive, with no randomization or causal identification
claim. Adjusted effects and surrogate importance use the separate native
component-analysis contract below.

The ingestion normalizer preserves arbitrary graph, node and edge scientific
extensions, including the declared node `source_id`. These fields participate in
the canonical pipeline hash and remain in stored `graph_json`; edge-table rows
remain a structural indexing projection. Only consumed syntax aliases, derived
identities and the descriptive `human_label`, `label`, `title` and `description`
fields are discarded. Node `model_family` is descriptive and excluded as well.
The nested operator shorthand accepts only `class`/`operator`/`name` and `params`;
additional nested fields are rejected with an explicit normalization error.
Place execution settings on the node or in its parameter mapping.

Canonical graphs carry `arena_normalization_version: 2`, which records source
preservation of the provided producer input, not an independent scientific
attestation, and does not change semantic hashes. Existing hashes without
scientific extensions remain stable. Stored graphs without this marker cannot
establish matched component evidence: observations receive the exclusion reason
`matched_graph_source_requires_reingestion`. Earlier normalizers discarded
extensions, so qualification cannot be recovered from their normalized graphs.
Rebuild a fresh store from the retained original producer exports to qualify
historical evidence; ordinary idempotent reingestion into an existing store does
not upgrade its records. Observed rankings remain available for historical data.

## Paired relative gains and meta-analysis

`POST /api/quality-relative-gain` accepts the flat ranking cohort fields with
`method: relative_gain`, `baseline_id`, `candidate_id`, and optional
`effect_variances: {dataset_id: relative_effect_sampling_variance}`. IDs must come
from the candidate universe. The host uses exactly the same eligible matched
cohort and its direct paired dataset means. An A/B and B/C comparison graph can
support an indirect rating, but cannot establish an A/C loss ratio.

Only strictly positive losses for known positive-loss metrics are eligible:
RMSE, MSE, MAE, median absolute error, NRMSE and log loss. Zero/nonpositive values,
R², accuracy and other arbitrary signed metrics are refused. The native
`paired_relative_gain` computes `(baseline-candidate)/baseline`; positive gain
means improvement. Without effect variances, mean gain and sample dispersion
are descriptive and pooled gain/standard error are unavailable. Supplied
variances must match the exact directly paired dataset IDs and be finite and
strictly positive. They must describe the relative effect, including paired
covariance; no fold variance or independence is inferred. The caller must assess
independence of study dataset effects for DerSimonian–Laird random effects.

The `n4a.quality-relative-gain/1` artifact contains `state`, `reason`, normalized
`request`/`request_id`, cohort scalar fields, `baseline_id`, `candidate_id`,
`candidates: [{id,label}]`, ordered `studies: [{dataset,baseline_loss,
candidate_loss,effect_variance,matched_blocks}]`, native `result`
(`n4m.relative_gain/1`, or null), the common `provenance`, warnings and capabilities.
`provenance.study_dataset_ids` gives the exact ordering passed to the native
kernel. `result.sample_sd` is descriptive effect dispersion, not a standard error.

The API clients expose `qualityRelativeGain(request)` and
`qualityRelativeGainRequests()`. Static builds accept opt-in
`relative_gain_requests=[{baseline_id: ..., candidate_id: ..., ...}]` within the
same total limit of 64 snapshots. Static retrieval matches the entire request,
including variance map, and otherwise returns
`static_relative_gain_request_not_precomputed` without numerical execution.

## Native adjusted effects and surrogate importance

`POST /api/component-analysis` accepts `ComponentAnalysisRequest`, with
`method: mixed_effects` or `fanova`. The optional full methods runtime must export
`n4m.component_analysis` and ABI 2.22 with the requested backend compiled in.
Missing runtimes and backend-disabled builds produce explicit unavailable
artifacts. The ranking endpoint directs these methods to the component-analysis
endpoint; they are not alternative rating algorithms.

Both analyses use qualified **original score observations**, including distinct
executions, rather than synthetic pair rows or cross-dataset raw-loss means.
Exact duplicate observations add no weight; conflicting values for the same
execution/score/fold are refused. Original trial imbalance can affect these fits.
Metric, direction, scope, partition, aggregation level, dataset/task/variant,
protocol, actual outer split and actual CV identities follow the same evidence
requirements as rankings. Graphs must carry source-preserving normalization
version 2. Strict linear graph qualification applies before factor extraction.
Rows have deterministic dataset/block/pipeline/execution order, and the artifact
records an input fingerprint and each original observation's execution, pipeline,
task and evaluation block.

`outcome_policy: scale_free` accepts known dimensionless metrics such as NRMSE,
R², CCC, accuracy and F1. Raw RMSE, MSE, MAE and log-loss values cannot be pooled
across tasks in this mode. For positive-loss metrics,
`outcome_policy: native_baseline_ratio` requires `baseline_pipeline_id`. Each
original observation must have one unambiguous baseline loss on its exact
actual evaluation block; several distinct baseline values are refused. Native
`normalize_loss` computes `loss / baseline_loss`, with a strictly positive
baseline and nonnegative numerator. Zero loss is supported without adding an
epsilon. Baselines are treated as fixed normalizers; their sampling uncertainty
is not propagated. Lower ratios are better.

A request declares up to 16 named `factors`. Supported dimensions are model,
preprocessing sequence/operator, feature selection, augmentation, and an exact
operator-qualified parameter. Model values are `operator@version` strings;
sequence/configuration values are canonical configuration strings; operator
presence uses its qualified version and `none` for absence. Parameter factors
require an explicit `model_slice` and a single operator occurrence. Multiple
implementation versions need an exact `operator_version` filter. No missing
conditional parameter is filled with zero, a category code or a default value.
Categorical parameter levels use the preview's canonical type-tagged string IDs;
this preserves distinctions such as integer `1` versus floating `1.0` in browser
JSON. Declare those exact IDs as levels/references. Integer native option values
(including seeds) must be exact JSON-safe integers, so static clients cannot
silently round a different request into the same cache entry.
The default `missing_policy: reject` refuses incomplete designs;
`exclude_complete_case` explicitly records omitted observations. Effects remain
conditional associations in the declared design, not causal contributions or
controls for every unmodeled pipeline setting.

Use `preview_only: true` to obtain observed factor values and source provenance
without fitting. The artifact is unavailable with reason `design_preview_only`,
and its design can be used to declare references and domains. This preview runs
without the numerical runtime. It does not attest the suitability of a design.

### Gaussian mixed effects

Categorical factors require an explicit `reference` drawn from the declared or
observed levels. The host constructs an intercept and treatment-coded columns;
numeric parameter factors contribute an uncentered slope. Columns are never
silently dropped. Native Gaussian ML/REML fits two random intercept terms:
dataset and dataset × complete actual evaluation-block identity. Multiple
datasets and multiple blocks are required; native rank deficiency, variance
confounding, nonconvergence and numerical refusal remain explicit statistical
states with no provisional estimates. Boundary fits retain their native flags.

The native result (`n4m.mixed_effects/1`) supplies beta, coefficient standard
errors, the full covariance matrix and variance components in exact design order
(residual first, then dataset and dataset/block). Standard errors are conditional
plug-in values; they omit variance-parameter uncertainty and do not establish
independent folds. No confidence interval, significance test or p-value is
manufactured by the host or UI. With loss-ratio outcomes, a negative coefficient
means a lower fitted response relative to the declared reference.

```json
{
  "method": "mixed_effects",
  "metric": "nrmse",
  "factors": [{
    "name": "model", "dimension": "model",
    "reference": "sklearn.linear_model.Ridge@unreported"
  }],
  "options": {"reml": true, "max_evaluations": 1000}
}
```

Select actual identifiers from a preview; the example does not imply those
operators or reference levels exist in a particular store.

### Random-forest fANOVA

Every categorical factor requires explicit `levels`; numeric parameters require
finite increasing `bounds`. These declarations define an independent uniform
product domain. They assert that the modeled Cartesian combinations are
scientifically feasible. Parameter analyses remain within one qualified model,
so the forest never predicts another model's inapplicable parameter settings.
Unseen categorical training levels are refused; numeric domains beyond observed
training values produce an extrapolation warning. Unsupported conditional search
spaces require a narrower slice rather than an invented Cartesian encoding.

Assign every eligible dataset explicitly to `training_datasets` or
`validation_datasets`. These sets must be disjoint; no split is inferred from row
order or individual repeated folds. An empty held-out set is allowed with an
explicit warning that surrogate generalization has not been established. Native
training RMSE and held-out RMSE/R² are preserved, including poor or negative
held-out R². Diagnostics describe prediction on the supplied held-out datasets;
they do not prove a generalization guarantee.

Native `n4m.fanova/1` fits a seeded regression forest and integrates first-order
and pairwise variance components over the declared product domain. Reported
fractions are normalized per informative tree and then averaged; they are not
the decomposition of the forest ensemble mean. `tree_sd` describes variation
between trees, not a standard error or confidence interval. Constant-response or
zero-surrogate-variance fits withhold importance. Larger importance indicates
surrogate variation, not the direction or size of an improvement and not
causality. Native limits on forest size, feature count, depth and exact-marginal
work are enforced without silently changing the requested domain or estimator.

### Artifact, static delivery and controls

`n4a.component-analysis/1` contains normalized `request`/`request_id`, explicit
`state`/`reason`, method and metric lens, `design` (factors, observed values,
columns, references, domains, model slice, outcome policy and group names),
original-observation `provenance`, the authoritative native `result`, and
warnings. Statistical refusal diagnostics remain visible when estimates are
unavailable. Every numerical output is JSON-finite or null.

The clients expose `componentAnalysis(request)` and
`componentAnalysisRequests()`. Static builds accept
`component_analysis_requests=[...]`, and the CLI accepts
`--component-analysis-requests analyses.json`. The existing combined limit of 64
snapshots includes rankings, relative gains and these analyses. Static lookup
matches the entire normalized request, including references, domains, source
filters, model slice, training/held-out dataset assignments, native options and
preview mode. A mismatch returns
`static_component_analysis_request_not_precomputed`; browsers never fit a
substitute analysis.

The browser's Analysis selector offers adjusted mixed effects and surrogate
functional ANOVA. Its simple design presets expose reference/domain previews,
qualified model/parameter selection, outcome normalization, explicit missing-case
policy and native options; no formula language or browser estimator is used.
Shared React views render native coefficients/SE/covariance and variance
components, or main/pair importance and tree dispersion, alongside exact HTML
tables and fit/generalization diagnostics.
