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
- `warnings` and `capabilities`, including explicit unqualified mixed-effects,
  fANOVA, matched-contribution and interaction reasons.

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
claim. Adjusted mixed effects and interaction importance remain unqualified.

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
