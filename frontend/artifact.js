// Validate the serialized boundary before passing data to shared typed components.
const object = (x) => x !== null && typeof x === "object" && !Array.isArray(x);
const string = (x) => typeof x === "string";
const count = (x) => Number.isSafeInteger(x) && x >= 0;
const finiteOrNull = (x) => x === null || (typeof x === "number" && Number.isFinite(x));
const strings = (x) => Array.isArray(x) && x.every(string);
const nullableObject = (x) => x === null || object(x);
const canonical = (x) => JSON.stringify(object(x) ? Object.fromEntries(Object.keys(x).sort().map(key => [key, x[key]])) : x);
export function validateQualityRanking(value, requested) {
  const fail = () => { throw new Error("The ranking response is incomplete or uses an unsupported format. Please try another selection."); };
  const r = value;
  if (!object(r) || r.schema_version !== "n4a.quality-ranking/1" || !["ready", "unavailable"].includes(r.state)
      || !string(r.request_id) || !object(r.request) || !object(r.config)
      || !(r.reason === null || string(r.reason)) || !Array.isArray(r.candidates)
      || !["observed_performance", "observed_component_performance"].includes(r.interpretation)
      || !strings(r.warnings) || !nullableObject(r.inference) || !nullableObject(r.convergence)
      || ![null, "approximate_posterior_sd"].includes(r.uncertainty_kind)
      || ![null, "lower_is_better", "higher_is_better"].includes(r.score_direction)) fail();
  for (const key of ["dimension", "method", "metric", "scope", "partition"]) {
    if (!string(r[key]) || r[key] !== r.request[key] || r[key] !== requested[key]) fail();
  }
  for (const key of ["dataset", "task", "collection", "protocol", "operator", "parameter", "split_criterion"]) {
    if ((r.request[key] ?? null) !== (requested[key] ?? null)) fail();
  }
  if (requested.direction && r.request.direction !== requested.direction) fail();
  if ((r.request.comparison_mode ?? "observed") !== (requested.comparison_mode ?? "observed")) fail();
  for (const key of ["aggregation", "order"]) if (r.request[key] !== requested[key]) fail();
  if (r.request.tie_tolerance !== requested.tie_tolerance || canonical(r.request.options) !== canonical(requested.options)) fail();
  const p = r.provenance;
  if (!object(p) || !strings(p.datasets) || !Array.isArray(p.blocks) || !Array.isArray(p.comparisons)
      || !string(p.aggregation) || !string(p.dataset_weighting) || !object(p.excluded)
      || !Object.values(p.excluded).every(count)) fail();
  for (const key of ["matched_blocks", "comparison_rows", "source_observations", "accepted_observations"]) if (!count(p[key])) fail();
  if (p.candidate_universe !== undefined && (!Array.isArray(p.candidate_universe)
      || !p.candidate_universe.every((c) => object(c) && string(c.id) && string(c.label) && strings(c.pipelines)))) fail();
  const ids = new Set();
  for (const c of r.candidates) {
    if (!object(c) || !string(c.id) || ids.has(c.id) || !string(c.label)) fail();
    ids.add(c.id);
    for (const key of ["score", "rank", "uncertainty", "average_rank"]) if (!finiteOrNull(c[key])) fail();
    for (const key of ["observations", "matches", "component", "wins", "ties", "losses"]) if (!count(c[key])) fail();
    if (c.uncertainty !== null && c.uncertainty < 0) fail();
  }
  if (r.coverage !== null) {
    if (!object(r.coverage)) fail();
    for (const key of ["blocks", "complete_blocks", "paired_comparisons", "connected_components"]) if (!count(r.coverage[key])) fail();
  }
  const cap = r.capabilities;
  if (!object(cap) || !strings(cap.methods) || !strings(cap.dimensions) || !object(cap.deferred) || !Object.values(cap.deferred).every(string)) fail();
  return r;
}

export function validateRelativeGain(value, requested) {
  const fail = () => { throw new Error("The relative gain response is incomplete or uses an unsupported format. Please try another selection."); };
  const r = value;
  if (!object(r) || r.schema_version !== "n4a.quality-relative-gain/1" || !["ready", "unavailable"].includes(r.state)
      || !string(r.request_id) || !object(r.request) || !(r.reason === null || string(r.reason))
      || !Array.isArray(r.candidates) || !r.candidates.every(c => object(c) && string(c.id) && string(c.label))
      || !Array.isArray(r.studies) || !strings(r.warnings) || !object(r.provenance) || !object(r.capabilities)) fail();
  for (const key of ["dimension", "metric", "scope", "partition", "baseline_id", "candidate_id"]) {
    if (!string(r[key]) || r[key] !== r.request[key] || r[key] !== requested[key]) fail();
  }
  for (const key of ["dataset", "task", "collection", "protocol", "operator", "parameter", "split_criterion", "comparison_mode", "aggregation", "order", "method", "tie_tolerance"]) {
    if ((r.request[key] ?? null) !== (requested[key] ?? null)) fail();
  }
  if (requested.direction && r.request.direction !== requested.direction) fail();
  if (canonical(r.request.options) !== canonical(requested.options) || canonical(r.request.effect_variances) !== canonical(requested.effect_variances)) fail();
  for (const s of r.studies) {
    if (!object(s) || !string(s.dataset) || !strings(s.matched_blocks)
        || typeof s.baseline_loss !== "number" || !Number.isFinite(s.baseline_loss) || s.baseline_loss <= 0
        || typeof s.candidate_loss !== "number" || !Number.isFinite(s.candidate_loss) || s.candidate_loss <= 0
        || !finiteOrNull(s.effect_variance) || (s.effect_variance !== null && s.effect_variance <= 0)) fail();
  }
  const p = r.provenance;
  if (!strings(p.datasets) || !Array.isArray(p.blocks) || !Array.isArray(p.comparisons)
      || !string(p.aggregation) || !string(p.dataset_weighting) || !object(p.excluded) || !Object.values(p.excluded).every(count)) fail();
  for (const key of ["matched_blocks", "comparison_rows", "source_observations", "accepted_observations"]) if (!count(p[key])) fail();
  if (!strings(r.capabilities.methods) || !strings(r.capabilities.dimensions) || !object(r.capabilities.deferred) || !Object.values(r.capabilities.deferred).every(string)) fail();
  if (r.result !== null) {
    const n = r.result;
    if (!object(n) || n.schema_version !== "n4m.relative_gain/1" || !count(n.n) || n.n !== r.studies.length || !string(n.effect) || !string(n.inference_assumption)
        || ![null, "dersimonian_laird"].includes(n.meta_analysis) || ![null, "caller_supplied_effect_variance"].includes(n.variance_source)) fail();
    for (const key of ["mean_gain", "sample_sd", "pooled_gain", "standard_error", "tau_squared", "q", "i_squared"]) if (!finiteOrNull(n[key])) fail();
    for (const key of ["sample_sd", "standard_error", "tau_squared", "q", "i_squared"]) if (n[key] !== null && n[key] < 0) fail();
    if (requested.effect_variances === null && (n.meta_analysis !== null || n.pooled_gain !== null || n.standard_error !== null)) fail();
  } else if (r.state === "ready") fail();
  return r;
}

const deepCanonical = (x) => JSON.stringify(canonicalObject(x));
function canonicalObject(x) {
  if (Array.isArray(x)) return x.map(canonicalObject);
  return object(x) ? Object.fromEntries(Object.keys(x).sort().map(key => [key, canonicalObject(x[key])])) : x;
}
const metricDirections = { mse:"min",rmse:"min",nrmse:"min",mae:"min",medae:"min",bias:"min",log_loss:"min",r2:"max",rpd:"max",rpiq:"max",ccc:"max",accuracy:"max",balanced_accuracy:"max",f1_macro:"max",f1_micro:"max",f1_weighted:"max",precision_macro:"max",recall_macro:"max",roc_auc:"max",mcc:"max" };
export function normalizeComponentRequest(params = {}) {
  const request = { method:"mixed_effects",metric:"nrmse",scope:"cv",partition:"validation",direction:null,dataset:null,task:null,collection:null,protocol:null,
    aggregation_level:"sample",model_slice:null,factors:[{name:"model",dimension:"model"}],outcome_policy:"scale_free",baseline_pipeline_id:null,
    missing_policy:"reject",training_datasets:[],validation_datasets:[],options:{},preview_only:false,...params };
  request.direction ??= metricDirections[String(request.metric).toLowerCase()] || null;
  request.factors = request.factors.map(factor => ({kind:"categorical",operator:null,operator_version:null,parameter:null,reference:null,levels:null,bounds:null,...factor}));
  return request;
}
export function validateComponentAnalysis(value, requested) {
  const fail = () => { throw new Error("The component analysis response is incomplete, belongs to another design, or uses an unsupported format."); };
  const r = value;
  if (!object(r) || r.schema_version !== "n4a.component-analysis/1" || !["ready","unavailable"].includes(r.state)
      || !string(r.request_id) || !object(r.request) || !(r.reason === null || string(r.reason)) || !strings(r.warnings)
      || deepCanonical(r.request) !== deepCanonical(normalizeComponentRequest(requested))) fail();
  for (const key of ["method","metric","scope","partition","direction"]) if (r[key] !== r.request[key]) fail();
  const d = r.design, p = r.provenance;
  if (!object(d) || !Array.isArray(d.factors) || !Array.isArray(d.columns) || !object(d.references) || !Array.isArray(d.domains)
      || !strings(d.group_names) || !string(d.outcome_policy) || !(d.model_slice === null || string(d.model_slice))) fail();
  if (!d.columns.every(column => object(column) && string(column.name) && (column.factor === null || string(column.factor)))) fail();
  if (!d.factors.every(factor => object(factor) && string(factor.name) && string(factor.dimension) && ["numeric","categorical"].includes(factor.kind) && Array.isArray(factor.observed_levels))) fail();
  if (!object(p) || !count(p.source_observations) || !count(p.accepted_observations) || !object(p.excluded)
      || !Object.values(p.excluded).every(count) || !strings(p.datasets) || !strings(p.training_datasets)
      || !strings(p.validation_datasets) || !Array.isArray(p.observations) || !Array.isArray(p.baseline_observations)) fail();
  function finiteTree(x) { return typeof x === "number" ? Number.isFinite(x) : Array.isArray(x) ? x.every(finiteTree) : object(x) ? Object.values(x).every(finiteTree) : true; }
  if (!finiteTree(r)) fail();
  const n = r.result;
  if (n === null) { if (r.state === "ready") fail(); return r; }
  if (!object(n) || !["ready","unavailable"].includes(n.state) || !(n.reason === null || string(n.reason)) || !object(n.diagnostics) || !strings(n.warnings)) fail();
  // Reject NaN/Infinity at every depth, including options, diagnostics and domains.
  if (r.method === "mixed_effects") {
    if (n.schema_version !== "n4m.mixed_effects/1") fail();
    const size = d.columns.length;
    if (r.state === "ready" && (n.state !== "ready" || !Array.isArray(n.beta) || n.beta.length !== size || !n.beta.every(x => typeof x === "number" && Number.isFinite(x))
      || !Array.isArray(n.standard_errors) || n.standard_errors.length !== size || !n.standard_errors.every(x => typeof x === "number" && Number.isFinite(x) && x >= 0)
      || !Array.isArray(n.covariance) || n.covariance.length !== size || !n.covariance.every(row => Array.isArray(row) && row.length === size && row.every(x => typeof x === "number" && Number.isFinite(x)))
      || !Array.isArray(n.variances) || n.variances.length !== d.group_names.length + 1 || !n.variances.every(x => typeof x === "number" && Number.isFinite(x) && x >= 0))) fail();
  } else if (r.method === "fanova") {
    if (n.schema_version !== "n4m.fanova/1" || !Array.isArray(n.feature_importance) || !Array.isArray(n.interactions)
      || n.domain_measure !== "uniform_product" || n.aggregation !== "mean_per_tree_normalized_components"
      || n.uncertainty_kind !== "tree_dispersion_not_confidence") fail();
    const validFraction = x => finiteOrNull(x) && (x === null || x >= 0);
    const ids = new Set();
    for (const row of n.feature_importance) {
      if (!object(row) || !count(row.index) || row.index >= d.factors.length || ids.has(row.index) || !validFraction(row.fraction) || !validFraction(row.tree_sd)) fail();
      ids.add(row.index);
    }
    for (const row of n.interactions) if (!object(row) || !Array.isArray(row.indices) || row.indices.length !== 2 || !row.indices.every(i => count(i) && i < d.factors.length)
      || row.indices[0] >= row.indices[1] || !validFraction(row.fraction) || !validFraction(row.tree_sd)) fail();
    if (r.state === "ready" && (n.state !== "ready" || ids.size !== d.factors.length)) fail();
  } else fail();
  return r;
}
