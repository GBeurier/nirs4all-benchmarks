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
