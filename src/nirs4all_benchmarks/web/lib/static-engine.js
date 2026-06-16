// Static query engine — answers every API call client-side from a JSON snapshot,
// so the SPA runs on GitHub Pages with no backend. Mirrors store/queries.py.

const DIRECTION = {
  mse: "min", rmse: "min", mae: "min", medae: "min", bias: "min", log_loss: "min",
  r2: "max", rpd: "max", rpiq: "max", ccc: "max", accuracy: "max", balanced_accuracy: "max",
  f1_macro: "max", f1_micro: "max", precision_macro: "max", recall_macro: "max", roc_auc: "max", mcc: "max",
};
const dir = (m) => DIRECTION[String(m).toLowerCase()] || "min";

// ── classify_role (mirror of indexing.py) ──
const KIND_TO_ROLE = { input: "input", model: "model", merge: "merge", mean: "merge", average: "merge",
  weighted_mean: "merge", vote: "merge", voting: "merge", stacking: "merge", bagging: "merge",
  aggregator: "merge", augmentation: "augmentation", split: "split", feature_selection: "feature_selection" };
const KEYWORDS = [
  ["augmentation", ["augment", "augmentation", "mixup", "jitter", "warp", "randomshift", "rotation", "addnoise", "gaussiannoise", "synthetic", "oversampl", "smote"]],
  ["scaler", ["standardscaler", "minmaxscaler", "robustscaler", "maxabsscaler", "normalizer", "standardize", "minmax", "scaler"]],
  ["feature_selection", ["selectkbest", "selectfrommodel", "rfe", "boruta", "vip", "cars", "_spa", "geneticselect", "featureselect", "variancethreshold", "pca", "kernelpca", "feature_select"]],
  ["merge", ["stacking", "stackingregressor", "stackingclassifier", "voting", "votingregressor", "bagging", "baggingregressor", "averaging", "weightedmean", "meanensemble", "concat", "blend"]],
  ["model", ["plsregression", "plsr", "_pls", "pls(", "opls", "ikpls", "mbpls", "simpls", "ridge", "lasso", "elasticnet", "linearregression", "logistic", "svr", "svc", "svm", "randomforest", "extratrees", "gradientboost", "histgradientboost", "xgb", "lgbm", "lightgbm", "catboost", "mlpregressor", "mlpclassifier", "cnn", "lstm", "gru", "transformerencoder", "tabpfn", "gaussianprocess", "gpr", "kneighbors", "knn", "decisiontree", "adaboost", "regressor", "classifier"]],
  ["sampler", ["kennard", "spxy", "sampling", "duplex", "sampler"]],
  ["split", ["kfold", "shufflesplit", "groupkfold", "stratified", "leaveoneout", "traintest", "split"]],
  ["preprocessing", ["snv", "msc", "emsc", "savgol", "savitzky", "detrend", "derivative", "deriv", "baseline", "gapsegment", "smooth", "normalize", "osc", "scatter", "resample", "crop", "trim", "haar", "wavelet", "continuum", "firstderiv", "secondderiv", "logtransform", "preprocess", "transform", "filter"]],
];
function classifyRole(operator, kind) {
  const k = (kind || "").trim().toLowerCase();
  if (KIND_TO_ROLE[k]) return KIND_TO_ROLE[k];
  const op = (operator || "").toLowerCase();
  const leaf = op.split(".").pop() || "";
  if (!leaf) return (k === "" || k === "transform") ? "preprocessing" : (k || "other");
  if (op.includes("augment")) return "augmentation";
  for (const [role, needles] of KEYWORDS) if (needles.some((n) => leaf.includes(n))) return role;
  if (k === "transform" || k === "") return "preprocessing";
  return k || "other";
}

// ── stats helpers ──
const mean = (a) => a.reduce((x, y) => x + y, 0) / (a.length || 1);
const pstdev = (a) => { if (a.length < 2) return 0; const m = mean(a); return Math.sqrt(mean(a.map((x) => (x - m) ** 2))); };
function quantile(sorted, p) { if (!sorted.length) return null; const i = (sorted.length - 1) * p; const lo = Math.floor(i), hi = Math.ceil(i); return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (i - lo); }
function median(a) { const s = a.slice().sort((x, y) => x - y); return quantile(s, 0.5); }
function pearson(xs, ys) { const mx = mean(xs), my = mean(ys); let n = 0, dx = 0, dy = 0; for (let i = 0; i < xs.length; i++) { n += (xs[i] - mx) * (ys[i] - my); dx += (xs[i] - mx) ** 2; dy += (ys[i] - my) ** 2; } return dx > 0 && dy > 0 ? n / Math.sqrt(dx * dy) : null; }

export async function createStaticApi(base = "/data") {
  const bundle = await (await fetch(`${base}/bundle.json`)).json();
  const M = bundle.metrics;
  const FAC = bundle.facets;
  const NODES = bundle.pipeline_nodes;
  const dsLabel = Object.fromEntries(bundle.datasets.map((d) => [d.dataset_fingerprint, d.name || d.dataset_fingerprint.slice(0, 10)]));
  // index facets by run_condition_hash
  const facByRc = new Map();
  for (const f of FAC) { if (!facByRc.has(f.run_condition_hash)) facByRc.set(f.run_condition_hash, []); facByRc.get(f.run_condition_hash).push(f); }
  // operators per pipeline (leaf, role)
  const opsByPipe = new Map();
  for (const n of NODES) { if (!n.operator || n.role === "input") continue; if (!opsByPipe.has(n.pipeline_dag_hash)) opsByPipe.set(n.pipeline_dag_hash, []); opsByPipe.get(n.pipeline_dag_hash).push({ leaf: n.operator.split(".").pop(), operator: n.operator, role: n.role }); }

  const runCache = new Map();
  const loadRun = async (h) => { if (runCache.has(h)) return runCache.get(h); const r = await (await fetch(`${base}/runs/${h}.json`)).json(); runCache.set(h, r); return r; };

  const sel = (metric, scope, { quar = false, partition = null, dataset = null } = {}) =>
    M.filter((r) => r.metric_name === metric && r.score_scope === scope && r.score_validity === "valid"
      && (quar || r.execution_validity === "valid")
      && (!partition || r.partition === partition)
      && (!dataset || r.dataset_fingerprint === dataset));

  function groupBy(rows, keyFn) { const g = new Map(); for (const r of rows) { const k = keyFn(r); if (!g.has(k)) g.set(k, []); g.get(k).push(r); } return g; }

  return {
    healthz: async () => ({ status: "ok", version: bundle.meta.version, store: "static", store_exists: true }),
    overview: async () => bundle.overview,
    collections: async () => bundle.collections,
    datasets: async () => bundle.datasets,
    pipelines: async () => bundle.pipelines,
    operators: async () => bundle.operators,
    parameters: async () => bundle.parameters,
    planned: async () => bundle.planned,

    leaderboard: async ({ metric = "rmse", scope = "cv", partition, dataset, include_quarantined = false, limit = 200 } = {}) => {
      const rows = sel(metric, scope, { quar: include_quarantined, partition, dataset });
      const out = [];
      for (const [rc, g] of groupBy(rows, (r) => r.run_condition_hash)) {
        const vals = g.map((r) => r.metric_value).filter((v) => v != null);
        out.push({ run_condition_hash: rc, pipeline_dag_hash: g[0].pipeline_dag_hash, pipeline_label: g[0].pipeline_label,
          main_model: g[0].main_model, dataset_fingerprint: g[0].dataset_fingerprint,
          mean: mean(vals), min: Math.min(...vals), max: Math.max(...vals), n_obs: g.length });
      }
      const d = dir(metric); const worst = d === "max" ? -Infinity : Infinity;
      out.sort((a, b) => ((a.mean ?? worst) - (b.mean ?? worst)) * (d === "max" ? -1 : 1));
      out.slice(0, limit).forEach((r, i) => { r.rank = i + 1; });
      return { metric, scope, direction: d, rows: out.slice(0, limit) };
    },

    matrix: async ({ metric = "rmse", scope = "cv", include_quarantined = false } = {}) => {
      const rows = sel(metric, scope, { quar: include_quarantined });
      const cells = []; const pipes = new Map(); const dsset = new Map();
      for (const [, g] of groupBy(rows, (r) => r.pipeline_dag_hash + "|" + r.dataset_fingerprint)) {
        const vals = g.map((r) => r.metric_value).filter((v) => v != null);
        pipes.set(g[0].pipeline_dag_hash, g[0].pipeline_label || g[0].pipeline_dag_hash.slice(0, 10));
        dsset.set(g[0].dataset_fingerprint, true);
        cells.push({ pipeline_dag_hash: g[0].pipeline_dag_hash, dataset_fingerprint: g[0].dataset_fingerprint,
          value: mean(vals), coverage: new Set(g.map((r) => r.execution_hash)).size });
      }
      return { metric, scope, direction: dir(metric),
        datasets: [...dsset.keys()].map((d) => ({ dataset_fingerprint: d, label: dsLabel[d] || d.slice(0, 10) })),
        pipelines: [...pipes].map(([p, l]) => ({ pipeline_dag_hash: p, label: l })), cells };
    },

    runs: async ({ metric = "rmse", scope = "cv", dataset, pipeline, operator, include_quarantined = true, limit = 500 } = {}) => {
      let rows = sel(metric, scope, { quar: include_quarantined, dataset });
      if (pipeline) rows = rows.filter((r) => r.pipeline_dag_hash === pipeline);
      if (operator) { const ok = new Set([...opsByPipe].filter(([, ops]) => ops.some((o) => o.operator === operator)).map(([p]) => p)); rows = rows.filter((r) => ok.has(r.pipeline_dag_hash)); }
      const out = [];
      for (const [, g] of groupBy(rows, (r) => r.execution_hash)) {
        const vals = g.map((r) => r.metric_value).filter((v) => v != null);
        out.push({ execution_hash: g[0].execution_hash, run_condition_hash: g[0].run_condition_hash, pipeline_dag_hash: g[0].pipeline_dag_hash,
          pipeline_label: g[0].pipeline_label, main_model: g[0].main_model, dataset_fingerprint: g[0].dataset_fingerprint,
          execution_validity: g[0].execution_validity, execution_status: g[0].execution_status, producer_capsule: g[0].producer_capsule,
          time_ms: g[0].time_ms, metric_value: mean(vals) });
      }
      const d = dir(metric); const worst = d === "max" ? -Infinity : Infinity;
      out.sort((a, b) => ((a.metric_value ?? worst) - (b.metric_value ?? worst)) * (d === "max" ? -1 : 1));
      return out.slice(0, limit);
    },

    operatorEffect: async ({ metric = "rmse", scope = "cv" } = {}) => {
      const rows = sel(metric, scope, {});
      const groups = new Map(); const roles = {};
      for (const r of rows) for (const o of (opsByPipe.get(r.pipeline_dag_hash) || [])) {
        if (!groups.has(o.operator)) groups.set(o.operator, []); groups.get(o.operator).push(r.metric_value); roles[o.operator] = o.role;
      }
      const series = [...groups].map(([op, vals]) => ({ operator: op, role: roles[op], n: vals.length, mean: mean(vals),
        median: median(vals), stdev: pstdev(vals), min: Math.min(...vals), max: Math.max(...vals), values: vals }));
      series.sort((a, b) => (a.mean - b.mean) * (dir(metric) === "max" ? -1 : 1));
      return { metric, scope, direction: dir(metric), series };
    },

    parameterEffect: async (params) => {
      const { param, metric = "rmse", scope = "cv" } = params;
      const rows = sel(metric, scope, {});
      const points = [];
      for (const r of rows) {
        const f = (facByRc.get(r.run_condition_hash) || []).find((x) => x.facet_key === `param:${param}`);
        if (!f) continue;
        points.push({ param: f.facet_num != null ? f.facet_num : f.facet_value, numeric: f.facet_num,
          metric_value: r.metric_value, dataset_fingerprint: r.dataset_fingerprint, pipeline_label: r.pipeline_label });
      }
      return { param_name: param, metric, scope, direction: dir(metric), points };
    },

    robustness: async ({ metric = "rmse", scope = "cv" } = {}) => {
      const rows = sel(metric, scope, {});
      const out = [];
      for (const [, g] of groupBy(rows, (r) => r.pipeline_dag_hash + "|" + r.dataset_fingerprint)) {
        const vals = g.map((r) => r.metric_value).filter((v) => v != null); const m = mean(vals), s = pstdev(vals);
        out.push({ pipeline_dag_hash: g[0].pipeline_dag_hash, dataset_fingerprint: g[0].dataset_fingerprint,
          label: g[0].pipeline_label || g[0].pipeline_dag_hash.slice(0, 10), n: vals.length, mean: m, stdev: s, cv_pct: m ? s / m * 100 : 0 });
      }
      out.sort((a, b) => a.stdev - b.stdev);
      return out;
    },

    stats: async ({ metric = "rmse", scope = "cv" } = {}) => {
      const rows = sel(metric, scope, {});
      const byExec = groupBy(rows, (r) => r.execution_hash);
      const execMean = []; const dsVals = new Map();
      for (const [, g] of byExec) { const v = mean(g.map((r) => r.metric_value)); execMean.push({ v, df: g[0].dataset_fingerprint, rc: g[0].run_condition_hash }); (dsVals.get(g[0].dataset_fingerprint) || dsVals.set(g[0].dataset_fingerprint, []).get(g[0].dataset_fingerprint)).push(v); }
      const summ = (arr) => { if (!arr.length) return { n: 0 }; const s = arr.slice().sort((a, b) => a - b); return { n: arr.length, mean: mean(arr), median: quantile(s, 0.5), std: pstdev(arr), min: s[0], max: s[s.length - 1], p25: quantile(s, 0.25), p75: quantile(s, 0.75) }; };
      const all = execMean.map((e) => e.v);
      const by_dataset = [...dsVals].sort().map(([df, vals]) => ({ dataset_fingerprint: df, label: dsLabel[df] || df.slice(0, 10), values: vals, ...summ(vals) }));
      // correlations: numeric facets vs per-execution mean (aggregate per run_condition)
      const rcMean = new Map(); for (const e of execMean) { if (!rcMean.has(e.rc)) rcMean.set(e.rc, []); rcMean.get(e.rc).push(e.v); }
      const byFacet = new Map();
      for (const f of FAC) { if (f.facet_num == null) continue; const ms = rcMean.get(f.run_condition_hash); if (!ms) continue; for (const v of ms) { if (!byFacet.has(f.facet_key)) byFacet.set(f.facet_key, []); byFacet.get(f.facet_key).push([f.facet_num, v]); } }
      const correlations = [];
      for (const [fk, pairs] of byFacet) { const xs = pairs.map((p) => p[0]), ys = pairs.map((p) => p[1]); if (xs.length >= 4 && new Set(xs).size >= 2) { const r = pearson(xs, ys); if (r != null) correlations.push({ facet: fk, r, abs_r: Math.abs(r), n: xs.length }); } }
      correlations.sort((a, b) => b.abs_r - a.abs_r);
      return { metric, scope, direction: dir(metric), summary: summ(all), values: all, by_dataset, correlations };
    },

    facets: async () => {
      const g = new Map();
      for (const f of FAC) { if (!g.has(f.facet_key)) g.set(f.facet_key, { vals: new Set(), role: f.role, numeric: false }); const e = g.get(f.facet_key); e.vals.add(f.facet_value); if (f.facet_num != null) e.numeric = true; if (f.role) e.role = f.role; }
      const out = [];
      for (const [k, e] of g) if (e.vals.size > 1 || k.startsWith("param:")) out.push({ facet_key: k, n_values: e.vals.size, role: e.role, numeric: e.numeric ? 1 : 0 });
      out.sort((a, b) => (a.facet_key.startsWith("param:") - b.facet_key.startsWith("param:")) || a.facet_key.localeCompare(b.facet_key));
      return out;
    },
    facetValues: async (arg) => {
      const key = typeof arg === "string" ? arg : (arg && arg.key);
      const g = new Map();
      for (const f of FAC) if (f.facet_key === key) { if (!g.has(f.facet_value)) g.set(f.facet_value, { facet_num: f.facet_num, n: 0 }); g.get(f.facet_value).n++; }
      return [...g].map(([facet_value, e]) => ({ facet_value, facet_num: e.facet_num, n: e.n }))
        .sort((a, b) => (a.facet_num == null) - (b.facet_num == null) || (a.facet_num - b.facet_num) || String(a.facet_value).localeCompare(b.facet_value));
    },

    pivot: async ({ group_by, color_by, metric = "rmse", scope = "cv", partition, dataset, include_quarantined = false, agg = "mean" } = {}) => {
      const rows = sel(metric, scope, { quar: include_quarantined, partition, dataset });
      const acc = new Map();
      for (const r of rows) {
        const gfs = (facByRc.get(r.run_condition_hash) || []).filter((f) => f.facet_key === group_by);
        const cfs = color_by ? (facByRc.get(r.run_condition_hash) || []).filter((f) => f.facet_key === color_by) : [null];
        for (const gf of gfs) for (const cf of cfs) {
          const key = gf.facet_value + "||" + (cf ? cf.facet_value : "");
          if (!acc.has(key)) acc.set(key, { group_value: gf.facet_value, group_num: gf.facet_num, color: cf ? cf.facet_value : null, color_num: cf ? cf.facet_num : null, vals: [], execs: new Set() });
          const e = acc.get(key); e.vals.push(r.metric_value); e.execs.add(r.execution_hash);
        }
      }
      const out = [...acc.values()].map((e) => ({ group_value: e.group_value, group_num: e.group_num, color: e.color, color_num: e.color_num,
        value: mean(e.vals), min: Math.min(...e.vals), max: Math.max(...e.vals), n: e.execs.size }));
      out.sort((a, b) => ((a.group_num != null ? a.group_num : Infinity) - (b.group_num != null ? b.group_num : Infinity)) || String(a.group_value).localeCompare(String(b.group_value)));
      return { group_by, color_by: color_by || null, metric, scope, direction: dir(metric), agg, rows: out };
    },

    parallel: async ({ dimensions, metric = "rmse", scope = "cv", include_quarantined = false } = {}) => {
      const dims = Array.isArray(dimensions) ? dimensions : String(dimensions).split(",").map((s) => s.trim()).filter(Boolean);
      const rows = sel(metric, scope, { quar: include_quarantined });
      const out = [];
      for (const [, g] of groupBy(rows, (r) => r.execution_hash)) {
        const fmap = {}; for (const f of (facByRc.get(g[0].run_condition_hash) || [])) if (dims.includes(f.facet_key) && !(f.facet_key in fmap)) fmap[f.facet_key] = f.facet_num != null ? f.facet_num : f.facet_value;
        const row = {}; for (const d of dims) row[d] = fmap[d] ?? null;
        row.metric = mean(g.map((r) => r.metric_value)); row.pipeline_label = g[0].pipeline_label; row.execution_hash = g[0].execution_hash;
        out.push(row);
      }
      return { dimensions: [...dims, "metric"], metric, scope, direction: dir(metric), rows: out };
    },

    graph: async ({ kind = "pipelines", metric = "rmse", scope = "cv", min_jaccard = 0.34 } = {}) => {
      const rows = sel(metric, scope, {});
      const pipeAgg = new Map();
      for (const [, g] of groupBy(rows, (r) => r.pipeline_dag_hash)) {
        const vals = g.map((r) => r.metric_value); pipeAgg.set(g[0].pipeline_dag_hash, { label: g[0].pipeline_label, main_model: g[0].main_model, score: mean(vals), execs: new Set(g.map((r) => r.execution_hash)) });
      }
      if (kind === "operators") {
        const stat = new Map();
        for (const [pdh, a] of pipeAgg) for (const o of (opsByPipe.get(pdh) || [])) { if (!stat.has(o.leaf)) stat.set(o.leaf, { operator: o.operator, role: o.role, pipes: new Set(), runs: 0, scoreSum: 0, scoreN: 0 }); const s = stat.get(o.leaf); s.pipes.add(pdh); s.runs += a.execs.size; s.scoreSum += a.score; s.scoreN++; }
        const nodes = [...stat].map(([leaf, s]) => ({ id: leaf, operator: s.operator, cluster: classifyRole(s.operator, s.role), size: s.pipes.size, score: s.scoreN ? s.scoreSum / s.scoreN : null, n_runs: s.runs }));
        const keep = new Set(nodes.map((n) => n.id)); const pair = new Map();
        for (const [pdh] of pipeAgg) { const present = [...new Set((opsByPipe.get(pdh) || []).map((o) => o.leaf))].filter((l) => keep.has(l)).sort(); for (let i = 0; i < present.length; i++) for (let j = i + 1; j < present.length; j++) { const k = present[i] + "||" + present[j]; pair.set(k, (pair.get(k) || 0) + 1); } }
        const edges = [...pair].map(([k, w]) => { const [a, b] = k.split("||"); return { source: a, target: b, weight: w }; });
        return { kind, metric, scope, direction: dir(metric), nodes, edges };
      }
      const pdhs = [...pipeAgg.keys()].sort((a, b) => pipeAgg.get(b).execs.size - pipeAgg.get(a).execs.size).slice(0, 150);
      const opset = new Map(); for (const p of pdhs) opset.set(p, new Set((opsByPipe.get(p) || []).map((o) => o.leaf)));
      const nodes = pdhs.map((p) => ({ id: p, label: pipeAgg.get(p).label || p.slice(0, 10), cluster: (pipeAgg.get(p).main_model || "other").split(".").pop(), size: pipeAgg.get(p).execs.size, score: pipeAgg.get(p).score }));
      const edges = [];
      for (let i = 0; i < pdhs.length; i++) { const a = opset.get(pdhs[i]); if (!a.size) continue; for (let j = i + 1; j < pdhs.length; j++) { const b = opset.get(pdhs[j]); if (!b.size) continue; const inter = [...a].filter((x) => b.has(x)).length; const uni = new Set([...a, ...b]).size; const jac = uni ? inter / uni : 0; if (jac >= min_jaccard) edges.push({ source: pdhs[i], target: pdhs[j], weight: Math.round(jac * 1000) / 1000 }); } }
      return { kind: "pipelines", metric, scope, direction: dir(metric), nodes, edges };
    },

    composition: async ({ metric = "rmse", scope = "cv" } = {}) => {
      const rows = sel(metric, scope, {});
      const byPipeScore = new Map(); const byPipeExecs = new Map();
      for (const [, g] of groupBy(rows, (r) => r.pipeline_dag_hash)) { byPipeScore.set(g[0].pipeline_dag_hash, mean(g.map((r) => r.metric_value))); byPipeExecs.set(g[0].pipeline_dag_hash, new Set(g.map((r) => r.execution_hash)).size); }
      const agg = new Map();
      for (const [pdh, ops] of opsByPipe) { if (!byPipeScore.has(pdh)) continue; for (const o of ops) { if (!agg.has(o.operator)) agg.set(o.operator, { role: o.role, pipes: new Set(), runs: 0, scoreSum: 0, scoreN: 0 }); const a = agg.get(o.operator); a.pipes.add(pdh); a.runs += byPipeExecs.get(pdh); a.scoreSum += byPipeScore.get(pdh); a.scoreN++; } }
      const out = [...agg].map(([operator, a]) => ({ operator, role: classifyRole(operator, a.role), operator_short: operator.split(".").pop(), n_pipes: a.pipes.size, n_runs: a.runs, score: a.scoreN ? a.scoreSum / a.scoreN : null }));
      out.sort((a, b) => a.role.localeCompare(b.role) || b.n_runs - a.n_runs);
      return { metric, scope, direction: dir(metric), rows: out };
    },

    runDetail: async (hash) => (await loadRun(hash)).detail,
    residuals: async (hash, partition) => { const rs = (await loadRun(hash)).residuals || []; return partition ? rs.filter((r) => r.partition === partition) : rs; },
    compare: async (a, b, partition = "validation") => {
      const ra = (await loadRun(a)).residuals.filter((r) => r.partition === partition);
      const rb = (await loadRun(b)).residuals.filter((r) => r.partition === partition);
      const mapB = new Map(rb.map((r) => [r.sample_id, r]));
      const paired = []; const xa = [], xb = [];
      for (const r of ra) { const o = mapB.get(r.sample_id); if (!o || r.residual == null || o.residual == null) continue; paired.push({ sample_id: r.sample_id, residual_a: r.residual, residual_b: o.residual, y_true: r.y_true }); xa.push(r.residual); xb.push(o.residual); }
      const rmse = (a2) => a2.length ? Math.sqrt(mean(a2.map((x) => x * x))) : null;
      return { n_common: paired.length, n_paired: paired.length, residual_correlation: xa.length > 1 ? pearson(xa, xb) : null, rmse_a: rmse(xa), rmse_b: rmse(xb), paired };
    },

    ingest: async () => { throw new Error("ingestion is disabled in the static demo — run the service locally to ingest"); },
    upload: async () => { throw new Error("upload is disabled in the static demo — run the service locally to upload"); },
  };
}
