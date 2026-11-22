// Leaderboard — one table per task family, one row per dataset target: the best default score,
// the model that reached it and an inline strip of every score on that target (DESIGN.md §9.2).
// The engine (static-engine.js / Queries.dataset_leaderboard) picks the metric, score context and
// best row; this view only presents them.

import { uniqueLabels } from "../lib/labels.js";

const FAMILIES = [["regression", "Regression"], ["classification", "Classification"]];
const METRIC_LABEL = { nrmse: "nRMSE", rmse: "RMSE", balanced_accuracy: "Balanced accuracy", accuracy: "Accuracy" };
const metricLabel = (m) => METRIC_LABEL[m] || m;
const modelLabel = (m) => (m ? m.split(".").pop() : "Unknown model");
const better = (direction) => (direction === "max" ? "higher is better" : "lower is better");
// Validation-level scores come before the retained test partition.
const SCOPE_ORDER = ["cv", "fold", "test"];
const SCOPE_LABEL = { cv: "cross-validation", fold: "per-fold validation", test: "test" };
const SPLIT_LABEL = { seeded_80_20_group_aware: "80/20 group-aware" };

function quantile(sorted, p) {
  const i = (sorted.length - 1) * p, lo = Math.floor(i), hi = Math.ceil(i);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (i - lo);
}

// Lightweight inline SVG (one per row instead of a chart): every score as a tick (a 32-bin
// histogram past 150 scores), the interquartile band, the median and the best score as a dot.
const W = 168, H = 26, PAD = 5, BINS = 32;
export function distributionStrip(dom, row) {
  const v = row.values, lo = v[0], hi = v[v.length - 1];
  const x = (s) => (hi > lo ? PAD + ((s - lo) / (hi - lo)) * (W - 2 * PAD) : W / 2);
  const q1 = quantile(v, 0.25), q3 = quantile(v, 0.75), med = quantile(v, 0.5);
  let marks;
  if (v.length <= 150) {
    marks = v.map((s) => dom.svg("line", { class: "spark-tick", x1: x(s), x2: x(s), y1: 8, y2: 18 }));
  } else {
    const counts = new Array(BINS).fill(0);
    for (const s of v) counts[hi > lo ? Math.min(BINS - 1, Math.floor(((s - lo) / (hi - lo)) * BINS)) : 0]++;
    const top = Math.max(...counts), step = (W - 2 * PAD) / BINS;
    marks = counts.map((c, i) => (c ? dom.svg("rect", { class: "spark-bin", x: PAD + i * step, width: Math.max(step - 0.5, 0.5),
      y: 20 - (12 * c) / top, height: (12 * c) / top }) : null));
  }
  const f = (s) => dom.fmt(s, 4);
  const label = `${v.length} scores, ${better(row.direction)} · best ${f(row.best_value)} · min ${f(lo)} · `
    + `median ${f(med)} · max ${f(hi)}`;
  return dom.svg("svg", { class: "spark", width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": label },
    dom.svg("title", {}, label),
    dom.svg("line", { class: "spark-axis", x1: PAD, x2: W - PAD, y1: 20, y2: 20 }),
    dom.svg("rect", { class: "spark-iqr", x: x(q1), y: 7, width: Math.max(x(q3) - x(q1), 1), height: 12, rx: 2 }),
    ...marks,
    dom.svg("line", { class: "spark-median", x1: x(med), x2: x(med), y1: 6, y2: 20 }),
    dom.svg("circle", { class: "spark-best", cx: x(row.best_value), cy: 13, r: 3.5 }));
}

function caption(data) {
  const parts = [];
  const rows = Object.values(data.families).flat();
  if (data.mode === "historical_reported") {
    const holdouts = new Set(rows.map((r) => `${r.split.method}|${r.split.seed}|${r.split.n_splits}`));
    const [method, seed, n] = holdouts.size === 1 ? [...holdouts][0].split("|") : [];
    parts.push(holdouts.size === 1
      ? `Each row uses its target's canonical holdout: seed ${seed} of ${n} (${SPLIT_LABEL[method] || method}).`
      : "Each row uses its target's canonical holdout: the first split method and lowest seed.",
    "Other holdout seeds are not pooled. The best score is the best producer-reported test score, not a validation-based selection; the strip shows every reported test score of that holdout.");
  } else {
    parts.push(`The best score is the best ${SCOPE_LABEL[data.scope] || data.scope} score among run conditions, each the mean of its ${data.partition} observations; the strip shows every run condition's score.`);
  }
  for (const [family, title] of FAMILIES) {
    const [primary, ...fallbacks] = data.metrics[family];
    const fallback = fallbacks.length ? `, else ${fallbacks.map(metricLabel).join(", else ")}` : "";
    parts.push(`${title}: ${metricLabel(primary)} (${better(family === "classification" ? "max" : "min")})${fallback}.`);
  }
  if (data.excluded?.unclassified_targets) {
    parts.push(`${data.excluded.unclassified_targets} target(s) with another or unreported task type are not shown.`);
  }
  return parts.join(" ");
}

function familyCard(dom, data, family, title) {
  const rows = data.families[family] || [];
  const metrics = data.metrics[family];
  if (!rows.length) return dom.el("div", { class: "card" }, dom.el("h2", {}, title), dom.empty(`No ${title.toLowerCase()} targets.`));
  // Distinct dataset fingerprints, tasks or units never share a label.
  const labels = uniqueLabels(rows, (r) => r.dataset_name,
    (r) => [dom.shortHash(r.dataset_fingerprint, 8), r.task_hash && dom.shortHash(r.task_hash, 6), r.metric_unit].filter(Boolean).join("/"));
  const labelled = rows.map((r, i) => ({ ...r, label: labels[i] }));
  const table = dom.table([
    { key: "label", label: "Dataset", render: (r) => dom.el("span", { title: r.dataset_fingerprint }, r.label) },
    { key: "best_value", label: `Best ${metricLabel(metrics[0])}`, num: true,
      sortKey: (r) => (r.best_value == null ? null : r.direction === "max" ? -r.best_value : r.best_value),
      render: (r) => (r.best_value == null ? "—" : dom.el("span", {}, dom.fmt(r.best_value, 4),
        r.metric !== metrics[0] ? dom.el("span", { class: "badge gray lb-metric",
          title: `${metricLabel(metrics[0])} is not reported for this target` }, metricLabel(r.metric)) : null)) },
    { key: "model", label: "Best model", sortKey: (r) => (r.best ? modelLabel(r.best.main_model) : null), render: (r) => {
      if (!r.best) return "—";
      const pipeline = r.best.pipeline_label || dom.shortHash(r.best.pipeline_dag_hash, 12);
      const href = r.best.execution_hash ? `#/run/${r.best.execution_hash}` : `#/runs/${r.best.pipeline_dag_hash}`;
      return dom.el("div", { class: "lb-model" },
        dom.badge(modelLabel(r.best.main_model), "role"),
        r.n_best > 1 ? dom.el("span", { class: "lb-tie", title: `Tied at the best score: ${r.tied_models.map(modelLabel).join(", ")}` },
          `+${r.n_best - 1} tied`) : null,
        dom.el("a", { class: "lb-pipe", href, title: pipeline }, pipeline));
    } },
    { key: "n_scores", label: "Score distribution", render: (r) => (r.values.length
      ? dom.el("div", { class: "lb-dist" }, distributionStrip(dom, r), dom.el("span", { class: "lb-n" }, `n=${dom.fmtInt(r.n_scores)}`))
      : dom.el("span", { class: "sub" }, `No ${metrics.map(metricLabel).join(" or ")} score (reports ${r.available_metrics.join(", ") || "none"})`)) },
  ], labelled);
  return dom.el("div", { class: "card" }, dom.el("h2", {}, title),
    dom.el("p", { class: "sub" }, `${rows.length} dataset target${rows.length === 1 ? "" : "s"}.`), table);
}

export default {
  id: "leaderboard",
  title: "Leaderboard",
  subtitle: "Best score, best model and score distribution for every dataset target.",
  icon: "🏆",
  async render({ root, api, dom, state, signal }) {
    const historical = Boolean(state._overview?.historical_demo);
    const scopes = state._overview?.score_scopes ?? [];
    const scope = SCOPE_ORDER.find((s) => scopes.includes(s)) ?? "cv";
    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, historical ? "Historical leaderboard" : this.title),
      dom.el("p", {}, historical ? "Producer-reported test scores. Scientific validity is unreported." : this.subtitle));
    const bar = historical ? null : dom.controls([
      { id: "quar", type: "toggle", label: "Include quarantined", value: false },
    ], () => refresh());
    const host = dom.el("div", {});
    dom.mount(root, head, bar?.element, host);

    const refresh = async () => {
      dom.mount(host, dom.loading());
      const data = await api.datasetLeaderboard(historical ? {} : { scope, include_quarantined: bar.get("quar") });
      if (signal?.aborted) return;
      dom.mount(host,
        data.scope === "test" && !historical
          ? dom.banner("No validation-level scores are stored, so rows rank on the retained test partition.", "warn") : null,
        dom.el("p", { class: "sub" }, caption(data)),
        ...FAMILIES.map(([family, title]) => familyCard(dom, data, family, title)));
    };
    await refresh();
  },
};
