// Leaderboard — configurable ranking, no canonical baseline (DESIGN.md §9.2).
// This is the reference pattern for every view: page-head + controls + reactive render.

import { uniqueLabels } from "../lib/labels.js";

const CLASSIFICATION = new Set(["binary", "multiclass", "multilabel"]);
const taskFamily = ({ task_type }) => CLASSIFICATION.has(task_type) ? "classification" : task_type;
const PREFERRED_METRICS = { regression: ["nrmse", "rmse"], classification: ["accuracy", "balanced_accuracy"] };

async function renderHistoricalLeaderboard({ root, api, dom, plot, state, signal }) {
  const targets = (await api.historicalLeaderboardTargets()).sort((a, b) => a.name.localeCompare(b.name));
  if (signal.aborted) return;
  let target = targets.find(d => d.dataset_fingerprint === state.dataset);
  let family = target ? taskFamily(target) : "regression";

  const filters = dom.el("div", { class: "controls" });
  const result = dom.el("div", {});
  function select(id, label, onChange) {
    const input = dom.el("select", { id, onchange: onChange });
    const field = dom.el("div", { class: "control" }, dom.el("label", { for: id }, label), input);
    filters.appendChild(field);
    return { input, field };
  }
  function setOptions(input, options, value) {
    input.replaceChildren(...options.map(([id, label]) => dom.el("option", { value: id }, label)));
    if (options.some(([id]) => id === value)) input.value = value;
  }

  const familyControl = select("lb-task", "Task", () => {
    family = familyControl.input.value;
    target = undefined;
    refreshTargets();
  });
  const targetControl = select("lb-target", "Dataset and target", () => {
    openTarget(targetControl.input.value);
  });
  const splitControl = select("lb-split", "Holdout", refreshMetrics);
  const metricControl = select("lb-metric", "Metric", renderResult);
  setOptions(familyControl.input, [["regression", "Regression"], ["classification", "Classification"]], family);

  dom.mount(root, dom.el("div", { class: "page-head" },
    dom.el("h1", {}, "Historical leaderboard"),
    dom.el("p", {}, "Producer-reported test scores. Scientific validity is unreported.")),
  filters, result);

  let chartNode = null;
  function openTarget(id) {
    target = targets.find(d => d.dataset_fingerprint === id && taskFamily(d) === family);
    targetControl.input.value = target?.dataset_fingerprint ?? "";
    refreshSplits();
  }

  function refreshTargets() {
    const options = targets.filter(d => taskFamily(d) === family);
    setOptions(targetControl.input, [["", "All targets"], ...options.map(d => [d.dataset_fingerprint, d.name])],
      target?.dataset_fingerprint);
    refreshSplits();
  }

  function refreshSplits() {
    const splits = target?.splits ?? [];
    setOptions(splitControl.input, splits.map(s => [s.id, `Seed ${s.seed} · ${
      s.method === "seeded_80_20_group_aware" ? "80/20 group-aware" : s.method
    }`]), splitControl.input.value);
    splitControl.field.style.display = target ? "" : "none";
    metricControl.field.style.display = target ? "" : "none";
    refreshMetrics();
  }

  function refreshMetrics() {
    const split = target?.splits.find(s => s.id === splitControl.input.value);
    const available = [...(split?.scores.keys() ?? [])].sort();
    const preferred = PREFERRED_METRICS[family].find(metric => available.includes(metric));
    const current = metricControl.input.value || state.metric;
    setOptions(metricControl.input, available.map(metric => [metric, metric]),
      available.includes(current) ? current : preferred);
    renderResult();
  }

  function renderResult() {
    if (chartNode) { plot.purge(chartNode); chartNode = null; }
    if (!target) {
      const summaries = targets.filter(d => taskFamily(d) === family).map(d => ({
        ...d, metrics: [...new Set(d.splits.flatMap(s => [...s.scores.keys()]))].sort().join(", "),
      }));
      const table = dom.table([
        { key: "name", label: "Dataset and target", render: d => dom.el("button", {
          type: "button", class: "btn", onclick: () => { openTarget(d.dataset_fingerprint); targetControl.input.focus(); },
        }, d.name) },
        { key: "n_run_conditions", label: "Reported runs", num: true, render: d => dom.fmtInt(d.n_run_conditions) },
        { key: "metrics", label: "Available metrics" },
      ], summaries);
      dom.mount(result, summaries.length ? dom.el("div", { class: "card" },
        dom.el("h2", {}, "Scores by dataset and target"), table) : dom.empty("No targets match this task."));
      return;
    }

    const metric = metricControl.input.value;
    const split = target.splits.find(s => s.id === splitControl.input.value);
    const { direction, rows } = split.scores.get(metric);
    const multiplier = direction === "max" ? -1 : 1;
    const sorted = [...rows].sort((a, b) => multiplier * (a.metric_value - b.metric_value));
    let rank = 1;
    const ranked = sorted.map((row, i) => {
      if (i > 0 && row.metric_value !== sorted[i - 1].metric_value) rank = i + 1;
      return { ...row, rank };
    });
    const bestCount = ranked.filter(row => row.rank === 1).length;
    const best = dom.fmt(ranked[0].metric_value, 4);
    const summary = `${ranked.length} reported scores for test seed ${split.seed}. ${bestCount} at the best score (${best}).`;
    const table = dom.table([
      { key: "rank", label: "Rank", num: true, render: r => dom.fmtInt(r.rank) },
      { key: "pipeline_label", label: "Pipeline", render: r => dom.el("a", { href: `#/run/${r.execution_hash}` },
        r.pipeline_label || dom.shortHash(r.pipeline_dag_hash, 12)) },
      { key: "main_model", label: "Model", render: r => dom.badge((r.main_model || "—").split(".").pop(), "role") },
      { key: "metric_value", label: metric, num: true, render: r => dom.fmt(r.metric_value, 4) },
    ], ranked, { sort: { key: "rank", dir: 1 } });
    chartNode = dom.el("div", { class: "plot", inert: true, "aria-hidden": "true" });
    dom.mount(result, dom.el("div", { class: "card" }, dom.el("h2", {}, target.name),
      dom.el("p", { class: "sub" }, summary), chartNode, table));
    plot.draw(chartNode, [{ type: "histogram", x: ranked.map(r => r.metric_value), marker: { color: "#0d9488" } }],
      { xaxis: { title: metric }, yaxis: { title: "Reported scores", rangemode: "tozero" } });
  }

  refreshTargets();
  return () => { if (chartNode) plot.purge(chartNode); };
}

export default {
  id: "leaderboard",
  title: "Leaderboard",
  subtitle: "Which pipelines win — ranked by the lens metric.",
  icon: "🏆",
  async render(ctx) {
    if (ctx.state._overview?.historical_demo) return renderHistoricalLeaderboard(ctx);
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "quar", type: "toggle", label: "Include quarantined", value: false },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Top pipelines"),
      dom.el("div", { class: "plot tall", id: "lb-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, chartCard, tableHost);

    const refresh = async () => {
      const plotNode = document.getElementById("lb-plot");
      dom.mount(tableHost, dom.loading());
      const lb = await api.leaderboard({
        metric: state.metric, scope: state.scope, dataset: state.dataset || undefined,
        include_quarantined: bar.get("quar"), limit: 200,
      });
      const rows = lb.rows || [];
      if (!rows.length) { plot.purge(plotNode); dom.mount(tableHost, dom.empty("No runs match these filters.")); return; }

      // Horizontal bar of the top 18, best at top, min–max as error bars.
      const top = rows.slice(0, 18).reverse();
      const barLabels = uniqueLabels(top, (r) => r.pipeline_label || r.pipeline_dag_hash.slice(0, 10),
        (r) => (r.run_condition_hash || r.pipeline_dag_hash).slice(0, 8));
      const best = lb.direction === "max" ? Math.max(...rows.map((r) => r.mean)) : Math.min(...rows.map((r) => r.mean));
      plot.draw(plotNode, [{
        type: "bar", orientation: "h",
        x: top.map((r) => r.mean),
        y: barLabels,
        error_x: { type: "data", symmetric: false,
          array: top.map((r) => (r.max ?? r.mean) - r.mean),
          arrayminus: top.map((r) => r.mean - (r.min ?? r.mean)), color: "rgba(15,31,26,.25)", thickness: 1 },
        marker: { color: top.map((r) => (Math.abs(r.mean - best) < 1e-9 ? "#00704A" : "#0d9488")) },
        hovertemplate: "%{y}<br>" + state.metric + " = %{x:.4f}<extra></extra>",
      }], { margin: { l: 220, r: 24, t: 10, b: 40 }, xaxis: { title: `${state.metric} (${lb.direction === "max" ? "↑ better" : "↓ better"})` } });

      const tbl = dom.table([
        { key: "rank", label: "#", num: true, render: (r) => dom.el("span", { class: `rank ${r.rank === 1 ? "top" : ""}` }, r.rank) },
        { key: "pipeline_label", label: "Pipeline", render: (r) => dom.el("span", {}, r.pipeline_label || r.pipeline_dag_hash.slice(0, 12)) },
        { key: "main_model", label: "Model", render: (r) => dom.badge((r.main_model || "—").split(".").pop(), "role") },
        { key: "mean", label: `${state.metric} (mean)`, num: true, render: (r) => dom.fmt(r.mean, 4) },
        { key: "min", label: lb.direction === "max" ? "worst obs" : "best obs", num: true, render: (r) => dom.fmt(r.min, 4) },
        { key: "max", label: lb.direction === "max" ? "best obs" : "worst obs", num: true, render: (r) => dom.fmt(r.max, 4) },
        { key: "n_obs", label: "obs", num: true },
      ], rows, { sort: { key: "rank", dir: 1 }, onRow: (r) => ctx.navigate(`runs/${r.pipeline_dag_hash}`) });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} pipelines ranked`),
        dom.el("div", { class: "sub" }, "Click a row to see every run of that pipeline."), tbl));
    };
    await refresh();
  },
};
