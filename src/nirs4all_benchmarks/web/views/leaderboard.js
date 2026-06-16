// Leaderboard — configurable ranking, no canonical baseline (DESIGN.md §9.2).
// This is the reference pattern for every view: page-head + controls + reactive render.

const datasetOptions = (state) => [
  { value: "", label: "All datasets" },
  ...((state._overview && []) || []),
];

export default {
  id: "leaderboard",
  title: "Leaderboard",
  subtitle: "Rank pipelines by any metric, at any score level, on any dataset.",
  icon: "🏆",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = state._overview || (await api.overview());
    const datasets = await api.datasets();
    const metrics = ov.metrics?.length ? ov.metrics : ["rmse"];

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "metric", type: "select", label: "Metric", options: metrics, value: state.metric },
      { id: "scope", type: "select", label: "Score level", options: ["cv", "test", "refit", "fold"], value: state.scope },
      { id: "dataset", type: "select", label: "Dataset", value: state.dataset, options: [
        { value: "", label: "All datasets" },
        ...datasets.map((d) => ({ value: d.dataset_fingerprint, label: d.name || d.dataset_fingerprint.slice(0, 10) })),
      ] },
      { id: "quar", type: "toggle", label: "Include quarantined", value: false },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Top pipelines"),
      dom.el("div", { class: "plot tall", id: "lb-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, chartCard, tableHost);

    const refresh = async () => {
      state.metric = bar.get("metric"); state.scope = bar.get("scope"); state.dataset = bar.get("dataset"); state.save();
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
      const best = lb.direction === "max" ? Math.max(...rows.map((r) => r.mean)) : Math.min(...rows.map((r) => r.mean));
      plot.draw(plotNode, [{
        type: "bar", orientation: "h",
        x: top.map((r) => r.mean),
        y: top.map((r) => r.pipeline_label || r.pipeline_dag_hash.slice(0, 10)),
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
        { key: "min", label: "best fold", num: true, render: (r) => dom.fmt(r.min, 4) },
        { key: "max", label: "worst fold", num: true, render: (r) => dom.fmt(r.max, 4) },
        { key: "n_obs", label: "obs", num: true },
      ], rows, { sort: { key: "rank", dir: 1 }, onRow: (r) => ctx.navigate(`runs/${r.pipeline_dag_hash}`) });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} pipelines ranked`),
        dom.el("div", { class: "sub" }, "Click a row to see every run of that pipeline."), tbl));
    };
    await refresh();
  },
};
