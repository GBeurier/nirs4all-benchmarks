// Robustness — score vs. variance across folds, seeds and splits.
// The robust corner is low variance with the better score: bottom-left for a metric where lower is
// better (rmse), bottom-right where higher is better (accuracy).

export default {
  id: "robustness",
  title: "Robustness",
  subtitle: "Score vs. variance across folds, seeds and splits.",
  icon: "📉",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));
    const subtitle = head.querySelector("p");

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Stability landscape"),
      dom.el("div", { class: "sub" }, "Each point is one pipeline on one dataset; marker size scales with the number of observations."),
      dom.el("div", { class: "plot tall", id: "rb-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, chartCard, tableHost);

    const refresh = async () => {
      const plotNode = document.getElementById("rb-plot");
      dom.mount(tableHost, dom.loading());
      const lens = { metric: state.metric, scope: state.scope, dataset: state.dataset || undefined };
      const [all, lb] = await Promise.all([api.robustness(lens), api.leaderboard({ ...lens, limit: 1 })]);
      const higherIsBetter = lb.direction === "max";
      subtitle.textContent = `${this.subtitle} ${higherIsBetter ? "Bottom-right (high score, low variance)" : "Bottom-left (low score, low variance)"} is best.`;
      // A variance needs at least two runs: a single run would sit at zero and look perfectly stable.
      const rows = all.filter((r) => r.n > 1);
      if (!rows.length) {
        plot.purge(plotNode);
        dom.mount(tableHost, dom.empty(all.length
          ? "Every pipeline has a single run per dataset, so run-to-run variance cannot be measured."
          : "No runs match these filters."));
        return;
      }

      // One trace per dataset, colored from the brand palette. Marker size ~ n.
      const fps = [...new Set(rows.map((r) => r.dataset_fingerprint))];
      const maxN = Math.max(...rows.map((r) => r.n || 1));
      const traces = fps.map((fp, i) => {
        const pts = rows.filter((r) => r.dataset_fingerprint === fp);
        return {
          type: "scatter", mode: "markers",
          name: dom.shortHash(fp, 10),
          x: pts.map((r) => r.mean),
          y: pts.map((r) => r.stdev),
          text: pts.map((r) => r.label),
          marker: plot.marker({
            color: plot.palette[i % plot.palette.length],
            size: pts.map((r) => 5 + 4 * Math.sqrt((r.n || 1) / maxN)),
            opacity: 0.7,
          }),
          hovertemplate: "%{text}<br>" + state.metric + " mean = %{x:.4f}<br>" + state.metric + " stdev = %{y:.4f}<extra></extra>",
        };
      });

      plot.draw(plotNode, traces, {
        margin: { l: 64, r: 18, t: 12, b: 52 },
        xaxis: { title: `${state.metric} (mean)` },
        yaxis: { title: `${state.metric} (stdev)` },
        annotations: [{
          x: (higherIsBetter ? Math.max : Math.min)(...rows.map((r) => r.mean)), y: Math.min(...rows.map((r) => r.stdev)),
          xref: "x", yref: "y", xanchor: higherIsBetter ? "right" : "left", yanchor: "bottom",
          text: higherIsBetter ? "high score + low variance = robust" : "low score + low variance = robust",
          showarrow: false, font: { size: 11, color: "#00704A" },
        }],
      });

      const tbl = dom.table([
        { key: "label", label: "Pipeline" },
        { key: "dataset_fingerprint", label: "Dataset", render: (r) => dom.el("span", { class: "hash" }, dom.shortHash(r.dataset_fingerprint, 10)) },
        { key: "n", label: "obs", num: true },
        { key: "mean", label: `${state.metric} (mean)`, num: true, render: (r) => dom.fmt(r.mean, 4) },
        { key: "stdev", label: `${state.metric} (stdev)`, num: true, render: (r) => dom.fmt(r.stdev, 4) },
        { key: "cv_pct", label: "CV %", num: true, render: (r) => dom.fmt(r.cv_pct, 1) + "%" },
      ], rows, { sort: { key: "stdev", dir: 1 }, onRow: (r) => ctx.navigate(`runs/${r.pipeline_dag_hash}`) });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} pipeline × dataset pairs`
          + (all.length > rows.length ? ` (${all.length - rows.length} with a single run not shown)` : "")),
        dom.el("div", { class: "sub" }, "Sorted by variance — most stable first. Click a row to see every run of that pipeline."), tbl));
    };
    await refresh();
  },
};
