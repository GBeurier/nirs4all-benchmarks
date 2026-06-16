// Robustness — score vs. variance across folds, seeds and splits.
// Bottom-left of the scatter (low score + low variance) is the robust corner.

export default {
  id: "robustness",
  title: "Robustness",
  subtitle: "Score vs. variance across folds, seeds and splits — bottom-left is best.",
  icon: "📉",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Stability landscape"),
      dom.el("div", { class: "sub" }, "Each point is one pipeline on one dataset; marker size scales with the number of observations."),
      dom.el("div", { class: "plot tall", id: "rb-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, chartCard, tableHost);

    const refresh = async () => {
      const plotNode = document.getElementById("rb-plot");
      dom.mount(tableHost, dom.loading());
      const rows = await api.robustness({ metric: state.metric, scope: state.scope });
      if (!rows.length) { plot.purge(plotNode); dom.mount(tableHost, dom.empty("No runs match these filters.")); return; }

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
          x: Math.min(...rows.map((r) => r.mean)), y: Math.min(...rows.map((r) => r.stdev)),
          xref: "x", yref: "y", xanchor: "left", yanchor: "bottom",
          text: "low score + low variance = robust",
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
        dom.el("h3", {}, `${rows.length} pipeline × dataset pairs`),
        dom.el("div", { class: "sub" }, "Sorted by variance — most stable first. Click a row to see every run of that pipeline."), tbl));
    };
    await refresh();
  },
};
