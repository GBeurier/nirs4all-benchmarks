// Operator-effect explorer — score distribution by which operator appears in the pipeline.

export default {
  id: "operator-effect",
  title: "Operator effect",
  subtitle: "Score distribution by which operator appears in the pipeline.",
  icon: "🧩",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = state._overview || (await api.overview());
    const metrics = ov.metrics?.length ? ov.metrics : ["rmse"];

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "metric", type: "select", label: "Metric", options: metrics, value: state.metric },
      { id: "scope", type: "select", label: "Score level", options: ["cv", "test", "refit", "fold"], value: state.scope },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Score by operator"),
      dom.el("div", { class: "sub" }, "One box per operator, over every run in which it appears."),
      dom.el("div", { class: "plot tall", id: "oe-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, chartCard, tableHost);

    const refresh = async () => {
      state.metric = bar.get("metric"); state.scope = bar.get("scope"); state.save();
      const plotNode = document.getElementById("oe-plot");
      dom.mount(tableHost, dom.loading());
      const oe = await api.operatorEffect({ metric: state.metric, scope: state.scope });
      const series = oe.series || [];
      if (!series.length) { plot.purge(plotNode); dom.mount(tableHost, dom.empty("No operators match these filters.")); return; }

      // Sort by mean, best first per direction (max → high first, else low first).
      const better = oe.direction === "max" ? 1 : -1;
      const sorted = series.slice().sort((a, b) => (a.mean - b.mean) * better);

      // Map distinct roles → palette colors.
      const roles = [...new Set(sorted.map((s) => s.role))];
      const roleColor = new Map(roles.map((r, i) => [r, plot.palette[i % plot.palette.length]]));

      // Box plot, best at top: Plotly stacks the first category lowest, so reverse.
      const ordered = sorted.slice().reverse();
      const traces = ordered.map((s) => ({
        type: "box",
        orientation: "h",
        name: s.operator.split(".").pop(),
        x: s.values,
        boxpoints: "outliers",
        marker: { color: roleColor.get(s.role) },
        line: { color: roleColor.get(s.role) },
        showlegend: false,
        hovertemplate: `${s.operator.split(".").pop()} · ${s.role}<br>` + state.metric + " = %{x:.4f}<extra></extra>",
      }));
      plot.draw(plotNode, traces, {
        margin: { l: 200, r: 24, t: 10, b: 44 },
        showlegend: false,
        xaxis: { title: `${state.metric} (${oe.direction === "max" ? "↑ better" : "↓ better"})` },
        yaxis: { automargin: true },
      });

      const tbl = dom.table([
        { key: "operator", label: "Operator", render: (r) => dom.el("span", {}, r.operator.split(".").pop()) },
        { key: "role", label: "Role", render: (r) => dom.badge(r.role || "—", "role") },
        { key: "n", label: "runs", num: true },
        { key: "mean", label: `${state.metric} (mean)`, num: true, render: (r) => dom.fmt(r.mean, 4) },
        { key: "median", label: "median", num: true, render: (r) => dom.fmt(r.median, 4) },
        { key: "stdev", label: "stdev", num: true, render: (r) => dom.fmt(r.stdev, 4) },
      ], series, { sort: { key: "mean", dir: better > 0 ? -1 : 1 } });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${series.length} operators`),
        dom.el("div", { class: "sub" }, "Sorted by mean score; best operators appear at the top of the chart."), tbl));
    };
    await refresh();
  },
};
