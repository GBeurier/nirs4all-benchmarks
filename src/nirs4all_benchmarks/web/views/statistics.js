// Statistics — score distributions, per-dataset spread, and what correlates with
// the score. Metric/score level come from the shared lens; this view is a one-shot
// (page-head + refresh) with no local controls bar.

function pretty(facetKey) {
  if (facetKey == null) return "";
  const k = String(facetKey);
  if (k.startsWith("param:")) return "param: " + k.slice(6).replace(/_/g, " ");
  return k.replace(/_/g, " ");
}

export default {
  id: "statistics",
  title: "Statistics",
  subtitle: "Score distributions, spread by dataset, and what correlates with the score.",
  icon: "📊",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("div", { class: "eyebrow" }, "Statistics"),
      dom.el("h1", {}, this.title),
      dom.el("p", {}, this.subtitle));

    const statsRow = dom.el("div", { class: "grid cols-4", id: "st-summary" });
    const distCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Score distribution"),
      dom.el("div", { class: "sub", id: "st-dist-sub" }, ""), dom.el("div", { class: "plot", id: "st-dist" }));
    const spreadCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Spread by dataset"),
      dom.el("div", { class: "sub" }, "Violin per dataset — width shows where runs concentrate."),
      dom.el("div", { class: "plot", id: "st-spread" }));
    const corrCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "What correlates with the score"),
      dom.el("div", { class: "sub" }, "Pearson r between each numeric facet and the metric (across runs)."),
      dom.el("div", { class: "plot", id: "st-corr" }));
    const tableHost = dom.el("div", {});

    dom.mount(root, head, statsRow,
      dom.el("div", { class: "grid cols-2", style: { marginTop: "16px" } }, distCard, spreadCard),
      dom.el("div", { style: { marginTop: "16px" } }, corrCard),
      dom.el("div", { style: { marginTop: "16px" } }, tableHost));

    const refresh = async () => {
      const st = await api.stats({ metric: state.metric, scope: state.scope });
      const s = st.summary || {};
      const dirArrow = st.direction === "max" ? "↑ higher is better" : "↓ lower is better";

      // summary stat cards
      const stat = (k, v) => dom.el("div", { class: "stat" },
        dom.el("div", { class: "k" }, k), dom.el("div", { class: "v" }, v));
      dom.mount(statsRow,
        stat("Runs", dom.fmtInt(s.n || 0)),
        stat("Mean", dom.fmt(s.mean, 4)),
        stat("Median", dom.fmt(s.median, 4)),
        stat("Std dev", dom.fmt(s.std, 4)),
        stat("Best", dom.fmt(st.direction === "max" ? s.max : s.min, 4)),
        stat("Worst", dom.fmt(st.direction === "max" ? s.min : s.max, 4)),
        stat("P25", dom.fmt(s.p25, 4)),
        stat("P75", dom.fmt(s.p75, 4)));

      document.getElementById("st-dist-sub").textContent =
        `${s.n || 0} runs · ${state.metric} (${dirArrow})`;

      if (!st.values || !st.values.length) {
        plot.purge(document.getElementById("st-dist"));
        dom.mount(tableHost, dom.empty("No runs at this score level."));
        return;
      }

      // distribution histogram
      plot.draw(document.getElementById("st-dist"), [{
        type: "histogram", x: st.values, marker: { color: "#0d9488" }, nbinsx: 28,
        hovertemplate: `${state.metric} %{x}<br>%{y} runs<extra></extra>`,
      }], { margin: { l: 48, r: 12, t: 8, b: 44 }, xaxis: { title: state.metric }, yaxis: { title: "runs" }, bargap: 0.04 });

      // violin per dataset
      const spreadTraces = (st.by_dataset || []).map((d, i) => ({
        type: "violin", y: d.values, name: d.label, box: { visible: true }, meanline: { visible: true },
        points: false, line: { color: plot.palette[i % plot.palette.length] },
        fillcolor: plot.palette[i % plot.palette.length] + "33",
      }));
      if (spreadTraces.length) {
        plot.draw(document.getElementById("st-spread"), spreadTraces,
          { margin: { l: 52, r: 12, t: 8, b: 40 }, yaxis: { title: state.metric }, showlegend: false });
      } else {
        plot.purge(document.getElementById("st-spread"));
      }

      // correlation bar (sorted by |r|)
      const corr = (st.correlations || []).slice(0, 16).reverse();
      if (corr.length) {
        plot.draw(document.getElementById("st-corr"), [{
          type: "bar", orientation: "h",
          x: corr.map((c) => c.r), y: corr.map((c) => pretty(c.facet)),
          marker: { color: corr.map((c) => (c.r < 0 ? "#0d9488" : "#E9362D")) },
          hovertemplate: "%{y}<br>r = %{x:.3f}<extra></extra>",
        }], {
          margin: { l: 180, r: 16, t: 8, b: 40 },
          xaxis: { title: "Pearson r (− tracks lower score · + tracks higher score)", range: [-1, 1], zeroline: true },
        });
      } else {
        plot.purge(document.getElementById("st-corr"));
        document.getElementById("st-corr").replaceWith(dom.empty("Not enough numeric facets to correlate."));
      }

      // per-dataset summary table
      const tbl = dom.table([
        { key: "label", label: "Dataset" },
        { key: "n", label: "runs", num: true },
        { key: "mean", label: "mean", num: true, render: (r) => dom.fmt(r.mean, 4) },
        { key: "median", label: "median", num: true, render: (r) => dom.fmt(r.median, 4) },
        { key: "std", label: "std", num: true, render: (r) => dom.fmt(r.std, 4) },
        { key: "min", label: "min", num: true, render: (r) => dom.fmt(r.min, 4) },
        { key: "max", label: "max", num: true, render: (r) => dom.fmt(r.max, 4) },
      ], st.by_dataset || []);
      dom.mount(tableHost, dom.el("div", { class: "card" }, dom.el("h3", {}, "Per-dataset summary"), tbl));
    };
    await refresh();
  },
};
