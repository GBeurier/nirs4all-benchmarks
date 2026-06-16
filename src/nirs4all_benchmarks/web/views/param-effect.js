// Parameter-effect explorer — how a hyperparameter moves the score (e.g. PLS n_components).
// Same reactive pattern as leaderboard.js: page-head + controls + refresh().

export default {
  id: "param-effect",
  title: "Parameter effect",
  subtitle: "How a hyperparameter moves the score (e.g. PLS n_components).",
  icon: "🎚",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = state._overview || (await api.overview());
    state._overview = ov;
    const metrics = ov.metrics?.length ? ov.metrics : ["rmse"];

    const params = await api.parameters();
    const paramNames = params.map((p) => p.name);
    const defaultParam = paramNames.includes("n_components") ? "n_components" : paramNames[0];

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    if (!paramNames.length) {
      dom.mount(root, head, dom.el("div", { class: "card" },
        dom.empty("No sweepable parameters in the store yet.")));
      return;
    }

    const datasets = await api.datasets();
    const dsName = new Map(datasets.map((d) => [d.dataset_fingerprint, d.name || d.dataset_fingerprint.slice(0, 10)]));
    const labelOf = (df) => dsName.get(df) || dom.shortHash(df, 10);

    const bar = dom.controls([
      { id: "param", type: "select", label: "Parameter", options: paramNames, value: defaultParam },
      { id: "metric", type: "select", label: "Metric", options: metrics, value: state.metric },
      { id: "scope", type: "select", label: "Score level", options: ["cv", "test", "refit", "fold"], value: state.scope },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", { id: "pe-title" }, "Parameter effect"),
      dom.el("div", { class: "sub", id: "pe-sub" }, ""),
      dom.el("div", { class: "plot tall", id: "pe-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, chartCard, tableHost);

    const refresh = async () => {
      const param = bar.get("param");
      state.metric = bar.get("metric"); state.scope = bar.get("scope"); state.save();
      const plotNode = document.getElementById("pe-plot");
      const titleNode = document.getElementById("pe-title");
      const subNode = document.getElementById("pe-sub");
      titleNode.textContent = `Effect of ${param} on ${state.metric}`;
      subNode.textContent = "";
      dom.mount(tableHost, dom.loading());

      const pe = await api.parameterEffect({ param, metric: state.metric, scope: state.scope });
      const points = pe.points || [];
      if (!points.length) {
        plot.purge(plotNode);
        dom.mount(tableHost, dom.empty("No runs sweep this parameter at this score level."));
        return;
      }

      const dirArrow = pe.direction === "max" ? " (↑)" : " (↓)";
      const yTitle = `${state.metric}${dirArrow}`;
      const isNumeric = points.some((p) => p.numeric != null);
      plot.purge(plotNode);

      if (isNumeric) {
        // One scatter + one mean line per dataset, colored from the brand palette.
        const byDataset = new Map();
        for (const p of points) {
          if (p.numeric == null) continue;
          if (!byDataset.has(p.dataset_fingerprint)) byDataset.set(p.dataset_fingerprint, []);
          byDataset.get(p.dataset_fingerprint).push(p);
        }
        const traces = [];
        let ci = 0;
        for (const [df, pts] of byDataset) {
          const color = plot.palette[ci % plot.palette.length];
          ci += 1;
          const name = labelOf(df);
          traces.push({
            type: "scatter", mode: "markers", name, legendgroup: df,
            x: pts.map((p) => p.numeric), y: pts.map((p) => p.metric_value),
            marker: { color, size: 6, opacity: 0.55 },
            hovertemplate: `${name}<br>${param} = %{x}<br>${state.metric} = %{y:.4f}<extra></extra>`,
          });
          // Per-x mean, sorted by x, to expose the U-curve / trend.
          const sums = new Map();
          for (const p of pts) {
            const e = sums.get(p.numeric) || { s: 0, n: 0 };
            e.s += p.metric_value; e.n += 1; sums.set(p.numeric, e);
          }
          const xs = [...sums.keys()].sort((a, b) => a - b);
          traces.push({
            type: "scatter", mode: "lines+markers", name: `${name} (mean)`,
            legendgroup: df, showlegend: false,
            x: xs, y: xs.map((x) => sums.get(x).s / sums.get(x).n),
            line: { color, width: 2 }, marker: { color, size: 7 },
            hovertemplate: `${name} mean<br>${param} = %{x}<br>${state.metric} = %{y:.4f}<extra></extra>`,
          });
        }
        plot.draw(plotNode, traces, {
          margin: { l: 56, r: 18, t: 12, b: 48 },
          xaxis: { title: param }, yaxis: { title: yTitle },
        });
      } else {
        // Categorical: one box per distinct value.
        const byValue = new Map();
        for (const p of points) {
          const key = String(p.param);
          if (!byValue.has(key)) byValue.set(key, []);
          byValue.get(key).push(p.metric_value);
        }
        const keys = [...byValue.keys()].sort();
        const traces = keys.map((k, i) => ({
          type: "box", name: k, y: byValue.get(k), boxpoints: "outliers",
          marker: { color: plot.palette[i % plot.palette.length] },
          hovertemplate: `${param} = ${k}<br>${state.metric} = %{y:.4f}<extra></extra>`,
        }));
        plot.draw(plotNode, traces, {
          margin: { l: 56, r: 18, t: 12, b: 60 }, showlegend: false,
          xaxis: { title: param }, yaxis: { title: yTitle },
        });
      }

      // Aggregated table: per distinct value → n + mean metric.
      const agg = new Map();
      for (const p of points) {
        const key = String(p.param);
        const e = agg.get(key) || { value: p.param, numeric: p.numeric, n: 0, s: 0 };
        e.n += 1; e.s += p.metric_value; agg.set(key, e);
      }
      const aggRows = [...agg.values()].map((e) => ({ value: e.value, numeric: e.numeric, n: e.n, mean: e.s / e.n }));
      aggRows.sort((a, b) => {
        if (a.numeric != null && b.numeric != null) return a.numeric - b.numeric;
        return String(a.value) < String(b.value) ? -1 : 1;
      });
      const tbl = dom.table([
        { key: "value", label: param, render: (r) => dom.el("span", {}, String(r.value)) },
        { key: "n", label: "runs", num: true },
        { key: "mean", label: `${state.metric} (mean)`, num: true, render: (r) => dom.fmt(r.mean, 4) },
      ], aggRows, { sort: { key: "value", dir: 1 } });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${aggRows.length} distinct values`),
        dom.el("div", { class: "sub" },
          `${points.length} valid runs across ${new Set(points.map((p) => p.dataset_fingerprint)).size} dataset(s).`),
        tbl));
    };

    await refresh();
  },
};
