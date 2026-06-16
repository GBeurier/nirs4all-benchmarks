// Parallel coordinates — trace runs across split, CV, model and parameters down to the score.
// Same reactive pattern as leaderboard.js: page-head + controls + refresh().

// Curated, ordered dimension list; only those present in /api/facets are offered.
const DIM_CANDIDATES = [
  "model_family", "cv_method", "split_method", "has_augmentation",
  "refit_strategy", "seed", "param:n_components",
];

// Friendly axis label: strip the "param:" prefix used for hyperparameter facets.
function pretty(facetKey) {
  return String(facetKey).startsWith("param:") ? facetKey.slice("param:".length) : facetKey;
}

export default {
  id: "parallel",
  title: "Parallel coordinates",
  subtitle: "Trace runs across split, CV, model and parameters down to the score.",
  icon: "〰",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = state._overview || (await api.overview());
    state._overview = ov;
    const metrics = ov.metrics?.length ? ov.metrics : ["rmse"];

    const facets = await api.facets();
    const facetKeys = new Set(facets.map((f) => f.facet_key));
    const dims = DIM_CANDIDATES.filter((k) => facetKeys.has(k));

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    if (dims.length < 2) {
      dom.mount(root, head, dom.el("div", { class: "card" },
        dom.empty("Not enough facets in the store for a parallel-coordinates view.")));
      return;
    }

    // Default the first four available dimensions to checked.
    const defaults = new Set(dims.slice(0, 4));
    const dimToggles = dims.map((k) => ({
      id: `dim:${k}`, type: "toggle", label: pretty(k), value: defaults.has(k),
    }));

    const bar = dom.controls([
      ...dimToggles,
      { id: "spacer", type: "spacer" },
      { id: "metric", type: "select", label: "Metric", options: metrics, value: state.metric },
      { id: "scope", type: "select", label: "Score level", options: ["cv", "test", "refit", "fold"], value: state.scope },
    ], () => refresh());

    const note = dom.el("div", { class: "sub" });
    const card = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Run trajectories"),
      dom.el("div", { class: "plot tall", id: "pc-plot" }), note);
    dom.mount(root, head, bar.element, card);

    const refresh = async () => {
      state.metric = bar.get("metric"); state.scope = bar.get("scope"); state.save();
      const plotNode = document.getElementById("pc-plot");
      plot.purge(plotNode);
      note.textContent = "";

      const chosen = dims.filter((k) => bar.get(`dim:${k}`));
      if (chosen.length < 2) {
        dom.mount(card, dom.el("h3", {}, "Run trajectories"),
          dom.banner("Select at least two dimensions.", "warn"));
        return;
      }
      // Re-establish the card body (banner path replaces it above).
      const freshPlot = dom.el("div", { class: "plot tall", id: "pc-plot" });
      dom.mount(card, dom.el("h3", {}, "Run trajectories"), freshPlot, note);

      const pc = await api.parallel({ dimensions: chosen.join(","), metric: state.metric, scope: state.scope });
      const rows = pc.rows || [];
      if (!rows.length) {
        dom.mount(freshPlot, dom.empty("No runs match these dimensions at this score level."));
        return;
      }

      // One Plotly parcoords dimension per chosen facet; numeric stays numeric,
      // categorical is encoded to integer codes with tick labels.
      const dimensions = chosen.map((k) => {
        const values = rows.map((r) => r[k]);
        const allNumeric = values.every((v) => typeof v === "number");
        if (allNumeric) return { label: pretty(k), values };
        const distinct = [...new Set(values.map((v) => String(v)))];
        const code = new Map(distinct.map((v, i) => [v, i]));
        return {
          label: pretty(k),
          values: values.map((v) => code.get(String(v))),
          tickvals: distinct.map((_, i) => i),
          ticktext: distinct,
        };
      });
      // Final axis = the metric itself.
      const metricValues = rows.map((r) => r.metric);
      dimensions.push({ label: state.metric, values: metricValues });

      plot.draw(freshPlot, [{
        type: "parcoords",
        line: {
          color: metricValues,
          colorscale: "Viridis",
          showscale: true,
          reversescale: pc.direction !== "max",
          colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
        },
        dimensions,
      }], { margin: { l: 60, r: 30, t: 40, b: 30 } });

      const dirArrow = pc.direction === "max" ? "↑ higher is better" : "↓ lower is better";
      note.textContent = `${rows.length} run(s); line color = ${state.metric} (${dirArrow}).`;
    };

    await refresh();
  },
};
