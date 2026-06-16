// Pipeline × Dataset score matrix — every pipeline's score on every dataset (heatmap).

export default {
  id: "matrix",
  title: "Pipeline × Dataset",
  subtitle: "Every pipeline's score on every dataset, side by side.",
  icon: "▩",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "quar", type: "toggle", label: "Include quarantined", value: false },
    ], () => refresh());

    const note = dom.el("div", { class: "sub" });
    const card = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Score matrix"),
      dom.el("div", { class: "plot tall", id: "mx-plot" }), note);
    dom.mount(root, head, bar.element, card);

    const refresh = async () => {
      const plotNode = document.getElementById("mx-plot");
      plot.purge(plotNode);
      note.textContent = "";
      const m = await api.matrix({
        metric: state.metric, scope: state.scope, dataset: state.dataset || undefined,
        include_quarantined: bar.get("quar"),
      });
      const cells = m.cells || [];
      const datasets = m.datasets || [];
      const pipelines = m.pipelines || [];
      if (!cells.length || !datasets.length || !pipelines.length) {
        dom.mount(plotNode, dom.empty("No cells match these filters."));
        return;
      }

      const datasetCol = new Map(datasets.map((d, i) => [d.dataset_fingerprint, i]));
      const pipelineRow = new Map(pipelines.map((p, i) => [p.pipeline_dag_hash, i]));
      const z = pipelines.map(() => datasets.map(() => null));
      for (const c of cells) {
        const r = pipelineRow.get(c.pipeline_dag_hash);
        const k = datasetCol.get(c.dataset_fingerprint);
        if (r == null || k == null) continue;
        z[r][k] = c.value;
      }

      // sequential is green->paper (low->high). direction "min" => lower is better => keep
      // green at low end; direction "max" => higher is better => reverse so green is high.
      const colorscale = m.direction === "max"
        ? plot.sequential.map(([s, col]) => [1 - s, col]).reverse()
        : plot.sequential;

      const xLabels = datasets.map((d) => d.label);
      const yLabels = pipelines.map((p) => p.label);

      plot.draw(plotNode, [{
        type: "heatmap",
        z, x: xLabels, y: yLabels,
        colorscale, hoverongaps: false,
        colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
        hovertemplate: "%{y}<br>%{x}<br>" + state.metric + " = %{z:.4f}<extra></extra>",
      }], {
        margin: { l: 230, r: 24, t: 12, b: 120 },
        xaxis: { title: "", tickangle: -40 },
        yaxis: { title: "", autorange: "reversed" },
      });

      note.textContent = "cell = mean " + state.metric
        + "; gaps = pipeline not run on that dataset.";
    };
    await refresh();
  },
};
