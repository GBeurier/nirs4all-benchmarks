// Pipeline × Dataset and Model × Dataset score matrices (heatmaps).

import { uniqueLabels } from "../lib/labels.js";

const GROUPINGS = {
  pipeline: {
    title: "Score matrix",
    rows: (m) => m.pipelines || [],
    key: (row) => row.pipeline_dag_hash,
    cellKey: (c) => c.pipeline_dag_hash,
    fallback: (row) => row.pipeline_dag_hash.slice(0, 8),
    note: (metric) => "cell = mean " + metric + "; gaps = pipeline not run on that dataset.",
  },
  model: {
    title: "Model × Dataset",
    rows: (m) => m.models || [],
    key: (row) => row.model,
    cellKey: (c) => c.model,
    fallback: (row) => row.model || "unknown",
    note: (metric) => "cell = unweighted mean of the model's pipeline means (" + metric
      + "); gaps = model not run on that dataset.",
  },
};

export default {
  id: "matrix",
  title: "Pipeline × Dataset",
  subtitle: "Every pipeline's and every model's score on every dataset, side by side.",
  icon: "▩",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "quar", type: "toggle", label: "Include quarantined", value: false },
    ], () => refresh());

    const cards = Object.entries(GROUPINGS).map(([by, g]) => {
      const note = dom.el("div", { class: "sub" });
      const card = dom.el("div", { class: "card" },
        dom.el("h3", {}, g.title),
        dom.el("div", { class: "plot tall", id: `mx-plot-${by}` }), note);
      return { by, g, note, card };
    });
    dom.mount(root, head, bar.element, ...cards.map((c) => c.card));

    const draw = async ({ by, g, note }) => {
      const plotNode = document.getElementById(`mx-plot-${by}`);
      plot.purge(plotNode);
      note.textContent = "";
      const m = await api.matrix({
        metric: state.metric, scope: state.scope, dataset: state.dataset || undefined,
        include_quarantined: bar.get("quar"), by,
      });
      const cells = m.cells || [];
      const datasets = m.datasets || [];
      const rows = g.rows(m);
      if (!cells.length || !datasets.length || !rows.length) {
        dom.mount(plotNode, dom.empty("No cells match these filters."));
        return;
      }

      const datasetCol = new Map(datasets.map((d, i) => [d.dataset_fingerprint, i]));
      const rowIndex = new Map(rows.map((row, i) => [g.key(row), i]));
      const z = rows.map(() => datasets.map(() => null));
      const counts = rows.map(() => datasets.map(() => null));
      for (const c of cells) {
        const r = rowIndex.get(g.cellKey(c));
        const k = datasetCol.get(c.dataset_fingerprint);
        if (r == null || k == null) continue;
        z[r][k] = c.value;
        counts[r][k] = c.n_pipelines ?? null;
      }

      // sequential is green->paper (low->high). direction "min" => lower is better => keep
      // green at low end; direction "max" => higher is better => reverse so green is high.
      const colorscale = m.direction === "max"
        ? plot.sequential.map(([s, col]) => [1 - s, col]).reverse()
        : plot.sequential;

      const xLabels = uniqueLabels(datasets, (d) => d.label, (d) => d.dataset_fingerprint.slice(0, 8));
      const yLabels = uniqueLabels(rows, (row) => row.label, g.fallback);
      const pipelines = by === "model" ? "<br>pipelines = %{customdata}" : "";

      plot.draw(plotNode, [{
        type: "heatmap",
        z, x: xLabels, y: yLabels,
        ...(by === "model" ? { customdata: counts } : {}),
        colorscale, hoverongaps: false,
        colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
        hovertemplate: "%{y}<br>%{x}<br>" + state.metric + " = %{z:.4f}" + pipelines + "<extra></extra>",
      }], {
        margin: { l: 230, r: 24, t: 12, b: 120 },
        xaxis: { title: "", tickangle: -40 },
        yaxis: { title: "", autorange: "reversed" },
      });

      note.textContent = g.note(state.metric);
    };
    const refresh = async () => { for (const c of cards) await draw(c); };
    await refresh();
  },
};
