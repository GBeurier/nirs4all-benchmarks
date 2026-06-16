// Overview — the landing dashboard: store health + best-per-dataset + activity.

export default {
  id: "overview",
  title: "Overview",
  subtitle: "The benchmark space at a glance.",
  icon: "▦",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = await api.overview();
    state._overview = ov;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, "The Arena"),
      dom.el("p", {}, "Reproducible, scored, weights-free benchmarks over the "
        + "model × pipeline × split × cv × rng × refit × dataset space."));

    const stat = (k, v, sub) => dom.el("div", { class: "stat" },
      dom.el("div", { class: "k" }, k),
      dom.el("div", { class: "v" }, String(v), sub ? dom.el("small", {}, " " + sub) : null));

    const stats = dom.el("div", { class: "grid cols-4" },
      stat("Datasets", ov.datasets),
      stat("Pipelines", ov.pipelines),
      stat("Run conditions", ov.run_conditions),
      stat("Executions", ov.valid_executions, `/ ${ov.executions}`),
      stat("Residual sets", ov.residual_sets),
      stat("Score sets", ov.score_sets),
      stat("Operators", ov.operators),
      stat("Quarantined", ov.quarantined_executions));

    dom.mount(root, head, stats,
      dom.el("div", { class: "grid cols-2", style: { marginTop: "16px" } },
        dom.el("div", { class: "card" }, dom.el("h3", {}, "Best pipeline per dataset"),
          dom.el("div", { class: "sub" }, `Lowest mean RMSE (cv) on each dataset.`),
          dom.el("div", { id: "ov-best" }, dom.loading())),
        dom.el("div", { class: "card" }, dom.el("h3", {}, "Runs per dataset"),
          dom.el("div", { class: "plot", id: "ov-runs" }))));

    // Best per dataset (one leaderboard per dataset, take #1).
    const datasets = await api.datasets();
    const bestRows = [];
    for (const d of datasets) {
      const lb = await api.leaderboard({ metric: "rmse", scope: "cv", dataset: d.dataset_fingerprint, limit: 1 });
      const top = lb.rows?.[0];
      bestRows.push({ dataset: d.name || d.dataset_fingerprint.slice(0, 10),
        df: d.dataset_fingerprint, pipeline: top?.pipeline_label || "—", rmse: top?.mean,
        n: d.n_run_conditions });
    }
    const bestTbl = dom.table([
      { key: "dataset", label: "Dataset" },
      { key: "pipeline", label: "Best pipeline (cv rmse)" },
      { key: "rmse", label: "rmse", num: true, render: (r) => dom.fmt(r.rmse, 4) },
      { key: "n", label: "runs", num: true },
    ], bestRows, { onRow: (r) => { state.dataset = r.df; state.save(); ctx.navigate("leaderboard"); } });
    dom.mount(document.getElementById("ov-best"), bestTbl);

    plot.draw(document.getElementById("ov-runs"), [{
      type: "bar",
      x: bestRows.map((r) => r.dataset), y: bestRows.map((r) => r.n),
      marker: { color: "#00704A" },
      hovertemplate: "%{x}<br>%{y} run conditions<extra></extra>",
    }], { margin: { l: 44, r: 16, t: 8, b: 60 }, yaxis: { title: "run conditions" } });
  },
};
