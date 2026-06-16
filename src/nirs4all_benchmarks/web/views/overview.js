// Overview — the landing dashboard: a brand hero + store health + best-per-dataset.

const QUICK = [
  { to: "leaderboard", icon: "🏆", title: "Leaderboard", desc: "Rank pipelines by any metric." },
  { to: "playground", icon: "🎛", title: "Playground", desc: "Group by split, aug, pp, model, param." },
  { to: "landscape", icon: "⛰", title: "Landscape 3D", desc: "The score surface in three dimensions." },
  { to: "network", icon: "🕸", title: "Network", desc: "Clustered mega-graph of pipelines." },
];

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
      dom.el("div", { class: "eyebrow" }, dom.el("span", { class: "pulse-dot" }), "NIRS pipeline benchmarks"),
      dom.el("h1", {}, "The ", dom.el("span", { class: "gradient-text" }, "Arena")),
      dom.el("p", {}, "Reproducible, scored, weights-free benchmarks over the "
        + "model × pipeline × split × cv × augmentation × dataset space — perfectly indexed, deeply explorable."));

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

    const quick = dom.el("div", { class: "grid cols-4", style: { marginTop: "16px" } },
      ...QUICK.map((q) => {
        const card = dom.el("div", { class: "card", style: { cursor: "pointer" } },
          dom.el("h3", {}, q.icon + "  " + q.title),
          dom.el("div", { class: "sub", style: { margin: "0" } }, q.desc));
        card.addEventListener("click", () => ctx.navigate(q.to));
        return card;
      }));

    dom.mount(root, head, stats, quick,
      dom.el("div", { class: "grid cols-2", style: { marginTop: "16px" } },
        dom.el("div", { class: "card" }, dom.el("h3", {}, "Best pipeline per dataset"),
          dom.el("div", { class: "sub" }, "Lowest mean RMSE (cv) on each dataset."),
          dom.el("div", { id: "ov-best" }, dom.loading())),
        dom.el("div", { class: "card" }, dom.el("h3", {}, "Runs per dataset"),
          dom.el("div", { class: "plot", id: "ov-runs" }))));

    const datasets = await api.datasets();
    const bestRows = [];
    for (const d of datasets) {
      const lb = await api.leaderboard({ metric: "rmse", scope: "cv", dataset: d.dataset_fingerprint, limit: 1 });
      const top = lb.rows?.[0];
      bestRows.push({ dataset: d.name || d.dataset_fingerprint.slice(0, 10), df: d.dataset_fingerprint,
        pipeline: top?.pipeline_label || "—", rmse: top?.mean, n: d.n_run_conditions });
    }
    dom.mount(document.getElementById("ov-best"), dom.table([
      { key: "dataset", label: "Dataset" },
      { key: "pipeline", label: "Best pipeline (cv rmse)" },
      { key: "rmse", label: "rmse", num: true, render: (r) => dom.fmt(r.rmse, 4) },
      { key: "n", label: "runs", num: true },
    ], bestRows, { onRow: (r) => { state.dataset = r.df; state.save(); ctx.navigate("leaderboard"); } }));

    plot.draw(document.getElementById("ov-runs"), [{
      type: "bar", x: bestRows.map((r) => r.dataset), y: bestRows.map((r) => r.n),
      marker: { color: "#0d9488" }, hovertemplate: "%{x}<br>%{y} run conditions<extra></extra>",
    }], { margin: { l: 44, r: 16, t: 8, b: 64 }, yaxis: { title: "run conditions" } });
  },
};
