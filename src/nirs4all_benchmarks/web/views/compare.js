// Residual compare — the sample-keyed showcase. Pair two runs on the same
// samples and inspect where they disagree: off-diagonal points = complementary
// models. Routed by #/compare.

export default {
  id: "compare",
  title: "Residual compare",
  subtitle: "Compare two runs’ residuals on the same samples — find complementary models.",
  icon: "⚖",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const runs = await api.runs({ metric: state.metric, scope: state.scope, dataset: state.dataset || undefined, limit: 500 });

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    if (!runs.length || runs.length < 2) {
      dom.mount(root, head, dom.banner("Need at least two runs to compare.", "warn"));
      return;
    }

    const runOptions = runs.map((r) => ({
      value: r.execution_hash,
      label: (r.pipeline_label || dom.shortHash(r.execution_hash, 10))
        + " · " + dom.shortHash(r.dataset_fingerprint, 8),
    }));

    const bar = dom.controls([
      { id: "a", type: "select", label: "Run A", options: runOptions, value: runOptions[0].value },
      { id: "b", type: "select", label: "Run B", options: runOptions, value: (runOptions[1] || runOptions[0]).value },
      { id: "partition", type: "select", label: "Partition", options: [
        { value: "validation", label: "validation" }, { value: "test", label: "test" },
      ], value: "validation" },
    ], () => refresh());

    const statsHost = dom.el("div", {});
    const scatterCard = dom.el("div", { class: "card", style: { marginTop: "16px" } },
      dom.el("h3", {}, "Residual A vs. residual B"),
      dom.el("div", { class: "sub" }, "Points off the diagonal = the two models err on different samples (complementary)."),
      dom.el("div", { class: "plot tall", id: "cmp-scatter" }));

    dom.mount(root, head, bar.element, statsHost, scatterCard);

    const stat = (k, v, sub) => dom.el("div", { class: "stat" },
      dom.el("div", { class: "k" }, k),
      dom.el("div", { class: "v" }, String(v), sub ? dom.el("small", {}, " " + sub) : null));

    const refresh = async () => {
      const a = bar.get("a"), b = bar.get("b"), partition = bar.get("partition");
      const plotNode = document.getElementById("cmp-scatter");

      if (a === b) {
        plot.purge(plotNode);
        dom.mount(statsHost, dom.banner("Pick two different runs to compare.", "info"));
        return;
      }

      dom.mount(statsHost, dom.loading());
      const cmp = await api.compare(a, b, partition);
      const paired = cmp.paired || [];

      dom.mount(statsHost, dom.el("div", { class: "grid cols-3" },
        stat("Common samples", dom.fmtInt(cmp.n_common), cmp.n_paired != null ? `· ${dom.fmtInt(cmp.n_paired)} paired` : null),
        stat("Residual corr", dom.fmt(cmp.residual_correlation, 3), "low/negative = complementary"),
        stat("RMSE A / B", dom.fmt(cmp.rmse_a, 3) + " / " + dom.fmt(cmp.rmse_b, 3))));

      if (!paired.length) {
        plot.purge(plotNode);
        scatterCard.appendChild(dom.empty("No paired sample-keyed residuals for these runs."));
        return;
      }
      if (!document.getElementById("cmp-scatter")) {
        scatterCard.appendChild(dom.el("div", { class: "plot tall", id: "cmp-scatter" }));
      }
      const node = document.getElementById("cmp-scatter");

      const ra = paired.map((p) => p.residual_a);
      const rb = paired.map((p) => p.residual_b);
      const all = ra.concat(rb).filter((v) => v != null && !Number.isNaN(v));
      const span = Math.max(Math.abs(Math.min(...all)), Math.abs(Math.max(...all))) || 1;
      const lim = span * 1.05;

      plot.draw(node, [
        { type: "scatter", mode: "markers", x: ra, y: rb,
          marker: plot.marker({ color: paired.map((p) => p.y_true),
            colorscale: plot.diverging, showscale: true, colorbar: { title: "observed", thickness: 12 } }),
          text: paired.map((p) => p.sample_id),
          hovertemplate: "%{text}<br>resid A %{x:.3f} · resid B %{y:.3f}<extra></extra>" },
        { type: "scatter", mode: "lines", x: [-lim, lim], y: [-lim, lim],
          line: { dash: "dash", color: "#94a3b8" }, hoverinfo: "skip", showlegend: false },
        { type: "scatter", mode: "lines", x: [-lim, lim], y: [0, 0],
          line: { dash: "dot", color: "#cbd5cf" }, hoverinfo: "skip", showlegend: false },
        { type: "scatter", mode: "lines", x: [0, 0], y: [-lim, lim],
          line: { dash: "dot", color: "#cbd5cf" }, hoverinfo: "skip", showlegend: false },
      ], { margin: { l: 56, r: 12, t: 10, b: 48 }, showlegend: false,
        xaxis: { title: "residual A", range: [-lim, lim] },
        yaxis: { title: "residual B", range: [-lim, lim], scaleanchor: "x", scaleratio: 1 } });
    };

    await refresh();
  },
};
