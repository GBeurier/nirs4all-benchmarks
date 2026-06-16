// Run explorer — filterable table of every individual run, with a metric histogram.

export default {
  id: "runs",
  title: "Run explorer",
  subtitle: "Every individual run — filter, sort, and drill into one.",
  icon: "🧪",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const pipelineFilter = ctx.param || "";
    const banner = pipelineFilter
      ? dom.banner(
          dom.el("span", {},
            "Filtered to pipeline ", dom.el("span", { class: "hash" }, dom.shortHash(pipelineFilter, 12)), ". ",
            dom.el("a", { href: "#", onclick: (e) => { e.preventDefault(); ctx.navigate("runs"); } }, "Clear filter")),
          "info")
      : null;

    const bar = dom.controls([
      { id: "operator", type: "search", label: "Operator", placeholder: "operator dotted path…", value: "" },
      { id: "quar", type: "toggle", label: "Include quarantined", value: true },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Metric distribution"),
      dom.el("div", { class: "sub" }, "Histogram of every run's metric value at the chosen score level."),
      dom.el("div", { class: "plot", id: "runs-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, banner, bar.element, chartCard, tableHost);

    const refresh = async () => {
      const plotNode = document.getElementById("runs-plot");
      dom.mount(tableHost, dom.loading());
      const operatorSearch = (bar.get("operator") || "").trim();
      const runs = await api.runs({
        metric: state.metric, scope: state.scope, dataset: state.dataset || undefined,
        pipeline: pipelineFilter || undefined, operator: operatorSearch || undefined,
        include_quarantined: bar.get("quar"), limit: 1000,
      });
      if (!runs.length) { plot.purge(plotNode); dom.mount(tableHost, dom.empty("No runs match these filters.")); return; }

      plot.draw(plotNode, [{
        type: "histogram",
        x: runs.map((r) => r.metric_value),
        marker: { color: "#0d9488" },
        hovertemplate: state.metric + " = %{x:.4f}<br>%{y} runs<extra></extra>",
      }], { margin: { l: 56, r: 18, t: 10, b: 48 }, xaxis: { title: state.metric }, yaxis: { title: "runs" } });

      const tbl = dom.table([
        { key: "metric_value", label: state.metric, num: true, render: (r) => dom.fmt(r.metric_value, 4) },
        { key: "pipeline_label", label: "Pipeline", render: (r) => dom.el("span", {}, r.pipeline_label || dom.shortHash(r.pipeline_dag_hash, 12)) },
        { key: "main_model", label: "Model", render: (r) => dom.badge((r.main_model || "—").split(".").pop(), "role") },
        { key: "dataset_fingerprint", label: "Dataset", render: (r) => dom.el("span", { class: "hash" }, dom.shortHash(r.dataset_fingerprint, 10)) },
        { key: "execution_validity", label: "Validity", render: (r) =>
          r.execution_validity === "valid" ? dom.badge("valid", "green")
            : r.execution_validity === "quarantined" ? dom.badge("quarantined", "amber")
            : dom.badge(r.execution_validity || "invalid", "red") },
        { key: "producer_capsule", label: "Capsule", render: (r) => dom.badge(r.producer_capsule || "—", "gray") },
        { key: "time_ms", label: "time (ms)", num: true, render: (r) => dom.fmtInt(r.time_ms) },
      ], runs, {
        sort: { key: "metric_value", dir: 1 },
        onRow: (r) => ctx.navigate(`run/${r.execution_hash}`),
      });
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${runs.length} runs`),
        dom.el("div", { class: "sub" }, "Click a row to open the run detail."), tbl));
    };
    await refresh();
  },
};
