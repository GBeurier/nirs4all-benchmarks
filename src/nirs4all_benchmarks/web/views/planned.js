// Planned runs — conditions registered against datasets that haven't been run yet.
// The Arena stores results; a nirs4all/dag-ml runner fulfils these and ingests them back.
// Same reactive pattern as leaderboard.js: page-head + refresh() + dom.empty.

// Humanize a facet key for display: drop the "param:" prefix, snake_case → Sentence case.
function pretty(facetKey) {
  if (!facetKey) return "—";
  const base = String(facetKey).replace(/^param:/, "");
  const words = base.replace(/[_-]+/g, " ").trim();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

export default {
  id: "planned",
  title: "Planned runs",
  subtitle: "Pipelines registered against datasets that haven't been run yet.",
  icon: "🗓",
  async render(ctx) {
    const { root, api, dom } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const banner = dom.banner(
      "The Arena stores results, it does not run compute — a nirs4all/dag-ml runner fulfils these planned conditions and ingests the result, which then appears across the dataviz.",
      "info");

    const bar = dom.controls([
      { id: "refresh", type: "button", label: "Refresh", onClick: () => refresh() },
    ]);

    const tableHost = dom.el("div", {});
    dom.mount(root, head, banner, bar.element, tableHost);

    const refresh = async () => {
      dom.mount(tableHost, dom.loading());
      const rows = await api.planned();
      if (!rows.length) {
        dom.mount(tableHost, dom.empty(
          "No planned runs. Upload a pipeline with target datasets (Contribute → Upload) to plan one."));
        return;
      }

      const tbl = dom.table([
        { key: "pipeline", label: "Pipeline",
          sortKey: (r) => r.human_label || r.pipeline_dag_hash || "",
          render: (r) => dom.el("span", {}, r.human_label || dom.shortHash(r.pipeline_dag_hash, 12)) },
        { key: "dataset", label: "Dataset",
          sortKey: (r) => r.dataset_name || r.dataset_fingerprint || "",
          render: (r) => dom.el("span", {}, r.dataset_name || dom.shortHash(r.dataset_fingerprint, 12)) },
        { key: "collection_id", label: "Collection",
          render: (r) => r.collection_id ? dom.chip(r.collection_id) : dom.el("span", { class: "muted" }, "—") },
        { key: "source", label: "Source",
          render: (r) => r.source ? dom.badge(r.source, "gray") : dom.el("span", { class: "muted" }, "—") },
        { key: "created_at", label: "Created",
          render: (r) => dom.el("span", { class: "muted" }, r.created_at || "—") },
      ], rows, {
        sort: { key: "created_at", dir: -1 },
        onRow: (r) => ctx.navigate(`runs/${r.pipeline_dag_hash}`),
      });

      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} planned condition${rows.length === 1 ? "" : "s"}`),
        dom.el("div", { class: "sub" }, "Click a row to see existing runs of that pipeline."),
        tbl));
    };

    await refresh();
  },
};
