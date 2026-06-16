// Pipelines — the canonical, topology-aware pipeline catalog.
// Each row is one pipeline_dag_hash: equivalent pipelines collapse to a single identity.

export default {
  id: "pipelines",
  title: "Pipelines",
  subtitle: "Canonical, topology-aware pipeline identities.",
  icon: "🧬",
  async render(ctx) {
    const { root, api, dom } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "q", type: "search", label: "Filter", placeholder: "Label, model or hash…" },
      { id: "structure", type: "select", label: "Structure", value: "", options: [
        { value: "", label: "All structures" },
        { value: "linear", label: "Linear" },
        { value: "branching", label: "Branching" },
      ] },
    ], () => refresh());

    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, tableHost);

    const all = await api.pipelines();

    const refresh = () => {
      const q = (bar.get("q") || "").trim().toLowerCase();
      const structure = bar.get("structure");
      let rows = all.slice();
      if (structure === "linear") rows = rows.filter((p) => p.is_linear);
      else if (structure === "branching") rows = rows.filter((p) => !p.is_linear);
      if (q) {
        rows = rows.filter((p) =>
          (p.human_label || "").toLowerCase().includes(q) ||
          (p.main_model || "").toLowerCase().includes(q) ||
          (p.pipeline_dag_hash || "").toLowerCase().includes(q));
      }

      if (!rows.length) {
        dom.mount(tableHost, dom.el("div", { class: "card" },
          dom.el("h3", {}, "Pipeline catalog"),
          dom.empty("No pipelines match these filters.")));
        return;
      }

      const tbl = dom.table([
        { key: "human_label", label: "Pipeline",
          sortKey: (r) => r.human_label || r.pipeline_dag_hash,
          render: (r) => dom.el("span", {}, r.human_label || dom.shortHash(r.pipeline_dag_hash, 12)) },
        { key: "main_model", label: "Model",
          render: (r) => dom.badge((r.main_model || "—").split(".").pop(), "role") },
        { key: "n_nodes", label: "nodes", num: true },
        { key: "is_linear", label: "Structure",
          sortKey: (r) => (r.is_linear ? 0 : 1),
          render: (r) => r.is_linear ? dom.badge("linear", "gray") : dom.badge("branching", "role") },
        { key: "n_run_conditions", label: "run conditions", num: true },
        { key: "pipeline_dag_hash", label: "dag hash",
          render: (r) => dom.el("span", { class: "hash" }, dom.shortHash(r.pipeline_dag_hash, 14)) },
      ], rows, {
        sort: { key: "n_run_conditions", dir: -1 },
        onRow: (r) => ctx.navigate(`runs/${r.pipeline_dag_hash}`),
      });

      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} pipeline${rows.length === 1 ? "" : "s"}`),
        dom.el("div", { class: "sub" },
          "pipeline_dag_hash is a topology-aware Merkle hash; two equivalent pipelines collapse to one row. "
          + "Click a row to see every run of that pipeline."),
        tbl));
    };

    refresh();
  },
};
