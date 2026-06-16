// Network — clustered mega-graph of the benchmark space (Cytoscape).
// Pipelines are linked by shared operators (Jaccard); operators by co-occurrence.
// Same reactive pattern as leaderboard.js / parallel.js: page-head + controls + refresh().

import { renderGraph, clusterColors } from "../lib/graph.js";

// Friendly label for a facet/dimension key: strip a "param:" prefix, underscores -> spaces.
function pretty(facetKey) {
  if (facetKey == null) return "";
  const k = String(facetKey);
  if (k.startsWith("param:")) return "param: " + k.slice("param:".length).replace(/_/g, " ");
  return k.replace(/_/g, " ");
}

export default {
  id: "network",
  title: "Network",
  subtitle: "Clustered mega-graph — pipelines linked by shared operators, operators linked by co-occurrence.",
  icon: "🕸",
  async render(ctx) {
    const { root, api, dom, state, navigate } = ctx;
    const ov = state._overview || (await api.overview());
    state._overview = ov;
    const metrics = ov.metrics?.length ? ov.metrics : ["rmse"];

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "kind", type: "select", label: "Graph", value: "pipelines", options: [
        { value: "pipelines", label: "Pipelines (shared operators)" },
        { value: "operators", label: "Operators (co-occurrence)" },
      ] },
      { id: "metric", type: "select", label: "Metric", options: metrics, value: state.metric },
      { id: "scope", type: "select", label: "Score level", options: ["cv", "test", "refit", "fold"], value: state.scope },
      { id: "layout", type: "select", label: "Layout", options: ["cose", "concentric", "grid"], value: "cose" },
      { id: "min_jaccard", type: "select", label: "Min Jaccard", options: ["0.2", "0.34", "0.5", "0.7"], value: "0.34" },
    ], () => refresh());

    const card = dom.el("div", { class: "card" },
      dom.el("h3", { id: "net-head" }, "Network"),
      dom.el("div", { class: "sub" },
        "node size = usage · color = cluster · hover to focus a neighbourhood · click a pipeline node to see its runs."),
      dom.el("div", { id: "net-legend", class: "controls", style: { marginBottom: "14px" } }),
      dom.el("div", { id: "net-body" }));
    dom.mount(root, head, bar.element, card);

    const refresh = async () => {
      const kind = bar.get("kind");
      state.metric = bar.get("metric"); state.scope = bar.get("scope"); state.save();
      const minJaccard = bar.get("min_jaccard");

      const headNode = document.getElementById("net-head");
      const legendNode = document.getElementById("net-legend");
      const bodyNode = document.getElementById("net-body");
      headNode.textContent = "Network";
      dom.clear(legendNode);
      dom.mount(bodyNode, dom.loading());

      const g = await api.graph({
        kind, metric: state.metric, scope: state.scope,
        min_jaccard: kind === "pipelines" ? minJaccard : undefined,
      });
      const nodes = g.nodes || [];
      const edges = g.edges || [];
      if (!nodes.length) {
        dom.clear(legendNode);
        dom.mount(bodyNode, dom.empty("No graph for these filters."));
        return;
      }

      const clusters = [...new Set(nodes.map((n) => String(n.cluster)))];
      headNode.textContent = `${dom.fmtInt(nodes.length)} nodes · ${dom.fmtInt(edges.length)} edges · ${dom.fmtInt(clusters.length)} clusters`;

      // Legend: one chip per cluster, colored to match the graph.
      const colorOf = clusterColors(clusters);
      dom.clear(legendNode);
      for (const c of clusters) {
        legendNode.appendChild(dom.el("span", {
          class: "chip",
          style: { backgroundColor: colorOf[c], color: "#fff", borderColor: "transparent" },
        }, c));
      }

      // Cytoscape needs a non-zero height before init: build the container with a
      // fixed inline height and attach it to the live DOM first, then renderGraph.
      const containerDiv = dom.el("div", {
        id: "net",
        class: "card",
        style: { height: "620px", padding: "0", overflow: "hidden" },
      });
      dom.mount(bodyNode, containerDiv);

      renderGraph(containerDiv, { nodes, edges }, {
        layout: bar.get("layout"),
        onNode: (n) => { if (kind === "pipelines") navigate("runs/" + n.id); },
      });
    };

    await refresh();
  },
};
