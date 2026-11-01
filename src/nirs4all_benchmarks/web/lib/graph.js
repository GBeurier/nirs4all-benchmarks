// Cytoscape helper for clustered network "mega graphs". Cytoscape is loaded
// globally via index.html (window.cytoscape). Nodes are colored by a cluster
// field and sized by a magnitude field; a force-directed cose layout reveals
// clusters of related pipelines / co-occurring operators.

import { palette } from "./plot.js";

export function clusterColors(clusters) {
  const map = {};
  clusters.forEach((c, i) => { map[String(c)] = palette[i % palette.length]; });
  return map;
}

// Edges kept per node (its strongest ones). Pipelines sharing operators link almost every
// pair (thousands of edges): unreadable, and the force layout's cost grows with them.
const MAX_EDGES_PER_NODE = 6;
// Above this many nodes, labels overlap into an illegible wall: show them on hover only.
const MAX_ALWAYS_LABELLED = 40;

// Keep each node's strongest edges; an edge survives if it is among the top for either end.
export function strongestEdges(edges, perNode = MAX_EDGES_PER_NODE) {
  const byNode = new Map();
  for (const e of edges) {
    for (const n of [e.source, e.target]) {
      if (!byNode.has(n)) byNode.set(n, []);
      byNode.get(n).push(e);
    }
  }
  const keep = new Set();
  for (const list of byNode.values()) {
    list.sort((a, b) => (b.weight || 0) - (a.weight || 0));
    for (const e of list.slice(0, perNode)) keep.add(e);
  }
  return edges.filter((e) => keep.has(e));
}

// data = { nodes:[{id,label,cluster,size,score,...}], edges:[{source,target,weight}] }
// opts = { onNode(nodeData), layout: "cose"|"concentric"|"grid" }
export function renderGraph(container, data, opts = {}) {
  if (!window.cytoscape) { container.textContent = "Cytoscape failed to load."; return null; }
  const nodes = data.nodes || [];
  const edges = data.edges || [];
  const clusters = [...new Set(nodes.map((n) => String(n.cluster)))];
  const colorOf = clusterColors(clusters);
  const sizes = nodes.map((n) => Number(n.size) || 1);
  const mn = Math.min(...sizes, 1), mx = Math.max(...sizes, 1);
  const scale = (s) => 16 + 44 * (((Number(s) || 1) - mn) / ((mx - mn) || 1));
  const alwaysLabelled = nodes.length <= MAX_ALWAYS_LABELLED;

  const elements = [
    ...nodes.map((n) => ({
      data: {
        id: String(n.id), label: n.label || String(n.id), cluster: String(n.cluster),
        _size: scale(n.size), score: n.score, _raw: n,
      },
    })),
    ...edges
      .filter((e) => e.source != null && e.target != null)
      .map((e, i) => ({ data: { id: `e${i}`, source: String(e.source), target: String(e.target), weight: e.weight || 1 } })),
  ];

  const cy = window.cytoscape({
    container,
    elements,
    wheelSensitivity: 0.2,
    style: [
      {
        selector: "node",
        style: {
          "background-color": (ele) => colorOf[ele.data("cluster")] || "#94a3b8",
          width: "data(_size)", height: "data(_size)",
          label: alwaysLabelled ? "data(label)" : "", "font-size": 9, "font-family": "'JetBrains Mono', monospace",
          color: "#334155", "text-valign": "bottom", "text-margin-y": 3,
          "text-max-width": 96, "text-wrap": "ellipsis", "text-overflow-wrap": "anywhere",
          "border-width": 1.5, "border-color": "#ffffff", "overlay-opacity": 0,
        },
      },
      {
        selector: "edge",
        style: {
          width: (ele) => 1 + 3 * (Number(ele.data("weight")) || 0),
          "line-color": "#cbd5e1", opacity: 0.5, "curve-style": "haystack", "haystack-radius": 0.3,
        },
      },
      { selector: "node.labelled", style: { label: "data(label)", "z-index": 10 } },
      { selector: "node:selected", style: { "border-color": "#0d9488", "border-width": 3.5 } },
      { selector: "node.dim", style: { opacity: 0.15 } },
      { selector: "edge.dim", style: { opacity: 0.04 } },
    ],
    layout: layoutFor(opts.layout, edges.length),
  });

  // hover highlight: focus a node + its neighbourhood
  cy.on("mouseover", "node", (evt) => {
    const n = evt.target; const hood = n.closedNeighborhood();
    cy.elements().addClass("dim"); hood.removeClass("dim"); hood.nodes().addClass("labelled");
  });
  cy.on("mouseout", "node", () => cy.elements().removeClass("dim labelled"));
  if (opts.onNode) cy.on("tap", "node", (evt) => opts.onNode(evt.target.data("_raw")));

  return { cy, colorOf, clusters };
}

function layoutFor(name, edgeCount) {
  if (name === "concentric") return { name: "concentric", animate: false, padding: 30, minNodeSpacing: 16 };
  if (name === "grid") return { name: "grid", animate: false, padding: 30 };
  // force-directed; tune repulsion to spread clusters
  return {
    name: "cose", animate: false, randomize: true, padding: 36,
    nodeRepulsion: 9000, idealEdgeLength: edgeCount > 200 ? 70 : 100,
    edgeElasticity: 80, gravity: 0.25, numIter: 1200, componentSpacing: 90,
  };
}
