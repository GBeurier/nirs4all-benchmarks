// Run detail — the canonical DAG + folds + scores + residuals for one execution.
// Routed by #/run/<execution_hash> (not shown in the nav).

const ROLE_COLORS = {
  input: "#94a3b8", transform: "#0d9488", model: "#00704A", merge: "#d97706",
  mean: "#d97706", stacking: "#4f46e5", pipeline: "#06b6d4",
};

function layoutDag(nodes, edges) {
  const byId = Object.fromEntries(nodes.map((n) => [n.node_id || n.id, n]));
  const adj = {}, indeg = {};
  for (const n of nodes) { adj[n.node_id || n.id] = []; indeg[n.node_id || n.id] = 0; }
  for (const e of edges) { if (adj[e.src]) { adj[e.src].push(e.dst); indeg[e.dst] = (indeg[e.dst] || 0) + 1; } }
  // longest-path depth
  const depth = {};
  const order = Object.keys(byId).sort();
  let changed = true, guard = 0;
  for (const id of order) depth[id] = 0;
  while (changed && guard++ < 100) {
    changed = false;
    for (const e of edges) {
      if (depth[e.dst] < depth[e.src] + 1) { depth[e.dst] = depth[e.src] + 1; changed = true; }
    }
  }
  const cols = {};
  for (const id of order) { (cols[depth[id]] ||= []).push(id); }
  return { byId, cols, depth };
}

function renderDag(nodes, edges) {
  if (!nodes.length) return null;
  const { byId, cols } = layoutDag(nodes, edges);
  const colKeys = Object.keys(cols).map(Number).sort((a, b) => a - b);
  const NW = 150, NH = 44, GX = 70, GY = 22;
  const maxRows = Math.max(...colKeys.map((c) => cols[c].length), 1);
  const W = colKeys.length * (NW + GX) + GX;
  const H = maxRows * (NH + GY) + GY;
  const pos = {};
  colKeys.forEach((c, ci) => {
    cols[c].forEach((id, ri) => {
      const colCount = cols[c].length;
      const offset = (maxRows - colCount) * (NH + GY) / 2;
      pos[id] = { x: GX + ci * (NW + GX), y: GY + offset + ri * (NH + GY) };
    });
  });
  const svgns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(svgns, "svg");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("class", "dag-svg");
  svg.style.maxHeight = "420px";
  // edges
  for (const e of edges) {
    const a = pos[e.src], b = pos[e.dst]; if (!a || !b) continue;
    const x1 = a.x + NW, y1 = a.y + NH / 2, x2 = b.x, y2 = b.y + NH / 2;
    const mx = (x1 + x2) / 2;
    const path = document.createElementNS(svgns, "path");
    path.setAttribute("d", `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "var(--border-2)");
    path.setAttribute("stroke-width", "1.5");
    svg.appendChild(path);
  }
  // nodes
  for (const [id, p] of Object.entries(pos)) {
    const n = byId[id];
    const role = (n.role || n.kind || "transform").toLowerCase();
    const color = ROLE_COLORS[role] || "#0d9488";
    const g = document.createElementNS(svgns, "g"); g.setAttribute("class", "dag-node");
    const rect = document.createElementNS(svgns, "rect");
    rect.setAttribute("x", p.x); rect.setAttribute("y", p.y);
    rect.setAttribute("width", NW); rect.setAttribute("height", NH); rect.setAttribute("rx", 8);
    rect.setAttribute("fill", color + "1f"); rect.setAttribute("stroke", color);
    g.appendChild(rect);
    const op = (n.operator || "").split(".").pop() || role;
    const t1 = document.createElementNS(svgns, "text");
    t1.setAttribute("x", p.x + 10); t1.setAttribute("y", p.y + 19); t1.setAttribute("fill", "var(--text)");
    t1.setAttribute("font-weight", "600"); t1.textContent = op.slice(0, 18);
    g.appendChild(t1);
    const t2 = document.createElementNS(svgns, "text");
    t2.setAttribute("x", p.x + 10); t2.setAttribute("y", p.y + 34); t2.setAttribute("fill", color);
    t2.setAttribute("font-size", "10"); t2.textContent = role;
    g.appendChild(t2);
    svg.appendChild(g);
  }
  return svg;
}

export default {
  id: "run",
  title: "Run detail",
  subtitle: "",
  icon: "🔬",
  hidden: true,
  async render(ctx) {
    const { root, api, dom, plot, param } = ctx;
    if (!param) { dom.mount(root, dom.empty("No run selected.")); return; }
    const d = await api.runDetail(param);
    const exec = d.execution, pipe = d.pipeline;

    const validity = exec.validity_status === "valid"
      ? dom.badge("valid", "green")
      : dom.badge(exec.validity_status, exec.validity_status === "quarantined" ? "amber" : "red");

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, pipe.human_label || (pipe.main_model || "Pipeline").split(".").pop()),
      dom.el("p", {},
        dom.el("span", { class: "hash" }, "exec " + dom.shortHash(exec.execution_hash, 14)), " ",
        validity, " ", dom.badge(exec.producer_capsule || "?", "gray")));

    // DAG card
    const dagCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Pipeline DAG"),
      dom.el("div", { class: "sub" }, `${d.nodes.length} nodes · ${d.edges.length} edges · `
        + (pipe.is_linear ? "linear" : "branching")),
      renderDag(d.nodes, d.edges) || dom.empty("No graph."));

    // condition KV
    const kv = dom.el("dl", { class: "kv" });
    const add = (k, v) => { kv.appendChild(dom.el("dt", {}, k)); kv.appendChild(dom.el("dd", {}, v ?? "—")); };
    add("pipeline_dag_hash", dom.shortHash(pipe.pipeline_dag_hash, 20));
    add("run_condition_hash", dom.shortHash(d.run_condition?.run_condition_hash, 20));
    add("dataset", dom.shortHash(d.run_condition?.dataset_fingerprint, 16));
    add("cv", d.cv ? `${(d.cv.engine_fold_set_fingerprint && "fold-set ") || ""}${dom.shortHash(d.cv.cv_instance_hash, 12)}` : "—");
    add("rng seed", d.rng?.root_seed ?? "—");
    add("refit", d.refit?.strategy ?? "—");
    add("nirs4all_identity", pipe.nirs4all_identity_hash ?? "—");
    add("engine_graph_fp", dom.shortHash(pipe.engine_graph_fingerprint, 16));
    const condCard = dom.el("div", { class: "card" }, dom.el("h3", {}, "Run condition"), kv);

    dom.mount(root, head, dom.el("div", { class: "grid cols-2" }, dagCard, condCard));

    // scores
    const cvScores = d.scores.filter((s) => s.scope === "cv");
    const scoreCard = dom.el("div", { class: "card", style: { marginTop: "16px" } },
      dom.el("h3", {}, "Scores"),
      dom.table([
        { key: "metric_name", label: "Metric" },
        { key: "scope", label: "Level", render: (r) => dom.badge(r.scope, "gray") },
        { key: "partition", label: "Partition" },
        { key: "fold_id", label: "Fold", render: (r) => r.fold_id || "—" },
        { key: "metric_value", label: "Value", num: true, render: (r) => dom.fmt(r.metric_value, 4) },
      ], d.scores, { sort: { key: "metric_name", dir: 1 } }));

    // fold bar for rmse
    const foldRows = d.scores.filter((s) => s.scope === "cv" && s.metric_name === "rmse" && s.fold_id);
    const folds = dom.el("div", { class: "card", style: { marginTop: "16px" } },
      dom.el("h3", {}, "Per-fold rmse"), dom.el("div", { class: "plot", id: "rd-folds" }));

    // residual scatter (validation)
    const resCard = dom.el("div", { class: "card", style: { marginTop: "16px" } },
      dom.el("h3", {}, "Predicted vs. observed (validation)"),
      dom.el("div", { class: "plot", id: "rd-scatter" }));
    root.appendChild(scoreCard); root.appendChild(folds); root.appendChild(resCard);

    if (foldRows.length) {
      plot.draw(document.getElementById("rd-folds"), [{
        type: "bar", x: foldRows.map((r) => r.fold_id), y: foldRows.map((r) => r.metric_value),
        marker: { color: "#0d9488" }, hovertemplate: "%{x}: %{y:.4f}<extra></extra>",
      }], { margin: { l: 48, r: 12, t: 8, b: 40 }, yaxis: { title: "rmse" } });
    } else { document.getElementById("rd-folds").replaceWith(dom.empty("No per-fold scores.")); }

    const res = await api.residuals(exec.execution_hash, "validation");
    if (res.length) {
      const yt = res.map((r) => r.y_true), yp = res.map((r) => r.y_pred);
      const lo = Math.min(...yt, ...yp), hi = Math.max(...yt, ...yp);
      plot.draw(document.getElementById("rd-scatter"), [
        { type: "scatter", mode: "markers", x: yt, y: yp,
          marker: { color: "#00704A", size: 6, opacity: 0.6 },
          text: res.map((r) => r.sample_id), hovertemplate: "%{text}<br>obs %{x:.3f} · pred %{y:.3f}<extra></extra>" },
        { type: "scatter", mode: "lines", x: [lo, hi], y: [lo, hi], line: { dash: "dash", color: "#94a3b8" }, hoverinfo: "skip", showlegend: false },
      ], { margin: { l: 52, r: 12, t: 8, b: 44 }, xaxis: { title: "observed" }, yaxis: { title: "predicted" }, showlegend: false });
    } else { document.getElementById("rd-scatter").replaceWith(dom.empty("No sample-keyed residuals for this run.")); }
  },
};
