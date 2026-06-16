// Composition — how pipelines are built. A 2-level hierarchy (role -> operator),
// sized by usage and colored by score, drawn as a sunburst or treemap.
// Same reactive pattern as leaderboard.js: page-head + controls + refresh().

// Render a facet/metric key nicely: strip a "param:" prefix, underscores -> spaces.
function pretty(facetKey) {
  if (facetKey == null) return "";
  const k = String(facetKey);
  if (k.startsWith("param:")) return "param: " + k.slice(6).replace(/_/g, " ");
  return k.replace(/_/g, " ");
}

export default {
  id: "composition",
  title: "Composition",
  subtitle: "How pipelines are built — roles and operators, sized by use, colored by score.",
  icon: "☀",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const bar = dom.controls([
      { id: "size_by", type: "select", label: "Size by", options: [
        { value: "n_runs", label: "runs" },
        { value: "n_pipes", label: "pipelines" },
      ], value: "n_runs" },
      { id: "chart", type: "select", label: "Chart", options: ["sunburst", "treemap"], value: "sunburst" },
    ], () => refresh());

    const chartCard = dom.el("div", { class: "card" },
      dom.el("h3", { id: "comp-title" }, "Pipeline composition"),
      dom.el("div", { class: "sub" }, "Inner ring = stage role; outer = operators. Size = usage; color = mean score."),
      dom.el("div", { class: "plot xtall", id: "comp-plot" }));
    const tableHost = dom.el("div", {});
    dom.mount(root, head, bar.element, chartCard, tableHost);

    const refresh = async () => {
      const size_by = bar.get("size_by");
      const chart = bar.get("chart");

      const plotNode = document.getElementById("comp-plot");
      const titleNode = document.getElementById("comp-title");
      titleNode.textContent = `Composition by ${pretty(state.metric)}`;
      dom.mount(tableHost, dom.loading());

      const comp = await api.composition({ metric: state.metric, scope: state.scope });
      const rows = comp.rows || [];
      if (!rows.length) {
        plot.purge(plotNode);
        dom.mount(tableHost, dom.empty("No runs match these filters."));
        return;
      }

      const dirArrow = comp.direction === "max" ? "↑ higher is better" : "↓ lower is better";

      // Build a 2-level hierarchy: root ("All") -> role -> operator.
      const ids = [];
      const labels = [];
      const parents = [];
      const values = [];
      const colors = [];

      // Root — value is filled in after summing children (branchvalues:"total"
      // requires parent value == sum of children, else the chart renders nothing).
      ids.push("All"); labels.push("All"); parents.push(""); values.push(0); colors.push(null);

      // Group rows by role to aggregate role-level value and weighted-mean score.
      const roleMap = new Map();
      for (const r of rows) {
        const role = r.role ?? "—";
        if (!roleMap.has(role)) roleMap.set(role, []);
        roleMap.get(role).push(r);
      }

      let total = 0;
      for (const [role, opsRows] of roleMap) {
        let roleValue = 0;
        let weightSum = 0;
        let scoreWeighted = 0;
        for (const r of opsRows) {
          const v = Math.max(1, Number(r[size_by]) || 0);
          roleValue += v;
          if (r.score != null && !Number.isNaN(r.score)) { weightSum += v; scoreWeighted += v * r.score; }
        }
        total += roleValue;
        ids.push(role); labels.push(pretty(role)); parents.push("All");
        values.push(roleValue);
        colors.push(weightSum > 0 ? scoreWeighted / weightSum : null);

        for (const r of opsRows) {
          ids.push(`${role}/${r.operator_short}`);
          labels.push(r.operator_short);
          parents.push(role);
          values.push(Math.max(1, Number(r[size_by]) || 0));
          colors.push(r.score);
        }
      }
      values[0] = total;  // root must total its children for branchvalues:"total"

      // Color range from operator scores only (the meaningful leaves).
      const scoreVals = rows.map((r) => r.score).filter((s) => s != null && !Number.isNaN(s));
      const cmin = scoreVals.length ? Math.min(...scoreVals) : 0;
      const cmax = scoreVals.length ? Math.max(...scoreVals) : 1;

      plot.purge(plotNode);
      plot.draw(plotNode, [{
        type: chart,
        ids, labels, parents, values,
        branchvalues: "total",
        marker: {
          colors,
          colorscale: plot.sequential,
          reversescale: comp.direction === "max",
          cmin, cmax,
          showscale: true,
          colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
        },
        hovertemplate: `%{label}<br>${pretty(size_by)} = %{value}<br>${state.metric} = %{color:.4f}<extra></extra>`,
      }], { margin: { l: 0, r: 0, t: 10, b: 0 } });

      // Companion table: role / operator / n_pipes / n_runs / score, sorted by role then score.
      const tblRows = rows.slice().sort((a, b) => {
        const ra = String(a.role ?? ""), rb = String(b.role ?? "");
        if (ra < rb) return -1; if (ra > rb) return 1;
        return (a.score ?? 0) - (b.score ?? 0);
      });
      const tbl = dom.table([
        { key: "role", label: "Role", render: (r) => dom.badge(pretty(r.role ?? "—"), "role") },
        { key: "operator_short", label: "Operator", render: (r) => dom.el("span", {}, r.operator_short ?? "—") },
        { key: "n_pipes", label: "pipelines", num: true },
        { key: "n_runs", label: "runs", num: true },
        { key: "score", label: state.metric, num: true, render: (r) => dom.fmt(r.score, 4) },
      ], tblRows);
      dom.mount(tableHost, dom.el("div", { class: "card" },
        dom.el("h3", {}, `${rows.length} operator${rows.length === 1 ? "" : "s"} across ${roleMap.size} role${roleMap.size === 1 ? "" : "s"}`),
        dom.el("div", { class: "sub" }, `score = mean ${state.metric} (${dirArrow}).`),
        tbl));
    };

    await refresh();
  },
};
