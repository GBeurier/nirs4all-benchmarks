// Landscape (3D) — the score surface over two facet dimensions, rendered in 3D.
// Same reactive pattern as leaderboard.js / playground.js: page-head + controls + refresh().
// Numeric facets feed Plotly directly; categorical facets are coded to integers with ticktext.

// Render a facet_key nicely: strip a "param:" prefix, underscores -> spaces.
function pretty(facetKey) {
  if (facetKey == null) return "";
  const k = String(facetKey);
  if (k.startsWith("param:")) return "param: " + k.slice(6).replace(/_/g, " ");
  return k.replace(/_/g, " ");
}

// A 3D scene turns every tick label into WebGL geometry: a hundred 64-character dataset
// fingerprints take seconds to draw and read as an illegible wall of text. Label a categorical
// axis only when it has few values, with short labels; hover always shows the full name.
const MAX_LABELLED_VALUES = 20;
const MAX_TICK_CHARS = 16;

// For one dimension: if every row value is a number, use it as-is.
// Otherwise map distinct strings -> integer codes; `names[code]` is the display name.
function codeDim(rows, key, displayName) {
  const values = rows.map((r) => r[key]);
  if (values.every((v) => typeof v === "number")) return { vals: values, names: null, categorical: false };
  const distinct = [...new Set(values.map((v) => String(v)))];
  const code = new Map(distinct.map((v, i) => [v, i]));
  return { vals: values.map((v) => code.get(String(v))), names: distinct.map(displayName), categorical: true };
}

// Axis for a dimension: numeric as-is; categorical with one short label per value, or none
// (title gives the count) when there are too many values to read.
function axisFor(title, d) {
  if (!d.categorical) return { title };
  const n = d.names.length;
  if (n > MAX_LABELLED_VALUES) return { title: `${title} (${n} values, hover for names)`, showticklabels: false };
  const short = (s) => (s.length > MAX_TICK_CHARS ? s.slice(0, MAX_TICK_CHARS - 1) + "…" : s);
  return { title, tickvals: d.names.map((_, i) => i), ticktext: d.names.map(short) };
}

// Full display value of row i on a dimension (for hover).
const shown = (d, i) => (d.categorical ? d.names[d.vals[i]] : String(d.vals[i]));

export default {
  id: "landscape",
  title: "Landscape (3D)",
  subtitle: "The score surface across up to three dimensions, in 3D.",
  icon: "⛰",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const facets = await api.facets();

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    if (facets.length < 2) {
      dom.mount(root, head, dom.el("div", { class: "card" },
        dom.empty("Need at least two facets in the store for a 3D landscape.")));
      return;
    }

    const keys = facets.map((f) => f.facet_key);
    const facetOptions = keys.map((k) => ({ value: k, label: pretty(k) }));

    // X default: param:n_components if present, else the first facet.
    const defaultX = keys.includes("param:n_components") ? "param:n_components" : keys[0];
    // Y default: a categorical axis if available, else the first key != x.
    const catPrefs = ["split_method", "preprocessing_op", "model_family"];
    const defaultY = catPrefs.find((k) => k !== defaultX && keys.includes(k))
      || keys.find((k) => k !== defaultX)
      || keys[0];

    const Z_METRIC = "__metric__";
    const bar = dom.controls([
      { id: "x", type: "select", label: "X axis", options: facetOptions, value: defaultX },
      { id: "y", type: "select", label: "Y axis", options: facetOptions, value: defaultY },
      { id: "z", type: "select", label: "Z axis",
        options: [{ value: Z_METRIC, label: "metric (score)" }, ...facetOptions], value: Z_METRIC },
      { id: "chart", type: "select", label: "Chart", options: ["scatter3d", "surface"], value: "scatter3d" },
    ], () => refresh());

    const note = dom.el("div", { class: "sub" },
      "Categorical axes are coded to integers; hover shows the full labels.");
    const card = dom.el("div", { class: "card" },
      dom.el("h3", { id: "ls-title" }, "Score landscape"),
      dom.el("div", { id: "ls-banner" }),
      dom.el("div", { class: "plot xtall", id: "ls-plot" }),
      note);
    dom.mount(root, head, bar.element, card);

    const refresh = async () => {
      const x = bar.get("x");
      const y = bar.get("y");
      const chart = bar.get("chart");
      const zKey = bar.get("z");
      const zReal = chart === "scatter3d" && zKey !== Z_METRIC && zKey !== x && zKey !== y;

      const plotNode = document.getElementById("ls-plot");
      const titleNode = document.getElementById("ls-title");
      const bannerNode = document.getElementById("ls-banner");
      dom.clear(bannerNode);
      const zLabel = zReal ? pretty(zKey) : state.metric;
      titleNode.textContent = chart === "scatter3d"
        ? `${pretty(x)} × ${pretty(y)} × ${zLabel}` + (zReal ? ` (color ${state.metric})` : "")
        : `${state.metric} over ${pretty(x)} × ${pretty(y)}`;

      if (x === y) {
        bannerNode.appendChild(dom.banner("Pick two different facets for the X and Y axes.", "warn"));
        plot.purge(plotNode);
        dom.mount(plotNode, dom.empty("Choose distinct dimensions."));
        return;
      }

      const dims = zReal ? [x, y, zKey] : [x, y];
      const pc = await api.parallel({
        dimensions: dims.join(","), metric: state.metric, scope: state.scope, dataset: state.dataset || undefined,
      });
      const rows = pc.rows || [];
      if (!rows.length) {
        plot.purge(plotNode);
        dom.mount(plotNode, dom.empty("No runs match these dimensions at this score level."));
        return;
      }

      const dir = pc.direction;
      const dirArrow = dir === "max" ? "↑ higher is better" : "↓ lower is better";
      const dsName = new Map((state._datasets || []).map((d) => [d.dataset_fingerprint, d.name || d.dataset_fingerprint]));
      const displayFor = (key) => (key === "dataset" ? (v) => dsName.get(v) || v : (v) => v);
      const dx = codeDim(rows, x, displayFor(x));
      const dy = codeDim(rows, y, displayFor(y));
      const xUniqCount = new Set(dx.vals).size;
      const yUniqCount = new Set(dy.vals).size;
      if (xUniqCount < 2 && yUniqCount < 2) {
        bannerNode.appendChild(dom.banner(
          "Both axes have a single distinct value — nothing to surface. Pick wider dimensions.", "info"));
      }


      if (chart === "scatter3d") {
        const color = rows.map((r) => r.metric);
        const dz = zReal ? codeDim(rows, zKey, displayFor(zKey)) : null;
        const z = zReal ? dz.vals : color;
        const zAxis = zReal ? axisFor(pretty(zKey), dz) : { title: state.metric };
        const labelFor = (r) => r.pipeline_label || (r.execution_hash ? String(r.execution_hash).slice(0, 10) : "run");
        plot.draw(plotNode, [{
          type: "scatter3d",
          mode: "markers",
          x: dx.vals,
          y: dy.vals,
          z,
          text: rows.map(labelFor),
          customdata: rows.map((r, i) => [shown(dx, i), shown(dy, i), zReal ? shown(dz, i) : "", r.metric]),
          marker: {
            size: 4,
            color,
            colorscale: "Viridis",
            reversescale: dir !== "max",
            showscale: true,
            colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
          },
          hovertemplate:
            "%{text}<br>" +
            `${pretty(x)} = %{customdata[0]}<br>` +
            `${pretty(y)} = %{customdata[1]}<br>` +
            (zReal ? `${pretty(zKey)} = %{customdata[2]}<br>` : "") +
            `${state.metric} = %{customdata[3]:.4f}<extra></extra>`,
        }], {
          margin: { l: 0, r: 0, t: 10, b: 0 },
          scene: {
            xaxis: axisFor(pretty(x), dx),
            yaxis: axisFor(pretty(y), dy),
            zaxis: zAxis,
          },
        });
      } else {
        // surface — aggregate rows by (coded x, coded y) -> mean metric on a grid.
        const xUnique = [...new Set(dx.vals)].sort((a, b) => a - b);
        const yUnique = [...new Set(dy.vals)].sort((a, b) => a - b);
        const xIdx = new Map(xUnique.map((v, i) => [v, i]));
        const yIdx = new Map(yUnique.map((v, i) => [v, i]));
        const sum = yUnique.map(() => xUnique.map(() => 0));
        const cnt = yUnique.map(() => xUnique.map(() => 0));
        rows.forEach((r, i) => {
          const xi = xIdx.get(dx.vals[i]);
          const yi = yIdx.get(dy.vals[i]);
          sum[yi][xi] += r.metric;
          cnt[yi][xi] += 1;
        });
        const zGrid = sum.map((srow, yi) => srow.map((s, xi) => (cnt[yi][xi] ? s / cnt[yi][xi] : null)));

        // sequential is green(low)->paper(high). For "max" higher is better, so reverse
        // to keep better=greener; for "min" keep green at the low (better) end.
        const colorscale = dir === "max"
          ? plot.sequential.map(([s, col]) => [1 - s, col]).reverse()
          : plot.sequential;

        plot.draw(plotNode, [{
          type: "surface",
          x: xUnique,
          y: yUnique,
          z: zGrid,
          colorscale,
          connectgaps: false,
          colorbar: { title: state.metric, titleside: "right", thickness: 14, len: 0.85 },
          hovertemplate:
            `${pretty(x)} = %{x}<br>` +
            `${pretty(y)} = %{y}<br>` +
            `${state.metric} = %{z:.4f}<extra></extra>`,
        }], {
          margin: { l: 0, r: 0, t: 10, b: 0 },
          scene: {
            xaxis: axisFor(pretty(x), dx),
            yaxis: axisFor(pretty(y), dy),
            zaxis: { title: state.metric },
          },
        });
      }

      note.textContent =
        `${rows.length} run(s) over ${pretty(x)} × ${pretty(y)}; color = ${state.metric} (${dirArrow}). `
        + `Categorical axes are coded to integers; ticks are labelled up to ${MAX_LABELLED_VALUES} values, hover shows full labels.`;
    };

    await refresh();
  },
};
