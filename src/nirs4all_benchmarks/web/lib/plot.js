// Plotly helpers + brand theme. Plotly (full build) is loaded globally (window.Plotly).
// One place for professional, consistent chart styling: muted grid, readable fonts,
// small markers, clean hover, brand colorway. Views pass only data + axis titles.

export const palette = ["#0d9488", "#06b6d4", "#10b981", "#4f46e5", "#d97706", "#E9362D", "#8b5cf6", "#0891b2"];

// Diverging colorscale for residuals (red below / green above).
export const diverging = [[0, "#E9362D"], [0.5, "#f1ede3"], [1, "#0d9488"]];
// Sequential: green (low) -> warm paper (high). Flip per metric direction at the call site.
export const sequential = [[0, "#0d9488"], [0.5, "#7cc4ba"], [1, "#f3efe5"]];

function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

// A compact, readable default marker. Pass extras to override (e.g. color array).
export function marker(extra = {}) {
  return { size: 6, opacity: 0.72, line: { width: 0.5, color: "rgba(255,255,255,0.85)" }, ...extra };
}

export function baseLayout(overrides = {}) {
  const text = cssVar("--text-2", "#475569");
  const grid = cssVar("--border", "#e2e8f0");
  const axis = {
    gridcolor: grid, zerolinecolor: "rgba(15,23,42,.12)", linecolor: grid,
    tickfont: { size: 11 }, automargin: true, title: { font: { size: 12 } },
  };
  return {
    font: { family: "Inter, system-ui, sans-serif", size: 12, color: text },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 56, r: 18, t: 18, b: 48 },
    xaxis: { ...axis },
    yaxis: { ...axis },
    colorway: palette,
    hovermode: "closest",
    hoverlabel: { bgcolor: "#0f1f1a", bordercolor: "#0f1f1a", font: { color: "#fff", size: 12, family: "Inter, sans-serif" } },
    legend: { orientation: "h", y: -0.22, font: { size: 11 }, bgcolor: "rgba(0,0,0,0)" },
    ...overrides,
  };
}

const CONFIG = {
  displayModeBar: "hover", responsive: true, displaylogo: false,
  modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"],
  toImageButtonOptions: { format: "png", scale: 2 },
};

export function draw(node, traces, layout = {}) {
  if (!window.Plotly) { node.textContent = "Plotly failed to load."; return; }
  window.Plotly.react(node, traces, baseLayout(layout), CONFIG);
}

export function purge(node) { if (window.Plotly && node) window.Plotly.purge(node); }
