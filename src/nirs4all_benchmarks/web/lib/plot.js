// Plotly helpers + brand theme. Plotly is loaded globally via index.html (window.Plotly).

export const palette = ["#00704A", "#0d9488", "#06b6d4", "#4f46e5", "#d97706", "#E9362D", "#10b981", "#8b5cf6"];

// Diverging colorscale for residuals (red below / green above), brand-aligned.
export const diverging = [
  [0, "#E9362D"], [0.5, "#f5f0e6"], [1, "#00704A"],
];
// Sequential for matrices (lower = better => greener at low end). Caller flips per direction.
export const sequential = [
  [0, "#00704A"], [0.5, "#7cb89d"], [1, "#f3efe5"],
];

function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

export function baseLayout(overrides = {}) {
  const text = cssVar("--text-2", "#43554e");
  const grid = cssVar("--border", "#e2e8e5");
  return {
    font: { family: "Inter, system-ui, sans-serif", size: 12, color: text },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 56, r: 18, t: 28, b: 48 },
    xaxis: { gridcolor: grid, zerolinecolor: grid, automargin: true },
    yaxis: { gridcolor: grid, zerolinecolor: grid, automargin: true },
    colorway: palette,
    hovermode: "closest",
    legend: { orientation: "h", y: -0.2 },
    ...overrides,
  };
}

const CONFIG = { displayModeBar: false, responsive: true, displaylogo: false,
  modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d"] };

export function draw(node, traces, layout = {}) {
  if (!window.Plotly) { node.textContent = "Plotly failed to load."; return; }
  window.Plotly.react(node, traces, baseLayout(layout), CONFIG);
}

export function purge(node) { if (window.Plotly && node) window.Plotly.purge(node); }
