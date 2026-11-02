// Plotly merges the rows, columns and traces of a categorical axis whose labels are equal, so
// distinct items that share a display label (many pipelines carry one method label; several
// dataset views share one name) must get distinct ones. Only labels that collide are suffixed.
export function uniqueLabels(items, label, suffix) {
  const count = new Map();
  for (const it of items) count.set(label(it), (count.get(label(it)) || 0) + 1);
  return items.map((it) => (count.get(label(it)) > 1 ? `${label(it)} · ${suffix(it)}` : label(it)));
}
