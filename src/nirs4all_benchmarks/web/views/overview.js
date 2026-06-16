// Overview — a run meta-analysis dashboard: headline findings + store health.
// Uses rmse @ cv as the headline lens; deeper slicing lives in the analysis views.

const METRIC = "rmse";
const SCOPE = "cv";

function bestOf(rows, direction) {
  if (!rows || !rows.length) return null;
  return rows.slice().sort((a, b) =>
    direction === "max" ? (b.value - a.value) : (a.value - b.value))[0];
}

export default {
  id: "overview",
  title: "Overview",
  subtitle: "The benchmark space at a glance.",
  icon: "▦",
  async render(ctx) {
    const { root, api, dom, plot, state } = ctx;
    const ov = await api.overview();
    state._overview = ov;

    const head = dom.el("div", { class: "page-head" },
      dom.el("div", { class: "eyebrow" }, dom.el("span", { class: "pulse-dot" }), "NIRS pipeline meta-analysis"),
      dom.el("h1", {}, "The ", dom.el("span", { class: "gradient-text" }, "Arena")),
      dom.el("p", {}, "Reproducible, scored, weights-free benchmarks over the "
        + "model × pipeline × split × cv × augmentation × dataset space — perfectly indexed, deeply explorable."));

    const findingsHost = dom.el("div", { class: "grid cols-3", id: "ov-findings" }, dom.loading("Mining findings…"));

    const stat = (k, v, sub) => dom.el("div", { class: "stat" },
      dom.el("div", { class: "k" }, k),
      dom.el("div", { class: "v" }, String(v), sub ? dom.el("small", {}, " " + sub) : null));
    const stats = dom.el("div", { class: "grid cols-4", style: { marginTop: "16px" } },
      stat("Datasets", ov.datasets), stat("Pipelines", ov.pipelines),
      stat("Run conditions", ov.run_conditions), stat("Executions", ov.valid_executions, `/ ${ov.executions}`),
      stat("Operators", ov.operators), stat("Residual sets", ov.residual_sets),
      stat("Score sets", ov.score_sets), stat("Quarantined", ov.quarantined_executions));

    dom.mount(root, head,
      dom.el("div", { class: "eyebrow", style: { margin: "4px 0 10px" } }, `Key findings · ${METRIC} (cv)`),
      findingsHost, stats,
      dom.el("div", { class: "card", style: { marginTop: "16px" } },
        dom.el("h3", {}, "Best pipeline per dataset"),
        dom.el("div", { class: "sub" }, "Lowest mean RMSE (cv) on each dataset — click to focus."),
        dom.el("div", { id: "ov-best" }, dom.loading())));

    // ── findings (computed from the analysis endpoints) ──
    const card = (icon, label, value, detail, to) => {
      const c = dom.el("div", { class: "finding", style: { cursor: to ? "pointer" : "default" } },
        dom.el("div", { class: "finding-icon" }, icon),
        dom.el("div", {},
          dom.el("div", { class: "finding-label" }, label),
          dom.el("div", { class: "finding-value" }, value),
          dom.el("div", { class: "finding-detail" }, detail)));
      if (to) c.addEventListener("click", () => ctx.navigate(to));
      return c;
    };
    const fmt = (x) => dom.fmt(x, 4);
    const pivot = (group_by) => api.pivot({ group_by, metric: METRIC, scope: SCOPE }).then((p) => p).catch(() => null);

    try {
      const [lb, byModel, byPp, byAug, bySplit, rob] = await Promise.all([
        api.leaderboard({ metric: METRIC, scope: SCOPE, limit: 1 }).catch(() => null),
        pivot("model_family"), pivot("preprocessing_op"), pivot("has_augmentation"),
        pivot("split_method"), api.robustness({ metric: METRIC, scope: SCOPE }).catch(() => null),
      ]);
      const findings = [];

      const top = lb?.rows?.[0];
      if (top) findings.push(card("🏆", "Top pipeline", fmt(top.mean), top.pipeline_label || top.pipeline_dag_hash.slice(0, 12), "leaderboard"));

      const bm = bestOf(byModel?.rows, "min");
      if (bm) findings.push(card("🧠", "Best model family", bm.group_value, `${fmt(bm.value)} mean rmse`, "playground"));

      const bp = bestOf(byPp?.rows, "min");
      if (bp) findings.push(card("🧪", "Best preprocessing", bp.group_value, `${fmt(bp.value)} mean rmse`, "operator-effect"));

      if (byAug?.rows?.length) {
        const yes = byAug.rows.find((r) => r.group_value === "yes");
        const no = byAug.rows.find((r) => r.group_value === "no");
        if (yes && no) {
          const lift = no.value - yes.value; // >0 ⇒ augmentation lowers rmse (helps)
          findings.push(card("✨", "Augmentation effect",
            (lift >= 0 ? "−" : "+") + fmt(Math.abs(lift)),
            lift >= 0 ? "augmentation improves rmse" : "augmentation hurts rmse", "playground"));
        }
      }

      if (bySplit?.rows?.length > 1) {
        const lo = bestOf(bySplit.rows, "min"), hi = bestOf(bySplit.rows, "max");
        findings.push(card("✂️", "Split sensitivity", fmt(hi.value - lo.value),
          `${lo.group_value} best · ${hi.group_value} worst`, "playground"));
      }

      if (rob?.length) {
        const stable = rob.slice().sort((a, b) => a.stdev - b.stdev)[0];
        findings.push(card("🛡", "Most robust", fmt(stable.stdev),
          `${stable.label} (std across runs)`, "robustness"));
      }

      dom.mount(findingsHost, ...(findings.length ? findings : [dom.empty("Seed some runs to see findings.")]));
    } catch (err) {
      dom.mount(findingsHost, dom.banner(`Could not compute findings: ${err.message}`, "warn"));
    }

    // ── best per dataset ──
    const datasets = await api.datasets();
    const bestRows = [];
    for (const d of datasets) {
      const lb = await api.leaderboard({ metric: METRIC, scope: SCOPE, dataset: d.dataset_fingerprint, limit: 1 });
      const top = lb.rows?.[0];
      bestRows.push({ dataset: d.name || d.dataset_fingerprint.slice(0, 10), df: d.dataset_fingerprint,
        pipeline: top?.pipeline_label || "—", rmse: top?.mean, n: d.n_run_conditions });
    }
    dom.mount(document.getElementById("ov-best"), dom.table([
      { key: "dataset", label: "Dataset" },
      { key: "pipeline", label: "Best pipeline (cv rmse)" },
      { key: "rmse", label: "rmse", num: true, render: (r) => dom.fmt(r.rmse, 4) },
      { key: "n", label: "runs", num: true },
    ], bestRows, { onRow: (r) => { state.dataset = r.df; state.save(); ctx.navigate("leaderboard"); } }));
  },
};
