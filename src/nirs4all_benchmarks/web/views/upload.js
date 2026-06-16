// Upload — the unified ingest endpoint. Hand the Arena a .n4a, a pipeline recipe,
// a dag-ml bundle, or an ArenaRunExport; the backend state machine decides what to do.

const STATUS_BADGE = {
  ingested: "green",
  registered: "green",
  rejected: "red",
  quarantined: "amber",
};

const DATASET_STATUS_BADGE = {
  already_run: "green",
  planned: "amber",
};

const LEVEL_BADGE = { error: "red", warn: "amber", warning: "amber", info: "gray" };

const PLACEHOLDER =
  "Paste a pipeline (JSON/YAML) or an ArenaRunExport manifest…\n"
  + '{ "arena_export_schema_version": 1, ... }';

// Humanise a facet/field key for display.
function pretty(facetKey) {
  if (!facetKey) return "—";
  const k = String(facetKey).replace(/^param:/, "");
  return k.replace(/[_:]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function cleanReportGrid(dom, cr) {
  // facts_written is a {table: count} dict — sum it (not an integer).
  const facts = Object.values(cr.facts_written || {}).reduce((a, b) => a + (Number(b) || 0), 0);
  return dom.el("dl", { class: "kv" },
    dom.el("dt", {}, "facts written"),
    dom.el("dd", {}, dom.fmtInt(facts)),
    dom.el("dt", {}, "residual rows"),
    dom.el("dd", {}, dom.fmtInt(cr.residual_rows ?? 0)),
    dom.el("dt", {}, "scores recomputed"),
    dom.el("dd", {}, dom.badge(cr.scores_recomputed ? "yes" : "no", cr.scores_recomputed ? "green" : "gray")));
}

function issuesList(dom, issues) {
  if (!issues?.length) return dom.el("div", { class: "muted", style: { fontSize: ".84rem" } }, "No issues reported.");
  return dom.el("div", { style: { display: "grid", gap: "8px" } },
    issues.map((it) => dom.el("div", { style: { display: "flex", alignItems: "baseline", gap: "8px" } },
      dom.badge((it.level || "info").toUpperCase(), LEVEL_BADGE[it.level] || "gray"),
      dom.el("div", { style: { fontSize: ".84rem" } },
        it.code ? dom.el("code", {}, it.code) : null,
        it.code ? " " : null,
        it.message || ""))));
}

export default {
  id: "upload",
  title: "Upload",
  subtitle: "Hand the Arena a .n4a, a pipeline (JSON/YAML), a dag-ml bundle, or an ArenaRunExport.",
  icon: "⬆",
  async render(ctx) {
    const { root, api, dom } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const info = dom.banner(
      "Four inputs are accepted: a .n4a bundle, a bare pipeline recipe (JSON/YAML), a dag-ml bundle, "
      + "or an ArenaRunExport manifest. Fitted artifacts are always stripped and never stored. "
      + "A bare pipeline is registered and PLANNED against the target datasets you pick "
      + "(already-run pairs are detected and shown). A results-bearing ArenaRunExport or dag-ml "
      + "bundle is INGESTED.",
      "info");

    // ── form inputs ──
    const fileInput = dom.el("input", { type: "file", accept: ".n4a,.json,.yaml,.yml" });
    const textarea = dom.el("textarea", {
      rows: 12, placeholder: PLACEHOLDER, spellcheck: "false",
      style: {
        width: "100%", fontFamily: "var(--mono)", fontSize: ".8rem", lineHeight: "1.5",
        padding: "10px 12px", borderRadius: "var(--radius-sm)",
        border: "1px solid var(--border-2)", background: "var(--surface)", color: "var(--text)",
        resize: "vertical",
      },
    });

    const datasets = await api.datasets();
    const datasetChecks = datasets.map((d) => {
      const cb = dom.el("input", { type: "checkbox", value: d.dataset_fingerprint });
      return { cb, label: d.name || dom.shortHash(d.dataset_fingerprint) };
    });
    const datasetList = datasetChecks.length
      ? dom.el("div", { style: { display: "grid", gap: "6px", maxHeight: "220px", overflowY: "auto" } },
          datasetChecks.map(({ cb, label }) =>
            dom.el("label", { class: "toggle", style: { paddingBottom: "0" } }, cb, label)))
      : dom.el("div", { class: "muted", style: { fontSize: ".84rem" } }, "No datasets registered yet.");

    const collectionInput = dom.el("input", { type: "text", value: "uploads", placeholder: "uploads" });
    const releaseToggle = dom.el("input", { type: "checkbox" });
    const uploadBtn = dom.el("button", { class: "btn primary" }, "Upload");

    const formCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "Submit"),
      dom.el("div", { class: "sub" }, "Choose a file or paste a recipe / manifest, pick the target datasets, then upload."),
      dom.el("div", { class: "control", style: { marginBottom: "12px" } },
        dom.el("label", {}, "File (.n4a / JSON / YAML)"), fileInput),
      textarea,
      dom.el("div", { class: "grid cols-2", style: { marginTop: "14px", alignItems: "start" } },
        dom.el("div", { class: "control" },
          dom.el("label", {}, "Target datasets"), datasetList),
        dom.el("div", { style: { display: "grid", gap: "12px" } },
          dom.el("div", { class: "control" },
            dom.el("label", {}, "Collection"), collectionInput),
          dom.el("label", { class: "toggle" }, releaseToggle, "Ingest as benchmark release"))),
      dom.el("div", { class: "controls", style: { marginTop: "16px", marginBottom: "0" } },
        dom.el("div", { class: "toolbar-right" }, uploadBtn)));

    const resultHost = dom.el("div", { style: { marginTop: "16px" } });

    dom.mount(root, head, info, formCard, resultHost);

    const renderResult = (res) => {
      const status = res.status || "unknown";

      const header = dom.el("div", { style: { display: "flex", alignItems: "center", flexWrap: "wrap", gap: "8px", marginBottom: "12px" } },
        dom.badge(status, STATUS_BADGE[status] || "gray"),
        res.kind ? dom.chip(res.kind) : null,
        res.message ? dom.el("span", { style: { fontSize: ".86rem", color: "var(--text-2)" } }, res.message) : null);

      const blocks = [header];

      if (res.pipeline_dag_hash) {
        blocks.push(dom.el("dl", { class: "kv" },
          dom.el("dt", {}, "pipeline"),
          dom.el("dd", {}, res.pipeline_label || "—"),
          dom.el("dt", {}, "pipeline dag"),
          dom.el("dd", {}, dom.el("a", { class: "hash", href: "#", onclick: (e) => { e.preventDefault(); ctx.navigate(`runs/${res.pipeline_dag_hash}`); } },
            dom.shortHash(res.pipeline_dag_hash, 16))),
          dom.el("dt", {}, "stripped artifacts"),
          dom.el("dd", {}, dom.fmtInt(res.stripped_artifacts ?? 0))));
      }

      if (res.datasets?.length) {
        blocks.push(dom.el("div", { style: { marginTop: "14px" } },
          dom.el("div", { class: "sub", style: { margin: "0 0 8px" } }, `Target datasets (${res.datasets.length})`),
          dom.table([
            { key: "token", label: "Token", render: (r) => dom.el("span", { class: "hash" }, dom.shortHash(r.token, 12)) },
            { key: "status", label: "Status", render: (r) => dom.badge(r.status || "—", DATASET_STATUS_BADGE[r.status] || "gray") },
            { key: "n_executions", label: "executions", num: true, render: (r) => dom.fmtInt(r.n_executions ?? 0) },
          ], res.datasets)));
      }

      if (res.ingestion) {
        const ing = res.ingestion;
        const cr = ing.clean_report || {};
        const allIssues = [...(ing.issues || []), ...(cr.issues || [])];
        blocks.push(dom.el("div", { style: { marginTop: "16px" } },
          dom.el("div", { class: "sub", style: { margin: "0 0 8px" } }, "Ingestion"),
          dom.el("dl", { class: "kv", style: { marginBottom: "12px" } },
            dom.el("dt", {}, "status"),
            dom.el("dd", {}, dom.badge(ing.status || "—", STATUS_BADGE[ing.status] || "gray")),
            dom.el("dt", {}, "validity"),
            dom.el("dd", {}, dom.badge(ing.validity_status || "—",
              ing.validity_status === "valid" ? "green" : ing.validity_status === "quarantined" ? "amber" : "gray"))),
          cleanReportGrid(dom, cr),
          dom.el("div", { style: { marginTop: "14px" } },
            dom.el("div", { class: "sub", style: { margin: "0 0 6px" } }, `Issues (${allIssues.length})`),
            issuesList(dom, allIssues))));
      }

      dom.mount(resultHost,
        dom.el("div", { class: "card" },
          dom.el("h3", {}, "Result"),
          ...blocks));
    };

    fileInput.addEventListener("change", () => {
      if (fileInput.files?.[0]) textarea.value = "";
    });

    uploadBtn.addEventListener("click", async () => {
      const fd = new FormData();
      if (fileInput.files?.[0]) {
        fd.append("file", fileInput.files[0]);
      } else if (textarea.value.trim()) {
        fd.append("text", textarea.value);
      } else {
        dom.mount(resultHost, dom.banner("Provide a file or paste a recipe/manifest.", "warn"));
        return;
      }
      const targets = datasetChecks.filter(({ cb }) => cb.checked).map(({ cb }) => cb.value);
      fd.append("target_datasets", targets.join(","));
      fd.append("collection", collectionInput.value.trim() || "uploads");
      fd.append("as_release", releaseToggle.checked ? "true" : "false");

      uploadBtn.disabled = true;
      dom.mount(resultHost, dom.loading("Uploading…"));
      try {
        const res = await api.upload(fd);
        renderResult(res);
      } catch (err) {
        dom.mount(resultHost, dom.banner(`Upload failed — ${err.message}`, "warn"));
      } finally {
        uploadBtn.disabled = false;
      }
    });
  },
};
