// Upload — ingest a weights-free ArenaRunExport manifest into the store.
// No data is fetched up front; everything is driven by the Ingest action.

const STATUS_BADGE = {
  committed: "green",
  already_ingested: "green",
  quarantined: "amber",
  rejected: "red",
};

const LEVEL_BADGE = { error: "red", warn: "amber", warning: "amber", info: "gray" };

const PLACEHOLDER = '{ "arena_export_schema_version": 1, ... }';

function countsList(dom, obj) {
  const entries = Object.entries(obj || {});
  if (!entries.length) return dom.el("span", { class: "muted" }, "—");
  return dom.el("div", { style: { display: "flex", flexWrap: "wrap", gap: "6px" } },
    entries.map(([k, v]) => dom.chip(`${k}: ${v}`)));
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
  title: "Upload a run",
  subtitle: "Ingest a weights-free ArenaRunExport bundle.",
  icon: "⬆",
  async render(ctx) {
    const { root, dom, api } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const info = dom.banner(
      "The Arena ingests a weights-free, content-addressed ArenaRunExport manifest (JSON): "
      + "run conditions, scores, and residual summaries only. Fitted models are never stored or "
      + "reconstructed — uploads carry no weights.",
      "info");

    // Form inputs.
    const fileInput = dom.el("input", { type: "file", accept: ".json" });
    const textarea = dom.el("textarea", {
      rows: 14, placeholder: PLACEHOLDER, spellcheck: "false",
      style: {
        width: "100%", fontFamily: "var(--mono)", fontSize: ".8rem", lineHeight: "1.5",
        padding: "10px 12px", borderRadius: "var(--radius-sm)",
        border: "1px solid var(--border-2)", background: "var(--surface)", color: "var(--text)",
        resize: "vertical",
      },
    });
    fileInput.addEventListener("change", () => {
      const file = fileInput.files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => { textarea.value = String(reader.result || ""); };
      reader.readAsText(file);
    });

    const collectionInput = dom.el("input", {
      type: "text", value: "uploads", placeholder: "uploads",
    });
    const releaseToggle = dom.el("input", { type: "checkbox" });
    const ingestBtn = dom.el("button", { class: "btn primary" }, "Ingest");

    const formCard = dom.el("div", { class: "card" },
      dom.el("h3", {}, "ArenaRunExport manifest"),
      dom.el("div", { class: "sub" }, "Choose a .json file or paste the manifest below."),
      dom.el("div", { class: "control", style: { marginBottom: "12px" } },
        dom.el("label", {}, "Manifest file"), fileInput),
      textarea,
      dom.el("div", { class: "controls", style: { marginTop: "14px", marginBottom: "0" } },
        dom.el("div", { class: "control" },
          dom.el("label", {}, "Collection"), collectionInput),
        dom.el("label", { class: "toggle" },
          releaseToggle, "Ingest as benchmark release (quarantine on leakage)"),
        dom.el("div", { class: "toolbar-right" }, ingestBtn)));

    const resultHost = dom.el("div", { style: { marginTop: "16px" } });

    dom.mount(root, head, info, formCard, resultHost);

    const renderResult = (res) => {
      const status = res.status || "unknown";
      const cr = res.clean_report || {};

      const overview = dom.el("dl", { class: "kv" },
        dom.el("dt", {}, "status"),
        dom.el("dd", {}, dom.badge(status, STATUS_BADGE[status] || "gray")),
        dom.el("dt", {}, "validity"),
        dom.el("dd", {}, dom.badge(res.validity_status || "—",
          res.validity_status === "valid" ? "green" : res.validity_status === "quarantined" ? "amber" : "gray")),
        dom.el("dt", {}, "run condition"),
        dom.el("dd", {}, dom.shortHash(res.run_condition_hash, 16)),
        dom.el("dt", {}, "execution"),
        dom.el("dd", {}, dom.shortHash(res.execution_hash, 16)),
        dom.el("dt", {}, "export hash"),
        dom.el("dd", {}, dom.shortHash(res.arena_export_hash, 16)));

      const reportGrid = dom.el("dl", { class: "kv", style: { marginTop: "4px" } },
        dom.el("dt", {}, "new dimensions"),
        dom.el("dd", {}, countsList(dom, cr.new_dimensions)),
        dom.el("dt", {}, "facts written"),
        dom.el("dd", {}, countsList(dom, cr.facts_written)),
        dom.el("dt", {}, "residual rows"),
        dom.el("dd", {}, dom.fmtInt(cr.residual_rows ?? 0)
          + (cr.residuals_truncated ? " (truncated)" : "")),
        dom.el("dt", {}, "residual key"),
        dom.el("dd", {}, cr.residual_key || "—"),
        dom.el("dt", {}, "scores recomputed"),
        dom.el("dd", {}, dom.badge(cr.scores_recomputed ? "yes" : "no",
          cr.scores_recomputed ? "green" : "gray")));

      const allIssues = [...(res.issues || []), ...(cr.issues || [])];
      const navigable = res.execution_hash && (status === "committed" || status === "already_ingested");

      dom.mount(resultHost,
        dom.el("div", { class: "card" },
          dom.el("h3", {}, "Ingestion result"),
          dom.el("div", { class: "sub" }, cr.source || "ArenaRunExport"),
          dom.el("div", { class: "grid cols-2" }, overview, reportGrid),
          cr.notes?.length
            ? dom.el("div", { style: { marginTop: "14px" } },
                dom.el("div", { class: "sub", style: { margin: "0 0 6px" } }, "Notes"),
                dom.el("ul", { style: { margin: "0", paddingLeft: "18px", fontSize: ".84rem", color: "var(--text-2)" } },
                  cr.notes.map((n) => dom.el("li", {}, n))))
            : null,
          dom.el("div", { style: { marginTop: "16px" } },
            dom.el("div", { class: "sub", style: { margin: "0 0 8px" } }, `Issues (${allIssues.length})`),
            issuesList(dom, allIssues)),
          navigable
            ? dom.el("div", { class: "controls", style: { marginTop: "16px", marginBottom: "0" } },
                dom.el("div", { class: "toolbar-right" },
                  dom.el("button", { class: "btn", onclick: () => ctx.navigate(`run/${res.execution_hash}`) },
                    "Open run detail")))
            : null));
    };

    ingestBtn.addEventListener("click", async () => {
      const raw = textarea.value.trim();
      if (!raw) { dom.mount(resultHost, dom.banner("Paste or choose a manifest before ingesting.", "warn")); return; }

      let manifest;
      try {
        manifest = JSON.parse(raw);
      } catch (err) {
        dom.mount(resultHost, dom.banner(`Invalid JSON: ${err.message}`, "warn"));
        return;
      }

      ingestBtn.disabled = true;
      dom.mount(resultHost, dom.loading("Ingesting…"));
      try {
        const res = await api.ingest(manifest, {
          collection: collectionInput.value.trim() || "uploads",
          as_release: releaseToggle.checked,
        });
        renderResult(res);
      } catch (err) {
        dom.mount(resultHost, dom.banner(`Ingestion failed — ${err.message}`, "warn"));
      } finally {
        ingestBtn.disabled = false;
      }
    });
  },
};
