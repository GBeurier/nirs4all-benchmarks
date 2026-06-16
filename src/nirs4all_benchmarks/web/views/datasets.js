// Datasets — the catalog of data regimes benchmarked in this store.

const TIER = { public: "green", restricted: "amber", private: "red", anonymized: "role" };

export default {
  id: "datasets",
  title: "Datasets",
  subtitle: "The data regimes benchmarked in this store.",
  icon: "🗂",
  async render(ctx) {
    const { root, api, dom, state } = ctx;

    const head = dom.el("div", { class: "page-head" },
      dom.el("h1", {}, this.title), dom.el("p", {}, this.subtitle));

    const gridHost = dom.el("div", {});
    dom.mount(root, head, gridHost);

    const refresh = async () => {
      dom.mount(gridHost, dom.loading());
      const ds = await api.datasets();
      if (!ds.length) { dom.mount(gridHost, dom.empty("No datasets in this store yet.")); return; }

      const kv = (label, value) => [dom.el("dt", {}, label), dom.el("dd", {}, value)];
      const cards = ds.map((d) => {
        const title = d.name || dom.shortHash(d.dataset_fingerprint);
        const axis = `${dom.fmt(d.axis_min)} – ${dom.fmt(d.axis_max)} ${d.axis_unit ?? ""}`.trim();
        const card = dom.el("div", { class: "card", style: { cursor: "pointer" } },
          dom.el("h3", {}, title),
          dom.badge(d.privacy_level || "unknown", TIER[d.privacy_level] || "gray"),
          dom.el("dl", { class: "kv", style: { marginTop: "12px" } },
            kv("domain", d.domain ?? "—"),
            kv("modality", d.modality ?? "—"),
            kv("signal type", d.signal_type ?? "—"),
            kv("samples", dom.fmtInt(d.n_samples)),
            kv("features", dom.fmtInt(d.n_features)),
            kv("axis", axis),
            kv("task", d.task_type ?? "—"),
            kv("run conditions", dom.fmtInt(d.n_run_conditions)),
            kv("fingerprint", dom.shortHash(d.dataset_fingerprint, 16))));
        card.addEventListener("click", () => {
          state.dataset = d.dataset_fingerprint; state.save(); ctx.navigate("leaderboard");
        });
        return card;
      });
      dom.mount(gridHost, dom.el("div", { class: "grid cols-3" }, cards));
    };
    await refresh();
  },
};
