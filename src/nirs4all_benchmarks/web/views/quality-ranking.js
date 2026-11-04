// Optional React island. Loading failures stay local to this page.
export default {
  id: "quality-ranking",
  title: "Quality ranking",
  subtitle: "Observed pipeline and component rankings from matched evidence.",
  icon: "≋",
  async render({ root, api, dom, state, signal }) {
    const host = dom.el("div", { class: "quality-ranking-host" });
    dom.mount(root, host);
    const local = signal || new AbortController().signal;
    try {
      const { mountQualityRanking } = await import("../generated/quality-ranking.js");
      if (local.aborted || !host.isConnected) return;
      return mountQualityRanking(host, { api, state: { metric: state.metric, scope: state.scope, dataset: state.dataset }, signal: local });
    } catch {
      if (!local.aborted && host.isConnected) dom.mount(host,
        dom.el("h1", {}, "Quality ranking"),
        dom.banner("Quality ranking is temporarily unavailable. Other pages remain available.", "warn"));
    }
  },
};
