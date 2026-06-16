// The Arena SPA — router, shared state, nav, view registry. No build step.

import { api } from "./lib/api.js";
import * as dom from "./lib/dom.js";
import * as plot from "./lib/plot.js";

import overview from "./views/overview.js";
import leaderboard from "./views/leaderboard.js";
import matrix from "./views/matrix.js";
import playground from "./views/playground.js";
import paramEffect from "./views/param-effect.js";
import operatorEffect from "./views/operator-effect.js";
import parallel from "./views/parallel.js";
import robustness from "./views/robustness.js";
import statistics from "./views/statistics.js";
import landscape from "./views/landscape.js";
import composition from "./views/composition.js";
import network from "./views/network.js";
import runs from "./views/runs.js";
import compare from "./views/compare.js";
import datasets from "./views/datasets.js";
import pipelines from "./views/pipelines.js";
import planned from "./views/planned.js";
import runDetail from "./views/run-detail.js";
import upload from "./views/upload.js";

const VIEWS = [
  overview, leaderboard, matrix, playground, paramEffect, operatorEffect, parallel,
  robustness, statistics, landscape, composition, network,
  runs, compare, datasets, pipelines, planned, upload, runDetail,
];
const BY_ID = Object.fromEntries(VIEWS.map((v) => [v.id, v]));

// Shared, persisted cross-view state (metric / scope / dataset filter).
const STATE_KEY = "arena.state.v1";
const state = {
  metric: "rmse", scope: "cv", dataset: "", _overview: null,
  ...(JSON.parse(localStorage.getItem(STATE_KEY) || "{}")),
  save() { localStorage.setItem(STATE_KEY, JSON.stringify({ metric: this.metric, scope: this.scope, dataset: this.dataset })); },
};

const GROUPS = [
  { name: "Explore", ids: ["overview", "leaderboard", "matrix", "playground"] },
  { name: "Effects", ids: ["param-effect", "operator-effect", "parallel", "robustness", "statistics"] },
  { name: "Graphs", ids: ["landscape", "composition", "network"] },
  { name: "Runs", ids: ["runs", "compare"] },
  { name: "Catalog", ids: ["datasets", "pipelines", "planned"] },
  { name: "Contribute", ids: ["upload"] },
];

function renderNav(activeId) {
  const nav = document.getElementById("side-nav");
  dom.clear(nav);
  for (const g of GROUPS) {
    nav.appendChild(dom.el("div", { class: "group" }, g.name));
    for (const id of g.ids) {
      const v = BY_ID[id];
      if (!v) continue;
      nav.appendChild(dom.el("a", {
        class: `item ${id === activeId ? "active" : ""}`, href: `#/${id}`,
      }, dom.el("span", { class: "ic" }, v.icon || "•"), v.title));
    }
  }
}

async function refreshHeader() {
  try {
    const ov = await api.overview();
    state._overview = ov;
    document.getElementById("header-pill").textContent =
      `${ov.valid_executions}/${ov.executions} runs · ${ov.pipelines} pipelines · ${ov.datasets} datasets`;
  } catch {
    document.getElementById("header-pill").textContent = "store empty";
  }
}

async function renderChrome() {
  let version = "";
  try { version = (await api.healthz()).version || ""; } catch { /* offline */ }
  const verChip = document.getElementById("ver-chip");
  if (verChip && version) verChip.textContent = "v" + version;

  const footer = document.getElementById("site-footer");
  if (footer) {
    footer.innerHTML = "";
    const link = (href, text) => dom.el("a", { href, target: "_blank", rel: "noopener" }, text);
    footer.appendChild(dom.el("div", { class: "footer-inner" },
      dom.el("div", { class: "footer-brand" },
        dom.el("img", { src: "/brand/icon.svg", alt: "", class: "footer-mark" }),
        dom.el("div", {},
          dom.el("strong", {}, "nirs4all-benchmarks"),
          dom.el("span", { class: "footer-ver" }, version ? ` · v${version}` : ""),
          dom.el("span", { class: "proto-badge sm" }, "prototype"),
          dom.el("div", { class: "footer-tag" }, "the Arena — reproducible, scored, weights-free NIRS pipeline benchmarks."))),
      dom.el("div", { class: "footer-links" },
        link("https://nirs4all.org", "nirs4all.org"),
        link("https://github.com/GBeurier/nirs4all-benchmarks", "GitHub"),
        link("https://github.com/GBeurier/nirs4all-benchmarks/blob/main/CHANGELOG.md", "Changelog"),
        dom.el("span", { class: "footer-muted" }, "code CeCILL-2.1 / AGPL-3.0 · results CC-BY-4.0"))));
  }
}

function parseHash() {
  const h = (location.hash || "#/overview").replace(/^#\/?/, "");
  const [id, ...rest] = h.split("/");
  return { id: id || "overview", param: rest.join("/") };
}

async function route() {
  const { id, param } = parseHash();
  const view = BY_ID[id] || BY_ID.overview;
  renderNav(view.id);
  const root = document.getElementById("view-root");
  dom.mount(root, dom.loading());
  const ctx = {
    root, api, dom, plot, state, param,
    navigate: (to) => { location.hash = `#/${to}`; },
  };
  try {
    await view.render(ctx);
  } catch (err) {
    dom.mount(root, dom.banner(`Failed to render: ${err.message}`, "warn"));
    console.error(err);
  }
}

window.addEventListener("hashchange", route);
window.addEventListener("DOMContentLoaded", async () => {
  if (!location.hash) location.hash = "#/overview";
  await renderChrome();
  await refreshHeader();
  await route();
});
// expose for views that change shared state and want the header to update
window.__arena = { refreshHeader };
