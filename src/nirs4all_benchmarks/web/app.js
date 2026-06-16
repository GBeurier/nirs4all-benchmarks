// The Arena SPA — router, shared state, nav, view registry. No build step.

import { api } from "./lib/api.js";
import * as dom from "./lib/dom.js";
import * as plot from "./lib/plot.js";

import overview from "./views/overview.js";
import leaderboard from "./views/leaderboard.js";
import matrix from "./views/matrix.js";
import paramEffect from "./views/param-effect.js";
import operatorEffect from "./views/operator-effect.js";
import robustness from "./views/robustness.js";
import runs from "./views/runs.js";
import compare from "./views/compare.js";
import datasets from "./views/datasets.js";
import pipelines from "./views/pipelines.js";
import runDetail from "./views/run-detail.js";
import upload from "./views/upload.js";

const VIEWS = [
  overview, leaderboard, matrix, paramEffect, operatorEffect, robustness,
  runs, compare, datasets, pipelines, upload, runDetail,
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
  { name: "Explore", ids: ["overview", "leaderboard", "matrix"] },
  { name: "Effects", ids: ["param-effect", "operator-effect", "robustness"] },
  { name: "Runs", ids: ["runs", "compare"] },
  { name: "Catalog", ids: ["datasets", "pipelines"] },
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
  await refreshHeader();
  await route();
});
// expose for views that change shared state and want the header to update
window.__arena = { refreshHeader };
