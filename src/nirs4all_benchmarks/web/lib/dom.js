// Tiny DOM toolkit for the no-build SPA. No framework — just helpers.

export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "html") node.innerHTML = v;
    else if (k === "text") node.textContent = v;
    else if (k === "style" && typeof v === "object") Object.assign(node.style, v);
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2).toLowerCase(), v);
    else if (k === "dataset" && typeof v === "object") Object.assign(node.dataset, v);
    else node.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    node.appendChild(typeof c === "string" || typeof c === "number" ? document.createTextNode(String(c)) : c);
  }
  return node;
}

export function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); return node; }
export function mount(node, ...children) { clear(node); for (const c of children.flat()) if (c) node.appendChild(c); return node; }

export function fmt(n, digits = 3) {
  if (n == null || Number.isNaN(n)) return "—";
  if (typeof n !== "number") return String(n);
  if (Math.abs(n) >= 1000 || (Math.abs(n) < 0.001 && n !== 0)) return n.toExponential(2);
  return n.toFixed(digits);
}
export function fmtInt(n) { return n == null ? "—" : Number(n).toLocaleString(); }
export function shortHash(h, n = 10) { return h ? String(h).slice(0, n) : "—"; }

export function badge(text, cls = "gray") { return el("span", { class: `badge ${cls}` }, text); }
export function chip(text) { return el("span", { class: "chip" }, text); }

export function loading(msg = "Loading…") {
  return el("div", { class: "loading" }, el("span", { class: "spinner" }), msg);
}
export function empty(msg = "No data yet.") { return el("div", { class: "empty" }, msg); }

export function banner(msg, kind = "info") { return el("div", { class: `banner ${kind}` }, msg); }

// Sortable table. columns: [{key,label,num?,render?(row,i),sortKey?(row)}]. onRow optional.
export function table(columns, rows, { onRow, sort } = {}) {
  let sortKey = sort?.key ?? null;
  let sortDir = sort?.dir ?? 1;
  const wrap = el("div", { class: "tbl-wrap" });
  const tbl = el("table", { class: "tbl" });
  const thead = el("thead");
  const tbody = el("tbody");

  function headerRow() {
    return el("tr", {}, columns.map((c) =>
      el("th", {
        class: c.num ? "num" : "",
        onclick: () => { if (sortKey === c.key) sortDir = -sortDir; else { sortKey = c.key; sortDir = c.num ? 1 : 1; } redraw(); },
      }, c.label + (sortKey === c.key ? (sortDir > 0 ? " ▲" : " ▼") : ""))
    ));
  }
  function redraw() {
    let data = rows.slice();
    if (sortKey) {
      const col = columns.find((c) => c.key === sortKey);
      const getv = col?.sortKey || ((r) => r[sortKey]);
      data.sort((a, b) => {
        const va = getv(a), vb = getv(b);
        if (va == null) return 1; if (vb == null) return -1;
        return (va < vb ? -1 : va > vb ? 1 : 0) * sortDir;
      });
    }
    clear(thead); thead.appendChild(headerRow());
    clear(tbody);
    for (const [i, r] of data.entries()) {
      const tr = el("tr", { class: onRow ? "clickable" : "" });
      if (onRow) tr.addEventListener("click", () => onRow(r));
      for (const c of columns) {
        const td = el("td", { class: c.num ? "num" : "" });
        const content = c.render ? c.render(r, i) : (c.num ? fmt(r[c.key]) : (r[c.key] ?? "—"));
        if (content instanceof Node) td.appendChild(content);
        else td.textContent = String(content);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }
  tbl.appendChild(thead); tbl.appendChild(tbody); wrap.appendChild(tbl);
  redraw();
  return wrap;
}

// A control bar. specs: array of {id,type,label,options?,value?,placeholder?,onInput,onClick}.
// type ∈ select | toggle | search | button | spacer. Returns {element, get(id), set(id,v)}.
export function controls(specs, onChange) {
  const values = {};
  const inputs = {};
  const bar = el("div", { class: "controls" });
  for (const s of specs) {
    if (s.type === "spacer") { bar.appendChild(el("div", { class: "toolbar-right" })); continue; }
    if (s.type === "button") {
      bar.appendChild(el("button", { class: `btn ${s.primary ? "primary" : ""}`, onclick: s.onClick }, s.label));
      continue;
    }
    if (s.type === "toggle") {
      values[s.id] = s.value ?? false;
      const cb = el("input", { type: "checkbox" });
      cb.checked = values[s.id];
      cb.addEventListener("change", () => { values[s.id] = cb.checked; onChange?.(s.id, cb.checked); });
      inputs[s.id] = cb;
      bar.appendChild(el("label", { class: "toggle" }, cb, s.label));
      continue;
    }
    const wrap = el("div", { class: "control" }, s.label ? el("label", {}, s.label) : null);
    if (s.type === "select") {
      values[s.id] = s.value ?? (s.options?.[0]?.value ?? s.options?.[0]);
      const sel = el("select");
      for (const o of s.options || []) {
        const val = o.value ?? o; const lbl = o.label ?? o;
        const opt = el("option", { value: val }, lbl);
        if (String(val) === String(values[s.id])) opt.selected = true;
        sel.appendChild(opt);
      }
      sel.addEventListener("change", () => { values[s.id] = sel.value; onChange?.(s.id, sel.value); });
      inputs[s.id] = sel; wrap.appendChild(sel);
    } else if (s.type === "search") {
      values[s.id] = s.value ?? "";
      const inp = el("input", { type: "search", placeholder: s.placeholder || "Search…" });
      inp.value = values[s.id];
      inp.addEventListener("input", () => { values[s.id] = inp.value; onChange?.(s.id, inp.value); });
      inputs[s.id] = inp; wrap.appendChild(inp);
    }
    bar.appendChild(wrap);
  }
  return {
    element: bar,
    get: (id) => values[id],
    set: (id, v) => { values[id] = v; if (inputs[id]) inputs[id].value = v; },
    options: (id, opts, value) => {
      const sel = inputs[id]; if (!sel) return;
      clear(sel);
      for (const o of opts) {
        const val = o.value ?? o; const lbl = o.label ?? o;
        const opt = el("option", { value: val }, lbl);
        if (value != null && String(val) === String(value)) opt.selected = true;
        sel.appendChild(opt);
      }
      values[id] = value ?? (opts[0]?.value ?? opts[0]);
    },
  };
}
