// API client for the Arena REST service. All methods return parsed JSON.

async function request(method, path, { params, body } = {}) {
  const url = new URL(path, window.location.origin);
  for (const [k, v] of Object.entries(params || {})) {
    if (v != null && v !== "") url.searchParams.set(k, v);
  }
  const opts = { method, headers: {} };
  if (body !== undefined) { opts.headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(body); }
  const res = await fetch(url, opts);
  if (!res.ok) {
    let detail;
    try { detail = (await res.json()).detail; } catch { detail = res.statusText; }
    throw new Error(`${res.status}: ${detail}`);
  }
  return res.json();
}

export const api = {
  get: (path, params) => request("GET", path, { params }),
  overview: () => request("GET", "/api/overview"),
  collections: () => request("GET", "/api/collections"),
  datasets: () => request("GET", "/api/datasets"),
  pipelines: () => request("GET", "/api/pipelines"),
  operators: () => request("GET", "/api/operators"),
  parameters: () => request("GET", "/api/parameters"),
  leaderboard: (params) => request("GET", "/api/leaderboard", { params }),
  matrix: (params) => request("GET", "/api/matrix", { params }),
  runs: (params) => request("GET", "/api/runs", { params }),
  operatorEffect: (params) => request("GET", "/api/operator-effect", { params }),
  parameterEffect: (params) => request("GET", "/api/parameter-effect", { params }),
  robustness: (params) => request("GET", "/api/robustness", { params }),
  runDetail: (hash) => request("GET", `/api/run/${hash}`),
  residuals: (hash, partition) => request("GET", `/api/run/${hash}/residuals`, { params: { partition } }),
  compare: (a, b, partition) => request("GET", "/api/compare", { params: { a, b, partition } }),
  ingest: (manifest, params) => request("POST", "/api/ingest", { params, body: manifest }),
  // ── faceting / pivot / playground / upload ──
  facets: () => request("GET", "/api/facets"),
  facetValues: (key) => request("GET", "/api/facet-values", { params: { key } }),
  pivot: (params) => request("GET", "/api/pivot", { params }),
  parallel: (params) => request("GET", "/api/parallel", { params }),
  planned: () => request("GET", "/api/planned"),
  graph: (params) => request("GET", "/api/graph", { params }),
  composition: (params) => request("GET", "/api/composition", { params }),
  // multipart upload (file and/or text + target datasets); returns the state-machine result
  upload: async (formData) => {
    const res = await fetch(new URL("/api/upload", window.location.origin), { method: "POST", body: formData });
    if (!res.ok) {
      let detail;
      try { detail = (await res.json()).detail; } catch { detail = res.statusText; }
      throw new Error(`${res.status}: ${detail}`);
    }
    return res.json();
  },
};
