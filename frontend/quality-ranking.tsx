import React, { Component, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { QualityRankingPanel, ComponentRankingView, RelativeGainView } from "nirs4all-ui/ranking";
import type { QualityRanking, QualityRelativeGain, RankingPlotProps } from "nirs4all-ui/ranking";
import { validateQualityRanking, validateRelativeGain } from "./artifact.js";
import css from "./quality-ranking.css";

const dimensions = [
  ["pipeline", "Pipelines"], ["model", "Models"], ["preprocessing_sequence", "Preprocessing sequences"],
  ["preprocessing_operator", "Preprocessing operators"], ["feature_selection", "Feature selection"],
  ["augmentation", "Augmentation"], ["parameter", "Parameter values"], ["split", "Split strategies"],
];
const methods = [["average_rank", "Average rank"], ["elo", "Elo"], ["bradley_terry", "Bradley–Terry"], ["trueskill", "TrueSkill 1"], ["openskill", "OpenSkill"]];
function Plot({ data, layout, config }: RankingPlotProps) {
  const host = useRef<HTMLDivElement>(null);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    const node = host.current;
    const height = Math.max(360, 70 + 30 * Math.max(0, ...data.map(trace => Array.isArray(trace.y) ? trace.y.length : 0)));
    const plotly = window.Plotly;
    if (!node || !plotly) { setFailed(true); return; }
    let active = true;
    Promise.resolve().then(() => {
      if (active) return plotly.react(node, data, { ...layout, autosize: true, margin: { l: 180, r: 24, t: 20, b: 55 }, height }, { ...config, responsive: true, displaylogo: false });
    }).catch(() => { if (active) setFailed(true); });
    const resize = () => { if (active && node.isConnected) Promise.resolve(plotly.Plots?.resize(node)).catch(() => {}); };
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(resize);
    observer?.observe(node);
    window.addEventListener("resize", resize);
    return () => { active = false; observer?.disconnect(); window.removeEventListener("resize", resize); plotly.purge(node); };
  }, [data, layout, config]);
  return <><div className="ranking-plot-scroll"><div ref={host} className="ranking-plot" aria-hidden="true" /></div>{failed ? <p>Chart unavailable. The complete results are in the table below.</p> : null}</>;
}
class Boundary extends Component<{ children: React.ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  render() { return this.state.failed ? <p role="alert">The ranking view could not be displayed. Choose another page or reload to try again.</p> : this.props.children; }
}
function RankingApp({ api, state, signal }: any) {
  const [analysis, setAnalysis] = useState("ranking");
  const [baseline, setBaseline] = useState("");
  const [candidate, setCandidate] = useState("");
  const [variances, setVariances] = useState("");
  const [universe, setUniverse] = useState<any[]>([]);
  const [gain, setGain] = useState<QualityRelativeGain | null>(null);
  const [dimension, setDimension] = useState("pipeline");
  const [method, setMethod] = useState("bradley_terry");
  const [comparisonMode, setComparisonMode] = useState("observed");
  const [partition, setPartition] = useState(state.scope === "test" ? "test" : "validation");
  const [direction, setDirection] = useState("");
  const [operator, setOperator] = useState("");
  const [parameter, setParameter] = useState("");
  const [task, setTask] = useState("");
  const [collection, setCollection] = useState("");
  const [protocol, setProtocol] = useState("");
  const [tolerance, setTolerance] = useState("0");
  const [options, setOptions] = useState("{}");
  const [ranking, setRanking] = useState<QualityRanking | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [metadata, setMetadata] = useState<any>({ operators: [], parameters: [], requests: [], mode: "service" });
  const generation = useRef(0);
  useEffect(() => {
    let active = true;
    Promise.allSettled([api.operators(), api.parameters(), api.qualityRankingRequests(), api.qualityRelativeGainRequests()]).then(([ops, params, index, gainIndex]) => {
      if (!active || signal.aborted) return;
      setMetadata({ operators: ops.status === "fulfilled" && Array.isArray(ops.value) ? ops.value : [], parameters: params.status === "fulfilled" && Array.isArray(params.value) ? params.value : [], ...(index.status === "fulfilled" ? index.value : {}), gainRequests: gainIndex.status === "fulfilled" && Array.isArray(gainIndex.value.requests) ? gainIndex.value.requests : [] });
    });
    return () => { active = false; generation.current++; };
  }, []);
  async function refresh() {
    const ticket = ++generation.current;
    setError(""); setRanking(null); setGain(null);
    try {
      const parsed = JSON.parse(options);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || Object.values(parsed).some(v => typeof v !== "number" || !Number.isFinite(v))) throw new Error("Method options must be an object containing finite numeric values.");
      const tie = Number(tolerance);
      if (!Number.isFinite(tie) || tie < 0 || tolerance.trim() === "") throw new Error("Tie tolerance must be a nonnegative number.");
      if (dimension === "preprocessing_operator" && comparisonMode === "matched" && !operator.trim()) throw new Error("Choose the preprocessing operator to compare with its absence.");
      if (dimension === "parameter" && (!operator.trim() || !parameter.trim())) throw new Error("Choose both an operator and a parameter.");
      const request = { metric: state.metric, scope: state.scope, dataset: state.dataset || null, dimension, method: analysis === "gain" ? "relative_gain" : method, partition,
        direction: direction || null, task: task || null, collection: collection || null, protocol: protocol || null,
        operator: dimension === "parameter" || (dimension === "preprocessing_operator" && comparisonMode === "matched") ? operator.trim() : null, parameter: dimension === "parameter" ? parameter.trim() : null,
        split_criterion: dimension === "split" ? "external_error" : null, tie_tolerance: tie, options: parsed,
        comparison_mode: ["pipeline", "split"].includes(dimension) ? "observed" : comparisonMode, aggregation: "method_default", order: "dataset_candidate_lexical" };
      setLoading(true);
      if (analysis === "gain") {
        if (!baseline || !candidate || baseline === candidate) throw new Error("Choose different baseline and candidate configurations.");
        const effects = variances.trim() ? JSON.parse(variances) : null;
        if (effects !== null && (!effects || typeof effects !== "object" || Array.isArray(effects) || Object.values(effects).some(v => typeof v !== "number" || !Number.isFinite(v) || v <= 0))) throw new Error("Effect variances must be an object containing positive finite values for the paired datasets.");
        const gainRequest = { ...request, baseline_id: baseline, candidate_id: candidate, effect_variances: effects };
        const result = validateRelativeGain(await api.qualityRelativeGain(gainRequest), gainRequest);
        if (ticket === generation.current && !signal.aborted) setGain(result as QualityRelativeGain);
      } else {
        const result = validateQualityRanking(await api.qualityRanking(request), request);
        if (ticket === generation.current && !signal.aborted) {
          setRanking(result as QualityRanking);
          setUniverse(result.provenance.candidate_universe || result.candidates);
        }
      }
    } catch (e) { if (ticket === generation.current && !signal.aborted) setError(e instanceof Error ? e.message : "Ranking results could not be loaded."); }
    finally { if (ticket === generation.current && !signal.aborted) setLoading(false); }
  }
  // Filter changes clear prior evidence immediately and cancel its pending display.
  useEffect(() => { generation.current++; setRanking(null); setGain(null); setError(""); setLoading(false); }, [analysis, baseline, candidate, variances, dimension, method, comparisonMode, partition, direction, operator, parameter, task, collection, protocol, tolerance, options]);
  useEffect(() => { setUniverse([]); }, [dimension, comparisonMode, partition, direction, operator, parameter, task, collection, protocol, tolerance, options]);
  useEffect(() => { void refresh(); }, []);
  function useSnapshot(request) {
    setAnalysis(request.method === "relative_gain" ? "gain" : "ranking");
    if (request.method === "relative_gain") { setBaseline(request.baseline_id); setCandidate(request.candidate_id); setVariances(request.effect_variances ? JSON.stringify(request.effect_variances) : ""); }
    setDimension(request.dimension); if (request.method !== "relative_gain") setMethod(request.method); setPartition(request.partition); setDirection(request.direction || "");
    setComparisonMode(request.comparison_mode || "observed"); setOperator(request.operator || ""); setParameter(request.parameter || "");
    setTask(request.task || ""); setCollection(request.collection || ""); setProtocol(request.protocol || ""); setTolerance(String(request.tie_tolerance ?? 0)); setOptions(JSON.stringify(request.options || {}));
  }
  const snapshots = Array.isArray(metadata.requests) ? [...metadata.requests, ...(metadata.gainRequests || [])].filter(item => item.request && item.request.metric === state.metric && item.request.scope === state.scope && (item.request.dataset || "") === (state.dataset || "")) : [];
  const select = (label, value, setter, choices) => <label>{label}<select value={value} onChange={e => setter(e.target.value)}>{choices.map(([id, text]) => <option key={id} value={id}>{text}</option>)}</select></label>;
  const text = (label, value, setter, list?) => <label>{label}<input value={value} list={list} onChange={e => setter(e.target.value)} /></label>;
  return <div className="quality-ranking"><header className="page-head"><h1>Quality ranking</h1><p>Compare observed pipelines and their components across matched datasets.</p></header>
    <p>Metric: {state.metric}. Score level: {state.scope}. Dataset: {state.dataset || "All datasets"}.</p>
    <p>Component rankings describe observed performance; they do not establish causal contributions.</p>
    {metadata.mode === "static" && snapshots.length ? <label>Available snapshot<select value="" onChange={e => { const item = snapshots[Number(e.target.value)]; if (item) useSnapshot(item.request); }}><option value="" disabled>Choose a matching configuration</option>{snapshots.map((item, i) => <option key={i} value={i}>{item.request.dimension} · {item.request.method} · {item.request.partition}{item.request.parameter ? ` · ${item.request.operator}::${item.request.parameter}` : ""}</option>)}</select></label> : null}
    <form onSubmit={e => { e.preventDefault(); void refresh(); }}>
      <div className="ranking-controls">{select("Analysis", analysis, setAnalysis, [["ranking", "Ranking"], ["gain", "Paired relative gain"]])}{select("Compare", dimension, setDimension, dimensions)}{analysis === "ranking" ? select("Ranking method", method, setMethod, methods) : null}
        {select("Partition", partition, setPartition, [["train", "Training"], ["validation", "Validation"], ["test", "Test"], ["final", "Final"]])}
        {!["pipeline", "split"].includes(dimension) ? select("Comparison mode", comparisonMode, setComparisonMode, [["observed", "Observed configurations"], ["matched", "Matched rest of pipeline"]]) : null}
        {select("Metric direction", direction, setDirection, [["", "Use metric definition"], ["min", "Lower is better"], ["max", "Higher is better"]])}</div>
      {dimension === "parameter" || (dimension === "preprocessing_operator" && comparisonMode === "matched") ? <div className="ranking-controls">{text("Operator (full name)", operator, setOperator, "ranking-operators")}{dimension === "parameter" ? text("Parameter name", parameter, setParameter, "ranking-parameters") : <p>Compare the selected operator with its absence while holding the rest of the linear pipeline fixed.</p>}
        <datalist id="ranking-operators">{metadata.operators.map((o, i) => <option key={i} value={o.entrypoint} />)}</datalist><datalist id="ranking-parameters">{metadata.parameters.map((p, i) => <option key={i} value={p.name} />)}</datalist></div> : null}
      {dimension === "split" ? <p>Split strategies use matched external error. Choose a test or final partition and a test or refit score level in the shared lens.</p> : null}
      {analysis === "gain" ? <><div className="ranking-controls">{select("Baseline", baseline, setBaseline, [["", "Choose baseline"], ...universe.map(c => [c.id, `${c.label} (${c.id})`]), ...(baseline && !universe.some(c => c.id === baseline) ? [[baseline, baseline]] : [])])}{select("Candidate", candidate, setCandidate, [["", "Choose candidate"], ...universe.map(c => [c.id, `${c.label} (${c.id})`]), ...(candidate && !universe.some(c => c.id === candidate) ? [[candidate, candidate]] : [])])}</div>
        {!universe.length ? <p>Load a ranking for this cohort first, or choose an available relative gain snapshot, to select configurations.</p> : null}
        <p>Relative gain requires directly paired positive losses. Indirect ratings do not establish loss ratios.</p>
        <details><summary>Optional study-effect variances</summary><p>Leave empty for descriptive gains. A pooled estimate requires sampling variances of the paired relative effects for every paired dataset, including covariance, and justified independent study effects.</p><label>Effect variances by dataset (JSON)<textarea value={variances} onChange={e => setVariances(e.target.value)} /></label></details></> : null}
      <details><summary>Additional cohort and method settings</summary><div className="ranking-controls">{text("Task identity", task, setTask)}{text("Collection", collection, setCollection)}{text("Scoring protocol", protocol, setProtocol)}
        <label>Tie tolerance<input type="number" min="0" step="any" value={tolerance} onChange={e => setTolerance(e.target.value)} /></label>
        <label>Method options (JSON)<textarea value={options} onChange={e => setOptions(e.target.value)} /></label></div></details>
      <button type="submit" disabled={loading}>{loading ? "Loading results…" : analysis === "gain" ? "Update relative gain" : "Update ranking"}</button>
    </form>
    {metadata.mode === "static" ? <p>This snapshot contains {(Array.isArray(metadata.requests) ? metadata.requests.length : 0) + (metadata.gainRequests?.length || 0)} precomputed ranking and relative gain configurations. Only an exact matching selection is available.</p> : null}
    <p role="status" aria-live="polite">{loading ? "Loading results." : gain ? `Relative gain ${gain.state}.` : ranking ? `Ranking ${ranking.state}.` : ""}</p>
    {error ? <p className="ranking-error" role="alert">{error}</p> : null}
    {gain ? <RelativeGainView relativeGain={gain} Plot={Plot} /> : null}
    {ranking ? dimension === "pipeline" ? <QualityRankingPanel ranking={ranking} Plot={Plot} /> : <ComponentRankingView ranking={ranking} Plot={Plot} /> : null}
  </div>;
}
export function mountQualityRanking(host: HTMLElement, props: any) {
  const style = document.createElement("style"); style.textContent = css; document.head.appendChild(style);
  const root = createRoot(host);
  root.render(<Boundary><RankingApp {...props} /></Boundary>);
  return () => { root.unmount(); style.remove(); };
}
