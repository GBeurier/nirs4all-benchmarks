import React, { useEffect, useRef, useState } from "react";
import { MixedEffectsView, FanovaView } from "nirs4all-ui/ranking";
import type { ComponentAnalysis, RankingPlotRenderer } from "nirs4all-ui/ranking";
import { normalizeComponentRequest, validateComponentAnalysis } from "./artifact.js";

const presets = [["model", "Model"], ["preprocessing_sequence", "Preprocessing sequence"], ["model_and_preprocessing", "Model and preprocessing sequence"], ["parameter", "Qualified parameter within one model"]];
export function ComponentAnalysisApp({ api, state, signal, method, Plot }: { api: any; state: any; signal: AbortSignal; method: "mixed_effects" | "fanova"; Plot: RankingPlotRenderer }) {
  const [preset, setPreset] = useState("model");
  const [partition, setPartition] = useState(state.scope === "test" ? "test" : "validation");
  const [outcome, setOutcome] = useState(["rmse","mse","mae","medae","log_loss"].includes(state.metric) ? "native_baseline_ratio" : "scale_free");
  const [baseline, setBaseline] = useState("");
  const [modelSlice, setModelSlice] = useState("");
  const [operator, setOperator] = useState("");
  const [operatorVersion, setOperatorVersion] = useState("");
  const [parameter, setParameter] = useState("");
  const [parameterKind, setParameterKind] = useState("numeric");
  const [references, setReferences] = useState<Record<string,string>>({});
  const [domains, setDomains] = useState<Record<string,string>>({});
  const [lower, setLower] = useState("");
  const [upper, setUpper] = useState("");
  const [training, setTraining] = useState<string[]>([]);
  const [validation, setValidation] = useState<string[]>([]);
  const [task, setTask] = useState("");
  const [collection, setCollection] = useState("");
  const [protocol, setProtocol] = useState("");
  const [missing, setMissing] = useState("reject");
  const [options, setOptions] = useState("{}");
  const [snapshot, setSnapshot] = useState<Record<string, unknown> | null>(null);
  const [metadata, setMetadata] = useState<any>({datasets:[],pipelines:[],operators:[],requests:[],mode:"service"});
  const [artifact, setArtifact] = useState<ComponentAnalysis | null>(null);
  const [preview, setPreview] = useState<any[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const generation = useRef(0);
  useEffect(() => {
    let active = true;
    Promise.allSettled([api.datasets(),api.pipelines(),api.operators(),api.componentAnalysisRequests()]).then(([datasets,pipelines,operators,index]) => {
      if (!active || signal.aborted) return;
      setMetadata({datasets:datasets.status==="fulfilled"?datasets.value:[],pipelines:pipelines.status==="fulfilled"?pipelines.value:[],operators:operators.status==="fulfilled"?operators.value:[],...(index.status==="fulfilled"?index.value:{})});
    });
    return () => { active=false;generation.current++; };
  }, []);
  useEffect(() => {generation.current++;setArtifact(null);setError("");setLoading(false);},[method,preset,partition,outcome,baseline,modelSlice,operator,operatorVersion,parameter,parameterKind,references,domains,lower,upper,training,validation,task,collection,protocol,missing,options,snapshot]);
  useEffect(() => {setPreview([]);},[method,preset,partition,outcome,baseline,modelSlice,operator,operatorVersion,parameter,parameterKind,task,collection,protocol,missing]);
  const makeFactor = (name: string, dimension: string, previewOnly: boolean) => {
    const factor: any = {name,dimension,kind:dimension==="parameter"?parameterKind:"categorical",operator:dimension==="parameter"?operator.trim():null,operator_version:dimension==="parameter"?operatorVersion.trim()||null:null,parameter:dimension==="parameter"?parameter.trim():null};
    if (references[name]) factor.reference=JSON.parse(references[name]);
    if (domains[name]) factor.levels=JSON.parse(domains[name]);
    if (dimension==="parameter" && parameterKind==="numeric" && !previewOnly && method==="fanova") {
      if (!lower.trim()||!upper.trim()||!Number.isFinite(Number(lower))||!Number.isFinite(Number(upper))||Number(lower)>=Number(upper)) throw new Error("Declare finite increasing numeric bounds for the parameter domain.");
      factor.bounds=[Number(lower),Number(upper)];
    }
    return factor;
  };
  async function refresh(previewOnly: boolean) {
    const ticket=++generation.current;
    setArtifact(null);setError("");setLoading(true);
    try {
      let request=snapshot;
      if (!request) {
        const parsed=JSON.parse(options);
        if (!parsed || typeof parsed!=="object" || Array.isArray(parsed) || Object.values(parsed).some(value=>typeof value!=="number" && typeof value!=="boolean" || typeof value==="number" && !Number.isFinite(value))) throw new Error("Native options must contain finite numbers or booleans.");
        const factors=preset==="model_and_preprocessing"?[makeFactor("model","model",previewOnly),makeFactor("preprocessing","preprocessing_sequence",previewOnly)]:[makeFactor(preset==="parameter"?parameter||"parameter":preset,preset,previewOnly)];
        request=normalizeComponentRequest({method,metric:state.metric,scope:state.scope,dataset:state.dataset||null,partition,model_slice:modelSlice.trim()||null,factors,outcome_policy:outcome,baseline_pipeline_id:outcome==="native_baseline_ratio"?baseline||null:null,task:task||null,collection:collection||null,protocol:protocol||null,missing_policy:missing,training_datasets:method==="fanova"?training:[],validation_datasets:method==="fanova"?validation:[],options:parsed,preview_only:previewOnly});
      }
      const result=validateComponentAnalysis(await api.componentAnalysis(request),request);
      if(ticket===generation.current&&!signal.aborted){setArtifact(result as ComponentAnalysis);if(result.design.factors.length)setPreview(result.design.factors);}
    } catch(e) {if(ticket===generation.current&&!signal.aborted)setError(e instanceof Error?e.message:"Analysis could not be loaded.");}
    finally {if(ticket===generation.current&&!signal.aborted)setLoading(false);}
  }
  const select=(label,value,setter,choices)=><label>{label}<select value={value} onChange={event=>setter(event.target.value)}>{choices.map(([id,text])=><option key={id} value={id}>{text}</option>)}</select></label>;
  const text=(label,value,setter,list?)=><label>{label}<input value={value} list={list} onChange={event=>setter(event.target.value)}/></label>;
  const snapshots=metadata.requests.filter(item=>item.request?.method===method&&item.request.metric===state.metric&&item.request.scope===state.scope&&(item.request.dataset||"")===(state.dataset||""));
  const datasetSelect=(label,value,setter)=><label>{label}<select multiple value={value} onChange={event=>setter([...event.target.selectedOptions].map(option=>option.value))}>{metadata.datasets.map(dataset=><option key={dataset.dataset_fingerprint} value={dataset.dataset_fingerprint}>{dataset.name||dataset.dataset_fingerprint}</option>)}</select></label>;
  return <div className="component-analysis-controls">
    <p>Metric: {state.metric}; scope: {state.scope}; dataset: {state.dataset||"All datasets"}. Fitting uses eligible original observations with explicit source and evaluation identities.</p>
    {metadata.mode==="static"?<p>Only exact precomputed analysis requests are available in this static snapshot.</p>:null}
    {snapshots.length?<label>Available analysis snapshot<select value="" onChange={event=>setSnapshot(snapshots[Number(event.target.value)]?.request??null)}><option value="" disabled>Choose an exact saved design</option>{snapshots.map((item,index)=><option key={index} value={index}>{item.request.method} · {item.request.factors.map(factor=>factor.name).join(", ")} · {item.request.partition}</option>)}</select></label>:null}
    {snapshot?<><p>A saved request is selected. Its exact design and native options will be used.</p><pre>{JSON.stringify(snapshot,null,2)}</pre><button type="button" onClick={()=>setSnapshot(null)}>Use editable controls</button></>:null}
    <form onSubmit={event=>{event.preventDefault();void refresh(false);}}>
      <fieldset disabled={snapshot!==null}><legend>{method==="mixed_effects"?"Adjusted effect design":"Surrogate importance domain"}</legend>
        <div className="ranking-controls">{select("Design preset",preset,setPreset,presets)}{select("Partition",partition,setPartition,[["validation","Validation"],["train","Training"],["test","Test"],["final","Final"]])}{select("Outcome scale",outcome,setOutcome,[["scale_free","Dimensionless metric"],["native_baseline_ratio","Native loss / paired baseline ratio"]])}</div>
        {outcome==="native_baseline_ratio"?select("Normalization baseline pipeline",baseline,setBaseline,[["","Choose a baseline"],...metadata.pipelines.map(pipeline=>[pipeline.pipeline_dag_hash,pipeline.human_label||pipeline.pipeline_dag_hash])]):null}
        {text("Model slice (qualified operator@version)",modelSlice,setModelSlice,"analysis-models")}
        <datalist id="analysis-models">{metadata.operators.filter(operator=>operator.role==="model").map((operator,index)=><option key={index} value={`${operator.entrypoint}@${operator.version||"unreported"}`}/>)}</datalist>
        {preset==="parameter"?<><p>A parameter analysis requires one model slice. Missing conditional parameters are never assigned an invented value.</p><div className="ranking-controls">{text("Qualified parameter operator",operator,setOperator)}{text("Operator version",operatorVersion,setOperatorVersion)}{text("Parameter name",parameter,setParameter)}{select("Parameter kind",parameterKind,setParameterKind,[["numeric","Numeric"],["categorical","Categorical"]])}</div>{method==="fanova"&&parameterKind==="numeric"?<div className="ranking-controls">{text("Numeric domain lower bound",lower,setLower)}{text("Numeric domain upper bound",upper,setUpper)}</div>:null}</>:null}
        <p>Preview the observed factor values, then explicitly select references or declare the fANOVA domains before fitting.</p>
        {preview.filter(factor=>factor.kind==="categorical").map(factor=><div key={factor.name} className="ranking-controls">{method==="mixed_effects"?select(`Reference for ${factor.name}`,references[factor.name]||"",value=>setReferences({...references,[factor.name]:value}),[["","Choose an explicit reference"],...factor.observed_levels.map(level=>[JSON.stringify(level),String(level)])]):<><label>Domain levels for {factor.name} (JSON)<textarea value={domains[factor.name]||""} onChange={event=>setDomains({...domains,[factor.name]:event.target.value})}/></label><button type="button" onClick={()=>setDomains({...domains,[factor.name]:JSON.stringify(factor.observed_levels)})}>Declare previewed levels for {factor.name}</button></>}</div>)}
        {method==="fanova"?<><p>Choose disjoint dataset identities for fitting and held-out assessment. Every eligible dataset must be assigned. Importance integrates the declared uniform product domain; it does not indicate a positive performance gain.</p><div className="ranking-controls">{datasetSelect("Training datasets",training,setTraining)}{datasetSelect("Held-out datasets",validation,setValidation)}</div></>:null}
        <details><summary>Additional cohort and native settings</summary><div className="ranking-controls">{text("Task identity",task,setTask)}{text("Collection",collection,setCollection)}{text("Scoring protocol",protocol,setProtocol)}{select("Missing factor policy",missing,setMissing,[["reject","Reject incomplete design"],["exclude_complete_case","Exclude incomplete cases explicitly"]])}<label>Native analysis options (JSON)<textarea value={options} onChange={event=>setOptions(event.target.value)}/></label></div></details>
      </fieldset>
      <button type="button" disabled={loading||snapshot!==null} onClick={()=>void refresh(true)}>Preview original-observation design</button> <button type="submit" disabled={loading}>{loading?"Loading analysis…":"Fit component analysis"}</button>
    </form>
    <p role="status" aria-live="polite">{loading?"Loading analysis.":artifact?`Component analysis ${artifact.state}.`:""}</p>
    {error?<p role="alert" className="ranking-error">{error}</p>:null}
    {artifact?method==="mixed_effects"?<MixedEffectsView analysis={artifact} Plot={Plot}/>:<FanovaView analysis={artifact} Plot={Plot}/>:null}
  </div>;
}
