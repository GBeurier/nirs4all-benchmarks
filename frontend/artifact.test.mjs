import { test } from "node:test";
import assert from "node:assert/strict";
import { validateQualityRanking } from "./artifact.js";
const request = { metric:"rmse",scope:"cv",partition:"validation",dimension:"pipeline",method:"trueskill",direction:null,dataset:null,options:{beta:2,tau:0.1},tie_tolerance:0,aggregation:"method_default",order:"dataset_candidate_lexical",comparison_mode:"observed" };
function fixture() { return {schema_version:"n4a.quality-ranking/1",request_id:"fixture",request:{...request,direction:"min",options:{tau:0.1,beta:2}},state:"unavailable",reason:"native_ranking_runtime_unavailable",...Object.fromEntries(["metric","scope","partition","dimension","method"].map(k=>[k,request[k]])),config:{},interpretation:"observed_performance",candidates:[],coverage:null,inference:null,convergence:null,uncertainty_kind:null,score_direction:null,warnings:[],provenance:{datasets:[],blocks:[],comparisons:[],aggregation:"dataset_pair_mean",dataset_weighting:"one pair per dataset",excluded:{},source_observations:0,accepted_observations:0,matched_blocks:0,comparison_rows:0},capabilities:{methods:[],dimensions:[],deferred:{}}}; }
test("normalizes option ordering and accepts backend-resolved direction",()=>assert.equal(validateQualityRanking(fixture(),request).request.direction,"min"));
test("rejects unsupported, nested-corrupt and mismatched evidence",()=>{
 for(const mutate of [r=>r.schema_version="future/2",r=>r.provenance.excluded=null,r=>r.request.dataset="different",r=>r.request.order="different",r=>r.request.aggregation="other",r=>r.candidates=[{id:"x",label:"X",score:NaN}],r=>r.capabilities.deferred=[]]){const r=fixture();mutate(r);assert.throws(()=>validateQualityRanking(r,request));}
});

const { validateRelativeGain } = await import("./artifact.js");
function gainFixture() {
 const req={...request,method:"relative_gain",options:{},baseline_id:"B",candidate_id:"A",effect_variances:null};
 return {request:req, artifact:{schema_version:"n4a.quality-relative-gain/1",state:"ready",reason:null,request_id:"gain",request:{...req,direction:"min"},dimension:req.dimension,metric:req.metric,scope:req.scope,partition:req.partition,direction:"min",baseline_id:"B",candidate_id:"A",candidates:[{id:"A",label:"A"},{id:"B",label:"B"}],studies:[{dataset:"D",baseline_loss:2,candidate_loss:1,effect_variance:null,matched_blocks:["block"]}],result:{schema_version:"n4m.relative_gain/1",effect:"(baseline-candidate)/baseline",meta_analysis:null,variance_source:null,inference_assumption:"caller_assessed",n:1,mean_gain:0.5,sample_sd:null,pooled_gain:null,standard_error:null,tau_squared:null,q:null,i_squared:null},provenance:fixture().provenance,warnings:[],capabilities:fixture().capabilities}};
}
test("validates supplied native gain without deriving statistics",()=>{const {request,artifact}=gainFixture();assert.equal(validateRelativeGain(artifact,request).result.mean_gain,0.5);});
test("rejects corrupt gain metadata and unsupported inference claims",()=>{
 for(const mutate of [r=>r.result.schema_version="unknown",r=>r.studies[0].effect_variance=-1,r=>r.result.pooled_gain=0.5,r=>r.result.n=2,r=>r.provenance.excluded=null,r=>r.result.standard_error=Infinity]){const {request,artifact}=gainFixture();mutate(artifact);assert.throws(()=>validateRelativeGain(artifact,request));}
});
