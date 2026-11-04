import assert from "node:assert/strict";
import test from "node:test";
import { normalizeComponentRequest, validateComponentAnalysis } from "./artifact.js";
const requested={factors:[{name:"model",dimension:"model",reference:"A"}],options:{reml:true,tolerance:1e-8}};
function fixture(){return {schema_version:"n4a.component-analysis/1",state:"ready",reason:null,request_id:"test",request:normalizeComponentRequest(requested),method:"mixed_effects",metric:"nrmse",scope:"cv",partition:"validation",direction:"min",design:{factors:[{name:"model",dimension:"model",kind:"categorical",observed_levels:["A","B"]}],columns:[{name:"Intercept",factor:null},{name:"B",factor:"model"}],references:{model:"A"},domains:[],model_slice:null,outcome_policy:"scale_free",group_names:["dataset","dataset_evaluation_block"]},provenance:{source_observations:24,accepted_observations:24,excluded:{},datasets:["D1","D2"],observations:[],baseline_observations:[],training_datasets:[],validation_datasets:[]},result:{schema_version:"n4m.mixed_effects/1",state:"ready",reason:null,beta:[0.4,-0.2],standard_errors:[0.1,0.1],covariance:[[0.01,0],[0,0.01]],variances:[0.1,0.2,0.3],diagnostics:{fit_state:"fitted_boundary",condition_number:3},warnings:[],options:{}},warnings:[]};}
test("accepts exact nested component designs with native coefficient order",()=>{
 assert.equal(validateComponentAnalysis(fixture(),{...requested,options:{tolerance:1e-8,reml:true}}).result.beta[1],-.2);
});
test("rejects changed reference/domain/slice/dataset identities and invalid native estimates",()=>{
 for(const mutate of [r=>r.request.factors[0].reference="B",r=>r.request.model_slice="other@1",r=>r.request.training_datasets=["D3"],r=>r.result.beta=[0.2],r=>r.result.standard_errors[0]=-1,r=>r.result.variances=[1,2],r=>r.result.diagnostics.condition_number=Infinity,r=>r.request.factors[0].levels=["A","B"],r=>r.schema_version="future/2"]){const r=fixture();mutate(r);assert.throws(()=>validateComponentAnalysis(r,requested));}
});
test("native unavailable states carry diagnostics without manufacturing estimates",()=>{
 const r=fixture();r.state="unavailable";r.reason="fixed_rank";r.result.state="unavailable";r.result.reason="fixed_rank";r.result.beta=[null,null];r.result.standard_errors=[null,null];
 assert.equal(validateComponentAnalysis(r,requested).reason,"fixed_rank");
});
test("checks fANOVA factor indices and product-domain semantics",()=>{
 const r=fixture();r.method="fanova";r.request.method="fanova";r.request.options={};r.design.columns=[{name:"model",factor:"model"}];
 r.result={schema_version:"n4m.fanova/1",state:"ready",reason:null,feature_importance:[{index:0,fraction:1,tree_sd:0}],interactions:[],diagnostics:{validation_r2:-.8},warnings:[],options:{},domain_measure:"uniform_product",aggregation:"mean_per_tree_normalized_components",uncertainty_kind:"tree_dispersion_not_confidence"};
 const req={...requested,method:"fanova",options:{}};
 assert.equal(validateComponentAnalysis(r,req).result.diagnostics.validation_r2,-.8);
 for(const mutate of [x=>x.result.feature_importance[0].index=2,x=>x.result.domain_measure="observed_distribution",x=>x.result.feature_importance.push({...x.result.feature_importance[0]}),x=>x.result.uncertainty_kind="confidence_interval"]){const bad=structuredClone(r);mutate(bad);assert.throws(()=>validateComponentAnalysis(bad,req));}
});
