"""Real shared React analysis views consume native static snapshots in Chrome."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore
from test_component_analysis import populate_native, request
from test_quality_ranking_ui import WEB, _chrome, _result


def test_native_component_views_snapshot_selection_and_route_cleanup(tmp_path: Path):
    if not os.environ.get("N4M_LIB_PATH") or not (WEB / "generated/quality-ranking.js").exists():
        pytest.skip("built browser island and native component runtime required")
    with ArenaStore(tmp_path / "store") as store:
        populate_native(store)
    fanova = request(
        method="fanova",
        factors=[{"name": "model", "dimension": "model", "levels": ["models.A@1", "models.B@1"]}],
        training_datasets=["D0", "D1", "D2"],
        validation_datasets=["D3"],
        options={"seed": 9, "n_trees": 16},
    )
    build_site(
        tmp_path / "store",
        tmp_path / "site",
        domain=None,
        ranking_requests=[{"metric": "nrmse"}],
        component_analysis_requests=[request(), fanova],
    )
    browser = r"""
localStorage.setItem("arena.state.v2",JSON.stringify({metric:"nrmse",scope:"cv",dataset:""}));
document.addEventListener("DOMContentLoaded",async()=>{
 const wait=async fn=>{for(let i=0;i<300;i++){if(fn())return;await new Promise(resolve=>setTimeout(resolve,40));}throw Error("Browser condition timed out");};
 const output=value=>{const node=document.createElement("pre");node.id="ranking-browser-result";node.textContent=JSON.stringify(value);document.body.appendChild(node);};
 const control=label=>[...document.querySelectorAll(".quality-ranking label")].find(node=>node.firstChild?.textContent===label)?.querySelector("select,input,textarea");
 const change=(label,value)=>{const node=control(label);if(!node)throw Error("Missing "+label);Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype,"value").set.call(node,value);node.dispatchEvent(new Event("change",{bubbles:true}));};
 const submit=()=>document.querySelector(".component-analysis-controls button[type=submit]").click();
 const status=()=>document.querySelector(".component-analysis-controls [role=status]")?.textContent;
 const result={},errors=[];window.addEventListener("unhandledrejection",event=>errors.push(String(event.reason)));
 try{
  await wait(()=>document.querySelector(".quality-ranking [role=status]")?.textContent==="Ranking ready.");
  let purges=0;const purge=window.Plotly.purge;window.Plotly.purge=(...args)=>{purges++;return purge(...args);};
  change("Analysis","mixed_effects");await wait(()=>control("Available analysis snapshot"));
  change("Available analysis snapshot","0");await new Promise(resolve=>setTimeout(resolve,50));submit();
  await wait(()=>status()==="Component analysis ready.");
  result.mixedText=document.querySelector(".component-analysis-controls").textContent;
  result.coefficients=[...document.querySelector(".component-analysis-controls figure table tbody").rows].map(row=>[...row.cells].map(cell=>cell.textContent));
  result.mixedPlots=document.querySelectorAll(".component-analysis-controls .js-plotly-plot").length;
  change("Analysis","fanova");await wait(()=>control("Available analysis snapshot"));change("Available analysis snapshot","0");await new Promise(resolve=>setTimeout(resolve,50));submit();
  await wait(()=>status()==="Component analysis ready.");
  result.fanovaText=document.querySelector(".component-analysis-controls").textContent;
  result.fanovaDiagnostics=JSON.parse([...document.querySelectorAll(".component-analysis-controls details")].find(node=>node.querySelector("summary")?.textContent==="Native fit and generalization diagnostics").querySelector("pre").textContent);
  result.importance=[...document.querySelector(".component-analysis-controls figure table tbody").rows].map(row=>[...row.cells].map(cell=>cell.textContent));
  const {api}=await import("/lib/api.js");const original=api.componentAnalysis;
  api.componentAnalysis=async request=>{const artifact=structuredClone(await original(request));artifact.request.factors[0].levels=["wrong@1"];return artifact;};
  submit();await wait(()=>document.querySelector(".component-analysis-controls [role=alert]"));
  result.validationError=document.querySelector(".component-analysis-controls [role=alert]").textContent;
  result.staleTables=document.querySelectorAll(".component-analysis-controls table").length;
  let release;api.componentAnalysis=request=>new Promise(resolve=>{release=()=>original(request).then(resolve);});submit();await wait(()=>release);
  location.hash="#/pipelines";await wait(()=>document.querySelector("#view-root h1")?.textContent==="Pipelines");release();await new Promise(resolve=>setTimeout(resolve,80));
  result.remainingIsland=document.querySelectorAll(".component-analysis-controls").length;result.purges=purges;result.errors=errors;output(result);
 }catch(error){output({error:String(error),result,body:document.querySelector("#view-root")?.textContent});}
});
"""
    result = _result(_chrome(tmp_path, tmp_path / "site", browser))
    assert result["mixedPlots"] == 1
    assert len(result["coefficients"]) == 2
    assert float(result["coefficients"][1][1]) == pytest.approx(0.2, abs=0.02)
    assert "not a confidence interval" in result["mixedText"]
    assert "conditional_plugin_standard_error" not in result["mixedText"] or "standard error" in result["mixedText"]
    assert result["importance"][0][0] == "model" and float(result["importance"][0][1]) == pytest.approx(1)
    assert "Training datasets: D0, D1, D2. Held-out datasets: D3" in result["fanovaText"]
    assert "validation_r2" in result["fanovaText"] and result["fanovaDiagnostics"]["validation_r2"] < 0
    assert "does not mean a configuration improves performance" in result["fanovaText"]
    assert "another design" in result["validationError"] and result["staleTables"] == 0
    assert result["remainingIsland"] == 0 and result["purges"] >= 4 and not result["errors"]
