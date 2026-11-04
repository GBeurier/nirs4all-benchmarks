"""Real React/Plotly island and route-lifecycle checks in headless Chrome.

Run npm ci && npm run build:frontend first; native cases also require a ranking-enabled libn4m.
"""
from __future__ import annotations

import functools
import html
import http.server
import json
import os
import re
import runpy
import shutil
import subprocess
import threading
from pathlib import Path

import pytest

from nirs4all_benchmarks.ranking import quality_ranking
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "src/nirs4all_benchmarks/web"


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


BROWSER = r"""
document.addEventListener('DOMContentLoaded', async () => {
 const wait = async (fn) => { for(let i=0;i<300;i++){if(fn()) return;await new Promise(r=>setTimeout(r,40));}throw new Error('Browser condition timed out'); };
 const output = (value) => { const n=document.createElement('pre');n.id='ranking-browser-result';n.textContent=JSON.stringify(value);document.body.appendChild(n); };
 const results={};
 const errors=[];window.addEventListener('unhandledrejection',e=>errors.push(String(e.reason)));
 const select=(label)=>[...document.querySelectorAll('.quality-ranking label')].find(n=>n.firstChild?.textContent===label)?.querySelector('select,input,textarea');
 const change=(label,value)=>{const n=select(label);if(!n)throw new Error('Control missing: '+label);const setter=Object.getOwnPropertyDescriptor(n instanceof HTMLSelectElement?HTMLSelectElement.prototype:n instanceof HTMLTextAreaElement?HTMLTextAreaElement.prototype:HTMLInputElement.prototype,'value').set;setter.call(n,value);n.dispatchEvent(new Event(n instanceof HTMLInputElement||n instanceof HTMLTextAreaElement?'input':'change',{bubbles:true}));};
 const update=()=>document.querySelector('.quality-ranking button[type=submit]').click();
 try {
   await wait(()=>document.querySelector('.quality-ranking button[type=submit]') && document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
   change('Ranking method','trueskill'); await new Promise(r=>setTimeout(r,50));update();
   await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.' && document.querySelector('.quality-ranking section')?.textContent.includes('Method: trueskill'));
   results.table=!!document.querySelector('.quality-ranking table');
   results.plotly=document.querySelectorAll('.quality-ranking .js-plotly-plot').length;
   results.rows=[...document.querySelector('.quality-ranking table tbody').rows].map(row=>[...row.cells].map(c=>c.textContent));
   results.text=document.querySelector('.quality-ranking').textContent;
   if (window.RANKING_TEST_DISPLAY_ONLY) { window.scrollTo(0,0);output(results);return; }
   const {api}=await import('/lib/api.js');
   const original=api.qualityRanking;
   const requests=[];api.qualityRanking=async request=>{requests.push(request);return original(request);};
   for(const dimension of ['model','preprocessing_sequence','preprocessing_operator','feature_selection','augmentation','split']) {
     change('Compare',dimension);await new Promise(r=>setTimeout(r,40));update();
     await wait(()=>requests.at(-1)?.dimension===dimension && document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking unavailable.');
   }
   results.dimensionRequests=requests.slice();
   change('Compare','preprocessing_operator');await new Promise(r=>setTimeout(r,40));change('Comparison mode','matched');await new Promise(r=>setTimeout(r,40));
   change('Operator (full name)','preprocessing.SNV');await new Promise(r=>setTimeout(r,40));update();
   await wait(()=>requests.at(-1)?.comparison_mode==='matched' && document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking unavailable.');
   results.matchedOperatorRequest=requests.at(-1);
   change('Comparison mode','observed');change('Compare','parameter');await new Promise(r=>setTimeout(r,50));
   change('Operator (full name)','models.PLS');change('Parameter name','alpha');await new Promise(r=>setTimeout(r,50));update();
   await wait(()=>requests.length>0 && document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
   results.parameterRequest=requests.at(-1);results.parameterText=document.querySelector('.quality-ranking').textContent;
   change('Partition','test');await new Promise(r=>setTimeout(r,50));update();
   await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking unavailable.');
   results.cacheMissText=document.querySelector('.quality-ranking').textContent;
   results.cacheMissTables=document.querySelectorAll('.quality-ranking table').length;
   change('Compare','pipeline');change('Partition','validation');await new Promise(r=>setTimeout(r,50));update();
   await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
   let lens=document.querySelectorAll('#context-bar select')[2];lens.value='D1';lens.dispatchEvent(new Event('change',{bubbles:true}));
   await wait(()=>requests.at(-1)?.dataset==='D1' && document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking unavailable.');
   results.datasetLensRequest=requests.at(-1);
   lens=document.querySelectorAll('#context-bar select')[2];lens.value='';lens.dispatchEvent(new Event('change',{bubbles:true}));
   await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
   let purges=0;const purge=window.Plotly.purge;window.Plotly.purge=(...args)=>{purges++;return purge(...args);};
   location.hash='#/pipelines';await wait(()=>document.querySelector('#view-root h1')?.textContent==='Pipelines');
   results.readyNavigationPurges=purges;
   location.hash='#/quality-ranking';await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
   let release;
   api.qualityRanking=request=>new Promise(resolve=>{release=()=>original(request).then(resolve);});
   update();
   await wait(()=>!!release);location.hash='#/pipelines';
   await wait(()=>document.querySelector('#view-root h1')?.textContent==='Pipelines');
   release();await new Promise(r=>setTimeout(r,100));
   results.afterNavigation=document.querySelector('#view-root h1')?.textContent;
   results.remainingIsland=document.querySelectorAll('.quality-ranking').length;
   results.errors=errors;results.purges=purges;
   output(results);
 } catch(error) {output({error:String(error),results,body:document.querySelector('#view-root')?.textContent});}
});
"""


def _chrome(tmp_path: Path, site: Path, script: str, screenshot: str | None = None) -> str:
    chrome = next((shutil.which(n) for n in ("google-chrome", "chromium", "chromium-browser") if shutil.which(n)), None)
    if not chrome:
        pytest.skip("Chrome or Chromium is required")
    page = site / "index.html"
    source = page.read_text()
    setup = 'localStorage.setItem("arena.state.v2",JSON.stringify({metric:"rmse",scope:"cv",dataset:""}));location.hash="#/quality-ranking";'
    page.write_text(source.replace("</head>", f"<script>{setup}\n{script}</script></head>"))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=str(site)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        args = [chrome, "--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--no-proxy-server",
                f"--user-data-dir={tmp_path / 'browser-profile'}", "--virtual-time-budget=18000", "--dump-dom"]
        if screenshot:
            args += [f"--screenshot={screenshot}", "--window-size=1440,1200"]
        return subprocess.run([*args, f"http://127.0.0.1:{server.server_port}/"], capture_output=True, text=True, timeout=45, check=True).stdout
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        page.write_text(source)


def _native_site(tmp_path: Path, disconnected: bool = False, gain: bool = False) -> Path:
    if not (WEB / "generated/quality-ranking.js").exists():
        pytest.skip("Run npm ci && npm run build:frontend to test the real shared React bundle")
    add = runpy.run_path(str(ROOT / "tests/test_quality_ranking.py"))["add"]
    with ArenaStore(tmp_path / "store") as store:
        for dataset, pipeline, value in [("D1", "A", 1), ("D1", "B", 2), ("D2", "B", 500), ("D2", "C", 600)]:
            add(store, dataset, pipeline, value, operator="models.PLS" if pipeline != "C" else "models.SVR", params={"alpha": 1 if pipeline == "A" else 2})
        if gain:
            add(store, "D3", "A", 3, operator="models.PLS", params={"alpha": 1})
            add(store, "D3", "B", 6, operator="models.PLS", params={"alpha": 2})
        if disconnected:
            add(store, "D3", "X", 3)
            add(store, "D3", "Y", 5)
        result = quality_ranking(store, {"method": "trueskill"})
        if result["reason"] == "native_ranking_runtime_unavailable":
            pytest.skip("A ranking-enabled native libn4m is required")
        assert result["state"] == "ready"
        assert result["coverage"]["complete_blocks"] == 0
        if not disconnected:
            assert [c["rank"] for c in result["candidates"]] == [1, 2, 3]
    site = tmp_path / "site"
    build_site(tmp_path / "store", site, domain=None, ranking_requests=[
        {"method": "bradley_terry"}, {"method": "trueskill"},
        {"method": "trueskill", "dimension": "parameter", "operator": "models.PLS", "parameter": "alpha"},
    ], relative_gain_requests=[
        {"baseline_id": "B", "candidate_id": "A"},
        {"baseline_id": "B", "candidate_id": "A", "effect_variances": {"D1": 0.01, "D3": 0.04}},
    ] if gain else [])
    return site


def _result(dom: str) -> dict:
    match = re.search(r'<pre id="ranking-browser-result">(.*?)</pre>', dom, flags=re.S)
    assert match, dom[-5000:]
    value = json.loads(html.unescape(match.group(1)))
    assert "error" not in value, value
    return value


def test_native_shared_ranking_selectors_exact_cache_and_route_cleanup(tmp_path: Path) -> None:
    site = _native_site(tmp_path)
    result = _result(_chrome(tmp_path, site, BROWSER))
    assert result["table"] and result["plotly"] == 2
    assert [row[0] for row in result["rows"]] == ["A", "B", "C"]
    assert [row[1] for row in result["rows"]] == ["1", "2", "3"]
    assert "0 complete blocks" in result["text"]
    request = result["parameterRequest"]
    assert request["operator"] == "models.PLS" and request["parameter"] == "alpha"
    assert request["metric"] == "rmse" and request["scope"] == "cv" and request["partition"] == "validation"
    assert "models.PLS@1::alpha" in result["parameterText"]
    assert result["cacheMissTables"] == 0
    assert "not precomputed" in result["cacheMissText"].lower()
    assert result["afterNavigation"] == "Pipelines" and result["remainingIsland"] == 0
    assert result["readyNavigationPurges"] >= 2
    assert result["datasetLensRequest"]["dataset"] == "D1"
    assert result["datasetLensRequest"]["metric"] == "rmse" and result["datasetLensRequest"]["scope"] == "cv"
    assert {r["dimension"] for r in result["dimensionRequests"]} == {"model", "preprocessing_sequence", "preprocessing_operator", "feature_selection", "augmentation", "split"}
    split_request = next(r for r in result["dimensionRequests"] if r["dimension"] == "split")
    assert split_request["split_criterion"] == "external_error"
    assert result["matchedOperatorRequest"]["operator"] == "preprocessing.SNV"
    assert result["matchedOperatorRequest"]["comparison_mode"] == "matched"
    assert not result["errors"]


def test_native_disconnected_ratings_keep_unranked_groups_visible(tmp_path: Path) -> None:
    site = _native_site(tmp_path, disconnected=True)
    result = _result(_chrome(tmp_path, site, "window.RANKING_TEST_DISPLAY_ONLY=true;" + BROWSER,
                            os.environ.get("N4A_UI_SCREENSHOT")))
    assert len(result["rows"]) == 5
    assert {row[1] for row in result["rows"]} == {"Not available"}
    assert "2 connected components" in result["text"]
    assert result["plotly"] == 2


@pytest.mark.parametrize("scenario", ["native_absent", "corrupt", "unsupported_schema", "missing_bundle"])
def test_unavailable_or_corrupt_ranking_is_local_to_page(tmp_path: Path, scenario: str) -> None:
    site = _native_site(tmp_path)
    bundle_path = site / "data/bundle.json"
    bundle = json.loads(bundle_path.read_text())
    if scenario == "native_absent":
        def missing_runtime(*args, **kwargs):
            raise ImportError("Controlled unavailable-runtime fixture")

        with ArenaStore(tmp_path / "store") as store:
            bundle["quality_rankings"] = [quality_ranking(store, artifact["request"], ranker=missing_runtime)
                                           for artifact in bundle["quality_rankings"]]
    elif scenario == "corrupt":
        for artifact in bundle["quality_rankings"]:
            artifact["provenance"]["excluded"] = None
    elif scenario == "unsupported_schema":
        for artifact in bundle["quality_rankings"]:
            artifact["schema_version"] = "unsupported/2"
    else:
        (site / "generated/quality-ranking.js").unlink()
    bundle_path.write_text(json.dumps(bundle))
    script = r"""
    document.addEventListener('DOMContentLoaded',async()=>{
      const wait=async(fn)=>{for(let i=0;i<200;i++){if(fn())return;await new Promise(r=>setTimeout(r,40));}throw Error('timeout');};
      await wait(()=>document.querySelector('#view-root')?.textContent.match(/unavailable|incomplete/i));
      const message=document.querySelector('#view-root').textContent;
      location.hash='#/pipelines';await wait(()=>document.querySelector('#view-root h1')?.textContent==='Pipelines');
      const out=document.createElement('pre');out.id='ranking-browser-result';out.textContent=JSON.stringify({message,next:document.querySelector('#view-root h1').textContent});document.body.appendChild(out);
    });
    """
    result = _result(_chrome(tmp_path, site, script))
    assert result["next"] == "Pipelines"
    expected = {"native_absent": "native ranking runtime is unavailable", "corrupt": "incomplete", "unsupported_schema": "unsupported format", "missing_bundle": "temporarily unavailable"}[scenario]
    assert expected in result["message"].lower()


def test_native_relative_gain_descriptive_and_supplied_variance_meta(tmp_path: Path) -> None:
    site = _native_site(tmp_path, gain=True)
    script = r"""
    document.addEventListener('DOMContentLoaded',async()=>{
      const wait=async(fn)=>{for(let i=0;i<300;i++){if(fn())return;await new Promise(r=>setTimeout(r,40));}throw Error('timeout');};
      const control=label=>[...document.querySelectorAll('.quality-ranking label')].find(n=>n.firstChild?.textContent===label)?.querySelector('select,textarea');
      const change=(label,value)=>{const n=control(label);Object.getOwnPropertyDescriptor(n instanceof HTMLSelectElement?HTMLSelectElement.prototype:HTMLTextAreaElement.prototype,'value').set.call(n,value);n.dispatchEvent(new Event(n instanceof HTMLSelectElement?'change':'input',{bubbles:true}));};
      const update=()=>document.querySelector('.quality-ranking button[type=submit]').click();
      try {
        await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.');
        change('Analysis','gain');await new Promise(r=>setTimeout(r,40));change('Baseline','B');change('Candidate','A');await new Promise(r=>setTimeout(r,40));update();
        await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Relative gain ready.');
        const descriptive=document.querySelector('.quality-ranking').textContent;
        const rows=[...document.querySelector('.quality-ranking table').rows].map(row=>[...row.cells].map(c=>c.textContent));
        change('Effect variances by dataset (JSON)','{"D1":0.01,"D3":0.04}');await new Promise(r=>setTimeout(r,40));update();
        await wait(()=>document.querySelector('.quality-ranking [role=status]')?.textContent==='Relative gain ready.' && document.querySelector('.quality-ranking').textContent.includes('DerSimonian'));
        const pooled=document.querySelector('.quality-ranking').textContent;
        const plots=document.querySelectorAll('.quality-ranking .js-plotly-plot').length;
        window.scrollTo(0,0);
        const out=document.createElement('pre');out.id='ranking-browser-result';out.textContent=JSON.stringify({descriptive,rows,pooled,plots});document.body.appendChild(out);
      } catch(error){const out=document.createElement('pre');out.id='ranking-browser-result';out.textContent=JSON.stringify({error:String(error),body:document.querySelector('#view-root')?.textContent});document.body.appendChild(out);}
    });
    """
    result = _result(_chrome(tmp_path, site, script, os.environ.get("N4A_UI_SCREENSHOT")))
    assert ["Mean gain", "0.5"] in result["rows"]
    assert ["Datasets", "2"] in result["rows"]
    assert "No pooled estimate" in result["descriptive"]
    assert "DerSimonian" in result["pooled"] and "Pooled gain" in result["pooled"]
    assert result["plots"] == 1


def test_missing_plotly_preserves_complete_shared_data_tables(tmp_path: Path) -> None:
    site = _native_site(tmp_path)
    (site / "vendor/plotly.min.js").unlink()
    script = r"""
    document.addEventListener('DOMContentLoaded',async()=>{
      for(let i=0;i<300;i++) {
        if(document.querySelector('.quality-ranking [role=status]')?.textContent==='Ranking ready.' && document.querySelector('.quality-ranking')?.textContent.includes('Chart unavailable.')) {
          const rows=document.querySelectorAll('.quality-ranking table:first-of-type tbody tr').length;
          const out=document.createElement('pre');out.id='ranking-browser-result';out.textContent=JSON.stringify({rows,text:document.querySelector('.quality-ranking').textContent});document.body.appendChild(out);return;
        }
        await new Promise(r=>setTimeout(r,40));
      }
    });
    """
    result = _result(_chrome(tmp_path, site, script))
    assert result["rows"] >= 3
    assert "complete results are in the table" in result["text"]
