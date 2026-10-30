"""Browser regressions for static Arena initialization."""

from __future__ import annotations

import functools
import http.server
import shutil
import subprocess
import threading
from pathlib import Path

import pytest

from nirs4all_benchmarks.fixtures import seed_store
from nirs4all_benchmarks.site_build import build_site


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


def test_static_site_recovers_from_startup_failures(tmp_path: Path) -> None:
    chrome = next((shutil.which(name) for name in ("google-chrome", "chromium", "chromium-browser")
                   if shutil.which(name)), None)
    if not chrome:
        pytest.skip("Chrome or Chromium is required for the browser startup test")

    seed_store(tmp_path / "store", collection_id="demo", demo=False)
    site = tmp_path / "site"
    build_site(tmp_path / "store", site, domain=None)
    index = site / "index.html"
    original = index.read_text(encoding="utf-8")
    handler = functools.partial(_QuietHandler, directory=str(site))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        cases = [
            ("normal", "", True),
            ("corrupt", 'localStorage.setItem("arena.state.v1", "{broken");', True),
            ("blocked", 'Object.defineProperty(window, "localStorage", {get() { throw new Error("blocked"); }});', True),
            ("snapshot_error", '''
              const realFetch = window.fetch;
              window.fetch = (url, options) => String(url).endsWith("/bundle.json")
                ? Promise.resolve(new Response("unavailable", {status: 503})) : realFetch(url, options);
            ''', False),
            ("snapshot_timeout", '''
              const realFetch = window.fetch;
              window.fetch = (url, options) => String(url).endsWith("/bundle.json")
                ? new Promise((resolve, reject) => options.signal.addEventListener("abort", () => reject(new Error("timeout"))))
                : realFetch(url, options);
            ''', False),
            ("retry", '''
              const realFetch = window.fetch;
              let first = true;
              window.fetch = (url, options) => {
                if (String(url).endsWith("/bundle.json") && first) {
                  first = false;
                  return Promise.resolve(new Response("unavailable", {status: 503}));
                }
                return realFetch(url, options);
              };
              const retry = setInterval(() => {
                const button = document.querySelector("#view-root button");
                if (button) { clearInterval(retry); button.click(); }
              }, 100);
            ''', True),
        ]
        for name, setup, succeeds in cases:
            index.write_text(original.replace("</head>", f"<script>{setup}</script></head>"), encoding="utf-8")
            result = subprocess.run(
                [chrome, "--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
                 f"--user-data-dir={tmp_path / name}", "--virtual-time-budget=20000", "--dump-dom",
                 f"http://127.0.0.1:{server.server_port}/"],
                capture_output=True, text=True, timeout=30, check=True,
            )
            assert "Booting the Arena" not in result.stdout, name
            if succeeds:
                assert "NIRS pipeline meta-analysis" in result.stdout, name
            else:
                assert "The benchmark data could not be loaded" in result.stdout, name
                assert ">Retry</button>" in result.stdout, name
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
