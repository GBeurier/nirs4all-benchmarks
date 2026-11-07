"""Run original C1–C4 reviewer cases without rebuilding a site or editing the probe."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from nirs4all_benchmarks.site_build import _build_bundle
from nirs4all_benchmarks.store import Queries


def main() -> None:
    folds_mode = len(sys.argv) == 3 and sys.argv[1] == "--folds"
    workspace_mode = len(sys.argv) == 3 and sys.argv[1] == "--workspace-folds"
    candidate = sys.argv[2] if folds_mode or workspace_mode else sys.argv[1] if len(sys.argv) == 2 else None
    if candidate is None or not Path(candidate).is_file():
        print("SKIP: external historical probe not supplied; use --fold-reviewer-probe for the bundled T1/T2 checks.")
        return
    probe = Path(candidate)
    source = probe.read_text(encoding="utf-8")
    print("Reviewer probe SHA-256:", hashlib.sha256(source.encode()).hexdigest(), flush=True)
    namespace = {"__name__": "__main__", "__file__": str(probe)}
    observed = {}

    def execute(text):
        exec(compile(text, str(probe), "exec"), namespace)

    def section(start, end):
        if source.count(start) != 1 or source.count(end) != 1:
            raise RuntimeError("Reviewer section boundaries changed; source was not rewritten")
        return source.split(start, 1)[1].split(end, 1)[0]

    try:
        if workspace_mode:
            scratch = Path(os.environ["TMPDIR"]).resolve(strict=True)
            original_mkdtemp = tempfile.mkdtemp

            def isolated_mkdtemp(*args, **kwargs):
                kwargs["dir"] = str(scratch)
                return original_mkdtemp(*args, **kwargs)

            # Execute the source and case calls unchanged; redirect its explicit
            # external temp parent into the guard's measured, disposable scratch.
            with patch("tempfile.mkdtemp", isolated_mkdtemp):
                execute(source)
            print("Original workspace-fold diagnostics completed in guarded scratch.", flush=True)
            return
        if folds_mode:
            if source.count("# C2\n") != 1:
                raise RuntimeError("Reviewer T1/T2 boundary changed; source was not rewritten")
            execute(source.split("# C2\n", 1)[0])
            print("Original T1/T2 diagnostics completed; C2–C4 and site rendering were not run.", flush=True)
            return
        execute(source.split("# B1 all outputs", 1)[0])

        def show(tag, value):
            observed[tag] = value
            print(f"[{tag}] {value}", flush=True)

        namespace["show"] = show
        # The original public fixture contains PARAM-PATH. Exercise packing in
        # memory; do not execute the subsequent site-rendering/output statements.
        execute(section("# B1 all outputs", '\nout = site(s, "B1")'))
        store = namespace["s"]
        bundle = _build_bundle(store, Queries(store))
        assert "/home/u/PARAM-PATH" not in json.dumps(bundle)
        show("C2 backend pack", {"parameter_path_present": False, "metric_rows": bundle["tables"]["metrics"]["n"],
                                  "parameters": bundle["parameters"], "site_rebuilt": False})
        store.close()

        execute(section("# B1e FK on card", "# B2\n"))
        execute(section("# B3\n", "# B4\n"))
        execute(section("# B4\n", "# B5\n"))
        citation = observed["B1e citation_id FK"]
        assert isinstance(citation, tuple) and citation[0] == "rejected" and "metadata_not_indexable" in citation[3]
        for case in ("cv-scope 2of5", "cv-scope+agg"):
            assert observed["B3 " + case][:2] == ("rejected", "reported")
        assert observed["B3 all ok"][0] == "metadata_valid"
        assert observed["B3 incomplete keeps"][:2] == ("metadata_valid", "reported")
        assert observed["B4 cross-run join"] == (True, 10.0, 7.0, "=> residual", 3.0)
        assert observed["B4 LOO n=1"] == ("committed", 7.0, [3.0])
        assert not list(namespace["TMP"].glob("out*"))
        print("Targeted original production cases completed; site rendering, B2/B5 and history replay were not run.", flush=True)
    finally:
        store = namespace.get("s")
        if store is not None:
            store.close()
        root = namespace.get("TMP")
        prefix = "arena-d-" if workspace_mode else "arena-c-" if folds_mode else "arena-rw-"
        parent = Path(os.environ["TMPDIR"]).resolve() if workspace_mode else Path("/var/tmp")
        if (isinstance(root, Path) and root.parent.resolve() == parent
                and root.name.startswith(prefix) and root.exists()):
            shutil.rmtree(root)
            print("Removed isolated reviewer-probe root:", root, flush=True)


if __name__ == "__main__":
    main()
