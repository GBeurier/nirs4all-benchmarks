"""Pinned conversion qualification in scratch only, with independent hash seeds."""

from __future__ import annotations
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    if sys.argv[1:2] == ["--child"]:
        from nirs4all_benchmarks.frozen_history import _prepare

        destination = Path(sys.argv[2])
        destination.mkdir()
        _, records = _prepare(
            ROOT / "data/catalog-history-2026-09",
            progress=lambda done, total: print(f"validated {done}/{total}", flush=True),
        )
        digest = hashlib.sha256(records).hexdigest()
        with (destination / "runs.jsonl.gz").open("wb") as file:
            with gzip.GzipFile(fileobj=file, filename="", mtime=0, mode="wb") as archive:
                archive.write(records)
        (destination / "result.json").write_text(json.dumps({"records_sha256": digest}))
        print("records_sha256", digest, flush=True)
        return
    with tempfile.TemporaryDirectory(prefix="v2-history-bootstrap-") as directory:
        scratch = Path(directory)
        digests = []
        for index, seed in enumerate(("17", "91")):
            destination = scratch / f"path-{index}"
            env = {**os.environ, "PYTHONHASHSEED": seed}
            subprocess.run([sys.executable, "-B", __file__, "--child", str(destination)], env=env, check=True)
            digests.append(json.loads((destination / "result.json").read_text())["records_sha256"])
        assert digests[0] == digests[1]
        from nirs4all_benchmarks.frozen_history import RECORDS_SHA256
        assert digests[0] == RECORDS_SHA256, "Bootstrap differs from the reviewed output pin"
        print("Two independent paths/hash seeds agree:", digests[0], flush=True)


if __name__ == "__main__":
    main()
