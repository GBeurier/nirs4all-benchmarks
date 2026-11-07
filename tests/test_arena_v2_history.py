"""Explicit full frozen-pack qualification; excluded from source distributions."""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from nirs4all_benchmarks.frozen_history import (
    RECORDS_SHA256,
    SOURCE_ARCHIVE_SHA256,
    SOURCE_MANIFEST_SHA256,
    FrozenHistoryRefusal,
    _convert_record,
    convert_frozen_history,
    strict_gzip,
    verified_source,
)
from nirs4all_benchmarks.history import ingest_history, load_history_exports
from nirs4all_benchmarks.store import ArenaStore, Queries

SOURCE = Path(__file__).resolve().parents[1] / "data/catalog-history-2026-09"


def test_full_pack_in_one_transaction_remains_unranked_and_restricted(tmp_path, monkeypatch):
    output = tmp_path / "converted"
    report = convert_frozen_history(
        SOURCE, output, progress=lambda n, total: print(f"converted {n}/{total}", flush=True)
    )
    assert report["output"]["records_sha256"] == RECORDS_SHA256
    assert convert_frozen_history(SOURCE, output) == report  # Identical publication is a no-op.
    metadata, exports = load_history_exports(output)
    assert len(exports) == 10824
    _, source_lines = verified_source(SOURCE)
    for ordinal, (line, converted) in enumerate(zip(source_lines, exports, strict=True)):
        original = json.loads(line)
        assert [o["metric_value"] for o in converted["scores"]["observations"]] == [
            o["metric_value"] for o in original["scores"]["observations"]
        ]
        source = converted["provenance"]["frozen_source"]
        assert source["ordinal"] == ordinal and source["record_sha256"] == hashlib.sha256(line).hexdigest()
        assert source["source_claims"]["leakage_attestation"] == original["leakage_attestation"]
        assert converted["provenance"]["historical_source"] == original["provenance"]["historical_source"]
    assert sum(len(e["scores"]["observations"]) for e in exports) == 32286
    assert sum(e["dataset"]["visibility"] == "restricted" for e in exports) == 5826
    assert all(all(v is None for v in e["leakage_attestation"].values()) for e in exports)
    assert all(e["execution"]["terminal_status"] is None for e in exports)
    assert all(
        o["aggregation_level"] == "unreported" and o["fold_id"] is None and o["source_fold_id"] is None
        for e in exports
        for o in e["scores"]["observations"]
    )
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_history(
            store,
            output,
            _loaded=(metadata, exports),
            progress=lambda n, total: print(f"staged {n}/{total}", flush=True),
        )
        assert result["statuses"] == {"committed": 10824}
        assert store.count("executions") == store.count("metadata_receipts") == 10824
        assert store.count("metric_observations") == 32286
        assert store.query_one("SELECT COUNT(*) AS n FROM v_ranked_metrics")["n"] == 0
        q = Queries(store)
        assert q.leaderboard(scope="test")["rows"] == []
        unknown = q.unknown_context_rows(limit=50000)
        assert unknown["ranked"] is False
        assert len(unknown["rows"]) > 0
        assert all(r["visibility"] == "unreported" for r in unknown["rows"])
        assert store.query_one("SELECT COUNT(*) AS n FROM executions WHERE visibility='restricted'")["n"] == 5826
        assert list(store.exports_dir.iterdir()) == []
    assert hashlib.sha256((SOURCE / "manifest.json").read_bytes()).hexdigest() == SOURCE_MANIFEST_SHA256
    assert hashlib.sha256((SOURCE / "runs.jsonl.gz").read_bytes()).hexdigest() == SOURCE_ARCHIVE_SHA256
    # Reuse the genuinely validated full buffer to inject a publication durability
    # failure, without inventing a tiny fake history pack or revalidating records.
    import os

    import nirs4all_benchmarks.frozen_history as frozen

    records = strict_gzip((output / "runs.jsonl.gz").read_bytes())
    monkeypatch.setattr(frozen, "_prepare", lambda *args, **kwargs: (metadata, records))
    original_fsync = os.fsync
    calls = 0

    def fail_parent(fd):
        nonlocal calls
        calls += 1
        if calls == 4:
            raise OSError("injected parent durability failure")
        return original_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_parent)
    with pytest.raises(OSError, match="injected parent durability failure"):
        convert_frozen_history(SOURCE, tmp_path / "failed-publication")
    assert not (tmp_path / "failed-publication").exists()
    assert list(tmp_path.glob(".frozen-history-*")) == []


@pytest.mark.parametrize("mutate", [lambda b: b + b"garbage", lambda b: b + gzip.compress(b"other\n")])
def test_gzip_refuses_tail_and_multiple_members(mutate):
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_gzip_invalid"):
        strict_gzip(mutate(gzip.compress(b"{}\n")))


@pytest.mark.parametrize("value", [b"{}", b"{}\n\n", b"\n{}\n"])
def test_jsonl_requires_single_final_newline_without_blanks(value):
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_jsonl_invalid"):
        strict_gzip(gzip.compress(value))


def test_source_tamper_symlink_exact_keys_and_strict_json(tmp_path):
    metadata, lines = verified_source(SOURCE)
    assert metadata["executions"] == 10824
    line = json.loads(lines[0])
    line["cv"]["extra"] = None
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_field_gap:cv"):
        _convert_record(json.dumps(line).encode(), 0)
    for bad in (b'{"a":1,"a":2}', b'{"a":NaN}'):
        with pytest.raises(ValueError):
            _convert_record(bad, 0)
    root = tmp_path / "tampered"
    root.mkdir()
    (root / "manifest.json").write_bytes((SOURCE / "manifest.json").read_bytes() + b" ")
    (root / "runs.jsonl.gz").write_bytes((SOURCE / "runs.jsonl.gz").read_bytes())
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_source_hash_mismatch"):
        convert_frozen_history(root, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()
    (root / "manifest.json").unlink()
    (root / "manifest.json").symlink_to(SOURCE / "manifest.json")
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_unsafe_input"):
        convert_frozen_history(root, tmp_path / "refused")
    output = tmp_path / "exists"
    output.mkdir()
    (output / "manifest.json").write_text("{}")
    with pytest.raises(FrozenHistoryRefusal, match="frozen_history_output_exists"):
        convert_frozen_history(SOURCE, output)
    assert sorted(p.name for p in output.iterdir()) == ["manifest.json"]
