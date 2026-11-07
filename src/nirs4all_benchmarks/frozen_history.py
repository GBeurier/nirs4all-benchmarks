"""One explicit, pinned frozen-reference conversion. Never a legacy runtime reader."""

from __future__ import annotations

import copy
import ctypes
import errno
import gzip
import hashlib
import io
import os
import shutil
import stat
import tempfile
import zlib
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.identity import canonical_json
from nirs4all_benchmarks.identity.hashing import is_hash
from nirs4all_benchmarks.ingestion.offline import validate_metadata
from nirs4all_benchmarks.scoring import METRIC_DIRECTION

SOURCE_MANIFEST_SHA256 = "46fcab13599f366ee3db586ba328d117b6d7a478351cd90c7191cdc3fb71e37e"
SOURCE_ARCHIVE_SHA256 = "e04427694d39d9815c41fefc87a5d20d425b0b28a78bfcdb2f7edb29b709abe7"
RECORDS_SHA256 = "d29af7685161345775d69ac03a1c0f86120a7442bb99cc7921461e67176d4945"
# Verified with PYTHONHASHSEED=17 and 91, two distinct temporary output paths.
CONVERTER_ID = "nirs4all-benchmarks.frozen-history/2"
MAPPING_VERSION = "arena.frozen-history.c1-c9/1"
COUNTS = (10824, 32286)
FIELD_MAPPING = {
    "leakage_attestation": "null; original claims retained",
    "execution.status": "unreported; original ok retained",
    "scores.observations.aggregation_level": "unreported; original sample retained",
    "scores.observations.scope/partition": "test/test preserved",
    "scores.observations.direction": "known registry direction",
    "cv.nested/within_train_only/none": "unreported; source claims retained",
    "cv.train_only_oof/n_folds/split": "preserved",
    "residuals": "no key or residual claim; original retained",
    "visibility.restricted": "restricted/local_policy",
    "visibility.public": "unreported; public claim retained",
    "identity_basis": "source_declared_unverified",
    "pipeline.source_method/source_csv": "moved verbatim to frozen_source.pipeline",
}
_EXPECTED = {
    "root": [
        {
            "arena_export_schema_version",
            "cv",
            "dataset",
            "dataset_variant",
            "execution",
            "leakage_attestation",
            "pipeline",
            "producer",
            "provenance",
            "refit",
            "residuals",
            "rng",
            "scores",
            "split",
            "task",
        }
    ],
    "cv": [{"method", "n_folds", "nested", "within_train_only"}, {"method", "nested", "within_train_only"}],
    "dataset": [{"dataset_card", "dataset_fingerprint", "n_features", "n_samples", "visibility"}],
    "dataset_variant": [{"dataset_variant_hash", "variant_spec"}],
    "execution": [{"execution_id", "os", "status"}],
    "leakage_attestation": [{"group_leakage_checked", "nested_cv_safe", "oof_enforced", "unsafe_flags"}],
    "pipeline": [{"graph", "human_label", "nodes", "source_csv", "source_method"}],
    "producer": [{"capsule"}],
    "provenance": [{"historical_source"}],
    "refit": [{"params", "selection_scope", "strategy", "train_scope"}, {"params", "strategy", "train_scope"}],
    "residuals": [{"key", "pseudonymized", "publishable"}],
    "rng": [{"determinism_flags", "framework_seeds", "root_seed"}],
    "scores": [{"aggregation_policy", "metric_implementation", "observations", "score_version"}],
    "split": [{"method", "params", "partition_summary"}],
    "task": [{"encoding", "target_name", "task_hash", "task_type"}],
    "observation": [
        {"aggregation_level", "direction", "metric_name", "metric_value", "n_samples", "partition", "scope"}
    ],
}
_MANIFEST_KEYS = {
    "archive_sha256",
    "by_method",
    "by_task_type",
    "collection_id",
    "datasets",
    "derived_metrics",
    "description",
    "excluded_datasets",
    "executions",
    "metric_observations",
    "name",
    "protocol_evidence",
    "schema_version",
    "seeds",
    "selection",
    "selection_policy",
    "sources",
    "tasks",
}


class FrozenHistoryRefusal(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _read(path: Path) -> bytes:
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 128_000_000:
            raise FrozenHistoryRefusal("frozen_history_unsafe_input")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NOATIME", 0)
        with os.fdopen(os.open(path, flags), "rb") as stream:
            result = stream.read(128_000_001)
    except OSError as exc:
        raise FrozenHistoryRefusal("frozen_history_unsafe_input") from exc
    try:
        after = path.lstat()
    except OSError as exc:
        raise FrozenHistoryRefusal("frozen_history_source_changed") from exc
    if (info.st_ino, info.st_size, info.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
        raise FrozenHistoryRefusal("frozen_history_source_changed")
    return result


def strict_gzip(data: bytes) -> bytes:
    decompressor = zlib.decompressobj(31)
    try:
        result = decompressor.decompress(data, 128_000_001)
    except zlib.error as exc:
        raise FrozenHistoryRefusal("frozen_history_gzip_invalid") from exc
    if len(result) > 128_000_000 or not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
        raise FrozenHistoryRefusal("frozen_history_gzip_invalid")
    if not result.endswith(b"\n") or result.endswith(b"\n\n") or any(not line for line in result.split(b"\n")[:-1]):
        raise FrozenHistoryRefusal("frozen_history_jsonl_invalid")
    return result


def verified_source(directory: str | Path) -> tuple[dict[str, Any], list[bytes]]:
    root = Path(directory)
    metadata_bytes, archive_bytes = _read(root / "manifest.json"), _read(root / "runs.jsonl.gz")
    if (
        hashlib.sha256(metadata_bytes).hexdigest() != SOURCE_MANIFEST_SHA256
        or hashlib.sha256(archive_bytes).hexdigest() != SOURCE_ARCHIVE_SHA256
    ):
        raise FrozenHistoryRefusal("frozen_history_source_hash_mismatch")
    metadata = strict_loads(metadata_bytes)
    if (
        set(metadata) != _MANIFEST_KEYS
        or metadata["archive_sha256"] != SOURCE_ARCHIVE_SHA256
        or (metadata["executions"], metadata["metric_observations"]) != COUNTS
    ):
        raise FrozenHistoryRefusal("frozen_history_manifest_invalid")
    lines = strict_gzip(archive_bytes).split(b"\n")[:-1]
    if len(lines) != COUNTS[0]:
        raise FrozenHistoryRefusal("frozen_history_count_mismatch")
    return metadata, lines


def _keys(name: str, value: dict[str, Any]) -> None:
    if set(value) not in _EXPECTED[name]:
        raise FrozenHistoryRefusal("frozen_history_field_gap:" + name)


def _convert_record(line: bytes, ordinal: int) -> ArenaRunExport:
    old = strict_loads(line)
    _keys("root", old)
    if type(old["arena_export_schema_version"]) is not int or old["arena_export_schema_version"] != 1:
        raise FrozenHistoryRefusal("frozen_history_field_gap:version")
    for name in _EXPECTED:
        if name not in {"root", "observation"}:
            _keys(name, old[name])
    result = copy.deepcopy(old)
    result["arena_export_schema_version"] = 2
    result["identity_basis"] = {
        name: {"kind": "source_declared_unverified"}
        for name in ("dataset_fingerprint", "task_hash", "dataset_variant_hash")
    }
    claims = {
        "leakage_attestation": old["leakage_attestation"],
        "execution": {"status": old["execution"]["status"]},
        "dataset": {"visibility": old["dataset"]["visibility"]},
        "residuals": old["residuals"],
        "cv": {k: old["cv"][k] for k in ("nested", "within_train_only")},
        "scores": {
            "observations": [{"aggregation_level": o["aggregation_level"]} for o in old["scores"]["observations"]]
        },
    }
    result["leakage_attestation"] = {}
    result["execution"]["status"] = "unreported"
    result["execution"]["terminal_status"] = None
    visibility = old["dataset"]["visibility"]
    if visibility not in {"public", "restricted"}:
        raise FrozenHistoryRefusal("frozen_history_field_gap:visibility")
    result["dataset"].update(
        visibility="restricted" if visibility == "restricted" else "unreported",
        visibility_source="local_policy" if visibility == "restricted" else "unreported",
    )
    result["cv"].update(nested=None, within_train_only=None)
    if old["cv"]["method"] == "none":
        claims["cv"]["method"] = "none"
        result["cv"]["method"] = "unreported"
    elif old["cv"]["method"] != "train_only_oof":
        raise FrozenHistoryRefusal("frozen_history_field_gap:cv.method")
    result["residuals"] = {"key": None, "pseudonymized": None, "publishable": {}, "inline": None}
    pipeline = {k: result["pipeline"].pop(k) for k in ("source_method", "source_csv")}
    result["provenance"]["frozen_source"] = {
        "format": "arena-run-export/1",
        "archive_sha256": SOURCE_ARCHIVE_SHA256,
        "ordinal": ordinal,
        "record_sha256": hashlib.sha256(line).hexdigest(),
        "source_claims": claims,
        "pipeline": pipeline,
    }
    for observation in result["scores"]["observations"]:
        _keys("observation", observation)
        if observation["scope"] != "test" or observation["partition"] != "test":
            raise FrozenHistoryRefusal("frozen_history_field_gap:score.partition")
        direction = METRIC_DIRECTION.get(observation["metric_name"])
        if direction is None:
            raise FrozenHistoryRefusal("frozen_history_field_gap:score.direction")
        observation.update(
            aggregation_level="unreported",
            fold_id=None,
            source_fold_id=None,
            direction=direction,
            direction_source="registry",
        )
    model = ArenaRunExport.model_validate(result, strict=True)
    validation = validate_metadata(model)
    if validation.status != "metadata_valid":
        raise FrozenHistoryRefusal("frozen_history_field_gap:" + validation.issues[0]["code"])
    return model


def _prepare(directory: str | Path, *, progress: Any = None) -> tuple[dict[str, Any], bytes]:
    metadata, lines = verified_source(directory)
    records = []
    observations = restricted = 0
    for ordinal, line in enumerate(lines):
        model = _convert_record(line, ordinal)
        observations += len(model.scores.observations)
        restricted += model.dataset.visibility == "restricted"
        records.append(canonical_json(model.to_manifest()).encode() + b"\n")
        if progress and (ordinal + 1) % 500 == 0:
            progress(ordinal + 1, len(lines))
    if observations != COUNTS[1] or restricted != 5826:
        raise FrozenHistoryRefusal("frozen_history_count_mismatch")
    return metadata, b"".join(records)


def _publish(source: Path, destination: Path) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    rename = getattr(library, "renameat2", None)
    if rename is None:
        raise FrozenHistoryRefusal("frozen_history_atomic_publish_unsupported")
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise FileExistsError from None
        raise FrozenHistoryRefusal("frozen_history_atomic_publish_failed")


def _existing_output(out: Path) -> dict[str, Any]:
    try:
        existing = strict_loads(_read(out / "manifest.json"))
        validate_pack_manifest(existing)
        archive = _read(out / "runs.jsonl.gz")
        if (
            existing.get("kind") != "arena.frozen-history/2"
            or existing.get("mapping_version") != MAPPING_VERSION
            or existing.get("source", {}).get("manifest_sha256") != SOURCE_MANIFEST_SHA256
            or existing.get("source", {}).get("archive_sha256") != SOURCE_ARCHIVE_SHA256
            or existing.get("output", {}).get("records_sha256") != RECORDS_SHA256
            or hashlib.sha256(strict_gzip(archive)).hexdigest() != RECORDS_SHA256
            or hashlib.sha256(archive).hexdigest() != existing["output"]["archive_sha256"]
        ):
            raise FrozenHistoryRefusal("frozen_history_output_exists")
        return existing
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise FrozenHistoryRefusal("frozen_history_output_exists") from exc


def validate_pack_manifest(value: Any) -> None:
    """Verify this converter's exact v2 envelope, without opening any v1 input."""
    keys = {
        "schema_version",
        "kind",
        "converter_id",
        "mapping_version",
        "converter_source_sha256",
        "source",
        "output",
        "collection_id",
        "name",
        "description",
        "field_mapping",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 2
        or value["kind"] != "arena.frozen-history/2"
        or value["converter_id"] != CONVERTER_ID
        or value["mapping_version"] != MAPPING_VERSION
        or value["field_mapping"] != FIELD_MAPPING
        or not is_hash(value["converter_source_sha256"])
    ):
        raise FrozenHistoryRefusal("unsupported_history_pack")
    if value["source"] != {
        "manifest_sha256": SOURCE_MANIFEST_SHA256,
        "archive_sha256": SOURCE_ARCHIVE_SHA256,
        "executions": COUNTS[0],
        "metric_observations": COUNTS[1],
        "path": "data/catalog-history-2026-09",
    }:
        raise FrozenHistoryRefusal("unsupported_history_pack")
    output = value["output"]
    if (
        not isinstance(output, dict)
        or set(output) != {"records_sha256", "archive_sha256", "executions", "metric_observations"}
        or output["records_sha256"] != RECORDS_SHA256
        or not is_hash(output["archive_sha256"])
        or type(output["executions"]) is not int
        or type(output["metric_observations"]) is not int
        or (output["executions"], output["metric_observations"]) != COUNTS
        or value["collection_id"] != "catalog_history_2026_09"
        or any(not isinstance(value[key], str) for key in ("name", "description"))
    ):
        raise FrozenHistoryRefusal("history_count_or_metadata_mismatch")


def convert_frozen_history(directory: str | Path, out: str | Path, *, progress: Any = None) -> dict[str, Any]:
    if Path(directory).is_symlink():
        raise FrozenHistoryRefusal("frozen_history_unsafe_input")
    source, out = Path(directory).resolve(), Path(out).absolute()
    if (
        source == out
        or source in out.resolve().parents
        or out.is_symlink()
        or any((parent / "arena.sqlite").exists() for parent in (out, *out.parents))
        or any(parent.is_symlink() for parent in out.parents)
    ):
        raise FrozenHistoryRefusal("frozen_history_output_location_invalid")
    if RECORDS_SHA256 is None:
        raise FrozenHistoryRefusal("frozen_history_output_pin_unavailable")
    if out.exists():
        verified_source(source)
        return _existing_output(out)
    metadata, records = _prepare(source, progress=progress)
    digest = hashlib.sha256(records).hexdigest()
    if digest != RECORDS_SHA256:
        raise FrozenHistoryRefusal("frozen_history_output_hash_mismatch")
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, filename="", mode="wb", mtime=0, compresslevel=9) as stream:
        stream.write(records)
    archive = buffer.getvalue()
    manifest = {
        "schema_version": 2,
        "kind": "arena.frozen-history/2",
        "converter_id": CONVERTER_ID,
        "mapping_version": MAPPING_VERSION,
        "converter_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "source": {
            "manifest_sha256": SOURCE_MANIFEST_SHA256,
            "archive_sha256": SOURCE_ARCHIVE_SHA256,
            "executions": COUNTS[0],
            "metric_observations": COUNTS[1],
            "path": "data/catalog-history-2026-09",
        },
        "output": {
            "records_sha256": digest,
            "archive_sha256": hashlib.sha256(archive).hexdigest(),
            "executions": COUNTS[0],
            "metric_observations": COUNTS[1],
        },
        "collection_id": metadata["collection_id"],
        "name": metadata["name"],
        "description": metadata["description"],
        "field_mapping": FIELD_MAPPING,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".frozen-history-", dir=out.parent))
    published = False
    try:
        for name, content in (("runs.jsonl.gz", archive), ("manifest.json", canonical_json(manifest).encode() + b"\n")):
            with (stage / name).open("wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        fd = os.open(stage, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        if (
            hashlib.sha256(_read(source / "manifest.json")).hexdigest() != SOURCE_MANIFEST_SHA256
            or hashlib.sha256(_read(source / "runs.jsonl.gz")).hexdigest() != SOURCE_ARCHIVE_SHA256
        ):
            raise FrozenHistoryRefusal("frozen_history_source_changed")
        try:
            _publish(stage, out)
            published = True
        except FileExistsError:
            return _existing_output(out)
        fd = os.open(out.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        published = False  # Durable publication succeeded; leave the owned output in place.
        return manifest
    finally:
        if published:
            # Roll back our new output if parent durability fails; never replace an
            # existing output. The stage path ceased to exist at publication.
            _publish(out, stage)
        if stage.exists():
            shutil.rmtree(stage)
