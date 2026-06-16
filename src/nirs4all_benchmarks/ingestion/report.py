"""REPORT — the human-and-machine-readable clean report of an ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CleanReport:
    """What an ingestion did: dedup hits, dropped fields, degraded keys, quarantines."""

    source: str = "ArenaRunExport"
    new_dimensions: dict[str, int] = field(default_factory=dict)
    deduped_dimensions: dict[str, int] = field(default_factory=dict)
    facts_written: dict[str, int] = field(default_factory=dict)
    dropped_fields: list[str] = field(default_factory=list)
    issues: list[dict[str, str]] = field(default_factory=list)
    residual_key: str = "sample_id"
    residual_rows: int = 0
    residuals_truncated: bool = False
    scores_recomputed: bool = False
    notes: list[str] = field(default_factory=list)

    def record_upsert(self, table: str, inserted: bool) -> None:
        target = self.new_dimensions if inserted else self.deduped_dimensions
        target[table] = target.get(table, 0) + 1

    def record_fact(self, table: str, n: int = 1) -> None:
        self.facts_written[table] = self.facts_written.get(table, 0) + n

    def to_json(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "new_dimensions": self.new_dimensions,
            "deduped_dimensions": self.deduped_dimensions,
            "facts_written": self.facts_written,
            "dropped_fields": self.dropped_fields,
            "issues": self.issues,
            "residual_key": self.residual_key,
            "residual_rows": self.residual_rows,
            "residuals_truncated": self.residuals_truncated,
            "scores_recomputed": self.scores_recomputed,
            "notes": self.notes,
        }
