"""Artifact-driven qualification for the native-backend release train.

The qualification runner is deliberately independent from sibling source trees.
Every runtime and input reaches it through an absolute, content-addressed path.
"""

from __future__ import annotations

from nirs4all_benchmarks.qualification.contract import (
    COMPONENTS,
    QUALIFICATION_MANIFEST_SCHEMA_VERSION,
    QUALIFICATION_REPORT_SCHEMA_VERSION,
    ComponentOutput,
    QualificationManifest,
    QualificationReport,
)
from nirs4all_benchmarks.qualification.runner import load_manifest, run_qualification

__all__ = [
    "COMPONENTS",
    "QUALIFICATION_MANIFEST_SCHEMA_VERSION",
    "QUALIFICATION_REPORT_SCHEMA_VERSION",
    "ComponentOutput",
    "QualificationManifest",
    "QualificationReport",
    "load_manifest",
    "run_qualification",
]
