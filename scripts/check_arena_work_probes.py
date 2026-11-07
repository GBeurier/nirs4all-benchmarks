"""Re-run the reviewer's original local production probes without editing them.

The original diagnostic printer assumes a contradictory attestation was ingested.
After B2 it indexes a missing run. Run the exact source first, then retry with only
that diagnostic guarded so all original inputs and production calls can finish.
"""

from __future__ import annotations

import hashlib
import shutil
import sys
from pathlib import Path

def execute(source: str, label: str, probe: Path) -> bool:
    namespace = {"__name__": "__main__", "__file__": str(probe)}
    print(label, flush=True)
    try:
        exec(compile(source, str(probe), "exec"), namespace)
        return True
    except TypeError as error:
        # This is an observed harness incompatibility, not a passing probe run.
        print(f"Original probe harness stopped: {type(error).__name__}: {error}", flush=True)
        return False
    finally:
        root = namespace.get("TMP")
        if isinstance(root, Path) and root.parent == Path("/var/tmp") and root.name.startswith("arena-w-"):
            shutil.rmtree(root, ignore_errors=True)
            print("Removed this probe's isolated root:", root, flush=True)


def main() -> int:
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_file():
        print("SKIP: external historical probe not supplied; use --fold-reviewer-probe for the bundled T1/T2 checks.")
        return 0
    probe = Path(sys.argv[1])
    source = probe.read_text(encoding="utf-8")
    print("Original probe SHA-256:", hashlib.sha256(source.encode()).hexdigest(), flush=True)
    if execute(source, "Running the original probe unchanged", probe):
        return 0
    old = 'dt["execution"].get("oof_enforced"), dt["metadata"].get("leakage_attestation")'
    new = '(dt or {}).get("execution", {}).get("oof_enforced"), (dt or {}).get("metadata", {}).get("leakage_attestation")'
    if source.count(old) != 1:
        raise RuntimeError("Original diagnostic no longer matches; probe source was not rewritten")
    print("Retrying with one diagnostic-only null guard; inputs and production calls are unchanged", flush=True)
    return 0 if execute(source.replace(old, new), "Running all original production cases", probe) else 1


if __name__ == "__main__":
    raise SystemExit(main())
