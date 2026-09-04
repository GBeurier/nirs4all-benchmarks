# Bounded local soak probe

`soak-plan.v1.json` is a short, reusable WSL-local process probe. It runs the
already-built Studio Rust sidecar readiness command three times and verifies the
exact runtime files after every invocation. The runner records wall duration,
per-command latency, direct-process peak RSS, direct-process peak file descriptor
count, exit status, and bounded stdout/stderr hashes.

Run it from an installed or editable checkout:

```bash
n4a-benchmarks soak-run \
  --plan docs/soak-local/soak-plan.v1.json \
  --workspace-root /path/to/nirs4all-workspace \
  --json-out /tmp/n4a-soak-report.json
```

The harness never invokes a shell. Commands, working directories, expected exit
codes, timeouts, and integrity roles are declared in the plan. It stops at the
first failure. Reports use sorted JSON, normalized UTC timestamps, bounded output
digests, and always retain `release_eligible: false`.

The runner samples the whole process group, so wrapper scripts and their worker
processes contribute to peak RSS, open-descriptor and process-count receipts. A
command also fails if it returns while a process-group member is still running.

This plan is not SOAK-001 or PERF-002 closure. It has no representative user
corpus, sustained-duration run, descendant-process accounting, frozen performance
budget, or Windows/macOS qualification. The prepared Studio `backend-dist/` must
already exist and match the recorded runtime identity; missing artifacts fail
closed.

`soak-report.current-head.v1.json` records the 2026-09-03 probe against Studio
`86d5e5033d62240815e532038b6e769b14b25c2b`: three readiness invocations and
three content-integrity checks passed. It remains `release_eligible: false` for
the holds above and is not evidence of a sustained or representative soak.

## R3 functional campaign (prepared, not yet run)

`soak-plan.r3-functional.v1.json` is the next candidate campaign. It runs three
isolated cycles of the four canonical public user examples, the public native
tuning/conformal and run/export/predict/retrain workspace tests, and the existing
SQLite concurrency/transaction/compaction checks, followed by 30 Studio
Rust-sidecar readiness checks. This is strictly a functional/non-crash campaign.

The current plan pins Python R3 commit
`3567bd4abcaa64443a1946748a579f0803e91889`, while execution uses a dedicated
environment populated from the published `nirs4all==1.0.0rc2` wheel—not an
editable checkout. Prepare it once after RC2 publication:

```bash
uv venv /home/delete/nirs4all/_runtimes/nirs4all-r3-1.0.0rc2 --python 3.12
uv pip install \
  --python /home/delete/nirs4all/_runtimes/nirs4all-r3-1.0.0rc2/bin/python \
  "nirs4all[dev,native]==1.0.0rc2"
/home/delete/nirs4all/_runtimes/nirs4all-r3-1.0.0rc2/bin/python -c \
  "import importlib.metadata as m, nirs4all; assert m.version('nirs4all') == '1.0.0rc2'; assert '_worktrees' not in nirs4all.__file__"
```

The plan invokes each public script through the dedicated wheel environment; the
checkout supplies only the pinned scripts and datasets. Python safe-path mode
plus pytest's importlib mode prevents the source checkout from shadowing the
wheel during direct native and store checks. Each example receives a distinct
`NIRS4ALL_WORKSPACE` under `_receipts/soak-r3-functional/pass-N/`.

Studio is pinned to candidate commit
`89b5278a47ae4d38d6b508fabdd6e712f96942c0`. Its packaged runtime checksum must
still be refreshed from the final matrix artifact because the sidecar embeds the
new Python source identity. After that matrix is green and RC2 is published, run
exactly:

```bash
n4a-benchmarks soak-run \
  --plan docs/soak-local/soak-plan.r3-functional.v1.json \
  --workspace-root /home/delete/nirs4all \
  --json-out /tmp/n4a-soak-r3-functional.v1.json
```

Allow roughly 20–60 minutes on a normal research workstation. The fixed
repetition counts are the acceptance unit; elapsed time is evidence, not a reason
to add arbitrary cycles. Keep these receipts together:

- the sorted JSON report and its SHA-256;
- the executed plan and its SHA-256;
- the command output hashes embedded in the report and the optional console log;
- the twelve isolated public-example workspaces under `_receipts/`;
- the installed RC2 version check plus the environment's `uv pip freeze` output;
- the Python and Studio commit/tree identities plus `studio-runtime.sha256`;
- the final CI matrix URLs and published-version identities when they exist.

Even after a green local campaign, `release_eligible` remains false until the
Studio identity is final, release matrices are green, and the intended artifacts
are published. PERF-002 budgets remain a separate approval decision; this report
only supplies measured latency/RSS/FD/process-count observations.
