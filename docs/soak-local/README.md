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

This plan is not SOAK-001 or PERF-002 closure. It has no representative user
corpus, sustained-duration run, descendant-process accounting, frozen performance
budget, or Windows/macOS qualification. The prepared Studio `backend-dist/` must
already exist and match the recorded runtime identity; missing artifacts fail
closed.
