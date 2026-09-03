# SEC-001 / SOAK-001 / PERF-002 local hardening evidence

The bounded WSL campaign advanced all three lots without closing them. Existing
repository harnesses found no product crash, store corruption, handle-lifecycle
failure or security finding in the selected cases. Three stale test fixtures
were repaired; no product behavior changed.

| Surface | Existing harness result | Scope |
|---|---:|---|
| Methods | pass | 10 hostile C-ABI cases and 3 threading cases in the CTest executable |
| Formats | 6 pass | named reader refusals; not fuzzing |
| Core | 49 pass | Archive V1/V2/V3 validation, tamper and bounded-input paths |
| DAG-ML | 11 pass | handle release, parallel scheduler, cache tamper and two release perf probes |
| Python | 22 pass | native Session ownership plus SQLite/ArrayStore concurrency |
| Studio | 8 pass | bounded/tamper-evident CPython host plus concurrent job registry |
| Tools | 70 pass | hostile `.n4a`, immutable source/path policy and real conversion goldens |

The machine-readable evidence, exact candidates, commands and observed test
durations are in
[`hardening-report.v1.json`](hardening-report.v1.json). The existing PERF-001
four-surface timings remain in
[`../performance-compare/performance-report.v1.json`](../performance-compare/performance-report.v1.json).

This report is intentionally `release_eligible=false`. Clang 16 is absent on
the local host, so the official Methods ASan/UBSan/TSan lanes were not run.
Formats has no executable fuzz harness or adversarial corpus yet. No sustained
user-corpus, RSS, file-descriptor or handle-count soak was run, performance
budgets are not globally approved, and the Linux/macOS/Windows release matrices
remain external holds.
