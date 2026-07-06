# Security policy

`nirs4all-benchmarks` (the *Arena*) runs reproducible, scored NIRS pipelines over
`nirs4all-datasets` and publishes a static, no-build SPA to **benchmarks.nirs4all.org**.

Security-relevant surface:

- **Inputs it parses** — pipeline recipes / benchmark configs and the read-only dataset catalogue. A
  crafted recipe/config should fail closed with a clean error and must not escape the benchmark store
  or execute unintended code; catalogue access is read-only.
- **Published site** — the generated SPA is static (no server-side code, no user data). Take care that
  scored outputs / provenance embedded in the site do not leak anything private.
- **No secrets.** Runs locally / in CI over public datasets; PyPI publishing uses OIDC Trusted Publishing.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public GitHub issue. Email
**nirs4all-admin@cirad.fr** with the affected version, a description, and reproduction steps.
