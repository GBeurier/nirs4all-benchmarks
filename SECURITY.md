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

The local project-token service requires the independent Access verifier and is limited
to loopback, a single process and startup snapshots. It is not a sandbox or a classifier
for secrets, weights or personal data in opaque client-supplied metadata. Authorized
project readers receive the submitted metadata as data; client paths and URLs are never
followed. The unauthenticated offline service must not be exposed to a network. Project
availability is shared through the process-wide store lock. Direct ASGI hosts and proxies
must protect their own logs.

Arena store files require operator-controlled access. Unkeyed receipt and metadata hashes detect some partial inconsistencies; they do not provide tamper resistance against a writer who can consistently rewrite stored facts and hashes.

Private project receipts retain explicitly allowlisted user-declared labels verbatim, without automatic path redaction. The public publication gate remains closed.

The private context card and its local digest do not authenticate a coherent
rewrite of a store. The offline API is for the trusted local store owner: its
first-accepted shared labels can originate in private exports, while the new
per-export card is not exposed by that API. Authenticated project readers see
only their project's per-export card. Public projections use selected public
receipts, not shared labels; the publication gate remains closed. Do not publish
offline output by hand.
