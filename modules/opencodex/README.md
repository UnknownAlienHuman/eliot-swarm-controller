# OpenCodex provider-service module — attach-only observer (first slice)

OpenCodex (`lidge-jun/opencodex`, pinned **v2.73.0**, commit
`569e3e7dae48bafc54b8a1a7e3a85129befe2d98`, MIT) is a provider/protocol
proxy with an authenticated Management API. It is **not** a session
owner and not a replacement for the native Codex lifecycle: execution —
threads, turns, goals, steering, replies, resume, family — stays with
the native Codex backend (C07, `modules/codex/`). This module attaches
to an explicitly configured, already-running OpenCodex service and
performs **read-only** Management reads, feeding compact, honest facts
into reports and `doctor.inspect`. It is disabled by default: the
shipped route example is `enabled = false`, and the module config lives
outside the repository.

Artifact: **`opencodex-2.73.0-bridge.1`** — Node.js, standard library
only, no dependencies, no vendored upstream code.

## What it does

- `describe` — entrypoint, upstream pin, capability matrix, observed
  service identity after an attach.
- `open` / `attach` — probe only: `GET /api/system/health` then
  `GET /api/system/memory`. Attach creates nothing; the service binding
  identity is *endpoint + observed pid + observed version* (the pid is
  an observation, never an identity to act on).
- `snapshot` — one bounded JSON object assembled from the read-only
  Management reads: system health (with the pinned spend-ledger
  scalars), system memory, providers, per-provider protocols, models
  and usage aggregates. Every section degrades independently to
  `{state: "unknown", reason}`; a failed read never fabricates an
  empty healthy list and never fails the whole snapshot.
- `shutdown` — **detach only**: the HTTP client is dropped and the
  token resolution forgotten. The adapter never calls `POST /api/stop`,
  `POST /api/system/restart` or `POST /api/system/codex-restart`, never
  signals the service pid, and a controller restart does not touch the
  service. The proxy is shared and externally owned.

## What it does not do

- `send`, `configure`, `reply` are reported **unavailable**, never
  emulated. Configuration mutations (protocol settings, model settings,
  sub-agent surface, client-integration apply/rollback) are a later
  slice behind saved Operations with preview/`planFingerprint`
  readback; nothing in this artifact writes to the service. The client
  issues **GET requests only** — there is no code path that can emit
  another method, and reads never start, restart, install or
  reconfigure the service.
- No owned launch: the service is started and owned by the operator
  (`lifecycleOwner: "external"` is the only value this artifact
  accepts; a binding claiming ELIOT owns the lifecycle is refused).
- No account-pool management (`/api/codex-auth/*` is out of scope), no
  transcript duplication (`/api/logs` is not read), no metrics scrape.

## Configuration

Copy `module.example.json` outside the repository and keep it
untracked. Endpoint, expected version, credential reference and
lifecycle owner are four separate facts:

- `endpoint` — the existing service (documented default listener
  `http://127.0.0.1:10100`; always operator-configured, never assumed).
- `expectedVersion` — compared by exact string. A mismatch is
  readiness `unknown` with the observed version recorded — not an
  attach failure and not a pass.
- `adminTokenEnv` — the **name** of the environment variable holding
  the service's Management (admin) credential. That credential is
  separate from any data-plane/proxy admission key and from the Codex
  app-server token. The token is read at attach time, held in memory
  only, sent only as `X-OpenCodex-API-Key`, and never passed to
  Codex/model tools, never persisted in a Task/Operation, never
  printed. Loopback dashboard sessions and remote pairing are GUI
  mechanisms and are never scraped or reused.
- `lifecycleOwner` — `"external"`.

Run one operation:

```sh
node bridge.mjs --config /path/to/module.json snapshot
```

## Honest labelling of routed models

A routed model's label is built only from evidence (issue #1, audit §D):

- **Provider** — the opencodex provider id, with `adapter` and
  `adapterSource` from `GET /api/protocols?provider=`.
- **Model** — the client-facing id as requested; `wireModel` and
  `servedModel` only where an upstream usage record provides them.
  Absent `servedModel` stays absent — never backfilled from the
  requested id or from a model's self-description.
- **Billing** — derived **only** from the provider's `authMode`:
  `forward` → the ChatGPT plan behind the caller's Codex login;
  `oauth` → that provider account's subscription; `key` → the key's
  own account. `estimatedCostUsd` is shown only with the qualifier
  "configured-pricing estimate — not an invoice or subscription
  charge".

**The Max trap:** a Claude model routed through the `anthropic`
provider with `authMode: "oauth"` spends the operator's Claude
subscription *via OpenCodex's stored login*. It is **not** the
controller's native Claude route and **not** a "Claude Max route";
upstream reports quota windows but no subscription tier, so no label
here claims Pro/Max. The native Max route remains `modules/claude/`'s
own; the two never share an alias, a route record or a doctor line.
Symmetrically, bare native GPT ids on the `openai` forward lane are
the ChatGPT-plan native lane, and session affinity (thread → account)
is upstream-managed by OpenCodex — ELIOT neither sets, verifies nor
moves it.

Snapshot semantics worth restating: `activeTurnCount` is the proxy's
own in-flight inference count; **`activeTurnCount = 0` is not proof
that all native Codex children stopped**. `isDraining` reports an
upstream-owned drain; observing it never authorises a restart.
Upstream conflicts and failures (401, 403 origin gate, 409
`sibling_instance`, 503 fail-closed or `catalog_busy`, malformed JSON,
missing fields) are recorded as `unknown` with the raw status/code
preserved as evidence.

## Doctor surface

`doctor.inspect` never calls the service. When an OpenCodex binding
records a snapshot through the normal module path, doctor projects the
newest recorded snapshot per binding into `services.opencodex[]`
(endpoint, observed/expected version, pid, uptime, active turns,
draining, RSS, continuation-retention scalars, provider labels, usage
incompleteness, staleness) and raises addressed findings
(`opencodex_unobserved`, `opencodex_stale`,
`opencodex_version_mismatch`, `opencodex_unavailable`). No finding ever
recommends restarting the shared service; the next step points at the
operator/service owner. With no OpenCodex binding recorded, the
section is absent, not empty.

## Fixtures and self-test

Fixtures under `fixtures/` are **reconstructions from the pinned
upstream contracts** (docs-site Management API reference and
`src/server/management/system-routes.ts` at the pin), each carrying its
provenance — they are **not** captures from a live 2.73.0 service, and
no live service has been installed or qualified by this slice
(Windows behaviour likewise unverified).

```sh
node selftest.mjs   # fake Management server, 11 fixture tests, no network
```

The load-bearing tests: a full attach+snapshot cycle issues **GET
requests only**; the admin token appears in no snapshot and no bridge
stdout; detach leaves the fake service answering; zero data-plane
(`/v1/*`) requests are made — small health reads consume no model call.

## Activation and rollback

Activation is a new binding: copy the example config out, name the
token env var, and record snapshots through the module seam. Disabling
or removing the module/binding leaves the baseline Muse, OpenCode
and Codex routes unchanged and leaves the OpenCodex service itself
untouched — rollback is removing the binding, nothing more. See
[UPDATE.md](UPDATE.md) for the pinned update procedure.
