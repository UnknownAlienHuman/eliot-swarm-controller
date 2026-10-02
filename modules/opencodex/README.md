# OpenCodex provider-service module — observer + saved-Operation configuration (slices 1–2)

OpenCodex (`lidge-jun/opencodex`, pinned **v2.73.0**, commit
`569e3e7dae48bafc54b8a1a7e3a85129befe2d98`, MIT) is a provider/protocol
proxy with an authenticated Management API. It is **not** a session
owner and not a replacement for the native Codex lifecycle: execution —
threads, turns, goals, steering, replies, resume, family — stays with
the native Codex backend (C07, `modules/codex/`). This module attaches
to an explicitly configured, already-running OpenCodex service,
observes it through read-only Management reads, and — slice 2 —
executes single, operator-requested configuration changes as saved
Operations with preview/confirmation and readback. It is disabled by
default: the shipped route example is `enabled = false`, and the
module config lives outside the repository.

Artifact: **`opencodex-2.73.0-bridge.2`** — Node.js, standard library
only, no dependencies, no vendored upstream code.

## What it does

- `describe` — entrypoint, upstream pin, capability matrix, observed
  service identity after an attach.
- `open` / `attach` — probe only: `GET /api/system/health` then
  `GET /api/system/memory`. Attach creates nothing; the service binding
  identity is *endpoint + observed pid + observed version* (the pid is
  an observation, never an identity to act on).
- `snapshot` — one bounded JSON object assembled from read-only
  Management reads: system health (with the pinned spend-ledger
  scalars), system memory, providers, per-provider protocols, models,
  usage aggregates, and a `configuration` section (resolved protocol
  settings, per-client integration states, Aside profile aggregates,
  the sub-agent surface state) that serves as the recorded
  applied-evidence doctor projects. Every section degrades
  independently to `{state: "unknown", reason}`; a failed read never
  fabricates an empty healthy list and never fails the whole snapshot.
  Snapshot/attach issue **GET requests only**.
- `preview` / `configure` — one saved Operation per operator request
  file (next section).
- `journal` — read-only listing of the upstream integration journal
  (opIds, kinds, snapshot/undoable facts; file locations are dropped),
  the source an operator picks a restore from.
- `shutdown` — **detach only**: the HTTP client is dropped and the
  token resolution forgotten. The adapter never calls `POST /api/stop`,
  `POST /api/system/restart` or `POST /api/system/codex-restart`, never
  signals the service pid, and a controller restart does not touch the
  service. The proxy is shared and externally owned.

## Configuration Operations (slice 2)

One Operation = one operator-selected change, described by a request
file the operator writes and the controller saves with the printed
record:

```sh
node bridge.mjs --config /path/to/module.json preview   /path/to/request.json
node bridge.mjs --config /path/to/module.json configure /path/to/request.json
```

Every record carries the module-contract block —
`operation_contract` with `effect_scope`, `order_scope`
(`single_change`), `completion_condition`
(`native_configuration_applied`), `replay_policy`
(`readback_only_no_mutation_replay`), `fallback_used: false`,
`contract_revision` and the `application_boundary` — plus the
recorded scope, the requested change, what was sent (at most one
mutation), the normalised receipt, per-element outcomes where the
upstream envelope has them, the readback evidence and a verification
verdict. `saved` and `applied` are separate facts in the record and
are never inferred from each other.

Rules that hold for every kind:

- **Version gate.** A preview/configure runs only against an attached
  service whose observed version matches the pin exactly. Under a
  mismatch the record is `unknown`/`version_mismatch` and **no write
  is sent** — the mutation contracts are pinned to this release.
- **One mutation, never replayed.** An unknown (lost) mutation
  response is reconciled by readback GETs inside the same Operation;
  the mutation itself is never re-sent.
- **Readback decides.** A 200/`ok:true` answer confirms the save only.
  The record's verdict comes from fresh GETs afterwards (declared
  model fields, integration states, surface state).
- **Strict requests.** Unknown fields, wrong types and out-of-range
  values are refused locally with the reason recorded and no request
  sent — including the places where upstream is lax (a subagent
  roster over five, which the pin would silently truncate).

Kinds, in the audit's order:

| Kind | Upstream writer | Notes |
| --- | --- | --- |
| `protocol_settings` | `PATCH /api/protocols/settings` | The only writer in the protocols family. `preview` lists the differing leaves; a `managedMessagesNativeOAuth` without the native lane is refused locally, mirroring the pin's 400. Closing Messages also closes `claudeCode` upstream in the same save — an upstream fact, recorded via readback. |
| `model_settings` | `PUT /api/model-settings` | One routed provider + one exact model per Operation (`openai`/`combo` are refused locally — not routed). Receipt `saved`/`changed`/`hasOverrides`/`catalogRefresh` is recorded; readback compares the stored declarations (`contextWindowDeclared`, `inputModalitiesDeclared`) and lists per-client integration states. Empty `reasoningEfforts: []` is the pin's explicit "no rungs" override, never read as a clear; `null` clears. |
| `subagent_v2` | `PUT /api/v2` | Sub-agent surface settings as one Operation. Applies to **new sessions only**; existing sessions keep their binding/surface. The pin's `enabled`↔mode conflicts are checked locally against the merged state. Upstream warnings are recorded verbatim. |
| `injection_model` | `PUT /api/injection-model` | Effort is validated against the ladder the GET serves before any write. The prompt is operator prose: only set/unset is recorded. |
| `effort_caps` | `PUT /api/effort-caps` | Global + sub-agent ceilings, ladder-validated. `modelPinnedEfforts` is **not** written by this Operation (recorded refusal `model_pinned_efforts_not_in_slice`). |
| `subagent_models` | `PUT /api/subagent-models` | The featured roster (max five — enforced locally). Picker order is not written by this Operation (`picker_order_not_in_slice`). |
| `subagent_model_fallback` | `PUT /api/subagent-model-fallback` | **Upstream's own** stored chain + poll interval. ELIOT implements no fallback logic of its own; this only sets upstream configuration the operator explicitly selected. |
| `client_integration` | preview `POST /api/client-integrations/preview` → `PUT /api/client-integrations/{clientId}` | Apply/disable/overwrite for one client, **only** through the confirm flow below. |
| `client_integration_restore` | preview `POST /api/client-integrations/restore/preview` → `POST /api/client-integrations/restore` | Rollback via the upstream journal (opId from the apply record or `journal`). |
| `aside_profile` / `aside_profile_restore` | preview `POST …/aside/profiles/{id}/preview` → `PUT …/aside/profiles/{id}` / `POST …/aside/profiles/{id}/restore` | The canonical per-profile flow; one profile per Operation. |

### The integration confirm flow

Client-integration changes are the highest-risk surface in the issue,
so they are never sent unbound:

1. `preview` returns the upstream plan — state, `changes[]` (managed
   schema paths and `$snapshot`/`$ownership`/`$journal` markers, never
   file locations or config values), `canApply`, `willChange`, and the
   opaque `fingerprint`.
2. The operator confirms by saving a `configure` request carrying
   `operation` + `planFingerprint` — **both or neither**, enforced
   locally before any request (a missing binding is
   `plan_binding_required`; half a binding is
   `plan_binding_both_or_neither`; a binding naming a different
   operation than the request performs is `plan_operation_mismatch`).
3. Upstream re-plans before writing. If the world moved, it answers
   `409 integration_preview_stale` **with a freshly computed plan**:
   the record's outcome is `stale`, the fresh plan is included for a
   new operator decision, and the mutation is **never blind-retried**.
4. A `409 integration_preview_unavailable` (no usable model roster
   retained) is recorded as `unknown` with the pin's documented remedy
   read — `GET /api/client-integrations` — included as evidence.
5. Writer refusals (conflict/unsafe/drift/expired snapshot) keep their
   code, state, `reason` and `residual`; a snapshot's **existence** is
   recorded as a boolean — its path, like every file location the
   integration routes serve (`configPath`, `snapshotPath`,
   `conflictPaths`), is never copied into a record or snapshot.

The Aside **bulk** PUT (`PUT /api/client-integrations/aside/profiles`)
is never sent: the pin refuses plan bindings for it ("a confirmed plan
applies to one profile"), so it cannot meet the issue's confirmation
rule. The 200/207 partial-envelope shape is still handled by the
shared per-element parser (`clientIntegrations[]`/`results[]`
envelopes wherever an upstream answer carries them — an element
missing outcome fields does not establish success), and the fake
server pins that envelope contract in the selftest.

### What stays out of this slice (recorded, not silent)

- Model visibility / selection / disabled-model writes
  (`PUT /api/model-visibility`, `PUT /api/selected-models`,
  `PUT /api/disabled-models`): not enumerated for slice 2; their
  convergence receipts are parsed by the shared per-element parser
  when they appear in readback evidence, but no Operation writes them.
- Aside bulk apply and `POST /api/client-integrations/aside/sync`:
  no plan binding exists for them (see above).
- Journal retirement (`DELETE …/journal`): retention management, not
  a configuration change; not implemented.
- Everything the issue forbids in any slice: global PATH/config edits
  by ELIOT itself, automatic shim restore, compaction changes,
  account-pool rotation (`/api/codex-auth/*`), ELIOT-side fallback
  chains, sidecars. `send`/`reply` remain **unavailable** — execution
  belongs to the native Codex backend.

## What it does not do

- No owned launch: the service is started and owned by the operator
  (`lifecycleOwner: "external"` is the only value this artifact
  accepts; a binding claiming ELIOT owns the lifecycle is refused).
- No account-pool management, no transcript duplication (`/api/logs`
  is not read), no metrics scrape.
- Undoing a configuration change is a **new saved Operation** (an
  inverse settings Operation, or an integration restore through the
  journal) — never an automatic rollback by the bridge.

## Configuration

Copy `module.example.json` outside the repository and keep it
untracked. Endpoint, expected version, credential reference and
lifecycle owner are four separate facts:

- `endpoint` — the existing service (documented default listener
  `http://127.0.0.1:10100`; always operator-configured, never assumed).
- `expectedVersion` — compared by exact string. A mismatch is
  readiness `unknown` with the observed version recorded — not an
  attach failure and not a pass; for mutations it is a hard gate.
- `adminTokenEnv` — the **name** of the environment variable holding
  the service's Management (admin) credential. That credential is
  separate from any data-plane/proxy admission key and from the Codex
  app-server token. The token is read at attach time, held in memory
  only, sent only as `X-OpenCodex-API-Key`, and never passed to
  Codex/model tools, never persisted in a Task/Operation, never
  printed. Loopback dashboard sessions and remote pairing are GUI
  mechanisms and are never scraped or reused.
- `lifecycleOwner` — `"external"`.

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
`opencodex_version_mismatch`, `opencodex_unavailable`). From bridge.2
snapshots it additionally projects a `configuration` summary (protocol
Messages surface + unrepresentable policy, integration totals/current,
Aside profile totals/applied, sub-agent mode and chosen roster) taken
**only from the recorded snapshot**, and raises
`opencodex_integration_attention` for a client integration recorded in
`conflict` or `unsafe` state. Snapshots recorded by bridge.1 carry no
configuration section: the field is null and no finding is derived
from its absence. No finding ever recommends restarting the shared
service; the next step points at the operator/service owner. With no
OpenCodex binding recorded, the section is absent, not empty.

## Fixtures and self-test

Fixtures under `fixtures/` are **reconstructions from the pinned
upstream contracts** (docs-site Management API reference and the
management route sources at the pin), each carrying its provenance —
they are **not** captures from a live 2.73.0 service, and no live
service has been installed or qualified by these slices (Windows
behaviour likewise unverified). The fake Management server is stateful
for the configuration surface (initial state in
`fixtures/configuration-state.json`) and reproduces the pin's
documented behaviours: plan fingerprints that go stale after an
intervening change, preview-unavailable, a lost mutation response, a
failed catalog refresh, and the Aside 200/207 partial envelope.

```sh
node selftest.mjs   # fake Management server, 28 fixture tests, no network
```

The load-bearing tests: a full attach+snapshot cycle issues **GET
requests only**; mutations happen only inside `configure`, exactly
once per Operation; the admin token appears in no snapshot, no record
and no bridge stdout; a stale plan is returned, never retried; a lost
response is reconciled by reading; the version gate blocks every
write; detach leaves the fake service answering; zero data-plane
(`/v1/*`) requests are made — small health reads consume no model call.

## Activation and rollback

Activation is a new binding: copy the example config out, name the
token env var, and record snapshots through the module seam.
Configuration Operations are activated by the operator saving request
files; nothing runs on a schedule and nothing is applied implicitly.
Disabling or removing the module/binding leaves the baseline Muse,
OpenCode and Codex routes unchanged and leaves the OpenCodex service
itself untouched — rollback of the *module* is removing the binding.
Rollback of a *configuration change* is a new saved Operation (above),
never an automatic restore. See [UPDATE.md](UPDATE.md) for the pinned
update procedure.
