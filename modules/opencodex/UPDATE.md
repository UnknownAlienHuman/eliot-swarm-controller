# OpenCodex bridge — update and rollback

Per module-contract §11: take the upstream unit whole at its boundary,
record contract sources, changed files, verification, activation and
rollback here. This module vendors **no** upstream code — it is an
original, dependency-free client of the documented Management API — so
there is nothing to merge and no license text ships inside the module.

## Pins

| Fact | Value |
| --- | --- |
| Upstream | `lidge-jun/opencodex` (https://github.com/lidge-jun/opencodex) |
| Release | **v2.73.0** (published 2026-09-30) |
| Source commit | `569e3e7dae48bafc54b8a1a7e3a85129befe2d98` |
| Upstream license | **MIT**, © 2026 opencodex contributors (read from the pin's `LICENSE`) |
| Module artifact | `opencodex-2.73.0-bridge.2` (slice 1 shipped as `bridge.1`) |
| Runtime | Node.js ≥ 22, standard library only, zero dependencies |

## Contract sources (read at the pin)

- Issue #1 (this repository) — purpose, boundaries, acceptance.
- Upstream docs-site `management-api.md` at the pin, in full —
  endpoint matrix, admin auth (`X-OpenCodex-API-Key` / bearer), error
  forms (401/403/404/413/503, `409 sibling_instance`).
- `src/server/management/system-routes.ts` at the pin, in full —
  `GET /api/system/health` (source-only contract; absent from the
  docs-site matrix) and `GET /api/system/memory` exact shapes.
- `src/server/management-api.ts` at the pin — dispatcher order (origin
  gate → body cap → sibling guard → route families).
- `src/lib/spend-reservation-ledger.ts` at the pin —
  `spendLedgerDiagnosticsSnapshot()` field list.
- Module contract v2 §§2, 4–7, 11; architecture §§3–4, 7–10, 15–16.

Known contract risks carried honestly: `/api/system/health` may drift
without a docs trail (tolerant parser; version recorded with every
snapshot); the full `/api/models` row schema was not read from source
(only doc-named fields are kept); the health `version` string format
vs the release tag is assumed, not live-verified (exact-string compare,
any difference is `unknown`); the live docs site moves — where it and
the pin differ, the pin wins.

## Changed files in this slice

- `modules/opencodex/bridge.mjs` — the adapter (config, GET-only
  Management client, attach probe, snapshot assembly, CLI seam).
- `modules/opencodex/control.mjs` — pure normalisation/version/
  redaction/unknown-mapping helpers.
- `modules/opencodex/fixtures/` — pinned-contract fixture
  reconstructions + `fake-management-server.mjs`.
- `modules/opencodex/selftest.mjs` — 11 fixture tests.
- `modules/opencodex/module.example.json`, `package.json`,
  `README.md`, this file.
- Host: `src/doctor.rs` (recorded-snapshot section + findings +
  `opencodex_provider_service` known gap), `config/controller.example.toml`
  (disabled Codex+OpenCodex route example), root `README.md` pointer,
  `THIRD_PARTY_NOTICES.md` attribution.

## Verification

Through the real adapter, against the fake Management server (the
fixtures are reconstructions — this is fixture verification, **not**
live qualification):

```sh
node --check modules/opencodex/bridge.mjs modules/opencodex/control.mjs \
  modules/opencodex/selftest.mjs modules/opencodex/fixtures/fake-management-server.mjs
node modules/opencodex/selftest.mjs   # 11/11 fixture tests
cargo test --locked --lib doctor      # host doctor section tests
```

**Live verification is NOT performed.** No opencodex 2.73.0 service
was installed or run for this slice; billing behaviour, session
affinity, protocol conversion and Windows behaviour remain
unqualified until exercised against an operator-installed service.

## Update procedure (new upstream release)

1. Read the new release's pin-side sources for every contract this
   module consumes (management-api docs, system routes, ledger
   snapshot, restart contracts if a later slice uses them). Do not
   carry field assumptions across releases silently.
2. Update the fixtures to the new contracts (keeping the provenance
   header and the "reconstruction, not capture" label), adjust
   normalisers in `control.mjs`, and re-run the selftest.
3. Bump the artifact id (`opencodex-<version>-bridge.1`; a bridge-only
   fix increments the `bridge.N` suffix) and `UPSTREAM` in
   `bridge.mjs`, this file and `module.example.json` together.
4. New version = new bindings. Existing bindings finish on the old
   artifact; nothing is migrated in place.

## Activation and rollback

- **Activation:** an operator copies `module.example.json` outside the
  repository, points it at their running service, sets the named admin
  token env var, and records snapshots through the module seam.
  Doctor's `services.opencodex` section appears from the first
  recorded snapshot.
- **Rollback:** disable or remove the binding. The controller returns
  to exactly its prior behaviour (no OpenCodex section in doctor), and
  the OpenCodex service is untouched — this module never started,
  stopped or reconfigured it, so there is no service-side rollback to
  perform. Rolling back the bridge binary never rolls back any remote
  effect, because this artifact performs none.

## Slice 2 — requested configuration changes via saved Operations

Artifact bumped to **`opencodex-2.73.0-bridge.2`** (same upstream pin;
`bridge.mjs`, `module.example.json` and this file updated together per
the procedure below). Capability change: `configure`/`preview` are
implemented for the pinned configuration families; `send`/`reply`
remain unavailable.

### Contract sources (read at the pin, in full)

- `src/server/management/protocol-routes.ts` +
  `protocol-settings-patch.ts` — `PATCH /api/protocols/settings` body,
  merged-state rollout rule, 400/409/500 semantics, and the
  `GET /api/protocols` response shape (`surfaces`/`settings` from
  `src/protocols/settings.ts`) used for readback.
- `src/server/management/model-routes.ts` — `PUT /api/model-settings`
  validation (unknown fields named, routed providers only, exact
  modelId), the stored-state receipt (`saved`, `changed`,
  `hasOverrides`, `catalogRefresh`), and
  `src/server/management/model-rows.ts` for the readback row fields
  (`contextWindowDeclared`, `inputModalitiesDeclared` stored;
  `reasoningEfforts`/`defaultReasoningEffort` effective).
- `src/server/management/agent-settings-routes.ts` — the five
  sub-agent surface GET/PUT pairs (`/api/v2`, `/api/injection-model`,
  `/api/effort-caps`, `/api/subagent-models`,
  `/api/subagent-model-fallback`), their validation rules,
  partial-write 502 behaviour on `/api/v2`, and the "applies to new
  sessions" warnings; `src/reasoning-effort.ts` for the effort ladder.
- `src/server/management/integration-routes.ts`,
  `aside-profile-routes.ts`, `src/integrations/mutation-plan.ts`,
  `state.ts`, `writer.ts`, `aside-profiles.ts`, `owned-refresh.ts` —
  the preview/plan/`planFingerprint` binding (both-or-neither,
  operation agreement), `409 integration_preview_stale` with a fresh
  plan, `409 integration_preview_unavailable` and its remedy read,
  writer refusal mapping (409/410/500 with `reason`, `residual`,
  `snapshotPath`), state/journal shapes, the Aside per-profile
  canonical paths, and why the bulk PUT takes no binding ("a confirmed
  plan applies to one profile").
- Audit §B.1 (slice-2 scope and order) and issue #1 point 4.

### Changed files in this slice

- `modules/opencodex/bridge.mjs` — request validation per kind, the
  version-gated configure/preview executors (one mutation per
  Operation, lost-response reconcile by readback, stale-plan return
  without retry), the snapshot `configuration` section, the `journal`
  read, CLI `configure <request.json>` / `preview <request.json>`.
- `modules/opencodex/control.mjs` — normalisers for protocol
  settings, model-settings receipts and model rows, integration
  plans/states/outcomes/journal, per-element partial envelopes, and
  the five sub-agent surface states. File locations and upstream free
  text are never copied.
- `modules/opencodex/fixtures/fake-management-server.mjs` — stateful
  slice-2 surface with `stale_plan`, `preview_unavailable`,
  `lost_response`, `refresh_failed` and `partial` scenarios;
  `fixtures/configuration-state.json` — the initial mutable state
  (labelled reconstruction).
- `modules/opencodex/selftest.mjs` — 28 fixture tests (17 new).
- `modules/opencodex/README.md`, `module.example.json`,
  `package.json`, this file.
- Host: `src/doctor.rs` — `services.opencodex[].configuration`
  projection from recorded snapshots only + the
  `opencodex_integration_attention` finding (+1 test); root `README.md`
  pointer updated to bridge.2.

### Verification

```sh
node --check modules/opencodex/bridge.mjs modules/opencodex/control.mjs \
  modules/opencodex/selftest.mjs modules/opencodex/fixtures/fake-management-server.mjs
node modules/opencodex/selftest.mjs   # 28/28 fixture tests
cargo test --locked --lib doctor      # host doctor section tests
```

**Live verification is still NOT performed.** No opencodex 2.73.0
service was installed or run for this slice; the mutation behaviours
above are verified against the fake server's reconstructions of the
pinned contracts, not against a live service.

### Activation and rollback (slice 2)

- **Activation:** existing bindings keep working after re-pointing
  their config at `bridge.2` (the config's `moduleArtifactId` must
  match). No configuration change is applied by the upgrade itself;
  changes happen only when an operator saves a request file and runs
  `configure`.
- **Rollback of the module:** remove the binding / return the config
  to a bridge.1 artifact. Doctor keeps projecting recorded snapshots;
  bridge.1 snapshots simply have no `configuration` section.
- **Rollback of a configuration change:** a new saved Operation — an
  inverse settings Operation, or an integration restore through the
  upstream journal. The bridge never restores anything automatically.
  (This supersedes the slice-1 note above for bridge.2: the artifact
  now performs exactly the remote effects an operator requested, one
  per saved Operation, each with its readback evidence recorded.)
