# Muse bridge — update and rollback

Contract sources: architecture §15, module contract §§3 and 11, and the
[module README](README.md). This file is the per-module record those
documents require: pins, what may change, how a change is verified, how
the new version is activated for new bindings, and how to roll back.

## Pins

| Fact | Value |
|---|---|
| Module artifact | `muse-sdk-1.3.0-bridge.8` (route `module_artifact_id`, module config `moduleArtifactId`, `config/controller.example.toml`) |
| SDK package | `@muse-code/sdk` **1.3.0**, exact, via `package.json` + `package-lock.json`; installed module-locally with `npm ci`, never globally |
| SDK canonical source | `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b` (MIT); the MSP schema is pinned at that commit |
| Bridge code | ELIOT-owned `*.mjs` in this directory (`bridge`, `checkpoint`, `control`, `owned`, `results`, `settings`) |
| Native executable | Operator-installed Muse binary named by the private module config (`command`/`args`). The controller does not install, select or update it; its version is a separate fact from the SDK and bridge versions |
| Node | `>=22` (`package.json` engines) |

## What may change

- Bridge `*.mjs` files — ELIOT code, changed directly.
- `package.json` / `package-lock.json` — only to move the SDK pin,
  deliberately, to an exact version. No `@latest`, no range drift, no
  global installs, nothing installed as a side effect of reading status.
- `module.example.json`, the route in controller config, the README and
  this file — the artifact-id references move together with the pin.

The SDK package itself is never edited in place. A defect in the SDK is
handled at the bridge boundary or by a new pin, not by patching
`node_modules`.

## Version numbering

- SDK version change: artifact becomes `muse-sdk-<new-sdk>-bridge.1`.
- Bridge-only change on the same SDK: increment the suffix,
  `bridge.7` → `bridge.8`.

Any change to shipped bridge code or to the SDK pin gets a new artifact
id. Reusing an existing id for different code is forbidden: bindings
record the id, and the host trusts it as the version identity.

## Verification (before activation)

Required pre-activation checks are below. The scoped PR workflow parses changed
JavaScript files; that result alone does not include SDK import or the fixture
selftest. Record each executed check separately for the exact candidate:

```bash
for file in modules/muse/*.mjs; do node --check "$file"; done
cd modules/muse
npm ci --ignore-scripts --no-audit --no-fund
node --input-type=module -e 'import { spawnMspConnection, Connection } from "@muse-code/sdk"; if (typeof spawnMspConnection !== "function" || typeof Connection !== "function") throw new Error("SDK export mismatch");'
```

If bridge behavior changed, also run the fixture selftest and the
bounded fixture invocation described in the README:

```bash
cd modules/muse
node selftest.mjs
```

The selftest pins the R18 observation derivations (durability profile,
host-death record, gap-fill record, failed-reconcile classification) and
the checkpoint round-trip for same-ID reconciliation against fixtures
authored from the pinned SDK sources. It also drives the actual bridge and
pinned SDK connection against bounded local fixture host/native processes to
verify restored-child freshness, unknown-server-request rejection, and
exact-turn stale rejection. These fixtures do not start the installed Muse
executable or call a model. Syntax/import and fixture success are not live Muse, Max
or Windows qualification; live qualification of the new pin against the
installed runtime is a separate step and never follows from these
checks.

Bridge.8 changes pending-request freshness and inventory replacement only. The
SDK pin and native command/recovery protocol are unchanged. The current bridge
rejects a configuration naming an earlier artifact; do not relabel an existing
checkpoint or overwrite a running bridge to bypass that check. The fixture
selftest uses bridge.8 for its generated current-bridge configuration while the
saved historical checkpoint fixture remains unchanged. No earlier fixture pass
is transferred to bridge.8.

## Activation

**Current PR blocker:** `module.example.json` still names bridge.7; its update was
not published because the write tool rejected that action. Do not activate this
PR until the example and the selected artifact are consistent.

Activation is per binding, through the recorded artifact id:

1. Stage the new bridge as its own copy with its own module-state
   directory. Never overwrite the scripts or executable of a running
   bridge, and never retrofit proof into it (README rule); a running
   bridge keeps the exact code it started with.
2. Set the new artifact id in both the controller route
   (`module_artifact_id`) and the private module config
   (`moduleArtifactId`). They must match exactly.
3. Reserve new bindings with `agent.open`. A binding records the
   artifact id of its route at reservation; the bridge announces its id
   in `module.hello` and the host rejects a mismatch with
   `ARTIFACT_MISMATCH`. New bindings therefore run the new bridge;
   existing bindings keep the artifact id they recorded (at the
   bridge.8 update, `muse-sdk-1.3.0-bridge.7`) and are still served
   by the old bridge. The two bridges never cross-serve a binding.
4. Retire the old bridge only after its bindings are released and its
   managed process group has departed on its own. The module-run owner
   is never killed to make room for the new version.

Nothing here installs a module or a service: staging files, editing
config and launching `swarm module-run` remain explicit operator
actions. Automatic module/service installation stays pending (root
README).

## Rollback

Point the route and the module config back to the previous artifact id
and the previous bridge copy; new bindings then use the old version
again. Bindings already created keep the artifact they recorded —
rollback does not migrate them, does not rewrite Store records, and
does not undo native effects the new bridge already performed (module
contract §11). If the new bridge misbehaves, stop reserving bindings
on it and reconcile its existing work through the recorded-session
recovery path in the README, not by editing artifact ids in the Store.
