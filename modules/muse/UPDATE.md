# Muse bridge — update and rollback

Contract sources: architecture §15, module contract §§3 and 11, and the
[module README](README.md). This file is the per-module record those
documents require: pins, what may change, how a change is verified, how
the new version is activated for new bindings, and how to roll back.

## Pins

| Fact | Value |
|---|---|
| Module artifact | `muse-sdk-1.3.0-bridge.13` (uninstalled source candidate; route `module_artifact_id` and module config `moduleArtifactId` must match before new bindings) |
| SDK package | `@muse-code/sdk` **1.3.0**, exact, via `package.json` + `package-lock.json`; installed module-locally with `npm ci`, never globally |
| SDK canonical source | `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b` (MIT); the MSP schema is pinned at that commit |
| Bridge code | ELIOT-owned `*.mjs` in this directory (`bridge`, `checkpoint`, `control`, `owned`, `results`, `settings`) |
| Native executable | Operator-installed Muse binary named by the private module config (`command`/`args`). The controller does not install, select or update it; its version is a separate fact from the SDK and bridge versions |
| Node | `>=22` (`package.json` engines) |

## What may change

- Bridge `*.mjs` files — ELIOT code, changed directly.
- Proposed trusted descriptor template: `modules/muse/module-descriptor.template.json`.
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
  `bridge.12` → `bridge.13`.

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
exact-turn stale rejection, plus a valid oversized `view/gap` with fitting
request siblings through the bounded observation frame. The Store attention
fixture consumes that expected projection and checks the existing addressed
refresh action and the unknown-session fallback. These fixtures do not start the installed Muse
executable or call a model. Syntax/import and fixture success are not live Muse, Max
or Windows qualification; live qualification of the new pin against the
installed runtime is a separate step and never follows from these
checks.

Bridge.11 retains the TaskPrompt v1 and native usage behavior introduced in
bridge.9 and the checkpoint corruption guard introduced in bridge.10. It also
bounds the complete `module.observe` JSON-RPC frame. Request details may be
projected to identity-only summaries; exact request identities that fit remain
in the pending array. Any identities that do not fit stay unresolved in the
bridge inventory and are represented by partial coverage, counts, an identity
digest and a gap. An opaque `view/gap` bracket is retained byte-exact when it
fits; otherwise both cursors are omitted together with an explicit incomplete
marker, while the authoritative in-memory gap remains intact. The existing
Store attention projection suggests addressed `agent.refresh` re-anchoring for
the recorded root/known child and falls back to root refresh for an unknown
session. No splice-fill, native decision or gap-closure claim is fabricated.
The Store receives no fabricated native ID or decision.

Bridge.12 is the historical source that retains the bridge.11 observation and checkpoint behavior and adds session-scoped native MCP admission: initialize requests the documented `sessionMcp` grant, and a configured `mcpServers` map is validated against the closed transport union and forwarded as `session/start` config over the raw Connection (the pinned facade omits `config` until its grant check lands). Retained installed CLI 1.4.0 observations show `sessionMcp` in both durable and ephemeral profiles. `session/start` accepted config even when a required stdio executable was unavailable, so its ACK proves admission only and says nothing about server liveness. The bridge refuses out-of-union transports before sending; a separate direct native decode refused an unknown transport with -32602. The documented reads (`session/read`, `view/subscribe`, `view/page`, `skill/list` and `session/list`) exposed no native MCP inventory, and an eight-second view watch on an MCP-attached session delivered only `session/started`. No model, turn, native inventory, or dispatch claim follows from admission; the Store gate refuses dispatch.

The bridge.13 source candidate also keeps productive sends, Task dispatch, and
work-starting goals behind any unresolved model configure. A native setter ACK
does not release that fence; exact reconciliation waits for the requested
selectors to appear in a fresh session read. Replies, refresh and targeted
reconciliation remain available, and an ACK-backed reconciliation does not
resend the setter. The `pending-configure-recovery` selftest exercises the
actual bridge subprocess before and after restart, including the restored
checkpoint, the old-model rejection, the later matching readback, and the
separation of an omitted provider from the observed provider.

A present checkpoint containing JSON null fails with CHECKPOINT_CORRUPT before
host connection or native launch; an absent checkpoint keeps its existing
fresh-state behavior. The SDK pin is unchanged. The current bridge rejects a
configuration naming an earlier artifact; the bridge.13 source candidate
requires `moduleArtifactId` to name bridge.13. Do not relabel a bridge.12
checkpoint or overwrite a running bridge to bypass that check. The host must
retain and accept the exact bridge.13 descriptor before activation. Earlier
fixture passes are not transferred to bridge.13.

## Activation

The host native-usage consumer retains bridge.9 through bridge.12 recognition
and adds bridge.13, checking each observation against its binding's exact
artifact. Register the exact bridge.13 descriptor before activating this
source for new bindings.

Activation is per binding, through the recorded artifact id:

1. Stage the new bridge as its own copy with its own module-state
   directory. Never overwrite the scripts or executable of a running
   bridge, and never retrofit proof into it (README rule); a running
   bridge keeps the exact code it started with.
2. Set `muse-sdk-1.3.0-bridge.13` in both the controller route
   (`module_artifact_id`) and the private module config
   (`moduleArtifactId`). They must match exactly.
3. Reserve new bindings with `agent.open`. A binding records the
   artifact id of its route at reservation; the bridge announces its id
    in `module.hello` and the host rejects a mismatch with
    `ARTIFACT_MISMATCH`. New bindings therefore select bridge.13; existing
    bridge.9 through bridge.12 bindings keep their recorded artifact id and
    original code. The bridges never cross-serve a binding.
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
