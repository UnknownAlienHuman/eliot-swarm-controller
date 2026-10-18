# Muse SDK bridge — current integration

The default Muse route currently uses `muse-sdk-1.3.0-bridge.13` and the
`@muse-code/sdk` 1.3.0 resolution in this directory's manifest/lockfile. The
[Rust adapter](../../crates/swarm-adapter-muse/registration/INTEGRATION.md) has a
working candidate and a disabled descriptor. Its
[2026-10-17 native result receipt](../../docs/remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json)
records an applied dispatch and a publicly readable result on the recorded Core;
it does not qualify the default Node route or complete native family recovery.

Refactoring chooses one executor by required behavior and total owned code
across Rust, SDK drivers and glue. The
[transition plan](../../docs/agent-operations/implementation.md#current-format-and-existing-data)
removes the replaced implementation; these source versions do not promise
permanent support for older bridges. [UPDATE.md](UPDATE.md) separates dependency
updates, protocol changes and concrete saved-work transitions.

## Ownership and setup

Muse owns provider authorization, settings, native tools and inference. The
bridge owns the SDK connection and maps controller Operations to native command
IDs. Host Store owns Task/Attempt admission, rights and acceptance.

The existing Node setup uses `npm ci --ignore-scripts` in this directory. Select
the route through the [module installation guide](../../docs/agent-operations/module-installation.md)
and copy `module.example.json` to private configuration with the real Host
endpoint, scoped module credential and native executable/arguments.

```toml
[[routes]]
alias = 'muse-manager'
runtime = 'muse'
module_artifact_id = 'muse-sdk-1.3.0-bridge.13'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
# modelId, providerId and reasoningEffort are optional native overrides.
```

Current code requires the route and private `moduleArtifactId` to match
bridge.13 and checks restored checkpoint identity. These are present source
constraints, not a rule to create a permanent runtime namespace for every SDK
update. Model/provider/effort omission uses native defaults; an explicit
override must match readback. An observed model is never copied into a requested
route constraint.

After reserving the binding and creating its scoped module credential, the
existing managed invocation is:

```powershell
swarm module-run --state-dir C:\SwarmModules\MC --command 'C:\Program Files\nodejs\node.exe' -- C:\SwarmCode\modules\muse\bridge.mjs --config C:\SwarmConfig\muse.json
```

Use a module-state directory separate from host data. The managed owner records
the bridge/native process family and prevents a second launcher from adopting
live work. Direct `node bridge.mjs` bypasses that ownership path. Runtime
upgrades must handle any actual admitted work explicitly; they do not relabel
its receipts or replay its prompt.

## Current SDK and MCP boundary

The bridge uses the SDK's public `spawnMspConnection`, `Connection`,
`checkServedFingerprint` and `MspError`. The SDK owns wire framing and RPC
routing; ELIOT owns Operation mapping and its checkpoint. The SDK `Session`,
`PendingCommandSet` and `GapFiller` are not currently used. A proposed SDK
substitution must remove existing transport/fold/glue and preserve explicit
control over native input replay.

The bridge requests the native `sessionMcp` grant and can forward a configured
`mcpServers` map. Observed `session/start` acceptance does not establish that
the server or its tools are usable. The inspected native reads exposed no tool
inventory, and the existing Store launch gate still refuses dispatch in that
case. This is a [known source gap](../../docs/agent-operations/refactoring.md):
[V3 requires asynchronous MCP observations](../../docs/agent-operations/implementation.md#launch-and-results),
so unavailable inventory must not become a new permanent launch prerequisite.

## Commands and observation

| Host operation | Current bridge behavior |
| --- | --- |
| `task.dispatch` | Consumes the Store TaskPrompt and dispatch context, validates the input identity and submits that prompt once. Maximum prompt size is 1,000,000 UTF-8 bytes. |
| `agent.send` | Next-turn input or steering addressed to `expected_turn_id`. |
| `agent.reply` | An explicit answer to current native approval/input request IDs. Presentation acknowledgement alone does not decide a request. |
| `agent.configure` | Applies an explicit native setter. Model changes wait for matching native readback; an ACK alone does not release the input fence. |
| `agent.goal` | Native set/edit/pause/resume/clear. Goal completion is separate from Task acceptance. |
| `agent.refresh` | Reads the root or observed child and refreshes pending requests; does not resume it. |
| `agent.reconcile` | Addresses the original unresolved Operation; no automatic blanket replay. |

Use `swarm call METHOD --file FILE` with binding/generation and the method's
arguments. Configure one native setter per Operation. While a model change is
unresolved, current code holds sends, dispatch and work-starting goals;
replies, refresh and reconciliation remain available.

Observations fit the existing 1 MiB Host frame. Omitted pending identities or
oversized gaps produce partial coverage; they do not create decisions or prove
that missing children completed. Opaque cursor pairs are preserved together or
reported unavailable. Child freshness requires an exact current read; a restored
snapshot or idle parent does not prove current family completion.

## Recovery

The checkpoint contains native session identity, relevant settings, pending
commands/ACKs and undelivered outcomes/result pages. Native transcript and
credentials remain with their existing owners. A Host disconnect reconnects the
same bridge; it does not close the SDK or resend native input.

After bridge loss, the current recovery path first checks that the previous
managed family has departed. Read `agent.state` for the current boot and submit
an explicit `agent.recover` request for that binding:

```json
{"binding_id":"EXISTING_BINDING","generation":1,"expected_boot_id":"CURRENT_BOOT","reason":"Resume the recorded session"}
```

Recovery uses `session/resume` for the recorded native session. It has no
fresh-session or original-prompt fallback. An unknown admitted command needs
targeted reconciliation with its original ID. Corrupt/missing checkpoints and
uncertain creation remain reported recovery gaps; do not infer a successful
effect or silently issue it again.

This mechanism serves current work. Old inactive bridge generations are handled
through the finite export/transition in [UPDATE.md](UPDATE.md), rather than kept
as additional executors indefinitely.

## Task-specific children

Native managers spawn children. `attempt.bind_producer` associates an observed
session/turn with the Task without launching another writer or accepting a result.

```powershell
swarm task claim TASK --revision 1 --binding-id BINDING --generation 1
swarm family BINDING --generation 1
swarm --request-id bind-worker-1 task bind ATTEMPT --assignment worker-1 --session CHILD --turn TURN --observation-id OBSERVATION
```

The caller still needs current Task/binding rights. Reusing a child session does
not reuse the authority of its previous turn; family pagination does not prove
complete discovery. `subagent/readResult` changes native state and is not used
as an observation helper.

## Result retrieval and complete local export

Node `results.mjs` reads the requested native item revision using `view/page`
and `item/readOutput`. It does not substitute a newer revision or follow
arbitrary stored paths/URIs.

```json
{"kind":"result","session_id":"PARENT_CONTAINING_ITEM","item_id":"OBSERVED_ITEM","item_revision":1}
```

Supported kinds are `result` (structured subagent result), `message` (agent
message), and `output`/`patch` with the required `output_ref`. An optional
`before_cursor` is native pagination; `expected_digest` identifies the source.

```powershell
swarm --request-id page-1 result BINDING --generation 1 --file selector.json --offset 0 --length 65536
# Read operation.get, then its result.details.artifact_ref:
swarm artifact get ARTIFACT_ID
swarm artifact read ARTIFACT_ID --offset 0 --length 65536
```

A result call returns one page. Continue with `next_offset_bytes`, the same
selector/revision and a new request ID. Native-source offsets differ from local
page offsets; respect UTF-8/base64 at split boundaries. The 64 KiB page limit is
not a whole-result limit.

Use the existing [complete-result workflow](../../README.md#complete-results-without-model-roundtrips)
for `artifact.assemble`, `artifact.parts` and local export. This gathers actual
bytes without another model call. File completeness remains separate from Task
submission, review and acceptance.

## Verification and remaining work

Use existing `selftest.mjs` cases for changed bridge behavior and the project's
[scoped checks](../../docs/ci-scoped-builds.md). Historical passes are scoped
evidence in [implementation status](../../docs/implementation-status.md);
they are not extra instructions or permanent release gates.

Outstanding work is recorded in the
[refactoring map](../../docs/agent-operations/refactoring.md) and the
[Rust integration status](../../crates/swarm-adapter-muse/registration/INTEGRATION.md).
The native result receipt narrows the Rust candidate's gaps; it does not settle
all native family/recovery behavior or choose which complete implementation
should remain.
