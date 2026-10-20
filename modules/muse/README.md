# Muse route — retained Node executor

## Current decision

New Muse work uses the current Node route:

```text
muse-sdk-1.3.0-bridge.13
@muse-code/sdk 1.3.0
```

The disabled custom Rust candidate `muse-rust-controller.1@0.1.2` is a retirement
target. It is not a second supported new-work route and is not the basis of a shared
adapter framework.

This choice is based on current behavior, not language preference:

- Node bridge.13 is the configured/default route;
- it maps Store Operations to explicit native command IDs before native I/O;
- it supports start/resume, send/steer, reply, configure, native Goal, refresh,
  reconcile, result, usage and child/session observation;
- recovery resumes the recorded session with no fresh-session/original-prompt fallback;
- the Rust candidate is disabled, duplicates wire/schema/transport/fold/journal/
  process/observation/result machinery and has only a narrow result qualification.

Historical Rust receipts remain historical evidence. They do not require permanent
Rust execution or a Node↔Rust journal converter.

## Ownership

Muse owns provider credentials, native session state, tools and inference.

The bridge owns:

- one SDK connection and current session owner;
- mapping one Eliot Operation to one native command ID;
- native action/readback/reconciliation;
- bounded normalized observation and result extraction;
- exact already-formed delivery back to Store until ACK.

Host Store owns Tasks, Attempts, Operations, authorization, workspace authority,
review and acceptance. Native Goal completion never accepts a Task.

The bridge runs through `swarm module-run`; direct `node bridge.mjs` bypasses Eliot
process custody.

## Current command boundary

| Store action | Muse mapping |
| --- | --- |
| `task.dispatch` | validate the current task packet and submit once |
| `agent.send` | next turn or exact steer using expected turn |
| `agent.reply` | answer the current native request ID |
| `agent.configure` | one explicit setter plus matching readback |
| `agent.goal` | native set/edit/pause/resume/clear |
| `agent.refresh` | safe session/child/pending-request readback |
| `agent.reconcile` | read back the original unresolved Operation |
| `agent.recover` | resume the recorded session after confirmed old-family departure |
| `agent.result` | read the requested native item/revision and return bounded bytes |

A Host disconnect reconnects Eliot IPC; it does not close Muse or resend native
input. A native command rejected by the server can fail; a transport loss after
submission remains uncertain and is reconciled by its original command ID.

## Recovery

The retained session record names the exact native session and selected settings.
Recovery:

1. verifies the previous Eliot-owned family departed;
2. addresses the current binding/boot;
3. calls native session resume for the recorded session;
4. never creates a fresh session;
5. never resends the original Task prompt.

Corrupt/missing state or uncertain native creation remains a visible gap. A new
adapter process does not silently adopt another boot's unresolved delivery or effect.

## Results

The bridge reads a selected native session/item/revision. It never substitutes the
newest result or follows arbitrary stored file paths.

```text
agent.result
  -> exact native read
  -> module.result
  -> immutable artifact
  -> artifact.read / artifact.assemble
```

Result availability is separate from Task submission, review and acceptance.

## Simplification work

Refactor the checkpoint into four concerns:

```text
SessionRecord
  native session, workspace, effective settings and source epoch

NativeEffectRecord
  one unresolved command ID plus action/readback coordinates

StoreDelivery
  exact already-formed outcome/result parameters until ACK

ObservationCheckpoint
  replaceable latest normalized state plus cursor/coverage
```

Delete full descriptor/schema echo negotiation, repeated Task/source/package hashes,
artifact-version source allowlists, copied Task/proof state in model prompts, generic
retention structures outside these records and duplicate process ownership outside
`swarm-process`.

Use module-owner Job/process-group custody and bounded shutdown.

## Official Rust SDK

The reviewed official `muse-code-sdk` Rust crate is version 1.4.11, MIT and requires
Rust 1.97+. It provides spawn/attach, typed MSP, sessions, turns, approvals, fold, gap
recovery and bounded close.

It is a future replacement candidate, not an additional route. Its high-level
`send_user_turn` currently mints a command ID internally, while Eliot needs the
Operation→native-command association before uncertain submission. The low-level
custom-minter constructor does not run the normal handshake.

Re-evaluate only when upstream exposes the required production seam, or when a small
adapter can use public low-level Connection/generated types while deleting more code
than it adds.

Do not upgrade Node SDK/host from 1.3.0 during executor consolidation. SDK and host
versions are paired; upgrade them in a separate exact-version live-qualified
transition.

## Rust candidate retirement

Before deleting `crates/swarm-adapter-muse`:

1. enumerate concrete unsettled Rust 0.1.2 Operations/results;
2. finish/read back with the current owner or export required result bytes;
3. record explicit disposition for unresolved work;
4. remove the Cargo member, descriptor, route/configuration, package rules, custom
   wire/schema, fixtures and Rust-only qualification branches;
5. preserve completed results through Store/artifact history, not the executor.

Do not translate private Node/Rust journals or rewrite old receipts.

## Verification

Keep focused cases for:

- one Operation → one command ID;
- no prompt replay after Host/native uncertainty;
- same-session recovery only;
- native Goal independent of Eliot Goal;
- complete result bytes and exact Store redelivery after lost ACK;
- current request/reply identity;
- bounded close and process-family custody;
- source-scoped usage/coverage;
- explicit gaps for missing children/cursors.

Historical receipts prove only their named artifact/revision and do not qualify a new
SDK/host version.