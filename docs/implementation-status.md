# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Runtime source was audited through
`8355bdf0fbe561b5163bd3ab1ba6db24c0f8ddbc`; later commits in this series change
documentation only. `main` contains one completed runtime reduction and a source-backed
refactoring program.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — confirmed excess/deletion boundaries.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

## Audit coverage

The repository has **not** received a complete current-head line-by-line audit of every
runtime path. Issue #153 used `ed1df44` on 2026-10-14; current source is hundreds of
commits ahead. Historical findings remain evidence, not current qualification.

| Track | Coverage |
| --- | --- |
| Product boundary, ownership and main sources of overengineering | deep |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review |
| Queue, WorkDispatch, provider state and capacity | deep |
| Workspace/code scope, process custody and scheduling | deep |
| TaskPrompt and review/correction/acceptance automation | deep connected review |
| Public method routing/authorization ownership | deep structural review; no full privilege-matrix execution |
| Store schema/migration ownership | deep structural review; corruption/upgrade matrix not fully executed |
| IPC/MCP multiplex, subscriptions, task projection and native streams | deep source review; live load not qualified |
| Check, Script and Forge execution ownership | deep source review; one-executor deletions specified |
| Claude adapter state machine | deep connected source review; installed route not re-qualified |
| Codex adapter state machine | deep connected source review; installed route not re-qualified |
| Passive observation and harness executor duplication | architecture plus selected source paths |
| Command, Muse and OpenCode internal state machines | partial targeted review |
| GitHub effects, mailbox/coordination and gateway | partial |
| Installed/native behavior, Windows custody and scale | unqualified except named historical receipts/open issues |

“Architecture audit mostly complete” is accurate. “All code and runtime behavior
checked” is not.

## Completed runtime source change

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation pass counts as runtime reduction.

## Current implementation sequence

```text
A0-A3  Antigravity route/result/durable delivery/typed actions
B0-B4  current queue, explicit routes, provider state, root capacity, dispatch_once
C0-C1  Task path scope and one workspace write lease
D0-D1  minimal process owner and conservative restart
E0-E3  one schedule model and in-process admission loop
F0-F2  compact TaskPacket; Store identity outside model text
G0-G4  ordinary review/correction/acceptance transitions
H0-H3  split public/internal/adapter protocols; one domain authorizer
I0-I3  numeric migrations; remove SQL-text compatibility
J0-J3  concurrent MCP IPC, one wake pump, exact pending-input read,
       native content ring off writer
K0-K4  one Check executor, one Script wire protocol, one Forge remote-effect owner,
       no per-Operation executable hashing
L       shared adapter shell only after two current routes delete duplication
M0-M4  Claude: split native effect/Store delivery, persist result, simplify callbacks
N0-N6  Codex: split session/effect/delivery/observation, map native Goal, delete
       full-snapshot checkpoint and retired executor
O       Command, Muse, OpenCode, Goals, passive observation and remaining deletion
```

## Confirmed current gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | result can be lost; outcome depends on observation/proof; v5/v6 selectors disagree | self-contained terminal result and exact Store-delivery spool on v6 |
| Adapter bus | vendor strings, profile ladders, repeated hello/delivery and proof-heavy receipts | typed semantic protocol and shared shell only after two consumers |
| Execution | parallel generations for most harnesses | one current executor/format per retained runtime |
| Queue | WorkDispatch copies Task facts into cursors/pending/slots | ranked query plus atomic admission |
| Provider/capacity | vendor proof matrix and second lifecycle ledger | source-scoped facts and transactional root count |
| Workspace | advisory `code.scope.*` beside real lease | Task path prefixes plus one lease |
| Scheduling | three overlapping control systems | one in-process due/current-state loop |
| Prompt | identity JSON, launch packet and hashes enter model/adapters | compact TaskPacket and Store-owned identity |
| Review/correction | disposition/repair/acceptance workflows repeat candidate state | common Store transitions and thin `*_once` queries |
| Public API | parallel registries/routers/visibility rules include removal-target products | separate protocols and domain-owned authorizers |
| Store schema | base SQL SHA, extension digests and custom DDL verifiers | numeric transactional migrations |
| MCP tools | frontend mutex serializes the already multiplexed client | clone current `Arc<Client>` and release lock before RPC |
| MCP subscriptions | up to 16 pollers/forwarders/queues per session mirror durable facts | one coalesced wake pump; Store cursor is authority |
| MCP task projection | `tasks/get` scans every `report.attention` page | exact Operation/binding pending-input read |
| Native streams | ephemeral ring operations run on the single Store writer | reader authorization plus ring operation outside writer |
| Checks | optional executor leaves standalone and hidden-host execution active | standalone `swarm-checks` for all new work; old receipts readback only |
| Check probes | source/tool/Git-hook probes still spawn hidden host `check-worker` | bounded modes in selected check executable; no probe daemon |
| Scripts | one standalone executor but duplicated host/worker DTOs and hidden compatibility entrypoint | shared `swarm-scripts` wire types and finite deletion of old readers |
| Forge | valid local/remote split, but mixed custody, custom capture and per-effect worker/Git hashes | one remote worker, bounded local Git, shared OS primitives, install-time package verification |
| Claude journal | native intent, Store outbox, retention, cleanup, tombstones, boot markers and references share one 183-KB file | minimal native-effect record plus exact Store-delivery records |
| Claude interactions | live callbacks have a second 119-KB retention engine | current pending callback plus one reply Operation; bridge loss remains unknown |
| Claude result | terminal body is memory-only and lost Store result ACK cannot be resent | persist/read back body and retry identical Store page, never native work |
| Codex implementation | almost the whole adapter lives in one 355-KB file | explicit connection/session/effect/delivery/observation owners |
| Codex checkpoint | each change rewrites a full snapshot containing session, all live Operations, outcomes, result pages, observations, raw events and usage, plus SHA sidecar/tombstones | small independent records with different lifetimes; no full-state rewrite |
| Codex events | raw app-server events are buffered, copied into checkpoint and then copied into one observation | normalized latest activity/usage plus bounded exact pending request/terminal facts |
| Codex Goal | current adapter declares `agent.goal` unsupported but shared contracts maintain Goal-continuation admission/terminal proof around ordinary sends | map native `thread/goal/set|clear`; remove pseudo-continuation proofs |
| Codex input identity | native `clientUserMessageId`, thread/turn and `expectedTurnId` exist, but receipts also repeat prompt/Task/package hashes | native IDs plus Operation; Store owns Task identity |
| Observation | unbound joined Governor view incomplete; metric hashing remains | local provider+OS+Git view without provider calls |
| Product breadth | council, ratification/ACK and automatic projection remain | outside compact-core completion |
| Tooling | compiler/checkout/cache equality and artifact validators remain | generic package boundary plus route-specific live qualification |

These are findings and planned deletions, not completed runtime repairs.

## Claude adapter audit

The Rust owner correctly writes native intent before SDK input, assigns an input UUID,
correlates SDK frames to Operation and refuses guessed replay. The official SDK already
supplies startup, streaming query, permission callbacks/hooks, session IDs and history
readers.

The retained implementation is much larger than required:

- `lib.rs`: 234,105 bytes;
- `journal.rs`: 183,134 bytes;
- `interactions.rs`: 118,974 bytes;
- these three Rust files exceed 536 KB before `native_state.rs` and the JS driver.

`OperationJournal` combines native intent/outcome, Store delivery, result references,
startup/recovery, cleanup/compaction, tombstones and boot markers. `InteractionJournal`
repeats retention/tombstones for live callbacks whose JavaScript resolver cannot
survive bridge loss.

A concrete delivery defect remains: `agent.result` sends `module.result` directly. A
lost Store reply becomes `MODULE_RESULT_UNKNOWN` and the adapter refuses the identical
page, conflating forbidden native prompt replay with safe exact Store-delivery retry.
The body is also bounded memory-only, so adapter restart can lose a known terminal
body.

Target M0-M4: one live session owner, minimal NativeEffectRecord, exact StoreDelivery,
persist/read back terminal result, one live interaction projection, no generic
retention/tombstone/reference framework.

## Codex adapter audit

The attach-only process boundary is useful: the adapter never starts or kills the
Codex app-server. It also uses strong native identities—thread ID,
`clientUserMessageId`, turn ID and `expectedTurnId`—and keeps exact pending result pages
until Store acknowledgment.

The rest is concentrated in `crates/swarm-adapter-codex/src/lib.rs` (354,609 bytes).
Its `Checkpoint` combines:

- package/boot/binding identity;
- root phase, opening Operation, workspace and requested/effective model;
- all live Operation records and acknowledged IDs;
- native-effect state, dispatch and Goal-continuation receipts;
- pending Store outcomes/result pages;
- pending observation and up to 1,024/1 MiB native events;
- account usage and retirement state.

Every `save()` clones and serializes the complete checkpoint, creates a new file,
fsyncs it, writes a SHA sidecar, syncs the directory and prunes previous snapshots.
Terminal acknowledgments also create sharded tombstone files. This is algorithmic
write amplification proportional to all retained live state for each mutation; no
runtime latency claim is made here.

The native event path first buffers events in memory, appends them to
`pending_native_events`, saves the entire checkpoint, copies the vector into a Store
observation, then clears it after ACK. Store is already the durable observation
journal; Codex should retain only latest normalized activity/usage and exact unsettled
request/terminal facts.

Current root verification repeats thread/workspace/provider/model coordinates in both
the checkpoint and the acknowledged `agent.open` Operation and uses a
Pending/Candidate/Active proof state. Thread readback is necessary, but it should
produce one current SessionRecord rather than a compatibility proof graph.

The current adapter reports `agent.goal` unsupported even though current app-server
protocol has native thread Goal set/clear. Shared Eliot contracts instead wrap ordinary
send completion in Goal-continuation admission and a hashed terminal-event proof. The
native Goal should be mapped directly; ordinary Eliot Goal messages remain ordinary
`Send` Operations.

### Codex implementation slices

```text
N0  AppServerConnection
    One WebSocket owner/read pump with bounded pending RPC and one source epoch.
    Keep app-server externally owned; connection loss never kills or replaces it.

N1  SessionRecord
    thread ID, workspace, requested/effective route, source epoch and last readback.
    Replace root phase/proof duplication and repeated acknowledged open record.

N2  NativeEffectRecord
    one record per unsettled Open/Send/Steer/Cancel/Goal/Reply action:
    Operation, action, thread, clientUserMessageId/request ID, turn/expectedTurnId,
    exact bounded native params identity and disposition/readback cursor.

N3  StoreDelivery
    exact `module.outcome`/`module.result` params until ACK. Keep result byte/range
    hashes; delete full checkpoint, acknowledged set, tombstone tree and result-page
    state from the native-effect record after finite transition.

N4  source observations
    latest activity/usage checkpoint plus explicit source gap. Store only exact
    pending request/terminal facts needed for control/result association; do not
    checkpoint every app-server event or raw account patch.

N5  native controls
    map `agent.goal` to thread Goal set/edit/pause/resume/clear as supported; map
    exact steer with `expectedTurnId`; use `clientUserMessageId` as logical input ID.
    Delete GoalContinuationAdmission*, GoalTerminalEventRef/digests and prompt/Task
    identity echoes from adapter receipts.

N6  retire old code
    remove Python executor/SDK/fixtures, descriptor version ladder and legacy v2/v3
    checkpoint readers after concrete unsettled work is finished/exported.
```

Completion: open/resume → next-turn/steer → native Goal → terminal/result; reconnect
resends only exact Store delivery; app-server restart never replays input; usage remains
source-scoped; checkpoint writes are bounded by the changed record, not all live state.

## External-effect audit result

CheckRun, ScriptRun and Forge have different domain semantics. Their shared boundary is
only process custody/capture/private files already owned by `swarm-process`; a
universal EffectRunner is not justified. The target is one execution route and one
wire contract per domain, with package verification outside each Operation.

## Runtime evidence retained

Historical receipts remain scoped to their named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned protocol, TaskPacket, queue, correction flow, MCP,
adapter reductions, passive observation, executor deletion or installed fleet.

## Remaining audit tracks before a complete current-head claim

1. Complete state-machine audits for Command ACP, Muse and OpenCode, then select
   executor retirements. Claude/Codex are source-audited but still need connected/live
   qualification after implementation.
2. Finish GitHub/mailbox/gateway effect tracing and execute the public method → role →
   exact object → Store/effect matrix.
3. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after numeric migration is implemented.
4. Measure IPC/Store fairness and writer occupancy after J0-J3; source review
   identifies mechanisms but is not load qualification.
5. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries.

## Verification status

This pass changes documentation only. Runtime implementation begins with A0/A1 and
minimal affected formatting/warnings-denied Clippy/build checks. Connected tests run
when each vertical slice closes; broad qualification follows the complete product
path. Record each landed source slice and net owned-code change here.
