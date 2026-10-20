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
| Command Batch/ACP state machines | deep connected source review; current installed agents not re-qualified |
| Passive observation and harness executor duplication | architecture plus selected source paths |
| Muse and OpenCode internal state machines | partial targeted review |
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
O0-O6  Command: one Batch profile, one stable ACP v1 route, one session/effect/
       delivery model, live permissions only, owner-led process custody
P       Muse, OpenCode, Goals, passive observation and remaining deletion
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
| Codex checkpoint | each mutation rewrites a full snapshot containing session, all live Operations, outcomes, result pages, observations, raw events and usage | small independent records with different lifetimes |
| Codex Goal | adapter rejects `agent.goal` while upstream has native Goal; Eliot adds continuation proof around sends | native Goal mapping plus ordinary Eliot Goal messages |
| Command profiles | Batch v3/v4/v5 and ACP artifact profiles v1/v2 are active; both ACP profiles use ACP protocol v1 | one current Batch profile and one stable ACP v1 route; historical readers only |
| Command storage | Batch `RunStore` and ACP `AcpJournal` separately implement admission, effect, outcome, result ACK and cleanup; together main runtime files exceed 730 KB | one minimal native-effect/session model plus one exact Store-delivery model per semantic route |
| Command receipts | admission repeats artifact, route/input/prompt/Task/snapshot hashes; module host exact-compares capability/schema arrays | Operation + authenticated binding + native session/effect IDs; package metadata stays installation-only |
| Command permissions | pending permission is a live oneshot but journal machinery treats callbacks as durable workflow | observe current callback, admit one exact Reply Operation; process loss makes it unknown, never resumable/replayed |
| Command process family | ACP spawns direct child with `kill_on_drop`; source correctly admits direct exit is not family departure | module owner Job/group is custody authority; exact family readback before replacement/cleanup |
| ACP protocol | Eliot `AcpV1/AcpV2` names are package generations, while both use stable ACP v1 `session/load`; upstream ACP v2 is still alpha and removes load | name current stable ACP v1 honestly; do not import draft v2 until stable and needed |
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
The body is bounded memory-only, so adapter restart can lose a known terminal body.

Target M0-M4: one live session owner, minimal NativeEffectRecord, exact StoreDelivery,
persist/read back terminal result, one live interaction projection, no generic
retention/tombstone/reference framework.

## Codex adapter audit

The attach-only process boundary is useful: Eliot never owns the app-server process.
Strong native identities already exist: thread ID, `clientUserMessageId`, turn ID and
`expectedTurnId`.

Almost all implementation is in one 354,609-byte `lib.rs`. Its full `Checkpoint`
contains package/boot/binding, root phase/configuration, all Operations and ACK IDs,
native effects, dispatch/Goal proofs, pending outcomes/result pages, observation, raw
events, usage and retirement. Every mutation clones/serializes the full state, writes
and fsyncs a new file, writes a SHA sidecar, syncs the directory and prunes snapshots;
terminal ACKs add sharded tombstones.

Events exist in the in-memory queue, checkpoint and Store observation. Root proof
repeats thread/workspace/provider/model in checkpoint and open Operation. Current
adapter rejects native Goal despite upstream Goal set/clear and compensates with
Goal-continuation proofs around ordinary sends.

Target N0-N6: one app-server connection, one SessionRecord, per-effect native records,
exact StoreDelivery, latest normalized observation, direct native Goal/steer/input
identity, and deletion of full checkpoints/tombstones/Python executor/profile ladders.

## Command Batch/ACP audit

The crate combines two valid products but five historical Eliot profiles:

```text
BatchV3, BatchV4, BatchV5
AcpV1, AcpV2
```

`AcpV2` is an Eliot artifact/schema generation, not ACP protocol v2. Both ACP paths
import ACP v1 types and use optional `session/load`. Current upstream ACP v2 remains an
alpha draft and replaces load with resume/replay cursor. The refactor must not add a
third protocol path now.

Main runtime surface exceeds 730 KB before tests:

- `adapter/acp.rs`: 304,695 bytes;
- `journal.rs`: 199,925 bytes;
- `adapter.rs`: 98,916 bytes;
- `native.rs`: 67,841 bytes;
- `result_page.rs`: 49,938 bytes;
- `module_host.rs`: 19,239 bytes.

Batch `RunStore` and ACP `AcpJournal` each persist command/admission, native intent and
receipt, outcome, result page/ACK and cleanup state. Batch additionally has release
intent/tombstones and profile-specific namespaces; ACP has session state/output
capture and another per-Operation directory tree. The same Store-delivery contract is
implemented twice.

ACP recovery has a good boundary: capture is installed before `session/load`; load is
used only if advertised; failure never creates a fresh session or replays TaskPrompt;
requested model/effort/mode are read back. Preserve this behavior in one stable ACP v1
route.

Live permission requests contain a JavaScript/Rust oneshot reply channel. Files can
prove request/reply uncertainty but cannot recreate that resolver. On adapter/process
loss the request becomes unknown/retired; never send an old reply to a replacement
session.

The adapter spawns the native child itself with pipes and `kill_on_drop`, while source
honestly records `family_departure_claimed=false`. Direct child exit is not descendant
family departure. Custody must be delegated to the existing module-owner Job/process
group, with explicit close/cancel semantics separate from process cleanup.

### Command implementation slices

```text
O0  profile cut
    New work uses BatchV5 and one current stable ACP v1 route. Enumerate concrete
    unsettled BatchV3/V4/AcpV1 artifacts, finish/export them, then remove profile
    enums, namespaces, capability/schema branches and installers.

O1  one stable ACP session owner
    SessionRecord = native session, workspace, effective options/capabilities,
    source epoch and owner Operation. Preserve optional session/load no-replay
    recovery; no draft ACP v2 parallel implementation.

O2  NativeEffectRecord
    one exact record per prompt/reply/cancel/close/load until native disposition.
    Keep session/request/prompt IDs and readback cursor; remove Task/package/hash
    proof copies and generic evidence directories.

O3  StoreDelivery
    exact outcome/result params until ACK, shared by current Batch and ACP only after
    both delete their duplicate code. It cannot invoke command/ACP or choose recovery.

O4  live permission and observations
    keep one current pending request plus bounded latest tool/child/activity facts.
    Admit reply before resolving live callback. Remove callback persistence that
    pretends the resolver survives process loss.

O5  process custody
    native child runs under the module owner group/Job; bounded shutdown and exact
    family readback own replacement. `kill_on_drop` direct-child status alone never
    releases family custody.

O6  semantic commands
    ACP maps Open/Dispatch/Send/Reply/Cancel/Shutdown/Refresh/Result/Reconcile.
    Batch exposes only one-shot Open/Dispatch/Refresh/Result/Reconcile/Shutdown as
    actually supported. Remove `native.command.cancel_turn`,
    `native.command.close_session`, release-evidence public commands and full
    descriptor/schema echo.
```

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

1. Complete state-machine audits for Muse and OpenCode, then select executor
   retirements. Claude, Codex and Command are source-audited but still need connected/
   live qualification after implementation.
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
