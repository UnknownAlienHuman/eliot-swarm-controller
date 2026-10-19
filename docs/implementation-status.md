# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Runtime source was audited through
`8355bdf0fbe561b5163bd3ab1ba6db24c0f8ddbc`; this commit changes documentation only.
`main` contains one completed runtime reduction and a source-backed refactoring
program.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — confirmed excess/deletion boundaries.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

## Audit coverage

The repository has **not** received a complete current-head line-by-line audit of every
runtime path. The last broad audit, Issue #153, used `ed1df44` on 2026-10-14; current
source is hundreds of commits ahead. Historical findings remain evidence, not current
qualification.

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
| Claude adapter state machine | deep connected source review; live installed route not re-qualified |
| Passive observation and harness executor duplication | architecture plus selected source paths |
| Codex, Command, Muse and OpenCode internal state machines | partial targeted review |
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
M0-M4  Claude: split native-effect state from Store delivery, persist terminal result,
       simplify live interactions, delete retention/tombstone/reference machinery
N       one executor per remaining harness, Goals, passive observation and deletion
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
| Claude journal | native intent, Store outbox, retention, cleanup, tombstones, boot markers and references share one 183-KB file | minimal native-effect record plus exact Store-delivery records with separate lifetimes |
| Claude interactions | live callbacks have a second 119-KB journal with retention, pending index and tombstones | current pending callback projection plus one reply Operation; bridge loss settles unknown, never replayed |
| Claude result | final body is retained only in bounded in-memory `NativeControl`; `module.result` is sent directly | persist exact terminal result bytes/identity before memory loss or read them from supported session history |
| Claude result ACK | lost `module.result` ACK becomes `MODULE_RESULT_UNKNOWN` and exact resend is forbidden | never replay native work, but retry the identical already-formed Store page until exact ACK/readback |
| Observation | unbound joined Governor view incomplete; metric hashing remains | local provider+OS+Git view without provider calls |
| Product breadth | council, ratification/ACK and automatic projection remain | outside compact-core completion |
| Tooling | compiler/checkout/cache equality and artifact validators remain | generic package boundary plus route-specific live qualification |

These are findings and planned deletions, not completed runtime repairs.

## Claude adapter audit

The current Rust/Node route has a sound basic safety boundary: the Rust owner writes a
native-effect intent before SDK input, gives each logical input a UUID, maps SDK frames
back to the retained Operation, keeps callback replies exact, and never guesses that a
lost pipe means the prompt did not run. The official SDK supplies the useful native
primitives already used by the bridge: `startup()`, streaming `query()`,
`canUseTool`/hooks, session IDs and session-history readers.

The implementation around those primitives is much larger than the retained product
requires:

- `crates/swarm-adapter-claude/src/lib.rs`: 234,105 bytes;
- `journal.rs`: 183,134 bytes;
- `interactions.rs`: 118,974 bytes;
- these three files alone exceed 536 KB before `native_state.rs` and the JS driver.

`OperationJournal` combines six different responsibilities:

1. native-effect intent/outcome;
2. Store outcome delivery;
3. result-operation references;
4. startup inventory/recovery;
5. retention/cleanup/compaction;
6. tombstones and boot-dispatch markers.

`InteractionJournal` repeats the same storage machinery for callbacks even though the
SDK callback resolver belongs to the live query and cannot be reconstructed after
bridge loss. Persistence can prove that a request/reply became uncertain; it cannot
resume the JavaScript callback.

A concrete delivery defect remains: `agent.result` builds and sends a `module.result`
page directly. If the Store reply is lost, the adapter saves an Unknown outcome and
`settle_result_intent_without_replay` refuses to submit the identical page again. That
correctly forbids replaying the **native prompt**, but incorrectly treats the already
formed idempotent **Store delivery** as another native effect. The body also exists
only in the bounded in-memory result records, so adapter restart can lose a known
terminal body even when the native session history still exists.

### Claude implementation slices

```text
M0  one live session owner
    Keep startup/query/input queue/frame pump and one ordered action loop.
    Remove full descriptor echo and repeated Task/source hash validation as protocol
    slices H/F migrate.

M1  NativeEffectRecord
    One small private record per input/reply/open/shutdown only until native
    disposition is known. Fields: Operation, action, boot/session, native input or
    request ID, exact bounded payload identity, disposition/readback cursor.
    No Store ACK, cleanup queue, tombstone or cross-operation reference graph.

M2  StoreDelivery
    Persist exact already-formed `module.outcome` and `module.result` envelopes.
    Reconnect resends identical bytes; exact ACK deletes them. This layer cannot
    execute SDK input or choose recovery.

M3  terminal result capture
    At the SDK result frame, persist bounded immutable body + session/input/frame
    identity before it can disappear from `NativeControl`, then deliver the exact
    page. For larger/unavailable bodies use the supported session-history reader and
    an explicit coverage gap; do not maintain a transcript mirror.

M4  live interactions
    Store exposes the current pending request as observation. `agent.reply` retains
    one exact reply Operation before resolving the live callback. ACK settles it;
    pipe/bridge loss makes it Unknown/Retired. Never replay a reply into a replacement
    query. Delete interaction retention state, pending index, tombstones and cleanup
    cursor after current callers switch.
```

Completion requires a connected open → dispatch → callback/reply → result path, lost
Eliot ACK recovery without native replay, bridge loss with honest Unknown, exact
session/result attribution, and net deletion across Rust + JS.

## External-effect audit result

The domain boundaries are not identical:

- CheckRun owns profile/input resolution, cancellation, coverage and verdict;
- ScriptRun owns bundle/interpreter/input/result schema and controller effects;
- Forge owns exact accepted candidate, repository/ref serialization and unknown push
  readback.

Their common boundary is only physical process custody/capture/private files, already
owned by `swarm-process`. Therefore the target is not a universal runner. It is one
execution route and one wire contract per domain, with package verification outside
each Operation.

## Runtime evidence retained

Historical receipts remain scoped to their named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned protocol, TaskPacket, queue, correction flow, MCP,
Claude simplification, passive observation, executor deletion or installed fleet.

## Remaining audit tracks before a complete current-head claim

1. Complete state-machine audits for Codex, Command ACP, Muse and OpenCode, then
   select executor retirements. Claude is now source-audited but still needs live
   qualification after implementation.
2. Finish GitHub/mailbox/gateway effect tracing and execute the public method → role →
   exact object → Store/effect matrix.
3. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after the numeric migration slice is implemented.
4. Measure IPC/Store fairness and writer occupancy after J0-J3; source review identifies
   the mechanisms but is not load qualification.
5. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries.

## Verification status

This pass changes documentation only. Runtime implementation begins with A0/A1 and
the minimal affected formatting/warnings-denied Clippy/build checks. Connected tests
run when each vertical slice closes; broad qualification follows the complete product
path. Record each landed source slice and net owned-code change here.
