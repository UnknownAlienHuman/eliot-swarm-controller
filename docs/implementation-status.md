# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Runtime source was audited through
`8355bdf0fbe561b5163bd3ab1ba6db24c0f8ddbc`; later commits in this documentation
series change no runtime source. `main` contains one completed runtime reduction and a
source-backed refactoring program.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — confirmed excess/deletion boundaries.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

## Audit coverage

The repository has **not** received a complete current-head line-by-line audit of
every runtime path. Issue #153 used `ed1df44` on 2026-10-14; current source is
hundreds of commits ahead. Historical findings remain evidence, not qualification of
the new source.

| Track | Coverage |
| --- | --- |
| Product boundary, ownership and main overengineering mechanisms | deep |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review |
| Queue, WorkDispatch, provider state and capacity | deep |
| Workspace/code scope, process custody and scheduling | deep |
| TaskPrompt and review/correction/acceptance | deep connected review |
| Public method routing/authorization ownership | deep structural review; no full privilege-matrix execution |
| Store schema/migration ownership | deep structural review; corruption/partial-upgrade matrix not executed |
| IPC/MCP multiplex, subscriptions, task projection and native streams | deep source review; live load not qualified |
| Check, Script and Forge execution ownership | deep source review; one-executor deletions specified |
| Claude adapter state machine | deep connected source review; installed route not re-qualified |
| Codex adapter state machine | deep connected source review; installed route not re-qualified |
| Command Batch/ACP state machines | deep connected source review; installed agents not re-qualified |
| Muse Node/Rust state machines and upstream SDK | deep source review; installed route not re-qualified |
| OpenCode built-in/standalone state machines and current upstream contract | deep source review; selected installed contract not re-qualified |
| Passive joined observation | architecture plus selected source paths |
| GitHub effects, mailbox/coordination and gateway | partial |
| Installed/native behavior, Windows custody and scale | unqualified except named historical receipts/open issues |

“Architecture and main reduction audit mostly complete” is accurate. “Every line and
runtime behavior checked” is not.

## Completed runtime source change

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation pass counts as runtime reduction.

## Current implementation sequence

```text
A0-A3  Antigravity v6, self-contained result, durable delivery, typed actions
B0-B4  current queue, explicit routes, provider state, root count, dispatch_once
C0-C1  Task path scope and one workspace write lease
D0-D1  minimal process owner and conservative restart
E0-E3  one schedule model and in-process admission loop
F0-F2  compact TaskPacket; Store identity outside model text
G0-G4  ordinary review/correction/acceptance transitions
H0-H3  split public/internal/adapter protocols; domain authorizers
I0-I3  numeric migrations; remove SQL-text compatibility
J0-J3  concurrent MCP IPC, one wake pump, exact pending-input read,
       native content ring off writer
K0-K4  one Check executor, one Script wire, one Forge remote-effect owner,
       no per-Operation executable hashing
L       shared adapter shell only after two current routes delete duplication
M       Claude: one session/effect/delivery/observation/result model
N       Codex: one app-server owner, native Goal, small records
O       Command: one Batch profile, one stable ACP v1 route
P0-P6  Muse: keep Node bridge, simplify it, retire custom Rust, no third executor
Q0-Q6  OpenCode: select one exact native contract, standalone Rust only,
       exact parent result, restart readback, internal MCP, delete built-in host path
R       native/Eliot Goals and passive local Governor observation
S       delete optional product engines, compatibility/tooling bureaucracy
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
| Public API | parallel registries/routers include removal-target products | separate public/internal/adapter protocols |
| Store schema | base SQL SHA, extension digests and custom DDL verifiers | numeric transactional migrations |
| MCP tools | frontend mutex serializes an already multiplexed client | clone current `Arc<Client>` and release lock before RPC |
| MCP subscriptions | multiple pollers/queues mirror durable facts | one coalesced wake pump; Store cursor is authority |
| MCP task projection | `tasks/get` scans attention pages | exact Operation/binding pending-input read |
| Native streams | ephemeral ring operations run on Store writer | authorize in Store, operate ring outside writer |
| Checks | standalone and hidden-host execution coexist | standalone `swarm-checks` for all new work |
| Scripts | one executor but duplicated wire types/compat entrypoint | `swarm-scripts` as sole wire owner |
| Forge | valid local/remote split but mixed custody/custom capture/per-effect hashes | one remote worker, bounded local Git, shared OS primitives |
| Claude | generic journal, separate callback retention and memory-only terminal body | minimal native effect, exact Store delivery, persisted result |
| Codex | monolith and full-snapshot checkpoint | explicit connection/session/effect/delivery/observation owners |
| Command | five artifact profiles and two large persistence models | one Batch, one stable ACP v1, minimal effect/delivery |
| Muse | active Node plus disabled 600-KB+ custom Rust source and two private journals | retain/simplify Node now; finite Rust deletion; official Rust SDK only as future replacement |
| OpenCode | active built-in host executor plus disabled standalone; old standalone contract lacks assistant parent while current upstream exposes it | select/live-qualify one exact native contract; standalone Rust only; exact parent result; delete built-in executor and giant journal machinery |
| Observation | unbound joined Governor view incomplete; metric hashing remains | local provider+OS+Git view without provider calls |
| Product breadth | council, ratification/ACK and automatic projection remain | outside compact-core completion |
| Tooling | compiler/checkout/cache equality and artifact validators remain | generic package boundary plus route-specific qualification |

These are findings and planned deletions, not completed runtime repairs.

## Adapter audit decisions

### Claude

Retain one Rust adapter with thin official SDK driver. Split into live session,
minimal NativeEffectRecord, exact StoreDelivery, latest Observation and persisted
terminal result. A lost JS resolver becomes unknown, not durable resume. Delete old
Node generations after finite transition.

### Codex

Retain the Rust app-server adapter and externally owned app-server. Replace the
full-snapshot checkpoint/tombstones with SessionRecord, per-effect records, exact
StoreDelivery and latest observation. Map native Goal directly. Delete Goal proof
packets, retired Python executor and profile ladders.

### Command Batch/ACP

Keep one Batch and one stable ACP v1 route. Eliot `AcpV2` is an artifact generation,
not upstream protocol v2. Preserve `session/load` no-fresh-fallback recovery and
option readback. Replace duplicate journals with minimal session/effect/delivery
owners. Module-owner Job/process group owns descendants.

### Muse

Keep `muse-sdk-1.3.0-bridge.13` as the only new-work executor during refactoring. It
is the active/default route and preserves explicit Operation-derived native command
IDs, targeted recovery and native Goal/result/usage behavior.

Retire disabled `muse-rust-controller.1@0.1.2` after concrete work disposition. Its
main state-machine files already exceed 605 KB before additional owner/observation/
result files. It duplicates transport, fold, journal, process custody and results.

The official upstream Rust SDK 1.4.11 is a future replacement candidate, not a third
route. It supplies typed MSP/session/fold/gap recovery and bounded close, but its
high-level turn path mints command IDs internally. Re-evaluate only when the required
production seam exists or a small low-level adapter deletes more code than it adds.

### OpenCode

Converge on `crates/swarm-adapter-opencode` as the only new-work executor after Q0-Q4.
The current built-in host `runtime/opencode_v2` remains only until standalone closes
the exact native contract.

The prospective standalone route is pinned to an older public projection with no
assistant input parent and correctly returns `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`.
Current upstream source reviewed at
`930c0c8b6a3c41ceb73b9372f878b5a1186b4d4e` exposes durable inbox identity,
process-global Session ownership, execution claims/restart interruption and public
assistant `parentID`. This is source evidence, not installed qualification.

OpenCode sequence:

```text
Q0  select and live-qualify one exact native contract with durable inbox,
    execution claim/restart terminal and assistant parentID
Q1  bind exact assistant result by assistant ID + parent input ID + part IDs
Q2  map steer/reply/cancel/background and exact restart readback
Q3  internalize MCP setup/readiness; remove first-input gate and public native.mcp.*
Q4  switch current route/package/config to standalone; freeze built-in new work
Q5  disposition built-in work; delete runtime/opencode_v2, legacy demand/worker slot,
    built-in profiles/launcher paths/fixtures
Q6  shrink standalone journal to SessionRecord, NativeEffectRecord,
    StoreDelivery and latest Observation; delete old pinned profiles
```

OpenCode has no reviewed native Harness Goal in the selected upstream source. Eliot
Goal sends ordinary messages. The external service remains externally owned; Eliot
never restarts/kills it. If restart cannot prove exact continuation or final
interruption for an input, the Operation stays unknown.

## Runtime evidence retained

Historical receipts remain scoped to their named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify planned protocol, TaskPacket, queue, correction flow, MCP,
adapter reductions, passive observation, executor deletion or installed fleet.

## Remaining audit tracks before a complete current-head claim

1. Finish GitHub/mailbox/gateway effect tracing and execute the public method → role →
   exact object → Store/effect privilege matrix.
2. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after numeric migration is implemented.
3. Measure IPC/Store fairness and writer occupancy after J0-J3; source review is not
   load qualification.
4. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries, not completion proof.
5. Re-run minimal current-head format/Clippy/build checks for each landed runtime
   slice; connected and broad tests follow completed product paths.

## Verification status

The latest OpenCode audit changes documentation only. Runtime implementation starts
with A0/A1, followed by the connected cases in
[implementation verification](agent-operations/implementation.md#verification).

Do not interpret older broad CI or fixtures as qualification of the planned protocol,
prompt, queue, review/correction flow, adapter consolidation, observation or executor
deletion. Record each landed source slice and net owned-code change here.