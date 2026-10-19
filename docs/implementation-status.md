# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Runtime source was audited through
`96a0d5a6632cfbd541e804d7969d3e76e716bf6a`; this commit changes documentation only.
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
| Public method routing/authorization ownership | deep structural review; no full privilege matrix execution |
| Store schema/migration ownership | deep structural review; corruption/upgrade matrix not fully executed |
| IPC/MCP multiplex, subscriptions, task projection and native streams | deep source review; live load not qualified |
| Passive observation and executor duplication | architecture plus selected source paths |
| Claude, Codex, Command, Muse and OpenCode internal state machines | partial targeted review |
| Checks, scripts, Forge, GitHub effects, mailbox/coordination and gateway | partial |
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
K-L    shared adapter shell after proof, one executor per harness, Goals,
       passive observation and remaining Store/product deletion
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
| Observation | unbound joined Governor view incomplete; metric hashing remains | local provider+OS+Git view without provider calls |
| Product breadth | council, ratification/ACK and automatic projection remain | outside compact-core completion |
| Tooling | compiler/checkout/cache equality and artifact validators remain | generic package boundary plus route-specific live qualification |

These are findings and planned deletions, not completed runtime repairs.

## IPC/MCP audit result

The host IPC server already bounds connections, in-flight requests, frames and write
time and drains admitted work after disconnect. The client already correlates
concurrent out-of-order replies and never replays an uncertain request. Do not replace
it with a broker.

The unnecessary complexity is above that boundary:

- MCP holds its client-slot mutex during the whole awaited request, reserializing the
  multiplexed client;
- one MCP session can create 16 independent 250 ms subscription pollers and queues;
- lag handling scans additional Store pages to count dropped category matches even
  though notifications are documented as hints;
- `tasks/get` pages global attention state to find one Operation;
- ephemeral native stream publication/read uses the writer thread.

The implementation now defines J0-J3 to remove these layers without changing
uncertain-effect semantics.

## Runtime evidence retained

Historical receipts remain scoped to their named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned protocol, TaskPacket, queue, correction flow, MCP
simplification, passive observation, executor deletion or installed fleet.

## Remaining audit tracks before a complete current-head claim

1. Complete per-adapter state-machine audit for current Claude, Codex, Command ACP,
   Muse, OpenCode and Antigravity and select executor retirements.
2. Trace every external effect owner: native prompts, process launch/kill, filesystem,
   Git, GitHub, checks and scripts; verify one retry/readback owner and no guessed replay.
3. Execute the public method → role → exact object → Store/effect matrix, especially
   removed-product boundaries and participant/GM projections.
4. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after the numeric migration slice is implemented.
5. Measure IPC/Store fairness and writer occupancy after J0-J3; current source review
   identifies the mechanisms but is not a load qualification.
6. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries.

## Verification status

This pass changes documentation only. Runtime implementation begins with A0/A1 and
the minimal affected formatting/warnings-denied Clippy/build checks. Connected tests
run when each vertical slice closes; broad qualification follows the complete product
path. Record each landed source slice and net owned-code change here.
