# Implementation status

## Current state

**Overall: AUDIT_PROGRAM_COMPLETE / RUNTIME_REFACTOR_NOT_STARTED EXCEPT ONE DELETION.**
Runtime source was audited through
`85b40e63a9225db05a480c027cca49f4913e4540` on 2026-10-20. The major product,
ownership, adapter, Store, automation, observation, messaging, GitHub and gateway
surfaces now have source-backed target/deletion boundaries.

This does **not** mean every line or live behavior is qualified. Remaining work is
implementation, exact privilege/migration execution matrices, load measurement and
owner-machine qualification.

- [Architecture](agent-operations/architecture.md) — product law.
- [Implementation](agent-operations/implementation.md) — only work order.
- [Findings](agent-operations/refactoring.md) — source-backed excess/deletion map.
- [Observability](agent-operations/observability.md) — Governor source audit and R0–R6.
- [Donor map](agent-operations/donor-map.md) — accepted mechanisms and limits.

## Audit coverage

| Track | Coverage |
| --- | --- |
| Product boundary, ownership and overengineering mechanisms | deep |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review |
| Queue, WorkDispatch, route/provider/capacity | deep |
| Workspace/code scope/process custody/scheduling | deep |
| TaskPacket and review/correction/acceptance | deep connected review |
| Public method routing/authorization ownership | deep structural review; execution matrix remains |
| Store schema/migration ownership | deep structural review; concrete corruption/partial-upgrade execution remains |
| IPC/MCP multiplex/subscriptions/task projection/native streams | deep source review; live load remains |
| Check/Script/Forge execution ownership | deep source review |
| Claude | deep connected source review; installed route not re-qualified |
| Codex | deep connected source review; installed route not re-qualified |
| Command Batch/ACP | deep connected source review; installed agents not re-qualified |
| Muse Node/Rust and upstream SDK | deep source review; installed route not re-qualified |
| OpenCode built-in/standalone and current upstream | deep source review; exact installed contract not re-qualified |
| Passive Governor provider/process/worktree/remote join | deep source review; collectors/live fleet not qualified |
| Mailbox and coordination products | deep source review; compact replacement not implemented |
| GitHub source/import/effects/automatic projection | deep source review; compact replacement not implemented |
| Remote gateway | deep source review; private-file/session fixes not implemented |
| Windows/native restart, mixed providers and scale | unqualified except named historical receipts/issues |

“Major structural/source audit complete” is accurate. “All runtime paths tested and
qualified” is false.

## Implemented runtime reduction

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation pass counts as runtime reduction.

## Implementation order

```text
A  Antigravity v6 + self-contained terminal + exact Store delivery
B  current queue + explicit routes + source provider facts + root count
C  one workspace write lease
D  minimal process custody and restart
E  one scheduler/current-state automation loop
F  compact TaskPacket
G  ordinary review/correction/acceptance
H  separate public/internal/adapter protocols
I  numeric migrations and WAL read conversion
J  concurrent IPC/MCP and exact hot reads
K  one Check, Script, remote-Git and GitHub effect owner
L  shared adapter shell after two current users delete duplication
M  Claude reduction
N  Codex reduction
O  Command Batch/ACP reduction
P  Muse Node consolidation and custom-Rust retirement
Q  OpenCode standalone consolidation
R  joined local Governor observation
S  optional-product/tooling deletion
T  normalized single mailbox; delete coordination platform
U  GitHub source/import/effect separation
V  private single-tenant standalone gateway
```

The first runtime coding path remains **A0 → A1 → A2 → A3**. Do not start with a
shared framework or a repository-wide schema rewrite.

## Confirmed current gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | terminal can be lost; observation/proof coupling; v5/v6 drift | self-contained terminal and exact v6 Store-delivery spool |
| Adapter bus | vendor strings, profile ladders, repeated hello/delivery/proofs | typed semantics; shared shell only after two users |
| Execution | parallel generations for most harnesses | one current executor/format per runtime |
| Queue | WorkDispatch copies Task state into another workflow | ranked query + atomic `dispatch_once` |
| Provider/capacity | vendor proof matrix + second lifecycle ledger | source facts + transactional root count |
| Workspace | `code.scope.*` beside real lease | Task path prefixes + one lease |
| Scheduling | three control systems | one in-process due/current-state loop |
| Prompt | controller JSON/hashes enter model/adapters | compact TaskPacket; identity in Store |
| Review/correction | disposition/repair/acceptance engines repeat candidate state | common Store transitions + thin `*_once` |
| Public API | parallel registries/routers retain removal products | separate protocols + domain authorizers |
| Store schema | SQL text SHA/extension verifiers | numeric transactional migrations |
| MCP | serialized client, many pollers, global attention scan | direct multiplex + one pump + exact reads |
| Native streams | ephemeral ring on writer | short authorization + ring outside writer |
| Checks/Scripts/Forge | duplicate execution/wire/custody and per-effect hashes | one owner per domain + shared OS primitives |
| Claude | generic journals/callback store/memory-only terminal | session/effect/delivery/observation/result records |
| Codex | giant full checkpoint/raw event copies/tombstones | connection/session/effect/delivery/latest observation |
| Command | five profiles/two persistence models | one Batch + stable ACP v1 |
| Muse | active Node + disabled custom Rust | simplify Node, delete Rust after disposition |
| OpenCode | built-in + standalone + universal journal | exact current standalone contract only |
| Governor | binding-rooted inventory and parallel partial views | independent sources/subjects + assignment edges + one join |
| Process/Git | receipt CLI and launch-time lease facts | live source collectors with source time/gaps |
| Remote status | only during Forge publication | independent bounded remote-ref cache |
| Mailbox | messages embedded in Operation JSON; global timeline scan | normalized recipient/cursor rows; one message ID |
| Coordination | legacy/typed messages, threads, consult, cards, contracts, watches, Concilium | one mailbox + optional tiny Task thread |
| GitHub poll | GET represented as workflow; mutable page cursor | bounded source collector with stable high-water/coverage |
| GitHub import | poll transaction creates/revises Tasks | source facts first; explicit import second |
| GitHub work pool | duplicate selection queue | ordinary Eliot queue/lane |
| GitHub writes | direct label effect + Forge PR effect | one GitHub API effect owner; Forge only remote Git |
| GitHub automation | activation/catch-up/quarantine/effect-drain workflow | explicit integration action only |
| Gateway secrets | plain file reads without private-file enforcement | shared bounded private-file loader |
| Gateway tenancy | one credential/bearer shared by many sessions | one gateway = one identity/profile; bounded sessions |
| Product breadth | council/ratification/automatic projection remain | outside compact Core; delete |
| Tooling | checkout/compiler/cache equality/artifact validators | generic package boundary + route qualification |

These are findings and planned deletions, not completed repairs.

## Adapter decisions

### Claude

Retain one Rust adapter with a thin official SDK driver. Split live session,
NativeEffectRecord, exact StoreDelivery, latest Observation and persisted/readable
terminal result. Lost callback resolver remains unknown. Delete old Node generations
and generic journals after concrete disposition.

### Codex

Retain Rust app-server adapter and externally owned app-server. Use connection,
session, per-effect, StoreDelivery and latest-observation records. Map native Goal.
Delete full checkpoints, event mirrors, tombstones, Goal proofs, Python executor and
profile ladders.

### Command Batch/ACP

Keep one Batch and one stable ACP v1. Preserve `session/load` no-fresh fallback and
option readback. Delete profile/schema ladders, duplicate journals and vendor-public
methods.

### Muse

Keep `muse-sdk-1.3.0-bridge.13` during refactor. Simplify its records and custody.
Disposition and delete disabled custom Rust. Reconsider official Rust SDK only when it
supports the required command-ID boundary and is a net deletion.

### OpenCode

Live-qualify one exact installed contract with durable inbox, per-session execution
owner, restart terminal and assistant `parentID`. Keep standalone Rust only; bind
result by exact assistant + parent input + part IDs. Delete built-in runtime,
legacy demand/MCP/owned-service paths and giant journal machinery after cutover.

## Integration decisions

### Mailbox/coordination

Use one durable message ID equal to the admitted Operation ID. Normalize messages by
recipient and cursor. Keep only send/read/reply/cancel and, if proven necessary, a
small Task-scoped thread grouping. Delete delivery-ID indirection, participant token
registry, cards, consult/watch/contracts/integration/Concilium/code-scope products and
custom index/backfill/cursor families.

### GitHub

Collection is a source reader, not an Operation workflow. Separate source facts from
Task import. Delete GitHub work-pool selection. Preserve exact write-once/readback
semantics in one GitHub API effect owner. Delete automatic projection workflow. Feed
remote refs into Governor observation independent of publication.

### Gateway

Keep a standalone loopback-only single-tenant bridge with fixed Eliot identity/profile.
Use shared private-file loading, bound sessions/idle time and remove kernel-host
compatibility wrapper. Do not expand it into a platform.

## Remaining work before a full qualification claim

1. Implement A–V in vertical slices and record net production deletion.
2. Execute the public method → role → exact object → Store/effect privilege matrix on
   the reduced public surface.
3. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after numeric migrations exist.
4. Measure IPC/Store fairness and writer occupancy after J0–J3.
5. Perform owner-machine qualification for provider restart, Windows process-family
   custody, mixed-provider execution, remote gateway and scale. Open Issues #1, #3,
   #4, #5, #11, #140 and #159 remain evidence boundaries.
6. Run minimal format/warnings-denied Clippy/build per landed runtime slice. Focused
   connected cases run when a vertical path closes; broad tests follow the completed
   product.

## Verification status

This audit pass changes documentation only. No new Clippy/build/test result is
claimed. Historical receipts remain scoped to their named artifact/revision and do not
qualify the planned protocol, queue, mailbox, GitHub, gateway, Governor collectors or
executor deletion.
