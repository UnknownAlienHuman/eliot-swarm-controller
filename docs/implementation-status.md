# Implementation status

## Current state

**Overall: AUDIT_PROGRAM_COMPLETE / RUNTIME_REFACTOR_IN_PROGRESS.**

The major product, ownership, adapter, Store, automation, observation, messaging,
GitHub and gateway surfaces have source-backed target and deletion boundaries. Runtime
source and the first Antigravity implementation slices were rechecked through
`eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` on 2026-10-20.

This does **not** mean that every line or live behavior is qualified. Remaining work
includes implementation of the documented slices, exact privilege and migration
execution matrices, load measurement and owner-machine qualification.

- [Architecture](agent-operations/architecture.md) — product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — source-backed excess and deletion map.
- [Observability](agent-operations/observability.md) — Governor source audit and R0–R6.
- [Donor map](agent-operations/donor-map.md) — accepted mechanisms and limits.

## Implemented runtime changes

### Duplicate host CLI removal

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

### Antigravity A0 — current v6 coordinate

Commit `cba98b755ff8d93405f91751a00d6dd6d91bbb60` synchronized the active Rust
Antigravity coordinate on version 6 across the Store capability selector, route
example, installer, package policy and native qualification. Historical v5 receipts
remain historical; active new-work tooling no longer selects v5.

A0 changes coordinates and deletes its one-shot CI runner. It is not a live native
turn qualification.

### Antigravity A1a — terminal outcome no longer depends on observation

Commit `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` made the warm-stream terminal
receipt self-contained at Store settlement:

- exact Operation and binding generation;
- current adapter boot;
- native conversation/scope;
- positive result ordinal;
- response digest;
- closed terminal status and Eliot disposition.

The Store validator no longer requires an `observation_id` or a duplicate
`local_execution_results` entry before accepting the terminal outcome. The production
Rust diff for this commit is 106 additions and 371 deletions, net **−265 Rust lines**;
the temporary one-shot workflow and patch script were also removed.

The one-shot pipeline formatted the changed Rust and ran warnings-denied Clippy for
`swarm-antigravity-adapter` and `swarm-kernel-host` before creating the final commit.
This is scoped build evidence, not installed/live Antigravity qualification.

## Active Antigravity slice

A0 is complete. A1 is only partly complete.

```text
A0   DONE  active v6 coordinate
A1a  DONE  terminal outcome decoupled from observation
A1b  NEXT  delete dispatch proof, refresh prerequisite and proof digests
A2   NEXT  durable exact Store-delivery spool
A3   NEXT  keep only real typed Antigravity actions
```

Still present after A1a:

- `AntigravityDispatchSessionProof` in shared contracts;
- proof generation during `agent.refresh`;
- latest-refresh lookup and proof validation in launch dispatch;
- assignment/lease/session proof digests;
- mandatory refresh-before-dispatch behavior;
- in-memory `pending_receipts` with no crash-safe Store-delivery spool;
- retained Node bridge profiles in the shared warm-route validator;
- unsupported/placeholder command surface that A3 must remove.

Therefore the original lost-delivery defect is **not fully fixed**. A terminal receipt
is now independently valid once delivered, but process exit can still lose an
unacknowledged in-memory outcome/result until A2 lands.

## Audit coverage

| Track | Coverage |
| --- | --- |
| Product boundary, ownership and overengineering mechanisms | deep |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review; A0/A1a landed |
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
| Windows/native restart, mixed providers and scale | unqualified except named receipts/issues |

“Major structural/source audit complete” is accurate. “Every runtime path tested and
qualified” is false.

## Implementation order

```text
A  Antigravity v6, self-contained terminal, exact durable Store delivery
B  current queue, explicit routes, source provider facts, root count
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

Do not skip from A1a to a shared adapter framework. The next connected change is A1b,
then A2 and A3.

## Confirmed current gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | A0/A1a landed; proof gate and in-memory delivery remain | no dispatch proof, exact durable Store delivery, real actions only |
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
| Coordination | messages, threads, consult, cards, contracts, watches, Concilium | one mailbox + optional tiny Task thread |
| GitHub poll | GET represented as workflow; mutable page cursor | bounded source collector with stable coverage/high-water |
| GitHub import | poll transaction creates/revises Tasks | source facts first; explicit import second |
| GitHub work pool | duplicate selection queue | ordinary Eliot queue/lane |
| GitHub writes | direct label effect + Forge PR effect | one GitHub API effect owner; Forge only remote Git |
| GitHub automation | activation/catch-up/quarantine/effect-drain workflow | explicit integration action only |
| Gateway secrets | plain file reads without private-file enforcement | shared bounded private-file loader |
| Gateway tenancy | one credential/bearer shared by many sessions | one gateway = one identity/profile; bounded sessions |
| Product breadth | council/ratification/automatic projection remain | outside compact Core; delete |
| Tooling | checkout/compiler/cache equality/artifact validators | generic package boundary + route qualification |

## Selected runtime and integration decisions

- **Claude:** retain one Rust adapter with a thin official SDK driver; split session,
  effect, Store delivery, latest observation and persisted result.
- **Codex:** retain Rust app-server adapter and external app-server; map native Goal;
  delete full checkpoints, event mirrors, tombstones, Python executor and profiles.
- **Command:** retain one Batch and one stable ACP v1; preserve exact `session/load`
  recovery and option readback; delete profile/journal duplication.
- **Muse:** keep Node bridge `.13` during refactor; simplify it and retire disabled
  custom Rust after concrete disposition.
- **OpenCode:** live-qualify one exact current native contract; keep standalone Rust;
  bind results by exact assistant + parent input + part IDs; delete built-in runtime.
- **Mailbox/coordination:** one normalized mailbox and optional minimal Task thread;
  delete participant/card/contract/watch/Concilium workflow products.
- **GitHub:** collection is a source reader, import is explicit, writes have one API
  effect owner, remote Git push stays Forge, automatic projection is deleted.
- **Gateway:** one loopback gateway process equals one Eliot identity/profile; secrets
  use the shared private bounded file primitive.

## Remaining work before a full qualification claim

1. Finish A1b–A3 and record the exact net production deletion.
2. Implement B–V in vertical slices, deleting each replaced path in the same change.
3. Execute the public method → role → exact object → Store/effect privilege matrix on
   the reduced public surface.
4. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after numeric migrations exist.
5. Measure IPC/Store fairness and writer occupancy after J0–J3.
6. Perform owner-machine qualification for provider restart, Windows process-family
   custody, mixed-provider execution, remote gateway and scale. Open Issues #1, #3,
   #4, #5, #11, #140 and #159 remain evidence boundaries.
7. Run minimal formatting and warnings-denied Clippy/build for each landed runtime
   slice. Focused connected cases run when the vertical path closes; broad tests follow
   the completed product.

## Verification status

A0 and A1a have scoped source/build evidence. They have not been live-qualified
against an installed Antigravity harness on the owner machine. This status update is
documentation only and claims no new Clippy/build/test result.
