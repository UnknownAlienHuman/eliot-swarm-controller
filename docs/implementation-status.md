# Implementation status

## Current state

**Overall: AUDIT_INCOMPLETE / RUNTIME_REFACTOR_IN_PROGRESS.**

The major product, ownership, adapter, Store, automation, observation, messaging,
GitHub and gateway surfaces have source-backed findings and proposed deletion
boundaries. That is a substantial structural review, not proof that all current code,
requirements, cross-module contracts and failure paths have been audited.

Status and CI evidence were rechecked against
`912df90f86320ff556c1dba23badc2c69e518e34` on 2026-10-21. This status correction is
not a new exhaustive source review. The earlier `AUDIT_PROGRAM_COMPLETE` declaration
was premature and is withdrawn.

Still required at source-review level: reconcile retained requirements against the
old specifications/scripts; trace retained public actions through current callers,
authorizers, Store transitions, adapters and result consumers; cover failure,
cancellation and restart branches; and reconcile findings against intervening source
changes. These are audit gaps, not merely missing live tests. Maintain the evidence in
the existing findings/implementation documents, not another roadmap or workflow.

Privilege/migration execution matrices, load measurement and owner-machine
qualification are separate remaining verification work. They do not need to precede
the owner's requested code-first implementation and minimal affected Clippy checks.

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

### Antigravity A1a — terminal outcome decoupling landed; verification open

Commit `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` changed the warm-stream terminal
receipt to carry its own Store settlement facts:

- exact Operation and binding generation;
- current adapter boot;
- native conversation/scope;
- positive result ordinal;
- response digest;
- closed terminal status and Eliot disposition.

The Store validator no longer requires an `observation_id` or a duplicate
`local_execution_results` entry before accepting the terminal outcome. The recorded
production Rust diff for that commit is 106 additions and 371 deletions, net
**−265 Rust lines**; its temporary one-shot workflow and patch script were also
removed. This is a per-commit delta, not a cumulative current-head deletion count.

The original one-shot pipeline recorded formatting and warnings-denied Clippy for
`swarm-antigravity-adapter` and `swarm-kernel-host`. Those checks do not establish
correctness of later changes or rejected/cancelled terminal paths. The failed repair
run described under Verification status leaves that boundary open; A1a is not a
verified-complete slice.

## Active Antigravity slice

A0 is complete. A1 source changes have landed but are only partly complete and have
an unresolved verification result.

```text
A0   DONE                 active v6 coordinate
A1a  LANDED / CHECK OPEN   terminal decoupling; rejected/cancelled path unresolved
A1b  NOT IMPLEMENTED       delete dispatch proof, refresh prerequisite and digests
A2   NOT IMPLEMENTED       durable exact Store-delivery spool
A3   NOT IMPLEMENTED       keep only real typed Antigravity actions
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

Therefore the original lost-delivery defect is **not fully fixed**. Decoupling removes
one delivery dependency, but process exit can still lose an unacknowledged in-memory
outcome/result until A2 lands. Rejected/cancelled terminal evidence also remains an
open check; do not claim every terminal outcome is already independently valid.

## Audit coverage

The labels below record prior qualitative review depth, not an exhaustive inventory
of current files, branches or contracts. Reviewing every named subsystem does not
prove all its paths were reviewed. Earlier block counts and percentage estimates
must not be presented as measured audit completeness.

| Track | Recorded review depth and known limit |
| --- | --- |
| Product boundary, ownership and overengineering mechanisms | deep structural review |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review; A0/A1a landed; latest rejected-terminal check failed |
| Queue, WorkDispatch, route/provider/capacity | deep targeted review |
| Workspace/code scope/process custody/scheduling | deep targeted review |
| TaskPacket and review/correction/acceptance | deep connected review |
| Public method routing/authorization ownership | deep structural review; complete current-path coverage and execution matrix remain unestablished |
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

Source-backed reduction findings exist across the major subsystems. Exhaustive
current-head source audit and complete runtime qualification are both unestablished.

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

Resolve the A1a discrepancy before claiming that slice complete. The subsequent
connected changes remain A1b, A2 and A3, not a shared adapter framework.

## Confirmed current gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | A0/A1a landed; verification open; proof gate and in-memory delivery remain | no dispatch proof, exact durable Store delivery, real actions only |
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

## Remaining audit and implementation work

Source audit is not complete merely because all implementation headings exist. Close
current caller-to-consumer and failure-branch coverage in the existing findings,
including preservation of required behavior from old specifications and Claude
scripts. Keep proposed simplifications distinct from demonstrated equivalence.

1. Resolve A1a verification, finish A1b–A3 and record exact net production deletion.
2. Implement B–V in vertical slices, deleting each replaced path in the same change.
3. Complete source tracing of public method → role → exact object → Store/effect
   authorization and then execute the privilege matrix on the reduced surface.
4. Check current migration/retained-format readers and writers; execute concrete
   migration/corruption/partial-upgrade cases after numeric migrations exist.
5. Review IPC/Store concurrency and cancellation boundaries; measure fairness and
   writer occupancy after J0–J3.
6. Perform owner-machine qualification for provider restart, Windows process-family
   custody, mixed-provider execution, remote gateway and scale. Open Issues #1, #3,
   #4, #5, #11, #140 and #159 remain evidence boundaries.
7. Run minimal formatting and warnings-denied Clippy/build for each landed runtime
   slice. Focused connected cases run when the vertical path closes; broad tests follow
   the completed product.

## Verification status

Evidence checked on 2026-10-21 at source head `912df90f`:

- [Rust run 39463118348](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/39463118348)
  succeeded in classification/tooling jobs. Rust package compilation, Clippy and Rust
  formatting jobs were skipped. It is not a successful Rust verification of this head.
- [Repair run 39463118074, job 118767359791](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/39463118074/job/118767359791)
  applied temporary runtime/test patches, compiled the test binary, then failed the
  Antigravity terminal regression: `completion_condition` was `Null` instead of
  `native_terminal_result_observed` in the rejected-terminal assertion. This evidence
  concerns that patched runner tree, not an untouched checkout of `main`.
- The repair run did not reach its Clippy or commit/push steps. Its temporary repair
  must not be described as a published or verified runtime fix. Root cause still
  requires matching the producer projection, intended contract and test expectation.

A0 and the original A1a commit have historical scoped source/build evidence, not live
qualification of an installed Antigravity harness. This status-only correction claims
no new compilation, Clippy, tests or runtime repair.
