# Implementation status

## Current state

**Overall: AUDIT_INCOMPLETE / RUNTIME_REFACTOR_IN_PROGRESS.**

The major product and code-growth mechanisms now have source-backed findings and a
single vertical implementation order. Historical continuity requirements were
rechecked against the current mailbox/GM paths, Claude Swarm evidence, GitHub
integration and remote gateway through
`6e470726c633209f98fea2bfd7bd12d039f285f1` on 2026-10-21.

This is still not an exhaustive current-head proof. Retained public actions, failure
branches and source changes must continue to be reconciled as implementation lands.
Live installed runtimes, Windows custody, migrations, load and scale remain separate
qualification work.

- [Architecture](agent-operations/architecture.md) — product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — source-backed excess, preservation and
  deletion map.
- [Observability](agent-operations/observability.md) — Governor source audit and R0–R6.
- [Donor map](agent-operations/donor-map.md) — accepted mechanisms and limits.

## Landed runtime changes

### Duplicate host CLI

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

### Antigravity A0 — active v6 coordinate

Commit `cba98b755ff8d93405f91751a00d6dd6d91bbb60` synchronized active Rust
Antigravity selection on version 6 across Store capability selection, route example,
installer, package policy and native qualification. Historical v5 receipts remain
history. This is not a live native turn qualification.

### Antigravity A1a — terminal decoupling landed, check unresolved

Commit `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` removed the terminal receipt's
required Store `observation_id` and duplicate `local_execution_results` dependency.
The recorded production Rust delta for that commit is net −265 lines.

A later temporary repair run compiled the test binary but failed the connected rejected
terminal regression: projected `completion_condition` was `Null` instead of
`native_terminal_result_observed`. It did not reach Clippy or commit/push. The green
main Rust workflow for `912df90f` skipped changed-Rust compilation/Clippy. Therefore
A1a is landed but not verified complete.

## Active Antigravity slice

```text
A0   DONE                 active v6 coordinate
A1a  LANDED / CHECK OPEN   terminal decoupling; rejected/cancelled path unresolved
A1b  NOT IMPLEMENTED       delete dispatch proof, refresh prerequisite and digests
A2   NOT IMPLEMENTED       durable exact Store-delivery spool
A3   NOT IMPLEMENTED       keep only real typed actions
```

Still present:

- `AntigravityDispatchSessionProof` and refresh producer;
- launch lookup/validation of latest refresh proof;
- assignment/lease/session proof digests and mandatory refresh-before-dispatch;
- crash-unsafe in-memory `pending_receipts`;
- retained Node bridge profiles in shared route validation;
- unsupported/placeholder command surface.

The original lost-delivery defect is not fixed until A2.

## Audit coverage

The labels below describe evidence depth, not percentage completion.

| Track | Current evidence and remaining limit |
| --- | --- |
| Product boundary/ownership/overengineering | deep structural review |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity result/delivery | deep connected review; A0/A1a landed; latest rejected-terminal check failed |
| Queue, WorkDispatch, route/provider/capacity | deep targeted review |
| Workspace scope, process custody and scheduling | deep targeted review |
| TaskPacket and review/correction/acceptance | deep connected review |
| Public method routing/authorization | deep structural review; exhaustive current-path/privilege execution remains |
| Store schema/migrations | deep structural review; concrete corruption/partial-upgrade execution remains |
| IPC/MCP multiplex/subscriptions/hot reads | deep source review; live fairness/load remains |
| Check, Script and Forge ownership | deep source review |
| Claude | deep connected source review; installed route not re-qualified |
| Codex | deep connected source review; installed route not re-qualified |
| Command Batch/ACP | deep connected source review; installed agents not re-qualified |
| Muse Node/Rust/upstream SDK | deep source review; installed route not re-qualified |
| OpenCode built-in/standalone/upstream | deep source review; exact installed contract not re-qualified |
| Passive Governor provider/process/worktree/remote join | deep source review; collectors/live fleet not qualified |
| Mailbox/GM continuity/coordination | deep source + historical requirement review; replacement not implemented |
| GitHub collection/import/effects/projection | deep source review; replacement not implemented |
| Remote gateway | deep source review; private-file/session fixes not implemented |
| Windows/native restart, mixed providers and scale | unqualified except named receipts/issues |

The previous `AUDIT_PROGRAM_COMPLETE` declaration remains withdrawn. Major findings are
mature enough for implementation; exhaustive source and live qualification are not
complete.

## Requirement preservation checked in this pass

### Manager continuity

Current `gm.handover` changes designation/epoch only. It does not rewrite Attempt
owner, restart native work or reassign historical mail. Its report/mailbox watermarks
are explicitly resynchronization hints, not delivery acknowledgements. Same durable
`client_id` reconnects to its own state; a different successor receives current
Task/Attempt/native authority through exact GM scope.

### Mailbox/coordination

Current delivery is durable but embedded in settled Operation JSON. `message.read`
scans the global observation timeline; typed coordination adds `cmsg-*` and random
delivery IDs beside Operation/request IDs. The compact replacement keeps directed
mail, cursor/readback, reply/cancel and historical recipients while deleting
participant/card/consult/watch/contract/integration/Concilium/code-scope workflow
products.

### GitHub

Current source poll is a GET wrapped in Operation/lease/recovery state. It persists a
page number while reading `updated desc`, and the poll commit also creates/revises
Tasks and updates a duplicate work pool. Managed labels use one direct effect path;
PR descriptions use a second Forge-worker API path; automatic projection adds another
workflow state machine. The target separates collector, explicit import and one
GitHub API effect owner while preserving immutable repository identity, bounded reads,
at-most-one write and exact readback.

### Gateway

Current gateway correctly retains loopback-only bind, fixed credential/profile,
restricted MCP profile, bearer/Host/Origin/body/header/request/connection bounds and
timeout-as-unknown semantics. Remaining source defects are plain secret-file reads,
missing explicit negotiated-session/idle bounds and the extra compatibility launcher.
One listener remains one Eliot principal/profile; TLS/edge identity stays external.

## Implementation order

```text
A  Antigravity v6, terminal, exact durable Store delivery
B  current queue, routes, provider facts, root count
C  one workspace write lease
D  minimal process custody/restart
E  one scheduler/current-state automation loop
F  compact TaskPacket
G  ordinary review/correction/acceptance
H  separate public/internal/adapter protocols
I  numeric migrations and WAL reads
J  concurrent IPC/MCP and exact hot reads
K  one Check, Script, remote-Git and GitHub API effect owner
L  shared adapter shell after two current users delete duplication
M  Claude reduction
N  Codex reduction
O  Command Batch/ACP reduction
P  Muse Node consolidation/custom-Rust retirement
Q  OpenCode standalone consolidation
R  joined local Governor observation
S  optional-product/tooling deletion
T  normalized mailbox; delete coordination platform
U  GitHub source/import/effect separation
V  private single-tenant standalone gateway
```

Resolve A1a discrepancy first. Then A1b, A2 and A3. Do not jump to a shared adapter
framework.

## Confirmed gaps and targets

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | A0/A1a landed; check open; proof gate/in-memory delivery remain | no dispatch proof, durable exact Store delivery, real actions only |
| Adapter bus | vendor strings, profiles, repeated hello/delivery/proofs | typed semantics; shared shell only after two users |
| Execution | parallel runtime generations | one current executor/format per runtime |
| Queue | WorkDispatch copies Task state into workflow state | ranked query + atomic `dispatch_once` |
| Provider/capacity | provider proof matrix + second lifecycle ledger | source facts + transactional root count |
| Workspace | advisory `code.scope.*` beside real lease | Task path prefixes + one lease |
| Scheduling | three control systems | one in-process due/current-state loop |
| Prompt | controller JSON/hashes enter model/adapters | compact TaskPacket; identity in Store |
| Review/correction | repair/disposition/acceptance workflows repeat candidate state | common Store transitions + thin `*_once` |
| Public API | parallel registries/routers include removal products | separate protocols + domain authorizers |
| Store schema | SQL text SHA/extension verification | numeric transactional migrations |
| MCP | serialized client, many pollers, global attention scan | direct multiplex + one pump + exact reads |
| Native streams | ephemeral ring on Store writer | short authorization + ring outside writer |
| Checks/Scripts/Forge | duplicate execution/wire/custody/hashes | one owner per domain + shared OS primitives |
| Claude | generic journals/callback store/memory-only terminal | session/effect/delivery/observation/result records |
| Codex | giant checkpoint/event copies/tombstones | connection/session/effect/delivery/latest observation |
| Command | five profiles/two persistence models | one Batch + stable ACP v1 |
| Muse | active Node + disabled custom Rust | simplify Node, delete Rust after disposition |
| OpenCode | built-in + standalone + universal journal | exact current standalone contract only |
| Governor | binding-rooted inventory/parallel partial views | source subjects + assignment edges + one join |
| Mailbox | Operation JSON + global timeline scan + multiple message IDs | normalized recipient rows + one message ID |
| Coordination | threads/consult/cards/contracts/watches/Concilium/code scope | mailbox + optional tiny Task thread |
| GitHub poll | GET as workflow + mutable page cursor | bounded source collector with stable coverage/high-water |
| GitHub import | poll transaction mutates Tasks | source facts first; explicit import second |
| GitHub work pool | duplicate selection queue | ordinary Eliot queue/lane |
| GitHub writes | direct label + Forge PR effect | one GitHub API effect owner; Forge only remote Git |
| GitHub automation | activation/catch-up/quarantine/drain workflow | explicit integration action only |
| Gateway secrets | plain file reads | shared bounded private-file loader |
| Gateway tenancy | one identity shared by unbounded negotiated sessions | one process = one identity/profile; bounded sessions/idle |
| Product breadth | council/ratification/automatic projection remain | delete outside compact Core |
| Tooling | checkout/compiler/cache equality and validators | generic package boundary + route qualification |

## Remaining audit and qualification work

1. Continue retained action tracing from caller through authorization, Store mutation,
   external effect/readback and final consumer, including cancellation/restart/late
   result branches.
2. Reconcile old requirement evidence for remaining retained domains and refresh
   findings after every runtime slice.
3. Execute public privilege matrix on the reduced surface.
4. Execute migration/corruption/partial-upgrade cases after numeric migrations exist.
5. Measure IPC/Store fairness and writer occupancy after J0–J3.
6. Owner-machine qualification: provider restart, Windows process family, mixed
   providers, gateway and scale. Open Issues #1, #3, #4, #5, #11, #140 and #159 remain
   evidence boundaries.
7. Run minimal formatting and warnings-denied Clippy/build for every landed runtime
   slice; focused connected cases when the vertical path closes.

## Verification status

Evidence checked for source head `912df90f`:

- Rust run `39463118348` succeeded only in classification/tooling jobs; changed Rust
  compilation, Clippy and rustfmt jobs were skipped.
- Repair run `39463118074`, job `118767359791`, modified a temporary runner tree,
  compiled the test binary and failed the Antigravity terminal regression because
  `completion_condition` was null. It did not reach Clippy or push.

This documentation pass claims no new runtime compilation, Clippy, test or live
qualification result.
