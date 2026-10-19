# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Runtime source at
`e6b03b72ccec03b378c70b883f51ba2f34fde8d2` contains one completed code
reduction and a source-backed refactoring program. Later commits in this audit pass
change documentation only.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — confirmed excess/deletion boundaries.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

## Audit coverage

The repository has **not** received a complete current-head line-by-line audit of
every runtime path.

Issue #153 audited `ed1df447482d87ced09b60a12b301a7b0cfc4092` on
2026-10-14. Current runtime source is 644 commits ahead. That interval added or
substantially changed Muse Rust, Claude, Codex, Command ACP, OpenCode, Store
automation, launcher, custody, observation, checks, scripts, Forge, MCP and CI. The
old audit remains evidence, not current-head qualification.

Current coverage:

| Track | Coverage |
| --- | --- |
| Product boundary and ownership | deep source review; compact target defined |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review; first executable slice defined |
| Queue, route/provider state, capacity and WorkDispatch | deep review; duplicate state machines identified |
| Workspace/code scope and process custody | deep review; one-authority targets defined |
| Scheduling/optional workers | deep review; three overlapping control systems identified |
| TaskPrompt/model context | connected cross-adapter review; repeated identity/hash/JSON confirmed |
| Review/correction/acceptance automation | connected review; disposition/repair duplication confirmed |
| Public method routing/authorization | connected cross-registry review; no bypass claim, structural drift confirmed |
| Store schema/migrations | deep startup/migration review; exact-SQL compatibility system confirmed |
| Passive observation and executor duplication | architecture plus selected source paths; not every branch |
| Claude, Codex, Command, Muse and OpenCode internal state machines | partial targeted review; not complete line-by-line |
| Checks, scripts, Forge, GitHub effects, mailbox/gateway | partial ownership/error-boundary review |
| IPC/MCP fairness, backpressure and pagination | partial |
| Installed/native behavior, Windows custody and scale | unqualified except named historical receipts/open owner-machine cases |

Thus the architecture/reduction audit is mature; a complete current-runtime audit is
not yet proven.

## Completed runtime source change

Commit `25a8e3b` removed the host's duplicate general CLI and MCP launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation work is counted as runtime deletion.

## Current implementation sequence

```text
A0  synchronize active Antigravity v6 coordinates
A1  remove Antigravity observation/proof coupling
A2  add minimal durable Store-delivery spool
A3  keep only real typed Antigravity actions

B0  one current-state queue projection
B1  explicit ordered route policy; lane remains a view
B2  source-scoped provider availability/usage
B3  transactional root count; remove capacity ledger
B4  dispatch_once instead of WorkDispatch workflow state

C0  Task path-prefix write scope
C1  workspace lease as sole write authority; remove code.scope.*

D0  minimal process owner record
D1  conservative restart reconciliation

E0  one automation-entry schedule model
E1  one in-process wake/timer admission loop
E2  delete standalone automation scheduler service/protocol
E3  keep workers only for real external effects/custody

F0  compact model-visible TaskPacket
F1  separate Store identity from prompt text/adapter receipts
F2  one renderer for initial assignment, correction and resume

G0  review_once current-state query
G1  one task.request_changes transition and optional child agent.send
G2  bounded candidate-specific correction/no-progress handling
G3  accept_once through the common acceptance gate
G4  reduce automation steps to thin policy toggles

H0  separate public, internal-host and adapter protocols
H1  domain-owned typed parse/authorize/execute handlers
H2  normalized Operation scope; remove method-name visibility SQL
H3  delete each retired surface from Store, MCP, CLI, policy and tests together

I0  numeric schema_migrations/current version
I1  one finite bridge from known digest-marker states
I2  delete exact base/extension SQL digests, custom DDL lexer/reference DB and duplicate verifiers
I3  readers validate application ID + supported schema version
```

Shared adapter plumbing, executor retirement, passive observation and remaining Store
reduction follow only after vertical slices prove their boundaries.

## Confirmed structural gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity result | terminal outcome depends on observation/proof; in-memory delivery can disappear | self-contained outcome plus exact Store-delivery spool |
| Adapter bus | string/vendor commands, package/schema ladders, repeated hello/delivery loops | typed semantic commands and proven shared plumbing only |
| Execution | parallel generations remain for most harnesses | one current executor/format per retained runtime |
| Prompt | controller identity JSON and repeated hashes enter model/adapters | bounded TaskPacket; identity stays in Store |
| Queue | WorkDispatch copies Task facts into cursors/pending/slots/links | ranked query plus one atomic admission |
| Routes/provider | duplicated policy/proof records and sticky per-binding facts | explicit ordered aliases plus source-scoped freshness |
| Capacity | second meta ledger mirrors Operation lifecycle and counts read/control calls | transactional root count/read projection |
| Review/correction | disposition, repair and acceptance engines repeat candidate identity | shared Task transitions and thin `*_once` automation |
| Workspace | advisory code scope beside actual lease | Task path prefixes plus one workspace lease |
| Process | owner record repeats package/protocol/contract | minimal custody identity and conservative recovery |
| Scheduling | in-process, standalone and automation loops overlap | one in-process due/current-state admission loop |
| Public API | registry, MCP, validator, router, apply, participant and visibility maps overlap | three protocols plus domain-owned exact authorization |
| Schema | base and extensions pinned to exact SQL; special migration compilers/verifiers | numeric transactional migrations and finite marker bridge |
| Observation | unbound joined Governor view incomplete; process sampling hashes executables | local provider + OS + Git facts without provider calls |
| Acceptance | direct/automatic paths do not cleanly share all exact candidate evidence | one candidate-specific review/check gate |
| Store/automation | writer-heavy reads, meta indexes, transfer chains and optional engines | bounded readers, explicit migrations, stable identities, deletion |
| Product breadth | council, ratification/ACK, publication projection remain | outside compact-core completion |
| Tooling | compiler/checkout/cache equality and artifact-specific validators | generic package boundary plus route-specific behavior checks |

These are findings/plans, not completed repairs.

## Runtime evidence retained

Existing receipts remain scoped to named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the compact protocol, TaskPacket, queue, correction flow,
migrations, passive observation, executor deletion or installed fleet.

## Remaining audit tracks before “complete current-head audit”

1. Finish per-adapter state-machine audits for current Claude, Codex, Command ACP,
   Muse, OpenCode and Antigravity implementations; select exact executor retirement.
2. Trace all external effects end to end: native prompts, process launch/kill,
   filesystem, Git/GitHub, checks and scripts; prove one retry/readback owner.
3. Complete IPC/MCP backpressure, slow-consumer, pagination, fairness, cancellation and
   writer-occupancy review.
4. Recheck retained-format/corruption behavior after the numeric migration design is
   implemented; current exact-DDL verification is understood but not yet replaced.
5. Review remaining Store object-read/authorization domains after removal-target API
   surfaces are deleted; do not spend effort perfecting visibility for products being removed.
6. Run minimal current-head format/Clippy/build checks per implemented slice, then
   connected tests for closed paths.
7. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries.

## Verification status

This audit pass changes documentation only. Runtime implementation begins with A0/A1,
using formatting and minimal affected warnings-denied Clippy/build checks, followed by
the connected cases in
[implementation verification](agent-operations/implementation.md#verification).

Do not interpret old CI/fixtures as qualification of the planned protocol, prompt,
queue, review flow, migration system, observation or executor deletion. Record each
landed source slice and net owned-code change here.
