# R42. Audit closeout index: proof state, implementation owner and remaining handoffs

**Snapshot:** 8 October 2026. **ELIOT evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`.

This index answers one operational question: **is the source audit and preparation for implementation complete?**

It is not a new architecture document. It maps the reviewed findings to exact implementation owners, records refutations/corrections and makes the remaining preparation work explicit. A count of Markdown pages or open PRs is not completion.

## 1. Closeout states

| State | Meaning |
|---|---|
| `READY_HANDOFF` | Current source chain is identified; one PR owns a connected vertical slice with files/symbols, ordered edits, deletion list, forbidden designs, fixtures and scoped gate. |
| `CODE_CANDIDATE` | Production code exists on a PR branch, but its exact current head still requires its declared compiler/behavioral gates. |
| `PARTIAL_OWNER` | A PR owns part of the class, but a named production seam or consumer remains outside its current handoff. |
| `NEEDS_SOURCE_PASS` | Confirmed/credible high-value issue has no sufficiently exact implementation card yet. |
| `POLICY_DECISION` | Source behavior is understood, but changing it requires an explicit product/owner decision rather than an inferred bug fix. |
| `REFUTED` | The stated failure mechanism is contradicted by the current source/contract; no implementation task should be created for that allegation. |
| `CONDITIONAL` | The code predicate exists, but reachability through the public validator/producer is not yet established. |
| `DEFERRED_LOW` | Confirmed low-impact/local defect, intentionally grouped behind a higher-level owner or final cleanup wave. |

A suspicion registry entry is never promoted directly to `READY_HANDOFF`. It must be traced through the current producer, persisted form, consumer and public/effect boundary.

## 2. Definition: audit and implementation preparation complete

Preparation is complete only when all of the following are true on this index:

1. every confirmed P0/P1 or systemic defect class is `READY_HANDOFF`, `CODE_CANDIDATE`, `POLICY_DECISION` or `REFUTED`;
2. no P0/P1 row remains `NEEDS_SOURCE_PASS` or ambiguously owned by two PRs;
3. each ready PR names the exact current source symbols, one public production caller, the existing ELIOT functions to reuse and the old responsibility to delete;
4. donor use has exact source revision/license, one adapted mechanism and an explicit non-import boundary;
5. cross-PR shared files/types have one serial owner and a rebase order;
6. the implementation wave graph has no cycle and no “temporary” duplicate authority without a deletion condition;
7. the main audit removes refuted allegations and labels conditional evidence honestly;
8. final project-wide qualification is separated from source-audit completion. Docs-only handoffs do not claim working code.

**Current result: not complete.** Sections 5–7 list the remaining preparation blocks.

## 3. Existing implementation ownership map

| R / PR | Primary responsibility | State |
|---|---|---|
| R01 / #27 | module birth/image/owner identity, readiness, restart and process-family stop | `READY_HANDOFF` |
| R02 / #28 | OpenCode owner, journal/IPC recovery, result paging and stop ownership | `READY_HANDOFF`; consumes R41 durable journal seam |
| R03 / #29 | Codex exact steer without history preflight; ACK/input/terminal separation | `READY_HANDOFF` |
| R04 / #30 | Muse pending inventory/stage update correctness | `CODE_CANDIDATE` with an unpublished example-version mismatch still named in PR |
| R05 / #31 | exact result provenance and expected Attempt binding | `READY_HANDOFF` |
| R06 / #32 | coordination work context, code-scope and cross-task relation contract | `READY_HANDOFF` |
| R07 / #33 | contracts/Concilium terminal lifecycle and advisory/authority separation | `PARTIAL_OWNER`; exact contract decision producer is R32 |
| R08 / #34 | mailbox sequence, watch delivery, subscription cutoff/backpressure | `READY_HANDOFF` |
| R09 / #35 | review assignment replacement, late results and bounded listing | `READY_HANDOFF` |
| R10 / #36 | exact automation impact lookup and disable independent of derived diagnostics | `CODE_CANDIDATE`; scoped Rust gate outstanding |
| R11 / #37 | host terminal event projection and event-shape convergence | `CODE_CANDIDATE` for the small producer fix; remaining DTO/legacy alias scope still open |
| R12 / #38 | scheduler source isolation, no-progress backoff and issuance fairness | `READY_HANDOFF` |
| R13 / #39 | exact resource/capacity evidence and workspace release | `READY_HANDOFF` |
| R14 / #40 | frontend/method/schema boundary and bounded schema reuse | `READY_HANDOFF` |
| R15 / #41 | Codex/Muse native usage producers and correct field-specific merge | `READY_HANDOFF`; Store/launch consumer is R40 |
| R16 / #42 | Claude official permissions, defer and exact reply pipeline | `READY_HANDOFF`; consumes R41 durable journal seam |
| R17 / #43 | OpenCode loop-step input, interactions and current background contract | `READY_HANDOFF` |
| R18 / #44 | Command ACP manager session backend with ELIOT-owned process/session lifecycle | `READY_HANDOFF` |
| R19 / #45 | Antigravity warm stream, configuration readback and cumulative usage | `READY_HANDOFF` |
| R20 / #46 | Store-owned compact TaskPrompt envelope; remove raw snapshot renderers | `READY_HANDOFF` |
| R21 / #47 | one correction package for all selected findings | `READY_HANDOFF` |
| R22 / #48 | executable Requirement→profile revision→CheckRun evidence | `READY_HANDOFF` |
| R23 / #49 | fail-closed Operation get/list/delta scope | `READY_HANDOFF` |
| R24 / #50 | live MCP allowed-method membership before target IPC | `READY_HANDOFF` |
| R25 / #51 | exact Task/Attempt/submission/check/family read scope | `READY_HANDOFF` |
| R26 / #52 | one ArtifactReadGrant for metadata/bytes/parts/assembly | `READY_HANDOFF` |
| R27 / #53 | Task create/claim/release coalescing identity and queued-check release closure | `READY_HANDOFF` |
| R28 / #54 | task.dispatch semantic reuse with one native effect and current receipt identity | `READY_HANDOFF` |
| R29 / #55 | caller-owned request ID for effect-sensitive MCP methods | `READY_HANDOFF` |
| R30 / #56 | one typed current-GM designation and monotonic epoch fence | `READY_HANDOFF` |
| R31 / #57 | closed RuntimeCommand registry; unknown method fails closed | `READY_HANDOFF` |
| R32 / #58 | canonical contract proposal digest and live ratify/reject producer | `READY_HANDOFF` |
| R33/R36/R37 / #59 | donor failures, verified registry and minimum-code playbook | `READY_HANDOFF`; this closeout index extends it |
| R34 / #60 | automation poison-fact isolation and per-domain transactions | `READY_HANDOFF` |
| R35 / #61 | finite CheckRun/ScriptRun/probe completion, partial capture and cleanup-pending | `READY_HANDOFF` |
| R38 / #62 | resumable Git hook install/revoke plus reachable immutable `hook.emit` | `READY_HANDOFF` |
| R39 / #63 | crash-repairable shared host/module state marker | `READY_HANDOFF` |
| R40 / #64 | typed provider condition and final route/root admission gate | `READY_HANDOFF` |
| R41 / #65 | OpenCode/Claude torn-tail journal salvage and durable file create/remove | `READY_HANDOFF` |

PR #26 remains the minimal compilation repair for the reviewed baseline. It is a prerequisite candidate, not an architectural owner for the runtime defects above.

## 4. Critical-list disposition

This table maps the short critical list from the supplied audit to current verified ownership. It does not repeat the full evidence.

| Critical class | Disposition / owner |
|---|---|
| HEAD compile drift | #26 `CODE_CANDIDATE` |
| proposal digest mismatch and missing ratify/reject | R32/#58 |
| code-scope reads the wrong context shape | R06/#32 |
| Concilium actor/fingerprint/terminal lifecycle | R06/#32 + R07/#33; decision closure R32/#58 |
| Codex Goal continuation selects/rejects the wrong controller artifact | `NEEDS_SOURCE_PASS` — see P0-A |
| supervisor birth identity reconstructed differently | R01/#27 |
| Claude candidate can be self-consistent but belong to another Attempt | R05/#31 |
| OpenCode journal/outbox recovery bricked by damaged tail | R02/#28 + R41/#65 |
| Operation/native diagnostics default-open or weakly scoped | R23/#49; Task/Artifact siblings R25/#51 and R26/#52 |
| launcher queue/dashboard/exceptions fleet-wide reads | `PARTIAL_OWNER` — object grant vocabulary exists in R23/R25, but launcher-specific projections still need a source card; see P1-D |
| on-behalf review visibility/cancel authority | R23/#49 plus R30/R24 current authority; exact remaining link validators belong to domain owners |
| GM epoch/read policies diverge | R30/#56 |
| MCP facade ignores live allowed methods | R24/#50 |
| schedule.run_now manufactures a request ID | R29/#55 |
| CLI/catalog method-shape drift | R14/#40 + R31/#57; exact method families migrate vertically |
| script.revise authority asymmetry | `NEEDS_SOURCE_PASS` — see P0-B |
| unknown native methods treated supported | R31/#57 |
| supervisor event claims authenticated hello without this boot's hello | R01/#27 + R11/#37; exact producer/consumer fixture must remain in R01 integration |
| Forge expected-old preflight is not atomic CAS | `POLICY_DECISION`, not hidden: current Forge documentation explicitly states the race and non-force policy. See §6.1. |
| DST fold kills cron | `REFUTED` for current implementation: UTC→zoned DateTime is unambiguous, current source has explicit fall-overlap fixture and the consolidated audit records no double-fire/fold failure. Do not create a fix from the earlier allegation. |
| host failure `secondary_codes` disappears | small producer correction in R11/#37; common DTO remains there |

## 5. Systemic-class disposition

| Systemic class | Current owner/state |
|---|---|
| one poison subject blocks a reconcile/domain/global pass | R34/#60; scheduler family sequencing R12/#38 |
| transient prerequisite is persisted terminal or cursor advances past it | `PARTIAL_OWNER`: R34 vocabulary + each concrete domain PR; common acceptance/publication/work/goal state table still needs closeout review after R34 lands |
| cancelled/rejected/settled semantics differ across automation consumers | `NEEDS_SOURCE_PASS` — see P0-C |
| legacy `module:` interception hides older runtime outcomes | R11/#37 owns event projection/alias convergence; exact production regression fixture required there |
| producer/consumer shapes drift by one field/digest dialect | R05/R20/R22/R30/R31/R32 and R14 method registry; no universal schema framework |
| evidence trusted without expected-object recomputation | R05, R13, R28, R31; specific consumers remain domain-owned |
| unbounded process/group/capture waits | R35/#61; long-lived module lifecycle R01/#27 |
| adapter journals/checkpoints and retained histories grow without policy | `PARTIAL_OWNER`: crash consistency R41; retention/compaction remains P0-D |
| single Store writer throughput target unmeasured | `NEEDS_SOURCE_PASS`: benchmark and transaction-cost attribution P1-E; do not prescribe sharding first |
| duplicated method/schema/adapter/executor implementations | R14/R20/R31/R35 and R37 deletion gate; remaining duplicate removal follows live caller migration |

## 6. Corrections and policy boundaries

### 6.1 Forge publication

Current Forge performs an exact remote-ref preflight, one ordinary non-force push, then exact readback. The normative document explicitly states that this is not atomic compare-and-swap: another writer can move the ref between preflight and push; ordinary Git fast-forward protection prevents rewind but does not bind the update to the exact old OID.

Therefore:

- the earlier statement “there is no force-with-lease, therefore it silently overwrites a branch” is too broad;
- adding `--force-with-lease` is not an automatic bug fix because owner policy currently rejects force and documents the remaining race;
- changing to an exact lease/CAS publication contract requires an explicit owner/product decision, migration of request/receipt semantics and real remote fixtures;
- the confirmed Forge capture/evidence bugs remain implementation work (P0-E), independent of that policy decision.

### 6.2 Artifact overwrite allegation

Result/page publication uses create-new temp + hard-link to deterministic destination and verifies an existing destination. A different replay does not replace the old artifact. Crash-before-DB-commit can leave an exact orphan that a retry verifies/registers. The overwrite allegation is `REFUTED`; read authorization remains R26.

### 6.3 Donor count

The verified donor registry has 31 entries. Discovery candidates are not counted until source revision, license, transferable mechanism, ELIOT invariant, negative boundary and adoption fixture are recorded. More searching alone is not an audit completion condition.

## 7. Remaining preparation blocks

### P0-A — Codex Goal continuation/controller artifact

Need a source pass that traces:

```text
GoalTerminalEvidence producer
→ descriptor/artifact selector
→ Store runtime ingest
→ automation_goal_progression verification
→ continuation command admitted to the currently selected Codex controller
```

Deliver one PR card deciding which Rust/native artifact owns continuation, removing the other active writer and preserving historical readback only. Do not patch another artifact-ID allowlist.

### P0-B — script mutation authority and start-gate lifecycle

Trace `script.register/revise/activate/run`, GM/Operator authority, worker `go.json/deny.json` ordering, bundle publication and terminal settlement. Separate:

- script definition authority;
- run admission;
- worker start authorization;
- finite execution (already R35).

Produce one connected handoff that removes the revise authority asymmetry and closes the deny-vs-go race without a second script state machine.

### P0-C — terminal Operation/disposition consistency

Enumerate the closed terminal/nonterminal sets used by acceptance, publication, GitHub projection, repair, Goal progression, WorkDispatch, cancellation and semantic-slot reuse. Build one data-only internal classification only where live consumers migrate together. Define for each state:

```text
reusable effect result
retry/no-effect
readback-only
pending prerequisite
terminal failure
```

Do not hide domain-specific evidence behind a universal retry enum. This source pass must allocate exact owners and deletion targets.

### P0-D — retained-state budgets and compaction

R41 solves torn-tail durability, not unbounded growth. Source-review:

- OpenCode/Claude/Command operation journals and outboxes;
- Codex retained operation/checkpoint size behavior;
- observer segments/retention failure;
- capacity ledger rewrite/history cost.

Produce separate bounded cards only where deletion authority is provable. Do not introduce one global LRU that can delete unresolved intent/evidence.

### P0-E — Forge/GitHub worker evidence

Confirmed source issue: bounded capture counts every input byte but currently hashes only the retained prefix, so `stderr_sha256` and `stderr_bytes` describe different bodies after truncation. Review all Forge/GitHub worker result fields, output-limit states, per-page error aggregation and exact remote readback. Keep the non-force CAS policy question separate.

### P1-D — launcher read projections

Reuse R23/R25 positive object grants to audit `swarm.dashboard`, queue, exceptions and inspection projections. Decide which are intentionally fleet-wide for local Operator/current GM and which must be project/Task scoped. Do not create a second launcher ACL layer.

### P1-E — Store writer performance harness

Before group commit, sharding or another database:

- measure per-method writer hold time, queue depth, transaction duration, WAL/fsync and reconciler share;
- replay bounded representative mutations without paid models;
- identify whether head-of-line work occurs inside SQLite transaction or surrounding async/file/native I/O;
- set a measured target and create an optimization card only for the observed bottleneck.

### P1-F — final test-gate repair index

Map the known 25 baseline failures, strict Clippy warnings and vacuous/under-asserting fixtures to owning implementation PRs. Do not modify product behavior to satisfy impossible fixtures. The final index must distinguish compiler gate, public-path behavioral fixtures, platform-native qualification and paid live qualification.

## 8. Serial implementation graph

```text
#26 compile baseline

R39 state marker
→ R01 module identity/lifecycle
→ R35 finite child/capture
→ R38 hook Git child

R13 resource identity
→ R15 provider producers
→ R40 route/root admission

R41 durable journal files
→ R02 OpenCode recovery
→ R16 Claude recovery/permissions

R30 GM authority
→ R23 Operation reads
→ R25 Task graph reads
→ R26 Artifact reads

R31 command registry
→ R24 live MCP authorization
→ R29 request-ID guard
→ R14 frontend/schema extraction

R06 context
→ R32 contract decision
→ R07 Concilium closure
→ R08/R09 coordination delivery/review lifecycle

R12 scheduler source/pacing
→ R34 poison isolation
→ automation domain consumers

R20 TaskPrompt + R22 executable requirements
→ R21 correction package
```

PRs that edit one shared owner rebase in this order. They do not create local duplicate types/functions to avoid waiting.

## 9. Next closeout sequence

1. prepare P0-A through P0-E as exact implementation cards;
2. prepare P1-D through P1-F;
3. update this matrix so no P0/P1 remains `NEEDS_SOURCE_PASS`;
4. reconcile every PR head/base/link and CI status;
5. issue one final implementation-wave document containing only cards, dependencies and owner/worktree allocation;
6. mark source audit/preparation `COMPLETE` without claiming production implementation or qualification.

Until step 6, the accurate status is: **audit active; implementation preparation incomplete**.
