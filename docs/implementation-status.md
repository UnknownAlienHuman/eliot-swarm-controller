# Implementation status

## Current state

**AUDIT_INCOMPLETE / TARGET_DOCUMENTS_RECONCILED / RUNTIME_REFACTOR_IN_PROGRESS.**

This pass rechecked active target documents, selected current callers and historical
requirements through `main@81194026d6813836207cbc9efc53c58e4324a963` on
2026-10-21. It found active documentation that still instructed agents to rebuild
separate repair workflows, full descriptor/schema negotiation and ambiguous manual
qualification. Those contradictions are corrected in the pending documentation
commit.

This is not an exhaustive current-head source audit or live fleet qualification. The
private original Claude-script ZIP was not re-executed; script details use the retained
source review. No claim that every defect has been found is supported.

- [Architecture](agent-operations/architecture.md) — short product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Configuration](agent-operations/configuration.md) — one agent policy/automation
  loop and its positive actions.
- [Delivery](agent-operations/delivery.md) — autonomous work cycle.
- [Communication](agent-communication-program.md) — one mailbox and direct agent
  collaboration.
- [Modularity](agent-operations/modularity.md) — compact package/process/protocol
  boundaries.
- [Findings](agent-operations/refactoring.md),
  [observability](agent-operations/observability.md) and
  [donor map](agent-operations/donor-map.md) — source evidence and limits.

## Landed runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains |
| Active Antigravity route aligned on v6 | `cba98b7` | Not an installed native-turn qualification |
| Terminal receipt decoupled from required observation | `eeea355`, recorded net −265 production Rust lines for that commit | Dispatch proof and crash-unsafe delivery remain |
| One dispatch producer retained across SUCCESS/ERROR/CANCELED | `c92120e`; run `39579583956`, job `119153571930`, passed connected terminal regression and minimal Store Clippy | Scoped patched-runner evidence, not crash/installed/scale qualification |

Current Antigravity state:

```text
A0   LANDED                  active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/result delivery across adapter restart
A3   NOT IMPLEMENTED          real typed actions and one active route only
```

The original crash-loss defect remains: an adapter can lose unacknowledged
`pending_receipts`. A2 must persist complete Store deliveries and allow an authorized
successor to reconcile them to the original Operation after fencing the previous
owner, without replaying native input or relabelling the origin boot.

This documentation pass changes no runtime source and claims no new build/test result.

## Historical behavior preservation matrix

| Required behavior / failure lesson | Target owner and mechanism | Current gap |
| --- | --- | --- |
| Native manager may spawn/delegate before producer binding; controller must not send a second initial prompt | `Attempt.start_owner`, `START_OWNED_BY_MANAGER`, producer correlation | Preserve while simplifying dispatch receipts and TaskPacket |
| Full agent-managed work cycle with minimal human involvement | One policy loop using ordinary `dispatch`, `review`, `return_changes`, `accept`, optional `publish` | Existing WorkDispatch/review-disposition/repair/publication engines must be folded, not merely disabled |
| Harness Goal and Eliot reminders can coexist | Adapter native `Goal`; independent scheduled/event `goal_message` → `agent.send` | Current Goal progression remains vendor-branched and proof-heavy |
| Forms/permissions must not hang or be auto-denied | Native permission rules; exact adapter `Reply`; unresolved judgment to Governor | Capability/policy parity still differs by adapter and installed version |
| Queue prioritizes corrections/unfinished work and cannot stall behind 128 blocked items | Current ranked query + bounded keyset scan + `dispatch_once` | WorkDispatch cursor/pending state still owns selection |
| Reuse suitable sessions; rotate only by policy after disposition | Route continuation policy + binding/native-state checks | Current launch path often opens new sessions; no consolidated policy path |
| All review findings returned together; missing reviewer replaceable | Current `reviews.rs` replacement classes; one `task.request_changes` + correction `Send` | Separate ReviewDisposition/RepairDispatch workflows remain |
| Acceptance proves each requirement, not prose “done” or only differential cleanliness | Frozen per-requirement evidence plan + candidate-specific review/check gate | Active compact docs now preserve this; manual/automatic gate convergence still requires source work |
| Compact task context, no raw controller proof JSON | One TaskPacket renderer for initial/correction/resume; Store retains identity/evidence plan | `TaskPromptEnvelopeV1`, dispatch contexts and hash echo chains remain across adapters |
| Provider state, quota, connection and activity are separate | Adapter-normalized source facts with freshness/coverage; route admission derives decision | Current binding/artifact-oriented usage/provider ledgers remain |
| Governor sees sessions/processes/worktrees locally, including unmatched subjects | Provider/process/Git/remote collectors + one joined Store projection | Current inventory begins from bindings and parallel views |
| Unknown write never blindly retries; unrelated work continues | One native/external effect owner + exact readback + resource-scoped hold | Several domains still use duplicated journals/proof workflows |
| Result survives IPC and adapter failure without replay | Route-local exact StoreDelivery + successor reconciliation to original Operation | Antigravity A2 and equivalent adapter reductions not implemented |
| One manager owns Git/worktree; leaf edits are disjoint; review input immutable | Task path scope → one workspace lease; manager serializes Git; frozen candidate | `code.scope.*` duplicates authority and review/worktree paths need consolidation |
| Scripts can perform required bounded controls without arbitrary method passthrough | Existing helpers plus only needed typed Send/Reply/Configure/Goal actions; child Operations under current rights | Current script enum exposes only message/notification/task-create |
| Direct peer work without councils/contracts | Normalized mailbox + optional tiny Task thread + scoped grants | Current coordination platform and global timeline mailbox scan remain |
| GitHub intake/import/write can run automatically without owning Task lifecycle | Source collector → explicit policy import → one API effect owner; Forge only remote Git | Poll/import/work-pool/projection and two write owners remain |
| Same Manager reconnect and GM handover preserve work/history | Stable client identity; handover changes designation/epoch only; current projections for successor | Mailbox normalization and reduced continuity path not implemented |
| Process restart never kills an unrelated PID or duplicates a native owner | PID + OS birth/image/group/Job; conservative adoption/recovery | Current process/provenance layers and owner races need connected reduction |

Deleting historical machinery is valid only when the corresponding positive path in
this table is connected and the removed callers/config/fixtures/storage disappear in
the same slice.

## Documentation defects corrected in this pass

| Active-document contradiction | Correction |
| --- | --- |
| `configuration.md` still made `review_disposition` and `repair_dispatch` target actions | Replaced with `review` and `return_changes` current-state actions; useful automation remains |
| `modularity.md` still prescribed full descriptor/schema echo, strict receipt identity and hashed TaskPrompt as target | Reclassified them as transitional formats; target is protocol major + semantic capability subset + compact TaskPacket |
| `module-installation.md` left Muse undecided between Node and custom Rust | Fixed selected route: keep Node bridge `.13`, finitely retire custom Rust; no third executor |
| Full product qualification described as a manual/release gate | Defined an automatable public qualification path; owner-machine names environment, not manual execution |
| Worker guidance emphasized prohibitions but not replacement behavior | Added a positive-path invariant: trigger → ordinary command → owner → readback/result → automatic next action or one Governor decision |
| Coordination simplification could remove participant rights or wake behavior | Communication target preserves authentication/scoped grants and defines automatic attention/resume behavior |

## Audit completion criteria

A source audit may be declared complete only when all of the following are evidenced
against one named current head:

1. Every retained public action has a traced caller → typed parse → exact role/object
   authorization → Store transition → native/external effect → readback/result → final
   consumer path.
2. Failure, cancellation, restart, late-result, owner-transfer and uncertain-outcome
   branches are included for each retained effect domain.
3. Every historical product requirement selected for retention maps to a concrete
   target owner and connected implementation; every rejected historical behavior has
   an explicit reason.
4. Active target documents agree. Historical documents are labelled evidence/history
   and cannot reintroduce a removed target mechanism.
5. Every planned deletion names the automatic positive replacement and finite data/
   unsettled-work transition. No routine behavior is delegated to a human.
6. Public privilege and migration/retained-format matrices are executed on the reduced
   surface.
7. Installed provider/native restart, Windows custody, IPC/Store fairness,
   mixed-provider behavior, autonomous end-to-end cycle and scale are separately
   qualified in their required environment.

Items 1–5 remain incomplete globally, although many major domains have deep targeted
reviews. Items 6–7 are largely unexecuted. Therefore the audit is not ready for a
final-complete claim.

## Next work

Do not create another audit program. Continue the existing implementation order while
closing the matching caller-to-consumer audit path in the same slice:

1. Finish Antigravity A1b–A3 and preserve the repaired producer lifecycle.
2. Replace queue/provider/capacity and duplicate automation state with current-state
   actions.
3. Connect compact TaskPacket and common review/correction/acceptance path.
4. Consolidate one selected executor per harness and the local Governor projection.
5. Remove coordination/GitHub/tooling products only with their positive automatic
   replacements connected.

Use minimal affected format/Clippy/build during construction and a focused connected
case when each vertical path closes. Full installed/native/load qualification follows
the connected product, not another documentation gate.
