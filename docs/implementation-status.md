# Implementation status

## Current state

**AUDIT_INCOMPLETE / TARGET_DOCUMENTS_RECONCILED / RUNTIME_REFACTOR_IN PROGRESS.**

Runtime source findings are reconciled through
`c92120ed70ca7e625440a8a19517c481e73b13c9`; active target documents through
`4c052141792b9e12818b9bed9b666b086242b9cc` on 2026-10-22. The latest audit pass
found two additional source/target gaps: unbounded vendor-branched Goal progression and
an unconditional dependency gate at Task claim. Their selected target behavior is now
specified in configuration/task policy and the findings map.

This is not an exhaustive current-head audit or live fleet qualification. The private
original Claude-script ZIP was not re-executed; details use the retained source review.
No claim that every defect has been found is supported.

- [Architecture](agent-operations/architecture.md) — product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Configuration](agent-operations/configuration.md) — one agent policy loop, route,
  dependency and continuation policy.
- [Delivery](agent-operations/delivery.md) — autonomous work cycle.
- [Task policy](task-policy.md) — specification/evidence/TaskPacket boundary.
- [Communication](agent-communication-program.md) — one mailbox/direct collaboration.
- [Modularity](agent-operations/modularity.md) — package/process/protocol boundaries.
- [Findings](agent-operations/refactoring.md),
  [observability](agent-operations/observability.md) and
  [donor map](agent-operations/donor-map.md) — source evidence and limits.

## Landed runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains |
| Active Antigravity route aligned on v6 | `cba98b7` | Not installed native-turn qualification |
| Terminal receipt decoupled from required observation | `eeea355`, recorded net −265 production Rust lines for that commit | Dispatch proof/crash-unsafe delivery remain |
| One dispatch producer retained across SUCCESS/ERROR/CANCELED | `c92120e`; run `39579583956`, job `119153571930`, connected regression + minimal Store Clippy passed | Scoped patched-runner evidence, not crash/installed/scale proof |

Current Antigravity:

```text
A0   LANDED                  active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/result delivery across adapter restart
A3   NOT IMPLEMENTED          real typed actions and one active route only
```

The crash-loss defect remains: unacknowledged `pending_receipts` can disappear. A2 must
persist complete Store deliveries and allow an authorized successor to reconcile them
to the original Operation after fencing the previous owner, without replaying native
input or changing origin boot.

Recent documentation commits change no runtime source. Their GitHub documentation
validation passed; no new runtime compilation/test claim is made.

## Historical behavior preservation matrix

| Required behavior / lesson | Target owner/mechanism | Current gap |
| --- | --- | --- |
| Native-manager start never receives second controller prompt | `Attempt.start_owner`, `START_OWNED_BY_MANAGER`, producer correlation | Preserve while simplifying dispatch/TaskPacket |
| Full agent-managed cycle with minimal human work | One policy loop: dispatch, review, return_changes, accept, optional publish | Separate workflow consumers remain |
| Harness Goal and Eliot messages coexist | adapter native Goal + independent `goal_message` Send | Current progression is vendor-branched/proof-heavy |
| Continuation is bounded by selected policy, not infinite/default age | occurrence/turn/elapsed/cost/no-progress budgets; observable progress; drain/checkpoint rotation | Current settings contain only `goal_id` and admit per terminal event |
| Forms/permissions do not hang or default to deny/first choice | native rules + exact Reply + one Governor decision | Capability/policy parity differs by adapter/version |
| Queue prioritizes corrections/unfinished work and cannot stall | ranked query + keyset scan + `dispatch_once` | WorkDispatch cursor/pending state owns selection |
| Dependency map cannot unnecessarily block preparatory work | dispatch policy `hold` or `allow_preparatory`; acceptance always exact | Current `task.claim` resolves all dependencies first |
| Reuse compatible sessions; rotate only after safe disposition | continuation policy + binding/native facts + drain/checkpoint | Current launch often opens new session; no consolidated policy |
| All review findings together; missing reviewer replaceable | current replacement classes + one request-changes/Send | ReviewDisposition/RepairDispatch remain |
| Acceptance proves every requirement | frozen evidence plan + candidate-specific checks/reviews/dependencies | common manual/automatic gate still requires source work |
| Compact context, no proof JSON | one TaskPacket renderer; Store owns identity/evidence | `TaskPromptEnvelopeV1`/hash echo remains |
| Provider quota/connection/activity are separate | source facts with freshness/coverage; route derives decision | binding/artifact ledgers remain |
| Governor sees unmatched sessions/processes/worktrees | provider/process/Git/remote collectors + one joined view | inventory begins from bindings/parallel views |
| Unknown write never blindly retries | one effect owner + exact readback + resource-scoped hold | duplicated journals/proof workflows remain |
| Result survives adapter failure without replay | exact StoreDelivery + successor reconciliation | Antigravity A2/equivalent adapters not implemented |
| One manager owns Git; leaf writers disjoint; review immutable | path scope → workspace lease; manager Git; frozen candidate | `code.scope.*` and mutable review paths remain |
| Scripts retain required bounded controls | common helpers + only needed typed actions under current rights | current enum only message/notification/task-create |
| Peer collaboration without council platform | normalized mailbox + tiny optional thread + scoped grants | coordination platform/global timeline mailbox remain |
| GitHub integration can be automated without owning Task lifecycle | collector → policy import → one API effect owner; Forge remote Git | poll/import/work-pool/projection/two writers remain |
| Reconnect/handover preserves work/history | stable client; handover designation/epoch; current projections | mailbox normalization not implemented |
| Restart never kills unrelated PID/duplicates owner | PID+birth/image/group/Job + conservative recovery | process/provenance layers and races remain |

Deleting old machinery is valid only when the corresponding positive path is connected
and old callers/config/fixtures/storage disappear in the same slice.

## Active-document corrections completed

- Replaced target `review_disposition`/`repair_dispatch` workflow actions with current
  `review`/`return_changes` actions.
- Reclassified full descriptor/schema echo, strict receipt identity and hashed
  TaskPrompt as transitional, not target protocol.
- Selected Muse Node bridge `.13`; custom Rust is a finite retirement, not a second
  route or third SDK migration.
- Replaced “manual qualification gate” with an automatable public product path in the
  required owner-machine environment.
- Added the positive-path rule for agents: every deletion must identify trigger,
  ordinary command, owner, readback/result and automatic next action/Governor decision.
- Preserved participant/reviewer rights and automatic wake while deleting the
  coordination platform.
- Removed stale MCP documentation claiming an already-fixed operator-only Task planner
  defect.
- Preserved the frozen per-requirement evidence plan while simplifying TaskPacket.
- Added explicit continuation/no-progress/rotation policy and dependency dispatch mode;
  neither uses a new planner/workflow database.

## Audit completion criteria

A final-complete source audit requires evidence against one named current head that:

1. every retained public action is traced caller → parse → role/object authorization →
   Store transition → effect/readback → final consumer;
2. cancellation, restart, late result, transfer and partial-write branches are covered;
3. every retained historical requirement maps to a concrete owner/implementation and
   every rejected behavior has a reason;
4. active target documents agree and historical docs cannot reintroduce target work;
5. every deletion has an automatic positive replacement and finite data/unsettled-work
   transition;
6. privilege and migration/retained-format/corruption matrices run on the reduced
   surface;
7. installed providers, Windows custody, IPC/Store fairness, mixed routes, gateway,
   autonomous end-to-end cycle and scale are qualified in their environment.

Items 1–5 remain incomplete globally despite deep targeted reviews. Items 6–7 are
largely unexecuted. The audit is not ready for a final-complete claim.

## Next work

Continue the existing implementation order; do not create another audit program.
Finish Antigravity A1b–A3, then replace queue/provider/capacity/automation copies with
current-state actions, connect TaskPacket + common review/correction/acceptance, and
consolidate one selected executor/local Governor view per runtime. Close the matching
caller-to-consumer audit path in each slice.

Use minimal affected format/Clippy/build during construction and one focused connected
case when the vertical path closes. Full installed/native/load qualification follows
the connected product, not another documentation gate.
