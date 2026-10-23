# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS_WORKING_DRAFT / RUNTIME_REFACTOR_IN_PROGRESS.**

Repository verified on **2026-10-23** at
`3c4c8599f1614460fe69d54d0b59715a26ada797`. The latest runtime change is
`c92120ed70ca7e625440a8a19517c481e73b13c9` (2026-10-21). Comparing those revisions
shows **16 documentation-only commits**. Documentation progress is not runtime progress.

The interrupted research was not lost: `c2b998b` published AR-041–AR-044;
`3c4c859` published AR-045–AR-047. The last connected source trace concerns shutdown,
control attachment demand and resource retirement. The present checkpoint synchronizes
status and indexes that evidence; it does not claim another completed source trace.

- [Resume point and 47 finding records](agent-operations/audit-register.md#resume-here).
- [Evidence/source index](agent-operations/audit-index.md): direct links to prior traces,
  current source entry points, target owners and unclosed coverage.
- [Architecture](agent-operations/architecture.md): product purpose and boundaries.
- [Implementation](agent-operations/implementation.md): the only work order.

The 47 records include defects, preserved mechanisms and qualified/partial boundaries;
they are not 47 repaired bugs or a measured audit percentage. No exhaustive current-head
audit, complete historical-requirement mapping or live fleet qualification is claimed.
The private Claude-script ZIP has not been freshly re-executed; its retained review
remains a historical source.

## Landed runtime changes

| Change | Retained evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity route aligned on v6 | `cba98b7` | Not installed native-turn qualification. |
| Terminal receipt decoupled from required observation | `eeea355`, recorded net −265 production Rust lines for that commit | Dispatch proof and crash-unsafe delivery remain. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930`, connected regression and minimal Store Clippy | Scoped patched-runner evidence; not rerun by this checkpoint and not crash/installed/scale proof. |

```text
A0   LANDED                  active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK             largely unreduced current runtime; see implementation
```

A2 must connect adapter capture, ordinary Store settlement, exact page delivery,
historical completion authority and complete-source retention. A file spool alone
cannot repair queue/boot/release barriers. No native prompt is replayed and old output
keeps its original Operation/Attempt/boot. Unacknowledged Antigravity receipts and
uncaptured bodies remain at risk in current code.

## What the recent audit established

| Boundary | Recorded result | Not yet established |
| --- | --- | --- |
| Result/continuation/restart, AR-001–035 | Exact selected caller/Store/adapter traces, useful assembly/grants, cross-route delivery and source-cost findings | Every command/installed contract, first historical capture across revocation, live restart/power-loss. |
| Queue/root/provider, AR-036–044 | History-dependent admission, pending saturation, native-default mismatch, stale-state precedence; preserve root dedup and stable scanning | Complete direct-call concurrency, route-edit continuity and all cancellation dispositions. |
| Shutdown/resource retirement, AR-045–047 | Non-ready shutdown barrier, intentional-detach/demand mismatch, independent binding/service/workspace holds | Post-binding cancellation through the finalizer; actual detach/Goal/child behavior; safe eventual retirement. |
| Other implementation areas | Inherited structural and selected source reviews | Remaining chains listed in the audit index; no whole-project completion claim. |

No AR entry is closed merely because its target documentation was corrected. Preserve
refuted/narrowed conclusions: committed old-boot outcomes can replay-ACK; root records
already deduplicate; removing source identity checks cannot recover missing bytes.

## Historical behavior preservation matrix

This is the retained working requirement map, not a claim that every old requirement
or script has been traced end to end.

| Required behavior / lesson | Target owner/mechanism | Current gap |
| --- | --- | --- |
| Native-manager start never receives second controller prompt | `Attempt.start_owner`, `START_OWNED_BY_MANAGER`, producer correlation | Preserve while simplifying dispatch/TaskPacket. |
| Full agent-managed cycle with minimal human work | One policy loop using ordinary dispatch, capture, review, return_changes, accept and selected publication | Separate workflow consumers and capture/readback gaps remain. |
| Harness Goal and Eliot messages coexist | Adapter native Goal + independent `goal_message` Send | Progression remains vendor-branched/proof-heavy. |
| Bounded continuation, not default infinite/age-based work | Selected occurrence/turn/elapsed/cost/no-progress policy; observable progress; drain/checkpoint rotation | Current Goal settings/progression do not implement the complete target. |
| Forms/permissions do not hang or default to first answer/deny | Native rules + exact Reply + addressed Governor decision | Capability/policy parity differs by adapter/version. |
| Corrections/unfinished work precede fresh work without starvation | Ranked current query + fair keyset scan + `dispatch_once` | WorkDispatch copied state and 128-subject intake barrier. |
| Dependencies allow authorized preparatory work | Dispatch `hold` or `allow_preparatory`; acceptance enforces exact requirements | `task.claim` resolves all dependencies first. |
| Reuse compatible sessions; rotate only after safe disposition | Continuation policy + binding/native facts + drain/checkpoint | Launch/reuse and retirement remain disconnected in places. |
| All findings together; missing reviewer replaceable | Existing replacement classes + one request-changes/Send | ReviewDisposition/RepairDispatch remain; delivery classification needs repair. |
| Acceptance proves every requirement | Frozen evidence plan + candidate-specific checks/reviews/dependencies | Common direct/automatic gate still requires source work. |
| Compact context, no controller proof JSON | One TaskPacket renderer; Store identity/evidence | TaskPrompt envelope/hash echo remains. |
| Provider quota/connection/activity remain separate | Real source facts with freshness; route derives decision | Binding/artifact ledgers and stale-copy precedence remain. |
| Governor sees unmatched sessions/processes/worktrees | Provider-local/OS/Git/remote collectors + one joined view | Binding-rooted inventory and parallel views remain. |
| Unknown native/external write never blindly repeats | One effect owner + exact readback + resource-scoped hold | Duplicated journals and automatic readback gaps remain. |
| Result survives failure and continuation result remains attributable | Exact StoreDelivery + source lifetime + successor completion | Antigravity A2 and equivalent cross-route gaps are open. |
| One manager/worktree; disjoint leaf edits; immutable review | Task path scope, workspace lease, manager-owned Git, frozen candidate | Separate code-scope product and resource-release integration remain. |
| Scripts retain required controls | Existing helpers or only needed typed actions under current rights | Current enum covers message/notification/task-create only. |
| Peer collaboration without council platform | One mailbox, optional tiny thread, scoped grants | Coordination platform/global-timeline mailbox remains. |
| GitHub automation without owning Task lifecycle | Collector → policy import → API effect owner; Forge owns remote Git | Poll/import/work-pool/projection and duplicate writers remain. |
| Handover preserves work/history | Stable client; designation/epoch change; current projections | Mailbox normalization and historical cleanup gaps remain. |
| Restart never kills unrelated PID or duplicates owner | PID+birth/image/group/Job and conservative recovery | Full process/custody race matrix is not qualified. |

Deleting old machinery requires a connected positive path in the same slice, not
routine human work, a new workflow engine or a permanent compatibility facade.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for retained actions:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover and partial writes. It also
needs historical requirement mapping, agreement between active contracts and concrete
replacement/disposition for each deletion. These remain incomplete globally.

Executed qualification is a separate record: privilege/migration/corruption cases,
installed provider contracts, Windows custody, fairness/load and autonomous end-to-end
work. Most of that remains unexecuted. Source inspection cannot substitute for it, and
missing a live machine does not excuse leaving source paths untraced.

## Next work

**Audit:** resume the post-binding launch cancellation/finalizer chain in
[audit-register](agent-operations/audit-register.md#resume-here), then route-edit budget
continuity and direct-admission transactions. [Audit index](agent-operations/audit-index.md)
locates prior evidence without rereading the whole chat or walking checkpoint chains.

**Runtime:** implementation still starts with the unfinished Antigravity A1b–A3 path;
B–V remain the single subsequent order. Use existing owners, remove replaced machinery
and keep ordinary automation connected. During construction run minimal affected
format/Clippy/build; connected and installed/load checks follow the usable path.

This checkpoint changes documentation only. It makes no new Clippy/test/benchmark,
independent-verifier or installed-runtime claim.
