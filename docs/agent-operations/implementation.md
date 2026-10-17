# Implementation Plan — Rust Operations on Behalf of the Manager

Revision 22 · 2026-10-17 · Canonical Version 3 scope and acceptance index.

O1–O11 name product requirements; [M1–M6](modularity.md#6-implementation-sequence-and-completion-criteria)
name package/process changes. The [refactoring map](refactoring.md) owns the current
source queue and deletions. [Implementation status](../implementation-status.md)
records what exists and what was actually checked. A completion criterion below
is a target, not a report that a block passed.

## 1. Delivery discipline

Follow [AGENTS.md](../../AGENTS.md) for one manager/worktree, integration and scoped
verification. Complete the producer, admission/handler and result consumer together.
Manual and enabled automatic callers share the same Store/action owners.
Implementation slices remove replaced mechanisms; no extra workflow DSL, activation
ledger or framework-only milestone is needed.

## 2. Dependencies

Prioritize the productive local path: workspace, launch, dispatch, attributable
result, submission, review, repair and acceptance. Required V3 GitHub, hook, script,
cron/rule and Goal scopes remain below; completing all of them is not a prerequisite
for reducing the current code or fixing local delivery.

O1/O2 provide shared admission, storage and intake. O3/O4/O5 can use those boundaries
independently with non-overlapping file ownership. O7 local review does not depend
on GitHub or script interpreters. O8/O9 reuse common entries, cursors and scheduler.
O10 is integrated with each exposed slice; O11 follows completed source.

## 3. O1 — Ownership, configuration and shared actions

Owners: [Configuration](configuration.md), [on-behalf execution](architecture.md#2-same-action-handler-for-both-callers)
and [relational migration](refactoring.md#relational-conversion-detail).

Completion criterion: a manager can configure and enable only the named helper in
one call, retain manual access, read/explain/cancel its on-behalf Operations, and
stop future starts without losing observed results. Preserve revision CAS,
activation cuts, shared semantic slots, retained inputs and disable/start ordering.
New automation must not inherit the internal Scheduler's legacy cross-owner exception.

## 4. O2 — Durable intake and shared observation

Owners: [durable dispatch](architecture.md#23-durable-dispatch-not-notification-delivery),
[monitoring](architecture.md#3-monitoring-without-model-interrogation) and
[WAL reader migration](refactoring.md#wal-migration-detail).

Completion criterion: monitoring works with all automation off; committed work
survives lost/coalesced notifications and reconnects. Cursor advancement commits
with its Operation/reservation or pending subject. Indexed capacity changes wake
waiting work without failed-job storms; damaged sources and slow viewers do not
block unrelated owners. Keep authorization, pagination and journal cuts in the
same bounded read snapshot; watches are hints, not durable delivery.

## 5. O3 — Runtime adapters, streams, turn-correlated results and direct control

Owners: [launch](architecture.md#13-direct-launch-pipeline-and-non-blocking-mcp-readiness),
[result correlation](architecture.md#41-deterministic-turn-scoped-result-correlation-agentresult),
[SDK boundary](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries)
and [module lifecycle](modularity.md#5-demand-driven-lifecycle-unified-supervision-and-recovery).

Completion criterion: supported runtimes accept manual/automatic dispatch and
return the complete attributable assistant result without a pre-prompt MCP gate.
Keep explicit native input/turn identity where supplied; use the specified
serialized-turn association where needed. Exact steer/reply/cancellation and
bounded native streams keep their actual capability limits. Requested, effective
and observed model/usage remain distinct. A donor replaces native mechanics while
Eliot retains one durable effect/recovery authority and historical identity bytes.

## 6. O4 — Optional GitHub/Git and distribution

Owners: [GitHub APIs](module-api.md#o4-one-source-mapped-github-issue-label),
[queue ownership](delivery.md#3-queue-and-ownership), [remote effects](delivery.md#9-github-boundary)
and [donor retry policy](donor-map.md#retry-ownership).

Completion criterion: one `octocrab` backend replaces GitHub API transport while
preserving account/host selection, permissions, bounded reads, durable intake and
uncertain-write readback. Import/read never dispatches; local work remains usable
without GitHub. Source revision, projection echo, accepted push, PR update and
actual merge are separate identities/effects. Native Git stays outside Store
transactions with existing ownership and per-target serialization.

## 7. O5 — Hooks

Owners: [hook execution](architecture.md#5-hooks) and [verified post-commit facts](module-api.md#o5-verified-post-commit-facts).

Completion criterion: authenticated bounded observation reaches the shared event
path; only a selected enabled helper acts on it. Preserve installed hook settings,
phase/veto semantics and source readback. Async after-hooks cannot veto past work;
models, installation and long-running actions stay off callback paths. A missing
optional hook integration does not disable unrelated manual work.

## 8. O6 — Optional script bundles

Owners: [script contract](architecture.md#9-external-scripts), [generic event ScriptRun](configuration.md#generic-system-event-scriptrun)
and [invocation API](module-api.md#o6-one-granted-message-to-the-exact-attempt-owner).

Completion criterion: direct runs and selected event triggers share one runner and
committed intake. Capture immutable bundle/environment/schema inputs, bounded output,
real descendant ownership and scoped invocation rights. Taskless events use their
versioned context without invented Attempt rights; normalized/legacy occurrences
share deduplication. Disable blocks new independent effects, not fictional recall
of started OS work. Native delivery does not depend on optional interpreters.

## 9. O7 — Local reviewed delivery and selected automation

Owner: [Delivery](delivery.md), especially [review](delivery.md#6-review-assignment-verdict-and-audited-state),
[return/repair](delivery.md#7-return-and-repair) and [acceptance/publication](delivery.md#8-acceptance-upload-and-publication).

Completion criterion: manual submission A, review findings, applied return,
correction B and fresh review complete through existing candidate-bound handlers.
Enabled handoffs use the same slots. Applied submission creates eligibility;
assigned auditor authorship, manager disposition and owner repair stay distinct.
Preserve legitimate corrected descendants, unresolved findings, late historical
results and the explicit correction phase. Local review has no first-push dependency.

## 10. O8 — Cron and typed event rules

Owners: [typed rules](architecture.md#6-typed-rules-and-legitimate-progression),
[cron](architecture.md#7-cron-and-shared-reminders) and [event rules API](module-api.md#o8-event-rules-and-hook-assisted-joins).

Completion criterion: one `croner` scheduler over indexed Store rows supports
manager enable/disable, manual run-now and restart without duplicated occurrences.
Retain calendar/DST/overlap/misfire semantics, old interval slots and latest-only
ordinary catch-up. Cursor and action/pending disposition commit together. Generic
event selection does not create a second scheduler or bypass typed action validation.

## 11. O9 — Goal and reminders

Owners: [server Goal](architecture.md#8-server-goal), [shared reminders](architecture.md#7-cron-and-shared-reminders)
and [Goal continuation](module-api.md#o9-one-goal-continuation-for-one-completed-terminal-eventref).

Completion criterion: tracking existing Tasks, one-shot notices and selected
progression coexist with one actual continuation owner. Achievement uses declared
evidence/evaluation; unknown native continuation requires readback before competing
input. Reuse shared subject/timer indexes. No new Task graph or hidden model loop.

## 12. O10 — Shared MCP/CLI and cross-PR consistency

Owners: [MCP contracts](architecture.md#10-mcp-and-cross-program-contracts),
[canonical surfaces](../mcp-canonical-surfaces-and-topologies.md),
[catalog/loading](../mcp-tool-catalog-and-loading.md) and [code reduction](refactoring.md#code-reduction-candidates).

Completion criterion: each implemented method has its actual caller, policy,
validation, handler and result projection. `swarm-cli` owns application CLI mapping;
remove its duplicate in `swarm-kernel-host`. Keep the small eager cores and deferred
schemas through `rmcp`; catalog presentation never replaces Store authorization.
Assigned review reads/submission remain exact-assignment scoped after release
while authenticated and unrevoked. Ordinary Participant submission still requires
its authorized current revision/Attempt or retained producer basis. Neither acquires
manager/GM rights. No generic execute-method tool or new public aliases.

## 13. O11 — Qualification

These are final product scenarios, not prerequisites for unrelated source slices.
Retain exact source/platform/artifact and distinguish fixtures from native evidence.

| Cases | Required outcome |
| --- | --- |
| Default, one-call enable, manual coexistence | New/untrusted-imported entries are off; only selected helpers run. Full manual delivery remains available without a new mode/grant ceremony. |
| Launch and result | Codex, OpenCode, Antigravity, Command and Claude complete `agent.open -> task.dispatch -> agent.result` with full attributable output and asynchronous MCP discovery. |
| Current rights and on-behalf visibility | No forged Principal/Scheduler shortcut; authorized manager reads, explains and cancels through concurrent WAL snapshots without cross-manager disclosure. |
| Commit/signalling crash and capacity wait | Lost notification/reconnect loses no committed fact or duplicates an action; waiting subjects resume on prerequisite/capacity changes. |
| Activation cut and config edit | Enablement races safely with new events; omitted inclusion drains no old backlog. New admissions use new preferences; retained jobs keep inputs. |
| Disable/start and manual/automatic race | Disable-first prevents start; start-first remains in-flight/readback. Equivalent work shares one slot, incompatible choices conflict, uncertain effects are not replayed. |
| Review/repair, loops and source freeze | A remains immutable during review, applied return permits correction B, and B can receive fresh audit. Echoes/new IDs on unchanged defects do not consume new model work. |
| Review replacement and late/revoked result | Reassignment follows observed prior disposition; late evidence remains historical and cannot author the replacement. Revocation bars new effects; unauthenticated verdicts are rejected. |
| Native/capability truth | Family coverage and process ownership remain scoped; listed schemas do not imply model consumption. No extra model launch solely for a checkbox. |
| GitHub transport/projections | Projection echoes do not revise Tasks; `octocrab`/transport middleware does not repeat ambiguous writes or publication. |
| Cron/restart | Old slots, DST, calendar edits and toggles cannot duplicate consumed work; ordinary recovery does not become unbounded replay. |
| Scale and isolation | Slow streams and busy owners do not starve completion/control; no per-participant polling or full-history scans. |

Run synthetic registered populations separately from paid active models. Measure
bounded Store latency/read concurrency, gaps/fairness, RSS/handles, context volume,
repeated work and manager attention. Stage small native teams before larger fleets;
registration count is not active-turn capacity. CI performs no paid model runs,
live repository writes, service installation or owner-machine cleanup.

## 14. Migration and completion

Use [existing Store migration owners](refactoring.md#relational-conversion-detail)
for domain tables/indexes. Operations remain execution authority; preserve frozen
policies, legacy receipts and uncertain effects. The never-deployed PR #23 global
mode/control API needs no aliases. Adopt deliberate new policy through its common
handler, not by weakening current rights from a future tool description.

Setup records trusted local runtime/source/credential choices and enables no sample
work. Ordinary crash recovery preserves owned saved choices; unattributable imports
are not enabling consent. Completion is a usable integrated delivery cycle with
selected helpers, one authority per responsibility and no duplicated execution path.
