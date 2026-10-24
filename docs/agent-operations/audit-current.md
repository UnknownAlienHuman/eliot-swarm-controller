# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). AR-079–088 and their full Forge/GitHub-effects
trace are frozen at
[`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md).
[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes; and
[status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / GitHub collection, Task import and remote observation**.
- Source inspected at **`b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b`**.
  Runtime source is still **`c92120ed70ca7e625440a8a19517c481e73b13c9`**
  from 2026-10-21; the **23 intervening commits are documentation-only**.
- Connected chain:
  `github::{client,observer,work_pool,pulls,projection}` →
  `store/github` setup/poll/lease/page/fact commit →
  poll-time `task.create/task.revise` and `github_work_pool_members` →
  WorkDispatch source consumption and GitHub effect authorization →
  remote-ref observation boundary.
- Overall audit: **incomplete, 99 entries**. AR-089–099 are recorded below; prior
  dispositions remain in immutable checkpoints.
- Preserved in this pass: immutable repository-ID checks; exact request identity; safe
  GET retry; explicit partial/failure coverage; one-window duplicate instability
  rejection; immutable Issue origin keys; local Task divergence conflict instead of
  overwrite.
- Open in this pass: source setup/poll is Operator-only and coupled to Forge config;
  mutable numeric pagination is not a stable cut; common sources cannot reach complete;
  collection creates/revises Tasks in the same transaction; work-pool selection is not
  an admission gate; Issue state/comments are not imported correctly; unused fact/PR/
  projection paths remain; independent remote-ref collection is absent.
- **Next exact chain:** `store/mailbox.rs` →
  `store/object_reads.rs::{timeline,message.read}` → `store/gm.rs` handover/resync →
  `coordination/{thread,watch,contract,integration,concilium,code_scope}` callers →
  MCP catalog/profile/subscription entries. Preserve direct addressed collaboration and
  historical recipients while proving which coordination products can be deleted.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- Still unqualified: live GitHub pagination under concurrent updates, API rate limits,
  account behavior, source scale, remote-ref collection, migrations and autonomous
  end-to-end import/dispatch.
- Documentation only. No runtime code, Clippy, test, benchmark or installed-system
  qualification changed in this checkpoint.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named chain. A documentation commit never closes a runtime
finding.

<a id="findings-ar-089099"></a>
## Findings AR-089–099

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-089"></a>AR-089 | Source setup and authority | **Open coupling.** Setup and poll require local Operator; setup also requires an existing Forge project mapping. Read-only collection is therefore coupled to publication configuration and cannot be enabled by scoped Manager policy. | Separate source identity/policy from Forge and permit bounded configured collection without routine operator calls. |
| <a id="ar-090"></a>AR-090 | Poll workflow and recovery | **Preserve plus open.** The exact GET request/lease moves `queued → sending`; `outcome_unknown` can safely repeat the read. No host collector resumes it, and queued cancellation is explicitly unsupported. | Replace per-pass Operation/lease workflow with collector epoch/cursor/current-state recovery while keeping exact safe-read semantics. |
| <a id="ar-091"></a>AR-091 | Mutable pagination cut | **Open.** Issues are read by numeric page sorted `updated desc`; `next_page` persists across separate polls. Items can move into already-scanned pages, so one cycle can delay/miss them and cannot represent an atomic complete cut. | Stable server cursor/high-water or overlap plus periodic full reconciliation with documented ordering/deletion semantics. |
| <a id="ar-092"></a>AR-092 | Coverage convergence | **Open.** One poll reads at most 8×50 raw rows. `complete_from_page_one` cannot become true for larger sources, and any stored Issue with comments keeps the source partial because comments are never selected. | Aggregate multi-pass cycle coverage and selected comment/PR coverage; explicit gaps must be resolvable, not permanent normal state. |
| <a id="ar-093"></a>AR-093 | Collection versus Task authority | **Open ownership violation.** `persist_snapshot` commits source facts, creates/revises Tasks, adds work-pool rows and settles the poll in one writer transaction. | Collector commits source facts only; explicit direct/policy `source_import` performs ordinary Task mutations separately. |
| <a id="ar-094"></a>AR-094 | Source change under live work | **Open.** When the local Task still equals the last imported spec, poll invokes ordinary `task.revise`, including beneath another Manager's unreleased Attempt. The frozen old Attempt remains and can block the new revision. | Hold/import conflict or attributable Governor decision; never revise beneath live work automatically. Preserve local-divergence conflict. |
| <a id="ar-095"></a>AR-095 | Work-pool selection | **Open product mismatch.** Poll creates ordinary open Tasks before selection. WorkDispatch consumes general `task.create/revise/claim` facts and does not read `github_work_pool_members`; unselected GitHub work can launch. Apply cannot select zero tasks and preview ignores selection order. | Import is the selection boundary; imported Tasks use the ordinary queue. Delete duplicate work-pool selected/order state. |
| <a id="ar-096"></a>AR-096 | Issue state and comments | **Open semantic gap.** Task projection uses title/body but not Issue state. Close/reopen can add a source fact without revising the Task, leaving a closed Issue's Task open. Comments remain only a count/gap and labels are absent. | Explicit import policy for close/reopen; required comments/fields are captured with revisions or block import. |
| <a id="ar-097"></a>AR-097 | Source fact history | **Open excess.** `github_issue_facts` retains chained A→B→A transitions indefinitely; selected source search found no runtime reader besides writes/schema. | Keep current facts and only immutable revisions referenced by admitted imports/effects; remove unused universal event history. |
| <a id="ar-098"></a>AR-098 | PR/projection source paths | **Open disconnected code.** Issues collection filters PRs, while `pulls::read_pull_request` and `projection::{Check,Summary,Label}Projection` have no current consumers outside their defining files. | Connect one exact current source/projection consumer or delete the paths during U/R migration. |
| <a id="ar-099"></a>AR-099 | Independent remote-ref observation | **Open.** Remote-ref reads exist only inside Forge publication/reconciliation. There is no independent repository/ref/head/time/error source for Governor state. | Add a small remote-ref collector independent of publication; it grants no push authority. |

<a id="trace-2026-10-24-github-sources"></a>
## Source trace at b7a5fe1: GitHub sources and import

Inspected 2026-10-24. This is source evidence, not a live GitHub collection/import or
scale qualification.

### AR-089–090 — a read-only source is implemented as an Operator workflow

`github.source.setup` first verifies the repository through the installed `gh` account,
then requires the local Operator and an existing Forge project whose canonical
repository matches the source. `github.source.poll` also requires the exact Operator
caller. Managers/current GM can inspect sources and select mapped work, but cannot run
routine collection.

Each poll is an ordinary Operation plus one `github_poll_leases` row. Before GET it
moves to `sending`; after restart `outcome_unknown` can move back to `sending` and issue
the exact GET again. Preserve that read retry: it is not a mutation replay. However,
there is no collector loop that resumes the source. A queued poll cannot be cancelled
through the ordinary Operation path because cancellation is explicitly rejected while
the source lease exists. A lost caller can therefore leave collection waiting for a
manual exact resume/read.

Target collection uses a configured source owner with collector epoch/cursor/freshness,
not a user Operation per page window. Setup must not require publication configuration.

### AR-091–092 — page numbers do not define a stable or completable snapshot

The client calls `/issues?state=all&sort=updated&direction=desc&per_page=50&page=N`.
The source persists `next_page`; one poll reads at most eight pages. Duplicate IDs that
change inside one bounded window correctly reject the snapshot, but there is no
protection against an Issue moving from an unscanned later page to an already-scanned
earlier page between polls. It can be absent from the rest of that cycle and appear only
when a later cycle restarts at page one. Reaching the end therefore does not establish
one atomic repository cut.

Coverage is `complete` only when a single pass starts at page one, reaches the end, has
no mapping conflicts and no stored Issue with comments. A source with more than 400 raw
Issues can never meet `complete_from_page_one`; every pass that reaches the end began on
a later page. Any Issue with `comments > 0` also keeps the source partial indefinitely,
because comments are represented as an unresolved gap and never fetched.

Keep explicit gaps, but make coverage aggregate over a stable/reconciled cycle and give
required comments a real capture policy.

### AR-093–094 — collection currently owns Task revision

`persist_snapshot` validates source identity, inserts immutable fact rows, projects a
TaskSpec, calls ordinary `task.create` or `task.revise`, writes current Issue mapping and
work-pool rows, updates source coverage and settles the poll in one immediate writer
transaction. One read can therefore hold the writer while creating/revising hundreds of
Task Operations.

The mapping has one useful fence: if the local Task spec differs from the last imported
spec digest, source refresh reports `local_task_spec_changed` instead of overwriting it.
Preserve that conflict.

When the Task still equals the last imported spec, changed title/body invokes
`task.revise`. The Operator is allowed to revise beneath another Manager's live Attempt.
`tasks::revise` advances the Task revision and resets acceptance while explicitly
preserving the existing Attempt and its frozen snapshot. New work for the updated
revision can then be blocked by the unreleased old Attempt; late old output remains
historical. A background GET must not make that product/work disposition implicitly.

Split collection from import. A source change under live work becomes one bounded
conflict/decision with exact old/new revisions, not an automatic revision.

### AR-095 — selected work is not what WorkDispatch selects

Every mapped Issue creates an ordinary open Task and a `task.create` observation before
`github.work_pool.apply`. WorkDispatch accepts all committed `task.create`,
`task.revise` and `task.claim` facts and has no dependency on the GitHub work-pool table.
Repository-wide source search found work-pool selection consumers in preview/apply,
GitHub effect targeting and automation authorization, not queue/launch admission.
Therefore an unselected GitHub Task can be admitted by ordinary WorkDispatch.

`github.work_pool.apply` also requires 1..200 Task IDs, so it cannot express an empty
selection, and preview orders by Task ID rather than retained selection order. The work
pool is neither a reliable queue nor a clean import boundary. Select source facts by
admitting `source_import`; once a Task exists, use the ordinary queue.

### AR-096–098 — source semantics and retained code are incomplete

`fact_digest` includes Issue state, comment count and URL, but `task_spec` includes only
its title and body. A close/reopen transition can create a new source fact while the
projected Task spec remains identical, so no revision or closure occurs and the Task
stays open. Comments produce only a gap entry; labels are not part of the intake model.
This is honest partial data, but it is not sufficient for automatic implementation
work.

The append-only `github_issue_facts` transition chain has no selected runtime reader;
current Issue rows plus Task mutation Operations own the active state/history needed by
current consumers. Retain referenced source revisions, not an unused universal mirror.

The Issues endpoint filters out PRs. `github/pulls.rs` implements exact PR readback, and
`github/projection.rs` defines candidate-bound DTOs, but selected source search found no
current callers outside those files. They are not a connected PR collector, Governor
view or publication path. Connect the required typed path or delete it; do not cite
source-file existence as implemented behavior.

### AR-099 — remote truth appears only during publication

Exact remote-ref readback exists in Forge and is correctly used to settle an ambiguous
push. No independent collector records repository/ref/head, source time, freshness and
error when publication is absent. Local tracking refs cannot substitute for current
remote state. Add one small read-only source and join it into Governor observation;
publication remains an optional effect.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `store/github.rs` | Source setup/read/poll, lease, failure/commit, fact retention, Task mapping/revision and work-pool selection. |
| `github/client.rs` | `gh api` bounds, repository identity, mutable Issue pagination, PR/label reads and writes. |
| `github/observer.rs` | Bounded page-window assembly and within-window instability check. |
| `github/work_pool.rs` | Immutable origin keys, fact/source revisions and title/body Task projection. |
| `github/protocol.rs` | Public setup/poll/work-pool/effect request shapes and selection bounds. |
| `migrations/006_github.sql` | Source/current/fact/work-pool/lease lifetimes and indexes. |
| `automation/work_dispatch.rs`, `store/automation_work_dispatch.rs` | General Task-fact admission independent of GitHub selection. |
| `store/tasks.rs` | Task create/revise rights, live-Attempt revision and frozen Attempt preservation. |
| `github/pulls.rs`, `github/projection.rs` | Typed but disconnected PR/projection helpers. |
| Forge/effect source search | Remote-ref reads only inside publication/readback; no independent collector. |

Selected paths, not whole-project certification. Live API movement, rate limiting,
repository deletion/transfer, comments, large sources and remote-ref freshness were not
executed.

## Next work

Continue the mailbox/coordination chain named in [Resume here](#resume-here). The next
pass must preserve one addressed durable message path, reconnect cursor, exact
reply/cancel and historical recipients while deleting duplicate cards/contracts/
Concilium/code-scope authority. Do not replace direct agent collaboration with a human
relay or another workflow platform.
