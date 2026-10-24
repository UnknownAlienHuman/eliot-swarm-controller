# Current audit register

This is the active continuation register. The full AR-001–078 evidence remains frozen
in the [2026-10-23 register snapshot](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/38db378c559dd0d95cd60db26702ffa73a954258/docs/agent-operations/audit-register.md).
[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes; and
[status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / Forge and GitHub external-effect ownership**.
- Source inspected at **`38db378c559dd0d95cd60db26702ffa73a954258`**.
  Runtime source is still **`c92120ed70ca7e625440a8a19517c481e73b13c9`**
  from 2026-10-21; the **22 intervening commits are documentation-only**.
- Connected chain:
  `store/forge::{reserve,begin,dispatch_authorized,reconcile}` →
  `swarm-forge-worker` remote validation, ancestry, push and readback →
  `store/github_effects` managed labels →
  `store/github_pr_effects` PR description →
  publication/GitHub-projection automation → restart, transfer and unknown effect.
- Overall audit: **incomplete, 88 entries**. AR-001–078 retain their dispositions in
  the frozen register; AR-079–088 are recorded below.
- Preserved in this pass: exact accepted-candidate publication intent; durable
  `sending` before remote mutation; expected-ref lease plus fast-forward guard; one
  write attempt; exact remote identity/readback; worker process custody; label and PR
  resource slots; no write replay after uncertainty.
- Open in this pass: Forge's sticky process-tree hold has no ordinary evidence-driven
  release; PR PATCH is still executed by the Forge worker; managed-label and PR unknown
  effects require explicit Manager/Operator reconciliation; recovery still depends on
  mutable GitHub source/work-pool rows; publication and projection retain duplicate
  cursor/pending/quarantine engines.
- **Next exact chain:** `github::{client,observer,pulls}` →
  `store/github` poll/lease/page/fact commit → Task mutation and
  `github_work_pool_members` selection → automation import/projection consumers →
  independent remote observation. Separate collection, Task import and writes; prove
  that source refresh/remap cannot erase recovery authority for an admitted effect.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- Still unqualified: live Git server behavior, GitHub CLI/API behavior, Windows/Linux
  process custody, power loss, load/fairness and the autonomous end-to-end work cycle.
- Documentation only. No runtime code, Clippy, test, benchmark or installed-system
  qualification changed in this checkpoint.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named chain. A documentation commit never closes a runtime
finding.

<a id="findings-ar-079088"></a>
## Findings AR-079–088

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-079"></a>AR-079 | Forge intent, slot and write boundary | **Preserve.** Admission freezes accepted candidate, commit/tree, target ref, expected old ref/create intent, project policy and actor. The exact slot is rechecked and the Operation becomes `sending` before authorization reaches the worker. | Keep one Operation and one remote-Git owner while deleting manifests and automation copies. |
| <a id="ar-080"></a>AR-080 | Ref update concurrency | **Scope correction / preserve.** The worker verifies predecessor ancestry and pushes with `--force-with-lease=<target>:<expected>`; create uses an empty expected value. Older documentation describing an ordinary non-force push with no compare-and-swap guard is false. | Live server qualification, protected-branch behavior and exact error/readback classification. |
| <a id="ar-081"></a>AR-081 | Forge restart and process-tree hold | **Preserve plus open.** `sending`/unknown work becomes readback-only and requires original worker-family departure. An unconfirmed process tree creates a sticky repository-wide manual hold that later matching ref readback cannot clear. | Add an exact evidence-driven administrative disposition; never clear by time, PID guess or successful ref readback alone. |
| <a id="ar-082"></a>AR-082 | Publication automation ownership | **Open duplication.** Publication owns global/per-entry cursors, activation/catch-up, pending retries, quarantine and recent state around the already durable Forge Operation. | One current-state loop admits the ordinary publication Operation; preserve occurrence/cause identity and fair bounded scanning. |
| <a id="ar-083"></a>AR-083 | Managed-label effect | **Preserve.** Store marks `sending`, performs at most one label mutation, then validates immutable repository/Issue IDs and exact desired state. Unknown recovery is readback-only. | Move execution under the selected GitHub API owner without weakening desired-state convergence. |
| <a id="ar-084"></a>AR-084 | Automatic label recovery | **Open.** The automatic effect drain selects only `queued`; `outcome_unknown` returns `GITHUB_EFFECT_READBACK_REQUIRED` and needs a separate current-GM/Operator reconcile Operation. | Bounded automatic `reconcile_once` using the same ordinary readback command; unchanged uncertainty must coalesce, not require routine human work. |
| <a id="ar-085"></a>AR-085 | Historical label target lifetime | **Open coupling.** Reconciliation re-resolves the admitted repository/Issue through current `github_sources`, `github_issue_items`, Task/work-pool mapping and the effect slot. Source remap/deletion can block readback of an otherwise fully retained effect. | Retain immutable effect target independent of collector/import lifecycle; current source facts may detect conflict but must not erase recovery identity. |
| <a id="ar-086"></a>AR-086 | PR metadata effect owner | **Open duplication.** `github_pr_effects` owns admission and GitHub readback, but `swarm-forge-worker` also executes the `gh api ... PATCH` mutation. One binary therefore owns remote Git and a GitHub REST write. | Move PR PATCH to the single GitHub API effect owner; Forge remains remote Git only. |
| <a id="ar-087"></a>AR-087 | PR recovery and historical target | **Preserve plus open.** Recovery requires exact worker-family departure, publication/Task/Attempt, repository/PR ID, open same-repository head/base, published head SHA, desired title/body and resource slot. It is still explicit-GM and requires a current `github_sources` repository mapping. | Automatic successor readback from retained immutable target, independent of source registry survival. |
| <a id="ar-088"></a>AR-088 | GitHub projection lifecycle | **Open duplication.** GitHub projection owns activation watermarks, per-entry state, quarantine and a durable queued-effect drain cursor; unknown effects are intentionally excluded. | Delete the projection workflow after enabled policy queries current accepted/source facts and calls the same explicit effect owner as a direct agent. |

<a id="trace-2026-10-24-forge-github-effects"></a>
## Source trace at 38db378: Forge and GitHub effects

Inspected 2026-10-24. This is source evidence, not a live push, GitHub mutation or
process qualification.

### AR-079–080 — Forge already has the critical ref guard

Forge admission resolves the exact accepted Task/Attempt/submission/candidate and
acceptance Operation, freezes the commit/tree, repository, target ref, expected old ref
or create intent, policy revision and GM epoch, and reserves one semantic slot. Before
external I/O, the final transaction rechecks current authority or the explicit transfer
continuation, exact slot, selected project policy and accepted candidate, then changes
the Operation to `sending`.

The worker validates the configured push endpoint and remote safety settings, reads the
remote ref, and rejects a changed predecessor before push. For an existing target it
runs `merge-base --is-ancestor <expected> <candidate>`. The push uses a direct argv with
`--force-with-lease=<target>:<expected>` and the exact candidate refspec. For creation,
the expected value is empty, requiring the target not to exist. Exact post-push remote
readback is still required for settlement.

This corrects the old Forge document: the implementation is not an unguarded ordinary
push. Preserve the expected-ref lease and ancestry check while simplifying proof
packets. A readback showing the candidate proves final remote state, not which actor
performed an identical concurrent update; that distinction does not authorize another
write.

### AR-081 — uncertainty is safe but can become permanently manual

The worker starts only after private plan, process-group owner and matching authorization
are retained. A timeout, transport/process ambiguity or unreadable result never causes
a second push. Restart converts admitted work to exact readback-only execution and
requires the original worker family to be gone.

If family emptiness cannot be proved, the result records
`process_tree_unconfirmed=true`. `require_no_process_tree_hold` then blocks every later
Forge push for the repository. The hold is deliberately sticky even when later remote
readback shows the candidate, because ref state does not prove process departure. This
is safe, but the only documented resolution is manual operator intervention. Add a
narrow administrative disposition backed by exact process-custody evidence; do not
weaken the hold or clear it from elapsed time.

### AR-082 — publication reserves an Operation through another workflow engine

`automation_publication` consumes acceptance observations, maintains a global cursor,
per-entry activation/catch-up state, pending retries, quarantine and recent summaries,
then reserves the ordinary Forge Operation. Those copies do not execute Git and are not
its recovery authority.

The target host loop should query current accepted candidates and publication policy,
atomically admit the same Forge Operation and let Forge own effect/readback state. Keep
the acceptance occurrence and exact effect slot as domain identity; delete copied
pending/cursor lifecycle only after the positive automatic path is connected.

### AR-083–085 — label writes are disciplined but recovery is not autonomous

The managed-label owner retains the exact source/Task/Issue/label desired state. A fresh
call first performs an exact repository/Issue read. If mutation is needed, `begin_write`
commits `sending`; the API is called once; a second exact read settles or leaves the
Operation unknown. An unknown call performs no write.

Direct and automated queued paths use the same resource slot and desired-state
semantics. However, the automated drain pages only queued Operations. Its unknown branch
returns `GITHUB_EFFECT_READBACK_REQUIRED`; a current GM or Operator must admit
`github.effect.reconcile_managed_label`. That readback can settle the original without
rewriting it, but routine correctness is no longer automatic.

The retained result and receipt contain repository/Issue numbers and IDs, source
revision and desired label. Nevertheless recovery reconstructs the target through
current source, Issue mapping, Task/work-pool rows and slot. Collector/import cleanup or
remap can therefore remove the ability to read an admitted external effect. Recovery
must use the immutable admitted target; current source rows are additional conflict
facts, not the owner of historical identity.

### AR-086–087 — PR PATCH is in the wrong worker

`github_pr_effects` correctly binds an update to one applied Forge publication and the
exact Task/Attempt, repository/PR ID, target/base branch, published head SHA and desired
title/body. It reserves a repository/PR slot, commits `sending`, and requires exact PR
readback. Unknown reconciliation verifies worker-family departure and never repeats the
PATCH.

The mutation itself is still delegated to `swarm-forge-worker` as a
`github_pr_description` / `PatchOnce` plan. That worker hashes and launches `gh`, writes
the JSON body once and reports only an unknown write outcome; Store then uses the
GitHub API owner for readback. This splits one GitHub API domain across Forge and
GitHub owners. Move the PATCH into the GitHub API effect worker and leave Forge with
remote Git push/readback only.

PR recovery retains more immutable publication context than the label path, but still
requires a current `github_sources` row for the repository and a current GM/Operator
reconcile call. Preserve all exact PR identity fences while making successor readback a
bounded host correctness duty.

### AR-088 — projection is admission policy, not effect recovery

`automation_github_projection` has its own global/per-entry watermarks, recent state,
quarantine and queued-effect drain cursor. It explicitly excludes unknown Operations
from its drain and forwards only queued work to `call_automatic`.

Replace this with the one host current-state loop. Enabled policy may admit the same
managed-label/PR Operation as a direct Manager, but the effect owner alone controls
`queued → sending → settled|rejected|outcome_unknown` and exact readback. Disabling or
transferring policy changes future admissions; it neither repeats nor abandons an
already admitted write.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `store/forge.rs` | Direct/on-behalf reserve, effect slot, final authority/accepted-candidate recheck, `sending`, worker authorization, finish, restart/reconcile and process-tree hold. |
| `swarm-forge-worker/src/main.rs` | Plan/authorization, remote validation, ancestry, force-with-lease push, exact readback, PR PATCH, bounded process/output and result envelope. |
| `store/github_effects.rs` | Direct/automatic label admission, semantic slot, one write, exact readback, unknown result and explicit reconcile. |
| `store/github_pr_effects.rs` | Publication-bound PR target, native worker custody, one PATCH, exact PR readback and explicit reconciliation. |
| `store/automation_publication.rs` | Acceptance cursor, activation/catch-up, pending retry/quarantine and Forge reservation. |
| `store/automation_github_projection.rs` | Acceptance projection, per-entry state, queued-only effect drain and transfer relocation. |
| `docs/forge-publication.md` | Current contract claims compared with source; stale non-force/no-CAS text corrected by this checkpoint. |

Selected paths, not whole-file certification. Git server, GitHub service, credentials,
branch protection, network failure and platform process trees were not executed.

## Next work

Continue the GitHub collection/import chain named in [Resume here](#resume-here). The
next pass must distinguish immutable source facts from Task import and from external
mutation, then connect an independent remote-ref view. Do not create another poll or
projection workflow.
