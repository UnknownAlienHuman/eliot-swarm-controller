# Forge publication — current source contract and transition

This document describes the current Forge path. The compact target, including GitHub
metadata writes and automatic successor readback, is
[External effects](agent-operations/external-effects.md). Implementation order remains
in [agent operations](agent-operations/implementation.md#k--selected-effect-owners);
source findings are in [AR-079–088](agent-operations/audit-current.md#findings-ar-079088).

## Responsibility

Forge publishes one already accepted candidate commit to one configured remote branch.
It does not accept Tasks, merge pull requests, edit GitHub metadata, import Issues or
own a generic publication workflow.

```text
accepted candidate
-> durable forge.publish Operation
-> exact target/expected-ref slot
-> `sending`
-> one lease-bound push
-> exact remote-ref readback
```

Acceptance and publication remain separate facts. A published ref never accepts a
Task; an accepted candidate is not automatically public unless enabled policy admits
the publication Operation.

## Admission

The retained publication intent freezes:

- project and canonical repository;
- exact Attempt, Task revision, submission and acceptance Operation;
- candidate reference, digest, commit and tree;
- selected remote and target branch;
- expected old commit or explicit create intent;
- project publication-policy revision;
- admitting GM epoch and original request identity;
- `force=false`.

Admission validates the accepted candidate and reserves one exact effect slot. Identical
intent can reuse the original Operation. A conflicting unresolved effect cannot replace
its slot by changing request ID.

Before external I/O, the final transaction rechecks current authority or explicit
transfer continuation, accepted candidate, project policy, execution mode and slot,
then changes the Operation to `sending`. No push is authorized from a queued row.

## Worker boundary

`swarm-forge-worker` receives one private plan and owner token. It joins the owned
process group, publishes exact process identity and waits for a matching authorization
receipt. The worker verifies the selected Git executable/image, repository, remote
endpoint and unsafe Git configuration before mutation.

The worker owns no Task or acceptance state. Store owns the Operation and decides
whether the plan may cross the write boundary.

## Ref precondition and push

The current implementation has an expected-ref guard; it is not an unguarded ordinary
push.

1. Resolve and validate the configured push endpoint.
2. Read the target ref from that endpoint.
3. Require the exact expected old commit, or absence for creation.
4. When updating an existing ref, run
   `merge-base --is-ancestor <expected> <candidate>`.
5. Issue one direct-argv push with:

```text
git push --porcelain --no-verify --no-follow-tags --recurse-submodules=no
  --receive-pack=git-receive-pack
  --force-with-lease=<target-ref>:<expected-old-or-empty>
  <validated-endpoint> <candidate-commit>:<target-ref>
```

`force=false` means Eliot rejects a candidate that is not fast-forward from the expected
predecessor. `--force-with-lease` supplies the receive-time expected-ref check; it does
not authorize arbitrary history rewrite.

A nonzero push exit can be classified as a known failure only with bounded exact
rejection evidence and/or readback showing the expected ref remains. Otherwise the
outcome stays unknown.

## Readback and recovery

The worker reads the exact remote ref after the push. Store settles Applied only when it
observes the candidate commit at the target. A timeout, lost response, unreadable
process result or failed readback never causes another push.

After restart, `sending` work becomes `outcome_unknown`. The successor executes a
readback-only plan after the original worker family is proved departed. It preserves the
original Operation, candidate, expected-ref intent and authorship.

Remote ref state and process departure are different facts. Observing the candidate
cannot prove that a timed-out child process is gone.

## Process-tree hold

If the worker family cannot be proved empty, Forge records
`process_tree_unconfirmed=true` and blocks later pushes for the canonical repository.
The hold is intentionally sticky; successful ref readback or elapsed time does not clear
it.

Current source exposes this as manual operator intervention. Target behavior adds a
narrow administrative disposition backed by exact retained job/owner/PID-birth/image/
group departure evidence. That disposition releases only the process hold. It does not
fabricate a push result or permit a second write.

## Automation boundary

Current `automation_publication` owns activation/catch-up watermarks, per-entry state,
pending retries, quarantine and recent summaries before reserving Forge work. Those are
transitional copies, not publication truth.

Target automation queries current accepted candidates and policy, then admits the same
ordinary Forge Operation as a direct Manager. Forge alone owns the effect slot,
`queued/sending/unknown` lifecycle and readback. Disable/edit/transfer affects future
admission and cannot abandon or repeat an admitted publication.

## GitHub metadata is not Forge

Current `swarm-forge-worker` also contains a `github_pr_description` PATCH plan. That is
a confirmed ownership violation: PR title/body is a GitHub API mutation, not remote Git
publication. Move it to the single GitHub API effect owner together with managed labels.
After the cut, Forge worker contains push/readback only.

## Finite transition

1. Inventory queued/sending/unknown Forge and PR-description worker Operations.
2. Preserve exact worker/process and remote-ref readback for every unsettled Forge push.
3. Add automatic bounded successor readback for unknown publication Operations.
4. Move PR PATCH to the GitHub API owner and disposition concrete old worker jobs.
5. Switch publication policy to the one host current-state loop.
6. Delete publication cursor/pending/quarantine state and GitHub PATCH worker plan,
   validation, fixtures and configuration together.
7. Keep completed Operation history readable without retaining an executor fallback.

## Verification boundary

Required connected cases:

- identical publication intent returns one Operation; conflicting slot is rejected;
- candidate/authority/policy change before `sending` performs no push;
- expected predecessor ancestry and force-with-lease are both exercised;
- create fails safely when the target appears concurrently;
- lost push response and host restart perform readback only;
- exact candidate readback settles the original Operation;
- process-tree uncertainty blocks only the repository and cannot be cleared by time or
  ref state;
- exact process departure can release the hold without changing publication outcome;
- GM handover permits authorized successor readback without changing authorship;
- PR PATCH is absent from Forge worker after migration;
- enabled policy publishes without a routine human step;
- unrelated repositories continue while one repository is held unknown.

Current source inspection is not live Git/server/process qualification.
