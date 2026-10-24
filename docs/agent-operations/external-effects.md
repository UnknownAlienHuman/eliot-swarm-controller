# External effects — one owner, one write, exact readback

This is the implementation contract for remote Git and GitHub API mutations.
[Implementation K/U](implementation.md#k--selected-effect-owners) owns code order;
[audit index](audit-index.md#evidence-checkpoints) links source evidence;
[GitHub sources](github-sources.md) owns collection/import; and
[status](../implementation-status.md) records landed behavior.

## Product law

```text
authorized intent
-> one durable Operation and one exact resource/effect identity
-> commit `sending` before external I/O
-> at most one mutation attempt
-> exact readback
-> settled | rejected-before-write | outcome_unknown
```

A timeout, disconnect, process exit or lost response never authorizes another mutation.
Unknown holds only its exact resource while unrelated work continues. Recovery reads;
it does not write.

## Owners

| Domain | Effect owner | Read-only sources |
| --- | --- | --- |
| Remote Git ref publication | Forge | local Git state and independent remote-ref collector |
| GitHub Issue/PR metadata | one GitHub API effect owner | GitHub source collector and exact API readback |
| GitHub collection | source collector | no mutation authority |
| Task import | Store Manager/policy mutation | selected immutable source facts |
| Automation | host current-state loop | admits ordinary Operations; owns no external lifecycle |

Forge must not execute GitHub REST PATCH. The GitHub collector must not create Tasks or
write labels. Automation must not keep another copy of effect state.

## Durable intent

The admitted Operation retains enough immutable identity to recover after current
configuration, ownership or source indexes change:

```text
Operation ID and original request identity
admitting actor / represented Manager / policy occurrence
project, Task revision, Attempt and accepted candidate when applicable
external service/account/repository identity
resource identity (ref, Issue ID, PR ID)
desired mutation and content digest
precondition (expected ref/head/base/revision)
effect slot key
worker/process receipt when a worker is used
```

Mutable source/work-pool rows can report a conflict. They cannot be the only copy of an
already admitted effect target.

## Effect slot versus idempotency

The slot serializes one resource; the Operation identifies one desired effect.

- Forge slot: canonical repository + target ref, with candidate and expected-ref intent
  in the Operation.
- Label slot: immutable repository ID + Issue ID + label; desired presence is retained
  in the Operation.
- PR metadata slot: immutable repository ID + PR ID; published head/base and desired
  title/body digest are retained in the Operation.

An identical request returns the original Operation. A conflicting request cannot
replace an unresolved slot. A new request ID never bypasses an unknown effect.

## Remote Git publication

Preserve the current useful Forge boundary:

1. freeze exact accepted candidate commit/tree and publication policy;
2. recheck authority, acceptance, project policy and slot in the final transaction;
3. set `sending` before releasing worker authorization;
4. validate canonical push endpoint and unsafe Git configuration;
5. read the target ref and compare it with expected old/missing state;
6. for an existing ref, prove expected predecessor is an ancestor of the candidate;
7. issue one direct-argv push with
   `--force-with-lease=<target>:<expected>` and exact candidate refspec;
8. settle only from exact remote-ref readback.

The lease is an expected-ref guard. Do not describe this as an unguarded ordinary push.
`force=false` in Eliot policy means the candidate must be fast-forward from the expected
predecessor; it does not remove the lease option used to enforce the expected ref.

### Process uncertainty

Worker-family ownership and departure are separate from remote ref state. A matching
ref does not prove that a timed-out process is gone. Preserve the hold when process-tree
emptiness is unknown.

Add one narrow administrative resolution that accepts only exact process-custody
proof: original job/owner token, PID/birth/image/group identity and observed departure.
It may release the process hold, not fabricate a push outcome. Ref readback still
settles the Operation separately.

## GitHub metadata mutations

One GitHub API owner handles both managed labels and PR title/body updates.

For each effect:

1. resolve and retain immutable repository/resource IDs before admission;
2. perform an initial exact GET;
3. settle without a write when desired state already exists;
4. otherwise commit `sending`;
5. issue one API mutation;
6. perform exact GET readback;
7. settle only when repository/resource identity and desired state match;
8. otherwise retain `outcome_unknown`.

Desired-state convergence does not prove which actor performed an identical change. It
still proves that another write is unnecessary.

The PR owner additionally verifies open/unmerged state, same-repository head/base,
exact branches and published head SHA. The label owner verifies immutable repository
and Issue IDs, not only names/numbers.

## Successor readback

Readback is a correctness duty of the host loop:

```text
outcome_unknown Operation
+ retained immutable target
+ no active readback child
+ current scoped authority for disclosure/readback
+ worker departure proof when applicable
-> one ordinary readback Operation
```

The readback Operation never owns the mutation slot and never sends the write. A
confirmed observation settles the original first, then itself. An inconclusive or
unavailable read settles the lookup with `resolved=false` or remains due according to
bounded policy; the original stays unknown. Unchanged uncertainty coalesces into one
Governor decision while other effects progress.

GM handover changes who may initiate future work. It does not rewrite the original
caller, accepted candidate, effect target or worker. An authorized successor may read
and settle the original through retained scope without adopting authorship.

## Source, import and effect separation

```text
collector GET -> immutable source facts + freshness/coverage/gaps
policy import -> explicit Task create/revise
accepted candidate -> explicit external-effect Operation
```

[GitHub sources](github-sources.md) defines collector cuts, coverage, explicit import
and independent remote observation. Collection must not mutate Tasks in the same
transaction. Import must not write GitHub. Effect recovery must not require a source
row or work-pool selection to survive. Remote-ref observation runs independently of
publication and records source time/error.

## Automation

Enabled policy queries current facts and admits the same ordinary effect Operation as a
direct Manager call. Keep only the domain occurrence/effect identity needed for
idempotency. Remove:

- publication and GitHub-projection global/per-entry cursors;
- copied pending/retry state and quarantine lifecycle;
- separate queued-effect drain cursor;
- automatic-only actor semantics;
- manual fallback for ordinary unknown readback.

Disable/edit/transfer affects future admission. An admitted queued effect is rechecked
at the write boundary; an admitted sending/unknown effect remains owned by its original
Operation and readback path.

## Finite transition

1. Inventory concrete queued/sending/unknown Forge, label and PR Operations and their
   slots/workers.
2. Add retained immutable targets where current recovery depends on source/work-pool
   rows; migrate only unsettled records.
3. Connect one bounded host `reconcile_once` to existing explicit readback handlers.
4. Move PR PATCH from `swarm-forge-worker` to the selected GitHub API worker.
5. Switch automatic publication/metadata admission to the ordinary current-state loop.
6. Delete copied cursors, pending lists, quarantine/effect-drain state and obsolete
   worker plan/fixtures/config together.
7. Leave completed history readable without keeping an executable compatibility path.

Do not build a generic distributed transaction framework, universal outbox or second
external-effects database.

## Verification

- concurrent identical admission returns one Operation; conflicting desired effect is
  blocked by the exact slot;
- cancellation before `sending` performs no external I/O;
- authority/target change before `sending` rejects without write;
- Forge proves ancestry and uses the exact expected-ref lease;
- lost push response never produces a second push; exact ref readback settles it;
- process-tree uncertainty cannot be cleared by ref state or elapsed time;
- label and PR mutation each execute at most once and settle by immutable-ID readback;
- host restart and GM handover automatically recover unknown effects without changing
  authorship or requiring routine operator action;
- source/work-pool deletion cannot erase readback authority for an admitted effect;
- PR PATCH no longer exists in the Forge worker;
- collection, import, publication and metadata writes have distinct owners;
- removal of automation engines does not make routine publication manual;
- unrelated external effects proceed while one exact resource remains unknown.
