# GitHub sources — collect facts, import work explicitly

This is the target contract for GitHub Issue/PR collection, Task import and independent
remote observation. [Implementation U0–U2/U5](implementation.md#u--github-sources-import-and-effects)
owns code order; [external effects](external-effects.md) owns GitHub mutations;
[current audit](audit-current.md#findings-ar-089099) records source evidence.

## Product law

```text
bounded collector GET
-> current immutable-identity source facts + freshness/coverage/gaps
-> explicit source_import decision
-> ordinary Task create/revise
-> ordinary queue and work policy
```

Collection is read-only with respect to Tasks. Import is a Store planning mutation and
performs no GitHub write. Publication and metadata effects are separate Operations with
one-write/exact-readback semantics.

## Owners

| Responsibility | Owner |
| --- | --- |
| GitHub API reads and provider pagination | one configured source collector |
| Current Issue/PR facts, source cursors, freshness and gaps | Store source records |
| Selecting facts and creating/revising Tasks | Manager or enabled `source_import` policy |
| Task ranking, claim and launch | ordinary queue/current-state loop |
| Labels and PR metadata writes | one GitHub API effect owner |
| Remote Git ref publication | Forge |
| Repository/ref/head observation | independent remote-ref collector |

A collector has no Task-planning authority. A work-pool row is not a second queue.
Forge configuration is not a prerequisite for read-only source discovery.

## Source identity and configuration

A source is bound to immutable provider identity:

```text
source_id
provider/host
repository_id
current owner/name/path as presentation facts
project/import policy scope
collector epoch
cursor/high-water/overlap state
last attempted/successful read
freshness and coverage
explicit gaps/errors
```

Repository rename or transfer updates presentation coordinates only after immutable-ID
readback. A changed repository ID is a conflict, never silent rebinding.

Provider credentials remain outside Store and outside model-visible configuration. A
scoped Manager/policy may enable collection for its project under current configuration;
routine polling must not require the local Operator or an existing Forge publication
mapping.

Disabling collection stops future reads. It does not delete current source facts,
imported Tasks or recovery identity for admitted external effects.

## Collection and pagination

Do not persist a numeric page across separate reads of a list sorted by mutable
`updated_at`. Items can move between pages while the cycle is in progress, so such a
cursor is not a stable cut.

Use a provider contract that can establish a resumable source cut. Depending on the
selected API, this may be:

- stable immutable-ID/creation-order traversal plus a complete reconciliation cut;
- an exact server cursor/high-water token;
- overlapping updated-since windows combined with periodic full reconciliation.

The chosen algorithm must document its ordering key, overlap, deduplication, restart
cursor and deletion/absence semantics. A timestamp or page number alone is not a
revision token.

Each pass is bounded by rows, bytes, pages and elapsed time. Network I/O occurs outside
Store transactions. Results commit in short bounded batches with the same collector
epoch/cut identity. A later pass resumes the cut; it does not create a user-facing poll
workflow or source-read lease.

GET retries are safe only for the same retained cut/range. A failed read preserves the
last facts as stale and records a gap. Failure or an empty partial page never means an
empty repository.

## Current facts and coverage

Current facts are keyed by immutable repository and item IDs. Retain selected bounded
fields required by actual consumers:

```text
item kind and immutable ID / current number
state, title and body
selected comments or explicit comment gap
selected labels/milestone/relations only when import policy uses them
head/base/merge identity for PRs when required
source revision and provider update marker
observed_at and collector cut/epoch
```

A source revision is derived from the selected content used by import, not merely
`updated_at`. State and comments have separate meaning from title/body. Required
comments are fetched and revisioned or block import explicitly; a count-only gap cannot
be called complete.

Coverage is aggregate across one collector cycle:

```text
full | partial | unknown
cut start/end or high-water
ranges/pages completed and continuation
item/comment/PR coverage
truncation, instability and error gaps
last successful full reconciliation
```

A source larger than one bounded pass can still converge to full coverage across
passes. Reaching the end from page N does not prove pages 1..N−1 belong to the same
snapshot. Deletion is inferred only by a complete reconciliation contract, never by
absence from a partial scan.

Keep one current fact per item plus immutable source revisions referenced by admitted
imports/effects. Do not retain an unbounded A→B→A event chain with no reader.

## Explicit `source_import`

Import names exact current facts and expected source revisions:

```text
source_id
item immutable IDs
expected source revisions
project and import policy revision
selected Task mapping action
client/occurrence identity
```

The import transaction rechecks the source facts and current Manager rights, then calls
ordinary `task.create` or `task.revise`. It records the source-to-Task relation and the
applied source/spec digest. Collection and import are never committed as one mutation.

### Existing and live Tasks

- If the Task still equals the last imported spec and no live Attempt conflicts, policy
  may admit an explicit revision for a changed source fact.
- If the local Task diverged, report an import conflict; never overwrite local intent.
- If an unreleased Attempt exists, do not revise beneath its frozen snapshot. Hold the
  import or create one attributable Governor decision with old/new source revisions and
  concrete options.
- A closed/reopened Issue follows explicit import policy: hold, revise, archive/close or
  ignore. It never silently leaves an ordinary open Task dispatchable merely because
  title/body did not change.
- Imported PRs use a distinct typed source relation; Issue and PR identity are never
  conflated because the Issues endpoint returns both.

A source change does not automatically cancel native work, accept a Task or close a
workspace. Those remain ordinary Task/resource decisions.

## Queue and selection

Import is the selection boundary. Once imported, the Task enters the ordinary queue and
uses the same project priority, dependency, correction and admission policy as any
other Task.

Delete `github_work_pool_members`, selected/order state and `github.work_pool.*` after
concrete consumers move. A separate source work pool must not:

- create open Tasks before selection;
- act as a hidden effect permission;
- duplicate queue order;
- require a nonempty selection merely to clear all work;
- allow WorkDispatch to consume unselected `task.create` facts.

Preview reads source facts and proposed import consequences without reserving work.

## PR and remote observation

A typed PR collector is useful only when a current consumer uses its exact head/base,
state and content revision. Connect it to source records or delete the disconnected
helper/types; file existence is not a product path.

Remote repository/ref observation is independent of publication:

```text
canonical repository + ref
observed head or absence
method/source time
freshness and error
```

It feeds the Governor view and can support readback, but it grants no push authority and
cannot settle a different effect without exact Operation/resource identity.

## Restart and handover

Collector restart increments its epoch and resumes from retained source cut/high-water.
There is no per-pass user Operation to steal or cancel. A bounded safety rescan recovers
a lost wake.

GM handover changes future import authority. It does not rewrite source authorship,
existing Task owners, imported revisions or external-effect targets. Current authorized
policy may continue collection and read current facts; importing a fact remains an
attributable ordinary mutation.

## Finite transition

1. Inventory configured sources, active poll leases, partial cursors, current Issue
   rows, fact chains, mapped Tasks and work-pool selections.
2. Finish or safely dispose each read-only queued/sending/unknown poll; GET uncertainty
   may be reread, but preserve its last coverage/error.
3. Materialize source records with collector epoch, aggregate cycle coverage and exact
   current fact revisions. Keep immutable revisions referenced by current Tasks/effects.
4. Stop poll-time Task creation/revision. Add explicit direct/policy `source_import`.
5. For mapped live Tasks, retain their applied source/spec digest and surface conflicts;
   never revise beneath a live Attempt during migration.
6. Switch consumers from work-pool selection to source relations or ordinary Task
   queue/effect identity.
7. Connect the needed PR and remote-ref collectors; delete disconnected projection
   DTOs or wire them to one real consumer.
8. Delete `github.source.poll`, poll leases, mutable page cursor, `github.work_pool.*`,
   work-pool rows and unreferenced fact history together with their API/CLI/MCP/tests.

Completed Operation/Task history remains readable; it does not keep the old collector
or import workflow executable.

## Verification

- repository identity survives rename/transfer and rejects ID substitution;
- mutable list updates during pagination cannot skip an item from a completed cut;
- a source larger than one pass converges to full aggregate coverage;
- failed/partial reads retain stale facts and explicit gaps, never an empty source;
- required comments/PR fields are either captured with revisions or block import;
- collector commits no Task or external mutation;
- explicit import is idempotent by source fact/revision and does not overwrite local
  divergence;
- source change during a live Attempt creates a bounded conflict/decision, not a hidden
  `task.revise`;
- closed Issue policy cannot leave an unintended open dispatchable Task;
- unimported facts never reach WorkDispatch;
- imported Tasks use ordinary queue order with no GitHub work-pool authority;
- restart resumes collector progress without a user poll Operation/lease;
- independent remote-ref observation works without publication;
- no unbounded unused fact chain, disconnected PR/projection path or compatibility
  source workflow remains after cutover.
