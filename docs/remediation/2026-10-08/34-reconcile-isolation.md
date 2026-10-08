# R34. Automation reconcile: poison-fact isolation and one truthful disposition model

**PR task; production code has not been changed.**  
Evidence baseline: `40591a295af94b1541ec2ba30afe8e3247701a71`, reviewed 8 October 2026.

This task owns the bounded Store automation reconcilers:

- review/script dispatch;
- WorkDispatch;
- automatic publication;
- Goal progression;
- GitHub projection;
- review disposition.

It does **not** own calendar/source scheduling or credential issuance pacing; those remain R12/#38. It does not own the domain semantics being changed in R21/R22/R32 or adapter lifecycle.

## 1. Confirmed current failure chain

### 1.1. Five domains share one transaction and one `?` chain

`Store::reconcile_automations_once` creates one `TransactionBehavior::Immediate` transaction and calls, in order:

```text
automation_dispatch::reconcile
automation_work_dispatch::reconcile
automation_publication::reconcile
automation_goal_progression::reconcile
automation_github_projection::reconcile
```

Every call is followed by `?`. A semantic/corruption error in the last domain rolls back successful cursor/admission work from the first four. The caller then invokes no post-commit effects or script preparation for that pass.

There is no product requirement that these five independently configured consumers commit atomically as one aggregate. They have different cursors, slots, pending queues and source facts. The current common transaction couples their failures without giving a useful cross-domain invariant.

### 1.2. Entry enumeration fails the whole domain

Examples:

- `automation_work_dispatch::enabled_entry_page` parses and validates every selected sealed entry with `?`; one malformed entry prevents all later valid entries in the page.
- `automation_dispatch::reconcile` calls `reconcile_entry` and `reconcile_script_trigger_entry` with `?` for each entry.
- analogous enabled-entry page and loop shapes exist in publication, Goal progression, GitHub projection and review disposition.

This is not only “fail closed.” The damaged object is retained, so the same first object can poison every later pass indefinitely.

### 1.3. Existing SAVEPOINTs do not provide subject isolation

`review_disposition::consume_isolated` correctly rolls back partial writes, but returns the original error. Its caller uses `?`, so the complete entry/domain transaction still aborts.

Worse, `event_identity(event)?` is executed **before** `consume_isolated`. A malformed event never reaches the savepoint.

`automation_publication::consume_event_isolated` is closer to the required shape:

- known temporary prerequisites become `Pending`;
- known no-longer-applicable facts become `Skipped`;
- reservation changes are protected by a savepoint.

But unknown errors still abort the domain, and its `recheck_pending` path can repeatedly surface a retained damaged event. The same classification vocabulary is not used by sibling reconcilers.

A SAVEPOINT guarantees rollback of a subject's partial writes. It does not decide whether the subject is retryable, terminal, damaged or infrastructure-fatal.

### 1.4. Pending recheck can be structurally unsafe if errors are later caught

`automation_work_dispatch::recheck_pending` removes the entire pending vector with `mem::take`, rebuilds `keep`, and has fallible `?` calls inside the loop. Today transaction rollback protects the retained record because the error escapes. A future outer “catch and continue” would commit a shortened in-memory state unless the helper is changed first.

R34 must therefore make subject isolation explicit inside each pending mutation, not merely catch errors one level higher.

### 1.5. The host/worker path amplifies the error

The host reconciliation loop calls:

```text
store.reconcile_automations_once().await?
store.reconcile_review_dispositions_once().await?
```

A retained poison fact is therefore not represented as a bounded degraded result; it exits the current reconciler future through the ordinary error path. Host restart/isolation policy is owned elsewhere, but R34 must stop routine retained-data damage from being expressed as process failure.

## 2. Audit corrections and boundaries

### 2.1. Do not swallow every error

The required behavior is not:

```rust
let _ = reconcile_subject(...);
```

The following remain hard errors and roll back their current domain transaction:

- SQLite prepare/read/write/commit failures;
- inability to create, rollback or release a savepoint;
- serializer failure for a value produced in the same code path;
- broken global/domain cursor or state record whose safe continuation cannot be established;
- impossible duplicate/collision in the domain's own durable identity;
- unknown error code not explicitly classified by that domain.

A database/storage error must never be projected as `skipped`, `quarantined` or successful progress.

### 2.2. Do not create a string-prefix “universal classifier”

Current code already contains fragile classifiers based on prefixes/substrings. R34 introduces one common **result vocabulary**, but every domain supplies an exhaustive local mapping from its own typed/contextual error to that vocabulary.

Unknown code is fatal. Adding a new domain error without classifying it must fail a test/build guard, not silently become retryable.

### 2.3. Quarantine is evidence, not deletion

A damaged retained fact is not erased or rewritten. The reconciler retains:

```text
domain
entry identity
source observation/operation identity when available
safe error code
first/last observed time
bounded occurrence count
source digest or exact pointer, not raw secret-bearing payload
```

The cursor advances past that fact only in the same transaction that records the quarantine/disposition. The original Observation, Operation and automation entry remain immutable.

### 2.4. No second workflow engine or broker

SQLite remains the only authoritative Store. No Restate/DBOS/Temporal server, message broker, generic DLQ service or new event ledger is added.

## 3. One internal disposition vocabulary

Add a small Store-private module, for example `store/automation_reconcile.rs`.

Names below are proposed, not existing API:

```rust
enum SubjectDisposition<T> {
    Applied(T),
    Pending {
        code: &'static str,
        wake_when: BoundedWakeSet,
        not_before_ms: Option<i64>,
    },
    Skipped {
        code: &'static str,
    },
    Quarantined {
        code: &'static str,
        evidence: QuarantineEvidence,
    },
}

enum DomainPassStatus {
    Progressed,
    Idle,
    Degraded,
}
```

The helper around a subject savepoint may accept:

```rust
with_subject_savepoint(
    tx,
    static_savepoint_name,
    subject_identity,
    run,
    classify_domain_error,
)
```

Required behavior:

1. create savepoint;
2. run one exact subject;
3. `Applied` → release savepoint;
4. known `Pending` / `Skipped` / `Quarantined` error → rollback subject writes, release savepoint, return a typed disposition;
5. unknown/fatal error → rollback subject writes, release savepoint, return `Err`;
6. a rollback/release error replaces neither the primary error nor the fact that DB integrity is now uncertain; return a Store failure carrying the primary code as bounded diagnostic context.

Do not expose this enum through MCP or persist Rust enum debug text.

## 4. Domain transaction separation

`reconcile_automations_once` should no longer wrap all domains in one transaction.

Use one Store job/writer turn if useful, but a distinct transaction per independent domain:

```text
review/script dispatch transaction
WorkDispatch transaction
publication transaction
Goal progression transaction
GitHub projection transaction
```

A domain result is returned as:

```json
{
  "status": "progressed|idle|degraded",
  "processed": 4,
  "pending": 1,
  "quarantined": 1,
  "cursor": 123,
  "errors": [{"code":"...","subject":"bounded-id"}]
}
```

Rules:

- known subject quarantine degrades only that domain;
- a broken domain-global state/cursor returns a domain error and leaves its transaction unchanged;
- other domains are still attempted after a **semantic/domain-local** error;
- SQLite/connection/commit uncertainty aborts the outer Store call; do not continue writing through a questionable connection;
- post-commit GitHub/script/native effects run only for Operations actually committed by their domain;
- one domain cannot report `progressed` merely because it was invoked.

If executing multiple transactions inside one kernel job makes error provenance or commit behavior unclear, use separate `Store::run` calls. Do not invent a cross-transaction “all succeeded” receipt.

## 5. Migrate current domains, not an unused framework

The shared vocabulary is accepted only with all named current callers migrated in this PR.

### 5.1. Review and ScriptRun dispatch

In `automation_dispatch`:

- isolate each automation entry;
- inside an entry, isolate each selected source fact before mutation;
- malformed source identity/provenance that can never become valid → `Quarantined`;
- unavailable current manager/source proof that can later appear → `Pending`;
- superseded/not-selected/not-current facts → `Skipped`;
- Store failures remain fatal;
- review and script sub-consumers report independently so a damaged script fact does not suppress review dispatch for the same entry.

Do not advance the source cursor until pending or quarantine evidence is durable.

### 5.2. WorkDispatch

In `automation_work_dispatch`:

- malformed enabled entry does not hide later entries; record entry quarantine by exact meta key/digest;
- `AUTOMATION_WORK_SOURCE_GAP` is not automatically equivalent to a permanent skip: classify missing/transient lineage separately from malformed immutable fact;
- `AUTOMATION_WORK_NOT_READY` must be verified before being classified stale; if prerequisites can appear without a new source fact, retain `Pending`;
- replace `mem::take` pending recheck with item-local mutation or restore-on-error structure;
- cursor advances only after `Admitted`, durable `Pending`, `Skipped` or durable `Quarantined`.

R34 does not change launch settings or semantic slot identity.

### 5.3. Publication

Keep the useful structure of `consume_event_isolated`, but:

- use the common disposition type;
- apply the same classifier to new-event and pending-recheck paths;
- a damaged exact retained event/operation cannot be retried forever;
- transient Forge preparation/slot holds remain pending;
- a no-longer-current accepted candidate is skipped;
- Store/commit failures are fatal.

Do not repeat a publish whose external outcome is unknown.

### 5.4. Goal progression

- distinguish source gap, source damage, native state still unsettled and terminal invalid evidence;
- `GOAL_NATIVE_STATE_UNRESOLVED` / sending predecessor is pending, not a permanent terminal slot;
- damaged source evidence may quarantine one fact, not the complete Goal progression domain;
- global slot identity and continuation admission remain exact;
- no generic retry of native input.

R15/R41 owns quota facts; R34 does not classify provider quota.

### 5.5. GitHub projection

- isolate each retained projection fact and each pending recheck;
- rejected/cancelled/unknown effect states use the same explicit state classification as publication;
- a malformed projection cannot block unrelated repositories/entries;
- no projection cursor moves past an unrecorded transient;
- GitHub API/transport unknown remains readback, not replay.

### 5.6. Review disposition

Move `event_identity` and all derived writes inside the subject-isolation boundary.

- malformed immutable review event → quarantine exact observation;
- missing but potentially late prerequisite → pending;
- superseded/historical/no-longer-applicable result → skipped;
- current valid result → applied;
- subject rollback must not abort later results or other entries;
- pending retry mutation must preserve the pending item on all errors.

R09/#35 owns assignment replacement and late-result applicability. R21/#47 owns CorrectionPackage. R22/#48 owns requirement evidence. R34 only makes their consumer failure handling truthful and isolated.

## 6. Scheduler/worker split with R12

R12/#38 remains the owner of:

- independent interval/calendar/goal due-source invocation in `automation_scheduler::call`;
- no-progress/backoff in `swarm-automation` worker;
- uncertain scheduler admission readback;
- Store-owned issuance cursor/backoff and queue fairness.

R34 owns:

- poison automation entry/fact isolation inside automation domains;
- one result vocabulary for applied/pending/skipped/quarantined;
- per-domain transaction separation;
- no false global success.

The scheduler receipt should be able to carry the R34 per-domain degraded summary without treating it as an IPC failure. R12 must not reclassify a R34 quarantine as retryable scheduler failure.

## 7. Donor findings: exact reuse and exact rejection

### 7.1. Internal publication path — primary implementation donor

`automation_publication::consume_event_isolated` already demonstrates the useful core:

```text
explicit pending codes
explicit skip codes
SAVEPOINT around effect reservation
unknown error propagates
```

Reuse and generalize this shape only while migrating live callers. Do not preserve its divergence between new-event and recheck paths.

### 7.2. Restate Rust SDK — explicit terminal class, not infinite retry

The current Restate Rust SDK has a distinct `TerminalError`; ordinary failures retry, terminal failures stop retrying. The useful lesson is that retryability is an explicit type/decision, not inferred from message text.

Do **not** copy:

- infinite retry as ELIOT default;
- Restate server/runtime;
- durable closure replay;
- terminal error as permission to delete source evidence.

ELIOT additionally needs `Pending`, `Skipped` and `Quarantined`, because a retained controller fact is not merely a failed RPC invocation.

Source:

- <https://github.com/restatedev/sdk-rust/blob/06b2ed39846bf2728e134c871cafec208ca3d967/src/errors.rs>

### 7.3. Ractor — local supervision isolation, not actor-runtime adoption

Ractor production users reported that implicit parent stop/child handling caused a real teardown bug; the maintainer recommends explicit child stop-and-wait and treats kill as last resort. The relevant lesson here is failure containment and explicit lifecycle, not importing an actor framework.

A broken optional automation consumer should not terminate unrelated domains or masquerade as whole-host failure.

Source:

- <https://github.com/slawlor/ractor/issues/398>

### 7.4. Goose scheduler — anti-pattern for truthful outcome

Goose field reports show mid-stream failures recorded as successful scheduled completion. R34 must avoid the symmetric mistake: “other domains ran” is not success for a degraded domain, while one quarantined subject is not whole-pass infrastructure failure.

Source:

- <https://github.com/aaif-goose/goose/issues/11051>

## 8. Exact public-path tests

Add tests through `Store::reconcile_automations_once` / scheduler-facing public Store path, not helper-only tests.

### 8.1. Cross-domain isolation

1. Seed valid WorkDispatch and publication facts.
2. Seed one damaged review/script fact.
3. Run one automation pass.
4. Assert:
   - damaged subject has a durable quarantine/disposition;
   - WorkDispatch/publication commit their own cursor/Operation;
   - result reports one degraded domain, not global success;
   - second pass creates no duplicate Operation/effect.

### 8.2. Entry isolation

- malformed automation entry sorts before a valid entry;
- valid entry is still processed;
- malformed entry is not reparsed on every wake without a bounded occurrence update;
- deleting/fixing the entry has an explicit recovery path; no silent “clean” status.

### 8.3. Subject savepoint

- subject writes one partial row then fails with a classified damage code;
- partial row is absent;
- quarantine and cursor are committed;
- following subject applies successfully.

### 8.4. Pending versus quarantine

- prerequisite temporarily absent, later appears without new source fact → retained pending applies;
- immutable payload digest mismatch → quarantine, no repeated retry;
- stale/superseded candidate → skipped;
- unknown error code → transaction error, no cursor movement.

### 8.5. Storage fault

Inject SQLite/write/commit failure:

- no `degraded` success receipt;
- no cursor/quarantine commit;
- later domains are not written through the failed connection;
- primary Store error is preserved.

### 8.6. Pending mutation safety

Cause an error during the second pending item:

- first processed item follows normal disposition;
- failing item and untouched tail remain retained unless the whole domain rolls back;
- no `mem::take` loss after caught subject error.

### 8.7. Worker integration with R12

With Tokio paused time:

- repeated stale/no-progress pages have bounded delay;
- a R34 `degraded` domain summary does not cause immediate IPC retry;
- real cursor/state progress resets backoff.

## 9. Removal and simplification

After migration remove:

- per-domain savepoint boilerplate replaced by the used helper;
- error-prefix/substr classifiers superseded by exact domain mappings;
- `consume_isolated` implementations that rollback then return every semantic error as fatal;
- pending recheck code that differs from new-event classification;
- the single five-domain transaction in `reconcile_automations_once`;
- comments claiming an isolated consumer when its error still aborts the complete pass.

Do not add a generic public automation error framework, a new workflow DSL or a new table merely to rename existing state.

## 10. Ownership and integration order

- R10/#36: disable/config impact diagnostics. R34 must preserve “disable can succeed despite derived diagnostic damage.”
- R11/#37: event projection shapes. R34 consumes retained events; it does not redefine their payloads.
- R12/#38: scheduler source isolation/worker pacing/issuance fairness.
- R21/#47, R22/#48, R32/#58: domain semantics. Rebase their exact consumer changes onto the R34 disposition seam; do not run parallel writers in the same functions.
- R30/#56: typed GM authority must be used by domain context builders after it lands.
- R33/#59: donor research; no runtime dependency follows automatically from that document.

Implementation order:

1. private disposition/savepoint helper with one migrated publication caller;
2. publication recheck parity;
3. review disposition;
4. WorkDispatch;
5. review/script dispatch;
6. Goal progression;
7. GitHub projection;
8. split domain transactions/result envelope;
9. R12 worker/scheduler integration;
10. scoped Clippy and exact tests.

The helper is not accepted as a standalone “framework” commit without the first production caller in the same change.

## 11. Minimal gate after connected code

Manager runs:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

Then the exact Store/program tests named in §8. Broad workspace/native/model tests remain the final qualification phase.

Current documentation/CI success does not establish fixed reconciliation behavior.
