# R23. Operation read scope: fail-closed object authorization для get/list/delta

**Статус:** implementation handoff. В текущей ветке production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённая утечка

`OPERATION_VISIBILITY_SQL` содержит default-open branch:

```sql
op.method NOT IN (...)
AND op.method NOT LIKE 'coordination.%'
AND op.method NOT LIKE 'concilium.%'
AND op.method NOT LIKE 'review.%'
AND op.method NOT LIKE 'automation.%'
AND op.method NOT LIKE 'script.%'
AND op.method NOT LIKE 'goal.%'
AND op.method NOT LIKE 'hook.%'
AND op.method NOT LIKE 'github.%'
AND op.caller_id != 'eliot-internal-automation-v1'
```

Любой аутентифицированный Manager или Observer поэтому видит generic Operations, включая `swarm.launch`, `agent.*`, `task.dispatch` и будущий новый method, если разработчик не добавил его в exclusion list.

`operation.get` затем возвращает полный `operations::get_operation`:

```text
caller_id
method/state
Task/Attempt/binding/generation
operation_contract
native_mcp parent/phase
native_refs
result
timestamps
```

`get_operation_for_current_manager` добавляет native-MCP, workspace-launch и participant issuance diagnostics по слабому `operation_reader`. `current_manager` защищает только manager-action cards. Следовательно, unrelated Manager получает не только факт существования Operation, но и retained native/workspace diagnostics.

`operation.list` использует тот же predicate и возвращает full Operations. `report.delta`/Operations subscription используют `timeline_visibility_sql`, внутри которого тот же predicate; linked observation payload также раскрывается.

Это противоречит документации:

- MCP profiles narrow tools but do not replace application object authorization;
- manager high-level tools do not bypass Task/Attempt/Operation authority;
- Participant gets only exact current/historical assignment projections;
- global diagnostics reserved to verified local Operator.

## 2. Результат

Одна provider-neutral функция решает **может ли principal читать exact Operation и на каком уровне**. `operation.get`, `operation.list`, linked `report.delta`/subscriptions и diagnostic decorators используют один resolver.

```text
Principal + exact Operation + retained relation
  -> OperationReadGrant
  -> one closed projection
  -> get / list / delta
```

Unknown method/relationship is fail-closed. Добавление нового method в registry не делает его автоматически публичным.

No external IAM service, CEL/OpenFGA policy engine, ACL table or second authorization DSL.

## 3. Не переписывать Participant path

Current Participant handling уже отделено до generic `read()`:

- Concilium/review/thread/integration Operations проходят свои retained-scope authorizers;
- current attempt owner/producer получает только bounded candidate-origin projection для `source.capture`/`agent.result`;
- own coordination Operation требует exact current Task/Attempt/binding/generation;
- full manager/native caller fields остаются private.

Сохранить эти specialized paths. R23 не должен заменить их generic raw Operation projection.

Однако current participant branch несколько раз возвращает `operations::get_operation` после specialized authorization. Это допустимо только для closed coordination/review receipts whose public fields are explicitly intended. В рамках R23 inventory all such methods and use the same projection level enum; raw full projection is never an accidental default.

## 4. Один internal resolver

Добавить небольшой module, например:

```text
store/operation_read_scope.rs
```

Private types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperationReadLevel {
    Summary,
    Receipt,
    Diagnostic,
}

pub(super) struct OperationReadGrant {
    pub level: OperationReadLevel,
    pub basis: OperationReadBasis,
}

pub(super) enum OperationReadBasis {
    LocalOperator,
    ExactCaller,
    CurrentTaskManager,
    CurrentGmTaskScope,
    OnBehalfLink,
    DirectedDelivery,
    RetainedReviewScope,
    RetainedCoordinationScope,
    LaunchChildParent,
}
```

`basis` is diagnostics/audit metadata returned only where appropriate; it is not a new public grant or persisted authority.

Main API:

```rust
pub(super) fn resolve_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<Option<OperationReadGrant>>;
```

One exact Operation row is loaded once into a named struct. Helpers consume this row rather than independently re-querying and accepting different shapes.

No default branch that returns `Some` based on method absence from a denylist.

## 5. Positive authorization rules

Evaluate in deterministic order.

### 5.1 Local Operator

Verified bootstrap local Operator:

```text
Diagnostic
```

Call `require_local_operator`, not `role == Operator` alone.

### 5.2 Exact caller

Exact `operation.caller_id == principal.client_id`:

```text
Receipt
```

Historical own receipt remains readable after Task completion or role handover, subject to current registered/non-disabled principal from `current_principal`.

Do not automatically expose separate host-global diagnostics merely because the caller owns the Operation. Diagnostic details are added only if the exact method/receipt owns those facts or the principal also has a diagnostic grant.

### 5.3 Current Task/Attempt manager

For an Operation carrying exact Task/Attempt:

- load Task/project and Attempt;
- require tuple coherence;
- current unreleased Attempt owner == principal, or existing `current_manager_has_task_scope` returns true under the exact project;
- result:

```text
Receipt
```

For a current GM whose authority covers exact Task/project, `Receipt`; method-specific manager action cards may use `Diagnostic` only when current existing policy explicitly requires current-GM authority.

Task ID without coherent Attempt does not infer scope from Task name alone. Taskless Operation cannot use this branch.

### 5.4 Validated on-behalf operation

Use existing `any_on_behalf_operation_link` + `on_behalf_visible_to`.

- validate method/action/link identity exactly;
- preserve historical effective-manager behavior encoded per link kind;
- use `Receipt`;
- only current GM/project branch provided by existing validated link policy may inherit successor visibility.

Do not duplicate link parsing or add generic `Ok(true)` for Review outside its exact `belongs_to/current GM scope` path.

### 5.5 Directed mailbox receipt

Keep existing sender/recipient/cancellation resolution:

- exact sender/canceler;
- exact original recipient from unique settled delivery + digest;
- `Receipt` projection limited to directed delivery fields;
- no native refs/manager diagnostics.

### 5.6 Retained review/coordination scopes

Use existing exact authorizers:

- `reviews::authorize_operation_read`;
- `coordination_threads::authorize_operation_read`;
- `concilium::authorize_operation_read`;
- `integration::authorize_operation_read`;
- contract/code-scope exact own/historical paths.

These return a scoped receipt projection, not generic diagnostic access.

### 5.7 Launch child inheritance

Current `launch_child_parent` validates exact parent request ID, prerequisite, Task/Attempt/binding, lease, manifest and automation link. Child gets at most the grant resolved for its exact parent, narrowed to `Receipt`; no child broadens parent visibility.

Malformed retained linkage is `INVALID_RECEIPT`, not invisible public data.

### 5.8 Observer

Observer has no Task/Attempt ownership or caller relation by role alone. Therefore generic `operation.get/list` does not expose Operations solely because the method is read-only.

First slice:

- remove `operation.get` and `operation.list` from the Observer MCP allowlist and deferred Observer catalog;
- remove Operations subscription category from Observer unless another exact scoped requirement grants it;
- Observer keeps aggregate `swarm.dashboard`, `host.status`, bounded public reports whose projection is independently authorized.

Do not create an arbitrary `PublicOperation` method allowlist in this PR. A future public-summary feature needs a named consumer and closed data contract.

## 6. Projection levels

### Summary

For future bounded lists or manager dashboards only:

```text
operation_id
method
state
Task/Attempt IDs only when grant basis is task-scoped
created_at_ms / updated_at_ms
```

No caller ID, binding, contract, native refs, result body or diagnostics.

### Receipt

Closed method-independent envelope:

```text
operation_id
method
state
Task/Attempt/binding tuple allowed by grant
created/updated
bounded result receipt
```

The result is method-specific and bounded. Existing raw `get_operation` is an internal fact loader, not the public Receipt projection.

For methods without a closed result projector, `operation.get` returns Summary plus `result_status="not_projected"`, not raw JSON.

### Diagnostic

Local Operator global view and explicitly authorized current-manager diagnostics:

- internal caller/binding/contract/native refs;
- manager action/readback cards;
- bounded corruption/gap markers.

Even Diagnostic must respect existing byte bounds and secret redaction; it is not raw DB export.

## 7. Replace SQL denylist with candidate query + exact resolver

`OPERATION_VISIBILITY_SQL` currently tries to be both candidate selector and authority. Split responsibilities.

### 7.1 operation.get

```text
load exact operation
resolve_operation_read
project by level
```

No preliminary default-open SQL.

### 7.2 operation.list

Do not materialize all Operations. Use a bounded candidate scan:

```text
scan immutable admission/order position
  -> resolve exact grant
  -> project selected rows
  -> stop on item/byte bound
```

Current OFFSET over `(created_at_ms, random operation_id)` is not a reliable live cursor. Do not silently reinterpret its integer `after` as a new sequence.

For R23 use a versioned cursor over a stable Store position. Preferred options in order:

1. a retained immutable admission observation ID proven one-to-one with Operation;
2. a small explicit monotone `operation_sequence` assigned inside the same Store transaction;
3. if neither is true on current schema, add the minimal column/index migration rather than timestamp+UUID or OFFSET.

Before choosing, prove writer coverage for rejected/coalesced/queued Operations. One Operation must have exactly one list position.

Separate:

- last scanned;
- last emitted;
- first authorized but not emitted due byte/item limit.

Unauthorized rows can advance scan cursor with explicit filtered count; an authorized row not emitted cannot be skipped.

### 7.3 report.delta / Operations subscription

Observation visibility must call the same resolver for linked `operation_id` before payload projection. SQL may use a safe over-approximation for bounded I/O, but final authority is the resolver.

One invisible linked observation is filtered, not a page-wide `NOT_FOUND`. The cursor advances through filtered rows with explicit `filtered_items/coverage`; Store/corruption error remains error/gap.

Do not expose the raw observation payload if its Operation grants only Summary. Add method/family-specific safe projection or an ID-only resync reference.

Mailbox-specific delivery visibility remains its exact addressed projection.

## 8. Current-manager diagnostics

Change `get_operation_for_current_manager` into projection decorators over an already resolved grant:

```rust
fn project_operation(
    db: &Connection,
    operation: &OperationRow,
    grant: &OperationReadGrant,
) -> Result<Value>;
```

Rules:

- `owned_service_*`, module recovery/outcome cards: existing current GM/exact manager policy;
- native MCP/workspace/participant issuance diagnostics require `Diagnostic`, not generic `Receipt`;
- unrelated Manager never receives these cards;
- corrupt optional diagnostic produces bounded diagnostic gap, not visibility escalation;
- missing linked binding may degrade that card without hiding the base authorized receipt where current policy permits.

Delete `operation_reader` as a substitute for manager authority.

## 9. Frontend policy

Update together:

- `swarm-contracts::method_policy` stays method-class only; do not encode object ACL there;
- `swarm-mcp::profiles` removes Observer Operation tools;
- MCP catalog audiences and `mcp.authorization` agree;
- Tasks projection checks the actual profile and application read grant;
- Operations subscription admission reflects scoped availability;
- CLI `operation get/list` remains available to Manager/GM/Operator, but application may return NOT_FOUND/filtered page;
- documentation profile tables updated.

No hard-coded duplicate method list in Store and MCP for object semantics. Frontend list only controls method visibility; Store resolver controls objects.

## 10. Donors and what not to copy

### Internal exact-scope authorizers

Primary donors:

- `participant_operation_get` and its candidate-origin projections;
- `reviews/coordination_threads/concilium/integration::authorize_operation_read`;
- `current_manager_has_task_scope`;
- `any_on_behalf_operation_link/on_behalf_visible_to`;
- `launch_child_parent`.

Use their exact retained identities; do not create a broad common `is_manager=true` shortcut.

### AgentGateway CEL / OpenFGA / Zanzibar-style systems

Useful concept: positive relation-based authorization and deny-by-default. Not useful as dependency here:

- all required relations already live transactionally in SQLite Store;
- an external policy language would duplicate Task/Attempt/link semantics;
- list/report still needs exact bounded projection and cursor logic;
- no multi-service authorization consumer has been named.

R23 therefore uses a typed local resolver. No external service/crate.

### MCP profiles

Profile allowlist is defense-in-depth only. It does not prove object scope and must not be used to compensate for Store default-open behavior.

## 11. Audit corrections / bounded claims

- Participant generic leak is **not** claimed: Store routes Participant `operation.get` through specialized authorization before generic read.
- `on_behalf_visible_to` is not globally `Ok(true)`: Review returns true only after exact link `belongs_to(principal)`; non-owner paths use current GM scope and link-kind checks.
- Message sender/recipient visibility is already intentionally addressed and should remain.
- The confirmed leak is generic Manager/Observer default-open plus diagnostic decoration, not every specialized family.

## 12. Files/symbols

Primary:

- new small `store/operation_read_scope.rs`;
- `store/mod.rs::{OPERATION_VISIBILITY_SQL,operation_visible_to,timeline_visibility_sql,read}`;
- `store/operations.rs::{get_operation,get_operation_for_current_manager}` refactored into internal load + closed projector;
- existing authorizers in automation/reviews/coordination families reused, not duplicated;
- `swarm-mcp/src/mcp/profiles.rs`, catalog/docs/subscription admission;
- focused Store/MCP public-entry tests.

R14/#40 later moves common method/schema data. Do not block the security fix on frontend crate extraction, and do not rebuild R14 inside R23.

## 13. Removal list

After migration remove:

- default-open negative method predicate;
- `operation_visible_to` duplicate SQL/Rust semantics;
- raw public `get_operation` projection;
- `operation_reader` diagnostic gate;
- Observer Operation tools/category without object scope;
- page-wide NOT_FOUND on a filtered report row;
- OFFSET/timestamp+UUID list cursor once versioned position is active;
- duplicated family visibility clauses that the resolver now delegates to exact authorizers.

No compatibility union or hidden fallback to old visibility.

## 14. Criteria

### Security

- [ ] Unrelated Manager cannot read/list/delta a direct `swarm.launch`, `agent.send`, `task.dispatch` or future new method.
- [ ] Observer cannot call generic Operation methods/category in restricted profile or direct application role.
- [ ] Exact caller retains own receipt after Task completion.
- [ ] Current Task manager/current GM sees only authorized Task-scoped receipts.
- [ ] Automation effective manager sees validated on-behalf receipt; another manager does not.
- [ ] Directed sender/recipient visibility remains exact.
- [ ] Participant candidate/review/thread/concilium paths retain bounded specialized projections.
- [ ] Local verified Operator retains global diagnostic view.
- [ ] Unknown new method is invisible except exact caller/operator/current explicit relation.

### Projection

- [ ] Receipt never contains diagnostic-only fields.
- [ ] List and delta use same resolver as get.
- [ ] Invisible row is filtered without page failure or cursor leak.
- [ ] Byte/item bound does not skip first authorized unreturned row.
- [ ] Corrupt link/operation is an explicit bounded gap/error, not public fallback.

### Simplification

- [ ] One object resolver, no default-open denylist.
- [ ] No external ACL engine/table.
- [ ] No raw Operation JSON exposed by generic frontend.
- [ ] No duplicate object policy in MCP profile.

## 15. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add `OperationRow`, grant enum and exact get resolver; wire `operation.get`.
2. Refactor closed projections and diagnostic decorators.
3. Replace list candidate/pagination with versioned stable cursor and same resolver.
4. Replace delta/subscription linked-event visibility.
5. Narrow Observer profile/catalog/admission.
6. Remove old SQL/raw projection paths.
7. Scoped formatting and minimal Clippy.

Do not merge a resolver type without get/list/delta callers.

## 16. Minimal gate

After complete code:

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad tests/native execution remain final phase. Focused security tests should enter through Store/MCP public methods and verify absence of leaked fields, not only helper booleans.

PR report names base/head SHA, exact grant rules, removed default-open branches/raw projectors, stable cursor choice, Clippy result and unrun live qualification.

## 17. Non-goals

- new global Observer inventory;
- external policy service/DSL;
- changing Task/Attempt ownership;
- changing mutation authorization;
- hiding directed mailbox receipts from legitimate recipient;
- rewriting historical Operations;
- exposing raw DB rows to simplify debugging;
- implementing R14 frontend extraction;
- changing native adapter protocols.
