# R23. Operation read scope: fail-closed object authorization для get/list/delta

**Статус:** implementation handoff. Production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённая проблема

`OPERATION_VISIBILITY_SQL` содержит default-open ветку: любой method, который не попал в перечень исключений и prefixes, виден любому аутентифицированному caller. В неё попадают `swarm.launch`, `agent.*`, `task.dispatch` и будущий новый method, если разработчик не обновил denylist.

Generic `operation.get/list` возвращают raw внутреннюю запись:

```text
caller_id
method/state
Task/Attempt/binding/generation
operation_contract
native_mcp/native_refs
result
timestamps
```

`report.delta` и Operations subscription используют тот же visibility predicate. `get_operation_for_current_manager` защищает current-GM action cards, но native-MCP/workspace/issuance diagnostics добавляет по слабому `operation_reader`, поэтому unrelated Manager получает retained diagnostics.

Это противоречит проектной границе: MCP profile определяет набор методов, но не выдаёт object authority; high-level manager tools не обходят Task/Attempt/Operation scope.

## 2. Важная коррекция: Observer profile не удалять

`observer` — session-fixed frontend surface, а не роль/ACL. По умолчанию `local-observer` может быть связан с **локальным Operator credential**. Поэтому нельзя удалять `operation.get/list` или Operations category из observer profile только из-за object leak: это сломает штатную read-only диагностику локального Operator.

Правильная матрица:

| Credential / relation | Observer frontend method visible? | Object result |
|---|---:|---|
| verified local Operator | да | global Diagnostic grant |
| exact caller / current scoped Manager | да | scoped Receipt grant |
| separate Role::Observer без relation | да | NOT_FOUND / filtered page |
| hidden method by selected profile/live method policy | нет | rejected before target IPC by R24 |

Следовательно:

- профиль можно оставить неизменным;
- Store resolver остаётся единственной object-security boundary;
- `mcp.authorization.allowed_methods` — method membership, не object grant;
- tests обязаны использовать разные credentials при одном profile, чтобы не спутать local Operator и Observer role.

## 3. Результат

Одна provider-neutral функция решает, может ли principal читать exact Operation и какой projection разрешён:

```text
Principal + exact Operation + retained relation
  -> OperationReadGrant
  -> closed projection
  -> operation.get / operation.list / report.delta / subscriptions
```

Unknown method/relationship fail-closed. Добавление нового method в registry не делает его публичным.

No external IAM/CEL/OpenFGA service, ACL table или второй policy DSL.

## 4. Один internal resolver

Добавить небольшой модуль, например:

```text
store/operation_read_scope.rs
```

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum OperationReadLevel {
    Summary,
    Receipt,
    Diagnostic,
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

pub(super) struct OperationReadGrant {
    pub level: OperationReadLevel,
    pub basis: OperationReadBasis,
}

pub(super) fn resolve_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<Option<OperationReadGrant>>;
```

Load exact Operation once into a named internal row. Helpers consume this row instead of re-querying and accepting different shapes.

No branch based on `method NOT IN (...)` may return access.

## 5. Positive grant rules

Evaluate deterministically.

### 5.1 Verified local Operator

`require_local_operator` succeeds:

```text
Diagnostic
```

`role == Operator` alone is insufficient.

### 5.2 Exact caller

`operation.caller_id == principal.client_id`:

```text
Receipt
```

Historical own receipt remains readable after Task completion, subject to current registered/non-disabled principal. Exact caller does not automatically receive unrelated host-global diagnostic cards.

### 5.3 Current Task/Attempt manager or current GM scope

For coherent Task/Attempt tuple:

- load Task and Attempt;
- verify revision/identity;
- exact current Attempt owner or existing `current_manager_has_task_scope`;
- result: `Receipt`.

Task ID without coherent Attempt does not infer authority. Taskless Operation cannot use this branch.

### 5.4 Validated on-behalf link

Reuse:

```text
any_on_behalf_operation_link
on_behalf_visible_to
```

Validate method/action/link identity. Preserve historical effective-manager and transfer rules encoded per link kind. Result: `Receipt`.

Do not create generic `is_manager` fallback.

### 5.5 Directed mailbox receipt

Keep exact sender/recipient/cancellation resolution:

- sender/canceler;
- original recipient from unique settled delivery + digest;
- directed Receipt only;
- no native refs or manager diagnostics.

### 5.6 Retained review/coordination scope

Delegate to existing exact authorizers:

- review;
- coordination thread/contract;
- Concilium;
- integration;
- exact code-scope/participant paths.

They return scoped Receipt, never Diagnostic.

### 5.7 Launch child

Validate exact parent request, Task/Attempt/binding, lease, manifest and automation link. Child inherits at most the parent grant and never broadens it.

Malformed linkage is `INVALID_RECEIPT`, not public fallback.

## 6. Participant path

Current Store intercepts Participant `operation.get` before generic read and uses specialized review/coordination/candidate authorization. Preserve it.

Inventory each Participant method that currently returns raw `operations::get_operation`; route it through the same projection level after its exact domain authorizer. Do not replace current exact checks with a generic Participant grant.

The audit does **not** claim a generic Participant leak.

## 7. Closed projections

### Summary

```text
operation_id
method
state
Task/Attempt IDs only when grant is Task-scoped
created_at_ms
updated_at_ms
```

No caller ID, binding, contract, native refs, result body or diagnostics.

### Receipt

```text
operation_id
method
state
permitted Task/Attempt/binding tuple
created/updated
bounded method-specific result receipt
```

If no closed result projector exists, return Summary plus `result_status="not_projected"`; do not expose raw JSON.

### Diagnostic

Verified local Operator and explicitly authorized current-manager diagnostic paths may include internal binding/native/readback cards, still bounded/redacted.

Raw `get_operation` becomes an internal fact loader.

## 8. get/list/delta use one resolver

### operation.get

```text
load exact Operation
resolve grant
project by level
```

No default-open SQL.

### operation.list

Use a bounded stable scan and exact resolver. Current OFFSET over `(created_at_ms, random operation_id)` is not a stable live cursor.

Before choosing cursor source, prove one-to-one coverage for queued/rejected/coalesced Operations. Preferred order:

1. immutable admission observation ID;
2. explicit monotone `operation_sequence` assigned in the same transaction;
3. minimal schema migration if neither exists.

Separate:

- last scanned;
- last emitted;
- first authorized-but-not-emitted row.

Unauthorized rows may advance the scan cursor with explicit filtered coverage. An authorized row that does not fit the response must not be skipped.

### report.delta / subscriptions

For an observation with `operation_id`, call the same resolver before payload projection.

- invisible linked fact: filter and advance with explicit coverage;
- damaged retained relation: gap/error by current policy;
- one invisible row must not fail the whole page;
- payload projection cannot exceed the Operation grant level;
- mailbox addressed projection remains separate and exact.

## 9. Diagnostic decorators

Refactor `get_operation_for_current_manager` into a projector over an already resolved grant:

```rust
fn project_operation(
    db: &Connection,
    operation: &OperationRow,
    grant: &OperationReadGrant,
) -> Result<Value>;
```

- module/owned-service cards: current exact policy;
- native MCP/workspace/issuance details require `Diagnostic`;
- unrelated Manager gets no diagnostic card;
- optional card corruption becomes bounded diagnostic gap, not visibility escalation;
- delete `operation_reader` as a substitute for manager authority.

## 10. Frontend consistency

- Keep observer/profile method surface unless a separate frontend product decision changes it.
- R24 intersects profile and live `allowed_methods` before target IPC.
- R23 performs object authorization after IPC/direct Store call.
- Same Observer profile with local Operator credential may return global diagnostics; with separate Observer credential it returns no unrelated objects.
- Catalog/help must say method presence does not imply object inventory.
- CLI Manager/GM/Operator commands remain; application may return NOT_FOUND/filtered pages.

## 11. Donors

Use internal exact relation validators:

- Participant candidate projection;
- review/thread/Concilium/integration authorizers;
- current manager Task scope;
- on-behalf links;
- launch-child parent proof.

AgentGateway CEL/OpenFGA/Zanzibar supply the general positive-relation idea but are not dependencies: relations already live transactionally in Store, and an external policy system would not solve projection/cursor correctness.

## 12. Files

Primary:

- new `store/operation_read_scope.rs`;
- `store/mod.rs::{OPERATION_VISIBILITY_SQL,operation_visible_to,timeline_visibility_sql,read}`;
- `store/operations.rs` internal loader and closed projections;
- existing domain authorizers reused;
- MCP docs/tests only where claims/fixtures need correction.

R14 later moves schema/catalog data; do not block R23 on it or implement R14 here.

## 13. Removal list

After migration remove:

- default-open negative method predicate;
- duplicated SQL/Rust visibility semantics;
- raw public Operation projection;
- `operation_reader` diagnostic gate;
- page-wide NOT_FOUND caused by a filtered row;
- unstable OFFSET cursor;
- duplicate domain clauses replaced by exact authorizers.

Do **not** remove Observer tools solely as an object-security fix.

## 14. Criteria

- [ ] Unrelated Manager/Observer cannot read/list/delta direct `swarm.launch`, `agent.send`, `task.dispatch` or unknown future methods.
- [ ] Same observer profile + verified local Operator retains global Diagnostic view.
- [ ] Exact caller retains bounded receipt.
- [ ] Current Task manager/current GM sees exact scoped receipts.
- [ ] Validated on-behalf actor sees its receipt; another manager does not.
- [ ] Directed sender/recipient visibility remains exact.
- [ ] Participant review/coordination/candidate paths preserve specialized scope.
- [ ] Receipt never contains Diagnostic-only fields.
- [ ] get/list/delta use one resolver.
- [ ] Invisible row is filtered without page failure or cursor leak.
- [ ] Unknown new method is fail-closed except explicit positive relation.
- [ ] No external ACL engine/table.

## 15. Implementation order

One manager/worktree. Writers do not run Cargo.

1. Add internal Operation row, grant enum and exact get resolver.
2. Wire closed projectors and diagnostic decorators.
3. Replace list pagination and visibility.
4. Replace delta/subscription linked-event visibility.
5. Add public-boundary fixtures for local Operator, Observer role, exact caller and unrelated Manager using the same profile.
6. Remove old SQL/raw paths.
7. Scoped formatting and Clippy.

Do not merge resolver-only code without get/list/delta callers.

## 16. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad/native tests remain final phase.

## 17. Non-goals

- changing mutation authority;
- new public global Observer inventory;
- external policy service;
- raw DB export;
- rewriting historical Operations;
- implementing frontend extraction;
- changing native adapters;
- removing methods from observer profile as a substitute for Store authorization.
