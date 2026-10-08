# R13. Resource evidence: честный capacity ledger и exact workspace owner

**Статус:** implementation handoff. Production-код в этой ветке ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Исходные карточки: AUD-016/017/022/031. Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Результат

Capacity projection не уничтожает reservations при повреждении, не принимает чужое execution proof и не двигает phase назад. Workspace lease удерживается только доказательствами, которые могут принадлежать её exact Attempt/resource lineage; новый queued launch той же Task не считается владельцем старого worktree.

```text
Operation / Attempt / Binding / native execution evidence
  → one exact ResourceEvidence derivation
  → capacity projection
  → workspace release predicate
  → reports / attention
```

Это repair достоверности и resource fencing. Оно **не** вводит числовой лимит менеджеров, модель fallback, route scheduler или автоматическое освобождение по TTL.

## 2. Нормативные документы

- [Owner decisions §1.2–1.4, §2.2](../../owner-decisions.md): один manager/worktree, exact evidence, no heuristic kill/restart, authoritative facts не очищаются broad LRU.
- [Launcher assignment context](../../swarm-launcher-assignment-context.md): exact Task/Attempt/lease/binding identity.
- [Communication tool contracts](../../agent-communication-tool-contracts.md): advisory code scope, coverage и explicit release.
- [Audit v3](../../agent-operations/modularity.md) и текущий единый аудит: execution, collaboration и collision domains различны.
- [R15/#41](native usage branch): provider quota evidence; R13 не дублирует её collector или incident classifier.

Исторические SHA — координаты source review, не pin runtime/model/version.

## 3. Что подтверждено кодом

### 3.1 Existing malformed ledger silently becomes empty

`capacity.rs::load_ledger`:

```rust
Some(mut ledger) if ledger.is_object() => {
    if !ledger["entries"].is_object() {
        ledger["entries"] = json!({});
    }
    ledger
}
_ => json!({"scope": null, "entries": {}, "updated_at_ms": 0})
```

Отсутствующий record и существующий malformed/non-object record имеют один результат. Следующий `sync_operation/sync_attempt` сохраняет новый ledger и необратимо уничтожает retained reservations.

`all_ledgers` при malformed JSON, напротив, ошибкой останавливает projection всех scopes. Один и тот же дефект одновременно fail-open на write и fail-closed глобально на read.

### 3.2 Phase machine реально двигается назад

Комментарий `merge_entry` обещает:

```text
A release is final; an entry never moves backwards.
```

Но для любого derived state кроме active/released функция безусловно пишет `phase="reserved"`. Если persisted active evidence позднее стало partial/damaged/missing, sync превращает effective writer обратно в pending admission вместо integrity gap.

### 3.3 Execution proof не связан с operation

`derive_operation` читает `op.native_refs.input_execution` и доверяет `disposition`, `execution_started`, `native_run_id` без проверки `proof.operation_id == op.operation_id` и связанной input/session identity.

Это не только теоретическое замечание: helper `capacity_tests::proof` hard-codes:

```json
{"operation_id":"op-1","native_input_id":"input-op-1"}
```

а test `scopes_are_isolated_per_service` сохраняет этот proof на Operation `op-a`; capacity принимает его как active. Тест закрепляет отсутствие binding check.

### 3.4 Roster verification односторонний

`roster_check` проверяет, что каждая admission Operation и producer имеет соответствующий ledger entry/phase. Он не проверяет обратное: лишний retained ledger entry, для которого больше нет exact authoritative source, продолжает входить в counts.

Scope migration или damaged cleanup может оставить orphan, который увеличивает desired/effective writers, но roster всё ещё объявляется known.

### 3.5 Workspace predicate слишком широкий

`workspace_lifecycle::has_unresolved_native_work` удерживает lease, если существует **любая** queued/sending/native_accepted/outcome_unknown Operation:

- exact lease Operation;
- Attempt start Operation;
- любая Operation той же Attempt;
- любая Operation всей Task;
- любая Operation того же binding/generation.

Плюс `active_binding` повторяет task-wide и attempt-wide queries.

После завершения predecessor Attempt новый `swarm.launch`/другая queued Operation той же Task может удерживать старую lease, хотя ещё никогда не использовала её registration/lease/process resource. Это подтверждённая ложная attribution, а не доказанный live deadlock всей системы.

### 3.6 Capacity сейчас report, не admission policy

`capacity_available` строится только в `capacity_items`, затем читается report/attention/observer. Оно означает «в ledger есть pending admission при known roster/new_work/no quota incident», а не свободный slot и не разрешение создать ещё одного manager.

`capacity_available=false` после перехода reserved→active просто означает `pending_admissions == 0`; это не измерение максимума. В R13 нельзя переименовывать этот факт в route capacity limit либо строить launcher gate на его current meaning.

### 3.7 Подтверждённые границы / снятые подозрения

- OpenCode `execution_started` в рассмотренном producer — `null | EventRef`, не boolean. Audit suspicion про literal `false` не подтверждён рабочим writer. Новый parser всё равно должен отвергать wrong type, а не считать любой non-null start.
- `runtime:service` collision не заявляется дефектом этой поставки: OpenCode service ID уже ограничен безопасным alphabet, большинство других routes получают binding-scoped fallback. Если новый runtime вводит произвольный service namespace, он обязан предоставить structured scope identity, а не расширять строковый key эвристикой.
- Route manager limits/fallback остаются отдельной policy: owner-decisions edition 1 не задаёт числового admission rule.

## 4. Один internal evidence type, не новый framework

В `capacity.rs` добавить private closed representation:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
enum CapacityPhase {
    Reserved,
    Active,
    Released,
}

#[derive(Debug, Clone)]
struct ResourceEvidence {
    entry_id: String,
    kind: ResourceEntryKind,
    operation_id: Option<String>,
    attempt_id: Option<String>,
    assignment_id: Option<String>,
    binding_id: String,
    binding_generation: i64,
    phase: CapacityPhase,
    execution_identity: Option<ExecutionIdentity>,
    release_reason: Option<String>,
    unknown_since_ms: Option<i64>,
}
```

Это private derivation; не переносить его в `swarm-contracts` без второго consumer. JSON остаётся transport/storage boundary, не внутренний алгоритм.

`ExecutionIdentity` содержит только реально проверенные значения:

```text
operation_id
native_session_id
native_input_id
native_run_id?
source observation/event ref
terminal disposition?
```

No default empty strings. Wrong/missing/conflicting identity produces `EvidenceState::Damaged/Unknown`, никогда Released.

## 5. Typed ledger load: Missing ≠ Valid ≠ Damaged

```rust
enum LedgerLoad {
    Missing,
    Valid(CapacityLedger),
    Damaged(CapacityLedgerDamage),
}
```

### Missing

Разрешает создать первый ledger только из exact current source fact.

### Valid

Closed validation:

- schema/version marker;
- exact scope key and structured scope identity;
- entries object bounded;
- key == `entry.entry_id`;
- known kind/phase;
- required identity fields by kind;
- nonnegative timestamps and coherent phase timestamps;
- released entry has release evidence and cannot carry pending/active fields;
- no duplicate source identity under different entry IDs.

### Damaged

- сохранить raw bytes/digest/size as bounded diagnostic, не копировать raw payload в report;
- не переписывать ledger;
- не считать roster/capacity known;
- report/attention возвращает one scoped gap and continues other scopes;
- mutation, к которой capacity является derived projection, **не должна откатываться только из-за malformed capacity ledger**.

Implement one local helper:

```rust
fn record_capacity_projection_gap(
    tx: &Connection,
    scope_key: &str,
    code: &str,
    raw_digest: &str,
    now: i64,
) -> Result<()>;
```

Use existing Incident/Observation primitives with stable dedup; no new API. SQL/storage errors still propagate. Only recognized derived-ledger shape damage is isolated.

`sync_operation/sync_attempt/sync_binding` keep `Result<()>` to avoid changing every business caller: on `Damaged`, record the scoped gap and return `Ok(())` without ledger write. Do not silently swallow database errors.

### Read path

Replace `all_ledgers() -> Result<Vec<(String, Value)>>` with per-row parse:

```text
valid row   → aggregate
malformed   → one damaged ScopeAgg/gap
other rows  → continue
SQL error   → fail request
```

One damaged scope cannot hide healthy scopes.

### Repair/deletion condition

R13 does not add `capacity.rebuild`. Raw damaged record stays retained. Future explicit repair may rebuild only after a bounded complete scan of authoritative rows and compare-and-swap against retained raw digest. Do not auto-delete or overwrite it during ordinary sync.

## 6. Exact execution evidence derivation

One function is used by both sync and roster recheck:

```rust
fn derive_operation_evidence(db: &Connection, op: &Value) -> Result<EvidenceDerivation>;
```

It must not have a weaker read-only twin.

### 6.1 Operation proof

For `native_refs.input_execution`:

- exact object shape for supported schema/revision;
- `proof.operation_id == operation.operation_id`;
- exact binding/generation context belongs to Operation;
- nonempty native session/input IDs;
- when Operation already retains input/session identity, equal values required;
- `execution_started` is null or exact event object; wrong type damaged;
- native_run_id consistent with start event;
- terminal disposition requires exact terminal event/correlation;
- admission-only proof remains Reserved;
- start without terminal is Active;
- exact terminal for same execution is Released;
- uncertainty/recovery_pending never releases.

Provider-specific detailed proof validation may remain in its producer. Capacity validates only the common identity/effect boundary it consumes.

### 6.2 Producer evidence

Require exact producer:

- unique `assignment_id` under exact Attempt;
- Attempt binding/generation matches ledger scope;
- nonempty native session;
- run ID and terminal belong to the same producer;
- malformed/oversized producer list → unknown/damaged, not release.

### 6.3 Precedence

Use one monotone evidence lattice:

```text
Released(exact terminal) > Active(exact start) > Reserved(exact admission) > Unknown/Damaged
```

A lower-quality later observation cannot overwrite a higher-quality retained phase. Conflicting terminal/start identities mark roster unknown; they do not select whichever branch appears first.

`merge_entry` becomes:

```rust
fn merge_entry(entry: &mut CapacityEntry, evidence: &ResourceEvidence) -> MergeResult
```

- Released remains final;
- Active + Reserved stays Active;
- Active + conflicting identity → conflict/gap;
- Reserved + Active promotes once and preserves first activation time;
- exact terminal releases and preserves terminal evidence;
- entry attribution fields cannot be rewritten after release.

## 7. Bidirectional roster verification

Build expected map from authoritative rows for the scope:

```text
entry_id → derived ResourceEvidence
```

Then compare both directions:

1. expected missing from ledger → diverged;
2. phase/identity mismatch → diverged;
3. ledger entry absent from expected → orphan/diverged;
4. duplicate source identity → damaged;
5. unattributed native family activity → unknown as today.

Released retained entries may remain as historical accounting, but they do not count as desired/effective writers and their source identity must remain valid/bounded. If historical source is intentionally no longer queryable, mark coverage partial instead of roster known.

Do not “repair” orphan by deleting it in report code.

## 8. Workspace lease: exact resource attribution

Replace current broad boolean with one predicate returning reasons:

```rust
struct WorkspaceResourceUse {
    unresolved: bool,
    reasons: Vec<ResourceHold>,
}

enum ResourceHold {
    LeaseOperation,
    AttemptStartOperation,
    ExactAttemptOperation { operation_id: String },
    ExactBindingGeneration,
    ExactOwnedServiceStart,
    CheckResource,
    ExactProducer,
    EvidenceGap,
}
```

No `TaskWideOperation` variant.

### 8.1 Operations that may hold this lease

An Operation counts only if one of these is exact:

- `operation_id == lease.operation_id`;
- `operation_id == attempt.start_operation_id`;
- `attempt_id == lease.attempt_id` **and** its binding/generation or launch linkage identifies this lease;
- binding/generation equals the exact Attempt binding and operation belongs to the same Attempt/resource lineage;
- owned service row names exact lease ID/generation or exact Attempt+binding/generation;
- CheckRun claims a resource for this exact Attempt;
- producer belongs to exact Attempt and is nonterminal.

A queued Operation with only `task_id == lease.task_id` does not hold the old resource. A binding for a successor Attempt does not hold predecessor lease merely because Task ID is equal.

### 8.2 State table

| Lease / Attempt | Exact holds | Transition |
|---|---|---|
| held, terminal+released Attempt | none | Released |
| held, terminal+released | exact active/unknown evidence | Stale / active_or_unknown |
| stale originating held | none + exact source evidence complete | Released |
| preparing/outcome_unknown | no proven no-effect/departure | Stale/unknown, never TTL release |
| missing/damaged attribution | EvidenceGap | Stale/unknown |
| successor Task operation only | none for predecessor | does not fence predecessor |

No filesystem deletion or process stop in lifecycle sweep. Release means Store no longer considers the lease an active authority; cleanup remains a separate exact operation.

## 9. Collision domains

Keep workspace lease and advisory code scope separate:

```text
workspace lease = physical mutable worktree/resource owner
code scope      = planned paths/symbols and integration risk
```

### Same mutable worktree

Overlapping writer lease is a hard launch conflict until exact release/departure evidence.

### Different worktrees, same repository/candidate lineage

Potential merge/integration risk. Report to manager; not the same filesystem lock.

### Read-only reviewer

No writer collision.

### Expired advisory scope

Expiry means observation may be stale; it does not prove writer departure. Return unknown/stale coverage, not false clean or permanent physical lock.

R06/#32 owns context and accepted scope identity. R13 owns current active/conflict readers and resource classification; do not reimplement R06 DTO.

## 10. Capacity report terminology

Current `capacity_available` is misleading. In R13 either:

- rename the projection to `pending_admission_recorded`, or
- keep field for wire stability but set an explicit semantic marker:

```json
{
  "capacity_available": true,
  "capacity_semantics": "recorded_pending_admission_not_route_limit"
}
```

Do not use it as launcher authorization. New numerical policy belongs to a separate route-policy PR after R15 produces reliable quota/provider facts and owner policy defines desired limits.

Quota incident no longer resolves from unrelated success under R15. R13 consumes only resulting scoped condition; it does not classify 429 or credits.

## 11. Functions/files

Primary:

- `store/capacity.rs::{scope_facts,load_ledger,all_ledgers,derive_operation,derive_producer,merge_entry,sync_operation,sync_attempt,sync_binding,collect_scopes,roster_check,capacity_items}`;
- `store/capacity_tests.rs` — replace malformed proof fixtures with exact identities and add negative mismatch cases;
- `store/workspace_lifecycle.rs::{transition_for,has_unresolved_native_work,effect_status_for_lease}`;
- `store/workspace.rs::reject_scope_conflicts` only where physical workspace collision semantics are needed;
- `store/code_scopes.rs` current active/conflict readers after R06 rebase.

Narrow callers:

- lifecycle mutation hooks continue calling sync, but derived damage does not abort their primary effect;
- report/attention/doctor consume scoped gaps;
- launcher must not treat current `capacity_available` as a slot gate.

Do not modify adapter execution readers except if they must provide an existing missing common identity field; their detailed native contracts remain adapter-owned.

## 12. Removal list

After migration remove:

- silent malformed→empty normalization;
- Value-based phase branch that permits active→reserved;
- unbound proof reads from `derive_operation`;
- one-way roster comparison;
- task-wide workspace operation clauses;
- duplicate task/attempt/binding queries between `linked_operation` and `active_binding`;
- misleading tests with proof for another operation;
- any new compatibility fallback that writes legacy malformed ledger.

No second capacity ledger, no arbitrary route limits and no TTL cleaner.

## 13. Criteria

### Ledger/evidence

- [ ] Missing ledger initializes; malformed existing ledger remains byte-retained and yields scoped gap.
- [ ] One damaged scope does not hide healthy scopes or rollback unrelated business mutation.
- [ ] Entry key/identity/phase schema validated closed.
- [ ] Active cannot regress to Reserved.
- [ ] Exact terminal releases; terminal for another operation/input/session does not.
- [ ] Test `op-a` with proof `operation_id=op-1` rejects/marks gap.
- [ ] Extra orphan ledger entry makes roster unknown rather than increasing a known count.
- [ ] Released attribution cannot be rewritten by later sync.

### Workspace

- [ ] New queued launch for same Task but another Attempt does not hold predecessor lease.
- [ ] Exact old Attempt/binding/process unknown still holds it.
- [ ] Owned service/check/producer hold only exact resource lineage.
- [ ] Damaged/oversized producer evidence fails closed.
- [ ] No release by age, idle, Task name or absence alone.
- [ ] No filesystem/process side effect in lifecycle reconciliation.

### Reporting/policy

- [ ] Capacity projection names its report-only semantics.
- [ ] No numeric limit/fallback/model change added.
- [ ] Healthy scopes remain visible alongside damaged scope.
- [ ] Shared-worktree conflict and separate-worktree integration risk are distinguishable.

## 14. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add private typed ledger/evidence parser and exact negative fixtures.
2. Replace load/sync/merge behavior; scoped damage projection.
3. Make roster bidirectional.
4. Replace workspace broad predicate with exact holds.
5. Rebase code-scope readers after R06.
6. Update report terminology/docs and remove old paths.
7. Scoped formatting and minimal Clippy.

Do not merge DTO/helpers without connected sync, report and workspace consumers.

## 15. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

If shared contract code is added after implementation review, include only its package. Broad/native/load tests remain final phase.

PR report names base/head SHA, exact ledger/evidence types, removed task-wide predicates, scoped damage handling, Clippy result and remaining live qualification.

## 16. Dependencies / non-goals

Dependencies:

- R06/#32 — current work context/code-scope accept identity;
- R15/#41 — provider quota facts/incidents;
- R01/#27 and R02/#28 — process/owner evidence producers.

Non-goals:

- route concurrency policy;
- automatic model fallback;
- session rotation;
- killing native processes;
- filesystem cleanup;
- rewriting capacity into a second DB/service;
- automatic repair/deletion of damaged ledger;
- global code-scope IAM;
- changing historical lease/evidence records.
