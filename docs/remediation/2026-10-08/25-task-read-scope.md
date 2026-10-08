# R25. Task graph read scope: Task/Attempt/submission/acceptance/check/family

**Статус:** implementation handoff. Production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

Generic Store `read()` вызывает без `Principal`:

```text
tasks::get_task
tasks::list
tasks::get_attempt
submissions::describe
acceptance::describe
checks::describe
producers::family
```

Participant/AssignedReviewer частично перехватываются exact guards до generic read. Manager и Role::Observer идут в principal-free path.

Это противоречит documented object boundary: MCP profile задаёт методную поверхность, но не выдаёт Task ownership; `assignment-read` не должен автоматически давать global Task inventory.

## 2. Важная коррекция: observer frontend сохранить

`observer` — frontend profile. Default `local-observer` может использовать verified local Operator credential, которому глобальная read-only диагностика разрешена.

Поэтому R25 **не удаляет Task/Attempt/evidence methods из observer profile** как security fix.

Один и тот же profile должен вести себя по credential/relation:

| Credential | Результат |
|---|---|
| verified local Operator | global bounded diagnostic view |
| current scoped Manager/GM | exact Task graph |
| current Participant / assigned reviewer | exact assignment graph |
| separate Role::Observer без relation | NOT_FOUND / filtered page |

R24 проверяет current method membership до IPC. R25 остаётся object authorization в Store.

## 3. Scope R25

R25 покрывает один coherent graph:

```text
Task
Attempt
Task submission
Task acceptance
CheckRun
Agent family / producer set
```

Не входят:

- Operation reads — R23/#49;
- artifact metadata/bytes/parts/assemble — R26/#52;
- fleet-wide binding/route monitoring;
- mutation authority;
- requirement/check semantics — R22/#48.

## 4. One exact identity

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskGraphIdentity {
    pub project_id: String,
    pub task_id: String,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub binding_id: Option<String>,
    pub binding_generation: Option<i64>,
}
```

Identity derives from retained object, not request JSON.

Object loaders:

```rust
identity_for_task
identity_for_attempt
identity_for_submission
identity_for_acceptance
identity_for_check
identity_for_family
```

Each validates internal coherence:

- Attempt points to existing Task/revision;
- submission document, producer Operation and candidate agree;
- acceptance decision and accepted Task pointers agree;
- CheckRun spec/Operation/Attempt/candidate agree;
- binding/family belongs to exact Attempt and producer assignments.

## 5. One read resolver

```rust
pub(super) enum TaskReadLevel {
    Summary,
    Detail,
    Evidence,
}

pub(super) enum TaskReadBasis {
    LocalOperator,
    CurrentTaskManager,
    CurrentGmProjectScope,
    CurrentParticipant,
    RetainedAssignedReviewer,
    ExactObjectCaller,
}

pub(super) fn resolve_task_read(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
    requested: TaskReadLevel,
) -> Result<Option<TaskReadGrant>>;
```

### Local Operator

`require_local_operator` → global Evidence.

### Current Task manager/current GM

Use exact Task/project/Attempt coherence plus existing `current_manager_has_task_scope`/Attempt control.

Manager role alone is not sufficient.

### Current Participant

Use current registration/basis:

- exact Task/Attempt only;
- own candidate/submission/check/family through exact producer relation;
- no sibling/other Attempt inventory.

### Assigned reviewer

Reuse exact review assignment scope:

- Task/Attempt/submission/candidate;
- exact linked CheckRuns/family evidence needed by review;
- retained history only where assignment contract explicitly permits.

### Exact object caller

Check/submission/acceptance caller may retain its own bounded receipt. This does not grant the rest of Task graph.

## 6. Closed projections

### Summary

```text
task_id
project_id only when grant permits
revision/state/phase
current Attempt/accepted status only when in scope
```

### Detail

Exact frozen spec/brief, current Attempt and permitted pointers for authorized manager/participant/reviewer.

### Evidence

Exact submission/check/acceptance/family receipts. Still bounded/redacted; never raw internal JSON.

`tasks::get_task/get_attempt` become internal loaders.

## 7. Method wiring

### task.get

Derive identity from stored Task, resolve grant, return Summary/Detail.

### task.list

Use bounded stable scan and resolver. No OFFSET over mutable list.

Separate:

- last scanned;
- last emitted;
- first authorized-but-not-emitted row.

Filtered rows may advance cursor with explicit coverage. One damaged foreign row must not wedge authorized page or be mislabeled as filtering.

### attempt.get

Exact Attempt + Task identity, current/historical relation as above.

### task.submission

Derive identity from immutable submission document + producing Operation + candidate. Participant/reviewer specialized guard remains; Manager uses Task grant; Observer role without relation receives NOT_FOUND.

### task.acceptance

Bind exact Task/Attempt/candidate and accepted pointers. Current manager/GM/local Operator or explicit retained evidence relation only.

### check.get

Check row + check.run Operation + exact Attempt/candidate/profile. Current Task scope, assigned review evidence or exact caller only.

### agent.family

Binding/generation → exact Attempt/Task. Return bounded partial-aware family only to local Operator or exact current manager/participant/reviewer relation.

R25 does not change `agent.state/list`; fleet monitoring needs separate explicit design.

## 8. Frontend consistency

- Keep observer profile method surface for local Operator diagnostics.
- Store resolver differentiates Operator credential from Role::Observer.
- R24 live method membership runs before target IPC but does not replace object authorization.
- Catalog/help must state that method visibility is not global Task inventory.
- Direct IPC and MCP use the same Store resolver.

Tests use the same observer profile with two credentials:

```text
local Operator → authorized global diagnostic
separate Observer role → unrelated object NOT_FOUND / filtered
```

## 9. Pagination/gaps

For list-like methods:

```text
stable source position
→ bounded candidates
→ exact grant
→ bounded projection
→ explicit filtered/damaged coverage
```

Do not reveal invisible object through total count or cursor side channel beyond bounded filtered coverage. SQL/storage failure remains error.

## 10. Artifact boundary

Task projections may return artifact refs only when subsequent artifact authorization can derive the same TaskGraphIdentity. R26 owns artifact bytes/metadata and reuses this type.

R25 does not infer artifact authority from Task ID alone.

## 11. Audit corrections

- Participant generic leak is not claimed where Store already intercepts exact review/candidate evidence.
- `agent.state/list` are outside this slice.
- Observer profile method presence is intentional for local Operator compatibility; global object access for every Observer credential is not.
- This fix enforces the project’s documented object scope; it does not invent a stronger confidentiality model.

## 12. Donors

Use existing internal exact authorizers:

- review evidence scope;
- participant candidate/submission/check authorization;
- current manager Task scope;
- frozen Task/Attempt validation;
- CheckRunner candidate identity.

No external IAM/graph service/table.

## 13. Files

- new `store/task_read_scope.rs`;
- `store/mod.rs::read` Task/Attempt/evidence/family arms;
- `store/tasks.rs` internal loaders, projections, stable list scan;
- `store/submissions.rs::describe`;
- `store/acceptance.rs::describe`;
- `store/checks.rs::describe`;
- `store/producers.rs::family`;
- MCP docs/tests only where claims/fixtures change.

Coordinate with R23/R24/R26 frontend edits through one integration owner.

## 14. Removal list

After migration remove:

- principal-free public Task graph handlers;
- raw Task/Attempt projection;
- OFFSET Task pagination;
- duplicated Participant/reviewer identity derivation replaced by TaskGraphIdentity;
- tests that verify only shape, not unrelated-principal denial.

Do **not** remove observer methods solely as object security.

## 15. Criteria

- [ ] Same observer profile + local Operator can inspect global bounded Task diagnostics.
- [ ] Same profile + unrelated Role::Observer cannot get/list exact Task/Attempt/evidence.
- [ ] Current manager/GM reads exact authorized Task graph.
- [ ] Participant sees exact current Task/Attempt and own evidence only.
- [ ] Assigned reviewer sees exact assignment evidence and permitted history.
- [ ] Exact object caller reads own bounded receipt, not Task inventory.
- [ ] Submission/acceptance/check/family share TaskGraphIdentity derivation.
- [ ] Stable list cursor does not skip authorized rows.
- [ ] Damaged foreign object does not leak or wedge healthy page.
- [ ] Direct IPC and MCP decisions match.

## 16. Implementation order

1. Add TaskGraphIdentity/resolver; wire task.get/attempt.get.
2. Replace task.list pagination.
3. Wire submission/acceptance/check/family.
4. Add Store/MCP fixtures with local Operator vs Observer role under same profile.
5. Remove raw/principal-free paths.
6. Rebase R22/R09/R26 consumers.
7. Scoped formatting and Clippy.

One manager/worktree; writers do not run Cargo. Do not merge resolver-only code without all production readers.

## 17. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad/native tests remain final phase.

## 18. Non-goals

- Operation authorization;
- artifact authorization;
- fleet monitoring redesign;
- mutation authority changes;
- external IAM;
- public Task inventory feature;
- historical data rewriting;
- requirement/acceptance semantics;
- removing observer profile methods as a substitute for Store scope.
