# R03b. Codex: provisional thread candidate → exact verified root

**PR #29 · companion implementation handoff · production-код ещё не изменён.**

**Source baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`. Этот блок меняет тот же `crates/swarm-adapter-codex/src/lib.rs`, что и [R03 steer](../2026-10-07/03-codex-steer.md); отдельный PR/manager не создавать. Upstream SHA ниже — координата source review, не version pin.

## 1. Результат

`thread/start` response становится активным `journal.state.native_root_id` только после exact проверки:

```text
server identity/scope
thread ID
model provider
model
canonical workspace
```

Thread, созданный с другой конфигурацией, сохраняется как **provisional root candidate** exact `agent.open` Operation. Он:

- не становится current native root;
- не принимает `task.dispatch`/`agent.send`/goal;
- блокирует второй `agent.open` в том же binding generation;
- разрешается только exact `agent.reconcile` readback либо explicit binding retirement/replacement;
- не удаляется/архивируется автоматически.

Один binding generation не создаёт второй root, пока outcome первого `thread/start` не доказан.

## 2. Подтверждённый дефект

Current `open_operation`:

1. сохраняет open intent;
2. вызывает `thread/start`;
3. извлекает returned thread ID/provider/model/cwd;
4. **до** вычисления/проверки `config_exact` пишет:

```rust
journal.state.native_root_id = Some(thread_id);
journal.state.effective_model_provider = ...;
journal.state.effective_model = ...;
record.native_root_id = Some(thread_id);
```

5. сохраняет checkpoint;
6. только потом при mismatch возвращает `EffectOutcome::Unknown` / `THREAD_CONFIGURATION_MISMATCH`.

После этого:

- новый open получает `THREAD_ALREADY_OPEN`;
- send видит adopted root, затем read-thread preflight отвергает configuration mismatch;
- `reconcile_open` читает тот же mismatched root и возвращает Unknown;
- clear/replacement path отсутствует.

В native thread input не отправляется, поэтому это availability/root-ownership defect, а не доказанная prompt injection. Но binding permanently wedged and projects an unverified root as its sole root identity.

## 3. Audit correction: что нельзя сделать

Нельзя исправлять это так:

```text
configuration mismatch
→ clear all identity
→ immediately call thread/start again
```

Первый `thread/start` уже мог создать native thread. Повтор создаст второй root.

Также не нужно автоматически вызывать `thread/delete` или `thread/archive`:

- это новый внешний effect;
- failure/readback/idempotency потребуют отдельной Operation;
- returned/mismatched identity нельзя молча уничтожать под видом validation cleanup;
- operator may need the retained thread for diagnosis.

Безопасный default — hold provisional candidate and require exact reconcile or binding retirement.

## 4. Не добавлять новый checkpoint subsystem

Существующий `OperationRecord` уже хранит:

```text
method/kind/state
requested provider/model/workspace
native_scope_key
native_root_id
outcome
```

Unknown `agent.open` records остаются в live operation map; terminal Applied/Rejected records later compact to tombstones. Этого достаточно.

Добавить один private classifier over bounded existing state, например:

```rust
enum OpenSlot<'a> {
    Vacant,
    Pending {
        operation_id: &'a str,
        record: &'a OperationRecord,
    },
    Candidate {
        operation_id: &'a str,
        root_id: &'a str,
        record: &'a OperationRecord,
    },
    Active {
        root_id: &'a str,
    },
}

fn classify_open_slot(state: &Checkpoint) -> Result<OpenSlot<'_>, AdapterError>;
```

Rules:

- at most one non-rejected `agent.open` intent without adopted root;
- `Candidate` = open record has returned root, global root is absent;
- `Active` = exact global root and consistent open record;
- two live candidates, candidate + different active root, or Applied open without matching global root = damaged checkpoint;
- Rejected-before-native open does not occupy slot;
- Unknown/pending open does occupy slot;
- do not infer slot from arbitrary result JSON text.

No extra table/file or generic state-machine framework.

## 5. Initial open algorithm

### 5.1 Admission

Order:

1. exact same operation ID → existing outcome/readback, no native call;
2. `classify_open_slot`;
3. `Pending|Candidate|Active` under a different operation ID → no `thread/start`;
4. only `Vacant` may create a new open intent.

Return a precise no-effect diagnostic for an occupied slot, including only safe existing operation/root references. Do not return `THREAD_ALREADY_OPEN` for a candidate as though it were verified active.

Suggested distinction:

```text
OPEN_SLOT_PENDING
OPEN_ROOT_CANDIDATE_UNVERIFIED
THREAD_ALREADY_OPEN
```

These are adapter diagnostics, not new public methods.

### 5.2 Effect and response

Persist intent before `thread/start` exactly as today.

After response:

1. require nonempty thread ID;
2. read observed provider/model/cwd;
3. canonicalize requested and observed workspace once;
4. compute `config_exact` **before mutating global root fields**.

#### Exact response

In one checkpoint update:

```text
record.native_root_id = returned thread
journal.state.native_root_id = returned thread
journal.state.effective_model_provider/model = observed exact values
```

Then save and return Applied.

#### Mismatched response

In one checkpoint update:

```text
record.native_root_id = returned thread candidate
journal.state.native_root_id remains None
effective global config remains unset
```

Return Unknown with bounded exact observed configuration and diagnostic `THREAD_CONFIGURATION_MISMATCH`.

The returned thread ID remains bound to this open Operation for reconciliation. No send/read path may treat it as active.

#### Missing thread ID / ambiguous response

Save no candidate root. The pending open intent still occupies the slot because effect may have occurred; return Unknown. Do not retry `thread/start`.

### 5.3 Checkpoint save failure

A failure after native response remains Unknown. Do not convert it to Rejected/no-effect. Preserve returned identity in the immediate outcome when available, but do not claim it was durably adopted.

## 6. Exact root promotion helper

Add one private helper shared by initial exact response and reconcile:

```rust
fn adopt_verified_root(
    journal: &mut Journal,
    operation_id: &str,
    observed: &VerifiedThreadConfiguration,
) -> Result<(), AdapterError>;
```

`VerifiedThreadConfiguration` is a small private typed value:

```rust
struct VerifiedThreadConfiguration {
    root_id: String,
    native_scope_key: String,
    model_provider: String,
    model: String,
    workspace_root: String,
}
```

Constructor performs exact nonempty/bounded/path validation. The adoption helper CAS-checks:

- exact open operation;
- record candidate root absent or same;
- no different active root;
- requested provider/model/workspace equal observed;
- current attached server scope equals retained scope;
- open slot still belongs to the same operation.

Only this helper writes global root/effective configuration. Remove direct assignments elsewhere.

## 7. `reconcile_open`

Current function treats `record.native_root_id` as the already adopted root. Change semantics: it may be provisional.

Algorithm:

1. load exact target open record/receipt;
2. require retained native scope and requested configuration;
3. require candidate root if response provided one; if absent, no blind list/latest-thread selection;
4. attach to exact server identity/scope;
5. `thread/read(candidate_root)`;
6. compare thread ID/provider/model/canonical cwd;
7. exact → call `adopt_verified_root`, save checkpoint, return Applied;
8. mismatch/not-found/transport uncertainty → keep candidate/pending slot and return Unknown;
9. never call `thread/start` from reconcile.

A later exact read may promote the same candidate. A persistent mismatch remains visible for operator/binding retirement.

Do not clear candidate merely because one read returns not-found: current contract has not established that this proves the original start effect never existed across server/storage transitions.

## 8. Send/goal/control precondition

Every root-using command requires:

```text
classify_open_slot == Active
command.native_root_id == active root
requested route == retained requested configuration
current thread/read == exact active configuration
```

A candidate root cannot be supplied manually in `command.native_root_id` to bypass adoption.

R03 steer then removes only the full-history `active_turns.page_limited` prerequisite. It does not weaken root configuration proof.

R43/#66 Codex Goal controller must use the same `Active` classifier; no separate «root present» predicate.

R15/#41 continuous reader/account projection may attach to the app-server without an agent root, but it must not project a provisional candidate as the binding's live session.

## 9. Outcome/readback details

Keep three facts separate:

```text
requested_configuration
observed_candidate_configuration
adopted_active_configuration
```

For mismatch Unknown include:

```json
{
  "completion_condition": null,
  "diagnostic_code": "THREAD_CONFIGURATION_MISMATCH",
  "native_root_state": "candidate_unverified",
  "candidate_thread": {
    "id": "...",
    "model_provider": "...",
    "model": "...",
    "cwd": "...",
    "workspace_status": "workspace_exact|workspace_mismatch"
  },
  "native_replay": false
}
```

Do not label `effective_model_status=thread_configuration_verified` until adoption helper succeeds.

Manager attention/readback may recommend:

```text
agent.reconcile exact open Operation
or retire/replace binding
```

It must not recommend resend of `agent.open`.

## 10. No automatic destructive cleanup

Current upstream app-server exposes thread archive/delete methods, but this PR does not call them. A future cleanup Operation would need:

- exact current authority;
- exact candidate root and server identity;
- explicit effect intent;
- unknown-outcome readback;
- policy for retaining diagnosis/history.

Do not hide that new effect inside `agent.open` validation.

## 11. Delete after migration

- assignments to `journal.state.native_root_id` before configuration verification;
- global root presence as the only open-slot classifier;
- `effective_model_provider/model` writes from mismatched response;
- generic `THREAD_ALREADY_OPEN` for pending/candidate states;
- any local retry/fallback `thread/start` after Unknown/mismatch;
- duplicated configuration comparison outside the typed constructor/adoption helper;
- any goal/send path that accepts candidate root by string equality alone.

Historical operation outcomes remain readable.

## 12. Required public-path tests

### Initial open

1. Exact ID/provider/model/cwd → one thread/start, Active root, Applied.
2. Wrong provider → Unknown candidate, global root absent.
3. Wrong model → same.
4. Wrong cwd after canonicalization → same.
5. Missing thread ID → Unknown pending, no active/candidate ID invented.
6. Same operation retry → no second thread/start, byte-equivalent retained outcome.
7. Different open while Pending/Candidate → no native request.

### Reconcile/restart

8. Candidate persists through checkpoint restart and still fences new open.
9. Candidate later reads exact → promoted once to Active.
10. Candidate still mismatched → remains Unknown; no root promotion.
11. Candidate read not-found/transport error → remains held; no replay.
12. Two candidate open records or candidate + different global root → checkpoint damage, not arbitrary first selection.

### Downstream safety

13. `task.dispatch`, `agent.send`, Goal and result commands reject candidate root before native write.
14. Exact active root continues through R03 steer without full history preflight.
15. Account/quota read works independently and does not publish candidate as live binding root.
16. No thread/archive/delete request occurs in any mismatch/reconcile fixture.

Tests use a recorded/fake app-server transcript at the real adapter command boundary, not only `classify_open_slot` unit tests.

## 13. Implementation order and conflicts

One manager/worktree for PR #29:

1. add open-slot classifier and configuration type;
2. move global adoption after exact verification;
3. make reconcile promote only exact candidate;
4. gate all root-using commands through Active;
5. implement R03 steer preflight simplification;
6. remove duplicated/old predicates;
7. scoped gate and transcript fixtures.

Cross-PR:

- #41 touches Codex connection/read-pump/account paths; use the same server identity but do not move root adoption there.
- #66 uses active Codex root for Goal; rebase after classifier.
- #46 later changes Task prompt bytes only.
- #65 may change checkpoint durability mechanics; do not turn candidate mismatch into a storage fallback.

## 14. Gate

After connected code:

```sh
cargo clippy --locked -p swarm-adapter-codex --lib --bins -- -D warnings
```

Then exact adapter transcript fixtures; broad/live app-server qualification is final phase. Submission records base/head SHA, classifier/adoption callers, deleted predicates, scoped gate and unqualified live cases.

This document does not change code, app-server threads, subscription credentials, models or working sessions.