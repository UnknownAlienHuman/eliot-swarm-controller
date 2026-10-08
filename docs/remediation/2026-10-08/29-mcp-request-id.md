# R29. MCP caller-owned request ID: schema и runtime используют один predicate

**Статус:** implementation handoff. Production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main` и rebase после владельца того же `ProfiledFacade::call_tool` seam.

## 1. Результат

Для операций, где потерянный первый ответ нельзя безопасно восстановить без заранее известного логического ID, MCP facade требует caller-owned `client_request_id` **до target IPC** во всех профилях, включая Full.

Первый подтверждённый набор:

```text
swarm.launch
schedule.run_now
```

Один private predicate определяет одновременно:

- required JSON schema;
- runtime before-IPC validation;
- отсутствие UUID generation в нижнем facade;
- tests/catalog assertions.

Остальные Full-profile mutations пока сохраняют документированную compatibility-семантику: при omission локальный facade генерирует ID и возвращает его только если ответ получен. R29 не расширяет scope до глобального removal этой совместимости.

## 2. Нормативная семантика

`client_request_id` — caller-owned logical operation identity, а не server-generated receipt number.

Нужный порядок:

```text
caller chooses ID
→ caller can persist it locally
→ facade validates it
→ Store creates/reads Operation by caller + ID
→ external/local effect may begin
→ lost transport reply
→ caller retries exact same ID
→ Store returns retained receipt, no replay
```

Если server генерирует ID только внутри dispatch:

```text
server generates ID
→ effect commits
→ response is lost
→ caller never learns ID
→ exact readback/retry identity is unavailable
```

Возвращение generated ID в успешно полученном результате не исправляет lost-response boundary.

## 3. Текущие определения расходятся

### 3.1 Tool description

`schedule.run_now` описан как:

```text
Supply the same client_request_id to read back this manual invocation.
```

Store действительно использует caller ID для deterministic manual CheckRun identity.

### 3.2 Input schema

`input_schema` уже делает `client_request_id` required, когда:

```rust
require_request_id
|| spec.method == "swarm.launch"
|| spec.method == "schedule.run_now"
```

Его description также говорит `Choose it before dispatch` для обоих методов.

### 3.3 Profiled runtime guard

`ProfiledFacade::call_tool` проверяет request ID только при:

```rust
(self.profile != McpToolProfile::Full || spec.method == "swarm.launch")
```

`schedule.run_now` здесь отсутствует.

### 3.4 Inner facade

`McpFacade::call` для любой mutation без ID выполняет:

```rust
let id = Uuid::new_v4().to_string();
params["client_request_id"] = id;
```

и добавляет ID в полученный result. Если ProfiledFacade не остановил вызов, Full-profile `schedule.run_now` достигает Store с server-generated identity.

Итого:

```text
advertised schema: request ID required
runtime call guard: omission accepted in Full
inner effect path: ID generated after caller request
```

Это подтверждённый producer/consumer drift одного поля.

## 4. Почему `schedule.run_now` требует pre-known ID

`Store::schedule_run_now`:

1. требует `client_request_id`;
2. включает его в `invocation_identity`;
3. выводит deterministic internal `check_request_id` через `manual_run_now_check_request_id(manager, client_request_id, project, automation)`;
4. до current target checks ищет existing `check.run` Operation по этому ID;
5. exact retry возвращает retained receipt;
6. differing retained invocation identity конфликтует;
7. только при отсутствии prior Operation может быть допущен CheckRun.

Таким образом, Store path корректен **при условии, что caller знает ID до первого вызова**. MCP Full нарушает именно это предусловие.

`swarm.launch` уже имеет special runtime guard по той же причине. R29 не создаёт новую концепцию, а устраняет drift между двумя уже объявленными effect-sensitive methods.

## 5. Audit correction

Исходное замечание «`schedule.run_now` всегда молча генерирует UUID» слишком широкое.

- Restricted profiles уже требуют caller ID перед IPC.
- Advertised Full schema тоже делает поле required; schema-conforming client его передаст.
- Дефект проявляется, если Full-profile caller вызывает tool без поля: server-side runtime guard слабее своей schema и допускает вызов.

Система не должна полагаться на добровольное соблюдение advertised JSON schema. Handler обязан повторить load-bearing effect invariant до IPC.

## 6. Один private predicate

Добавить рядом с schema/facade helpers:

```rust
fn mutation_requires_caller_request_id(
    profile: McpToolProfile,
    method: &str,
    read_only: bool,
) -> bool {
    !read_only
        && (profile != McpToolProfile::Full
            || matches!(method, "swarm.launch" | "schedule.run_now"))
}
```

Либо разделить data-only части:

```rust
fn effect_requires_preknown_request_id(method: &str) -> bool;
fn profile_requires_request_id(profile: McpToolProfile, method: &str, read_only: bool) -> bool;
```

Выбрать минимальную форму, которая реально используется минимум schema builder и runtime guard. Не создавать public policy framework или новый contracts crate type ради двух методов.

## 7. Подключение predicate

### 7.1 Input schema

`input_schema` сейчас принимает boolean `require_request_id`. Чтобы не держать special methods вторым условием, передавать уже вычисленное exact значение либо переименовать параметр:

```rust
fn input_schema(
    spec: &ToolSpec,
    read_only: bool,
    request_id_required: bool,
) -> Arc<JsonObject>;
```

Внутри:

- добавить property `client_request_id` для mutations;
- `required.push` только по exact boolean;
- описание `Choose it before dispatch` только когда boolean true;
- compatibility description только когда false.

`tool_from_spec` должен получать profile/effective requirement, а не повторно угадывать method exceptions.

### 7.2 Full/base tool list

`McpFacade::list_tools` представляет local Full compatibility surface. Для каждого tool вычислить:

```rust
mutation_requires_caller_request_id(McpToolProfile::Full, spec.method, read_only)
```

Не передавать безусловный `false` и затем снова добавлять special methods внутри schema.

### 7.3 Catalog pages/search

`catalog::list_tools_page` и `search_catalog` строят tools через `tool_from_spec`. Использовать тот же profile-aware predicate. Catalog tool schema и direct `tools/list` обязаны быть byte-consistent для одинакового profile/method.

Не кэшировать authorization в этом PR; R14/#40 владеет schema reuse, R24/#50 — live allowed_methods.

### 7.4 Profiled dispatch

`ProfiledFacade::call_tool`:

```rust
if mutation_requires_caller_request_id(self.profile, spec.method, *read_only) {
    require_caller_request_id(&arguments)?;
}
```

Validation остаётся before inner facade/IPC. Error remains invalid params/method surface semantics; no target Operation.

### 7.5 Inner defense

`McpFacade::call` знает method/read_only, но не profile. Добавить fail-closed defense for the exact effect-sensitive set:

```rust
if !read_only
    && effect_requires_preknown_request_id(method)
    && params.get("client_request_id").is_none()
{
    return tool_error(Error::invalid(
        "this mutation requires caller-owned client_request_id before dispatch"
    ));
}
```

После этого generated ID path применяется только к Full-compatible methods, где omission действительно разрешён.

Это защищает tests/direct construction of `McpFacade`, future wrapper mistakes и any route that bypasses ProfiledFacade. Не полагаться только на schema or one outer wrapper.

## 8. ID validation

`require_caller_request_id` сейчас проверяет nonempty string. Не вводить здесь новый UUID-only contract: Store accepts caller-owned bounded logical IDs, and other clients may use deterministic strings.

Но runtime/schema должны совпасть с existing Store validation:

- nonempty;
- existing maximum and character policy from canonical mutation validator;
- no trimming/rewrite after caller chooses identity.

Если current MCP schema describes merely `{type:string}`, do not invent a narrower frontend-only grammar in R29. R14 may later reuse canonical descriptors.

## 9. What not to change

Do not modify `Store::schedule_run_now` algorithm. It already:

- resolves lost replies before current target checks;
- derives deterministic internal ID;
- verifies retained invocation identity;
- rechecks current Manager and exact automation target before new admission;
- prevents duplicate CheckRun on exact retry.

Do not replace it with MCP JSON-RPC request ID: protocol frame IDs identify one transport request, not durable logical effects and are not guaranteed reusable by caller after reconnect.

Do not use MCP task ID as the pre-effect request identity: task handle is produced from the Operation **after** admission.

## 10. Tests

Tests must use public MCP handler/facade paths and record forwarded calls; helper-only assertions are insufficient.

### T1. Full schema

For Full profile:

- `swarm.launch` requires `client_request_id`;
- `schedule.run_now` requires `client_request_id`;
- one ordinary compatibility mutation does not require it, documenting retained behavior.

### T2. Restricted schema

For Manager/restricted profile every mutation requires `client_request_id`; reads do not.

### T3. Full runtime denial before IPC

Invoke `schedule_run_now` without ID through public Full `tools/call`:

- exact invalid-params error;
- zero inner/IPC `schedule.run_now` calls;
- zero Store Operations/CheckRuns;
- no generated ID in error or state.

Repeat for `swarm.launch` to ensure one predicate covers both.

### T4. Inner defense

Construct/test base `McpFacade` path without profile wrapper:

- effect-sensitive missing ID rejected;
- target Client.request not called;
- ordinary compatibility mutation may still generate and echo ID.

### T5. Caller ID preserved

Provide deterministic non-UUID ID:

```text
manual-run/project-X/revision-7
```

Assert exact bytes reach Store; no normalization/replacement.

### T6. Lost reply/readback identity

At Store integration boundary:

1. send `schedule.run_now` with known ID;
2. drop/ignore first facade response after Store commit;
3. reconnect/retry same ID/payload;
4. same CheckRun/Operation receipt returned;
5. no second CheckRun.

The test need not execute external check process; admission identity is the target.

### T7. Changed payload conflict

Reuse same ID with another project/automation. Store returns `REQUEST_ID_CONFLICT`; facade does not generate a replacement ID.

### T8. Catalog/direct schema equality

For each profile, compare `schedule.run_now` tool schema from normal `tools/list` and `swarm.tools.search`/catalog path. Required field semantics must match.

## 11. Files

Primary:

- `crates/swarm-mcp/src/mcp/mod.rs`
  - request-ID predicate;
  - `input_schema` / `tool_from_spec` callers;
  - `McpFacade::list_tools`;
  - `McpFacade::call` inner defense;
  - `ProfiledFacade::call_tool`.
- `crates/swarm-mcp/src/mcp/catalog.rs`
  - pass exact profile-aware requirement without duplicating rules.
- MCP frontend/catalog integration tests.

Read-only verification:

- `crates/swarm-kernel-host/src/store/schedule_run_now.rs`;
- `crates/swarm-kernel-host/src/store/schedule_run_now_tests.rs`;
- `crates/swarm-kernel-host/src/model.rs` mutation validator.

## 12. Cross-PR ownership

R24/#50 also changes `ProfiledFacade::call_tool` for live Store authorization. Avoid parallel writers:

1. R24 should stabilize first or one manager rebases/combines the exact seam.
2. Final order in `call_tool`:

```text
method exists
→ session profile exposes method
→ current Store allowed_methods permits method (R24)
→ caller-owned request ID requirement (R29)
→ target IPC
```

Both live denial and request-ID denial occur before target IPC. Error precedence fixtures must cover both. Do not make authorization revision an idempotency token.

R14/#40 later extracts/reuses schemas; it consumes this single predicate rather than copying exceptions. R23/#49 object authorization and R28/#54 Task dispatch receipts are unrelated.

## 13. Deletion list

After implementation remove:

- duplicated `spec.method == "swarm.launch" || spec.method == "schedule.run_now"` checks inside `input_schema`;
- ProfiledFacade's narrower one-off `spec.method == "swarm.launch"` condition;
- any test that asserts schema only without proving zero IPC;
- comments implying Full can omit ID for **every** mutation.

Do not add another method-name list in catalog/tests.

## 14. Minimal gate

After code:

```sh
cargo clippy --locked \
  -p swarm-mcp \
  --lib --bins -- -D warnings
```

Then exact MCP frontend/catalog tests. Kernel-host Clippy is unnecessary unless Store code changes; broad/native qualification later.

## 15. Non-goals

- remove Full compatibility for all mutations;
- change Store Operation idempotency;
- use transport request IDs as effect IDs;
- generate IDs in model prompts;
- retry mutations automatically;
- change schedules or execute checks;
- add UUID-only restriction;
- create public request-ID policy service;
- cache live authorization;
- modify credentials, routes or running MCP sessions.
