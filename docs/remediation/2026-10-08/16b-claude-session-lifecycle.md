# R16b. Claude: terminal harness lifecycle, current readback и один result intent

**PR #42 · companion implementation handoff · production-код ещё не изменён.**

**Source baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`. Этот блок дополняет [R16](../2026-10-07/16-claude-interactions.md) и меняет те же Claude-файлы; отдельный manager/PR не создавать. Release numbers в исходниках — наблюдение существующего artifact, не allowlist пользовательской установки.

## 1. Точный результат

Одна Claude module instance владеет не более чем одним Node SDK harness generation. После terminal harness event она:

1. сохраняет `Unknown` для всех intents без outcome;
2. больше не выдаёт cached state как текущий native readback;
3. не принимает новый prompt, reply, refresh, reconcile или result против умершего root;
4. закрывает свой input owner и завершает module instance;
5. оставляет module/process-family departure и replacement общим R01/R35 механизмам.

Новая native сессия не создаётся внутри старого module boot. Replacement допускается только после Store/supervisor lifecycle и подтверждённого ухода прежней process family. Никакого повторного `agent.open` или prompt по локальной эвристике.

`agent.result` после потерянного/неполного `module.result` ACK сохраняет один `Unknown` outcome поверх уже записанного intent. Он не пытается второй раз записать intent и не падает с `ADAPTER_INTENT_EXISTS`.

## 2. Audit correction: что является дефектом

Фраза «Claude session нельзя повторно открыть в том же adapter process» сама по себе задаёт неверную цель. После неясного SDK startup/session termination безопасный путь — retire module instance, а не запускать второй root рядом с потенциально живым native descendant.

Подтверждены четыре конкретные проблемы.

### 2.1. Dead harness остаётся активным объектом

`run_owned` хранит отдельно:

```text
Option<NativeHarness>
harness_alive
native_prepared
latest_state
session_root
```

`harness_ended`, direct `Exited` и `ReadFailure` меняют первые два boolean и очищают pending control, но не:

- `take()` old harness input owner;
- очищают current `session_root`;
- инвалидируют `latest_state` как current;
- завершают module instance.

`agent.open` затем всегда видит `harness.is_some()` и отвечает `SESSION_ALREADY_OPEN`, а loop продолжает обслуживать другие commands.

### 2.2. Refresh/reconcile используют stale cache

`handle_refresh` не получает liveness state. Если retained root/boot/scope совпадают с `latest_state`, он возвращает `native_observation_available:true` и `Applied`, даже когда Node/SDK stream уже завершён.

`handle_reconcile` также ищет exact execution в cached `latest_state` без отдельной current-liveness проверки и может поставить `resolved:true` после terminal harness event.

Исторически наблюдённый факт допустим как historical evidence, но не как текущая native observation и не как доказательство, что session/root остаётся живым.

### 2.3. Result Unknown повторно пишет intent

`handle_result` до `link.result(...)` вызывает:

```rust
journal.write_intent(...)?;
```

При malformed/non-recorded ACK или transport error он вызывает `save_result_unknown`, который строит тот же intent и снова вызывает `journal.write_intent(...)`.

`OperationJournal::write_intent` намеренно отклоняет любую существующую запись как `ADAPTER_INTENT_EXISTS`. Поэтому именно Unknown path, который должен сохранить неопределённость, завершает adapter ошибкой до `save_outcome`.

### 2.4. Node startup failure оставляет zombie session object

`bridge.mjs::prepare` создаёт `sess`, присваивает `session = sess`, затем вызывает fallible `prepareQuery`. Catch отправляет `operation_unknown` и возвращается, но `session` остаётся `ended:false`. Следующая подготовка получает `SESSION_ALREADY_OPEN`.

Поскольку SDK startup уже был вызван, такой catch нельзя превращать в safe local retry. Это unknown native-start boundary; bridge generation должен завершиться.

## 3. Не писать новую supervisor framework

Использовать существующие границы:

| Нужная гарантия | Существующий владелец |
|---|---|
| Module/worker boot, process family, replacement only after departure | R01 / PR #27, `swarm-supervisor`, `swarm-process` |
| Bounded child termination/capture primitives | R35 / PR #61 |
| Claude Operation intent/outcome/outbox | `OperationJournal` в этом adapter |
| Host candidate/result provenance | R05 / PR #31 |
| Torn-tail/durable file primitive | R41 / PR #65 |

R16b не создаёт nested process registry, second Store, actor runtime или generic adapter SDK. Node child уже наследует managed module owner group. При terminal harness failure Claude adapter завершает module instance; общий owner доказывает уход всей группы до replacement.

## 4. Минимальная private lifecycle model

Заменить независимые booleans/Options одним Claude-specific owner object. Возможная форма, имя не является обязательным API:

```rust
enum HarnessPhase {
    NotStarted,
    Preparing { open_operation_id: String },
    Prepared { open_operation_id: String },
    Live { root_id: String },
    Departed { last_root_id: Option<String>, diagnostic_code: String },
}

struct ClaudeHarnessLifecycle {
    process: Option<NativeHarness>,
    phase: HarnessPhase,
    current_snapshot: Option<Value>,
}
```

Обязательные методы должны быть маленькими и предметными:

```rust
fn can_prepare(&self) -> bool;
fn current_snapshot(&self, boot_id: &str, root_id: &str) -> Option<&Value>;
fn mark_prepared(...);
fn adopt_root(...);
fn mark_departed(...);
fn take_process(&mut self) -> Option<NativeHarness>;
```

Не добавлять universal lifecycle DSL. `Value` можно оставить только на SDK transport boundary; phase/identity не должны оставаться набором несвязанных JSON/boolean полей.

## 5. Exact transition table

| Current | Event | Next / действие |
|---|---|---|
| `NotStarted` | local validation/import rejected before SDK startup | `NotStarted`; exact Operation rejected; corrected new open может быть рассмотрен Store |
| `NotStarted` | `agent.open` intent + child spawn + prepare sent | `Preparing`; intent уже durable |
| `Preparing` | exact `prepared` frame | `Prepared`; open outcome Applied/rootless |
| `Preparing` | SDK startup unknown / pipe uncertainty | `Departed`; save Unknown; terminate bridge/module instance; no local retry |
| `Prepared` | first exact SDK init/root adoption | `Live{root}` |
| `Prepared|Live` | `harness_ended`, child exit, frame read failure | `Departed`; unresolved intents Unknown; current snapshot invalidated; module instance terminates |
| `Live` | exact current frame | replace bounded current snapshot |
| `Departed` | any native command | no effect; module is terminating/not ready |

`harness_ended` and direct child exit may both arrive. Terminal transition must be idempotent and retain the first exact diagnostic plus any later direct exit code as supplementary evidence, not execute cleanup twice.

## 6. Owner loop changes

### 6.1. `handle_frame` returns disposition, not overloaded bool

Current boolean means «re-open ModuleLink», while terminal harness events also return true. Replace it with a small private enum:

```rust
enum FrameDisposition {
    Continue,
    Rehello,
    RetireModule,
}
```

- root adoption → `Rehello`;
- ordinary frame → `Continue`;
- harness terminal/read failure/direct exit → `RetireModule`.

`run_owned` on `RetireModule`:

1. marks unresolved intents Unknown once;
2. drops/takes harness stdin owner;
3. stops calling `module.next`;
4. makes one best-effort bounded outbox flush if host link is usable; local journal remains source of recovery if it is not;
5. returns a typed adapter termination error/status so outer module owner handles departure/replacement.

Do not reconnect with `native_ready:false` and continue serving stale root commands.

### 6.2. Current state invalidation

At terminal transition:

```text
current_snapshot = None
current root authority = None
phase = Departed(last_root, reason)
```

A bounded historical diagnostic may retain:

```text
last_root_id
last_snapshot_digest
last_native_event_count
terminal diagnostic/direct exit code
```

It is not returned as `native_observation_available:true`.

## 7. Node bridge startup boundary

Refactor `prepare` into explicit local phases without assigning global `session` before success.

### Before SDK invocation

Path/schema/import/required-interface errors:

- emit `operation_rejected`;
- close local resources;
- global `session` remains null;
- no native effect claimed.

### After calling `prepareQuery` / SDK startup

Any thrown error is effect-ambiguous unless official API proves otherwise:

- emit exactly one `operation_unknown` for the open Operation;
- close `sess.input` and any acquired query/prepared handles best-effort;
- mark local session ended;
- close readline/stdin loop or otherwise terminate the Node bridge process;
- do not set global session back to reusable idle state;
- Rust receives terminal frame/exit and retires module instance.

Do not `process.exit()` before the Unknown frame is flushed. Do not wait forever for SDK cleanup; common process-family owner handles descendants.

On successful `prepareQuery`, assign the completed `sess` to global `session` and emit `prepared`.

## 8. Refresh semantics

`agent.refresh` is a read Operation, but its successful execution must not imply live native state.

### Live phase

Only `HarnessPhase::Live` with exact boot/root/scope can produce:

```text
native_observation_available=true
snapshot_freshness=current
native_session_state=live
```

### Prepared/rootless phase

A root-specific refresh is rejected as identity mismatch/not-ready. Prepared diagnostics belong to open outcome/describe, not a fabricated session snapshot.

### Departed/unknown phase

The module instance retires before accepting new commands. A retained historical readback, if later needed, comes from Store/journal with explicit:

```text
snapshot_freshness=historical
native_session_state=departed|unknown
native_observation_available=false
```

Do not call `compact_refresh_observation` after current snapshot invalidation.

## 9. Reconcile semantics

Order:

1. exact target journal receipt and binding generation;
2. already saved target outcome;
3. exact **current** SDK observation only when phase is Live and boot/root/input all match;
4. otherwise unresolved with explicit readback-unavailable reason.

A cached event from a departed phase is not current readback. Exact immutable evidence already persisted in the target outcome/result page remains usable through that persisted record.

`agent.reconcile` itself may return Applied to mean «readback attempt recorded», but:

```text
resolved=false
native_readback=current_sdk_observation_unavailable
session_state=departed|unknown
```

must remain truthful. It cannot resolve an Operation solely from stale in-memory `latest_state`.

## 10. `agent.result`: intent once, outcome once

Refactor result flow:

```text
validate target/result
→ build receipt/native intent
→ write_intent exactly once
→ call module.result exactly once
→ recorded exact artifact: save Applied
→ malformed/lost ACK: save Unknown over existing intent
```

Replace `save_result_unknown` with a helper that **requires** the existing exact state:

```rust
fn save_result_unknown_after_intent(
    journal: &OperationJournal,
    command: &RuntimeCommand,
    expected_receipt: &ModuleReceiptIdentity,
    ...
) -> Result<bool>;
```

It:

1. loads `journal.get(operation_id)`;
2. requires exact receipt/method/target identity;
3. requires no previous outcome;
4. appends only `Unknown` outcome;
5. never calls `write_intent`;
6. returns whether ModuleLink reconnect is useful.

A lost `module.result` response may mean Store already persisted the artifact. Unknown is retained; adapter does not submit the page again. Later host-side exact artifact/readback may resolve it. R05 owns candidate scope/provenance, not this adapter outcome transition.

## 11. Pending permissions/questions on harness death

R16 live callback requests are process-local. On terminal lifecycle:

- mark every live callback request `callback_lost`/cancelled in the bounded observation;
- remove resolver/listener once;
- late `agent.reply` rejects with exact stale boot/request fingerprint;
- do not replay a permission decision into a replacement session.

Official durable-defer requests may survive only when the selected SDK/session contract proves persistence and resume. They are not converted from live callbacks automatically.

## 12. Delete after migration

- independent `harness_alive`, `native_prepared`, `session_root`, `latest_state` authority checks;
- boolean `handle_frame` disposition;
- continuing `module.next` after terminal harness event;
- stale-cache refresh/reconcile path;
- second `journal.write_intent` in result Unknown path;
- global Node `session` assignment before successful preparation;
- `SESSION_ALREADY_OPEN` zombie after startup failure;
- any same-process retry of an effect-ambiguous SDK startup.

Historical journal records and exact result artifacts remain readable.

## 13. Required tests

### Node fixture tests

1. Path/import failure before SDK call → one rejected frame; session remains unassigned.
2. `prepareQuery` throws after invocation → one Unknown frame, bridge terminates, no second prepare accepted.
3. Normal prepared → initial input → result → harness end transition exactly once.
4. Stop/startup race emits no duplicate terminal frames/outcomes.

### Rust adapter tests

1. `harness_ended` invalidates current root/snapshot and causes `RetireModule`; no subsequent `module.next`.
2. direct `Exited` after `harness_ended` is idempotent and does not overwrite first cause.
3. dead cached snapshot cannot produce current refresh.
4. dead cached execution cannot set reconcile `resolved:true`.
5. malformed `module.result` ACK after intent → one Unknown outcome, no `ADAPTER_INTENT_EXISTS`, no page replay.
6. lost ACK with Store-side artifact → host exact readback resolves without second `module.result`.
7. unresolved send/open on harness death becomes Unknown with known boot/root/input identities preserved.
8. late callback reply after lifecycle death rejected without native effect.
9. replacement module is not started until shared owner proves old process-family departure.

Tests must enter through adapter/bridge public loop, not call only the private lifecycle helper.

## 14. Implementation order and ownership

One manager/worktree for PR #42:

1. add private lifecycle/disposition types and public-loop fixtures;
2. fix Node prepare ownership/terminal behavior;
3. make harness terminal retire module instance;
4. remove stale refresh/reconcile currentness;
5. fix result Unknown intent/outcome transition;
6. integrate R16 live/deferred interactions against the same lifecycle;
7. remove old fields/branches;
8. scoped gates.

Conflicts:

- #61/#27 land shared process/departure primitives first or are rebased by the same manager; #42 does not edit those implementations independently.
- #65 may change journal file mechanics; #42 changes semantic calls (`intent once`), not torn-tail storage.
- #31 owns host candidate provenance; #46 later owns compact Task prompt.

## 15. Gate and handoff

After connected production code:

```sh
cargo clippy --locked \
  -p swarm-adapter-claude \
  -p swarm-contracts \
  -p swarm-kernel-host \
  -p swarm-mcp \
  --lib --bins -- -D warnings

node --check crates/swarm-adapter-claude/sdk-harness/bridge.mjs
```

Broad/native subscription qualification remains final phase. Submission records:

```text
base/head SHA
lifecycle type and public callers
removed stale/duplicate paths
Node fixture results
scoped Clippy/node syntax
remaining official-SDK capability gaps
```

Docs CI or this handoff does not qualify the runtime.