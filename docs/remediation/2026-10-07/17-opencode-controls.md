# R17. OpenCode V2: loop-step input, вопросы, permissions и background без дублирования runtime

**Draft-задание, 8 октября 2026. Production-код этого блока ещё не написан.**

Основа ELIOT: `40591a295af94b1541ec2ba30afe8e3247701a71`. Текущий upstream OpenCode прочитан на `5d9cd9b259f0456522f318a7435501d03cfbee79`. SHA — координаты source review, **не** launch allowlist и не требование удерживать release. Работать с выбранной подписочной/локальной установкой владельца через документированный V2 HTTP; обновления не отключать, отдельный inference API/account не вводить.

## 1. Результат

Standalone `swarm-adapter-opencode` получает один законченный native control slice:

1. точная idempotent admission следующего хода и OpenCode loop-step input;
2. durable readback admission/promotion/terminal через session history;
3. наблюдение и адресный ответ на V2 questions и permissions;
4. background synchronous subagents через current capability + boolean endpoint;
5. один bounded event reader и `module.observe`, а не polling каждого viewer;
6. переключённый production caller и условие удаления соответствующих дублей из `kernel-host/runtime/opencode_v2`.

Не добавлять четвёртую реализацию, CLI-probe, чтение private DB, regex по журналам, fallback на legacy forms или общий vendor-RPC passthrough.

## 2. Нормативные границы

Читать перед кодом:

- `docs/agent_swarm.module-contract-v2.md`: Operation сохраняется до эффекта; native ACK, сохранённый input, terminal и Task acceptance — разные ступени; vendor-only функция остаётся `native.*`, пока нет второго одинакового consumer.
- `docs/owner-decisions.md` §1.2–1.4/2.2: один manager/worktree, writers без Cargo, никаких heuristic kill и самовольного удаления evidence.
- `docs/agent-operations/modularity.md`: adapter не зависит от другого adapter и не владеет второй БД.
- [Session V2 prompt/history/events](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/protocol/src/groups/session.ts), [prompt implementation](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/core/src/session.ts), [input admission](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/core/src/session/input.ts).
- [Question schema/service/routes](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/core/src/question.ts) и [protocol group](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/protocol/src/groups/question.ts).
- [Permission schema/service/routes](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/core/src/permission.ts) и [protocol group](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/protocol/src/groups/permission.ts).
- [Experimental capability/background route](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/opencode/src/server/routes/instance/httpapi/groups/experimental.ts) и [handler](https://github.com/anomalyco/opencode/blob/5d9cd9b259f0456522f318a7435501d03cfbee79/packages/opencode/src/server/routes/instance/httpapi/handlers/experimental.ts).

Current upstream не переписывает смысл уже установленного сервиса задним числом. Перед native qualification проверить фактически выставленную схему/endpoint выбранного подключения. Отсутствие одной experimental capability ограничивает только её, не весь route.

## 3. Почему не использовать generic `agent.send delivery=steer`

У ELIOT `delivery:"steer"` уже означает `native_expected_target`: caller передаёт `expected_turn_id`, а native runtime атомарно отвергает другую цель. OpenCode V2 input имеет `delivery:"steer"|"queue"`, но **не** принимает expected active-turn ID. Его `steer` означает продвижение на следующем безопасном шаге loop.

Нельзя:

- назвать OpenCode input exact steer;
- игнорировать полезный native loop-step режим;
- добавить `next_loop_step` в общий RuntimePort до второго действительно эквивалентного consumer вопреки module contract.

В R17 ввести один закрытый vendor method:

```text
native.opencode.loop_step
```

Поля: `client_request_id`, `binding_id`, `generation`, `text`. Никаких `expected_turn_id`, model/sandbox/permission overrides или произвольного native JSON. Capability: `native.opencode.loop_step`; отдельная command schema. Если позже появится второй runtime с тем же exact meaning, lift в общий delivery class делается прямой заменой consumers, не compatibility alias.

Обычный `agent.send delivery=next_turn` остаётся общим методом и маппится на OpenCode `delivery:"queue"`.

## 4. Карта существующего ELIOT-кода

| Участок | Изменение |
|---|---|
| `crates/swarm-adapter-opencode/src/config.rs::NativeOptions` | Удалить обязательный `expected_version` и текст «exact version». Сохранить service identity, connection file, directory и explicit provider/model/variant. Observed version — диагностика, не admission gate. |
| `module_runtime.rs::CAPABILITIES`, config/command/event schemas | Добавить ровно реально подключённые methods; обновить contract identity как новый artifact build, не переименовать старые checkpoints. |
| `native.rs::admit_input`, `input_payload`, `reconcile_input`, `read_input_status` | Передавать explicit native delivery; сохранять/проверять его в intent и readback. Queue и loop-step не взаимозаменяемы. |
| `lib.rs::handle_send`, `intent_for`, `handle_reconcile`, `flush_outbox` | Новый vendor operation и существующий next-turn используют один durable admission primitive; reply/background имеют свои intents/readback. Не replay effect из reconnect loop. |
| `journal.rs` | R02/#28 владеет decoder/salvage/outbox repair. R17 добавляет новые typed intent payloads только после принятия его seam; не писать второй журнал. |
| `module_link.rs`, `HostSession`, `module.observe` outbox | Один retained observation stream взаимодействий/event cursor; host ACK предшествует advance локального cursor. |
| `swarm-kernel-host/src/model.rs` | Добавить закрытую форму vendor method. Не ослаблять validation неизвестных methods. |
| `store/runtime.rs` | Допустить/приоритизировать agent.reply/background/refresh как сейчас; новый loop-step — control input, который не обгоняет unresolved write без определённого контракта. |
| `store/module_handshake.rs::native_command_capability` | Explicit mapping `native.opencode.loop_step`; **исправить общий дефект**: неизвестный native method не должен превращаться в `Some(true)`. Legacy route без descriptor сохраняет только прежний известный agent.* contract. |
| `swarm-contracts::method_policy`, MCP catalog/schema/CLI | Один typed request на новый method; Manager/GM/Operator по существующей binding authority. Participant не получает vendor control автоматически. |
| `kernel-host/runtime/opencode_v2/{effects,background,...}` | Источник проверенных invariants, не второй product implementation. После переключения callers соответствующие writable paths удалить/вывести из сборки; read-only historical evidence остаётся только при реальном consumer. |

R02/#28 исправляет journal/IPC/EOF/owner lifetime. R17 не переписывает его работу, но production integration должна быть поверх исправленного journal. Если #28 ещё не принят, R17 branch может написать native/control modules и contract changes, но не объявляет recovery-qualified до интеграции seam.

## 5. Input admission: exact ID, payload и delivery

Current `V2Session.prompt`:

1. строит prompt;
2. использует caller ID либо создаёт новый message ID;
3. durable `SessionInput.admit`;
4. возвращает прежнюю запись только при полном совпадении session/prompt/delivery;
5. после admission делает wake, если `resume !== false`.

Следовательно:

```text
same ID + same session/prompt/delivery = та же admission
same ID + другой prompt или delivery   = conflict
admission                              ≠ wake/processing/terminal
```

### 5.1 До native POST

`OperationIntent` сохранить:

- operation/binding/generation/native root/scope;
- deterministic `native_input_id`;
- canonical prompt bytes/digest;
- native delivery `queue|steer`;
- `resume:true` как effect request, но не часть native admitted-input equivalence;
- module receipt и route/model identity.

При повторной ELIOT Operation с тем же caller request ID изменённый text/delivery уже должен конфликтовать на Store/Journal. Нельзя переписать intent новым payload.

### 5.2 После ответа

HTTP response проверяет exact ID/session/prompt/delivery. Это `native_input_admitted`, не terminal. Затем durable history различает:

- `session.next.prompt.admitted` — input сохранён;
- `session.next.prompted` — input продвинут в loop;
- `session.next.step.started` — execution began;
- `step.ended|step.failed` — terminal одного assistant step.

При queue необязательно сразу увидеть prompted. При loop-step promoted event может появиться позже. Не считать отсутствие немедленного promoted отказом.

### 5.3 Lost reply

Автоматический reconnect **не** повторяет POST. Reconcile читает exact input/history/message:

- exact admitted input найден → settle input admission;
- promoted найден → добавить execution boundary;
- terminal найден → сохранить terminal отдельно;
- nothing found при неполной/gapped истории → Unknown;
- доказанный full interval без input, если API действительно даёт такую полноту → Rejected/no-effect только по формальному контракту, не по timeout.

Хотя upstream допускает exact same-ID repeat, R17 не включает blind resend: между transport loss и проверкой неизвестны server generation/location/auth. Повтор допустим лишь как отдельная будущая explicit reconciliation policy с доказанной byte-equivalence и той же service identity.

## 6. Один bounded durable event reader

Использовать `/api/session/:id/history?after=<exclusive-seq>&limit=<bounded>` для finite catch-up. SSE `/event?after=` — только coalesced wake/low-latency source; correctness остаётся у durable history.

Новый внутренний компонент, возможное имя `SessionEventReader`:

- одна state machine на observed session;
- `after_sequence` применяется только после ACK соответствующей `module.observe`;
- page count, events/page, canonical bytes и per-event text/content bounds;
- strict monotonic aggregate sequence; duplicate exact event coalesces, conflict под тем же sequence — integrity error;
- hasMore=true требует advancing page; empty/nonadvancing page — gap/error;
- live deltas не копируются в Store: durable full-value events only;
- SSE disconnect/reconnect не меняет native execution и не создаёт новый prompt.

Observation хранит compact state:

```text
session_id
covered_through_sequence
coverage: complete_to_cursor | partial | gap
last_native_activity_ms (только native events, не наш incoming send)
pending_questions[]
pending_permissions[]
last_prompt/step terminal refs
children/family refs, если они реально наблюдены
```

Не помещать полный transcript и tool output в observation. Exact result bytes читаются существующим paged result path.

Для root reader запускается после verified open. Для ребёнка — только после observed parent edge и bounded ownership check. Нет family observation — `partial`, не empty.

## 7. Questions: native ordered answers, no prompt workaround

Current request:

```text
id, sessionID, questions[], optional tool{messageID,callID}
question: question, header, options[], multiple?, custom?
reply: answers: string[][] в порядке questions
```

### Observation

Из session question list и durable `question.v2.asked` строить bounded attention item:

- exact `session_id`, `request_id`;
- canonical request fingerprint;
- ordered question descriptors;
- tool ownership IDs, если present;
- source event sequence/freshness.

Fingerprint считается от canonical native request без локального display decoration. Новый native request с тем же ID и другими bytes — conflict, не update формы.

### Reply

`agent.reply` получает closed envelope:

```json
{
  "method":"opencode.question.reply",
  "params":{
    "session_id":"...",
    "request_id":"...",
    "fingerprint":"...",
    "answers":[["..."],["..."]]
  }
}
```

Отдельный `opencode.question.reject` без answers. Перед POST:

1. binding/generation/current authority;
2. target root или observed owned child;
3. current pending request exact bytes/fingerprint;
4. answer count == questions count;
5. each answer valid для options/multiple/custom native shape; свободный custom не переопределяет option labels.

Intent записывается до HTTP. POST 204 — native ACK. Durable `question.v2.replied` с exact answers либо `question.v2.rejected` — readback. Исчезновение из pending list без event не доказывает, какой ответ применён.

Lost response: history read only; никогда не отправлять ответы новым model prompt.

## 8. Permissions: first slice `once` и `reject`, не `always`

Current request содержит action, resources, optional save/metadata/source. Native reply:

```text
once | always | reject
```

Но semantics различны:

- `once` разрешает один request;
- `reject` отклоняет target **и все остальные pending permission requests той же session**;
- `always` сохраняет project-level allow rules и может автоматически разрешить другие pending requests, включая другие sessions той же location/project.

Поэтому generic `agent.reply` в R17 поддерживает только:

```text
opencode.permission.once
opencode.permission.reject
```

`always` не маскировать как обычный ответ. Если владельцу нужен remembered allow, это отдельная будущая policy mutation `native.opencode.permission.remember` с усиленной authority, preview saved rules и readback `/api/permission/saved`. До неё `always` = unsupported before native HTTP.

### Permission attention/reply

Attention fingerprint связывает full current request. Для reject перед POST прочитать полный bounded pending set этой session и сохранить IDs, потому что native effect каскадный. После durable reply:

- `once`: закрыть target;
- `reject`: закрыть target и все IDs из pre-effect set, которые native session event/readback подтверждает как cleared; если coverage неполна — gap, не invented completion;
- stale fingerprint/not found before POST → Rejected no effect;
- not found after possible POST → Unknown, пока history не показывает exact reply.

Feedback message при reject — bounded optional field, если native payload разрешает. Raw metadata/resources остаются data, не исполняются ELIOT.

## 9. Background: current boolean contract, без synthetic-text heuristic

Текущий upstream:

```text
GET  /experimental/capabilities
  -> { backgroundSubagents: boolean }
POST /experimental/session/:sessionID/background
  -> boolean, true если хотя бы один synchronous task job promoted
```

Handler выбирает только running task jobs, принадлежащие target parent session и ещё не background. Значит:

1. pre-read capability;
2. false → `UNSUPPORTED_CAPABILITY` **до** effect;
3. verify target root/owned child;
4. persist intent;
5. POST один раз;
6. true → Applied, `changed:true`;
7. false при supported capability → Applied documented no-op, `changed:false`, а не unsupported;
8. transport loss → Unknown; POST не повторять.

Не переносить старый `background.rs` parser фиксированного synthetic notice. Current endpoint возвращает boolean и current upstream TODO прямо указывает, что durable BackgroundJob status/cancellation ещё расширяется. Пока нет exact job readback, lost-response background остаётся Unknown.

`agent.background` сохраняется как common operation: семантика «убрать synchronous child blocking, не остановить его» уже явно определена. Response должен назвать target session, capability observation и changed flag; не заявлять конкретные child IDs, которых endpoint не вернул.

## 10. Host contract и capabilities

Standalone descriptor после реализации заявляет только:

```text
agent.open
agent.send/next_turn
agent.reply
agent.background
agent.refresh
agent.reconcile
agent.result
task.dispatch
native.opencode.loop_step
существующие native.mcp.* при сохранённом реальном consumer
```

`agent.refresh` делает read-only catch-up interaction/history и публикует observation; не resume и не writer lease. `agent.reply` формы доступны только если pending inventory/history routes реально подтверждены на current service.

В `native_command_capability` unknown method должен быть fail-closed для selected descriptor. Иначе новый dispatch method обходил бы descriptor gate — уже доказанный системный дефект аудита. Legacy binding без descriptor сохраняет только перечисленные исторические agent methods; vendor-native extension там unavailable.

Добавить explicit command schemas для reply и vendor loop-step. Не принимать произвольный `reply.method` только по префиксу `opencode.`; закрытый enum + typed fields.

## 11. Удаление pin/downgrade и выбор текущего native contract

Удалить `expected_version` из route/native config и supervisor env. Не заменить его regex range либо списком разрешённых releases.

Вместо этого:

- health/version сохраняется как observed diagnostic;
- required endpoint/shape проверяется адресно до использования capability;
- background использует `/experimental/capabilities`;
- questions/permissions/history получают typed response validation;
- несовместимый endpoint ограничивает только конкретную функцию и виден в observation/attention;
- обновление, изменившее обязательную wire-форму, даёт schema/capability gap, а не переключение на старый hidden endpoint.

Legacy `/form` не является fallback current V2 question API. Новые writes после cutover идут только через V2 questions/permissions. Исторические старые result artifacts можно читать отдельным immutable reader; это не причина сохранять старый executor.

## 12. Переиспользование и удаление дублей

Из built-in `runtime/opencode_v2` взять invariants, не crate dependency:

- exact root/location/model identity;
- child ownership checks;
- intent-before-effect и readback-only reconcile;
- bounded projections/results;
- separation reply/background/input.

Не переносить:

- version equality gate;
- old form wire format;
- `delivery=steer` rejection;
- synthetic background notice parser;
- Store-specific vendor branches;
- старый owned-service implementation, если standalone owner уже выполняет тот же контракт после R02.

После переключения route/descriptor/callers и fixture/live qualification удалить соответствующие writable paths из `kernel-host/runtime/opencode_v2`. Разрешён отдельный deletion PR сразу следом, но до него old path disabled for new bindings и имеет named removal condition. Нельзя годами развивать обе реализации.

## 13. Порядок реализации для агента

### Slice A — contract и delivery

1. убрать `expected_version` из typed config/descriptor/docs;
2. добавить vendor method + closed schema/capability/policy/MCP/CLI;
3. добавить delivery в intent/payload/readback;
4. queue vs loop-step transcript fixtures;
5. переключить один real caller.

### Slice B — durable event reader

1. typed history page/event decoder;
2. per-session cursor + observation outbox;
3. prompt admitted/promoted/step terminal fold;
4. gap/cycle/size handling;
5. current authority/host ACK before cursor advance.

### Slice C — questions and permissions

1. pending list decoders + fingerprints;
2. bounded attention observation;
3. agent.reply typed dispatch;
4. durable event reconcile;
5. cascade reject and deny `always` before effect.

### Slice D — background

1. capability read;
2. bool POST mapping;
3. lost-response unknown;
4. remove/deprecate old notice-based path after caller switch.

Каждый slice в этом PR должен иметь production caller. Не принимать public helper/DTO без связанного dispatch/readback.

## 14. Сценарии готового кандидата — пока не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Same input ID + same queue payload | Одна native admission, matching receipt/readback. |
| Same ID + changed text или queue→steer | Conflict, старый intent/input неизменны. |
| next_turn и native loop-step | Queue и safe-step promotion различаются; loop-step не claims exact turn. |
| Lost prompt response, admitted/history observed | Нет второго POST; admission восстанавливается. |
| Admitted, wake/terminal ещё нет | Accepted input + execution unknown/pending, не Task complete. |
| Question reply/reject | Exact fingerprint/answers, durable corresponding event. |
| Permission once | Только target закрыт. |
| Permission reject с 3 pending | Каскад отражён; stale attention очищена по evidence. |
| Permission always через generic reply | Rejected before HTTP as unsupported policy mutation. |
| Reply чужому child/stale fingerprint | Rejected before effect. |
| Background capability false | Unsupported before POST. |
| Capability true + POST true/false | Applied changed true / Applied no-op false. |
| Lost background response | Unknown, no replay. |
| Event page duplicate/conflict/gap/nonadvancing/oversize | Coalesce exact duplicate либо explicit integrity/gap; cursor не перескакивает. |
| SSE disconnect/viewer disconnect | Durable catch-up продолжается; native Task/children не отменяются. |
| Updated service with supported shapes | Работает без version pin; unrelated unknown field не ломает route. |
| Required shape absent | Только capability unavailable, никакого hidden fallback/downgrade. |

Фикстуры строятся из записанных официальных wire-форм; model/provider calls не нужны. Native qualification выбранной подписочной установки выполняется в финальной фазе и фиксирует observed version/schema без запрета будущих обновлений.

## 15. Gates и сдача

Один manager/worktree. Writers получают непересекающиеся symbols и не запускают Cargo. После законченного вертикального пути manager выполняет scoped formatting и:

```sh
cargo clippy --locked \
  -p swarm-adapter-opencode \
  -p swarm-contracts \
  -p swarm-kernel-host \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad tests/native/load — итоговая фаза. Минимальная сдача:

```text
candidate SHA
actual switched production caller
input/reply/background intents and readback paths
removed/disabled duplicate path and deletion condition
commands + exit status actually run
remaining unsupported native capabilities
```

Эта документация не означает, что перечисленные tests или runtime functionality уже существуют. Не merge/activate по одному Markdown CI.
