# R19. Antigravity stream-json: выбранная модель, последовательные turns и честная cumulative usage

**Draft-задание, 8 октября 2026. Production-код этого блока ещё не написан.**

ELIOT исследован на `40591a295af94b1541ec2ba30afe8e3247701a71`. Официальная документация Antigravity CLI прочитана 08.10.2026. Ни внешний release, ни модель не закреплять в коде: route задаёт текущий выбор владельца, CLI обновляется штатно, а adapter проверяет фактический init/result текущего процесса. Подключение использует cached subscription credentials; новый API key/account/provider route не вводится.

## 1. Результат

Довести существующий warm `stream-json` artifact до честного контракта:

- удалить hard-coded `REQUIRED_MODEL_ID` и exact-model equality с одной строкой;
- запускать выбранные владельцем model/effort/agent/permission options;
- проверять init conversation/cwd/model/agent/permission evidence без выдуманного readback effort;
- сериализовать prompts: один active turn, следующий input только после terminal result;
- сохранять intent до stdin write и не повторять input после неизвестного исхода;
- отделить current-turn response от cumulative session counters;
- считать turn delta только из совместимых последовательных snapshots;
- не отправлять в stream slash commands или `control_request`;
- корректно сливать child observations без стирания полей при частичном update;
- не приписывать следующему turn terminal с отсутствующим/чужим conversation ID;
- закрывать stdin только как graceful finish-after-current-turn, не как immediate cancel;
- явно оставить machine-readable subscription balance/quota unavailable, пока официальный stream его не выдаёт.

Не создавать второй Antigravity backend, не заменять CLI Python SDK/API route, не читать TUI/private storage, не использовать `agy models` или `/credits` как automation probe.

## 2. Нормативные источники

- [Headless mode](https://www.antigravity.google/docs/cli/headless/): JSON/stream-json, init/step_update/result, multi-turn stdin, cumulative fields, errors, model/effort/agent, permissions, exit semantics.
- [AI credits](https://www.antigravity.google/docs/cli/credits/): balance/quota доступны через interactive status/panels and settings, не через документированный structured warm stream.
- `docs/agent_swarm.module-contract-v2.md`: intent before effect, readback instead of blind replay, native state у harness.
- `docs/owner-decisions.md` и `docs/agent-operations/modularity.md`: один manager/worktree, explicit owner policy, adapter outside Store, no heuristic kill.
- ELIOT current files: `modules/antigravity-rust/src/{wire,launch,stream,controller,contract,result_page}.rs`.

Документация scripts прошлой кампании — только regression examples, не API contract.

## 3. Что официально гарантирует stream-json

### 3.1 Input

Warm process запускается:

```text
agy --input-format stream-json --output-format stream-json
```

На stdin разрешён один NDJSON message type:

```json
{"event":"user","message":{"content":"..."}}
```

`content` — string или список `text` blocks. Другие block types завершают session с error. Нераспознанное `event` name лишь предупреждается/пропускается; malformed line, missing event, `control_request/control_response` и slash command inside stream завершают session.

Отсюда:

- generic text prompt — единственная write capability первого slice;
- нет exact steer, out-of-band control reply или mid-turn configure;
- `/model`, `/usage`, `/credits` нельзя использовать в рабочем stdin;
- следующий prompt пишется только после result предыдущего turn.

### 3.2 Output

Одна session:

```text
init (один раз)
step_update* для turn 1
result turn 1
step_update* для turn 2
result turn 2
...
```

`init`: conversation_id, cwd, tools, permission_mode и при explicit override model/agent.

`step_update`: conversation_id, step_index, state, step_type, text_delta, duration, per-step usage, tool_info либо subagent_info.

`result`:

- `response` относится только к текущему turn;
- `conversation_id` относится ко всей session;
- `num_turns`, `duration_seconds`, `usage` — **cumulative по session**;
- status — SUCCESS/ERROR/CANCELED/INTERRUPTED/INVALID/WAITING/RUNNING.

Не суммировать cumulative result snapshots. Не добавлять к ним per-step usage второй раз.

### 3.3 Process end

Закрытие stdin просит session завершиться после current turn и затем exit 0 при clean end. Это drain/finish-taken, не interrupt. Для immediate stop нужен отдельный explicit owner control/process signal contract; R19 его не добавляет.

Headless authentication использует cached credentials. Без auth процесс завершается `authentication required`, а не ожидает UI. Adapter не инициирует login/logout.

## 4. Карта текущего ELIOT-кода

| Участок | Что изменить |
|---|---|
| `wire.rs::REQUIRED_MODEL_ID` | Удалить. Никакой replacement constant/range/list. |
| `launch.rs::build_launch_spec` | Читать bounded current route modelId/effort/agent/permission options; передавать как argv; сохранять exact launch evidence. |
| `contract.rs::template/claim` | Artifact identity обновляется как build/revision ELIOT, не external CLI release. Capabilities остаются только реально подключёнными. |
| `stream.rs::Init` и `apply_init` | Validate exact conversation/cwd and requested model/agent/permission evidence. Не объявлять effective effort, которого init не сообщает. |
| `stream.rs::Turn/apply_result` | Добавить typed cumulative Usage и basis; validate current conversation and result ordinal; не терять terminal error/status. |
| `stream.rs::apply_step` | Хранить bounded per-step usage отдельно; silent text cap повышает gap/truncated flag. |
| `stream.rs::apply_children` | Обновлять только поля, реально присутствующие в event; omission не очищает workspace_uris/log/role. Explicit empty array может очистить. |
| `controller.rs` pending write/terminal | Один active ELIOT prompt; intent before write; stdin flush = transport attempted, не native admission. Terminal привязать к expected result ordinal/conversation. |
| `controller.rs` process/stream end | stdout EOF, child exit и init failure сохраняют evidence; не очищать init-failure до readback. No re-open while child disposition unknown. |
| `controller.rs` observation/outcome | Requested/effective fields разделены; cumulative usage snapshot и optional safe delta. No quota/balance claim. |
| `result_page.rs` | Exact terminal/current response page; malformed offset/length reject, not default page 0. |
| `config/controller.example.toml`, module README/UPDATE | Убрать одну конкретную model equality и version-like language; route examples — placeholders/current choice, не рекомендации удерживать slug. |

Existing `OperationIdentity` допускает methods, которых descriptor не заявляет. R19 не расширяет contract до reply/configure/goal/background/recover: stream-json их не поддерживает. Unknown methods должны отвергаться descriptor gate до module effect.

## 5. Route options и launch

Предлагаемая closed native-options форма:

```text
workspaceRoot: absolute path
modelId: non-empty current native slug selected by owner
reasoningEffort?: low|medium|high
agent?: bounded native agent name
expectedPermissionMode?: bounded native mode (readback assertion, не setter)
dangerouslySkipPermissions?: false by default; true only explicit owner route
```

`dangerouslySkipPermissions=true` — отдельный сильный owner decision; он auto-approves all tool calls. Adapter не устанавливает его автоматически и не выводит из Manager role.

Без dangerous flag CLI использует native settings permission mode. Headless tool requiring unavailable approval soft-denies action and продолжает run; stderr notice/tool error должен появиться в observation, а не приводить к скрытому yolo retry.

### 5.1 No model enumeration probe

Adapter не запускает `agy models` перед session и не парсит его human output. Requested model идёт в native launch. Unknown model даёт structured ERROR/exit nonzero; classified as configuration/model-unavailable evidence. Никакого fallback к default/первой модели.

### 5.2 Launch evidence

До spawn сохранить:

- binding/generation/operation;
- executable path/image identity по существующему owner contract;
- workspace identity;
- requested model/effort/agent/permission policy;
- exact argv digest (без secrets);
- boot/process owner.

После init сохранить observed conversation/cwd/model/agent/permission_mode/tools. Requested != observed — open Unknown/Rejected according to exact stage, child remains owned until departure; binding not ready.

## 6. Prepared open и init readback

`agent.open` запускает process без model prompt. `init` — prepared-session boundary.

Readiness requires:

1. one valid init before any accepted input;
2. nonempty conversation ID;
3. canonical/verified cwd equals workspaceRoot;
4. when adapter passed `--model`, init model exists and exact equals requested;
5. when adapter passed `--agent`, init agent exists and exact equals requested;
6. permission mode matches `expectedPermissionMode`, if configured;
7. tools list parsed within bound with truncation/coverage;
8. process still live and stream healthy.

Official init does not expose effective effort. Therefore:

- record `requested_reasoning_effort` and `application="native_launch_argument"`;
- do **not** record `effective_reasoning_effort` as confirmed;
- successful init proves CLI accepted launch configuration, not actual inference implementation of effort;
- if product later requires stronger readback, capability remains partially observed until vendor exposes it. Не использовать text response to infer effort.

Second init on same process = protocol integrity gap, not new session adoption.

## 7. One-active-turn state machine

Warm stdin has no native prompt/request ID. Adapter owns one sequential slot.

State per process:

```text
ReadyIdle
WritePrepared(operation_id, expected_result_ordinal, expected_num_turns?)
WriteAttempted
TurnRunning(user_input observed?)
TerminalObserved
ReadyIdle
StreamDispositionUnknown / Exited
```

### 7.1 Before stdin write

Persist exact intent:

- operation/binding/generation/root conversation/process boot;
- text bytes/digest;
- canonical NDJSON bytes/digest;
- expected next local result ordinal;
- prior cumulative num_turns/usage snapshot if known;
- module receipt/Task dispatch context.

Only then write+flush. A failed local validation is Rejected before effect. Write/flush error after partial write = Unknown. Не повторять line.

### 7.2 Admission evidence

stdin write success = transport attempted, not native admission. Stronger evidence:

- valid `step_update` for same conversation, expected new turn and `step_type=user_input`; or
- exact following terminal result when no user-input step was retained.

Because native input ID отсутствует, normalized dispatch receipt can use adapter-owned stable input identity only if contract clearly marks it non-provider-issued and the same identity is bound to sequential slot. Не фабриковать ID as if vendor returned it.

### 7.3 Terminal correlation

Accept a result for current pending Operation only when:

- same conversation ID as init;
- no earlier unresolved turn;
- local result ordinal = expected next;
- cumulative `num_turns` is compatible with prior snapshot (normally +1; first observed snapshot establishes baseline when prior unavailable);
- result appears after the write attempt and any relevant user-input step;
- no stream gap makes ordering unknown.

Missing/mismatched conversation ID **does not** remain queued for the next prompt. Current operation becomes Unknown/integrity gap; adapter stops admitting inputs until stream/process is reconciled. This prevents result N+1 settling operation N.

## 8. Terminal statuses and replay

Preserve native status verbatim.

| Status | ELIOT semantic |
|---|---|
| SUCCESS | Turn terminal succeeded; Applied execution evidence, not Task acceptance. |
| ERROR | Turn terminal failed. No prompt replay; retain error/tool effects. |
| CANCELED | Native turn cancelled; terminal unsuccessful. |
| INTERRUPTED | Native turn interrupted; terminal unsuccessful. |
| INVALID | Native run ended invalid; terminal unsuccessful/integrity details retained. |
| WAITING/RUNNING in result | Not a normal successful terminal; explicit incomplete/Unknown unless official current contract classifies differently. |

Current generic `EffectOutcome` is too coarse to imply whether earlier tools changed files. Do not attach `native_no_effect:true` to ERROR/CANCELED/INTERRUPTED/INVALID. Existing Rejected mapping may remain for terminal unsuccessful result only if details and all consumers distinguish it from **pre-admission rejection** and replay remains forbidden. Otherwise use Accepted/input-admitted plus separate terminal event status; choose one consistent producer/consumer contract in this PR, not text-only diagnostics.

Task acceptance never follows from SUCCESS alone.

## 9. Cumulative usage: preserve snapshot, derive delta conservatively

Add typed fields:

```text
UsageSnapshot {
  input_tokens
  output_tokens
  thinking_tokens
  cache_read_tokens
  total_tokens
}
TurnUsageEvidence {
  cumulative
  delta?: UsageSnapshot
  basis: fresh_process_zero | previous_terminal | resumed_baseline | reset_or_gap
  process_boot_id
  conversation_id
  native_num_turns
  native_duration_seconds
}
```

Do not recompute `total_tokens` from other fields; native accounting may define overlap differently. Validate finite/nonnegative numeric types and bounded integer conversion.

### 9.1 Delta rule

A delta exists only when:

- same process boot and conversation;
- previous terminal snapshot known with complete usage;
- current num_turns > previous and normally increments exactly by one for one submitted prompt;
- every cumulative counter is >= previous;
- cumulative duration is finite and >= previous;
- no intervening gap/malformed frame.

Delta = checked subtraction per field. If any counter decreases, conversation changes, first snapshot after resume has no reliable baseline, or num_turns jumps, keep cumulative snapshot and mark `reset_or_gap`; no negative/guessed delta.

### 9.2 Step usage

`step_update.usage` is scoped to that step. Store bounded per-step diagnostics or aggregates separately. Never add step usage to terminal cumulative usage. Never sum cumulative result snapshots across turns.

Credits/subscription balance are not derived from token usage.

## 10. Quota and credits boundary

Official CLI exposes credits/model quotas in interactive status/panels/slash commands. Stream-json docs do not expose a machine-readable credits balance event.

Therefore R19 reports:

```text
subscription_quota_status: unavailable_not_exposed_in_stream
usage: token counts observed
```

It does not:

- send `/usage`, `/credits`, `/model` into stream (would terminate it);
- start a second `agy -p /credits` or TUI scraper;
- read private credential/cache files;
- buy credits/change `useG1Credits`;
- infer money from tokens;
- switch model when balance unknown.

If vendor later adds a structured endpoint/event, it becomes a new documented producer reviewed separately, not a regex fallback.

## 11. Children and native activity

`subagent_info.subagents[]` gives type_name, role, conversation_id, log_uri, workspace_uris. This is observation, not direct child control.

Merge rules:

- exact conversation ID is key;
- missing optional field preserves previous observed value;
- field explicitly present empty/null follows documented native semantics and may clear;
- workspace_uris replaced only when field present; omission no longer wipes it;
- new child beyond bound creates gap/partial coverage, not silent omission;
- conflicting role/type/log identity under same child ID creates integrity gap;
- child status remains observed/unknown unless native update gives a documented status;
- last_native_activity derives from step/subagent events, not wall-clock poll and not incoming ELIOT prompt.

Never open `log_uri` directly as arbitrary file. It remains native reference unless official API/read authorization exists.

## 12. Text, tool and malformed-frame bounds

Existing `MAX_TEXT_CHARS_PER_STEP` saturation must set a truncation/gap marker. `text_chars` alone cannot claim exact response length after truncation.

Tool info:

- bound tool name/error type/diagnostic strings;
- do not retain raw parameters/output in observation;
- full result only through artifact path if required;
- permission soft-denial reported distinctly from provider/model failure where structured tool error permits.

Unknown output `event` type: count/diagnostic, keep stream (forward compatibility). Malformed JSON/oversize/current known event with invalid required fields: gap; current operation cannot gain exact terminal evidence from an unordered stream.

stdout EOF before child exit and child exit before stdout drain are separate. Do not erase retained init/terminal evidence when noting failure. Current code paths `child_exited/stdout_failed` must preserve the strongest fact already observed.

## 13. Graceful shutdown and recovery

R19 does not implement immediate interrupt. Explicit graceful stop:

1. stop admitting new prompts;
2. wait current turn terminal or retain Unknown;
3. close stdin once;
4. drain final result/stdout;
5. await process/family departure with bounded owner logic;
6. report session end.

Host IPC disconnect does none of this. Observer unsubscribe does none of this.

Cross-process `--conversation` exists for one-shot resume, but a correct warm-stream recovery contract was not established here. Do not silently start a new process under same binding generation after adapter/child loss and assume exact pending turn recovery. Current first slice leaves `agent.recover` unavailable unless a later source review proves session/load and unresolved-turn identity end-to-end.

## 14. Result pages and normalized provenance

`agent.result` status/result pages:

- validate offset/length types; malformed values rejected, no default zero;
- exact target operation and module receipt;
- exact current-turn response hash/chars/status/cumulative usage evidence;
- no claim of Task completion;
- full response bytes paged through immutable artifact, not compact observation;
- if terminal conversation/result ordinal mismatch, no page attributed to target.

Existing normalized dispatch receipt outer/inner binding problem belongs R05/#31 and must be integrated, not copied.

## 15. Code cleanup

- delete `REQUIRED_MODEL_ID` and dependent guards/tests/docs;
- remove unreachable/redundant launch policy guards after one authoritative validator remains;
- remove or connect `Controller.journal`; write-only retained memory is deleted, not kept for future use;
- centralize one route-options parser used by launch/readback/observation;
- merge child updates in one function with field-presence semantics;
- retain one source of terminal status classification.

No adapter SDK extraction until a second real consumer and live caller exist. Do not add compatibility alias for old model constant.

## 16. Порядок реализации

### Slice A — options/open

1. typed route options, remove model constant;
2. launch argv and owner evidence;
3. init readback model/agent/permission/cwd;
4. readiness and mismatch outcomes;
5. update descriptor/docs/examples.

### Slice B — serialized prompt lifecycle

1. intent + expected ordinal before write;
2. one-active-turn gate;
3. user_input/result correlation;
4. stream/process failure without evidence loss;
5. no replay.

### Slice C — usage/children

1. typed cumulative usage snapshots;
2. safe delta and reset/gap basis;
3. step usage separation;
4. child partial merge and activity;
5. compact observation.

### Slice D — results/cleanup

1. exact page validation/provenance;
2. full response artifact/page;
3. remove dead journal/guards/model constant;
4. switch/update production callers and docs.

Каждый slice содержит caller. Не принимать типы/fields без producer и reader.

## 17. Критерии итогового кандидата — пока не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Route выбирает другую валидную модель | Запускается выбранная модель без code change/release allowlist; init exact readback. |
| Unknown model | Native structured ERROR/nonzero; no fallback/default. |
| Requested agent/model mismatch in init | Binding not ready; evidence retained; child owned until departure. |
| Requested effort high, init effort absent | requested/application recorded; no invented effective effort. |
| Expected permission mode differs | Open rejected/not-ready before first input. |
| Permission soft-denied tool | Run continues; structured diagnostic, no automatic yolo retry. |
| Two sequential prompts | Second written only after first result; result ordinals/conversation exact. |
| Result missing/wrong conversation | Current Operation Unknown; result not used by next prompt; admission stops pending reconcile. |
| stdin partial/write failure | Unknown, no repeat. |
| Cumulative results 1→2 turns | Snapshot replaced; checked delta only once. |
| Counter decreases/turn jumps | Cumulative retained, delta absent, reset_or_gap. |
| Step usage + result usage | Not double-counted. |
| workspace_uris omitted in later child update | Prior value retained; explicit present empty handled separately. |
| Text cap reached | truncation/gap explicit. |
| Unknown event vs malformed line | Forward-compatible count / integrity gap respectively. |
| Close stdin during active turn | Final result drained, then clean exit; not immediate cancel. |
| Host IPC reconnect | Warm process/session unchanged. |
| Credits requested by reader | unavailable_not_exposed; no slash command/second process/private DB. |
| Result selector malformed offset | Rejected, not page zero. |

Fixtures use official NDJSON forms without model calls. Native qualification uses current installed subscription-backed CLI and records observed init/turn shapes, not allowed release numbers.

## 18. Gates и сдача

Один manager/worktree; writers без Cargo. После связанного кода manager выполняет scoped formatting и:

```sh
cargo clippy --locked \
  -p antigravity-rust \
  -p swarm-contracts \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Точное package name сверить по workspace metadata, не угадывать в отчёте. Broad tests/native later.

Сдать:

```text
candidate SHA
route-options/open/readback caller chain
prompt intent/write/result correlation
usage snapshot/delta basis
child merge behavior
removed model pin/dead state
actual commands and exit statuses
remaining unavailable controls/quota/recovery
```

Эта документация не запускает Antigravity, не меняет settings/login/subscription, не квалифицирует Rust и не обновляет main.
