# R16. Claude: official permission/question flow → attention → exact decision

**PR #42 · переработано 8 октября 2026 · production-код ещё не изменён.** Source ELIOT: `40591a295af94b1541ec2ba30afe8e3247701a71`. Release numbers в репозитории описывают существующий код, а не допустимые версии пользовательского harness.

## Результат

Claude-сессия использует текущую subscription authorization и официальный Agent SDK permission flow. Approval или `AskUserQuestion` появляется в scoped attention и получает ровно одно решение через существующую Operation/`agent.reply`. Для долгого ожидания применяется официальный durable `defer + persisted session + resume`, а не попытка восстановить JavaScript Promise из JSON.

PR не включает новый executor, отдельный inference API, смену аккаунта, universal auto-allow, новый model loop или подмену hooks callback-ом.

## Нормативные источники

- [Module contract](../../agent_swarm.module-contract-v2.md): ownership, Operation, replay/readback, attention.
- [Native harness integration](../../agent-operations/native-harness-integration.md): official-doc source hierarchy и capability semantics.
- [Claude permissions](https://code.claude.com/docs/en/agent-sdk/permissions).
- [Claude approvals and user input](https://code.claude.com/docs/en/agent-sdk/user-input).
- [Claude environment variables](https://code.claude.com/docs/en/env-vars).
- [Owner decisions](../../owner-decisions.md), §1.2–1.4/2.2.

Исторические скрипты/брифы не определяют expected SDK payload. Их инциденты можно добавить в fixtures только после сопоставления с официальной native формой.

## Первый участок кода

Открыть `crates/swarm-adapter-claude/sdk-harness/bridge.mjs::prepare`, затем `handle`, `pump`, `safeSdkFrame` и Rust consumers.

Сегодня:

- `prepare` отвергает всё кроме package version `0.3.287`;
- `canUseTool` немедленно возвращает deny;
- Node control поддерживает prepare/send/stop, но не decision reply;
- CAPABILITIES не объявляет `agent.reply`;
- Rust adapter сохраняет собственную Operation/journal identity, но не живой callback request.

Устранить весь связанный путь, а не только удалить одну проверку версии.

## Карта существующих функций

| Участок | Изменение и сохранённая гарантия |
|---|---|
| `sdk-harness/bridge.mjs::{prepare,handle,pump,safeSdkFrame}` | Проверка required exports/options вместо release equality; official permission callback/hook frames. Reader stream не блокируется ожиданием manager decision. |
| `sdk-harness/prepared-query.mjs::prepareQuery` | One-shot prepared ownership сохраняется. Reply/defer/resume не запускает вторую query для того же admitted input. |
| `src/sdk_harness.rs::{NativeHarness,HarnessFrame}` | Typed frames pending/deferred/decision/closed; byte/count bounds. |
| `src/lib.rs::{handle_command,CommandInvocation,open_link}` | Existing Operation, binding/boot и `agent.reply`; reconnect не повторяет prompt или decision. |
| `src/native_state.rs::NativeControl` | Current live request, deferred request и historical denial/cancelled — разные states. |
| `src/journal.rs::OperationJournal`, `src/receipt.rs` | Persist intent before decision; same request replay reads result; changed payload conflicts. |
| `src/config.rs::NativeOptions`, `src/module_runtime.rs::{CAPABILITIES,capabilities_match}` | Объявить только реально подключённые live/deferred reply capabilities; version — observation. |
| Host `store/module_handshake.rs`, `store/runtime.rs`, `store/capacity.rs` | Authenticated observation → scoped attention → current-authority admission. Не открывать raw tool input другим scopes. |

Новые public types получают реальных callers в этом же PR. Отдельный endpoint на каждый Claude tool не требуется.

## 1. Сохранить subscription route

Официальная документация: `ANTHROPIC_API_KEY`, если установлен, заменяет Claude Pro/Max/Team/Enterprise subscription; в non-interactive mode он используется всегда. Поэтому adapter обязан:

- не инъецировать `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN` или другой provider route без явной конфигурации владельца;
- не наследовать случайный key из управляющего процесса, если route объявлен subscription-backed;
- наблюдать выбранный auth/provider mode без публикации credentials;
- при несовпадении fail before model input, а не молча переключаться;
- не использовать `CLAUDE_CODE_SIMPLE`, если нужен OAuth/keychain subscription: этот режим официально не читает OAuth credentials.

ELIOT IPC credential не является provider key. Status/Doctor не выполняет login/logout и не меняет environment работающей сессии.

## 2. Compatibility определяется требуемыми интерфейсами

Удалить `packageInfo.version !== '0.3.287'` и все зависимые ложные проекции. Не заменять его новым release/range allowlist.

До первого input проверить:

- импорт выбранного package/entrypoint;
- query/prepared-query API, который реально использует driver;
- permission mode option и `canUseTool` signature;
- hook registration и `defer`, если включён durable mode;
- required stream message/session identity forms;
- abort/cancellation signal;
- observed runtime/package versions для диагностики.

Отсутствующий required interface делает **эту capability** unavailable. Read-only describe и независимые функции не отклоняются только из-за нового optional field или другого release number.

Не подменять текущую установку historical bundled executable. Идентичность ELIOT artifact bytes сохраняется отдельно от version внешнего harness.

## 3. Официальный permission evaluation

Фактический порядок:

1. `PreToolUse` hooks;
2. deny rules;
3. ask rules;
4. permission mode;
5. allow rules и native auto-approved calls;
6. `canUseTool`, если запрос всё ещё unresolved.

Следствия для implementation:

- `canUseTool` не видит все tool calls;
- auto-approved call не становится pending attention;
- правило, обязательное для каждого вызова, реализуется `PreToolUse` hook;
- `dontAsk` не вызывает callback и отклоняет то, что иначе спросило бы;
- bare allow rules могут shadow callback; этот факт должен быть виден в describe/diagnostics;
- `bypassPermissions` не ограничивается `allowedTools`; deny/ask/hooks остаются отдельными слоями;
- после Agent SDK 0.3.286 отсутствие `permissionMode` не равно явному `default`.

Хранить:

```text
requested_permission_mode: Explicit(mode) | Inherited
observed/effective_mode: Observed(mode) | Unknown
callback_coverage: unresolved_only
hook_coverage: configured matcher/revision
```

Нельзя объявить effective mode по исходному config без native evidence.

## 4. Два режима ожидания, а не один выдуманный recovery

### 4.1 Live callback

Использовать, когда Node owner остаётся жив до решения:

```text
canUseTool(toolName,input,{signal,suggestions})
  → bounded PendingNativeInteraction
  → module.observe
  → attention
  → agent.reply Operation
  → exact live resolver
  → PermissionResultAllow/Deny
```

Pending state содержит разные identities:

- adapter boot/connection generation;
- native session/root identity;
- local request ID;
- native toolUseID, когда предоставлен;
- tool name;
- canonical input fingerprint;
- request kind (`permission`/`ask_user_question`);
- cancellation signal state;
- created/observed times.

Original input и Promise/resolver остаются в Node memory. Store получает bounded redacted projection и fingerprint. Reply не редактирует произвольный input в первом slice; allow пропускает original input, deny содержит bounded message.

### 4.2 Durable defer

Официальная документация рекомендует `PreToolUse` hook с `defer`, если человеческий ответ может быть дольше жизни процесса. Реализовать только после проверки точного hook/defer type текущего SDK:

1. Hook фиксирует bounded request identity и возвращает официальное `defer` decision.
2. Native session сохраняется своим механизмом; процесс может завершиться без живого Promise.
3. ELIOT attention ссылается на deferred request/session, а не callback resolver.
4. Решение допускает documented session resume/continuation с точным persisted request contract.
5. Если установленный interface не предоставляет необходимый resume, capability `durable_reply` unavailable; не симулировать её новым prompt.

Live pending callback не сериализуется как будто resumable. Node loss:

- live request → `callback_lost`, решение не отправляется;
- deferred request → остаётся answerable только если native persisted-session contract это подтверждает.

## 5. AskUserQuestion

`AskUserQuestion` проходит через `canUseTool` и использует official shape:

```text
questions[1..4] {
  question,
  header,
  options[2..4] {label, description, preview?},
  multiSelect
}
answers { question_text → label | labels | custom_text }
```

При allow вернуть original questions и collected answers. Не посылать answer как новый conversation prompt.

Границы:

- если adapter ограничивает tools array, `AskUserQuestion` должен быть в ней;
- option preview optional; HTML preview проходит native validation, но UI ELIOT всё равно treats it as untrusted/redacted content;
- duplicate question text делает answer map ambiguous — запрос не auto-answer; manager видит capability gap/invalid request;
- free text и multi-select сохраняют official form;
- текущая документация: `AskUserQuestion` недоступен subagents, запущенным через Agent tool. Не объявлять child support;
- custom multi-step forms, которых native tool не выражает, не встраиваются в этот callback.

## 6. Reply admission и one-shot settlement

`agent.reply` проверяет до эффекта:

- current Principal и binding/generation;
- current adapter boot/connection;
- pending kind/state;
- native/local request identity;
- input fingerprint;
- decision shape;
- Operation request ID/digest.

Allow/Deny или AskUserQuestion answers — разные typed decisions. `allow always`/permission-rule write не входит в первый slice: official suggestions можно наблюдать, но изменение settings требует отдельной authority и отдельной Operation.

Гонки:

- reply выигрывает → resolver settles once, listener снимается;
- AbortSignal/query close выигрывает → request cancelled, late reply rejected;
- same reply retry → сохранённый результат, без второго resolve;
- changed reply → request conflict;
- reused toolUseID в другом boot/input → отказ;
- ACK decision ≠ tool completion ≠ Task acceptance.

## 7. Attention и observability

Attention item содержит только необходимое:

- runtime/binding/session reference;
- request kind и tool name;
- bounded redacted summary;
- fingerprint и freshness;
- mode/coverage diagnostics;
- supported actions (`allow_once`, `deny`, `answer_questions`, `unavailable`).

Не публиковать raw file content, environment, credentials, полный Bash body без policy/redaction или Promise internals. Generic report reader не обходит scoped authorization.

Historical immediate deny остаётся historical result, не pending. Auto-approved call может отражаться в diagnostics/events, но не требует reply.

## 8. Критерии — ещё не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Compatible SDK/runtime update | Нет version-number отказа; required capability проверена заново. |
| Subscription route + случайный API key в parent env | Adapter не переключается молча; fail before prompt либо очищенный documented environment. |
| Auto-approved tool | Нет ложного pending callback/attention. |
| Ask/deny rule | Callback вызывается по official order; exact request виден. |
| `dontAsk` | Нет ожидания несуществующего callback; native deny отражён честно. |
| Live reply/abort в обоих порядках | Один settlement. |
| Long human delay | Official defer/persist/resume либо truthful unavailable; не восстановленный Promise. |
| AskUserQuestion multi-select/free-text | Original questions + правильный answers map; no new prompt. |
| Subagent AskUserQuestion | Не обещается; отражается native limitation. |
| Host reconnect / Node loss | Same live Node сохраняет pending; потерянный Node не replay-ит decision/input. |
| Allow ACK, tool ещё работает | Decision applied, tool/Task terminal не заявлен. |

## 9. Проверка и сдача

Один manager/worktree; writers не запускают Cargo. Порядок:

1. compatibility/auth boundary;
2. official permission mode/hook wiring;
3. live pending/reply;
4. durable defer, только если current interface подтверждён;
5. Store attention/reader/admission;
6. fixtures официальных callback/hook shapes;
7. native qualification current subscription installation.

После законченного vertical slice:

```sh
cargo clippy --locked -p swarm-adapter-claude -p swarm-contracts -p swarm-kernel-host -p swarm-mcp --lib --bins -- -D warnings
node --check crates/swarm-adapter-claude/sdk-harness/bridge.mjs
```

Broad/native tests — итоговая фаза. Сдать exact SHA, current official definitions, removed version gates, auth mode evidence, callback/defer paths и remaining unavailable capabilities. Документация не исправляет runtime сама по себе.

R05/#31 владеет result provenance; R15/#41 — Codex/Muse quota; R14/#40 — schema extraction. Их код не дублировать.