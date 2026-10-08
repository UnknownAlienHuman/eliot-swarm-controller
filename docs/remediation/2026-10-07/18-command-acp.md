# R18. Command Code ACP: полноценный manager backend, не batch с псевдосессией

**Draft-задание, 8 октября 2026. Production-код этого блока ещё не написан.**

ELIOT исследован на `40591a295af94b1541ec2ba30afe8e3247701a71`. ACP specification прочитана на `5ed386e81033918fe5e3a0503beaa6030280666e`, Rust SDK — на `3f042ac1d8f7bc6e493098f255e55f8d25942d45`. Это координаты review, не runtime pins. Command Code подключается через установленный `cmd acp` и существующий login/subscription; адаптер не вызывает `cmd login`, не инъецирует API key, не отключает обновления и не меняет provider/account.

## 1. Результат

Добавить **новый самостоятельный Command ACP artifact** для менеджерской интерактивной работы:

- один owned `cmd acp` process на binding/worktree;
- ACP initialize + capability negotiation;
- `session/new` без model input, либо exact `session/load` при доказанном recovery;
- model/effort/mode через advertised session configuration и native readback;
- streaming prompt turn, tool/plan/reasoning updates, questions/permissions;
- explicit cancel и close, не disconnect-as-stop;
- compact family observation по native `agent`/`agent_output` tool events;
- persisted intent до каждого ACP effect и readback без prompt replay;
- result artifact/page из exact terminal turn.

Существующий batch artifact остаётся отдельным профилем для one-shot/CI. Не превращать `cmd -p` в фальшивую session и не прятать ACP за fallback: route заранее выбирает `command-batch` либо `command-acp`.

## 2. Нормативные источники

- [Command ACP](https://commandcode.ai/docs/acp): `cmd acp`, initialize → session/new → session/prompt, existing account, one process per project directory, multiple threads, model/effort/mode, permissions/questions, sessions/list/load/close.
- [Command sessions](https://commandcode.ai/docs/sessions): durable transcript per project, committed turns, resume/load, tree/checkpoints.
- [Command permissions](https://commandcode.ai/docs/permissions): vendor permission engine, deny > ask > allow, mode semantics, subagent restrictions.
- [Command agents](https://commandcode.ai/docs/agents) и [tools](https://commandcode.ai/docs/reference/tools): native `agent`/`agent_output`, background child IDs, tool names and limits.
- [ACP v1 schema](https://github.com/agentclientprotocol/agent-client-protocol/blob/5ed386e81033918fe5e3a0503beaa6030280666e/docs/protocol/v1/schema.mdx): initialize, sessions, prompt/update, permission requests, cancel/close, config options.
- [ACP Rust SDK](https://github.com/agentclientprotocol/rust-sdk/tree/3f042ac1d8f7bc6e493098f255e55f8d25942d45): generated protocol types, framing, connection roles, TCK utilities.
- ELIOT module contract, owner decisions and modularity: Operation before effect, no blind replay, one manager/worktree, adapter outside Store.

Command docs and generic ACP schema have different authority: ACP defines transport and generic fields; Command docs define Command-specific model/effort/permission/subagent semantics. Не выводить Command feature только из того, что generic ACP type теоретически существует.

## 3. Отдельный artifact, а не мутация batch

Current `swarm-adapter-command`/`command-mod-0.1.0-glue.4`:

- `agent.open` — preflight only;
- `task.dispatch` — one `cmd -p` child;
- нет durable root session;
- `agent.send/reply/configure/goal/steer` unavailable.

Это честный batch profile. Новый artifact, возможное внутреннее имя:

```text
eliot-command.acp-rust.1
```

не использует внешнюю release в имени и не требует exact Command version. Route выбирает его явно. Batch historical receipts остаются read-only; ACP не «resume» batch run.

Один `cmd acp` process обслуживает одну project directory. В R18 одна ELIOT binding/generation владеет одним ACP process и одним root Command session. Хотя ACP process умеет несколько threads, мультиплексировать разные ELIOT bindings через один stdio process сейчас не нужно: binding-scoped credential, lifecycle и worktree ownership важнее экономии одного процесса. Native subagents остаются внутри root Command session.

## 4. Процесс и SDK: взять протокол целиком, ownership оставить ELIOT

Использовать официальный Rust SDK для schema/framing/roles. Не писать ручной JSON-RPC ACP parser.

Но default `AcpAgent` connection убивает child/process group при Drop. Для ELIOT это неприемлемо: host IPC reconnect или временная ошибка клиента не должны автоматически убивать принятую native работу.

Использовать low-level `AcpAgent::spawn_process()` либо эквивалентный публичный constructor:

- SDK создаёт stdio pipes и protocol connection;
- ELIOT сохраняет raw Child/stdio owner в `CommandAcpOwner`;
- child остаётся внутри точного `swarm module-run` owner group/Job;
- host ModuleLink reconnect не закрывает ACP transport;
- explicit shutdown сначала завершает ACP session/process по контракту, затем проверяет departure;
- replacement adapter допускается только после доказанного departure прежней process family.

Не импортировать SDK Conductor как новый orchestration layer: здесь один ACP agent и один client. Conductor/Proxy остаются донорами для будущей реальной proxy-chain, не обязательной обёрткой над одной Command session.

`stderr` сохраняется bounded tail из SDK/собственного capture; stdout — только ACP. Диагностика не попадает в protocol stream. Process exit, ACP EOF и host-link loss — три разные причины.

## 5. Initialize и capabilities

### 5.1 Client capabilities

Command выполняет file/terminal operations собственными tools и permission engine; official docs прямо говорят, что editor file/terminal services не используются. Поэтому ELIOT client **не рекламирует** ACP fs/terminal capabilities, пока не реализует их как настоящие services.

Initialize request:

- latest protocol version, которую поддерживает выбранный SDK;
- bounded clientInfo;
- `fs.readTextFile=false`, `fs.writeTextFile=false`, `terminal=false`, auth terminal false, если соответствующая версия протокола содержит эти поля;
- нужные prompt media capabilities только при реальном consumer.

Initialize response сохраняет:

- negotiated protocol version;
- agentInfo;
- exact agentCapabilities;
- auth methods;
- connection generation.

Unknown optional capability не ломает unrelated method. Required unsupported capability ограничивает конкретную функцию. Если agent выбирает protocol version, которую SDK не поддерживает, connection закрывается как incompatible; это не повод запустить старый binary или сменить account.

### 5.2 Authentication

`cmd acp` использует существующий `cmd login`. Если session/new возвращает `auth_required`, observation/attention сообщает auth requirement и advertised method. R18 не вызывает login/logout автоматически, не извлекает credentials и не подставляет другой provider.

`authenticate` можно добавить позже как explicit operator action только если текущий официальный flow требует его. Первый slice не меняет account state скрытно.

## 6. Session open/recovery

### 6.1 Fresh open

`agent.open` — единственная команда, которая запускает ACP process и создаёт root session.

Порядок:

1. validate route/worktree/model/effort/mode request;
2. persist ELIOT open intent и owner identity;
3. spawn `cmd acp` under module owner;
4. initialize/negotiation;
5. `session/new {cwd, mcpServers?}` без prompt;
6. validate returned session ID and project directory;
7. persist root ID **до** configure или first input;
8. configure selected model/effort/mode;
9. wait for exact config readback/update;
10. report binding ready.

Не вызывать model для healthcheck. Empty ACP session не доказательство account/model execution, но достаточно для prepared-open после successful negotiation/configuration.

### 6.2 Recovery

Checkpoint сохраняет:

- ELIOT binding/generation/boot/service scope;
- Command ACP session ID;
- cwd/worktree identity;
- last observed config options/mode;
- unresolved ELIOT operations and exact ACP request IDs;
- compact update cursor/turn state, если ACP даёт такой stable marker.

После adapter process loss:

1. module owner доказывает departure прежнего ACP process group;
2. новый adapter запускает fresh `cmd acp` process;
3. initialize;
4. если negotiated agent advertises `loadSession`, вызвать exact `session/load` с retained ID/cwd;
5. принять streamed history и сверить root/config;
6. не отправлять исходный Task prompt повторно.

Если load unsupported или exact session missing/corrupt, binding остаётся recovery gap. **Нельзя** создавать новую session под старой generation и переигрывать prompt.

Host-link reconnect при живом adapter вообще не трогает ACP process/session.

## 7. Model, effort и permission mode

Command ACP advertises model/effort and mode through session config/mode surface. Не pin option IDs по памяти.

Нормализатор читает advertised options:

- category `model` → exact native option/value for requested model;
- category `thought_level` → requested effort, если модель его поддерживает;
- category `mode` либо v1 `session/set_mode` → requested permission mode.

Алгоритм:

1. Session/new/load returns current options/modes.
2. Найти unique supported option by category and exact value ID.
3. Persist configure Operation before setter.
4. Call native setter.
5. Wait for returned/streamed config-options or current-mode update.
6. Only then admit first Task input.

Если effort не поддержан выбранной моделью, не молча fall back к provider default: reject requested configuration с available values. Если requested model виден в full catalog, но plan его не покрывает, native refusal остаётся Rejected/ProviderCondition; адаптер не выбирает другую модель.

Mode semantics принадлежат Command permission engine. `yolo` не обходит explicit deny rules и safety breaker; ELIOT не переопределяет это локальным boolean. Owner-selected mode snapshot входит в Attempt/config evidence.

Не изменять `.commandcode/settings.json` автоматически. Project/user permissions, hooks, MCP servers and custom agents — native configuration владельца. ELIOT только наблюдает effective options и принимает explicit configure operation, если protocol поддерживает.

## 8. Prompt turn и streaming fold

`task.dispatch`/`agent.send next_turn` используют `session/prompt` на exact root session. Generic ACP не даёт Codex-like expected-turn steer, поэтому Command ACP в R18 заявляет `agent.send/next_turn`, **не** `agent.send/steer`.

### 8.1 Intent before request

Persist:

- ELIOT Operation/binding/generation/session;
- canonical content blocks и digest;
- ACP JSON-RPC request ID/connection generation;
- selected config snapshot;
- immutable Task dispatch receipt where applicable.

ACP request ID коррелирует reply в текущем connection, но не объявляется cross-restart idempotency key. Lost `session/prompt` reply не разрешает повтор.

### 8.2 Updates

Client-side `session/update` fold использует generated SDK enum, не strings/regex. Сохранять compact state:

- user/agent message chunks with bounded assembly;
- reasoning/thought progress as diagnostic, not hidden-chain export;
- tool call start/update/terminal;
- plan/todo update;
- current mode/config options;
- usage/context/cost fields, если конкретная negotiated schema их предоставляет;
- stop reason / terminal response.

Raw high-volume deltas не копировать в Store целиком. Full final assistant result — separate artifact/page. Unknown extension update сохранять bounded diagnostic count/type, но не ломать весь session, если SDK contract разрешает extension.

`session/prompt` response/terminal stop reason доказывает завершение turn, не Task acceptance. Applied Task dispatch требует native admission according to exact ACP/Command evidence; final result/acceptance отдельно.

### 8.3 Lost reply/recovery

ACP schema не даёт универсальный caller-owned prompt idempotency key. Не resend prompt после EOF/timeout.

После exact session/load можно пытаться сопоставить retained input только по тем stable IDs/metadata, которые реально возвращает Command/ACP. Если protocol history не сохраняет ELIOT request identity и identical text не unique, outcome остаётся Unknown. Не внедрять видимый маркер в пользовательский prompt без отдельного approved contract.

## 9. Permission requests и вопросы

ACP `session/request_permission` — server request, а не notification. Reader loop не должен блокироваться в ожидании менеджера: request помещается в pending map, а RPC handler ждёт oneshot/deferred; отдельный reader продолжает принимать updates.

Attention item сохраняет:

- ACP request ID / session ID / connection generation;
- exact tool call update/title;
- all native permission option IDs, kinds and labels;
- canonical fingerprint;
- source tool/call IDs, если присутствуют;
- observed_at/freshness.

`agent.reply` выбирает **native option ID**, а не строку, угаданную из label. Перед response: current binding authority, exact live session/request/fingerprint and option membership.

### 9.1 First slice policy

Command UI предлагает allow once, remember/always, reject once/always. Persistent/remembered option меняет native permission policy. Generic one-shot reply в R18 допускает только options, которые native request классифицирует как one-time allow/reject.

Persistent options остаются unsupported before response, пока не появится отдельная explicit policy mutation с stronger authority и readback effective rules. Не считать label `always` достаточным contract — использовать typed option kind из ACP.

Если `ask_user_question` или plan approval приходят через тот же ACP permission request, ELIOT показывает их как native choice request. Не автоответить «первым вариантом» без owner policy. Official Command headless auto-answer behavior не переносить в ACP manager route.

### 9.2 Cancel obligations

Перед `session/cancel` клиент обязан завершить pending permission requests в соответствии с ACP cancellation contract. Реализовать один ordered cancel transaction:

1. freeze pending request set;
2. persist cancel intent;
3. respond cancelled to each exact pending request using protocol outcome;
4. send `session/cancel` notification;
5. await prompt response/updates with Cancelled stop reason;
6. record unresolved items/gaps separately.

Не закрывать pending requests при host IPC disconnect. Adapter/ACP connection остаются live.

## 10. Explicit cancel и close

Нужны два разных control effects:

- `native.command.cancel_turn` — cancel current prompt turn; session remains loadable.
- `native.command.close_session` либо common retire path — native `session/close` only if advertised; cancels ongoing work and frees active session resources.

Оба доступны только Manager/GM/Operator with current binding authority. Disconnect/observer unsubscribe не вызывает ни один.

Cancel — ACP notification, поэтому нет обычного RPC ACK. Evidence: original prompt returns `StopReason::Cancelled` and terminal updates. Lost connection after notification → Unknown, no automatic second cancel unless native contract explicitly makes it safe.

Close — method gated by `sessionCapabilities.close`. After close, binding becomes retired; no send/resume under same generation. Session/delete — destructive retained-history action and **not included** in first slice.

## 11. Native subagents: observe, do not invent direct controller API

Command subagents are calls to native tools:

- `agent` starts foreground/background worker; background result includes `agent_id`;
- `agent_output` waits/status/kills by `agent_id`;
- subagents cannot spawn deeper subagents.

ACP exposes tool call/update rows, not a dedicated universal child-control RPC. Therefore R18:

- detects exact `agent` tool calls from typed ToolCall updates;
- parses documented structured result for `agent_id` when available;
- links child to parent session/tool call/turn;
- observes later `agent_output` calls/status/result;
- reports family coverage partial when native ID/status unavailable;
- does **not** claim direct `subagent.stop/send` capability.

To control a child through `agent_output`, the native manager/model must invoke the tool. ELIOT may send a new explicit next-turn instruction asking manager to act, but this is a new model turn, not direct child control and not hidden automatic kill. Do not advertise it as exact native stop.

Route/manager policy may set target use of custom agents in task instructions, but Command decides delegation. ELIOT does not edit agent Markdown files or enforce pool size through private session files in R18.

## 12. MCP/tools boundary

Command loads its own MCP servers and, in an ACP editor, can also consume client-provided servers. R18 first slice advertises no client MCP servers unless explicitly selected and implemented. This prevents accidental duplication/name shadowing.

If ELIOT later supplies MCP servers over ACP:

- use existing ELIOT MCP profile and exact permission policy;
- detect same-name collision; Command-owned config wins per official docs unless product contract says otherwise;
- no auth tokens in Task prompt;
- server failure is bounded attention, not session death where Command docs say nonfatal.

Command performs filesystem/terminal actions through its own tools/permission engine. ACP client must not proxy them through ELIOT generic shell and thereby bypass native permission rules.

## 13. Quota, errors and cost

Command ACP docs state credits/limit/sign-in failures carry native API status/code. Normalize structured evidence into existing ProviderCondition work, but R18 does not implement account balance polling or routing policy.

Distinguish:

- auth_required;
- rate/usage/credits limit by actual structured native code;
- model/plan unavailable;
- overload/retryable;
- protocol/transport failure;
- maxTurns on native subagent as child outcome, not provider quota.

Do not parse human text unless official contract exposes no structured field; such fallback remains `Unknown{raw_class}` and must be fixture-backed. Cost/context meter is turn/session observation, not billing balance.

## 14. Host/Store changes

New artifact capabilities, only after actual implementation:

```text
agent.open
agent.send/next_turn
task.dispatch
agent.reply
agent.configure
agent.refresh
agent.reconcile
agent.result
native.command.cancel_turn
native.command.close_session (only when native close advertised)
```

No `agent.send/steer`, no direct child control, no goal until official Command ACP contract for those semantics is proven.

Store/descriptor:

- exact command schemas and selected capability gate;
- unknown native method fail-closed;
- route options: workspaceRoot plus requested model/effort/mode; no external version pin and no fixed model equality;
- pre-input barrier waits for session/config readback;
- pending ACP permission requests appear in existing attention with exact fingerprint;
- compact `module.observe` includes connection/session/config/turn/tool/family state.

One root session per binding. A second live ACP root for same worktree lease is conflict/attention, not implicit adoption.

## 15. Code ownership and migration

Preferred implementation placement:

```text
crates/swarm-adapter-command-acp/
  owner.rs       # child/stdio/process-family lifecycle
  protocol.rs    # SDK connection/client implementation
  session.rs     # open/load/configure/prompt/cancel/close
  fold.rs        # session/update compact fold
  interactions.rs# permission deferreds + agent.reply
  journal.rs     # shared ELIOT intent/outbox primitive when adapter-kit exists
  result.rs      # exact terminal/result paging
```

Do not bolt ACP state into existing batch `native.rs` and turn every function into `if batch else session`. Shared durable Operation/outbox code may later move into a small adapter SDK, but R18 must have live callers before extraction.

Batch route remains explicit. After ACP qualification, manager workloads move to ACP; batch stays only for declared one-shot consumers. Remove contradictory README claims that Command itself is sessionless, while retaining accurate batch artifact docs.

No dependency on `modules/command` JS bridge. Historical JS/batch evidence remains readable; new ACP writes use the new artifact.

## 16. Порядок реализации

### Slice A — owned ACP transport

1. new crate/artifact/descriptor/route example;
2. `spawn_process` + retained Child under module owner;
3. initialize/capability/auth observation;
4. bounded reader/writer/stderr;
5. host reconnect without ACP teardown.

### Slice B — prepared session

1. session/new + root checkpoint;
2. config option/mode projection;
3. configure model/effort/mode + readback;
4. session/load recovery after verified process departure;
5. no fresh-session fallback.

### Slice C — prompt/update/result

1. task.dispatch/next_turn intent;
2. session/prompt streaming fold;
3. exact terminal outcome/result artifact;
4. lost-reply Unknown/recovery from exact history only;
5. agent.result paging.

### Slice D — interactions and stop

1. server permission request deferred map;
2. attention/fingerprint/agent.reply one-time options;
3. explicit cancel with pending-response obligations;
4. session close/retire capability;
5. child observations from native tool calls.

Каждый slice содержит production caller. Не сливать transport-only crate без open/session consumer.

## 17. Критерии итогового кандидата — пока не выполнены

| Сценарий | Требуемый результат |
|---|---|
| Host IPC reconnect во время ACP turn | ACP process/session/turn живы; нет cancel или нового prompt. |
| Adapter crash, ACP group departed, loadSession supported | Exact same session loaded, history replayed, no prompt replay. |
| loadSession unsupported/missing root | Recovery gap, не новая session под старым binding. |
| Requested model/effort/mode available | Set + native readback before first input. |
| Effort unsupported / model not in plan | Explicit reject; no silent fallback/model switch. |
| Permission request while tool updates continue | Reader не deadlock; attention available; updates continue. |
| Exact allow-once/reject-once | One native option response; durable outcome. |
| Persistent option through generic reply | Rejected before response. |
| Cancel with 2 pending permissions | Both receive protocol cancel outcome; prompt ends Cancelled or remains explicit gap. |
| Host disconnect with pending question | Request not auto-denied/approved; reconnect keeps same native deferred. |
| Background `agent` tool returns ID | Child linked to parent/tool; family partial if status unavailable. |
| Attempt to direct-stop child via nonexistent ACP method | Capability unavailable; no fake success/new prompt. |
| Lost session/prompt response | No resend; exact history reconcile or Unknown. |
| Close unsupported / supported | Capability gap / exact retire; no session/delete. |
| Existing batch route | Unchanged one-shot behavior; no accidental ACP fallback. |

Fixtures use ACP transcript/TCK with fake Command-shaped updates; no model calls. Separate native qualification uses current installed subscription-backed Command and records negotiated protocol/capabilities, not an allowlisted release.

## 18. Gates и сдача

Один manager/worktree; writers без Cargo. После полного slice manager выполняет scoped formatting и минимальный warnings-denied Clippy для нового crate, contracts, host, MCP/CLI and process owner. Exact package names определяются после создания crate; broad tests/native later.

Минимальная сдача:

```text
candidate SHA
new artifact and selected route
actual initialize/session/config/prompt/permission/result callers
process/session recovery boundary
batch-vs-ACP routing and no fallback
commands and exit statuses actually run
remaining unavailable capabilities
```

Docs/fixture success не является live Command qualification. Эта поставка не меняет product code, current Command sessions, login, settings or subscriptions.
