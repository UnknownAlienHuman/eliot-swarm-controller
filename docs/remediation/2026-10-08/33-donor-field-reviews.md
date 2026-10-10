# R33 — отзывы пользователей о донорах и ELIOT adoption gates

**Исследованный ELIOT:** `40591a295af94b1541ec2ba30afe8e3247701a71`
**Дата внешней проверки:** 2026-10-08
**Статус:** research/implementation handoff. Production-код не изменён.

## 1. Метод

Проверены не только README, но и воспроизводимые issues, self-host/production reports, независимые отзывы и признанные maintainers ограничения. Issue tracker переоценивает долю отказов, поэтому вывод — не рейтинг продукта, а ответ на три вопроса:

1. какой механизм реально полезен ELIOT;
2. какой failure boundary надо сначала воспроизвести;
3. какой продукт или слой нельзя переносить целиком.

Вердикты:

- **TAKE** — законченный маленький production slice;
- **SPIKE** — сначала изолированный прототип и фикстуры;
- **REFERENCE** — только механизм или корпус отказов;
- **DO NOT IMPORT** — не переносить движок целиком.

## 2. Сводка

| Донор | Что реально ценно | Что ломалось у пользователей | Вердикт |
|---|---|---|---|
| ACP Rust SDK | Client/Agent/Proxy/Conductor, version guards | restore routing и replay до response; Windows console popup | TAKE + SPIKE |
| ractor | local supervision и Kill→Stop→Supervision→Work | per-actor memory; cluster not production-ready | TAKE semantics |
| Kingfisher | Rust rule corpus, fingerprints, validation fixtures | false-negative validation, JSON count drift, `--jobs 1` hang | TAKE corpus only |
| AgentGateway | data-only CEL policy | unavailable phase fields, claim arrays panic, one backend kills fanout | SPIKE |
| MCPProxy | progressive disclosure, approval snapshot, quarantine | BM25 14% Top-1/916 tools; stale index after approval | TAKE quarantine; benchmark search |
| Langfuse | trace UI, prompt/version/evals | ClickHouse/migrations, OTLP drops/drift, unbounded query overload | sink/UI only |
| OpenTelemetry Rust | vendor-neutral OTLP | shutdown deadlocks, span loss, runtime coupling/leak reports | bounded exporter spike |
| DBOS | step memo, durable ID before effect | empty-ID ghost forks, offset-before-intent, drain/replay races | REFERENCE |
| Restate | awakeables/durable correlation | restart resumes immediately, memory/failover, active-but-stuck partition | REFERENCE |
| portable-pty | PTY abstraction | Windows read/EOF/order and ConPTY cursor behavior | SPIKE |
| alacritty_terminal | VT parser/state | parser historically coupled to GUI; no process ownership | parser spike only |
| Landlock | unprivileged filesystem confinement | BestEffort silently degrades to no sandbox | capability semantics |
| seccompiler | compact seccomp BPF compiler | Linux/arch/vDSO constraints; repo moved to monorepo | Linux spike only |
| Snyk Agent Scan | MCP/skill threat taxonomy | scan executes commands/network; CLI contract experimental | taxonomy/fixtures only |
| CCCC | stored/delivered/read/replied separation | whole-ledger bootstrap memory; disabled actors revived; PTY dialogs | TAKE message semantics |
| Paseo | owned subscription/detach cleanup | one hung provider blocks import; sessions disappear | lifecycle shape only |
| Goose | verified bytes + base directory DTO | headless scheduler, false success, provider/tool parity | TAKE DTO only |
| Claw | advisory council+dissent, worktree separation | wrong resume node, predecessor survives handoff, regex CVE | advisory separation only |

## 3. ACP Rust SDK

Полезны typed roles, Conductor/Proxy extension boundary и strict version guard. Issue [#323](https://github.com/agentclientprotocol/rust-sdk/issues/323) показал главное правило restore: route должен быть установлен **до** публикации `session/load|resume`, иначе replay notification может прийти до response и потеряться. На failure/cancel route должен быть удалён. Issue [#214](https://github.com/agentclientprotocol/rust-sdk/issues/214) закрепляет Windows spawn policy.

Взять:

```text
route install → restore effect → response/early events → route cleanup
```

Обязательные тесты:

- `acp_restore_routes_before_response`;
- `acp_cancel_removes_only_new_route`;
- `acp_version_guard_rejects_cross_protocol`;
- `acp_windows_child_has_no_console_window`.

Не использовать production `without_acp_version_guard`; не связывать core с draft protocol v2.

## 4. CCCC

Сильная часть — разные durable facts:

```text
stored ≠ runtime delivery ≠ read ≠ reply ≠ cancelled obligation ≠ completion
```

Полевые reports:

- [#125](https://github.com/ChesterRa/cccc/issues/125): первый bootstrap каждого MCP bridge сканировал весь ledger; 58 MB ledger дал около 207 MiB PSS на bridge, 35 bridges — 3.80 GiB PSS. Это retained whole-ledger cost, не accumulating leak.
- [#98](https://github.com/ChesterRa/cccc/issues/98): `enabled=false` терялся после restart или соседних writes, а `@all` считался accepted для disabled actors без процесса.
- [#95](https://github.com/ChesterRa/cccc/issues/95): bracketed-paste ошибочно считался prompt readiness; Enter подтвердил Claude startup dialog `No, exit`.

Взять message semantics и cursor-local unread scan. Не брать JSONL ledger, scheduler и PTY escape heuristics.

Тесты:

- disabled или paused recipient не входит в broadcast;
- restart не воскрешает disabled state;
- unrelated actor update не перезаписывает состояние;
- inbox summary зависит от unread tail, не всей истории;
- vendor SessionStart или hook, а не bracketed-paste, допускает delivery.

## 5. ractor

README задаёт правильный lifecycle priority: Kill, Stop, SupervisionEvent, ordinary work. Actor может быть supervisor без отдельного global system. Но `ractor_cluster` прямо не production-ready. Issue [#262](https://github.com/slawlor/ractor/issues/262) фиксирует, что actors первоначально потребляли excessive memory; maintainers удаляли maps, monitors и broadcast sender.

Переносить сначала семантику и тесты, не dependency:

- kill interrupts current async work;
- stop ждёт текущую работу и выигрывает перед следующим user message;
- supervision flood не starve stop;
- ordinary mailbox bounded или backpressured;
- actor-count memory benchmark на ELIOT масштабе.

Cluster не брать.

## 6. Tool discovery и policy

### 6.1 MCPProxy

Нужны approved immutable tool snapshot, schema digest, quarantine и progressive `retrieve_tools/describe_tool`.

Собственный benchmark MCPProxy сообщил pure BM25 Top-1 около **14%** при 916 tools; hybrid и reranking лучше. Значит search — usability, не authority. Issue [#873](https://github.com/smart-mcp-proxy/mcpproxy-go/issues/873) показал stale index после approval.

До интеграции нужен ELIOT benchmark:

- exact method name Top-1 = 100%;
- 100–200 annotated natural-language queries;
- role, profile, task и cwd filtering **до** ranking;
- approval атомарно обновляет index;
- schema, auth и catalog revision инвалидируют cache;
- quarantined tool не появляется через другой index.

### 6.2 AgentGateway

CEL-подобный data-only policy полезен, но context обязан быть phase-typed.

Reports:

- [#1228](https://github.com/agentgateway/agentgateway/issues/1228): array JWT claim мог panic;
- [#3092](https://github.com/agentgateway/agentgateway/issues/3092): `mcp.tool.arguments` разрешался в фазе, где arguments недоступны; rule никогда не match;
- [#981](https://github.com/agentgateway/agentgateway/issues/981): claims исчезали при сочетании policy layers;
- [#1266](https://github.com/agentgateway/agentgateway/issues/1266): один dead backend валил всю multi-target session;
- [#1506](https://github.com/agentgateway/agentgateway/issues/1506): sequential fanout суммировал latency.

Нужны compile-time schemas `ingress/tool_list/tool_call/result`, rejection unavailable field, no panic arrays/maps/null, explicit merge precedence и per-backend partial failure.

CEL не заменяет Store object authorization.

### 6.3 Kingfisher

Брать Rust rules, scanner и fingerprints как второй слой поверх Atlas. Не принимать CLI verdict.

Reports:

- [#433](https://github.com/mongodb/kingfisher/issues/433): wrong live-validation logic;
- [#449](https://github.com/mongodb/kingfisher/issues/449): aggregate JSON findings расходился с items;
- [#469](https://github.com/mongodb/kingfisher/issues/469): remote Git scan зависал с `--jobs 1`.

Для каждого imported rule: known-live fixture, one-character mutation, revoked/expired/permission-denied split, no network by default, exact target provenance и pinned bundle version.

### 6.4 Snyk Agent Scan

Брать только taxonomy и adversarial fixtures. README прямо предупреждает: raw CLI output experimental; scan MCP config может запускать команды и network requests. Issue [#392](https://github.com/snyk/agent-scan/issues/392) показывает false positives на обычной repository documentation.

Scanner сам является effect и запускается только в disposable sandbox с consent. ELIOT не парсит нестабильные score или risk names как contract.

## 7. Observability

### 7.1 Langfuse

Пользователи ценят trace visualization, prompt versioning, eval datasets и self-hosting. Независимый review одновременно отмечает slow bulk queries и менее зрелые complex/high-traffic workflows.

Operational reports:

- [#13859](https://github.com/langfuse/langfuse/issues/13859): unbounded trace query перегружал ClickHouse;
- [#13920](https://github.com/langfuse/langfuse/issues/13920): OTLP `tools` array silently dropped observation за HTTP 200;
- [#14889](https://github.com/langfuse/langfuse/issues/14889): token aggregate null;
- [#12371](https://github.com/langfuse/langfuse/issues/12371): duplicated metadata;
- [#11924](https://github.com/langfuse/langfuse/issues/11924): missing ClickHouse table ломала traces page.

Использовать только secondary sink или UI после Store commit. Sink unavailable не откатывает Operation. Queue bounded, drops counted, query time/row/byte bounded, replay idempotent, PII redacted.

### 7.2 OpenTelemetry Rust

Брать `tracing` и explicit bounded OTLP/HTTP exporter. Не тащить global provider или auto-instrumentation в core.

Reports: [#3542](https://github.com/open-telemetry/opentelemetry-rust/issues/3542) lifecycle/shutdown umbrella, [#3176](https://github.com/open-telemetry/opentelemetry-rust/issues/3176) current-thread force_flush deadlock, [#2978](https://github.com/open-telemetry/opentelemetry-rust/issues/2978) gRPC span loss, [#2778](https://github.com/open-telemetry/opentelemetry-rust/issues/2778) memory leak report.

Тестировать current-thread и multithread shutdown deadlines, HTTP/gRPC differential counts, short-lived process flush, drop counters и 24h soak. Exporter никогда не работает на Store writer thread.

## 8. Durable execution

### 8.1 DBOS

Брать stable nonempty ID, durable intent до offset или dispatch, per-step memo и explicit replay boundary.

Reports:

- [#759](https://github.com/dbos-inc/dbos-transact-py/issues/759): empty workflow ID ghost-forked duplicate every recovery pass;
- [#733](https://github.com/dbos-inc/dbos-transact-py/issues/733): Kafka offset before durable workflow creation;
- [#785](https://github.com/dbos-inc/dbos-transact-py/issues/785): deactivate не остановил dequeue thread;
- [#762](https://github.com/dbos-inc/dbos-transact-py/issues/762): completed async workflow body re-executed;
- [#761](https://github.com/dbos-inc/dbos-transact-py/issues/761): OAOO read outside retry lock;
- [#767](https://github.com/dbos-inc/dbos-transact-py/issues/767): cancellation race silently lost.

Каждый ELIOT effect должен иметь crash matrix C0…C5: before intent, after intent, effect accepted или reply lost, reply before result commit, result before source cursor, cancel или drain race.

DBOS runtime не импортировать.

### 8.2 Restate

Брать awakeable или interaction ID minted before dispatch, attach/readback и ingress outcome taxonomy.

Reports:

- [#4312](https://github.com/restatedev/restate/issues/4312): restart сразу возобновляет pending work, без paused inspection;
- [#4354](https://github.com/restatedev/restate/issues/4354): invoker memory budget;
- [#5033](https://github.com/restatedev/restate/issues/5033): leader принимал appends, но не применял log;
- [#5152](https://github.com/restatedev/restate/issues/5152): retry contract должен различать infrastructure admission и terminal invocation failure.

Нужны paused recovery, bounded gradual resume, health по applied progress и states `not_admitted/admitted_unknown/terminal_failed`. Restate server не брать.

## 9. Terminal driving и sandbox

### 9.1 portable-pty + alacritty_terminal

Разделять PTY transport и VT parser. Windows reports [#1396](https://github.com/wezterm/wezterm/issues/1396) и [#4784](https://github.com/wezterm/wezterm/issues/4784) показывают различия read/EOF/order и ConPTY cursor inheritance. Parser correctness не доказывает process/session ownership.

Сначала Windows transcript suite: live read, EOF descendants, resize/cursor, alternate screen, bracketed paste/startup dialogs, split UTF-8, bounded capture, group kill, differential replay.

### 9.2 Landlock + seccompiler

Landlock BestEffort может означать отсутствие sandbox — см. [OpenShell #584](https://github.com/NVIDIA/OpenShell/issues/584). Нужен typed `requested/available/enforced/abi/degradation_reason`; hard-required отказывает start, best-effort пишет explicit degraded fact.

`seccompiler` — Linux или architecture-specific second layer. Standalone repo archived и перенесён в rust-vmm monorepo ([#76](https://github.com/rust-vmm/rust-vmm/issues/76)). Тестировать kernel, architecture, vDSO, network, file и exec matrix. Windows имеет отдельный Job, ACL или AppContainer path.

## 10. Paseo, Goose и Claw

### Paseo

Брать `SessionDelivery` ownership, detach cleanup и demand tracking. Issues [#2574](https://github.com/getpaseo/paseo/issues/2574) и [#2512](https://github.com/getpaseo/paseo/issues/2512) показывают disappearing sessions и global spinner, когда один provider завис. Тест: one bad provider не блокирует другие; cancel joins children; missing session — tombstone; external answer resolves stale form; startup failure cleans terminal, process и UI.

### Goose

Брать `ValidatedScheduleRecipe`: verified bytes и original base dir, передаваемые одним typed object. Не брать scheduler или model loop.

Issues [#11051](https://github.com/aaif-goose/goose/issues/11051), [#10765](https://github.com/aaif-goose/goose/issues/10765), [#10784](https://github.com/aaif-goose/goose/issues/10784): mid-stream failure reported success, headless scheduler ждёт client, structured-output tool отсутствовал на local provider path. Нужен headless=interactive contract suite, exact failure propagation, no overlap, provider tool-set parity и explicit unsupported capability.

### Claw

Брать advisory recommendation и dissent, а также worktree discipline. Не брать consensus authority или workflow engine.

Issue [#117](https://github.com/Enderfga/claw-orchestrator/issues/117): resume выбирал первый pending node, а не реально running boundary. [CVE-2026-10291](https://github.com/advisories/GHSA-95f6-rfpg-c3w8): ReDoS в regex validation. Тестировать exact resume identity, predecessor fencing, capability discovery и safe search expressions.

## 11. Порядок внедрения

### A. Маленькие production slices

1. CCCC message facts.
2. MCPProxy approval и quarantine.
3. ACP stable-v1 restore routing.
4. Goose validated input DTO.
5. Claw recommendation и dissent separation.

### B. Isolated spikes

6. Phase-typed CEL.
7. ractor lifecycle priority.
8. Bounded OTLP exporter.
9. Windows PTY или VT harness.
10. Landlock или seccomp capability matrix.

### C. Fixture или reference only

11. Kingfisher rule corpus.
12. Snyk threat taxonomy.
13. DBOS или Restate crash-ordering corpus.
14. Langfuse optional trace sink.

## 12. Общий acceptance rule

Каждый imported mechanism обязан в одной поставке:

1. назвать текущую дублирующую или кривую ответственность ELIOT;
2. подключить реальный producer → persisted fact → consumer path;
3. добавить donor-derived failure fixture;
4. удалить прежнюю копию или shortcut;
5. не расширить authority;
6. пройти scoped formatting и warnings-denied Clippy после connected code.

Запрещены новые framework-first PR, где donor abstraction появляется раньше живого consumer.