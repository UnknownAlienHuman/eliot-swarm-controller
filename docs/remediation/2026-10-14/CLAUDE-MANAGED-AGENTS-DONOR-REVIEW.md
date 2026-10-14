# Claude Managed Agents: исходники, границы донора и применение в ELIOT

Проверено **2026-10-14**. ELIOT: `ed1df447482d87ced09b60a12b301a7b0cfc4092`.
Донор: официальный `anthropics/anthropic-sdk-go`, **v1.80.0**, commit
`bdcae90baad7d21949fc694f779377f62ff68631` от 2026-10-09.
Статус: **REFERENCE — прочитанные механизмы для адресной адаптации на Rust**, не новая зависимость и не реализованный адаптер сервиса.

## 1. Что именно проверено

Исходная [страница release notes](https://platform.claude.com/docs/en/release-notes/overview) описывает всю Claude Platform, а не один open-source проект. Для swarm-контроллера непосредственно относится запись **2026-10-09** о dynamic workflows в Claude Managed Agents: `multiagent_20261001`, beta header `managed-agents-2026-04-01`, события `workflow_run.*`.

[Managed Agents](https://platform.claude.com/docs/en/managed-agents/overview) предоставляет hosted harness. В [self-hosted sandbox](https://platform.claude.com/docs/en/managed-agents/self-hosted-sandboxes) локально исполняются инструменты; оркестрация остаётся у Anthropic, входы и результаты инструментов передаются облачному control plane. Это не возможность скачать и самостоятельно запустить весь сервис.

В проверенных официальных документах и репозиториях **не найден опубликованный исходный код hosted harness, серверной оркестрации или исполнителя dynamic workflows**. Этот вывод ограничен проведённым поиском. SDK, типы API, клиентский worker и демонстрационные приложения не являются исходниками сервера.

При этом **реальный открытый код полезного клиентского worker существует**. [LICENSE Go SDK](https://github.com/anthropics/anthropic-sdk-go/blob/bdcae90baad7d21949fc694f779377f62ff68631/LICENSE) — MIT: разрешены использование, изменение и распространение с сохранением copyright/permission notice. Это отдельная лицензия отдельного репозитория: она не распространяется на облачный сервис или коммерческий `@anthropic-ai/claude-agent-sdk`, уже используемый ELIOT. При буквальном переносе или переводе существенного кода на Rust сохранить MIT attribution/notices. В этом PR код донора не копируется, поэтому запись об установленной зависимости в THIRD_PARTY_NOTICES не добавляется.

## 2. Проверенные единицы исходников

Ссылки закреплены на прочитанном commit, а не на движущемся main. Указаны прочитанные диапазоны, не заявляется аудит всего SDK.

| Единица | Прочитанный механизм | Git blob |
|---|---|---|
| [lib/environments/poller.go](https://github.com/anthropics/anthropic-sdk-go/blob/bdcae90baad7d21949fc694f779377f62ff68631/lib/environments/poller.go), 1–245, 270–530 | `bearerReqOpts` удаляет родительский `X-Api-Key` перед scoped Bearer; `WorkPollerOptions` разделяет `Drain` и `AutoStop`; `Next` различает постоянную ошибку, backoff и отсутствие работы | `a0e3f397bb9d356ba6c3a1b59374b7b03edaed5e` |
| [lib/environments/worker.go](https://github.com/anthropics/anthropic-sdk-go/blob/bdcae90baad7d21949fc694f779377f62ff68631/lib/environments/worker.go), bootstrap и `handleItem` до подготовки memory stores | `Run` отключает poller AutoStop; cleanup принадлежит обработчику. Heartbeat запускается до медленного setup; потеря lease отменяет setup и запрещает прежнему владельцу force-stop. Один session read используется для skills/memory | `b0087b4579812fe1218a2b434c80751e2dcbd1a7` |
| [betasessiontoolrunner.go](https://github.com/anthropics/anthropic-sdk-go/blob/bdcae90baad7d21949fc694f779377f62ff68631/betasessiontoolrunner.go), 1–250, 490–760 | `DispatchedToolCall` разделяет `IsError`, `Posted`, `Confirmation`; `streamLoop` открывает live stream перед history reconcile; `idleClock` учитывает незавершённые подтверждения; `drainInFlight` не гарантирует завершение инструментов при timeout | `8769cf1e32c9aadf27f71ee6039f43bc065a5fea` |

`SessionToolRunner` исполняет запрошенные инструменты и отправляет результаты. Он не содержит серверный цикл модели. Прочитанный Go helper также не доказывает корректность всех новых dynamic-workflow сценариев: его serial-dispatch/idle предположения нужно отдельно сопоставлять с актуальным API.

## 3. Где применять, что уже есть и что исправлять

Основание ELIOT: [owner-decisions](../../owner-decisions.md), [operations donor map](../../agent-operations/donor-map.md), [delivery](../../agent-operations/delivery.md), существующие #42, #46, #65, #69, #99 и #149. Исторический donor registry из #136 не превращается в новый текущий backlog; его датированный счётчик доноров не изменяется.

### 3.1. Результат действия, доставка результата и cleanup — разные факты

**Донор:** `DispatchedToolCall.IsError` и `.Posted` независимы; ошибка инструмента не равна ошибке отправки его результата. `Drain` и `AutoStop` тоже отвечают за разные границы.

**ELIOT уже имеет** отдельные `OperationJournal`, `InteractionJournal`, `EffectOutcome`, `flush_outbox` и состояния callback (`Pending`, `ReplyIntent`, `Acknowledged`, `Rejected`, `Unknown`, `Retired`) в `crates/swarm-adapter-claude/src/lib.rs` и `interactions.rs`. Не создавать второй журнал и не заменять их Go worker. Остаток connected result-ACK/permission работы принадлежит #42/#65: сохранять native outcome независимо от ошибки ACK, а повторную доставку разрешать только по существующему exact receipt/readback контракту. `Posted=false` из RAM донора не даёт права повторить инструмент или модель.

**Конкретная действующая правка — #149, не новая Issue:** в [retention.rs](../../../crates/swarm-adapter-opencode/src/journal/retention.rs) `reclaim_acknowledged` всё ещё хранит `removal_durability_unknown` только внутри одного вызова и удаляет элемент из индекса. Следующий пустой вызов способен вернуть `complete=true` без нового подтверждения удаления.

Исправлять связанно:

- `crates/swarm-adapter-opencode/src/journal.rs::RetentionSummary`: развести факт опустошения очереди, число namespace deletions и результат подтверждения durability. Имена новых полей выбрать в текущем коде; это не внешний API донора.
- `journal/retention.rs::reclaim_acknowledged`: не превращать новый пустой проход в подтверждение прежнего Unknown; сохранять ограниченное состояние/возможность подтверждения через существующее представление, а не новый per-file ledger.
- `src/lib.rs::attempt_journal_retention`: планировать повтор по реально оставшейся или повторяемой работе, а не по отсутствию Windows directory-fsync. Отдельно показывать Unknown/неподдерживаемую гарантию, не устраивать бесконечный retry.

Использовать уже доставленный `RemovalOutcome`. Не восстанавливать ACKed файлы ради диагностики, не снимать native fences, не считать эту аналогию готовой реализацией fsync. Именно двухпроходный дефект установлен кодом ELIOT; донор предоставляет только пример раздельных осей результата, а не исправляет его автоматически.

### 3.2. Один владелец lifecycle; потеря lease не разрешает остановить преемника

**Донор:** `EnvironmentWorker.Run` передаёт `AutoStop=false`; `handleItem` после остановки heartbeat проверяет причину потери lease и не вызывает force-stop от имени бывшего владельца.

**Точка ELIOT:** `crates/swarm-adapter-claude/src/lib.rs::run_owned` и существующая родительская process-family custody из #42/#71. Текущий `run_owned` уже передаёт retirement родителю и прямо требует его доказательства departure. Не дублировать этот путь worker-local reaper или новым lease registry.

Для оставшейся интеграции #42 сохранять одно направление: точный boot/actor → принадлежащий ему native harness → сохранённые поздние факты → родительское подтверждение family departure → допустимый replacement. Ответ от старого boot не завершает Operation нового boot. Истечение чужой серверной lease и SDK drain timeout не являются доказательствами локальной смерти процесса. Это адресный reuse lifecycle-принципа, не утверждение нового дефекта в уже исправленной custody.

### 3.3. Восстановление наблюдения без окна потери событий

**Донор:** `SessionToolRunner.streamLoop`; официальный [events-and-streaming](https://platform.claude.com/docs/en/managed-agents/events-and-streaming) требует открыть stream, затем прочитать историю, затем объединить поток по exact event IDs.

**Точки ELIOT:** существующие transport/readback слои, а не Store mutation. [OpenCode guide](../../../modules/opencode/README.md) уже различает volatile SSE, bounded GET и отсутствие Last-Event-ID replay guarantee. `swarm-adapter-claude::run_owned` уже держит общий bounded frame channel. #41 не выдавать заново как отсутствие continuous reader.

При добавлении или исправлении конкретного gap path использовать один reader, cursor/snapshot и ограниченное объединение duplicate IDs; при потере coverage показывать gap. Если native API не поддерживает исторический readback, сохранять Unknown — не подставлять историю Claude Managed Agents вместо него. Не переносить полную history-перечитку и растущие RAM seen-наборы в горячий fleet path. Наличие чужого механизма само по себе не доказывает дефект текущего ELIOT reader.

### 3.4. #99 не решается общим sandbox или Claude session threads

В [multiagent orchestration](https://platform.claude.com/docs/en/managed-agents/multiagent-orchestration) агенты одной session разделяют sandbox/filesystem и vault credentials; отдельный context thread не равен отдельному ELIOT assignment grant.

Поэтому не переносить этот shared-authority подход в `store/coordination.rs` и `store/integration.rs`. #99 уже требует двух самостоятельно проверенных Task/revision/Attempt contexts, точной relation и target-addressed delivery. Исправление должно охватить `write_relevance_indexes`/`peer_find`, `normalize_send`/`send`, `inbox`/`delivery_matches_scope` и `sync_integration`. Снятие одного scope guard, project-wide ACL или alias native thread → Participant не реализует этот контракт. Подробная текущая постановка остаётся в #99; нового owner-decision ожидания или дополнительной Issue нет.

### 3.5. Не смешивать Managed Agents с текущим Claude SDK bridge

#147 уже исправлена в проверенном main: обычный `modules/claude/bridge-v5.mjs` использует pass-through `first-query-control.mjs`, а descriptor больше не требует fault directory. Не восстанавливать fault composition и не переписывать bridge под облачный API.

Адресный documentation remainder #46: в [modules/claude/README.md](../../../modules/claude/README.md) абзац, начинающийся `At agent.open, SDK 0.3.287 startup()`, всё ещё обещает snapshot-rendered первый prompt. `bridge-v5.mjs::sendNative` передаёт `taskPrompt.envelope.prompt`. Исправить этот абзац: bridge.5 передаёт Store TaskPrompt без изменений; snapshot rendering относится только к retained bridge.3/.4. Ни обновление cloud SDK, ни смена native subscription для этого не нужны.

## 4. Что не переносить

- `WorkPoller.Next` после ошибки ACK вызывает best-effort discard/force-stop. Для неизвестного исхода ELIOT это не допустимая замена retained intent/readback.
- `AutoStop=true`, idle timer и reclaim по возрасту не становятся политикой освобождения ELIOT ресурсов. У SDK `drainInFlight` может вернуться с работающими инструментами.
- `endsTurn` донора считает неизвестный stop reason, отличный от `requires_action`, концом turn. ELIOT не должен превращать неизвестный native reason в доказанный успех/завершённую family.
- SDK RAM dedupe и успешный HTTP send не доказывают exactly-once после рестарта. Широкий API key не должен сопровождать scoped bearer; этот transport-принцип не выдаёт manager authority.
- Не импортировать Go tool loop, memory sync, Anthropic control plane или отдельный HTTP service как внутренний ELIOT контроллер. Облачная интеграция — отдельный opt-in runtime, если впоследствии будет запрошена, не обязательный шаг этого PR.

Актуальные [workflow runs](https://platform.claude.com/docs/en/managed-agents/workflow-runs) особенно важны как отрицательные примеры: `status_ended` с `completed` не доказывает успешность каждого task; interrupt primary thread не останавливает workflow; подтверждение tool call может быть принято после того, как уже перестало разрешать исполнение. Не выводить Task acceptance, family-empty или выполненный tool из таких ACK/status. Корреляция требует собственных `workflow_run_id`, `phase_started_id` и `session_thread_id`, а не времени или положения в списке. Эти поля — внешние native references, не ELIOT полномочия.

## 5. Доказательства и объём поставки

Проведены чтение официальных release notes/контрактов, проверка MIT LICENSE и указанных source units, сверка текущих ELIOT source/старых Issue/PR. Поиск точной фразы Managed Agents среди Issues/PR не обнаружил прежнего такого donor review. Это не заявление о полном аудите каждого файла SDK.

Поставка содержит только этот запрошенный обзор и адресную карту. Runtime-код, зависимости, услуги Anthropic, credentials, процессы и main не меняются; новых Issue и условий остановки разработки нет. Исходники донора не установлены и не исполнялись. Go/Rust builds, native SDK/model calls и нагрузочные проверки не запускались; локального Cargo в этой сессии нет. Не заявляются производительность, экономия токенов или live qualification.

При реализации существующих задач использовать их текущего manager/worktree. Код сначала, минимальный affected Clippy на готовом срезе. В согласованной фазе проверки нужны connected границы, а не новый suite: для #149 второй вызов/reopen без нового durability evidence; для #42 lost ACK без native replay и late old-boot reply без воздействия на successor. Донорские примеры не заменяют эти ELIOT consumers и не вводят обязательную cloud-квалификацию.
