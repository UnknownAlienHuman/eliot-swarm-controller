# Harness ELIOT: существующие подписки и рабочие нативные интерфейсы

**Уточнение владельца — 8 октября 2026.** Нельзя закреплять версии внешних harness/SDK, требовать старый release, выключать штатные обновления или заменять рабочее подписочное подключение отдельным платным inference API. Это требование реализации, а не уже выполненное изменение кода. Исторические SHA/версии ниже обозначают прочитанные исходники; они не становятся условиями запуска.

## 1. Основа — материалы владельца, затем код и документация того же интерфейса

Рабочие источники: `ELIOT-Swarm-AUDIT-2026-10-06(2).md`, разделы B5, B6, B9; исходный `Launch-CommandCode.ps1` из Library; [реестр доноров](../agent_swarm.donors-20260929.toml), поле `policy.runtime_default`: installed native harness and existing authorized account. В аудите v3 §5.4 уже запрещено навязывать downgrade/фиксированный release.

Архивы `eliot-root-scripts-20261002-1030.zip` и `eliot-swarm-control-20261001.zip` найдены в Library. В этом проходе инструменты не выдали их raw bytes или распакованный текст. Их содержимое не объявляется прочитанным. Отдельный `Launch-CommandCode.ps1` прочитан полностью; для остальных скриптов ниже указаны сведения из аудита, а не вымышленное чтение их функций. Исторические параметры запуска и численные ограничения скрипта не являются нынешней политикой.

| Harness | Рабочий путь и источник | Что переносить в контроллер |
|---|---|---|
| Codex | `codex_as.py`, `Launch-Codex.ps1`; B5.4: существующий app-server и native account, профиль lane | Текущие настройки профиля, reasoning, инструменты и subagents; turn/steer; goal/readback; account notifications. Не второй inference client. |
| Muse | `muse_as.py`, `Launch-MuseCode.ps1`; B5.3: muse serve / MSP вместо неуправляемого после старта exec | Тот же subscription runtime, SDK/MSP connection, session/view, steer, вопросы, goal, адресные child controls. |
| OpenCode | `oc_run.py`, `oc_http.py`, `answer_forms_http.py`; B5.2 | HTTP действующего сервиса: prompt delivery:steer, background, form reply, native session log. Не перезапускать общий сервис ради запроса. |
| Command Code | Прочитанный Launch-CommandCode.ps1, строки файла 10–14, 28–40, 49–55 | Уже headless-менеджер с инструментами agent и agent_output; run_in_background; явные Model/Effort и рабочая директория. Batch lifetime не означает отсутствие субагентов. |
| Antigravity | B5.6: рабочий agy -p с output-format stream-json, model/effort и cwd/add-dir | Существующий подписочный CLI; native init/result, child events и завершение потока. Другой SDK не подменяет этот путь. |
| OpenCodex | B5.1/B6: уже выбранный proxy; [модуль](../../modules/opencodex/README.md) | Management/readback текущего сервиса; не новый владелец threads и не смена плана оплаты. |
| Claude | B4.2: Claude Code выполнял роль root; [текущий модуль](../../modules/claude/README.md) | Сохранить существующую нативную авторизацию; runtime controls/callbacks подключать к ней, а не предлагать оплату inference отдельно. |
| Kilo / Zed | B5.7 и B5.1: Kilo работал через свой service API; Zed оставался резервным вариантом | Не приписывать OpenCode V2 протокол Kilo; не объявлять непроверенный Zed production-ready. |

**Native API не означает платный model API.** HTTP OpenCode, MSP Muse, Codex app-server и ACP Command — интерфейсы управления harness. Наличие token/key во внутреннем протоколе само по себе не определяет тариф. Нормальное обновление авторизации выполняет сам harness; ELIOT не требует новую учётную запись или отдельный платёжный маршрут.

## 2. Версия — наблюдение, совместимость — проверка нужной функции

Установленный runtime выбирается штатным путём владельца. На подключении сохранять его reported version, протокол, доступные методы/формы и источник настроек. Не сравнивать release с единственной зашитой строкой и не менять её на другой вечный pin, диапазон или разрешительный список release-номеров.

Проверять конкретную используемую форму ответа и capability. Неизвестные необязательные поля не ломают независимые функции; отсутствие обязательной гарантии делает недоступной соответствующую операцию, не весь harness. Не изобретать capability endpoint: использовать реально существующие initialize/schema/catalog сведения и обычный адресный readback. Проверка совместимости не отправляет пробный prompt и не обходит нативные разрешения.

После штатного обновления заново разрешить путь для нового запуска/подключения и проверить нужный контракт. Старую живую сессию не перезапускать и её процесс не подменять. Не считать отсутствие старого каталога bin/<hash> основанием установить старый бинарник. В B5.4 прямо описан перенос Codex CLI приложением в новый каталог.

Не путать это с идентичностью нашей работы: точные Operation ID, target session/turn, boot и хэши уже полученных bytes сохраняются как доказательства. Они не предписывают, какую версию внешнего продукта пользователь обязан запускать. Lockfile сборки не является разрешением замораживать обновляемый пользовательский harness.

### Обнаруженные нарушения этого правила в коде

Исследован `main` 40591a295af94b1541ec2ba30afe8e3247701a71. Это source-level проверка, не воспроизведение сбоя на машине владельца.

| Участок | Ошибочная связка | Как исправлять |
|---|---|---|
| `crates/swarm-adapter-claude/sdk-harness/bridge.mjs::prepare` | packageInfo.version !== '0.3.287' → SDK_VERSION_MISMATCH ещё до проверки используемых exports | Убрать release-equality gate; проверить фактически используемый import/startup/query/callback контракт и сообщать реальную версию. R16/#42. |
| `crates/swarm-kernel-host/src/runtime/opencode_v2/mcp.rs`, `mcp_install.rs`, `src/native_mcp.rs`, `src/store/native_mcp.rs`, `src/store/launcher_mcp_tools.rs` | Несколько независимых требований 2.0.7/expected_version | Один adapter-owned контракт используемых native MCP возможностей; переключить readers/writers вместе, убрать release allowlist из host. Не удалять service/boot/target проверки. |
| `modules/antigravity-rust/src/{wire,launch,main}.rs` | REQUIRED_MODEL_ID = gemini-3.8-flash-high и equality filters | Модель берётся из текущего выбора владельца/маршрута и фактического native каталога; наблюдаемый результат отдельно. Не замена константы следующим названием и не silent model fallback. |

Эти gates пока остаются в production-коде. Нельзя назвать их исправленными одним обновлением этой документации. Исторические versions в UPDATE/vendor inventory описывают существующую реализацию; их нормативные требования freeze должны быть переписаны при подключении исправленного пути.

## 3. Сохранять полезную нативную семантику

| Потребность | Конкретный путь | Что не подменять |
|---|---|---|
| Codex steer | send_operation → native_send_payload → turn/steer с caller expectedTurnId → reconcile_send | Не сканировать всю историю для допуска. Не повторять потерянный input и не превращать steer в turn/start. R03/#29. |
| OpenCode steer при foreground-блокировке | Адресный prompt delivery:steer; при выбранном менеджером background — штатный session.background | Loop-step delivery полезна без обещания atomic turn guard. Background не cancel; не делать его автоматически только из-за тишины. |
| OpenCode вопрос | Прочитать form; ответить тому же formID по текущей схеме | Steer не заменяет ответ заблокировавшей форме. Политика ответа — от владельца, не от слова Recommended. |
| Muse child control | Нативные subagent/sendMessage, followupTask, interrupt/stop/close/resume/reopen, где поддержаны | Parent session + настоящий subagentId. Native tool taskId не наша Task. Наблюдение не захватывает child writer. |
| Command children | agent с run_in_background; agent_output для результата/статуса, как в прочитанном launcher | Не объявлять headless запуск sessionless по возможностям модели. Отдельное подключение во время уже идущего -p требует собственного контракта. |
| Claude вопрос/permission | Живой callback → scoped attention → agent.reply → однократный resolve | Не новая query/текст нового хода. Незаданный mode остаётся inherited до effective readback. R16/#42. |
| Остановка продолжения Codex | Отдельные goal clear и turn interrupt с текущими IDs, как в B5.4 | Выход клиента не прекращает серверную цель. Не выполнять stop в read-only отчёте. |

Сначала использовать существующие producers/consumers. Встроенный `runtime/opencode_v2` уже содержит forms/background/settings/log; переносить проверенные единицы в adapter, не писать третью реализацию. Partial family остаётся partial; ни parent idle, ни wrapper exit не доказывают завершение детей.

## 4. Квоты — в контексте той же подписки

R15/#41 получает account/rateLimits read + updates Codex и usage/read + usage/changed Muse на их существующих connections. Нативное время наблюдения отдельно от времени получения; buckets/windows отдельно друг от друга; usage estimate не остаток подписки, отсутствие наблюдения не ноль. Не складывать одну квоту по числу детей.

В `NativeClient::receive_response` Rust Codex сейчас теряются no-id notifications; нужен один постоянный reader с разделением replies/events/requests. В Muse уже есть onNotification — добавить initial usage read туда же, а не отдельный poller для каждого ребёнка.

`capacity::note_outcome` закрывает quota incident на Applied/Accepted без доказательства восстановления нужного окна. Убрать эту причинную ошибку, не вводя новый тарифный ledger. RateLimited, exhausted, auth-required и overload различать по нативному событию, не по тексту чужого сообщения. Никаких покупок, automatic account switching или запуска нового платного API для измерения квоты.

Antigravity CLI сам связан с подпиской и имеет credit/quota UI. Наличие TUI-команды не доказывает её допустимость в рабочем stream-json stdin; не посылать slash-команду в активный поток наугад. Получение machine-readable остатка требует проверенного интерфейса именно CLI, не замены другим SDK. Отсутствие такого поля ограничивает только отчёт о квоте, не уже рабочий запуск.

У OpenCodex имя `mintMuseApiKey` не доказывает отдельную API-оплату. Исследовать фактический путь выбранного сервиса, cached read и обычное native auth refresh; не требовать у владельца новый ключ. Явное изменение аккаунта/плана/кредитов остаётся отдельным действием, не побочным эффектом Doctor.

## 5. Доноры: проверенные границы, не новые обязательные продукты

- **Текущий реестр ELIOT:** использовать полные предусмотренные SDK/транспортные единицы; review SHA — координата, не install lock. Сначала сверять реального consumer и уже работающий путь.
- **Command ACP:** официальная страница прямо говорит, что редактор использует существующий login Command. Это вариант для интерактивного управления, не условие запуска уже работающего менеджера -p. Issue #19 не блокирует существующий Command путь. Исторический ACPX review не обязывает устанавливать ту версию; перед реализацией проверять актуальную выбранную единицу.
- **Tokio:** существующие owned channels и wait(&mut Child) полезны; внешний caller должен продолжать владеть Child. Не переносить kill-on-drop политику другого проекта.
- **CCCC / Paseo:** брать locality чтения и владение observer subscription; не второй ledger и не отмену Task при disconnect.

## 6. Поставка и проверка

Одна исправленная цепочка producer → persisted fact → consumer в существующем PR. Сначала source/документация именно выбранного подписочного интерфейса, затем код и минимальный scoped gate менеджера. Широкие tests/native — итоговая фаза. Один manager/worktree; writers без Cargo.

Рабочие скрипты не выводятся до паритета на тех же сценариях (B9 аудита). Не наследовать из исторического launcher --no-auto-update, фиксированное число детей, max-turns или старую модель как нынешние требования. Новые глобальные запреты/лимиты не добавлять.

## Источники этого уточнения

- Материалы владельца: ELIOT-Swarm-AUDIT-2026-10-06(2).md §§B5/B6/B9; audit v3 §5.4; Launch-CommandCode.ps1, Library file_000000008e2481f691cd91ba2eb98333, исходные строки 1–56. Имена локальных пользователей и весь частный script в public repository не копируются.
- [Реестр ELIOT](../agent_swarm.donors-20260929.toml), policy.runtime_default/versioning.
- [Claude literal gate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-claude/sdk-harness/bridge.mjs#L231-L241).
- [OpenCode MCP gate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/runtime/opencode_v2/mcp.rs).
- [Antigravity model gate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/antigravity-rust/src/launch.rs).
- [Command ACP: reuse existing login](https://commandcode.ai/docs/acp), проверено 08.10.2026.
- [Antigravity CLI: subscription credits](https://www.antigravity.google/docs/cli/credits/), проверено 08.10.2026.
- [Codex app-server](https://developers.openai.com/codex/app-server/) и [ChatGPT-plan usage](https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server), проверено 08.10.2026. Для действующего сервера не заводить новый auth flow по примеру страницы.
