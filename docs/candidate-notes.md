# Кандидаты и идеи из прежнего исследования harness

**Сведено 30.09.2026 из исследования rev5 от 29.09.2026 и его intake.** Ниже сохранён отбор полезных механизмов, а не новый рейтинг и не повторный source-аудит. CODE/DOC/ISSUE в исходнике — уровни свидетельств его автора; локальная квалификация этих продуктов нами не выполнена.

Исходное исследование выбирало самостоятельный Windows harness с GUI/телефоном и Go/Muse. Наш продукт — headless-контроллер родных executors. Его обязательные маршруты, полные разрешения и выбранный General Manager не меняются этим списком. Сохранённые [доноры](agent_swarm.donors-20260929.toml) отдельно определяют кандидатов к переиспользованию кода; запись здесь не добавляет зависимости.

## Полезные механизмы

| Источник | Что стоит сохранить | Ограничение / условие дальнейшего рассмотрения |
|---|---|---|
| **Kilo CLI**, H §9 | Durable one-shot wakeup и recurring cron, связь Goal/waiting, coalescing пропущенных срабатываний, tree cancellation. | Сохранение следующего срока до send не гарантирует доставку текущего: crash может потерять wakeup. Использовать наши Operation/reconciliation. Active Goal после restart описана как paused; completion self-report. Swarm board не назначает и не будит сам. |
| **CodeGraff + Harness**, Graff 0.0.302.10 / Harness 0.2.99, H §4 | Разделение standing objective/run, holding при background jobs, one-level process children, per-child profiles, headless/ACP interfaces и one-shot schedules. | Run credits/deadline/holding не сохраняют active execution после crash. Router на рассмотренном source строил Chat Completions, не нужный Muse Responses. Лицензия названа modified AGPL: точные условия/closure требуется проверить до переноса кода. 25-turn cap — свойство донора, не наше правило. |
| **Sinew 0.1.51**, H §5 | Пример peer task/message board и наглядное разделение persisted Goal от owner продолжения. | Продолжение запускал React-effect открытого ChatPane. SQLite Goal не доказывает always-on execution; custom Go provider отсутствовал в исследованном пути. GUI/relay не переносим в core. |
| **Pi-Go 0.2.4**, H §6 | Отдельные worker processes; single/parallel/chain; protocol-per-model вместо одного endpoint для всего каталога. | Hard-coded каталог без нужной Muse; persistent Goal/scheduler не найдены. Не приписывать ему квалификацию отдельного проекта Pi. Наличие session-header пути не было подтверждено. |
| **OpenChamber 2.0.4**, H §12 | Контрпримеры stale continuation после Pause/Clear, lost wakeup при inflight tick, stranded Goal после restart, счётчик до admission. | Закрытый issue и closed-unmerged patch не означают shipped исправление. UI/mobile полезны другой задаче; второй Goal-owner нам не нужен. |
| **Delta 0.17.1**, H §8 | Worker/Scout/Reviewer, native peer messages, связанные review/history, versioned profiles, separate/shared copies. | Native toolkit не равен read-only OS policy. Поддержанный headless dispatch/goal/scheduler не установлен. Delta runtime не равен открытому Zed source; repository/conversation sync имеет облачную границу. Full-access сам по себе не отвергает продукт для владельца, но функция/лицензия и внешнее управление остаются вопросами. |
| **Muse Code + Helicon**, H §11 | MSP, native Goal/loop/workflows и готовые facade methods вместо terminal scraping. | Сохранять Meta subscription/Max. Взять фасад вместо собственной обвязки, а не поставить ещё один scheduler поверх SDK. Community custom-endpoint example не подтверждает Go auxiliary/session-header path. Точный кандидат есть в donor inventory. |
| **OpenCodex**, H §10/24 | Model-aware routing, каталог, provider/session affinity, разделение model visibility и реального transport. | Изученная безопасность 2.58.0 не аттестует native-пакеты 2.70.0. Исторические v2 encrypted delegation, provider tags/mobile, private fields и long tool names требуют version-specific проверки. Это optional inference route, не наш обязательный broker или default GM. |
| **Agent Teams AI**, H §15 | Lead/peer messaging, review obligations, stall nudges, provider-aware launches. | По H Electron/Node stack и непросмотренный exact Go/Muse путь; не готовая маленькая headless-подмена только по наличию доски задач. |
| **Vicoa / JetBrains Air**, H §13–14 | Полезное различие remote session control, task board и расписаний новых tasks. | Интерфейс не владеет native Goal автоматически; cloud credits/workspace могут отличаться от local subscription. Ни GUI, ни телефон сейчас не обязательны. |
| **Goose / jcode**, H §16 | Rust execution, recipes/scheduling либо durable initiatives и swarm. | Сохранённая initiative не доказывает auto-continuation; mixed protocol/Go-Muse paths в прочитанном source имели ограничения. Язык ядра не доказывает совместимость native подписки. |
| **Crush / VT Code / amux**, H §16 | Узкие TUI/ACP/worker или fleet-примеры. | Нужный полный Goal/durable scheduling/Windows пакет не подтверждён; amux ориентировался на tmux/macOS/Linux. Не включать в первый срез ради одинакового языка. |
| **Tatsu Code**, H §7 | Сравнение task agents и cross-agent relay, обратимой portable-установки. | Закрытый продукт; исходников-доноров не установлено. Нужные MCP/Goal/scheduler/Go-функции в H не подтверждены. |
| **SWE-agent / SWE-bench**, H §2.5/18E | Worker submits candidate; evaluator независимо проверяет именно кандидат и требования. | Research evaluator не готовый native Windows controller. При полном shell доступе прототип не обещает физически защищённый verifier без отдельной изоляции. |

## Идеи, уже включённые без дополнительных зависимостей

1. Goal сохранён, run активен, есть дети и есть расписание — отдельные факты. Native goal и наш таймер не управляют одним continuation одновременно.
2. Ожидание наблюдаемой background работы — нормальное состояние; процессный timestamp и model polling не заменяют событие результата.
3. Различать control IPC/MCP, native harness protocols и inference Responses/Messages/Chat. Контроллер не становится новым inference gateway.
4. Совместимость маршрута включает используемые auxiliary paths: children, compaction, memory, titles — если включены в данном профиле. Успешный основной chat этого не доказывает. Служебные model calls имеют видимую цену, не выдаются за бесплатный heartbeat.
5. Board message, durable admission, native delivery, ответ, изменение ownership и полезность результата — разные наблюдения. GM не должен вручную копировать реплики коллег.
6. Реализация одной native-функции не оправдывает перенос всего model loop/UI/remote stack. Берётся целая пригодная SDK/crate-единица с зависимостями и notices; fixtures не переписываются по этому конспекту.

## Где искать исходные доказательства

[Исследование H][H] содержит версии, ограничения и разделы исходников для каждого кандидата; [intake][I] отделяет применимые идеи от старых требований к GUI/телефону. Они доступны в Git history, не являются дополнительными инструкциями для кодирующего агента.

Для адресного source-read сохранены первоначальные ссылки:

- Kilo: [Goal](https://kilo.ai/docs/code-with-ai/agents/goals), [wakeup source](https://github.com/Kilo-Org/kilocode/tree/main/packages/opencode/src/kilocode/wakeup), [tool/board contract](https://kilo.ai/docs/automate/tools).
- Graff: [goals](https://github.com/justrach/codegraff/blob/main/docs/goals.md), [loop](https://github.com/justrach/codegraff/blob/main/src/loop_run.zig), [router](https://github.com/justrach/codegraff/blob/main/src/router_config.zig), [Harness](https://github.com/justrach/harness).
- Sinew: [ChatPane continuation](https://github.com/Paseru/sinew/blob/main/src/components/chat/ChatPane.tsx). Pi-Go: [provider](https://github.com/dimetron/pi-go/blob/main/internal/provider/opencode.go), [subagents](https://github.com/dimetron/pi-go/blob/main/internal/tools/subagent.go).
- OpenChamber: [runtime v2.0.4](https://github.com/openchamber/openchamber/blob/v2.0.4/packages/web/server/lib/session-goal/runtime.js), [issue #3279](https://github.com/openchamber/openchamber/issues/3279), [PR #3285](https://github.com/openchamber/openchamber/pull/3285).
- Delta: [subagents](https://delta.dev/docs/agents/subagents), [data storage](https://delta.dev/docs/privacy-and-security/data-storage), [roadmap](https://delta.dev/roadmap). Helicon: [repository](https://github.com/HarjjotSinghh/helicon).
- [OpenCodex](https://github.com/lidge-jun/opencodex), [Agent Teams AI](https://github.com/777genius/agent-teams-ai), [Vicoa](https://github.com/vicoa-ai/vicoa), [Air](https://www.jetbrains.com/air/), [SWE-agent](https://github.com/SWE-agent/SWE-agent).

Ссылки на `main` и web docs здесь не являются новыми пинами: они приведены из прежнего исследования для будущего чтения. Ни релизы, ни выводы о лицензиях в этой чистке заново не проверялись. Решение собственного прототипа и порядок C01→C02→Muse/OpenCode сохранены.

[H]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/Harness_and_OpenCode_Go_master_2026-09-29_rev5.md
[I]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.harness-intake-20260929.md
