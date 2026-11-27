# Общий ACP-плагин (Gemini CLI, Goose и другие, первая волна)

> Источник фактов: целевая архитектура v2.6 (27.11.2026). Текст ниже шапки перенесён из неё дословно; «раздел N» и «приложение X» — номера v2.6, ссылка ↗ ведёт туда, где он теперь. Общая суть — [ARCHITECTURE.md](../../ARCHITECTURE.md), связи — [GRAPH.md](../../GRAPH.md), навигация — [INDEX.md](../../INDEX.md).

**Зачем.** Базовый путь для всех харнессов, которые реализуют ACP: запуск, промпт, отмена, продолжение сессии одним плагином. Gemini CLI остаётся здесь для тех, у кого он работает.

**Владеет.** ACP-адаптер: session/prompt, session/cancel, session/load и session/resume; для v2 (Preview) — session/prompt во время работы и resume с replayFrom; Gemini AfterAgent, AfterTool, BeforeModel.

**Инварианты.**
- В ACP v1 session/cancel — уведомление; подтверждение остановки приходит результатом исходного prompt со stopReason cancelled.
- В ACP v1 нет входа в текущий ход; где этого мало, нужны нативные плагины.

**Статус.** Сверено по исходникам; Gemini CLI и хуки запуском не проверены.

---

| Харнесс | Вход в текущий ход | Не дать закончить | Interrupt | Продолжение сессии |
| --- | --- | --- | --- | --- |

| Gemini CLI | AfterTool additionalContext или переписывание запроса в BeforeModel | AfterAgent deny или block: причина становится новым промптом; предел 100 ходов на промпт (client.ts:86, 1004) | новый ACP-промпт прерывает текущий ход | ACP session/load |

| ACP | v1: нет; v2 (статус Preview): session/prompt во время работы | — | session/cancel | v1 session/load и session/resume; v2 session/resume с replayFrom |

## Вердикт (раздел 10)

Вердикты по харнессам: общий ACP-плагин — базовый путь для всех, кто реализует ACP; нативный плагин Codex — ради turn/steer, turn/interrupt и thread/inject\_items; нативный плагин Claude Code — ради хуков и режима маски секретов на macOS, Linux и WSL2; облачный плагин — задача наружу, результат ветка провайдера, которую интегратор забирает в refs/swarm/cloud/ и сканирует gitleaks до того, как она станет кандидатом (раздел 10а [↗](cloud-agents.md)).

ACP уже реализуют Gemini CLI, OpenCode, Goose, а Claude Code и Codex — через адаптеры. Один ACP-плагин покрывает базовый путь: запуск, промпт, отмена, продолжение сессии. В ACP v1 нет входа в текущий ход, в v2 есть (Preview, объявлен 20.11.2026). Поэтому нативные плагины нужны там, где ACP мало: turn/steer, turn/interrupt и inject\_items в Codex, хуки Claude Code (additionalContext, Stop) и квоты провайдера.

## Остановка поколения (раздел 3)

2. Запрос остановки: interrupt или cancel харнесса. В ACP v1 session/cancel — уведомление, подтверждение приходит результатом исходного prompt со stopReason cancelled; у каждого протокола свой признак.

## Gemini CLI и Antigravity (раздел 11а)

- Antigravity CLI (agy) с 18.06.2026 заменил Gemini CLI для бесплатных пользователей и Google One. Плагин второй волны: headless -p со stream-json, хуки с таймаутом 30 с. Gemini CLI остаётся для тех, у кого он работает, через общий ACP-плагин.

## Проверено и гипотезы (раздел 13а)

| Утверждение | Статус | Чем проверено |
| --- | --- | --- |

| Хуки, steer и interrupt Codex, Gemini CLI, Antigravity CLI, OpenCode; Codex inject\_items во время хода; работа через прокси со своим CA | не проверено; сверено с документацией на 27.11 | их CLI в песочнице нет; только исходники; критерии этапов 8–10 |

| Gemini CLI: 100 ходов | проверено по коду | client.ts:86, 958, 1004: предел на один запрос пользователя; предел на сессию — model.maxSessionTurns, по умолчанию выключен (config.ts:1258); для бесплатных аккаунтов и Google One Gemini CLI заменён Antigravity CLI с 18.06.2026 |
