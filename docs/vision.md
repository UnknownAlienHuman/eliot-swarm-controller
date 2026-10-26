# Vision

## Owner statement (source of truth)

Quoted as given by the owner on 2026-10-25. If another document contradicts it, that
document is a bug.

> 1. Проект модульный, довольно легкий.
> 2. Он запускает чужие (вендорские) harness и управляет ими.
> 3. В том числе поддерживает прокси OpenCodex и ему подобные (то есть может запускать harness с прокси).
> 4. Не только запускает, но и мониторит в реальном времени разными методами как работу агентов, так и их жизнь (процессы), Git активность и т.п.
> 5. Не только запускает и мониторит, но еще и управляет, например делает hooks, steer, abort, inject и т.п.
> 6. Делает это как по команде, так и в автоматическом режиме. В том числе может ставить свой Goal, а также:
> 7. Программируется. Мы можем задавать actions (например сделать fetch, push, launch и т.п.), реагируя на какие-то действия агентов, либо по таймеру (привет cron).
> 8. Есть общая шина и унификация. Основной агент не подбирает команды и спец.термины для каждого конкретного harness. Он просто рассылает команды, а наш CLI сам переводит их (с помощью плагинов) на язык и стандарт нужного целевого harness.
> 9. Поддержка любого количества агентов (хоть миллион), а также дети разных уровней.
> 10. Общение агентов. Агенты могут минуя менеджера договориться между собой и согласовать какие-то нюансы по проекту (например общие переменные и т.п). Агенты могут запросить у CLI кто конкретно над таким участком работает (проверка базируется на Git).
> 11. Все это на Rust, легко и быстро. Плагины на этапе обкатки и скрипты могут быть на других языках, в будущем переведем на Rust.
>
> Система модульная, должна разрабатываться слоями (логическими контурами). Сначала пишем код и проверяем, что ядро и шина и т.п. работают. Потом подключаем какой-то один harness. Отлаживаем систему, пишем мониторинг. Потом остальные harness. Потом скрипты и т.п.

## What Eliot is

Eliot is a small local controller for vendor coding-agent harnesses. It launches,
observes and controls them through one harness-neutral language, automates bounded
reactions, and lets agents coordinate directly. Native model loop, tools, context,
subagents, permissions and detailed history stay native.

## Requirements

| ID | Requirement | Owner point | Layer |
| --- | --- | --- | --- |
| R1 | Small modular core; harness-specific code in plugins; one `eliot` binary; explicit [code budgets](roadmap.md#code-budgets). | 1 | all |
| R2 | Launch a supported headless harness with explicit profile/model/permissions/workspace/environment/args. | 2 | L2, L4 |
| R3 | Launch through explicit OpenCodex/similar provider proxy, attached or managed. | 3 | L2 |
| R4 | Real-time native/process/Git/filesystem monitoring with source freshness/gaps; unknown never becomes zero. | 4 | L3 |
| R5 | Semantic send/steer/inject/interrupt/stop/resume/request/configure/goal controls, rejecting unsupported semantics. | 5 | L1–L4 |
| R6 | The same controls work explicitly and through rules/timers/goals; native and Eliot goals are independent. | 6 | L2, L6 |
| R7 | Programmable committed-event/time actions, built-in Git/agent actions and scripts. | 7 | L6 |
| R8 | One bus/command language; plugins translate native protocols. | 8 | L1 |
| R9 | No logical architecture cap on registered agents/child depth; configurable accident guards and honest live-host limits. | 9 | L1, L5, L7 |
| R10 | Direct addressed mail/shared variables and Git-backed `who works here`. | 10 | L3, L5 |
| R11 | Rust core; bring-up plugins/scripts may use other languages and retained ones can be ported later. | 11 | L8 |

## Non-goals

- No Task/Issue/review/acceptance/publication engine. Such project workflows are
  composed from the bounded [automation primitives](spec/automation.md), agents and
  scripts.
- No multi-user security platform. One local OS user is the trust boundary; tokens
  fence stale generations and attribution. Remote access requires a later real auth
  boundary.
- No GUI in core. CLI/JSON/live table use the same API; later UI is a client.
- No proof ceremony: no document hashes, proof packets, audit registers or readiness
  report hierarchy.
- No promise that documentation alone eliminates runtime defects. Release requires the
  feature-complete L7 real-path qualification campaign.

## Product acceptance

The product is ready only when the L7 suite and current owner-machine live runs prove:

1. Codex launches through OpenCodex and native/process/Git/child activity is visible.
2. The same semantic controls work on required harnesses; unsupported verbs reject
   clearly without hidden fallback.
3. A manager launches/controls/waits for workers on different harnesses using only
   Eliot verbs and exact generations.
4. Event rule push and periodic fetch run 24 hours without missed firing admission,
   duplicate external writes or blind replay after uncertainty.
5. Eliot Goal reaches a passing check or bounded budget/no-progress terminal state and
   recovers correctly across restart.
6. Two agents exchange one addressed message/CAS variable and `who` identifies current
   file-area work without false lock claims.
7. 10,000 fake agents maintain bounded memory/WAL and responsive paginated CLI.
8. Windows exact custody and Linux exact/degraded behavior match their advertised
   guarantees; no cross-agent or attached-service kill.
