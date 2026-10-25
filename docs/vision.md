# Vision

## Owner statement (source of truth)

Quoted as given by the owner on 2026-10-25. If any other document contradicts it,
this statement wins and the other document is a bug.

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

Eliot is a small local controller for vendor coding-agent harnesses (Codex, Claude
Code, OpenCode, Antigravity, ...). It **launches** them, **watches** them in real time,
**controls** them through one harness-neutral command language, **automates**
reactions with rules and timers, and lets agents **coordinate** with each other.

Eliot does not contain a model loop, tools, an editor or a chat UI. Everything the
harness does natively stays native; Eliot drives and observes it from outside.

## Requirements

Each requirement maps to the owner statement and to the layer that delivers it
([roadmap](roadmap.md)).

| ID | Requirement | Owner point | Layer |
| --- | --- | --- | --- |
| R1 | Small modular core; all harness-specific code lives in plugins. One `eliot` binary. Code budgets in [roadmap](roadmap.md#code-budgets). | 1 | all |
| R2 | Launch any supported harness headless with a profile: model, permissions, workspace, env, args. | 2 | L2, L4 |
| R3 | Launch a harness through a provider proxy (OpenCodex and similar); the proxy is a managed or attached service. | 3 | L2 |
| R4 | Real-time monitoring from several sources: native event stream, native transcripts, native hooks, OS processes, Git, filesystem. Every source reports its own freshness; missing data is `unknown`, never `zero`. | 4 | L3 |
| R5 | Control verbs: `send`, `steer`, `inject`, `interrupt` (abort), `stop`, `resume`, `approve`/`deny`, `answer`, `configure`, `goal`, plus native hook decisions. | 5 | L1–L4 |
| R6 | Every action is available by explicit command (CLI, agent, API) and automatically (rules, timers, Goal loop). Native Goal is used where the harness has one; otherwise the Eliot Goal loop drives the agent. | 6 | L5 |
| R7 | Programmable automation: rules (`event → actions`), timers (`every`, `cron`, `at`), built-in actions (git fetch/push, launch, send, ...) and scripts in any language. | 7 | L5 |
| R8 | One bus and one command language. The caller never uses harness-specific terms; the plugin translates. | 8 | L1 |
| R9 | No architectural limit on the number of agents; parent/child trees of any depth; native subagents appear as children. | 9 | L1, L7 |
| R10 | Agents talk to each other directly (messages, shared variables) and can ask "who works on this path" — answered from Git facts. | 10 | L6 |
| R11 | Core in Rust. Plugins and scripts may be written in any language during bring-up and are ported to Rust later. | 11 | L8 |

## Non-goals

- **No Task/Issue/review/acceptance/publication engine in the core.** Such workflows
  are built from agents, rules and scripts (examples in
  [automation](spec/automation.md#workflow-recipes)). v1 drowned in this machinery.
- **No multi-user security platform.** One local owner; agents are processes of the
  same OS user. Inside a host, identity is for attribution and routing, not a security
  boundary. Remote access is a separate later layer.
- **No GUI in the core.** CLI, JSON output and a live `eliot top` table. A TUI or web
  view is a client of the same API.
- **No proof ceremony.** No document hashes, receipts, proof packets, audit registers,
  readiness reports or provenance checks on every operation.

## Product acceptance

The product is done when all of these work on the owner's Windows machine:

1. `eliot launch --profile codex-opencodex "..."` starts Codex through OpenCodex, and
   `eliot watch` shows its turns, tool calls, child agents, processes and commits live.
2. The same `eliot send|steer|inject|interrupt|stop` commands work for Codex, Claude
   Code and OpenCode without harness-specific flags. Unsupported verbs fail fast with
   a clear reason.
3. A rule `on turn.completed (role=writer) → git push` and a timer `every 30m → git
   fetch` run unattended for 24 hours without duplicate pushes or lost firings.
4. An Eliot Goal with a check command drives an agent until the check passes, and
   stops on budget or no progress.
5. Two agents agree on a shared variable through messages; one of them runs
   `eliot who src/foo.rs` and gets the other agent.
6. 10,000 simulated agents (fake plugin) run on one host with flat memory and a
   responsive CLI.
