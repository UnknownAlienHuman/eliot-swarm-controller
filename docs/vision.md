# Vision

## Owner statement (source of truth)

Original product statement, given on 2026-10-25, preserved verbatim. The requirements
remain binding; later explicit owner clarifications determine current implementation
order. No architecture document may silently remove a requirement.

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

On 2026-10-27 the owner clarified the immediate sequence: core/bus, one useful harness
(Codex with Luna), then automation/scripts/agent communication and additional harnesses.
[Roadmap](roadmap.md) is the single current schedule. A first usable Codex path must not
wait for the complete adapter catalogue or automation architecture.

On 2026-11-07 the owner additionally clarified that a cloud Dot/MuseAgent must be able to
connect as an MCP caller through the local StreamableHTTP MCP server, exposed via
`cloudflared` at `swarm.iuriilisenkov.cloud`, and that Swarm must automatically detect
and diagnose stalled or unhealthy agents and notify the supervisor/agent. The cloud MCP
caller is a remote identity/connection/call principal, not a controlled local native
harness or PID; native process statistics alone do not prove model progress. The cloud
endpoint is authenticated with Cloudflare Access Managed OAuth and native JWT validation
per the existing account documentation, while the local One OS user trust boundary
remains. This clarification replaces the earlier treatment of remote access as Later.

On 2026-11-22 the owner stated a principle that every layer must follow:

> «Истина хранится вне harness!!! Она хранится в Git!!!! Это ещё в первой архитектуре должно было быть написано и повторяться!!!»

The owner then specified: «В Git, в файловой системе». The truth about work lives in Git
and in the file system, outside any harness. A harness session, its model, its context
window and its transcript are disposable. The task's state is its worktree (the branch,
its commits and the files on disk) and the task card that names it. Any model on any
harness can continue a task from there, for any reason: a provider limit, a cheaper or
stronger model, a failure, or the Owner's choice. Moving work between models is the
ordinary path, not a special recovery feature. See
[Work truth lives in Git and the file system](architecture.md#work-truth-lives-in-git-and-the-file-system).

## What Eliot is

### Owner clarification, 2026-11-15

> "Everything should be automated: routing, assignments and so on."
> "Everything you'd have to dig into goes to Settings."
> "The system must allow a hot start, working out of the box."
> "Unified settings: everything the human does and everything the machine does is stored in one place."
> "Finish it and make it right."

The private daemon data directory owns one settings source and typed API, including
desktop preferences and bounded history of human and machine edits. First use discovers
installed harnesses, creates editable native-default profiles and registers the opened
Git checkout. Policy selects a profile before admission and records its choice; uncertain
external effects are never retried on another route. Assignment uses existing rules and
timers, not a Task engine. The S1-S4 increments in the roadmap own delivery order.

A small local Rust controller that launches vendor coding harnesses, observes native/
process/Git facts and exposes one CLI/API for control, bounded automation and direct
agent coordination. Vendor model loops, tools and detailed native transcripts remain
vendor-owned. Eliot is not another coding harness.

## Requirements

| ID | Requirement | Owner point | Delivery |
| --- | --- | --- | --- |
| R1 | Small modular core; vendor code isolated in plugins; one binary. | 1 | all |
| R2 | Headless launch with an explicit profile or routing role, plus model/permissions/workspace. | 2 | L2 Codex, L6 adapters |
| R3 | Explicit OpenCodex or compatible proxy route; adopted/managed ownership. | 3 | L3 |
| R4 | Real-time native/process/Git/fs monitoring with honest freshness/gaps. | 4 | L2–L3; hooks only where needed |
| R5 | Uniform send/steer/inject/interrupt/stop/resume/request/configuration/Goal controls; unsupported semantics reject. | 5 | incremental L2–L6 |
| R6 | Explicit and automated commands; independent native/Eliot Goals. | 6 | L4 |
| R7 | Event rules, timers/cron, scripts and useful actions. | 7 | L4 |
| R8 | One command language and event bus; caller never translates vendor terms. | 8 | L1 |
| R9 | Indexed logical fleet and hierarchical children, configurable fanout guards rather than arbitrary architecture limits. | 9 | L1, L5, L7 |
| R10 | Addressed messages, shared variables and Git-backed `who`. | 10 | L3, L5 |
| R11 | Rust core; other-language plugin/script bring-up allowed. | 11 | all |

## Non-goals

No Task/Issue/acceptance engine, multi-user security platform,
proof packets, readiness registers or operational document-hash ceremony. The former
GUI non-goal is superseded: one optional Windows desktop companion (`swarm-desktop`,
driven by the daemon) is approved; the daemon/CLI remains the headless authority and
default build. Protocol
acknowledgement and mutation idempotency are ordinary implementation correctness, not
permission to construct a generic receipt/provenance platform.

The daemon can perform repository operations, including branch integration, when an
authorized manager requests them or an applicable automation rule is configured.
Repository integration has no default trigger.

Projects can opt into a bounded review recipe using ordinary rules. A reviewer examines
the identified worktree and Swarm delivers its assessment as quoted data to the manager.
The manager interprets the assessment and decides what to do. External facts can enter
rules as script events while task, issue and pull request state stays in the source system.

One OS user is the local trust boundary for local control; the cloud endpoint is an
authenticated owner MCP caller, distinct from a locally controlled native harness.
A large logical fleet does not mean one machine can run a million vendor processes.

## Product acceptance

The complete selected local product must demonstrate on the owner's Windows machine:

1. Codex/Luna works directly first, then through the explicitly configured OpenCodex
   route; watch shows actual turns/tools/results, later process/Git/child facts.
2. The same command language controls Codex, Claude Code and OpenCode where each has
   the required native semantics. Unsupported controls fail clearly, never imitate a
   weaker operation.
3. A manager launches workers, sends/waits/steers and collects results through Eliot.
   Two Agents exchange mail, CAS a variable and identify a working area with `who`.
4. Rules/timers/scripts and bounded Goals operate unattended without replaying uncertain
   prompts/pushes/setup. Crash recovery exposes unknown outcomes until exact settlement.
5. Retention and representative 10,000-Agent load remain bounded and responsive;
   Windows force-stop verifies its owned Job, and Unix reports its actual weaker custody.

These are final qualification goals, not conditions that must all be implemented before
one useful Codex session runs. [L7](roadmap.md#l7--qualification-repair-and-release)
qualifies the connected product; compilation or documentation review alone does not.

The cloud MCP caller contour and the stalled/unhealthy-agent diagnosis and notification
contour are required product contours, tracked as roadmap increments, not Later.
