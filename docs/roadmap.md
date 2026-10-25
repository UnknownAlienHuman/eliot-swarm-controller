# Roadmap

Build in layers. A layer starts only when the previous layer's **exit test** passes on
`main`. Each task is one PR. Tick the box in the same PR that completes the task —
this file is the only status tracker.

Rules for every task: implement → tests → `just check` green → PR with what was done,
how it was tested and net lines added. No design reports, audit files or status
documents. If a spec is wrong or missing something, fix the spec in the same PR.

## Code budgets

Production lines (excluding tests and comments), checked by `just loc` in CI. A PR
that exceeds a budget must raise it in this table with a one-line reason.

| Crate | Budget |
| --- | --- |
| `eliot-core` | 3,000 |
| `eliot-store` | 2,000 |
| `eliot-daemon` | 9,000 |
| `eliot-plugin` | 800 |
| `eliot` (binary) | 3,000 |
| each plugin | 3,000 |

For scale: v1 reached 533,000 lines of Rust; its largest single file had 9,105.

---

## L0 — Reset

Goal: a clean workspace where layer 1 can start.

- [ ] **L0.1 Archive v1.** Create tag `v1-final` on the last v1 commit. Delete v1
      code from `main`: `crates/`, `modules/`, `migrations/`, `src/`, `tests/`,
      `tools/`, `vendor/`, `config/`, `docs/owner-decisions.md`, old workflows,
      `Justfile`, `Cargo.lock`. Update `THIRD_PARTY_NOTICES.md` (nothing vendored).
- [ ] **L0.2 Skeleton.** Workspace `Cargo.toml` (edition 2024, `resolver = "3"`,
      workspace lints: `clippy::all` deny, `unsafe_code` deny except in
      `eliot-daemon::platform`), empty crates from [architecture](architecture.md#repository-layout-target),
      `rust-toolchain.toml` pinned to current stable, new `Justfile` with `check`
      (`fmt --check`, `clippy -D warnings`, `test`), `loc`, `run`.
- [ ] **L0.3 CI.** One workflow: `just check` on `windows-latest` and `ubuntu-latest`,
      plus `just loc`. Nothing else.

**Exit test:** CI green on the empty workspace.

## L1 — Core and bus (fake harness only)

Goal: everything except real harnesses works and is tested end-to-end.
Specs: [core](spec/core.md), [API](spec/api.md), [plugin protocol](spec/plugin-protocol.md).

- [ ] **L1.1 Core types.** Ids, `Agent`, `AgentState`, `Event`, event kinds, verbs,
      `Command`, `Selector`, the state reducer with table-driven tests for every
      transition in [states](spec/core.md#states).
- [ ] **L1.2 Store.** Migration runner (`user_version`), L1 schema, writer task with
      group commit, reader pool, queries used by `agent.list`/`events.read`. Tests on a
      temp file DB, including reopen.
- [ ] **L1.3 Bus.** Persist-then-broadcast for P events, broadcast-only for E events,
      subscribe-from-seq with replay then live, lag → `events.lagged`. Test with a
      slow subscriber.
- [ ] **L1.4 Daemon + IPC.** `eliot daemon`, lock file, `daemon.json`, named pipe /
      Unix socket, JSON-RPC with multiplexing, `daemon.status`, graceful shutdown.
- [ ] **L1.5 Plugin host.** Spawn plugin processes, `initialize`, request/notify both
      ways, timeouts, restart with backoff, re-attach, `unknown` on lost replies.
- [ ] **L1.6 Router.** `command.submit` validation order (exists → supported →
      allowed → state), persistence, dispatch, outcome recording, selectors,
      idempotent command ids.
- [ ] **L1.7 Fake plugin.** `eliot plugin fake`: deterministic harness driven by a
      JSON script in the prompt (`say`, `sleep_ms`, `tool`, `approval`, `question`,
      `spawn`, `crash`, `hang`, `exit`); plain text = echo. Supports every verb; plugin
      config `verbs = [...]` narrows the declared set so rejection paths can be tested.
      `resume` works from its state dir (`<data_dir>/plugins/fake/`).
- [ ] **L1.8 CLI.** `daemon`, `status`, `launch`, `ls`, `show`, `watch`, `events`,
      `send`, `steer`, `inject`, `interrupt`, `stop`, `resume`, `approve`, `deny`,
      `answer`, `cmd show`; `--json` everywhere; exit codes from [API](spec/api.md#cli).
- [ ] **L1.9 Conformance kit.** `eliot plugin-test <cmd>` runs the protocol suite;
      the fake plugin passes it.
- [ ] **L1.10 Agent env.** Inject `ELIOT_HOME`, `ELIOT_AGENT_ID`,
      `ELIOT_AGENT_TOKEN`, `PATH`; token → caller identity; `control` checks.

**Exit test (`crates/eliot/tests/e2e_l1.rs`, runs in CI):** start daemon in a temp
dir; launch 3 fake agents (one is a child of another); send/steer/interrupt/approve
and see the expected events in order; with a narrowed fake profile an unsupported verb
is rejected with no plugin I/O; kill the plugin mid-command → command `unknown`,
plugin restarts, agents re-attach; kill and restart the daemon → the same agents are
listed, now `lost` (their processes died with the daemon), and `resume` brings one
back to `idle` without resending its prompt; launch 1,000 fake agents in under 30 s
with daemon RSS under 200 MB.

## L2 — First real harness: Codex

Goal: one real harness fully controlled, including a proxy.
Specs: [Codex](harnesses/codex.md), [OpenCodex](harnesses/opencodex.md),
[configuration](spec/configuration.md).

- [ ] **L2.1 Codex plugin: launch and turns.** Start `codex app-server` (stdio),
      `initialize`, start thread, start turn, map notifications to events, final text,
      usage. Fixture tests from a recorded real session (stored under
      `plugins/codex/fixtures/`, labelled as recorded with version and date).
- [ ] **L2.2 Control verbs.** `send` (queued while busy), `steer` (active turn id),
      `interrupt`, `stop`, `resume`, `configure` (reports `applies`).
- [ ] **L2.3 Approvals and questions.** Native approval requests → `approval.requested`;
      `approve`/`deny`; `decide` for inline policy.
- [ ] **L2.4 Native goal.** `goal.*` mapped to Codex thread goals; `goal.updated`.
      `show` displays the last `goal.updated`; budgets and the `goals` table come in L5.6.
- [ ] **L2.5 Children.** Subagent threads → `child` → `origin=native` agents.
- [ ] **L2.6 Profiles and proxy.** Profile resolution, `permissions` mapping,
      `[proxies]` attach/launch/health as a refcounted service, Codex pointed at
      OpenCodex.
- [ ] **L2.7 Live check script.** `just live-codex` (owner runs it; real model): launch,
      steer, interrupt, approve, goal, child, through OpenCodex and without. Paste the
      output into the PR.

**Exit test:** conformance kit passes against the Codex plugin with fixtures; the
owner's `just live-codex` run passes.

## L3 — Monitoring

Specs: [monitoring](spec/monitoring.md).

- [ ] **L3.1 Process monitor.** Identities, descendants, stats, `agent.lost`, Job
      object / process group custody, `stop --force` verification.
- [ ] **L3.2 Workspaces.** `--worktree` creates `git worktree add` under
      `<data_dir>/worktrees`, per-agent Git identity, cleanup on `stop` (keeps the
      branch).
- [ ] **L3.3 Git monitor.** Status/commit/diff sets with `--no-optional-locks`,
      fs-triggered refresh, backoff for slow repos, `git.*` events.
- [ ] **L3.4 Transcripts and discovery.** Codex rollout tail; `eliot discover`,
      `eliot attach --observe-only`.
- [ ] **L3.5 Liveness.** Stall flag, source health, `unknown` display.
- [ ] **L3.6 Views.** `eliot top`, richer `show`, `watch` formatting.

**Exit test:** killing a native Codex process yields `agent.lost` within 5 s; a commit
in an agent worktree yields `git.commit` within 20 s; an idle-but-alive agent is never
shown as lost; a Codex session started by hand is discovered and watched.

## L4 — Other harnesses

One task per harness, in this order. Each: plugin + fixtures + conformance kit +
`just live-<name>` + its [harness page](harnesses/README.md) updated with what was
verified. Plugins may be TypeScript/Python first (R11).

- [ ] **L4.1 Hook shim.** `eliot hook`, `hook.event`, session-scoped hook config
      generation, inject delivery via hook context, default on timeout.
- [ ] **L4.2 Claude Code** (stream-json CLI or Agent SDK; hooks for inject/approvals).
- [ ] **L4.3 OpenCode** (`opencode serve` HTTP + SSE; attach to a running server).
- [ ] **L4.4 Antigravity** (`agy` stream-json, warm process).
- [ ] **L4.5 ACP generic** (any ACP agent, e.g. Gemini CLI; covers Zed-compatible agents).
- [ ] **L4.6 Muse, Command Code, Kilo** — only if the owner still uses them.

**Exit test:** the same scripted scenario (launch, send, steer-or-reject, interrupt,
approve, stop) passes on every L4 harness, with the capability matrix in
[harnesses](harnesses/README.md) matching reality.

## L5 — Automation

Specs: [automation](spec/automation.md).

- [ ] **L5.1 Rules engine.** TOML loading/reload, event triggers, matchers,
      placeholders, limits, loop guard, `rule.*` events, `eliot rule test`.
- [ ] **L5.2 Timers.** `every`, `cron`, `at`, catch-up policy, persisted last fire.
- [ ] **L5.3 Actions.** Agent actions, git actions (fetch/pull/push/worktree with
      readback), `msg.send`, `var.set`, `notify`, `emit`.
- [ ] **L5.4 Scripts.** `run` action with the NDJSON contract; example scripts.
- [ ] **L5.5 Approvals policy.** Approval rules incl. inline `decide`.
- [ ] **L5.6 Goal loop.** Eliot mode and native-mode budgets, `no_progress`, parent
      notification.
- [ ] **L5.7 Recipes.** `examples/` with the three [workflow recipes](spec/automation.md#workflow-recipes).

**Exit test:** a 24 h soak with the fake plugin: 1 rule per minute firing, cron fetch,
a goal reaching `done`, a goal hitting `no_progress`; zero duplicate actions, zero
missed timer firings, loop guard suppresses a deliberately looping rule.

## L6 — Coordination

Specs: [coordination](spec/coordination.md).

- [ ] **L6.1 Mailbox.** Send/inbox/read, modes, selectors, delivery format, `--wait`.
- [ ] **L6.2 Shared vars.** Scopes, CAS, `watch`, `var.changed`.
- [ ] **L6.3 Who + claims.** In-memory Git sets, history mapping, claim conflicts.
- [ ] **L6.4 Hierarchy ops.** Tree selectors, `stop --tree`, parent notifications,
      `max_depth`.
- [ ] **L6.5 MCP server.** `eliot mcp` exposing msg/var/who/claim/ls/send tools for
      harnesses that prefer tools over shell.

**Exit test:** two real agents (different harnesses) agree on a shared variable via
messages; one runs `eliot who` and names the other; a claim conflict notifies both.

## L7 — Scale and hardening

- [ ] **L7.1 Load test.** 10,000 fake agents, 2,000 events/s for 1 h: flat RSS, CLI
      `ls`/`show` p95 < 100 ms, no event loss for persisted kinds.
- [ ] **L7.2 Plugin sharding.** `instances = N` per plugin.
- [ ] **L7.3 Retention.** Event pruning in batches; DB size stable in the load test.
- [ ] **L7.4 Drain.** `daemon.shutdown --drain`; timers and rules stop admitting work.
- [ ] **L7.5 Windows hardening.** No console windows for children, long argv via
      files, UTF-8 without BOM everywhere, path quoting tests.

## L8 — Port to Rust

- [ ] Port each non-Rust plugin and script that is kept, behind the same conformance kit.

## L9 — Later (not planned yet)

Remote access (authenticated gateway), multi-host federation, TUI/web view. Each needs
an owner decision first ([decisions](decisions.md)).
