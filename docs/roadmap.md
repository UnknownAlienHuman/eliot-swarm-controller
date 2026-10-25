# Roadmap

Build in layers. A layer starts only when the previous layer's **exit test** passes on
`main`. Each task is one PR. Tick the box in the same PR that completes the task —
this file is the only status tracker.

Rules for every task: implement → tests → `just check` green → PR with what was done,
how it was tested and net lines added. No design reports, audit files or status
documents. If a spec is wrong or missing something, fix the spec in the same PR.

Live runs against real harnesses (`just live-<name>`, `just record-<name>`) are
allowed on a developer machine where the harness is installed and logged in; keep them
short (cheapest model/effort that shows the behavior). They never run in CI.

## Code budgets

Production lines per crate, checked by `just loc` in CI against `budgets.toml`
(created in L0.2 with the values below). Counted with `tokei` as code lines (no
comments/blank) in `src/**/*.rs`, excluding `tests/` directories and files named
`tests.rs` or `*_tests.rs` — keep unit tests in those files, not inline. A PR that
exceeds a budget raises it in `budgets.toml` and in this table with a one-line reason.

| Crate | Budget |
| --- | --- |
| `eliot-core` | 3,000 |
| `eliot-store` | 2,000 |
| `eliot-daemon` | 9,000 |
| `eliot-plugin` | 800 |
| `eliot` (binary) | 3,000 |
| each plugin crate (`plugins/<name>`) | 3,000 |

For scale: v1 reached 533,000 lines of Rust; its largest single file had 9,105.

---

## L0 — Reset

Goal: a clean workspace where layer 1 can start.

- [ ] **L0.1 Archive v1.** Push tag `v1-final` at the `main` commit this PR starts
      from (the last commit that contains v1 code). Delete v1 code from `main`:
      `crates/`, `modules/`, `migrations/`, `src/`, `tests/`, `tools/`, `vendor/`,
      `config/`, `docs/owner-decisions.md`, all `.github/workflows/*`, `Justfile`,
      `Cargo.lock`, `THIRD_PARTY_NOTICES.md` (nothing third-party remains). Replace
      `v1-final` / `dc6f70aa` hints in docs only if the tag points elsewhere.
- [ ] **L0.2 Skeleton.** Workspace `Cargo.toml` (edition 2024, `resolver = "3"`,
      workspace lints: `clippy::all` deny, `unsafe_code` deny except in
      `eliot-daemon::platform`), the crates from
      [architecture](architecture.md#repository-layout-target) as empty libraries plus
      the `eliot` binary, `plugins/fake` as an empty library crate,
      `rust-toolchain.toml` pinned to current stable, `budgets.toml`, and a `Justfile`
      with `check` (`cargo fmt --check`, `cargo clippy --all-targets -D warnings`,
      `cargo test`) and `loc`.
- [ ] **L0.3 CI.** One workflow: `just check` on `windows-latest` and `ubuntu-latest`,
      plus `just loc` (installs `tokei`); Markdown-only changes skip Cargo. Nothing
      else. Add per-package scoping only when a full check takes over 10 minutes.

**Exit test:** CI green on the empty workspace.

## L1 — Core and bus (fake harness only)

Goal: everything except real harnesses works and is tested end-to-end.
Specs: [core](spec/core.md), [API](spec/api.md), [plugin protocol](spec/plugin-protocol.md),
[configuration](spec/configuration.md).

- [ ] **L1.1 Core types.** Ids, `Agent`, `AgentState`, `Event`, event kinds, verbs,
      `Command`, selectors, and the reducer with one test per row (and per ignored
      input) of the [state table](spec/core.md#states).
- [ ] **L1.2 Store.** Migration runner (`user_version`), L1 schema, writer task with
      group commit, reader pool, queries used by `agent.list`/`events.read`. Tests on a
      temp file DB, including reopen (on Windows close every connection before
      deleting the temp dir).
- [ ] **L1.3 Bus.** Persist-then-broadcast for P events, broadcast-only for E events,
      subscribe-from-seq with replay then live, lag → `events.lagged`. Test with a
      slow subscriber.
- [ ] **L1.4 Daemon + IPC.** `eliot daemon [--foreground]`, `eliot daemon stop
      [--drain]`, lock file, `daemon.json`, named pipe / Unix socket, JSON-RPC with
      multiplexing, `daemon.status`, graceful shutdown.
- [ ] **L1.5 Config.** Load `config.toml` (global + project), profiles with defaults
      and `extends`, implicit profiles (`--harness H`), `[plugins.<name>]` sections;
      unknown keys are errors. Proxies are parsed but used only in L2.6.
- [ ] **L1.6 Plugin host.** Spawn plugin processes, `initialize`, requests and
      notifications both ways, timeouts, restart with backoff, re-attach, `unknown` on
      lost replies, plugin exit on stdin EOF.
- [ ] **L1.7 Router.** `command.submit` validation order (launch creates the agent;
      others: exists → supported → allowed → state), persistence, per-agent ordered
      dispatch (`interrupt`/`stop` skip the line), outcome recording, selectors,
      idempotent command ids, profile `max_live` launch queue.
- [ ] **L1.8 Fake plugin.** `plugins/fake`, run as `eliot plugin fake`, implementing the
      [fake plugin spec](spec/plugin-protocol.md#fake-plugin).
- [ ] **L1.9 CLI.** `daemon`, `status`, `launch`, `ls`, `show`, `watch`, `events`,
      `send` (incl. `--wait`), `steer`, `inject`, `interrupt`/`abort`, `stop`,
      `resume`, `approve`, `deny`, `answer`, `wait`, `result`, `cmd show`; `--json`
      everywhere; exit codes from [API](spec/api.md#exit-codes).
- [ ] **L1.10 Conformance kit.** `eliot plugin-test <cmd>` runs the
      [suite](spec/plugin-protocol.md#conformance-kit) against a plugin; the fake
      plugin passes it.
- [ ] **L1.11 Agent env and control.** Inject `ELIOT_HOME`, `ELIOT_AGENT_ID`,
      `ELIOT_AGENT_TOKEN`, `PATH`, `system_note`; token → caller identity; `control`
      checks from [coordination](spec/coordination.md#who-may-do-what).

**Exit test (`crates/eliot/tests/e2e_l1.rs`, runs in CI):** start the daemon in a temp
dir and drive it only through the `eliot` binary:

1. launch fake agents A and B, and C with `--parent A`; `ls --tree A` shows A and C;
2. `send A '{"steps":[{"say":"hi"},{"approval":{"kind":"command","command":"rm x"}},{"say":"done"}]}'`
   → events in order: `turn.started`, `agent.busy`, `message.agent "hi"`,
   `approval.requested`, `agent.waiting`; `approve A` → `approval.resolved`,
   `agent.busy`, `message.agent "done"`, `turn.completed status=completed`, `agent.idle`;
3. `steer` on an idle agent → exit code 3 (`bad_state`); `steer` during a
   `sleep_ms` step → `command.applied`; `interrupt` during a sleep →
   `turn.completed status=interrupted`;
4. with a second plugin entry `[plugins.fake-narrow]` (`cmd = ["eliot", "plugin",
   "fake"]`, `verbs = ["launch","send","stop"]`) and `launch --harness fake-narrow`,
   `steer` → exit code 3 (`unsupported`) and the plugin received no request;
5. `send A '{"steps":[{"hang":null}]}'`, then `steer A x` (no reply comes), then kill
   the fake plugin process → the `steer` becomes `unknown` (exit code 4) and is not
   resent; the plugin restarts; A, B and C are `lost` (their sessions died with it);
   `resume A` → `agent.idle` with no `message.user` repeating an old prompt;
6. kill the daemon, start it again → the same agents are listed with the states they
   had (live ones now `lost`, since their plugin died with the daemon);
7. `send B '{"steps":[{"sleep_ms":500},{"say":"ok"}]}' --wait` prints `ok`;
   `wait A B --for idle` returns 0;
8. with `[profiles.fake2] harness = "fake"`, `max_live = 2`, launching 3 agents leaves
   the third `created` until one of the first two stops;
9. launch 1,000 fake agents in under 30 s with daemon RSS under 200 MB.

## L2 — First real harness: Codex

Goal: one real harness fully controlled, including a proxy.
Specs: [Codex](harnesses/codex.md), [OpenCodex](harnesses/opencodex.md),
[configuration](spec/configuration.md#proxies).

- [ ] **L2.1 Recording and replay.** `ELIOT_RECORD` / `ELIOT_REPLAY` support in
      `eliot-plugin` ([format](spec/plugin-protocol.md#recordings)); `just record-codex`
      records the scenarios listed in the [Codex page](harnesses/codex.md#scenarios)
      into `plugins/codex/fixtures/`.
- [ ] **L2.2 Codex plugin: launch and turns.** Start `codex app-server` (stdio),
      `initialize`, start thread, start turn, map notifications to events, final text,
      usage; tests replay the fixtures.
- [ ] **L2.3 Control verbs.** `send` (queued while busy), `steer` (active turn id),
      `interrupt`, `stop`, `resume`, `configure` (reports `applies`).
- [ ] **L2.4 Approvals and questions.** Native requests → `approval.requested` /
      `question.asked`; `approve`/`deny`/`answer`. Host `decide` answers `ask` until
      L5.5.
- [ ] **L2.5 Native goal.** `goal.set|pause|resume|clear` mapped to Codex thread goals
      with the [status mapping](harnesses/codex.md#verb-mapping); `goal.updated`.
      `check` and `budget` are rejected as `unsupported` until L5.6.
- [ ] **L2.6 Children.** Subagent threads → `child` → `origin=native` agents.
- [ ] **L2.7 Permissions and proxy.** `permissions` mapping from the Codex page;
      `[proxies]` attach/launch/health as a refcounted service; Codex pointed at
      OpenCodex with the [default wiring](harnesses/codex.md#proxy-opencodex-or-any-responses-compatible-endpoint).
- [ ] **L2.8 Live check.** `just live-codex`: launch, steer, interrupt, approve, goal,
      child — through OpenCodex and without. Paste the output into the PR.

**Exit test:** `eliot plugin-test` passes against the Codex plugin in replay mode (CI);
`just live-codex` passes on a machine with Codex installed.

## L3 — Monitoring

Specs: [monitoring](spec/monitoring.md).

- [ ] **L3.1 Process monitor.** Identities, descendants, stats, `session.lost`, Job
      object / process group custody, `stop --force` verification.
- [ ] **L3.2 Workspaces.** `workspace_mode = "worktree"` / `--worktree` creates
      `git worktree add` under `<data_dir>/worktrees`, runs the profile's `setup`
      commands (a new worktree has no ignored files, submodules or LFS content),
      per-agent Git identity. On `stop` the worktree is removed only if
      `git status --porcelain` is empty and HEAD is on a branch (kept); otherwise it
      stays and `eliot doctor` lists it. After a squash merge, start from a new
      worktree on the new base; never continue the merged branch.
- [ ] **L3.3 Git monitor.** Status/commit/diff sets with `--no-optional-locks`,
      fs-triggered refresh, backoff for slow repos, `git.*` events.
- [ ] **L3.4 Transcripts and discovery.** Codex rollout tail; `eliot discover`,
      `eliot attach --observe-only`.
- [ ] **L3.5 Liveness.** Stall flag, source health, `unknown` display.
- [ ] **L3.6 Views.** `eliot top`, richer `show`, `watch` formatting.

**Exit test:** killing a native Codex process yields `agent.lost` within 10 s; a
commit in an agent worktree yields `git.commit` within 20 s; an idle-but-alive agent
is never shown as lost; a Codex session started by hand is discovered and watched.

## L4 — Other harnesses

One task per harness, in this order. Each: plugin + recordings + conformance kit in
replay mode + `just live-<name>`; correct the harness page and the
[matrix](harnesses/README.md) where reality differs. Plugins may be TypeScript/Python
first (R11).

- [ ] **L4.1 Hook shim.** `eliot hook`, `hook.event`, session-scoped hook config
      generation, inject delivery via hook context, default on timeout.
- [ ] **L4.2 Claude Code** (Agent SDK; in-process hooks for inject/steer/approvals).
- [ ] **L4.3 Muse** (MSP over stdio; official Rust SDK or low-level connection).
- [ ] **L4.4 OpenCode** (`opencode serve` HTTP + SSE; attach to a running server).
- [ ] **L4.5 Antigravity** (`agy` stream-json, warm process).
- [ ] **L4.6 ACP generic** (any ACP agent, e.g. Gemini CLI).
- [ ] **L4.7 Others** ([Command Code, Kilo, Zed](harnesses/others.md)) — when needed.

**Exit test:** the same scripted scenario (launch, send, steer, interrupt, approve,
stop) passes on every L4 harness, where each verb either works or is rejected with a
reason that matches the harness page.

## L5 — Automation

Specs: [automation](spec/automation.md). Time comes from an injectable clock so timer
tests run in simulated time.

- [ ] **L5.1 Rules engine.** TOML loading/reload, event triggers, matchers,
      placeholders, limits, loop guard, `rule.*` events, `eliot rule test`.
- [ ] **L5.2 Timers.** `every`, `cron`, `at`, catch-up policy, persisted last fire.
- [ ] **L5.3 Actions.** Agent actions, git actions (fetch/pull/push/worktree with
      readback), `msg.send`, `var.set`, `notify`, `emit`.
- [ ] **L5.4 Scripts.** `run` action with the NDJSON contract; example scripts.
- [ ] **L5.5 Approvals policy.** Approval rules incl. inline `decide`.
- [ ] **L5.6 Goal loop.** Eliot mode and native-mode budgets, `no_progress`, parent
      notification, `goals` table.
- [ ] **L5.7 Recipes.** `examples/` with the three [workflow recipes](spec/automation.md#workflow-recipes).

**Exit test:** 24 h of simulated time with the fake plugin and a local Git remote: a
rule firing every minute, a cron fetch, a goal reaching `done`, a goal hitting
`no_progress`; zero duplicate actions, zero missed timer firings, and the loop guard
suppresses a deliberately looping rule.

## L6 — Coordination

Specs: [coordination](spec/coordination.md).

- [ ] **L6.1 Mailbox.** Send/inbox/read, modes, selectors, delivery format, `--wait`.
- [ ] **L6.2 Shared vars.** Scopes, CAS, `watch`, `var.changed`.
- [ ] **L6.3 Who + claims.** In-memory Git sets, history mapping, claim conflicts.
- [ ] **L6.4 Hierarchy ops.** Tree selectors, `stop --tree`, parent notifications,
      `max_depth`.
- [ ] **L6.5 MCP server.** `eliot mcp` for harnesses that prefer tools over shell. The
      tool set follows the caller's control: `self` gets inbox, msg, var, who, claim,
      ls; `tree`/`project` add launch, send, wait, result, steer, stop. One bridge per
      session: it loads nothing at start (a v1 donor bridge that loaded its whole
      ledger used ~207 MiB each), holds no Store handle, exits when stdin closes, and
      `eliot doctor` lists orphans. `eliot show` reports whether the agent has actually
      used `eliot`/MCP yet (`unused` / `used`), so a harness that cannot reach it is
      visible.

**Exit test:** two real agents (different harnesses) agree on a shared variable via
messages; one runs `eliot who` and names the other; a claim conflict notifies both.

## L7 — Scale and hardening

- [ ] **L7.1 Load test.** 10,000 fake agents, 2,000 persisted events/s for 1 h: flat
      RSS, CLI `ls`/`show` p95 < 100 ms, no event loss for persisted kinds; reads never
      wait behind the writer. (v1 reference: status p95 ≈ 1.2 ms with 200 IPC clients
      on Windows, but at most 285 persisted events/s because it committed and fsynced
      every event — group commit is what makes the target reachable.)
- [ ] **L7.2 Plugin sharding.** `instances = N` per plugin.
- [ ] **L7.3 Retention.** Event pruning in batches; DB size stable in the load test.
- [ ] **L7.4 Drain.** `eliot daemon stop --drain` stops new launches, rule and timer
      firings; running turns finish; approvals, answers and results keep working.
- [ ] **L7.5 Windows hardening.** Tests for the [process start rules](spec/monitoring.md#starting-processes):
      no console windows, Job assignment at creation, long inputs via files,
      `PATH`/`Path` merge, paths with spaces and non-ASCII characters.
- [ ] **L7.6 Service mode.** Codex `app-server --listen unix://…` (or
      `ws://127.0.0.1:<port>` where unix sockets do not work) and `opencode serve` run
      as Eliot services in their own
      Job without kill-on-close; the plugin connects to them; a daemon restart
      re-attaches and running turns continue. `eliot service list|stop`.
- [ ] **L7.7 Doctor.** `eliot doctor`: harness processes that belong to no live
      agent, services nobody uses, worktrees of stopped agents, data dir and DB size,
      stale sources. Reports and suggests commands; changes nothing by itself.
- [ ] **L7.8 Autostart.** `eliot daemon install` registers a per-user logon task
      (Windows Task Scheduler / systemd user unit). No promise of continuity across
      logoff or reboot: stdio sessions end (D-18) and are resumed.
- [ ] **L7.9 Redaction.** Pattern-based secret redaction of persisted event text
      (token-shaped strings), on top of the L1 known-value masking; donor:
      `v1-final:vendor/atlas/crates/atlas-redact`.

## L8 — Port to Rust

- [ ] Port each non-Rust plugin and script that is kept, behind the same conformance kit.

## L9 — Later (not planned yet)

Remote access (authenticated gateway), multi-host federation, TUI/web view, Eliot
Memory OS integration. Each needs an owner decision first ([decisions](decisions.md)).
Notes for then: v1 studied Gemini Spark as a remote MCP client (observer role first;
Spark silence is never terminal evidence; a Google write confirmation is not Eliot
authorization; MCP revision 2026-07-28 deprecates dynamic client registration) —
`git show v1-final:docs/gemini-spark-integration.md`. Memory OS: disabled mode
contacts nothing, an absent or stale packet is an explicit failure —
`git show v1-final:docs/eliot-memory-os-integration.md`.
