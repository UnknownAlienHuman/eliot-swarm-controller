# Roadmap

Build in layers. A layer starts only when the previous layer's **exit test** passes on
`main`. Each task is one PR and lands with its own tests. Tick the box in the same PR
that completes the task — this file is the only status tracker. There is no separate
"qualification later" phase: a layer whose exit test does not pass is not done
([D-22](decisions.md)).

How to do a task: [AGENTS.md](../AGENTS.md).

Live runs against real harnesses (`just live-<name>`, `just record-<name>`) are
allowed on a developer machine where the harness is installed and logged in; keep them
short (cheapest model/effort that shows the behavior). They never run in CI.

## Code budgets

Production lines per crate, checked by `just loc` in CI against `budgets.toml`
(created in L0.2; one entry per crate directory, e.g. `"crates/eliot-core" = 3000`).
For each entry `just loc` runs `tokei <dir>/src --exclude '*tests.rs'` and compares
the Rust code lines (no comments or blanks); keep unit tests in `tests.rs` /
`*_tests.rs` files or `tests/`, not inline. A PR that exceeds a budget raises it in
`budgets.toml` and in this table with a one-line reason.

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

- [ ] **L0.1 Archive v1.** Push tag `v1-final` at `dc6f70aa` (the last commit with
      both v1 code and v1 docs). Delete v1 from `main`: `Cargo.toml`, `Cargo.lock`,
      `rust-toolchain.toml`, `crates/`, `modules/`, `migrations/`, `src/`, `tests/`,
      `tools/`, `vendor/`, `config/`, `docs/owner-decisions.md`, all
      `.github/workflows/*`, `Justfile`, `THIRD_PARTY_NOTICES.md`. Check: `git
      ls-files` lists only `docs/`, `README.md`, `AGENTS.md`, `CLAUDE.md`,
      `.gitignore`, `.gitattributes` (there is no Rust code, so no Cargo gate).
- [ ] **L0.2 Skeleton.** Workspace `Cargo.toml` (edition 2024, `resolver = "3"`,
      workspace lints: `clippy::all` deny, `unsafe_code` deny except in
      `eliot-daemon::platform`) with only the `eliot` binary crate (`eliot --version`);
      `rust-toolchain.toml` pinned to current stable, `budgets.toml`, and a `Justfile`
      with `check` (`cargo fmt --check`, `cargo clippy --all-targets -D warnings`,
      `cargo test`), `loc` and `docs` (`lychee --offline --include-fragments docs
      README.md AGENTS.md`: every relative link and anchor resolves). Every other crate
      is created by the first task that puts code in it (`eliot-core` L1.1,
      `eliot-store` L1.2, `eliot-daemon` L1.3, `eliot-plugin` and `plugins/fake` L1.8).
- [ ] **L0.3 CI.** One workflow that always runs (so its check always reports):
      `just check` on `windows-latest` and `ubuntu-latest` plus `just loc` and
      `just docs` (installs `tokei` and `lychee`); a first step skips the Cargo steps
      when only Markdown changed. Nothing else. Add per-package scoping only when a full
      check takes over 10 minutes.

**Exit test:** CI green on the empty workspace.

## L1 — Core and bus (fake harness only)

Goal: everything except real harnesses works and is tested end-to-end.
Specs: [core](spec/core.md), [API](spec/api.md), [plugin protocol](spec/plugin-protocol.md),
[configuration](spec/configuration.md).

- [ ] **L1.1 Core types.** Ids (positive `i64`, ULID command ids and the derived
      forms), `Agent`, `AgentState`, `Event`, event kinds, verbs, `Command`, selectors,
      and the reducer with one test per row (and per ignored input) of the
      [state table](spec/core.md#states), including "terminal state cancels queued
      commands".
- [ ] **L1.2 Store.** Migration runner (`user_version`), L1 schema (`agents`,
      `events`, `commands`, `turns`), writer task with group commit, reader pool,
      queries used by `agent.list`/`events.read`; duplicate `(agent, native_id)`
      returns the stored `seq`; a failed commit stops admission. Tests on a temp file
      DB, including reopen (on Windows close every connection before deleting the temp
      dir).
- [ ] **L1.3 Bus.** Persist-then-broadcast for P events, broadcast-only for E events;
      a plugin's persisted event is answered only after its commit;
      subscribe-from-seq: attach to the live stream first and buffer, replay from the
      Store up to a watermark `seq`, then emit buffered events above the watermark
      (deduplicated by `seq`); per-subscriber bounded queue (items; events are at
      most 64 KiB) with `events.lagged`; `history_gap` for pruned ranges. Test with a
      slow subscriber and with events committed during replay.
- [ ] **L1.4 Daemon + IPC.** `eliot daemon [--foreground]`, `eliot daemon stop
      [--drain | --force]` (drain = let running turns finish, admit nothing new;
      without a flag it refuses while turns run), lock file, `daemon.json` with the PID +
      start-time liveness check, named pipe / Unix socket, JSON-RPC with
      multiplexing, `daemon.status`.
- [ ] **L1.5 Config.** Load `config.toml` (global + project), profiles with defaults
      and `extends`, project overlays that may only narrow
      ([rules](spec/configuration.md#project-overlays)), implicit profiles
      (`--harness H`), accounts, `[plugins.<name>]` sections; unknown keys are errors.
      Proxies are parsed but used only in L2.7.
- [ ] **L1.6 Plugin host.** Spawn plugin processes following the
      [process start rules](spec/monitoring.md#starting-processes): one process and one
      Job object / process group per agent, one for a `multi_session` plugin;
      `initialize`, requests and notifications both ways, persisted `event` requests
      answered after commit, timeouts, restart with backoff (multi-session), re-attach,
      `unknown` on lost replies, plugin exit on stdin EOF, kill-and-verify after the
      stop timeout.
- [ ] **L1.7 Router.** `command.submit` validation order (launch: preflight, then
      create the agent; others: exists → supported → allowed → state), persistence,
      linked first send for `launch`/`resume` with a prompt (and `initial` for
      `start_requires_input`), per-agent ordered dispatch (`interrupt`/`stop` skip the
      line, both carry the active `turn_id`), Store-held queue of sends to busy agents
      and `cancel`, outcome recording, selectors with `max_selector_targets` and
      `command.children`, idempotent ids with `id_expired`, one agent row per native
      subject, profile + account `max_live` launch queue, restart turns `dispatched`
      into `unknown`, owner `cmd resolve`.
- [ ] **L1.8 Fake plugin.** `plugins/fake`, run as `eliot plugin fake`, implementing the
      [fake plugin spec](spec/plugin-protocol.md#fake-plugin), including the
      `start_requires_input` option.
- [ ] **L1.9 CLI.** `daemon`, `status`, `launch` (incl. `--wait`), `ls`, `show`,
      `watch`, `events`, `send` (incl. `--wait`), `steer`, `inject`,
      `interrupt`/`abort`, `stop`, `resume`, `approve`, `deny`, `answer`, `configure`,
      `wait`, `result`, `cmd show|children|cancel|resolve`; `--json` and `--id`
      everywhere; exit codes from [API](spec/api.md#exit-codes).
- [ ] **L1.10 Conformance kit.** `eliot plugin-test <cmd>` runs the
      [suite](spec/plugin-protocol.md#conformance-kit) against a plugin; the fake
      plugin passes it in both start modes.
- [ ] **L1.11 Agent env and control.** The [minimal environment](spec/configuration.md#environment)
      plus `ELIOT_HOME`, `ELIOT_AGENT_ID`, `ELIOT_AGENT_TOKEN`, `PATH`, `system_note`
      (only commands this build has); token → caller identity; `control` checks from
      [coordination](spec/coordination.md#who-may-do-what); masking of known
      credential values in events ([core](spec/core.md#events)).

**Exit test (`crates/eliot/tests/e2e_l1.rs`, runs in CI):** start the daemon in a temp
dir and drive it only through the `eliot` binary:

1. launch fake agents A and B, and C with `--parent A`; `ls --tree A` shows A and C;
2. `send A '{"steps":[{"say":"hi"},{"approval":{"kind":"command","command":"rm x"}},{"say":"done"}]}'`
   → events in order: `turn.started`, `agent.busy`, `message.agent "hi"`,
   `approval.requested`, `agent.waiting`; `approve A` → `approval.resolved`,
   `agent.busy`, `message.agent "done"`, `turn.completed status=completed`, `agent.idle`;
3. `launch --wait 'hello'` prints `hello`; `cmd show` lists the launch `applied` at
   `session.ready` and `<id>/send` `applied` with its own `turn_id`. The same holds
   with a second entry `[plugins.fake-input]` (`cmd = ["eliot", "plugin", "fake"]`,
   `start_requires_input = true`);
4. `steer` on an idle agent → exit code 3 (`bad_state`); `steer` during a
   `sleep_ms` step → `command.applied`; `interrupt` during a sleep →
   `turn.completed status=interrupted`;
5. with a plugin entry `[plugins.fake-narrow]` (`cmd = ["eliot", "plugin",
   "fake"]`, `verbs = ["launch","send","stop"]`) and `launch --harness fake-narrow`,
   `steer` → exit code 3 (`unsupported`) and the plugin received no request;
6. `send A '{"steps":[{"hang":null}]}'`, then `eliot steer A x --id S1` (no reply
   comes; after its wait the CLI exits 6, pending), then kill the fake plugin process
   → `eliot cmd show S1` reports `unknown` (exit 4) and the steer is not resent; the
   plugin restarts; A, B and C are `lost` (their sessions died with it) and every
   event stored before the kill appears exactly once; `resume A` → `agent.idle` with
   no `message.user` repeating an old prompt;
7. kill the daemon, start it again → the same agents are listed with the states they
   had (live ones now `lost`, since their plugin died with the daemon);
8. launch D; `send D '{"steps":[{"sleep_ms":500},{"say":"ok"}]}' --wait` prints `ok`;
   while D sleeps in another turn, a second `send D x` is `queued` and runs after it;
   `resume A`, then `wait A D --for idle` returns 0;
9. with `[profiles.fake2] harness = "fake"`, `max_live = 2`, launching 3 agents leaves
   the third `created` until one of the first two stops; the same with
   `[accounts.acc] max_live = 2` shared by two profiles;
10. resubmitting a command with the same `--id` returns the first result; a fresh id
    with a ULID time 2 days old → exit 1 (`invalid`, `id_expired`); with
    `limits.max_selector_targets = 2` in the test config and 3 live agents,
    `send @all x` → exit 1 and no command is created;
11. launch 1,000 fake agents in under 30 s with daemon RSS under 200 MB.

## L2 — First real harness: Codex

Goal: one real harness fully controlled, including a proxy.
Specs: [Codex](harnesses/codex.md), [OpenCodex](harnesses/opencodex.md),
[configuration](spec/configuration.md#proxies).

- [ ] **L2.1 Recording and replay.** `ELIOT_RECORD` / `ELIOT_REPLAY` support in
      `eliot-plugin` ([format](spec/plugin-protocol.md#recordings)) and `just
      record-codex <scenario>`. Each later L2 task records its own
      [scenarios](harnesses/codex.md#scenarios) in the same PR, once its verbs exist.
- [ ] **L2.2 Codex plugin: launch and turns.** First commit the schema generated by
      the installed `codex app-server generate-json-schema` under `plugins/codex/schema/`
      (its Codex version goes into the fixture metadata). Then: one plugin process and
      one `codex app-server` (stdio) per agent in the agent's custody, `initialize`,
      `thread/start` → `session.ready`, the linked send → `turn/start`, map
      notifications to events, final text, usage; record `turn` and `two-turns`; tests
      replay them.
- [ ] **L2.3 Control verbs.** `send`, `steer` (exact active turn id), `inject`,
      `interrupt`, `stop` (also clears an active native goal), `resume`, `configure`
      (reports `applies`), plus the `no-reply` and `reattach` replay scenarios the
      conformance kit needs.
- [ ] **L2.4 Approvals and questions.** Native requests → `approval.requested` /
      `question.asked`; `approve`/`deny`/`answer` (the plugin holds the native request
      until the command arrives).
- [ ] **L2.5 Native goal.** `goal.set|pause|resume|clear` mapped to Codex thread goals
      with the [status mapping](harnesses/codex.md#verb-mapping); `goal.updated`;
      CLI `eliot goal set|pause|resume|clear|show`. `check` and `budget` are rejected
      as `unsupported` until L6.6.
- [ ] **L2.6 Children.** Subagent threads → `child` → `origin=native` agents
      (`own_identity = false`: they share the app-server's environment); over-limit
      children are still recorded.
- [ ] **L2.7 Permissions and proxy.** `permissions` mapping from the Codex page;
      `[proxies]` attach/launch/health as a refcounted service with `source.health`;
      account holds and probe ([accounts](spec/configuration.md#accounts)),
      `eliot hold`; Codex pointed at OpenCodex with the
      [default wiring](harnesses/codex.md#proxy-opencodex-or-any-responses-compatible-endpoint).
- [ ] **L2.8 Live check.** `just live-codex`: launch, send, steer, inject, interrupt,
      approve, goal, child — through OpenCodex and without. Paste the output into the
      PR.

**Exit test:** `eliot plugin-test` passes against the Codex plugin in replay mode (CI);
`just live-codex` passes on a machine with Codex installed.

## L3 — Monitoring

Specs: [monitoring](spec/monitoring.md).

- [ ] **L3.1 Process monitor.** Identities (with the Linux boot id), descendants from
      custody, stats, `session.lost`, `stop --force` verification, `degraded` custody
      reporting on Unix (custody itself exists since L1.6).
- [ ] **L3.2 Workspaces.** `--worktree BRANCH [--base REF]` following
      [workspaces](spec/monitoring.md#workspaces): `base_oid` frozen at admission,
      `git worktree add` under `<data_dir>/worktrees`, `setup` run once and never
      rerun after an unknown outcome, per-agent Git identity through the environment
      ([coordination](spec/coordination.md#who-works-on-this)), `eliot worktree
      remove`.
- [ ] **L3.3 Git monitor.** Status/commit/diff sets with `--no-optional-locks`,
      fs-triggered refresh, backoff for slow repos, `git.*` events, `agent_edits`.
- [ ] **L3.4 Transcripts and discovery.** Codex rollout tail (observe-only reads
      rollouts only) with [cursors](spec/plugin-protocol.md#cursors-and-replay)
      (`source_cursors`); `native_scope` of a rollout is `codex:<absolute CODEX_HOME>`;
      paged `eliot discover` (excluding sessions live agents already control), `eliot
      attach` and `attach --control --profile P` on the same agent row; rollout
      fixtures recorded in this PR.
- [ ] **L3.5 Liveness.** Stall flag, source health, `source.gap`, `unknown` display; an
      injectable clock for all timers (used again by L6).
- [ ] **L3.6 Views.** `eliot top`, richer `show`, `watch` formatting.

**Exit test:** killing a native Codex process yields `agent.lost` within 10 s; a
commit in an agent worktree yields `git.commit` within 20 s; an idle-but-alive agent
is never shown as lost; a Codex session started by hand is discovered and watched,
and restarting the daemon while it is watched stores no event twice.

## L4 — Other harnesses

One task per harness, in this order. Each: plugin + recordings + conformance kit in
replay mode + `just live-<name>`; set `start_requires_input` and `child_environment`
from what the recordings show; correct the harness page and the
[matrix](harnesses/README.md) where reality differs. Plugins may be TypeScript/Python
first (R11).

- [ ] **L4.1 Hook shim.** `eliot hook`, `hook.event`, session-scoped hook config
      generation, inject delivery via hook context, default on timeout. Build it with
      the first plugin that needs it (Codex/Antigravity hooks or the Claude CLI port);
      skip while no plugin uses it.
- [ ] **L4.2 Claude Code** (Agent SDK; in-process hooks for inject/steer/approvals).
- [ ] **L4.3 Muse** (MSP over stdio; official Rust SDK or low-level connection).
- [ ] **L4.4 OpenCode** (`opencode serve` HTTP + SSE; one server per agent; attach to a
      running server).
- [ ] **L4.5 Antigravity** (`agy` stream-json, warm process).
- [ ] **L4.6 ACP generic** (any ACP agent, e.g. Gemini CLI).
- [ ] **L4.7 Others** ([Command Code, Kilo, Zed](harnesses/others.md)) — when needed.

**Exit test:** the same scripted scenario (launch, send, steer, inject, interrupt,
approve, stop) passes on every L4 harness, where each verb either works or is rejected with a
reason that matches the harness page.

## L5 — Coordination

Specs: [coordination](spec/coordination.md).

- [ ] **L5.1 Mailbox.** Send/inbox/ack/read, modes, selectors, delivery format,
      `--wait`, wake-up notices (full text for agents without their own identity),
      `writes` table for non-command write ids.
- [ ] **L5.2 Shared vars.** Scopes, CAS, `watch`, `var.changed`.
- [ ] **L5.3 Who + claims.** Git sets + `agent_edits`, history mapping, claim
      conflicts, claims dropped when the agent stops or detaches.
- [ ] **L5.4 Hierarchy ops.** Tree selectors, `stop --tree`, parent notifications,
      `max_depth`, `max_children_per_agent`, `max_tree_agents`.
- [ ] **L5.5 MCP server.** `eliot mcp` for harnesses that prefer tools over shell. The
      tool set follows the caller's control: `self` gets inbox, msg, var, who, claim,
      ls; `tree`/`project` add launch, send, wait, result, steer, stop. One bridge per
      session: it loads nothing at start (a v1 donor bridge that loaded its whole
      ledger used ~207 MiB each), holds no Store handle and exits when stdin closes.
      Optional: build it only if a harness cannot run shell commands. `eliot show`
      reports whether the agent has actually used `eliot`/MCP yet (`unused` / `used`),
      so a harness that cannot reach it is visible.

**Exit test:** two real agents (different harnesses) agree on a shared variable via
messages; one runs `eliot who` and names the other; a claim conflict notifies both; an
`eliot inbox` killed before its acknowledgement shows the same messages again.

## L6 — Automation

Specs: [automation](spec/automation.md). Time comes from an injectable clock so timer
tests run in simulated time.

- [ ] **L6.1 Rules engine.** TOML loading/reload, event triggers, matchers,
      placeholders, limits, loop guard, `rule.*` events, `rule_cursors`, `eliot rule
      test`.
- [ ] **L6.2 Timers.** `every` (anchored slots), `cron` (DST rules), `at`, catch-up,
      in-flight skip, firing keys committed with their commands.
- [ ] **L6.3 Actions.** Actions stored as commands and run in order; agent actions,
      git actions (fetch/pull/push/worktree with readback), `msg.send`, `var.set`,
      `notify`, `emit`; project rules limited to their project.
- [ ] **L6.4 Scripts.** `run` action with the NDJSON contract; example scripts.
- [ ] **L6.5 Approvals policy.** Approval rules; synchronous answers for hook
      decisions; escalation of unanswered requests.
- [ ] **L6.6 Goal loop.** Eliot mode and native-mode budgets, read-only progress
      fingerprint, `no_progress`, round keys, parent notification, `goals` table.
- [ ] **L6.7 Recipes.** `examples/` with the [workflow recipes](spec/automation.md#workflow-recipes).

**Exit test:** 24 h of simulated time with the fake plugin and a local Git remote: a
rule firing every minute, a cron fetch, a goal reaching `done`, a goal hitting
`no_progress`; the daemon is killed and restarted twice during the run; zero duplicate
actions, zero missed timer firings, a rule with a broken placeholder fails once per
event without blocking the next, and the loop guard suppresses a deliberately looping
rule.

## L7 — Scale and hardening

- [ ] **L7.1 Load test.** 10,000 fake agents, 2,000 persisted events/s for 1 h: flat
      RSS, CLI `ls`/`show` p95 < 100 ms, no event loss for persisted kinds; reads never
      wait behind the writer. (v1 reference: status p95 ≈ 1.2 ms with 200 IPC clients
      on Windows, but at most 285 persisted events/s because it committed and fsynced
      every event — group commit is what makes the target reachable.)
- [ ] **L7.2 Linux exact custody.** Only if the owner runs agents on Linux: a
      delegated cgroup v2 per agent, so `stop --force` and daemon death are exact there
      too; until then Unix custody stays `degraded`.
- [ ] **L7.3 Retention.** Event and command pruning in batches; DB size stable in the
      load test.
- [ ] **L7.4 Drain under load.** Verify that drain stops rule, timer and goal-loop
      firings while approvals, answers and results keep working, with 1,000 busy
      agents.
- [ ] **L7.5 Windows hardening.** Edge-case tests for the
      [process start rules](spec/monitoring.md#starting-processes) (applied since
      L1.6): paths with spaces and non-ASCII characters, very long prompts, `PATH`/
      `Path` merge, nested Jobs.
- [ ] **L7.6 Service mode.** Codex `app-server --listen unix://…` (or
      `ws://127.0.0.1:<port>` where unix sockets do not work) and `opencode serve` run
      as Eliot services in their own Job without kill-on-close; the plugin connects to
      them; a daemon restart re-attaches and running turns continue; `detach` becomes
      possible for these sessions. `eliot service list|stop`.
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
