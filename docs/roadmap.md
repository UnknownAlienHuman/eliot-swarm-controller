# Roadmap

Build the product in vertical layers. During implementation, write working code first
and run only formatting plus the smallest changed-package Clippy gate. Do **not** stop
each layer to build a broad test campaign. The acceptance cases below are retained as
one qualification backlog and are implemented/executed after the feature-complete
local product exists (L7).

Each task is one PR and one manager-owned worktree. Tick its box in the PR that lands
the implementation. This file is the only status tracker.

How to implement a task: [AGENTS.md](../AGENTS.md).

Real-harness research/recording is allowed when needed to discover a protocol fact.
Keep it short and use the cheapest model/effort that proves the behavior. Formal live
qualification belongs to L7 and never runs in normal CI.

## Implementation gate

Every code PR runs:

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add another changed package only when the edited contract makes it compile together.
No workspace-wide Clippy, broad test suite, load run or live model run during the
implementation phase. A PR may add a tiny deterministic regression test only when the
code cannot be reviewed safely without it; it is not a reason to build a framework.

At L7 the complete product gets the conformance, end-to-end, crash/restart, platform,
security and load campaign described under **Qualification backlog**.

## Code budgets

Production Rust lines per crate, checked by `just loc` against `budgets.toml`.
`tokei <dir>/src --exclude '*tests.rs'` counts code only. A PR that exceeds a budget
must either delete/simplify code or change the budget in this table with one concrete
reason.

| Crate | Budget |
| --- | ---: |
| `eliot-core` | 3,000 |
| `eliot-store` | 2,500 |
| `eliot-daemon` | 10,000 |
| `eliot-plugin` | 1,000 |
| `eliot` (binary) | 3,000 |
| each plugin crate (`plugins/<name>`) | 3,000 |

The slightly larger Store/daemon/SDK budgets include generation fencing, exact process
custody and the automation action journal. They do not authorize a workflow engine.
For scale, v1 reached about 533,000 Rust lines and one 9,105-line file.

---

## L0 — Reset and skeleton

Goal: remove the v1 implementation without losing it as a donor, then create the
smallest compilable workspace.

- [ ] **L0.1 Archive v1.** Create tag `v1-final` at `dc6f70aa`. Delete the v1
      implementation from `main`: `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`,
      `crates/`, `modules/`, `migrations/`, `src/`, `tests/`, `tools/`, `vendor/`,
      `config/`, `docs/owner-decisions.md`, all old workflows, `Justfile`, and
      `THIRD_PARTY_NOTICES.md`. Keep the new docs, root README/agent instructions and
      repository metadata. v1 remains readable with `git show v1-final:<path>`.
- [ ] **L0.2 Skeleton.** Rust 2024 workspace, resolver 3, pinned stable toolchain and
      only the `eliot` binary with `eliot --version`. Add `budgets.toml` and a
      `Justfile` with `fmt`, scoped `clippy`, `loc`, and separate deferred
      `test`/`qualify` targets. Workspace lints deny `clippy::all`; unsafe is denied
      except a narrowly documented platform module.
- [ ] **L0.3 CI.** One always-reported workflow on Windows and Ubuntu. During L0–L6 it
      runs Markdown/link validation, `cargo fmt --check`, changed-package Clippy and
      `just loc`; Markdown-only changes skip Cargo. Full tests are enabled only by the
      L7 qualification PR.

**Layer completion:** the reset branch contains only the new documentation and the
minimal workspace; scoped Clippy is green on both platforms.

## L1 — Core, Store, bus and fake harness

Goal: one complete local command path with exact identity and no real provider.
Specs: [core](spec/core.md), [API](spec/api.md),
[plugin protocol](spec/plugin-protocol.md), [configuration](spec/configuration.md).

- [ ] **L1.1 Core model.** Ids, agent generation, current turn/request binding,
      events, commands, selectors, states and the pure reducer. Wrong generation or
      wrong turn is an ignored historical input, never current completion. An open
      stop command is separate from observed agent state.
- [ ] **L1.2 Store.** Numbered migrations, L1 schema, one writer task with bounded
      group commit, read pool and queries used by list/get/events/commands. Implement
      the real failure boundary: failed commit blocks admissions and retains the same
      pending result; it cannot persist a health event through the failed commit.
- [ ] **L1.3 Bus.** Persist-then-broadcast for durable events; fan-out-only ephemeral
      events; subscribe by registering live buffer first, replaying to a persisted
      watermark, then releasing buffered events above it. Bounded subscriber queues
      report lag explicitly.
- [ ] **L1.4 Daemon and IPC.** Foreground/background daemon, OS lock,
      generation-safe `daemon.json`, user-only named pipe/Unix socket, multiplexed
      JSON-RPC and daemon status. Implement admission mode and exact drain semantics:
      no new work, pre-admitted work continues, answers/stop/readback remain usable.
- [ ] **L1.5 Configuration.** Global + registered-project config, profiles/defaults,
      one-level `extends`, implicit profiles, plugin config, strict unknown keys and
      current-generation Eliot environment variables. Parse proxies for L2.
- [ ] **L1.6 Plugin host and custody.** Implement protocol framing, deadlines,
      bidirectional requests and source ownership. Support both manifest modes:
      per-agent process/Job group for process-owning harnesses and shared process only
      for isolated native services/fake. `agent.start` commits one matching
      `session.ready` before success. Lost replies become unknown; no control verb is
      retried automatically.
- [ ] **L1.7 Router.** Preflight launch; exact validation of target/capability/control,
      generation, turn and request; immutable admission; readiness lanes; FIFO queued
      sends that do not block current-turn control; selectors; idempotent admission
      ids; native identity uniqueness; profile launch queue; stale-generation expiry;
      owner resolution of unknown outcomes.
- [ ] **L1.8 Fake plugin.** Shared deterministic plugin implementing scripted turns,
      requests, children, crash/hang/exit, stable native ids, generation/turn/request
      rejection and persisted replay/readback state.
- [ ] **L1.9 CLI.** Daemon/status, launch/list/show/watch/events, all core verbs,
      wait/result and command show/cancel/resolve with JSON output, stable ids and
      documented exit codes.
- [ ] **L1.10 Agent identity and control.** Inject `ELIOT_HOME`, agent id,
      generation, current token and PATH; stale supplied tokens fail rather than
      becoming owner. Apply self/tree/project control and known-value redaction.
- [ ] **L1.11 Qualification hooks.** Add the minimal `eliot plugin-test` command and
      fixture/replay interfaces, but defer the complete conformance suite and its
      execution to L7. The implementation must not create another test framework.

**Layer completion:** through the CLI, a fake agent can launch, run a turn, wait for an
approval, receive an exact answer, complete, queue a second turn, be interrupted, stop
and resume as a new generation. Changed-package Clippy is green. The complete edge
case matrix is listed under L7 rather than blocking implementation here.

## L2 — First real harness: Codex

Goal: one useful real harness through its native app-server and optional OpenCodex
proxy.

- [ ] **L2.1 Installed schema and recordings.** Generate the app-server schema from
      the installed Codex version. Add bounded record/replay transport and record only
      the scenarios needed while implementing each verb; metadata includes version
      and date, secrets removed.
- [ ] **L2.2 Per-agent launch and turns.** One plugin process + one stdio app-server
      per Eliot agent, inside exact custody. Initialize, start/resume one thread,
      establish effective model/provider/cwd, emit `session.ready`, map turns,
      messages, tools and usage. A lost thread-start reply is read back, never retried.
- [ ] **L2.3 Control.** Send, exact-turn steer/inject/interrupt, stop with native goal
      clear, resume as a new Eliot generation, configure with `applies`. Stale
      generation/turn requests are rejected before native I/O; unknown controls are
      not retried.
- [ ] **L2.4 Requests.** Map native approvals/questions with exact request ids and
      implement approve/deny/answer. No default choice and no "latest request" guess.
- [ ] **L2.5 Native goal.** Set/pause/resume/clear, status mapping and goal-clear on
      stop. Check/budget remain unsupported until Eliot automation exists.
- [ ] **L2.6 Children.** Discover/register subagent threads with exact parent thread
      and parent generation; show partial family coverage honestly.
- [ ] **L2.7 Permissions and proxy.** Native permissions mapping; managed/attached
      proxy health and refcounting; account/profile launch holds; effective endpoint
      readback; OpenCodex wiring without editing owner global config.
- [ ] **L2.8 Useful path.** One manual developer run demonstrates launch → tool/write
      → final text and exact stop through Codex, with and without OpenCodex. Save the
      recording/output for L7; do not launch a broad test campaign yet.

**Layer completion:** Codex performs real work through Eliot and its changed packages
pass scoped Clippy. Unverified protocol facts remain named on the harness page and are
resolved before L7 qualification.

## L3 — Monitoring and workspaces

Specs: [monitoring](spec/monitoring.md).

- [ ] **L3.1 Process monitor.** `(pid,start_time)`, custody enumeration, bounded stats,
      exact group verification and `session.lost`. Shared-service death is reconciled
      per native session; it does not blindly lose/kill every agent.
- [ ] **L3.2 Worktrees.** Create/reuse named branch worktrees under the data directory,
      run explicit setup argv, inject per-agent Git identity and remove only stopped,
      clean worktrees. Keep branches; never blanket clean/reset.
- [ ] **L3.3 Git/filesystem monitor.** Read-only `--no-optional-locks` status/commit/
      diff sets, watcher debounce, slow-repo backoff and the bounded changed-file
      fingerprint. Never use a temporary index, `git add` or object writes to detect
      progress.
- [ ] **L3.4 Transcripts and discovery.** Codex rollout discovery/observe-only attach,
      defensive cursors and explicit source ownership. A transcript cannot duplicate
      the controlled live stream; handoff without a provable cursor reports a gap.
- [ ] **L3.5 Liveness.** Progress-bearing events, stalls, source health/gaps and unknown
      display with one injectable clock. CPU/Git/mail activity does not clear a model
      stall.
- [ ] **L3.6 Views.** Top/show/watch expose generation, current turn, open stop,
      custody, source owner/gaps, progress age, usage and Git facts without creating
      another projection database.

**Layer completion:** process, source, Git and workspace facts are visible on a real
Codex agent; no monitor changes agent work or guesses completion. Scoped Clippy green.

## L4 — Remaining harnesses

One task per harness. Use the native mechanism and one plugin; do not emulate a missing
capability. Each plugin declares per-agent/shared mode, updates its harness page and
records the scenarios required for L7 conformance.

- [ ] **L4.1 Hook shim.** Build only with the first plugin that needs a fact absent
      from its primary stream. Session-scoped config, generation token, bounded input,
      safe timeout and no duplicate lifecycle ownership.
- [ ] **L4.2 Claude Code.** Agent SDK first; per-agent plugin/process custody;
      in-process hooks only for assigned facts and decisions.
- [ ] **L4.3 Muse.** Official MSP/SDK, exact session/turn ids and native goal/control.
- [ ] **L4.4 OpenCode.** Attach/launch `opencode serve`, HTTP/SSE generation selected
      from installed schema, shared-service session isolation and gap rebuild from
      native messages.
- [ ] **L4.5 Antigravity.** `agy` stream-json in per-agent custody, exact current
      Store/native state, no separate refresh-proof ceremony.
- [ ] **L4.6 ACP generic.** Native ACP JSON-RPC and only capabilities the agent
      actually declares.
- [ ] **L4.7 Others.** Command Code, Kilo, Zed only when needed.

**Layer completion:** every required harness can launch and perform its documented
useful path; unsupported verbs fail explicitly. No cross-harness framework beyond the
already proven plugin protocol.

## L5 — Coordination

Specs: [coordination](spec/coordination.md).

- [ ] **L5.1 Mailbox.** Durable send/inbox/read, selector expansion, sender generation,
      presentation state and bounded text. Queue mail does not grant control. Wake is
      recipient-configured, generation-bound, coalesced and never blindly retried.
- [ ] **L5.2 Shared variables.** Project/tree/global scopes, CAS, watch and source
      attribution including generation.
- [ ] **L5.3 Who and claims.** Existing Git/native edit facts, history mapping,
      generation-bound advisory claims and conflict messages. No locks.
- [ ] **L5.4 Hierarchy.** Tree selectors, stop-tree, parent notifications and bounded
      accidental fan-out. Parent completion notices obey mailbox wake policy.
- [ ] **L5.5 MCP client surface.** Build only if a required harness cannot use shell.
      Stateless stdio bridge, caller control enforced by daemon, no Store handle or
      ledger preload.

**Layer completion:** two different real harnesses exchange one addressed message,
CAS one shared fact and see each other's file area. No broadcast chatter or hidden
manager relay.

## L6 — Automation and useful workflows

Specs: [automation](spec/automation.md).

- [ ] **L6.1 Rules.** Strict TOML load/reload, committed-event triggers, matchers,
      safe placeholders, rate/depth/self-trigger guard and dry-run rendering.
- [ ] **L6.2 Durable run/action journal.** Immutable firing key and rendered actions;
      queued/dispatched/applied/failed/unknown states; restart turns dispatched into
      unknown; exact readback/manual resolution; no blind external rerun.
- [ ] **L6.3 Timers.** Anchored interval, cron with DST semantics, one-shot, latest-only
      catch-up and in-flight skip using the run journal.
- [ ] **L6.4 Actions.** Agent commands, mailbox/vars, Git fetch/pull/push/worktree,
      notify and emit. Every external writer has explicit readback or remains unknown;
      an unknown push blocks only later pushes to that repository.
- [ ] **L6.5 Scripts.** Explicit argv/custody, bounded stdin/stdout/stderr. Buffer all
      stdout actions; after exit 0 validate and append them transactionally before
      dispatch. Unknown script is never rerun.
- [ ] **L6.6 Approval policy.** Exact generation/turn/request binding, synchronous hook
      decision where supported, one escalation for unanswered requests.
- [ ] **L6.7 Goal loop.** Native/Eliot modes, generation + revision fence, bounded
      checks/cost/turns/no-progress, read-only workspace fingerprint and no continuation
      while an unknown command/request is open.
- [ ] **L6.8 Minimal recipes.** A few readable examples—writer/reviewer, main refresh,
      nightly maintenance, quota notification and session rotation. No workflow DSL.

**Feature-complete milestone:** local Eliot can launch/control/observe required
harnesses, coordinate agents and run bounded automation. Freeze feature work and start
qualification. Scoped Clippy must be green for every crate; this is the first point at
which the broad test campaign starts.

## L7 — Qualification, repair and hardening

Implement and run the tests now, against the feature-complete system. Repair the root
cause in code/spec; do not add compatibility wrappers or proof documents.

### L7.1 Deterministic core/end-to-end suite

- [ ] Reducer table: every state transition, duplicate input, wrong generation, wrong
      turn/request and late terminal input.
- [ ] Store: migrations/reopen, group commit, commit failure/recovery, no acceptance
      before commit and no replay of external effects.
- [ ] Bus: subscribe/replay boundary, persisted order, ephemeral best-effort semantics,
      slow-subscriber lag and source duplicate suppression.
- [ ] CLI fake-harness path: launch, turn, approval/question, queue ordering,
      configure-before-queued-send, exact steer/interrupt, stop failure without wedged
      state, resume generation, stale token, stale queued send, selectors/max-live.
- [ ] Crash/restart: plugin and daemon loss, unknown command/action, no retry including
      stop/interrupt, exact snapshot settlement, no old prompt/event replay.
- [ ] Drain: rejects new work, continues all pre-admitted commands/actions, permits
      answers/stop/readback, reports blockers and force-loses only owned groups.

### L7.2 Plugin conformance and real harnesses

- [ ] Complete `eliot plugin-test` for manifest/session mode, ready-before-success,
      generation/turn/request fences, event dedupe/source handoff, no unknown retry,
      exact custody, attach/readback and unsupported verbs.
- [ ] Replay every required plugin fixture on Windows and Linux where supported.
- [ ] Run `just live-<name>` for each required harness: launch, useful tool/write,
      send, supported steer/inject/interrupt, approval/question, goal/child, stop,
      resume, provider/proxy route. Use cheapest models and save compact output in the
      PR, not a permanent report hierarchy.
- [ ] Verify requested vs effective model/provider/permissions and the source owner /
      gap behavior on reconnect.

### L7.3 Coordination and automation

- [ ] Mail bursts coalesce; queue does not bypass authority; stale-generation wake does
      not reach replacement; unknown presentation is not retried.
- [ ] Vars CAS, `who`, claims, parent notifications and fan-out limits.
- [ ] 24 h simulated clock: interval/cron/one-shot/catch-up, no duplicate firing-key
      admissions, no missed latest slot, loop suppression.
- [ ] Crash every action phase. Queued resumes; dispatched becomes unknown; exact
      readback settles Git/command actions; scripts/webhooks are never blindly rerun.
- [ ] Goal done/no-progress/budget/pause/clear, concurrent native goal, stale
      generation/revision and incomplete fingerprint.

### L7.4 Platform, security and recovery

- [ ] Windows paths/non-ASCII/long input/PATH casing/console/Job inheritance/file locks;
      Unix process groups/socket permissions.
- [ ] Current-user IPC, private files, secret masking/redaction, symlink/special-file
      negatives and no invalid-token owner fallback.
- [ ] PID reuse, surviving descendants, plugin start/stop race, shared service isolation,
      attached service never killed, daemon/service re-attach.
- [ ] Git worktree cleanliness/removal, no index contention from monitoring, exact
      push readback and unknown writer hold.

### L7.5 Load and retention

- [ ] 10,000 fake agents and 2,000 persisted events/s for 1 h: flat bounded memory,
      no persisted-event loss, list/show p95 below 100 ms and readers not serialized
      behind the writer.
- [ ] Per-agent plugin churn with hundreds of simulated process-owning agents; shared
      plugin pool with thousands of sessions; no cross-agent force stop.
- [ ] Event retention in bounded batches and stable database size.
- [ ] Drain 1,000 busy fake agents without admitting new automation or dropping
      results/requests.

### L7.6 Hardening features proven necessary by qualification

- [ ] Service mode for Codex/OpenCode only after exact per-session re-attach is proven;
      service list/stop/detach and daemon restart continuity.
- [ ] `eliot doctor` read-only report for unmatched owned processes, unused services,
      stale worktrees, disk/database size and source gaps.
- [ ] Per-user autostart. Do not promise stdio-session survival across logoff/reboot;
      resume is explicit.
- [ ] Pattern-based persisted-text redaction on top of known-value masking; use the
      narrow v1 donor if still appropriate.

**Qualification exit:** full `just qualify` is green on Windows and Ubuntu; every
required live harness has a current compact run; all confirmed failures found during
qualification are repaired in code and their tests remain. Only then call v2 ready.

## L8 — Consolidation

- [ ] Port a retained non-Rust plugin only when qualification shows the runtime
      dependency is materially costly. Keep the same protocol and conformance suite.
- [ ] Delete temporary bridges, duplicate paths and fixtures that no longer correspond
      to a supported harness version.
- [ ] Re-check code budgets and split only real boundaries, not future concepts.

## L9 — Later, only by owner decision

Authenticated remote access, multi-host federation, TUI/web view and Eliot Memory OS
integration. Each needs a concrete use case and a new decision. v1 research remains
available under `v1-final`; it is evidence, not active implementation instructions.
