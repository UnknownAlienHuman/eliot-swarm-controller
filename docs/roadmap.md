# Roadmap

Build order: **L0 workspace → L1 core/Store/bus → L2 direct Codex → L3 monitoring and
workspaces → L4 automation/scripts → L5 agent communication → L6 more harnesses →
L7 qualification**. Codex alone must already be useful at L2; another vendor, proxy,
rule engine or conformance framework must not delay that path.

The owner's 2026-10-27 clarification puts automation and coordination on the first
working harness before expanding the adapter catalogue. Requirements are unchanged.
This page owns stage numbers; harness pages describe capabilities, not another schedule.

Each checkbox is a useful increment, not a mandatory separate PR. Use one PR per coherent
increment; combine closely coupled checkboxes when they form one reviewable change. One
manager owns its branch/worktree and PR, never pushes directly to `main`, and integrates
and merges only after scoped checks and independent review. Tick only implemented behavior.
Read the linked spec sections for the current increment, not all future features.

## Implementation gate

```text
cargo fmt --check
cargo test -p <changed-package> --all-targets
cargo clippy -p <changed-package> --all-targets -- -D warnings
```

Run the tests and Clippy for each changed package, not the entire workspace by default.
Every completed connected increment has deterministic Cargo test coverage for its
caller-to-result path; use the fake for fake-suitable behavior. CI runs those tests and
checks, and does not call a model. A brief runtime exercise is useful development
evidence; record its actual result in the PR/CI. Full crash, load, vendor and platform
qualification remains at L7. Paid-model runs are not routine tests.

## Code budgets

There is no blocking line-count quota, `budgets.toml` gate or required `just loc` system.
Review complexity by current callers and responsibility. Do not split crates to satisfy
an arbitrary line limit. The target architecture is a ceiling on concepts, not a demand
to create every crate/table immediately. Keep test/check evidence in PRs and CI, not in
per-task report packets or another status registry.

## L0 — Archive and smallest workspace

- [x] **L0.1 Archive/reset.** v1 is preserved on branch `archive/v1-final` at
      `dc6f70aa08ea5fad3da04624ce50c46d45edd57c`. The legacy implementation, migrations,
      tests, tooling, vendor files, workflows and embedded owner-decisions are removed
      from main; v2 docs and root agent instructions remain. The archive retains the
      original third-party notices. Use the pinned commit, not an assumed tag.
- [x] **L0.2 Build.** Rust 2024 workspace, resolver 3, pinned Rust 1.99.1, one dependency-
      free `eliot` binary with help/version. CI runs fmt, scoped Clippy and brief
      help/version smoke commands on Windows/Ubuntu. No empty framework crates or
      test platform.

**Next:** L4.2 actions/scripts.

## L1 — Core, Store, bus and one fake command path

Read [core](spec/core.md), [API](spec/api.md) and the minimal profile in
[plugin protocol](spec/plugin-protocol.md#first-harness-subset).
Donors: the **L1–L2 implementation map** in [donors](reference/donors.md).

- [x] **L1.1 State/Store/bus.** Agent, generation, command, turn, event, one pure
      reducer and one SQLite writer. Commit event and projection before publishing.
      Allocate non-reused event ids. Implement bounded query/subscription replay.
- [x] **L1.2 Daemon/CLI.** Private data directory and local IPC, automatic owner
      credential, `daemon`, `launch`, `send`, `ls`, `show`, `watch`, `result`, `stop`
      and command readback. One explicit global profile; no project overlay system yet.
- [x] **L1.3 Plugin path.** Per-Agent process, framing/deadlines/custody, one fixed
      authoritative stream, commit acknowledgement, generation/turn binding and unknown
      input gate. Add a tiny fake that emits ready, echo turn and terminal result.
      Stop/restart must not resend an uncertain input.

**Result:** CLI → router/Store → fake → event bus → CLI works. The fake is a transport
exercise, not a simulator product. Add its approval/crash/input-created-session features
only when a real integration needs them.

**Not in L1:** shared plugin pools, source handoff, discovery/control takeover, native
children, hooks, Goals, selectors, mail, rule revisions, scripts, worktree setup, proxy
supervision, account probes, MCP, recording framework or load tools. Their contracts
remain documented; their handlers/tables are added with their first caller.

## L2 — First useful harness: Codex with Luna

Read [Codex bring-up](harnesses/codex.md#first-useful-path) and only the native operations
it uses. Start with the owner's installed Codex and existing login. Prefer the explicitly
selected Luna model returned by that account's native `model/list`; never substitute a
model or billing route silently.

- [x] **L2.1 Session/input/result.** Add the Codex plugin to the existing path:
      initialize/initialized → model discovery → thread/start → turn/start → events →
      final result. One app-server per Agent. Preserve requested/effective model and cwd.
- [x] **L2.2 Control.** Exact-turn steer/interrupt, exact approval/question replies,
      stop and explicit resume of the retained thread. Implement the unknown outcome
      path as part of each verb, not as a later recovery subsystem.
- [x] **L2.3 Use it.** In a disposable existing workspace, launch one small file-edit
      task, watch tool/text events, collect its result, send another turn, stop and
      resume. Keep this brief; do not build all L7 fixtures before continuing.

**Result:** an owner can use Eliot to do real development with Codex/Luna. No worktree
creation, OpenCodex, second harness, native child support or automation is required for
this first useful milestone. Unavailable Luna is an explicit profile error, not a reason
to invent a model alias or silently switch accounts.

## L3 — Monitoring, workspaces and the optional proxy route

Read [monitoring](spec/monitoring.md), relevant [configuration](spec/configuration.md)
sections, and [OpenCodex](harnesses/opencodex.md) only for the proxy task.

- [x] **L3.1 Observation.** Exact process identity, source drain before loss, bounded
      stats, busy-vs-waiting liveness, read-only Git/fs monitor and `agent_edits`.
      Includes process/stats/liveness, Git/fs observation, shared commit delivery and
      durable native edit attribution.
- [x] **L3.2 Workspaces.** Existing workspaces remain the default. Add opt-in worktree
      creation with frozen base OID, one preparation row, explicit setup and no replay
      of unknown setup. Exact readback and clean-only removal; preserve the branch.
- [x] **L3.3a Discovery/views.** Paged metadata discovery without Agent creation or
      native session control. `show/watch/top` query existing projections, including
      workspace/setup process ownership alongside harness processes.
- [x] **L3.3b Observation/history.** Exact-subject observe attachment, private native
      and rollout history pages, and reader-only detach. Preserve unknown native state
      and keep observation separate from controlled identity and effects.
- [x] **L3.3c Live recovery.** Codex observe-only live rollout facts after settled
      exact-subject attach; gap-backed `observe` to `transcript` handoff and bounded
      reconciliation. Commit ownership/cursor boundaries before publication; keep
      private history private and never replay an uncertain external effect.
- [x] **L3.4 Proxy/capacity.** First attach to an already-running OpenCodex endpoint.
      Add managed service launch only when actually needed. Add account/proxy caps and
      persistent provider holds with that route; freeze admitted nonsecret route/store
      selectors across daemon replacement. No automatic model/account fallback.

**Result:** useful Codex work is observable and restart outcomes are explicit. The direct
route still works without any proxy installation or worktree machinery.

## L4 — Automation and scripts on Codex

Read [automation](spec/automation.md). Reuse the existing router and command storage.
No second executor, workflow graph or acceptance engine.

- [x] **L4.1 Rules/timers.** Global and owner-registered project rules use event-time
      subjects, durable cursors, bounded matching/reload/backlog and finite history
      catch-up. Interval/cron/once/manual timers and action limits use ordinary command
      admission; connected timer and event-rule action support is defined in the
      [automation spec](spec/automation.md).
- [ ] **L4.2 Actions/scripts.** Agent commands first, then bounded explicit scripts.
      Frozen inputs, scoped principal, exit/output handling and unknown outcomes use
      normal commands. Add Git fetch/push wrappers only with their concrete recipe;
      direct harness Git remains harness work, not intercepted by Eliot.
- [ ] **L4.3 Goals/requests.** Native Goal where installed Codex supports it; Eliot
      continuation/checks with turn/cost/no-progress limits. No check runs under the
      Store lock. Neither a failed check nor a missing terminal fact authorizes replay.
- [ ] **L4.4 Recipes.** One timer, one event action and one bounded Goal using Codex.
      No second harness is necessary to demonstrate these behaviors.

**Result:** the first harness can work unattended under explicit limits. Hook support
is added only for a native decision not expressible by the already-working stream.

## L5 — Agent communication on the same harness

Read [coordination](spec/coordination.md). Use two Codex Agents initially.

- [ ] **L5.1 Mail/variables.** Addressed mail, separate delivery and acknowledgement,
      bounded coalesced notices, shared variables and CAS. No automatic status chatter.
- [ ] **L5.2 Work attribution.** `who` reads L3 Git/edit projections; claims are
      advisory. No second ownership database or file-lock promise.
- [ ] **L5.3 Hierarchy.** Controller-launched children and tree selectors, then native
      child discovery/control where supported. An existing child is never hidden by a
      fanout limit. Parent completion is not child completion.

**Result:** two Agents exchange an actual message, share one variable and identify each
other's file area without a manager relay. Cross-vendor communication is a later check,
not a prerequisite for mail.

## L6 — Additional harnesses and only demonstrated missing features

Read the selected [harness page](harnesses/README.md). Integrate one at a time through
the existing protocol. Start with Claude Code and OpenCode for the product acceptance
scenarios. Muse, Antigravity, ACP and other adapters are opt-in tasks, not blockers for
Codex or for unrelated adapters.

- [ ] **L6.1 Claude Code.** Native SDK/stream, exact callbacks and reported application
      points. Delayed hook context is inject, not exact-turn steer.
- [ ] **L6.2 OpenCode.** Select one installed API. Shared mode only if per-session
      attribution/environment/stop are supported; otherwise per-Agent server.
- [ ] **L6.3 Cross-harness use.** Reuse L4/L5 rules/mail against the added plugins.
      Add hooks, shared mode, native children or MCP only when a concrete caller needs
      the capability. Missing semantics reject; no compatibility executor.
- [ ] **L6.4 Selected optional adapters.** Add only those explicitly selected for the
      release; record the selection here. Unselected adapters stay absent.

**Feature-complete local scope:** core, Codex, monitoring/workspaces, automation,
coordination and the selected adapters. Do not hold this milestone for an unused vendor
or speculative remote/GUI/service feature.

## L7 — Qualification, repair and release

Now build/run the complete campaign against the connected product. Keep results in the
PR/CI, not a permanent audit/readiness hierarchy.

- [ ] Core/Store/API/reducer and event replay, including retention and non-reused ids.
- [ ] CLI fake E2E, exact generations/turns/requests, queue ordering and unknown outcomes.
- [ ] Crash at admission, native effect, event commit, reply and source-drain boundaries;
      verify no repeated prompt/setup/script/push and no false terminal result.
- [ ] Common plugin conformance, sanitized recordings and compact live runs for the
      selected harnesses/routes. Verify effective model/account/permissions.
- [ ] Rules/timers/hot reload, Goal limits, mail/ack, CAS, hierarchy and Git recovery.
- [ ] Windows Job/handles/paths; supported Linux behavior and honest degraded custody;
      private IPC, credentials, resource bounds and adopted-service isolation.
- [ ] Retention/WAL/backpressure and representative load. The 10,000-fake-Agent,
      2,000-events/s owner run is a release capacity check, not a prerequisite to L2.

Fix the root cause and keep focused regressions. No runtime readiness claim until the
relevant real path is exercised. A missing optional harness does not block qualification
of the explicitly selected release scope.

## Later

Service continuity across daemon replacement, remote/multi-host operation, GUI and
Memory OS require an actual use case. The current contracts are not instructions to
build those products in advance.
