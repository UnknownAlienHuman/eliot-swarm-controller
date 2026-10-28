# Roadmap

Build order: **L0 workspace → L1 core/Store/bus → L2 direct Codex → L3 monitoring and
workspaces → L4 automation/scripts → L5 agent communication → L6 more harnesses →
L7 qualification**. Codex alone must already be useful at L2; another vendor, proxy,
rule engine or conformance framework must not delay that path.

The owner's 2026-10-27 clarification puts automation and coordination on the first
working harness before expanding the adapter catalogue. Requirements are unchanged.
This page owns stage numbers; harness pages describe capabilities, not another schedule.

Each checkbox is a useful increment, not a mandatory separate PR. One manager owns one
worktree. Combine closely coupled increments; tick only behavior actually implemented.
Read the linked spec sections for the current increment, not all future features.

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> -- -D warnings
```

Use the same command for library-only and binary-only packages. Run it for changed
packages, not the entire workspace by default. No model calls in CI. A short manual
exercise of the newly connected path is development, not a new acceptance ceremony.
Build the full deterministic/crash/platform/load test campaign at L7; do not postpone
ordinary compilation or discovering whether the current path runs until then.

## Code budgets

There is no blocking line-count quota, `budgets.toml` gate or required `just loc` system.
Review complexity by current callers and responsibility. Do not split crates to satisfy
an arbitrary line limit. The target architecture is a ceiling on concepts, not a demand
to create every crate/table immediately.

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

**Next:** L3.3c live recovery. The direct Codex/Luna and Windows workspace paths have
been exercised; v1 remains available as a source donor.

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

Checked on Windows on 2026-10-27 with Rust 1.99.1: formatting, scoped Clippy and a
debug build; two-Agent echo/control isolation, mutation-id retry/conflict, stale-generation
rejection, replay/live watch, stop/drain and restart without input replay. A focused
31 × 30,000-byte echo exercise returned every event across two bounded pages; a refused
commit acknowledgement retained unknown outcome and blocked the next input. Full L7
qualification and real Codex execution remain separate increments.

**Not in L1:** shared plugin pools, source handoff, discovery/control takeover, native
children, hooks, Goals, selectors, mail, rule revisions, scripts, worktree setup, proxy
supervision, account probes, MCP, recording framework or load tools. Their contracts
remain documented; their handlers/tables are added with their first caller.

## L2 — First useful harness: Codex with Luna

The direct path was exercised on Windows on 2026-10-27 with installed Codex 0.161.0
and `gpt-6-luna`: three completed turns created/read/updated one file, streamed text
and tool events, stopped and resumed the same native thread in generation 2 without
replaying input, then stopped and drained. Native readback confirmed the requested
`medium` effort, provider and sandbox. This is a useful-path check, not L7 qualification.
Three additional native control turns checked steer, one-shot approve/deny, interrupt
with an unanswered request, and a plan-mode question/answer. Same-ID approval retry
returned the retained result; an unoffered question ID rejected before a native reply.
All three control runs stopped/drained with no source gaps or duplicate native ids.

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
      The process/stats/liveness path is implemented and exercised on Windows with
      fake and native Codex. Git/fs observation, shared commit delivery and durable
      native edit attribution are connected and exercised on Windows.
- [x] **L3.2 Workspaces.** Existing workspaces remain the default. Add opt-in worktree
      creation with frozen base OID, one preparation row, explicit setup and no replay
      of unknown setup. Exact readback and clean-only removal; preserve the branch.
- [x] **L3.3a Discovery/views.** Paged metadata discovery without Agent creation or
      native session control. `show/watch/top` query existing projections, including
      workspace/setup process ownership alongside harness processes.
- [x] **L3.3b Observation/history.** Exact-subject observe attachment, private native
      and rollout history pages, and reader-only detach. Preserve unknown native state
      and keep observation separate from controlled identity and effects.
- [ ] **L3.3c Live recovery.** Transcript source handoff with explicit gaps and
      reconciliation. Commit family ownership and cursor boundaries before successor
      publication; never replay an uncertain external effect.
- [ ] **L3.4 Proxy/capacity.** First attach to an already-running OpenCodex endpoint.
      Add managed service launch only when actually needed. Add account/proxy caps and
      persistent provider holds with that route; freeze admitted nonsecret route/store
      selectors across daemon replacement. No automatic model/account fallback.

L3.1 checks on Windows on 2026-10-27: formatting, scoped Clippy and debug build;
read-only Git traps left index/HEAD/refs/objects unchanged; persisted HEAD recovered a
commit across daemon restart. A native Luna edit retained its exact cause/timestamp
through rapid stop/resume, and a live observer cleared terminal-generation provenance
only after clean readback. Two fake Agents each received one shared commit event;
filesystem observation did not invent authorship or model progress. One focused Windows
regression proved the Git leaf Job blocks child execution. Privileged/root Linux and
other Unix Git contexts return unavailable; Linux runtime and full L7 qualification
remain unverified. All exercise Agents and owned daemons stopped.

L3.2 Windows checks on 2026-10-27: formatting, scoped Clippy and debug build; fake
worktree echo, frozen-base comparison and preserved source dirt. Native Luna completed
one short turn; resume retained that thread and an advanced dirty checkout, kept the
completed setup count unchanged, and did not replay the original turn. Crash during
setup retained unknown index 0, no native authority and one observed effect; changed
second-step input failed before spawning and cancelled linked input. Clean removal
refused dirt, live subdirectory borrowers and retained-source dependencies, then removed
allowed scratch without unlinking clean tracked files, retained branch data and proved
path/registration absence. Same-ID retry after removal ignored a moved source ref and
did not recreate the checkout. Schema 5→6 preserved existing native-reference/turn
digests. All owned test processes exited. Linux fake worktree lifecycle runs in CI;
full crash/platform/vendor qualification remains L7.

L3.3a Windows checks on 2026-10-28, with Rust 1.99.1 and Codex 0.161.0: formatting,
scoped Clippy and debug build; fake enumeration returned three subjects over two pages,
and native enumeration returned two distinct references with bound cursors. Unloaded
native status stayed unknown, timestamps used milliseconds, and discovery left both
Agent count and durable event high-water unchanged. Focused cursor regressions cover
changed environment/store selection before native history can be enumerated.
During setup, `show/top/processes` exposed target generation 1 and real OS samples while
the Agent remained generation 0 with no native stream. Cross-scope/generation process
cursors rejected; cancellation retained generation 0 and explicit process exits.
Clean removal closed the workspace source, retained branches, and process-record hashes
survived daemon restart. The normal fake worktree echo/stop/remove path also passed.
Active-reader drain/force stopped their owned paths and rejected new discovery.
Windows background startup no longer retains caller capture pipes; a live check covered
spaces, Unicode and a trailing slash. All exercise daemons exited; no model input was
sent for this increment. Observe attachment/history and live source handoff remain
separate L3.3b/L3.3c increments.

L3.3b Windows native-read checks on 2026-10-28: integrated formatting, scoped Clippy,
focused discovery/store-policy/opened-file regressions and debug build passed.
The fake observe lifecycle passed with two history pages. A retained Codex 0.161.0
subject attached in generation 1 without native session control; its single persisted
turn was reported complete and items were read across two bounded pages. Four control
verbs rejected, same-ID attach retry kept one reader, and history added no observation
or native lifecycle events. Detach advanced the fence to generation 2, retained unknown
state across restart, and explicit attach used generation 3. Retrying the old detach
ID did not close the new reader. No native session-control invocation or session-end
event was observed; global config was unchanged and owned daemons exited.
An isolated schema-6 copy migrated to 7 with native-reference, Agent, turn, command,
process, unique-index and allocated-id digests unchanged; the source database was
unchanged. A separate rollout run returned one verified subject, one turn followed
by an empty page at the current end, and ten items for that exact native turn.
It reported no partial tail, detached, and drained with daemon exit 0. The config
copy matched its source; no model/session-control invocation or source edit occurred.
This private-history evidence does not establish live source handoff, which remains
the separate unchecked L3.3c increment.

**Result:** useful Codex work is observable and restart outcomes are explicit. The direct
route still works without any proxy installation or worktree machinery.

## L4 — Automation and scripts on Codex

Read [automation](spec/automation.md). Reuse the existing router and command storage.
No second executor, workflow graph or acceptance engine.

- [ ] **L4.1 Rules/timers.** Event-time subject, persisted cursor, bounded matching,
      interval/cron/one-shot and deterministic command admission. Add atomic definition
      replacement and bounded backlog when enabling hot reload, not before basic firing.
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
