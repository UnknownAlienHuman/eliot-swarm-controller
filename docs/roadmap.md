# Roadmap

Build order: **L0 workspace → L1 core/Store/bus → L2 direct Codex → L3 monitoring and
workspaces → L4 automation/scripts → L5 agent communication → L6 more harnesses →
L7 qualification**. Codex alone must already be useful at L2; another vendor, proxy,
rule engine or conformance framework must not delay that path.

The owner's 2026-10-27 clarification puts automation and coordination on the first
working harness before expanding the adapter catalogue. Requirements are unchanged.
This page owns stage numbers; harness pages describe capabilities, not another schedule.

Each checkbox is a useful increment; tick only implemented behavior.
See AGENTS.md for the work loop.

## Unified settings and hot start (2026-11-15)

**Next:** L7 qualification.

- [x] **S1 Settings store/API.** One revision-guarded `settings.read/save` path for
      profiles, routing, projects, daemon limits/retention, automation, mail,
      cloud MCP and desktop preferences; strict section validation, dry run and
      bounded Store history with server-derived author. Replace desktop JSON.
- [x] **S2 Hot start.** Private directory creation, visible native-default profiles
      for installed harnesses, automatic Git project registration, daemon startup
      from desktop, explicit safe remedy for unsafe existing roots, and a real
      profile-free launch from a clean directory.
- [x] **S3 Routing.** Ordered role candidates, installed/provider-hold/quota/capacity
      selection before admission, recorded choice/reason, authorized child scope
      and no retry or reroute after dispatch or uncertain delivery.
- [x] **S4 Assignment through automation.** Settings-managed existing rules/timers
      keep a configured number of Agents busy on a brief queue through ordinary
      routing-aware launch; retain exact command identity and unknown gates.

S1-S4 are implemented. The configuration and automation specs own their contracts;
fleet-wide and longer-running qualification remains in L7.

## Repairs to implemented paths

Checked layers record delivered increments, not full crash/load qualification.

- [x] **Restart capacity recovery.** Reconcile exact prior-generation custody and release
      or transfer the reservation under the [capacity contract](spec/configuration.md#accounts-and-configured-capacity).
      A retained old slot currently blocks resume even without a cap and can exhaust a
      shared scope. Completion: proven closure frees/transfers that slot, stale closure
      cannot free a newer slot, and unresolved custody stays visibly blocked without replay.
- [x] **Git fetch failure settlement.** Implement the [Git settlement contract](spec/automation.md#failed-transfer-and-unknown-readback):
      after interruption/custody checks, handle a definite nonzero exit before reading the
      private ref. Exact unchanged tracking-ref readback permits `failed`; a missing exit
      status is not a definite nonzero exit and still requires the private ref, and
      uncertain custody/readback/CAS must not be treated as definite failure.
      Completion: the next same-repository mutation can run after a failed transfer;
      genuinely unknown work remains fenced.
- [x] **Unknown-outcome readback/resolution.** Connect exact readback and constrained owner
      resolution under the [API contract](spec/api.md#input-turn-and-unknown-gates), including
      a send with a lost native turn association. Release only the resolved command/turn
      gate; never invent native completion or override capacity. Advertise the RPC/CLI
      methods only when their handlers exist.
- [x] **Separate existing responsibilities.** Extract Store admission/dispatch and Router
      command execution before adding more branches to those owners. Separate timer
      scheduling and event-rule traversal where they share only storage plumbing.
      [Architecture](architecture.md#internal-module-boundaries) names the boundaries;
      retain transaction/effect ordering and the one writer/router. No crate/trait/LOC
      target or parallel executor.
- [x] **Ordered Store reads.** For `list_unsettled_commands`, obtain an ordered partial-index
      plan that preserves observer exclusions and `created_at,rowid` order. Verify planner
      statistics/migration behavior; merely adding an index may leave the old sort plan.
      For `list_processes`, pair `(owner,host_boot_id,pid,start_time)` with separate first-page
      and cursor-range queries; the optional-cursor OR prevents a direct range seek.
      Check identical pages and near/tail cursor plans on the bundled SQLite version.
- [x] **Linux process sampling.** Share one bounded `/proc` enumeration per monitoring
      sweep across the current targets, instead of repeating it for each generation.
      Follow [sampling work](spec/monitoring.md#sampling-work): preserve identity checks,
      per-owner limits, incomplete samples and honest Unix custody. Measure enumeration
      count and sample age before claiming a latency improvement.
- [x] **Unstarted plugin cleanup.** Validate before creating an instance directory where
      possible; clean the exact generated empty directory on pre-spawn errors in
      `PluginHost::start` and partial-create errors in `create_instance_dir`. Use empty-only
      removal as the reader path does. Post-spawn or nonempty plugin data needs its own
      lifetime evidence, not recursive deletion.
- [x] **Reconcile historical issues.** Revalidate #200 against the current generation
      model; its pre-reset binding allocator/validator design is not an implementation
      dependency. Mark the v1 work in #1, #3, #4, #5, #11, #140, #153 and #159 historical
      or superseded after preserving any still-relevant requirement in its owning spec
      and current layer. #199 is now the owner-authorized M1 increment below. Open issue text must not
      silently restore the archived runtime or override this build order.

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

## L1 — Core, Store, bus and first command path (completed)

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
      input gate. (L1 bootstrap used a temporary fake emitting ready/echo/terminal
      result; completed history — removed; current adapters are the five native
      adapters with explicit profiles. Stop/restart must not resend an uncertain input.)

**Result (completed history):** CLI → router/Store → temporary fake → event bus → CLI
worked. The fake was a transport exercise, not a simulator product, and is no longer a
runtime dependency.

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
- [x] **L3.3d Observe what matters.** `activity.heavy` episodes with basename-only
      categories, `session.growth` warnings with no auto-compact, and `harness.drift`
      against pinned verified contracts surface in `swarm top` (`OBS`), `swarm stats`,
      `swarm heavy`, `swarm doctor` and the desktop Details Observe section.

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
- [x] **L4.2 Actions/scripts.** Agent commands first, then bounded explicit scripts.
      Frozen inputs, scoped principal, exit/output handling and unknown outcomes use
      normal commands. Add Git fetch/push wrappers only with their concrete recipe;
      direct harness Git remains harness work, not intercepted by Eliot.
      Agent actions, bounded Run scripts and `git.fetch` are connected; the remaining
      wrappers stay unsupported as specified in [automation](spec/automation.md).
      Bounded `command.children` readback for retained expansions is connected
      through the same Router/CLI, and that target API is advertised.
- [x] **L4.3 Goals/requests.** Eliot continuation/checks with turn and
      no-progress limits run through explicit idle rounds, automatic `on_idle`
      driving and explicit cadence slot rounds; cadence goals are manual-only
      (no auto-fire) and the read-only workspace fingerprint feeds
      no-progress while incomplete observations never prove it. Native mode
      and cost limits stay unsupported as specified in
      [automation](spec/automation.md). No check runs under the Store lock.
      Neither a failed check nor a missing terminal fact authorizes replay.
- [x] **L4.4 Recipes.** One timer, one event action and one bounded Goal using Codex.
      No second harness is necessary to demonstrate these behaviors.

**Result:** the first harness can work unattended under explicit limits. Hook support
is added only for a native decision not expressible by the already-working stream.

## L5 — Agent communication on the same harness

Read [coordination](spec/coordination.md). Use two Codex Agents initially.

- [x] **L5.1 Mail/variables.** Addressed queued mail, separate delivery and acknowledgement,
      bounded coalesced generic notices, shared variables and CAS. No automatic status chatter.
      `mail.send` presentation modes `send`/`steer`/`inject` are `unsupported`; that is a
      mail-path limit, not the uniform R5 system-control contract. Unread bounds come from
      the config-file `[mail]` table and acknowledged mail older than
      `daemon.retention.mail_days` is pruned in bounded retention batches (unread mail is
      never pruned; a removed message id older than the retry window reads
      `history_gap`).
- [x] **L5.2 Work attribution.** `who` reads L3 Git/edit projections; claims are
      advisory. No second ownership database or file-lock promise.
- [x] **L5.3 Hierarchy.** Controller-launched children and tree selectors, then native
      child discovery/control where supported. An existing child is never hidden by a
      fanout limit. Parent completion is not child completion.
- [x] **H1 Manager change.** Owner-only atomic subtree/all-children re-parent through
      Store, router, CLI and MCP; preserve frozen native identities and snapshots.
      Codex Sol manager to Antigravity Argon manager, exact next-turn wait, scoped
      tree stop, old-manager rejection and individual return moves qualified on
      2026-11-15. [Handoff](spec/coordination.md#handoff) also designs task/Agent switch.
- [x] **H2 Task handoff.** Atomic transfer to an existing Agent: selected queued SENDs,
      bounded brief first, claims, paused Goal and children through ordinary commands.
      Failed/unknown brief holds successors for explicit Owner recovery, never resend.
      Qualified 2026-11-15: Codex Sol A to Antigravity Argon B, ordinary brief then
      transferred task, no-effect original cancellation, exact claim expiry preserved,
      same-ID replay without another turn, and active-source refusal.
- [x] **H3 Agent switch.** Owner launch-plus-handoff through ordinary durable
      commands, reserved IDs before external preparation, promptless native launch
      and exact committed readiness before the shared H2 transfer. A definitely
      created B is finished through ordinary `swarm.handoff`; no finish mutation.
      Qualified 2026-11-15: Codex Sol low to a new Antigravity Hydrogen low,
      brief then transferred task completed, no-effect original cancellation,
      claim expiry preserved, same-ID replay without another Agent/turn,
      released-gate readback and active-source refusal before launch.

**Result:** two Agents exchange an actual message, share one variable and identify each
other's file area without a manager relay. Codex/Luna and OpenCode/OpenRouter exchanged queued
findings and exact acknowledgements under their own Agent principals, advancing the project CAS
value from version 0 to 2. A mail-notice turn consumed one next_input context; a reply notice
woke the other Agent. No Owner body relay/manual wake. A bounded two-item FIFO ordering and
one CAS conflict case are now qualified. A native-agent run verified raw `who` output with empty
edit attribution and the unseen-arrival acknowledgement race. Live `who` also showed a singly
owned Codex Luna Agent with complete, nonempty attribution for dirty `docs/spec/cloud-mcp.md`.

## L6 — Additional harnesses and only demonstrated missing features

Read the selected [harness page](harnesses/README.md). Integrate one at a time through
the existing protocol. Start with Claude Code and OpenCode for the product acceptance
scenarios. Muse, Antigravity, ACP and other adapters are opt-in tasks, not blockers for
Codex or for unrelated adapters.

- [x] **L6.1 Claude Code.** Native SDK/stream, exact callbacks and reported application
      points. The separate shared context-injection and hook-event integration remains
      tracked in L6.6.
- [x] **L6.2 OpenCode.** Select one installed API. Shared mode only if per-session
      attribution/environment/stop are supported; otherwise per-Agent server.
      Inject, detach and fork/background stay unconnected.
- [x] **L6.3 Cross-harness use.** Reuse L4/L5 rules/mail against the added plugins.
      Keep each control at its actual native application point; remaining shared context
      injection and hook-event integration is tracked in L6.6. Missing semantics reject;
      no compatibility executor.
- [x] **L6.4 Selected optional adapters.** Add only those explicitly selected for the
      release; record the selection here. Unselected adapters stay absent.
      Muse Code selected by the owner 2026-11-03; Antigravity selected 2026-11-04.

- [x] **L6.5 MCP stdio for an owner-selected caller.** Add one local `swarm mcp`
      server through the existing authenticated API and its bounded query/history surface.

- [x] **L6.7 Cloud MCP caller.** Connect cloud Dot/MuseAgent as a remote MCP caller via
      the local StreamableHTTP MCP server exposed through `cloudflared` at
      `swarm.iuriilisenkov.cloud`. Preserve remote identity/connection/call binding
      through the existing router/bus and authenticate the endpoint with Cloudflare
      Access Managed OAuth/native JWT validation per the existing account documentation.
      Distinguish this caller from a controlled local native harness/PID. Completion:
      a cloud caller can perform an authenticated MCP call without local process custody.
      The selected Muse caller qualified this criterion on the corrected connected path on
      2026-11-08 with authenticated reads and an operational notify; verified remote
      identity/session/call binding passed through the same router/Store/bus. The origin
      rejected anonymous requests. Dot and expired/revoked/cross-subject runtime cases remain unqualified (L7).

- [x] **L6.8 Stalled/unhealthy-agent diagnosis and notification.** Automatically detect
      stalled/unhealthy agents from read-only native/process/turn/event facts, without
      treating native process statistics as proof of model progress. On unhealthy or
      no-progress, run read-only diagnosis and deliver actual mail/notify through the
      existing router/bus to the supervisor/agent, without automatic job resend or false
      Done. Completion: a stalled agent produces a delivered supervisor/agent diagnosis
      notification and leaves the uncertain outcome explicit.

- [x] **L6.6 R5 system context control.** Connect typed `inject` through the existing
      command rows and router: FIFO `next_input` context is prepended inside the same
      native SEND and applies only with its exact turn association; `turn_safe_point`
      binds the captured turn and commits its context claim before the authenticated
      plugin hook response; `active_turn` dispatches directly as the plugin's
      `agent.command` inject through the actual native turn/steer effect
      (`codex.turn/steer`, `muse.turn/steer`) with expiry and exact-turn validation
      at dispatch. Normalize `hook.before_tool` through the existing event bus
      and `on.event` rules. Preserve exact-turn steer/interrupt and existing approval
      callbacks; adapters report the actual mechanism/application point or `unsupported`,
      never substitute a weaker SEND. Claude Code 2.1.278
      `PreToolUse.additionalContext` was the first native consumer. Antigravity
      supports `before_model`, `turn_safe_point`, and exact-turn steer/interrupt.
      Delivered: Claude exact-turn hook steer with controls served while a steer
      reply is pending, proven no-effect refusals settled `rejected`, and actual
      FIFO/safepoint/active-turn contexts consumed on installed-agent runtime.

The selected local scope remains in progress while the applicable L7
qualification is incomplete; do not describe it as feature complete.

- [ ] **Local adopted backends via OpenCode.** One documented usable
      OpenCode + adopted-backend profile (Strata, Unsloth Studio, LM Studio
      and llama.cpp presets with Ollama/vLLM on the same OpenAI-compatible
      provider path; managed startup deferred to adopted endpoints only).
      Completion: installed backend/harness versions with model/quantization
      and effective context recorded; a coding turn with an actual tool
      result, context/output-limit handling, interrupt/stop, and two Agents
      sharing the server without cross-stop or extra model loading;
      missing-key rejection and conflicting project settings checked. No
      owner-global configuration edits or automatic harness/model/account
      fallback. Codex against these backends stays unsupported.

- [ ] **Failure diagnostics that say why and where (#384).** Typed `where`/`origin`
      on every settled failure, sanitized daemon log with standard/debug modes,
      crash markers surfaced in `daemon.status`, startup output and `swarm why`,
      owner-scoped `diagnose` plus `swarm why <command|Agent|agent:turn>` with a
      causal chain and one-line summary, `preceded_by` on turn outcomes, and the
      same summary in desktop Agent details. Qualify on a private daemon:
      nonexistent command, killed turn, invalid model, `max_live`, plugin panic
      and forced daemon panic with the marker consumed exactly once.

## L7 — Qualification, repair and release

Implement the bounded retention path first, then build/run the complete campaign against
the connected product. Keep results in the PR/CI, not a permanent audit/readiness hierarchy.

- [x] **Physical retention.** Wire retention settings to bounded Store cleanup invoked
      by daemon maintenance. Implement event-prefix pruning and atomic command/child
      cleanup under the [Core retention contract](spec/core.md#retention), including the
      rule history floor, replay/idempotency window and source recovery dependencies.
      Daemon maintenance deletes expired history via bounded Store pruning;
      queries/backlog handling alone do not. Keep open/unknown work and current
      projections; no full rewrite, forced unknown resolution or separate cleanup ledger.
- [x] Core/Store/API/reducer and event replay, including retention and non-reused ids.
      Qualified 2026-11-15 on release 70E7859B with real Muse/Codex Agents. Covered:
      replay equal to live; restart without effect replay; a mid-turn kill settling
      `unknown`; same-id idempotency and `id_conflict`; fresh ids and seq across
      restarts; cursor gaps; deletion of 25 h-old rows (nonzero counts,
      `history_gap`, preserved unknown turns and vars, idempotent second pass).
- [x] CLI E2E on the real harness, exact generations/turns/requests, queue ordering and unknown outcomes.
      Qualified 2026-11-14 on Codex and Muse; the unknown-turn send fence it exposed was fixed (#468).
- [x] Crash at admission, native effect, event commit, reply and source-drain boundaries;
      verify no repeated prompt/setup/script/push and no false terminal result.
      Qualified 2026-11-14, including 3 in-window kills between terminal commit and reply.
- [x] Live runs for the
      selected harnesses/routes. Verify effective model/account/permissions.
      Qualified 2026-11-14 on Codex, Claude, Muse, OpenCode and Antigravity (release 492640C8).
- [x] Rules/timers/hot reload, Goal limits, mail/ack, CAS, hierarchy and Git recovery.
      Qualified 2026-11-14 with real Muse Agents on a private daemon.
- [x] Windows Job/handles/paths, private IPC, credentials and resource bounds.
      Qualified 2026-11-16 on release DCD64B86 with real Codex/Muse Agents: forced stop
      and daemon kill leave no process of the Agent tree (tool grandchildren included)
      and stopping one Agent spares another; only the daemon holds the Store handles;
      space/non-ASCII workspaces and data roots past MAX_PATH work, while workspace roots
      at or past the process-creation limit are refused with the OS error; the pipe and
      data root are current-user-only and inherited ACLs are refused; `pass_env` values
      never reach the data dir or readbacks; `max_live`, frame and page bounds hold.
- [x] Linux via the supported Docker image, with real native Agent turns, private
      IPC/data permissions and honest degraded process custody. See [Docker](../docker/README.md).
      Image built and Codex/Luna launch, tool turn, follow-up send and OpenCode free-model
      tool turn qualified on 2026-11-16, including space/percent/non-ASCII paths.
      Claude 2.1.325 with `claude-haiku-5-5` qualified on 2026-11-16 after the
      scope-hash fix: launch, one-line tool turn (`pwd` plus `hello.txt`), follow-up
      `send` (append `world`, verified on disk), and stop, all with the documented
      `CLAUDE_CONFIG_DIR` credential mount; readiness commits, never unknown.
      Muse/Antigravity Linux binaries install and run, but their authenticated
      turns are not qualified here. Custody stays honestly degraded (see Docker README).
- [ ] Adopted-service isolation (with the local adopted backends). Deferred by the owner.
- [ ] Retention/WAL/backpressure and representative load. The 10,000-Agent,
      2,000-events/s owner run is a release capacity check, not a prerequisite to L2.

Fix the root cause and keep focused regressions. No runtime readiness claim until the
relevant real path is exercised. A missing optional harness does not block qualification
of the explicitly selected release scope.

## Desktop companion (optional WinUI 3 client)

- [x] **D1 Styled real chat.** One optional Windows package (`apps/swarm-desktop`,
      binary `swarm-desktop`) on WinUI 3 through `windows-reactor`: native window with
      one shell (see D7) and Material 3 roles generated from one seed (tonal palettes
      through `material-colors`, written as WinUI lightweight-styling resources at
      the application root for the shown scheme; system colors under High Contrast;
      a System/Light/Dark switch saved per user); project/profile selection and an
      Agent list grouped by project with children under their manager (branches
      read through `agent.list` `parent`);
      `launch(text)` with its linked command; send to an existing Agent; the real
      final output from result/turn readback; pending requests with exact bindings;
      interrupt and steer bound to the current generation and turn, and resume of a
      stopped, failed or lost Agent at its generation (with optional linked text).
      First input is
      submitted once (one ULID per user action, retries reuse it); a late reply
      cannot erase a newer draft and switching conversations cannot retarget
      controls; reopening stops or duplicates nothing. Headless and non-Windows
      builds stay WinUI-free. (Refs #407)
- [x] **D2 Retained activity and long conversations.** The selected Agent's
      retained conversation is rebuilt from existing reads: `agent.result` turns,
      its journal window (`turn.started` links each input command,
      `message.completed` progress and `tool.*` activity) and `command.get` input
      text. The coverage statement covers turns against the limit, the scanned
      window, whether creation was reached, the retention floor and evictions.
      Markdown renders in native `RichTextBlock` controls (http/https links only;
      HTML literal) with Copy message and Copy code. A virtualized turn list keeps
      stable keys and bounded resident turns and bytes. Live events follow a
      committed `events.bounds` cursor: a limit-1 wake with an explicit `wait_ms`,
      then drains from `next`. A lag or `history_gap` marks the view stale and
      rereads it; transport failures back off and resnapshot. Jump to latest is
      explicit. Reactor 0.100.0 has no scroll offset API, so the view does not
      auto-follow the bottom. (Refs #407)
- [x] **D3 Swarm monitor.** An openable monitor panel uses one demand-driven
      refresh chain, active only while open. Each refresh reads `agent.list` per
      attention state with observations (with `total` as the exact counter), then
      `agent.get` and `usage.summary` for the selected Agent only. At most four
      reads run concurrently. Rows show attention first, with the daemon-reported
      sample age and TTL, coverage and clock regression. CPU is core-equivalent
      and differenced only between samples of matching process identities; a
      changed set is marked partial. RSS is shown, the UI-only history is capped
      and deduplicated; CPU and memory are bar charts with the latest value and a
      labelled scale. Only the actions that apply are offered, bound at click time
      to the Agent, generation and turn shown: Interrupt, Stop, Force stop
      (confirmed inline), Resume and, for observe-only Agents, Detach.
      (Refs #407)
- [x] **D4 Launch context, profiles and automation settings.** A new task
      picks Auto, a profile from the daemon's sanitized `profile.list` summaries,
      or an Owner ad-hoc harness with its catalogue's models and efforts. The
      Agent details show requested settings beside the effective native
      readback. Settings › Models and profiles keeps one draft of the `profiles`
      settings section as `[profiles.*]` TOML: form fields (dropdowns for effort,
      permissions and approval policy, keeping a value outside the documented set)
      and the source view both edit it. Validate and Save go through
      `settings.save` (D8). An invalid draft is never written and stays in the
      editor. The section states that running Agents keep their frozen settings. Settings › Automation lists rules
      and timers read-only through `rule.list`; no pause or resume operation
      exists. (Refs #407)

- [x] **D5 Scripts.** Existing script entries under the approved Run roots
      (`<data_dir>/scripts/`, registered `.eliot/scripts/`) open in a native
      multiline editor. Edits go through `script.list/read/save`: Run's own
      capture rules, a revision guard and temporary-file replacement, with the BOM
      and CRLF kept. Saving never executes. Run is the existing Owner path,
      `timer.run` of a timer whose `run` action names the entry, with the daemon's
      custody, timeout and unknown-outcome rules. "Open in editor" starts a
      user-selected `.exe` with literal argv and no Eliot credentials. (Refs #407)
- [x] **D6 Coordination actions.** The composer's model chip configures model and
      effort from `harness.models` read under that Agent's frozen route (`agent`),
      with the daemon's per-field `configure` refusals; the details show
      requested, next-turn (revision, application point) and effective values with
      their turn-proven verified marks. Change manager moves an Agent, or all of a
      manager's children, to another manager through `swarm.reparent`, each move
      a button bound to the generations shown. A Hand off / Switch dialog selects the original task SEND
      and up to 64 undispatched SENDs, then hands off to an existing Agent
      (`swarm.handoff`) or switches to a new profile or ad-hoc Agent
      (`swarm.switch`, cross-harness). It shows cancellations, successors, brief
      state, claim/Goal/child transfer and the successor gate, reads results back
      only with the same id, and offers Owner `command.resolve`/`command.cancel`
      for a held or failed brief and an ordinary handoff to finish a switch.
- [x] **D7 One shell.** The rail of separate screens is replaced by one shell
      (owner review 2026-11-15, the Codex app as reference). A sidebar holds
      one tonal New task row (Ctrl+N), Search tasks (Ctrl+K: title, project and
      harness, through WinUI's AutoSuggestBox), Automations and Usage,
      collapsible projects with their Agents nested (manager, then workers,
      collapsed), Recents, a calm fleet line that opens the monitor ("6
      running"), a separate "N need attention" row that lists only Agents
      waiting on a request or controlled Agents in `unknown` (observe-only
      native children never count), and Settings. A project is a repository:
      checkouts registered with the same Git common directory group under the
      repository's name, and a task row names its worktree only where the
      project spans several. Running tasks stay on top, then newest first;
      stopped tasks last changed before today fold into "Show N older"; a
      collapsed project shows its running count, and `[desktop]
      collapsed_projects` keeps the collapse across restarts. Titles come with
      each `agent.list` page (the first meaningful line of the summary
      `preview`, Markdown removed, else the name, else `<harness> · <profile>`).
      Reactor 0.100.0 has no key events and no N/K accelerator keys, so the
      shortcuts and popover keys come from hooks scoped to the window's thread;
      a search suggestion reached with ↑/↓ is only highlighted, and Enter or a
      click opens it. The conversation has a one-line header (title, status
      dot, details toggle, a "…" menu for Hand off/Switch, Change manager,
      Resume, Detach, Stop and Force stop); coverage, ids and readback live in
      the details panel. The composer of a conversation carries its project,
      the model-and-effort and access chips, Send (which steers the running
      turn, D10), and Interrupt only while a turn runs; a new task's composer
      is D9. Settings replaces the
      sidebar with its own navigation and shows one centred page of at most
      840 DIPs (D9). One component system at Codex density with Material 3
      color roles (owner reviews 2026-11-15 and 2026-11-16), every size from
      one tokens module: corners 8 for controls and row highlights, 12 for
      containers (cards, menus, popovers, Details sections, dialogs), 16 for
      the composer, a circle only for the icon-only Send; heights 32 for
      controls and rows, 36 for form rows, 28 for tasks under a project; four
      type styles (20/600, 14/600, 14/400, 12/400); a 4/8 spacing grid;
      16/20-DIP icons. One action pattern: filled primary (one per page),
      outlined secondary, text tertiary, destructive text in the error color;
      orange and red appear only for the Full access warning and destructive
      actions. The Details panel has only a close button. (Refs #407)
- [x] **D8 Settings and routing in the desktop.** Every Settings page reads and
      writes the unified `settings.read/save` (S1) through `config_client`; the
      desktop has no other store. Appearance and Startup write `[desktop]`
      theme and startup ("reopen the last task" opens the most recently active
      Agent). Routing edits each role's ordered candidates; Projects edits
      path, allowed profiles and Agent actions; Automations edits assignments
      ("keep N busy from folder X", S4) above the read-only rules and timers,
      and the script entries; Cloud MCP edits its six fields; Advanced holds
      every section as JSON and the settings history (author, time, sections,
      revision). Each save shows its result and restart need (revisions only in
      Advanced); a fresh read keeps drafts with unsaved edits. Main's startup
      behaviors stay: Owner daemon autostart, the opened checkout's
      `project.discover`, the fresh private directory remedy and the one-time
      legacy `swarm-desktop.json` import. Auto launches with no profile, so
      admission routes (S3); before launch the model picker shows
      `routing.preview`'s pick and reason, and the Agent details show the
      recorded `profile_choice`. (Refs #407)
- [x] **D9 A desktop you can work in.** Owner review 2026-11-16, the Codex,
      OpenCode and Antigravity apps as references. A new task's composer has
      no profiles: the project chip (a searchable project list and "Other
      folder…") on top, "Describe a task…", then the model chip
      (`Harness · Model · effort`), the access chip and Send. The model chip
      (Ctrl+M) opens OpenCode's picker pattern fed by `models.catalog` (k76a):
      a search over harnesses, providers and models; Auto with Swarm's pick
      and reason; Favorites and Recent (`[desktop]` `favorite_models` /
      `recent_models`); then Codex, Claude Code, OpenCode (by provider), Muse
      and Antigravity, each row with its provider, effort, a `default` badge
      and a check on the current one; held models are greyed with the reason
      and a missing harness says "not installed" once. Claude Code has no live
      catalogue, so its group offers the CLI default and the model id typed in
      the search, with the documented effort vocabulary. ↑/↓ move, ←/→ change
      the effort (or click it on the highlighted row), Enter chooses, Ctrl+F
      marks a favorite, Esc closes; saved presets are a group at the end. A
      model choice is an Owner ad-hoc launch with `harness`, `model`, `effort`
      and `access`. The access chip opens Codex's menu (Ask for approval /
      Approve for me / Full access → `ask` / `auto` / `full`, Full in the
      warning color, refused up front for OpenCode), remembered in
      `[desktop] last_access`. The popovers are drawn above the composer, not
      as WinUI flyouts (Reactor 0.100.0 cannot close a flyout it opened).
      Settings follows Codex: a Search field over the settings of every page,
      then the groups General (Appearance, Startup, Keyboard shortcuts),
      Agents (Models & presets, Default access, Routing, Usage), Work
      (Projects, Automations, Worktrees & cleanup), Integrations (Cloud MCP)
      and Advanced (the settings file and history); each page has a title, a
      one-line subtitle and cards of rows with the control on the right, and
      "Open config.toml" where a section backs it. Usage shows each harness's
      remaining provider quota as rings (weekly and 5-hour where `harness.quota`
      reports them, "Not supported" otherwise), running Agents and the tokens
      of Agents active today. Worktrees & cleanup lists the managed worktrees
      under the data directory by project with Open; cleanup waits for
      `cleanup.plan` / `cleanup.apply`. Shortcuts: Ctrl+N, Ctrl+K, Ctrl+M,
      Ctrl+,. (Refs #407)
- [x] **D10 The owner's second pass (2026-11-16).** Picker groups (each
      harness, and each OpenCode provider) collapse, remembered in `[desktop]
      collapsed_model_groups`; OpenCode's Manage models (the sliders button at
      the end of the search row) toggles provider groups and single models into
      `[desktop] model_visibility`, the picker lists only shown models and a
      search still finds a hidden one, marked hidden. The selected effort
      segment is filled, so it reads on a highlighted search row. Settings ›
      Models & presets picks a preset's model with the same picker filtered to
      its harness, and its dropdowns carry that harness's real fields (Codex
      sandbox and approval policy, Claude Code permission mode, Muse approval
      and sandbox, Antigravity mode); no text box offers Windows text
      prediction. Enter sends and Shift+Enter keeps the line break (the window
      hook marks the line break Enter typed); one Send steers the active turn
      while one runs and, after a definite `bad_state`/`stale_generation`/
      `unsupported` refusal, goes as the next message with one line saying so.
      The message shows at once with "Sending…"/"Starting Codex…" until its
      turn starts. Right-click menus on projects (New task here, Open folder,
      Rename, Collapse/Expand, Remove project) and on tasks and chats (Rename,
      Stop, Archive/Unarchive, Delete after a confirmation naming it, Copy
      Agent ID, Open folder) are drawn in the window; Rename writes `[desktop]
      display_names`, Remove project the `projects` section. A Chats section
      and "No project (chat)" launch `chat: true`; Archive/Delete call
      `agent.archive`/`agent.delete` and Show archived filters `agent.list`:
      these, and Claude/Antigravity/OpenCode/Muse quota windows (read by shape,
      shown with reset times and the composer's usage ring), need the k92
      daemon contract and say so on an older daemon. Details and monitor speak
      plain words and hide empty rows (`swarm why`'s origin line stays);
      observed native children nest under their parent and stay out of
      Recents. (Refs #407)

- [ ] **K92 Backend quota and chat lifecycle (#407).** Implemented provider
      readers, durable archive/delete, settings project removal and private chats.
      Private Windows Owner proof covers real Codex/Claude desktop launch/send,
      a private chat, archive/unarchive/delete/replay, directory cleanup and Muse
      stream-observed quota. OpenCode Go numerical readback remains blocked by
      provider HTTP 403; Antigravity returned real windows before a later HTTP 401.
      Native first-token time is not exposed by the consumed completed-message
      surface. Preserve the unrelated native-archive Unknown fence; do not replay it.

## M1 — Memory one-shot inference (#199)

- [x] **M1.1 Scoped first call.** Closed bounded DTOs, one actual non-agentic native
      text profile, private `memory_inference` principal, recoverable credential install/
      staged rotation, atomic write/call/sequence admission, replay/conflict without
      another provider call, own result/write reads, honest cancel and dependency-free
      release. Qualify on a private Windows daemon; record actual checks/evidence in
      the PR. Owner brought #199 into scope on 2026-11-14.
- [x] **M1.2 Controller memory continuity.** Fixed-window call/write inventory,
      controller backup-cut/restore readback, dependency-protected release/retention
      and safe previous-credential retirement. Qualified on a private Windows daemon
      with Claude Code 2.1.325 / `claude-haiku-5-5`, including restart and pruning gaps.
      Memory must still implement its durable mutation/backup owner integration and
      an actual episode revision consumed later. Keep #199 open until that integration
      and evidence exist. Embeddings remain unsupported.

## Later

- [ ] **K69 manager cleanup (owner clarification 2026-11-16, #384).**
      Implemented generation/tree artifact custody, bounded plan/apply, handle
      deletion, branch verification, native archive, retirement opt-in, CLI and
      Advanced Settings. The audit repair binds exclusive per-generation roots,
      refuses hard links/repositories/configuration, requires a fresh remote
      default, and preserves frozen native scope and uncertain archive fences.
      Requalified on 2026-11-16 with a private release daemon and real Muse
      `muse-spark-1.3-contributor`/low: manager a1 stopped children a2/a3 and
      applied two plans, removing both managed worktrees, the merged branch,
      temp/source artifacts and a released held directory (2,715 logical bytes).
      The unmerged branch, initially dirty worktree, live-held directory, nested
      repository, hard-linked Cargo targets and a substituted junction were
      kept/refused as appropriate; sibling registration was denied. Static gate:
      fmt, locked workspace all-targets build, warnings-denied Clippy and 2,014
      tests passed. All owned processes exited and the private daemon drained.
      Keep this item open: existing Agent policy prohibits managed-worktree
      launches, so the private Owner created the children under the manager;
      the exact manager-created-worktree scenario remains unqualified. Frozen
      Codex archive scope and malformed acknowledgements have source/regression
      coverage, without a native fault-injection run. Automatic approval review
      blocked deletion of the private proof directory and five scratch directories
      left by failing-before tests.

Service continuity across daemon replacement requires an actual use case. The
current contracts are not instructions to build that product in advance.
The optional desktop companion is tracked above.
Remote/cloud MCP access is not Later: it is the required L6.7 contour above.
