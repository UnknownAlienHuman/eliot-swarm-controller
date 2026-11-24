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
      and frozen admissions across uncertain delivery.
      - [x] **Provider failover (#818).** An opted-in role launch continues on the
        next eligible profile after a confirmed pre-work refusal. A private Windows
        daemon switched a Codex HTTP 429 to a successful turn in the same worktree
        and showed the decision in `swarm why`.
      - [x] **Provider hold recovery (#828).** A due hold admitted a fresh role
        launch as a probe on a private Windows daemon. A second HTTP 429
        extended the hold. After the profile switched to OpenCode free, its
        queued launch succeeded at the next deadline and cleared the hold.
      - [x] **Owner quota reserve (#872).** On a private Windows daemon, the
        reserve refused an automatic Claude child and timer launch while the
        Owner's Claude launch completed. A worker role recorded `quota_reserve`
        and selected an OpenCode free profile that completed a native turn.
        `swarm quota` showed retained Owner and automation usage in the active
        Claude account windows.
- [x] **S4 Assignment through automation.** Settings-managed existing rules/timers
      keep a configured number of Agents busy on a brief queue through ordinary
      routing-aware launch; retain exact command identity and unknown gates.

S1-S4 are implemented. The configuration and automation specs own their contracts;
fleet-wide and longer-running qualification remains in L7.

## Agent automation quality (2026-11-20)

Swarm takes the mechanical part of managing Agents: delivering the needed
facts, bounding actions, waiting, checking result and change scope, stopping
blind repeats and accounting cost. Agents keep the engineering work. Each item
extends an existing owner (Goal, assignment, handoff, routing, usage); no task
engine, second scheduler or critic loop.

- [x] **A1 Useful check feedback.** A Goal check's stdout and stderr both reach
      the bounded observation and continuation, each labelled with its own
      completeness; the continuation marks it as diagnostic data and never asks
      for a rerun to read the log. The no-progress digest covers both streams
      and counts only fully captured output; exit and unknown are unchanged.
- [x] **A2 Exact brief and required sources.** The launch/send brief carries the
      result, required reading, write area, explicit limits and the check; named
      sources are delivered with path and version or their absence is explicit;
      limits survive handoff and continuation.
- [x] **A3 Change scope and checker trust.** A Goal's `scope` and
      `check_inputs` get a starting point at `set`, or after workspace setup
      and before first input when installed by launch; each round the daemon,
      outside the writable checkout, compares the change with it. A change
      outside `scope` blocks without a check or continuation; a pass after a
      check input changed, or with the change set unverified, blocks for a
      separate review instead of completing.
- [x] **A4 Assignment owns completion.** A brief-queue assignment declares one
      turn or Goal-owned work before dispatch; Goal-owned work retires on the
      Goal's terminal state, not on the first `turn.completed`.
- [x] **A5 Honest no-progress.** The Goal fingerprint is a fresh read of the
      round's workspace before the check: HEAD plus the Git monitor's content
      `dirty_digest`; a failed or capped read never extends the no-progress
      streak.
- [x] **A6 Short handoff state.** The handoff brief adds Store facts, the
      current instruction and limits, and the executor's last decisions and
      next step, keeping machine facts and model claims distinct.
- [x] **A7 Goal cost budget.** `max_cost` budgets the native session cost
      since a set-time or pre-first-launch-input baseline in its verbatim unit;
      incomplete, stale, mixed-unit, reset or decreasing data is
      `cost_unverified` and blocks, never zero.
- [x] **A8 Bounded escalation.** The second identical Goal failure in a row
      gets one diagnosis continuation; an identical failure after it applies
      the Goal's one Owner-authorized `escalation` (a model/effort `configure`
      with its reason, admitted before the continuation); never a model ladder,
      wider rights or a replayed unknown.
- [x] **A9 Flow control and review digest.** A manager's epoch-bound
      `project budget --pause` holds every new admission in its assignment
      scope until `--resume`; `agent.patch_digest` (files, packages, new
      dependencies, public interface, checker/lint changes, new suppressions)
      focuses the independent review.
- [x] **Project formatting and checks (#833, #811).** Swarm applies the project
      formatter in the managed worktree and delivers a failed fast check in the
      same OpenCode turn. The routing role installs the bounded project Goal.
- [x] **Project launch policy (#868).** The registered project's defaults reach
      ordinary launches, assigned managers, their children and timer launches.
      A failed Goal check continues the Agent's work. Explicit opt-outs retain
      their reason and appear in the project's 24-hour adoption count.
- [x] **Swarm autonomy (#883).** Owner CLI and MCP report windowed Store counts
      with source coverage. A private Claude Haiku and OpenCode run contributed
      native and Swarm facts. The report read an upgraded Store with 600,000 events.
- [x] **Store-derived deadlines and attention (#873).** Three Claude Haiku
      checks raised the project deadline. OpenCode's timed-out check passed on
      one retry in the same Goal round. Doctor showed learned native and Git
      quiet-gap thresholds, a cold-pair fallback, and explicit deadline priority.
- [x] **File wake (#833).** OpenCode arms a file condition, ends its turn and
      receives one follow-up when the file appears in its worktree.
- [x] **Background-process wake (#879, #881, #883).** Claude Haiku started a
      60-second background command and ended its turn; Swarm woke it once with
      exit code 7. An Agent with its own wake got no duplicate; a stopped Agent got nothing.
- [x] **Worktree notes and task cards (#881, #882, #883).** Claude Haiku
      received one in-turn note with the full path and corrective action for
      two edits to the same outside file. OpenCode 2.2.7 and Antigravity 1.5.14
      each received one task card on the next input after manual compaction.
- [x] **Claude self-started turns (#907).** After a background shell or subagent ended,
      real Haiku Agents showed `busy` and `swarm result` listed the native turn with its
      final text. A `send` during that turn merged into one turn with the reply captured.
- [x] **Project command guard and shell identity (#833).** OpenCode refuses
      `git stash` with the configured text. Its shell omits the scoped identity
      variables while its managed MCP tool authenticates as the Agent.
- [x] **Check-bound commit and push (#833).** An opted-in project rule commits
      the checked tree with the first line of the Agent's final answer. A private
      bare remote read back the same commit.

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

## Multi-project Swarm (#657)

- [x] **1. Desktop project identity.** Fix Move and Handoff selection using the
  registration ID and preserve server-side authorization scope.
- [x] **2a. Manager assignment core.** Persist atomic assignment and epoch, expose
  CLI/MCP claim, launch, read and release, and fence admission and queued dispatch.
- [x] **2b. Manager worktree and worker inheritance.** Reserve the manager's workspace
  with launch, use existing preparation and setup, and have workers inherit it.
- [x] **2c. Project and global capacity and readback.** Extend transactional
  reservations and return exact project, manager and fleet usage.
- [x] **3. Project transfer.** Build on H1/H2/H3 to transfer authority, team and
  workspace bindings atomically, with explicit drain and unknown handling.
- [x] **4a. Headless chat and work organization.** Grouped projection with
  attach/detach placement and tags (schema v64). Continue-in-project returns
  later through the existing switch/handoff path (#657).
- [x] **4b. Fair queue.** Arbitrate automation assignments across projects and support
  explicit queued interactive launch, wait readback and scoped manager budgets.
- [x] **5. Desktop and migration.** The sidebar's projects are the `chat.organization`
  groups, read in one bounded read (manager, standard worktree branch, assignment
  and transfer state, "Needs assignment", roots competing with an assigned manager,
  exact server totals, queued launches with their wait reason); `agent.list` rows
  carry their group, so the desktop groups nothing itself. A task's menu attaches,
  detaches and tags under the link revision it read; a capacity refusal offers Queue
  it, and a lost Queue it reply resolves its intent before the draft returns.
  Legacy path labels stay in the no-project bucket; history is not reinterpreted.

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
      categories, `session.growth` warnings, and harness version facts
      surface in `swarm top` (`OBS`), `swarm stats`,
      `swarm heavy`, `swarm doctor` and the desktop Details Observe section.
      The `harness.drift`-against-pinned-contracts event is superseded by the
      #932 per-harness self-check fact (installed version plus last pass).
- [x] **L3.3e Monitoring reactions (#835, #833).** On OpenCode 2.2.7, a stalled
      turn queued one question, received one interrupt and produced one Owner
      notice. A killed idle Agent resumed its retained session once. Three
      matching tool failures produced one fact and an Owner rule steered the
      exact turn. The retained reaction steps appear in `swarm why`.

- [x] **L3.4 Proxy/capacity.** First attach to an already-running OpenCodex endpoint.
      Add managed service launch only when actually needed. Add account/proxy caps and
      persistent provider holds with that route. Freeze admitted nonsecret route/store
      selectors across daemon replacement.

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
      project guards apply when a harness sends a command permission request.
      Agent actions, bounded Run scripts, `git.fetch` and check-bound `git.publish`
      are connected through the recipes in [automation](spec/automation.md).
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
- [x] **L4.5 Rule editing and outcome branches.** Create, edit, enable, disable and
      delete rule files through the Owner API and CLI. A failed Run can launch a
      task, and its successful terminal turn can emit a notification.
- [x] **L4.6 Worktree review and keyed rules (#874, #875).** A configured review
      uses a passed project check and the author fingerprint, then inserts the
      review for the manager. Script events drive rules with bounded keys and limits.

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
  - [x] **Child launch admission.** On 2026-11-21, private OpenCode Agents proved
        that a `self` caller's prompted and promptless child launches are refused
        before allocation, and a `tree` caller's child completes its linked first turn.
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
      commands, source preflight before launch, promptless native launch and
      exact committed readiness before the shared H2 transfer. A definite late
      refusal admits an ordinary stop for the exact unused successor.
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

- [x] **H4 Work moves from Git (#918, #922).** One switch serves an Owner or manager
      request, a routing choice, a provider hold and a lost or failed session, with
      authority never wider. The daemon snapshots worktrees into private refs, keeps the
      task and journal in Git, builds the brief from Git, file-system and Store facts, and
      signals work that is not in Git. Qualified on a private daemon with real Claude Haiku
      and OpenCode (2026-11-22) and Codex (2026-11-23) Agents, including a killed harness,
      a deleted task file and a Codex successor's commit.
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
      Delivered: Claude exact-turn hook steer held in Store until the soonest tool
      hook of that turn (#906), proven no-effect refusals settled `rejected`, and actual
      FIFO/safepoint/active-turn contexts consumed on installed-agent runtime.

The selected local scope remains in progress while the applicable L7
qualification is incomplete; do not describe it as feature complete.

- [ ] **Local models through harnesses.** Owner decision, 2026-11-18.
      - Swarm supports local models through the harness that runs them: OpenCode providers, Zed's own providers and other harnesses, including a harness whose local API is non-standard.
      - A profile selects the harness's local provider and model. Catalog, readiness, limits and errors stay honest.
      - Swarm adds **no backend-specific integrations**: no Strata, Unsloth Studio, LM Studio, llama.cpp, Ollama or vLLM presets, and no management of model servers. The existing `eliot-local` OpenCode path is reduced to one generic OpenAI-compatible provider shape. #408's backend-specific scope is closed.
      - Completion: one real local-model turn per supported harness on an endpoint the owner provides.

- [ ] **Zed harness (#720): postponed and frozen.** Owner decision, 2026-11-18.
      - **Why:** the installed Zed (1.28.1) can't run its native agent headless, and it exposes no agent ACP server. The #720 design needs a separate host built from Zed's sources. Swarm doesn't build Zed.
      - **Frozen sources:**
        - branch `freeze/zed-native-p1`: the #720 stage-1 `plugins/zed` adapter, never run against a built host;
        - branch `freeze/zed-eval-cli`: the earlier one-shot `eval-cli` batch runner;
        - the host crate stays outside this repository, as a patch on Zed `761786a4`.
      - **Resume when Zed releases a headless agent mode with ACP.** Start from #720 and the frozen branches, against the installed release. Until then, no Zed work.

- [x] **Failure diagnostics that say why and where (#384).** Typed `where`/`origin`
      on every settled failure, sanitized daemon log with standard/debug modes,
      crash markers surfaced in `daemon.status`, startup output and `swarm why`,
      owner-scoped `diagnose` plus `swarm why <command|Agent|agent:turn>` with a
      causal chain and one-line summary, `preceded_by` on turn outcomes, and the
      same summary in desktop Agent details. Qualify on a private daemon:
      nonexistent command, killed turn, invalid model, `max_live`, plugin panic
      during startup, and forced daemon panic with the marker consumed exactly once.

## L7 — Qualification, repair and release

Implement the bounded retention path first, then build/run the complete campaign against
the connected product. Keep results in the PR/CI, not a permanent audit/readiness hierarchy.

- [x] Finish the adapter boundary for native catalogue and quota translation,
      instruction-file discovery, and native diagnostics (#932). Keep native
      translation in adapters and let the daemon own routing and settlement.
      Remove the corresponding shared-source name exceptions as those paths move.
      Share repeated mechanisms and retain the supported behavior. Tracked Rust
      lines from `896694b45` to this change grew from 9,347 to 9,420 for
      Antigravity (+73), 9,460 to 9,502 for Claude (+42), 16,412 to 16,482
      for Codex (+70), 13,311 to 13,473 for Muse (+162), and 15,300 to
      15,386 for OpenCode (+86).
      A private Owner stand matched all five CLI catalogues and three quota readings and completed Claude Haiku, proxied Codex Luna and OpenCode free turns.
- [x] Daemon update preflights a Store snapshot, fences admission, and starts the
      replacement or rolls back on the same schema. Deadline Stops use ordinary
      custody and promptless continuation in parent-first order (#836).
- [x] Private stand creation imports named profiles and supports offline Store
      snapshots. Registered teardown forces owned shutdown when drain fails (#837).
- [x] Unrunnable Goal checks settle with a typed reason and retained round events
      expose check outcomes (#802).
- [x] Daemon logs attribute errors and count command storms. `swarm status` and
      the startup log identify the executable directory, commit and hash (#806).
- [x] Automatic stand artifact registration and expiry, and cleanup of stand-created
      worktrees and branches in external repositories (#837).
- [x] **Physical retention.** Wire retention settings to bounded Store cleanup invoked
      by daemon maintenance. Implement event-prefix pruning and atomic command/child
      cleanup under the [Core retention contract](spec/core.md#retention), including the
      rule history floor, replay/idempotency window and source recovery dependencies.
      Daemon maintenance deletes expired history via bounded Store pruning;
      queries/backlog handling alone do not. Keep open/unknown work and current
      projections. Default event and exited-child windows are active, and live
      maintenance reclaims pages incrementally or reuses them on older Stores.
- [x] **Automatic artifact cleanup.** Apply the configured age policy through
      [guarded cleanup](spec/coordination.md#registered-artifacts-and-cleanup-k69).
      Preview the first sweep, require explicit Owner enablement for removal,
      and protect archived, dirty, and unmerged Agents.
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
- [ ] Retention/WAL/backpressure and representative load. The 10,000-Agent,
      2,000-events/s owner run is a release capacity check, not a prerequisite to L2.

Fix the root cause and keep focused regressions. No runtime readiness claim until the
relevant real path is exercised. A missing optional harness does not block qualification
of the explicitly selected release scope.

## Desktop companion (optional WinUI 3 client)

- [x] **D1 Styled real chat.** One optional Windows package (`apps/swarm-desktop`,
      binary `swarm-desktop`) on WinUI 3 through `windows-reactor`: native window with
      one shell (see D7) and the owner-approved Material 3 theme roles, written as
      WinUI lightweight-styling resources at the application root for the shown
      scheme (system colors under High Contrast; theme choice saved per user);
      project/profile selection and an
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
      its retained events from creation, read through the agent index
      (`turn.started` links each input command, `message.completed` progress and
      `tool.*` activity) and `command.get` input text. The coverage statement
      covers turns against the limit, whether creation is still retained, the
      retention floor and evictions; "Load older" pages turns only.
      Markdown renders in native `RichTextBlock` controls (http/https links only;
      HTML literal) with Copy message and Copy code. The turn list keeps
      stable keys and bounded resident turns and bytes; every resident turn is
      laid out (an ItemsRepeater under the anchored viewer left turns
      unrealized). Live events follow a
      committed `events.bounds` cursor: a limit-1 wake with an explicit `wait_ms`,
      then drains from `next`. A lag or `history_gap` marks the view stale and
      rereads it; transport failures back off and resnapshot. The transcript is a
      WinUI `ScrollView` anchored at its far edge (`VerticalAnchorRatio` 1): it
      opens at the newest turn and follows new activity while it is at the end,
      and keeps its place when read further up. Reactor 0.100.0 has no scroll
      offset, so there is no "N new" jump button; a send scrolls the transcript
      to its end through UI Automation's scroll pattern (its inner scroller,
      found under the transcript's automation id). (Refs #407)
- [x] **D3 Swarm monitor.** An openable monitor panel uses one demand-driven
      refresh chain, active only while open. The last successful fleet and
      selected-Agent details stay visible on reopen and failed refreshes, with
      inline read errors and a timestamp when cached data is older than one
      refresh interval. Each refresh reads `agent.list` once
      across live states, archived ones included (the fleet line counts
      them), with observations and `state_totals` (the exact
      count of each persisted lifecycle state in the caller's scope, native
      observed children included), alongside selected-Agent identity, usage
      and diagnosis. At most four
      reads run concurrently. The pane follows `Conversation.dc.html`:
      Monitor and Details tabs with the close button, a pill search field
      and a connected state filter (All, Working, Attention, Idle) that
      stays as chosen while no Agent is in that state. The
      list groups Needs
      attention (waiting, failed,
      lost, a controlled Agent in `unknown`, or an open `x.user.agent.attention`
      report read incrementally from the journal), Working, Idle and a collapsed
      Recently stopped (last 24 hours, its rows read every tenth refresh). Each
      Agent is one row under its group's label ("Working · 2"): status dot in
      its group's color, title and one trailing word: what it needs, in the
      attention color; for a working Agent its model; else how long it has
      been quiet (the daemon's `last_progress_at` over turns, messages, tools
      and requests, or the time since its record changed). A live controlled
      Agent adds its Git-aware work line under them (k209: "+120 −30 · 4 files
      · 2 commits · wrote 3m ago", or why Git could not be measured, never
      zeros; a failed or lost one only when measured), or, while a
      `no_code_progress` report is open, "Working without writing code for
      14m" in the attention color; a running sidebar task shows the same work
      line under its title, and a task with an open report its words in the
      attention color (k210: the Monitor's reports, one read). Why it ended or
      the task it runs under is in its tooltip and accessible name; native
      observed children nest collapsed under
      their parent row and read as their activity, or "Ended with parent".
      Details carry what a row leaves out: its Code work (commits ahead of
      the base, files and lines, last write and commit, this turn's tool calls
      and messages), what it does now, its tokens and
      CPU time, the daemon-reported sample age and TTL, coverage and clock
      regression. CPU is core-equivalent
      and differenced only between samples of matching process identities; a
      changed set is marked partial. RSS is shown, the UI-only history is capped
      and deduplicated; CPU and memory are bar charts with the latest value and a
      labelled scale. Only the actions that apply are offered, bound at click time
      to the Agent, generation and turn shown: Interrupt, Stop, Force stop
      (confirmed inline), Resume and, for observe-only Agents, Detach. The
      search (title, id, harness, model, project) narrows the list, the
      recently stopped Agents included (k195 P3-2); the panel's
      Monitor/Details switch highlights the pane shown, however it opened.
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
      editor. The section states that running Agents keep their frozen settings. Settings › Automations shows
      each automation as one card in plain words: what it does, its state
      (Running, Paused, Off, Starting, Unknown, or Failed with the reason),
      its last scheduled and next
      run, its folders, and Technical details with the raw keys. Rules-file
      rules and timers are read-only (`rule.list`) with Run now (`timer.run`);
      a settings assignment also has Pause/Resume (its `enabled` key) and Edit
      in a short labelled form. The card confirms a pause from effective timer
      readback. Confirmed worker downtime appears on every card and survives an
      unreadable status refresh. Run now retains the exact timer's ID across a
      lost reply and reads the admitted command to settlement before enabling
      another run. Only applied launches count as started. The cards show
      rules with current source membership. (Refs #407)

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
      with the daemon's per-field `configure` refusals. While that read is
      in flight, the launch catalogue previews the harness's choices; editing
      requires the exact Agent route. The details show
      requested, next-turn (revision, application point) and effective values with
      their turn-proven verified marks. "Move under another task…" (a task's
      "…" and right-click menus, not the details panel) moves an Agent with its
      own workers under another running or ready task of the same project
      through `swarm.reparent`: a picker with a search box and one
      confirmation, bound to the generations shown; the daemon's refusal reads
      in plain words. The picker pages `agent.list {project, state}` itself
      (at most 1,000 rows, totals included; not the sidebar's page) and says how
      many tasks it left unread; a chat's search has no project filter, so it
      says it read every project. A Hand off / Switch dialog selects the original task SEND
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
      collapsed), a fleet line that opens the monitor and says what
      is live in at most two counts, so it is never cut off ("2 working · 1
      needs attention", "12 idle", "All quiet" when nothing is; every count
      is its accessible name), a separate "N need attention" row that lists Agents waiting
      on a request, failed or lost ones, and controlled Agents in `unknown`
      (observe-only native children never count), and Settings. A project is a repository
      and nests one level (k142): Project → Subprojects (its linked worktrees) →
      Tasks. Checkouts registered with the same Git common directory form one
      project, named by the owner's display name, else the repository name of
      its `origin` remote (`project.list`'s read-only `origin`), else its main
      checkout's folder, never a registration ID or a worktree's folder; a
      subproject reads as its branch in words (`feat/k141-monitor-charts` is
      "k141 monitor charts", a detached one its folder). Tasks in the main
      checkout sit under the project; each row shows how many tasks work in
      it; a registered folder that is gone says "missing" with a one-click
      Remove. A task folder the desktop's project list does not hold yet
      lists under its repository from Git's own files (the `.git` file, then
      `commondir` and `HEAD`). The picker, Settings › Projects and the Monitor
      use the same names. A button in the sidebar's
      header hides it to a rail (show sidebar, New task, the Monitor with the
      fleet's dot, Settings) for the session; Settings keeps its navigation.
      Navigation works as in Codex (owner review 2026-11-19): the app title
      goes home (the new-task view); a project's name opens its page (its
      tasks by state as the daemon reads them, branches and worktrees, check,
      automations and New task) and its chevron folds it; Back and
      Forward (the header's arrows, Alt+←/→, the keyboard's Back and Forward
      keys and the mouse's side buttons) walk the places visited: a
      project's new task, a conversation, a project page, a Settings page.
      A conversation reopens by Agent id through the daemon, whatever the
      sidebar has loaded, and only a deleted Agent leaves the history. WinUI 3 takes
      mouse input outside the thread's message queue, so the side buttons
      come from a low-level mouse hook that only reads them over the
      desktop's own foreground window. Running tasks stay on top, then newest first;
      stopped tasks last changed before today, and subprojects with nothing
      live and nothing from today, fold into "Show N older" (a project whose
      tasks are all older keeps its newest three in view), and `[desktop]
      collapsed_projects` keeps a project's or subproject's collapse across
      restarts. Titles come with
      each `agent.list` page (the first meaningful line of the summary
      `preview`, Markdown removed, else the name, else `<harness> · <profile>`).
      Reactor 0.100.0 has no key events and no N/K accelerator keys, so the
      shortcuts and popover keys come from hooks scoped to the window's thread;
      a search suggestion reached with ↑/↓ is only highlighted, and Enter or a
      click opens it. The conversation has a one-line header (title, status
      dot, details toggle, a "…" menu for Hand off/Switch, Move under another
      task, Resume, Detach, Stop and Force stop); coverage, ids and readback live in
      the details panel. The composer of a conversation carries + (attach files)
      and the model-and-effort chip (Codex's effort popover: the route's effort
      steps, applied for the next turn, the model as "Requested model X" until a
      turn carries it, and "Change model…" into the picker filtered to that
      Agent's route; one plain line instead when the harness keeps them), and
      one trailing button as in Codex (owner review 2026-11-19): Send; while a
      turn runs, Stop (a rounded square; it interrupts that turn) with an empty
      field and Steer once there is text (Enter steers too, D10); a stopped or
      lost task's one Resume, with the typed message once there is one. There is
      no separate Interrupt control. The usage ring beside it is Material's
      determinate circular indicator, in the attention color only when a real
      reading of the account the task runs on has less than 20 % left; where the
      preset runs through a proxy, or that is not known, it is a neutral outline
      ring. A steered message reads as "Steer" over its text in the user's
      bubble shape, filled with the selected role at half strength. Copy under a
      message shows only while the message is under the pointer or the action
      has keyboard focus. A desktop started minimized or without activation
      moves keyboard focus to the composer only once its window is the
      foreground window, and it opens no console window (a GUI-subsystem
      binary). A new task's composer is D9. Settings replaces the
      sidebar with its own navigation and shows one centred page of at most 720
      DIPs (D9). One tactile Material 3 component system with Codex's
      logic (owner reviews 2026-11-15, 2026-11-16 and 2026-11-19), every
      size from one tokens module and every button from one Material
      control template (Uno.Themes' Material styles and Material
      Components' state layers as the pattern): a state layer in the
      content color on hover (8 %) and press (12 %), a ripple from the press
      point, a slight press scale on M3's emphasized motion (150 ms for states,
      250 ms for a popover's entrance, a fade and a 4-DIP rise), the FAB's key
      and ambient shadows drawn under it as the design's CSS shadows render
      (each blur a Gaussian falloff; WinUI's ThemeShadow draws nothing outside
      a popup without receivers), deeper on hover; the composer, popovers and in-window menus
      lifted the same way (elevation 0 to 3). Shapes: everything pressed is a
      pill (buttons, icon buttons, chips, rows, segments, dropdowns; radius half
      the height), cards and tool rows 12, the composer 20, dialogs 28. The
      approved design's density (2026-11-19): rows and controls 32, chips 28,
      the FAB 36, the header 52, form rows 40; the sidebar 264, the Monitor pane
      320, the conversation column 720, a nested task 28 in; the type scale
      28/16/15/14/13/12 in one family (Segoe UI Variable), buttons 14/500; a
      4-DIP grid; icons 18 in rows and icon buttons, 16 in chips, 14 for a
      dropdown's chevron, Stop a rounded square. One action pattern: filled primary (one per page),
      outlined secondary, text tertiary and destructive text in the error color.
      Text roles keep at least 4.5:1 contrast against their surfaces in Light and
      Dark, including secondary text, placeholders and disabled labels.
      The attention role marks warnings, errors, failures, low quota and
      destructive actions. The Details panel has only a close button. (Refs #407)
- [x] **D8 Settings and routing in the desktop.** Every Settings page reads and
      writes the unified `settings.read/save` (S1) through `config_client`; the
      desktop has no other store. Settings reopens on its last document
      with the read time, refreshes in the background and shows errors inline.
      General writes `[desktop]` theme and
      startup ("reopen the last task" opens the most recently active Agent),
      and Default access `last_access`, each when chosen. Routing edits each role's ordered candidates; Projects edits
      path, allowed profiles and Agent actions; Automations creates, edits,
      pauses and removes assignments (S4) through a short form beside the
      rules and timers (D4), and the script entries; Cloud MCP edits its six fields; Advanced holds
      every section as JSON and the settings history (author, time, sections,
      revision). A control change saves at once (a typed one when typing
      rests), one revision-checked save at a time; the line under the page
      says Saving…, Saved (with any restart need) or the refusal, and a
      refused draft stays. The assignment form stays open until its save is
      confirmed and shows refusals beside its retained inputs. A stale revision is read again and the change sent
      once more only when no one changed that page's sections; an unknown
      outcome is read back, never resent. Only the raw editors (the presets'
      TOML and every section's JSON in Advanced) keep Save, Validate and
      Discard; revisions stay in Advanced, and a fresh read keeps drafts with
      unsaved edits. Main's startup
      behaviors stay: Owner daemon autostart, the opened checkout's
      `project.discover`, the fresh private directory remedy and the one-time
      legacy `swarm-desktop.json` import. Auto launches with no profile, so
      admission routes (S3); before launch the model picker shows
      `routing.preview`'s pick and reason, and the Agent details show the
      recorded `profile_choice`. (Refs #407)
- [x] **D9 A desktop you can work in.** Owner review 2026-11-16, the Codex,
      OpenCode and Antigravity apps as references. A new task's composer has
      no profiles: one card holds the project selector (a searchable project
      list and "Other folder…") and the New worktree chip as its first row (a
      fixed fact where the project prepares a worktree for every task, its
      `workspace = "worktree"`; a switch that sends `worktree` where the folder
      is shared), "Ask Eliot Swarm to do anything", then + (attach files), the
      access chip (a preset's access is an outlined chip that is not a control,
      Full access in the attention tone), the model chip (`Harness · Model ·
      effort`) and Send. Attached files travel as the message's closing block,
      `Attached files:` and one `- <absolute path>` line each (the daemon's
      input is text; the agent reads the files); the transcript shows that block
      under the owner's bubble as file chips, and files an answer names as chips
      under it. The model chip
      (Ctrl+M) opens OpenCode's picker pattern fed by `models.catalog` (k76a):
      a search over harnesses, providers and models; Auto with Swarm's pick
      and reason; Favorites and Recent (`[desktop]` `favorite_models` /
      `recent_models`); then Codex, Claude Code, OpenCode (by provider), Muse
      and Antigravity, each row with the model's full name, a `default` badge,
      a check on the current one and its effort as one short chip (efforts
      read `low`, `med`, `high`, `xhigh`, `max`, `ultra` across the desktop,
      in full words for screen readers); held models are greyed with the reason
      and a missing harness says "not installed" once. Claude Code has no live
      catalogue, so its group offers the CLI default and the model id typed in
      the search, with the documented effort vocabulary. ↑/↓ move, ←/→ change
      the effort (or click it in the highlighted row's compact effort
      segment), Enter chooses, Ctrl+F
      marks a favorite, Esc closes; saved presets are a group at the end. A
      model choice is an Owner ad-hoc launch with `harness`, `model`, `effort`
      and `access`. The access chip opens Codex's menu (Ask for approval /
      Approve for me / Full access → `ask` / `auto` / `full`, Full in the
      warning color, refused up front for OpenCode), remembered in
      `[desktop] last_access`; the chip itself is borderless, Full access as
      warning text. The popovers overlay the conversation right above the
      composer, so nothing behind them moves, and are not WinUI flyouts
      (Reactor 0.100.0 cannot close a flyout it opened). A click on the
      conversation or into the message field, Tab, or Esc closes one, so its
      keys never take the composer's Enter (the message field's press is seen
      by the window's pointer hook while the pointer is over it: WinUI's
      TextBox keeps its presses and Reactor has no focus events).
      Settings follows Codex: a Search field over the settings of every page,
      then the groups General (General: appearance and startup on one page;
      Keyboard shortcuts), Agents (Models & presets, Default access, Usage),
      Work (Projects, Automations, Worktrees & cleanup), Integrations (Cloud
      MCP) and Advanced (Routing, the settings file and history); each page has a title, a
      one-line subtitle and cards of rows with the control on the right, and
      "Open config.toml" where a section backs it. Usage shows each harness's
      remaining provider quota as two-line rows ("Weekly limit remaining",
      "Resets in 5 days", the share left and its ring; weekly and 5-hour where
      `harness.quota` reports them, "Not supported" otherwise), running Agents
      and the tokens of Agents active today; the store's size and retention are
      in Advanced. Worktrees & cleanup lists the managed worktrees
      under the data directory by project, and Clean up finished work
      reviews a `cleanup.plan` by kind (sizes, keep or remove reasons, a checkbox
      per group) and applies the checked groups' safe items through
      `cleanup.apply` after one confirmation. Automations show times on the
      local clock. Every row that shows a path (chat messages and tool cards,
      Details' project folder and worktree, Projects, Automations, Worktrees &
      cleanup, the settings file) opens it in File Explorer on click or
      Enter/Space, with a hover state and the tooltip "Open in Explorer": a
      file selected in its folder, a folder opened, through
      absolute `%SystemRoot%\explorer.exe` with explicit argv. This includes
      checkout reveal controls in Projects and cleanup and UNC and verbatim
      references in chat. A missing path or a path Explorer cannot represent
      shows the reason under the clicked item. Shortcuts: Ctrl+N, Ctrl+K, Ctrl+M,
      Ctrl+,. (Refs #407)
- [x] **D10 The owner's second pass (2026-11-16).** Picker groups (each
      harness, and each OpenCode provider) collapse, remembered in `[desktop]
      collapsed_model_groups`; OpenCode's Manage models (the sliders button at
      the end of the search row) toggles provider groups and single models into
      `[desktop] model_visibility` with toggle switches and links Auto's order
      in Settings › Routing (the picker's one filter control); the picker lists only
      shown models and a search still finds a hidden one, marked hidden. Every
      picker open revalidates `models.catalog`; a harness whose read failed
      shows its typed error with Retry. The selected effort
      segment is filled, so it reads on a highlighted search row. Settings ›
      Models & presets picks a preset's model with the same picker filtered to
      its harness (a field like its dropdowns naming the model as the
      catalogue does, the picker just under the field and never past the card),
      and its dropdowns carry that harness's real fields (Codex
      sandbox and approval policy, Claude Code permission mode, Muse approval
      and sandbox, Antigravity mode). A preset shows its agent app (a chooser
      creates a preset with `cmd` omitted so the daemon follows the installed
      version; explicit Owner overrides remain on their source preset, and
      Duplicate also omits `cmd`)
      and a Subagents card: Can start (`child_profiles`), the agent app's own
      subagents (`native_subagents`; fixed Allow on Muse and Antigravity) and
      Manages its subagents through Swarm (`control = "tree"`); a refused change
      shows its reason by its field. No text box offers Windows text
      prediction. Enter sends and Shift+Enter keeps the line break (the window
      hook marks the line break Enter typed); one Send steers the active turn
      while one runs and, after a definite `bad_state`/`stale_generation`/
      `unsupported` refusal, goes as the next message with one line saying so.
      The message shows at once with "Sending…"/"Starting Codex…" until its
      turn starts. Right-click menus on projects and subprojects (New task
      here, Open folder, Rename, Collapse/Expand, Archive finished tasks,
      Delete stopped tasks, Remove project) and on tasks and chats (Rename,
      Stop while live, idle included, Archive/Unarchive, Delete after a
      confirmation naming it, with a refused step's typed error, Copy
      Agent ID, Open folder) are drawn in the window and open from a right
      click, the row's "⋯" (shown under the pointer, with keyboard focus in
      the row and on the open task), Shift+F10 or the context-menu key; archiving the open chat
      returns to a new task with "Archived · Undo", and the fleet line counts
      live archived Agents; Rename writes `[desktop]
      display_names`, Remove project the `projects` section. A Chats section
      and "No project (chat)" launch `chat: true`; Archive/Delete call
      `agent.archive`/`agent.delete` and Show archived filters `agent.list`:
      these, and Claude/Antigravity/OpenCode/Muse quota windows (read by shape,
      each window a limit, its share left and when it resets, a refusal in
      plain words, Muse's allowance tier and "as of HH:MM" when reported,
      and the composer's usage ring, which opens Codex's usage popover
      over the composer with Manage usage), need the k92
      daemon contract and say so on an older daemon. Details and monitor speak
      plain words and hide empty rows (`swarm why`'s origin line stays);
      Swarm workers and observed native subagents nest under their parent in
      the monitor, collapsed, with their count and states ("22 workers · 2
      working"); a worker that needs attention is named on its parent's
      row, which stays in its own section. Older tasks stay under "Show N
      older" and chats under Chats. Delete
      follows the Store (stopped or failed trees, once drained); a lost task
      offers Resume or Archive instead. Bulk actions (k142) ask once, naming
      the count and what stays and why, then show progress and a result:
      Settings › Projects checks projects (Select all, Remove selected, Remove
      unused: no live task, and the folder gone or no task ever there) and
      removes eligible selections in one revision-checked settings save. The
      daemon refuses the save if any removed registration is in use. One-click
      missing removal uses stale-project cleanup. The Projects caption's "…"
      (everything) and each project's menu archive finished root trees or delete
      stopped root trees, including archived roots for deletion. Scoped actions
      use daemon-verified checkout membership. Delete confirms trees and affected
      tasks. Each `agent.archive`/`agent.delete` tree refusal is reported.
      Worktrees & cleanup's "Select all safe" checks every
      group with something safe to remove before `cleanup.apply`. (Refs #407)

- [x] **Per-item desktop cleanup and full cleanup paging (2026-11-21).** Task row
      and header actions archive, unarchive and delete eligible trees, and
      Show archived is visible in the sidebar. Remove from Swarm unregisters
      a project and leaves its checkout on disk. Task deletion uses the same
      worktree and native session cleanup guards as the cleanup API, including
      OpenCode native session deletion. Cleanup previews continue through a
      large inventory using the same opaque token in the CLI and MCP.

- [x] **Daemon restart and startup choices (#826, 2026-11-21).** A daemon that
      fails while the desktop runs is started again through the desktop's startup
      path, one daemon per data directory, retrying only transient failures with
      capped growing pauses and a calm attempt notice above the composer; the reads
      that failed meanwhile wait for it instead of showing transport errors, and the
      open conversation continues once it answers. A daemon its Owner stopped
      (`swarm daemon stop`, the `owner-stop` marker) is not restarted: the desktop
      says so and offers Start. Otherwise the
      [startup failure view](spec/configuration.md) shows the daemon's reason with
      Retry first and a confirmed new empty data directory last; a new directory is
      remembered only once its daemon answers.

- [x] **Owner pages for autonomous Swarm (#883, 2026-11-22).** The project editor
      edits the launch policy (#868) with Validate and Save on the read revision,
      shows the store's refusal, and counts the last day's launches with policy
      and their opt-outs. The Monitor shows `autonomy.get` rows with source and
      coverage for 24 h or 7 days. The transcript shows Swarm notes, Swarm and
      peer mail as collapsed notes and owner mail as owner text (inline mail
      read by delivery, since its command text is redacted). The Inbox pane
      replaces the Monitor's Attention filter: `owner.inbox` items by kind with
      the recommended action, other choices and Snooze through `owner.decide`,
      the daemon's refusal of a stale choice, and per-kind counts in the sidebar.

- [ ] **K92 Backend quota and chat lifecycle (#407).** Implemented provider
      readers, durable archive/delete, settings project removal and private chats.
      Private Windows Owner proof covers real Codex/Claude desktop launch/send,
      a private chat, archive/unarchive/delete/replay, directory cleanup and Muse
      stream-observed quota. OpenCode Go numerical readback remains blocked by
      provider HTTP 403; Antigravity returned real windows before a later HTTP 401.
      Native first-token time is not exposed by the consumed completed-message
      surface. Preserve the unrelated native-archive Unknown fence; do not replay it.

  - [x] **Muse native allowance.** On a private Windows stand with Muse Code
    1.4.25, two Owner quota calls returned the same provider windows. The
    process log recorded one native read and one cache hit. Zero model turns.

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
      Frozen Codex archive scope and malformed acknowledgements have source/regression
      coverage, without a native fault-injection run. Automatic approval review
      blocked deletion of the private proof directory and five scratch directories
      left by failing-before tests.

      - [x] **Agent-launched managed child worktrees (2026-11-22).** A private
        Claude manager launched Claude and OpenCode children in parallel, refused
        an unrelated base, and integrated both child branches with the project
        check. Cleanup classified and removed both worktrees after the checked
        merge reached the private upstream. Both child refs survived worktree removal.

Service continuity across daemon replacement requires an actual use case. The
current contracts are not instructions to build that product in advance.
The optional desktop companion is tracked above.
Remote/cloud MCP access is not Later: it is the required L6.7 contour above.
