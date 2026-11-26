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
      the Git task record preserves the original input and ordered Owner
      corrections across continuation. Briefs expose omitted inputs and reserve
      the mandatory task and source budget before a switch dispatches.
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
- [x] **A6 Short handoff state.** The handoff brief carries Git, file-system
      and Store facts with the bound task, current limits and ordered Owner
      corrections. The successor continues from those facts and the files on disk.
- [x] **A7 Goal cost budget.** `max_cost` budgets the native session cost
      since the latest snapshot selected at `set` or a launch-zero baseline
      in its verbatim unit;
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
- [ ] **A10 Goal audit and continuation (Owner decision 2026-11-26).** A blocked
      Goal (`round_budget`, `turn_limit`, `no_progress`, `escalation_refused`)
      gets a read-only audit turn on the `[routing]` role `audit`, then a handoff
      in the same worktree with the audit text and a fresh budget, up to
      `policy.budget.max_audits` cycles; scope findings stay Owner stops.
- [ ] **A11 Default Goal objective (Owner decision 2026-11-26).** With no
      policy or explicit Goal, the first linked input becomes the Goal
      objective, carried in the task card and shown in the chat.
- [ ] **A12 Task card after every compaction.** Extends the checked
      "Worktree notes and task cards" item (manual compaction on OpenCode and
      Antigravity): the task card (brief, branch, Goal or first input) reaches
      the Agent after manual and automatic compaction on Codex, Claude Code,
      OpenCode, Muse and Antigravity.
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
      The Owner inbox shows decisions waiting over 24 hours from the existing
      projection. Turn-end reminders recognize fast project-check receipts and
      exact completed native checks.
      The age filter narrows the Owner inbox before paging. Checked review has
      a recipe installer, the Owner can configure quota reserves per profile,
      and Windows permission guards recognize executable
      basenames and quoted paths.
- [x] **Child completion wakes by default (#883).** A new `child.run`
      returns its exact result if ready and registers the existing completion
      wake while the child is pending. A fresh self-registered completion can
      wake an eligible idle parent during the ordinary mail cooldown.
      Explicit bounded waits remain available.
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

- [x] **Scoped remote managers.** Bind verified Access grants to project
  assignments, fence the grant's worker controls, and transfer authority
  through the Owner's project-transfer intent.

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
  Reject stale Owner and Agent launch intents, and move pending and unread
  child wakes and cleanup with the workers while retaining the original receipts.
- [x] **4a. Headless chat and work organization.** Grouped projection with
  attach/detach placement and tags (schema v64). The Owner can continue a
  projectless chat in an active assignment through switch and handoff, with
  a prepared worktree and a retained link to the original chat.
  - [x] **Content grouping.** Owner-only `chat.group` applies ordered literal
    rules to the bounded final text of an exact completed retained turn,
    revision-guards the link, and reads back its `content_source`; text and
    rule literals are not stored.
- [x] **4b. Fair queue.** Arbitrate automation assignments across projects and support
  explicit queued interactive launch, wait readback and scoped manager budgets.
  Ordinary direct launches share the same project rotation and capacity
  reservations, with an explicit Owner override.
- [x] **5. Desktop and migration.** The sidebar's projects are the `chat.organization`
  groups, read in one bounded read (manager, standard worktree branch, assignment
  and transfer state, "Needs assignment", roots competing with an assigned manager,
  exact server totals, queued launches with their wait reason); `agent.list` rows
  carry their group, so the desktop groups nothing itself. A task's menu attaches,
  detaches and tags under the link revision it read; a capacity refusal offers Queue
  it, and a lost Queue it reply resolves its intent before the draft returns.
  Headless legacy grouping uses retained registration, worktree and parent
  evidence. Independent roots surface before an explicit manager claim,
  which requires the current assignment revision. Organization member pages
  and exact totals seek the retained membership index.
- [x] **6. Project overview.** Read scoped workspace Git status and retained
  turn usage coverage with project and fleet totals in one Store snapshot.
- [x] **7. Historical assignment usage.** Attribute command-bound turns to the
  assignment at dispatch. Page retained turn usage by epoch and report
  unattributed turns separately.

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
- [x] **L3.2 Workspaces.** Top-level launches in registered projects default to
      managed worktrees; `workspace = "shared"` keeps the registered checkout.
      Children share their parent's workspace unless a child worktree is requested.
      Creation freezes the base OID and uses one preparation row, explicit setup
      and no replay of unknown setup. Exact readback and clean-only removal preserve
      the branch.
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
      A private Owner stand matched all five CLI catalogues and three usage-window readings and completed Claude Haiku 5.5, read-only GPT-6 Luna through OpenCodex, and OpenCode DeepSeek V4.1 Flash Free turns.
- [x] Daemon update preflights a Store snapshot, fences admission, and starts the
      replacement or rolls back on the same schema. Deadline Stops use ordinary
      custody and promptless continuation in parent-first order (#836).
- [x] Private stand creation imports named profiles and supports offline Store
      snapshots. Registered teardown forces owned shutdown when drain fails (#837).
- [x] Bound native Codex SQLite initialization failure to one proven pre-effect
      retry (#384, #456). Windows recheck on 2026-11-24 using Codex 0.167.1 and
      four concurrent lanes completed 32 exact attach, native history and detach
      cycles each on historical C2BB9062 and active 11DFDFD5. Neither batch
      surfaced an initialization failure. The underlying native cause and
      recovered internal retries remain unverified.
- [x] Roll forward Claude managed MCP registration (#384, #385). A private
      11DFDFD5 run on Claude Code 2.1.330 and Haiku 5.5 wrote a version-1 project
      variable through MCP with the Agent as the Store-recorded writer.
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
- [ ] Retention/WAL/backpressure and representative load. Run this last, after
      the other roadmap work, with 100 Agents total on free or cheap models
      (OpenCode free, Claude Haiku). The later target is 10,000 Agents and 2,000
      committed events/s. Report sustained ingress separately from retained
      replay and client work. Store-only write probes and replay throughput
      leave this check open.

Fix the root cause and keep focused regressions. No runtime readiness claim until the
relevant real path is exercised. A missing optional harness does not block qualification
of the explicitly selected release scope.

## Desktop companion (optional WinUI 3 client)

**Current direction: native Windows Fluent through WinUI 3 and Reactor.** The
[Fluent design system](design/fluent-design-system.md) owns the UI contract;
D12 below is the only active desktop plan.

D1–D10 delivered the existing chat, exact controls, retained activity, monitoring,
settings, routing, handoff and project/Agent management. Their old layout and
Reactor 0.100 workarounds are
[implementation history](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e2a7b96f1f81713db3fceb3f66634191fc7bc638/docs/roadmap.md#desktop-companion-optional-winui-3-client),
not current UI instructions.

**D11 Material is frozen**, preserved on
[freeze/material-ui](https://github.com/UnknownAlienHuman/eliot-swarm-controller/tree/4f411a71c295434f8e61b17b13bc95e29a49eda1).
Resume it only after Reactor/WinUI can support the required controls, states,
typography and interaction completely and the Owner explicitly reopens that
direction. A Reactor update alone does not reopen it. Material fonts, palettes,
templates and compatibility layers are outside current work.

- [x] **Cleanup and archive.** Per-item and tree archive/delete use daemon
  custody and cleanup guards. Project removal preserves its checkout.
- [x] **Startup and restart (#826, #984).** Private daemon recovery respects
  an explicit Owner stop. Idle-session continuation is promptless, generation-bound
  and requires proven custody; uncertain work remains fenced.
- [x] **Automation and decisions (#883).** Project policy, adoption counts,
  autonomy coverage and Owner Inbox use daemon projections and exact decisions.

- [ ] **D12 Native Fluent desktop (owner decision 2026-11-25).** Stock WinUI
      templates, states and theme resources through Reactor, laid out by the
      brand book [Fluent design system](design/fluent-design-system.md). One
      manager and worktree; the stages close in order. An open stage includes
      its remaining Windows acceptance, not only missing source code.
  - [ ] **M0 Compatibility.** Track upstream windows-rs (Reactor and its
    companion crates from one git source; `Cargo.lock` records the resolved
    commit), the 0.101 API adaptations, runtime bootstrap, scoped monospace
    for code, scroll/focus/IME sanity on Windows.
  - [ ] **M1 Fluent foundation.** Remove the global M3 overrides; native
    themes, shell and sidebar, title bar, one set of app layout constants;
    System, Light, Dark and Contrast work.
    - [x] **M1a Global Material layer removed.** No M3 colors, shapes,
      implicit styles, templates, fonts, ripple, shadows or palette engine;
      stock controls in their own states, the system UI font, Segoe Fluent
      Icons, System/Light/Dark through Reactor's window theme with legacy
      palettes read as Light or Dark, and Reactor theme brushes for the few
      accent or critical marks. The shell layout is M1b.
    - [ ] **M1b Window layout.** The §6.2 size classes and docking formula
      from one window-size observer that posts only on a class or docking
      change; no widths derived from the window (Grid star/Auto columns,
      the 720 reading column centred by MaxWidth); resizable sidebar and
      context pane (pointer, arrow keys, persisted in `[desktop] panels`,
      Reset panel widths in Settings); the compact sidebar in the native
      navigation pane; the pane over the main area, its Close returning
      to the conversation, when it cannot dock; one row rule for rows and
      chips.
  - [ ] **M2 Main working path.** Home, conversation, composer, decisions,
    native menus and long history: launch, read, send, steer, stop and decide
    with draft, focus and scroll kept.
    - [ ] **M2a Home, header, status marks, composer, turn blocks.** The
      §9.1 header (title in the star cell over project · branch and a
      readable state; Goal (the goal's state once set), Changes, Agents,
      Details and More as same-height stock buttons, the pane commands
      pressed while their panel is open, secondary ones in More at the
      narrowest main area, each tooltip one line with its shortcut; the
      pane one panel with its title, Full view and Close); §4.2 static status marks with no progress ring in rows and at
      most one activity indicator per region; the §9.4 composer with Send,
      Steer, Stop turn and Resume named in words, the pending ring on the
      action being confirmed, one bar height and the parameters on their
      own line in a narrow main area; §8.1–8.2 Home (the question, the
      selected project or its stated state, the composer, three quick
      actions that send nothing); §9.1 turn blocks (the owner's message at
      the right sized to its text, the finished work log one collapsed
      line of duration and confirmed counters, the running one open on its
      last five steps, the answer and changed files without a card); the
      project poll alerts once per failing streak.
    - [ ] **M2b Step disclosure, history and Jump to latest, decision
      panel.** §9.2 steps keyed for streaming (an open step and its
      selection kept, its log kept open when the turn ends): the whole
      command, the folder it ran in, the state and exit code, the retained
      output with Copy and how much of it this is, View changes for an
      edit, MCP tools in words; §9.3 "at the end" from the ScrollView's
      own metrics (`observe_view`, `VerticalAnchorRatio=1` kept), Jump to
      latest with the count of new parts, one scroll to the real end, the
      reading position kept when an older page is built above, the
      history's boundary named at its top; §10.1 the decision panel for
      approvals (each grant its own button), questions (native radio
      buttons, a numbered sequence), unknown outcomes (last read, Check
      again, the technical ID under Details) and Goal blocked (reason,
      budget used, the daemon's actions), read from `owner.inbox` by the
      daemon's counts after the history loads, so a restart rebuilds the
      Resolve card; a turn's end typed on its part, not told by its label.
    - [ ] **M2c Menus, Goal dialog, model picker, context.** §7.4 task and
      project menus dispatch by each action's stable MenuItem key, in the
      table's groups, with an often-needed action that cannot run now
      disabled with its reason and no unsupported feature shown; the
      header's "…" is the same task menu (Goal is the header's own
      button). §18.1 one Goal ContentDialog from the header's Goal, the
      open chat's composer (Owner decision 2026-11-26: a Goal by hand in the
      chat; with none set, the first message shows as the objective; the
      header shows audit cycles), the task menu's Goal… and Details › Goal…:
      text, the check argv and
      round/turn limits in their source units, Save/Pause/Resume/Clear by
      the goal's status over the existing `goal.*` calls, errors in the
      dialog, Save named and never the Enter default. §8.3 the model
      picker within the flyout's content width (no sideways scroll), rows
      with name, provider, effort and their own Ready/Quota unknown/On hold
      label, the current model's ID and effort ComboBox, Manage models in
      Settings › Models. §9.5 "Context 54%", the token count ("Context 54k
      tokens") or just "Context" while the harness reports none, with the
      tokens and freshness in the tooltip.
  - [ ] **M3 Context and projects.** Real diff, Agents, Details and Terminal,
    project assignment and transfer, fleet and Inbox.
  - [ ] **M4 Management.** Settings, model, routing and auth, Automations and
    Scripts, Usage, Archive, cleanup and recovery. Usage follows the open
    Agent's harness, the project's active/recent harness, or the fleet/composer
    selection; unavailable quota never borrows another harness's number.
    The [design contract](design/fluent-design-system.md) owns the display
    and read behavior.
  - [ ] **M5 Acceptance and cleanup.** Windows review, targeted profiling,
    dead Material assets and dependencies removed, documentation.

- [ ] **K92 Backend quota and chat lifecycle (#407).** Private Windows Owner
      proof covers real Codex/Claude desktop launch/send and private chats.
      Native first-token time remains unavailable on the completed-message
      surface the daemon consumes.

  - [x] **Chat lifecycle and project removal.** On a private Windows stand on
    2026-11-24, Claude Haiku 5.5 ran in a chat without a project. Migration 130
    repaired its generation-1 directory binding from a Store with the chat
    already at generation 2. Resume preserved the directory in generation 3.
    Apply skipped the old plan and a fresh plan removed the directory. Archive,
    unarchive, delete and exact delete replay passed. Settings unregistered an
    unused project and preserved its checkout.
  - [ ] **OpenCode Go numerical quota.** Last recorded private readback was
    refused by the provider with HTTP 403.
  - [ ] **Antigravity numerical quota.** Private readback returned real windows
    before a later provider HTTP 401. New numerical evidence remains blocked.
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

- [x] **K69 manager cleanup (owner clarification 2026-11-16, #384).**
      Implemented generation/tree artifact custody, bounded plan/apply, handle
      deletion, branch verification, native archive, retirement opt-in, CLI and
      Advanced Settings. On a private Windows stand on 2026-11-24, binary
      SHA-256 `a309e41c19fd9684cb3d16c23b0e94316224a338340159cfa943c3b27ddbe445`
      ran a self-scoped Claude Code manager configured for `claude-haiku-5-5`
      without submitting a turn. Principal `agent:a1:1` kept its stopped child's
      worktree when it became dirty or unmerged after planning. A fresh plan
      removed the clean, merged worktree and kept its branch tip. All three
      derived commands retained the manager principal. Direct Agent removal
      and cross-principal replay were refused. The owned Agents and daemon
      exited, and the stand was removed. Workspace formatting, locked release
      all-target builds and warnings-denied release Clippy passed for
      `eliot-store`, `eliot-daemon`, and `eliot`.
      Earlier binary SHA-256
      `765e0b96741d300cfbb97d12dd04bd503c817aac1151c93a1db2b64cccce6ab5`
      served `claude-haiku-5-5` to manager `agent:a1:1`, which removed a
      child-registered temp directory. Owner cleanup archived the exact branch
      tip and verified removal of an OpenCode native session without a model
      turn. The owned stand was removed. Frozen Codex archive scope and malformed
      acknowledgements have source coverage without a native fault-injection run.

      - [x] **Agent-launched managed child worktrees (2026-11-22).** A private
        Claude manager launched Claude and OpenCode children in parallel, refused
        an unrelated base, and integrated both child branches with the project
        check. Cleanup classified and removed both worktrees after the checked
        merge reached the private upstream. Both child refs survived worktree removal.

Service continuity across daemon replacement requires an actual use case. The
current contracts are not instructions to build that product in advance.
The optional desktop companion is tracked above.
Remote/cloud MCP access is not Later: it is the required L6.7 contour above.
