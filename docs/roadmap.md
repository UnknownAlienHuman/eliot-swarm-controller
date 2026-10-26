# Roadmap

Build connected vertical layers. L0–L6 use code-first implementation with formatting
and the smallest changed-package Clippy gate. The complete deterministic/live/platform/
load qualification campaign starts after the local product is feature-complete, in L7.

One manager owns one worktree/branch/PR. One task lands one useful connected result,
not scaffolding. This file is the only status tracker.

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add only packages whose changed contracts must compile together. No workspace-wide
campaign/model call/load run before L7. Tiny regression tests are allowed only when
source review cannot safely establish the change.

## Code budgets

| Crate | Production Rust budget |
| --- | ---: |
| `eliot-core` | 3,200 |
| `eliot-store` | 3,200 |
| `eliot-daemon` | 11,500 |
| `eliot-plugin` | 1,300 |
| `eliot` | 3,200 |
| each plugin | 3,000 |

The increases cover exact allocation/dedupe, turn/edit projections, platform custody,
durable trigger cursors and recovery—not a workflow engine.

---

## L0 — Archive, reset, skeleton

- [ ] **L0.1 Archive/delete v1.** Create `v1-final` at `dc6f70aa`; delete old code,
      migrations/tools/vendor/config/workflows/owner decisions/Justfile/notices from
      main. Keep new docs/root metadata.
- [ ] **L0.2 Minimal workspace.** Rust 2024 resolver 3, pinned stable, only
      `eliot --version`, budgets and fmt/scoped-clippy/loc plus deferred qualify.
- [ ] **L0.3 Minimal CI.** Windows + Ubuntu docs/link/whitespace, fmt,
      changed-package Clippy and loc. Full qualification enabled only at L7.

## L1 — Core, Store, bus, fake path

- [ ] **L1.1 Identity/reducer.** Signed-64-bit ids/counters, generation 0, fixed
      derived ids, expected vs target generation, canonical native subject, exact
      turn/request state, unknown-turn blocking/resolution, terminal token revocation,
      terminal cleanup/goal pause and pure reducer.
- [ ] **L1.2 Store.** Atomic root/id allocation, routing-lifecycle token hash/null
      constraint, native subject uniqueness, retention-independent native-event
      receipts, command/input/turn projections, one writer/group commit, query-only
      paginated readers, commit-failure boundary and WAL pressure.
- [ ] **L1.3 Bus/event intake.** Persist-before-broadcast, live-buffer→watermark→paged
      replay, explicit lag/history gap, commit-acknowledged plugin `event.put`/
      `process.put`, stable native-id resend/dedupe after event-row pruning, safe receipt
      reclamation and stale diagnostics excluded.
- [ ] **L1.4 Daemon/IPC.** Private data root, lock/birth identity, user-only local IPC,
      multiplexed JSON-RPC, live OS custody check before no-token owner fallback,
      fail-closed ambiguous peer identity, admission mode and exact drain/force.
- [ ] **L1.5 Configuration.** Global vs project authority, minimal environment,
      global∩project script pass-env validation, explicit secret refs/accounts/caps/
      provider holds, plugin modes, private paths, WAL/process/Git/backlog limits and
      named webhooks.
- [ ] **L1.6 Plugin host/custody.** Framing/deadlines and bounded unacknowledged/pre-ready
      spool; session vs linked initial outcome; attach `not_active_unknown`; canonical
      child scope; Windows Job; Linux exact cgroup/equivalent + guardian and honest
      degraded fallback; shared-control capability checks.
- [ ] **L1.7 Router/readiness.** Atomic exact binding, linked launch/resume send,
      FIFO future sends without blocking current controls, selector fixed ids,
      capacity reservations, stale expiry, no unknown retry and command/turn owner
      resolution with separate semantics.
- [ ] **L1.8 Fake plugin.** Isolated shared sessions, stable event ids, commit-ack
      resend, separate start/initial outcomes, exact snapshot including unknown turn,
      requests/children/crash/hang/exit.
- [ ] **L1.9 CLI/API.** Linked command output/wait, turn show/resolve, exact wait/result
      through turn projection, canonical attach, replay gap, command/action/mail
      readback and exits.
- [ ] **L1.10 Identity/secrets.** Complete token triple, hash-at-rest, terminal/detach
      revocation without generation reset, prefixed pattern redaction, invalid-token no
      owner fallback, known-secret rejection/masking.
- [ ] **L1.11 Qualification hooks.** Minimal recording/replay/plugin-test entrypoints;
      full cases deferred to L7.

**Completion:** fake CLI path launches with linked prompt, preserves every event across
commit/reconnect and event retention, answers requests, queues/controls exact turns,
exposes missing terminal outcome as unknown without inventing completion, stops/resumes
a new generation, survives restart and rejects stale/duplicate input.

## L2 — Codex + provider route

- [ ] **L2.1 Installed schema/recording.** Commit generated installed schema and bounded
      sanitized record/replay.
- [ ] **L2.2 Per-agent session.** One plugin + app-server in exact custody;
      `thread/start` without first turn; effective settings; ready committed before
      launch success.
- [ ] **L2.3 Linked first input/control.** Separate turn command, exact steer/inject/
      interrupt, stop+goal clear, resume next generation and configure application
      point; every lost-reply/turn-terminal boundary read back or kept unknown.
- [ ] **L2.4 Requests.** Exact approvals/questions.
- [ ] **L2.5 Native goal.** Status/budget and generation pause/rebind.
- [ ] **L2.6 Children.** Canonical child scope/ref and partial coverage.
- [ ] **L2.7 Accounts/proxy.** Permissions, atomic account/proxy reservations,
      persisted scoped holds/explicit probes, managed/attached OpenCodex and effective
      route readback.
- [ ] **L2.8 Useful manual path.** Compact real work run direct/proxy; retain evidence
      for L7 without broad campaign.

## L3 — Monitoring and workspaces

- [ ] **L3.1 Process monitor.** Boot/birth identity, exact/degraded backend and guardian
      health, commit-ack process facts, live peer membership classification, bounded
      source drain and correct lost.
- [ ] **L3.2 Workspace journal.** Generation-0 preparing/ready/unknown state,
      deterministic path, exact worktree readback, non-replayed setup, safe scratch and
      clean-only explicit removal.
- [ ] **L3.3 Git/fs/edit projection.** Read-only monitor, slow backoff, bounded
      fingerprint and durable `agent_edits` updated/cleared independently of events.
- [ ] **L3.4 Discovery/source handoff.** Canonical attach, observe-only transcript,
      exact cursor/ack handoff or visible gap, no duplicate lifecycle.
- [ ] **L3.5 Liveness/restart.** Progress-only stalls and current-state reconciliation
      of custody, source, workspace and unknown command/turn/action effects.
- [ ] **L3.6 Views.** Generation/turn/open stop/workspace state, custody/guardian,
      caller-identity failures, source gaps, account holds, Store/WAL and Git/edit facts.

## L4 — Other required harnesses

Each plugin selects mode from proven capabilities and rejects weaker substitutions.

- [ ] Hook shim only for facts absent from primary source.
- [ ] Claude Code per-agent route; record whether session can start before first input.
- [ ] Muse per-agent MSP; session then linked turn.
- [ ] OpenCode shared control only after two-session environment/token/Git attribution,
      attach/readback and isolated stop; otherwise per-agent or observe-only.
- [ ] Antigravity per-agent; record init-before-input behavior.
- [ ] ACP per-agent; session/new then linked prompt.
- [ ] Others only on concrete need.

## L5 — Coordination

- [ ] **Mailbox.** Principal-scoped selector expansion, `notice|full` presentation,
      capability-aware/coalesced wake, immutable one-open inbox batch + ack, restart
      reconciliation and secret rejection.
- [ ] **Variables.** Project/tree/global bounded CAS with exact principal/generation.
- [ ] **Who/claims.** Durable edit projection + Git facts/history, project-relative
      generation claims and conflict mail.
- [ ] **Hierarchy.** Canonical child upsert, tree limits/selectors and parent notices
      through ordinary mail rules.
- [ ] **MCP.** Only if a required harness cannot use CLI; stateless and principal-scoped.

## L6 — Automation

- [ ] **Principals/rule revisions.** Global vs project scope, canonical revision,
      event-time subject snapshot, first-load high-water, strict rendering and durable
      failed firing for deterministic render/authority errors.
- [ ] **Durable cursor/retention.** Per-rule committed cursor, restart scanning,
      deterministic-failure cursor advance, disabled/pause advance policy, explicit
      skipped range on backlog limit and bounded retention floor.
- [ ] **Run/action journal.** Frozen concrete targets/bindings, fixed ids, ordered
      states and no unknown replay.
- [ ] **Timers.** Schedule/action revisions, interval/cron/at, DST, latest catch-up and
      startup reconciliation.
- [ ] **Actions.** Scoped commands/mail/vars, exact Git readback, named allowlisted
      webhooks, secret refs and narrow unknown writer hold.
- [ ] **Scripts.** Scoped automation token, project-safe cwd, global∩project pass-env
      validation before value resolution, bounded streams, validate-all + atomic append
      and no unknown rerun.
- [ ] **Approvals.** Exact request; synchronous answer only after durable admission;
      safe `ask` on failure.
- [ ] **Goals.** Generation/revision-bound run identity, integer micro-USD, marker or
      journaled process check, unknown check/turn blocking and startup reconciliation.
- [ ] **Minimal recipes.** Writer/reviewer, fetch, nightly, quota notification and
      checkpoint/rotation—no DSL.

**Feature-complete milestone:** freeze features; scoped Clippy clean; start one complete
qualification campaign.

## L7 — Qualification and repair

### Core, Store and API

- [ ] all reducer rows, generation-0/no-token and live-token constraints, detach/
      terminal revocation without generation reset, stale/wrong/duplicate input;
- [ ] id bounds/derived collisions/counters/root allocation/canonical native subject;
- [ ] event commit-ack lost reply, original event-row pruning, resend dedupe through
      compact receipt, and safe old-generation receipt reclamation;
- [ ] launch/resume linked input at every failure point;
- [ ] turn projection survives retention; command applied vs turn terminal remain
      separate; reconnect missing terminal → unknown; active-turn manual resolve rejects;
- [ ] commit failure/crash, WAL pinned-reader/hard limit and replay history gap;
- [ ] peer connects before process registration: live Job/cgroup/PGID membership blocks
      no-token owner fallback; ambiguous membership rejects;
- [ ] drain/force/attached detach/unknown blockers.

### Recovery, automation and coordination

- [ ] event commit before/after rule cursor, mutable labels after event, rule edit/
      pause/restart/backlog skipped-range retention/replay;
- [ ] matching event with missing placeholder/invalid action creates one failed run,
      advances cursor and permits the next valid event; Store commit failure does not;
- [ ] global vs project principal escape negatives, including project script attempting
      a globally allowed but project-disallowed environment reference before process start;
- [ ] timer/goal/mail/workspace/launch/provider startup reconciliation;
- [ ] every action/check/script crash phase and no external replay;
- [ ] busy agent with/without inject, notice/full distinction, lost inbox/ack, one open
      batch and generation replacement;
- [ ] edit attribution and turn results across retention/restart.

### Plugins, platform and live paths

- [ ] ready vs initial, exact bindings, canonical children, commit-ack source handoff,
      reconnect `not_active_unknown`, bounded buffers and unsupported-before-I/O;
- [ ] Windows Job no-breakaway/daemon death;
- [ ] Linux exact cgroup+guardian failure/restart and degraded fallback rejecting force;
- [ ] workspace worktree/setup crash/readback/remove cases;
- [ ] OpenCode two-session environment/token/Git isolation and isolated stop;
- [ ] compact live run for every required harness/proxy.

### Security, load and retention

- [ ] minimal env vs inherit warning; project config/profile/pass-env/script/webhook
      escape negatives;
- [ ] token pattern/hash/revocation, short/unredactable secret rejection and output masking;
- [ ] private data/IPC/symlink/reparse/traversal/device path negatives;
- [ ] 10,000 fake agents + 2,000 persisted events/s for one hour, bounded receipt/WAL/
      RSS growth, responsive paginated reads and no persisted loss;
- [ ] active-rule cursor/native receipt/turn/edit retention under long-running agents;
- [ ] drain 1,000 busy fake agents without new automation or lost results.

**Exit:** qualification green on Windows and supported Linux paths, current compact
live runs, every discovered failure repaired in code/spec. Only then is v2 ready.

## L8/L9

Port retained non-Rust plugins only for measured benefit; delete temporary duplicates;
keep budgets honest. Remote access, multi-host, GUI and Memory OS require new owner
decisions.
