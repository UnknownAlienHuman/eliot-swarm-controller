# Roadmap

Build connected vertical layers. L0–L6 use code-first implementation with formatting and
the smallest changed-package Clippy gate. The complete deterministic/live/platform/
load qualification campaign starts only after the local product is feature-complete,
in L7.

One manager owns one worktree/branch/PR. One roadmap task should land one useful
connected result, not scaffolding. This file is the only status tracker.

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add only packages whose changed contracts must compile together. No workspace-wide
campaign, model call or load run before L7. Tiny regression tests are allowed only when
review cannot safely establish the change.

## Code budgets

| Crate | Production Rust budget |
| --- | ---: |
| `eliot-core` | 3,200 |
| `eliot-store` | 3,000 |
| `eliot-daemon` | 11,000 |
| `eliot-plugin` | 1,200 |
| `eliot` | 3,200 |
| each plugin | 3,000 |

The increases cover exact id allocation/dedupe, platform custody, durable trigger
cursors and recovery—not a workflow engine. Exceed only with deletion/simplification
attempt plus one concrete reason.

---

## L0 — Archive, reset, skeleton

- [ ] **L0.1 Archive/delete v1.** Create `v1-final` at `dc6f70aa`. Delete old code,
      migrations, tools, vendor/config, old workflows, `owner-decisions`, old
      `Justfile` and notices from `main`. Keep new docs/root metadata.
- [ ] **L0.2 Minimal workspace.** Rust 2024 resolver 3, pinned stable toolchain,
      `eliot --version`, budgets and Just targets for fmt/scoped-clippy/loc plus
      deferred test/qualify.
- [ ] **L0.3 Minimal CI.** Windows + Ubuntu: docs/whitespace, fmt, changed-package
      Clippy, loc. Full qualification is enabled only at L7.

**Completion:** v1 is safely tagged, main contains the minimal v2 workspace, scoped
Clippy green.

## L1 — Core, Store, bus, fake path

- [ ] **L1.1 Core identity/reducer.** Signed-64-bit checked ids; generation 0 reserve;
      fixed derived ids; expected vs target generation (resume); canonical native
      subject; exact turn/request validation; terminal cleanup/goal pause; pure reducer.
- [ ] **L1.2 Store.** Counter-based agent allocation (`root=id` atomically), migration
      runner, token hashes, unique native subject/event dedupe, one writer/group commit,
      query-only paginated readers, commit-failure boundary, WAL monitoring/checkpoints.
- [ ] **L1.3 Bus.** Persist-before-broadcast; live-buffer→watermark→paged replay;
      explicit lag; core duplicate/stale diagnostics never normal lifecycle.
- [ ] **L1.4 Daemon/IPC.** Private data root, lock/daemon identity, user-only pipe/socket,
      multiplexed JSON-RPC, admission mode and exact drain/force behavior.
- [ ] **L1.5 Configuration.** Strict global/project merge; minimal environment default,
      explicit pass-env/secret refs, accounts/profile caps, plugin modes, private paths,
      WAL/process/Git limits. Parse proxies/webhooks for later use.
- [ ] **L1.6 Plugin host/custody.** Protocol framing/deadlines; bounded pre-ready buffer;
      start session vs linked initial input outcomes; exact child scope; Windows Job;
      Linux exact cgroup/equivalent + guardian and honest degraded fallback; shared
      control capability checks.
- [ ] **L1.7 Router/readiness.** Atomic expected/target generation binding, linked
      launch/resume send, FIFO future sends without blocking current controls, selector
      expansion with fixed ids, max-live queue, stale generation expiry, no unknown
      retry, owner resolution.
- [ ] **L1.8 Fake plugin.** Shared isolated sessions; stable event ids; separate start/
      initial outcomes; exact snapshot, requests, children, crash/hang/exit.
- [ ] **L1.9 CLI/API.** Core commands, linked-command output/wait behavior, canonical
      attach, event replay, command readback and documented exit codes.
- [ ] **L1.10 Agent identity/secrets.** Complete token triple, hash-at-rest, invalid
      token no owner fallback, known-secret rejection/masking, minimal environment.
- [ ] **L1.11 Qualification hooks.** Minimal recording/replay/plugin-test entrypoints
      only; complete cases deferred to L7.

**Completion:** CLI fake path can launch with linked prompt, answer a request, queue and
control exact turns, stop, resume a new generation, recover state, and reject stale/
duplicate input. Scoped Clippy green.

## L2 — Codex + provider route

- [ ] **L2.1 Installed schema/recording.** Generate from installed Codex; bounded
      sanitized record/replay.
- [ ] **L2.2 Per-agent session.** One plugin + app-server in exact custody; thread start
      without initial input where possible; effective route/settings; ready before
      launch success.
- [ ] **L2.3 Linked first input/control.** Send, exact steer/inject/interrupt, stop with
      native-goal clear, resume next generation, configure application point. Start-
      requires-input behavior, if any, reports initial command separately.
- [ ] **L2.4 Requests.** Exact approval/question ids and answers.
- [ ] **L2.5 Native goal.** Native goal mapping/status, generation pause/rebind.
- [ ] **L2.6 Children.** Exact child native scope/ref and partial-coverage honesty.
- [ ] **L2.7 Accounts/proxy.** Permissions, account+proxy caps, persisted scoped provider
      holds/probes, managed/attached OpenCodex and effective endpoint readback.
- [ ] **L2.8 Useful manual path.** One compact real work run with and without proxy;
      retain output for L7 rather than starting broad qualification.

## L3 — Monitoring and workspaces

- [ ] **L3.1 Process monitor.** Birth identity, exact/degraded custody inventory,
      guardian/cgroup/Job verification, bounded terminal source drain and correct lost.
- [ ] **L3.2 Worktrees.** Explicit branch worktrees/setup, safe relative scratch,
      per-agent Git identity, clean-only explicit removal, branch kept.
- [ ] **L3.3 Git/fs + edit projection.** Read-only no-optional-locks monitor, slow
      backoff, bounded fingerprint, `agent_edits` projection updated/cleared without
      event-history reconstruction.
- [ ] **L3.4 Discovery/source handoff.** Observe-only transcript discovery, canonical
      native attach, exact cursor handoff or gap, no duplicate lifecycle.
- [ ] **L3.5 Liveness/restart.** Progress-only stalls and current-state reconciliation
      of custody/sources/unknown commands.
- [ ] **L3.6 Views.** Generation/turn/open-stop, custody capability, gaps, account holds,
      Store/WAL and Git/edit facts.

## L4 — Other required harnesses

Each plugin selects per-agent/shared mode from real capabilities, records installed
schema/behavior and rejects weaker substitutions.

- [ ] Hook shim only for facts absent from primary stream.
- [ ] Claude Code per-agent SDK/CLI route.
- [ ] Muse per-agent MSP route.
- [ ] OpenCode: shared control only after per-session environment + isolated stop are
      proven; otherwise per-agent server or observe-only shared attach.
- [ ] Antigravity per-agent stream-json.
- [ ] ACP per-agent subprocess.
- [ ] Command/Kilo/Zed only on concrete need.

## L5 — Coordination

- [ ] **Mailbox.** Durable selector expansion, capability-aware/coalesced wake,
      no inject→send fallback while busy, batch acknowledgement, restart unread
      reconciliation and secret rejection.
- [ ] **Variables.** Scoped bounded CAS and generation attribution.
- [ ] **Who/claims.** Durable edit projection + Git sets/history, generation-bound
      advisory claims and conflict mail.
- [ ] **Hierarchy.** Canonical native child upsert, tree limits/selectors and parent
      notifications through normal mailbox rules.
- [ ] **MCP surface.** Only if a required harness cannot use CLI; stateless and
      daemon-authorized.

## L6 — Automation

- [ ] **Rule revisions/cursors.** Strict TOML, canonical revision, first-load high-water,
      durable per-rule event cursor, restart scanning and no implicit historical replay.
- [ ] **Run/action journal.** Frozen concrete targets/bindings, fixed ids, ordered
      action states and no unknown replay.
- [ ] **Timers.** Durable schedule revision, interval/cron/at, DST, latest catch-up and
      startup reconciliation.
- [ ] **Actions.** Commands/mail/vars, exact Git readback, named webhook endpoints,
      secret env references and narrow unknown writer holds.
- [ ] **Scripts.** Explicit argv/custody, bounded streams, validate-all then atomic
      append, no unknown rerun.
- [ ] **Approvals.** Exact request binding; synchronous decision only after durable
      commit; safe ask on failure.
- [ ] **Goals.** Generation/revision-bound rounds, integer micro-USD, durable in-flight
      identity and startup current-state reconciliation.
- [ ] **Minimal recipes.** Writer/reviewer, fetch, nightly, quota notification and
      checkpoint/rotation—no DSL.

**Feature-complete milestone:** freeze features; every crate scoped-Clippy clean; begin
one complete qualification campaign.

## L7 — Qualification and repair

### Core/Store/API

- [ ] all reducer rows plus stale/wrong/duplicate input;
- [ ] id bounds/derived-id collisions, counter/root allocation, token hash;
- [ ] unique native subject/event dedupe;
- [ ] launch/resume linked initial input under every lost-reply point;
- [ ] Store commit failure/crash and WAL pinned-reader limits;
- [ ] bus replay boundary and no stale diagnostic automation;
- [ ] drain/force/unknown blockers.

### Recovery/automation/coordination

- [ ] event committed before/after rule cursor transaction, rule edit/restart;
- [ ] timer/goal/mail/launch/provider-hold startup reconciliation;
- [ ] every action crash phase and no external replay;
- [ ] mail wake with/without inject, lost inbox/ack and generation replacement;
- [ ] edit attribution across retention/restart/commit/terminal generation.

### Plugins/platform/live

- [ ] plugin conformance: ready vs initial, exact bindings, source handoff, canonical
      children, bounded buffers, no unknown retry;
- [ ] Windows Job no-breakaway and daemon death;
- [ ] Linux exact cgroup+guardian; degraded fallback honestly rejects force; escaped
      process cannot be falsely reported contained;
- [ ] OpenCode two-session environment/token/Git isolation and isolated stop;
- [ ] compact live run for every required harness/proxy.

### Security/load/retention

- [ ] minimal env does not leak unrelated keys; inherit warns; durable secret payloads
      reject; output masking;
- [ ] private data/IPC/symlink/path traversal negatives;
- [ ] 10,000 fake agents, 2,000 persisted events/s for one hour, bounded WAL/RSS,
      responsive paginated reads and no persisted loss;
- [ ] retention with live long-running agents preserves edit/current projections;
- [ ] drain 1,000 busy fake agents without new automation or lost results.

**Exit:** full qualification green on Windows and Ubuntu where supported, current live
runs retained compactly, all discovered failures repaired in code/spec. Only then is v2
ready.

## L8/L9

Port retained non-Rust plugins only for measured benefit, delete temporary duplicate
paths, and keep code budgets honest. Remote access, multi-host, GUI and Memory OS need
new owner decisions.
