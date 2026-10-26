# Roadmap

Build connected vertical layers. L0–L6 are code-first with formatting and the smallest
changed-package Clippy gate. The complete deterministic/live/platform/load campaign
starts only after the local product is feature-complete, in L7.

One manager owns one worktree/branch/PR. One task lands one useful connected result, not
scaffolding. This file is the only status tracker.

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add only packages whose changed contracts compile together. No workspace-wide campaign,
model call or load run before L7. A tiny regression is allowed only when source review
cannot establish safety.

## Code budgets

| Crate | Production Rust budget |
| --- | ---: |
| `eliot-core` | 3,300 |
| `eliot-store` | 3,600 |
| `eliot-daemon` | 11,800 |
| `eliot-plugin` | 1,500 |
| `eliot` | 3,300 |
| each plugin | 3,000 |

Increases cover receipts/derived IDs, frozen bindings, source ownership/native replay
receipts, input/turn/edit/workspace projections, custody, rule activations and recovery—not
a workflow engine.

---

## L0 — Archive, reset, skeleton

- [ ] **L0.1 Archive/delete v1.** Create `v1-final` at `dc6f70aa`; delete v1 Rust/Node/
      Python implementation, migrations, vendor/config, old workflows, old Justfile,
      notices and old tooling. **Preserve** v2 docs/root metadata plus
      `.github/workflows/v2-docs.yml`, `tools/ci/validate-v2-docs.py` and
      `tools/ci/validate-v2-schema.py`.
- [ ] **L0.2 Minimal workspace.** Rust 2024 resolver 3, pinned stable, only
      `eliot --version`, budgets and fmt/scoped-clippy/loc plus deferred qualify.
- [ ] **L0.3 Minimal CI.** Windows + Ubuntu docs/link/schema/whitespace, fmt,
      changed-package Clippy and loc. Full qualification enabled only at L7.

## L1 — Core, Store, bus and fake vertical path

- [ ] **L1.1 Identity/reducer.** Signed-64 ids/counters with pre-mutation exhaustion;
      external ULID WriteId vs fixed DerivedId; generation 0; canonical native subject;
      exact turn/request; async-input/unknown-turn gates; `routing_enabled` separate
      from native control/observe state; pure reducer.
- [ ] **L1.2 Store/schema.** Atomic root/id allocation; private owner token + routing-key
      identity; routing hash lifecycle; frozen `agent_bindings`; external receipts with
      minimum/FK-aware retention and ULID floor; source epochs/family owners/native-event
      receipts; commands/links/native-input/turn projections; one writer/group commit;
      query-only paginated readers; commit-failure and WAL-pressure boundary.
- [ ] **L1.3 Bus/source intake.** Persist-before-broadcast; live-buffer→watermark→paged
      replay; explicit lag/history gap; source open before native effect; contiguous
      `event.put` and verified `process.put`; strict-cursor/event-id/no-replay modes;
      predecessor-aware atomic source handoff; lost-ack/digest/hole/stale handling.
- [ ] **L1.4 Daemon/IPC.** Private data root, explicit replaceable owner credential,
      nonreplaceable routing key, client ULID state, lock/boot/process identity,
      user-only IPC, multiplexed JSON-RPC, missing-credential rejection, admission and
      exact drain/force/abandon-degraded behavior.
- [ ] **L1.5 Configuration.** Owner-defined global routes; project overlays only narrow
      safe fields; immutable launch/attach binding; minimal env and global∩project script
      refs; accounts/caps/holds/probes; selector/frame/page/event limits; source spool,
      native-receipt retention and WAL limits; named webhooks/safe paths.
- [ ] **L1.6 Plugin host/custody.** Framing/deadlines and artifact refs; bounded pages/
      spool; source open before start; session vs linked initial outcome; atomic handoff;
      attach unknown-turn/readback; retry-safe child registration with optional direct
      routing; Windows Job; Linux cgroup+guardian/degraded fallback; service PID isolation.
- [ ] **L1.7 Router/readiness.** External concrete/fanout parent receipts, derived linked/
      selector children, atomic bounded selector snapshot + pagination; FIFO inputs
      without blocking controls; immutable Agent binding; control-attach capacity;
      stale expiry and exact unknown readback/assertion.
- [ ] **L1.8 Fake plugin.** Isolated shared sessions; persistent source open/sequence/
      replay receipts/spool; both start modes; async inbox/turn association with optional
      native input id; coalesced inputs, unknown turn; paged discovery/snapshot;
      child guard/routing/token retry; crash/hang/exit.
- [ ] **L1.9 CLI/API.** Explicit `--owner`; monotonic WriteId generation/reuse/floor;
      linked command/fanout count-digest + paged children; exact wait/result/turn
      resolve; paged discover; observe/control attach binding; replay gaps/exit codes.
- [ ] **L1.10 Secrets/identity.** Owner-token isolation from child environments;
      deterministic Agent-token derivation/hash-at-rest; terminal/detach/routing-disabled
      revocation; source-spool masking; secret-bearing durable payload rejection.
- [ ] **L1.11 Qualification hooks.** Minimal record/replay/plugin-test entrypoints only;
      full suite remains L7.

**Completion:** fake CLI path launches with linked prompt, survives lost replies at every
Store/plugin boundary, suppresses vendor replay after event pruning, maps inputs with or
without native input ids to turns, preserves wait/results, paginates fleet operations,
handles attach/routing/child guards, and rejects stale/duplicate/unauthorized facts.

## L2 — Codex + explicit provider route

- [ ] **L2.1 Installed schema/recordings.** Commit generated schema from owner's Codex;
      bounded sanitized record/replay.
- [ ] **L2.2 Per-Agent session.** One plugin + app-server in exact custody; source epochs
      open before `thread/start`; thread starts before first turn; effective settings;
      ready committed before launch success.
- [ ] **L2.3 Linked input/control.** Separate turn command; exact steer/inject/interrupt;
      stop + native-goal clear; resume next generation/new binding; configure application
      point; every lost reply/terminal boundary read back or unknown.
- [ ] **L2.4 Requests.** Exact approvals/questions and synchronous hook deadline where
      actually supported.
- [ ] **L2.5 Native Goal.** Status/budget, durable eligibility sequence and generation
      pause/rebind.
- [ ] **L2.6 Children.** Canonical child subject/registration; guard behavior; declare
      whether distinct child environment exists; parent attribution when absent.
- [ ] **L2.7 Accounts/proxy.** Permissions, atomic account/proxy reservations, persisted
      holds/explicit probes, managed/attached OpenCodex and effective route.
- [ ] **L2.8 Useful manual path.** Compact real work run direct/proxy; retain evidence
      for L7 without broad campaign.

## L3 — Monitoring and workspaces

- [ ] **L3.1 Process monitor.** Boot/birth identity; OS-authoritative start/exit/stats;
      idempotent plugin descendant registration; service vs Agent ownership; exact/
      degraded backend and guardian health; correct lost vs `orphan_possible`.
- [ ] **L3.2 Workspace journal.** `workspaces` + `workspace_steps`; generation-0
      preparing/ready/unknown; deterministic path; frozen local base OID; exact Git
      readback; non-replayed setup; safe scratch and clean-only removal.
- [ ] **L3.3 Git/fs/edit projection.** Read-only monitor, slow backoff, frozen-base diff,
      bounded fingerprint and durable `agent_edits`; current `who` filters live controlled
      generations while retaining cleanup provenance.
- [ ] **L3.4 Discovery/source handoff.** Canonical observe/control attach, paged native
      discovery, strict-cursor/event-id/none recovery and atomic family handoff or gap.
- [ ] **L3.5 Liveness/restart.** Progress-only stalls; reconcile custody/sources/
      workspaces/unknown commands-inputs-turns/actions/routing-key health.
- [ ] **L3.6 Views.** Generation/binding/routing/turn/input gate/open stop/workspace,
      custody/guardian/orphans, service ownership, source modes/watermarks/gaps, holds,
      Store/WAL and Git/edit facts.

## L4 — Other required harnesses

Each plugin selects mode from proven capabilities, records installed behavior and
rejects weaker substitution.

- [ ] Hook shim only for facts absent from primary source.
- [ ] Claude Code per-Agent; source open before SDK query/input; record start-before-input,
      exact request and child-environment facts.
- [ ] Muse per-Agent MSP; source open before session/start, then linked turn.
- [ ] OpenCode shared control only after two-session environment/token/Git attribution,
      attach/readback and isolated stop; source open before session create; shared PID
      remains service-owned.
- [ ] Antigravity per-Agent; source open before process/input; record init-before-input
      and child-environment limitations.
- [ ] ACP per-Agent; source/open routing before `session/new`, then linked prompt.
- [ ] Others only on concrete need.

## L5 — Coordination

- [ ] **Mailbox.** Bounded selector expansion; `wake_on_mail=false` queue-only;
      routed Agents receive one coalesced notice; routing-disabled native children use
      routed parent relay or remain queue-only; full content requires explicit
      inject/send/steer control; immutable bounded batch + ack, restart reconciliation,
      retention/gaps and secret rejection.
- [ ] **Variables.** Project/tree/global bounded CAS with exact principal/generation.
- [ ] **Who/claims.** Live-controlled current edits + Git/history, project-relative
      generation claims with uniqueness/conflict mail and transactional release.
- [ ] **Hierarchy.** Canonical native child upsert even above controller guard; reduced
      authority/routing/wake; parent notices through normal mail policy.
- [ ] **MCP.** Only if a required harness cannot use CLI; stateless, principal-scoped and
      same WriteId/readback semantics.

## L6 — Automation

- [ ] **Principals and rule activations.** Global/project scope; persisted compiled
      definitions; definition digest distinct from activation id; A→B→A fresh
      activation; event-time subject; first-load high-water and exact old→new handoff.
- [ ] **Cursors/retention.** Active/retiring/replay interval cursors, restart drain from
      old definition, deterministic-failure advance, explicit backlog skipped range and
      bounded retention floor.
- [ ] **Generic run/action journal.** `(kind,owner activation,trigger)` for event/timer/
      manual/Goal; frozen targets, derived ids, ordered states, durable rate limits and
      no unknown replay.
- [ ] **Timers.** Activation/time boundary, interval/cron/at, DST, latest catch-up and
      startup reconciliation, including action-only edit/revert.
- [ ] **Actions.** Scoped commands/mail/vars; exact Git OID/readback; named allowlisted
      webhooks; secret refs and narrow unknown writer hold.
- [ ] **Scripts.** Read-only scoped token, project-safe cwd, global∩project env validation
      before lookup, bounded streams, validate-all + atomic append, no unknown rerun.
- [ ] **Approvals.** Exact async request actions; deadline-bound non-dispatchable
      synchronous proposals; safe default on late/failed commit.
- [ ] **Goals.** Durable eligibility sequence for on-idle, anchored cadence slots,
      generation/revision, integer micro-USD, journaled check, unknown blocking and
      startup reconciliation; routing-disabled target rejects unless parent-mediated
      continuation is qualified.
- [ ] **Minimal recipes.** Writer/reviewer, fetch, nightly, quota notification and
      checkpoint/rotation—no DSL.

**Feature-complete milestone:** freeze features; every crate scoped-Clippy clean; begin
one complete qualification campaign.

## L7 — Qualification and repair

### Core, SQLite and API

- [ ] execute every documented DDL with foreign keys; CHECK/FK/UNIQUE rows for Agent
      binding, routing-enabled/disabled live states, detach/terminal/token lifecycle,
      nullable native input id and source-family ownership;
- [ ] external receipt vs derived command; selector links; command retention longer/
      shorter than receipt window; dependency-order pruning/floor;
- [ ] native event-id replay after event-row pruning, strict-cursor resume and none-mode
      gap; receipt mismatch/reclamation boundary;
- [ ] source open before native effect, lost open reply, predecessor handoff atomicity,
      sequence stale/hole/vendor duplicate and safe source reclamation;
- [ ] owner/Agent/automation missing/invalid credential rejection; routing-key restore/
      mismatch; routed/routing-disabled child and reattach token lifecycle;
- [ ] linked launch/resume input at every failure point; direct turn association with no
      native input id; coalesced inputs, unknown terminal and constrained resolution;
- [ ] selector/discover pages at 0/1/1,000/10,000/overflow and exact frame limits;
- [ ] observe vs control attach profile/capacity/readback and detach reservation;
- [ ] Store failure/crash, WAL pinned reader/hard limit and replay history gap;
- [ ] drain/force/degraded abandon/unknown blockers.

### Recovery, automation and coordination

- [ ] A→B→A event rule while first A drains; unique activations/persisted definitions;
      exact H boundary has no overlap/gap;
- [ ] invalid placeholder/action/authority/fanout creates one failed run and next event
      proceeds; Store failure does not advance;
- [ ] timer action/trigger edit and A→B→A at due boundary exactly once;
- [ ] Goal on-idle same eligibility after restart exactly once; next terminal-state
      change creates next round; cadence slot exactly once;
- [ ] global/project overlay/account/proxy/script/webhook escape negatives;
- [ ] timer/Goal/mail/workspace/launch/provider startup reconciliation;
- [ ] every action/check/script crash phase and no external replay;
- [ ] routed notice, routing-disabled parent relay/queue-only, explicit full control
      delivery, lost inbox/ack and one open batch; edits/turn results across retention/
      restart.

### Plugins, platform and live paths

- [ ] ready vs initial, source open before native I/O, stable open/replacement, bounded
      spool/pages/frame, all dedupe modes and unsupported-before-I/O;
- [ ] child parent conflict, guard-exceeded recording, routing-disabled attribution and
      memory-only token retry when environment is qualified;
- [ ] OS enumeration + plugin registration of same PID publishes one start; shared
      service PID cannot become Agent custody; service stop isolation;
- [ ] Windows Job no-breakaway/daemon death;
- [ ] Linux cgroup+guardian failure/restart; degraded fallback rejects exact force and
      reports possible orphan;
- [ ] workspace base-ref movement after admission, Git/setup lost replies/readback/remove;
- [ ] OpenCode two-session environment/token/Git isolation and isolated stop;
- [ ] compact live run for every required harness/proxy.

### Security, load and retention

- [ ] owner token never inherited; missing credential never becomes owner; same-user
      trust boundary documented honestly;
- [ ] minimal env/project-overlay negatives; routing key/token hash/pattern; short-secret
      rejection; source-spool/output masking; private data/symlink/device paths;
- [ ] 10,000 fake Agents + 2,000 persisted events/s for one hour; bounded source epochs/
      native receipts/pages/WAL/RSS and responsive reads with no persisted loss;
- [ ] active/retiring activations, receipts/FKs, source/native receipts, bindings,
      workspaces, turns/edits/mail under long retention combinations;
- [ ] drain 1,000 busy fake Agents without new automation or lost results.

**Exit:** qualification green on Windows and supported Linux paths; compact current live
runs; every discovered defect repaired in code/spec. Only then is v2 ready.

## L8/L9

Port non-Rust plugins only for measured benefit; delete temporary duplicate routes and
keep budgets honest. Remote access, multi-host, GUI and Memory OS require new owner
decisions.
