# Roadmap

Build connected vertical layers. L0–L6 are code-first with formatting and smallest
changed-package Clippy gate. The complete deterministic/live/platform/load campaign
starts only after local product is feature-complete, in L7.

One manager owns one worktree/branch/PR. One task lands one useful connected result,
not scaffolding. This file is the only status tracker.

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add only packages whose changed contracts compile together. No workspace-wide campaign,
model call or load run before L7. Tiny regression allowed only when source review cannot
establish safety.

## Code budgets

| Crate | Production Rust budget |
| --- | ---: |
| `eliot-core` | 3,200 |
| `eliot-store` | 3,400 |
| `eliot-daemon` | 11,800 |
| `eliot-plugin` | 1,400 |
| `eliot` | 3,300 |
| each plugin | 3,000 |

Increases cover receipts/derived IDs, source ownership/watermarks, input/turn/edit
projections, custody, rule activations and recovery—not a workflow engine.

---

## L0 — Archive, reset, skeleton

- [ ] **L0.1 Archive/delete v1.** Create `v1-final` at `dc6f70aa`; delete old code,
      migrations/tools/vendor/config/workflows/owner decisions/Justfile/notices from
      main. Keep v2 docs/root metadata.
- [ ] **L0.2 Minimal workspace.** Rust 2024 resolver 3, pinned stable, only
      `eliot --version`, budgets and fmt/scoped-clippy/loc plus deferred qualify.
- [ ] **L0.3 Minimal CI.** Windows + Ubuntu docs/link/whitespace, fmt,
      changed-package Clippy and loc. Full qualification enabled only at L7.

## L1 — Core, Store, bus and fake vertical path

- [ ] **L1.1 Identity/reducer.** Signed-64 ids/counters with pre-mutation exhaustion;
      external ULID WriteId vs fixed internal DerivedId; generation 0; canonical native
      subject/child parent conflict; expected/target generation; exact turn/request;
      async-input/unknown-turn gates; token version/revocation; pure reducer.
- [ ] **L1.2 Store/schema.** Atomic root/id allocation; private routing-key identity +
      token hash lifecycle; external receipts with minimum/FK-aware retention and ULID
      floor; canonical native subject; source open/watermark/family ownership; commands
      with nullable receipt, links, native-input/turn projections; one writer/group
      commit; query-only paginated readers; commit-failure and WAL pressure boundary.
- [ ] **L1.3 Bus/source intake.** Persist-before-broadcast; live-buffer→watermark→paged
      replay; explicit lag/history gap; idempotent `source.open/close`; contiguous
      `event.put` and verified per-agent `process.put`; one unacknowledged envelope;
      digest/stale/hole/vendor-duplicate handling; transactional family handoff.
- [ ] **L1.4 Daemon/IPC.** Private data root/routing key/client ULID state, lock/boot/
      process identity, user-only local IPC, multiplexed JSON-RPC, birth-pinned live OS
      custody classification before no-token owner, fail-closed ambiguity, admission and
      exact drain/force/abandon-degraded behavior.
- [ ] **L1.5 Configuration.** Global/project authority; fully resolved profile/extends
      allowlists; immutable launch binding; minimal env and global∩project script refs;
      accounts/caps/holds/probes; selector limit; source spool, retention and WAL limits;
      named webhooks and safe paths.
- [ ] **L1.6 Plugin host/custody.** Framing/deadlines; stable source open id/key and
      bounded spool; session vs linked initial outcome; attach unknown-turn readback;
      retry-safe child registration/token; Windows Job; Linux cgroup+guardian and
      degraded fallback; reject shared-service PID as Agent custody.
- [ ] **L1.7 Router/readiness.** External concrete/fanout parent receipts, derived linked/
      selector children, atomic deterministic selector snapshot with hard limit and
      paginated children; FIFO future inputs without blocking controls; capacity
      reservations; stale expiry; exact unknown readback/assertion rules.
- [ ] **L1.8 Fake plugin.** Isolated shared sessions; persistent source open/sequence/
      spool; lost-ack resend; separate start/initial; async inbox/turn association,
      coalesced inputs, unknown turn; requests/children/token retry; crash/hang/exit.
- [ ] **L1.9 CLI/API.** Monotonic WriteId generation before send, retry reuse and floor;
      linked command/fanout count-digest + paginated children; exact wait/result/turn
      resolve; canonical attach/reattach token version; replay gaps; exit codes.
- [ ] **L1.10 Secrets/identity.** Routing key recovery state, deterministic token
      derivation/hash-at-rest, terminal/detach revocation, source-spool masking,
      secret-bearing durable payload rejection, invalid-token no owner fallback.
- [ ] **L1.11 Qualification hooks.** Minimal record/replay/plugin-test entrypoints only;
      full suite remains L7.

**Completion:** fake CLI path launches with linked prompt, survives lost replies at every
Store/plugin boundary, maps async inputs to turns, preserves exact wait/results after
history pruning, handles selector fanout, stops/resumes/re-attaches safely and rejects
stale/duplicate/unauthorized facts.

## L2 — Codex + explicit provider route

- [ ] **L2.1 Installed schema/recordings.** Commit generated schema from owner's Codex;
      bounded sanitized record/replay.
- [ ] **L2.2 Per-agent session.** One plugin + app-server in exact custody; thread start
      before first turn; stable source opens; effective settings; ready committed before
      launch success.
- [ ] **L2.3 Linked input/control.** Separate turn command; exact steer/inject/interrupt;
      stop + native-goal clear; resume next generation; configure application point;
      every lost reply/terminal boundary read back or unknown.
- [ ] **L2.4 Requests.** Exact approvals/questions and synchronous hook deadline where
      actually supported.
- [ ] **L2.5 Native goal.** Status/budget and generation pause/rebind.
- [ ] **L2.6 Children.** Canonical child subject/registration; declare whether distinct
      child environment exists; parent attribution when absent.
- [ ] **L2.7 Accounts/proxy.** Permissions, atomic account/proxy reservations, persisted
      scoped holds/explicit probes, managed/attached OpenCodex and effective route.
- [ ] **L2.8 Useful manual path.** Compact real work run direct/proxy; retain evidence
      for L7 without broad campaign.

## L3 — Monitoring and workspaces

- [ ] **L3.1 Process monitor.** Boot/birth identity; OS-authoritative start/exit/stats;
      idempotent plugin descendant registration; service vs agent ownership; exact/
      degraded backend and guardian health; live peer classification; correct lost vs
      `orphan_possible`.
- [ ] **L3.2 Workspace journal.** Generation-0 preparing/ready/unknown; deterministic
      path; resolve base ref to frozen local OID at admission; exact worktree readback;
      non-replayed setup; safe scratch and clean-only explicit removal.
- [ ] **L3.3 Git/fs/edit projection.** Read-only monitor, slow backoff, frozen-base diff,
      bounded fingerprint and durable `agent_edits`; current `who` filters live controlled
      generations while keeping cleanup provenance.
- [ ] **L3.4 Discovery/source handoff.** Canonical attach/observe-only transcript,
      stable open id/key, family-owner handoff with cursor or gap, no duplicate lifecycle.
- [ ] **L3.5 Liveness/restart.** Progress-only stalls; reconcile custody/sources/
      workspaces/unknown commands-inputs-turns/actions/routing-key health.
- [ ] **L3.6 Views.** Generation/turn/input gate/open stop/workspace binding, custody/
      guardian/orphans, service ownership, source families/watermarks/gaps, caller
      classification, holds, Store/WAL and Git/edit facts.

## L4 — Other required harnesses

Each plugin selects mode from proven capabilities, records installed behavior and
rejects weaker substitution.

- [ ] Hook shim only for facts absent from primary source.
- [ ] Claude Code per-agent; record start-before-input, exact request and child-env facts.
- [ ] Muse per-agent MSP; session then linked turn; exact child/token attribution.
- [ ] OpenCode shared control only after two-session environment/token/Git attribution,
      attach/readback and isolated stop; shared server PID remains service-owned.
- [ ] Antigravity per-agent; record init-before-input and child environment limitations.
- [ ] ACP per-agent; session/new then linked prompt; declared capability-dependent child.
- [ ] Others only on concrete need.

## L5 — Coordination

- [ ] **Mailbox.** Bounded deterministic selector expansion, notice/full presentation,
      capability-aware wake, immutable bounded open batch + ack, restart reconciliation,
      retention/history gaps and secret rejection.
- [ ] **Variables.** Project/tree/global bounded CAS with exact principal/generation.
- [ ] **Who/claims.** Live-controlled current edits + Git/history, project-relative
      generation claims with uniqueness/conflict mail and transactional release.
- [ ] **Hierarchy.** Canonical child upsert/conflict, depth/tree/fanout guards and parent
      notices through ordinary mail rules.
- [ ] **MCP.** Only if required harness cannot use CLI; stateless, principal-scoped and
      same WriteId/readback semantics.

## L6 — Automation

- [ ] **Principals and rule activations.** Global/project scope; persisted canonical
      compiled definitions; definition digest distinct from activation id; A→B→A fresh
      activation; event-time subject; first-load high-water and exact old→new handoff.
- [ ] **Cursors/retention.** Active/retiring/replay interval cursors, restart drain from
      persisted old definition, deterministic-failure advance, explicit backlog skipped
      range and bounded retention floor.
- [ ] **Generic run/action journal.** `(kind,owner activation,trigger)` for event/timer/
      manual/goal; frozen targets, derived command ids, ordered states, durable rate
      limits and no unknown replay.
- [ ] **Timers.** Activation/time boundary, interval/cron/at, DST, latest catch-up and
      startup reconciliation, including action-only edit/revert.
- [ ] **Actions.** Scoped commands/mail/vars; exact Git OID/readback; named allowlisted
      webhooks; secret refs and narrow unknown writer hold.
- [ ] **Scripts.** Read-only scoped token, project-safe cwd, global∩project env validation
      before lookup, bounded streams, validate-all + atomic append, no unknown rerun.
- [ ] **Approvals.** Exact async request actions; deadline-bound non-dispatchable
      synchronous proposals; safe default on late/failed commit.
- [ ] **Goals.** Generic goal-run identity, generation/revision, integer micro-USD,
      journaled check, unknown input/turn/check blocking and startup reconciliation.
- [ ] **Minimal recipes.** Writer/reviewer, fetch, nightly, quota notification and
      checkpoint/rotation—no DSL.

**Feature-complete milestone:** freeze features; every crate scoped-Clippy clean; begin
one complete qualification campaign.

## L7 — Qualification and repair

### Core, SQLite and API

- [ ] execute every documented migration/DDL with foreign keys; CHECK/FK/UNIQUE rows for
      generation-0/live/detach/terminal/token-version and source-family ownership;
- [ ] external receipt vs internal derived command; selector linked children; command
      retention longer/shorter than idempotency window; dependency-order pruning/floor;
- [ ] A concrete/fanout retry with changed wait/display options returns same effect;
      payload/principal mismatch conflicts; old absent ID expires;
- [ ] id bounds/counter exhaustion/root allocation/derived collision/canonical subject;
- [ ] source open lost reply, open digest mismatch, family conflict/handoff, event and
      process lost ack, sequence stale/hole/vendor duplicate, source-history pruning and
      safe reclamation;
- [ ] routing-key restore/mismatch/recovery; child/reattach lost reply returns same token
      version; terminal/detach revocation;
- [ ] linked launch/resume input at every failure point; async inbox association,
      coalesced inputs, unknown terminal and constrained resolution;
- [ ] selector 0/1/10,000/10,001 targets, no partial admission, paged children and
      history gap after link pruning;
- [ ] Store commit failure/crash, WAL pinned reader/hard limit and replay history gap;
- [ ] drain/force/detach/degraded abandon/unknown blockers.

### Recovery, automation and coordination

- [ ] A→B→A event rule while first A drains; unique activations and persisted old
      definitions survive restart; exact H boundary has no overlap/gap;
- [ ] invalid placeholder/action/authority/fanout creates one failed run and next event
      proceeds; Store failure does not advance;
- [ ] timer action/trigger edit and A→B→A at due boundary exactly once;
- [ ] generic Goal run journal, durable rate limits across restart, no fake rule fields;
- [ ] global/project/profile/extends/account/proxy/pass-env/script/webhook escape negatives;
- [ ] timer/goal/mail/workspace/launch/provider startup reconciliation;
- [ ] every action/check/script crash phase and no external replay;
- [ ] mail fanout limit, busy with/without inject, notice/full, lost inbox/ack and one
      open batch; edit attribution/turn results across retention/restart.

### Plugins, platform and live paths

- [ ] ready vs initial, exact bindings, stable source open/close, bounded spool,
      child registration parent conflict/token retry, unsupported-before-I/O;
- [ ] OS enumeration + plugin registration of same PID publishes one start; shared
      service PID cannot become Agent custody; service stop isolation;
- [ ] Windows Job no-breakaway/daemon death;
- [ ] Linux cgroup+guardian failure/restart; degraded fallback rejects exact force and
      reports possible orphan;
- [ ] workspace base-ref movement after admission, Git/setup lost replies/readback/remove;
- [ ] OpenCode two-session environment/token/Git isolation and isolated stop;
- [ ] compact live run for every required harness/proxy.

### Security, load and retention

- [ ] live peer connects before process projection: Job/cgroup/PGID classification blocks
      no-token owner; ambiguous membership rejects;
- [ ] minimal env/inherit warning; routing key/token hash/pattern; short secret rejection;
      source-spool/output masking; private data/IPC/symlink/reparse/traversal/device;
- [ ] 10,000 fake agents + 2,000 persisted events/s for one hour; bounded source epochs,
      fanout pages, WAL/RSS and responsive reads with no persisted loss;
- [ ] active/retiring activations, receipts/FKs, source watermarks, turns/edits/mail under
      long retention combinations;
- [ ] drain 1,000 busy fake agents without new automation or lost results.

**Exit:** qualification green on Windows and supported Linux paths; compact current live
runs; every discovered defect repaired in code/spec. Only then is v2 ready.

## L8/L9

Port non-Rust plugins only for measured benefit; delete temporary duplicate routes and
keep budgets honest. Remote access, multi-host, GUI and Memory OS require new owner
decisions.
