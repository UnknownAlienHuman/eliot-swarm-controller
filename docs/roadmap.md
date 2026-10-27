# Roadmap

Build connected vertical layers. During L0–L6 write the real path first and run the
smallest compile/lint gate. Do not stop every partial layer to build a broad test
campaign. After the connected local product exists, L7 builds and runs the complete
deterministic, crash/restart, conformance, live, platform and load qualification.

Each task normally owns one manager worktree, branch and PR. Tick its box in the PR that
lands the connected implementation. This file is the only status tracker.

Implementation rules: [AGENTS.md](../AGENTS.md). Exact donor mechanisms:
[reference/donors](reference/donors.md).

## Implementation gate

```text
cargo fmt --check
cargo clippy -p <changed-package> --lib --bins -- -D warnings
```

Add another package only when the changed contract must compile together. A tiny
regression is allowed only where review cannot establish one critical local contract.
No model calls in CI. Recordings and named qualification cases may be collected while
implementing a harness, but the complete suite runs in L7.

## Code budgets

Production Rust lines per crate, checked by `just loc` against `budgets.toml` after the
skeleton exists.

| Crate | Budget |
| --- | ---: |
| `eliot-core` | 3,000 |
| `eliot-store` | 2,500 |
| `eliot-daemon` | 10,000 |
| `eliot-plugin` | 1,000 |
| `eliot` binary | 3,000 |
| each plugin | 3,000 |

A budget increase needs one concrete current reason. v1 reached roughly 533,000 Rust
lines and one 9,105-line file; future possibilities are not reasons.

---

## L0 — Archive and minimal workspace

- [ ] **L0.1 Archive/delete v1.** Create `v1-final` at `dc6f70aa`, then delete the v1
      implementation, migrations, vendor/config, old tests/workflows/tooling,
      `docs/owner-decisions.md`, old `Justfile` and notices. Keep current v2 docs/root
      metadata.
- [ ] **L0.2 Skeleton.** Rust 2024 resolver 3, pinned stable toolchain, one `eliot`
      binary with `--version`, workspace lints, budgets and `Justfile` targets for fmt,
      scoped Clippy, loc and deferred qualification.
- [ ] **L0.3 Minimal CI.** Windows and Ubuntu: Markdown/link/whitespace, fmt,
      changed-package Clippy and loc. Markdown-only changes skip Cargo. No model/live
      call and no schema defined by Markdown.

**Layer result:** clean minimal workspace; no v1 route is extended.

## L1 — Core, Store, bus and fake vertical path

Goal: one complete local command/event path with no real provider.
Specs: [core](spec/core.md), [API](spec/api.md),
[plugin protocol](spec/plugin-protocol.md), [configuration](spec/configuration.md).

- [ ] **L1.1 Core model.** Signed-64 ids, stable Agent/native subject, one monotonic
      generation fence, exact turn/request ids, verbs, commands, selectors, event-time
      subject and pure reducer. No binding revisions or Task/workflow model.
- [ ] **L1.2 Store.** Numbered migrations, L1 projections, one writer/group commit,
      read pool and query paths. Persist command principal/method/payload identity,
      source owners/state, turns and current Agent state. Generation-changing dispatch
      advances the fence and commits the one target-generation token before native I/O.
      Commit failure blocks new writes and never repeats the external cause.
- [ ] **L1.3 Bus/source intake.** Persist-before-broadcast; bounded live fan-out;
      subscribe live-buffer → persisted watermark replay → live. Plugin native-record
      batch commits events, daemon-computed digests, source state/cursor and reducer
      atomically and is acknowledged afterward. One family owner; exclusive cursor or
      stable `gap_id`; a repeated native record is wholly new or an exact duplicate;
      mixed replay or same id/different payload/cursor is integrity failure.
- [ ] **L1.4 Daemon/IPC/principals.** Data-dir lock, boot/PID identity, current-user IPC,
      background/foreground daemon, status/drain/force. Owner CLI automatically uses
      private owner token; Agent/run/plugin principals are scoped; missing identity never
      becomes owner.
- [ ] **L1.5 Configuration.** Global profiles/routes, narrowing project overlays,
      minimal environment, accounts/proxies, bounded frozen note/setup/script inputs and
      strict keys. Existing Agent snapshots do not silently follow reloads.
- [ ] **L1.6 Plugin host/custody.** Typed framing/deadlines, per-Agent plugin generation
      and shared-service mode only after exact isolation. Source plan/state and target
      generation token/environment are committed before native I/O; event batch commit
      ack, idempotent child registration, attach/snapshot/readback, Windows Job and
      honest degraded Unix process group.
- [ ] **L1.7 Router/readiness lanes.** Immutable admission, expected generation,
      linked first input, exact turn/request controls, FIFO future sends, unknown input/
      turn/transition gates, selectors/pagination, idempotent external ids,
      profile/account/proxy launch queue and no replay after restart.
- [ ] **L1.8 Fake plugin.** Deterministic isolated sessions supporting session-first and
      input-created starts, exact generation/turn/request rejection, atomic
      native-record batches, cursor/gap/source-state readback, requests/children,
      crash/hang/exit and snapshot readback.
- [ ] **L1.9 CLI.** Daemon/status, launch/list/show/watch/events, core verbs,
      wait/result/turn, command children/cancel/resolve, JSON output, stable ids and
      documented exit codes.
- [ ] **L1.10 Identity/secrets.** Agent token lifecycle by generation, dispatch-time
      rotation/revocation, no independent token for routing-disabled children,
      known-value masking and durable payload rejection.
- [ ] **L1.11 Qualification entrypoints.** Minimal recording/replay and `plugin-test`
      command surface only; complete cases are implemented/run in L7.

**Layer result:** fake Agent can launch, receive linked input, run/interrupt a turn,
wait on an exact request, stop and resume without stale-generation effects. Scoped
Clippy is green. This is an implementation milestone, not a qualification claim.

## L2 — First real harness: Codex + OpenCodex route

Goal: one useful real harness through its native app-server.

- [ ] **L2.1 Installed schema/recording transport.** Generate schema from the owner's
      installed Codex; add bounded sanitized record/replay transport and record only
      scenarios required while implementing current verbs.
- [ ] **L2.2 Session and turns.** One plugin + app-server per controlled Agent
      generation; arm native source before `thread/start`; exact target-generation
      environment/effective settings; ready before turn; linked `turn/start`; final
      text/usage.
- [ ] **L2.3 Control/readback.** Send, exact-turn steer/inject/interrupt, stop with
      native Goal clear, resume as new generation, configure application point. Every
      lost response is exact readback or unknown; no repeated effect.
- [ ] **L2.4 Requests.** Exact approval/question ids and approve/deny/answer.
- [ ] **L2.5 Native Goal.** Set/pause/resume/clear/status, generation binding and stop
      cleanup.
- [ ] **L2.6 Children.** Canonical subagent subjects, parent generation, idempotent
      registration, honest environment/identity limitations and guard-exceeded
      visibility.
- [ ] **L2.7 Account/proxy.** Native permissions, persisted capacity/holds/probe,
      managed/adopted OpenCodex, effective endpoint/billing readback.
- [ ] **L2.8 Useful manual path.** One compact direct and proxy run demonstrates real
      tool/write/control/stop. Save recording/output for L7; do not launch a broad
      campaign.

**Layer result:** Codex performs useful work through Eliot; unverified installed-protocol
facts remain explicitly named until L7.

## L3 — Monitoring and workspaces

Specs: [monitoring](spec/monitoring.md).

- [ ] **L3.1 Process monitor.** Boot/PID/birth identity, Agent vs service custody,
      OS-authoritative lifecycle/stats and source-drain-before-loss. Exact Windows,
      degraded Unix reporting.
- [ ] **L3.2 Workspaces.** Frozen local base OID, one durable preparation row with
      `creating|setup_ready|setup_running|ready|unknown|failed`, exact worktree readback,
      frozen setup inputs, no rerun after unknown setup and explicit clean-only removal.
- [ ] **L3.3 Git/filesystem/edit projection.** Read-only Git commands without hooks,
      fsmonitor, external diff, textconv or unapproved/custom remote helpers; watcher
      debounce, slow backoff, frozen-base diff, generation-bound `agent_edits` and
      bounded progress fingerprint.
- [ ] **L3.4 Discovery/source handoff.** Paged Codex discovery/observe attach,
      authoritative stream vs transcript ownership, opaque cursor recovery or stable
      explicit gap, canonical Agent row.
- [ ] **L3.5 Liveness/restart.** Progress-only stall, source freshness, provider holds
      and level-based reconciliation keys for Agents/workspaces/services/Git.
- [ ] **L3.6 Views.** Generation, turn/input gate, source owner/state/cursor/gap,
      custody, provider hold, workspace phase, requested/effective route and Git/edit
      facts.

**Layer result:** real Codex process/native/Git facts are visible without monitors
changing work or guessing completion.

## L4 — Remaining required harnesses

One task per harness. Use the exact native mechanism and one plugin route. Each plugin
states process mode, source-family ownership, cursor/gap behavior, child environment and
unsupported verbs. Collect recordings needed for final conformance.

- [ ] **L4.1 Hook shim.** Build only for a proven fact/decision absent from the primary
      stream. Session-scoped config, generation/request/deadline, safe fallback and no
      duplicate family publication.
- [ ] **L4.2 Claude Code.** Agent SDK first; per-Agent plugin; exact callbacks and source
      ownership.
- [ ] **L4.3 Muse.** MSP/official SDK, exact session/turn/request ids and native Goal.
- [ ] **L4.4 OpenCode.** Shared service only after exact session demultiplex,
      environment attribution, readback and isolated stop are proven; otherwise one
      server per Agent or observe-only.
- [ ] **L4.5 Antigravity.** Warm stream-json process per Agent; no invented in-stream
      control.
- [ ] **L4.6 ACP generic.** Native ACP JSON-RPC and only negotiated capabilities.
- [ ] **L4.7 Others.** Command Code, Kilo, Zed only on concrete need.

**Layer result:** required harnesses perform their documented useful path; unsupported
semantics fail explicitly.

## L5 — Coordination

Specs: [coordination](spec/coordination.md).

- [ ] **L5.1 Mailbox.** Delivery id, sender/recipient generation and content digest;
      exact recipients; queue vs full-content command authority; generic routed notice;
      exact-id inbox acknowledgement.
- [ ] **L5.2 Shared variables.** Project/tree/global scopes, CAS and watch.
- [ ] **L5.3 Who/claims.** Git facts plus durable current-generation edits, history and
      generation-bound advisory claims/conflicts.
- [ ] **L5.4 Hierarchy.** Tree selectors, parent notices and fanout guards without
      erasing already-created native children.
- [ ] **L5.5 MCP surface.** Build only if a required harness cannot use shell; stateless
      bridge, caller principal/control enforced by daemon.

**Layer result:** two different real harnesses exchange one addressed message, agree on
one CAS variable and see each other's file area without manager relay or broadcast
chatter.

## L6 — Automation and Goals

Specs: [automation](spec/automation.md).

- [ ] **L6.1 Rules.** Strict TOML, committed-event cursor, event-time subject,
      match/render guardrails, referenced-file digest watching and current+retiring
      definition handoff at high-water H.
- [ ] **L6.2 Timers.** Anchored interval, cron/DST, one-shot, latest-only catch-up and
      durable schedule revision/key.
- [ ] **L6.3 Actions as commands.** Agent, Git, mail, var, notify and emit commands in
      order with deterministic ids, generation binding and verb-specific readback. Git
      fetch and push endpoint sets are frozen/validated separately; push readback uses
      every effective `pushurl`, never the fetch URL by assumption.
- [ ] **L6.4 Scripts.** Frozen file bytes/digest, scoped run token, bounded streams,
      validate-all then atomically create stdout actions; unknown script never reruns.
- [ ] **L6.5 Approval policy.** Exact async request commands and deadline-bound
      synchronous hook response with no late queued approval.
- [ ] **L6.6 Goal loop.** Native/Eliot modes, generation/revision/round key, bounded
      check/budget/no-progress, read-only fingerprint and unknown input/turn blocking.
- [ ] **L6.7 Minimal recipes.** Writer/reviewer, fetch, nightly, quota notification and
      rotation—ordinary readable rules/scripts, no DSL.

**Feature-complete milestone:** freeze feature work; every changed crate scoped-Clippy
clean. Begin the complete qualification campaign.

## L7 — Complete qualification and repair

Build/run the tests now against the connected product. Repair root causes in code/spec;
do not add proof documents or compatibility wrappers.

### L7.1 Core, Store and fake E2E

- [ ] reducer table including stale generation/turn/request and duplicate inputs;
- [ ] migrations/reopen, commit failure, group commit, source-owner/state/cursor/gap/
      daemon-digest and idempotent write identity;
- [ ] bus replay/live boundary, lag/history gap and all-new/exact-duplicate native-record
      batch atomicity;
- [ ] CLI fake path: linked launch/input, requests, FIFO/readiness lanes, unknown input/
      turn/transition gate, dispatch-time target token, stop/resume generation,
      selectors and capacity;
- [ ] daemon/plugin crash at each admission/commit/ack/readback boundary; no replay.

### L7.2 Plugin conformance and live harnesses

- [ ] one black-box suite for manifest, source plan before native I/O, generation fence,
      target environment/token, cursor/gap/state, exact turn/request, unknown non-retry,
      attach/readback, child registration and custody;
- [ ] replay current fixtures on supported platforms;
- [ ] one compact live run per required harness/proxy, cheapest useful route;
- [ ] requested vs effective route/model/permissions and child/source behavior.

### L7.3 Monitoring, coordination and automation

- [ ] process exit vs final source drain, adopted service isolation, workspace
      `setup_running` crash recovery and Git monitor noninterference;
- [ ] exact mail recipient/ack crash cases, vars CAS, who/claims/hierarchy;
- [ ] rule edit/high-water plus referenced-file change, deterministic failure cursor
      advance, timer/DST, persisted provider probe and every external action crash phase;
- [ ] fetch URL vs pushurl validation/readback, including rejected custom remote helper;
- [ ] Goal round/restart/generation/budget/no-progress/unknown gates.

### L7.4 Platform, security and recovery

- [ ] Windows Job/handles/paths/non-ASCII/long input/file locks;
- [ ] degraded Unix claims and optional exact Linux custody if required;
- [ ] current-user IPC/private files, missing-principal rejection, generation-token
      revocation, minimal env, project overlay/path escape/secret negatives;
- [ ] drain/force with unknown blockers and attached services never killed.

### L7.5 Load and retention

- [ ] CI-sized load each PR after suite exists; full owner run: 10,000 fake Agents and
      2,000 persisted events/s for one hour with bounded RSS/WAL and responsive reads;
- [ ] retention of events/commands/source state/turns/edits/mail without replay or lost
      current state;
- [ ] drain 1,000 busy fake Agents without new automation or lost result/request.

**Qualification exit:** complete suite green on Windows and supported Linux paths,
current compact live runs and every discovered defect retained as a regression. Only
then call v2 ready.

## L8 — Consolidation

- [ ] Port retained non-Rust plugins only for measured benefit behind the same protocol.
- [ ] Delete temporary duplicate routes/fixtures and recheck budgets.
- [ ] Split only proven boundaries, not future concepts.

## L9 — Later, owner decision only

Authenticated remote access, multi-host federation, GUI/TUI client and Eliot Memory OS
integration. Each requires a concrete use case and a new decision.
