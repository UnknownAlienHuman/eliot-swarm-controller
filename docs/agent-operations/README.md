# Agent Operations — Rust Control Plane and Configurable Delivery

Revision 2 · researched 2026-10-03 · inspected main `35e499ae73b622d873c44873f6993ee3fcbea87b`.

**Status: design and implementation contract; not a claim of implemented or live-qualified functionality.** This revision replaces the first revision's fixed dependency recommendations and internal JS/Python bridge development plan. It updates PR #23 without modifying PR #22 or the owner's machine.

## Product decision

The manager provides an ordered pool of work and execution preferences. ELIOT distributes eligible work, observes execution without questioning models, sends complete candidates to auditors, routes actionable corrections back, and publishes successful work automatically or after an explicit manager gate.

```text
manager: work pool + roles + runtime preferences + delivery policy
                              |
                    Rust admission/distributor
                              |
           manager-owned Attempt and one mutable worktree
                              |
                completed immutable submission
                              |
                   assigned auditor(s)
                 /            |             \
       changes requested    audited       inconclusive
              |               |                |
       original owner    auto or GM gate   diagnosis/review
              |               |                |
       new submission    acceptance + publication
                              |
                    remote readback + bookkeeping
```

Peer consultation remains information, not assignment. Server dispatch is a distinct manager-authorized action. Ordinary handoffs do not need a fresh Root approval.

## Read only what the work needs

- [Architecture](architecture.md): Rust boundaries, monitoring/streams, hooks, actions, cron, Goal, scripts and durable recovery.
- [Delivery](delivery.md): queue distribution, reviewer contracts, repair, audited state, GitHub effects and publication.
- [Configuration](configuration.md): small agent-facing MCP configuration path, runtime/model preferences, updates and examples.
- [Donor map](donor-map.md): inspected source units, official API contracts, what to reuse and what not to inherit.
- [Implementation](implementation.md): source ownership, integration order and acceptance scenarios.

These are one program, not a chain of superseding amendments. Architecture owns service boundaries; Delivery owns delivery transitions; Configuration owns settings and public configuration methods. Evidence documents do not grant authority. PR #22 supplies the shared Participant, coordination, watch, launcher and deferred-catalog concepts; do not implement them twice.

## Non-negotiable boundaries

**Rust owns every internal subsystem.** Host, Store, scheduler, Goal evaluator, dispatch, monitoring, GitHub client, webhook ingress, hooks registry/helper, MCP gateway, configuration, review routing and script runner are Rust. Python and PowerShell are optional user extensions executed by the Rust runner. They are not mandatory startup, queue, review, push or recovery components. ELIOT-owned JavaScript/TypeScript daemons are not an alternative implementation of this requirement.

External native harnesses, Git and tunnel executables remain external products. ELIOT controls them through documented protocols and Rust adapters; it does not rewrite their model loops. Existing non-Rust bridges are migration inputs, not evidence that the Rust target is complete.

**No software-version pins in this program.** Do not require one old crate release, fixed CLI build, model revision, commit-based dependency or hash-named binary location. Use maintained Rust libraries, normal compatible dependency requirements, installed executable discovery and actual protocol/capability checks. Resolve preferred model/CLI choices from configuration. Source review dates and observed versions are evidence, not install restrictions.

A candidate SHA, configuration revision or the bytes used by an already-started script identify work that happened. They do not force future work to use an obsolete software version. Scripts normally follow their active definition at the next admission, and runtime preferences apply to new launches; active work is not silently rewritten.

**One manager, one mutable worktree and one in-flight product submission.** Writers can work in non-overlapping assignments within it. They do not independently run Cargo, change delivery policy or publish. Reviewers consume immutable source evidence. The manager prepares the next Issue without modifying a candidate under review.

## Current source facts that implementation must not overlook

| Source | Existing behavior | Required extension |
|---|---|---|
| `src/store/submissions.rs::reserve/finish` | `task.submit` has durable admission and a later applied `task.submission` result | Trigger review from the applied result, not from a queued request or the agent saying done |
| `src/store/submissions.rs::request_changes` | Requires current GM/operator and returns a durable mailbox message, not native input | Scoped delegated review disposition and a separately authorized repair handoff; merely exposing a reviewer MCP tool is insufficient |
| `src/policy.rs` | Recognizes one compiled owner-policy edition and document digest | Keep old Attempt evidence readable; add an authorized configurable workflow-policy binding rather than editing a digest to grant rights |
| `src/scheduler.rs`, `src/store/schedules.rs` | One-shot/interval CheckRuns, latest-only catch-up, retained receipts | Cron, dynamic actions and indexed definitions in the same scheduler |
| `src/mcp/subscriptions.rs` | Bounded committed-fact polling with lag/resync | Shared Rust projector plus separate native text stream; not polling each model |
| `docs/forge-publication.md` | Accepted exact candidate, non-force push and uncertain-effect readback | Delegated publication, optional review-branch upload and PR operations with distinct authority |
| Existing `modules/*` | Some controller adapters/mods use Python or JS/TS | Replace owned control/translation logic with Rust routes before claiming the new end-to-end path complete |

## Explicit policy changes required when behavior lands

Update [Owner Decisions](../owner-decisions.md) and the relevant implementation together, not by silently reinterpreting old text:

1. Sections 1 and 5: authorized workflow service may apply a scoped review disposition and, when separately delegated, accept/publish an eligible exact candidate. It does not become GM. Human/GM-only operations remain protected.
2. Section 3: server Goal may advance the assigned pool under a standing grant; native `agent.goal` retains its own meaning and cannot compete with server continuation.
3. Section 4 and [Schedules](../schedules.md): extend configured CheckRuns to agent-configurable cron and registered actions; preserve old schedule receipts.
4. Section 6 and [Forge publication](../forge-publication.md): distinguish an optional review-branch upload from accepted publication and from merge. Preserve existing non-force and readback guarantees.
5. Module contract/update instructions: new owned integration logic is Rust and compatibility-based, not a prescribed frozen SDK package. Preserve existing live bindings until safe handover; no automatic service restart.

The new direction does not authorize repository-protection changes, arbitrary credential access, heuristic killing, implicit model spending outside a grant or broad test workflows during code construction.

## Practical defaults

Use the `reviewed_delivery` preset. Agent setup selects a work pool, writer/auditor profiles, concurrency and either `auto_after_audit` or `manager_gate`. Built-in Rust stages handle the ordinary path; scripts and custom event rules are optional.

`audited` is an exact-candidate review fact. A GitHub label is only its display projection. `audited`, `accepted`, `uploaded`, `merged` and `published` must never be interchangeable statuses.

Invalid configuration retains the last valid configuration. Unavailable routes delay only relevant work; invalid source data cannot stop the entire fleet. A changed preferred model does not kill an existing session. No free-text comment, mention or reminder starts a handoff by itself.

## Validation and privacy

This PR changes documentation only. It runs no models, scripts, native hooks, schedulers, installers or forge effects against a project. Source/API research does not establish performance or live compatibility.

Real deployment endpoints, tunnel/account identifiers, credentials and private filesystem paths remain local setup inputs. Examples use logical handles and placeholders. The existing local gateway audit is operating evidence, not permission to publish its deployment details or reproduce its JS/PM2 stack as ELIOT's internal architecture.
