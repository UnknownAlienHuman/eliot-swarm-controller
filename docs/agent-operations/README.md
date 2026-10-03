# Agent Operations — Manual Control and Opt-in Rust Automation

Revision 3 · reviewed 2026-10-03 · source baseline `504199d14135c030ad3951a3c5023a098a3d03f0`.

**Status: design and implementation contract; not implemented or live-qualified behavior.** This revision updates the six existing PR #23 documents together. PR #22 and the owner's installation are not changed.

## Product decision

**Manual management is the default and a complete product path. Automation is an optional tool explicitly configured and activated by an authorized manager.** Registering agents, importing a work pool, selecting a preset, granting permissions or increasing fleet size must not activate it.

A manager can work with one or several agents by selecting every task, launch, audit, correction, continuation and publication. The same manager can later delegate selected transitions for a larger pool without changing the work's identity or moving to another controller.

```text
                        one durable Rust control plane
                                      |
             +------------------------+-----------------------+
             |                                                |
    manual control (default)                       delegated transitions (opt-in)
    manager chooses each next step                 manager chooses scope and stages once
             |                                                |
             +---- same Task / Attempt / Operation handlers ---+
                                      |
                    exact candidate, audit and effect evidence
```

Observation is independent of unattended execution. Dashboard, native streams, bounded Git/GitHub reads, direct peer communication and authoritative result recording continue for configured sources in manual mode. They do not ask models for status or start work.

## Read only what the work needs

- [Configuration](configuration.md): manual default, manager activation, stage selection, pause/resume, runtime preferences and examples.
- [Architecture](architecture.md): Rust boundaries, shared admission, control/effect ordering, monitoring, hooks, cron, Goal, scripts and recovery.
- [Delivery](delivery.md): complete manual path, optional queue distribution, audit, repair and GitHub publication.
- [Donor map](donor-map.md): inspected source, official contracts and the limits of adopted ideas.
- [Implementation](implementation.md): O1–O11 ownership, integration order and acceptance scenarios.

These documents form one contract, not a chain of superseding appendices. Configuration owns control settings; Architecture owns execution guarantees; Delivery owns transitions. Source evidence never grants authority. PR #22 supplies Participant identities, communication, watches, launcher and deferred MCP concepts; implement each only once.

## Control contract at a glance

| State | Behavior |
|---|---|
| `manual` | No unattended execution. Managers issue exact one-shot commands; observations and peer collaboration remain available. |
| `assisted` | Manual execution plus bounded deterministic suggestions. Suggestions are not queued commands or extra model turns. |
| `delegated` | Only the manager-selected stages/definitions and scope execute automatically, within current grants and capacity. |
| Paused delegation | Stop future unattended effect starts; retain settings, pending work and evidence. Already started actions drain or report unknown. |

The mode never changes because of agent count, queue length, model output, a hook, a new plugin or a remembered grant. New projects have no delegated stages, enabled rules/schedules/Goals or automatic GitHub writes.

Saving configuration is not activation. A manager can prepare a `reviewed_delivery` definition, inspect its dry run, then explicitly enable selected stages. `manager_gate` controls the final publication decision only; it is not a synonym for fully manual operation.

Returning to manual does not kill agents, erase candidates, release occupied worktrees or reverse a sent push. The result identifies queued actions held, running effects, unresolved external continuations and what is safe to control manually. A local stop must not claim it cancelled a native Goal or GitHub auto-merge already accepted elsewhere.

## Non-negotiable boundaries

**Rust owns every internal subsystem:** host, Store, authorization, configuration, native adapters, process supervision, monitoring, GitHub client, queue/review routing, hooks, scheduler, Goal, reminders, MCP/gateway and script runner. Python and PowerShell are optional user extensions, not mandatory queue/push/recovery components. Existing owned non-Rust bridges are migration inputs; external vendor executables remain external products.

**No software-version pins.** Do not prescribe an obsolete crate, fixed CLI/model release, commit dependency or hash-named executable location. Use maintained Rust libraries, compatible requirements, installed-runtime discovery and actual protocol/capability checks. Observed versions, candidate SHAs and the bytes used by an admitted script are evidence, not restrictions on future software updates.

**One manager, one mutable worktree and one in-flight product submission.** Writers follow non-overlapping assignments in that worktree, do not run Cargo or change control policy, and cannot publish/accept their own work. Reviewers inspect immutable candidate evidence. The next task cannot mutate a candidate still in review.

**Permission is not activation.** A standing grant defines a ceiling; the manager's control decision selects what may run unattended. A configuration editor, script author, auditor or executor does not obtain activation rights merely by being an agent. Managers may delegate configuration preparation; changing effective control requires explicit management authority over that scope.

## Existing source facts to preserve

| Source | Existing behavior | Required extension |
|---|---|---|
| `src/store/submissions.rs::reserve/finish` | Queued `task.submit` followed by an applied `task.submission` result | Make applied submission available for manual review; dispatch automatically only if enabled. |
| `src/store/submissions.rs::request_changes` | GM/operator guard; exact-candidate feedback and mail, no native input | Narrow delegated disposition and separate repair dispatch; preserve the manual path and stale-review guards. |
| `src/policy.rs` | One compiled accepted policy edition and source digest | Preserve historical Attempt evidence when adding explicitly accepted policy editions. |
| `src/scheduler.rs`, `src/store/schedules.rs` | Configured once/interval CheckRuns and retained receipts | Extend the same Rust scheduler; new schedules default disabled and old receipts remain readable. |
| `src/mcp/subscriptions.rs` | Committed-fact polling with lag/resync | Shared source projector and separate native content streams, independent of automation mode. |
| `docs/forge-publication.md` | Accepted exact candidate, non-force publication and uncertain-effect readback | Same safety for manual/delegated execution; review upload and PR merge remain distinct operations. |
| `docs/owner-decisions.md` | Read-only reports do not change work; `new_work=disabled` drains admissions | Add a scoped automation control gate, not a replacement for host admission or current authority. |

## Policy changes when the implementation lands

Update the relevant Owner Decisions and code together: delegated review/acceptance rights where authorized; opt-in server Goal; dynamic schedules/actions; distinct review upload and publication; Rust integration and compatibility-based updates. Keep historic policy editions readable. Do not edit a documentation hash to manufacture authority.

Manual and delegated commands must share the same candidate, permission, resource-ownership and verification checks. Manual means the manager selects the action, not that protections or required audits are bypassed.

## Practical use

For a small team, use the normal manager core: inspect, launch one assignment, read streams, choose an auditor, review findings, return or publish. No delivery preset, cron, Goal, GitHub App installation or standing automation grant is required merely to run local manually assigned work.

For a larger team, prepare `reviewed_delivery`, choose writer/auditor profiles and explicitly activate selected transitions. Dispatch and review can be automatic while correction, acceptance and publication remain manual. Alternatively, eligible end-to-end delivery can be delegated inside a standing grant. Scripts are optional in either case.

`audited`, `accepted`, `uploaded`, `merged` and `published` are different facts. A GitHub label is only a projection. Local audit status can update in manual mode; a remote label/comment/write still needs an explicit command or enabled projection policy.

Invalid configuration retains the last valid definition and cannot reset a pause. Future runtime preferences do not restart active agents. Restart resumes unattended work only under the manager's explicit restart policy, not simply because a preset or enabled-looking file exists.

## Validation and privacy

This is a documentation-only PR. No model, script, native hook, scheduler, installer, service or forge action is run against the owner's projects. Source/API research and static example checks are not runtime qualification.

Real deployment endpoints, infrastructure identifiers, credentials and private paths remain local setup inputs. Repository examples use logical handles and placeholders. Historical scripts and deployment audits are evidence, not instructions to reproduce their non-Rust services or auto-enable their workflows.
