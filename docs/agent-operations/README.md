# Agent Operations — Manager-Owned Automation

Revision 4 · 2026-10-03 · reviewed against main `504199d14135c030ad3951a3c5023a098a3d03f0`.

**Status: proposed Rust implementation and documentation contract, not shipped functionality.** This revision corrects the existing six PR #23 documents in place. PR #22 and the user's installation are unchanged.

## Product rule

**Management is manual by default. A manager may enable whichever automations help with their work. Those automations execute on that manager's behalf.**

There is no application-wide choice between manual, assisted and delegated modes. Manual commands and enabled automations coexist. With no automations enabled, the manager chooses every step. Enabling an audit handoff does not enable distribution, repair or publication.

```text
manager issues a command -----------------------+
                                               |
manager enables an automation                  v
  -> matching event/time -> act for manager -> same Rust action handler
                                               |
                                      retained Operation/result
```

The manager can configure, enable, edit or disable their automations through one small configuration interface. No additional Root approval or special automation grant is required for actions already permitted to that manager. Actions the manager cannot perform directly remain unavailable through automation.

Every automatic invocation records the owning manager, the automation and its triggering event, and the service that executed it. This is explicit attribution, not token sharing, a forged human GitHub author or a second manager identity.

## Read only what the change needs

- [Configuration](configuration.md): per-automation enablement, manager ownership, models/runtimes and the MCP editing path.
- [Architecture](architecture.md): Rust services, on-behalf authorization, observation, hooks, scheduler, Goal, scripts and recovery.
- [Delivery](delivery.md): manual commands and optional distribution, audit, repair and GitHub effects.
- [Donor map](donor-map.md): source evidence, useful existing components and limits of reuse.
- [Implementation](implementation.md): O1–O11 production paths and qualification cases.

These files form one contract. No superseding appendix is required. PR #22 supplies Participant identities, peer coordination, watches, launcher and deferred MCP concepts; implement each once.

## What the manager sees

A list of their automations, each with `enabled`, scope, trigger/actions, preferred profiles, last/next invocation and any concrete problem. The dashboard may say "Manual control; two automations enabled". It does not require a global mode transition to use a reminder or cron job.

Examples of independent choices:

- Notify an auditor when a verified commit is observed, without starting the auditor.
- Assign an auditor when an immutable submission is ready, leaving repair and publication manual.
- Distribute a selected queue using chosen runtimes, while the manager performs all reviews.
- Run a complete configured review/repair/publication path within the manager's existing rights.

Observation, streams, peer messages, requested reminders, ordinary work and manual commands remain usable whether zero or many automations are enabled. Agent count does not change these settings.

## Boundaries that still matter

**Rust internals:** host, Store, authorization, adapters, monitoring, GitHub client, hooks, distribution/review, scheduler, Goal, MCP/gateway, configuration and script runner are Rust. Python/PowerShell are optional external extensions. Existing owned non-Rust bridges are migration inputs, not the target architecture. Vendor executables remain external products.

**No software-version pins:** no prescribed old crate, fixed CLI/model release or hash-named executable path. Use maintained libraries, compatible dependency requirements and installed protocol/capability checks. Candidate SHAs, configuration revisions and the bytes of a completed invocation identify evidence; they do not freeze future software.

**One manager, one mutable worktree and one in-flight product submission.** Writers receive non-overlapping work within that assignment; they do not independently run Cargo, publish or accept their own work. Reviewers read the exact candidate.

**Same action, same safeguards:** manual and automatic requests share identity, permissions, candidate verification, resource ownership and effect deduplication. An enabled flag cannot bypass a missing publication right, supply reviewer evidence or make an uncertain push safe to repeat.

**Durable settings, not session tricks:** enabled automations belong to a stable manager identity and persist across client disconnect/restart. Recovery checks ownership and unfinished effects before continuing. Disabling stops future starts, not already running agents or remote operations. No mandatory re-enable ceremony on every host restart.

## Existing source anchors

| Unit | Current behavior | Required integration |
|---|---|---|
| `src/store/submissions.rs::reserve/finish` | Queued admission then applied `task.submission` | Review only a retained applied submission. |
| `src/store/submissions.rs::request_changes` | GM/operator guard; exact feedback and mail, no native input | Implement scoped manager feedback rights explicitly; automation uses those same rights. Repair delivery is a separate action. |
| `src/policy.rs` | Compiled policy edition and digest | Keep historical Attempts readable while adopting the accepted workflow changes. |
| `src/scheduler.rs`, `src/store/schedules.rs` | Configured once/interval CheckRuns and retained receipts | Extend the existing scheduler, preserving prior occurrences. |
| `src/mcp/subscriptions.rs` | Committed-fact polling with lag/resync | Shared Rust projector and separate native content streams. |
| `docs/forge-publication.md` | Accepted candidate, non-force push, unknown-effect readback | Reuse for both callers; review upload and PR merge have distinct effects. |

When protected manager capabilities are added, update Owner Decisions and code together. An automation never acquires a capability merely because the Rust service can reach an internal function. Preserve current GM/epoch checks where they apply.

## Scope and privacy

This PR edits documentation only; it activates no automation and runs no model, script, hook, installer, scheduler or forge effect. Static review is not runtime qualification. Real deployment endpoints, credentials, infrastructure identifiers and private paths remain local; examples use placeholders.
