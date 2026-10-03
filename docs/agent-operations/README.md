# Agent Operations — Monitoring, Hooks and Server Automation

Revision 1 · researched 2026-10-03 · source baseline `35e499ae73b622d873c44873f6993ee3fcbea87b`.

**Status: proposed product extension, not implemented or live-qualified by this PR.** This is a separate program from [PR #22](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/22); no changes to that PR, native services, local configuration or credentials are included.

## Decision

ELIOT owns continuous observation and authorized automation even when a manager model, desktop UI or MCP client is disconnected. A manager assigns a work pool and policy once; ordinary observation, reminders, approved scripts and eligible scheduled actions do not need repeated manager prompts.

```text
GitHub / Git / native runtime streams / supported hooks / OS observations
                                 |
                   validate, correlate, record
                                 |
           existing ELIOT Store + compact live-stream cache
                     /                         \
   dashboard, streams, attention       event/time/goal evaluation
           |                                     |
    MCP / CLI / optional UI           one authorized action admission
                                                 |
                                  existing durable Operations
                                     /           |          \
                              native Rust    script runner   RuntimePort
```

No second Task store, external broker, provider loop or general workflow language is required. New durable automation indexes belong to the existing SQLite database. High-volume text deltas do not become one database row per token.

## Four documents; one contract

- [Architecture and contracts](architecture.md): monitoring, streams, GitHub, hooks, actions, cron, server Goal, scripts, roles and MCP.
- [Donor map](donor-map.md): exact inspected sources, reuse units, version/MSRV constraints and limits of the evidence.
- [Implementation plan](implementation.md): complete vertical slices, source ownership, integration dependencies, migration and qualification.

These documents complement the canonical architecture and [Owner Decisions](../owner-decisions.md). They do not create another precedence ladder of retrospective amendments. Changes to existing policy are enumerated below; other policy remains unchanged.

## Requirement coverage

| Requested capability | Product decision |
|---|---|
| Dashboard without questioning agents | One host-owned observation pipeline; native events first, bounded shared read-only reconciliation where needed; no model status prompts. |
| Read outputs/reasoning | On-demand scoped live/history stream. Only text, reasoning content or summaries deliberately exposed by the native API; unavailable fields remain unavailable. |
| Hooks wherever supported | Adapter capability/install matrix; native veto hooks, observational hooks and wrapper events have different guarantees. |
| Server cron/scheduler/Goal/reminders | Extend the existing persistent scheduler. Independent server Goal references existing Tasks and completion evidence. Reminders reuse one watch/notification service. |
| PowerShell/Python and Rust actions | Versioned named script registry plus owned process runner; core notify/check/publish/merge actions remain typed Rust operations. |
| Commit -> auditor | Exact Git commit observation -> one durable audit notice; optional review dispatch uses a standing manager grant and concurrency limit. |
| Roles and custom roles | Versioned capability bundles with object scope and bounded delegation. Multiple auditors are supported; current GM designation is not a role-name string. |
| Deferred MCP | Keep the small core and grouped catalog from #22. New domains are discoverable on demand, never globally eager. |

## Baseline: preserve versus extend

The following are source observations, not claims that the new system is already present.

| Existing source | What is already usable | Gap this program closes |
|---|---|---|
| [`src/scheduler.rs`](../../src/scheduler.rs), [`docs/schedules.md`](../schedules.md) | Persistent one-shot/interval CheckRun scheduling, latest-only catch-up, Store admission and retained receipts | Cron/timezones, dynamic definitions, script/native action kinds and larger indexed registry |
| [`src/mcp/subscriptions.rs`](../../src/mcp/subscriptions.rs) | Bounded committed-fact subscriptions, lag notification and authoritative resync | Currently per-subscription polling; no native live-token stream or shared fleet projector |
| [`modules/command/mod/eliot-command.ts`](../../modules/command/mod/eliot-command.ts) | Existing native ModApi event listener and exact control journal | No universal hooks registry; streaming partials intentionally excluded; synchronous journaling/whole-inbox reads need bounded integration |
| [`modules/muse/observe.mjs`](../../modules/muse/observe.mjs) | Durability/death facts and explicit unfilled gap observation | Do not claim the SDK GapFiller is already active on this compact bridge path |
| [`modules/claude/README.md`](../../modules/claude/README.md) | SDK-owned input and compact stream/family mapping | Full live stream, hook installation and several native controls are not wired by that mapping |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted-candidate, non-force publication; owned process tree; uncertain-effect readback | PR creation/merge and GitHub work-pool synchronization are separate work |
| [`src/mcp/profiles.rs`](../../src/mcp/profiles.rs), [`Cargo.toml`](../../Cargo.toml) | Closed MCP allowlists; Tokio/RMCP/SQLite and native adapters | Custom role definitions and the deferred catalog must remain distinct from permission |

The older module-contract status paragraphs are not sufficient inventories: some describe earlier slices. Implementation status must be checked against these exact source units and their active callers.

## Explicit policy extension, not silent reinterpretation

1. Owner Decisions §3 intentionally forbids an automatic OpenCode re-prompt loop in 0.1. This proposal adds a separately named **server Goal** with explicit standing execution grants. It does not change the meaning of `agent.goal`, replay unknown input, or infer authority from peer mail.
2. Owner Decisions §4 and `schedules.md` intentionally limit the first scheduler to configured CheckRuns. The new version adds cron and other typed actions while preserving old schedule IDs, receipts and catch-up semantics.
3. Module installation remains operator-controlled. Hook setup edits require an explicit installation plan, preserve existing hooks and may not silently restart a shared service. An already delegated project-local hook/script configuration may be managed within that delegation.
4. Script authors may create, revise, activate and run scripts within an existing grant without routing every edit to Root. Wider OS trust, credentials, network access, global hooks or protected effects require the corresponding authority.
5. Peer communication still does not start work. A server rule or Goal may start authorized work under a **standing manager grant**; that is a different, auditable cause, not an exception hidden inside messaging.
6. One worktree per manager and exact-candidate acceptance remain. The new runner does not create a worktree per tiny script or share one mutable candidate across competing managers.

The implementation must update the named policy sections in the same delivery as the new behavior. Until then, existing behavior remains authoritative.

## What gets reused

Keep the existing SDKs, RuntimePort, Store receipts, artifact/redaction layer, process ownership and CheckRunner. Use a complete cron-expression library rather than writing a parser. Use a complete file-event library and a process-metrics library as sensors, not ownership authorities. Learn schedule overlap from Temporal, script versions/grants from Windmill and bounded recipe capture from Goose without importing their independent runtimes/databases. Exact selection and limitations are in the donor map.

## Deployment and evidence boundary

ELIOT must run as a locally installed long-lived host for server automation to survive client exit. This PR neither installs a service nor changes startup settings. Recovery after an actual machine reboot is tested later on the owner's machine.

No live models, scripts, schedulers, hooks, GitHub webhooks or forge effects were run for this design. Library compatibility is a researched candidate, not a build result. Keep states separate: `documented`, `source_reviewed`, `implemented`, `fixture_checked`, `live_qualified`.

Real deployment domains, tunnel/account identifiers, local private paths and secrets are local setup inputs. Repository examples use `YOUR_DOMAIN`, `mcp.example.com`, logical project handles and secret references only.
