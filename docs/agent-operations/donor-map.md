# Donor Map — Rust Operations and Manager-Owned Automation

Revision 10 · Reduction review 2026-10-18. Waku, Paseo loading and Muse Connection/Session were rechecked at the recorded commits; Cargo metadata documentation was checked. Other observations retain their dates.

`CODE` means an inspected source behavior; `DOC` means official/library documentation; `OWNER_AUDIT` means supplied operating evidence; `DESIGN` means an ELIOT proposal. Earlier inspections are not new live qualification. Commit/blob identities locate evidence, not required installed versions.

## 1. Product decisions are not inferred from donors

**DESIGN, owner direction:** management is manual by default. Each manager enables the helpers they want, acting on their behalf inside existing rights. No global operating modes, extra stage-activation ledger or Root approval for already permitted actions. Rust owns the control plane and durable work; native SDK drivers follow section 9. Python/PowerShell user scripts are optional external extensions.

The operations program now also includes [Modular Runtime](modularity.md) and [Observability](observability.md), requested on 2026-10-05. They define build/process and diagnostic boundaries, not an extra workflow or permission engine. Source study does not prove the proposed combined system works; earlier donor observations below remain historical unless explicitly rechecked.

## 2. Earlier ELIOT evidence

Earlier ELIOT inspection used `504199d14135c030ad3951a3c5023a098a3d03f0`.
Use [source findings](refactoring.md) for current call sites and corrected audit
claims at `906f797`. [Architecture](architecture.md) owns the product manifesto;
[implementation](implementation.md) owns source changes and donor choices;
[delivery](delivery.md) owns work transitions. Earlier owner-policy editions are
historical decisions, not runtime rights.

## 3. Library reuse at a required boundary

Use a library when its API replaces a needed owned mechanism. Keep current rmcp/ACP/croner/Atlas integrations; select further replacements by the caller and total owned code removed. Several listed crates are not direct dependencies, and `vendor/cccc-windows-process` does not exist. Current GitHub code calls `gh`; octocrab is optional. The table records reuse boundaries, not required integrations.

| Component | Maintained source | Mechanism supplied | ELIOT responsibility | Reinvention to remove |
|---|---|---|---|---|
| `rmcp` | [API](https://docs.rs/rmcp/latest/rmcp/); actual callers: `crates/swarm-mcp/src/mcp/mod.rs`, `crates/swarm-mcp/src/mcp/catalog.rs`, `crates/swarm-gateway/src/gateway.rs` | MCP protocol types, server transport and tool routing. | Role/profile filtering, scoped credentials, Store admission and bounded I/O. The existing non-MCP `ModuleLink` protocol remains its own contract. | Reimplementing MCP framing or routing beside rmcp. |
| `agent-client-protocol` | [API](https://docs.rs/agent-client-protocol/latest/agent_client_protocol/); actual caller: `crates/swarm-adapter-command/src/adapter/acp.rs` | Native ACP types, session/turn envelopes and content blocks for the ACP adapter. | Binding generation, digest verification and Store receipt mapping. Zed eval-cli stays a sessionless CLI adapter unless its selected native protocol changes. | Ad-hoc parsing of ACP wire messages beside the crate. |
| `octocrab` | [API](https://docs.rs/octocrab/latest/octocrab/), [builder](https://docs.rs/octocrab/latest/octocrab/struct.OctocrabBuilder.html); **not an Eliot dependency** | Potential Rust GitHub API client, if replacing a retained transport is worthwhile. | Existing ownership still covers credentials/account selection, action rights, durable intent, bounded safe reads and uncertain-write readback. | If adopted, remove the replaced `gh` API path; do not add a parallel client or treat octocrab as required core work. |
| `croner` | [API](https://docs.rs/croner/latest/croner/); actual caller: `crates/swarm-kernel-host/src/scheduler/calendar.rs` | Cron expression parsing and occurrence evaluation with timezone support. | Manager enabled choice, due identity, overlap/catch-up and persistence. | Handwritten cron parsers or per-rule sleep loops. |
| `sysinfo` | [API](https://docs.rs/sysinfo/latest/sysinfo/) | Selective cross-platform process/resource observation, parent-child tree refresh and CPU/RSS sampling (`swarm-process`, `swarm-observer`). | Verified birth/Job ownership and safe lifecycle decisions; metrics do not authorize kill. | Spawning `tasklist`/`ps` subprocesses or writing raw Win32/procfs metric pollers. |
| `notify` | [API](https://docs.rs/notify/latest/notify/) | Event-driven filesystem and parent-directory atomic-save observation (`config`, workspace hints). | Exact readback, revision validation and source-health gaps. | Busy-waiting `sleep` loops polling file `mtime`. |
| `tracing`, `tracing-subscriber`, `tracing-appender` | [`reload`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/reload/), [`non_blocking`](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/) | Structured spans/events, dynamic `reload` filter swaps without restart, and bounded `non_blocking` writer (`swarm-telemetry`, `swarm-observer`). | Byte-budget accounting, `atlas-redact` scrubbing prior to sink write, and scope expiry. | Custom logging frameworks or duplicate handmade ring queues. |
| `atlas-redact` | `vendor/atlas/crates/atlas-redact`; actual callers: `crates/swarm-kernel-host/src/redaction.rs` and `crates/swarm-adapter-claude/src/interactions.rs` | Stateless redaction of each supplied string/JSON value; it does not retain cross-chunk state or detect a secret assembled across chunk boundaries. | Apply it at each retained diagnostic/stream boundary where coverage is required; current call sites do not establish that all stream chunks and logs pass through it. | Regex-per-call ad-hoc secret stripping. Do not claim complete diagnostic coverage from the dependency alone. |
| `cccc-windows-process` | [Inspected complete crate](https://github.com/ChesterRa/cccc/tree/c270e276591879c6edb7d08af67bec7035585c7e/crates/cccc-windows-process) | Suspended-child creation, owned primary-thread handle, resume and cleanup-on-failed-bootstrap, integrated through `swarm-process`. | Job creation/assignment and family policy are outside this small crate. Preserve owner-first non-killing observation for active agent families and separate bounded-check termination. | Copying its suspended-launch mechanism; assuming the donor supplies the full Job/family subsystem. |
| `tokio` / `rusqlite` / `serde` | Project manifest and library documentation. | Async runtime, WAL relational storage (1 writer + pooled read-only connections), and typed serialization. | Current domain schema, authorized snapshots and single Store authority. | Storing collections as JSON blobs in `meta` or serializing all reads through the single writer thread. |

No old release, fixed CLI/model equality or callback-time package installation is prescribed. Ordinary compatible requirements and recorded build resolution are not a policy freezing future software. [Cargo's dependency documentation](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html) distinguishes dependency requirements from a particular resolved build.

### Retry ownership

**DOC, rechecked 2026-10-03:** Octocrab's builder documents retry-capable middleware and selectable transport behavior. This does not prove every endpoint/request is safe to resend.

**DESIGN:** safe reads may retry under a shared bounded policy; ambiguous writes must not be invisibly replayed by middleware underneath Store. One effect has one retry/readback owner. Current calls use `gh` in `crates/swarm-kernel-host/src/github/client.rs`; keep account/host selection and that effect boundary if replacing the transport.

## 4. Tokio change notification is not an event log

**DOC, rechecked 2026-10-03:** [`tokio::sync::watch`](https://docs.rs/tokio/latest/tokio/sync/watch/) retains the latest value, with receiver-local seen state. It is suitable for revision/freshness notification, not guaranteed delivery of every intermediate event.

**DESIGN:** committed Observations/Operations and per-consumer cursors are the durable source. Commit routing/Operation or pending-subject state with cursor advancement. Notify only afterward, and recover even if the notification is lost. Snapshot/cursor subscriptions must cover events racing with reconnect. This uses the existing Store pattern rather than adding a message broker.

## 5. Windmill on-behalf execution

**DOC, rechecked 2026-10-03:** [roles/run on behalf](https://www.windmill.dev/docs/core_concepts/roles_and_permissions), [jobs](https://www.windmill.dev/docs/core_concepts/jobs), [scheduling](https://www.windmill.dev/docs/core_concepts/scheduling), [draft/deploy](https://www.windmill.dev/docs/core_concepts/draft_and_deploy).

Windmill distinguishes permission-bearing execution identity and other job metadata. Its documented schedules/triggers associate execution ownership with their editor. Deployed content and a particular invocation are different objects.

**DESIGN:** take visible execution ownership, independent enablement and retained invocation inputs. Do not inherit last-editor ownership changes, another queue/database or unrestricted privileged tokens. In ELIOT, editing a script never silently changes its manager. Current rights are checked for new effects, and the actual assigned auditor remains the verdict author.

The source supports individual patterns, not full ELIOT correctness, external-script isolation or exact GitHub delivery.

## 6. Temporal schedule semantics

**DOC, rechecked 2026-10-03:** [Schedules](https://docs.temporal.io/schedule) distinguishes a schedule from its already started executions, manual triggering, overlap, catch-up/backfill and calendar semantics.

**DESIGN:** use the distinction, not a Temporal cluster or all its defaults. Disabling future starts does not cancel running work. One-shot manual execution remains possible. Keep intentional re-enable separate from ordinary outage recovery; ordinary ELIOT missed-run handling is latest-only. Never import unlimited historical replay, automatic terminate-and-replace or a new Task graph as an incidental feature.

A repaired candidate returning to audit is legitimate business progression, not timer replay. Generic loop controls must preserve that path while suppressing repeated actions on unchanged evidence.

## 7. MCP discovery and non-blocking readiness

**DOC, rechecked 2026-10-03:** [MCP Tools](https://modelcontextprotocol.io/specification/2025-06-18/server/tools) defines paged discovery, schemas and list-change notification. The cited protocol edition locates the reviewed contract; it is not a requirement to freeze negotiated protocol support.

**DESIGN:** catalogue visibility, authorization, harness discovery and model use are different facts. A server listing a tool or emitting `list_changed` does not prove that a client loaded it into the model context. Capture observed readiness levels asynchronously as telemetry; **never block `task.dispatch` or initial prompt delivery waiting for an idle or headless agent to connect to MCP (`awaiting_native_mcp`)**, and do not launch extra paid model turns merely to manufacture a probe result.

Keep hard server/application authorization and small role surfaces with deferred groups. The normal reviewer result surface is `review.submit`, not automatic Task mutation merely because a schema is visible.

## 8. Architectural donor patterns

The [donor inventory](../agent_swarm.donors-20260929.toml) records inspected libraries, patterns and source revisions. Select a pattern only for a retained responsibility; it does not implement ELIOT correlation, durability or authorization. Do not import another product to reproduce its architecture:

| Donor | Inspected unit | Useful mechanism | Boundary |
|---|---|---|---|
| **Waku** | [`crates/waku-core/src/driver/opencode.rs` at 10bcd728](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs) | One sequential command/event worker; `session.execution.*` settles an open turn once. ELIOT adds the [admitted-Operation window](implementation.md#launch-and-results) and retains its body. | This is inspected donor evidence, not a local dependency. Missing parent pointers alone do not reject an attributable result; settlement is not ELIOT input-correlation proof. |
| **Paseo** | [`AgentClient`/`AgentSession`](https://github.com/getpaseo/paseo/blob/main/packages/server/src/server/agent/agent-sdk-types.ts), rechecked 2026-10-18; [`agent-loading.ts` at b4af508e](https://github.com/getpaseo/paseo/blob/b4af508e2a9e5a34a8b0ffb8dfaff6fd679da6c7/packages/server/src/server/agent/agent-loading.ts). | One session-facing vocabulary, declared capabilities and optional native controls; initialization is coalesced and waits for close. | Borrow this narrow shape into existing Eliot contracts. Do not copy its timeline/UI/persistence model, broad feature flags or large provider implementations. Eliot retains Operation/result attribution. |
| **CCCC** | Recorded delivery unit `crates/cccc-core/src/connect_delivery.rs`; separate Windows crate linked above. | Delivery identity, actor generation and recipient-scoped reply/cancel patterns; complete suspended-child primitive where needed. ELIOT owns its SQLite commit-before-notify rule. | Keep delivery and process units distinct; neither imports a CCCC scheduler, ledger or observer-loss stop policy. |
| **Agent of Empires (AoE)** | Recorded `src/plugin/automation_policy.rs` at a8e8b3b1. | Active + reserved accounting and unknown-mode handling. The donor's pending reservations are in memory; indexed relational durability is the ELIOT V3 requirement. | Migrate current per-entry `meta` records and retained legacy ledgers into Store tables while preserving held reservations. |
| **Claw** | Recorded verifier/guard pattern at ffa595ab; see inventory. | Guarded writer, caller-owned checks and evidence-based verdicts. ELIOT implements bounded check execution through its existing process/capture owners. | Keep CheckRunner execution outside Store transactions; do not import another journal or workflow authority. |
| **OpenCnid/Symphony** | `src/watch.rs`, `src/workflow.rs`, `src/agent/claude_code.rs` | Parent-directory watching for atomic editor saves with debounce (`notify`), clean workspace/prompt separation, and structured stdio process management. | Expose watcher errors as explicit health gaps rather than swallowing them silently. |
| **Goose** | `crates/goose/src/scheduler/common.rs` | Bounded recipe/bundle capture and source-base-directory resolution at admission time (`swarm-scripts`). | Capture immutable script revision inputs at admission; do not import a second scheduler database. |

## 9. Official Vendor SDKs and Native Protocol Boundaries

Use the [common Eliot commands](module-api.md#common-harness-commands), translated
inside the selected adapter. The native APIs below were inspected on 2026-10-18;
their existence does not mean the current Eliot adapter maps them. Muse `next`
documents experimental MSP; OpenCode v2 is a different surface from older server
guides. Use the actual selected protocol, without retaining every historical dialect
or exposing vendor RPCs to the manager.

### Command mappings

| Harness | Native mechanism to reuse | Current Eliot work |
| --- | --- | --- |
| Codex | App-server `thread/start`/`resume`, `turn/start`/`steer`/`interrupt`; [Goal types](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs) provide `thread/goal/set`, `get`, `clear` and updates. [Goals](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex) can continue an idle thread themselves. | [Rust adapter](../../crates/swarm-adapter-codex/src/lib.rs) handles input/readback/usage but declares `agent.goal`, reply and configure unsupported. Implement mappings; do not infer vendor limitations from that declaration. |
| Muse | MSP session/turn commands; [goal/set](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/goal-set/) can start an idle unfinished Goal. [Sessions and turns](https://meta-models.github.io/muse-code-sdk/next/guides/msp-concepts/sessions-and-turns/) separates interrupt/cancel, unqueue and background-task controls. | Both current adapters map queued input and `turn/unqueued`, but do not expose those turn-stop commands. Preserve the existing no-run terminal handling when adding shared cancellation; select one executor. |
| Claude | [Published SDK types](https://app.unpkg.com/@anthropic-ai/claude-agent-sdk@0.3.301/files/sdk.d.ts): Query input/resume, permission callbacks, model/mode setters and interrupt. Its receipt can identify inputs that remain queued. No Goal control was found in the reviewed Query interface. | The current [SDK driver](../../crates/swarm-adapter-claude/sdk-harness/bridge.mjs) handles callbacks but omits these live controls and quota mapping. Declare native Goal unsupported on this surface; the Governor may select Eliot Goal messages. Preserve cancellation scope. |
| OpenCode | [v2 session API](https://dev.opencode.ai/v2/docs/api): prompt admission with `steer`/`queue`, durable inbox and interruption. `resume=true` after interruption can run steering/control items while queued prompts remain parked. No Goal route was found in the reviewed session API. | Map the selected HTTP/SSE surface directly in Rust. Keep native inbox ordering. Move controller-generated continuation into the Eliot message plugin; do not label it a native Goal or add an SDK bridge just to call HTTP. |
| Command ACP | [ACP prompt lifecycle](https://agentclientprotocol.com/protocol/v1/prompt-turn), session updates, advertised configuration and cancellation through the existing `agent-client-protocol` crate. | Reuse protocol types at this adapter boundary. ACP is not Eliot's bus and need not wrap native APIs of every other harness. |

Muse can end a reclaimed queued turn with `turn/unqueued`, or fail launch before
a started event. Preserve those cases; local adapters already recognize unqueued
turns. Shared Goal/cancellation behavior belongs to [module API](module-api.md).

Native Goal is the default where available. The Eliot plugin remains independently
selectable on every runtime with suitable message delivery, including runtimes
already running a native Goal. The [original Swarm runner](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md#two-goal-mechanisms)
demonstrates native Codex Goal and ordinary steers in the same session.

### Local observation sources

Source recheck: 2026-10-18. Collect these beside the existing event stream and
retain the result locally. The Governor reads the shared projection; it does not
invoke these readers or a provider API on every status request. These are observed
source contracts, not evidence that Eliot already implements their readers.

| Harness | Local source and correlation | Implementation limit |
| --- | --- | --- |
| Codex | [SessionMeta/SessionMetaLine](https://github.com/openai/codex/blob/main/codex-rs/protocol/src/protocol.rs) records thread/session/parent IDs, cwd and optional Git metadata in rollout JSONL. [Thread](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs) exposes an optional disk path. Retain that path during normal session setup; the old scripts demonstrate local rollout reading. | The path is marked unstable; ephemeral threads have no durable log. Parse the selected format in the Codex adapter. Captured Git metadata and recent rollout activity do not establish current branch or process liveness. |
| Claude | Official [session docs](https://code.claude.com/docs/en/agent-sdk/sessions) provide local disk enumeration/read through SDK `listSessions` and `getSessionMessages`. The published types above also expose `getSessionInfo`, `listSubagents` and `getSubagentMessages`; session info can contain cwd/branch. | Use a known project/session and bounded requests; returned pagination does not guarantee bounded underlying file I/O. Message history is not a live process status. Preserve native parent-tool/agent relationships where available; do not infer missing parents. |
| OpenCode | Current [session SQL](https://github.com/anomalyco/opencode/blob/dev/packages/core/src/session/sql.ts) defines `session_v2` with parent/project/workspace/directory fields and `session_message` with session ID, sequence and JSON data. The original `Agent-Watch.py` reads these locally in read-only mode. | Keep a small adapter-owned read-only query for the selected schema, including open records updated in place. No vendor migration, private DB writes or parallel historical schema families. An unreadable schema is a coverage gap, not an empty fleet. |
| Muse | [Session](https://meta-models.github.io/muse-code-sdk/next/generated/msp/types/session/) exposes the durable log path, workspace and optional content-activity/parent/branch/worktree facts. The original `muse_owner_activity.py` reads `session.jsonl` streams. | Empty path means no durable log. `parentSessionId` names the containing top-level session and need not identify the spawning parent. `notLoaded` is this host's view, not global death. Retain available facts without inventing a tree or live state. |

Use SDK local readers where they reduce maintained code; keep small file/SQL readers
inside the selected adapter where needed. Known paths/native IDs, incremental reads
and source gaps replace title/mtime guesses. Ordinary filesystem identifiers and
cursors suffice; no per-observation hash, signature or provenance registry is needed.
The [implementation](implementation.md#passive-agent-observation) joins these sources
with existing process ownership and workspace leases.

### Quota sources

| Source | What the adapter can report |
| --- | --- |
| Codex [account protocol](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/account.rs) | `account/rateLimits/read` supplies account buckets/windows and optional credit/spend/permission facts. Updated notifications are sparse: merge or refetch. Thread token usage is separate. Preserve field age; a rolling update does not prove ordinary usage recovered. |
| Muse [SubscriptionUsage](https://meta-models.github.io/muse-code-sdk/next/generated/msp/types/subscriptionusage/) | `usage/read` and `usage/changed` supply current/weekly usage and reset times. `observedAtMs` is arrival time, not a live provider clock. No absolute remaining allowance is supplied. Credential/account state is a separate read. |
| Claude SDK types above | `rate_limit_event` carries status/reset hints; its `utilization` unit is not documented in that type. The experimental structured usage read supplies explicit percentage windows, sometimes unavailable. If used, `skipBehaviors:true` avoids transcript scanning. Keep this unstable read optional and inside the adapter; do not guess event units. |
| OpenCode [Console Budgets API](https://dev.opencode.ai/v2/docs/console/api/budgets/) | Workspace-member monthly budgets have their own account/auth scope. They are not remaining Go subscription quota or a session token counter. Absent native account evidence stays unavailable. |

Preserve native units and unavailable fields, including genuine over-100 usage.
The [existing projection](module-api.md#activity-and-usage-reads) owns the shared meaning.

### Muse SDK ownership and code reduction

The inspected SDK [Connection](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/connection/connection.ts)
takes a transport and caller command ID. The existing [Node bridge](../../modules/muse/bridge.mjs)
already supplies that ID and `maxAttempts:1`; add no retry wrapper.
[Session](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/session.ts)
also coordinates recovery/replay, while [SessionFold](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/fold/session-fold.ts)
is an independent event projection. Compare lower-level reuse against the two
existing executors; do not import another replay owner or create a third bridge.

Rust [msp.rs](../../crates/swarm-adapter-muse/src/msp.rs) has 1,597 physical lines,
927 before its explicit test module, and already uses jsonrpsee/tokio-util. Those
are not automatically removable lines: count Rust, JS and IPC glue together.
Connection close hands the submission tail to its process-owning transport, whose
budget covers handoff to writes, not native completion. Eliot already distinguishes
submission and response; its unbounded owner stop wait needs the
[existing-owner fix](implementation.md#recovery-and-progress), not another close
coordinator. Source inspection does not establish the installed npm artifact.

### Optional GitHub service boundaries

Keep the existing `gh` effect owner unless a retained requirement justifies replacing
it. [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api)
supply paging and bounded readback; [webhook signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)
belongs at authenticated intake. [Check runs](https://docs.github.com/en/rest/checks/runs)
and [PR endpoints](https://docs.github.com/en/rest/pulls/pulls) describe remote candidate
evidence and effects, not local Task acceptance. Trigger disable and run cancellation
are distinct [Actions operations](https://docs.github.com/en/rest/actions/workflow-runs).
No merge queue or publication engine is required for local delivery.

## 10. Owner operating evidence

Historical MANAGER-BRIEF and control-plane audits motivated current ownership,
retained candidate/review identity, durable routing, immutable invocation inputs
and scoped recovery. Their old versions, mandatory counts/timers, stage policies
and cleanup commands are not current defaults. The active [configuration](configuration.md),
[delivery](delivery.md) and [recovery](implementation.md#recovery-and-progress)
contracts own the resulting rules; do not copy incident tables into agent tasks.

## 11. Donor integration discipline

**Source recheck, 2026-10-18:** [Cargo metadata](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
already exposes package targets and the resolved graph as machine-readable data.
Use it when package tooling needs those facts; remove the duplicate coordinate
allowlist and private-source regex machinery. Keep files required by a selected
adapter as ordinary package contents: the current policy file also lists the
Antigravity Node bridge resources. Removing a policy must not omit that used bridge.
Cargo lock resolution is a build input, not a requirement that two communicating
processes have identical checkouts, compilers or cache directories.

A dependency is integrated when a real caller uses it and its replaced mechanism
is removed. Pattern-only donors do not justify importing another workflow engine.
Use the [package ownership map](shared-packages.md) and [implementation choices](implementation.md#donor-choices)
for the concrete boundary, preserving one durable effect and retry owner.

## 12. Schema generation candidate

**DOC, inspected 2026-10-17:** [Schemars 1.2.2](https://docs.rs/schemars/1.2.2/schemars/)
derives JSON Schema from Rust DTOs and recognizes Serde field rules.
[SchemaSettings](https://docs.rs/schemars/1.2.2/schemars/generate/struct.SchemaSettings.html)
selects schema dialect and serialization/deserialization purpose; defaults and
schema structure are not a stable wire-output guarantee across crate updates.
`Cargo.lock` already contains 1.2.2 transitively; contracts do not yet use it directly.

**CODE / decision, rechecked at `f1b854b6`:** no direct shared-input DTO replacement
was established for the inspected MCP families. `TOOL_ITEMS` defines fields and
required lists; `build_input_schema` adds request-ID behavior and method refinements.
`TaskSpec` and coordination request parsers are host-owned; script schemas describe
user-defined values. The shared descriptor-install request is a different local
Operator contract. Generating these schemas would first require type moves or new
models, so this is not a ready code-reduction assignment.

Revisit alongside an independently needed wire-type extraction only if it deletes
more owned schema/refinement code than it adds. Preserve required/null handling,
bounds/descriptions, dialect, request-ID rules and catalog revision/cache behavior.
Keep runtime authorization, cross-field and UTF-8 byte validation at their current
owners. Do not introduce parallel DTOs, another schema DSL or a generated-code
pipeline to claim a donor integration. Historical payload/digest bytes stay exact.
