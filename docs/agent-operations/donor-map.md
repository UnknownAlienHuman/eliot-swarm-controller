# Donor Map — Rust Operations and Manager-Owned Automation

Revision 10 · Reduction review 2026-10-18. Waku, Paseo loading and Muse Connection/Session were rechecked at the recorded commits; Cargo metadata documentation was checked. Other observations retain their dates.

`CODE` means an inspected source behavior; `DOC` means official/library documentation; `OWNER_AUDIT` means supplied operating evidence; `DESIGN` means an ELIOT proposal. Earlier inspections are not new live qualification. Commit/blob identities locate evidence, not required installed versions.

## 1. Product decisions are not inferred from donors

**DESIGN, owner direction:** management is manual by default. Each manager enables the helpers they want, acting on their behalf inside existing rights. No global operating modes, extra stage-activation ledger or Root approval for already permitted actions. Rust owns the control plane and durable work; native SDK drivers follow section 9. Python/PowerShell user scripts are optional external extensions.

The operations program now also includes [Modular Runtime](modularity.md) and [Observability](observability.md), requested on 2026-10-05. They define build/process and diagnostic boundaries, not an extra workflow or permission engine. Source study does not prove the proposed combined system works; earlier donor observations below remain historical unless explicitly rechecked.

## 2. Earlier ELIOT evidence

Earlier ELIOT inspection used `504199d14135c030ad3951a3c5023a098a3d03f0`.
Use the [current refactoring map](refactoring.md) for the recheck at `f1b854b6`,
including completed audit fixes, actual dependency use and current call sites.
The [architecture](architecture.md), [delivery contract](delivery.md) and
[reduction program](refactoring.md) own the current target. Earlier owner-policy
editions record decisions; their document hashes are not future runtime rights.

## 3. Library reuse at a required boundary

Use a library when its actual API replaces a needed owned mechanism. This table is a boundary map, not an obligation to implement every listed subsystem. Several crates are not direct dependencies, and `vendor/cccc-windows-process` does not exist. Keep current rmcp/ACP/croner/Atlas integrations; select further replacements by the concrete caller and net code removed. A library name in a document or lockfile is not integration, and adding a wrapper to satisfy the table is not progress.

| Component | Maintained source | Mechanism supplied | ELIOT responsibility | Reinvention to remove |
|---|---|---|---|---|
| `rmcp` | [API](https://docs.rs/rmcp/latest/rmcp/) | Actual MCP server/client transport and tool/resource routing. | Role/profile filtering, scoped credentials, Store admission and bounded I/O. The existing non-MCP `ModuleLink` protocol remains its own contract. | Reimplementing MCP framing or routing beside rmcp. |
| `agent-client-protocol` | [API](https://docs.rs/agent-client-protocol/latest/agent_client_protocol/) | Native ACP wire protocol, session/turn envelopes and content blocks for ACP adapters. | Binding generation, digest verification and Store receipt mapping. Zed eval-cli stays a sessionless CLI adapter unless its selected native protocol changes. | Ad-hoc parsing of an actual ACP protocol beside the crate. |
| `octocrab` | [API](https://docs.rs/octocrab/latest/octocrab/), [builder](https://docs.rs/octocrab/latest/octocrab/struct.OctocrabBuilder.html) | Complete Rust GitHub REST/GraphQL client, pagination, auth and bounded read retry (`swarm-forge-worker`, GitHub intake). | Credentials, action rights, durable intent, shared rate pacing, non-idempotent write readback. | Custom handwritten HTTP/GitHub REST stacks over raw `reqwest` or `curl`. |
| `croner` | [API](https://docs.rs/croner/latest/croner/) | Complete cron expression evaluator and timezone-aware occurrence calculation (`swarm-automation` / scheduler). | Manager enabled choice, due identity, overlap/catch-up and persistence. | Handwritten cron parsers or per-rule sleep loops. |
| `sysinfo` | [API](https://docs.rs/sysinfo/latest/sysinfo/) | Selective cross-platform process/resource observation, parent-child tree refresh and CPU/RSS sampling (`swarm-process`, `swarm-observer`). | Verified birth/Job ownership and safe lifecycle decisions; metrics do not authorize kill. | Spawning `tasklist`/`ps` subprocesses or writing raw Win32/procfs metric pollers. |
| `notify` | [API](https://docs.rs/notify/latest/notify/) | Event-driven filesystem and parent-directory atomic-save observation (`config`, workspace hints). | Exact readback, revision validation and source-health gaps. | Busy-waiting `sleep` loops polling file `mtime`. |
| `tracing`, `tracing-subscriber`, `tracing-appender` | [`reload`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/reload/), [`non_blocking`](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/) | Structured spans/events, dynamic `reload` filter swaps without restart, and bounded `non_blocking` writer (`swarm-telemetry`, `swarm-observer`). | Byte-budget accounting, `atlas-redact` scrubbing prior to sink write, and scope expiry. | Custom logging frameworks or duplicate handmade ring queues. |
| `atlas-redact` | `vendor/atlas` (`crates/atlas-redact`) | Deterministic secret/token/credential redaction across streaming chunks and diagnostic records before disk or IPC fanout. | Calling the redactor on every stream chunk and log line before persistence. | Regex-per-call ad-hoc secret stripping or unredacted diagnostic capture. |
| `cccc-windows-process` | [Inspected complete crate](https://github.com/ChesterRa/cccc/tree/c270e276591879c6edb7d08af67bec7035585c7e/crates/cccc-windows-process) | Suspended-child creation, owned primary-thread handle, resume and cleanup-on-failed-bootstrap, integrated through `swarm-process`. | Job creation/assignment and family policy are outside this small crate. Preserve owner-first non-killing observation for active agent families and separate bounded-check termination. | Copying its suspended-launch mechanism; assuming the donor supplies the full Job/family subsystem. |
| `tokio` / `rusqlite` / `serde` | Project manifest and library documentation. | Async runtime, WAL relational storage (1 writer + pooled read-only connections), and typed serialization. | Current domain schema, authorized snapshots and single Store authority. | Storing collections as JSON blobs in `meta` or serializing all reads through the single writer thread. |

No old release, fixed CLI/model equality or callback-time package installation is prescribed. Ordinary compatible requirements and recorded build resolution are not a policy freezing future software. [Cargo's dependency documentation](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html) distinguishes dependency requirements from a particular resolved build.

### Retry ownership

**DOC, rechecked 2026-10-03:** Octocrab's builder documents retry-capable middleware and selectable transport behavior. This does not prove every endpoint/request is safe to resend.

**DESIGN:** inspect the chosen library configuration and actual method before use. Safe reads may retry under a shared bounded policy; ambiguous non-idempotent writes must not be invisibly replayed by middleware underneath Store. One effect has one retry/readback owner. Do not replace the whole client with a second handwritten GitHub stack merely to control this boundary.

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
| **Waku** | [`crates/waku-core/src/driver/opencode.rs` at 10bcd728](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs) | One sequential command/event worker; `session.execution.*` settles an open turn once. ELIOT adds the [admitted-Operation window](architecture.md#41-deterministic-turn-scoped-result-correlation-agentresult) and retains its body. | Missing parent pointers alone do not reject an attributable result. Keep the inventory exclusions; upstream settlement is not ELIOT input-correlation proof. |
| **Paseo** | [`agent-loading.ts` at b4af508e](https://github.com/getpaseo/paseo/blob/b4af508e2a9e5a34a8b0ffb8dfaff6fd679da6c7/packages/server/src/server/agent/agent-loading.ts); earlier timeline units remain in the inventory. | `pendingAgentInitializations` coalesces initialization, and `waitForAgentClose` separates closing from new initialization. | Reuse the single-flight/lifecycle pattern; it is not evidence of ELIOT result attribution or durable completion. |
| **CCCC** | Recorded delivery unit `crates/cccc-core/src/connect_delivery.rs`; separate Windows crate linked above. | Delivery identity, actor generation and recipient-scoped reply/cancel patterns; complete suspended-child primitive where needed. ELIOT owns its SQLite commit-before-notify rule. | Keep delivery and process units distinct; neither imports a CCCC scheduler, ledger or observer-loss stop policy. |
| **Agent of Empires (AoE)** | Recorded `src/plugin/automation_policy.rs` at a8e8b3b1. | Active + reserved accounting and unknown-mode handling. The donor's pending reservations are in memory; indexed relational durability is the ELIOT V3 requirement. | Migrate current per-entry `meta` records and retained legacy ledgers into Store tables while preserving held reservations. |
| **Claw** | Recorded verifier/guard pattern at ffa595ab; see inventory. | Guarded writer, caller-owned checks and evidence-based verdicts. ELIOT implements bounded check execution through its existing process/capture owners. | Keep CheckRunner execution outside Store transactions; do not import another journal or workflow authority. |
| **OpenCnid/Symphony** | `src/watch.rs`, `src/workflow.rs`, `src/agent/claude_code.rs` | Parent-directory watching for atomic editor saves with debounce (`notify`), clean workspace/prompt separation, and structured stdio process management. | Expose watcher errors as explicit health gaps rather than swallowing them silently. |
| **Goose** | `crates/goose/src/scheduler/common.rs` | Bounded recipe/bundle capture and source-base-directory resolution at admission time (`swarm-scripts`). | Capture immutable script revision inputs at admission; do not import a second scheduler database. |

## 9. Official Vendor SDKs and Native Protocol Boundaries

Use official protocol crates at actual protocol boundaries: ACP through
`agent-client-protocol`, MCP through `rmcp`. Documented native APIs such as
[Codex app-server](https://developers.openai.com/codex/app-server/) and
[OpenCode HTTP/SSE](https://opencode.ai/docs/) may be called directly from Rust.
A vendor SDK may supply native transport/session handles through a thin driver;
Rust owns `ModuleLink`, supervision, admitted identity and durable Store receipts.
No SDK introduces a second Eliot job ledger or independent uncertain-input replay.

**CODE, Muse SDK rechecked at recorded source commit `a7c10c5`:**
[`Connection`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/connection/connection.ts)
accepts a transport and a caller-supplied `CommandOptions.commandId`.
The high-level [`Session`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/session.ts)
creates pending-command/turn/gap helpers and drives command replay during recovery.
[`SessionFold`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/fold/session-fold.ts)
provides a separate in-memory event projection. This source observation does not
prove equality with the installed npm tarball; the [donor inventory](../agent_swarm.donors-20260929.toml)
records `published_artifact_version_verified = false` for this SDK source pin.

**DESIGN, bounded replacement candidate:** compare lower-level `Connection` and
direct event-fold reuse with the existing Rust transport and Node bridge, without
the `Session` recovery coordinator. Rust must
persist `NativePending` before I/O and validate/retain the exact command/boot/ACK
identity. Direct adoption of `Session` would add another replay owner and is not
accepted as that replacement. The [Rust transport](../../crates/swarm-adapter-muse/src/msp.rs)
is 1,597 physical lines, with 927 preceding its explicit test module; these are not
927 removable lines. It already uses `jsonrpsee` and `tokio-util`. A new Rust–Node
bridge could exceed the saving. Count all
owned languages, IPC glue and retained compatibility before adopting it; do not
build a new wrapper solely to move Rust lines elsewhere.

Accept installed runtimes by documented protocol/capability compatibility.
Selected local artifact digests and historical request/checkpoint bytes remain
exact. Source refactoring does not wait for all-runtime native qualification;
use a [finite transition](refactoring.md#retire-executors-without-losing-history)
for actual required data, then remove the old executor and readers.

GitHub references:

- [Webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api): durable authenticated intake, paging, dedupe and reconciliation via `octocrab`.
- [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches): exact candidate evidence, remote IDs and truthful conclusions; labels are not acceptance.
- [PR endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue): head/base/actual merged result differ; queue capability is optional, not a local distributor dependency.
- [Disable workflows](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows), [cancel a run](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/cancel-a-workflow-run), [trigger workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow): trigger settings, live execution and credential behavior differ. A local disable does not alter remote settings or prove cancellation.

## 10. Owner operating evidence

Historical MANAGER-BRIEF and control-plane audits motivated current ownership,
retained candidate/review identity, durable routing, immutable invocation inputs
and scoped recovery. Their old versions, mandatory counts/timers, stage policies
and cleanup commands are not current defaults. The active [configuration](configuration.md),
[delivery](delivery.md) and [recovery](architecture.md#11-recovery-and-failure-isolation)
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

The inspected Waku driver has one sequential command/event worker and one turn
settlement function. Paseo's 153-line loading unit coalesces initialization and
waits for close. These narrow patterns support one owner with fewer competing
paths; copying either entire orchestrator would add unrelated behavior.

Muse `Connection.command` defaults to up to three attempts, only for its explicit
`overloaded`/`backpressured` no-admission errors. The existing
[Node bridge](../../modules/muse/bridge.mjs) already supplies `commandId` and
`maxAttempts:1`; do not invent a missing retry-control wrapper. Its high-level
`Session` additionally owns recovery/replay. Compare the existing lower-level
reuse and required result/ownership consumers before choosing between the two
Muse implementations. Source commit observations do not certify an npm artifact.

A dependency is integrated when a real caller uses it and its replaced mechanism
is removed. Pattern-only donors do not justify importing another workflow engine.
Use the [package ownership map](shared-packages.md) and [deletion candidates](refactoring.md#code-reduction-candidates)
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
