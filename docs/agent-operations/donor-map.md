# Donor Map — Rust Operations and Manager-Owned Automation

Revision 7 · V3 source/target reconciliation 2026-10-17. Waku, Paseo loading and CCCC suspended-child source were rechecked at their recorded commits; other source observations retain their recorded dates.

`CODE` means an inspected source behavior; `DOC` means official/library documentation; `OWNER_AUDIT` means supplied operating evidence; `DESIGN` means an ELIOT proposal. Earlier inspections are not new live qualification. Commit/blob identities locate evidence, not required installed versions.

## 1. Product decisions are not inferred from donors

**DESIGN, owner direction:** management is manual by default. Each manager enables the helpers they want, acting on their behalf inside existing rights. No global operating modes, extra stage-activation ledger or Root approval for already permitted actions. Rust owns every internal subsystem; Python/PowerShell are optional external extensions.

The operations program now also includes [Modular Runtime](modularity.md) and [Observability](observability.md), requested on 2026-10-05. They define build/process and diagnostic boundaries, not an extra workflow or permission engine. Source study does not prove the proposed combined system works; earlier donor observations below remain historical unless explicitly rechecked.

## 2. Earlier ELIOT evidence

Inspected main baseline: `504199d14135c030ad3951a3c5023a098a3d03f0`. Recheck current symbols when implementation starts.

| Unit | Source fact | Reuse/required change |
|---|---|---|
| [`src/model.rs`](../../crates/swarm-kernel-host/src/model.rs) | Manager actor has no invented generation; internal Scheduler has a special ownership path; negative not-observer is not a complete capability model. | Keep identity semantics; new on-behalf actions resolve their manager, not the Scheduler shortcut. |
| [`src/store/mod.rs`](../../crates/swarm-kernel-host/src/store/mod.rs) | Store owns serialized DB work and shared change notification; several effects use dedicated outer methods. | One common action/authorization boundary, no recursive public IPC in a transaction; notification is not durable delivery. |
| [`src/store/schedules.rs`](../../crates/swarm-kernel-host/src/store/schedules.rs), [`docs/schedules.md`](../schedules.md) | Considered cursor, request identity and scheduled check admission are coupled in one transaction. | Extend this property to event routing/pending subjects; preserve legacy slots and receipts. |
| [`src/store/submissions.rs`](../../crates/swarm-kernel-host/src/store/submissions.rs) | Reserve is queued; finish commits applied `task.submission`; feedback is guarded and not a native prompt. | Applied-submission triggers; explicit auditor result versus manager disposition versus repair delivery. |
| [`docs/task-policy.md`](../task-policy.md), [`docs/owner-decisions.md`](../owner-decisions.md) | Frozen policy/source/Attempt evidence, one manager worktree, GM-fenced effects and current first-slice restrictions. | Add accepted policy and shared code deliberately; keep earlier evidence recognizable. A design document does not silently grant rights. |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted non-force publication, process disposition and readback; preflight is not atomic remote old-ref CAS. | Preserve actual guarantees for manual/automatic callers and separate upload/merge. |
| [`src/mcp/subscriptions.rs`](../../crates/swarm-mcp/src/mcp/subscriptions.rs) | Existing bounded committed-fact polling and lag/resync. | Shared projectors plus separate live-native rings, not a transcript per subscriber. |

PR #22's proposed reviewer core included `task.request_changes`, while its Participant path forbids Task transitions. PR #23 supplies `review.assign/submit`. **DESIGN:** reconcile the two at implementation: assigned auditor submits its anchored verdict; manager or manager-owned automation applies guarded feedback. No role escalation by MCP visibility.

## 3. Mandatory Rust Crate Donors (No Handwritten Reinvention)

V3 requires direct donor use at the matching subsystem boundary. This table is a migration target: several named crates are not yet in Cargo dependencies, and `vendor/cccc-windows-process` does not yet exist. See the [current integration snapshot](refactoring.md#3-current-gaps-against-v3). Use each library for its actual API; ELIOT still owns the listed application semantics.

| Component | Maintained source | Mandatory use in ELIOT | ELIOT remains responsible for | Forbidden anti-pattern |
|---|---|---|---|---|
| `rmcp` | [API](https://docs.rs/rmcp/latest/rmcp/) | Actual MCP server/client transport and tool/resource routing. | Role/profile filtering, scoped credentials, Store admission and bounded I/O. The existing non-MCP `ModuleLink` protocol remains its own contract. | Reimplementing MCP framing or routing beside rmcp. |
| `agent-client-protocol` | [API](https://docs.rs/agent-client-protocol/latest/agent_client_protocol/) | Native ACP wire protocol, session/turn envelopes and content blocks for ACP adapters. | Binding generation, digest verification and Store receipt mapping. Zed eval-cli stays a sessionless CLI adapter unless its selected native protocol changes. | Ad-hoc parsing of an actual ACP protocol beside the crate. |
| `octocrab` | [API](https://docs.rs/octocrab/latest/octocrab/), [builder](https://docs.rs/octocrab/latest/octocrab/struct.OctocrabBuilder.html) | Complete Rust GitHub REST/GraphQL client, pagination, auth and bounded read retry (`swarm-forge-worker`, GitHub intake). | Credentials, action rights, durable intent, shared rate pacing, non-idempotent write readback. | Custom handwritten HTTP/GitHub REST stacks over raw `reqwest` or `curl`. |
| `croner` | [API](https://docs.rs/croner/latest/croner/) | Complete cron expression evaluator and timezone-aware occurrence calculation (`swarm-automation` / scheduler). | Manager enabled choice, due identity, overlap/catch-up and persistence. | Handwritten cron parsers or per-rule sleep loops. |
| `sysinfo` | [API](https://docs.rs/sysinfo/latest/sysinfo/) | Selective cross-platform process/resource observation, parent-child tree refresh and CPU/RSS sampling (`swarm-process`, `swarm-observer`). | Verified birth/Job ownership and safe lifecycle decisions; metrics do not authorize kill. | Spawning `tasklist`/`ps` subprocesses or writing raw Win32/procfs metric pollers. |
| `notify` | [API](https://docs.rs/notify/latest/notify/) | Event-driven filesystem and parent-directory atomic-save observation (`config`, workspace hints). | Exact readback, revision validation and source-health gaps. | Busy-waiting `sleep` loops polling file `mtime`. |
| `tracing`, `tracing-subscriber`, `tracing-appender` | [`reload`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/reload/), [`non_blocking`](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/) | Structured spans/events, dynamic `reload` filter swaps without restart, and bounded `non_blocking` writer (`swarm-telemetry`, `swarm-observer`). | Byte-budget accounting, `atlas-redact` scrubbing prior to sink write, and scope expiry. | Custom logging frameworks or duplicate handmade ring queues. |
| `atlas-redact` | `vendor/atlas` (`crates/atlas-redact`) | Deterministic secret/token/credential redaction across streaming chunks and diagnostic records before disk or IPC fanout. | Calling the redactor on every stream chunk and log line before persistence. | Regex-per-call ad-hoc secret stripping or unredacted diagnostic capture. |
| `cccc-windows-process` | [Pinned complete crate](https://github.com/ChesterRa/cccc/tree/c270e276591879c6edb7d08af67bec7035585c7e/crates/cccc-windows-process) | Suspended-child creation, owned primary-thread handle, resume and cleanup-on-failed-bootstrap, integrated through `swarm-process`. | Job creation/assignment and family policy are outside this small crate. Preserve owner-first non-killing observation for active agent families and separate bounded-check termination. | Copying its suspended-launch mechanism; assuming the donor supplies the full Job/family subsystem. |
| `tokio` / `rusqlite` / `serde` | Project manifest and library documentation. | Async runtime, WAL relational storage (1 writer + pooled read-only connections), and typed serialization. | Relational schema design, forward-compatible internal readers, and single Store authority. | Storing collections as JSON blobs in `meta` or serializing all reads through the single writer thread. |

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

The [donor inventory](../agent_swarm.donors-20260929.toml) distinguishes complete libraries from pattern-only sources and records exact revisions, licenses and exclusions. V3 requires the mechanisms below, while an upstream pattern does not by itself implement ELIOT Operation correlation, durability or authorization. Keep pattern-only sources as patterns and integrate complete libraries where the table above names a library boundary:

| Donor | Inspected unit | Mechanism to adopt in ELIOT | Anti-sabotage rule |
|---|---|---|---|
| **Waku** | [`crates/waku-core/src/driver/opencode.rs` at 10bcd728](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs) | One sequential command/event worker; `session.execution.*` settles an open turn once. ELIOT adds the [admitted-Operation window](architecture.md#41-deterministic-turn-scoped-result-correlation-agentresult) and retains its body. | Missing parent pointers alone do not reject an attributable result. Keep the inventory exclusions; upstream settlement is not ELIOT input-correlation proof. |
| **Paseo** | [`agent-loading.ts` at b4af508e](https://github.com/getpaseo/paseo/blob/b4af508e2a9e5a34a8b0ffb8dfaff6fd679da6c7/packages/server/src/server/agent/agent-loading.ts); earlier timeline units remain in the inventory. | `pendingAgentInitializations` coalesces initialization, and `waitForAgentClose` separates closing from new initialization. | Reuse the single-flight/lifecycle pattern; it is not evidence of ELIOT result attribution or durable completion. |
| **CCCC** | Recorded delivery unit `crates/cccc-core/src/connect_delivery.rs`; separate Windows crate linked above. | Delivery identity, actor generation and recipient-scoped reply/cancel patterns; complete suspended-child primitive where needed. ELIOT owns its SQLite commit-before-notify rule. | Keep delivery and process units distinct; neither imports a CCCC scheduler, ledger or observer-loss stop policy. |
| **Agent of Empires (AoE)** | Recorded `src/plugin/automation_policy.rs` at a8e8b3b1. | Active + reserved accounting and unknown-mode handling. The donor's pending reservations are in memory; indexed relational durability is the ELIOT V3 requirement. | Migrate current per-entry `meta` records and retained legacy ledgers into Store tables while preserving held reservations. |
| **Claw** | Recorded verifier/guard pattern at ffa595ab; see inventory. | Guarded writer, caller-owned checks and evidence-based verdicts. ELIOT implements bounded check execution through its existing process/capture owners. | Keep CheckRunner execution outside Store transactions; do not import another journal or workflow authority. |
| **OpenCnid/Symphony** | `src/watch.rs`, `src/workflow.rs`, `src/agent/claude_code.rs` | Parent-directory watching for atomic editor saves with debounce (`notify`), clean workspace/prompt separation, and structured stdio process management. | Expose watcher errors as explicit health gaps rather than swallowing them silently. |
| **Goose** | `crates/goose/src/scheduler/common.rs` | Bounded recipe/bundle capture and source-base-directory resolution at admission time (`swarm-scripts`). | Capture immutable script revision inputs at admission; do not import a second scheduler database. |

## 9. Official Vendor SDKs and Native Protocol Boundaries

Native references: [Codex app-server](https://developers.openai.com/codex/app-server/), [Claude Agent SDK / CLI](https://code.claude.com/docs/en/cli-reference), [OpenCode Server & SDK](https://opencode.ai/docs/plugins/), [Muse SDK integration](../../modules/muse/README.md), [Agent Client Protocol (ACP)](https://docs.rs/agent-client-protocol/latest/agent_client_protocol/).

**Effective SDK delegation rule:**
1. Where an official Rust protocol crate exists (`agent-client-protocol` for ACP-speaking adapters, `rmcp` for MCP) or the vendor exposes a clean documented JSON-RPC/HTTP contract (Codex `app-server`, OpenCode HTTP/SSE), the Rust adapter (`swarm-adapter-*`) speaks that contract directly and owns `ModuleLink`, process supervision, request/prompt digests, and Store receipts.
2. Where a vendor's session state machine, authentication handshake, or streaming assembly is packaged in an official non-Rust SDK (`@muse-code/sdk` in `modules/muse`, `@anthropic-ai/claude-agent-sdk` in `modules/claude`, `openai-codex` in `modules/codex`, `acpx` in `modules/acpx`), **retain and use the official vendor SDK as a thin, stateless protocol driver** behind the Rust adapter boundary until feature-complete parity is proven. **Never rewrite complex vendor SDK internals from scratch into half-broken Rust parsers or block model results because a handwritten parser lacks session state.**
3. Accept installed vendor runtimes (Bun, Node, Python, OpenCode, Claude CLI, Codex) by **protocol and capability compatibility**, not by hardcoded version strings (such as `Bun == 1.4.0`) or rigid global binary hashes.

GitHub references:

- [Webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api): durable authenticated intake, paging, dedupe and reconciliation via `octocrab`.
- [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches): exact candidate evidence, remote IDs and truthful conclusions; labels are not acceptance.
- [PR endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue): head/base/actual merged result differ; queue capability is optional, not a local distributor dependency.
- [Disable workflows](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows), [cancel a run](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/cancel-a-workflow-run), [trigger workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow): trigger settings, live execution and credential behavior differ. A local disable does not alter remote settings or prove cancellation.

## 10. Owner operating evidence

The supplied MANAGER-BRIEF and control-plane audit are historical evidence with changing instructions, not current product defaults. Do not copy old fixed versions, mandatory counts/timers, stage policies or cleanup commands.

| Incident | ELIOT contract response |
|---|---|
| Stale queue snapshots created duplicate ownership. | Current shared manual/automatic reservation in relational SQLite tables. |
| Late A review/return changed B. | Candidate/review-attempt anchors and historical late results. |
| Deliveries were lost behind marker variants/backlogs. | Durable routing cursor coupled with action or pending state; bounded current-work inclusion. |
| Native Goal/children revived an old manager. | One real continuation/lifecycle owner, not wrapper-based detection. |
| Reminders became a model's entire assignment. | Notices are information; separate authorized work delivery. |
| Same checklist blocked repaired code; new whitespace bypassed old defects. | Relevant source/evidence progression rather than bytes of checklist or random event IDs. |
| Scripts changed while executing. | Captured invocation inputs; future admissions use new settings. |
| Malformed data or failed Git read broke every queue. | Scoped gaps, last valid state and fair pending-subject reevaluation. |
| GitBookkeeping failure repeated real work. | Separate projections/effects and retained remote result. |
| Long shared logs/tool catalog overwhelmed participants. | Bounded streams/current context and deferred schemas; no transcript copies per observer. |
| Reported missing-parent result refusals and launcher MCP readiness holds. | V3 Operation-owned turn correlation and asynchronous MCP discovery; verify each runtime path before calling a hold a circular deadlock. |

## 11. Summary of Donor Boundary Discipline

The intended result is a small, high-performance shared Rust kernel/protocol (`swarm-contracts`, `swarm-store`, `swarm-kernel`), independently compiled execution/observer workers (`swarm-bus`, `swarm-automation`, `swarm-checks`, `swarm-forge-worker`, `swarm-script-worker`, `swarm-observer`), and thin, reliable runtime adapters (`swarm-adapter-*`) that leverage official vendor SDKs and vetted Rust donor crates (`rmcp`, `agent-client-protocol`, `octocrab`, `croner`, `sysinfo`, `notify`, `tracing`, `atlas-redact`, `cccc-windows-process`) to their full potential.
