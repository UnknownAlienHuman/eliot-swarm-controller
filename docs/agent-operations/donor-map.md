# Donor Map — Rust Operations and Manager-Owned Automation

Revision 6 · reviewed 2026-10-05. Earlier source observations retain their recorded dates.

`CODE` means an inspected source behavior; `DOC` means official/library documentation; `OWNER_AUDIT` means supplied operating evidence; `DESIGN` means an ELIOT proposal. Earlier inspections are not new live qualification. Commit/blob identities locate evidence, not required installed versions.

## 1. Product decisions are not inferred from donors

**DESIGN, owner direction:** management is manual by default. Each manager enables the helpers they want, acting on their behalf inside existing rights. No global operating modes, extra stage-activation ledger or Root approval for already permitted actions. Rust owns every internal subsystem; Python/PowerShell are optional external extensions.

The operations program now also includes [Modular Runtime](modularity.md) and [Observability](observability.md), requested on 2026-10-05. They define build/process and diagnostic boundaries, not an extra workflow or permission engine. Source study does not prove the proposed combined system works; earlier donor observations below remain historical unless explicitly rechecked.

## 2. Earlier ELIOT evidence

Inspected main baseline: `504199d14135c030ad3951a3c5023a098a3d03f0`. Recheck current symbols when implementation starts.

| Unit | Source fact | Reuse/required change |
|---|---|---|
| [`src/model.rs`](../../src/model.rs) | Manager actor has no invented generation; internal Scheduler has a special ownership path; negative not-observer is not a complete capability model. | Keep identity semantics; new on-behalf actions resolve their manager, not the Scheduler shortcut. |
| [`src/store/mod.rs`](../../src/store/mod.rs) | Store owns serialized DB work and shared change notification; several effects use dedicated outer methods. | One common action/authorization boundary, no recursive public IPC in a transaction; notification is not durable delivery. |
| [`src/store/schedules.rs`](../../src/store/schedules.rs), [`docs/schedules.md`](../schedules.md) | Considered cursor, request identity and scheduled check admission are coupled in one transaction. | Extend this property to event routing/pending subjects; preserve legacy slots and receipts. |
| [`src/store/submissions.rs`](../../src/store/submissions.rs) | Reserve is queued; finish commits applied `task.submission`; feedback is guarded and not a native prompt. | Applied-submission triggers; explicit auditor result versus manager disposition versus repair delivery. |
| [`docs/task-policy.md`](../task-policy.md), [`docs/owner-decisions.md`](../owner-decisions.md) | Frozen policy/source/Attempt evidence, one manager worktree, GM-fenced effects and current first-slice restrictions. | Add accepted policy and shared code deliberately; keep earlier evidence recognizable. A design document does not silently grant rights. |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted non-force publication, process disposition and readback; preflight is not atomic remote old-ref CAS. | Preserve actual guarantees for manual/automatic callers and separate upload/merge. |
| [`src/mcp/subscriptions.rs`](../../src/mcp/subscriptions.rs) | Existing bounded committed-fact polling and lag/resync. | Shared projectors plus separate live-native rings, not a transcript per subscriber. |

PR #22's proposed reviewer core included `task.request_changes`, while its Participant path forbids Task transitions. PR #23 supplies `review.assign/submit`. **DESIGN:** reconcile the two at implementation: assigned auditor submits its anchored verdict; manager or manager-owned automation applies guarded feedback. No role escalation by MCP visibility.

## 3. Whole Rust libraries

| Component | Maintained source | Take | ELIOT remains responsible for |
|---|---|---|---|
| Octocrab | [API](https://docs.rs/octocrab/latest/octocrab/), [builder](https://docs.rs/octocrab/latest/octocrab/struct.OctocrabBuilder.html) | Complete Rust GitHub client with narrow low-level endpoint access when needed. | Credentials, action rights, durable intent, shared rate pacing, retry/readback policy. |
| Croner | [API](https://docs.rs/croner/latest/croner/) | Complete expression evaluator and supported timezone integration. | Manager enabled choice, due identity, overlap/catch-up and persistence. |
| sysinfo | [API](https://docs.rs/sysinfo/latest/sysinfo/) | Shared selective process/resource observations. | Real process ownership and safe decisions; metrics do not authorize kill. |
| notify | [API](https://docs.rs/notify/latest/notify/) | Platform file-change hints/fallback. | Exact readback, invalidation and source-health gaps. |
| Existing Tokio/rusqlite/RMCP/serde | Project manifest and library documentation. | Existing asynchronous, transactional and typed protocol machinery. | One authority and bounded application semantics. |

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

## 7. MCP discovery and actual capability

**DOC, rechecked 2026-10-03:** [MCP Tools](https://modelcontextprotocol.io/specification/2025-06-18/server/tools) defines paged discovery, schemas and list-change notification. The cited protocol edition locates the reviewed contract; it is not a requirement to freeze negotiated protocol support.

**DESIGN:** catalogue visibility, authorization, harness discovery and model use are different facts. A server listing a tool or emitting `list_changed` does not prove that a client loaded it into the model context. Capture only observed readiness levels; do not launch extra paid model turns to manufacture a probe result. Missing mandatory reporting capability affects its launch, not the whole fleet.

Keep hard server/application authorization and small role surfaces with deferred groups. The normal reviewer result surface is `review.submit` when implemented, not automatic Task mutation merely because a schema is visible.

## 8. Source-level donors retained

### OpenCnid/Symphony

[Repository](https://github.com/OpenCnid/symphony); independent Rust implementation, not the language of OpenAI's reference. The following were inspected in an earlier source pass; `watch.rs` was reread for section 11. The other units were not re-reviewed and none was compiled in this review:

| Unit | Useful pattern | Boundary |
|---|---|---|
| `src/watch.rs`, blob `4a41dbf7c25cda53a61e9c40e4623e3b5e302664` | Parent-directory observation, atomic-save handling and debounce. | Ignored watcher errors must become visible gaps; use shared revision validation. |
| `src/workflow.rs`, blob `c968e53123f311720bd23a34d0822f95337270b1` | Configuration, source directory and prompt separation. | Do not copy unbounded reads or introduce YAML/prompt authority. |
| `src/agent/claude_code.rs`, blob `fa76bcffe0eeda62a6f270491c164978dbfda5b4` | Rust structured process/stdio with workspace context. | Requested identity is not observed native identity; root exit is not child cleanup. |

Complete unit/license/notices review and live ELIOT qualification are required before code reuse.

### Paseo

Previously inspected [profiles](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/agent-profiles.md) and [Hub workflows](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/hub/workflows.md) package provider/model/options and when-to-use guidance.

**DESIGN:** take complete named choices and future-assignment updates, not TypeScript internals, silent rerouting or profile text as authority. The evidence commit is not an install requirement; main-only source is not proof of installed release parity.

### Goose

Previously inspected [scheduler/common.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler/common.rs) provides useful bounded recipe capture and source-base-directory handling.

**DESIGN:** retain invocation inputs and relevant dependencies. Do not import its independent schedule registry or assume capturing one entrypoint freezes every imported support file.

## 9. Native and GitHub boundaries

Native references: [Codex app-server](https://developers.openai.com/codex/app-server/), [Claude CLI](https://code.claude.com/docs/en/cli-reference), [Claude hooks](https://code.claude.com/docs/en/hooks), [Gemini CLI hooks](https://geminicli.com/docs/hooks/reference/), [OpenCode plugins](https://opencode.ai/docs/plugins/). These are integration references, not assertions of installed support. Read actual protocols from Rust and preserve exact native lifecycle ownership. Async after-hooks are not vetoes; wrapper exit is not proof native Goal/children stopped.

GitHub references:

- [Webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api): durable authenticated intake, paging, dedupe and reconciliation.
- [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches): exact candidate evidence, remote IDs and truthful conclusions; labels are not acceptance.
- [PR endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue): head/base/actual merged result differ; queue capability is optional, not a local distributor dependency.
- [Disable workflows](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows), [cancel a run](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/cancel-a-workflow-run), [trigger workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow): trigger settings, live execution and credential behavior differ. A local disable does not alter remote settings or prove cancellation.

Revalidate endpoint and installed-native schemas during implementation; prior source review is not a permanent API-version requirement. No bypass of protections, forged human GitHub actor or privileged untrusted-code workaround.

## 10. Owner operating evidence

The supplied MANAGER-BRIEF and control-plane audit are historical evidence with changing instructions, not current product defaults. Do not copy old fixed versions, mandatory counts/timers, stage policies or cleanup commands.

| Incident | ELIOT contract response |
|---|---|
| Stale queue snapshots created duplicate ownership. | Current shared manual/automatic reservation. |
| Late A review/return changed B. | Candidate/review-attempt anchors and historical late results. |
| Deliveries were lost behind marker variants/backlogs. | Durable routing cursor coupled with action or pending state; bounded current-work inclusion. |
| Native Goal/children revived an old manager. | One real continuation/lifecycle owner, not wrapper-based detection. |
| Reminders became a model's entire assignment. | Notices are information; separate authorized work delivery. |
| Same checklist blocked repaired code; new whitespace bypassed old defects. | Relevant source/evidence progression rather than bytes of checklist or random event IDs. |
| Scripts changed while executing. | Captured invocation inputs; future admissions use new settings. |
| Malformed data or failed Git read broke every queue. | Scoped gaps, last valid state and fair pending-subject reevaluation. |
| GitBookkeeping failure repeated real work. | Separate projections/effects and retained remote result. |
| Long shared logs/tool catalog overwhelmed participants. | Bounded streams/current context and deferred schemas; no transcript copies per observer. |

These failures motivate the changes. They do not prove any ELIOT throughput, model quality or Windows soak result. The integrated qualification matrix remains future work; this PR executes no live automation.

## 11. Module/recovery/logging source recheck — 2026-10-05

The [ELIOT recheck](modularity-review-2026-10-05.md) uses source `2aec51bb`.
The following donor files were read in this review; none was installed, copied
into the product or native-qualified by this documentation change.

| Evidence read | Mechanism to reuse | Do not import or claim |
|---|---|---|
| **CODE — Paseo** [`agent-loading.ts` at `8ced059b`](https://github.com/getpaseo/paseo/blob/8ced059b4da95a1d4931e991ded21d7597045f5f/packages/server/src/server/agent/agent-loading.ts) | One in-flight initialization per agent, close/start barriers and lazy restore from a stored handle. Implement the same single-flight/late-reply discipline in the Rust module supervisor. | Its RAM promise map is not a durable ELIOT receipt. Its no-handle create branch must not become a fallback for an ELIOT operation with unknown prior native effect. |
| **CODE — Paseo** [`plugin-process.ts` at the same source](https://github.com/getpaseo/paseo/blob/8ced059b4da95a1d4931e991ded21d7597045f5f/packages/server/src/server/plugins/plugin-process.ts), registration/connect portion | Separate registered provider metadata and explicit connection creation; duplicate connection IDs and pending-connect tombstones; validate events before projection. Use a common descriptor/IPC contract, not core runtime-name switches. | This inspected portion does not prove all OS crash/restart behavior. Do not copy the TypeScript controller or create a second provider/session authority. |
| **CODE — CCCC** [`cccc-windows-process/src/suspended.rs` at `c270e276`](https://github.com/ChesterRa/cccc/blob/c270e276591879c6edb7d08af67bec7035585c7e/crates/cccc-windows-process/src/suspended.rs), with its `lib.rs` boundary | Small platform package, suspended creation until ownership is established, owned handles and failed-start RAII cleanup. Retain ELIOT's working owner-first path; use the complete selected crate only if child-first creation is actually needed. | Its pre-resume Drop kills/reaps a child; that is **not** the policy for already-admitted native work after bridge loss. No whole CCCC daemon/queue or unreviewed license/adoption claim. |
| **CODE — OpenCnid/Symphony** [`src/watch.rs` blob `4a41dbf7`](https://api.github.com/repos/OpenCnid/symphony/git/blobs/4a41dbf7c25cda53a61e9c40e4623e3b5e302664), reread | Watch parent directory for atomic editor saves, debounce notifications, reread/revalidate authoritative configuration elsewhere. Reuse maintained `notify` when needed, not this whole orchestrator. | The callback discards watcher errors; ELIOT must expose a coverage/health gap instead. A missed notification is not evidence of no change. |
| **DOC — Rust** [Cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html), [profiles](https://doc.rust-lang.org/cargo/reference/profiles.html) | Package-selective builds, one lock/cache, explicit default-members, fast local profile versus optimized packaging. | Rust `mod` is not independent compilation. A statically linked crate can still require consumer relinking. No measured speedup or support for Cargo features newer than the actual compiler is implied. |
| **DOC — Rust tracing** [`reload` 0.3.23](https://docs.rs/tracing-subscriber/0.3.23/tracing_subscriber/reload/), [`non_blocking` 0.2.5](https://docs.rs/tracing-appender/0.2.5/tracing_appender/non_blocking/) | Complete filter-reload/structured logging libraries, bounded off-thread writer, retained flush guard and dropped-record counter. Add these behind the telemetry/recorder boundary. | A line-count queue is not a byte limit; diagnostic logs are not durable business receipts; a guard cannot flush after SIGKILL/power loss. These versions identify the read docs, not newly installed dependencies. |

Keep the existing unchanged Atlas redaction unit and official native SDK
boundaries. Migrate ELIOT-owned state/transport glue into Rust modules against
supported public contracts; do not reimplement vendor model loops or copy private
internals. If a required native SDK has no supported Rust/wire equivalent, retain
an explicitly temporary isolated compatibility bridge and track that gap; do
not silently break an existing route or call the migration complete.

The intended result is a small shared Rust kernel/protocol, separate replaceable
execution/observer modules and one authority. It is not a collection of all donor
frameworks installed together. Library/pattern selection is separate from actual
version, source-integrity, license and live-compatibility verification at adoption.
