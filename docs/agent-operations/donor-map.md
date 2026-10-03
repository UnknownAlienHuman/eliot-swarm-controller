# Donor Map — Rust Operations, Manual Control and Optional Automation

Revision 3 · research/review date 2026-10-03.

`CODE` identifies inspected source, `DOC` official documentation, `OWNER_AUDIT` supplied operating evidence, and `DESIGN` ELIOT's proposal. Source review does not establish compiled/live qualification. Observed commits/blob IDs identify evidence, not installation requirements or software-version pins. Donor implementation language never overrides ELIOT's Rust-only internal architecture.

## 1. ELIOT source facts

Original inspection was at `35e499ae73b622d873c44873f6993ee3fcbea87b`; the manual-control revision uses main `504199d14135c030ad3951a3c5023a098a3d03f0`. These are research anchors, not constraints on future main.

| Existing unit | Finding | Integration decision |
|---|---|---|
| [`src/store/submissions.rs`](../../src/store/submissions.rs) | Admission is queued; `finish` publishes applied `task.submission`; `request_changes` requires GM/operator and stores mail | Make submitted candidates selectable in manual mode. Automatic review/disposition/repair require separately enabled stages and guards. |
| [`src/policy.rs`](../../src/policy.rs) | One compiled current policy edition/digest is recognized | Preserve historical evidence when adopting explicit new policy; document text cannot grant rights. |
| [`src/scheduler.rs`](../../src/scheduler.rs), [`src/store/schedules.rs`](../../src/store/schedules.rs) | Configured once/interval CheckRuns and transactional receipts | Extend the same scheduler; no parallel Python scheduler or default activation of new definitions. |
| [`src/mcp/subscriptions.rs`](../../src/mcp/subscriptions.rs) | Committed-fact polling, bounded queues, lag/resync | Shared projector independent of manual/delegated mode, plus separate native live content. |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted exact candidate, non-force publication, uncertain-effect readback; old-ref preflight is not atomic CAS | Same handler/safety for manual and delegated requests; no second publication queue with weaker checks. |
| [`docs/owner-decisions.md`](../owner-decisions.md) | Managers own assignments/workspace; read-only reporting does not start work; host new-work control and exact GM-fenced publication remain distinct | A scoped automation switch must not remove manual tools or replace host/GM/security gates. |
| Existing [`modules`](../../modules), RuntimePort/process owners | Some owned bridges are non-Rust; exact native/lifecycle boundaries already exist | Port owned integration logic to Rust; preserve bindings, uncertainty and source coverage. |

The prior PR definitions were not yet sufficient for full manual control: README recommended the reviewed-delivery preset, its main example enabled automatic publication, and manager-gate described only the final gate. This revision changes those defaults and all affected handoff contracts directly rather than adding a contradictory note.

## 2. Rust libraries to reuse whole

Inspect maintained compatibility when implementing; do not prescribe an old release or silently install/upgrade the user's software. No runtime wildcard package downloads.

| Unit | Official/library source | Take | Keep in ELIOT |
|---|---|---|---|
| Octocrab | [Rust client API](https://docs.rs/octocrab/latest/octocrab/) | Complete GitHub client behind a narrow Rust port | Credentials, durable intent, rate coordination and reconciliation. |
| Croner | [Current crate API](https://docs.rs/croner/latest/croner/) | Complete cron-expression evaluation with one supported timezone integration | Activation, due identity, missed-run/overlap policy and persistence. |
| sysinfo | [Current crate API](https://docs.rs/sysinfo/latest/sysinfo/) | Shared selective process/resource sampler | Actual process ownership and decisions. |
| notify | [Current crate API](https://docs.rs/notify/latest/notify/) | Platform file observation and supported fallback | Bounded exact readback, invalidation and source-health reporting. |
| Existing Tokio/rusqlite/RMCP/serde | Project manifest and maintained library docs | Existing async/storage/typed protocol foundations | One Rust authority, not separate controllers. |

Octocrab is a community client, not GitHub's permission or effect authority. Missing typed endpoints may use its narrow underlying transport; they do not justify a Python/`gh` daemon. sysinfo metrics are observations, not proof a process can be killed. notify events are hints, not an exactly-once ledger.

**DOC:** [Cargo dependency requirements](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html). Compatible requirements and recorded build resolution are distinct from an instruction to freeze all future runs on an old crate/CLI/model release. Support/toolchain changes are explicit code work.

## 3. Rust donor: OpenCnid/Symphony

[OpenCnid/symphony](https://github.com/OpenCnid/symphony) is an independent Rust implementation, not a claim that OpenAI's reference is Rust. Units inspected in the preceding source pass:

| Unit | Useful inspected behavior | Caveat |
|---|---|---|
| [`src/watch.rs`](https://github.com/OpenCnid/symphony/blob/main/src/watch.rs), blob `4a41dbf7c25cda53a61e9c40e4623e3b5e302664` | Parent-directory watching tolerates atomic saves, debounces and asks the owner to revalidate | Watch errors were ignored; ELIOT must expose gaps. File change must never clear a manager pause. |
| [`src/workflow.rs`](https://github.com/OpenCnid/symphony/blob/main/src/workflow.rs), blob `c968e53123f311720bd23a34d0822f95337270b1` | Configuration, prompt and source directory stay distinct | Its inspected file read was unbounded. Do not import unrestricted YAML/prompt authority. |
| [`src/agent/claude_code.rs`](https://github.com/OpenCnid/symphony/blob/main/src/agent/claude_code.rs), blob `fa76bcffe0eeda62a6f270491c164978dbfda5b4` | Rust process/structured stdio and workspace validation | Requested ID is not observed native identity; tool-bridge failure cannot satisfy required capabilities; root exit is not descendant cleanup. |

**DESIGN:** focused transport/configuration patterns only, not another scheduler, permission model or proven fleet implementation. Full unit/license/notices and ELIOT-specific qualification remain required for actual reuse.

## 4. Paseo profiles and bounded selection

**DOC, inspected source:** [Agent profiles](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/agent-profiles.md) and [Hub workflows](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/hub/workflows.md).

Profiles combine provider/model/mode/thinking and when-to-use notes; edits affect future selections rather than already launched agents. Workflow choices select bounded complete named configurations, not arbitrary fragments that accidentally enlarge authority.

**DESIGN:** use that convenience in Rust runtime profiles. The manager can choose a profile manually without enabling a workflow. Notes do not assign tasks or activate automation. Take selection UX, not Paseo's TypeScript control plane. Source-document presence does not establish parity with an installed release.

## 5. Windmill draft versus deployed content

**DOC:** [Draft/deploy](https://www.windmill.dev/docs/core_concepts/draft_and_deploy), [roles and permissions](https://www.windmill.dev/docs/core_concepts/roles_and_permissions).

Useful distinctions are draft editing, deployed runnable content, invocation identity and scoped automation accounts. A script path can select active content while previous runs retain what they executed.

**DESIGN:** save validated definitions through preview/apply, select active scripts for future invocations, and separately require manager activation of triggers. Content deployment does not imply unattended execution. Authorized authors need not ask Root for each harmless edit, but their edits cannot remove a control hold or broaden the enabled effect envelope. No second Windmill queue/database/runtime is imported.

## 6. Manual controls and pause semantics checked in this pass

### 6.1 Temporal schedules

**DOC, rechecked 2026-10-03:** [Schedule, Pause and Policies](https://docs.temporal.io/schedule).

Temporal distinguishes a schedule from executions it has already started. Pausing stops future schedule actions, not those executions; a manual trigger remains available. Overlap/catch-up/backfill have separate policies.

**DESIGN:** copy this separation, not its service or all defaults. ELIOT keeps one-shot manual methods usable while autonomous scheduling is off, reports in-flight actions, and gives resume an explicit current-eligible/future-only choice. Its normal misfire behavior remains latest-only, not a large automatic catch-up window. ELIOT's project-level control also covers rules, delivery and Goals; a schedule-only pause would be insufficient.

### 6.2 GitHub trigger disabling versus run cancellation

**DOC, rechecked 2026-10-03:** [Disable/enable workflows](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows) and [cancel a workflow run](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/cancel-a-workflow-run).

The documented surfaces distinguish disabling future triggers from cancelling an identified run. They do not establish that toggling a local ELIOT flag stops already delegated remote work.

**DESIGN:** record external execution/request IDs and report their actual disposition. A local pause does not change repository settings, cancel a remote merge or disable unrelated user workflows. Cancellation is separately authorized and verified. This prevents a false 'fully manual' banner while remote/native work can still continue.

### 6.3 Claude Agent Teams

**DOC, rechecked 2026-10-03:** [Agent Teams](https://code.claude.com/docs/en/agent-teams).

The documented feature is opt-in and distinguishes teams from lighter sessions/subagents. Its page warns that team coordination costs more and is less suited to highly sequential or same-file work. These are vendor-specific defaults, not evidence of ELIOT fleet capacity.

**DESIGN:** make small-team manual use first-class rather than forcing every project through a workflow. Retain direct peer collaboration and passive visibility. Do not turn on ELIOT distribution merely because a native team feature is available or because a team grows.

### 6.4 Goose source capture

**CODE, prior pass:** [scheduler/common.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler/common.rs).

Bounded validated recipe bytes and retained source base directory are useful for external-script bundles. Do not import its separate schedule registry or agent scheduler. Capturing an entrypoint does not capture mutable dependencies. The commit identifies the inspected unit, not an install pin.

## 7. Native interfaces remain capability-specific

**DOC:** [Codex app-server](https://developers.openai.com/codex/app-server/), [Claude CLI](https://code.claude.com/docs/en/cli-reference) and [hooks](https://code.claude.com/docs/en/hooks), [Gemini CLI hooks](https://geminicli.com/docs/hooks/reference/), [OpenCode plugins](https://opencode.ai/docs/plugins/).

Use documented protocols from Rust, not terminal-string inference or copied commercial internals. Required tool/reporting/stream capabilities need actual readback. Gemini CLI is not Gemini Spark; public OpenCode APIs must match the installed service. Hook/SDK presence alone proves no wiring.

Async after-hooks are not vetoes. Native Stop/Goal/child-result behavior can continue a session independently of the ELIOT client. Local manual control is not proof that a native continuation was cleared. Report capability gaps, preserve one lifecycle owner and avoid blind replacement starts.

## 8. GitHub delivery constraints retained

### Intake and work identity

**DOC:** [Webhook best practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api).

Raw-body authentication, repository context, durable intake, dedupe, paging and reconciliation remain necessary. No event header/comment grants executable authority. ELIOT observes source changes in every mode but dispatches only from an explicit command or selected stage.

### Checks and review

**DOC:** [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

Commit-scoped Checks, append-style annotations and correlation IDs require retained remote IDs and careful replay/readback. Neutral/skipped required-check behavior is not a successful ELIOT audit; labels are not candidate-bound authority.

**DESIGN:** local evidence can update without an automatic action. Remote labels, summaries and checks are writes that the manager separately requests or enables. A bookkeeping failure cannot rerun a completed push.

### Merge and external continuation

**DOC:** [PR endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).

Expected PR head and integration base are different guards. Queue/enqueue or asynchronous request acknowledgement is not actual merge. Repository/plan capabilities must be checked; no native merge queue is a prerequisite for this user-owned project's local distributor. Revalidate selected endpoint schemas and result retention during implementation rather than hardcoding research-era API assumptions.

**DESIGN:** one Rust publication authority, integrated-candidate verification, no protection bypass and no local-mutex claim of exclusion over third-party writers. Already accepted external continuation remains visible during manual takeover.

### Workflow triggering

**DOC:** [Trigger workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

Credential/event-specific triggering rules affect expected checks. Determine actual setup semantics and surface missing checks; do not execute untrusted code under privileged workflow credentials to force completion. Local manual mode does not rewrite GitHub's trigger configuration.

## 9. Owner evidence, not copied policy

The supplied MANAGER-BRIEF and control-plane audits contain operating incidents and changing historical rules. Current user directions and repository policy govern this program; old version pins, compulsory timers and automatic launch counts are not copied.

| Historical incident | Contract response |
|---|---|
| Old queue snapshots caused duplicate ownership | Shared exact reservation for manual and automatic starts. |
| Late review/HOLD of A changed B | Candidate-bound feedback, stale result retained historically. |
| Native Goal/child completion revived an old manager | One continuation owner and explicit external/native drain state. |
| A stop command ended wrappers but not descendants | Report owned processes/effect disposition, not assumed cancellation. |
| Reminders became a model's whole task | Notice is not task/continuation; manual mode preserves explicit watches only. |
| Editing live scripts changed their execution | Captured invocation bytes; next run resolves active content. |
| Malformed source/config stopped all queues | Isolate one invalid input and retain last valid configuration. |
| Old backlog overwhelmed fresh deliveries | Resume recomputes current eligibility; no blanket historical replay. |
| The only useful path required automation scripts | First-class direct manager methods over the same Rust services. |

The manual-control design and its race/recovery scenarios are ELIOT proposals. Donor documentation supports individual distinctions; no source proves this full Rust/native/GitHub combination already works.

## 10. Reuse and qualification boundary

Reuse whole maintained Rust libraries where appropriate; keep Store/Operations/process/artifacts as owners. Source-level donor reuse requires complete unit/license review. Non-Rust products contribute ideas only. No dependency, code or live installation change is part of this PR.

Required qualification includes default-off behavior, direct-manual completeness, mixed-stage routing, disable versus in-flight races, manual/auto dedupe, hot reload and restart consent, external continuation visibility, actual native tools, Windows process ownership, GitHub permissions and staged fleet load. Source review alone satisfies none of those execution claims.
