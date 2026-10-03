# Donor Map — Rust Agent Operations and GitHub Delivery

Revision 2 · research date 2026-10-03.

`CODE` identifies inspected source, `DOC` official documentation, `OWNER_AUDIT` supplied operating evidence, and `DESIGN` ELIOT's adaptation. Source review does not establish compiled or live qualification. Observed commits, blob IDs and dates below identify evidence; they are not installation requirements or dependency pins. No donor's implementation language overrides the owner's requirement that ELIOT's internal systems are Rust.

## 1. Reuse ELIOT's existing authority

Inspected main: `35e499ae73b622d873c44873f6993ee3fcbea87b`. Use the current implementation when work starts; these source references explain this review's findings rather than freezing future main.

| Existing unit | Source finding | Integration decision |
|---|---|---|
| [`src/store/submissions.rs`](../../src/store/submissions.rs) | `reserve` returns queued admission; `finish` records applied `task.submission`; `request_changes` requires GM/operator and creates mail, not native input | Start review from applied submission. Add narrow delegated disposition and separate authorized repair dispatch through the same guarded transitions |
| [`src/policy.rs`](../../src/policy.rs) | Accepted edition and source digest are compiled; the projection recognizes only the current edition | Preserve historical Attempt evidence while adding authorized configurable workflow policy. A renamed role or edited document does not bypass the existing gate |
| [`src/scheduler.rs`](../../src/scheduler.rs), [`src/store/schedules.rs`](../../src/store/schedules.rs) | One-shot/interval CheckRun scheduling and transactional receipts | Extend the same Rust scheduler; do not add another scheduler database |
| [`src/mcp/subscriptions.rs`](../../src/mcp/subscriptions.rs) | Bounded committed-fact polling, lag and resync; not provider token streaming | Preserve the contract, centralize source/projector work and add separate live content |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted-candidate non-force publication, owned process cleanup, readback after uncertain writes; preflight is not atomic expected-old CAS | Reuse exact-candidate/effect handling. Review-branch upload and PR merge need explicit additional contracts |
| [`src/runtime/owner.rs`](../../src/runtime/owner.rs), [`src/platform/process_group.rs`](../../src/platform/process_group.rs) | Recorded lifecycle/process ownership boundaries | Reuse ownership, not process-name heuristics; source-specific compatibility still requires verification |
| Existing [`modules`](../../modules) and module contract | Some owned adapters are Python/JS/TS and source-specific SDK bindings | Port owned control/translation to Rust. These implementations are migration evidence, not permitted internal scripting shortcuts |

The current source prevents automatic reviewer return unless the authorization path is extended. The design fixes that actual obstacle rather than only adding reviewer tools to MCP.

## 2. Rust units to take whole

Do not select an obsolete release merely to retain an old toolchain floor. Inspect current compatibility and update the Rust integration/toolchain policy explicitly when needed. Do not silently upgrade a user's installation or download dependencies at runtime.

| Unit | Source | Take | Keep in ELIOT |
|---|---|---|---|
| **Octocrab** | [Rust client documentation](https://docs.rs/octocrab/latest/octocrab/) | Complete GitHub client behind one narrow Rust GitHub port; typed endpoint handlers and controlled lower-level requests where needed | Credentials, permission checks, rate coordination, durable intent and reconciliation |
| **Croner** | [Current crate API](https://docs.rs/croner/latest/croner/) | Complete expression evaluator with a selected supported timezone integration | Due occurrence identity, missed-run policy, overlap, action admission and persistence |
| **sysinfo** | [Current crate API](https://docs.rs/sysinfo/latest/sysinfo/) | One shared selective metrics collector | Process ownership and decisions; metrics are observations, not permission to kill |
| **notify** | [Current crate API](https://docs.rs/notify/latest/notify/) | Complete platform file watcher and supported fallback | Debounced invalidation, exact Git/config reads and source-health reporting |
| Existing Tokio/rusqlite/RMCP/serde stack | Repository manifest and current library documentation | Existing runtime, Store, typed data and protocol building blocks | One control plane, not separate script/cron/chat services |

Octocrab is a Rust community client, not GitHub's authorization or job authority. Keep one GitHub request/rate boundary rather than a client per viewer or two competing GitHub loops. A missing typed endpoint can use that same client's narrow lower-level transport; it is not a reason to introduce Python or shell `gh` orchestration.

`sysinfo` supports reusing `System` and selective refresh; some metrics require observations across time and some platforms are unsupported. Detect unsupported coverage rather than reporting empty healthy state. `notify` documents backend/filesystem limitations: events are hints, not an exactly-once change ledger.

**DOC:** [Cargo dependency requirements](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html). Use ordinary compatible requirements rather than mandatory exact release equality or commit dependencies. Build-resolution records and observed software versions are diagnostic evidence, not a policy forcing future work onto the same old release. Unchecked wildcard downloads are not the alternative.

## 3. Rust orchestration donor: OpenCnid/Symphony

This is the independent Rust implementation at [OpenCnid/symphony](https://github.com/OpenCnid/symphony), not a claim that OpenAI's reference implementation is Rust. The following files were inspected on 2026-10-03; source blob IDs identify what was read, not what ELIOT must install.

| Inspected unit | Concrete useful behavior | Do not inherit blindly |
|---|---|---|
| [`src/watch.rs`](https://github.com/OpenCnid/symphony/blob/main/src/watch.rs), blob `4a41dbf7c25cda53a61e9c40e4623e3b5e302664` | Watches the parent directory, tolerates atomic replacement, debounces and notifies the orchestrator to reread/validate | Callback ignores watcher errors; ELIOT must expose source health/gaps rather than silently lose monitoring |
| [`src/workflow.rs`](https://github.com/OpenCnid/symphony/blob/main/src/workflow.rs), blob `c968e53123f311720bd23a34d0822f95337270b1` | Keeps parsed configuration, prompt and source location distinct; resolves relative paths against the source | The inspected load path uses an unbounded file read. Keep ELIOT's bounded typed configuration; do not import a new YAML/prompt authority unnecessarily |
| [`src/agent/claude_code.rs`](https://github.com/OpenCnid/symphony/blob/main/src/agent/claude_code.rs), blob `fa76bcffe0eeda62a6f270491c164978dbfda5b4` | Rust Tokio process/stdio path builds Claude stream-JSON argv and validates workspace before launch | A requested session ID is not observed native identity; proceeding after optional tool-bridge failure cannot satisfy a required reporting capability; process-root exit is not Windows descendant cleanup |

**DESIGN:** use these as focused Rust transport/configuration patterns, not another orchestrator or a finished GitHub delivery solution. Only the named portions were inspected; no fleet benchmark, installed Windows run or whole-project security review was performed. Any actual code reuse still requires complete unit/license/notices review and ELIOT-specific lifecycle qualification.

## 4. Paseo: convenient profiles and bounded routing

**DOC, source inspected:**

- [`public-docs/agent-profiles.md`](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/agent-profiles.md).
- [`public-docs/hub/workflows.md`](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/hub/workflows.md).

The profile document combines provider, model, mode, thinking/features and `When to use`; changing a profile affects future selections, not launched agents. The workflow document restricts dynamic authority selection to finite complete named configurations rather than merging arbitrary provider/environment fragments. Capability declarations and the task prompt have separate jobs.

**DESIGN:** adopt that UX in Rust as runtime profiles and explicit candidate tuples. Expose selection reasons and fallbacks through the catalogue. Keep native option values native. Use ELIOT's existing Task/Attempt and execution grant, not Paseo's workflow runtime or TypeScript server. The inspected documentation does not prove ELIOT compatibility or all features in an installed Paseo release.

## 5. Windmill: active definitions instead of permanent script pins

**DOC:** [Draft and deploy](https://www.windmill.dev/docs/core_concepts/draft_and_deploy), [Roles and permissions](https://www.windmill.dev/docs/core_concepts/roles_and_permissions).

Useful distinctions: editing a draft does not modify the deployed runnable; deployment conflict detection is explicit; a script path refers to its latest deployed content while previous execution content remains identifiable. Service accounts and scoped runnable permissions separate automation identity from interactive users.

**DESIGN:** `get -> preview -> apply`, one active project configuration and named active scripts. Each new invocation resolves the active definition and records what it actually used; an existing invocation never changes underneath the process. Agents may author/configure inside a standing grant, with no Root approval round for every eligible edit. Permissions remain server-side: a limited UI does not stop API access.

Take the data/UX pattern, not Windmill's server, queues, database or non-Rust extension machinery. Its product deployment-history guarantee is not evidence that arbitrary external effects execute exactly once.

## 6. Temporal and Goose: narrower reusable ideas

**DOC:** [Temporal schedules](https://docs.temporal.io/schedule). Borrow the distinction between schedule and execution, pausing future starts and cancelling active work, overlap and catch-up. Keep ELIOT's latest-only default and explicit bounded alternatives. Do not import a Temporal cluster for local cron or call a durable timer exactly-once publication.

**CODE, prior source pass:** [Goose scheduler/common.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler/common.rs). `ValidatedScheduleRecipe`, bounded regular-file handling and retained `recipe_base_dir` are useful for captured external-script bundles. Do not import the separate `schedule.json` or agent scheduler. Entry-point bytes alone do not capture mutable imported support files.

The source commit is evidence of the reviewed unit, not an instruction to vendor or install that commit.

## 7. Native interfaces usable from Rust

**DOC:**

- [Codex app-server](https://developers.openai.com/codex/app-server/).
- [Claude CLI reference](https://code.claude.com/docs/en/cli-reference) and [hooks](https://code.claude.com/docs/en/hooks).
- [Gemini CLI hooks](https://geminicli.com/docs/hooks/reference/).
- [OpenCode plugin documentation](https://opencode.ai/docs/plugins/).

Codex's documented protocol is a Rust-client integration surface; its native IDs and supported discovery/events are preferable to terminal scraping. Claude documents structured input/output, partial/subagent forwarding and native hook-related facilities; a Rust process/codec adapter can use those documented boundaries without owning a Node SDK bridge. Individual installed capabilities still need readback and qualification; absence of a flag from help output alone is not conclusive capability discovery.

Async hooks do not veto a completed effect. Completion and Stop-style callbacks may have continuation semantics and must not become a hidden re-prompt loop. Gemini CLI support does not imply Gemini Spark/Antigravity support. Public OpenCode documentation must be matched to the actual installed V2 surface before mapping fields.

When a function exists only in a vendor-specific non-Rust plugin API with no suitable external interface, report the Rust integration gap. Do not ship an undisclosed JS internal subsystem, invent parity or claim a commercial SDK can simply be translated/copied. Native third-party binaries themselves are outside the ELIOT-owned language boundary.

## 8. GitHub API details that change the design

### 8.1 Work identity, webhook delivery and request pacing

**DOC:** [Webhook best practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST best practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api).

Verify raw-body HMAC and repository/installation context, durably adopt the delivery, then process it. Delivery redirection/replay is not new work. Use conditional/paged reconciliation and actual rate-limit/reset evidence. The webhook is not a trusted source of arbitrary executable instructions, and event header strings alone do not grant authority.

**DESIGN:** one stable Task origin per external item; source changes create revisions. One shared Rust GitHub port and pending-effect projection prevent per-agent polling and comment spam.

### 8.2 Audited state and Checks

**DOC:** [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

Checks address commits; annotation updates append. Required GitHub checks can accept neutral/skipped conclusions, so ELIOT must not represent an inconclusive audit that way. `external_id` is useful correlation, not a guaranteed deduplication key. GitHub alone can set its stale conclusion.

**DESIGN:** retain exact-candidate review evidence internally; labels are display only. Read back uncertain check creation/annotation batches. Report coverage and permissions by token/installation, not by assuming every token has identical capabilities. Several internal auditors sharing an App are not several independent GitHub user approvals.

### 8.3 Merge and integrated verification

**DOC:** [Pull request REST endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).

The merge API's `sha` guards the PR head, not arbitrary base movement. The documented async path returns a request UUID; enqueued is not merged. Async results expire after their documented retention period, so a missing old result needs PR/ref readback, not another merge. Stacked merges may include other PRs.

GitHub merge queues are restricted by repository ownership/plan and require merge-group checks. They are not a prerequisite for ELIOT's queue or publication path. Current ELIOT is user-owned; qualify actual repository capabilities rather than assume a native queue exists.

**DESIGN:** use one Rust publication queue; verify the actual integration candidate through the selected qualified path. A local mutex cannot exclude external GitHub writers. Never bypass repository rules or turn a manager confirmation into a nonexistent base-CAS guarantee.

### 8.4 Workflow triggering

**DOC:** [Triggering workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

A push using `GITHUB_TOKEN` and a push using an App token do not have the same downstream workflow behavior. The current documentation also has event-specific exceptions; do not generalize to 'every token-created event is ignored'.

**DESIGN:** determine required CI triggers at setup and make missing expected checks visible. Do not execute untrusted PR code with privileged workflow credentials merely to work around a missing status.

## 9. Owner evidence: failure cases, not copied policy

The supplied MANAGER-BRIEF is operational history with multiple revisions; current user instructions and repository policy govern this program. The supplied `Manager -> Orchestrator -> Executors` audit is labelled owner evidence, not a fresh measurement of each upstream donor.

| Recorded failure | Contract response |
|---|---|
| Several lines repeatedly selected the same Issue from an old queue | Stable origin, fresh readiness and atomic ownership reservation |
| A submitted result was missed because it used another marker filename | Applied typed submission, not arbitrary marker-file conventions |
| Late HOLD or review of A overwrote new submission B | Exact submission/candidate guards and historical late result |
| Corrected code remained blocked by an unchanged checklist | Bind findings/revalidation to relevant source and evidence, not checklist bytes alone |
| Comments/mentions triggered coordination-only model ping-pong | Typed workflow disposition; peer text cannot dispatch work |
| Parent ended while native children or Goal continued | One continuation/lifecycle owner; terminal wrapper is insufficient |
| Raw deltas duplicated final output and grew logs rapidly | Bounded live stream separate from terminal evidence |
| A malformed model-authored line stopped every queue | Validate and isolate one source/configuration error; retain last valid state |
| An implicit CLI health probe restarted a shared service | Direct qualified read-only native protocol, not health-check subprocesses |
| Labels said complete while audit evidence disagreed | Labels are projections; exact review/acceptance facts decide |
| Mixed Issues on one moving branch invalidated review | One manager worktree and one in-flight product candidate |

Do not import historical fixed version, compulsory time-limit or additional worktree recommendations from donor audits. They are overridden where the current product contract differs.

## 10. Reuse boundaries

Whole libraries: compatible maintained Rust GitHub client, cron evaluator, metrics and file watcher. Existing ELIOT Store/Operations/process/artifact foundations remain owners. Rust donor implementation units can be reused after full unit/license review; non-Rust products contribute patterns only. No copied code or dependency change occurs in this documentation PR.

Validation still needed: real native capabilities, Windows process ownership, runtime updates, model discovery and billing attribution, webhook/Checks/merge behavior with the selected installation, full review-repair-publication flow and load. No current donor source proves the entire ELIOT combination already works.
