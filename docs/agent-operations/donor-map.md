# Donor Map — Rust Operations and Automation on Behalf of a Manager

Revision 4 · reviewed 2026-10-03.

`CODE` means source inspected in the indicated review; `DOC` official/library documentation; `OWNER_AUDIT` supplied operating evidence; `DESIGN` ELIOT's choice. Earlier source-review observations are not new live tests. Dates, commits and blob IDs identify evidence, never installation requirements or software-version pins.

## 1. Product choice versus donor evidence

**DESIGN, current owner direction:** manual management is the baseline; each manager can enable automations that act on their behalf. There is no global assisted/delegated mode or extra approval layer for actions that manager already controls. This is the product rule, not a benchmark finding or a donor default to be inferred.

The previous Revision 3 added unnecessary separate mode/stage/control state. Revision 4 replaces those prescriptions in the six existing documents. Useful receipt, concurrency, revocation and unknown-effect protections remain implementation details of the same action path.

## 2. ELIOT source anchors

Original inspection used `35e499ae73b622d873c44873f6993ee3fcbea87b`; the current review uses main `504199d14135c030ad3951a3c5023a098a3d03f0`. Inspect current main when implementation starts.

| Existing unit | Source finding | Adaptation |
|---|---|---|
| [`src/store/submissions.rs`](../../src/store/submissions.rs) | Queued reserve; applied `task.submission` in finish; `request_changes` uses GM/operator authority and stores mail | Trigger review only from applied evidence. Add intended scoped manager capabilities to the common handler, not an automation-only bypass. |
| [`src/policy.rs`](../../src/policy.rs) | Compiled current edition/digest | Keep historical Attempts recognizable when adopting explicit policy changes. |
| [`src/scheduler.rs`](../../src/scheduler.rs), [`src/store/schedules.rs`](../../src/store/schedules.rs) | Once/interval CheckRuns with transactional receipts | Extend the existing Rust scheduler with manager-owned definitions; preserve considered occurrences. |
| [`src/mcp/subscriptions.rs`](../../src/mcp/subscriptions.rs) | Bounded committed-fact polling and lag/resync | Share readers/projectors; keep native live presentation separate. |
| [`docs/forge-publication.md`](../forge-publication.md) | Accepted-candidate non-force push; uncertain-effect readback; preflight not atomic old-ref CAS | Manual and automatic callers retain these same limits. |
| [`docs/owner-decisions.md`](../owner-decisions.md) | Manager workspace ownership, observational reads and separate GM/host gates | Automation exercises its owning manager's rights, not all permissions of the host process. |
| Runtime/process ownership and [`modules`](../../modules) | Some owned bridges are non-Rust; lifecycle ownership already matters | Port owned translation to Rust without heuristic live-session replacement. |

The current submissions source was reread in this pass. It confirms that simply adding an auditor MCP tool would not create a working scoped feedback path; that needs a real shared authorization change. A retained review finding also does not itself send native input.

## 3. Whole Rust libraries

| Component | Maintained source | Reuse | Keep in ELIOT |
|---|---|---|---|
| Octocrab | [API](https://docs.rs/octocrab/latest/octocrab/) | Complete Rust GitHub client and narrow lower-level transport where needed | Credentials, current manager permissions, shared rate budgets and effect recovery. |
| Croner | [API](https://docs.rs/croner/latest/croner/) | Complete expression evaluation and compatible timezone integration | Enabled owner, due identity, catch-up, overlap and persistence. |
| sysinfo | [API](https://docs.rs/sysinfo/latest/sysinfo/) | Shared selective resource/process sampling | Actual lifecycle ownership; metrics never authorize killing. |
| notify | [API](https://docs.rs/notify/latest/notify/) | Platform watcher and supported fallback | Exact readback, revisions and source-health reporting. |
| Existing Tokio/rusqlite/RMCP/serde | Project manifest and maintained documentation | Existing async/Store/protocol machinery | One authority, not a parallel controller per feature. |

Octocrab is a community client, not GitHub authority. Missing typed endpoints do not justify a Python/`gh` control daemon. Metrics and file events have platform limitations; unknown coverage is not an empty successful result or an exactly-once event stream.

**DOC:** [Cargo dependency requirements](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html). Use ordinary compatible requirements and explicit integration/toolchain changes when needed. No obsolete release recommendation, exact-version runtime gate, uncontrolled wildcard download or callback-time installer. Recorded build resolution is not a policy freezing future compatible software.

## 4. Windmill: the directly relevant on-behalf pattern

**DOC, rechecked 2026-10-03:** [Roles and run on behalf](https://www.windmill.dev/docs/core_concepts/roles_and_permissions), [Jobs](https://www.windmill.dev/docs/core_concepts/jobs), [Schedules](https://www.windmill.dev/docs/core_concepts/scheduling), [Draft/deploy](https://www.windmill.dev/docs/core_concepts/draft_and_deploy).

Windmill distinguishes the creator of a job from its permission-bearing identity. Its documented on-behalf setting uses that selected identity's access and makes attribution visible. Schedules/triggers associate executions with an owner; schedules can be enabled independently and scripts resolve deployed content.

**DESIGN:** adopt explicit owner, enabled setting and permission/technical-executor attribution. A manager enables their helper once; no per-trigger approval is needed. Reuse ELIOT's existing authorization and Operations rather than Windmill's queues or database.

Do not copy ownership changes based merely on the last editor. In ELIOT, editing a script or imported file cannot quietly make a run act for a more privileged identity. Owner changes use authenticated management authority. Script runs receive scoped invocation access, not an unrestricted copy of the manager credential.

Windmill documentation supports the pattern, not the claim that ELIOT's Rust implementation, external effects or isolation have already been qualified. Its other implementation languages and defaults do not override Rust-only internals or ELIOT's default-off choices.

## 5. Focused Rust donor: OpenCnid/Symphony

[OpenCnid/symphony](https://github.com/OpenCnid/symphony) is an independent Rust implementation, not a claim about the language of OpenAI's reference. These units were inspected in the preceding source pass:

| Unit and evidence identity | Useful behavior | Do not inherit blindly |
|---|---|---|
| [`src/watch.rs`](https://github.com/OpenCnid/symphony/blob/main/src/watch.rs), blob `4a41dbf7c25cda53a61e9c40e4623e3b5e302664` | Parent-directory watching, atomic-save handling, debounce and owner-triggered revalidation | Ignored watcher errors; ELIOT reports gaps and rejects stale revisions. |
| [`src/workflow.rs`](https://github.com/OpenCnid/symphony/blob/main/src/workflow.rs), blob `c968e53123f311720bd23a34d0822f95337270b1` | Separate configuration, prompt and source directory | Unbounded file read and an unnecessary new YAML/prompt authority. |
| [`src/agent/claude_code.rs`](https://github.com/OpenCnid/symphony/blob/main/src/agent/claude_code.rs), blob `fa76bcffe0eeda62a6f270491c164978dbfda5b4` | Rust structured process/stdio path and workspace validation | Requested ID mistaken for observed identity, optional bridge failure masking missing reporting, root exit mistaken for child cleanup. |

Reuse only after complete unit/license/notices review and ELIOT-specific qualification. This source pass establishes no whole-project fleet or Windows reliability guarantee.

## 6. Paseo profiles

**DOC, previously inspected source:** [Agent profiles](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/agent-profiles.md), [Hub workflows](https://github.com/getpaseo/paseo/blob/5375f43a051c724d080e41efd73e84ccb6082ff5/public-docs/hub/workflows.md).

Profiles group provider/model/native options and when-to-use notes; choices can be whole named configurations rather than incompatible fragments. New defaults concern future selections, not silently mutated running sessions.

**DESIGN:** take this UX for Rust runtime profiles and expose requested/effective route values. Manager model preferences do not turn on unrelated automations. Notes guide choice, not scope or permission. Do not import the TypeScript control plane or attribute main-only source to an installed stable product. Source commits here are evidence, not dependency pins.

## 7. Pause, manual invocation and external owners

### Temporal

**DOC, preceding recheck:** [Schedules](https://docs.temporal.io/schedule). Schedule definition, manual invocation, already-started execution, overlap and catch-up are distinct.

**DESIGN:** disabling recurrence stops future starts, not the existing execution; direct manual actions stay usable. Reuse semantics, not a Temporal cluster or its defaults. ELIOT settings persist across normal restart; current ownership/unknown-effect checks still precede new effects. Intentional re-enable does not replay all historical events.

### GitHub Actions

**DOC, preceding recheck:** [Disable/enable workflows](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows), [Cancel a run](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/cancel-a-workflow-run).

**DESIGN:** maintain exact external request/run identity. Disabling a local automation does not modify repository settings or cancel a remote merge/run. Offer supported cancellation separately and read back the result; do not display quiescence while another owner can still act.

### Claude Agent Teams

**DOC, preceding recheck:** [Agent Teams](https://code.claude.com/docs/en/agent-teams). The documented opt-in feature and lighter alternatives support a useful manually managed path rather than requiring every project to run a workflow engine.

**DESIGN:** retain direct peer collaboration and visibility. Native team availability and agent count do not enable ELIOT automations. Backend tool/stream/continuation capabilities still need real qualification.

### Goose

**CODE, prior pass:** [scheduler/common.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler/common.rs). Bounded recipe capture and its source base directory are useful for external-script invocation integrity. Do not import another schedule registry, assume the entrypoint captures imported dependencies, or require that old commit for installation.

## 8. Native interfaces

**DOC:** [Codex app-server](https://developers.openai.com/codex/app-server/), [Claude CLI](https://code.claude.com/docs/en/cli-reference), [Claude hooks](https://code.claude.com/docs/en/hooks), [Gemini CLI hooks](https://geminicli.com/docs/hooks/reference/), [OpenCode plugins](https://opencode.ai/docs/plugins/).

These are integration references, not proof of installed support. Use documented protocols from Rust; verify required reports/tools/callbacks. Public OpenCode APIs must match the actual V2 route. Gemini CLI does not establish Spark support. Missing private/non-Rust-only capabilities are named gaps.

Async after-hooks are not vetoes. Native Goal/Stop/child events may continue work independently of the observing client. Preserve actual continuation/lifecycle ownership and do not infer completion from wrapper exit.

## 9. GitHub contracts retained

- **Intake:** [webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [REST practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api). Authenticate raw delivery/context; deduplicate, page and reconcile. Source content is not a manager command.
- **Review:** [Check runs](https://docs.github.com/en/rest/checks/runs), [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches). Exact commit and remote IDs matter. Append-style annotations require readback-aware retry. Neutral/skipped behavior is not evidence of an ELIOT pass; labels never carry exact-candidate authority.
- **Merge:** [PR endpoints](https://docs.github.com/en/rest/pulls/pulls), [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue). Head, base and actual integrated result are different. Queue/async acknowledgements are not landed commits. Qualify available repository/endpoint behavior; a native merge queue is not a product prerequisite.
- **Workflow triggers:** [Trigger workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow). Token/event combinations affect expected checks. Do not fix a missing check by running untrusted code with privileged workflow credentials.

**DESIGN:** one Rust GitHub/effect boundary, actual authenticated App/user attribution, explicit on-behalf manager context internally, no protection bypass and no blind external-effect replay. ELIOT cannot fabricate a human author or global base-CAS exclusion from a local lock. Revalidate endpoint schemas while implementing; prior research is not a permanent API-version constraint.

## 10. Owner operating evidence

The supplied MANAGER-BRIEF/control-plane audits describe past incidents and changing historical decisions. Current user instructions govern this program; do not copy old pins, mandatory timers or fixed launch counts.

| Incident | Design response |
|---|---|
| Stale queues caused duplicate work | One current reservation across manual and automatic callers. |
| Late review A changed submission B | Exact candidate anchoring and retained historical late findings. |
| Native children/Goal revived an old manager | One lifecycle owner; not client/wrapper-based termination. |
| Stop ended wrappers but left writers | Honest in-flight/unknown ownership and addressed cancellation. |
| Reminder replaced the current task | Notification is information, not implicit assignment. |
| Live script edits changed execution | Retain admitted bundle; next independent run resolves active content. |
| Malformed data stalled every queue | Isolate that source/definition and keep last valid state. |
| Old deliveries overwhelmed fresh work | Current eligibility and semantic dedupe, not global history replay. |
| Scripts became the only usable work path | First-class manual handlers in Rust, with optional automatic callers. |

No donor proves the complete combination already works. Future qualification must cover owner attribution/current permissions, independently enabled entries, manual coexistence, disable/start races, restart recovery, stale candidate handling, actual native capabilities, Windows process ownership and GitHub readback. This documentation PR runs none of those live workflows.
