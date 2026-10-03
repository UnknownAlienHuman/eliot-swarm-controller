# Configuration Contract — Agent-Friendly Automation and Runtime Preferences

Revision 2 · 2026-10-03 · proposed Rust application/MCP contract.

This document defines configuration for [Architecture](architecture.md) and the [Delivery preset](delivery.md). All examples are design schemas, not claims that current main accepts these fields. Logical handles and catalogue placeholders are resolved during local setup.

## 1. The small configuration path

An authorized agent should not have to wire a scheduler, six callbacks and three credentials merely to get code reviewed and published.

```text
swarm.tools.search: configure reviewed delivery
  -> automation.config.get + runtime.catalog
  -> automation.config.preview
  -> automation.config.apply
  -> automation.explain / swarm.dashboard
```

`automation.config.get` returns the active project definition, its revision, permitted editable fields, source/provenance, available presets and a bounded summary of grants/gaps. It never returns secrets or the complete fleet inventory.

`automation.config.preview` validates a typed desired configuration against the caller, project, selected pool, available runtimes/models, required checks and publication capabilities. It returns the effective diff, chosen routes, denied changes, future-versus-active effects and a plan digest. It makes no assignments, installs nothing and spends no model tokens. Catalogue refresh, when requested, uses only qualified read-only discovery.

`automation.config.apply` accepts the desired definition, `expected_revision`, `plan_digest` and caller-owned request ID. It rechecks mutable facts and atomically activates the valid project revision. Retry returns the original receipt. A stale preview returns a conflict and a fresh diff, not a silent merge over another agent's settings.

`automation.explain` answers why an item is running, waiting, returned, audited or awaiting publication, with its exact next eligible step and required permission/evidence. It does not ask another agent for an explanation or mutate the queue.

Runtime profiles have the analogous `runtime.profile.get/list/preview/apply` operations. They are independent configuration objects referenced by the project definition, not new MCP security profiles. A project configuration may atomically create/update its owned profiles as part of the same preview/apply plan; the small path must not require a separate round trip for every field.

## 2. One logical configuration authority

The existing Store owns activated configuration. A local TOML/JSON file is an import/export representation of the same typed document, not a second scheduler database. The user chooses MCP-managed configuration or an authorized file-managed source for that project. The source mode, revision and authority are explicit; two competing watchers must not overwrite each other.

A trusted file edit goes through the same validation and activation path. The watcher observes the parent directory so atomic replacement is detected; missed events are repaired by bounded readback. Invalid input preserves the last valid revision and publishes a diagnostic identifying the field. Never crash the host, silently set defaults, or stop all running agents because a file was temporarily half-written.

A repository file edited by a writer is a proposed configuration change, not automatic authority. Source selection, watched path and activation rights are setup-owned or delegated. Untrusted Issue text, PR content and tool output cannot select executables, grant network access or change audit/publication policy.

`expected_revision` protects editing concurrency. It is not a prescribed software release. An already admitted job retains its execution snapshot; the active configuration determines subsequent jobs.

## 3. Standard preset

`reviewed_delivery` installs a fixed typed path: eligible Task -> manager/executors -> applied submission -> required auditor slots -> repair or audited -> acceptance/publication policy -> bookkeeping. It starts no work until enabled and an applicable standing grant exists.

Users can set work order, profiles, review coverage, concurrency, eligible triggers, repair routing and publication mode without writing code. Scripts/custom rules extend it but are not necessary for the ordinary path. A custom rule cannot bypass its required candidate/audit checks.

Illustrative project configuration:

```toml
schema = "eliot-agent-operations"
project = "project-a"
preset = "reviewed_delivery"
enabled = true
execution_grant = "project-a-delivery"

[work]
pool = "manager-selected-issues"
order = "manager_order"
manager_profile = "implementation-manager"
writer_profile = "writer"
max_active_managers = 4
max_in_flight_per_manager = 1

[review]
profile = "auditor"
required_reviewers = 1
max_parallel_reviews = 2
coverage_policy = "project-current-phase"
repair_route = "original_owner_first"

[publication]
mode = "auto_after_audit"
route = "project-a-publication"
review_transport = "local_candidate"
close_issue = false

[notifications]
audit_submissions = true
wip_commit_notices = false
manager_exceptions_only = true

[profiles.implementation-manager]
role = "manager"
when_to_use = "Own one Issue, integrate writers, review diffs and submit the candidate."
apply_changes = "next_assignment"
fallback_on = ["capacity_unavailable"]
[[profiles.implementation-manager.candidates]]
route = "manager-primary"
model = "MODEL_FROM_MANAGER_CATALOG"
[profiles.implementation-manager.candidates.native_options]
effort = "EFFORT_FROM_MANAGER_CATALOG"

[profiles.writer]
role = "executor"
when_to_use = "Implement the manager's assigned non-overlapping code change."
apply_changes = "next_assignment"
fallback_on = ["capacity_unavailable"]
[[profiles.writer.candidates]]
route = "writer-primary"
model = "MODEL_FROM_WRITER_CATALOG"
[profiles.writer.candidates.native_options]
effort = "EFFORT_FROM_WRITER_CATALOG"

[profiles.auditor]
role = "auditor"
when_to_use = "Review the captured candidate against the assigned requirements."
apply_changes = "next_assignment"
fallback_on = ["capacity_unavailable"]
[[profiles.auditor.candidates]]
route = "review-primary"
model = "MODEL_FROM_REVIEW_CATALOG"
[profiles.auditor.candidates.native_options]
effort = "EFFORT_FROM_REVIEW_CATALOG"
```

The example is syntactically complete but catalogue placeholders are not launchable model IDs. Setup/agent catalogue selection replaces them with a provider's actual accepted ID or alias. `4`, `1` and `2` are editable example capacities, not fixed fleet limits; one in-flight mutable product candidate per manager is an ownership rule.

`publication.mode = "manager_gate"` changes only the final decision path. `route` references a locally configured publication target/method and permitted repository scope; it is not an arbitrary URL, refspec or shell command. `review_transport = "local_candidate"` audits before push. The optional `review_branch` mode requires the distinct candidate-upload permission from Delivery.

`close_issue = false` avoids equating code audit with full project closure. Projects may enable closure with an explicit completion contract and permission; this is a visible setting, not a hardcoded prohibition.

## 4. Runtime catalogue and preference resolution

`runtime.catalog` returns bounded pages of installed/configured executors and their observed capabilities, models/aliases, native options, transport, effective privilege/trust, account-capacity group and freshness. Discover from installed protocols/manifests and qualified read-only inventory, not a hardcoded model list in Rust.

Keep separate:

```text
ELIOT role                  responsibility and application permissions
route / executor            chosen native harness and Rust adapter
model / provider            requested model, alias and billing path
native_options              harness-native effort/mode/tool settings
MCP profile / surface       authorized tools and their presentation
OS execution profile        actual filesystem/network/process authority
```

The same model through two providers can have different quota, pricing and semantics. Neither a role label nor a model name determines transport or permission.

Resolution order is host security ceiling -> activated project policy -> role profile -> authorized per-Task overrides. Lower levels cannot relax higher-level restrictions. Model/effort options remain native values; do not pretend `high`, `xhigh` or `max` mean the same thing in every harness.

A profile's candidate list is ordered. Each entry is a complete compatible route/model/options tuple, not fragments blended across providers. The resolver records requested and effective values, selection reason, fallback used, catalogue freshness and missing capabilities. Human-readable `when_to_use` guides selection; it cannot grant capabilities or add Task requirements.

A caller may choose an explicit native model ID or provider-supported alias. No default profile contains a frozen product-version requirement. Preferred aliases are resolved again for a new assignment; a running assignment's model is never silently switched because the alias changed upstream.

Fallback is opt-in and limited to named eligible causes and candidate routes. Default examples allow unavailable capacity, not all errors. An authentication failure, unknown delivery or missing security capability never silently reroutes to another provider. No fallback buys credits, changes billing source, lowers required review coverage or enables a paid model outside the grant.

Rate limits and subscription quotas are tracked by their actual shared account/provider scope. Two routes using the same account do not obtain two independent budgets. Unknown quota is not infinite capacity. Use native reset/retry evidence; do not classify every HTTP 429 as the same condition.

## 5. Choosing and updating executables without pins

The local runtime registry resolves installed executables through trusted installation locations or documented vendor discovery. Remote tools normally select the resulting logical handle, not a hash-named path or command string. An agent with explicit local setup rights can select another installed executable/transport through preview/apply; routine preference changes do not require reinstalling anything.

Accept a newer compatible native runtime based on documented protocol and capability evidence, not exact version equality. Retain observed versions for diagnosis. Tolerate additive native event fields; unknown consequential variants remain unsupported for that operation. ELIOT-authored configuration is stricter: unknown keys are errors to catch typos rather than silently ignore them.

New transport/core feature requirements must be satisfied before launch. A process starting successfully is not proof that result delivery, required MCP tools or hook callbacks work. The launcher records actual capabilities. Optional stream gaps do not block unrelated implementation; missing required submission/reporting paths do.

There is no automatic downgrade to an old CLI or crate to make an outdated document true. Update the Rust adapter and build/toolchain support as an explicit code delivery. Dependencies use normal compatible release requirements; exact `=version`/Git revision restrictions are not the default. Do not replace them with unchecked wildcard downloads at runtime. Cargo's recorded build resolution and diagnostic provenance do not become a runtime restriction forbidding newer compatible installations.

No installer runs at a timer/hook event. The owner controls system-level installations and services. Updating preferences does not restart native services, change PATH or rewrite an active process's dependencies.

## 6. Safe changes while work is running

| Change | Effect |
|---|---|
| Queue priority within authorized pool | Future admission; existing ownership is preserved |
| Writer/auditor preferred model or executor | Next assignment; live work keeps its actual route |
| Parallelism increased | Additional eligible starts within grant/capacity |
| Parallelism decreased or project paused | Drain future admission; no heuristic termination |
| Active script changed | Next new invocation resolves new content; old request retries keep their original receipt |
| New/stronger required audit | Applies according to explicit transition preview; an old verdict is not silently reused |
| Grant/credential revoked | New calls/effects recheck immediately; uncertain effects are read back |
| Optional display/retention setting | May apply immediately without deleting referenced evidence |
| Existing native Goal owner changed | Supported pause/clear and reconciliation before new continuation owner |

The preview must list affected queued work and which operations are already beyond cancellation. A config change cannot retroactively undo a push, invalidate evidence by deleting it or turn an unknown model delivery into a safe retry.

Native hot model/options updates, where genuinely supported, remain a separate explicit `agent.configure` operation with its own effective-boundary/readback contract. They are not a side effect of editing a future-default profile.

## 7. Scripts and schedules use active selectors

A normal schedule action references `script_id = "project-a/report-changes"` and `selector = "active"`, not a permanently frozen script release. At each new occurrence, ELIOT resolves the active definition and records the content/environment used for that invocation. Script updates do not replay completed occurrences.

Users may stage script revisions, validate them and activate them inside their authorized trust envelope. Support files are captured for running-code integrity. No mandatory approval for every harmless edit within an existing grant; increasing OS/network/secret/publication authority requires the corresponding delegable right.

Built-in notify/review/publish actions are selected by registered action kind and typed arguments. They do not need a script file. New schedules, event rules and Goal settings follow the same preview/apply validation and current scope checks. A reminder remains a notice unless an explicit granted execution action is attached.

## 8. Custom roles without permission surprises

Role presets are editable capability bundles within grant limits. Managers can choose profiles for executors and auditors, assign multiple auditors and define project-local specialist roles when delegated to do so. An auditor cannot lower its own required coverage, replace its immutable candidate or turn a pass into a publication right.

Configuration tools return `editable_fields` and precise denied changes. A request within existing rights applies without another human turn. An expansion returns the smallest missing capability/scope and its appropriate approver, not a generic instruction to ask Root about everything.

Approval is bound to the requested scope/effects, not a model's friendly role name. Loading a deferred tool, changing a profile, selecting another CLI or following a repository-supplied workflow never widens the grant.

## 9. Deferred tool groups

Normal eager cores remain those from PR #22. Discover these only as needed:

| Group | Canonical convenience methods |
|---|---|
| automation-config | `automation.config.get`, `automation.config.preview`, `automation.config.apply`, `automation.explain` |
| runtime-profiles | `runtime.catalog`, `runtime.profile.get`, `runtime.profile.list`, `runtime.profile.preview`, `runtime.profile.apply` |
| review-delivery | `review.assign`, `review.get`, `review.submit`, plus existing submission/acceptance methods when authorized |
| schedules / scripts / hooks / goals | Existing proposed typed management operations from Architecture |
| GitHub / forge | Work-pool/source reads and separately authorized projection/upload/publication operations |

One application method has one MCP tool mapping. Dot-form names in these documents are application names; use the existing facade's validated wire-name mapping consistently, without introducing duplicate public aliases. No `execute_arbitrary_tool` or mode-switching mega-tool.

Configuration objects are small enough to inspect, diff and edit in one exchange. Long history, logs, source documents and full model/tool catalogues remain paged or referenced. The agent receives actionable validation errors and can repair its proposed configuration without triggering work while experimenting.
