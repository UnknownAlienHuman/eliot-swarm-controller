//! MCP facade over the same application API used by the CLI.
//!
//! The General Manager's MCP client launches `swarm mcp` as a child process
//! and speaks MCP over stdio. This process is only a client of the running
//! host: it forwards each tool call over the existing local IPC with the
//! configured credential, exactly like a CLI invocation. It never opens the
//! database, never opens a TCP listener, and closing it does not stop the
//! host or cancel admitted work (donor qualification points for `api.mcp`).
//!
//! Tool surface: one tool per public application method, named after the
//! method with `.` replaced by `_`. There is deliberately no universal
//! passthrough or shell tool: every tool forwards to exactly one typed
//! application method, and the application layer keeps validating the
//! original request (field allow-lists, idempotency, role checks).
//!
//! Mutations follow the CLI's request-ID discipline. Restricted profiles
//! require a caller-known `client_request_id`, chosen and retained before
//! dispatch, so the caller can reconcile a lost reply without a hidden
//! retry. The explicit local `full` compatibility profile may omit it; in
//! that case one is generated and echoed only when a result arrives. A
//! failed transport is dropped, never silently retried: the next tool call
//! reconnects first.
//!
//! Tasks projection (R20, Documentation Program §17.1): the server
//! advertises the `io.modelcontextprotocol/tasks` extension. When the
//! client declares the same extension, a mutation whose Operation is
//! still in flight returns an MCP task seed whose `taskId` is the
//! existing Operation's ID; the Operation stays the single authority
//! for state, acceptance and completion, and this facade stores no task
//! state of its own. `tasks/get` projects the Operation read model
//! (`operation.get`, plus the Operation's exact pending native-input
//! attention items from `report.attention`, mapped to elicitation input
//! requests). `tasks/cancel` submits the existing addressed
//! `operation.cancel` — which only cancels queued Operations — so an
//! in-flight Operation refuses with the store's own reason instead of
//! inventing a parallel cancel path. RMCP's `TaskManager` is never
//! used: nothing in this facade executes work a second time. Clients
//! that do not declare the extension receive exactly the pre-Tasks
//! responses (the durable Operation handle in the structured result).
//! `tasks/update` is deliberately not implemented: a pending native
//! input is answered by the addressed `agent_reply` tool call, which is
//! a durable Operation of its own, not by a free-form elicitation
//! response.
//!
//! Subscriptions (R20, §17.3): the facade advertises the
//! `eliot/subscriptions` extension and answers two protocol methods of
//! its own, `eliot/subscribe` / `eliot/unsubscribe` (custom requests,
//! not tools). A subscription is a bounded, read-only freshness hint
//! over committed facts only — committed report transitions, mailbox
//! deliveries and Operation state transitions, all filtered from the
//! one committed observation stream `report.delta` reads, never from
//! a volatile or native live stream. Each subscription's queue holds
//! at most a fixed number of undelivered notifications; on overflow
//! the subscriber receives exactly one explicit `lagged` marker for
//! the skipped range and resyncs through the exact reads
//! (`report.delta` / `message.read` / `operation.get`) from the last
//! delivered cursor. Notifications carry the S2 projection frame of
//! the page they were read from, so a subscriber can detect gaps
//! itself. Subscriptions die with the session: a reconnect is not
//! replay continuity, and state is re-established by cursor + resync.
//! The pollers share one dedicated IPC connection, separate from the
//! tool-call connection, under the same link discipline (see
//! `subscriptions.rs` for the full contract and the RMCP seam).
//! Fact: the facade keeps no cache; authoritative reads are forwarded
//! to the host with no stale-on-error caching.

use crate::{
    config::{Config, Ipc, McpToolProfile},
    error::{Error, Result},
    ipc,
    model::{self, Credential},
};
use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams, ContentBlock,
        CreateTaskResult, CustomRequest, CustomResult, DetailedTask, ElicitRequest,
        ElicitRequestParams, ElicitationSchema, ExtensionCapabilities, GetTaskParams,
        GetTaskResult, Implementation, InputRequest, InputRequests, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, Task as McpTask, TaskPayload,
        Tool, ToolAnnotations,
    },
    service::{Peer, RequestContext, RoleServer},
    transport::stdio,
};
use serde_json::{Map, Value, json};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

type JsonObject = Map<String, Value>;

#[derive(Clone, Copy)]
struct Field {
    name: &'static str,
    kind: &'static str,
}

const fn f(name: &'static str, kind: &'static str) -> Field {
    Field { name, kind }
}
const S: &str = "string";
const SN: &str = "string_or_null";
const I: &str = "integer";
const IN: &str = "integer_or_null";
const B: &str = "boolean";
const O: &str = "object";
const A: &str = "array";

struct ToolSpec {
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
}

const fn read(
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
) -> (bool, ToolSpec) {
    (
        true,
        ToolSpec {
            method,
            description,
            fields,
            required,
        },
    )
}
const fn mutation(
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
) -> (bool, ToolSpec) {
    (
        false,
        ToolSpec {
            method,
            description,
            fields,
            required,
        },
    )
}

/// (read_only, spec). The read/write split matches `Store`'s read
/// classification plus the CLI's
/// request-ID treatment (`agent.result` and `host.mode` are mutations).
static TOOLS: &[(bool, ToolSpec)] = &[
    // Read-only methods.
    read(
        "swarm.tools.search",
        "Search the caller's authorized MCP catalog by goal, purpose, or exact tool name. Returns metadata and a safe surface/reconnect recommendation only; it never executes a selected method.",
        &[
            f("query", S),
            f("purpose", S),
            f("task_id", S),
            f("exact_method", S),
            f("loaded_catalog_revision", S),
            f("max_results", I),
        ],
        &["query"],
    ),
    read(
        "swarm.context.get",
        "Read the authenticated Participant's current Task/Attempt context or a manager-selected exact assignment context, including relevant cards and selector-bounded peer discovery.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("contract_key", S),
            f("path", S),
            f("symbol", S),
            f("interface", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &[],
    ),
    read(
        "swarm.dashboard",
        "Read one bounded retained-facts dashboard. Observer results are aggregate-only and omit assignment details and manager exceptions.",
        &[f("limit", I)],
        &[],
    ),
    read(
        "swarm.queue.get",
        "Page the current Task queue with optional project and state filters; queue ordering is the stored creation order, not a priority score.",
        &[
            f("after", I),
            f("limit", I),
            f("project_id", S),
            f("task_state", S),
        ],
        &[],
    ),
    read(
        "swarm.agent.inspect",
        "Inspect one exact Attempt and its bounded binding, operation, check, peer, and overlap neighborhood.",
        &[
            f("attempt_id", S),
            f("operation_after", I),
            f("operation_limit", I),
            f("check_after", I),
            f("check_limit", I),
            f("peer_after_client_id", S),
            f("peer_limit", I),
            f("overlap_after", I),
            f("overlap_limit", I),
        ],
        &["attempt_id"],
    ),
    read(
        "swarm.exceptions.get",
        "Page manager-actionable items from the bounded attention projection; results include explicit coverage gaps.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "swarm.launch.preview",
        "Preview one exact Task launch configuration under current manager authority. This validates configuration only; it never starts a model or native session.",
        &[
            f("task_id", S),
            f("expected_task_revision", I),
            f("route", S),
            f("agent_profile", S),
            f("mcp_profile", S),
            f("mcp_surface", S),
            f("workspace_policy", S),
            f("requested_model", SN),
            f("requested_effort", SN),
            f("budget", O),
            f("stop_conditions", A),
            f("purpose", S),
        ],
        &[
            "task_id",
            "expected_task_revision",
            "route",
            "agent_profile",
            "mcp_profile",
            "mcp_surface",
            "workspace_policy",
            "requested_model",
            "requested_effort",
            "budget",
            "stop_conditions",
            "purpose",
        ],
    ),
    mutation(
        "swarm.launch",
        "Submit one exact, digest-bound launch plan under Manager or local Operator authority. This records a durable intent; current execution remains blocked until trusted workspace admission is available.",
        &[
            f("task_id", S),
            f("expected_task_revision", I),
            f("route", S),
            f("agent_profile", S),
            f("mcp_profile", S),
            f("mcp_surface", S),
            f("workspace_policy", S),
            f("requested_model", SN),
            f("requested_effort", SN),
            f("budget", O),
            f("stop_conditions", A),
            f("purpose", S),
            f("plan_digest", S),
        ],
        &[
            "task_id",
            "expected_task_revision",
            "route",
            "agent_profile",
            "mcp_profile",
            "mcp_surface",
            "workspace_policy",
            "requested_model",
            "requested_effort",
            "budget",
            "stop_conditions",
            "purpose",
            "plan_digest",
        ],
    ),
    read(
        "coordination.participant.get",
        "Read one participant registration only within an authenticated current Task/Attempt scope.",
        &[
            f("client_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
        ],
        &["client_id"],
    ),
    read(
        "coordination.participant.list",
        "Page the redacted participant roster for one exact manager-owned Task/Attempt scope.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &["task_id", "task_revision", "attempt_id"],
    ),
    read(
        "coordination.peer.find",
        "Find current participants only by one exact contract, path, symbol, or interface selector in the authenticated scope; never walks the roster.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("contract_key", S),
            f("path", S),
            f("symbol", S),
            f("interface", S),
            f("fields", A),
            f("limit", I),
            f("after_client_id", S),
        ],
        &[],
    ),
    read(
        "swarm.overlap.check",
        "Compare bounded selector facts for possible overlap in the caller's current scope; missing Git or workspace evidence remains unknown.",
        &[
            f("task_id", SN),
            f("task_revision", IN),
            f("attempt_id", SN),
            f("paths", A),
            f("symbols", A),
            f("contracts", A),
            f("candidate_ref", SN),
        ],
        &[],
    ),
    read(
        "coordination.work_card.get",
        "Read the caller's current work card, or a selected participant's card in an exact manager-owned scope.",
        &[
            f("participant_id", S),
            f("fields", A),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &[],
    ),
    read(
        "coordination.work_card.list",
        "Page current work cards matching exactly one relationship selector in the caller's scope.",
        &[
            f("contract_key", S),
            f("path", S),
            f("symbol", S),
            f("interface", S),
            f("fields", A),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &[],
    ),
    read(
        "coordination.contract_card.get",
        "Read one contract card by exact key, or the bounded matching cards in an exact manager-owned scope.",
        &[
            f("contract_key", S),
            f("participant_id", S),
            f("fields", A),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &["contract_key"],
    ),
    read(
        "coordination.contract_card.list",
        "Page current contract cards by one exact contract key in the caller's scope.",
        &[
            f("contract_key", S),
            f("fields", A),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_client_id", S),
        ],
        &["contract_key"],
    ),
    read(
        "coordination.inbox",
        "Read bounded durable deliveries addressed to the authenticated Participant in its exact current scope.",
        &[f("limit", I), f("after_operation_id", S)],
        &[],
    ),
    read(
        "coordination.watch.list",
        "Page one-shot terminal-operation watches in the authenticated Participant scope or an exact Manager/Operator Task/Attempt scope.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("limit", I),
            f("after_watch_id", S),
        ],
        &[],
    ),
    read(
        "review.get",
        "Read one immutable review assignment visible to its exact assigned reviewer or the authorized manager/operator.",
        &[f("review_assignment_id", S)],
        &["review_assignment_id"],
    ),
    read(
        "review.list",
        "Page review assignments after filtering to the caller's authorized assignment scope.",
        &[
            f("task_id", S),
            f("attempt_id", S),
            f("submission_ref", S),
            f("after", I),
            f("limit", I),
        ],
        &[],
    ),
    read(
        "swarm.review.context",
        "Read the exact assigned-review packet and immutable submission/candidate metadata; content bytes remain behind artifact.read.",
        &[f("review_assignment_id", S)],
        &["review_assignment_id"],
    ),
    read(
        "automation.config.get",
        "Page revisioned automation definitions owned by the authenticated Manager and scoped to one project. The current GM may optionally select owner_manager_id to recover another Manager's retained state in that same project; the original owner is preserved.",
        &[
            f("project_id", S),
            f("owner_manager_id", S),
            f("after", I),
            f("limit", I),
        ],
        &["project_id"],
    ),
    read(
        "automation.config.preview",
        "Validate an automation change plan and return its digest and conflicts without applying it.",
        &[f("project_id", S), f("changes", A)],
        &["project_id", "changes"],
    ),
    read(
        "automation.config.explain",
        "Explain one automation's dispatch state and linked operations by exact project and automation ID. The current GM may optionally select owner_manager_id to recover another Manager's retained state; the original owner is preserved.",
        &[
            f("project_id", S),
            f("automation_id", S),
            f("owner_manager_id", S),
        ],
        &["project_id", "automation_id"],
    ),
    read("host.status", "Controller status snapshot.", &[], &[]),
    read(
        "route.list",
        "Configured routes; not live qualification.",
        &[],
        &[],
    ),
    read(
        "client.list",
        "Registered clients (operator only).",
        &[],
        &[],
    ),
    read(
        "task.get",
        "One task with its current attempt state.",
        &[f("task_id", S)],
        &["task_id"],
    ),
    read(
        "task.list",
        "Page tasks by offset.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "task.submission",
        "One immutable submission with paged requirement claims.",
        &[f("submission_ref", S), f("after", I), f("limit", I)],
        &["submission_ref"],
    ),
    read(
        "task.acceptance",
        "One acceptance decision and its current/revoked status.",
        &[f("acceptance_operation_id", S)],
        &["acceptance_operation_id"],
    ),
    read(
        "attempt.get",
        "One attempt and its disposition.",
        &[f("attempt_id", S)],
        &["attempt_id"],
    ),
    read(
        "operation.get",
        "One operation: the durable handle for async work.",
        &[f("operation_id", S)],
        &["operation_id"],
    ),
    read(
        "operation.list",
        "Page operations, optionally filtered by state.",
        &[f("after", I), f("limit", I), f("state", S)],
        &[],
    ),
    read(
        "agent.state",
        "Observed state of one binding generation.",
        &[f("binding_id", S), f("generation", I)],
        &["binding_id", "generation"],
    ),
    read(
        "agent.list",
        "Page known bindings with their observed state.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "agent.family",
        "A retained family observation; not a live query or a complete inventory claim.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("observation_id", I),
            f("after", I),
            f("limit", I),
        ],
        &["binding_id", "generation"],
    ),
    read(
        "check.get",
        "One check run and its evidence state.",
        &[f("check_id", S)],
        &["check_id"],
    ),
    read("check.profiles", "Configured check profiles.", &[], &[]),
    read(
        "artifact.get",
        "Artifact metadata.",
        &[f("artifact_id", S)],
        &["artifact_id"],
    ),
    read(
        "artifact.read",
        "One byte range of an immutable artifact.",
        &[
            f("artifact_id", S),
            f("offset_bytes", I),
            f("length_bytes", I),
        ],
        &["artifact_id"],
    ),
    read(
        "artifact.parts",
        "Page the provenance manifest of a whole assembled result.",
        &[f("artifact_id", S), f("after", I), f("limit", I)],
        &["artifact_id"],
    ),
    read(
        "report.delta",
        "Incremental report entries after a cursor.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "report.attention",
        "Page controller-owned attention items.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "report.capacity",
        "Page active and reserved capacity accounting.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "message.read",
        "Read directed mailbox messages after a cursor; reading does not delete.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    // Mutations. Restricted profiles require a stable client_request_id;
    // only the explicit local Full compatibility profile permits omission.
    mutation(
        "coordination.participant.register",
        "Register a Participant for one exact current Task/Attempt. Send only the precomputed token hash; keep the raw credential local. A sponsored reviewer remains unusable until bound to its exact review slot.",
        &[
            f("client_id", S),
            f("token_hash", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("participation_basis", O),
            f("binding_id", SN),
            f("binding_generation", IN),
            f("native_session_id", SN),
            f("display_alias", SN),
            f("inbound_policy", SN),
            f("review_profile", SN),
        ],
        &[
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis",
        ],
    ),
    mutation(
        "coordination.participant.disable",
        "Disable one registered Participant under the exact Attempt manager; the grant revision advances and coordination history is retained.",
        &[f("client_id", S), f("expected_grant_revision", I)],
        &["client_id"],
    ),
    mutation(
        "coordination.work_card.publish",
        "Publish the authenticated Participant's bounded work card for its current Task/Attempt scope.",
        &[f("fields", O)],
        &["fields"],
    ),
    mutation(
        "coordination.work_card.withdraw",
        "Withdraw the authenticated Participant's current work card while preserving its history.",
        &[],
        &[],
    ),
    mutation(
        "coordination.contract_card.publish",
        "Publish one bounded contract card under an exact key in the authenticated Participant's current scope.",
        &[f("contract_key", S), f("fields", O)],
        &["contract_key", "fields"],
    ),
    mutation(
        "coordination.contract_card.withdraw",
        "Withdraw one exact contract card in the authenticated Participant's current scope.",
        &[f("contract_key", S)],
        &["contract_key"],
    ),
    mutation(
        "coordination.send",
        "Deliver a bounded non-null JSON body to one active Participant in the same current Task/Attempt scope; this is typed coordination, not raw mailbox access.",
        &[f("recipient", S), f("body", "non_null_json")],
        &["recipient", "body"],
    ),
    mutation(
        "coordination.sync_integration",
        "Publish one integration offer or requirement linked to the authenticated Participant's current contract card; results are advisory and do not accept work or wake a model.",
        &[f("contract_key", S), f("offer", O), f("requirement", O)],
        &["contract_key"],
    ),
    mutation(
        "coordination.consult",
        "Resolve one exact card owner and ask one bounded question only when that unique live owner's card lacks the requested field.",
        &[
            f("target", O),
            f("field", S),
            f("question_kind", S),
            f("question", S),
            f("why_needed", S),
            f("expected_answer", S),
            f("blocking", B),
            f("reply_deadline_ms", IN),
            f("evidence_refs", A),
        ],
        &[
            "target",
            "field",
            "question_kind",
            "question",
            "why_needed",
            "expected_answer",
            "blocking",
            "evidence_refs",
        ],
    ),
    mutation(
        "coordination.watch.create",
        "Create one bounded, one-shot watch over a supported retained fact in the authenticated Participant scope or exact Manager/Operator Task/Attempt scope.",
        &[
            f("watch_kind", S),
            f("address", O),
            f("expires_at_ms", I),
            f("delivery", S),
            f("one_shot", B),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
        ],
        &[
            "watch_kind",
            "address",
            "expires_at_ms",
            "delivery",
            "one_shot",
        ],
    ),
    mutation(
        "coordination.watch.cancel",
        "Cancel one watch by its exact ID; the stored watch scope is reauthorized before the change.",
        &[f("watch_id", S)],
        &["watch_id"],
    ),
    mutation(
        "review.assign",
        "Assign a reviewer to the exact current submitted candidate, choosing one reviewer identity or registered review profile. Replacement requires the prior assignment, reason, and evidence refs together.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("reviewer_client_id", SN),
            f("review_profile", SN),
            f("replaces_review_assignment_id", SN),
            f("replacement_reason", SN),
            f("replacement_evidence_refs", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
        ],
    ),
    mutation(
        "review.submit",
        "Record the authenticated AssignedReviewer's immutable verdict and evidence for its exact assigned review slot. It does not apply Task feedback, start repair, or publish.",
        &[
            f("review_assignment_id", S),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("verdict", S),
            f("coverage", S),
            f("findings", A),
            f("evidence_refs", A),
            f("requirement_reviews", A),
        ],
        &[
            "review_assignment_id",
            "submission_ref",
            "candidate_ref",
            "verdict",
            "coverage",
            "findings",
            "evidence_refs",
        ],
    ),
    mutation(
        "automation.config.apply",
        "Apply a revision-checked Manager-owned automation plan. Preview first and pass its digest when available; activation does not start a model turn.",
        &[f("project_id", S), f("changes", A), f("preview_digest", SN)],
        &["project_id", "changes"],
    ),
    mutation(
        "host.mode",
        "Enable or disable admission of new work on the host: new_work is the string \"enabled\" or \"disabled\".",
        &[f("new_work", S)],
        &["new_work"],
    ),
    mutation(
        "client.register",
        "Register a scoped client by token hash (operator only). The caller generates the token and keeps the credential; only its SHA-256 hash is sent.",
        &[
            f("client_id", S),
            f("role", S),
            f("token_hash", S),
            f("binding_id", S),
            f("binding_generation", I),
        ],
        &["client_id", "role", "token_hash"],
    ),
    mutation(
        "source.capture",
        "Capture an exact Git commit of a local repository as a fixed-source candidate.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("repository", S),
            f("commit", S),
        ],
        &["attempt_id", "expected_revision", "repository", "commit"],
    ),
    mutation(
        "check.run",
        "Run a configured check profile against a captured candidate.",
        &[
            f("attempt_id", S),
            f("candidate_ref", S),
            f("profile_id", S),
            f("profile_revision", S),
        ],
        &[
            "attempt_id",
            "candidate_ref",
            "profile_id",
            "profile_revision",
        ],
    ),
    mutation(
        "check.cancel",
        "Request cancellation of one check run; the reply confirms the durable request, not termination.",
        &[f("check_id", S), f("reason", S)],
        &["check_id", "reason"],
    ),
    mutation(
        "task.create",
        "Create a task from a specification; no native effects.",
        &[f("project_id", S), f("spec", O), f("origin_key", S)],
        &["project_id", "spec"],
    ),
    mutation(
        "task.revise",
        "Replace a task specification by CAS on its revision; resets current acceptance.",
        &[f("task_id", S), f("expected_revision", I), f("spec", O)],
        &["task_id", "expected_revision", "spec"],
    ),
    mutation(
        "task.claim",
        "Reserve an attempt for a task; start_owner defaults to native_manager.",
        &[
            f("task_id", S),
            f("expected_revision", I),
            f("owner_id", S),
            f("start_owner", S),
            f("binding_id", S),
            f("binding_generation", I),
        ],
        &["task_id", "expected_revision"],
    ),
    mutation(
        "task.dispatch",
        "Start a claimed controller-start attempt or reuse its existing start operation. Supply the exact launch_operation_id for launch-owned Attempts; prerequisite_operation_id remains a runtime configuration prerequisite.",
        &[
            f("attempt_id", S),
            f("launch_operation_id", S),
            f("text", S),
            f("prerequisite_operation_id", S),
        ],
        &["attempt_id"],
    ),
    mutation(
        "task.submit",
        "Seal an immutable submission and requirement report for an attempt; not acceptance.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("expected_submission_ref", SN),
            f("candidate_ref", S),
            f("summary", S),
            f("claims", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "expected_submission_ref",
            "candidate_ref",
            "summary",
        ],
    ),
    mutation(
        "task.request_changes",
        "Return one anchored finding about the exact current submission/candidate.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("finding_id", S),
            f("reason", S),
            f("requirement_ids", A),
            f("evidence", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "finding_id",
            "reason",
            "evidence",
        ],
    ),
    mutation(
        "task.accept",
        "Accept the exact sealed proposal as a separate decision owner after review.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("expected_feedback_observation_id", I),
            f("reason", S),
            f("reviews", A),
            f("check_ids", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "expected_feedback_observation_id",
            "reason",
            "reviews",
        ],
    ),
    mutation(
        "forge.publish_ref",
        "Publish an accepted source-snapshot commit to one locally allowlisted Git ref; never force-push or replay an uncertain push.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("accepted_operation_id", S),
            f("candidate_ref", S),
            f("expected_policy_revision", S),
            f("target_ref", S),
            f("expected_old_ref", SN),
            f("expected_create", B),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "accepted_operation_id",
            "candidate_ref",
            "expected_policy_revision",
            "target_ref",
            "expected_create",
        ],
    ),
    mutation(
        "task.invalidate_acceptance",
        "Revoke one named acceptance decision; never restarts its producer.",
        &[
            f("acceptance_operation_id", S),
            f("reason", S),
            f("evidence", A),
        ],
        &["acceptance_operation_id", "reason", "evidence"],
    ),
    mutation(
        "attempt.release",
        "End task-specific ownership after the producer's disposition is known.",
        &[
            f("attempt_id", S),
            f("outcome", S),
            f("reason", S),
            f("assignment_closed", B),
        ],
        &["attempt_id", "outcome"],
    ),
    mutation(
        "attempt.bind_producer",
        "Associate an already observed native run with an attempt; never spawns a worker.",
        &[
            f("attempt_id", S),
            f("assignment_id", S),
            f("native_session_id", S),
            f("native_run_id", S),
            f("observation_id", I),
        ],
        &[
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
            "observation_id",
        ],
    ),
    mutation(
        "agent.open",
        "Reserve a lane binding and prepare its native root through an operation.",
        &[f("lane_id", S), f("route", S)],
        &["lane_id", "route"],
    ),
    mutation(
        "agent.send",
        "Deliver next-turn input or an exact-turn steer to one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("text", S),
            f("delivery", S),
            f("expected_turn_id", S),
            f("prerequisite_operation_id", S),
        ],
        &["binding_id", "generation", "text"],
    ),
    mutation(
        "agent.reply",
        "Submit native decisions/answers for a binding's current requests.",
        &[f("binding_id", S), f("generation", I), f("reply", O)],
        &["binding_id", "generation", "reply"],
    ),
    mutation(
        "agent.configure",
        "Apply settings to one binding through its native configuration path.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("settings", O),
            f("prerequisite_operation_id", S),
        ],
        &["binding_id", "generation", "settings"],
    ),
    mutation(
        "agent.goal",
        "Set, edit, pause, resume or clear the goal of one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("action", S),
            f("objective", S),
        ],
        &["binding_id", "generation", "action"],
    ),
    mutation(
        "agent.refresh",
        "Refresh one binding's observed state from its native runtime.",
        &[f("binding_id", S), f("generation", I), f("session_id", S)],
        &["binding_id", "generation"],
    ),
    mutation(
        "agent.reconcile",
        "Reconcile one operation's unknown outcome by readback only.",
        &[f("binding_id", S), f("generation", I), f("operation_id", S)],
        &["binding_id", "generation", "operation_id"],
    ),
    mutation(
        "agent.recover",
        "Explicitly recover a recorded native session for one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("expected_boot_id", S),
            f("reason", S),
        ],
        &["binding_id", "generation", "expected_boot_id", "reason"],
    ),
    mutation(
        "agent.result",
        "Request one native result page into a retained artifact.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("selector", O),
            f("offset_bytes", I),
            f("length_bytes", I),
        ],
        &["binding_id", "generation", "selector"],
    ),
    mutation(
        "artifact.assemble",
        "Assemble ordered retained result pages into one verified whole artifact.",
        &[f("page_refs", A), f("expected_sha256", S)],
        &["page_refs"],
    ),
    mutation(
        "operation.cancel",
        "Cancel one queued operation by its handle.",
        &[f("operation_id", S), f("reason", S)],
        &["operation_id", "reason"],
    ),
    mutation(
        "gm.handover",
        "Designate a registered client as GM under the current application epoch rules.",
        &[
            f("client_id", S),
            f("binding_id", S),
            f("binding_generation", I),
        ],
        &["client_id"],
    ),
    mutation(
        "agent.background",
        "Background one addressed native session through its existing operation contract.",
        &[f("binding_id", S), f("generation", I), f("session_id", S)],
        &["binding_id", "generation"],
    ),
    mutation(
        "message.send",
        "Send a durable directed mailbox message.",
        &[
            f("recipient", S),
            f("text", S),
            f("in_reply_to", SN),
            f("in_reply_to_digest", SN),
            f("admission_deadline_ms", I),
            f("delivery_deadline_ms", I),
            f("reply_deadline_ms", I),
        ],
        &["recipient", "text"],
    ),
    mutation(
        "message.cancel",
        "Cancel one sent mailbox delivery by its exact delivery ID and payload digest.",
        &[f("delivery_id", S), f("payload_digest", S), f("reason", SN)],
        &["delivery_id", "payload_digest"],
    ),
];

/// Canonical application methods advertised by MCP. The catalog search is a
/// local facade method and is intentionally excluded from the Store registry.
pub(crate) fn registered_application_methods() -> Vec<&'static str> {
    TOOLS
        .iter()
        .filter_map(|(_, spec)| (spec.method != "swarm.tools.search").then_some(spec.method))
        .collect()
}

fn tool_name(method: &str) -> String {
    method.replace('.', "_")
}

fn input_schema(spec: &ToolSpec, read_only: bool, require_request_id: bool) -> Arc<JsonObject> {
    let mut properties = JsonObject::new();
    for field in spec.fields {
        properties.insert(field.name.to_string(), field_schema(field.kind));
    }
    if !read_only {
        properties.insert(
            "client_request_id".to_string(),
            json!({
                "type": "string",
                "description": if require_request_id || spec.method == "swarm.launch" {
                    "Caller-owned stable logical request ID. Choose it before dispatch and reuse it to reconcile a lost reply; the server does not retry mutations."
                } else {
                    "Caller-owned stable logical request ID. Reuse it to reconcile a lost reply; the local full compatibility profile generates one only when omitted and a result arrives."
                }
            }),
        );
        if spec.method == "swarm.launch" {
            properties["client_request_id"]["minLength"] = json!(1);
            properties["client_request_id"]["maxLength"] = json!(128);
        }
    }
    let mut schema = json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false,
    });
    let mut required = spec.required.to_vec();
    if !read_only && (require_request_id || spec.method == "swarm.launch") {
        required.push("client_request_id");
    }
    if !required.is_empty() {
        schema["required"] = json!(required);
    }
    refine_input_schema(spec.method, &mut schema);
    match schema {
        Value::Object(map) => Arc::new(map),
        _ => unreachable!("schema literal is an object"),
    }
}

fn refine_input_schema(method: &str, schema: &mut Value) {
    let properties = &mut schema["properties"];
    match method {
        "task.dispatch" => {
            properties["launch_operation_id"] = json!({
                "type":"string",
                "minLength":1,
                "maxLength":128,
                "pattern":"^\\S+$",
                "description":"Exact parent swarm.launch Operation for a launch-owned Attempt. Omit only for a legacy unlinked Attempt; prerequisite_operation_id remains a runtime configuration prerequisite."
            });
        }
        "review.submit" => {
            properties["requirement_reviews"] = json!({
                "type":"array",
                "description":"Optional structured evidence for acceptance; when supplied it must cover every frozen Task requirement on a complete pass with no findings.",
                "items":{
                    "type":"object",
                    "properties":{
                        "requirement_id":{"type":"string","minLength":1},
                        "rationale":{"type":"string","minLength":1},
                        "evidence":{
                            "type":"array",
                            "minItems":1,
                            "items":{"type":"string","minLength":1,"description":"Exact assigned submission_ref or candidate_ref artifact ID."}
                        }
                    },
                    "required":["requirement_id","rationale","evidence"],
                    "additionalProperties":false
                }
            });
        }
        "coordination.sync_integration" => {
            properties["client_request_id"]["minLength"] = json!(1);
            properties["client_request_id"]["maxLength"] = json!(128);
            properties["client_request_id"]["pattern"] = json!("^\\S+$");
            properties["contract_key"] =
                json!({"type":"string","minLength":1,"maxLength":256,"pattern":"^\\S+$"});
            properties["offer"] = json!({
                "oneOf":[sync_offer_schema(),{"type":"null"}]
            });
            properties["requirement"] = json!({
                "oneOf":[sync_requirement_schema(),{"type":"null"}]
            });
            append_all_of(
                schema,
                json!({
                    "oneOf":[
                        {
                            "properties":{
                                "offer":{"type":"object"},
                                "requirement":{"not":{"type":"object"}}
                            },
                            "required":["offer"]
                        },
                        {
                            "properties":{
                                "requirement":{"type":"object"},
                                "offer":{"not":{"type":"object"}}
                            },
                            "required":["requirement"]
                        }
                    ]
                }),
            );
        }
        "swarm.overlap.check" => {
            properties["task_id"] =
                json!({"type":"string","minLength":1,"maxLength":256,"pattern":"^\\S+$"});
            properties["task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["attempt_id"] =
                json!({"type":"string","minLength":1,"maxLength":256,"pattern":"^\\S+$"});
            properties["paths"] = json!({
                "type":"array",
                "maxItems":24,
                "uniqueItems":true,
                "description":"Literal relative paths. The total number of paths, symbols, and contracts together is at most 24.",
                "items":{"type":"string","minLength":1,"maxLength":1024}
            });
            properties["symbols"] = json!({
                "type":"array",
                "maxItems":24,
                "uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":1024,"pattern":"^\\S+$"}
            });
            properties["contracts"] = json!({
                "type":"array",
                "maxItems":24,
                "uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":256,"pattern":"^\\S+$"}
            });
            properties["candidate_ref"] =
                json!({"type":["string","null"],"minLength":1,"maxLength":128});
            require_all_or_none_scope(schema);
            schema["anyOf"] = json!([
                {"required":["paths"],"properties":{"paths":{"minItems":1}}},
                {"required":["symbols"],"properties":{"symbols":{"minItems":1}}},
                {"required":["contracts"],"properties":{"contracts":{"minItems":1}}},
                {"required":["candidate_ref"],"properties":{"candidate_ref":{"type":"string"}}}
            ]);
        }
        "coordination.consult" => {
            properties["target"] = json!({
                "oneOf": [
                    {"type":"object","properties":{"contract_key":{"type":"string","minLength":1,"maxLength":256}},"required":["contract_key"],"additionalProperties":false},
                    {"type":"object","properties":{"path":{"type":"string","minLength":1,"maxLength":1024}},"required":["path"],"additionalProperties":false},
                    {"type":"object","properties":{"symbol":{"type":"string","minLength":1,"maxLength":1024}},"required":["symbol"],"additionalProperties":false},
                    {"type":"object","properties":{"interface":{"type":"string","minLength":1,"maxLength":1024}},"required":["interface"],"additionalProperties":false}
                ]
            });
            properties["field"] = json!({"type":"string","minLength":1,"maxLength":128});
            properties["question_kind"] = json!({
                "type":"string",
                "enum":["contract_shape","integration_point","identity","failure_semantics","status_fact","assumption_check","scope_overlap","compatibility","predecessor_fact"]
            });
            properties["question"] = json!({"type":"string","minLength":1,"maxLength":2048});
            properties["why_needed"] = json!({"type":"string","minLength":1,"maxLength":1024});
            properties["expected_answer"] = json!({"const":"one_fact"});
            properties["reply_deadline_ms"] = json!({"type":["integer","null"],"minimum":1});
            properties["evidence_refs"] = json!({
                "type":"array","maxItems":32,"uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":512}
            });
        }
        "coordination.watch.create" => {
            properties["watch_kind"] = json!({
                "type":"string",
                "enum":[
                    "operation_terminal",
                    "contract_revision_changed",
                    "task_revision_changed",
                    "attempt_disposition_changed",
                    "exact_deadline_reached"
                ]
            });
            properties["address"] = json!({"type":"object"});
            properties["expires_at_ms"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["delivery"] = json!({"const":"mailbox_header"});
            properties["one_shot"] = json!({"const":true});
            properties["task_id"] = json!({"type":"string","minLength":1,"maxLength":128});
            properties["task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["attempt_id"] = json!({"type":"string","minLength":1,"maxLength":128});
            append_all_of(schema, watch_address_union());
            require_all_or_none_scope(schema);
        }
        "coordination.watch.list" => {
            properties["task_id"] = json!({"type":"string","minLength":1,"maxLength":128});
            properties["task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["attempt_id"] = json!({"type":"string","minLength":1,"maxLength":128});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":50});
            properties["after_watch_id"] = json!({"type":"string","minLength":1,"maxLength":128});
            require_all_or_none_scope(schema);
        }
        "coordination.watch.cancel" => {
            properties["watch_id"] = json!({"type":"string","minLength":1,"maxLength":128});
        }
        "swarm.launch.preview" | "swarm.launch" => {
            properties["task_id"] = json!({"type":"string","minLength":1,"maxLength":512});
            properties["expected_task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            for name in ["route", "agent_profile", "mcp_profile", "mcp_surface"] {
                properties[name] = json!({"type":"string","minLength":1,"maxLength":256});
            }
            properties["workspace_policy"] = json!({"const":"manager_owned_worktree"});
            for name in ["requested_model", "requested_effort"] {
                properties[name] = json!({"type":["string","null"],"maxLength":256});
            }
            properties["budget"] = json!({
                "type":"object",
                "properties":{
                    "max_turns":{"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64},
                    "max_duration_ms":{"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64},
                    "max_cost_units":{"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64}
                },
                "required":["max_turns","max_duration_ms","max_cost_units"],
                "additionalProperties":false
            });
            properties["stop_conditions"] = json!({
                "type":"array","maxItems":16,
                "items":{"type":"string","minLength":1,"maxLength":512}
            });
            properties["purpose"] = json!({"type":"string","minLength":1,"maxLength":128});
            if method == "swarm.launch" {
                properties["plan_digest"] = json!({
                    "type":"string",
                    "minLength":71,
                    "maxLength":71,
                    "pattern":"^sha256:[0-9a-f]{64}$"
                });
            }
        }
        _ => {}
    }
}

fn require_all_or_none_scope(schema: &mut Value) {
    append_all_of(
        schema,
        json!({
            "oneOf": [
                {"not":{"anyOf":[{"required":["task_id"]},{"required":["task_revision"]},{"required":["attempt_id"]}]}},
                {"required":["task_id","task_revision","attempt_id"]}
            ]
        }),
    );
}

fn sync_offer_schema() -> Value {
    let availability = json!({
        "type":"object",
        "properties":{
            "path":{"type":["string","null"],"minLength":1,"maxLength":1024,"description":"Literal relative path."},
            "symbol":{"type":["string","null"],"minLength":1,"maxLength":1024,"pattern":"^\\S+$"}
        },
        "anyOf":[
            {"required":["path"],"properties":{"path":{"type":"string"}}},
            {"required":["symbol"],"properties":{"symbol":{"type":"string"}}}
        ],
        "additionalProperties":false
    });
    json!({
        "type":"object",
        "properties":{
            "readiness":{"type":"string","enum":["draft","implementation_ready","observed"]},
            "will_be_available_at":availability,
            "candidate_ref":{"type":["string","null"],"minLength":1,"maxLength":128},
            "assumptions":{
                "type":"array",
                "maxItems":20,
                "uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":512}
            }
        },
        "required":["readiness","will_be_available_at"],
        "additionalProperties":false
    })
}

fn sync_requirement_schema() -> Value {
    let dimensions = [
        "version",
        "producer",
        "consumer",
        "carrier",
        "contract",
        "inputs",
        "outputs",
        "serialization",
        "ownership",
        "availability",
        "limits",
        "result_disposition",
        "retry_semantics",
        "canonical_sources",
    ];
    json!({
        "type":"object",
        "properties":{
            "consumer_path":{"type":["string","null"],"minLength":1,"maxLength":1024,"description":"Literal relative path."},
            "consumer_symbol":{"type":["string","null"],"minLength":1,"maxLength":1024,"pattern":"^\\S+$"},
            "required_dimensions":{
                "type":"object",
                "minProperties":1,
                "maxProperties":16,
                "propertyNames":{"enum":dimensions},
                "additionalProperties":{"not":{"type":"null"}},
                "description":"Required dimension values; canonical JSON must be at most 8192 bytes."
            },
            "must_be_ready_before":{"type":"string","minLength":1,"maxLength":256},
            "assumptions":{
                "type":"array",
                "maxItems":20,
                "uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":512}
            }
        },
        "required":["required_dimensions","must_be_ready_before"],
        "anyOf":[
            {"required":["consumer_path"],"properties":{"consumer_path":{"type":"string"}}},
            {"required":["consumer_symbol"],"properties":{"consumer_symbol":{"type":"string"}}}
        ],
        "additionalProperties":false
    })
}

fn watch_address_union() -> Value {
    let operation_terminal = json!({
        "type":"object",
        "properties":{"operation_id":{"type":"string","minLength":1,"maxLength":128}},
        "required":["operation_id"],
        "additionalProperties":false
    });
    let contract_revision_changed = json!({
        "type":"object",
        "properties":{
            "task_id":{"type":"string","minLength":1,"maxLength":128},
            "attempt_id":{"type":"string","minLength":1,"maxLength":128},
            "contract_key":{"type":"string","minLength":1,"maxLength":256},
            "client_id":{"type":"string","minLength":1,"maxLength":128},
            "expected_revision":{"type":"integer","minimum":0,"maximum":9223372036854775807_i64}
        },
        "required":["task_id","attempt_id","contract_key","client_id","expected_revision"],
        "additionalProperties":false
    });
    let task_revision_changed = json!({
        "type":"object",
        "properties":{
            "task_id":{"type":"string","minLength":1,"maxLength":128},
            "expected_revision":{"type":"integer","minimum":1,"maximum":9223372036854775807_i64}
        },
        "required":["task_id","expected_revision"],
        "additionalProperties":false
    });
    let attempt_disposition_changed = json!({
        "type":"object",
        "properties":{
            "attempt_id":{"type":"string","minLength":1,"maxLength":128},
            "expected_state":{"type":"string","enum":["reserved","running","submitted","needs_correction","recovery_pending"]}
        },
        "required":["attempt_id","expected_state"],
        "additionalProperties":false
    });
    let exact_deadline_reached = json!({
        "type":"object",
        "properties":{
            "operation_id":{"type":"string","minLength":1,"maxLength":128},
            "deadline_field":{"const":"reply_deadline_ms"},
            "expected_deadline_ms":{"type":"integer","minimum":1,"maximum":9223372036854775807_i64}
        },
        "required":["operation_id","deadline_field","expected_deadline_ms"],
        "additionalProperties":false
    });
    json!({
        "oneOf":[
            {"properties":{"watch_kind":{"const":"operation_terminal"},"address":operation_terminal}},
            {"properties":{"watch_kind":{"const":"contract_revision_changed"},"address":contract_revision_changed}},
            {"properties":{"watch_kind":{"const":"task_revision_changed"},"address":task_revision_changed}},
            {"properties":{"watch_kind":{"const":"attempt_disposition_changed"},"address":attempt_disposition_changed}},
            {"properties":{"watch_kind":{"const":"exact_deadline_reached"},"address":exact_deadline_reached}}
        ]
    })
}

fn append_all_of(schema: &mut Value, constraint: Value) {
    if let Some(all_of) = schema.get_mut("allOf").and_then(Value::as_array_mut) {
        all_of.push(constraint);
    } else {
        schema["allOf"] = json!([constraint]);
    }
}

fn field_schema(kind: &str) -> Value {
    match kind {
        "string" => json!({"type": "string"}),
        "string_or_null" => json!({"type": ["string", "null"]}),
        "integer" => json!({"type": "integer"}),
        "integer_or_null" => json!({"type": ["integer", "null"]}),
        "boolean" => json!({"type": "boolean"}),
        "object" => json!({"type": "object"}),
        "array" => json!({"type": "array"}),
        "non_null_json" => json!({"not": {"type": "null"}}),
        _ => json!({}),
    }
}

fn raw_schema(value: Value) -> Arc<JsonObject> {
    match value {
        Value::Object(map) => Arc::new(map),
        _ => unreachable!("schema literal is an object"),
    }
}

fn message_scope_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "client_id": {"type": "string"},
            "role": {"type": ["string", "null"]},
            "binding_id": {"type": ["string", "null"]},
            "binding_generation": {"type": ["integer", "null"]},
        },
        "required": ["client_id", "role", "binding_id", "binding_generation"],
        "additionalProperties": false,
    })
}

fn message_actor_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "client_id": {"type": "string"},
            "role": {"type": ["string", "null"]},
            "generation": {"type": ["integer", "null"]},
        },
        "required": ["client_id", "role", "generation"],
        "additionalProperties": false,
    })
}

fn output_schema(method: &str) -> Option<Arc<JsonObject>> {
    match method {
        "message.send" => Some(raw_schema(json!({
            "type": "object",
            "properties": {
                "operation_id": {"type": "string"},
                "message_id": {"type": "string"},
                "delivery_id": {"type": "string"},
                "sender": {"type": "string"},
                "recipient": {"type": "string"},
                "source_scope": message_scope_schema(),
                "target_scope": message_scope_schema(),
                "actor": message_actor_schema(),
                "payload_digest": {"type": "string"},
                "admission_deadline_ms": {"type": ["integer", "null"]},
                "delivery_deadline_ms": {"type": ["integer", "null"]},
                "reply_deadline_ms": {"type": ["integer", "null"]},
                "text": {"type": "string"},
                "in_reply_to": {"type": ["string", "null"]},
                "reply_to": {
                    "type": ["object", "null"],
                    "properties": {
                        "delivery_id": {"type": ["string", "null"]},
                        "payload_digest": {"type": ["string", "null"]},
                    },
                    "required": ["delivery_id", "payload_digest"],
                    "additionalProperties": false,
                },
                "cancellation": {"type": "null"},
                "delivery": {"type": "string"},
            },
            "required": [
                "operation_id", "message_id", "delivery_id", "sender", "recipient",
                "source_scope", "target_scope", "actor", "payload_digest",
                "admission_deadline_ms", "delivery_deadline_ms", "reply_deadline_ms",
                "text", "in_reply_to", "reply_to", "cancellation", "delivery",
            ],
            "additionalProperties": false,
        }))),
        "message.cancel" => Some(raw_schema(json!({
            "type": "object",
            "properties": {
                "operation_id": {"type": "string"},
                "cancellation": {
                    "type": "object",
                    "properties": {
                        "delivery_id": {"type": "string"},
                        "payload_digest": {"type": "string"},
                    },
                    "required": ["delivery_id", "payload_digest"],
                    "additionalProperties": false,
                },
                "cancelled_by": message_actor_schema(),
                "reason": {"type": ["string", "null"]},
                "original_record_changed": {"type": "boolean"},
                "delivery": {"type": "string"},
            },
            "required": [
                "operation_id", "cancellation", "cancelled_by", "reason",
                "original_record_changed", "delivery",
            ],
            "additionalProperties": false,
        }))),
        _ => None,
    }
}

fn tool_from_spec(read_only: bool, spec: &ToolSpec, require_request_id: bool) -> Tool {
    let mut tool = Tool::new(
        tool_name(spec.method),
        spec.description,
        input_schema(spec, read_only, require_request_id),
    );
    if let Some(schema) = output_schema(spec.method) {
        tool = tool.with_raw_output_schema(schema);
    }
    if read_only {
        tool = tool.with_annotations(ToolAnnotations::new().read_only(true));
    }
    tool
}

fn find_tool(name: &str) -> Option<&'static (bool, ToolSpec)> {
    TOOLS
        .iter()
        .find(|(_, spec)| tool_name(spec.method) == name)
}

pub(crate) struct McpFacade {
    root: PathBuf,
    credential: Credential,
    ipc_config: Arc<Ipc>,
    client: Mutex<Option<ipc::Client>>,
    /// The subscription pollers' own IPC connection (§17.3): lazily
    /// connected, shared by every poller of this session, and never
    /// used for tool calls — a dead pump link cannot wedge a tool
    /// call, and a dropped tool link cannot stall a poller.
    pump_client: Arc<Mutex<Option<ipc::Client>>>,
    subscriptions: Arc<subscriptions::SubscriptionHub>,
}

impl McpFacade {
    pub fn new(root: PathBuf, credential: Credential, ipc_config: Arc<Ipc>) -> Self {
        Self {
            root,
            credential,
            ipc_config,
            client: Mutex::new(None),
            pump_client: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(subscriptions::SubscriptionHub::new(
                subscriptions::MAX_QUEUE_DEPTH,
                subscriptions::POLL_INTERVAL,
            )),
        }
    }

    /// One forwarded request over the lazily connected IPC link. A failed
    /// link is never reused or silently retried under a possibly
    /// different outcome; the next request reconnects first.
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        request_on(
            &self.client,
            &self.root,
            &self.credential,
            &self.ipc_config,
            method,
            params,
        )
        .await
    }

    /// `eliot/subscribe` (§17.3): open a bounded subscription over
    /// committed facts. The request's peer is captured for the
    /// subscription's forwarder; the acknowledgement names the
    /// starting cursor and the resync contract.
    async fn subscribe(
        &self,
        params: Option<Value>,
        peer: Peer<RoleServer>,
    ) -> std::result::Result<CustomResult, McpError> {
        let params = params.unwrap_or_else(|| json!({}));
        let (categories, after) =
            subscriptions::parse_subscribe(&params).map_err(protocol_error)?;
        let source = Arc::new(subscriptions::PumpSource::new(
            self.root.clone(),
            self.credential.clone(),
            self.ipc_config.clone(),
            self.pump_client.clone(),
        ));
        let ack = self
            .subscriptions
            .subscribe(source, peer, categories, after)
            .map_err(protocol_error)?;
        Ok(CustomResult(ack))
    }

    /// `eliot/unsubscribe`: stop one subscription of this session.
    fn unsubscribe(&self, params: Option<Value>) -> std::result::Result<CustomResult, McpError> {
        let params = params.unwrap_or_else(|| json!({}));
        let id = subscriptions::parse_unsubscribe(&params).map_err(protocol_error)?;
        let ack = self
            .subscriptions
            .unsubscribe(&id)
            .map_err(protocol_error)?;
        Ok(CustomResult(ack))
    }

    async fn call(&self, method: &str, mut params: Value, read_only: bool) -> CallToolResult {
        let mut generated_request_id = None;
        if !read_only {
            if !params.is_object() {
                return tool_error(Error::invalid("tool arguments must be an object"));
            }
            if params.get("client_request_id").is_none() {
                let id = model::new_id();
                params["client_request_id"] = json!(id);
                generated_request_id = Some(id);
            }
        }
        match self.request(method, params).await {
            Ok(mut result) => {
                if let Some(id) = generated_request_id
                    && let Value::Object(map) = &mut result
                {
                    map.entry("client_request_id").or_insert_with(|| json!(id));
                }
                CallToolResult::structured(result)
            }
            Err(e) => tool_error(e),
        }
    }

    /// The full task projection of one Operation, read fresh from the
    /// host. This is a read model only: it stores nothing and executes
    /// nothing.
    async fn project_task(&self, task_id: &str) -> Result<DetailedTask> {
        let operation = self
            .request("operation.get", json!({"operation_id": task_id}))
            .await?;
        let inputs = if is_terminal_state(&operation) {
            InputRequests::new()
        } else {
            self.pending_inputs(&operation).await?
        };
        Ok(project_operation(&operation, inputs))
    }

    /// The Operation's pending native-input items, taken from the same
    /// `report.attention` projection managers read (§8.3) so there is
    /// exactly one definition of a pending attention item. Every page is
    /// read: a page cut could silently drop a pending request, which
    /// would understate what the task is waiting for.
    async fn pending_inputs(&self, operation: &Value) -> Result<InputRequests> {
        if operation["binding_id"].as_str().is_none() {
            return Ok(InputRequests::new());
        }
        let mut items = Vec::new();
        let mut after = 0_i64;
        loop {
            let page = self
                .request("report.attention", json!({"after": after, "limit": 200}))
                .await?;
            if let Some(batch) = page["items"].as_array() {
                items.extend(batch.iter().cloned());
            }
            let total = page["total_items"].as_i64().unwrap_or(0);
            let next = page["next_after"].as_i64().unwrap_or(after);
            if next <= after || next >= total {
                break;
            }
            after = next;
        }
        Ok(pending_input_requests(operation, &items))
    }

    /// Convert a finished mutation reply into the MCP task seed for its
    /// Operation — only for Tasks-negotiated clients, and only while
    /// the Operation is still in flight. Anything else (terminal
    /// Operations, error replies, replies without an Operation handle,
    /// a failed readback) keeps the plain result the caller would have
    /// received before the Tasks projection existed; a mutation that
    /// already succeeded never fails retroactively here.
    async fn task_seed_response(&self, result: CallToolResult) -> CallToolResponse {
        let Some(operation_id) = result
            .structured_content
            .as_ref()
            .filter(|_| result.is_error != Some(true))
            .and_then(|value| value["operation_id"].as_str().map(str::to_owned))
        else {
            return result.into();
        };
        match self.project_task(&operation_id).await {
            Ok(detailed) if !detailed.status().is_terminal() => {
                CallToolResponse::Task(CreateTaskResult::new(detailed.task))
            }
            _ => result.into(),
        }
    }
}

/// One forwarded request over one lazily connected IPC link slot.
/// The tool-call path and the subscription pump path each own a slot
/// and share exactly this discipline: connect on first use; on a
/// transport-class failure (the codes below) drop the link so the
/// next request reconnects first; never silently retry a request
/// whose outcome may differ on replay.
async fn request_on(
    slot: &Mutex<Option<ipc::Client>>,
    root: &std::path::Path,
    credential: &Credential,
    ipc_config: &Ipc,
    method: &str,
    params: Value,
) -> Result<Value> {
    let mut client = slot.lock().await;
    if client.is_none() {
        *client = Some(ipc::Client::connect(root, credential, ipc_config).await?);
    }
    let connected = client.as_mut().expect("client connected above");
    match connected.request(method, params).await {
        Ok(result) => Ok(result),
        Err(e) => {
            if matches!(
                e.code.as_str(),
                "DISCONNECTED"
                    | "OUTCOME_UNKNOWN"
                    | "PROTOCOL_ERROR"
                    | "IO_ERROR"
                    | "WRITE_TIMEOUT"
            ) {
                *client = None;
            }
            Err(e)
        }
    }
}

fn tool_error(error: Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(
        json!({"error": {"code": error.code, "message": error.message}}).to_string(),
    )])
}

/// Map an application error onto the JSON-RPC surface of `tasks/*`,
/// preserving the ELIOT code and message verbatim in `data`.
fn protocol_error(error: Error) -> McpError {
    let data = json!({"code": error.code, "message": error.message});
    match error.code.as_str() {
        "NOT_FOUND" | "INVALID_PARAMS" => {
            McpError::invalid_params(error.message.clone(), Some(data))
        }
        _ => McpError::internal_error(error.message.clone(), Some(data)),
    }
}

fn is_terminal_state(operation: &Value) -> bool {
    matches!(
        operation["state"].as_str(),
        Some("settled" | "rejected" | "cancelled")
    )
}

/// How `tasks/cancel` treats an Operation in this state. This mirrors
/// `operation.cancel` exactly: only a queued Operation can be cancelled
/// through it, a terminal one needs no cancellation, and an in-flight
/// one is refused — already-sent work requires native
/// cancellation/reconciliation, never local deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelAction {
    Ack,
    Submit,
    Refuse,
}

fn cancel_action(state: &str) -> CancelAction {
    match state {
        "settled" | "rejected" | "cancelled" => CancelAction::Ack,
        "queued" => CancelAction::Submit,
        _ => CancelAction::Refuse,
    }
}

/// Suggested `tasks/get` poll interval. Operations settle on native
/// evidence, which is slow relative to a local read; one second keeps
/// polling cheap without busy-looping the host.
const TASK_POLL_INTERVAL_MS: u64 = 1000;

/// Project one Operation record (the `operation.get` shape) into the
/// MCP `DetailedTask` shape. Pure: the caller supplies the Operation
/// and its pending input requests.
///
/// State mapping: `settled` → completed with the Operation's own
/// recorded result as the tool result; `rejected` → failed with the
/// recorded admission/native error preserved in `data`; `cancelled` →
/// cancelled; every in-flight state (`queued`, `sending`,
/// `native_accepted`, `outcome_unknown`, …) → working, or
/// input_required while exact native input requests are pending.
/// `outcome_unknown` stays working on purpose: the Operation has not
/// failed, its outcome is simply not yet proven, and the status
/// message names the exact ELIOT state.
fn project_operation(operation: &Value, input_requests: InputRequests) -> DetailedTask {
    let state = operation["state"].as_str().unwrap_or("unknown");
    let payload = match state {
        "settled" => TaskPayload::Completed {
            result: completed_result(operation),
        },
        "rejected" => TaskPayload::Failed {
            error: failure_error(operation),
        },
        "cancelled" => TaskPayload::Cancelled,
        _ if !input_requests.is_empty() => TaskPayload::InputRequired { input_requests },
        _ => TaskPayload::Working,
    };
    let task = McpTask::new(
        operation["operation_id"].as_str().unwrap_or_default(),
        payload.status(),
        iso8601_utc(operation["created_at_ms"].as_i64().unwrap_or(0)),
        iso8601_utc(operation["updated_at_ms"].as_i64().unwrap_or(0)),
    )
    .with_status_message(format!(
        "{} operation is {}",
        operation["method"].as_str().unwrap_or("unknown"),
        state
    ))
    .with_poll_interval_ms(TASK_POLL_INTERVAL_MS);
    DetailedTask::new(task, payload)
}

/// The terminal result of a settled Operation, in the same
/// `CallToolResult` shape a non-Tasks client receives in the mutation
/// reply: structured content is the Operation's recorded result.
fn completed_result(operation: &Value) -> Map<String, Value> {
    let result = operation["result"].clone();
    let structured = if result.is_object() {
        result
    } else {
        json!({"result": result})
    };
    match serde_json::to_value(CallToolResult::structured(structured)) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    }
}

/// The failure of a rejected Operation. The wire `code` is the generic
/// JSON-RPC internal-error code; the exact ELIOT error object is
/// preserved verbatim in `data`.
fn failure_error(operation: &Value) -> Map<String, Value> {
    let stored = operation["result"].clone();
    let message = stored["message"]
        .as_str()
        .unwrap_or("operation rejected")
        .to_string();
    let mut error = Map::new();
    error.insert("code".to_string(), json!(-32603));
    error.insert("message".to_string(), json!(message));
    error.insert("data".to_string(), stored);
    error
}

/// Filter the unified attention projection down to the Operation's own
/// pending native-input requests and map each to an elicitation input
/// request keyed by its native request ID. An item qualifies only by an
/// exact recorded address: kind `waiting_for_native_request` and the
/// Operation's own binding/generation. Nothing is paraphrased into a
/// form schema: the native request's own shape stays with the native
/// runtime, and the answer is the addressed `agent_reply` tool call.
fn pending_input_requests(operation: &Value, attention_items: &[Value]) -> InputRequests {
    let (Some(binding_id), Some(generation)) = (
        operation["binding_id"].as_str(),
        operation["binding_generation"].as_i64(),
    ) else {
        return InputRequests::new();
    };
    let mut requests = InputRequests::new();
    for item in attention_items {
        if item["kind"].as_str() != Some("waiting_for_native_request")
            || item["binding_id"].as_str() != Some(binding_id)
            || item["generation"].as_i64() != Some(generation)
        {
            continue;
        }
        let Some(request_id) = item["address"]["request_id"].as_str() else {
            continue;
        };
        requests.insert(
            request_id.to_string(),
            InputRequest::Elicitation(ElicitRequest::new(
                ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: input_request_message(item),
                    requested_schema: ElicitationSchema::new(Default::default()),
                },
            )),
        );
    }
    requests
}

fn input_request_message(item: &Value) -> String {
    let address = &item["address"];
    let text = |key: &str| address[key].as_str().unwrap_or("unknown");
    let mut message = format!(
        "Native {} request {} in session {} is waiting for a decision",
        text("request_kind"),
        text("request_id"),
        text("session_id"),
    );
    if let Some(fingerprint) = address["fingerprint"].as_str() {
        message.push_str(&format!(" (fingerprint {fingerprint})"));
    }
    message.push_str(&format!(
        ". Answer it with the agent_reply tool for binding {} generation {}: \
         a reply is a durable addressed operation of its own, so \
         tasks/update is not supported for it.",
        text("binding_id"),
        address["generation"],
    ));
    message
}

/// Format epoch milliseconds as an ISO 8601 UTC timestamp (the wire
/// shape MCP task timestamps use). The crate carries no date library;
/// operations store epoch ms, so convert directly.
fn iso8601_utc(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let day_ms = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        day_ms / 3_600_000,
        day_ms / 60_000 % 60,
        day_ms / 1_000 % 60,
        day_ms % 1_000
    )
}

/// Days since the Unix epoch → (year, month, day), proleptic Gregorian
/// (Howard Hinnant's civil-from-days algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

impl ServerHandler for McpFacade {
    fn get_info(&self) -> ServerConfig {
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(subscriptions::EXTENSION_ID.to_string(), JsonObject::new());
        let mut capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_tasks()
            .build();
        match &mut capabilities.extensions {
            Some(map) => {
                map.extend(extensions);
            }
            None => capabilities.extensions = Some(extensions),
        }
        ServerConfig::new(capabilities)
            .with_server_info(Implementation::new(
                "eliot-swarm-controller",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Tools map one-to-one onto the swarm controller's application API and are \
             executed against the running host over local IPC with the configured \
             credential. Restricted profiles require a caller-owned stable \
             client_request_id before every mutation dispatch, so a lost reply can \
             be reconciled without a hidden retry. Only the explicit local full \
             compatibility profile permits omission; there, a generated ID is \
             echoed only in a received result and cannot make a retry safe if that \
             response itself was lost. Restricted-profile tasks/cancel likewise \
             requires client_request_id in request _meta; full generates one if \
             omitted. Operations returned by mutations are durable handles \
             to poll with operation_get. If the client declares the \
             io.modelcontextprotocol/tasks extension, a mutation whose operation is \
             still in flight instead returns an MCP task whose taskId is that \
             operation's ID: poll it with tasks/get, and cancel a still-queued \
             operation with tasks/cancel. Pending native inputs surfaced by \
             tasks/get are answered with the agent_reply tool, not tasks/update. \
             The eliot/subscribe and eliot/unsubscribe protocol methods open and \
             close a bounded subscription over committed facts (categories: \
             reports, mailbox, operations): notifications/eliot/committed carries \
             each committed transition with its projection frame, and an explicit \
             notifications/eliot/lagged marks any range the bounded queue \
             skipped. Notifications are a freshness hint, never complete history: \
             resync through report_delta, message_read or operation_get from the \
             last delivered cursor, and after a reconnect re-subscribe with that \
             cursor — subscriptions do not survive the session.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let tools = TOOLS
            .iter()
            .map(|(read_only, spec)| tool_from_spec(*read_only, spec, false))
            .collect();
        Ok(ListToolsResult {
            tools,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let Some((read_only, spec)) = find_tool(&request.name) else {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        };
        let params = request
            .arguments
            .map(Value::Object)
            .unwrap_or_else(|| json!({}));
        let result = self.call(spec.method, params, *read_only).await;
        // SEP-2663: a task handle is returned only to a client that
        // declared the tasks extension, and only for mutations; every
        // other response is byte-identical to the pre-Tasks facade.
        if *read_only || !client_tasks_negotiated(&context) {
            return Ok(result.into());
        }
        Ok(self.task_seed_response(result).await)
    }

    /// SEP-2663 `tasks/get`: project the Operation the taskId names.
    /// The Operation store is the only state read; an unknown taskId is
    /// the store's own NOT_FOUND.
    async fn get_task(
        &self,
        request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskResult, McpError> {
        let detailed = self
            .project_task(&request.task_id)
            .await
            .map_err(protocol_error)?;
        Ok(GetTaskResult::new(detailed))
    }

    /// SEP-2663 `tasks/cancel`: submit the existing addressed
    /// `operation.cancel` for a queued Operation. A terminal Operation
    /// is acknowledged (there is nothing to cancel); an in-flight one
    /// is refused with the store's reason, because already-sent work
    /// requires native cancellation/reconciliation. The ack is
    /// cooperative: the resulting state is observed via `tasks/get`.
    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<(), McpError> {
        let operation = self
            .request("operation.get", json!({"operation_id": request.task_id}))
            .await
            .map_err(protocol_error)?;
        let state = operation["state"].as_str().unwrap_or_default();
        match cancel_action(state) {
            CancelAction::Ack => Ok(()),
            CancelAction::Refuse => Err(McpError::invalid_request(
                format!(
                    "operation {} is {state}: tasks/cancel submits operation_cancel, \
                     which cancels only queued operations",
                    request.task_id
                ),
                Some(json!({
                    "code": "NOT_QUEUED",
                    "message": "already-sent operations require native cancellation/reconciliation, not local deletion",
                })),
            )),
            CancelAction::Submit => {
                let cancel = json!({
                    "operation_id": request.task_id,
                    "reason": "mcp tasks/cancel",
                    "client_request_id": context
                        .meta
                        .get("client_request_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(model::new_id),
                });
                match self.request("operation.cancel", cancel).await {
                    Ok(_) => Ok(()),
                    Err(e) if matches!(e.code.as_str(), "NOT_QUEUED" | "CONFLICT") => {
                        // The operation left the queued state while the
                        // cancel was in flight; if it is terminal now,
                        // there is nothing left to cancel.
                        let current = self
                            .request("operation.get", json!({"operation_id": request.task_id}))
                            .await
                            .map_err(protocol_error)?;
                        match cancel_action(current["state"].as_str().unwrap_or_default()) {
                            CancelAction::Ack => Ok(()),
                            _ => Err(protocol_error(e)),
                        }
                    }
                    Err(e) => Err(protocol_error(e)),
                }
            }
        }
    }
    /// Facade-protocol methods (§17.3): `eliot/subscribe` and
    /// `eliot/unsubscribe`. Everything else keeps the router's
    /// default answer for a custom method: method not found.
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CustomResult, McpError> {
        match request.method.as_str() {
            subscriptions::SUBSCRIBE_METHOD => self.subscribe(request.params, context.peer).await,
            subscriptions::UNSUBSCRIBE_METHOD => self.unsubscribe(request.params),
            _ => Err(McpError::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method.clone(),
                None,
            )),
        }
    }
}

/// The production MCP boundary is a session-fixed view over the local facade.
/// Discovery intersects its presentation with live Store authorization;
/// manually addressed tools are still rejected by the hard profile before
/// their application method is forwarded.
pub(crate) struct ProfiledFacade {
    inner: McpFacade,
    profile: McpToolProfile,
    surface: catalog::Surface,
}

impl ProfiledFacade {
    #[cfg(test)]
    fn new(inner: McpFacade, profile: McpToolProfile) -> Self {
        Self::with_surface(inner, profile, catalog::Surface::role_default(profile))
    }

    fn with_surface(inner: McpFacade, profile: McpToolProfile, surface: catalog::Surface) -> Self {
        Self {
            inner,
            profile,
            surface,
        }
    }

    /// Ask the authenticated Store for current method membership on every
    /// catalog operation. Discovery is not cached across grant/scope changes.
    async fn catalog_authorization(
        &self,
        task_id: Option<&str>,
    ) -> std::result::Result<CatalogAuthorization, McpError> {
        let mut params = json!({});
        if let Some(task_id) = task_id {
            params["task_id"] = json!(task_id);
        }
        let value = self
            .inner
            .request("mcp.authorization", params)
            .await
            .map_err(|_| catalog_authorization_error())?;
        parse_catalog_authorization(&value, task_id).ok_or_else(catalog_authorization_error)
    }
}

struct CatalogAuthorization {
    revision: String,
    task_id: Option<String>,
    allowed_methods: BTreeSet<String>,
}

fn catalog_authorization_error() -> McpError {
    McpError::internal_error(
        "current application authorization could not be confirmed",
        None,
    )
}

fn parse_catalog_authorization(
    value: &Value,
    requested_task_id: Option<&str>,
) -> Option<CatalogAuthorization> {
    let object = value.as_object()?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "authorization_revision" | "basis" | "allowed_methods" | "role" | "task_id"
        )
    }) {
        return None;
    }
    let revision = object.get("authorization_revision")?.as_str()?.trim();
    if revision.is_empty() || revision.len() > 128 {
        return None;
    }
    if object.get("basis")?.as_str()? != "authenticated_store_scope" {
        return None;
    }
    if !matches!(
        object.get("role")?.as_str()?,
        "participant" | "manager" | "operator" | "observer"
    ) {
        return None;
    }
    let task_id = match object.get("task_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(task_id)) if !task_id.trim().is_empty() => Some(task_id.clone()),
        _ => return None,
    };
    if task_id.as_deref() != requested_task_id {
        return None;
    }
    let application_methods: BTreeSet<&str> =
        registered_application_methods().into_iter().collect();
    let mut allowed_methods = BTreeSet::new();
    for method in object.get("allowed_methods")?.as_array()? {
        let method = method.as_str()?;
        if method != "swarm.tools.search" && !application_methods.contains(method) {
            return None;
        }
        if !allowed_methods.insert(method.to_owned()) {
            return None;
        }
    }
    if !allowed_methods.contains("swarm.tools.search") {
        return None;
    }
    Some(CatalogAuthorization {
        revision: revision.to_owned(),
        task_id,
        allowed_methods,
    })
}

/// Build the same session-fixed, profile-enforced facade used by stdio MCP.
/// The caller supplies the configured principal and profile; neither is
/// derived from transport metadata.
pub(crate) fn profiled_facade(
    config: &Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<ProfiledFacade> {
    config.mcp.validate()?;
    let selected_name = profile_name.unwrap_or(&config.mcp.default_profile);
    let named_profile = config.mcp.profiles.get(selected_name).ok_or_else(|| {
        Error::new(
            "CONFIG_ERROR",
            format!("unknown MCP profile {selected_name:?}"),
        )
    })?;
    let profile = config
        .mcp
        .selected_tool_profile(Some(selected_name), &credential.client_id)?;
    let surface = catalog::Surface::configured(
        profile,
        named_profile.surface.as_deref(),
        &named_profile.deferred_groups,
        &named_profile.manual_tools,
    )
    .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))?;
    let facade = McpFacade::new(
        config.storage.data_dir.clone(),
        credential,
        Arc::new(config.ipc.clone()),
    );
    Ok(ProfiledFacade::with_surface(facade, profile, surface))
}

/// Canonical restricted Participant core schemas for comparison with an
/// independently observed native inventory. This grants no method authority.
pub(crate) fn participant_core_tool_contracts() -> Result<Vec<Value>> {
    catalog::validate_registry_metadata().map_err(|error| Error::invalid(error.to_string()))?;
    catalog::role_core(catalog::CoreRole::Participant)
        .methods
        .iter()
        .map(|method| {
            let (read_only, spec) = TOOLS
                .iter()
                .find(|(_, spec)| spec.method == *method)
                .ok_or_else(|| Error::invalid("Participant core has no canonical tool contract"))?;
            if !profiles::allows_method(McpToolProfile::Participant, method) {
                return Err(Error::invalid(
                    "Participant core tool is outside its hard profile",
                ));
            }
            Ok(json!({
                "method": method,
                "name": tool_name(method),
                "input_schema": input_schema(spec, *read_only, true).as_ref(),
            }))
        })
        .collect()
}

/// Describe the configured facade without claiming native tool loading.
pub(crate) fn launch_profile_surface(
    profile: McpToolProfile,
    surface_name: &str,
    groups: &[String],
    manual_tools: &[String],
) -> Result<Value> {
    let surface = catalog::Surface::configured(profile, Some(surface_name), groups, manual_tools)
        .map_err(|error| Error::invalid(error.to_string()))?;
    let suggested = surface.suggested();
    let core = catalog::role_core(surface.core);
    Ok(json!({
        "surface_id": suggested.id,
        "core_role": suggested.core_role,
        "core": suggested.core,
        "core_methods": core.methods,
        "deferred_groups": suggested.deferred_groups,
        "manual_tools": suggested.exact_manual_methods,
    }))
}

fn method_not_found(method: &str) -> McpError {
    McpError::new(
        rmcp::model::ErrorCode::METHOD_NOT_FOUND,
        method.to_string(),
        None,
    )
}

fn require_caller_request_id(params: &Value) -> std::result::Result<(), McpError> {
    if params
        .get("client_request_id")
        .and_then(Value::as_str)
        .is_none_or(|request_id| request_id.trim().is_empty())
    {
        return Err(McpError::invalid_params(
            "restricted-profile mutations require a caller-owned client_request_id before dispatch",
            None,
        ));
    }
    Ok(())
}

fn parse_catalog_search<'a>(
    params: &'a Value,
) -> std::result::Result<catalog::SearchRequest<'a>, McpError> {
    let object = params.as_object().ok_or_else(|| {
        McpError::invalid_params("catalog search arguments must be an object", None)
    })?;
    const FIELDS: &[&str] = &[
        "query",
        "purpose",
        "task_id",
        "exact_method",
        "loaded_catalog_revision",
        "max_results",
    ];
    if object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(McpError::invalid_params(
            "catalog search contains an unsupported argument",
            None,
        ));
    }
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| query.len() <= 512)
        .ok_or_else(|| {
            McpError::invalid_params("query must be a string of at most 512 bytes", None)
        })?;
    let optional_text =
        |name: &str, limit: usize| -> std::result::Result<Option<&'a str>, McpError> {
            match object.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(value)) if value.len() <= limit => Ok(Some(value)),
                _ => Err(McpError::invalid_params(
                    format!("{name} must be a string of at most {limit} bytes"),
                    None,
                )),
            }
        };
    let purpose = optional_text("purpose", 128)?;
    let task_id = optional_text("task_id", 128)?;
    let exact_method = optional_text("exact_method", 128)?;
    let loaded_catalog_revision = optional_text("loaded_catalog_revision", 64)?;
    if query.trim().is_empty() && exact_method.is_none() {
        return Err(McpError::invalid_params(
            "query must be non-empty unless exact_method is supplied",
            None,
        ));
    }
    if loaded_catalog_revision.is_some_and(|revision| {
        revision.len() != 64
            || !revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(McpError::invalid_params(
            "loaded_catalog_revision must be a lowercase SHA-256 digest",
            None,
        ));
    }
    let max_results = match object.get("max_results") {
        None => 5,
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=catalog::MAX_SEARCH_RESULTS as u64).contains(value))
            .map(|value| value as usize)
            .ok_or_else(|| {
                McpError::invalid_params(
                    format!(
                        "max_results must be between 1 and {}",
                        catalog::MAX_SEARCH_RESULTS
                    ),
                    None,
                )
            })?,
    };
    Ok(catalog::SearchRequest {
        query,
        purpose,
        task_id,
        exact_method,
        loaded_catalog_revision,
        max_results,
    })
}

fn catalog_protocol_error(error: catalog::CatalogError) -> McpError {
    match error {
        catalog::CatalogError::InvalidCursor
        | catalog::CatalogError::InvalidSurface
        | catalog::CatalogError::StaleCursor
        | catalog::CatalogError::CursorOutOfRange
        | catalog::CatalogError::StaleCatalogRevision => {
            McpError::invalid_params(error.to_string(), None)
        }
        catalog::CatalogError::IncompleteRegistry
        | catalog::CatalogError::ToolSchemaTooLarge
        | catalog::CatalogError::Serialization(_) => {
            McpError::internal_error(error.to_string(), None)
        }
    }
}

impl ServerHandler for ProfiledFacade {
    fn get_info(&self) -> ServerConfig {
        self.inner.get_info()
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let cursor = request.as_ref().and_then(|params| params.cursor.as_deref());
        let authorization = self.catalog_authorization(None).await?;
        let page = catalog::list_tools_page(
            self.profile,
            &self.surface,
            cursor,
            catalog::AuthorizationRevision {
                value: &authorization.revision,
                basis: catalog::AuthorizationBasis::AuthenticatedStoreScope,
            },
            |method, task_id| {
                authorization.allowed_methods.contains(method)
                    && task_id
                        .is_none_or(|task_id| authorization.task_id.as_deref() == Some(task_id))
            },
        )
        .map_err(catalog_protocol_error)?;
        Ok(ListToolsResult {
            tools: page.tools,
            next_cursor: page.next_cursor,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let Some((read_only, spec)) = find_tool(&request.name) else {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        };
        if !profiles::allows_method(self.profile, spec.method) {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        }
        if spec.method == "swarm.tools.search" {
            let arguments = Value::Object(request.arguments.clone().unwrap_or_default());
            let search = parse_catalog_search(&arguments)?;
            let authorization = self.catalog_authorization(search.task_id).await?;
            let result = catalog::search_catalog(
                self.profile,
                &self.surface,
                search,
                catalog::AuthorizationRevision {
                    value: &authorization.revision,
                    basis: catalog::AuthorizationBasis::AuthenticatedStoreScope,
                },
                |method, task_id| {
                    authorization.allowed_methods.contains(method)
                        && task_id
                            .is_none_or(|task_id| authorization.task_id.as_deref() == Some(task_id))
                },
            )
            .map_err(catalog_protocol_error)?;
            let value = serde_json::to_value(result)
                .map_err(|error| McpError::internal_error(error.to_string(), None))?;
            let Value::Object(result) = value else {
                return Err(McpError::internal_error(
                    "MCP catalog search produced a non-object result",
                    None,
                ));
            };
            return Ok(CallToolResult::structured(Value::Object(result)).into());
        }
        if (self.profile != McpToolProfile::Full || spec.method == "swarm.launch") && !*read_only {
            let arguments = request
                .arguments
                .as_ref()
                .map(|arguments| Value::Object(arguments.clone()))
                .unwrap_or_else(|| json!({}));
            require_caller_request_id(&arguments)?;
        }
        self.inner.call_tool(request, context).await
    }

    async fn get_task(
        &self,
        request: GetTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskResult, McpError> {
        if !profiles::allows_task_get(self.profile) {
            return Err(method_not_found("tasks/get"));
        }
        self.inner.get_task(request, context).await
    }

    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<(), McpError> {
        if !profiles::allows_task_cancel(self.profile) {
            return Err(method_not_found("tasks/cancel"));
        }
        if self.profile != McpToolProfile::Full
            && context
                .meta
                .get("client_request_id")
                .and_then(Value::as_str)
                .is_none_or(|request_id| request_id.trim().is_empty())
        {
            return Err(McpError::invalid_params(
                "restricted-profile tasks/cancel requires a caller-owned client_request_id in _meta",
                None,
            ));
        }
        self.inner.cancel_task(request, context).await
    }

    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CustomResult, McpError> {
        if request.method == subscriptions::SUBSCRIBE_METHOD {
            let params = request
                .params
                .as_ref()
                .cloned()
                .unwrap_or_else(|| json!({}));
            let (categories, _) =
                subscriptions::parse_subscribe(&params).map_err(protocol_error)?;
            if !categories
                .iter()
                .all(|category| profiles::allows_subscription_category(self.profile, *category))
            {
                return Err(method_not_found(subscriptions::SUBSCRIBE_METHOD));
            }
        }
        self.inner.on_custom_request(request, context).await
    }
}

/// Whether the connected client declared the tasks extension during
/// initialize — the gate for returning task handles from `tools/call`
/// and the same check the RMCP router applies to `tasks/*` methods.
fn client_tasks_negotiated(context: &RequestContext<RoleServer>) -> bool {
    context
        .client_capabilities()
        .is_some_and(|caps| caps.supports_tasks())
}

/// Serve MCP over stdio until the client disconnects. The host connection is
/// established lazily on the first catalog discovery, search, or tool call.
/// Discovery fails closed while the host cannot confirm current authorization.
pub async fn run(config: Config, credential: Credential) -> Result<()> {
    run_profiled(config, credential, None).await
}

/// Serve one MCP session after resolving its configured profile against the
/// credential identity. The wrapper remains fixed for the entire session.
pub async fn run_profiled(
    config: Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<()> {
    let facade = profiled_facade(&config, credential, profile_name)?;
    let service = facade
        .serve(stdio())
        .await
        .map_err(|e| Error::new("MCP_ERROR", e.to_string()))?;
    service
        .waiting()
        .await
        .map_err(|e| Error::new("MCP_ERROR", e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn tool_table_is_unique_and_complete() {
        let mut names = BTreeSet::new();
        let mut methods = BTreeSet::new();
        for (_, spec) in TOOLS {
            assert!(names.insert(tool_name(spec.method)), "duplicate tool");
            assert!(methods.insert(spec.method), "duplicate method");
            for required in spec.required {
                assert!(
                    spec.fields.iter().any(|field| &field.name == required),
                    "{} requires undeclared field {required}",
                    spec.method
                );
            }
        }
        let expected: BTreeSet<&str> = [
            "swarm.tools.search",
            "host.status",
            "route.list",
            "client.list",
            "task.get",
            "task.list",
            "task.submission",
            "task.acceptance",
            "attempt.get",
            "operation.get",
            "operation.list",
            "agent.state",
            "agent.list",
            "agent.family",
            "check.get",
            "check.profiles",
            "artifact.get",
            "artifact.read",
            "artifact.parts",
            "report.delta",
            "report.attention",
            "report.capacity",
            "message.read",
            "swarm.context.get",
            "swarm.dashboard",
            "swarm.queue.get",
            "swarm.agent.inspect",
            "swarm.exceptions.get",
            "swarm.launch.preview",
            "swarm.launch",
            "swarm.overlap.check",
            "coordination.participant.get",
            "coordination.participant.list",
            "coordination.peer.find",
            "coordination.work_card.get",
            "coordination.work_card.list",
            "coordination.contract_card.get",
            "coordination.contract_card.list",
            "coordination.inbox",
            "coordination.watch.list",
            "review.get",
            "review.list",
            "swarm.review.context",
            "automation.config.get",
            "automation.config.preview",
            "automation.config.explain",
            "host.mode",
            "client.register",
            "source.capture",
            "check.run",
            "check.cancel",
            "task.create",
            "task.revise",
            "task.claim",
            "task.dispatch",
            "task.submit",
            "task.request_changes",
            "task.accept",
            "forge.publish_ref",
            "task.invalidate_acceptance",
            "attempt.release",
            "attempt.bind_producer",
            "agent.open",
            "agent.send",
            "agent.reply",
            "agent.configure",
            "agent.goal",
            "agent.refresh",
            "agent.reconcile",
            "agent.recover",
            "agent.result",
            "artifact.assemble",
            "operation.cancel",
            "gm.handover",
            "agent.background",
            "message.send",
            "message.cancel",
            "coordination.participant.register",
            "coordination.participant.disable",
            "coordination.work_card.publish",
            "coordination.work_card.withdraw",
            "coordination.contract_card.publish",
            "coordination.contract_card.withdraw",
            "coordination.send",
            "coordination.consult",
            "coordination.sync_integration",
            "coordination.watch.create",
            "coordination.watch.cancel",
            "review.assign",
            "review.submit",
            "automation.config.apply",
        ]
        .into_iter()
        .collect();
        assert_eq!(methods, expected);
        assert_eq!(TOOLS.len(), 91);
        assert_eq!(TOOLS.iter().filter(|(read_only, _)| *read_only).count(), 45);
        assert_eq!(
            TOOLS.iter().filter(|(read_only, _)| !*read_only).count(),
            46
        );
    }

    #[test]
    fn schemas_are_closed_objects() {
        for (read_only, spec) in TOOLS {
            let schema = input_schema(spec, *read_only, false);
            assert_eq!(schema["type"], json!("object"));
            assert_eq!(schema["additionalProperties"], json!(false));
            if !read_only {
                assert!(schema["properties"].get("client_request_id").is_some());
            }
        }
        let send = find_tool("message_send").unwrap();
        let schema = input_schema(&send.1, send.0, false);
        for field in [
            "in_reply_to",
            "in_reply_to_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
        ] {
            assert!(schema["properties"].get(field).is_some(), "{field}");
        }
        let output = output_schema("message.send").unwrap();
        for field in [
            "delivery_id",
            "payload_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
            "in_reply_to",
            "reply_to",
            "cancellation",
        ] {
            assert!(output["properties"].get(field).is_some(), "{field}");
        }
    }
}

mod catalog;
mod profiles;
mod subscriptions;

#[cfg(test)]
mod profiles_tests;
#[cfg(test)]
mod subscriptions_tests;
#[cfg(test)]
mod tasks_tests;
