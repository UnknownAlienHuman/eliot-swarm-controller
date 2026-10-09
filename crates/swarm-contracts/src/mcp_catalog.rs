//! Data-only MCP method schemas, catalog metadata, and presentation profiles.
//!
//! These descriptors are shared by the host and the independently built
//! swarm-mcp frontend. They describe presentation and wire shape only;
//! current authorization remains a per-request Store decision.
use crate::{
    concilium_limits as limits, coordination_limits,
    error::{Error, Result as ContractResult},
    mcp_frontend::McpToolProfile,
    method_policy,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, OnceLock},
};

type JsonObject = Map<String, Value>;
#[derive(Clone, Copy)]
pub struct Field {
    pub name: &'static str,
    pub kind: &'static str,
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

const CONTRACT_DECISION_FIELDS: &[Field] = &[
    f("thread_id", S),
    f("expected_state_revision", I),
    f("proposal_id", S),
    f("proposal_revision_id", S),
    f("proposal_digest", S),
    f("task_id", S),
    f("task_revision", I),
    f("attempt_id", S),
    f("affected_scope_revisions", A),
    f("reason", S),
    f("conditions", A),
    f("caveats", A),
];
const CONTRACT_DECISION_REQUIRED: &[&str] = &[
    "thread_id",
    "expected_state_revision",
    "proposal_id",
    "proposal_revision_id",
    "proposal_digest",
    "task_id",
    "task_revision",
    "attempt_id",
    "affected_scope_revisions",
    "reason",
    "conditions",
    "caveats",
];
const AGENT_BINDING_SCOPE_FIELDS: &[Field] = &[f("binding_id", S), f("generation", I)];
const AGENT_BINDING_SCOPE_REQUIRED: &[&str] = &["binding_id", "generation"];

pub struct ToolSpec {
    pub method: &'static str,
    pub description: &'static str,
    pub fields: &'static [Field],
    pub required: &'static [&'static str],
    input_schema_cache: [OnceLock<Arc<JsonObject>>; 2],
    input_schema_bytes_cache: [OnceLock<std::result::Result<Arc<[u8]>, String>>; 2],
    output_schema_cache: OnceLock<Option<Arc<JsonObject>>>,
    output_schema_bytes_cache: OnceLock<std::result::Result<Option<Arc<[u8]>>, String>>,
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
            input_schema_cache: [OnceLock::new(), OnceLock::new()],
            input_schema_bytes_cache: [OnceLock::new(), OnceLock::new()],
            output_schema_cache: OnceLock::new(),
            output_schema_bytes_cache: OnceLock::new(),
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
            input_schema_cache: [OnceLock::new(), OnceLock::new()],
            input_schema_bytes_cache: [OnceLock::new(), OnceLock::new()],
            output_schema_cache: OnceLock::new(),
            output_schema_bytes_cache: OnceLock::new(),
        },
    )
}

/// (read_only, spec). The read/write split matches `Store`'s read
/// classification plus the CLI's
/// request-ID treatment (`agent.result` and `host.mode` are mutations).
pub static TOOLS: &[(bool, ToolSpec)] = &TOOL_ITEMS;
static TOOL_ITEMS: [(bool, ToolSpec); 158] = [
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
        "monitor.snapshot",
        "Capture one Manager-authorized current-state snapshot and an atomic observation-journal cut for race-free follow-up.",
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
        "Preview one exact Task launch configuration under current manager authority. The response also carries a compact decision_card from bounded Store projections; unknown remains explicit. It validates configuration only and never starts a model or native session.",
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
        "code.scope.inspect",
        "Read retained scope intents with exact Task and optional Attempt/identity/path/symbol/interface selectors; this is advisory and never locks files.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("scope_intent_id", S),
            f("client_id", S),
            f("path", S),
            f("symbol", S),
            f("interface", S),
            f("after_scope_id", S),
            f("limit", I),
        ],
        &["task_id"],
    ),
    read(
        "code.scope.conflicts",
        "Compare retained scope intents by exact bounded selectors; incomplete or unsupported coverage remains unknown rather than no-conflict.",
        &[
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("scope_intent_id", S),
            f("client_id", S),
            f("path", S),
            f("symbol", S),
            f("interface", S),
            f("after_scope_id", S),
            f("limit", I),
        ],
        &["task_id"],
    ),
    read(
        "concilium.preview",
        "Build a deterministic scoped plan and packet digest for a proposed Concilium; preview has no model or native effect.",
        &[f("proposal_operation_id", S)],
        &["proposal_operation_id"],
    ),
    read(
        "concilium.get",
        "Read one bounded Concilium projection in the authenticated scope; first-round peer positions remain blind until sealed.",
        &[f("concilium_id", S), f("limit", I), f("after_slot_id", S)],
        &["concilium_id"],
    ),
    read(
        "concilium.list",
        "Page Concilium projections visible in the authenticated scope with bounded Task/Attempt and state filters.",
        &[
            f("task_id", S),
            f("attempt_id", S),
            f("state", S),
            f("limit", I),
            f("after_concilium_id", S),
        ],
        &["task_id"],
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
        "coordination.thread.get",
        "Read one authorized retained coordination Thread with bounded message history; Store authorization is rechecked for the exact Thread.",
        &[f("thread_id", S), f("after_message_seq", I), f("limit", I)],
        &["thread_id"],
    ),
    read(
        "coordination.thread.list",
        "Page authorized retained Threads within one exact Task and optional Attempt; there is no global Thread scan.",
        &[
            f("task_id", S),
            f("attempt_id", S),
            f("state", S),
            f("topic_kind", S),
            f("limit", I),
            f("after_thread_id", S),
        ],
        &["task_id"],
    ),
    read(
        "coordination.contract.get",
        "Read one immutable contract proposal revision in its exact authorized Thread; use its revision ID and digest for decision or response calls.",
        &[
            f("thread_id", S),
            f("proposal_id", S),
            f("proposal_revision_id", S),
            f("after_observation_id", I),
            f("limit", I),
        ],
        &["thread_id", "proposal_id", "proposal_revision_id"],
    ),
    read(
        "coordination.contract.list",
        "Page metadata-only contract proposals in one authorized Thread; fetch an exact revision through coordination.contract.get.",
        &[f("thread_id", S), f("after_sequence", I), f("limit", I)],
        &["thread_id"],
    ),
    read(
        "coordination.agreement.get",
        "Read the bounded retained agreement cell and position history for one exact Task/Attempt and optional historical revision+digest pair.",
        &[
            f("cell_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("state_revision", I),
            f("material_digest", S),
            f("limit", I),
            f("after_position_id", S),
        ],
        &["cell_id", "task_id", "task_revision", "attempt_id"],
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
    read(
        "bus.events.page",
        "Read a bounded, non-acknowledging page of safe event metadata selected by one enabled ScriptRun consumer owned by the authenticated Manager.",
        &[
            f("project_id", S),
            f("consumer_id", S),
            f("after_observation_id", I),
            f("limit", I),
        ],
        &["project_id", "consumer_id"],
    ),
    read("host.status", "Controller status snapshot.", &[], &[]),
    read(
        "route.list",
        "Configured routes; not live qualification.",
        &[],
        &[],
    ),
    read(
        "module.catalog.get",
        "Page locally registered module descriptors and your own exact future route selections. Launch paths, argv, working directories and protected references are redacted; this read never probes or starts a module.",
        &[f("after", I), f("limit", I)],
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
        "logging.get",
        "Read the authenticated ordinary Manager's metadata or bounded Atlas-redacted-text diagnostic policy for its client, exact current Task/Attempt, owned Operation, binding route, or retained module descriptor, plus the live Producer projection.",
        &[
            f("client_id", SN),
            f("task_id", SN),
            f("task_revision", IN),
            f("attempt_id", SN),
            f("operation_id", SN),
            f("binding_id", SN),
            f("binding_generation", IN),
            f("module_id", SN),
        ],
        &[],
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
        AGENT_BINDING_SCOPE_FIELDS,
        AGENT_BINDING_SCOPE_REQUIRED,
    ),
    read(
        "agent.usage",
        "Observed usage counters for one binding generation.",
        AGENT_BINDING_SCOPE_FIELDS,
        AGENT_BINDING_SCOPE_REQUIRED,
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
        &[f("after", I), f("limit", I), f("head", B), f("through", I)],
        &[],
    ),
    read(
        "monitor.follow",
        "Read one bounded retained observation page after a monitor journal cursor, with explicit retention, gap, lag, and current-coverage facts.",
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
        "code.scope.propose",
        "Propose advisory paths, symbols or interfaces for exact Attempt ownership review; proposals reserve no files and invoke no Git process.",
        &[
            f("task_id", S),
            f("task_revision", IN),
            f("attempt_id", S),
            f("assignment_id", SN),
            f("mode", S),
            f("paths", A),
            f("symbols", A),
            f("interfaces", A),
            f("baseline_candidate_ref", S),
            f("reason", S),
            f("suggested_expires_at_ms", IN),
        ],
        &[
            "task_id",
            "attempt_id",
            "mode",
            "paths",
            "symbols",
            "interfaces",
            "baseline_candidate_ref",
            "reason",
        ],
    ),
    mutation(
        "code.scope.accept",
        "Accept one exact scope-intent proposal digest under current Task/Attempt authority, recording any explicit narrowing, broad-scope acknowledgement or override.",
        &[
            f("scope_intent_id", S),
            f("expected_state_revision", I),
            f("proposal_digest", S),
            f("mode", SN),
            f("paths", A),
            f("symbols", A),
            f("interfaces", A),
            f("expires_at_ms", IN),
            f("reason", S),
            f("acknowledge_broad_scope", B),
            f("override_scope_intent_ids", A),
        ],
        &[
            "scope_intent_id",
            "expected_state_revision",
            "proposal_digest",
            "reason",
        ],
    ),
    mutation(
        "code.scope.release",
        "Release one exact accepted scope intent with a reason and verified retained readback; expiry alone never releases it.",
        &[
            f("scope_intent_id", S),
            f("expected_state_revision", I),
            f("reason", S),
        ],
        &["scope_intent_id", "expected_state_revision", "reason"],
    ),
    mutation(
        "coordination.thread.open",
        "Open one durable mailbox-only Thread in an exact Task/Attempt with an explicit, verified Manager/Participant roster; this creates no assignment or model work.",
        &[
            f("task_id", S),
            f("attempt_id", S),
            f("assignment_id", SN),
            f("topic_kind", S),
            f("subject", S),
            f("participants", A),
            f("reasonability", O),
            f("related_scopes", A),
            f("body_ref", SN),
            f("delivery_mode", S),
            f("supersedes_thread_id", SN),
        ],
        &[
            "task_id",
            "attempt_id",
            "topic_kind",
            "subject",
            "participants",
            "reasonability",
            "delivery_mode",
        ],
    ),
    mutation(
        "coordination.message.send",
        "Send one bounded mailbox-only message to one exact current Thread roster recipient; it does not add recipients, assign work, or start a model call.",
        &[
            f("thread_id", S),
            f("recipient", S),
            f("speech_act", S),
            f("subject", S),
            f("summary", S),
            f("inline_body", SN),
            f("body_ref", SN),
            f("reply_to_message_id", SN),
            f("in_reply_to_digest", SN),
            f("requires_reply", B),
            f("reply_deadline_ms", IN),
            f("evidence_refs", A),
            f("proposal_revision_id", SN),
            f("delivery_mode", S),
        ],
        &[
            "thread_id",
            "recipient",
            "speech_act",
            "subject",
            "summary",
            "requires_reply",
            "delivery_mode",
        ],
    ),
    mutation(
        "coordination.thread.resolve",
        "Close one exact Thread as resolved, unresolved, or withdrawn under current scope authority and expected state revision; a contract Thread requires its exact ratification Operation to resolve.",
        &[
            f("thread_id", S),
            f("expected_state_revision", I),
            f("outcome", S),
            f("resolution_summary", S),
            f("selected_proposal_revision_id", SN),
            f("remaining_objections", A),
            f("follow_up_operation_ids", A),
            f("manager_ratification_operation_id", SN),
        ],
        &[
            "thread_id",
            "expected_state_revision",
            "outcome",
            "resolution_summary",
            "remaining_objections",
            "follow_up_operation_ids",
        ],
    ),
    mutation(
        "coordination.thread.withdraw",
        "Withdraw one exact Thread under creator/current-scope authority and expected state revision; retained history remains readable to authorized parties.",
        &[
            f("thread_id", S),
            f("expected_state_revision", I),
            f("reason", S),
        ],
        &["thread_id", "expected_state_revision", "reason"],
    ),
    mutation(
        "coordination.thread.supersede",
        "Supersede one exact Thread with an open successor on the same Task/Attempt under current manager authority.",
        &[
            f("thread_id", S),
            f("expected_state_revision", I),
            f("superseding_thread_id", S),
            f("reason", S),
        ],
        &[
            "thread_id",
            "expected_state_revision",
            "superseding_thread_id",
            "reason",
        ],
    ),
    mutation(
        "coordination.contract.propose",
        "Create or revise one immutable contract proposal inside an exact authorized coordination Thread; the canonical request is capped at 64 KiB.",
        &[
            f("thread_id", S),
            f("supersedes_revision_id", SN),
            f("topic", S),
            f("affected", O),
            f("statement", O),
            f("acceptance_conditions", A),
            f("claims", A),
            f("open_questions", A),
        ],
        &[
            "thread_id",
            "supersedes_revision_id",
            "topic",
            "affected",
            "statement",
            "acceptance_conditions",
            "claims",
            "open_questions",
        ],
    ),
    mutation(
        "coordination.contract.respond",
        "Record one exact participant response to an immutable contract revision; support is advisory and never ratifies a proposal.",
        &[
            f("thread_id", S),
            f("proposal_id", S),
            f("proposal_revision_id", S),
            f("proposal_digest", S),
            f("act", S),
            f("objection_basis", SN),
            f("reason", S),
            f("evidence_refs", A),
        ],
        &[
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "proposal_digest",
            "act",
            "objection_basis",
            "reason",
            "evidence_refs",
        ],
    ),
    mutation(
        "coordination.contract.ratify",
        "Ratify one exact current contract proposal under current manager authority, retaining immutable scope, decision and caveats. This does not accept the Task or start native work.",
        CONTRACT_DECISION_FIELDS,
        CONTRACT_DECISION_REQUIRED,
    ),
    mutation(
        "coordination.contract.reject",
        "Reject one exact current contract proposal under current manager authority. The immutable rejection leaves the Thread open and does not start native work.",
        CONTRACT_DECISION_FIELDS,
        CONTRACT_DECISION_REQUIRED,
    ),
    mutation(
        "coordination.integration.ack",
        "Record one Participant's accept/dissent position against the exact current agreement cell state and digests; acceptance applies only to this comparison and does not accept the Task or ratify a contract.",
        &[
            f("cell_id", S),
            f("expected_state_revision", I),
            f("expected_material_digest", S),
            f("expected_membership_digest", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("decision", S),
        ],
        &[
            "cell_id",
            "expected_state_revision",
            "expected_material_digest",
            "expected_membership_digest",
            "task_id",
            "task_revision",
            "attempt_id",
            "decision",
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
        "concilium.propose",
        "Propose one bounded Concilium under the authenticated participant or manager's current Task/Attempt scope; this creates manager attention only.",
        &[
            f("task_id", S),
            f("attempt_id", S),
            f("failed_thread_id", S),
            f("decision_question", S),
            f("material_conflict", S),
            f("participants", A),
            f("proposal_revision_ids", A),
            f("evidence_refs", A),
            f("expected_output", S),
            f("suggested_max_rounds", I),
            f("suggested_budget", O),
            f("close_condition", S),
        ],
        &[
            "task_id",
            "attempt_id",
            "failed_thread_id",
            "decision_question",
            "material_conflict",
            "participants",
            "proposal_revision_ids",
            "evidence_refs",
            "expected_output",
            "suggested_max_rounds",
            "suggested_budget",
            "close_condition",
        ],
    ),
    mutation(
        "concilium.open",
        "Commit the exact current previewed Concilium plan and slots; opening invokes no participant or model.",
        &[
            f("proposal_operation_id", S),
            f("plan_digest", S),
            f("confirmed_reasonable", B),
            f("manager_reason", S),
        ],
        &[
            "proposal_operation_id",
            "plan_digest",
            "confirmed_reasonable",
            "manager_reason",
        ],
    ),
    mutation(
        "concilium.position.submit",
        "Submit one structured response to the authenticated participant's exact Concilium slot and packet.",
        &[
            f("concilium_id", S),
            f("slot_id", S),
            f("packet_digest", S),
            f("position", O),
        ],
        &["concilium_id", "slot_id", "packet_digest", "position"],
    ),
    mutation(
        "concilium.round.advance",
        "Commit the manager-selected next Concilium round with compare-and-swap; Store builds each slot packet and this selects no speaker.",
        &[
            f("concilium_id", S),
            f("expected_state_revision", I),
            f("next_round", I),
            f("merged_proposal_digest", SN),
            f("manager_reason", S),
        ],
        &[
            "concilium_id",
            "expected_state_revision",
            "next_round",
            "manager_reason",
        ],
    ),
    mutation(
        "concilium.close",
        "Record a manager-authorized advisory Concilium outcome while preserving valid positions and dissent; no Task or contract is mutated.",
        &[
            f("concilium_id", S),
            f("expected_state_revision", I),
            f("result", S),
            f("recommendation", SN),
            f("manager_reason", S),
        ],
        &[
            "concilium_id",
            "expected_state_revision",
            "result",
            "manager_reason",
        ],
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
        "logging.set",
        "Persist one bounded diagnostic level and metadata or Atlas-redacted text policy for the authenticated ordinary Manager's client, exact current Task/Attempt, owned Operation, binding route, or retained module descriptor and apply it to the live Producer after commit.",
        &[
            f("client_id", SN),
            f("task_id", SN),
            f("task_revision", IN),
            f("attempt_id", SN),
            f("operation_id", SN),
            f("binding_id", SN),
            f("binding_generation", IN),
            f("module_id", SN),
            f("level", S),
            f("content", S),
            f("ttl_seconds", IN),
        ],
        &["level", "content"],
    ),
    mutation(
        "event.emit",
        "Record one bounded Manager-owned system event for an exact project. The event is durable and may trigger the configured ScriptRun action; payload and cause remain private to the owning Manager.",
        &[
            f("project_id", S),
            f("name", S),
            f("payload", "non_null_json"),
            f("dedupe_key", S),
            f("cause", O),
        ],
        &["project_id", "name", "payload", "dedupe_key"],
    ),
    mutation(
        "bus.consumer.admit",
        "Compare the current Manager-owned ScriptRun cursor, revalidate one bounded exact event page and its canonical ScriptRun actions, retain pending intents, and advance the existing cursor in the same Store transaction. The existing ScriptRun continuation admits script.run later.",
        &[
            f("project_id", S),
            f("consumer_id", S),
            f("automation_revision", I),
            f("expected_cursor", I),
            f("through_observation_id", I),
            f("occurrences", A),
        ],
        &[
            "project_id",
            "consumer_id",
            "automation_revision",
            "expected_cursor",
            "through_observation_id",
            "occurrences",
        ],
    ),
    mutation(
        "schedule.run_now",
        "Run the authenticated Manager's saved CheckRun action once without enabling recurrence. Supply the same client_request_id to read back this manual invocation.",
        &[f("project_id", S), f("automation_id", S)],
        &["project_id", "automation_id"],
    ),
    mutation(
        "automation.config.transfer",
        "Transfer one former-manager automation to the current GM while preserving its cursors and pending operations. Requires current GM or local Operator authority and the exact source revision.",
        &[
            f("project_id", S),
            f("former_owner_manager_id", S),
            f("automation_id", S),
            f("expected_revision", I),
        ],
        &[
            "project_id",
            "former_owner_manager_id",
            "automation_id",
            "expected_revision",
        ],
    ),
    mutation(
        "host.mode",
        "Enable or disable admission of new work on the host: new_work is the string \"enabled\" or \"disabled\".",
        &[f("new_work", S)],
        &["new_work"],
    ),
    mutation(
        "module.route.select",
        "Select one exact, already trusted module descriptor for your future bindings on a configured route. The selection is scoped to your authenticated Manager identity; it does not register or launch an artifact and does not change existing bindings or another Manager's route.",
        &[
            f("route_alias", S),
            f("module_id", S),
            f("artifact_id", S),
            f("version", S),
            f("expected_catalog_revision", I),
        ],
        &[
            "route_alias",
            "module_id",
            "artifact_id",
            "version",
            "expected_catalog_revision",
        ],
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
        "task.submit.recover",
        "Recover an unknown submission from its exact existing artifact as the current GM or operator; does not publish files or replay native work.",
        &[f("operation_id", S)],
        &["operation_id"],
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
        "Set, edit, pause, resume, continue or clear the goal of one binding; continue admits one input for an exact active revision.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("action", S),
            f("objective", S),
            f("expected_revision", I),
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
    read(
        "hook.source.get",
        "Read public metadata and bounded retained facts for one authenticated hook source; credentials are never returned.",
        &[f("source_id", S), f("after", I), f("limit", I)],
        &["source_id"],
    ),
    mutation(
        "hook.source.revoke",
        "Disable one exact setup-issued hook source using revision compare-and-swap.",
        &[f("source_id", S), f("expected_revision", I)],
        &["source_id", "expected_revision"],
    ),
    mutation(
        "goal.create",
        "Create a task-scoped tracking goal; it starts no Task, model, or native work.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
            f("expected_revision", I),
            f("objective", S),
            f("completion_evidence", O),
            f("reminder", O),
            f("enabled", B),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "expected_revision",
            "objective",
            "completion_evidence",
        ],
    ),
    mutation(
        "goal.revise",
        "Revise one task-scoped tracking goal with revision compare-and-swap; changing enabled state does not start work.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
            f("expected_revision", I),
            f("objective", S),
            f("completion_evidence", O),
            f("reminder", O),
            f("enabled", B),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "expected_revision",
        ],
    ),
    mutation(
        "goal.enable",
        "Enable one revision-checked Goal reminder entry; this never dispatches Task or model work.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
            f("expected_revision", I),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "expected_revision",
        ],
    ),
    mutation(
        "goal.disable",
        "Disable one revision-checked Goal reminder entry while retaining readback and history.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
            f("expected_revision", I),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "expected_revision",
        ],
    ),
    mutation(
        "goal.readback",
        "Persist a fresh evidence evaluation for one exact retained Goal scope under an Operation receipt.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
        ],
    ),
    read(
        "goal.get",
        "Read one exact task-scoped Goal and its retained completion/reminder projection.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("goal_id", S),
        ],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
        ],
    ),
    read(
        "goal.list",
        "Page Goals only within one exact retained project, Task revision, and Attempt scope.",
        &[
            f("project_id", S),
            f("task_id", S),
            f("task_revision", I),
            f("attempt_id", S),
            f("after_goal_id", S),
            f("limit", I),
        ],
        &["project_id", "task_id", "task_revision", "attempt_id"],
    ),
    mutation(
        "script.register",
        "Register one complete bounded trusted-local script bundle; invocation effects are explicit and limited to the bundle's closed grant list.",
        &[f("bundle", O)],
        &["bundle"],
    ),
    mutation(
        "script.revise",
        "Publish a complete replacement bundle under revision compare-and-swap, including its closed invocation effect grants.",
        &[f("script_id", S), f("expected_revision", I), f("bundle", O)],
        &["script_id", "expected_revision", "bundle"],
    ),
    read(
        "script.validate",
        "Validate one retained revision and report its declared invocation effects; it does not execute the script.",
        &[f("script_id", S), f("revision", I)],
        &["script_id", "revision"],
    ),
    mutation(
        "script.activate",
        "Select one retained script revision for future authorized runs; activation does not execute it.",
        &[f("script_id", S), f("revision", I)],
        &["script_id", "revision"],
    ),
    mutation(
        "script.run",
        "Start one authorized invocation for an exact Attempt and Task revision. task_owner_message requires its exact Attempt; manager_notification derives its recipient from a taskless event run owner; task_create accepts a strict TaskSpec in that event automation project. Effects are limited to one and reported with the completion.",
        &[
            f("script_id", S),
            f("expected_script_revision", I),
            f("attempt_id", S),
            f("expected_task_revision", I),
            f("input", "any"),
        ],
        &[
            "script_id",
            "expected_script_revision",
            "attempt_id",
            "expected_task_revision",
            "input",
        ],
    ),
    read(
        "script.get",
        "Read one script revision's metadata; bundle bytes are not returned.",
        &[f("script_id", S), f("revision", IN)],
        &["script_id"],
    ),
    read(
        "script.list",
        "Page authorized script registry metadata without bundle bytes.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "github.source.inspect",
        "Inspect one GitHub repository through the installed gh account and return bounded public facts.",
        &[f("host", S), f("owner", S), f("repo", S)],
        &["host", "owner", "repo"],
    ),
    mutation(
        "github.source.setup",
        "Register one explicitly inspected GitHub repository for bounded issue intake.",
        &[
            f("source_id", S),
            f("project_id", S),
            f("host", S),
            f("owner", S),
            f("repo", S),
            f("repository_id", I),
        ],
        &[
            "source_id",
            "project_id",
            "host",
            "owner",
            "repo",
            "repository_id",
        ],
    ),
    read(
        "github.source.get",
        "Read public metadata and bounded coverage state for one registered GitHub source.",
        &[f("source_id", S)],
        &["source_id"],
    ),
    mutation(
        "github.source.poll",
        "Poll one registered source once, retaining bounded issue facts and exact coverage.",
        &[f("source_id", S)],
        &["source_id"],
    ),
    read(
        "github.work_pool.preview",
        "Preview bounded source-mapped Tasks eligible for explicit local work-pool admission.",
        &[f("source_id", S), f("after", S), f("limit", I)],
        &["source_id"],
    ),
    mutation(
        "github.work_pool.apply",
        "Apply an explicit bounded selection of source-mapped Tasks to the existing local work pool.",
        &[f("source_id", S), f("task_ids", A)],
        &["source_id", "task_ids"],
    ),
    mutation(
        "github.effect.managed_label",
        "Set or remove one Eliot-managed label on a selected source-mapped Issue.",
        &[
            f("source_id", S),
            f("task_id", S),
            f("expected_task_revision", I),
            f("label", S),
            f("present", B),
        ],
        &[
            "source_id",
            "task_id",
            "expected_task_revision",
            "label",
            "present",
        ],
    ),
    mutation(
        "github.effect.reconcile_managed_label",
        "Read back one exact unknown Eliot-managed label Operation without sending a label write.",
        &[f("operation_id", S)],
        &["operation_id"],
    ),
    mutation(
        "github.pull_request.update_description",
        "Update the title and body of one open PR only when its exact repository, published head SHA, and caller-selected base ref match retained readback.",
        &[
            f("publication_operation_id", S),
            f("pull_request_id", I),
            f("pull_request_number", I),
            f("base_ref", S),
            f("title", S),
            f("body", S),
        ],
        &[
            "publication_operation_id",
            "pull_request_id",
            "pull_request_number",
            "base_ref",
            "title",
            "body",
        ],
    ),
    mutation(
        "github.pull_request.reconcile_description",
        "Read back one exact unknown PR description Operation; this route performs GET-only reconciliation and cannot send or retry a PATCH.",
        &[f("operation_id", S)],
        &["operation_id"],
    ),
];

/// Canonical application methods advertised by MCP. The catalog search is a
/// local facade method and is intentionally excluded from the Store registry.
pub fn registered_application_methods() -> Vec<&'static str> {
    // Discovery authorization follows the contracts policy. The typed table
    // remains the schema inventory; catalog validation rejects drift.
    method_policy::METHOD_REGISTRY
        .iter()
        .filter_map(|entry| {
            (entry.mcp
                && entry.method != "swarm.tools.search"
                && TOOLS.iter().any(|(_, spec)| spec.method == entry.method))
            .then_some(entry.method)
        })
        .collect()
}

/// Read/mutation semantics from the contracts policy shared with Store and CLI.
/// The typed TOOLS table remains schema inventory, not application authorization.
pub fn application_method_read_only(method: &str) -> Option<bool> {
    if !method_policy::is_mcp_method(method)
        || method == "swarm.tools.search"
        || !TOOLS.iter().any(|(_, spec)| spec.method == method)
    {
        return None;
    }
    method_policy::read_only(method)
}

pub fn tool_name(method: &str) -> String {
    method.replace('.', "_")
}

pub fn find_tool_spec(method: &str) -> Option<&'static (bool, ToolSpec)> {
    TOOLS.iter().find(|(_, spec)| spec.method == method)
}

fn concilium_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":limits::MAX_IDENTIFIER_BYTES,
        "pattern":"^\\S+$",
        "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
    })
}

fn concilium_optional_id_schema() -> Value {
    json!({"oneOf":[concilium_id_schema(),{"type":"null"}]})
}

fn concilium_client_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":limits::MAX_CLIENT_ID_BYTES,
        "pattern":"^\\S+$",
        "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
    })
}

fn concilium_text_schema(max_utf8_bytes: usize, min_length: usize) -> Value {
    let mut schema = json!({
        "type":"string",
        "minLength":min_length,
        "maxLength":max_utf8_bytes,
        "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
    });
    if min_length > 0 {
        schema["pattern"] = json!("\\S");
    }
    schema
}

fn concilium_reference_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":limits::MAX_EVIDENCE_REF_BYTES,
        "pattern":"\\S",
        "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
    })
}

fn concilium_digest_schema() -> Value {
    json!({
        "type":"string",
        "pattern":"^sha256:[0-9a-f]{64}$"
    })
}

fn concilium_evidence_refs_schema() -> Value {
    json!({
        "type":"array",
        "maxItems":limits::MAX_EVIDENCE_REFS,
        "uniqueItems":true,
        "items":concilium_reference_schema()
    })
}

fn concilium_participant_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "client_id":concilium_client_id_schema(),
            "generation":{"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64},
            "reason":concilium_text_schema(limits::MAX_PARTICIPANT_REASON_BYTES, 1)
        },
        "required":["client_id","reason"],
        "additionalProperties":false
    })
}

fn concilium_claim_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "claim_id":concilium_id_schema(),
            "stance":{"type":"string","enum":["support","oppose","uncertain"]},
            "fact":concilium_text_schema(limits::MAX_CLAIM_TEXT_BYTES, 1),
            "evidence_refs":concilium_evidence_refs_schema(),
            "counterexample":{"oneOf":[concilium_text_schema(limits::MAX_CLAIM_TEXT_BYTES, 1),{"type":"null"}]},
            "falsifier":concilium_text_schema(limits::MAX_CLAIM_TEXT_BYTES, 1),
            "assumptions":{"type":"array","items":concilium_text_schema(limits::MAX_QUESTION_BYTES, 1)},
            "confidence":{"type":"string","enum":["low","medium","high"]}
        },
        "required":["claim_id","stance","fact","evidence_refs","counterexample","falsifier","assumptions","confidence"],
        "additionalProperties":false
    })
}

fn concilium_position_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "position":{"type":"string","enum":["support","oppose","alternative","insufficient_evidence"]},
            "proposal_revision_id":{"type":["string","null"],"minLength":1,"maxLength":limits::MAX_IDENTIFIER_BYTES,"pattern":"^\\S+$","description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."},
            "claims":{"type":"array","maxItems":limits::MAX_CLAIMS_PER_POSITION,"items":concilium_claim_schema()},
            "required_change":{"type":"string","maxLength":limits::MAX_POSITION_TEXT_BYTES,"description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."},
            "unresolved_questions":{"type":"array","items":concilium_text_schema(limits::MAX_QUESTION_BYTES, 1)}
        },
        "required":["position","proposal_revision_id","claims","required_change","unresolved_questions"],
        "anyOf":[
            {"properties":{"claims":{"minItems":1}}},
            {"properties":{"required_change":{"minLength":1,"pattern":"\\S"}}},
            {"properties":{"unresolved_questions":{"minItems":1}}}
        ],
        "additionalProperties":false
    })
}

pub fn effect_requires_preknown_request_id(method: &str) -> bool {
    matches!(method, "swarm.launch" | "schedule.run_now")
}

pub fn mutation_requires_caller_request_id(
    profile: McpToolProfile,
    method: &str,
    read_only: bool,
) -> bool {
    !read_only && (profile != McpToolProfile::Full || effect_requires_preknown_request_id(method))
}

pub fn input_schema(
    spec: &ToolSpec,
    read_only: bool,
    request_id_required: bool,
) -> Arc<JsonObject> {
    let cache_index = usize::from(request_id_required);
    Arc::clone(
        spec.input_schema_cache[cache_index]
            .get_or_init(|| build_input_schema(spec, read_only, request_id_required)),
    )
}

pub fn input_schema_bytes(
    spec: &ToolSpec,
    read_only: bool,
    request_id_required: bool,
) -> std::result::Result<Arc<[u8]>, String> {
    let cache_index = usize::from(request_id_required);
    let schema = input_schema(spec, read_only, request_id_required);
    spec.input_schema_bytes_cache[cache_index]
        .get_or_init(|| {
            serde_json::to_vec(schema.as_ref())
                .map(Arc::<[u8]>::from)
                .map_err(|error| error.to_string())
        })
        .clone()
}

fn build_input_schema(
    spec: &ToolSpec,
    read_only: bool,
    request_id_required: bool,
) -> Arc<JsonObject> {
    let mut properties = JsonObject::new();
    for field in spec.fields {
        properties.insert(field.name.to_string(), field_schema(field.kind));
    }
    if !read_only {
        properties.insert(
            "client_request_id".to_string(),
            json!({
                "type": "string",
                "description": if request_id_required {
                    "Caller-owned stable logical request ID. Choose it before dispatch and reuse it to reconcile a lost reply; the server does not retry mutations."
                } else {
                    "Caller-owned stable logical request ID. Reuse it to reconcile a lost reply; the local full compatibility profile generates one only when omitted and a result arrives."
                }
            }),
        );
        if effect_requires_preknown_request_id(spec.method) {
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
    if !read_only && request_id_required {
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
    if matches!(method, "script.register" | "script.revise") {
        schema["$defs"] = json!({"ScriptValueSchema":script_value_schema_definition()});
    }
    if method.starts_with("concilium.") {
        schema["description"] = json!(format!(
            "Concilium input is limited to {} UTF-8 bytes. The wire parser enforces byte bounds because JSON Schema maxLength counts Unicode code points.",
            limits::MAX_CONCILIUM_REQUEST_BYTES
        ));
    }
    let properties = &mut schema["properties"];
    if matches!(
        method,
        "automation.config.preview" | "automation.config.apply"
    ) {
        properties["changes"] = automation_config_changes_schema();
    }
    match method {
        "concilium.propose" => {
            properties["client_request_id"] = json!({
                "type":"string","minLength":1,"maxLength":limits::MAX_CLIENT_REQUEST_ID_BYTES,"pattern":"^\\S+$",
                "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
            });
            for name in ["task_id", "attempt_id", "failed_thread_id"] {
                properties[name] = concilium_id_schema();
            }
            properties["decision_question"] = concilium_text_schema(limits::MAX_QUESTION_BYTES, 1);
            properties["material_conflict"] = concilium_text_schema(limits::MAX_CONFLICT_BYTES, 1);
            for name in ["expected_output", "close_condition"] {
                properties[name] = concilium_text_schema(limits::MAX_QUESTION_BYTES, 1);
            }
            properties["participants"] = json!({
                "type":"array",
                "minItems":1,
                "items":concilium_participant_schema()
            });
            properties["proposal_revision_ids"] = json!({
                "type":"array",
                "uniqueItems":true,
                "items":concilium_id_schema()
            });
            properties["evidence_refs"] = concilium_evidence_refs_schema();
            properties["suggested_max_rounds"] = json!({"type":"integer","minimum":1,"maximum":3});
            properties["suggested_budget"] = json!({"type":"object"});
        }
        "concilium.preview" => {
            properties["proposal_operation_id"] = concilium_id_schema();
        }
        "concilium.open" => {
            properties["client_request_id"] = json!({
                "type":"string","minLength":1,"maxLength":limits::MAX_CLIENT_REQUEST_ID_BYTES,"pattern":"^\\S+$",
                "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
            });
            properties["proposal_operation_id"] = concilium_id_schema();
            properties["plan_digest"] = concilium_digest_schema();
            properties["confirmed_reasonable"] = json!({"const":true});
            properties["manager_reason"] = concilium_text_schema(limits::MAX_QUESTION_BYTES, 1);
        }
        "concilium.position.submit" => {
            properties["client_request_id"] = json!({
                "type":"string","minLength":1,"maxLength":limits::MAX_CLIENT_REQUEST_ID_BYTES,"pattern":"^\\S+$",
                "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
            });
            properties["concilium_id"] = concilium_id_schema();
            properties["slot_id"] = concilium_id_schema();
            properties["packet_digest"] = concilium_digest_schema();
            properties["position"] = concilium_position_schema();
        }
        "concilium.round.advance" => {
            properties["client_request_id"] = json!({
                "type":"string","minLength":1,"maxLength":limits::MAX_CLIENT_REQUEST_ID_BYTES,"pattern":"^\\S+$",
                "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
            });
            properties["concilium_id"] = concilium_id_schema();
            properties["expected_state_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["next_round"] = json!({"type":"integer","enum":[2,3]});
            properties["merged_proposal_digest"] = json!({
                "oneOf":[concilium_digest_schema(),{"type":"null"}]
            });
            properties["manager_reason"] = concilium_text_schema(limits::MAX_QUESTION_BYTES, 1);
            append_all_of(
                schema,
                json!({
                    "oneOf":[
                        {
                            "properties":{
                                "next_round":{"const":2},
                                "merged_proposal_digest":{"type":"null"}
                            }
                        },
                        {
                            "properties":{
                                "next_round":{"const":3},
                                "merged_proposal_digest":concilium_digest_schema()
                            },
                            "required":["merged_proposal_digest"]
                        }
                    ]
                }),
            );
        }
        "concilium.close" => {
            properties["client_request_id"] = json!({
                "type":"string","minLength":1,"maxLength":limits::MAX_CLIENT_REQUEST_ID_BYTES,"pattern":"^\\S+$",
                "description":"The wire parser enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."
            });
            properties["concilium_id"] = concilium_id_schema();
            properties["expected_state_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["result"] = json!({
                "type":"string",
                "enum":["recommended","minority_report","insufficient_evidence","irreconcilable_contract","cancelled","failed"]
            });
            properties["recommendation"] = json!({
                "oneOf":[concilium_id_schema(),{"type":"null"}]
            });
            properties["manager_reason"] = concilium_text_schema(limits::MAX_QUESTION_BYTES, 1);
            append_all_of(
                schema,
                json!({
                    "oneOf":[
                        {
                            "properties":{
                                "result":{"const":"recommended"},
                                "recommendation":concilium_id_schema()
                            },
                            "required":["recommendation"]
                        },
                        {
                            "properties":{
                                "result":{"enum":["minority_report","insufficient_evidence","irreconcilable_contract","cancelled","failed"]},
                                "recommendation":{"type":["string","null"]}
                            }
                        }
                    ]
                }),
            );
        }
        "concilium.get" => {
            properties["concilium_id"] = concilium_id_schema();
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":limits::MAX_READ_PAGE_SIZE,"default":limits::DEFAULT_READ_PAGE_SIZE});
            properties["after_slot_id"] = concilium_optional_id_schema();
        }
        "concilium.list" => {
            properties["task_id"] = concilium_id_schema();
            properties["attempt_id"] = concilium_optional_id_schema();
            properties["state"] = json!({
                "oneOf":[
                    {"type":"string","enum":["proposed","planned","round_1_open","round_1_ready","round_2_open","round_2_ready","merge_available","completed","unresolved","cancelled","failed"]},
                    {"type":"null"}
                ]
            });
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":limits::MAX_READ_PAGE_SIZE,"default":limits::DEFAULT_READ_PAGE_SIZE});
            properties["after_concilium_id"] = concilium_optional_id_schema();
        }
        "logging.get" | "logging.set" => {
            for field in [
                "client_id",
                "task_id",
                "attempt_id",
                "operation_id",
                "binding_id",
                "module_id",
            ] {
                properties[field] = json!({
                    "type":["string","null"],
                    "minLength":1,
                    "maxLength":128,
                    "pattern":"^[A-Za-z0-9._:-]+$"
                });
            }
            properties["task_revision"] = json!({
                "type":["integer","null"],
                "minimum":1,
                "maximum":9223372036854775807_i64
            });
            properties["binding_generation"] = json!({
                "type":["integer","null"],
                "minimum":1,
                "maximum":9223372036854775807_i64
            });
            if method == "logging.set" {
                properties["level"] = json!({
                    "type":"string",
                    "enum":["off","error","warn","info","debug","trace"]
                });
                properties["content"] = json!({
                    "type":"string",
                    "enum":["metadata","redacted_text"]
                });
                properties["ttl_seconds"] = json!({
                    "type":["integer","null"],
                    "minimum":1,
                    "maximum":86400
                });
            }
            append_all_of(
                schema,
                json!({
                    "oneOf":[
                        {"not":{"anyOf":[{"required":["task_id"]},{"required":["task_revision"]},{"required":["attempt_id"]}]}},
                        {"required":["task_id","task_revision","attempt_id"]}
                    ]
                }),
            );
            append_all_of(
                schema,
                json!({
                    "oneOf":[
                        {"not":{"anyOf":[{"required":["binding_id"]},{"required":["binding_generation"]}]}},
                        {"required":["binding_id","binding_generation"]}
                    ]
                }),
            );
        }
        "module.catalog.get" => {
            properties["after"] =
                json!({"type":"integer","minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":8});
        }
        "module.route.select" => {
            properties["route_alias"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["module_id"] = json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9._:-]+$"});
            properties["artifact_id"] = json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9._-]+$"});
            properties["version"] = json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9.+_-]+$"});
            properties["expected_catalog_revision"] =
                json!({"type":"integer","minimum":0,"maximum":9223372036854775807_i64});
            properties["client_request_id"]["minLength"] = json!(1);
            properties["client_request_id"]["maxLength"] = json!(128);
            properties["client_request_id"]["pattern"] = json!("^\\S+$");
        }
        "schedule.run_now" => {
            properties["client_request_id"]["minLength"] = json!(1);
            properties["client_request_id"]["maxLength"] = json!(128);
            properties["client_request_id"]["pattern"] = json!("^\\S+$");
        }
        "event.emit" => {
            properties["project_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["name"] = json!({
                "type":"string",
                "minLength":1,
                "maxLength":256,
                "pattern":"^[A-Za-z0-9._:/@-]+$"
            });
            properties["payload"] = json!({
                "not":{"type":"null"},
                "description":"Bounded JSON event payload. It is retained only in the owner-visible observation ledger."
            });
            properties["dedupe_key"] =
                json!({"type":"string","minLength":1,"maxLength":256,"pattern":"^\\S+$"});
            properties["cause"] = json!({
                "type":["object","null"],
                "properties":{
                    "operation_id":{"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"},
                    "observation_id":{"type":"integer","minimum":1,"maximum":9223372036854775807_i64}
                },
                "minProperties":1,
                "maxProperties":2,
                "additionalProperties":false
            });
        }
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
                    "exact_deadline_reached",
                    "submission_reviewed"
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
        "bus.events.page" => {
            for field in ["project_id", "consumer_id"] {
                properties[field] =
                    json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            }
            properties["after_observation_id"] =
                json!({"type":"integer","minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":32});
        }
        "bus.consumer.admit" => {
            properties["client_request_id"]["minLength"] = json!(1);
            properties["client_request_id"]["maxLength"] = json!(128);
            properties["client_request_id"]["pattern"] = json!("^\\S+$");
            for field in ["project_id", "consumer_id"] {
                properties[field] =
                    json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            }
            for field in ["automation_revision", "through_observation_id"] {
                properties[field] =
                    json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            }
            properties["expected_cursor"] =
                json!({"type":"integer","minimum":0,"maximum":9223372036854775807_i64});
            properties["occurrences"] = json!({
                "type":"array","maxItems":32,"items":{"type":"object","properties":{
                    "observation_id":{"type":"integer","minimum":1,"maximum":9223372036854775807_i64},
                    "source_id":{"type":"string","minLength":1,"maxLength":256,"pattern":"^[A-Za-z0-9._:/@-]+$"},
                    "event_kind":{"type":"string","minLength":1,"maxLength":256,"pattern":"^[A-Za-z0-9._:/@-]+$"},
                    "status":{"type":["string","null"],"enum":["applied","completed","failed","incomplete","cancelled","rejected","sent","answered","invalidated","unknown",null]},
                    "action":{"type":"object","properties":{"kind":{"const":"script_run"},"script_id":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[A-Za-z0-9._-]+$"}},"required":["kind","script_id"],"additionalProperties":false}
                },"required":["observation_id","source_id","event_kind","status","action"],"additionalProperties":false}
            });
        }
        "hook.source.get" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["after"] =
                json!({"type":"integer","minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":64});
        }
        "hook.source.revoke" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["expected_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
        }
        "goal.create" | "goal.revise" | "goal.enable" | "goal.disable" | "goal.readback"
        | "goal.get" | "goal.list" => {
            for name in ["project_id", "task_id", "attempt_id", "goal_id"] {
                properties[name] =
                    json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            }
            properties["task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            if method == "goal.create" {
                properties["expected_revision"] = json!({"type":"integer","const":0});
            } else if matches!(method, "goal.revise" | "goal.enable" | "goal.disable") {
                properties["expected_revision"] =
                    json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            }
            if matches!(method, "goal.create" | "goal.revise") {
                properties["objective"] = json!({"type":"string","minLength":1,"maxLength":32768});
                properties["enabled"] = json!({"type":"boolean"});
                properties["completion_evidence"] = json!({
                    "type":"object",
                    "properties":{"kind":{"const":"task_acceptance"}},
                    "required":["kind"],
                    "additionalProperties":false
                });
                properties["reminder"] = json!({
                    "oneOf":[
                        {"type":"null"},
                        {
                            "type":"object",
                            "properties":{
                                "due_at_ms":{"type":"integer","minimum":1,"maximum":9223372036854775807_i64},
                                "cooldown_ms":{"type":"integer","minimum":0,"maximum":7776000000_i64}
                            },
                            "required":["due_at_ms","cooldown_ms"],
                            "additionalProperties":false
                        }
                    ]
                });
            }
            if method == "goal.list" {
                properties["after_goal_id"] =
                    json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
                properties["limit"] = json!({"type":"integer","minimum":1,"maximum":50});
            }
        }
        "script.register" | "script.revise" => {
            properties["bundle"] = script_bundle_request_schema();
            if method == "script.revise" {
                properties["script_id"] = script_id_schema();
                properties["expected_revision"] =
                    json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            }
        }
        "script.validate" | "script.activate" => {
            properties["script_id"] = script_id_schema();
            properties["revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
        }
        "script.run" => {
            properties["script_id"] = script_id_schema();
            properties["expected_script_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["attempt_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["expected_task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["input"] = json!({});
        }
        "script.get" => {
            properties["script_id"] = script_id_schema();
            properties["revision"] =
                json!({"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64});
        }
        "script.list" => {
            properties["after"] = json!({"type":"integer","minimum":0});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":100});
        }
        "github.source.inspect" => {
            properties["host"] =
                json!({"type":"string","minLength":1,"maxLength":253,"pattern":"^\\S+$"});
            properties["owner"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["repo"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
        }
        "github.source.setup" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["project_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["host"] =
                json!({"type":"string","minLength":1,"maxLength":253,"pattern":"^\\S+$"});
            properties["owner"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["repo"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["repository_id"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
        }
        "github.source.get" | "github.source.poll" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
        }
        "github.work_pool.preview" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["after"] = json!({"type":"string","minLength":1,"maxLength":512});
            properties["limit"] = json!({"type":"integer","minimum":1,"maximum":200});
        }
        "github.work_pool.apply" => {
            properties["source_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            properties["task_ids"] = json!({
                "type":"array",
                "minItems":1,
                "maxItems":200,
                "uniqueItems":true,
                "items":{"type":"string","minLength":1,"maxLength":512,"pattern":"^\\S+$"}
            });
        }
        "github.effect.managed_label" => {
            properties["source_id"] = json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9_.-]+$"});
            properties["task_id"] =
                json!({"type":"string","minLength":1,"maxLength":512,"pattern":"^\\S+$"});
            properties["expected_task_revision"] =
                json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            properties["label"] = json!({"type":"string","minLength":10,"maxLength":50,"pattern":"^eliot-[a-z0-9-]+$"});
            properties["present"] = json!({"type":"boolean"});
        }
        "github.pull_request.update_description" => {
            properties["publication_operation_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
            for name in ["pull_request_id", "pull_request_number"] {
                properties[name] =
                    json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64});
            }
            properties["title"] = json!({"type":"string","minLength":1,"maxLength":262144});
            properties["body"] = json!({"type":"string","maxLength":262144});
            properties["base_ref"] = json!({"type":"string","minLength":11,"maxLength":512,"pattern":"^refs/heads/[^\\s]+$"});
        }
        "github.effect.reconcile_managed_label" | "github.pull_request.reconcile_description" => {
            properties["operation_id"] =
                json!({"type":"string","minLength":1,"maxLength":128,"pattern":"^\\S+$"});
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
    refine_coordination_input_schema(method, schema);
}

fn refine_coordination_input_schema(method: &str, schema: &mut Value) {
    let coordination_methods = matches!(
        method,
        "coordination.thread.open"
            | "coordination.thread.get"
            | "coordination.thread.list"
            | "coordination.thread.resolve"
            | "coordination.thread.withdraw"
            | "coordination.thread.supersede"
            | "coordination.message.send"
            | "coordination.contract.propose"
            | "coordination.contract.respond"
            | "coordination.contract.ratify"
            | "coordination.contract.reject"
            | "coordination.contract.get"
            | "coordination.contract.list"
            | "coordination.integration.ack"
            | "coordination.agreement.get"
            | "code.scope.propose"
            | "code.scope.accept"
            | "code.scope.inspect"
            | "code.scope.conflicts"
            | "code.scope.release"
    );
    if !coordination_methods {
        return;
    }
    schema["description"] = json!(format!(
        "Coordination request is limited to {} UTF-8 bytes. The wire parser enforces byte bounds because JSON Schema maxLength counts Unicode code points.",
        coordination_limits::MAX_COORDINATION_REQUEST_BYTES
    ));
    let properties = &mut schema["properties"];
    if !matches!(
        method,
        "coordination.thread.get"
            | "coordination.thread.list"
            | "coordination.contract.get"
            | "coordination.contract.list"
            | "coordination.agreement.get"
            | "code.scope.inspect"
            | "code.scope.conflicts"
    ) {
        properties["client_request_id"] = json!({
            "type":"string",
            "minLength":1,
            "maxLength":coordination_limits::MAX_CLIENT_REQUEST_ID_BYTES,
            "pattern":"^\\S+$",
            "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
        });
    }
    match method {
        "coordination.thread.open" => {
            for field in ["task_id", "attempt_id"] {
                properties[field] = coordination_id_schema();
            }
            properties["assignment_id"] = coordination_optional_id_schema();
            properties["topic_kind"] = coordination_limited_id_schema(128);
            properties["subject"] =
                coordination_text_schema(coordination_limits::MAX_SUBJECT_BYTES, 1);
            properties["participants"] = json!({
                "type":"array",
                "minItems":1,
                "description":"Explicit registered Manager/Participant identities and exact optional generations; the total request byte bound supplies the collection bound.",
                "items":{
                    "type":"object",
                    "properties":{
                        "client_id":coordination_client_id_schema(),
                        "generation":{"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64},
                        "reason":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1)
                    },
                    "required":["client_id","reason"],
                    "additionalProperties":false
                }
            });
            properties["reasonability"] = json!({
                "type":"object",
                "properties":{
                    "blocking_fact":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "decision_needed":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "why_coordination_is_needed":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "expected_output":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "close_condition":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1)
                },
                "required":["blocking_fact","decision_needed","why_coordination_is_needed","expected_output","close_condition"],
                "additionalProperties":false
            });
            properties["related_scopes"] = json!({
                "type":"array",
                "maxItems":64,
                "uniqueItems":true,
                "items":{
                    "type":"object",
                    "properties":{
                        "kind":coordination_limited_id_schema(64),
                        "value":coordination_text_schema(coordination_limits::MAX_REFERENCE_BYTES,1)
                    },
                    "required":["kind","value"],
                    "additionalProperties":false
                }
            });
            properties["body_ref"] = coordination_optional_reference_schema();
            properties["delivery_mode"] = json!({"const":"mailbox_only"});
            properties["supersedes_thread_id"] = coordination_optional_id_schema();
        }
        "coordination.thread.get" => {
            properties["thread_id"] = coordination_id_schema();
            properties["after_message_seq"] =
                json!({"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = coordination_optional_page_limit_schema();
        }
        "coordination.thread.list" => {
            properties["task_id"] = coordination_id_schema();
            properties["attempt_id"] = coordination_optional_id_schema();
            properties["state"] = json!({"type":["string","null"],"enum":["open","resolved","unresolved","withdrawn","superseded",null]});
            properties["topic_kind"] = json!({"type":["string","null"],"minLength":1,"maxLength":128,"pattern":"^\\S+$","description":"The Store enforces the UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."});
            properties["limit"] = coordination_optional_page_limit_schema();
            properties["after_thread_id"] = coordination_optional_id_schema();
        }
        "coordination.message.send" => {
            properties["thread_id"] = coordination_id_schema();
            properties["recipient"] = coordination_client_id_schema();
            properties["speech_act"] = json!({"type":"string","enum":["inform","query","answer","propose","counterproposal","object","support","withdraw","not_understood","resolution_summary"]});
            properties["subject"] =
                coordination_text_schema(coordination_limits::MAX_SUBJECT_BYTES, 1);
            properties["summary"] =
                coordination_text_schema(coordination_limits::MAX_SUMMARY_BYTES, 1);
            properties["inline_body"] = json!({
                "type":["string","null"],
                "maxLength":coordination_limits::MAX_INLINE_BODY_BYTES,
                "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points. Inline text and body_ref cannot both be non-null."
            });
            properties["body_ref"] = coordination_optional_reference_schema();
            properties["reply_to_message_id"] = coordination_optional_id_schema();
            properties["in_reply_to_digest"] = json!({"type":["string","null"],"minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"});
            properties["requires_reply"] = json!({"type":"boolean"});
            properties["reply_deadline_ms"] =
                json!({"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64});
            properties["evidence_refs"] = coordination_evidence_refs_schema();
            properties["proposal_revision_id"] = coordination_optional_id_schema();
            properties["delivery_mode"] = json!({"const":"mailbox_only"});
            append_all_of(
                schema,
                json!({"not":{"required":["inline_body","body_ref"],"properties":{"inline_body":{"type":"string"},"body_ref":{"type":"string"}}}}),
            );
        }
        "coordination.thread.resolve" => {
            properties["thread_id"] = coordination_id_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["outcome"] =
                json!({"type":"string","enum":["resolved","unresolved","withdrawn"]});
            properties["resolution_summary"] =
                coordination_text_schema(coordination_limits::MAX_SUMMARY_BYTES, 1);
            properties["selected_proposal_revision_id"] = coordination_optional_id_schema();
            properties["remaining_objections"] = json!({"type":"array","maxItems":64,"uniqueItems":true,"items":coordination_text_schema(coordination_limits::MAX_REFERENCE_BYTES,1)});
            properties["follow_up_operation_ids"] = json!({"type":"array","maxItems":64,"uniqueItems":true,"items":coordination_text_schema(coordination_limits::MAX_REFERENCE_BYTES,1)});
            properties["manager_ratification_operation_id"] = coordination_optional_id_schema();
        }
        "coordination.thread.withdraw" => {
            properties["thread_id"] = coordination_id_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
        }
        "coordination.thread.supersede" => {
            properties["thread_id"] = coordination_id_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["superseding_thread_id"] = coordination_id_schema();
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
        }
        "coordination.contract.propose" => {
            properties["thread_id"] = coordination_prefixed_uuid_schema("coord-");
            properties["supersedes_revision_id"] =
                coordination_optional_prefixed_uuid_schema("cprev-");
            properties["topic"] =
                coordination_text_schema(coordination_limits::MAX_SUBJECT_BYTES, 1);
            properties["affected"] = json!({
                "type":"object",
                "properties":{
                    "paths":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES),
                    "symbols":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES),
                    "schemas":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES)
                },
                "required":["paths","symbols","schemas"],
                "additionalProperties":false,
                "anyOf":[
                    {"properties":{"paths":{"minItems":1}}},
                    {"properties":{"symbols":{"minItems":1}}},
                    {"properties":{"schemas":{"minItems":1}}}
                ],
                "description":"Each set is unique and bounded; the Store limits 64 total entries across all three arrays."
            });
            properties["statement"] = json!({
                "type":"object",
                "properties":{
                    "producer":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "consumer":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "identity":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "payload":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "observation_boundary":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "failure_semantics":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1),
                    "versioning":coordination_text_schema(coordination_limits::MAX_REASON_BYTES,1)
                },
                "required":["producer","consumer","identity","payload","observation_boundary","failure_semantics","versioning"],
                "additionalProperties":false
            });
            properties["acceptance_conditions"] = json!({"type":"array","maxItems":64,"uniqueItems":true,"items":coordination_text_schema(coordination_limits::MAX_SUMMARY_BYTES,1)});
            properties["claims"] = json!({"type":"array","items":{},"description":"Bounded arbitrary JSON claim values; the server assigns no invented claim semantics."});
            properties["open_questions"] = json!({"type":"array","maxItems":64,"uniqueItems":true,"items":coordination_text_schema(coordination_limits::MAX_SUMMARY_BYTES,1)});
        }
        "coordination.contract.respond" => {
            properties["thread_id"] = coordination_prefixed_uuid_schema("coord-");
            properties["proposal_id"] = coordination_prefixed_uuid_schema("cprop-");
            properties["proposal_revision_id"] = coordination_prefixed_uuid_schema("cprev-");
            properties["proposal_digest"] = json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[A-Fa-f0-9]{64}$"});
            properties["act"] =
                json!({"type":"string","enum":["counterproposal","object","support","withdraw"]});
            properties["objection_basis"] = json!({"type":["string","null"],"maxLength":128,"description":"Raw bounded basis is retained; empty or unrecognized basis is classified as no material progress."});
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
            properties["evidence_refs"] = coordination_evidence_refs_schema();
        }
        "coordination.contract.ratify" | "coordination.contract.reject" => {
            properties["thread_id"] = coordination_prefixed_uuid_schema("coord-");
            properties["proposal_id"] = coordination_prefixed_uuid_schema("cprop-");
            properties["proposal_revision_id"] = coordination_prefixed_uuid_schema("cprev-");
            properties["proposal_digest"] = json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[A-Fa-f0-9]{64}$"});
            for field in ["expected_state_revision", "task_revision"] {
                properties[field] = coordination_positive_integer_schema();
            }
            for field in ["task_id", "attempt_id"] {
                properties[field] = coordination_id_schema();
            }
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
            for field in ["conditions", "caveats"] {
                properties[field] = json!({"type":"array","maxItems":64,"uniqueItems":true,"items":coordination_text_schema(coordination_limits::MAX_SUMMARY_BYTES,1)});
            }
            properties["affected_scope_revisions"] = contract_decision_scope_schema();
        }
        "coordination.contract.get" => {
            properties["thread_id"] = coordination_prefixed_uuid_schema("coord-");
            properties["proposal_id"] = coordination_prefixed_uuid_schema("cprop-");
            properties["proposal_revision_id"] = coordination_prefixed_uuid_schema("cprev-");
            properties["after_observation_id"] =
                json!({"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = coordination_optional_page_limit_schema();
        }
        "coordination.contract.list" => {
            properties["thread_id"] = coordination_prefixed_uuid_schema("coord-");
            properties["after_sequence"] =
                json!({"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64});
            properties["limit"] = coordination_optional_page_limit_schema();
        }
        "coordination.integration.ack" => {
            properties["cell_id"] = coordination_sha256_hex_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["expected_material_digest"] = coordination_sha256_hex_schema();
            properties["expected_membership_digest"] = coordination_sha256_hex_schema();
            properties["task_id"] = coordination_id_schema();
            properties["task_revision"] = coordination_positive_integer_schema();
            properties["attempt_id"] = coordination_id_schema();
            properties["decision"] = json!({"type":"string","enum":["accept","dissent"]});
        }
        "coordination.agreement.get" => {
            for field in ["task_id", "attempt_id"] {
                properties[field] = coordination_id_schema();
            }
            properties["cell_id"] = coordination_sha256_hex_schema();
            properties["task_revision"] = coordination_positive_integer_schema();
            properties["state_revision"] =
                json!({"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64});
            properties["material_digest"] =
                json!({"oneOf":[coordination_sha256_hex_schema(),{"type":"null"}]});
            properties["limit"] = coordination_optional_page_limit_schema();
            properties["after_position_id"] = json!({"type":["string","null"],"pattern":"^p[0-9]{20}$","minLength":21,"maxLength":21});
            append_all_of(
                schema,
                json!({"oneOf":[
                    {"not":{"anyOf":[{"required":["state_revision"]},{"required":["material_digest"]}]}},
                    {"required":["state_revision","material_digest"]}
                ]}),
            );
        }
        "code.scope.propose" => {
            properties["task_id"] = coordination_id_schema();
            properties["task_revision"] =
                json!({"type":["integer","null"],"minimum":1,"maximum":9223372036854775807_i64});
            properties["attempt_id"] = coordination_id_schema();
            properties["assignment_id"] = coordination_optional_id_schema();
            properties["mode"] = code_scope_mode_schema();
            for field in ["paths", "symbols", "interfaces"] {
                properties[field] =
                    coordination_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES);
            }
            properties["baseline_candidate_ref"] =
                coordination_text_schema(coordination_limits::MAX_REFERENCE_BYTES, 1);
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
            properties["suggested_expires_at_ms"] =
                json!({"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64});
            append_all_of(
                schema,
                json!({"anyOf":[
                    {"required":["paths"],"properties":{"paths":{"minItems":1}}},
                    {"required":["symbols"],"properties":{"symbols":{"minItems":1}}},
                    {"required":["interfaces"],"properties":{"interfaces":{"minItems":1}}}
                ]}),
            );
        }
        "code.scope.accept" => {
            properties["scope_intent_id"] = code_scope_id_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["proposal_digest"] =
                json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[0-9a-f]{64}$"});
            properties["mode"] = code_scope_mode_schema();
            for field in ["paths", "symbols", "interfaces"] {
                properties[field] =
                    coordination_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES);
            }
            properties["expires_at_ms"] =
                json!({"type":["integer","null"],"minimum":0,"maximum":9223372036854775807_i64});
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
            properties["acknowledge_broad_scope"] = json!({"type":"boolean"});
            properties["override_scope_intent_ids"] =
                json!({"type":"array","items":code_scope_id_schema()});
        }
        "code.scope.inspect" | "code.scope.conflicts" => {
            properties["task_id"] = coordination_id_schema();
            properties["task_revision"] = json!({"type":["integer","null"],"minimum":1});
            properties["attempt_id"] = coordination_optional_id_schema();
            properties["scope_intent_id"] = code_scope_optional_id_schema();
            properties["client_id"] = json!({"type":["string","null"],"minLength":1,"maxLength":coordination_limits::MAX_CLIENT_ID_BYTES});
            for field in ["path", "symbol", "interface"] {
                properties[field] = json!({"type":["string","null"],"minLength":1,"maxLength":coordination_limits::MAX_REFERENCE_BYTES});
            }
            properties["after_scope_id"] = code_scope_optional_id_schema();
            properties["limit"] = coordination_page_limit_schema();
        }
        "code.scope.release" => {
            properties["scope_intent_id"] = code_scope_id_schema();
            properties["expected_state_revision"] = coordination_positive_integer_schema();
            properties["reason"] =
                coordination_text_schema(coordination_limits::MAX_REASON_BYTES, 1);
        }
        _ => {}
    }
}

fn coordination_text_schema(max_bytes: usize, min_length: usize) -> Value {
    let mut schema = json!({
        "type":"string",
        "minLength":min_length,
        "maxLength":max_bytes,
        "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
    });
    if min_length > 0 {
        schema["pattern"] = json!("\\S");
    }
    schema
}

fn coordination_limited_id_schema(max_bytes: usize) -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":max_bytes,
        "pattern":"^\\S+$",
        "description":"The Store enforces this UTF-8 byte limit; identifiers cannot contain whitespace. JSON Schema maxLength counts Unicode code points."
    })
}

fn coordination_prefixed_uuid_schema(prefix: &str) -> Value {
    let pattern = format!(
        "^{prefix}[0-9A-Fa-f]{{8}}-[0-9A-Fa-f]{{4}}-[0-9A-Fa-f]{{4}}-[0-9A-Fa-f]{{4}}-[0-9A-Fa-f]{{12}}$"
    );
    let length = prefix.len() + 36;
    json!({"type":"string","minLength":length,"maxLength":length,"pattern":pattern})
}

fn coordination_optional_prefixed_uuid_schema(prefix: &str) -> Value {
    json!({
        "oneOf":[coordination_prefixed_uuid_schema(prefix),{"type":"null"}]
    })
}

fn contract_decision_scope_schema() -> Value {
    json!({"type":"array","maxItems":50,"items":{
        "type":"object","additionalProperties":false,
        "required":["scope_intent_id","state_revision","digest","state","owner_client_id",
            "actor","assignment_id","participation_basis","mode","paths","symbols","interfaces",
            "expires_at_ms","override_scope_intent_ids"],
        "properties":{
            "scope_intent_id":code_scope_id_schema(),
            "state_revision":coordination_positive_integer_schema(),
            "digest":coordination_sha256_hex_schema(),
            "state":coordination_id_schema(),
            "owner_client_id":coordination_id_schema(),
            "actor":{"type":"object"},
            "assignment_id":coordination_optional_id_schema(),
            "participation_basis":{"type":["object","null"]},
            "mode":code_scope_mode_schema(),
            "paths":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES),
            "symbols":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES),
            "interfaces":coordination_unique_string_array_schema(coordination_limits::MAX_REFERENCE_BYTES),
            "expires_at_ms":{"type":["integer","null"],"minimum":0},
            "override_scope_intent_ids":coordination_unique_string_array_schema(coordination_limits::MAX_IDENTIFIER_BYTES)
        }
    },"description":"Exact relevant current scope references from the Store, sorted uniquely by scope_intent_id; the whole canonical decision request is limited to 64 KiB."})
}

fn coordination_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":coordination_limits::MAX_IDENTIFIER_BYTES,
        "pattern":"^\\S+$",
        "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
    })
}

fn coordination_optional_id_schema() -> Value {
    json!({
        "type":["string","null"],
        "minLength":1,
        "maxLength":coordination_limits::MAX_IDENTIFIER_BYTES,
        "pattern":"^\\S+$",
        "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
    })
}

fn code_scope_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":43,
        "maxLength":43,
        "pattern":"^cscope-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    })
}

fn code_scope_optional_id_schema() -> Value {
    json!({
        "type":["string","null"],
        "minLength":43,
        "maxLength":43,
        "pattern":"^cscope-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    })
}

fn coordination_client_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":coordination_limits::MAX_CLIENT_ID_BYTES,
        "pattern":"^\\S+$",
        "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
    })
}

fn coordination_optional_reference_schema() -> Value {
    json!({
        "type":["string","null"],
        "minLength":1,
        "maxLength":coordination_limits::MAX_REFERENCE_BYTES,
        "pattern":"^\\S+$",
        "description":"UTF-8 byte limit is enforced by the wire parser; JSON Schema maxLength counts Unicode code points."
    })
}

fn coordination_positive_integer_schema() -> Value {
    json!({"type":"integer","minimum":1,"maximum":9223372036854775807_i64})
}

fn coordination_page_limit_schema() -> Value {
    json!({
        "type":"integer",
        "minimum":1,
        "maximum":coordination_limits::MAX_READ_PAGE_SIZE,
        "default":coordination_limits::DEFAULT_READ_PAGE_SIZE
    })
}

fn coordination_optional_page_limit_schema() -> Value {
    json!({
        "type":["integer","null"],
        "minimum":1,
        "maximum":coordination_limits::MAX_READ_PAGE_SIZE,
        "default":coordination_limits::DEFAULT_READ_PAGE_SIZE
    })
}

fn coordination_sha256_hex_schema() -> Value {
    json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[0-9a-f]{64}$"})
}

fn coordination_string_array_schema(max_bytes: usize) -> Value {
    json!({
        "type":"array",
        "items":coordination_text_schema(max_bytes,1),
        "description":"The shared request byte cap bounds collection size; each UTF-8 string byte cap is enforced by the wire parser."
    })
}

fn coordination_unique_string_array_schema(max_bytes: usize) -> Value {
    let mut schema = coordination_string_array_schema(max_bytes);
    schema["uniqueItems"] = json!(true);
    schema["maxItems"] = json!(64);
    schema
}

fn coordination_evidence_refs_schema() -> Value {
    json!({
        "type":"array",
        "maxItems":coordination_limits::MAX_EVIDENCE_REFS,
        "uniqueItems":true,
        "items":coordination_text_schema(coordination_limits::MAX_REFERENCE_BYTES,1)
    })
}

fn code_scope_mode_schema() -> Value {
    json!({"type":"string","enum":["exclusive_edit","shared_edit","read_review"]})
}

fn automation_config_changes_schema() -> Value {
    json!({
        "type":"array",
        "minItems":1,
        "maxItems":32,
        "description":"One to 32 revision-checked entry changes. Store validates duplicate automation IDs and cross-field patch semantics.",
        "items":{
            "type":"object",
            "properties":{
                "automation_id":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[A-Za-z0-9._-]+$"},
                "expected_revision":{"type":"integer","minimum":0,"maximum":9223372036854775807_i64},
                "include_existing":{"type":"boolean","description":"Optional; defaults to false for the activation cut."},
                "patch":automation_config_patch_schema()
            },
            "required":["automation_id","expected_revision","patch"],
            "additionalProperties":false
        }
    })
}

fn automation_config_patch_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "enabled":{"type":"boolean"},
            "steps":{
                "type":"array",
                "maxItems":16,
                "uniqueItems":true,
                "items":{"type":"string","enum":[
                    "work_dispatch","review_dispatch","review_disposition","repair_dispatch",
                    "acceptance","publication","check_run","github_projection",
                    "goal_progression","script_run"
                ]}
            },
            "script_run":{
                "oneOf":[
                    {"type":"null"},
                    {
                        "type":"object",
                        "properties":{"script_id":script_id_schema()},
                        "required":["script_id"],
                        "additionalProperties":false
                    }
                ]
            },
            "github_projection":{
                "oneOf":[
                    {"type":"null"},
                    {
                        "type":"object",
                        "properties":{
                            "source_id":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9_.-]+$","description":"The Store enforces this UTF-8 byte limit; JSON Schema maxLength counts Unicode code points."},
                            "label":{"type":"string","minLength":10,"maxLength":50,"pattern":"^eliot-[a-z0-9-]*[a-z0-9]$","description":"The Store enforces a 10..=50 UTF-8 byte lowercase eliot-* label without a trailing hyphen; JSON Schema maxLength counts Unicode code points."},
                            "present":{"type":"boolean"}
                        },
                        "additionalProperties":false,
                        "description":"Optional partial merge patch: omitted fields retain their stored values; null clears the full setting. A new setting must provide source_id, label, and present together after merge."
                    }
                ]
            },
            "event_rules":automation_event_rules_schema()
        },
        "additionalProperties":true,
        "description":"The O8 enabled, steps, script_run, github_projection and event_rules fields are typed here. Other existing patch settings remain accepted; Store enforces their complete allowlist and shapes."
    })
}

fn automation_event_rules_schema() -> Value {
    json!({
        "oneOf":[
            {"type":"null"},
            {
                "type":"array",
                "maxItems":16,
                "description":"Omitted leaves this setting unchanged; null restores the stored absent/legacy form; an empty array disables automatic event routes. Each action must also be selected in the entry's steps, including steps retained by a partial patch; Store rejects duplicate rules.",
                "items":automation_event_rule_schema()
            }
        ]
    })
}

fn automation_event_rule_schema() -> Value {
    json!({
        "oneOf":[
            {
                "type":"object",
                "properties":{
                    "source":{"const":"task.submission"},
                    "predicate":{"const":"applied"},
                    "source_id":{"type":"null"},
                    "event_kind":{"type":"null"},
                    "status":{"type":"null"},
                    "action":{"type":"string","enum":["review_dispatch","script_run"]}
                },
                "required":["source","predicate","action"],
                "additionalProperties":false
            },
            {
                "type":"object",
                "properties":{
                    "source":{"type":"null"},
                    "predicate":{"type":"null"},
                    "source_id":automation_selector_name_schema(),
                    "event_kind":automation_selector_name_schema(),
                    "status":automation_event_status_schema(),
                    "action":{"const":"script_run"}
                },
                "required":["source_id","event_kind","action"],
                "additionalProperties":false
            },
            {
                "type":"object",
                "properties":{
                    "source":{"type":"null"},
                    "predicate":{"type":"null"},
                    "source_id":{"const":"controller"},
                    "event_kind":{"const":"task.submission"},
                    "status":{"const":"applied"},
                    "action":{"const":"review_dispatch"}
                },
                "required":["source_id","event_kind","status","action"],
                "additionalProperties":false
            }
        ]
    })
}

fn automation_selector_name_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":256,
        "pattern":"^[A-Za-z0-9._:/@-]+$",
        "description":"The Store parser enforces the 256-byte ASCII selector bound. Unknown but syntactically valid selectors may be saved and remain idle until a safe event source is available."
    })
}

fn automation_event_status_schema() -> Value {
    json!({
        "type":["string","null"],
        "enum":["applied","completed","failed","incomplete","cancelled","rejected","sent","answered","invalidated","unknown",null]
    })
}

fn script_bundle_request_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "script_id":script_id_schema(),
            "interpreter_kind":{"type":"string","enum":["python","powershell"]},
            "interpreter_path":{"type":"string","minLength":1},
            "entrypoint":{"type":"string","minLength":1,"maxLength":240},
            "argv":{"type":"array","maxItems":32,"items":{"type":"string","maxLength":4096}},
            "trust":{"const":"trusted_local"},
            "inherit_environment":{"type":"array","maxItems":32,"items":{"type":"string","minLength":1,"maxLength":256}},
            "controller_effects":{"type":"array","maxItems":1,"uniqueItems":true,"items":{"enum":["task_owner_message","manager_notification","task_create"]}},
            "input_schema":{"$ref":"#/$defs/ScriptValueSchema"},
            "result_schema":{"$ref":"#/$defs/ScriptValueSchema"},
            "files":{
                "type":"array","minItems":1,"maxItems":64,
                "items":{
                    "type":"object",
                    "properties":{
                        "path":{"type":"string","minLength":1,"maxLength":240},
                        "content_base64":{"type":"string","maxLength":349528}
                    },
                    "required":["path","content_base64"],
                    "additionalProperties":false
                }
            }
        },
        "required":["script_id","interpreter_kind","interpreter_path","entrypoint","trust","input_schema","result_schema","files"],
        "additionalProperties":false
    })
}

fn script_value_schema_definition() -> Value {
    json!({
        "oneOf":[
            {"type":"object","properties":{"type":{"const":"null"}},"required":["type"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"boolean"}},"required":["type"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"integer"}},"required":["type"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"number"}},"required":["type"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"string"},"max_bytes":{"type":"integer","minimum":0,"maximum":262144}},"required":["type","max_bytes"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"array"},"items":{"$ref":"#/$defs/ScriptValueSchema"},"max_items":{"type":"integer","minimum":0,"maximum":4096}},"required":["type","items","max_items"],"additionalProperties":false},
            {"type":"object","properties":{"type":{"const":"object"},"properties":{"type":"object","maxProperties":64,"additionalProperties":{"$ref":"#/$defs/ScriptValueSchema"}},"required":{"type":"array","maxItems":64,"uniqueItems":true,"items":{"type":"string","minLength":1,"maxLength":128}},"additional_properties":{"const":false}},"required":["type"],"additionalProperties":false}
        ]
    })
}

fn script_id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":64,
        "pattern":"^[a-z0-9_-]+$"
    })
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
    let submission_reviewed = json!({
        "type":"object",
        "properties":{
            "submission_ref":{"type":"string","minLength":1,"maxLength":128},
            "candidate_ref":{"type":"string","minLength":1,"maxLength":128}
        },
        "required":["submission_ref","candidate_ref"],
        "additionalProperties":false
    });
    json!({
        "oneOf":[
            {"properties":{"watch_kind":{"const":"operation_terminal"},"address":operation_terminal}},
            {"properties":{"watch_kind":{"const":"contract_revision_changed"},"address":contract_revision_changed}},
            {"properties":{"watch_kind":{"const":"task_revision_changed"},"address":task_revision_changed}},
            {"properties":{"watch_kind":{"const":"attempt_disposition_changed"},"address":attempt_disposition_changed}},
            {"properties":{"watch_kind":{"const":"exact_deadline_reached"},"address":exact_deadline_reached}},
            {"properties":{"watch_kind":{"const":"submission_reviewed"},"address":submission_reviewed}}
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

pub fn output_schema(method: &str) -> Option<Arc<JsonObject>> {
    let (_, spec) = find_tool_spec(method)?;
    spec.output_schema_cache
        .get_or_init(|| build_output_schema(method))
        .clone()
}

pub fn output_schema_bytes(method: &str) -> std::result::Result<Option<Arc<[u8]>>, String> {
    let Some((_, spec)) = find_tool_spec(method) else {
        return Ok(None);
    };
    spec.output_schema_bytes_cache
        .get_or_init(|| match output_schema(method) {
            Some(schema) => serde_json::to_vec(schema.as_ref())
                .map(|bytes| Some(Arc::<[u8]>::from(bytes)))
                .map_err(|error| error.to_string()),
            None => Ok(None),
        })
        .clone()
}

fn build_output_schema(method: &str) -> Option<Arc<JsonObject>> {
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

/// Frontend exposure filters. A new method is not exposed through a restricted profile
/// until it is present in the shared positive policy and allowed by that profile.
/// The host independently authorizes every forwarded request from the authenticated credential.
pub fn exposes_method(profile: McpToolProfile, method: &str) -> bool {
    if !method_policy::is_mcp_method(method) {
        return false;
    }
    if profile == McpToolProfile::Full {
        return true;
    }

    let observer_read = matches!(
        method,
        "swarm.tools.search"
            | "host.status"
            | "swarm.dashboard"
            | "task.get"
            | "task.list"
            | "task.submission"
            | "task.acceptance"
            | "attempt.get"
            | "operation.get"
            | "operation.list"
            | "concilium.get"
            | "concilium.list"
            | "agent.state"
            | "agent.list"
            | "agent.family"
            | "check.get"
            | "check.profiles"
            | "artifact.get"
            | "artifact.read"
            | "artifact.parts"
            | "report.delta"
            | "report.attention"
            | "report.capacity"
            | "message.read"
    );
    if observer_read
        && matches!(
            profile,
            McpToolProfile::Observer
                | McpToolProfile::Reviewer
                | McpToolProfile::Manager
                | McpToolProfile::Gm
        )
    {
        return true;
    }

    if method == "agent.usage" && matches!(profile, McpToolProfile::Manager | McpToolProfile::Gm) {
        return true;
    }

    if method == "module.catalog.get"
        && matches!(profile, McpToolProfile::Manager | McpToolProfile::Gm)
    {
        return true;
    }

    match profile {
        McpToolProfile::Observer => false,
        McpToolProfile::Reviewer => method == "task.request_changes",
        McpToolProfile::Participant => {
            method == "swarm.tools.search"
                || matches!(
                    method,
                    "concilium.propose"
                        | "concilium.position.submit"
                        | "concilium.get"
                        | "concilium.list"
                )
                || method_policy::participant_allowed(method)
        }
        McpToolProfile::AssignedReviewer => matches!(
            method,
            "swarm.tools.search"
                | "swarm.review.context"
                | "review.get"
                | "review.list"
                | "review.submit"
                | "task.submission"
                | "check.get"
                | "artifact.read"
                | "operation.get"
                | "concilium.get"
                | "concilium.list"
        ),
        McpToolProfile::Manager => matches!(
            method,
            "module.route.select"
                | "task.request_changes"
                | "task.create"
                | "task.revise"
                | "task.claim"
                | "task.dispatch"
                | "task.submit"
                | "task.submit.recover"
                | "attempt.release"
                | "attempt.bind_producer"
                | "operation.cancel"
                | "logging.get"
                | "logging.set"
                | "agent.open"
                | "agent.send"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.background"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.recover"
                | "agent.result"
                | "message.send"
                | "message.cancel"
                | "coordination.participant.register"
                | "coordination.participant.disable"
                | "coordination.participant.get"
                | "coordination.participant.list"
                | "coordination.thread.open"
                | "coordination.thread.get"
                | "coordination.thread.list"
                | "coordination.message.send"
                | "coordination.thread.resolve"
                | "coordination.thread.withdraw"
                | "coordination.thread.supersede"
                | "coordination.contract.propose"
                | "coordination.contract.respond"
                | "coordination.contract.ratify"
                | "coordination.contract.reject"
                | "coordination.contract.get"
                | "coordination.contract.list"
                | "coordination.agreement.get"
                | "code.scope.propose"
                | "code.scope.accept"
                | "code.scope.inspect"
                | "code.scope.conflicts"
                | "code.scope.release"
                | "swarm.context.get"
                | "coordination.peer.find"
                | "coordination.work_card.get"
                | "coordination.work_card.list"
                | "coordination.contract_card.get"
                | "coordination.contract_card.list"
                | "review.assign"
                | "review.get"
                | "review.list"
                | "swarm.review.context"
                | "automation.config.get"
                | "automation.config.preview"
                | "automation.config.apply"
                | "automation.config.transfer"
                | "automation.config.explain"
                | "bus.events.page"
                | "bus.consumer.admit"
                | "schedule.run_now"
                | "hook.source.get"
                | "hook.source.revoke"
                | "goal.create"
                | "goal.revise"
                | "goal.enable"
                | "goal.disable"
                | "goal.readback"
                | "goal.get"
                | "goal.list"
                | "script.register"
                | "script.revise"
                | "script.validate"
                | "script.activate"
                | "script.run"
                | "script.get"
                | "script.list"
                | "monitor.snapshot"
                | "monitor.follow"
                | "swarm.queue.get"
                | "swarm.agent.inspect"
                | "swarm.exceptions.get"
                | "swarm.launch.preview"
                | "swarm.launch"
                | "swarm.overlap.check"
                | "github.effect.managed_label"
                | "github.pull_request.update_description"
                | "github.pull_request.reconcile_description"
                | "coordination.watch.create"
                | "coordination.watch.list"
                | "coordination.watch.cancel"
                | "concilium.propose"
                | "concilium.preview"
                | "concilium.open"
                | "concilium.round.advance"
                | "concilium.get"
                | "concilium.list"
                | "concilium.close"
        ),
        McpToolProfile::Gm => {
            (exposes_method(McpToolProfile::Manager, method)
                && !matches!(
                    method,
                    "automation.config.preview" | "automation.config.apply"
                ))
                || matches!(
                    method,
                    "client.list"
                        | "client.register"
                        | "host.mode"
                        | "task.accept"
                        | "task.invalidate_acceptance"
                        | "forge.publish_ref"
                        | "github.source.inspect"
                        | "github.source.get"
                        | "github.work_pool.preview"
                        | "github.work_pool.apply"
                        | "github.effect.managed_label"
                        | "github.effect.reconcile_managed_label"
                        | "github.pull_request.update_description"
                        | "github.pull_request.reconcile_description"
                        | "gm.handover"
                )
        }
        McpToolProfile::Full => true,
    }
}

pub const MAX_PAGE_ITEMS: usize = 9;
pub const MAX_PAGE_JSON_BYTES: usize = 64 * 1024;
pub const MAX_SEARCH_RESULTS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolGroup {
    AcceptanceEffects,
    Administration,
    AssignmentRead,
    Core,
    Goals,
    GitHub,
    Hooks,
    MailboxRaw,
    ManagerCore,
    Monitoring,
    Review,
    RuntimeControl,
    RuntimeRecovery,
    Schedules,
    Scripts,
    TaskManagement,
    GitRead,
    ParticipantCoordination,
}

impl ToolGroup {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AcceptanceEffects => "acceptance-effects",
            Self::Administration => "administration",
            Self::AssignmentRead => "assignment-read",
            Self::Core => "core",
            Self::Goals => "goals",
            Self::GitHub => "github",
            Self::Hooks => "hooks",
            Self::MailboxRaw => "mailbox-raw",
            Self::ManagerCore => "manager-core",
            Self::Monitoring => "monitoring",
            Self::Review => "review",
            Self::RuntimeControl => "runtime-control",
            Self::RuntimeRecovery => "runtime-recovery",
            Self::Schedules => "schedules",
            Self::Scripts => "scripts",
            Self::TaskManagement => "task-management",
            Self::GitRead => "git-read",
            Self::ParticipantCoordination => "participant-coordination",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "acceptance-effects" => Self::AcceptanceEffects,
            "administration" => Self::Administration,
            "assignment-read" => Self::AssignmentRead,
            "core" => Self::Core,
            "goals" => Self::Goals,
            "github" => Self::GitHub,
            "hooks" => Self::Hooks,
            "mailbox-raw" => Self::MailboxRaw,
            "manager-core" => Self::ManagerCore,
            "monitoring" => Self::Monitoring,
            "review" => Self::Review,
            "runtime-control" => Self::RuntimeControl,
            "runtime-recovery" => Self::RuntimeRecovery,
            "schedules" => Self::Schedules,
            "scripts" => Self::Scripts,
            "task-management" => Self::TaskManagement,
            "git-read" => Self::GitRead,
            "participant-coordination" => Self::ParticipantCoordination,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAudience {
    Observer,
    LegacyReviewer,
    AssignedReviewer,
    Participant,
    Manager,
    GmOperator,
    FullCompatibility,
}

impl ToolAudience {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observer => "observer",
            Self::LegacyReviewer => "legacy-reviewer",
            Self::AssignedReviewer => "assigned-reviewer",
            Self::Participant => "participant",
            Self::Manager => "manager",
            Self::GmOperator => "gm-operator",
            Self::FullCompatibility => "full-compatibility",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadTier {
    Core,
    Searchable,
    ManualOnly,
}

impl LoadTier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Searchable => "searchable",
            Self::ManualOnly => "manual-only",
        }
    }
}

/// Search and presentation information for one implemented application
/// method. `audiences` describes intended fit; the Store checks the selected
/// credential's actual scope after any frontend profile filter.
#[derive(Debug, Clone, Copy)]
pub struct ToolMetadata {
    pub method: &'static str,
    pub group: ToolGroup,
    pub audiences: &'static [ToolAudience],
    pub load_tier: LoadTier,
    pub purpose: &'static str,
    pub when_to_use: &'static str,
    pub search_terms: &'static [&'static str],
    /// Exact request fields required by the executable ToolSpec. Empty means
    /// this legacy metadata row has not yet opted into the checked contract.
    pub required_input_fields: &'static [&'static str],
    /// Semantic prerequisites which are not request-field names.
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
}

const OBSERVER_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const ASSIGNMENT_READ_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const REVIEW_READ_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const PARTICIPANT_CORE_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const ALL_SEARCH_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const REVIEW_DISPOSITION_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::LegacyReviewer,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const MANAGER_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const TASK_SUBMIT_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const COORDINATION_READ_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const COORDINATION_ACTION_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const CONCILIUM_READ_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Observer,
    ToolAudience::LegacyReviewer,
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const CONCILIUM_PROPOSAL_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const CONCILIUM_POSITION_AUDIENCES: &[ToolAudience] =
    &[ToolAudience::Participant, ToolAudience::FullCompatibility];
const WATCH_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const PARTICIPANT_ONLY_AUDIENCES: &[ToolAudience] =
    &[ToolAudience::Participant, ToolAudience::FullCompatibility];
const ASSIGNED_REVIEWER_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::FullCompatibility,
];
const REVIEW_ASSIGNMENT_READ_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::AssignedReviewer,
    ToolAudience::Participant,
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const MANAGER_ONLY_AUDIENCES: &[ToolAudience] =
    &[ToolAudience::Manager, ToolAudience::FullCompatibility];
const GM_AUDIENCES: &[ToolAudience] = &[ToolAudience::GmOperator, ToolAudience::FullCompatibility];
const MANAGER_GM_AUDIENCES: &[ToolAudience] = &[
    ToolAudience::Manager,
    ToolAudience::GmOperator,
    ToolAudience::FullCompatibility,
];
const FULL_AUDIENCE: &[ToolAudience] = &[ToolAudience::FullCompatibility];

macro_rules! entry {
    ($method:literal, $group:ident, $aud:ident, $tier:ident, $purpose:literal, $when:literal, $terms:expr, $context:expr, $result:literal) => {
        ToolMetadata {
            method: $method,
            group: ToolGroup::$group,
            audiences: $aud,
            load_tier: LoadTier::$tier,
            purpose: $purpose,
            when_to_use: $when,
            search_terms: $terms,
            required_input_fields: &[],
            required_context: $context,
            result_policy: $result,
        }
    };
}

/// Opt one metadata row into an exact, machine-checked relationship with the
/// executable ToolSpec. The declared input fields must equal ToolSpec.required;
/// semantic prerequisites stay separate and cannot masquerade as arguments.
macro_rules! entry_with_inputs {
    ($method:literal, $group:ident, $aud:ident, $tier:ident, $purpose:literal, $when:literal, $terms:expr, $inputs:expr, $context:expr, $result:literal) => {
        ToolMetadata {
            method: $method,
            group: ToolGroup::$group,
            audiences: $aud,
            load_tier: LoadTier::$tier,
            purpose: $purpose,
            when_to_use: $when,
            search_terms: $terms,
            required_input_fields: $inputs,
            required_context: $context,
            result_policy: $result,
        }
    };
}

/// One metadata row for each current `TOOLS` entry. Proposed canonical names
/// without handlers are listed as role-core gaps below and never appear here.
pub const TOOL_METADATA: &[ToolMetadata] = &[
    entry!(
        "swarm.tools.search",
        Core,
        ALL_SEARCH_AUDIENCES,
        Core,
        "Find currently authorized MCP methods and the safe way to expose their schemas.",
        "Use when the current surface does not show a method needed for the task; this returns catalog metadata only.",
        &[
            "tool", "search", "catalog", "discover", "find", "load", "surface"
        ],
        &["task goal", "optional purpose"],
        "Bounded metadata matches; never executes a method or proves a harness loaded a schema."
    ),
    entry!(
        "host.status",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Read controller health and admission state.",
        "Use for a bounded host snapshot before diagnosing controller availability.",
        &["status", "health", "host", "admission"],
        &[],
        "One current status snapshot."
    ),
    entry!(
        "route.list",
        Administration,
        FULL_AUDIENCE,
        Searchable,
        "Inspect configured route definitions.",
        "Use when checking which configured routing rules exist.",
        &["routes", "routing", "configuration"],
        &["local route configuration"],
        "Configured routes only; no provider discovery."
    ),
    entry!(
        "module.catalog.get",
        Administration,
        MANAGER_ONLY_AUDIENCES,
        Searchable,
        "Read locally registered module contract descriptors and route selections.",
        "Use before selecting a module for a configured route or checking its exact advertised protocol schemas.",
        &[
            "module",
            "descriptor",
            "catalog",
            "artifact",
            "protocol",
            "route"
        ],
        &["local module catalog"],
        "Bounded metadata only; launch details and protected references are redacted, and no process is probed or started."
    ),
    entry!(
        "client.list",
        Administration,
        GM_AUDIENCES,
        Searchable,
        "Page registered client identities.",
        "Use for operator review of registered identities and bindings.",
        &["client", "identity", "credential", "binding"],
        &["operator scope"],
        "Bounded page of registered clients."
    ),
    entry!(
        "task.get",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Core,
        "Read one task and its current assignment state.",
        "Use when you have an exact task ID and need its current revision or disposition.",
        &["task", "assignment", "revision", "status"],
        &["task_id"],
        "One task projection."
    ),
    entry!(
        "task.list",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Core,
        "Page tasks matching the requested state.",
        "Use to find work in a known task state when the result set is bounded.",
        &["tasks", "queue", "work", "state"],
        &[],
        "Bounded task page with continuation position."
    ),
    entry!(
        "task.submission",
        Review,
        REVIEW_READ_AUDIENCES,
        Core,
        "Read one immutable task submission and its requirement claims.",
        "Use to inspect the submitted evidence before an authorized review action.",
        &["submission", "review", "claims", "evidence"],
        &["submission_ref"],
        "One immutable submission projection."
    ),
    entry!(
        "task.acceptance",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Read one acceptance decision and revocation state.",
        "Use when verifying whether an exact task result is currently accepted.",
        &["acceptance", "accepted", "revoked", "decision"],
        &["acceptance_operation_id"],
        "One acceptance projection."
    ),
    entry!(
        "attempt.get",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Read one attempt and its disposition.",
        "Use when the exact task-attempt identity is already known.",
        &["attempt", "disposition", "owner"],
        &["attempt_id"],
        "One attempt projection."
    ),
    entry!(
        "operation.get",
        Core,
        PARTICIPANT_CORE_AUDIENCES,
        Core,
        "Read one durable operation and its current state.",
        "Use to reconcile an asynchronous action from its operation ID.",
        &["operation", "progress", "async", "reconcile"],
        &["operation_id"],
        "One durable operation projection."
    ),
    entry!(
        "logging.get",
        Monitoring,
        MANAGER_ONLY_AUDIENCES,
        Searchable,
        "Read one ordinary Manager's client, Task/Attempt, owned Operation, binding route, or retained module diagnostic policy and live Producer status, including bounded Atlas-redacted-text capability.",
        "Use when reconciling bounded diagnostic detail for an authenticated Manager-owned scope; Task/Attempt context is inherited from an owned route when present.",
        &[
            "logging",
            "diagnostic",
            "telemetry",
            "level",
            "metadata",
            "redacted",
            "text",
            "filter"
        ],
        &[
            "optional client_id",
            "optional Task/Attempt selectors",
            "optional operation_id",
            "optional binding_id + binding_generation",
            "optional module_id"
        ],
        "Bounded policy and runtime metadata only; redacted text is producer-bounded and Atlas-redacted before observer queueing; native frames, raw capture, credentials, and cross-principal state remain unavailable."
    ),
    entry!(
        "operation.list",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Page operations, optionally filtered by state.",
        "Use for bounded monitoring when the operation ID is not yet known.",
        &["operations", "pending", "running", "state"],
        &[],
        "Bounded page filtered by state."
    ),
    entry!(
        "agent.state",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Read the observed state of one binding generation.",
        "Use when both the binding ID and generation are known.",
        &["agent", "binding", "generation", "runtime", "state"],
        &["binding_id", "generation"],
        "One observed binding state."
    ),
    entry!(
        "agent.usage",
        Monitoring,
        MANAGER_GM_AUDIENCES,
        Searchable,
        "Read observed usage counters for one binding generation.",
        "Use when both the binding ID and generation are known.",
        &["agent", "binding", "generation", "usage", "counters"],
        &["binding_id", "generation"],
        "One observed binding usage snapshot."
    ),
    entry!(
        "agent.list",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Page known bindings with observed state.",
        "Use to inspect a bounded slice of registered runtime bindings.",
        &["agents", "bindings", "runtime", "inventory"],
        &[],
        "Bounded page of known bindings."
    ),
    entry!(
        "agent.family",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Read a retained family observation for one binding generation.",
        "Use to inspect a specific retained observation; it is not a live query or complete inventory.",
        &["agent", "family", "observation", "history"],
        &["binding_id", "generation"],
        "Bounded retained observation page."
    ),
    entry!(
        "check.get",
        Review,
        REVIEW_READ_AUDIENCES,
        Core,
        "Read one check run and its evidence state.",
        "Use when reviewing the outcome of a known check run.",
        &["check", "validation", "run", "evidence"],
        &["check_id"],
        "One check-run projection."
    ),
    entry!(
        "check.profiles",
        AssignmentRead,
        OBSERVER_AUDIENCES,
        Searchable,
        "List configured check profiles.",
        "Use to select a configured check profile by its exact ID and revision.",
        &["check", "profiles", "verification", "configuration"],
        &["local check configuration"],
        "Configured profile list."
    ),
    entry!(
        "artifact.get",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Read immutable artifact metadata.",
        "Use to inspect artifact identity and size before reading bytes.",
        &["artifact", "metadata", "size", "digest"],
        &["artifact_id"],
        "One artifact metadata record."
    ),
    entry!(
        "artifact.read",
        Review,
        REVIEW_READ_AUDIENCES,
        Core,
        "Read one byte range from an immutable artifact.",
        "Use when exact artifact content is needed; request only the bounded byte range required.",
        &["artifact", "read", "bytes", "content", "evidence"],
        &["artifact_id", "offset_bytes", "length_bytes"],
        "One bounded byte range."
    ),
    entry!(
        "artifact.parts",
        AssignmentRead,
        ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Page the provenance manifest of an assembled result.",
        "Use to trace a whole assembled result to its immutable input parts.",
        &["artifact", "parts", "provenance", "manifest"],
        &["artifact_id"],
        "Bounded provenance page."
    ),
    entry!(
        "report.delta",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Read report entries after a durable cursor.",
        "Use for incremental report synchronization from a previously observed cursor.",
        &["report", "delta", "changes", "cursor", "sync"],
        &[],
        "Bounded incremental page."
    ),
    entry!(
        "monitor.follow",
        Monitoring,
        MANAGER_AUDIENCES,
        Searchable,
        "Read one bounded retained observation page after a monitor journal cursor.",
        "Use with the cursor from monitor.snapshot or a prior monitor.follow result.",
        &[
            "monitor", "follow", "journal", "cursor", "events", "lag", "gap"
        ],
        &["authenticated Manager", "journal cursor"],
        "Bounded visible page with explicit retention, gap, lag, and current-coverage facts."
    ),
    entry!(
        "report.attention",
        Monitoring,
        OBSERVER_AUDIENCES,
        Core,
        "Page controller-owned attention items.",
        "Use to find work that requires an operator or participant response.",
        &["attention", "blocked", "needs input", "pending"],
        &[],
        "Bounded attention page."
    ),
    entry!(
        "report.capacity",
        Monitoring,
        OBSERVER_AUDIENCES,
        Searchable,
        "Page active and reserved capacity accounting.",
        "Use to inspect capacity before deciding whether to admit more work.",
        &["capacity", "slots", "reserved", "active"],
        &[],
        "Bounded capacity page."
    ),
    entry!(
        "message.read",
        MailboxRaw,
        OBSERVER_AUDIENCES,
        Searchable,
        "Read directed mailbox messages after a cursor.",
        "Use for low-level mailbox diagnostics; reading does not delete a message.",
        &["mailbox", "message", "inbox", "delivery"],
        &[],
        "Bounded directed-message page."
    ),
    entry!(
        "host.mode",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Change whether the host admits new work.",
        "Use only for an explicit operator request to change admission mode.",
        &["host", "admission", "enable", "disable", "pause"],
        &["operator authorization", "new_work value"],
        "One durable mode change."
    ),
    entry!(
        "module.route.select",
        Administration,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Select one exact registered module descriptor for the caller's future bindings on a configured route.",
        "Use after reading the trusted module catalog; the exact module, artifact, version and catalog revision are required.",
        &["module", "artifact", "route", "select", "upgrade"],
        &[
            "authenticated Manager identity",
            "route_alias",
            "module_id",
            "artifact_id",
            "version",
            "expected_catalog_revision"
        ],
        "One revision-checked selection for this Manager's new bindings only; other Managers and existing bindings keep their own retained descriptors."
    ),
    entry!(
        "client.register",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Register a scoped client using a caller-generated token hash.",
        "Use only when explicitly onboarding a client; the raw credential stays local.",
        &["client", "register", "credential", "identity", "onboard"],
        &["operator authorization", "client identity", "token hash"],
        "One client registration; raw credentials are never returned."
    ),
    entry!(
        "source.capture",
        AcceptanceEffects,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Capture an exact Git commit as a fixed-source candidate for the current Task/Attempt.",
        "Use from the authenticated assigned Participant before task.submit; the repository and full commit are captured exactly.",
        &["source", "capture", "git", "commit", "candidate"],
        &["attempt_id", "expected_revision", "repository", "commit"],
        "One immutable source capture."
    ),
    entry!(
        "check.run",
        AcceptanceEffects,
        FULL_AUDIENCE,
        ManualOnly,
        "Run one configured check profile against a captured candidate.",
        "Use only after the exact candidate and check-profile revision are established.",
        &["check", "run", "test", "verify", "candidate"],
        &[
            "attempt_id",
            "candidate_ref",
            "profile_id",
            "profile_revision"
        ],
        "One check operation."
    ),
    entry!(
        "check.cancel",
        AcceptanceEffects,
        FULL_AUDIENCE,
        ManualOnly,
        "Request cancellation of one check run.",
        "Use only for an explicit cancellation request against the exact check ID.",
        &["check", "cancel", "stop"],
        &["check_id", "reason"],
        "Durable cancellation request; not proof of termination."
    ),
    entry!(
        "task.create",
        TaskManagement,
        MANAGER_AUDIENCES,
        Core,
        "Create a task from a specification without native effects.",
        "Use when an authorized manager needs a new durable unit of work.",
        &["task", "create", "new work", "specification"],
        &["project_id", "specification"],
        "One task record."
    ),
    entry!(
        "task.revise",
        TaskManagement,
        MANAGER_AUDIENCES,
        Searchable,
        "Replace a task specification by compare-and-swap on its revision.",
        "Use only when revising the exact current task revision.",
        &["task", "revise", "edit specification", "revision"],
        &["task_id", "expected_revision", "specification"],
        "One revision-checked task update."
    ),
    entry!(
        "task.claim",
        TaskManagement,
        MANAGER_AUDIENCES,
        Core,
        "Reserve an attempt for an existing task.",
        "Use when assigning a task to an exact owner and optional binding.",
        &["task", "claim", "assign", "attempt", "owner"],
        &["task_id", "expected_revision"],
        "One attempt reservation."
    ),
    entry!(
        "task.dispatch",
        TaskManagement,
        MANAGER_AUDIENCES,
        Core,
        "Start a claimed controller-start attempt or reuse its start operation; launch-owned Attempts require their exact launch Operation ID.",
        "Use after a valid claim. Supply launch_operation_id for a launch-owned Attempt; omit it only for a legacy unlinked Attempt. prerequisite_operation_id remains a runtime configuration prerequisite.",
        &["task", "dispatch", "start", "attempt", "launch"],
        &["attempt_id"],
        "One start operation or its existing durable handle."
    ),
    entry!(
        "task.submit",
        TaskManagement,
        TASK_SUBMIT_AUDIENCES,
        Searchable,
        "Seal an immutable submission and requirement report for an attempt.",
        "Use from the current assigned Participant or Attempt owner to submit its exact Task revision and Attempt; this does not accept the Task.",
        &["task", "submit", "submission", "candidate", "claims"],
        &[
            "attempt_id",
            "expected_revision",
            "candidate_ref",
            "summary"
        ],
        "One immutable submission."
    ),
    entry!(
        "task.submit.recover",
        TaskManagement,
        MANAGER_AUDIENCES,
        Searchable,
        "Recover an unknown submission from its exact existing artifact.",
        "Use as the current GM after a controller restart. Name the prior submission Operation; no file is created and no native work is replayed.",
        &["task", "submission", "recover", "restart", "unknown"],
        &["operation_id"],
        "One durable recovery receipt preserving the original submitting actor."
    ),
    entry!(
        "task.request_changes",
        Review,
        REVIEW_DISPOSITION_AUDIENCES,
        ManualOnly,
        "Record an anchored change request for the exact current submission.",
        "Use only for the existing legacy review-disposition path; it is not assigned-review evidence submission.",
        &["review", "finding", "changes", "submission", "candidate"],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "finding_id"
        ],
        "One revision-checked disposition with evidence."
    ),
    entry!(
        "task.accept",
        AcceptanceEffects,
        GM_AUDIENCES,
        ManualOnly,
        "Record acceptance for an exact submitted task result.",
        "Use only when an authorized operator explicitly accepts the exact current candidate and submission.",
        &["task", "accept", "acceptance", "publish gate"],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref"
        ],
        "One acceptance decision."
    ),
    entry!(
        "forge.publish_ref",
        AcceptanceEffects,
        GM_AUDIENCES,
        ManualOnly,
        "Publish the authorized candidate reference to the configured forge.",
        "Use only after explicit publish authorization for the exact accepted revision.",
        &["forge", "publish", "push", "reference"],
        &["attempt_id", "expected_revision", "repository", "ref"],
        "One guarded forge operation."
    ),
    entry!(
        "task.invalidate_acceptance",
        AcceptanceEffects,
        GM_AUDIENCES,
        ManualOnly,
        "Invalidate an acceptance tied to an outdated task revision.",
        "Use only when an explicit revision change requires revoking the prior acceptance.",
        &["acceptance", "invalidate", "revoke", "revision"],
        &["task_id", "expected_revision"],
        "One revision-checked invalidation."
    ),
    entry!(
        "attempt.release",
        TaskManagement,
        MANAGER_AUDIENCES,
        Searchable,
        "Release a reserved attempt using its exact revision.",
        "Use when an authorized manager explicitly releases an attempt reservation.",
        &["attempt", "release", "reservation"],
        &["attempt_id", "expected_revision"],
        "One revision-checked release."
    ),
    entry_with_inputs!(
        "attempt.bind_producer",
        TaskManagement,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Associate an already observed native producer run with one exact Attempt.",
        "Use only after retaining the exact assignment, native session/run and observation evidence; this starts no worker and consumes no result.",
        &[
            "attempt",
            "producer",
            "assignment",
            "native session",
            "native run",
            "observation"
        ],
        &[
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
            "observation_id"
        ],
        &[
            "current Attempt manager authority",
            "already observed exact producer evidence"
        ],
        "One evidence-bound producer association; no native effect or Task acceptance."
    ),
    entry!(
        "agent.open",
        RuntimeControl,
        MANAGER_AUDIENCES,
        Core,
        "Open or reuse the assigned native agent binding.",
        "Use after task admission when the manager must establish the exact runtime binding.",
        &["agent", "open", "runtime", "binding", "start"],
        &["binding_id", "generation"],
        "One binding operation; native session state remains separately observed."
    ),
    entry!(
        "agent.send",
        RuntimeControl,
        MANAGER_AUDIENCES,
        Core,
        "Send manager-owned steering text to one exact agent binding.",
        "Use for manager steering of an assigned agent, not peer mailbox delivery.",
        &["agent", "steer", "send", "prompt", "instruction"],
        &["binding_id", "generation", "text"],
        "One addressed manager-to-agent operation."
    ),
    entry!(
        "agent.reply",
        RuntimeControl,
        MANAGER_AUDIENCES,
        Searchable,
        "Reply to a specific native-input request.",
        "Use when the exact operation and request require a human or manager answer.",
        &["agent", "reply", "elicitation", "input request"],
        &["operation_id", "request_id", "response"],
        "One addressed reply operation."
    ),
    entry!(
        "agent.configure",
        RuntimeControl,
        MANAGER_AUDIENCES,
        Searchable,
        "Apply a validated configuration to one exact agent binding.",
        "Use only when an explicit manager workflow requires changing runtime configuration.",
        &["agent", "configure", "configuration", "profile"],
        &["binding_id", "generation", "configuration"],
        "One validated configuration operation."
    ),
    entry!(
        "agent.goal",
        RuntimeControl,
        MANAGER_AUDIENCES,
        Searchable,
        "Set, edit, pause, resume, continue or clear the goal of one exact binding.",
        "Use continue only to admit one input for an exact active controller Goal revision; expected_revision is required and 0 means no native Goal exists.",
        &[
            "agent",
            "goal",
            "objective",
            "continue",
            "expected revision"
        ],
        &[
            "binding_id",
            "generation",
            "action",
            "objective",
            "expected_revision"
        ],
        "One goal operation; continue admits one input and does not assert execution start or Goal completion."
    ),
    entry!(
        "agent.refresh",
        RuntimeRecovery,
        MANAGER_AUDIENCES,
        Searchable,
        "Refresh controller observations for an exact agent binding.",
        "Use to request fresh observed state when retained state may be old.",
        &["agent", "refresh", "reconcile", "observed state"],
        &["binding_id", "generation"],
        "One observation refresh operation."
    ),
    entry!(
        "agent.reconcile",
        RuntimeRecovery,
        MANAGER_AUDIENCES,
        Searchable,
        "Reconcile one binding against its durable controller identity.",
        "Use after a runtime discrepancy has been identified and an exact binding is known.",
        &["agent", "reconcile", "binding", "runtime state"],
        &["binding_id", "generation"],
        "One scoped reconciliation operation."
    ),
    entry!(
        "agent.recover",
        RuntimeRecovery,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Run the existing recovery path for one exact binding.",
        "Use only after an explicit manager recovery decision and exact binding review.",
        &["agent", "recover", "restart", "repair"],
        &["binding_id", "generation", "reason"],
        "One addressed recovery operation."
    ),
    entry!(
        "agent.result",
        AssignmentRead,
        MANAGER_AUDIENCES,
        Searchable,
        "Read the retained result for one exact agent binding.",
        "Use when checking a known binding's latest submitted result.",
        &["agent", "result", "output", "submission"],
        &["binding_id", "generation"],
        "One retained result projection."
    ),
    entry!(
        "artifact.assemble",
        AcceptanceEffects,
        FULL_AUDIENCE,
        ManualOnly,
        "Assemble an immutable result artifact from an exact manifest.",
        "Use only when an explicit workflow supplies the complete provenance manifest.",
        &["artifact", "assemble", "manifest", "provenance"],
        &["parts", "expected_digest"],
        "One immutable assembled artifact."
    ),
    entry!(
        "operation.cancel",
        RuntimeRecovery,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Request cancellation of an eligible queued operation.",
        "Use only for an explicit cancellation of the exact queued operation; running work is not terminated.",
        &["operation", "cancel", "stop", "queued"],
        &["operation_id", "reason"],
        "Durable cancellation request with explicit eligibility."
    ),
    entry_with_inputs!(
        "gm.handover",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Designate one registered eligible client as the current GM under the application epoch rules.",
        "Use only for an explicit guarded handover; optional binding identity is part of the request but not required.",
        &[
            "manager",
            "GM",
            "handover",
            "designation",
            "epoch",
            "operator"
        ],
        &["client_id"],
        &[
            "local Operator or exact current GM authority",
            "registered eligible target client"
        ],
        "One guarded GM designation with retained epoch identity."
    ),
    entry!(
        "agent.background",
        RuntimeControl,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Change foreground or background handling for one exact binding.",
        "Use only when the manager explicitly changes binding lifecycle presentation.",
        &["agent", "background", "foreground", "lifecycle"],
        &["binding_id", "generation"],
        "One addressed lifecycle operation."
    ),
    entry!(
        "message.send",
        MailboxRaw,
        MANAGER_AUDIENCES,
        Searchable,
        "Send one raw directed mailbox delivery.",
        "Use for low-level delivery with exact recipient and deadlines; this is not typed coordination.send.",
        &["message", "mailbox", "send", "delivery", "peer"],
        &["recipient", "text"],
        "One durable delivery operation."
    ),
    entry!(
        "message.cancel",
        MailboxRaw,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Cancel one sent mailbox delivery by exact ID and payload digest.",
        "Use only for an explicit cancellation of a known delivery.",
        &["message", "mailbox", "cancel", "delivery"],
        &["delivery_id", "payload_digest"],
        "One exact delivery cancellation request."
    ),
    entry!(
        "swarm.context.get",
        Core,
        COORDINATION_READ_AUDIENCES,
        Core,
        "Read the current authenticated Participant context or an exact manager-selected Task/Attempt context.",
        "Use before scoped coordination to establish the current Task/Attempt and relevant participant-owned cards.",
        &[
            "context",
            "task",
            "attempt",
            "assignment",
            "work card",
            "contract card"
        ],
        &["authenticated participant scope or exact manager scope tuple"],
        "Bounded current-scope projection with explicit selector coverage gaps."
    ),
    entry!(
        "swarm.dashboard",
        ManagerCore,
        OBSERVER_AUDIENCES,
        Core,
        "Read one bounded dashboard over retained controller facts.",
        "Use for an operator or manager snapshot; Observer output is aggregate-only and redacted.",
        &[
            "dashboard",
            "overview",
            "summary",
            "health",
            "queue",
            "capacity"
        ],
        &["optional limit"],
        "Bounded summary; Observer omits assignment detail and manager exceptions."
    ),
    entry!(
        "monitor.snapshot",
        Monitoring,
        MANAGER_AUDIENCES,
        Core,
        "Capture one Manager-authorized current-state snapshot and an observation-journal cut for race-free follow-up.",
        "Use before monitor.follow; updates committed after the returned cut are recoverable by its cursor.",
        &["monitor", "snapshot", "live", "journal", "cut", "state"],
        &["authenticated Manager"],
        "Current Store state plus an atomic durable observation cursor; no live process probe or model polling."
    ),
    entry!(
        "swarm.queue.get",
        ManagerCore,
        MANAGER_AUDIENCES,
        Core,
        "Page current Tasks with optional project and state filters.",
        "Use to find work in the bounded manager queue; order is creation time then Task ID.",
        &["queue", "tasks", "project", "state", "ready work"],
        &["optional after/limit/project_id/task_state"],
        "Bounded page; filters precede paging and priority is not recorded."
    ),
    entry!(
        "swarm.agent.inspect",
        ManagerCore,
        MANAGER_AUDIENCES,
        Core,
        "Inspect one exact Attempt and its bounded execution neighborhood.",
        "Use when the Attempt ID is known and current binding, operation, check, peer, or overlap evidence is needed.",
        &[
            "agent",
            "inspect",
            "attempt",
            "binding",
            "operation",
            "check",
            "peer",
            "overlap"
        ],
        &["attempt_id"],
        "Exact Attempt projection with separately bounded neighborhood pages."
    ),
    entry!(
        "swarm.exceptions.get",
        ManagerCore,
        MANAGER_AUDIENCES,
        Core,
        "Page manager-actionable retained attention items.",
        "Use to find exceptions requiring manager action; consult coverage gaps before treating the page as complete.",
        &[
            "exceptions",
            "attention",
            "blocked",
            "manager action",
            "needs attention"
        ],
        &["optional after/limit"],
        "Bounded manager-actionable page with explicit projection coverage."
    ),
    entry!(
        "swarm.launch.preview",
        ManagerCore,
        MANAGER_AUDIENCES,
        Core,
        "Validate one exact launch request under current Manager or local Operator authority and return its compact decision_card.",
        "Use before admitting a launch to review the exact Task revision, route, profiles, budget, stop conditions, requested configuration, and bounded assignment facts. Missing facts remain explicit unknown or evidence gaps.",
        &[
            "launch",
            "preview",
            "route",
            "agent profile",
            "MCP profile",
            "workspace policy",
            "budget"
        ],
        &[
            "task_id",
            "expected_task_revision",
            "route",
            "agent_profile",
            "mcp_profile",
            "mcp_surface",
            "workspace_policy",
            "budget",
            "stop_conditions",
            "purpose"
        ],
        "Read-only validation and retained preview row; it does not start an agent, native session, or model turn."
    ),
    entry!(
        "swarm.launch",
        ManagerCore,
        MANAGER_AUDIENCES,
        Core,
        "Submit one revision-checked, digest-bound launch intent after reviewing its preview.",
        "Use only after swarm.launch.preview for the same exact request and plan digest; the host verifies a registered workspace lease before claiming an Attempt and opening its native binding.",
        &[
            "launch",
            "admit",
            "plan digest",
            "workspace",
            "agent profile",
            "operation"
        ],
        &[
            "all required swarm.launch.preview fields",
            "plan_digest",
            "caller-owned client_request_id"
        ],
        "Durable phased launch Operation with exact workspace and binding progress; productive dispatch waits for scoped credentials and verified native MCP capability."
    ),
    entry!(
        "coordination.participant.get",
        AssignmentRead,
        MANAGER_AUDIENCES,
        Searchable,
        "Read one redacted Participant registration in a current exact scope.",
        "Use when the exact participant ID and Task/Attempt assignment tuple are known.",
        &[
            "participant",
            "identity",
            "registration",
            "scope",
            "assignment"
        ],
        &["client_id", "manager task_id/task_revision/attempt_id"],
        "One redacted Participant projection; credential material stays private."
    ),
    entry!(
        "coordination.participant.list",
        AssignmentRead,
        MANAGER_AUDIENCES,
        Searchable,
        "Page redacted Participant registrations for one exact current manager scope.",
        "Use only when a bounded roster is required; Participant profiles cannot call this method.",
        &[
            "participants",
            "roster",
            "assignment",
            "scope",
            "registered"
        ],
        &["task_id", "task_revision", "attempt_id"],
        "Bounded redacted roster with continuation and coverage metadata."
    ),
    entry!(
        "coordination.thread.open",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Open one durable, mailbox-only coordination Thread for an exact Task/Attempt and explicit verified roster.",
        "Use when a bounded cross-assignment question needs a durable scope, reasonability declaration and explicit participants; opening creates no assignment or model work.",
        &[
            "thread",
            "coordination",
            "open",
            "scope",
            "reasonability",
            "participants"
        ],
        &[
            "exact task_id/attempt_id",
            "explicit registered roster",
            "blocking fact and close condition"
        ],
        "Immutable Thread header and reasonability assessment; no recipient broadcast or model/native wake."
    ),
    entry!(
        "coordination.thread.get",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Read one retained Thread and its bounded message history when the caller is an authorized creator, sponsor, roster member or current manager.",
        "Use after an exact Thread ID is known; page from the last returned message sequence and treat message text as retained evidence, not authorization.",
        &["thread", "get", "messages", "coordination", "history"],
        &["thread_id", "optional after_message_seq and limit"],
        "Authorized bounded Thread projection with current scope and page coverage."
    ),
    entry!(
        "coordination.thread.list",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Page retained Threads after Store authorization filtering within one exact Task and optional Attempt.",
        "Use task_id to enumerate a caller-authorized scope; this method has no global Thread scan.",
        &["thread", "list", "coordination", "task", "attempt"],
        &[
            "task_id",
            "optional attempt_id/state/topic_kind and bounded cursor"
        ],
        "Bounded redacted Thread headers with continuation and coverage metadata."
    ),
    entry!(
        "coordination.message.send",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Core,
        "Send one bounded message to one explicit current roster recipient through the existing mailbox.",
        "Use for one exact Thread participant and speech act; provide an idempotency request ID and retain the returned message/operation identity.",
        &[
            "thread",
            "message",
            "send",
            "recipient",
            "mailbox",
            "proposal"
        ],
        &[
            "thread_id",
            "one roster recipient",
            "speech_act",
            "summary",
            "mailbox_only"
        ],
        "One durable addressed message; no broadcast, new queue, Task mutation or model/native call."
    ),
    entry!(
        "coordination.thread.resolve",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Searchable,
        "Close one exact Thread as resolved, unresolved, or withdrawn using its expected state revision.",
        "Use only when the caller has current scope authority; a contract Thread needs its exact manager-ratification Operation before it can resolve.",
        &["thread", "resolve", "unresolved", "close", "state revision"],
        &[
            "thread_id",
            "expected_state_revision",
            "outcome",
            "resolution_summary"
        ],
        "One retained closure transition; closure creates no follow-up work automatically."
    ),
    entry!(
        "coordination.thread.withdraw",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Searchable,
        "Withdraw one exact Thread under current creator or manager authority.",
        "Use when the Thread no longer needs a resolution; retain the reason and expected state revision.",
        &["thread", "withdraw", "close", "state revision"],
        &["thread_id", "expected_state_revision", "reason"],
        "One retained withdrawal transition; authorized history remains available."
    ),
    entry!(
        "coordination.thread.supersede",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Supersede one exact Thread with an already-open successor on the same Task/Attempt.",
        "Use when the scope is materially revised; name the exact successor and expected predecessor state revision.",
        &["thread", "supersede", "successor", "state revision"],
        &[
            "thread_id",
            "expected_state_revision",
            "superseding_thread_id",
            "reason"
        ],
        "One retained predecessor/successor linkage; no automatic work assignment."
    ),
    entry!(
        "coordination.contract.propose",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Searchable,
        "Create an immutable contract proposal revision in an exact authorized Thread.",
        "Use to state producer/consumer identity, payload, observation boundary, failure semantics and versioning; a revision is a proposal, not agreement.",
        &[
            "contract",
            "proposal",
            "producer",
            "consumer",
            "identity",
            "versioning"
        ],
        &[
            "thread_id",
            "affected path/symbol/schema sets",
            "seven-field statement",
            "64 KiB canonical request cap"
        ],
        "Immutable canonical proposal and digest; no implementation verification or implicit ratification."
    ),
    entry!(
        "coordination.contract.respond",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Searchable,
        "Record one exact participant response to an immutable contract revision and digest.",
        "Use counterproposal, object, support or withdraw; unsupported/empty objection basis is retained as no material progress.",
        &[
            "contract",
            "respond",
            "counterproposal",
            "object",
            "support",
            "withdraw"
        ],
        &[
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "proposal_digest",
            "act"
        ],
        "Immutable advisory response; support never ratifies a contract."
    ),
    entry!(
        "coordination.contract.ratify",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Ratify one exact current proposal under the authorized current manager's scope.",
        "Name the current proposal revision, digest, Thread revision and affected scope references; retain conditions and caveats.",
        &["contract", "ratify", "decision", "scope revision"],
        &[
            "thread_id",
            "proposal_revision_id",
            "proposal_digest",
            "affected_scope_revisions"
        ],
        "Immutable ratification and Observation; no Task acceptance or native work."
    ),
    entry!(
        "coordination.contract.reject",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Reject one exact current proposal under the authorized current manager's scope.",
        "Name the exact proposal and current scope and retain the rejection reason, conditions and caveats.",
        &["contract", "reject", "decision", "scope revision"],
        &[
            "thread_id",
            "proposal_revision_id",
            "proposal_digest",
            "affected_scope_revisions"
        ],
        "Immutable rejection and Observation; the Thread remains open."
    ),
    entry!(
        "coordination.contract.get",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Read one immutable proposal revision and its bounded response history in an authorized Thread.",
        "Use exact proposal and revision IDs; response/evidence text is available only through this scoped read, never a subscription hint.",
        &["contract", "proposal", "get", "revision", "responses"],
        &[
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "optional observation cursor and limit"
        ],
        "Scoped immutable revision packet with bounded response history."
    ),
    entry!(
        "coordination.contract.list",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Page metadata-only contract proposal headers in one authorized Thread.",
        "Use a monotonic sequence cursor when the exact proposal revision is not known; fetch a specific immutable revision through coordination.contract.get.",
        &["contract", "proposal", "list", "sequence", "thread"],
        &["thread_id", "optional after_sequence and limit"],
        "Metadata-only bounded proposal index page with continuation."
    ),
    entry!(
        "coordination.integration.ack",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Record one Participant's accept or dissent position against the exact agreement-cell state and comparison digests.",
        "Use only for the named cell, Task/Attempt and expected state/material/membership revisions; acceptance is not Task acceptance, code verification or contract ratification.",
        &[
            "integration",
            "agreement",
            "cell",
            "accept",
            "dissent",
            "position"
        ],
        &[
            "cell_id",
            "exact Task/Attempt",
            "expected revisions/digests",
            "accept or dissent"
        ],
        "One retained Participant position receipt; no new event stream or independent agreement authority."
    ),
    entry!(
        "coordination.agreement.get",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Read one bounded agreement cell and its paged positions for an exact Task/Attempt.",
        "Omit state_revision/material_digest for the current cell or supply both to read an exact historical version; continue with the returned position cursor.",
        &["agreement", "cell", "positions", "history", "get"],
        &[
            "cell_id",
            "task_id/task_revision/attempt_id",
            "paired optional revision and material digest"
        ],
        "Bounded cell and position projection with explicit current/history identity; position is advisory."
    ),
    entry!(
        "code.scope.propose",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Searchable,
        "Propose bounded paths, symbols or interfaces for manager review within one Task/Attempt.",
        "Use exact repository-relative selectors and a recorded baseline reference; a proposal does not reserve files, run Git or become accepted ownership.",
        &["code", "scope", "propose", "paths", "symbols", "interfaces"],
        &[
            "task_id",
            "attempt_id",
            "mode",
            "selectors",
            "baseline_candidate_ref",
            "reason"
        ],
        "One immutable advisory scope proposal with digest; no lock or process is created."
    ),
    entry!(
        "code.scope.accept",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Accept or explicitly revise one scope proposal under exact current manager authority.",
        "Name the expected state revision and proposal digest; broad-scope acknowledgement and override IDs are explicit recorded inputs.",
        &["code", "scope", "accept", "proposal", "override"],
        &[
            "scope_intent_id",
            "expected_state_revision",
            "proposal_digest",
            "reason"
        ],
        "Retained accepted scope intent and revision; never a filesystem lock."
    ),
    entry!(
        "code.scope.inspect",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Inspect retained active, stale or unknown scope intents with exact Task and optional selector filters.",
        "Use a bounded path, symbol, interface, actor or scope-intent selector; missing coverage remains explicit.",
        &["code", "scope", "inspect", "ownership", "baseline"],
        &["task_id", "optional exact scope filters and cursor"],
        "Bounded retained scope projection with coverage and gaps."
    ),
    entry!(
        "code.scope.conflicts",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Compare exact scope intents for path, symbol or interface overlap.",
        "Use to find recorded conflicts or coordination-required overlap; unknown selector coverage is never reported as no conflict.",
        &["code", "scope", "conflicts", "overlap", "coverage"],
        &["task_id", "one or more bounded scope selectors"],
        "Bounded advisory conflict classification with explicit unknown/partial coverage."
    ),
    entry!(
        "code.scope.release",
        ParticipantCoordination,
        COORDINATION_ACTION_AUDIENCES,
        Searchable,
        "Release one exact accepted scope intent under manager or current scope-owner authority.",
        "Use an expected state revision and reason; expiry alone never performs release.",
        &["code", "scope", "release", "owner", "revision"],
        &["scope_intent_id", "expected_state_revision", "reason"],
        "Previous/current state and revision with retained readback verification."
    ),
    entry!(
        "concilium.preview",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        Searchable,
        "Build the deterministic, scope-checked plan for one proposed Concilium.",
        "Use before opening a proposal to inspect exact participants, scopes, packet sizes, warnings, and the plan digest; preview starts no model or native work.",
        &["concilium", "preview", "plan", "participants", "digest"],
        &["proposal_operation_id", "authenticated manager scope"],
        "Deterministic plan and digest only; it commits no plan or model turn."
    ),
    entry!(
        "concilium.get",
        ParticipantCoordination,
        CONCILIUM_READ_AUDIENCES,
        Searchable,
        "Read one bounded Concilium projection under the caller's current authorized scope.",
        "Use when the exact Concilium ID is known; participant positions are masked until the applicable round is sealed.",
        &["concilium", "get", "position", "round", "dissent"],
        &["concilium_id", "optional limit and after_slot_id"],
        "Scope-filtered Concilium state with blind first-round position handling."
    ),
    entry!(
        "concilium.list",
        ParticipantCoordination,
        CONCILIUM_READ_AUDIENCES,
        Searchable,
        "Page Concilium projections visible in the caller's authorized scope.",
        "Use a bounded Task/Attempt filter when the exact Concilium ID is unknown; Store authorization is rechecked before paging.",
        &["concilium", "list", "task", "attempt", "round"],
        &["task_id; optional attempt_id/state and page cursor"],
        "Scope-filtered bounded page; participant projections preserve round-one blindness."
    ),
    entry!(
        "coordination.peer.find",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Core,
        "Find current peers by one exact relationship selector in the current scope.",
        "Use a contract, path, symbol, or interface selector to find relevant peers; it never walks the full roster.",
        &[
            "peer",
            "participant",
            "find",
            "owner",
            "contract",
            "path",
            "symbol",
            "interface"
        ],
        &["one exact relationship selector; manager also supplies exact scope tuple"],
        "Bounded selector-index page with stale and coverage gaps."
    ),
    entry!(
        "swarm.overlap.check",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Core,
        "Compare bounded current controller ownership and integration facts for possible overlap.",
        "Use with exact path, symbol, contract, or candidate selectors to identify recorded scope conflicts; missing Git, worktree, baseline, or capability evidence remains unknown.",
        &[
            "overlap",
            "ownership",
            "scope",
            "path",
            "symbol",
            "contract",
            "candidate",
            "conflict"
        ],
        &[
            "at least one selector or candidate_ref",
            "at most 24 combined paths/symbols/contracts",
            "authenticated Participant scope or exact Manager/Operator Task/Attempt scope"
        ],
        "Bounded retained-fact comparison only; Git evidence and absent coverage are reported as unknown, and no subprocess is started."
    ),
    entry!(
        "coordination.consult",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Resolve one exact current card owner and request one missing fact when the owner is unique and live.",
        "Use when an exact contract, path, symbol, or interface selector identifies a needed card field that is absent; ambiguous, unowned, and uncovered matches do not send a message.",
        &[
            "consult",
            "ask owner",
            "card field",
            "contract",
            "path",
            "symbol",
            "interface",
            "one fact"
        ],
        &[
            "one exact target selector",
            "field",
            "question_kind",
            "question",
            "why_needed",
            "evidence_refs",
            "authenticated current Participant scope"
        ],
        "Card answer or explicit resolution status; at most one exact-owner mailbox delivery, never a broadcast."
    ),
    entry!(
        "coordination.work_card.get",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Read the caller's or an exact in-scope Participant's current work card.",
        "Use when exact work-card fields or a participant card are needed beyond the context summary.",
        &[
            "work card",
            "participant",
            "provides",
            "requires",
            "integration",
            "scope"
        ],
        &["authenticated participant scope or manager scope tuple"],
        "One bounded current card projection or an explicit unavailable result."
    ),
    entry!(
        "coordination.work_card.list",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Page work cards matching one exact relationship selector.",
        "Use to compare current work cards for a specific contract, path, symbol, or interface.",
        &[
            "work cards",
            "list",
            "contract",
            "path",
            "symbol",
            "interface"
        ],
        &["one exact selector; manager also supplies exact scope tuple"],
        "Bounded selector-index page with stale and coverage gaps."
    ),
    entry!(
        "coordination.contract_card.get",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Read a contract card by exact contract key in the current scope.",
        "Use when the exact contract key is known; a manager may page matching owners in an exact scope.",
        &[
            "contract card",
            "agreement",
            "inputs",
            "outputs",
            "ownership",
            "limits"
        ],
        &["contract_key; authenticated participant scope or manager scope tuple"],
        "Bounded contract projection with explicit missing-card and coverage state."
    ),
    entry!(
        "coordination.contract_card.list",
        ParticipantCoordination,
        COORDINATION_READ_AUDIENCES,
        Searchable,
        "Page current contract cards for one exact contract key.",
        "Use to inspect which current participants published the named contract.",
        &["contract cards", "list", "agreement", "contract key"],
        &["contract_key; manager also supplies exact scope tuple"],
        "Bounded index page with explicit stale and coverage gaps."
    ),
    entry!(
        "coordination.inbox",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Read durable messages addressed to the authenticated Participant in its exact current scope.",
        "Use to receive scoped peer deliveries; inbound hold policy returns no items with an explicit state.",
        &["inbox", "messages", "peer", "delivery", "coordination"],
        &["authenticated participant; optional after_operation_id/limit"],
        "Bounded addressed-delivery page; reading is non-destructive."
    ),
    entry!(
        "coordination.watch.list",
        ParticipantCoordination,
        WATCH_AUDIENCES,
        Searchable,
        "Page one-shot terminal-operation watches in the caller's authenticated scope.",
        "Use when a known Task/Attempt scope needs a bounded view of watches; Participant callers omit the scope tuple and derive it from their live registration.",
        &[
            "watch",
            "operation",
            "terminal",
            "notification",
            "list",
            "scope"
        ],
        &[
            "authenticated Participant scope or exact Manager/Operator task_id/task_revision/attempt_id",
            "optional limit/after_watch_id"
        ],
        "Scope-filtered bounded watch page; authorization is rechecked before paging."
    ),
    entry!(
        "review.get",
        Review,
        REVIEW_ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Read one immutable review assignment under exact assignment-scope authorization.",
        "Use when the review_assignment_id is known and the assignment is visible to this reviewer or manager.",
        &["review", "assignment", "auditor", "slot", "evidence"],
        &["review_assignment_id"],
        "One immutable assignment and any retained result projection."
    ),
    entry!(
        "review.list",
        Review,
        REVIEW_ASSIGNMENT_READ_AUDIENCES,
        Searchable,
        "Page only review assignments authorized for this caller's exact assignment scope.",
        "Use when the exact assignment ID is not known but a Task, Attempt, or submission filter is available.",
        &["reviews", "assignments", "auditor", "task", "attempt"],
        &["optional task_id/attempt_id/submission_ref and page cursor"],
        "Scope filtering occurs before bounded paging."
    ),
    entry!(
        "swarm.review.context",
        Review,
        REVIEW_ASSIGNMENT_READ_AUDIENCES,
        Core,
        "Read the exact assigned-review packet and anchored submission/candidate metadata.",
        "Use before review.submit to inspect the assigned slot; retrieve content only through the exact artifact reference.",
        &[
            "review context",
            "audit",
            "submission",
            "candidate",
            "evidence",
            "review slot"
        ],
        &["review_assignment_id"],
        "Exact slot-scoped packet; artifact bytes remain a separately authorized read."
    ),
    entry!(
        "automation.config.get",
        Schedules,
        MANAGER_AUDIENCES,
        Searchable,
        "Page revisioned automation definitions for one project; current GM can select a former owner for continuity.",
        "Use to inspect retained definitions and their exact revision before a change or transfer.",
        &[
            "automation",
            "configuration",
            "definitions",
            "project",
            "revision"
        ],
        &[
            "project_id",
            "optional owner_manager_id",
            "optional after/limit"
        ],
        "Bounded owner-and-project-scoped page."
    ),
    entry!(
        "automation.config.preview",
        Schedules,
        MANAGER_ONLY_AUDIENCES,
        Searchable,
        "Validate a revision-checked automation plan without applying it.",
        "Optionally inspect the plan digest, conflicts, and calendar occurrences before applying changes.",
        &["automation", "preview", "plan", "conflict", "digest"],
        &["project_id", "1..32 unique automation changes"],
        "Read-only plan projection; does not enable or execute a model turn."
    ),
    entry!(
        "automation.config.explain",
        Schedules,
        MANAGER_AUDIENCES,
        Searchable,
        "Explain dispatch state and linked operations for one owned automation.",
        "Use with exact project and automation IDs; current GM can select owner_manager_id to inspect former-owner state.",
        &[
            "automation",
            "explain",
            "dispatch",
            "linked operations",
            "state"
        ],
        &["project_id", "automation_id", "optional owner_manager_id"],
        "One scoped entry explanation and bounded linked-work projection."
    ),
    entry!(
        "schedule.run_now",
        Schedules,
        MANAGER_ONLY_AUDIENCES,
        ManualOnly,
        "Run the exact saved CheckRun action once without enabling its recurrence.",
        "Use only for an explicit manual invocation of the authenticated Manager's selected CheckRun action; recurrence state and cron cursors are unchanged.",
        &["schedule", "run now", "manual", "check", "cron"],
        &["automation_id", "project_id", "client_request_id"],
        "One normal durable CheckRun Operation under the Manager's current action rights."
    ),
    entry!(
        "coordination.participant.register",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Register a Participant under one exact current Task/Attempt manager scope.",
        "Use only for an explicit onboarding decision; send a token hash and keep the raw credential local.",
        &[
            "participant",
            "register",
            "onboard",
            "scope",
            "credential hash"
        ],
        &[
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis"
        ],
        "One durable scoped registration; sponsored-reviewer credentials are unusable until exact slot binding."
    ),
    entry!(
        "coordination.participant.disable",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Disable one Participant grant while retaining coordination history.",
        "Use only after an explicit manager decision for the exact Participant and expected grant revision.",
        &["participant", "disable", "revoke", "grant revision"],
        &["client_id", "optional expected_grant_revision"],
        "One revision-checked grant change; history is not erased."
    ),
    entry!(
        "coordination.work_card.publish",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        ManualOnly,
        "Publish bounded work information for the authenticated Participant's current scope.",
        "Use when the Participant explicitly updates its current work card fields.",
        &[
            "work card",
            "publish",
            "provides",
            "requires",
            "paths",
            "interfaces"
        ],
        &["fields object; authenticated current Task/Attempt scope"],
        "One content-addressed card revision; no native effect."
    ),
    entry!(
        "coordination.work_card.withdraw",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        ManualOnly,
        "Withdraw the authenticated Participant's current work card.",
        "Use only when explicitly withdrawing the current work card; history remains retained.",
        &["work card", "withdraw", "unavailable"],
        &["authenticated current Task/Attempt scope"],
        "One durable availability change; prior card history remains."
    ),
    entry!(
        "coordination.contract_card.publish",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        ManualOnly,
        "Publish one bounded contract card under an exact contract key in the current scope.",
        "Use when explicitly publishing or revising a contract definition owned by this Participant.",
        &[
            "contract card",
            "publish",
            "agreement",
            "inputs",
            "outputs",
            "ownership"
        ],
        &[
            "contract_key",
            "fields object",
            "authenticated current scope"
        ],
        "One content-addressed contract revision; no native effect."
    ),
    entry!(
        "coordination.contract_card.withdraw",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        ManualOnly,
        "Withdraw one exact contract card owned by the authenticated Participant.",
        "Use only for an explicit withdrawal of the exact contract key; history remains retained.",
        &["contract card", "withdraw", "contract key", "unavailable"],
        &["contract_key", "authenticated current scope"],
        "One durable availability change; prior card history remains."
    ),
    entry!(
        "coordination.send",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Send one bounded typed coordination delivery to an active peer in the same current Task/Attempt.",
        "Use for explicit peer-to-peer coordination after confirming recipient identity and current scope.",
        &[
            "coordination",
            "send",
            "peer",
            "participant",
            "delivery",
            "message"
        ],
        &[
            "recipient",
            "non-null JSON body <= 8 KiB",
            "authenticated current scope"
        ],
        "One durable addressed delivery; no method passthrough or native effect."
    ),
    entry!(
        "coordination.sync_integration",
        ParticipantCoordination,
        PARTICIPANT_ONLY_AUDIENCES,
        Core,
        "Publish one structured integration offer or requirement linked to the caller's current contract card.",
        "Use after publishing the exact current contract card to record producer readiness/availability or consumer dimensions needed for comparison.",
        &[
            "integration",
            "sync",
            "compatibility",
            "offer",
            "requirement",
            "readiness",
            "dimensions",
            "contract"
        ],
        &[
            "contract_key",
            "exactly one non-null offer or requirement",
            "authenticated current Participant scope"
        ],
        "One durable advisory integration cell; identity and scope are Store-derived and the result never accepts work or wakes a model."
    ),
    entry!(
        "coordination.watch.create",
        ParticipantCoordination,
        WATCH_AUDIENCES,
        Core,
        "Create one exact-scope one-shot watch over a supported retained Operation, card, Task, Attempt, or consult-deadline fact.",
        "Use when an explicit caller-owned request needs a bounded mailbox-header notification for one exact subject and expected revision/state/deadline; Participant scope is derived and managers supply the exact Task/Attempt tuple.",
        &[
            "watch",
            "create",
            "operation",
            "contract revision",
            "task revision",
            "attempt disposition",
            "consult deadline",
            "one-shot",
            "mailbox notification"
        ],
        &[
            "one supported watch_kind and its exact address schema",
            "expires_at_ms",
            "delivery=mailbox_header",
            "one_shot=true",
            "authenticated Participant scope or exact Manager/Operator scope"
        ],
        "One durable exact-scope watch; trigger-time authorization is rechecked."
    ),
    entry!(
        "coordination.watch.cancel",
        ParticipantCoordination,
        WATCH_AUDIENCES,
        ManualOnly,
        "Cancel one exact stored operation watch after reauthorizing its stored scope.",
        "Use only when the caller explicitly requests cancellation of a known watch ID.",
        &["watch", "cancel", "operation", "watch_id"],
        &["watch_id", "stored watch scope remains authorized"],
        "One guarded watch cancellation; no unrelated watch or operation is changed."
    ),
    entry!(
        "concilium.propose",
        ParticipantCoordination,
        CONCILIUM_PROPOSAL_AUDIENCES,
        ManualOnly,
        "Propose one bounded Concilium and create manager attention only.",
        "Use when current participant-owned facts establish a material contract conflict that needs independent scoped positions; an authorized participant or manager may propose, and the proposal invokes no participant or changes no Task.",
        &[
            "concilium",
            "propose",
            "contract conflict",
            "manager attention"
        ],
        &[
            "exact Task/Attempt",
            "participants",
            "expected output",
            "client_request_id"
        ],
        "One proposal Operation and Concilium ID; no model turn, native input, or Task effect."
    ),
    entry!(
        "concilium.position.submit",
        ParticipantCoordination,
        CONCILIUM_POSITION_AUDIENCES,
        ManualOnly,
        "Submit one structured position to the authenticated Participant's exact Concilium slot.",
        "Use only with the exact slot and packet digest delivered for the current round; this does not expose another participant's unsealed round-one position.",
        &["concilium", "position", "slot", "claim", "dissent"],
        &[
            "concilium_id",
            "slot_id",
            "packet_digest",
            "bounded position",
            "client_request_id"
        ],
        "One retained slot response Operation; malformed output is isolated to its slot."
    ),
    entry!(
        "concilium.open",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Commit the exact previewed Concilium plan and its slots.",
        "Use only after reviewing the current deterministic preview and its digest; opening starts no model, agent, or native session.",
        &["concilium", "open", "manager", "plan digest", "slots"],
        &[
            "proposal_operation_id",
            "plan_digest",
            "confirmed_reasonable=true",
            "manager_reason",
            "client_request_id"
        ],
        "One manager-authorized plan Operation; it commits slots without dispatching them."
    ),
    entry!(
        "concilium.round.advance",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Commit the next explicit Concilium packet and round.",
        "Use after reading the exact current state and positions; advancing is a manager Operation and never selects or invokes a speaker.",
        &["concilium", "round", "advance", "cross-review", "manager"],
        &[
            "concilium_id",
            "expected_state_revision",
            "next_round",
            "manager_reason",
            "client_request_id"
        ],
        "One revision-checked round Operation; Store builds and retains exact slot packet digests, and round three requires a changed merged-proposal digest."
    ),
    entry!(
        "concilium.close",
        ParticipantCoordination,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Record the manager's advisory result for one exact Concilium.",
        "Use after reviewing all authorized positions and dissent; close does not ratify a contract, mutate a Task, or trigger follow-up work.",
        &[
            "concilium",
            "close",
            "advisory",
            "minority report",
            "dissent"
        ],
        &[
            "concilium_id",
            "expected_state_revision",
            "result",
            "manager_reason",
            "client_request_id"
        ],
        "One manager-authorized advisory result retaining valid positions and dissent."
    ),
    entry!(
        "review.assign",
        Review,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Assign one reviewer to the exact current submitted candidate.",
        "Use only after inspecting the exact submission; choose one reviewer or profile and preserve replacement evidence when replacing a slot.",
        &[
            "review",
            "assign",
            "reviewer",
            "auditor",
            "slot",
            "candidate"
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "exact reviewer or review profile"
        ],
        "One immutable assignment and durable Operation; no Task feedback or acceptance."
    ),
    entry!(
        "review.submit",
        Review,
        ASSIGNED_REVIEWER_AUDIENCES,
        Core,
        "Submit an immutable verdict for the authenticated review assignment's exact slot.",
        "Use only after reading swarm.review.context and the exact assigned evidence; the verdict does not change Task state.",
        &[
            "review", "submit", "verdict", "findings", "evidence", "auditor"
        ],
        &[
            "review_assignment_id",
            "submission_ref",
            "candidate_ref",
            "verdict",
            "coverage",
            "evidence_refs"
        ],
        "One exact-slot-scoped immutable result; no repair, feedback, or publication."
    ),
    entry!(
        "automation.config.apply",
        Schedules,
        MANAGER_ONLY_AUDIENCES,
        ManualOnly,
        "Apply a revision-checked automation definition plan owned by the authenticated Manager.",
        "Save the authenticated manager's selected changes directly; when a preview was used, supply its digest for the same plan.",
        &[
            "automation",
            "apply",
            "configuration",
            "activation",
            "preview digest"
        ],
        &[
            "project_id",
            "changes",
            "optional preview_digest",
            "client_request_id"
        ],
        "One guarded configuration update; enabling dispatch does not start a model turn."
    ),
    entry!(
        "logging.set",
        Monitoring,
        MANAGER_ONLY_AUDIENCES,
        ManualOnly,
        "Persist one metadata or bounded Atlas-redacted-text diagnostic level for the authenticated ordinary Manager's client, Task/Attempt, owned Operation, binding route, or retained module scope.",
        "Use after reading logging.get when the Manager owns the selected scope; an optional bounded ttl_seconds stores an absolute expiry. Native frames remain unsupported because no bounded frame producer exists.",
        &[
            "logging",
            "set",
            "diagnostic",
            "telemetry",
            "level",
            "metadata",
            "redacted",
            "text",
            "filter"
        ],
        &[
            "level",
            "content=metadata|redacted_text",
            "optional scope selector",
            "optional ttl_seconds",
            "client_request_id"
        ],
        "One durable Store meta policy and after-commit Producer reload; selected text is Atlas-redacted before the observer queue, while recorder file selection and retention remain the existing swarm-observer policy."
    ),
    entry!(
        "event.emit",
        Scripts,
        MANAGER_ONLY_AUDIENCES,
        ManualOnly,
        "Record one bounded Manager-owned system event for an exact project.",
        "Use when a Manager-owned producer needs to publish an arbitrary event that an exact configured ScriptRun selector may consume.",
        &["event", "emit", "custom", "system event", "dedupe"],
        &[
            "project_id",
            "name",
            "non-null JSON payload",
            "dedupe_key",
            "optional existing cause"
        ],
        "One durable owner-scoped observation and Operation; payload is visible only through the owner's existing report/Operation reads, while bus pages expose safe headers."
    ),
    entry!(
        "automation.config.transfer",
        Administration,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Transfer one retained automation to the current designated GM without resetting cursors or replaying effects.",
        "Use after GM handover and reading the former owner's exact automation revision; requires current GM or local Operator authority.",
        &[
            "automation",
            "transfer",
            "handover",
            "GM",
            "continuity",
            "ownership"
        ],
        &[
            "project_id",
            "former_owner_manager_id",
            "automation_id",
            "expected_revision",
            "client_request_id"
        ],
        "One atomic ownership relocation with retained pending operations and original history."
    ),
    entry!(
        "hook.source.get",
        Hooks,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Read non-secret metadata and bounded retained facts for one setup-issued repository hook source.",
        "Use to inspect one exact source before install readback or revocation; setup tokens are never returned.",
        &["hook", "source", "repository", "commit", "readback"],
        &["source_id", "optional after cursor", "optional limit"],
        "One scoped public source record and bounded commit facts; no credentials or installation mutation."
    ),
    entry!(
        "hook.source.revoke",
        Hooks,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Disable one setup-issued repository hook source with revision compare-and-swap.",
        "Use after reading the source revision; local wrapper restoration is a separate CLI action.",
        &["hook", "source", "revoke", "disable", "repository"],
        &["source_id", "expected_revision", "client_request_id"],
        "One retained source revocation receipt; never returns the source credential."
    ),
    entry!(
        "goal.create",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Create a task-scoped Goal that tracks declared completion evidence and an optional one-shot reminder.",
        "Use for one exact project, Task revision, and Attempt after reading the existing assignment.",
        &["goal", "tracking", "objective", "completion", "reminder"],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "objective",
            "completion_evidence"
        ],
        "A revisioned tracking definition; creation starts no Task, model, or native work."
    ),
    entry!(
        "goal.revise",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Revise one exact task-scoped Goal under revision compare-and-swap.",
        "Use after reading the Goal; provide at least one explicitly selected definition field.",
        &["goal", "tracking", "revise", "objective", "reminder"],
        &[
            "exact Goal scope",
            "expected_revision",
            "selected changed fields",
            "client_request_id"
        ],
        "One revisioned definition update; evidence readback does not change the definition revision."
    ),
    entry!(
        "goal.enable",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Enable a selected one-shot Goal reminder entry with compare-and-swap.",
        "Use only for the exact retained Goal revision after confirming its task scope.",
        &["goal", "enable", "reminder", "task-scoped"],
        &["exact Goal scope", "expected_revision", "client_request_id"],
        "Enables reminder eligibility only; it does not dispatch Task or model work."
    ),
    entry!(
        "goal.disable",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Disable one task-scoped Goal reminder entry with compare-and-swap.",
        "Use to stop new reminder notices while preserving Goal history and readback.",
        &["goal", "disable", "reminder", "task-scoped"],
        &["exact Goal scope", "expected_revision", "client_request_id"],
        "Stops future reminder eligibility and retains earlier evidence and receipts."
    ),
    entry!(
        "goal.readback",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Persist a fresh declared-evidence evaluation for one exact Goal scope.",
        "Use when a fresh completion projection is needed; the request is receipt-backed and task-scoped.",
        &["goal", "readback", "completion", "evidence", "accepted"],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id",
            "client_request_id"
        ],
        "A retained pending/completed/unknown evidence projection; prose and notices never prove completion."
    ),
    entry!(
        "goal.get",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Read one exact retained Goal and its current completion/reminder projection.",
        "Use with the full project, Task revision, and Attempt scope.",
        &["goal", "get", "tracking", "completion", "reminder"],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "goal_id"
        ],
        "One authorized retained Goal record; does not refresh evidence or start work."
    ),
    entry!(
        "goal.list",
        Goals,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Page Goals within one exact project, Task revision, and Attempt.",
        "Use to inspect the scoped retained Goal set with a bounded keyset page.",
        &["goal", "list", "tracking", "task-scoped"],
        &[
            "project_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "optional after_goal_id",
            "optional limit"
        ],
        "Bounded scoped Goal metadata; does not read unrelated Tasks or activate work."
    ),
    entry!(
        "script.register",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Register a complete bounded trusted-local Python or PowerShell bundle.",
        "Use after preparing all fixed files, interpreter identity, argv, environment, and closed input/result schemas.",
        &["script", "bundle", "register", "python", "powershell"],
        &["bundle", "client_request_id"],
        "One immutable bundle revision; no API capabilities or recurring trigger are enabled."
    ),
    entry!(
        "script.revise",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Publish a complete replacement script bundle under revision compare-and-swap.",
        "Use after reading the current script metadata and preparing the full replacement bundle.",
        &[
            "script",
            "bundle",
            "revise",
            "revision",
            "python",
            "powershell"
        ],
        &[
            "script_id",
            "expected_revision",
            "bundle",
            "client_request_id"
        ],
        "One complete new immutable bundle revision; partial file edits are rejected."
    ),
    entry!(
        "script.validate",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Validate a retained script bundle revision without executing it.",
        "Use to inspect captured bundle and interpreter metadata before activation.",
        &["script", "validate", "bundle", "interpreter"],
        &["script_id", "revision"],
        "Validation metadata only; no process starts and no bundle content is returned."
    ),
    entry!(
        "script.activate",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Select a retained script revision for future authorized invocations.",
        "Use after validation; activation does not execute the script or create a trigger.",
        &["script", "activate", "revision", "bundle"],
        &["script_id", "revision", "client_request_id"],
        "One selected future content revision; no invocation is started."
    ),
    entry!(
        "script.run",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Start exactly one authorized script invocation for one exact Attempt and Task revision.",
        "Use only with a validated activated revision and schema-valid input; repeat work needs a separately enabled trigger.",
        &["script", "run", "invoke", "python", "powershell"],
        &[
            "script_id",
            "expected_script_revision",
            "attempt_id",
            "expected_task_revision",
            "input",
            "client_request_id"
        ],
        "One queued invocation receipt; never accepts executable, argv, path, environment, API credential, or trigger overrides."
    ),
    entry!(
        "script.get",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Read one script registry item's metadata and optional revision.",
        "Use to check activation and revision before a direct run or edit.",
        &["script", "get", "registry", "revision"],
        &["script_id", "optional revision"],
        "Metadata only; no source bytes, credentials, process state, or execution effect."
    ),
    entry!(
        "script.list",
        Scripts,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Page script registry metadata without exposing bundle contents.",
        "Use for a bounded inventory of scripts visible to the current authorized Store scope.",
        &["script", "list", "registry", "inventory"],
        &["optional after offset", "optional limit"],
        "Bounded script metadata only; never lists filesystem files or starts an invocation."
    ),
    entry!(
        "bus.events.page",
        Scripts,
        MANAGER_ONLY_AUDIENCES,
        Searchable,
        "Read safe event headers selected by one enabled ScriptRun consumer.",
        "Use the existing durable Manager-owned cursor as the read position; this page does not acknowledge or advance it.",
        &["bus", "events", "page", "script trigger", "cursor"],
        &[
            "project_id",
            "consumer_id",
            "optional after_observation_id/limit"
        ],
        "Bounded safe metadata only; event payloads and source keys remain private."
    ),
    entry!(
        "bus.consumer.admit",
        Scripts,
        MANAGER_ONLY_AUDIENCES,
        ManualOnly,
        "Atomically retain exact ScriptRun pending intents and advance the existing consumer cursor.",
        "Read a fresh bus.events.page and pass its exact automation revision, cursor cut, and canonical occurrence/action projection.",
        &[
            "bus",
            "consumer",
            "admit",
            "script run",
            "cursor",
            "idempotency"
        ],
        &[
            "project_id",
            "consumer_id",
            "automation_revision",
            "expected_cursor",
            "through_observation_id",
            "occurrences",
            "client_request_id"
        ],
        "Existing cursor plus pending intent share this Store transaction; normal script.run Operation admission remains the later continuation."
    ),
    entry!(
        "github.source.inspect",
        GitHub,
        GM_AUDIENCES,
        ManualOnly,
        "Inspect one public GitHub repository using the already authenticated local gh account.",
        "Use the exact host, owner and repository before registering an intake source.",
        &["github", "repository", "inspect", "source", "issues"],
        &["host", "owner", "repo"],
        "Bounded public repository metadata; no credential or write effect."
    ),
    entry!(
        "github.source.setup",
        GitHub,
        FULL_AUDIENCE,
        ManualOnly,
        "Register one inspected GitHub repository for bounded issue observation.",
        "Use only from a local Full compatibility surface after confirming the exact repository ID.",
        &["github", "source", "setup", "repository", "issue intake"],
        &[
            "source_id",
            "project_id",
            "host",
            "owner",
            "repo",
            "repository_id",
            "client_request_id"
        ],
        "One local source registration; creates no Task or external GitHub write."
    ),
    entry!(
        "github.source.get",
        GitHub,
        GM_AUDIENCES,
        ManualOnly,
        "Read public status and bounded coverage for one registered GitHub source.",
        "Use to confirm source scope and the last retained poll disposition.",
        &["github", "source", "get", "coverage", "readback"],
        &["source_id"],
        "Source metadata and bounded coverage only; no raw token or comment body."
    ),
    entry!(
        "github.source.poll",
        GitHub,
        FULL_AUDIENCE,
        ManualOnly,
        "Poll one registered GitHub source once and retain bounded issue observations.",
        "Use only from a local Full compatibility surface with a setup-issued source ID.",
        &["github", "source", "poll", "issues", "coverage"],
        &["source_id", "client_request_id"],
        "One durable poll receipt with explicit coverage; incomplete reads do not infer deletion."
    ),
    entry!(
        "github.work_pool.preview",
        GitHub,
        GM_AUDIENCES,
        ManualOnly,
        "Preview source-mapped Tasks eligible for explicit local work-pool admission.",
        "Inspect the bounded preview before selecting exact Task IDs for apply.",
        &["github", "work pool", "preview", "task", "issue intake"],
        &["source_id", "optional after cursor", "optional limit"],
        "Bounded local preview; no Task dispatch, external write or model call."
    ),
    entry!(
        "github.work_pool.apply",
        GitHub,
        GM_AUDIENCES,
        ManualOnly,
        "Apply an explicit selection of source-mapped Tasks to the existing local work pool.",
        "Use after reviewing the exact preview and selecting bounded Task IDs.",
        &["github", "work pool", "apply", "task", "issue intake"],
        &["source_id", "task_ids", "client_request_id"],
        "One local admission receipt; does not start Task execution or call a model."
    ),
    entry!(
        "github.effect.managed_label",
        GitHub,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Set or remove one explicitly requested Eliot-managed label on a selected GitHub Issue.",
        "Use only for a source-mapped Task in the selected work pool; inspect its Operation after dispatch.",
        &["github", "issue", "managed label", "effect", "reconcile"],
        &[
            "source_id",
            "task_id",
            "expected_task_revision",
            "label",
            "present"
        ],
        "One durable desired-state effect; unknown writes are read back and never resent."
    ),
    entry!(
        "github.effect.reconcile_managed_label",
        GitHub,
        GM_AUDIENCES,
        ManualOnly,
        "Read back one exact unknown managed-label Operation under current GM or Operator authority.",
        "Use when an earlier label write is outcome-unknown; this method never sends a label write.",
        &["github", "issue", "managed label", "readback", "reconcile"],
        &["operation_id", "client_request_id"],
        "One ordinary readback Operation; only exact repository, Issue and desired-label evidence settles the original Operation."
    ),
    entry!(
        "github.pull_request.update_description",
        GitHub,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Update the title and body of one open PR tied to an exactly applied accepted-candidate publication.",
        "Use only after verifying the retained publication Operation and exact PR ID, number, repository, published head branch/SHA, and expected base branch.",
        &[
            "github",
            "pull request",
            "description",
            "title",
            "body",
            "reconcile"
        ],
        &[
            "publication_operation_id",
            "pull_request_id",
            "pull_request_number",
            "base_ref",
            "title",
            "body"
        ],
        "One durable title/body PATCH; it cannot create, retarget, close, merge, or change draft state, and unknown writes are read back without resending."
    ),
    entry!(
        "github.pull_request.reconcile_description",
        GitHub,
        MANAGER_GM_AUDIENCES,
        ManualOnly,
        "Read back one exact unknown PR description Operation and settle it only when the retained desired title and body are observed.",
        "Use after an ambiguous update or restart. This current Operator/GM action performs GET-only reconciliation and never retries the PATCH.",
        &[
            "github",
            "pull request",
            "description",
            "reconcile",
            "readback",
            "unknown operation"
        ],
        &["operation_id"],
        "A separate durable reconciliation Operation; it preserves the original caller/request and settles the target only after exact repository, PR, head, base, title, and body readback."
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreRole {
    Participant,
    Observer,
    LegacyReviewer,
    AssignedReviewer,
    Manager,
    FullCompatibility,
}

#[derive(Debug, Clone, Copy)]
pub struct RoleCore {
    pub role: CoreRole,
    pub methods: &'static [&'static str],
    pub gaps: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationBasis {
    AuthenticatedStoreScope,
}

impl AuthorizationBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthenticatedStoreScope => "authenticated_store_scope",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AuthorizationRevision<'a> {
    pub value: &'a str,
    pub basis: AuthorizationBasis,
}

#[derive(Debug, Clone, Serialize)]
pub struct SuggestedSurface {
    pub id: String,
    pub core_role: &'static str,
    pub core: &'static str,
    pub deferred_groups: Vec<&'static str>,
    pub exact_manual_methods: Vec<&'static str>,
}

const OBSERVER_CORE: &[&str] = &["swarm.tools.search", "swarm.dashboard", "operation.get"];
const LEGACY_REVIEWER_CORE: &[&str] = &[
    "swarm.tools.search",
    "task.submission",
    "artifact.read",
    "check.get",
    "operation.get",
];
const ASSIGNED_REVIEWER_CORE: &[&str] = &[
    "swarm.tools.search",
    "swarm.review.context",
    "task.submission",
    "artifact.read",
    "check.get",
    "review.submit",
    "operation.get",
];
const MANAGER_CORE: &[&str] = &[
    "swarm.tools.search",
    "swarm.dashboard",
    "monitor.snapshot",
    "monitor.follow",
    "swarm.queue.get",
    "swarm.agent.inspect",
    "swarm.exceptions.get",
    "swarm.launch.preview",
    "swarm.launch",
    "operation.get",
];
const PARTICIPANT_CORE: &[&str] = &[
    "swarm.context.get",
    "swarm.tools.search",
    "coordination.consult",
    "coordination.sync_integration",
    "coordination.send",
    "coordination.inbox",
    "coordination.watch.create",
    "swarm.overlap.check",
    "source.capture",
    "task.submit",
    "artifact.read",
    "operation.get",
];
const NO_FULL_COMPATIBILITY_CORE: &[&str] = &[];
const PARTICIPANT_GAPS: &[&str] = &[];
const OBSERVER_GAPS: &[&str] = &[];
const REVIEWER_GAPS: &[&str] = &[];
const ASSIGNED_REVIEWER_GAPS: &[&str] = &[];
const MANAGER_GAPS: &[&str] = &["swarm.agent.steer"];
const FULL_GAPS: &[&str] = &[];

pub const fn role_core(role: CoreRole) -> RoleCore {
    match role {
        CoreRole::Participant => RoleCore {
            role,
            methods: PARTICIPANT_CORE,
            gaps: PARTICIPANT_GAPS,
        },
        CoreRole::Observer => RoleCore {
            role,
            methods: OBSERVER_CORE,
            gaps: OBSERVER_GAPS,
        },
        CoreRole::LegacyReviewer => RoleCore {
            role,
            methods: LEGACY_REVIEWER_CORE,
            gaps: REVIEWER_GAPS,
        },
        CoreRole::AssignedReviewer => RoleCore {
            role,
            methods: ASSIGNED_REVIEWER_CORE,
            gaps: ASSIGNED_REVIEWER_GAPS,
        },
        CoreRole::Manager => RoleCore {
            role,
            methods: MANAGER_CORE,
            gaps: MANAGER_GAPS,
        },
        CoreRole::FullCompatibility => RoleCore {
            role,
            methods: NO_FULL_COMPATIBILITY_CORE,
            gaps: FULL_GAPS,
        },
    }
}

pub const fn role_core_for_profile(profile: McpToolProfile) -> CoreRole {
    match profile {
        McpToolProfile::Observer => CoreRole::Observer,
        McpToolProfile::Reviewer => CoreRole::LegacyReviewer,
        McpToolProfile::Participant => CoreRole::Participant,
        McpToolProfile::AssignedReviewer => CoreRole::AssignedReviewer,
        McpToolProfile::Manager => CoreRole::Manager,
        // The canonical GM/operator presentation uses the manager core; the
        // hard GM profile continues to carry the additional authority.
        McpToolProfile::Gm => CoreRole::Manager,
        McpToolProfile::Full => CoreRole::FullCompatibility,
    }
}

/// A presentation view is independent of the hard profile. It can only hide
/// methods; every returned method is still checked against the profile and
/// the caller-supplied object/work-context predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Surface {
    pub core: CoreRole,
    pub groups: BTreeSet<ToolGroup>,
    pub exact_manual_methods: BTreeSet<String>,
    pub legacy_full: bool,
}

impl Surface {
    pub fn role_default(profile: McpToolProfile) -> Self {
        Self {
            core: role_core_for_profile(profile),
            groups: BTreeSet::new(),
            exact_manual_methods: BTreeSet::new(),
            legacy_full: profile == McpToolProfile::Full,
        }
    }

    pub fn with_group(&self, group: ToolGroup) -> Self {
        let mut next = self.clone();
        next.groups.insert(group);
        next
    }

    pub fn with_exact_manual_method(&self, method: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.exact_manual_methods.insert(method.into());
        next
    }

    pub fn configured(
        profile: McpToolProfile,
        requested_core: Option<&str>,
        groups: &[String],
        exact_manual_methods: &[String],
    ) -> Result<Self, CatalogError> {
        let default_core = role_core_for_profile(profile);
        let core = match requested_core {
            None | Some("role-core") => default_core,
            Some("observer-core") => CoreRole::Observer,
            Some("reviewer-core") => CoreRole::LegacyReviewer,
            Some("assigned-reviewer-core") => CoreRole::AssignedReviewer,
            Some("participant-core") => CoreRole::Participant,
            Some("manager-core") => CoreRole::Manager,
            Some("full-legacy") if profile == McpToolProfile::Full => CoreRole::FullCompatibility,
            Some("full-legacy") | Some(_) => return Err(CatalogError::InvalidSurface),
        };
        if core != default_core {
            return Err(CatalogError::InvalidSurface);
        }

        let mut surface = Self::role_default(profile);
        for group_name in groups {
            let group = ToolGroup::parse(group_name).ok_or(CatalogError::InvalidSurface)?;
            surface.groups.insert(group);
        }
        for method in exact_manual_methods {
            let method = canonical_method(method).ok_or(CatalogError::InvalidSurface)?;
            if !exposes_method(profile, method)
                || metadata_for(method).map(|metadata| metadata.load_tier)
                    != Some(LoadTier::ManualOnly)
            {
                return Err(CatalogError::InvalidSurface);
            }
            surface.exact_manual_methods.insert(method.to_owned());
        }
        Ok(surface)
    }

    pub fn suggested(&self) -> SuggestedSurface {
        let role = role_core(self.core).role;
        SuggestedSurface {
            id: self.id(),
            core_role: core_role_name(role),
            core: core_surface_name(role),
            deferred_groups: self.groups.iter().map(|group| group.as_str()).collect(),
            exact_manual_methods: self
                .exact_manual_methods
                .iter()
                .filter_map(|method| metadata_for(method).map(|metadata| metadata.method))
                .collect(),
        }
    }

    pub fn id(&self) -> String {
        let mut hasher = Sha256::new();
        update_field(&mut hasher, b"eliot-mcp-surface-v1");
        update_field(&mut hasher, core_role_name(self.core).as_bytes());
        update_field(&mut hasher, &[u8::from(self.legacy_full)]);
        for group in &self.groups {
            update_field(&mut hasher, group.as_str().as_bytes());
        }
        for method in &self.exact_manual_methods {
            update_field(&mut hasher, method.as_bytes());
        }
        let digest: [u8; 32] = hasher.finalize().into();
        hex_digest(&digest)
    }

    pub fn includes(&self, metadata: &ToolMetadata) -> bool {
        if self.legacy_full {
            return true;
        }
        if metadata.load_tier == LoadTier::ManualOnly {
            return self.exact_manual_methods.contains(metadata.method);
        }
        role_core(self.core).methods.contains(&metadata.method)
            || self.groups.contains(&metadata.group)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    IncompleteRegistry,
    InvalidSurface,
    InvalidCursor,
    StaleCursor,
    CursorOutOfRange,
    StaleCatalogRevision,
    ToolSchemaTooLarge,
    Serialization(String),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteRegistry => f.write_str("MCP catalog metadata does not cover the tool registry"),
            Self::InvalidSurface => f.write_str("MCP presentation surface is invalid for this profile"),
            Self::InvalidCursor => f.write_str("MCP tools/list cursor is invalid"),
            Self::StaleCursor => f.write_str("MCP tools/list cursor is stale for the current authorized catalog, profile, or surface; relist from the first page"),
            Self::CursorOutOfRange => f.write_str("MCP tools/list cursor position is invalid"),
            Self::StaleCatalogRevision => f.write_str("MCP search catalog revision is stale; search the current authorized catalog"),
            Self::ToolSchemaTooLarge => f.write_str("one authorized MCP tool schema exceeds the bounded page size"),
            Self::Serialization(message) => write!(f, "MCP catalog serialization failed: {message}"),
        }
    }
}

impl std::error::Error for CatalogError {}

/// Check the one-to-one relationship between the executable method registry
/// and its descriptive metadata before producing any discovery result.
pub fn validate_registry_metadata() -> std::result::Result<(), CatalogError> {
    if TOOL_METADATA.len() != TOOLS.len() {
        return Err(CatalogError::IncompleteRegistry);
    }
    let mut seen = BTreeSet::new();
    for metadata in TOOL_METADATA {
        let Some((_, spec)) = find_tool_spec(metadata.method) else {
            return Err(CatalogError::IncompleteRegistry);
        };
        if !seen.insert(metadata.method) || !metadata_input_contract_matches(metadata, spec) {
            return Err(CatalogError::IncompleteRegistry);
        }
    }
    if TOOLS.iter().any(|(read_only, spec)| {
        !seen.contains(spec.method)
            || !method_policy::is_mcp_method(spec.method)
            || (spec.method != "swarm.tools.search"
                && method_policy::read_only(spec.method) != Some(*read_only))
    }) {
        return Err(CatalogError::IncompleteRegistry);
    }
    Ok(())
}

fn metadata_input_contract_matches(metadata: &ToolMetadata, spec: &ToolSpec) -> bool {
    if metadata.required_input_fields.is_empty() {
        return true;
    }
    let declared: BTreeSet<_> = metadata.required_input_fields.iter().copied().collect();
    let required: BTreeSet<_> = spec.required.iter().copied().collect();
    declared.len() == metadata.required_input_fields.len()
        && declared == required
        && declared
            .iter()
            .all(|field| spec.fields.iter().any(|candidate| candidate.name == *field))
}

pub fn metadata_for(method: &str) -> Option<&'static ToolMetadata> {
    TOOL_METADATA
        .iter()
        .find(|metadata| metadata.method == method)
}

pub fn update_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

pub fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub const fn profile_name(profile: McpToolProfile) -> &'static str {
    match profile {
        McpToolProfile::Observer => "observer",
        McpToolProfile::Reviewer => "reviewer",
        McpToolProfile::Participant => "participant",
        McpToolProfile::AssignedReviewer => "assigned_reviewer",
        McpToolProfile::Manager => "manager",
        McpToolProfile::Gm => "gm",
        McpToolProfile::Full => "full",
    }
}

const fn core_role_name(role: CoreRole) -> &'static str {
    match role {
        CoreRole::Participant => "participant",
        CoreRole::Observer => "observer",
        CoreRole::LegacyReviewer => "legacy-reviewer",
        CoreRole::AssignedReviewer => "assigned-reviewer",
        CoreRole::Manager => "manager",
        CoreRole::FullCompatibility => "full-compatibility",
    }
}

const fn core_surface_name(role: CoreRole) -> &'static str {
    match role {
        CoreRole::Participant => "participant-core",
        CoreRole::Observer => "observer-core",
        CoreRole::LegacyReviewer => "reviewer-core",
        CoreRole::AssignedReviewer => "assigned-reviewer-core",
        CoreRole::Manager => "manager-core",
        CoreRole::FullCompatibility => "full-legacy",
    }
}

fn canonical_method(value: &str) -> Option<&'static str> {
    TOOL_METADATA
        .iter()
        .find(|metadata| metadata.method == value || tool_name(metadata.method) == value)
        .map(|metadata| metadata.method)
}

/// Canonical restricted Participant tool schemas. This is a comparison
/// contract only and does not grant method authority.
pub fn participant_core_tool_contracts() -> ContractResult<Vec<Value>> {
    validate_registry_metadata().map_err(|error| Error::invalid(error.to_string()))?;
    role_core(CoreRole::Participant)
        .methods
        .iter()
        .map(|method| {
            let (read_only, spec) = find_tool_spec(method)
                .ok_or_else(|| Error::invalid("Participant core has no canonical tool contract"))?;
            if !exposes_method(McpToolProfile::Participant, method) {
                return Err(Error::invalid(
                    "Participant core tool is outside its frontend exposure profile",
                ));
            }
            Ok(json!({
                "method": method,
                "name": tool_name(method),
                "input_schema": input_schema(
                    spec,
                    *read_only,
                    mutation_requires_caller_request_id(
                        McpToolProfile::Participant,
                        spec.method,
                        *read_only,
                    ),
                )
                .as_ref(),
            }))
        })
        .collect()
}

/// Describe a configured presentation surface without claiming native loading.
pub fn launch_profile_surface(
    profile: McpToolProfile,
    surface_name: &str,
    groups: &[String],
    manual_tools: &[String],
) -> ContractResult<Value> {
    let surface = Surface::configured(profile, Some(surface_name), groups, manual_tools)
        .map_err(|error| Error::invalid(error.to_string()))?;
    let suggested = surface.suggested();
    let core = role_core(surface.core);
    Ok(json!({
        "surface_id": suggested.id,
        "core_role": suggested.core_role,
        "core": suggested.core,
        "core_methods": core.methods,
        "deferred_groups": suggested.deferred_groups,
        "manual_tools": suggested.exact_manual_methods,
    }))
}
