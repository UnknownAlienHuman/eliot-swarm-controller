//! Static MCP catalogue metadata and session-local discovery projections.
//!
//! Profile filters limit this frontend's presentation and dispatch. The
//! authenticated Store remains authoritative for each application method.
//! This module adds presentation only: role cores, bounded
//! `tools/list` pages, and a catalog-only search result that never dispatches
//! an application method.

use super::{TOOLS, ToolSpec, input_schema, output_schema, profiles, tool_from_spec, tool_name};
use crate::config::McpToolProfile;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rmcp::model::Tool;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt};

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
    const fn as_str(self) -> &'static str {
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
    const fn as_str(self) -> &'static str {
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
        FULL_AUDIENCE,
        ManualOnly,
        "Capture an exact Git commit as a fixed-source candidate.",
        "Use only when an explicit task workflow requires capturing a named commit.",
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
    entry!(
        "attempt.bind_producer",
        TaskManagement,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Bind an attempt to an exact producer binding generation.",
        "Use only when explicitly binding the producer identity for an attempt.",
        &["attempt", "producer", "binding", "generation"],
        &[
            "attempt_id",
            "expected_revision",
            "binding_id",
            "binding_generation"
        ],
        "One revision-checked producer binding."
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
    entry!(
        "gm.handover",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Transfer the local manager lease through the guarded handover path.",
        "Use only for an explicit operator handover to a named eligible client.",
        &["manager", "handover", "lease", "operator"],
        &["target_client_id", "expected_revision"],
        "One guarded manager handover."
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
        "Validate one exact launch request under current Manager or local Operator authority.",
        "Use before admitting a launch to review the exact Task revision, route, profiles, budget, stop conditions, and requested configuration.",
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
    const fn as_str(self) -> &'static str {
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
            if !profiles::exposes_method(profile, method)
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

    fn includes(&self, metadata: &ToolMetadata) -> bool {
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

#[derive(Debug)]
pub struct CatalogPage {
    pub tools: Vec<Tool>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationDisposition {
    AvailableOnServerSurface,
    ReconnectSurfaceRequired,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogMatch {
    pub group: &'static str,
    pub method: &'static str,
    pub audiences: Vec<&'static str>,
    pub load_tier: &'static str,
    pub title: &'static str,
    pub purpose: &'static str,
    pub when_to_use: &'static str,
    pub search_terms: &'static [&'static str],
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
    pub activation: ActivationDisposition,
    pub suggested_surface: SuggestedSurface,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchCoverage {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogSearchResult {
    pub catalog_revision: String,
    pub surface_revision: String,
    pub matches: Vec<CatalogMatch>,
    pub coverage: SearchCoverage,
    pub authorization_revision: String,
    pub authorization_basis: &'static str,
    /// Server-side schema visibility does not establish native client/model loading.
    pub server_surface_visibility: &'static str,
    pub harness_acknowledgement: &'static str,
    /// Only role-specific missing canonical handlers are reported. These are
    /// capability gaps, not synthetic tool definitions.
    pub gaps: Vec<&'static str>,
}

#[derive(Debug, Clone, Copy)]
pub struct SearchRequest<'a> {
    pub query: &'a str,
    pub purpose: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub exact_method: Option<&'a str>,
    pub loaded_catalog_revision: Option<&'a str>,
    pub max_results: usize,
}

struct AuthorizedView {
    entries: Vec<&'static ToolMetadata>,
    catalog_digest: [u8; 32],
}

struct SurfaceView {
    authorized: AuthorizedView,
    visible: Vec<&'static ToolMetadata>,
    surface_digest: [u8; 32],
}

#[derive(Debug)]
struct CursorClaims {
    catalog_digest: [u8; 32],
    surface_digest: [u8; 32],
    offset: usize,
}

/// Check the one-to-one relationship between the executable method registry
/// and its descriptive metadata before producing any discovery result.
pub fn validate_registry_metadata() -> Result<(), CatalogError> {
    if TOOL_METADATA.len() != TOOLS.len() {
        return Err(CatalogError::IncompleteRegistry);
    }
    let mut seen = BTreeSet::new();
    for metadata in TOOL_METADATA {
        if !seen.insert(metadata.method) || find_spec(metadata.method).is_none() {
            return Err(CatalogError::IncompleteRegistry);
        }
    }
    if TOOLS.iter().any(|(_, spec)| !seen.contains(spec.method)) {
        return Err(CatalogError::IncompleteRegistry);
    }
    Ok(())
}

pub fn metadata_for(method: &str) -> Option<&'static ToolMetadata> {
    TOOL_METADATA
        .iter()
        .find(|metadata| metadata.method == method)
}

/// Build `tools/list` from a pre-authorized view. The callback is evaluated
/// only after the fixed hard profile and before surface filtering, ordering,
/// revision calculation, cursor validation, or paging.
pub fn list_tools_page<F>(
    profile: McpToolProfile,
    surface: &Surface,
    cursor: Option<&str>,
    authorization: AuthorizationRevision<'_>,
    mut object_authorized: F,
) -> Result<CatalogPage, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    let view = surface_view(
        profile,
        surface,
        authorization,
        None,
        &mut object_authorized,
    )?;
    let claims = cursor.map(decode_cursor).transpose()?;
    let offset = if let Some(claims) = claims {
        if claims.catalog_digest != view.authorized.catalog_digest
            || claims.surface_digest != view.surface_digest
        {
            return Err(CatalogError::StaleCursor);
        }
        claims.offset
    } else {
        0
    };
    if offset >= view.visible.len() && offset != 0 {
        return Err(CatalogError::CursorOutOfRange);
    }

    let mut tools = Vec::new();
    let mut serialized_bytes = 0usize;
    // Leave room for the MCP result envelope, cursor and JSON punctuation.
    const ENVELOPE_RESERVE_BYTES: usize = 512;
    let mut end = offset;
    while end < view.visible.len() && tools.len() < MAX_PAGE_ITEMS {
        let metadata = view.visible[end];
        let (read_only, spec) =
            find_spec(metadata.method).ok_or(CatalogError::IncompleteRegistry)?;
        let tool = tool_from_spec(*read_only, spec, profile != McpToolProfile::Full);
        let byte_len = serde_json::to_vec(&tool)
            .map_err(|error| CatalogError::Serialization(error.to_string()))?
            .len();
        if tools.is_empty() && byte_len.saturating_add(ENVELOPE_RESERVE_BYTES) > MAX_PAGE_JSON_BYTES
        {
            return Err(CatalogError::ToolSchemaTooLarge);
        }
        if serialized_bytes
            .saturating_add(byte_len)
            .saturating_add(ENVELOPE_RESERVE_BYTES)
            > MAX_PAGE_JSON_BYTES
        {
            break;
        }
        serialized_bytes += byte_len;
        tools.push(tool);
        end += 1;
    }
    let next_cursor = (end < view.visible.len()).then(|| {
        encode_cursor(&CursorClaims {
            catalog_digest: view.authorized.catalog_digest,
            surface_digest: view.surface_digest,
            offset: end,
        })
    });

    Ok(CatalogPage { tools, next_cursor })
}

/// Search only the authorized registry. The result contains metadata and a
/// truthful activation disposition; it carries no schema, arguments, or
/// execution endpoint. This is catalog lookup, never a generic RPC tool.
pub fn search_catalog<F>(
    profile: McpToolProfile,
    surface: &Surface,
    request: SearchRequest<'_>,
    authorization: AuthorizationRevision<'_>,
    mut object_authorized: F,
) -> Result<CatalogSearchResult, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    let view = surface_view(
        profile,
        surface,
        authorization,
        request.task_id,
        &mut object_authorized,
    )?;
    let catalog_revision = hex_digest(&view.authorized.catalog_digest);
    if request
        .loaded_catalog_revision
        .is_some_and(|loaded| loaded != catalog_revision)
    {
        return Err(CatalogError::StaleCatalogRevision);
    }

    let query = normalize(request.query);
    let purpose = request.purpose.map(normalize).unwrap_or_default();
    let exact = request.exact_method.map(normalize);
    if query.is_empty() && exact.is_none() {
        return Ok(CatalogSearchResult {
            catalog_revision,
            surface_revision: hex_digest(&view.surface_digest),
            matches: Vec::new(),
            coverage: SearchCoverage::Complete,
            authorization_revision: authorization.value.to_owned(),
            authorization_basis: authorization.basis.as_str(),
            server_surface_visibility: "server_surface_only",
            harness_acknowledgement: "unknown",
            gaps: role_core(surface.core).gaps.to_vec(),
        });
    }

    // Authorization has already been applied while constructing `view`.
    // Search and result limiting never inspect denied metadata.
    let mut ranked = Vec::new();
    for metadata in &view.authorized.entries {
        if metadata.load_tier == LoadTier::ManualOnly && exact.is_none() {
            continue;
        }
        if let Some(exact) = exact.as_deref() {
            let exact_method = normalize(metadata.method);
            let exact_tool = normalize(&tool_name(metadata.method));
            if exact != exact_method && exact != exact_tool {
                continue;
            }
        }
        let score = search_score(metadata, &query, &purpose, exact.is_some());
        if score == 0 {
            continue;
        }
        ranked.push((score, metadata));
    }
    ranked.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.group.as_str().cmp(right.group.as_str()))
            .then_with(|| left.load_tier.as_str().cmp(right.load_tier.as_str()))
            .then_with(|| left.method.cmp(right.method))
    });

    let limit = request.max_results.clamp(1, MAX_SEARCH_RESULTS);
    let coverage = if ranked.len() > limit {
        SearchCoverage::Partial
    } else {
        SearchCoverage::Complete
    };
    let matches = ranked
        .into_iter()
        .take(limit)
        .map(|(_, metadata)| {
            let is_loaded = surface.includes(metadata);
            let mut suggested = surface.clone();
            if !is_loaded {
                if metadata.load_tier == LoadTier::ManualOnly {
                    suggested = suggested.with_exact_manual_method(metadata.method);
                } else {
                    suggested = suggested.with_group(metadata.group);
                }
            }
            CatalogMatch {
                group: metadata.group.as_str(),
                method: metadata.method,
                audiences: metadata
                    .audiences
                    .iter()
                    .map(|audience| audience.as_str())
                    .collect(),
                load_tier: metadata.load_tier.as_str(),
                title: metadata.purpose,
                purpose: metadata.purpose,
                when_to_use: metadata.when_to_use,
                search_terms: metadata.search_terms,
                required_context: metadata.required_context,
                result_policy: metadata.result_policy,
                activation: if is_loaded {
                    ActivationDisposition::AvailableOnServerSurface
                } else {
                    // This server does not know whether its harness can safely
                    // refresh the current session. Require an explicit next
                    // surface/reconnect; never claim auto-activation.
                    ActivationDisposition::ReconnectSurfaceRequired
                },
                suggested_surface: suggested.suggested(),
            }
        })
        .collect();

    Ok(CatalogSearchResult {
        catalog_revision,
        surface_revision: hex_digest(&view.surface_digest),
        matches,
        coverage,
        authorization_revision: authorization.value.to_owned(),
        authorization_basis: authorization.basis.as_str(),
        server_surface_visibility: "server_surface_only",
        harness_acknowledgement: "unknown",
        gaps: role_core(surface.core).gaps.to_vec(),
    })
}

fn surface_view<F>(
    profile: McpToolProfile,
    surface: &Surface,
    authorization: AuthorizationRevision<'_>,
    search_context: Option<&str>,
    object_authorized: &mut F,
) -> Result<SurfaceView, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    validate_registry_metadata()?;
    let mut entries: Vec<_> = TOOL_METADATA
        .iter()
        .filter(|metadata| profiles::exposes_method(profile, metadata.method))
        .filter(|metadata| object_authorized(metadata.method, search_context))
        .collect();
    entries.sort_by(|left, right| metadata_order(left, right));
    let catalog_digest = digest_entries(
        profile,
        authorization.value,
        search_context.unwrap_or_default(),
        &entries,
    )?;
    let visible: Vec<_> = entries
        .iter()
        .copied()
        .filter(|metadata| surface.includes(metadata))
        .collect();
    let surface_digest = digest_surface(surface, catalog_digest, profile, &visible)?;
    Ok(SurfaceView {
        authorized: AuthorizedView {
            entries,
            catalog_digest,
        },
        visible,
        surface_digest,
    })
}

fn metadata_order(left: &ToolMetadata, right: &ToolMetadata) -> std::cmp::Ordering {
    left.group
        .as_str()
        .cmp(right.group.as_str())
        .then_with(|| left.load_tier.cmp(&right.load_tier))
        .then_with(|| left.method.cmp(right.method))
}

fn digest_entries(
    profile: McpToolProfile,
    authorization_revision: &str,
    authorization_context: &str,
    entries: &[&'static ToolMetadata],
) -> Result<[u8; 32], CatalogError> {
    let mut hasher = Sha256::new();
    update_field(&mut hasher, b"eliot-mcp-authorized-catalog-v1");
    update_field(&mut hasher, profile_name(profile).as_bytes());
    update_field(&mut hasher, authorization_revision.as_bytes());
    update_field(&mut hasher, authorization_context.as_bytes());
    for metadata in entries {
        digest_metadata(&mut hasher, metadata);
        digest_tool_schema(&mut hasher, profile, metadata.method)?;
    }
    Ok(hasher.finalize().into())
}

fn digest_surface(
    surface: &Surface,
    catalog_digest: [u8; 32],
    profile: McpToolProfile,
    visible: &[&'static ToolMetadata],
) -> Result<[u8; 32], CatalogError> {
    let mut hasher = Sha256::new();
    update_field(&mut hasher, b"eliot-mcp-surface-view-v1");
    update_field(&mut hasher, &catalog_digest);
    update_field(&mut hasher, surface.id().as_bytes());
    update_field(&mut hasher, profile_name(profile).as_bytes());
    for metadata in visible {
        digest_metadata(&mut hasher, metadata);
        digest_tool_schema(&mut hasher, profile, metadata.method)?;
    }
    Ok(hasher.finalize().into())
}

fn digest_metadata(hasher: &mut Sha256, metadata: &ToolMetadata) {
    update_field(hasher, metadata.method.as_bytes());
    update_field(hasher, metadata.group.as_str().as_bytes());
    update_field(hasher, metadata.load_tier.as_str().as_bytes());
    update_field(hasher, metadata.purpose.as_bytes());
    update_field(hasher, metadata.when_to_use.as_bytes());
    update_field(hasher, metadata.result_policy.as_bytes());
    for audience in metadata.audiences {
        update_field(hasher, audience.as_str().as_bytes());
    }
    for term in metadata.search_terms {
        update_field(hasher, term.as_bytes());
    }
    for context in metadata.required_context {
        update_field(hasher, context.as_bytes());
    }
}

fn digest_tool_schema(
    hasher: &mut Sha256,
    profile: McpToolProfile,
    method: &str,
) -> Result<(), CatalogError> {
    let (read_only, spec) = find_spec(method).ok_or(CatalogError::IncompleteRegistry)?;
    update_field(hasher, tool_name(method).as_bytes());
    update_field(hasher, spec.description.as_bytes());
    let schema = input_schema(spec, *read_only, profile != McpToolProfile::Full);
    let bytes = serde_json::to_vec(schema.as_ref())
        .map_err(|error| CatalogError::Serialization(error.to_string()))?;
    update_field(hasher, &bytes);
    if let Some(output) = output_schema(method) {
        let bytes = serde_json::to_vec(output.as_ref())
            .map_err(|error| CatalogError::Serialization(error.to_string()))?;
        update_field(hasher, &bytes);
    } else {
        update_field(hasher, &[]);
    }
    Ok(())
}

fn search_score(metadata: &ToolMetadata, query: &str, purpose: &str, exact_selected: bool) -> u16 {
    if exact_selected {
        return 10_000;
    }
    let method = normalize(metadata.method);
    let tool = normalize(&tool_name(metadata.method));
    let searchable = normalize(&format!(
        "{} {} {} {} {} {}",
        metadata.group.as_str(),
        metadata.purpose,
        metadata.when_to_use,
        metadata.search_terms.join(" "),
        metadata.required_context.join(" "),
        metadata.result_policy
    ));
    let query_terms = terms(query);
    if query_terms.is_empty() {
        return 0;
    }
    let mut score = 0u16;
    let mut hits = 0usize;
    for term in query_terms {
        if method == term || tool == term {
            score = score.saturating_add(80);
            hits += 1;
        } else if method.split_whitespace().any(|word| word == term)
            || tool.split_whitespace().any(|word| word == term)
        {
            score = score.saturating_add(40);
            hits += 1;
        } else if searchable.split_whitespace().any(|word| word == term) {
            score = score.saturating_add(12);
            hits += 1;
        }
    }
    if hits == 0 || hits.saturating_mul(2) < terms(query).len() {
        return 0;
    }
    if !purpose.is_empty() {
        let purpose_terms = terms(purpose);
        if purpose_terms
            .iter()
            .all(|term| searchable.split_whitespace().any(|word| word == term))
            || purpose_group_matches(metadata.group, &purpose_terms)
        {
            score = score.saturating_add(20);
        } else {
            return 0;
        }
    }
    score
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn terms(value: &str) -> Vec<String> {
    const STOP_WORDS: &[&str] = &[
        "a", "an", "and", "are", "for", "find", "get", "how", "i", "in", "is", "me", "my", "of",
        "please", "search", "show", "the", "to", "use", "when", "with",
    ];
    normalize(value)
        .split_whitespace()
        .filter(|term| !STOP_WORDS.contains(term))
        .map(str::to_owned)
        .collect()
}

fn purpose_group_matches(group: ToolGroup, terms: &[String]) -> bool {
    let purpose = terms.join(" ");
    match purpose.as_str() {
        "implementation" | "build" | "implement" => {
            matches!(group, ToolGroup::TaskManagement | ToolGroup::RuntimeControl)
        }
        "review" | "audit" => matches!(group, ToolGroup::Review | ToolGroup::AssignmentRead),
        "coordination" | "collaboration" => {
            matches!(
                group,
                ToolGroup::ParticipantCoordination | ToolGroup::MailboxRaw
            )
        }
        "monitoring" | "diagnostics" | "debugging" => {
            matches!(group, ToolGroup::Monitoring | ToolGroup::RuntimeRecovery)
        }
        "administration" | "operator" => {
            matches!(
                group,
                ToolGroup::Administration | ToolGroup::AcceptanceEffects
            )
        }
        "acceptance" | "publishing" => group == ToolGroup::AcceptanceEffects,
        "github" | "github intake" | "work pool" => group == ToolGroup::GitHub,
        "goal" | "goals" | "reminder" | "tracking" => group == ToolGroup::Goals,
        "hook" | "hooks" | "git hooks" => group == ToolGroup::Hooks,
        "script" | "scripts" => group == ToolGroup::Scripts,
        _ => false,
    }
}

fn find_spec(method: &str) -> Option<&'static (bool, ToolSpec)> {
    TOOLS.iter().find(|(_, spec)| spec.method == method)
}

fn encode_cursor(claims: &CursorClaims) -> String {
    let mut bytes = Vec::with_capacity(73);
    bytes.push(1);
    bytes.extend_from_slice(&claims.catalog_digest);
    bytes.extend_from_slice(&claims.surface_digest);
    bytes.extend_from_slice(&(claims.offset as u64).to_be_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_cursor(cursor: &str) -> Result<CursorClaims, CatalogError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| CatalogError::InvalidCursor)?;
    if bytes.len() != 73 || bytes[0] != 1 {
        return Err(CatalogError::InvalidCursor);
    }
    let catalog_digest = bytes[1..33]
        .try_into()
        .map_err(|_| CatalogError::InvalidCursor)?;
    let surface_digest = bytes[33..65]
        .try_into()
        .map_err(|_| CatalogError::InvalidCursor)?;
    let offset = u64::from_be_bytes(
        bytes[65..73]
            .try_into()
            .map_err(|_| CatalogError::InvalidCursor)?,
    );
    let offset = usize::try_from(offset).map_err(|_| CatalogError::InvalidCursor)?;
    Ok(CursorClaims {
        catalog_digest,
        surface_digest,
        offset,
    })
}

fn update_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

const fn profile_name(profile: McpToolProfile) -> &'static str {
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

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
