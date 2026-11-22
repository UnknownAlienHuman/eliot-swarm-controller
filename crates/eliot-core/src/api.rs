//! Shared public API DTOs for events, views, reads and mutations. Authorization,
//! admission and effect handling remain in the Router and Store.

mod agent_lifecycle;
pub use agent_lifecycle::*;
mod agent_requests;
mod agent_views;
mod child_run;
mod cleanup;
pub use child_run::*;
pub use cleanup::*;
mod config_views;
mod coordination;
mod digests;
pub mod discovery;
mod git;
mod goals;
mod handoff;
pub mod harness;
mod hierarchy;
pub mod launch_queue;
mod maintenance;
mod monitor_views;
mod mutation_validation;
mod mutations;
mod native_pages;
pub mod native_profile;
mod native_requests;
mod notify;
mod observe;
mod organization;
mod overview;
pub use launch_queue::*;
mod provider;
mod read_validation;
mod reads;
mod resolutions;
mod rule_edit;
mod rules;
mod script_edit;
mod scripts;
mod string_list;
mod swarm_usage;
mod switch;
mod timers;
mod usage;
mod waits;
mod wakes;
mod workspaces;

pub(crate) use string_list::deserialize_string_list;

pub use agent_requests::{
    AgentEditsRequest, AgentGetRequest, AgentGitRequest, AgentListRequest, AgentPatchDigestRequest,
    AgentProcessesRequest, AgentStatsRequest, DaemonPanicTestRequest, DaemonStatusRequest,
    DiagnoseRequest, ProjectDiscoverRequest, decode_recent_cursor, encode_recent_cursor,
};
pub use agent_views::{
    AGENT_LIST_STATES, AGENT_NOTICE_MESSAGE_CHARS, AGENT_PREVIEW_CHARS, AgentConfigView,
    AgentEditsResponse, AgentGetResponse, AgentGitResponse, AgentListResponse, AgentListRow,
    AgentNativeNotice, AgentPatchDigestResponse, AgentProcessesResponse, AgentProviderHold,
    AgentStateTotal, AgentStatsResponse, AgentSummary, AttentionStepReadback,
    AutomaticCleanupPolicy, AutomaticCleanupSweep, AutomaticCleanupView, CloudMcpCustody,
    CloudMcpHealth, CloudMcpStatus, CrashMarker, DaemonBuildIdentity, DaemonPanicTestResponse,
    DaemonStatusResponse, DiagnoseResponse, DiagnoseStep, DrainReport, GitCredentialHelper,
    INSTRUCTION_FILE_PATHS, InstructionDeliveryView, InstructionFileStatus, InstructionFileView,
    InstructionReadStatus, InstructionSourceStatus, MAX_DIAGNOSE_ATTENTION_STEPS,
    MAX_DIAGNOSE_MESSAGE_CHARS, MAX_DIAGNOSE_STEPS, MAX_DIAGNOSE_SUMMARY_CHARS,
    MAX_PATCH_DIGEST_OUTSIDE_CLAIMS, MAX_PATCH_DIGEST_PATH_BYTES, NativeNotice, NativeNoticeKind,
    OwnedProcessKind, OwnedProcessSummary, PROCESS_CURSOR_MAX_BYTES,
    PROJECT_POLICY_OPT_OUT_REASON_MAX_BYTES, PROJECT_POLICY_OPTOUTS_MAX, PatchDigestFile,
    ProcessPageCursor, ProjectCheckoutView, ProjectDiscoverResponse, ProjectGitHubCliView,
    ProjectGitPublicationView, ProjectGitView, ProjectPolicyOptOutView, ProjectPolicyUsageView,
    ProjectRegistrationView, RetainedUnknownSummary, RetentionView, RunningTurnSummary,
    SourceOwner, SourceState, StatusBlocker, UnsettledCommandSummary, WorkerTaskHealth,
    WorkerTaskStatus, WorkspaceCustodyView, activity_state_name, agent_state_name,
    agent_status_line, diagnosis_reason, process_state_name, repository_name_from_url,
    sanitize_project_policy_opt_out_reason, stats_coverage_name,
    valid_project_policy_opt_out_reason,
};
pub use config_views::{
    CONFIG_REVISION_HEX_LEN, MAX_LISTED_PROFILES, MAX_PROFILES_SOURCE_BYTES, ProfileListRequest,
    ProfileListResponse, ProfileSummary, REDACTION_MARKER, RoutingPreviewRequest,
    RoutingPreviewResponse, RuleListEntry, RuleListRequest, RuleListResponse, SettingsClientSource,
    SettingsHistoryEntry, SettingsHistoryStatus, SettingsReadIssue, SettingsReadRequest,
    SettingsReadResponse, SettingsSaveRequest, SettingsSaveResponse, SettingsSection,
};
pub use coordination::{
    ClaimConflict, ClaimListRequest, ClaimListResponse, ClaimReleaseRequest, ClaimReleaseResponse,
    ClaimSetRequest, ClaimSetResponse, ClaimView, DEFAULT_CLAIM_TTL_SECS,
    DEFAULT_MAX_UNREAD_BYTES_PER_AGENT, DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT,
    MAIL_NOTICE_COOLDOWN_MS, MAX_CLAIM_GLOB_BYTES, MAX_CLAIM_NOTE_BYTES, MAX_CLAIM_TTL_SECS,
    MAX_CLAIMS_PER_AGENT, MAX_MAIL_ACK_IDS, MAX_MAIL_INBOX_ROWS, MAX_MAIL_RECIPIENTS,
    MAX_MAIL_TEXT_BYTES, MAX_MAIL_THREAD_BYTES, MAX_VAR_KEY_BYTES, MAX_VAR_NOTE_BYTES,
    MAX_VAR_SCOPE_BYTES, MAX_VAR_VALUE_BYTES, MAX_WHO_PATHS, MAX_WHO_WORKERS, MailAckRequest,
    MailAckResponse, MailDefinitionArgs, MailDelivery, MailDeliverySummary, MailInboxRequest,
    MailInboxResponse, MailLimits, MailMode, MailReadRequest, MailSendRequest, MailSendResponse,
    SWARM_MAIL_SENDER, VarDefinitionArgs, VarGetRequest, VarGetResponse, VarScope, VarSetRequest,
    VarSetResponse, WhoRequest, WhoResponse, WhoSharedWorkspace, WhoWorker, advisory_claim_matches,
    claim_glob_matches, claim_glob_matches_normalized, claim_globs_overlap,
    claim_globs_overlap_normalized, normalize_claim_glob, parse_var_scope,
};
pub use digests::{
    DIGEST_THREAD_PREFIX, SwarmDigestRequest, SwarmDigestResponse, digest_mode_completed,
    digest_text, digest_thread, fair_excerpt_cap,
};
pub use git::{
    DEFAULT_GIT_FETCH_TIMEOUT_MS, DEFAULT_GIT_PUBLISH_TIMEOUT_MS, FrozenGitFetch, FrozenGitPublish,
    GitFetchDefinitionArgs, GitFetchOutcome, GitPublishDefinitionArgs, GitPublishOutcome,
    GitPublishTrigger, MAX_GIT_PUBLISH_SUBJECT_BYTES, ProjectCheckPassed, is_git_oid,
    validate_git_publish_subject,
};
pub use goals::{
    AssignmentGoal, CHECK_SPAWN_RETRYABLE, DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalClearRequest,
    GoalContinuationStage, GoalCostLimit, GoalCounters, GoalDefinition, GoalEscalation,
    GoalGetRequest, GoalMarkerState, GoalMode, GoalPauseRequest, GoalResumeRequest, GoalRoundEvent,
    GoalRoundEventOutcome, GoalRoundMarker, GoalRoundOutcome, GoalRoundRequest, GoalRoundResponse,
    GoalScopeBase, GoalScopeStart, GoalSetRequest, GoalStatus, GoalView, MAX_GOAL_CADENCE_MS,
    MAX_GOAL_CHECK_ARG_BYTES, MAX_GOAL_CHECK_ARGS, MAX_GOAL_CHECK_TIMEOUT_MS, MAX_GOAL_LIMIT,
    MAX_GOAL_OBSERVATION_BYTES, MAX_GOAL_SCOPE_BASE_PATHS, MAX_GOAL_SCOPE_GLOBS,
    MIN_GOAL_CADENCE_MS, MIN_GOAL_CHECK_TIMEOUT_MS,
};
pub use handoff::{
    CommandCancelRequest, CommandCancelResponse, HandoffRequest, HandoffResponse,
    MAX_HANDOFF_SENDS, SendTransfer,
};
pub use harness::{
    CLAUDE_EXTERNAL_ROUTE_MARKERS, ConfigureSupport, HARNESS_QUOTA_SUPPORTED,
    HARNESS_QUOTA_UNKNOWN, HARNESS_QUOTA_UNSUPPORTED, HARNESS_READ_TTL_MS, HarnessCandidate,
    HarnessEntry, HarnessListRequest, HarnessListResponse, HarnessModelEntry, HarnessModelsRequest,
    HarnessModelsResponse, HarnessQuotaRequest, HarnessQuotaResponse, MAX_HARNESS_CREDENTIAL_BYTES,
    MAX_HARNESS_PROFILE_BYTES, MAX_HARNESS_QUOTA_NOTE_BYTES, MAX_HARNESS_SCOPE_BYTES,
    MUSE_REASONING_EFFORT_TIERS, ModelsCatalogEntry, ModelsCatalogFailure, ModelsCatalogRequest,
    ModelsCatalogResponse, SUPPORTED_HARNESSES, catalog_cursor, catalog_matches, catalog_provider,
    configure_uses_native_session, is_supported_harness,
};
pub use hierarchy::{
    DEFAULT_MAX_CHILDREN_PER_AGENT, DEFAULT_MAX_DEPTH, DEFAULT_MAX_SELECTOR_TARGETS,
    DEFAULT_MAX_TREE_AGENTS, HierarchyLimits, ReparentRequest, ReparentResponse, ReparentedAgent,
    parse_tree_selector, selector_child_id,
};
pub use maintenance::{
    MaintenancePurgeLegacyAttentionRequest, MaintenancePurgeLegacyAttentionResponse,
    MaintenanceRunRequest, MaintenanceRunResponse,
};
pub use monitor_views::{
    FilesystemInvalidationState, FilesystemInvalidationView, GitObservation, GitView,
    LivenessBasis, LivenessView, MonitoringSourceState, ObservationData, ObservationView,
    ProcessStatsView, StatsCoverage, WorkspaceObservation, WorkspaceView,
};
pub use mutations::{
    CommandSubmitRequest, CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse,
    ShutdownMode,
};
pub use native_pages::{
    DiscoverCoverage, DiscoverPage, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationPage, ObservationQuery,
};
pub use native_requests::{AgentDiscoverRequest, AgentHistoryRequest};
pub use notify::{EmitDefinitionArgs, NotifyDefinitionArgs};
pub use observe::{
    DEFAULT_OBSERVE_WINDOW_SECS, DoctorHarnessEntry, HarnessDoctorRequest, HarnessDoctorResponse,
    HeavyAgentTotals, HeavyEpisodeView, HeavySummaryView, MAX_DOCTOR_HARNESSES, MAX_HEAVY_EPISODES,
    MAX_OBSERVE_WINDOW_SECS, ObserveHeavyRequest, ObserveHeavyResponse, SessionGrowthView,
    WorkSummaryView, is_read_only_or_audit_profile,
};
pub use organization::{
    ChatAttachRequest, ChatDetachRequest, ChatOrganizationMutationResponse,
    ChatOrganizationRequest, ChatOrganizationResponse, ChatTagRequest, MAX_ORG_AGENTS,
    MAX_ORG_MEMBERS_PER_GROUP, MAX_ORG_MEMBERS_TOTAL, MAX_ORG_TAG_BYTES, MAX_ORG_TAGS,
    ORGANIZATION_CHANGED_KIND, OrgGroupView, OrgLinkView, OrgMemberView, OrgRowChange,
    OrganizationChangedEvent,
};
pub use overview::{
    FleetTotals, HarnessCount, LifecycleTotals, NativeChildView, OverviewCoverage,
    OverviewManagerView, OverviewWorkerView, ProjectOverviewEntry, ProjectOverviewRequest,
    ProjectOverviewResponse, ProjectScopeLimits, ProjectTotals, ScopeLimits, ScopeUsage,
};
pub use provider::{
    ProviderClearHoldRequest, ProviderClearHoldResponse, ProviderCorrectiveEvidence,
    ProviderHoldsRequest, ProviderHoldsResponse,
};
pub use reads::{
    AgentResultRequest, AgentResultResponse, CommandChildrenRequest, CommandChildrenResponse,
    CommandGetRequest, GenerationBoundary, GenerationReason, ResultCursor, RuleGetRequest,
    RuleStateView, TurnGetRequest,
};
pub use resolutions::{
    CommandResolution, CommandResolveRequest, CommandResolveResponse, SettleStoppedRequest,
    SettleStoppedResponse, StoppedCommandSettlement, TurnResolveRequest, TurnResolveResponse,
    TurnResolveStatus,
};
pub use rule_edit::{
    MAX_RULE_FILE_BYTES, MAX_RULE_PATH_BYTES, RULE_REVISION_HEX_LEN, RuleDeleteRequest,
    RuleDeleteResponse, RuleReadRequest, RuleReadResponse, RuleSaveRequest, RuleSaveResponse,
};
pub use rules::{
    MAX_RULE_REJECTION_MESSAGE_BYTES, MAX_RULE_SHOW_STDERR_BYTES, MAX_RULE_SHOW_STDERR_LINES,
    RuleCatchUpRequest, RuleLastOutcome, RuleLastRefusal, RuleLimit, RuleLimitScope, RuleShowView,
};
pub use script_edit::{
    MAX_SCRIPT_EDIT_BYTES, MAX_SCRIPT_ENTRIES, MAX_SCRIPT_PAGE_BYTES, MAX_SCRIPT_PATH_BYTES,
    ScriptEntryView, ScriptListRequest, ScriptListResponse, ScriptReadRequest, ScriptReadResponse,
    ScriptRootView, ScriptSaveRequest, ScriptSaveResponse,
};
pub use scripts::{
    FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun, RunDefinitionArgs,
    ScriptActionRecord, ScriptCommandContext, ScriptCustody, ScriptDirectExit, ScriptInputContext,
    ScriptInterpreter, ScriptProcessResult, ScriptRuleContext, ScriptRunApplied,
    ScriptTimerContext, ScriptTimerInvocation, bound_script_outcome_stderr,
};
pub use swarm_usage::{
    MAX_SWARM_MCP_TURNS, SWARM_MCP_EVENT_ORIGIN, SWARM_MCP_EVENT_TOOL, SwarmMcpToolEventData,
    SwarmMcpToolStatus, SwarmMcpTurnUsage,
};
pub use switch::{
    SWITCH_BLANK_SCOPE_ARG, SWITCH_PROJECT_REVISION_ARG, SwitchAgent, SwitchLaunch, SwitchRequest,
    SwitchResponse,
};
pub use timers::{CatchUp, TimerGetRequest, TimerRunRequest, TimerSchedule, TimerStateView};
pub use usage::{
    FirstCallInput, MAX_FIRST_CALL_TURNS, PROFILE_FIRST_CALL_WINDOW, ProfileFirstCallInput,
    SessionCostTotal, SessionCumulativeTotals, SessionFacts, SessionTokenSubtotal,
    TurnCounterMissing, TurnCounterTotals, TurnExpenditure, TurnScopeTotals, UsageScopeView,
    UsageSummaryRequest, UsageSummaryResponse,
};
pub use waits::{
    MAX_SWARM_WAIT_MS, MAX_SWARM_WAIT_TARGETS, MAX_WAIT_FACTS_FRAME_BYTES,
    MAX_WAIT_FINAL_TEXT_BYTES, MAX_WAIT_FRAME_BYTES, SwarmWaitRequest, SwarmWaitResponse,
    WaitEntryResult, WaitTurnRef, truncate_bytes,
};
pub use wakes::{
    AgentWakeCondition, AgentWakeRequest, AgentWakeResponse, DEFAULT_AGENT_WAKE_TIMEOUT_SECS,
    MAX_AGENT_WAKE_ID_BYTES, MAX_AGENT_WAKE_PATH_BYTES, MAX_AGENT_WAKE_PATH_COMPONENTS,
    MAX_AGENT_WAKE_RUN_ID_BYTES, MAX_AGENT_WAKE_TIMEOUT_SECS, MAX_AGENT_WAKE_TURN_ID_BYTES,
};
pub use workspaces::{ManagedWorkspaceView, WorkspaceListRequest, WorkspaceListResponse};

use crate::{Error, Event, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const MAX_WAIT_MS: u64 = 30_000;

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsFilter {
    #[serde(default, deserialize_with = "filter_values")]
    #[schemars(
        length(max = 1000),
        description = "At most 1000 nonempty strings, without trimming; duplicates count toward the bound."
    )]
    pub agents: Vec<String>,
    #[serde(default, deserialize_with = "filter_values")]
    #[schemars(
        length(max = 1000),
        description = "At most 1000 nonempty strings, without trimming; duplicates count toward the bound."
    )]
    pub kinds: Vec<String>,
    #[serde(default, deserialize_with = "filter_values")]
    #[schemars(
        length(max = 1000),
        description = "At most 1000 nonempty strings, without trimming; duplicates count toward the bound."
    )]
    pub sources: Vec<String>,
}

impl EventsFilter {
    pub fn validate(&self) -> Result<()> {
        for values in [&self.agents, &self.kinds, &self.sources] {
            validate_filter_values(values)?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsBoundsRequest {
    #[serde(default, deserialize_with = "object_filter")]
    pub filter: EventsFilter,
}

impl EventsBoundsRequest {
    pub fn validate(&self) -> Result<()> {
        self.filter.validate()
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventsBoundsResponse {
    pub pruned_through: i64,
    pub hi: i64,
}

impl EventsBoundsResponse {
    pub fn validate(&self) -> Result<()> {
        if self.pruned_through < 0 || self.hi < self.pruned_through {
            return Err(Error::new("unknown", "event bounds response is invalid"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsReadRequest {
    #[serde(default, deserialize_with = "event_after")]
    #[schemars(range(min = 0))]
    pub after: i64,
    #[serde(default = "default_limit", deserialize_with = "event_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
    #[serde(default, deserialize_with = "object_filter")]
    pub filter: EventsFilter,
}

impl Default for EventsReadRequest {
    fn default() -> Self {
        Self {
            after: 0,
            limit: DEFAULT_LIMIT,
            filter: EventsFilter::default(),
        }
    }
}

impl EventsReadRequest {
    pub fn validate(&self) -> Result<()> {
        validate_query(self.after, self.limit, &self.filter)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSubscribeRequest {
    #[serde(default, deserialize_with = "event_after")]
    #[schemars(range(min = 0))]
    pub after: i64,
    #[serde(default = "default_limit", deserialize_with = "event_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
    #[serde(default, deserialize_with = "object_filter")]
    pub filter: EventsFilter,
    #[serde(default, deserialize_with = "event_wait_ms")]
    #[schemars(
        range(min = 0),
        description = "Unsigned milliseconds; values above 30000 are accepted and clamped to 30000."
    )]
    pub wait_ms: u64,
}

impl Default for EventsSubscribeRequest {
    fn default() -> Self {
        Self {
            after: 0,
            limit: DEFAULT_LIMIT,
            filter: EventsFilter::default(),
            wait_ms: 0,
        }
    }
}

impl EventsSubscribeRequest {
    /// Large wait values are valid and are clamped when decoded from the wire.
    pub fn validate(&self) -> Result<()> {
        validate_query(self.after, self.limit, &self.filter)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsReadResponse {
    pub events: Vec<Event>,
    pub next: i64,
    pub high_water: i64,
    pub truncated: bool,
}

impl EventsReadResponse {
    pub fn validate(&self, after: i64) -> Result<()> {
        validate_event_response(after, self.next, self.high_water, &self.events)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsSubscribeResponse {
    pub events: Vec<Event>,
    pub next: i64,
    pub high_water: i64,
    pub truncated: bool,
    pub lagged: bool,
}

impl EventsSubscribeResponse {
    pub fn validate(&self, after: i64) -> Result<()> {
        validate_event_response(after, self.next, self.high_water, &self.events)
    }
}

fn validate_event_response(after: i64, next: i64, high_water: i64, events: &[Event]) -> Result<()> {
    if after < 0 || next < after || high_water < next {
        return Err(Error::new("unknown", "event response is invalid"));
    }
    let mut previous = after;
    for event in events {
        if event.seq <= previous || event.seq > next {
            return Err(Error::new("unknown", "event response is invalid"));
        }
        previous = event.seq;
    }
    Ok(())
}

fn default_limit() -> usize {
    DEFAULT_LIMIT
}

fn validate_query(after: i64, limit: usize, filter: &EventsFilter) -> Result<()> {
    if after < 0 {
        return Err(Error::new("invalid", "after must be a nonnegative integer"));
    }
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    filter.validate()
}

fn validate_filter_values(values: &[String]) -> Result<()> {
    if values.len() > MAX_LIMIT || values.iter().any(String::is_empty) {
        return Err(Error::new(
            "invalid",
            "event filter lists require at most 1000 non-empty strings",
        ));
    }
    Ok(())
}

fn event_after<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let after = i64::deserialize(deserializer)?;
    if after < 0 {
        return Err(serde::de::Error::custom(
            "after must be a nonnegative integer",
        ));
    }
    Ok(after)
}

fn event_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<usize, D::Error> {
    let limit = usize::deserialize(deserializer)?;
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(serde::de::Error::custom("limit must be between 1 and 1000"));
    }
    Ok(limit)
}

fn filter_values<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    let values = Vec::<String>::deserialize(deserializer)?;
    validate_filter_values(&values).map_err(serde::de::Error::custom)?;
    Ok(values)
}

fn object_filter<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<EventsFilter, D::Error> {
    // Require a map while retaining the original deserializer's nested field
    // path. Materializing a Value and decoding again loses that path.
    struct FilterObject;
    impl<'de> serde::de::Visitor<'de> for FilterObject {
        type Value = EventsFilter;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an event filter object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            EventsFilter::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(FilterObject)
}

fn event_wait_ms<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u64, D::Error> {
    Ok(u64::deserialize(deserializer)?.min(MAX_WAIT_MS))
}

pub mod inference;

mod project_assignment;
pub use project_assignment::*;
mod project_transfer;
pub use project_transfer::*;
