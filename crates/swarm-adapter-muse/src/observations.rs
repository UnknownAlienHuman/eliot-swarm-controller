//! Pure, bounded Muse MSP 1.3 native-observation projection, ported from
//! bridge.13. No process, filesystem, clock, or transport calls occur here.
//! Task admission/completion, native scheduling, approval decisions, and
//! transcript/tool-output streams remain outside this module. item/delta is
//! returned by borrowed handoff for a separately authorized stream path.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use swarm_contracts::native_usage::{
    NativeUsageSnapshot, ObservedUsageValue, UsageBucket, UsageCompleteness, UsageEvidence,
    UsageFreshness, UsageService, UsageWindow,
};

const MAX_ID: usize = 512;
const MAX_SCOPE: usize = 2048;
const MAX_USAGE_CONNECTION_ID: usize = 256;
const MAX_CHILDREN: usize = 2_000;
const MAX_PENDING: usize = 4_096;
const MAX_TURNS: usize = 256;
const MAX_GAPS: usize = 32;
const MAX_SEEN: usize = 512;
const MAX_STATE_BYTES: usize = 700_000;
const MAX_CURSOR: usize = 1024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionFence {
    pub(crate) binding_id: String,
    pub(crate) binding_generation: i64,
    pub(crate) native_root_id: String,
    pub(crate) native_scope_key: String,
    pub(crate) bridge_boot_id: String,
}

impl ProjectionFence {
    fn valid(&self) -> bool {
        valid_text(&self.binding_id, MAX_ID)
            && self.binding_generation > 0
            && valid_text(&self.native_root_id, MAX_ID)
            && valid_text(&self.native_scope_key, MAX_SCOPE)
            && valid_text(&self.bridge_boot_id, MAX_ID)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Execution {
    NotStarted,
    RecoveryRequired,
    Idle,
    Running,
    Completed,
    Failed,
    Interrupted,
    Unknown,
}

impl Execution {
    fn native(value: Option<&str>) -> Self {
        match value {
            Some("not_started") => Self::NotStarted,
            Some("recovery_required") => Self::RecoveryRequired,
            Some("idle") => Self::Idle,
            Some("running") => Self::Running,
            Some("completed") => Self::Completed,
            Some("failed") => Self::Failed,
            Some("interrupted") => Self::Interrupted,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FamilyCompleteness {
    Partial,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeStatus {
    Queued,
    Running,
    Idle,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    NotLoaded,
    Unknown,
}

impl NativeStatus {
    fn native(value: Option<&str>) -> Self {
        match value {
            Some("queued") => Self::Queued,
            Some("running") => Self::Running,
            Some("idle") => Self::Idle,
            Some("completed") => Self::Completed,
            Some("failed") => Self::Failed,
            Some("cancelled" | "canceled") => Self::Cancelled,
            Some("interrupted") => Self::Interrupted,
            Some("notLoaded" | "not_loaded") => Self::NotLoaded,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlStatus {
    Running,
    StopRequested,
    Stopped,
    Completed,
    Failed,
    Unknown,
}

impl ControlStatus {
    fn native(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "stopRequested" | "stop_requested" => Self::StopRequested,
            "stopped" => Self::Stopped,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Freshness {
    Fresh,
    Stale,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FreshnessReason {
    CheckpointRestored,
    CheckpointSnapshotUnverified,
    NativeReadNotYetVerified,
    NativeEventAfterRead,
    ParentChildEventAfterRead,
    NativeEventDuringRefresh,
    StableNativeRead,
    StableSessionListAfterSubscription,
    ChildReadFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ChildKind {
    #[serde(rename = "subagent")]
    Subagent,
    #[serde(rename = "reminderChild")]
    ReminderChild,
}

impl ChildKind {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value {
            Some("subagent") => Some(Self::Subagent),
            Some("reminderChild") => Some(Self::ReminderChild),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TurnEvent {
    #[serde(rename = "turn/started")]
    Started,
    #[serde(rename = "turn/completed")]
    Completed,
    #[serde(rename = "turn/unqueued")]
    Unqueued,
}

impl TurnEvent {
    fn parse(method: &str) -> Option<Self> {
        match method {
            "turn/started" => Some(Self::Started),
            "turn/completed" => Some(Self::Completed),
            "turn/unqueued" => Some(Self::Unqueued),
            _ => None,
        }
    }
    fn method(self) -> &'static str {
        match self {
            Self::Started => "turn/started",
            Self::Completed => "turn/completed",
            Self::Unqueued => "turn/unqueued",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Terminal {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Unknown,
}

impl Terminal {
    fn native(value: Option<&str>, event: TurnEvent) -> Option<Self> {
        if event == TurnEvent::Unqueued {
            return Some(Self::Cancelled);
        }
        match value {
            Some("completed" | "success") => Some(Self::Completed),
            Some("failed" | "error") => Some(Self::Failed),
            Some("cancelled" | "canceled") => Some(Self::Cancelled),
            Some("interrupted") => Some(Self::Interrupted),
            Some(_) => Some(Self::Unknown),
            None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Effort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    Ultra,
}

impl Effort {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value {
            Some("none") => Some(Self::None),
            Some("minimal") => Some(Self::Minimal),
            Some("low") => Some(Self::Low),
            Some("medium") => Some(Self::Medium),
            Some("high") => Some(Self::High),
            Some("xhigh") => Some(Self::Xhigh),
            Some("max") => Some(Self::Max),
            Some("ultra") => Some(Self::Ultra),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingKind {
    Approval,
    UserInput,
}

impl PendingKind {
    fn method(self) -> &'static str {
        match self {
            Self::Approval => "approval/request",
            Self::UserInput => "userInput/request",
        }
    }
    fn id_field(self) -> &'static str {
        match self {
            Self::Approval => "approvalId",
            Self::UserInput => "userInputId",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingEvent {
    Requested,
    Updated,
    Inventory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingGap {
    ApprovalUpdateWithoutRequest,
    InventoryMalformed,
    InventoryForeignSession,
    InventoryDuplicate,
    InventoryRaced,
    InventoryLimit,
    InventoryRestoredStale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GapReason {
    MalformedEvent,
    ForeignSession,
    SourceRevisionGap,
    SourceRevisionConflict,
    SourceRevisionStale,
    MissingProvenance,
    UnknownNativeMethod,
    ChildCycle,
    ChildIdentityConflict,
    ChildLimit,
    ChildReadRace,
    ChildReadFailed,
    PendingApprovalUpdateWithoutRequest,
    PendingInventoryMalformed,
    PendingInventoryForeignSession,
    PendingInventoryDuplicate,
    PendingInventoryRace,
    PendingInventoryLimit,
    NativeViewGap,
    OpaqueViewBracketIncomplete,
    UsageConnectionUnavailable,
    ProjectionHistoryTruncated,
    CheckpointRestored,
    PendingInventoryRestoredStale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadMethod {
    SessionRead,
    SessionList,
}

impl ReadMethod {
    fn reason(self) -> FreshnessReason {
        match self {
            Self::SessionRead => FreshnessReason::StableNativeRead,
            Self::SessionList => FreshnessReason::StableSessionListAfterSubscription,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChildReadFailure {
    Unavailable,
    Unsupported,
    InvalidSnapshot,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    id: String,
    sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceRange {
    first: Cursor,
    last: Cursor,
    stream: StreamIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamIdentity {
    id: String,
    kind: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionSnapshot {
    session_id: String,
    status: NativeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeErrorKind {
    CommandRejected,
    MethodNotFound,
    InvalidParams,
    Overloaded,
    Cancelled,
    Internal,
    Unknown,
}

impl NativeErrorKind {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value {
            Some("commandRejected") => Some(Self::CommandRejected),
            Some("methodNotFound") => Some(Self::MethodNotFound),
            Some("invalidParams") => Some(Self::InvalidParams),
            Some("overloaded") => Some(Self::Overloaded),
            Some("cancelled") => Some(Self::Cancelled),
            Some("internal") => Some(Self::Internal),
            Some(_) => Some(Self::Unknown),
            None => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Turn {
    session_id: String,
    turn_id: String,
    event_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<String>,
    event: TurnEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    terminal: Option<Terminal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    view_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_range: Option<SourceRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_kind: Option<NativeErrorKind>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultSource {
    #[serde(rename = "parentSessionId")]
    parent_session_id: String,
    #[serde(rename = "itemId")]
    item_id: String,
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none", rename = "viewCursor")]
    view_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Child {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "parentSessionId")]
    parent_session_id: String,
    #[serde(rename = "itemId")]
    item_id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "subagentId")]
    subagent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "parentTurnId")]
    parent_turn_id: Option<String>,
    status: NativeStatus,
    #[serde(skip_serializing_if = "Option::is_none", rename = "controlStatus")]
    control_status: Option<ControlStatus>,
    revision: u64,
    kind: ChildKind,
    snapshot_freshness: Freshness,
    snapshot_freshness_reason: FreshnessReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<SessionSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_turn: Option<Turn>,
    result_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_source: Option<ResultSource>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    kind: PendingKind,
    session_id: String,
    request_id: String,
    last_event: PendingEvent,
}

impl Pending {
    fn view(&self) -> Value {
        let mut params = Map::new();
        params.insert("sessionId".into(), json!(self.session_id));
        params.insert(self.kind.id_field().into(), json!(self.request_id));
        json!({"method":self.kind.method(),"params":params,"request_details_state":"unknown","last_event":self.last_event})
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct Gap {
    status: GapStatus,
    reason: GapReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
    cursors_complete: bool,
    projection_state: GapState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GapStatus {
    Unfilled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GapState {
    Incomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct PendingGapRecord {
    reason: PendingGap,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventProvenance {
    revision: u64,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    view_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_range: Option<SourceRange>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Model {
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ViewHealth {
    Healthy,
    Behind,
    Unavailable,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ViewHealthObservation {
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    status: ViewHealth,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRevision {
    session_id: String,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventFingerprint {
    revision: u64,
    method: String,
    payload_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_revision: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    execution: Execution,
    family_completeness: FamilyCompleteness,
    gaps: u64,
    native_events_seen: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_completed_turn: Option<Turn>,
    root_snapshot_freshness: Freshness,
    root_snapshot_freshness_reason: FreshnessReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionSnapshot>,
    observed_children: Vec<Child>,
    pending_requests: Vec<Pending>,
    turns: Vec<Turn>,
    projection_gaps: Vec<Gap>,
    pending_request_gaps: Vec<PendingGapRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gap_fill: Option<Gap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_usage: Option<NativeUsageSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<Model>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<Effort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    view_health: Option<ViewHealthObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_event: Option<EventProvenance>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservationProjector {
    fence: ProjectionFence,
    state: State,
    session_revisions: Vec<SessionRevision>,
    seen_events: VecDeque<EventFingerprint>,
    last_event_revision: Option<u64>,
    usage_connection_id: Option<String>,
    usage_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionError {
    InvalidFence,
    InvalidCheckpoint,
    Serialization,
    ObservationTooLarge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReductionStatus {
    Applied,
    Duplicate,
    Raced,
    Incomplete(GapReason),
    ForeignFence,
    Ignored,
}

pub(crate) struct EventReduction<'a> {
    pub(crate) projector: ObservationProjector,
    pub(crate) status: ReductionStatus,
    /// Borrowed original data for separately authorized streaming; never stored in Observation.
    pub(crate) stream_handoff: Option<&'a Value>,
}

pub(crate) struct ProjectionReduction {
    pub(crate) projector: ObservationProjector,
    pub(crate) status: ReductionStatus,
}

pub(crate) struct NativeEventInput<'a> {
    pub(crate) fence: &'a ProjectionFence,
    pub(crate) event_revision: u64,
    pub(crate) at_ms: i64,
    pub(crate) event: &'a Value,
}

pub(crate) struct PendingInventoryInput<'a> {
    pub(crate) fence: &'a ProjectionFence,
    pub(crate) session_id: &'a str,
    pub(crate) expected_session_revision: u64,
    pub(crate) inventory: &'a Value,
}

pub(crate) struct RootSnapshotInput<'a> {
    pub(crate) fence: &'a ProjectionFence,
    pub(crate) expected_session_revision: u64,
    pub(crate) at_ms: i64,
    pub(crate) read_method: ReadMethod,
    pub(crate) snapshot: &'a Value,
}

pub(crate) struct ChildSnapshotInput<'a> {
    pub(crate) fence: &'a ProjectionFence,
    pub(crate) child_session_id: &'a str,
    pub(crate) parent_session_id: &'a str,
    pub(crate) item_id: &'a str,
    pub(crate) parent_turn_id: Option<&'a str>,
    pub(crate) item_revision: u64,
    pub(crate) expected_session_revision: u64,
    pub(crate) at_ms: i64,
    pub(crate) read_method: ReadMethod,
    pub(crate) snapshot: Option<&'a Value>,
    pub(crate) failure: Option<ChildReadFailure>,
}
impl ObservationProjector {
    pub(crate) fn new(fence: ProjectionFence) -> Result<Self, ProjectionError> {
        if !fence.valid() {
            return Err(ProjectionError::InvalidFence);
        }
        Ok(Self {
            state: State {
                execution: Execution::NotStarted,
                family_completeness: FamilyCompleteness::Partial,
                gaps: 0,
                native_events_seen: 0,
                active_turn_id: None,
                last_completed_turn: None,
                root_snapshot_freshness: Freshness::Unknown,
                root_snapshot_freshness_reason: FreshnessReason::NativeReadNotYetVerified,
                session: None,
                observed_children: Vec::new(),
                pending_requests: Vec::new(),
                turns: Vec::new(),
                projection_gaps: Vec::new(),
                pending_request_gaps: Vec::new(),
                gap_fill: None,
                native_usage: None,
                model: None,
                reasoning_effort: None,
                view_health: None,
                native_event: None,
            },
            fence,
            session_revisions: Vec::new(),
            seen_events: VecDeque::new(),
            last_event_revision: None,
            usage_connection_id: None,
            usage_generation: 0,
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<Value, ProjectionError> {
        serde_json::to_value(self).map_err(|_| ProjectionError::Serialization)
    }

    pub(crate) fn restore(fence: ProjectionFence, value: Value) -> Result<Self, ProjectionError> {
        if !fence.valid() {
            return Err(ProjectionError::InvalidFence);
        }
        let mut saved: Self =
            serde_json::from_value(value).map_err(|_| ProjectionError::InvalidCheckpoint)?;
        if saved.fence != fence || !saved.valid_checkpoint() {
            return Err(ProjectionError::InvalidCheckpoint);
        }
        saved.fence = fence;
        saved.mark_restored();
        Ok(saved)
    }

    pub(crate) fn observation(&self) -> Result<Value, ProjectionError> {
        let mut value = self.full_observation()?;
        if json_bytes(&value)? <= MAX_STATE_BYTES {
            return Ok(value);
        }
        let digest = pending_digest(&self.state.pending_requests)?;
        value = json!({
            "execution":self.state.execution, "family_completeness":"partial",
            "gaps":self.state.gaps.saturating_add(1),
            "native_root_id":self.fence.native_root_id, "native_scope_key":self.fence.native_scope_key,
            "boot_id":self.fence.bridge_boot_id, "native_events_seen":self.state.native_events_seen,
            "observation_projection_state":"partial", "pending_requests":[],
            "pending_requests_state":"partial", "pending_requests_count":self.state.pending_requests.len(),
            "pending_requests_omitted_count":self.state.pending_requests.len(),
            "pending_requests_identity_sha256":digest,
        });
        for request in self.state.pending_requests.iter().rev().take(96).rev() {
            let mut rows = value["pending_requests"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            rows.push(request.view());
            value["pending_requests"] = Value::Array(rows);
            if json_bytes(&value)? > MAX_STATE_BYTES {
                value["pending_requests"].as_array_mut().unwrap().pop();
            }
        }
        let visible = value["pending_requests"].as_array().map_or(0, Vec::len);
        value["pending_requests_omitted_count"] =
            json!(self.state.pending_requests.len().saturating_sub(visible));
        if visible == self.state.pending_requests.len() {
            for key in [
                "pending_requests_state",
                "pending_requests_count",
                "pending_requests_omitted_count",
                "pending_requests_identity_sha256",
            ] {
                value.as_object_mut().unwrap().remove(key);
            }
        }
        for child in self.state.observed_children.iter().rev().take(32).rev() {
            bounded_push(
                &mut value,
                "observed_children",
                serde_json::to_value(child).map_err(|_| ProjectionError::Serialization)?,
            )?;
        }
        for turn in self.state.turns.iter().rev().take(64).rev() {
            bounded_push(
                &mut value,
                "turns",
                serde_json::to_value(turn).map_err(|_| ProjectionError::Serialization)?,
            )?;
        }
        let mut omitted = Vec::new();
        if value["observed_children"].as_array().map_or(0, Vec::len)
            < self.state.observed_children.len()
        {
            omitted.push("observed_children");
        }
        if value["turns"].as_array().map_or(0, Vec::len) < self.state.turns.len() {
            omitted.push("turns");
        }
        if !omitted.is_empty() {
            value["observation_projection_omitted_fields"] = json!(omitted);
        }
        for (key, optional) in [
            (
                "native_usage",
                self.state
                    .native_usage
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| ProjectionError::Serialization)?,
            ),
            (
                "session",
                self.state
                    .session
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| ProjectionError::Serialization)?,
            ),
            (
                "gap_fill",
                self.state
                    .gap_fill
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| ProjectionError::Serialization)?,
            ),
        ] {
            if let Some(item) = optional {
                bounded_set(&mut value, key, item)?;
            }
        }
        if self.state.gap_fill.is_some() && value.get("gap_fill").is_none() {
            let session_id = self
                .state
                .gap_fill
                .as_ref()
                .and_then(|gap| gap.session_id.clone());
            value["gap_fill"] = json!({
                "status":"unfilled",
                "reason":"opaque_gap_bracket_did_not_fit_complete_observation_frame",
                "session_id":session_id,
                "after":null,"next":null,"cursors_complete":false,
                "projection_state":"incomplete"
            });
        }
        if json_bytes(&value)? > MAX_STATE_BYTES {
            return Err(ProjectionError::ObservationTooLarge);
        }
        Ok(value)
    }

    pub(crate) fn apply_event<'a>(&self, input: NativeEventInput<'a>) -> EventReduction<'a> {
        if input.fence != &self.fence {
            return EventReduction {
                projector: self.clone(),
                status: ReductionStatus::ForeignFence,
                stream_handoff: None,
            };
        }
        let Some(method) = input.event.get("method").and_then(Value::as_str) else {
            return self.event_gap(None, GapReason::MalformedEvent, None);
        };
        let Some(params) = input.event.get("params").filter(|value| value.is_object()) else {
            return self.event_gap(None, GapReason::MalformedEvent, None);
        };
        let kind = EventKind::parse(method);
        let session = identity(params.get("sessionId"));
        if kind == Some(EventKind::ItemDelta) {
            if !session.as_deref().is_some_and(|id| self.known_session(id)) {
                return self.event_gap(session, GapReason::ForeignSession, None);
            }
            let mut next = self.clone();
            let mut disposition = ReductionStatus::Applied;
            if input.event_revision == 0 || input.event_revision > MAX_SAFE_INTEGER {
                next.gap(GapReason::MissingProvenance, session, None, None);
                return EventReduction {
                    projector: next,
                    status: ReductionStatus::Incomplete(GapReason::MissingProvenance),
                    stream_handoff: None,
                };
            }
            if let Some(last) = self.last_event_revision {
                if input.event_revision <= last {
                    let fingerprint = fingerprint(input.event_revision, method, params);
                    if self.seen_events.contains(&fingerprint) {
                        return EventReduction {
                            projector: self.clone(),
                            status: ReductionStatus::Duplicate,
                            stream_handoff: Some(input.event),
                        };
                    }
                    let reason = if input.event_revision == last {
                        GapReason::SourceRevisionConflict
                    } else {
                        GapReason::SourceRevisionStale
                    };
                    next.gap(reason, session, None, None);
                    return EventReduction {
                        projector: next,
                        status: ReductionStatus::Incomplete(reason),
                        stream_handoff: Some(input.event),
                    };
                }
                if input.event_revision > last.saturating_add(1) {
                    next.gap(GapReason::SourceRevisionGap, session.clone(), None, None);
                    disposition = ReductionStatus::Incomplete(GapReason::SourceRevisionGap);
                }
            }
            next.last_event_revision = Some(input.event_revision);
            next.remember(fingerprint(input.event_revision, method, params));
            return EventReduction {
                projector: next,
                status: disposition,
                stream_handoff: Some(input.event),
            };
        }
        let Some(kind) = kind else {
            let mut next = self.clone();
            next.state.native_events_seen = next.state.native_events_seen.saturating_add(1);
            next.gap(GapReason::UnknownNativeMethod, session, None, None);
            return EventReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::UnknownNativeMethod),
                stream_handoff: None,
            };
        };
        if input.event_revision == 0 || input.event_revision > MAX_SAFE_INTEGER || input.at_ms <= 0
        {
            return self.event_gap(session, GapReason::MissingProvenance, None);
        }
        let key = fingerprint(input.event_revision, method, params);
        if self.seen_events.contains(&key) {
            return EventReduction {
                projector: self.clone(),
                status: ReductionStatus::Duplicate,
                stream_handoff: None,
            };
        }
        let mut next = self.clone();
        let mut disposition = ReductionStatus::Applied;
        if let Some(last) = self.last_event_revision {
            if input.event_revision <= last {
                let reason = if input.event_revision == last {
                    GapReason::SourceRevisionConflict
                } else {
                    GapReason::SourceRevisionStale
                };
                next.gap(reason, session, None, None);
                return EventReduction {
                    projector: next,
                    status: ReductionStatus::Incomplete(reason),
                    stream_handoff: None,
                };
            }
            if input.event_revision > last.saturating_add(1) {
                next.gap(GapReason::SourceRevisionGap, session.clone(), None, None);
                disposition = ReductionStatus::Incomplete(GapReason::SourceRevisionGap);
            }
        }
        next.last_event_revision = Some(input.event_revision);
        next.remember(key);
        if !matches!(
            kind,
            EventKind::ApprovalRequest | EventKind::UserInputRequest
        ) {
            next.state.native_events_seen = next.state.native_events_seen.saturating_add(1);
        }
        let session = match session {
            Some(value) => value,
            None if kind == EventKind::UsageChanged => self.fence.native_root_id.clone(),
            None => {
                next.gap(GapReason::MalformedEvent, None, None, None);
                return EventReduction {
                    projector: next,
                    status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
                    stream_handoff: None,
                };
            }
        };
        if !next.known_session(&session) {
            next.gap(GapReason::ForeignSession, Some(session), None, None);
            return EventReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ForeignSession),
                stream_handoff: None,
            };
        }
        next.session_changed(&session);
        let command_id = identity(params.get("commandId"))
            .or_else(|| identity(params.pointer("/item/commandId")));
        let turn_id =
            identity(params.get("turnId")).or_else(|| identity(params.pointer("/item/turnId")));
        let item_id = identity(params.pointer("/item/itemId"));
        let item_revision = params.pointer("/item/revision").and_then(safe_u64);
        let view_cursor = cursor(params.get("viewCursor"));
        let source_range = parse_source_range(params.get("sourceRange"));
        if params.get("sourceRange").is_some() && source_range.is_none() {
            next.gap(
                GapReason::MissingProvenance,
                Some(session.clone()),
                None,
                None,
            );
            disposition = ReductionStatus::Incomplete(GapReason::MissingProvenance);
        }
        next.state.native_event = Some(EventProvenance {
            revision: input.event_revision,
            method: kind.method().into(),
            session_id: Some(session.clone()),
            command_id: command_id.clone(),
            turn_id: turn_id.clone(),
            item_id,
            item_revision,
            view_cursor: view_cursor.clone(),
            source_range: source_range.clone(),
        });
        match kind {
            EventKind::TurnStarted | EventKind::TurnCompleted | EventKind::TurnUnqueued => {
                let Some(turn_id) = turn_id else {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    return EventReduction {
                        projector: next,
                        status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
                        stream_handoff: None,
                    };
                };
                let Some(event) = TurnEvent::parse(method) else {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    return EventReduction {
                        projector: next,
                        status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
                        stream_handoff: None,
                    };
                };
                next.turn(
                    &session,
                    &turn_id,
                    input.event_revision,
                    command_id,
                    event,
                    Terminal::native(params.get("terminal").and_then(Value::as_str), event),
                    view_cursor,
                    source_range,
                    NativeErrorKind::parse(params.pointer("/error/kind").and_then(Value::as_str)),
                );
            }
            EventKind::SessionStatusChanged if session == self.fence.native_root_id => {
                next.state.execution =
                    Execution::native(params.get("status").and_then(Value::as_str));
                if next.state.execution == Execution::Unknown {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    disposition = ReductionStatus::Incomplete(GapReason::MalformedEvent);
                }
            }
            EventKind::ItemStarted | EventKind::ItemUpdated | EventKind::ItemCompleted => {
                if let Some(item) = params.get("item") {
                    if ChildKind::parse(item.get("kind").and_then(Value::as_str)).is_some() {
                        next.child_item(&session, item, view_cursor, turn_id.clone());
                    }
                }
            }
            EventKind::SessionReasoningEffortChanged if session == self.fence.native_root_id => {
                match Effort::parse(params.get("reasoningEffort").and_then(Value::as_str)) {
                    Some(value) => next.state.reasoning_effort = Some(value),
                    None => {
                        next.gap(GapReason::MalformedEvent, Some(session), None, None);
                        disposition = ReductionStatus::Incomplete(GapReason::MalformedEvent);
                    }
                }
            }
            EventKind::SessionModelChanged if session == self.fence.native_root_id => {
                let model_id = identity(params.get("modelId"));
                let provider_id = identity(params.get("providerId"));
                if (params.get("modelId").is_some() && model_id.is_none())
                    || (params.get("providerId").is_some() && provider_id.is_none())
                {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    disposition = ReductionStatus::Incomplete(GapReason::MalformedEvent);
                } else {
                    next.state.model = Some(Model {
                        model_id,
                        provider_id,
                    });
                }
            }
            EventKind::UsageChanged => {
                if !next.usage_changed(params, input.at_ms) {
                    next.gap(
                        GapReason::UsageConnectionUnavailable,
                        Some(session),
                        None,
                        None,
                    );
                    disposition =
                        ReductionStatus::Incomplete(GapReason::UsageConnectionUnavailable);
                }
            }
            EventKind::ApprovalRequest
            | EventKind::ApprovalRequested
            | EventKind::ApprovalUpdated
            | EventKind::UserInputRequest
            | EventKind::UserInputRequested => {
                let pending_kind = if matches!(
                    kind,
                    EventKind::UserInputRequest | EventKind::UserInputRequested
                ) {
                    PendingKind::UserInput
                } else {
                    PendingKind::Approval
                };
                let Some(request_id) = identity(params.get(pending_kind.id_field())) else {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    return EventReduction {
                        projector: next,
                        status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
                        stream_handoff: None,
                    };
                };
                if kind == EventKind::ApprovalUpdated
                    && !next.pending_has(&session, pending_kind, &request_id)
                {
                    next.pending_gap(
                        PendingGap::ApprovalUpdateWithoutRequest,
                        Some(session.clone()),
                        Some(request_id),
                    );
                    next.gap(
                        GapReason::PendingApprovalUpdateWithoutRequest,
                        Some(session),
                        None,
                        None,
                    );
                    return EventReduction {
                        projector: next,
                        status: ReductionStatus::Incomplete(
                            GapReason::PendingApprovalUpdateWithoutRequest,
                        ),
                        stream_handoff: None,
                    };
                }
                next.pending_upsert(
                    session,
                    pending_kind,
                    request_id,
                    if kind == EventKind::ApprovalUpdated {
                        PendingEvent::Updated
                    } else {
                        PendingEvent::Requested
                    },
                );
            }
            EventKind::ApprovalResolved | EventKind::UserInputSettled => {
                let pending_kind = if kind == EventKind::ApprovalResolved {
                    PendingKind::Approval
                } else {
                    PendingKind::UserInput
                };
                if let Some(id) = identity(params.get(pending_kind.id_field())) {
                    next.pending_remove(&session, pending_kind, &id);
                } else {
                    next.gap(GapReason::MalformedEvent, Some(session), None, None);
                    disposition = ReductionStatus::Incomplete(GapReason::MalformedEvent);
                }
            }
            EventKind::ViewGap | EventKind::SessionViewHealthChanged => {
                let text = params
                    .get("status")
                    .or_else(|| params.get("health"))
                    .and_then(Value::as_str);
                let health = match text {
                    Some("healthy") => ViewHealth::Healthy,
                    Some("behind" | "gap") => ViewHealth::Behind,
                    Some("unavailable") => ViewHealth::Unavailable,
                    _ => ViewHealth::Unknown,
                };
                next.state.view_health = Some(ViewHealthObservation {
                    method: kind.method().into(),
                    session_id: Some(session.clone()),
                    status: health,
                });
                let reason = if kind == EventKind::ViewGap {
                    let gap = gap_bracket(&session, params);
                    let reason = gap.reason;
                    next.state.gap_fill = Some(gap);
                    reason
                } else {
                    GapReason::NativeViewGap
                };
                next.gap(reason, Some(session), None, None);
                disposition = ReductionStatus::Incomplete(reason);
            }
            EventKind::SessionGoalChanged
            | EventKind::SessionStatusChanged
            | EventKind::SessionReasoningEffortChanged
            | EventKind::SessionModelChanged
            | EventKind::ItemDelta => {}
        }
        EventReduction {
            projector: next,
            status: disposition,
            stream_handoff: None,
        }
    }
    pub(crate) fn replace_pending(&self, input: PendingInventoryInput<'_>) -> ProjectionReduction {
        if input.fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        let mut next = self.clone();
        if !valid_text(input.session_id, MAX_ID) || !next.known_session(input.session_id) {
            next.pending_gap(
                PendingGap::InventoryForeignSession,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryForeignSession,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryForeignSession),
            };
        }
        if next.session_revision(input.session_id) != input.expected_session_revision {
            next.pending_gap(
                PendingGap::InventoryRaced,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryRace,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Raced,
            };
        }
        let Some(object) = input.inventory.as_object() else {
            next.pending_gap(
                PendingGap::InventoryMalformed,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryMalformed,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryMalformed),
            };
        };
        let Some(approvals) = object.get("approvals").and_then(Value::as_array) else {
            next.pending_gap(
                PendingGap::InventoryMalformed,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryMalformed,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryMalformed),
            };
        };
        let Some(user_inputs) = object.get("userInputs").and_then(Value::as_array) else {
            next.pending_gap(
                PendingGap::InventoryMalformed,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryMalformed,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryMalformed),
            };
        };
        if object.len() != 2 {
            next.pending_gap(
                PendingGap::InventoryMalformed,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryMalformed,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryMalformed),
            };
        }
        let mut rows = Vec::new();
        let error = parse_inventory(
            input.session_id,
            PendingKind::Approval,
            approvals,
            &mut rows,
        )
        .or_else(|| {
            parse_inventory(
                input.session_id,
                PendingKind::UserInput,
                user_inputs,
                &mut rows,
            )
        });
        if let Some(reason) = error {
            next.pending_gap(reason, Some(input.session_id.into()), None);
            let gap = pending_gap_reason(reason);
            next.gap(gap, Some(input.session_id.into()), None, None);
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(gap),
            };
        }
        let retained = next
            .state
            .pending_requests
            .iter()
            .filter(|row| row.session_id != input.session_id)
            .count();
        if retained.saturating_add(rows.len()) > MAX_PENDING {
            next.pending_gap(
                PendingGap::InventoryLimit,
                Some(input.session_id.into()),
                None,
            );
            next.gap(
                GapReason::PendingInventoryLimit,
                Some(input.session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::PendingInventoryLimit),
            };
        }
        next.state
            .pending_requests
            .retain(|row| row.session_id != input.session_id);
        next.state.pending_requests.extend(rows);
        ProjectionReduction {
            projector: next,
            status: ReductionStatus::Applied,
        }
    }

    pub(crate) fn apply_root_snapshot(&self, input: RootSnapshotInput<'_>) -> ProjectionReduction {
        if input.fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        let mut next = self.clone();
        let Some(snapshot) = session_snapshot(input.snapshot, &self.fence.native_root_id) else {
            next.gap(
                GapReason::MalformedEvent,
                Some(self.fence.native_root_id.clone()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
            };
        };
        if input.at_ms <= 0 {
            next.gap(
                GapReason::MalformedEvent,
                Some(self.fence.native_root_id.clone()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
            };
        }
        if next.session_revision(&self.fence.native_root_id) != input.expected_session_revision {
            next.state.root_snapshot_freshness = if next.state.session.is_some() {
                Freshness::Stale
            } else {
                Freshness::Unknown
            };
            next.state.root_snapshot_freshness_reason = FreshnessReason::NativeEventDuringRefresh;
            next.gap(
                GapReason::SourceRevisionConflict,
                Some(self.fence.native_root_id.clone()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Raced,
            };
        }
        next.state.execution = snapshot.status.into();
        next.state.model = Some(Model {
            model_id: snapshot.model_id.clone(),
            provider_id: snapshot.provider_id.clone(),
        });
        next.state.root_snapshot_freshness = Freshness::Fresh;
        next.state.root_snapshot_freshness_reason = input.read_method.reason();
        next.state.session = Some(snapshot);
        ProjectionReduction {
            projector: next,
            status: ReductionStatus::Applied,
        }
    }

    pub(crate) fn apply_child_snapshot(
        &self,
        input: ChildSnapshotInput<'_>,
    ) -> ProjectionReduction {
        if input.fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        let mut next = self.clone();
        let position = next.state.observed_children.iter().position(|child| {
            child.session_id == input.child_session_id
                && child.parent_session_id == input.parent_session_id
                && child.item_id == input.item_id
                && child.parent_turn_id.as_deref() == input.parent_turn_id
                && child.revision == input.item_revision
        });
        let Some(index) = position else {
            next.gap(
                GapReason::ChildIdentityConflict,
                Some(input.child_session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ChildIdentityConflict),
            };
        };
        if next.session_revision(input.child_session_id) != input.expected_session_revision {
            next.fail_child(index, FreshnessReason::NativeEventDuringRefresh);
            next.gap(
                GapReason::ChildReadRace,
                Some(input.child_session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Raced,
            };
        }
        if input.at_ms <= 0 || input.failure.is_some() || input.snapshot.is_none() {
            next.fail_child(index, FreshnessReason::ChildReadFailed);
            next.gap(
                GapReason::ChildReadFailed,
                Some(input.child_session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ChildReadFailed),
            };
        }
        let Some(snapshot) = session_snapshot(input.snapshot.unwrap(), input.child_session_id)
        else {
            next.fail_child(index, FreshnessReason::ChildReadFailed);
            next.gap(
                GapReason::ChildReadFailed,
                Some(input.child_session_id.into()),
                None,
                None,
            );
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ChildReadFailed),
            };
        };
        let child = &mut next.state.observed_children[index];
        child.snapshot = Some(snapshot);
        child.snapshot_freshness = Freshness::Fresh;
        child.snapshot_freshness_reason = input.read_method.reason();
        ProjectionReduction {
            projector: next,
            status: ReductionStatus::Applied,
        }
    }

    pub(crate) fn begin_usage_connection(
        &self,
        fence: &ProjectionFence,
        connection_id: &str,
        at_ms: i64,
    ) -> ProjectionReduction {
        if fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        let mut next = self.clone();
        if !valid_token(connection_id, MAX_USAGE_CONNECTION_ID) || at_ms <= 0 {
            next.gap(GapReason::MalformedEvent, None, None, None);
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
            };
        }
        next.usage_connection_id = Some(connection_id.into());
        next.usage_generation = 1;
        next.state.native_usage = Some(empty_usage(
            connection_id,
            "usage/read",
            1,
            at_ms,
            UsageCompleteness::NotObserved,
            None,
        ));
        ProjectionReduction {
            projector: next,
            status: ReductionStatus::Applied,
        }
    }

    /// usage/changed replaces the full cumulative native snapshot; it never
    /// adds percentages or counters to a previous notification.
    pub(crate) fn apply_usage_read(
        &self,
        fence: &ProjectionFence,
        expected_generation: u64,
        at_ms: i64,
        result: &Value,
        unsupported: bool,
    ) -> ProjectionReduction {
        if fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        if expected_generation != self.usage_generation || self.usage_connection_id.is_none() {
            return self.reduction(ReductionStatus::Raced);
        }
        let mut next = self.clone();
        let Some(revision) = next.usage_generation.checked_add(1) else {
            next.gap(GapReason::ProjectionHistoryTruncated, None, None, None);
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ProjectionHistoryTruncated),
            };
        };
        next.usage_generation = revision;
        let connection = next.usage_connection_id.as_deref().unwrap_or_default();
        let snapshot = if unsupported {
            Ok(empty_usage(
                connection,
                "usage/read",
                revision,
                at_ms,
                UsageCompleteness::Unsupported,
                None,
            ))
        } else if !result.is_object() {
            Ok(empty_usage(
                connection,
                "usage/read",
                revision,
                at_ms,
                UsageCompleteness::Invalid,
                None,
            ))
        } else if let Some(value) = result.get("usage") {
            muse_usage(connection, "usage/read", revision, at_ms, value)
        } else {
            Ok(empty_usage(
                connection,
                "usage/read",
                revision,
                at_ms,
                UsageCompleteness::NotObserved,
                None,
            ))
        };
        match snapshot {
            Ok(snapshot) => {
                next.state.native_usage = Some(snapshot);
                ProjectionReduction {
                    projector: next,
                    status: ReductionStatus::Applied,
                }
            }
            Err(_) => {
                next.gap(GapReason::MalformedEvent, None, None, None);
                ProjectionReduction {
                    projector: next,
                    status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
                }
            }
        }
    }

    pub(crate) fn close_usage_connection(
        &self,
        fence: &ProjectionFence,
        at_ms: i64,
    ) -> ProjectionReduction {
        if fence != &self.fence {
            return self.reduction(ReductionStatus::ForeignFence);
        }
        if at_ms <= 0 {
            let mut next = self.clone();
            next.gap(GapReason::MalformedEvent, None, None, None);
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::MalformedEvent),
            };
        }
        let mut next = self.clone();
        if next.usage_connection_id.is_none() {
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Ignored,
            };
        }
        let Some(revision) = next.usage_generation.checked_add(1) else {
            next.gap(GapReason::ProjectionHistoryTruncated, None, None, None);
            return ProjectionReduction {
                projector: next,
                status: ReductionStatus::Incomplete(GapReason::ProjectionHistoryTruncated),
            };
        };
        next.usage_connection_id = None;
        next.usage_generation = revision;
        if let Some(snapshot) = &mut next.state.native_usage {
            snapshot.freshness = UsageFreshness::Stale;
            snapshot.collection_issue = None;
            snapshot.ordinary_usage_allowed = None;
            snapshot.collected_at_ms = at_ms;
            snapshot.evidence.method = "connection/closed".into();
            snapshot.evidence.revision = revision;
            for bucket in &mut snapshot.buckets {
                bucket.provider_condition = None;
            }
        }
        ProjectionReduction {
            projector: next,
            status: ReductionStatus::Applied,
        }
    }

    fn usage_changed(&mut self, params: &Value, at_ms: i64) -> bool {
        let Some(connection) = self.usage_connection_id.clone() else {
            return false;
        };
        let Some(revision) = self.usage_generation.checked_add(1) else {
            return false;
        };
        let Ok(snapshot) = muse_usage(&connection, "usage/changed", revision, at_ms, params) else {
            return false;
        };
        self.usage_generation = revision;
        self.state.native_usage = Some(snapshot);
        true
    }

    fn mark_restored(&mut self) {
        self.state.execution = Execution::RecoveryRequired;
        self.state.family_completeness = FamilyCompleteness::Partial;
        self.state.native_events_seen = 0;
        self.session_revisions.clear();
        self.seen_events.clear();
        self.last_event_revision = None;
        self.usage_connection_id = None;
        self.state.root_snapshot_freshness = if self.state.session.is_some() {
            Freshness::Stale
        } else {
            Freshness::Unknown
        };
        self.state.root_snapshot_freshness_reason = if self.state.session.is_some() {
            FreshnessReason::CheckpointRestored
        } else {
            FreshnessReason::CheckpointSnapshotUnverified
        };
        for child in &mut self.state.observed_children {
            let exact = child
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.session_id == child.session_id);
            child.snapshot_freshness = if exact {
                Freshness::Stale
            } else {
                Freshness::Unknown
            };
            child.snapshot_freshness_reason = if exact {
                FreshnessReason::CheckpointRestored
            } else {
                FreshnessReason::CheckpointSnapshotUnverified
            };
        }
        if !self.state.pending_requests.is_empty() {
            self.state.pending_requests.clear();
            self.pending_gap(
                PendingGap::InventoryRestoredStale,
                Some(self.fence.native_root_id.clone()),
                None,
            );
            self.gap(
                GapReason::PendingInventoryRestoredStale,
                Some(self.fence.native_root_id.clone()),
                None,
                None,
            );
        }
        if let Some(snapshot) = &mut self.state.native_usage {
            snapshot.freshness = UsageFreshness::Stale;
            snapshot.collection_issue = None;
            snapshot.ordinary_usage_allowed = None;
            for bucket in &mut snapshot.buckets {
                bucket.provider_condition = None;
            }
        }
        self.gap(
            GapReason::CheckpointRestored,
            Some(self.fence.native_root_id.clone()),
            None,
            None,
        );
    }

    fn valid_checkpoint(&self) -> bool {
        self.fence.valid()
            && self.state.native_events_seen <= i64::MAX as u64
            && self.state.observed_children.len() <= MAX_CHILDREN
            && self.state.pending_requests.len() <= MAX_PENDING
            && self.state.turns.len() <= MAX_TURNS
            && self.state.projection_gaps.len() <= MAX_GAPS
            && self.state.pending_request_gaps.len() <= MAX_GAPS
            && self.session_revisions.len() <= MAX_CHILDREN + 1
            && self.seen_events.len() <= MAX_SEEN
            && unique(
                &self.state.observed_children,
                &self.state.pending_requests,
                &self.state.turns,
                &self.session_revisions,
            )
            && self
                .state
                .observed_children
                .iter()
                .all(|child| child.session_id != self.fence.native_root_id)
            && self.state.observed_children.iter().all(|child| {
                child.parent_session_id == self.fence.native_root_id
                    || self
                        .state
                        .observed_children
                        .iter()
                        .any(|parent| parent.session_id == child.parent_session_id)
            })
            && self
                .state
                .observed_children
                .iter()
                .all(|child| !self.would_cycle(&child.parent_session_id, &child.session_id))
            && self
                .state
                .pending_requests
                .iter()
                .all(|request| self.known_session(&request.session_id))
            && self
                .state
                .turns
                .iter()
                .all(|turn| self.known_session(&turn.session_id))
            && self
                .session_revisions
                .iter()
                .all(|entry| entry.revision > 0 && self.known_session(&entry.session_id))
            && self.state.native_usage.as_ref().is_none_or(|snapshot| {
                snapshot.validate().is_ok()
                    && self.usage_generation >= snapshot.evidence.revision
                    && self
                        .usage_connection_id
                        .as_ref()
                        .is_none_or(|connection| connection == &snapshot.connection_id)
            })
            && self
                .usage_connection_id
                .as_ref()
                .is_none_or(|connection| valid_token(connection, MAX_USAGE_CONNECTION_ID))
    }

    fn full_observation(&self) -> Result<Value, ProjectionError> {
        let mut value = json!({
            "execution":self.state.execution,"family_completeness":self.state.family_completeness,
            "gaps":self.state.gaps,"native_root_id":self.fence.native_root_id,
            "native_scope_key":self.fence.native_scope_key,"boot_id":self.fence.bridge_boot_id,
            "native_events_seen":self.state.native_events_seen,
            "root_snapshot_freshness":self.state.root_snapshot_freshness,
            "root_snapshot_freshness_reason":self.state.root_snapshot_freshness_reason,
            "observed_children":self.state.observed_children,"pending_requests":self.state.pending_requests.iter().map(Pending::view).collect::<Vec<_>>(),
            "turns":self.state.turns,"projection_gaps":self.state.projection_gaps,
            "pending_request_gaps":self.state.pending_request_gaps
        });
        optional_value(
            &mut value,
            "active_turn_id",
            self.state
                .active_turn_id
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "last_completed_turn",
            self.state
                .last_completed_turn
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "session",
            self.state
                .session
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "gap_fill",
            self.state
                .gap_fill
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "native_usage",
            self.state
                .native_usage
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "model",
            self.state
                .model
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "reasoning_effort",
            self.state
                .reasoning_effort
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "view_health",
            self.state
                .view_health
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        optional_value(
            &mut value,
            "native_event",
            self.state
                .native_event
                .as_ref()
                .map(serde_json::to_value)
                .transpose(),
        )?;
        Ok(value)
    }
    fn turn(
        &mut self,
        session: &str,
        id: &str,
        event_revision: u64,
        command: Option<String>,
        event: TurnEvent,
        terminal: Option<Terminal>,
        cursor: Option<String>,
        range: Option<SourceRange>,
        error: Option<NativeErrorKind>,
    ) {
        let found = self
            .state
            .turns
            .iter()
            .position(|turn| turn.session_id == session && turn.turn_id == id);
        if event == TurnEvent::Started
            && found.is_some_and(|i| self.state.turns[i].terminal.is_some())
        {
            return;
        }
        let turn = Turn {
            session_id: session.into(),
            turn_id: id.into(),
            event_revision,
            command_id: command,
            event,
            terminal,
            view_cursor: cursor,
            source_range: range,
            error_kind: error,
        };
        if let Some(i) = found {
            self.state.turns[i] = turn.clone();
        } else {
            if self.state.turns.len() >= MAX_TURNS {
                self.state.turns.remove(0);
                self.gap(
                    GapReason::ProjectionHistoryTruncated,
                    Some(session.into()),
                    None,
                    None,
                );
            }
            self.state.turns.push(turn.clone());
        }
        if session == self.fence.native_root_id {
            if event == TurnEvent::Started {
                self.state.active_turn_id = Some(id.into());
            } else {
                if self.state.active_turn_id.as_deref() == Some(id) {
                    self.state.active_turn_id = None;
                }
                self.state.last_completed_turn = Some(turn);
            }
        } else if let Some(child) = self
            .state
            .observed_children
            .iter_mut()
            .find(|child| child.session_id == session)
        {
            child.last_turn = Some(turn);
        }
    }

    fn child_item(
        &mut self,
        parent: &str,
        item: &Value,
        cursor: Option<String>,
        parent_turn: Option<String>,
    ) {
        let Some(kind) = ChildKind::parse(item.get("kind").and_then(Value::as_str)) else {
            return;
        };
        let Some(session) = identity(item.get("childSessionId")) else {
            self.gap(GapReason::MalformedEvent, Some(parent.into()), None, None);
            return;
        };
        let Some(item_id) = identity(item.get("itemId")) else {
            self.gap(GapReason::MalformedEvent, Some(parent.into()), None, None);
            return;
        };
        let Some(revision) = item.get("revision").and_then(safe_u64) else {
            self.gap(GapReason::MalformedEvent, Some(parent.into()), None, None);
            return;
        };

        if session == self.fence.native_root_id
            || session == parent
            || self.would_cycle(parent, &session)
        {
            self.gap(GapReason::ChildCycle, Some(parent.into()), None, None);
            return;
        }
        let index = self
            .state
            .observed_children
            .iter()
            .position(|child| child.session_id == session);
        if index.is_none() && self.state.observed_children.len() >= MAX_CHILDREN {
            self.gap(GapReason::ChildLimit, Some(parent.into()), None, None);
            return;
        }
        if let Some(i) = index {
            let old = &self.state.observed_children[i];
            if old.parent_session_id != parent && old.item_id == item_id && revision <= old.revision
            {
                self.gap(GapReason::ChildIdentityConflict, Some(session), None, None);
                return;
            }
            if old.parent_session_id == parent && old.item_id == item_id && revision <= old.revision
            {
                return;
            }
        }
        let has_result = item.get("result").is_some_and(|v| !v.is_null());
        let mut child = Child {
            session_id: session.clone(),
            parent_session_id: parent.into(),
            item_id: item_id.clone(),
            subagent_id: identity(item.get("subagentId")),
            parent_turn_id: parent_turn,
            status: NativeStatus::native(item.get("status").and_then(Value::as_str)),
            control_status: item
                .get("controlStatus")
                .and_then(Value::as_str)
                .map(ControlStatus::native),
            revision,
            kind,
            snapshot_freshness: Freshness::Unknown,
            snapshot_freshness_reason: FreshnessReason::NativeReadNotYetVerified,
            snapshot: None,
            last_turn: None,
            result_available: has_result,
            result_source: has_result.then(|| ResultSource {
                parent_session_id: parent.into(),
                item_id,
                revision,
                view_cursor: cursor,
            }),
        };
        if let Some(i) = index {
            let old = &self.state.observed_children[i];
            child.snapshot = old.snapshot.clone();
            child.last_turn = old.last_turn.clone();
            child.snapshot_freshness = if old.snapshot.is_some() {
                Freshness::Stale
            } else {
                Freshness::Unknown
            };
            child.snapshot_freshness_reason = if old.snapshot.is_some() {
                FreshnessReason::ParentChildEventAfterRead
            } else {
                FreshnessReason::NativeReadNotYetVerified
            };
            self.state.observed_children[i] = child;
        } else {
            self.state.observed_children.push(child);
        }
    }

    fn would_cycle(&self, parent: &str, child: &str) -> bool {
        let mut current = Some(parent);
        let mut hops = 0;
        while let Some(id) = current {
            if id == child {
                return true;
            }
            hops += 1;
            if hops > MAX_CHILDREN {
                return true;
            }
            current = self
                .state
                .observed_children
                .iter()
                .find(|entry| entry.session_id == id)
                .map(|entry| entry.parent_session_id.as_str());
        }
        false
    }

    fn session_changed(&mut self, session: &str) {
        let next = self.session_revision(session).checked_add(1);
        if let Some(next) = next {
            if let Some(entry) = self
                .session_revisions
                .iter_mut()
                .find(|entry| entry.session_id == session)
            {
                entry.revision = next;
            } else if self.session_revisions.len() < MAX_CHILDREN + 1 {
                self.session_revisions.push(SessionRevision {
                    session_id: session.into(),
                    revision: next,
                });
            } else {
                self.gap(
                    GapReason::ProjectionHistoryTruncated,
                    Some(session.into()),
                    None,
                    None,
                );
            }
        } else {
            self.gap(
                GapReason::ProjectionHistoryTruncated,
                Some(session.into()),
                None,
                None,
            );
        }
        if session == self.fence.native_root_id {
            if self.state.root_snapshot_freshness == Freshness::Fresh {
                self.state.root_snapshot_freshness = Freshness::Stale;
                self.state.root_snapshot_freshness_reason = FreshnessReason::NativeEventAfterRead;
            }
        } else if let Some(child) = self
            .state
            .observed_children
            .iter_mut()
            .find(|entry| entry.session_id == session)
        {
            if child.snapshot_freshness == Freshness::Fresh {
                child.snapshot_freshness = Freshness::Stale;
                child.snapshot_freshness_reason = FreshnessReason::NativeEventAfterRead;
            }
        }
    }

    pub(crate) fn usage_generation(&self) -> u64 {
        self.usage_generation
    }
    pub(crate) fn session_revision(&self, session: &str) -> u64 {
        self.session_revisions
            .iter()
            .find(|entry| entry.session_id == session)
            .map_or(0, |entry| entry.revision)
    }
    fn known_session(&self, session: &str) -> bool {
        session == self.fence.native_root_id
            || self
                .state
                .observed_children
                .iter()
                .any(|child| child.session_id == session)
    }
    fn pending_has(&self, session: &str, kind: PendingKind, id: &str) -> bool {
        self.state.pending_requests.iter().any(|entry| {
            entry.session_id == session && entry.kind == kind && entry.request_id == id
        })
    }
    fn pending_upsert(
        &mut self,
        session: String,
        kind: PendingKind,
        id: String,
        event: PendingEvent,
    ) {
        if let Some(entry) = self.state.pending_requests.iter_mut().find(|entry| {
            entry.session_id == session && entry.kind == kind && entry.request_id == id
        }) {
            entry.last_event = event;
            return;
        }
        if self.state.pending_requests.len() >= MAX_PENDING {
            self.pending_gap(PendingGap::InventoryLimit, Some(session.clone()), Some(id));
            self.gap(GapReason::PendingInventoryLimit, Some(session), None, None);
            return;
        }
        self.state.pending_requests.push(Pending {
            kind,
            session_id: session,
            request_id: id,
            last_event: event,
        });
    }
    fn pending_remove(&mut self, session: &str, kind: PendingKind, id: &str) {
        self.state.pending_requests.retain(|entry| {
            entry.session_id != session || entry.kind != kind || entry.request_id != id
        });
    }
    fn pending_gap(&mut self, reason: PendingGap, session: Option<String>, id: Option<String>) {
        self.state.pending_request_gaps.push(PendingGapRecord {
            reason,
            session_id: session,
            request_id: id,
        });
        if self.state.pending_request_gaps.len() > MAX_GAPS {
            self.state.pending_request_gaps.remove(0);
        }
    }
    fn gap(
        &mut self,
        reason: GapReason,
        session: Option<String>,
        after: Option<String>,
        next: Option<String>,
    ) {
        self.state.gaps = self.state.gaps.saturating_add(1);
        self.state.projection_gaps.push(Gap {
            status: GapStatus::Unfilled,
            reason,
            session_id: session,
            after: after.clone(),
            next: next.clone(),
            cursors_complete: after.is_some() && next.is_some(),
            projection_state: GapState::Incomplete,
        });
        if self.state.projection_gaps.len() > MAX_GAPS {
            self.state.projection_gaps.remove(0);
        }
        self.state.family_completeness = FamilyCompleteness::Partial;
    }
    fn remember(&mut self, key: EventFingerprint) {
        self.seen_events.push_back(key);
        if self.seen_events.len() > MAX_SEEN {
            self.seen_events.pop_front();
        }
    }
    fn fail_child(&mut self, index: usize, reason: FreshnessReason) {
        let child = &mut self.state.observed_children[index];
        child.snapshot_freshness = if child.snapshot.is_some() {
            Freshness::Stale
        } else {
            Freshness::Unknown
        };
        child.snapshot_freshness_reason = reason;
    }
    fn reduction(&self, status: ReductionStatus) -> ProjectionReduction {
        ProjectionReduction {
            projector: self.clone(),
            status,
        }
    }
    fn event_gap<'a>(
        &self,
        session: Option<String>,
        reason: GapReason,
        handoff: Option<&'a Value>,
    ) -> EventReduction<'a> {
        let mut next = self.clone();
        next.gap(reason, session, None, None);
        EventReduction {
            projector: next,
            status: ReductionStatus::Incomplete(reason),
            stream_handoff: handoff,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKind {
    TurnStarted,
    TurnCompleted,
    TurnUnqueued,
    SessionStatusChanged,
    SessionGoalChanged,
    SessionReasoningEffortChanged,
    SessionModelChanged,
    ItemStarted,
    ItemUpdated,
    ItemCompleted,
    ItemDelta,
    ViewGap,
    SessionViewHealthChanged,
    UsageChanged,
    ApprovalRequest,
    ApprovalRequested,
    ApprovalUpdated,
    ApprovalResolved,
    UserInputRequest,
    UserInputRequested,
    UserInputSettled,
}
impl EventKind {
    fn parse(method: &str) -> Option<Self> {
        Some(match method {
            "turn/started" => Self::TurnStarted,
            "turn/completed" => Self::TurnCompleted,
            "turn/unqueued" => Self::TurnUnqueued,
            "session/statusChanged" => Self::SessionStatusChanged,
            "session/goalChanged" => Self::SessionGoalChanged,
            "session/reasoningEffortChanged" => Self::SessionReasoningEffortChanged,
            "session/modelChanged" => Self::SessionModelChanged,
            "item/started" => Self::ItemStarted,
            "item/updated" => Self::ItemUpdated,
            "item/completed" => Self::ItemCompleted,
            "item/delta" => Self::ItemDelta,
            "view/gap" => Self::ViewGap,
            "session/viewHealthChanged" => Self::SessionViewHealthChanged,
            "usage/changed" => Self::UsageChanged,
            "approval/request" => Self::ApprovalRequest,
            "approval/requested" => Self::ApprovalRequested,
            "approval/updated" => Self::ApprovalUpdated,
            "approval/resolved" => Self::ApprovalResolved,
            "userInput/request" => Self::UserInputRequest,
            "userInput/requested" => Self::UserInputRequested,
            "userInput/settled" => Self::UserInputSettled,
            _ => return None,
        })
    }
    fn method(self) -> &'static str {
        match self {
            Self::TurnStarted => "turn/started",
            Self::TurnCompleted => "turn/completed",
            Self::TurnUnqueued => "turn/unqueued",
            Self::SessionStatusChanged => "session/statusChanged",
            Self::SessionGoalChanged => "session/goalChanged",
            Self::SessionReasoningEffortChanged => "session/reasoningEffortChanged",
            Self::SessionModelChanged => "session/modelChanged",
            Self::ItemStarted => "item/started",
            Self::ItemUpdated => "item/updated",
            Self::ItemCompleted => "item/completed",
            Self::ItemDelta => "item/delta",
            Self::ViewGap => "view/gap",
            Self::SessionViewHealthChanged => "session/viewHealthChanged",
            Self::UsageChanged => "usage/changed",
            Self::ApprovalRequest => "approval/request",
            Self::ApprovalRequested => "approval/requested",
            Self::ApprovalUpdated => "approval/updated",
            Self::ApprovalResolved => "approval/resolved",
            Self::UserInputRequest => "userInput/request",
            Self::UserInputRequested => "userInput/requested",
            Self::UserInputSettled => "userInput/settled",
        }
    }
}
fn parse_inventory(
    session: &str,
    kind: PendingKind,
    rows: &[Value],
    out: &mut Vec<Pending>,
) -> Option<PendingGap> {
    let mut seen = Vec::<String>::new();
    for row in rows {
        if !row.is_object() {
            return Some(PendingGap::InventoryMalformed);
        }
        let Some(native_session) = row.get("sessionId").and_then(Value::as_str) else {
            return Some(PendingGap::InventoryMalformed);
        };
        if native_session != session {
            return Some(PendingGap::InventoryForeignSession);
        }
        let Some(id) = identity(row.get(kind.id_field())) else {
            return Some(PendingGap::InventoryMalformed);
        };
        if seen.contains(&id)
            || out
                .iter()
                .any(|old| old.kind == kind && old.session_id == session && old.request_id == id)
        {
            return Some(PendingGap::InventoryDuplicate);
        }
        if out.len() >= MAX_PENDING {
            return Some(PendingGap::InventoryLimit);
        }
        seen.push(id.clone());
        out.push(Pending {
            kind,
            session_id: session.into(),
            request_id: id,
            last_event: PendingEvent::Inventory,
        });
    }
    None
}
fn pending_gap_reason(reason: PendingGap) -> GapReason {
    match reason {
        PendingGap::ApprovalUpdateWithoutRequest => GapReason::PendingApprovalUpdateWithoutRequest,
        PendingGap::InventoryMalformed => GapReason::PendingInventoryMalformed,
        PendingGap::InventoryForeignSession => GapReason::PendingInventoryForeignSession,
        PendingGap::InventoryDuplicate => GapReason::PendingInventoryDuplicate,
        PendingGap::InventoryRaced => GapReason::PendingInventoryRace,
        PendingGap::InventoryLimit => GapReason::PendingInventoryLimit,
        PendingGap::InventoryRestoredStale => GapReason::PendingInventoryRestoredStale,
    }
}
fn fingerprint(revision: u64, method: &str, params: &Value) -> EventFingerprint {
    let item = params.get("item").filter(|value| value.is_object());
    let weekly = params.get("weekly").filter(|value| value.is_object());
    let window = params.get("window").filter(|value| value.is_object());
    let material = json!({
        "session":identity(params.get("sessionId")),
        "command":identity(params.get("commandId")).or_else(||identity(params.pointer("/item/commandId"))),
        "turn":identity(params.get("turnId")).or_else(||identity(params.pointer("/item/turnId"))),
        "item_kind":item.and_then(|v|v.get("kind")).and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "child":item.and_then(|v|identity(v.get("childSessionId"))),
        "item_id":item.and_then(|v|identity(v.get("itemId"))),
        "item_revision":item.and_then(|v|v.get("revision")).and_then(Value::as_u64),
        "item_status":item.and_then(|v|v.get("status")).and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "control_status":item.and_then(|v|v.get("controlStatus")).and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "result_available":item.and_then(|v|v.get("result")).is_some_and(|v|!v.is_null()),
        "status":params.get("status").and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "terminal":params.get("terminal").and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "effort":params.get("reasoningEffort").and_then(Value::as_str).and_then(|v|valid_text(v,64).then_some(v)),
        "model":identity(params.get("modelId")),
        "provider":identity(params.get("providerId")),
        "approval":identity(params.get("approvalId")),
        "user_input":identity(params.get("userInputId")),
        "view_after":cursor(params.get("after")),
        "view_next":cursor(params.get("next")),
        "usage_observed_at":params.get("observedAtMs").and_then(safe_i64),
        "usage_tier":params.get("tier").and_then(Value::as_str).and_then(|v|valid_text(v,256).then_some(v)),
        "weekly_used":weekly.and_then(|v|v.get("usedPercent")).and_then(safe_i64),
        "weekly_reset":weekly.and_then(|v|v.get("resetsAtMs")).and_then(safe_i64),
        "window_used":window.and_then(|v|v.get("usedPercent")).and_then(safe_i64),
        "window_reset":window.and_then(|v|v.get("resetsAtMs")).and_then(safe_i64),
        "window_duration":window.and_then(|v|v.get("windowDurationMins")).and_then(safe_i64),
        "delta_payload_omitted":method=="item/delta",
    });
    let bytes = serde_json::to_vec(&material).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    let mut payload_sha256 = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut payload_sha256, "{byte:02x}");
    }
    EventFingerprint {
        revision,
        method: method.into(),
        payload_sha256,
        session_id: identity(params.get("sessionId")),
        command_id: identity(params.get("commandId"))
            .or_else(|| identity(params.pointer("/item/commandId"))),
        turn_id: identity(params.get("turnId"))
            .or_else(|| identity(params.pointer("/item/turnId"))),
        item_id: identity(params.pointer("/item/itemId")),
        item_revision: params.pointer("/item/revision").and_then(safe_u64),
    }
}
fn parse_source_range(value: Option<&Value>) -> Option<SourceRange> {
    let value = value?;
    let cursor = |value: &Value| -> Option<Cursor> {
        let id = value.get("id").and_then(Value::as_str)?;
        let sequence = value.get("sequence").and_then(safe_u64)?;
        valid_text(id, MAX_ID).then(|| Cursor {
            id: id.into(),
            sequence,
        })
    };
    let first = cursor(value.get("first")?)?;
    let last = cursor(value.get("last")?)?;
    let stream = value.get("stream")?;
    let id = stream.get("id").and_then(Value::as_str)?;
    let kind = stream.get("kind").and_then(Value::as_str)?;
    if !valid_text(id, MAX_ID) || !matches!(kind, "session" | "global") {
        return None;
    }
    Some(SourceRange {
        first,
        last,
        stream: StreamIdentity {
            id: id.into(),
            kind: kind.into(),
        },
    })
}
fn gap_bracket(session: &str, params: &Value) -> Gap {
    let after = cursor(params.get("after"));
    let next = cursor(params.get("next"));
    let complete = after.is_some() && next.is_some();
    Gap {
        status: GapStatus::Unfilled,
        reason: if complete {
            GapReason::NativeViewGap
        } else {
            GapReason::OpaqueViewBracketIncomplete
        },
        session_id: Some(session.into()),
        after,
        next,
        cursors_complete: complete,
        projection_state: GapState::Incomplete,
    }
}
fn cursor(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|text| text.len() <= MAX_CURSOR && !text.chars().any(char::is_control))
        .map(str::to_owned)
}
fn session_snapshot(value: &Value, expected: &str) -> Option<SessionSnapshot> {
    let id = value.get("sessionId").and_then(Value::as_str)?;
    let status = value.get("status").and_then(Value::as_str)?;
    if id != expected || !valid_text(id, MAX_ID) {
        return None;
    }
    let optional = |key: &str| -> Option<Option<String>> {
        match value.get(key) {
            None | Some(Value::Null) => Some(None),
            Some(Value::String(text)) if valid_text(text, MAX_ID) => Some(Some(text.clone())),
            _ => None,
        }
    };
    Some(SessionSnapshot {
        session_id: id.into(),
        status: NativeStatus::native(Some(status)),
        active_turn_id: optional("activeTurnId")?,
        model_id: optional("modelId")?,
        provider_id: optional("providerId")?,
        turn_count: match value.get("turnCount") {
            None | Some(Value::Null) => None,
            Some(n) => Some(safe_u64(n)?),
        },
    })
}
impl From<NativeStatus> for Execution {
    fn from(status: NativeStatus) -> Self {
        match status {
            NativeStatus::Idle => Self::Idle,
            NativeStatus::Running | NativeStatus::Queued => Self::Running,
            NativeStatus::Completed => Self::Completed,
            NativeStatus::Failed => Self::Failed,
            NativeStatus::Cancelled | NativeStatus::Interrupted => Self::Interrupted,
            NativeStatus::NotLoaded | NativeStatus::Unknown => Self::Unknown,
        }
    }
}
fn safe_u64(value: &Value) -> Option<u64> {
    value.as_u64().filter(|number| *number <= MAX_SAFE_INTEGER)
}
fn safe_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .filter(|number| number.unsigned_abs() <= MAX_SAFE_INTEGER)
}
fn identity(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|text| valid_text(text, MAX_ID))
        .map(str::to_owned)
}
fn valid_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn valid_token(value: &str, max: usize) -> bool {
    valid_text(value, max)
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}
fn unique(
    children: &[Child],
    pending: &[Pending],
    turns: &[Turn],
    revisions: &[SessionRevision],
) -> bool {
    children.iter().enumerate().all(|(i, c)| {
        valid_text(&c.session_id, MAX_ID)
            && c.session_id != c.parent_session_id
            && !children[..i].iter().any(|x| x.session_id == c.session_id)
    }) && pending.iter().enumerate().all(|(i, p)| {
        valid_text(&p.session_id, MAX_ID)
            && valid_text(&p.request_id, MAX_ID)
            && !pending[..i].iter().any(|x| {
                x.session_id == p.session_id && x.kind == p.kind && x.request_id == p.request_id
            })
    }) && turns.iter().enumerate().all(|(i, t)| {
        !turns[..i]
            .iter()
            .any(|x| x.session_id == t.session_id && x.turn_id == t.turn_id)
    }) && revisions
        .iter()
        .enumerate()
        .all(|(i, r)| !revisions[..i].iter().any(|x| x.session_id == r.session_id))
}
fn optional_value(
    value: &mut Value,
    key: &str,
    result: Result<Option<Value>, serde_json::Error>,
) -> Result<(), ProjectionError> {
    if let Some(v) = result.map_err(|_| ProjectionError::Serialization)? {
        value[key] = v;
    }
    Ok(())
}
fn json_bytes(value: &Value) -> Result<usize, ProjectionError> {
    serde_json::to_vec(value)
        .map(|b| b.len())
        .map_err(|_| ProjectionError::Serialization)
}
fn bounded_push(value: &mut Value, key: &str, item: Value) -> Result<(), ProjectionError> {
    let rows = value
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut next = rows;
    next.push(item);
    value[key] = Value::Array(next);
    if json_bytes(value)? > MAX_STATE_BYTES {
        value[key].as_array_mut().unwrap().pop();
    }
    Ok(())
}
fn bounded_set(value: &mut Value, key: &str, item: Value) -> Result<(), ProjectionError> {
    value[key] = item;
    if json_bytes(value)? > MAX_STATE_BYTES {
        value.as_object_mut().unwrap().remove(key);
    }
    Ok(())
}
fn pending_digest(requests: &[Pending]) -> Result<String, ProjectionError> {
    let mut digest = Sha256::new();
    for request in requests {
        let tuple = json!([
            request.kind.method(),
            request.session_id,
            request.request_id
        ]);
        digest.update(serde_json::to_vec(&tuple).map_err(|_| ProjectionError::Serialization)?);
        digest.update(b"\n");
    }
    let mut text = String::with_capacity(64);
    for byte in digest.finalize() {
        use std::fmt::Write as _;
        let _ = write!(&mut text, "{byte:02x}");
    }
    Ok(text)
}

fn empty_usage(
    connection: &str,
    method: &str,
    revision: u64,
    at: i64,
    completeness: UsageCompleteness,
    native_at: Option<i64>,
) -> NativeUsageSnapshot {
    NativeUsageSnapshot {
        schema_version: 1,
        service: UsageService::Muse,
        connection_id: connection.into(),
        auth_context_ref: None,
        ordinary_usage_allowed: None,
        collection_issue: None,
        collected_at_ms: at,
        freshness: UsageFreshness::Current,
        completeness,
        evidence: UsageEvidence {
            method: method.into(),
            revision,
            native_observed_at_ms: native_at,
        },
        buckets: Vec::new(),
    }
}
struct ParsedWindow {
    used: i64,
    resets: i64,
    duration: i64,
}
fn parsed_window(value: &Value, need_duration: bool) -> Option<ParsedWindow> {
    let used = safe_i64(value.get("usedPercent")?)?;
    let resets = safe_i64(value.get("resetsAtMs")?)?;
    let duration = match value.get("windowDurationMins") {
        Some(v) => safe_i64(v)?,
        None if !need_duration => 0,
        _ => return None,
    };
    if used < 0 || resets < 0 || (need_duration && duration <= 0) {
        return None;
    }
    Some(ParsedWindow {
        used,
        resets,
        duration,
    })
}
fn muse_usage(
    connection: &str,
    method: &str,
    revision: u64,
    at: i64,
    value: &Value,
) -> Result<NativeUsageSnapshot, ProjectionError> {
    let native_at = value
        .get("observedAtMs")
        .and_then(safe_i64)
        .filter(|v| *v >= 0);
    let parsed = (|| {
        let observed = safe_i64(value.get("observedAtMs")?)?;
        let tier = value.get("tier")?.as_str()?;
        if observed < 0 || !valid_text(tier, 256) {
            return None;
        }
        Some((
            observed,
            tier.to_owned(),
            parsed_window(value.get("weekly")?, false)?,
            parsed_window(value.get("window")?, true)?,
        ))
    })();
    let Some((observed, tier, weekly, window)) = parsed else {
        let snapshot = empty_usage(
            connection,
            method,
            revision,
            at,
            UsageCompleteness::Invalid,
            native_at,
        );
        snapshot
            .validate()
            .map_err(|_| ProjectionError::InvalidCheckpoint)?;
        return Ok(snapshot);
    };
    let bucket = UsageBucket {
        id: "subscription".into(),
        name: None,
        normal_model_slug: None,
        plan_type: Some(ObservedUsageValue {
            value: tier,
            collected_at_ms: at,
        }),
        primary: Some(UsageWindow {
            collected_at_ms: at,
            used_percent: window.used,
            resets_at_ms: Some(window.resets),
            window_duration_mins: Some(window.duration),
        }),
        secondary: Some(UsageWindow {
            collected_at_ms: at,
            used_percent: weekly.used,
            resets_at_ms: Some(weekly.resets),
            window_duration_mins: None,
        }),
        credits: None,
        individual_limit: None,
        spend_control_reached: None,
        rate_limit_reached_type: None,
        provider_condition: None,
    };
    let snapshot = NativeUsageSnapshot {
        schema_version: 1,
        service: UsageService::Muse,
        connection_id: connection.into(),
        auth_context_ref: None,
        ordinary_usage_allowed: None,
        collection_issue: None,
        collected_at_ms: at,
        freshness: UsageFreshness::Current,
        completeness: UsageCompleteness::Full,
        evidence: UsageEvidence {
            method: method.into(),
            revision,
            native_observed_at_ms: Some(observed),
        },
        buckets: vec![bucket],
    };
    snapshot
        .validate()
        .map_err(|_| ProjectionError::InvalidCheckpoint)?;
    Ok(snapshot)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fence() -> ProjectionFence {
        ProjectionFence {
            binding_id: "binding_fixture".into(),
            binding_generation: 4,
            native_root_id: "ses_root".into(),
            native_scope_key: "muse:fixture:namespace".into(),
            bridge_boot_id: "boot_fixture".into(),
        }
    }
    fn event(method: &str, params: Value) -> Value {
        json!({"method":method,"params":params})
    }
    fn apply<'a>(
        p: &ObservationProjector,
        f: &'a ProjectionFence,
        revision: u64,
        event: &'a Value,
    ) -> EventReduction<'a> {
        p.apply_event(NativeEventInput {
            fence: f,
            event_revision: revision,
            at_ms: 1_800_000_000_000,
            event,
        })
    }
    fn child_event(parent: &str, child: &str, item: &str, revision: u64, status: &str) -> Value {
        event(
            "item/updated",
            json!({"sessionId":parent,"turnId":"turn_parent","viewCursor":"cursor","item":{"kind":"subagent","childSessionId":child,"itemId":item,"subagentId":"sub_fixture","revision":revision,"status":status,"controlStatus":"running"}}),
        )
    }

    #[test]
    fn malformed_fence_is_rejected_without_state_change() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let mut foreign = fence();
        foreign.binding_generation += 1;
        let e = event(
            "session/statusChanged",
            json!({"sessionId":"ses_root","status":"idle"}),
        );
        let r = apply(&p, &foreign, 1, &e);
        assert_eq!(r.status, ReductionStatus::ForeignFence);
        assert_eq!(r.projector.state.gaps, 0);
    }

    #[test]
    fn duplicate_and_conflicting_revision_are_distinguished() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let e = event(
            "session/statusChanged",
            json!({"sessionId":"ses_root","status":"running"}),
        );
        let first = apply(&p, &f, 1, &e);
        let duplicate = apply(&first.projector, &f, 1, &e);
        assert_eq!(duplicate.status, ReductionStatus::Duplicate);
        assert_eq!(duplicate.projector.state.native_events_seen, 1);
        let conflict = event(
            "session/statusChanged",
            json!({"sessionId":"ses_root","status":"idle"}),
        );
        assert_eq!(
            apply(&first.projector, &f, 1, &conflict).status,
            ReductionStatus::Incomplete(GapReason::SourceRevisionConflict)
        );
    }

    #[test]
    fn revision_gap_and_idle_parent_keep_family_incomplete() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let child = apply(
            &p,
            &f,
            1,
            &child_event("ses_root", "ses_child", "item_child", 7, "running"),
        )
        .projector;
        let idle = event(
            "session/statusChanged",
            json!({"sessionId":"ses_root","status":"idle"}),
        );
        let r = apply(&child, &f, 3, &idle);
        assert_eq!(
            r.status,
            ReductionStatus::Incomplete(GapReason::SourceRevisionGap)
        );
        assert_eq!(r.projector.state.execution, Execution::Idle);
        assert_eq!(
            r.projector.state.family_completeness,
            FamilyCompleteness::Partial
        );
        assert_eq!(
            r.projector.state.observed_children[0].status,
            NativeStatus::Running
        );
    }

    #[test]
    fn old_item_revision_does_not_replace_child_provenance() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let first = apply(
            &p,
            &f,
            1,
            &child_event("ses_root", "ses_child", "item_child", 5, "completed"),
        )
        .projector;
        let older = apply(
            &first,
            &f,
            2,
            &child_event("ses_root", "ses_child", "item_child", 4, "running"),
        )
        .projector;
        assert_eq!(older.state.observed_children[0].revision, 5);
        assert_eq!(
            older.state.observed_children[0].status,
            NativeStatus::Completed
        );
    }

    #[test]
    fn pending_inventory_malformed_duplicate_and_race_are_atomic() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let mut seed = p.clone();
        seed.pending_upsert(
            "ses_root".into(),
            PendingKind::Approval,
            "approval_old".into(),
            PendingEvent::Requested,
        );
        let malformed = json!({"approvals":{},"userInputs":[]});
        let malformed_result = seed.replace_pending(PendingInventoryInput {
            fence: &f,
            session_id: "ses_root",
            expected_session_revision: 0,
            inventory: &malformed,
        });
        assert_eq!(
            malformed_result.status,
            ReductionStatus::Incomplete(GapReason::PendingInventoryMalformed)
        );
        assert_eq!(malformed_result.projector.state.pending_requests.len(), 1);
        let foreign = json!({"approvals":[{"sessionId":"ses_child","approvalId":"approval_foreign"}],"userInputs":[]});
        let bad = seed.replace_pending(PendingInventoryInput {
            fence: &f,
            session_id: "ses_root",
            expected_session_revision: 0,
            inventory: &foreign,
        });
        assert_eq!(
            bad.status,
            ReductionStatus::Incomplete(GapReason::PendingInventoryForeignSession)
        );
        assert_eq!(bad.projector.state.pending_requests.len(), 1);
        let empty = json!({"approvals":[],"userInputs":[]});
        let raced = seed.replace_pending(PendingInventoryInput {
            fence: &f,
            session_id: "ses_root",
            expected_session_revision: 1,
            inventory: &empty,
        });
        assert_eq!(raced.status, ReductionStatus::Raced);
        assert_eq!(raced.projector.state.pending_requests.len(), 1);
        let dup = json!({"approvals":[{"sessionId":"ses_root","approvalId":"a"},{"sessionId":"ses_root","approvalId":"a"}],"userInputs":[]});
        let d = seed.replace_pending(PendingInventoryInput {
            fence: &f,
            session_id: "ses_root",
            expected_session_revision: 0,
            inventory: &dup,
        });
        assert_eq!(
            d.status,
            ReductionStatus::Incomplete(GapReason::PendingInventoryDuplicate)
        );
        assert_eq!(d.projector.state.pending_requests.len(), 1);
    }

    #[test]
    fn approval_update_without_prior_request_is_incomplete() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let e = event(
            "approval/updated",
            json!({"sessionId":"ses_root","approvalId":"missing"}),
        );
        let r = apply(&p, &f, 1, &e);
        assert_eq!(
            r.status,
            ReductionStatus::Incomplete(GapReason::PendingApprovalUpdateWithoutRequest)
        );
        assert!(r.projector.state.pending_requests.is_empty());
    }

    #[test]
    fn malformed_opaque_gap_never_appears_filled() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let e = event(
            "view/gap",
            json!({"sessionId":"ses_root","after":17,"next":null}),
        );
        let r = apply(&p, &f, 1, &e);
        let gap = r.projector.state.gap_fill.as_ref().unwrap();
        assert!(!gap.cursors_complete);
        assert_eq!(gap.projection_state, GapState::Incomplete);
        assert!(gap.after.is_none() && gap.next.is_none());
    }

    #[test]
    fn restored_child_snapshot_is_stale() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let mut child = apply(
            &p,
            &f,
            1,
            &child_event("ses_root", "ses_child", "item_child", 1, "running"),
        )
        .projector;
        child.pending_upsert(
            "ses_root".into(),
            PendingKind::Approval,
            "old_approval".into(),
            PendingEvent::Requested,
        );
        let snapshot = json!({"sessionId":"ses_child","status":"idle","activeTurnId":null,"modelId":"model","providerId":"provider","turnCount":1});
        let read = child.apply_child_snapshot(ChildSnapshotInput {
            fence: &f,
            child_session_id: "ses_child",
            parent_session_id: "ses_root",
            item_id: "item_child",
            parent_turn_id: Some("turn_parent"),
            item_revision: 1,
            expected_session_revision: 0,
            at_ms: 1_800_000_000_001,
            read_method: ReadMethod::SessionRead,
            snapshot: Some(&snapshot),
            failure: None,
        });
        assert_eq!(read.status, ReductionStatus::Applied);
        let saved = read.projector.checkpoint().unwrap();
        let restored = ObservationProjector::restore(fence(), saved).unwrap();
        assert_eq!(
            restored.state.observed_children[0].snapshot_freshness,
            Freshness::Stale
        );
        assert!(restored.state.pending_requests.is_empty());
        assert_eq!(restored.state.native_events_seen, 0);
    }

    #[test]
    fn usage_update_replaces_full_cumulative_values_and_delta_is_handed_off() {
        let f = fence();
        let p = ObservationProjector::new(f.clone()).unwrap();
        let connection = p
            .begin_usage_connection(&f, "connection_fixture", 1_800_000_000_001)
            .projector;
        let first = json!({"usage":{"observedAtMs":1_800_000_000_002i64,"tier":"pro","weekly":{"resetsAtMs":1_900_000_000_000i64,"usedPercent":31},"window":{"resetsAtMs":1_850_000_000_000i64,"usedPercent":12,"windowDurationMins":60}}});
        let read = connection.apply_usage_read(&f, 1, 1_800_000_000_002, &first, false);
        let changed = event(
            "usage/changed",
            json!({"observedAtMs":1_800_000_000_003i64,"tier":"pro","weekly":{"resetsAtMs":1_900_000_000_000i64,"usedPercent":42},"window":{"resetsAtMs":1_850_000_000_000i64,"usedPercent":17,"windowDurationMins":60}}),
        );
        let latest = apply(&read.projector, &f, 1, &changed).projector;
        let usage = latest.state.native_usage.as_ref().unwrap();
        assert_eq!(
            usage.buckets[0].secondary.as_ref().unwrap().used_percent,
            42
        );
        assert_eq!(usage.buckets[0].primary.as_ref().unwrap().used_percent, 17);
        let delta = event(
            "item/delta",
            json!({"sessionId":"ses_root","delta":"private stream content"}),
        );
        let handed = apply(&latest, &f, 2, &delta);
        assert!(handed.stream_handoff.is_some());
        let observation = handed.projector.observation().unwrap();
        assert!(
            observation
                .to_string()
                .find("private stream content")
                .is_none()
        );
    }
}
