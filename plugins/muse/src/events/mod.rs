mod provider;
#[path = "../workflow_child.rs"]
mod workflow_child;

use eliot_core::{
    ContextItem, Error, MUSE_VIEW_UNAVAILABLE_REASON_PREFIX, ProviderRestriction, Result,
    SecretPolicy, SessionUsage, SourceHealthUpdate,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    event_actor::EventActor,
    native_child::{ChildAction, binding_from_ack, is_admission_rejection, register_child},
    native_data::{UNCLASSIFIED_NATIVE_EVENT, unclassified},
    protocol::{MAX_COMMAND_ID_BYTES, MAX_TEXT_BYTES},
    session::{
        EVENT_DEADLINE, MessageCommit, StreamSource, attach_final_text, attach_native_error,
        attach_provider_restriction, event_id as shared_event_id, native_no_write,
        next_input_outcomes, redact_request_details_for_commit, rejected_control, request_event_id,
        request_reply_applied, unknown_control,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::{path::PathBuf, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use self::workflow_child::{
    MAX_WORKFLOW_CHILDREN, WORKFLOW_CHILD_FAMILIES, WorkflowChildKey, WorkflowChildState,
    child_event_id, complete_event_data, is_workflow_item_frame, muse_child_key,
    should_bypass_parent_buffer, start_event_data,
};
use crate::native::{
    NativeClient, NativeFrame, StreamState, native_error_code,
    requests::{
        CorePending, describe_request, is_newer_approval_requirement, public_request_view,
        refresh_offer_from_raw, reply_for, same_approval_offer,
    },
    types::{
        NativeApprovalDecideResult, NativeApprovalResolvedNotification,
        NativeApprovalUpdatedNotification, NativeHistoryNoneReason, NativeItem,
        NativeItemCompletedNotification, NativeItemReadOutputParams, NativeItemReadOutputResult,
        NativeItemStartedNotification, NativePatchRef, NativeSessionClosedNotification,
        NativeSessionContextUsageNotification, NativeSessionReadResult,
        NativeSessionStartedNotification, NativeSessionTokenUsageNotification,
        NativeSessionViewHealthChangedNotification, NativeTurnCompletedNotification,
        NativeTurnStartedNotification, NativeTurnTerminal, NativeUserInputAnswerResult,
        NativeUserInputCancelResult, NativeUserInputSettledNotification, NativeViewHealth,
        NativeWorkflowChild, NativeWorkflowItemNotification, session_usage_from_context_usage,
        session_usage_from_token_usage, turn_usage, turn_usage_from_native,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_NATIVE_TURN_STARTS: usize = 1;
const MAX_PENDING_REQUESTS: usize = 128;
const MUSE_NATIVE_TURN_TRIGGER: &str = "muse.turn/started";
const RESOLVED_REQUEST_HISTORY: usize = 128;
/// Bound on remembered already-applied live view cursors. `view/subscribe`
/// replays `(after, head]` after live facts already queued past `after`, so a
/// replayed cursor must skip normalization instead of committing a second
/// time (the Store rejects the repeated native id and the source latches).
/// The bound covers the replay window; eviction only risks a repeat commit
/// for cursors older than a very long turn, which gaps the same way.
const APPLIED_VIEW_HISTORY: usize = 512;
/// Bound on persisted native file-change hints per completion, mirroring the
/// Codex adapter's bound exactly.
const MAX_CHANGED_PATHS: usize = 1_000;
/// Stored bytes fetched by the single `item/readOutput` patch read. The page
/// carries only patch headers downstream, so 128 KiB covers a large
/// multi-file patch while keeping the transient (never persisted) read small;
/// anything past the first page stays covered by `changed_paths_truncated`.
const PATCH_READ_BYTES: i64 = 128 * 1024;
/// Deadline for that one read. The event actor processes the next item only
/// after this read resolves, so the bound is tighter than the control-plane
/// RPC timeout; expiry keeps today's empty-paths coverage gap instead of
/// stalling the loop behind a hung host.
const PATCH_READ_DEADLINE: Duration = Duration::from_secs(10);

/// Already-applied live view cursors, newest last. A cursor is remembered
/// only after its notification normalized successfully, so a replayed copy
/// arriving later (same cursor, same fact) skips normalization while the
/// first application keeps the exact session/turn binding and conflict
/// checks. Extraction already scopes cursors to the owned session.
#[derive(Default)]
struct AppliedViewCursors {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl AppliedViewCursors {
    fn contains(&self, cursor: &str) -> bool {
        self.seen.contains(cursor)
    }

    fn insert(&mut self, cursor: String) {
        if self.seen.insert(cursor.clone()) {
            self.order.push_back(cursor);
            while self.order.len() > APPLIED_VIEW_HISTORY {
                if let Some(expired) = self.order.pop_front() {
                    self.seen.remove(&expired);
                }
            }
        }
    }
}

/// Identity of one applied view fact: the cursor names a view revision, so
/// the method plus the item (id, revision) or turn identity distinguishes
/// facts that share it. A replayed copy of the same fact yields the same key.
fn applied_fact_key(method: &str, cursor: &str, params: &Value) -> String {
    let item = params.get("item");
    let field = |value: Option<&Value>| match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        _ => String::new(),
    };
    let item_id = field(item.and_then(|item| item.get("itemId")));
    let revision = field(item.and_then(|item| item.get("revision")));
    let turn_id = field(
        params
            .get("turnId")
            .or_else(|| item.and_then(|item| item.get("turnId"))),
    );
    format!("{method}\u{1f}{cursor}\u{1f}{item_id}\u{1f}{revision}\u{1f}{turn_id}")
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub native_drained: bool,
    pub ready: bool,
    pub session_id: Option<String>,
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub last_turn: Option<TurnFacts>,
    pub provider_model_turn: Option<String>,
    pub model_mismatch: Option<(String, String)>,
    model_stop_at: Option<Instant>,
    /// Confirmed native endpoint exit observed after the last event drain:
    /// exit code plus bounded stderr tail. Endpoint-scoped, never a turn
    /// verdict: an unknown turn next to a recorded exit names the crash
    /// that ended observation instead of proving nothing.
    pub native_exit: Option<String>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    /// Lost live view; a private history read cannot re-arm that producer.
    pub view_unavailable: bool,
    /// A turn settled from a bounded durable-view readback while the live
    /// projection was unavailable (k167b): the live source is still
    /// unproven, so the next send keeps its gate until the owned session
    /// proves idle with a fresh `session/read`, even when the actor
    /// readback itself succeeds. Set at terminal commit, cleared by a live
    /// terminal or by `reestablish_idle` after exact idle proof; never
    /// cleared by `clear_stale_idle_latch`.
    pub needs_idle_proof: bool,
    /// The owned native host failed its MCP startup audit (E47): a
    /// host-level posture that persists across turns on that host. Latched
    /// at terminal commit and only cleared by a fresh
    /// host (Agent restart) drops it. While set, follow-up sends are
    /// rejected with the restart action instead of dispatching.
    pub mcp_startup_audit_failed: bool,
    pub view_unavailable_reason: Option<NativeHistoryNoneReason>,
    pub stopped: bool,
    /// Local observation counter: bumped on every stream observation that
    /// touches the pending-request set. An in-memory race guard for stale
    /// request snapshots (resume pointers, listPending pulls), never a
    /// persisted epoch.
    pub observation: u64,
}

impl EventStatus {
    /// Clear a stale idle latch left by a settled turn (k167 E44). Historical
    /// gaps stay in the journal; only the live latch resets so the next
    /// follow-up send can re-prove observation. Returns true when anything
    /// was cleared. Refuses to clear while a turn is still active. Never
    /// clears `needs_idle_proof` (a readback-settled terminal keeps its
    /// send gate until a fresh `session/read` proves the owned session
    /// idle) or `mcp_startup_audit_failed` (host-scoped E47 posture).
    pub(crate) fn clear_stale_idle_latch(&mut self) -> bool {
        if self.active_turn_id.is_some() || self.active_command_id.is_some() {
            return false;
        }
        let mut cleared = false;
        if self.view_unavailable {
            self.view_unavailable = false;
            cleared = true;
        }
        if self.view_unavailable_reason.is_some() {
            self.view_unavailable_reason = None;
            cleared = true;
        }
        if self.source_unknown {
            self.source_unknown = false;
            cleared = true;
        }
        if self.failure.as_deref().is_some_and(is_view_related_failure) {
            self.failure = None;
            cleared = true;
        }
        cleared
    }
}

/// View-related failure strings that must not block the next idle follow-up
/// send once its turn has settled (k167 E44). Anything else is left intact.
///
/// The MCP startup-audit failure is deliberately not listed here: that
/// failure names a host-level runtime posture, not a stale live-view latch,
/// so the dedicated host-scoped latch keeps blocking until the Agent
/// restarts on a fresh native host.
pub(crate) fn is_view_related_failure(reason: &str) -> bool {
    reason.contains("native view is unavailable")
        || reason.contains("native observation is unresolved")
        || reason.contains("native view readback is unavailable")
        || reason.contains("native observation remains unresolved")
}

/// Host-level MCP posture failure (E47): the owned native host failed its
/// MCP startup audit, so MCP stays disabled for that host's lifetime.
/// Never a stale live-view latch: it is latched host-scoped at terminal
/// commit and keeps rejecting follow-up sends until the host is replaced.
pub(crate) const MCP_STARTUP_AUDIT_CAUSE: &str = "MCP startup audit failed";

/// Whether free-form native failure text names the MCP startup-audit
/// posture (E47). Matches the fixed native substring, never the redacted
/// surroundings, so secret redaction cannot hide the posture.
pub(crate) fn is_mcp_startup_audit_failure(text: &str) -> bool {
    text.contains(MCP_STARTUP_AUDIT_CAUSE)
}

/// Exact idle proof for a follow-up send (k167b, #427): the raw
/// `session/read` value must name the exact session, report status
/// `idle`, and carry an explicitly null `activeTurnId`. A `running`
/// status with a missing `activeTurnId` deserializes to the same `None`
/// as an explicit null, so the raw value is checked for presence first:
/// omission is never idle proof. The history envelope must also be
/// healthy (no unavailable reason): an unavailable history cannot prove
/// the live source idle.
pub(crate) fn session_read_proves_idle(value: &Value, session_id: &str) -> bool {
    let Some(active) = value
        .get("session")
        .and_then(|session| session.get("activeTurnId"))
    else {
        return false;
    };
    if !active.is_null() {
        return false;
    }
    let Ok(readback) = serde_json::from_value::<NativeSessionReadResult>(value.clone()) else {
        return false;
    };
    readback.session.session_id == session_id
        && readback.session.status == "idle"
        && readback.history.unavailable_reason().is_none()
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TurnFacts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    pub turn_id: String,
    pub status: Option<String>,
    pub final_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_error: Option<String>,
    pub terminal: bool,
}

pub(crate) struct EventConfig {
    pub credential: String,
    pub agent_id: String,
    pub generation: i64,
    pub secrets: Vec<String>,
    pub model_hold_settings_path: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct EventHandle {
    actor: EventActor<Control, EventStatus>,
}

enum Control {
    ViewReadback {
        session_id: String,
        reason: Option<NativeHistoryNoneReason>,
        reply: oneshot::Sender<Result<EventStatus>>,
    },
    Ready {
        native_scope: String,
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateTurn {
        command_id: String,
        native_command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
        effort: Option<String>,
        model: Option<String>,
        reply: oneshot::Sender<Result<()>>,
    },
    ExpectTurnStart {
        command_id: String,
        native_command_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    RejectTurnStart {
        native_command_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    UnknownTurnStart {
        native_command_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    ReplyRequest {
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
        reply: oneshot::Sender<Result<Value>>,
    },
    CheckTurn {
        turn_id: String,
        reply: oneshot::Sender<bool>,
    },
    AdoptPulled {
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    BeginStop {
        reply: oneshot::Sender<Result<()>>,
    },
    EndStop {
        reply: oneshot::Sender<Result<()>>,
    },
    RecordNativeExit {
        text: String,
        reply: oneshot::Sender<()>,
    },
    ReestablishIdle {
        session_id: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Written,
    Unknown,
}

struct PendingNativeRequest {
    request: CorePending,
    reply_command_id: Option<String>,
    /// Native `commandId` of the outstanding reply while Writing, retained
    /// while its outcome stays Unknown so a later `approval/resolved` can
    /// prove (via `decidedByCommandId`) that this exact decide applied.
    /// Retained on every Unknown outcome (admitted-unproven and ambiguous
    /// alike); cleared only when the entry returns to Available or settles
    /// as Written for a non-decide reply.
    reply_native_command_id: Option<String>,
    reply_state: NativeReplyState,
    /// Observation counter at the last stream update of this entry.
    observed_at: u64,
}

/// Exact binding captured when an `approval/decide` is written, used to
/// qualify the control result against authoritative view frames. Generation
/// rides on the live turn: the captured turn is active at send time, and
/// only frames naming this exact session, turn, and approval can qualify.
#[derive(Clone)]
struct DecideCapture {
    session_id: String,
    turn_id: String,
    approval_id: String,
    decided_seq: i64,
    native_command_id: String,
}

/// Whitelisted view diagnostics for a decide qualification: whether a
/// terminal resolution for this exact decide was observed, and whether it
/// carried a decider id that matched ours. Booleans only, never payload.
#[derive(Clone, Default)]
struct DecideViewDiag {
    resolved_seen: bool,
    decider_present: bool,
    decider_match: bool,
    resolved_decisive: bool,
}

/// Exact decider proof: the resolution names this generation's outstanding
/// decide command id, non-empty on both sides. Anything else — missing,
/// blank, or another decider — proves nothing about our write.
fn resolved_decider_matches(decided_by: Option<&str>, ours: &str) -> bool {
    !ours.trim().is_empty() && decided_by.is_some_and(|id| !id.trim().is_empty() && id == ours)
}

/// How the decide RPC settled before view qualification: a clean admission
/// ack still needs view proof, while an ambiguous outcome additionally
/// carries the RPC error code and the reason to keep if unproven.
enum DecideRpcOutcome<'a> {
    Admitted,
    Ambiguous {
        error_code: Option<i64>,
        unproven_reason: &'a str,
    },
}

/// One decide awaiting view qualification: the exact sent binding plus the
/// control identities the result is settled under.
struct DecideQualification<'a> {
    command_id: &'a str,
    request_id: &'a str,
    turn_id: &'a str,
    capture: &'a DecideCapture,
    deadline: Instant,
    rpc: DecideRpcOutcome<'a>,
}

/// Attach the whitelisted decide diagnostics to a control outcome: the
/// numeric native error code when the RPC itself errored, and the
/// resolved-decider booleans when a terminal resolution was observed.
/// Nothing else is ever attached: no raw errors, payloads, or ids.
fn with_decide_diag(mut outcome: Value, error_code: Option<i64>, diag: &DecideViewDiag) -> Value {
    if let Some(fields) = outcome.as_object_mut() {
        if let Some(code) = error_code {
            fields.insert("native_error_code".to_owned(), Value::from(code));
        }
        if diag.resolved_seen {
            fields.insert(
                "resolved_decider_present".to_owned(),
                Value::from(diag.decider_present),
            );
            fields.insert(
                "resolved_decider_match".to_owned(),
                Value::from(diag.decider_match),
            );
        }
    }
    outcome
}

#[derive(Clone)]
enum BufferedNativeFrame {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Clone)]
struct BufferedFrame {
    frame: BufferedNativeFrame,
    size: usize,
}

fn buffered_frame_turn_id(frame: &BufferedNativeFrame) -> Option<&str> {
    let params = match frame {
        BufferedNativeFrame::Notification { params, .. }
        | BufferedNativeFrame::Request { params, .. } => params,
    };
    native_notification_turn_id(params)
}

fn native_notification_turn_id(params: &Value) -> Option<&str> {
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.pointer("/item/turnId").and_then(Value::as_str))
}

/// Store identities acknowledged during the active turn, independently of
/// the sink's most recent batch and the bounded view-cursor replay history.
#[derive(Default)]
struct TurnMessages {
    acknowledged: HashSet<String>,
    final_text: Option<String>,
    awaiting_model: Vec<(String, String)>,
    awaiting_bytes: usize,
}

#[derive(Default)]
struct TurnModel {
    expected: Option<String>,
    tasks: HashSet<String>,
}

impl TurnMessages {
    async fn complete(
        &mut self,
        source: &mut StreamSource<'_>,
        native_id: String,
        text: String,
        data: Value,
    ) -> Result<()> {
        if self.acknowledged.contains(&native_id) {
            // Readback may carry an older message/revision. Keep the latest
            // acknowledged text instead of replacing it from that snapshot.
            return Ok(());
        }
        // A blank or gapped reply is no terminal text: only a committed fact
        // becomes the turn's final text.
        if source.commit_message(native_id.clone(), data).await? != MessageCommit::Committed {
            return Ok(());
        }
        self.acknowledged.insert(native_id);
        self.final_text = Some(text);
        Ok(())
    }
}

struct EventProcessor {
    native: NativeClient,
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    secrets: Vec<String>,
    provider_subscription: Option<u64>,
    model_hold_settings_path: Option<PathBuf>,
    turn_model: TurnModel,
    status: EventStatus,
    buffer: VecDeque<BufferedFrame>,
    buffer_bytes: usize,
    pending_native_starts: HashMap<String, String>,
    uncertain_native_starts: HashSet<String>,
    uncertain_native_turns: HashSet<String>,
    active_native_command_id: Option<String>,
    turn_completion_usage: Option<(String, eliot_core::TurnUsage)>,
    gap_number: u64,
    accept_frames: bool,
    planned_stop: bool,
    loss_reason: Option<String>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    /// The native transport itself was lost (queue overflow, unplanned close,
    /// malformed frame, unplanned reader end): facts may be missing, so the
    /// source reports a gap once the native frames are drained.
    transport_loss: bool,
    view_stale_reported: bool,
    /// Advance only after successful processing/commit on this actor.
    /// Latest merged session-scoped snapshot from `session/tokenUsage`
    /// (cumulative counted-once totals plus estimated cost) and
    /// `session/contextUsage` (occupancy triple). Never turn expenditure.
    session_usage: Option<SessionUsage>,
    /// Exact active turn that supplied the latest sample, if any.
    session_usage_turn: Option<String>,
    /// Cursors already normalized on this actor. A replayed copy skips
    /// normalization; the first application did the binding and conflict
    /// checks. Remembered only on success, never on gaps or failures.
    applied_view_cursors: AppliedViewCursors,
    messages: TurnMessages,
    pending_requests: HashMap<String, PendingNativeRequest>,
    /// Native approval/user-input id to public pending id.
    native_index: HashMap<String, String>,
    /// Terminal resolutions by native id: a resolved request is never
    /// re-adopted from any snapshot, however new the snapshot claims to be.
    resolved_native_ids: HashSet<String>,
    resolved_native_order: VecDeque<String>,
    /// Turn-end drops by native id with the observation counter at the drop:
    /// a pulled snapshot entry is adopted only when no newer stream
    /// observation touched its id.
    dropped_native_at: HashMap<String, u64>,
    dropped_native_order: VecDeque<String>,
    /// Resume-pointer ids not yet seen on the stream: a re-issued request
    /// for one that binds to a dead turn is receipted and dropped as an
    /// orphan instead of gapping the source. Cleared silently at turn end.
    expected_requests: HashSet<String>,
    /// Pulled snapshot entries waiting for a live turn to bind to.
    stashed_entries: Vec<StashedPending>,
    /// Native scope bound at readiness: workflow-child registration needs the
    /// exact scope the parent session committed.
    native_scope: Option<String>,
    /// Workflow-child tracking by canonical key K. Children outlive the
    /// parent turn: parent completion never clears this map.
    workflow_children: HashMap<WorkflowChildKey, WorkflowChildState>,
    /// Bounded unknown markers for children whose observation was lost
    /// (overflow, rejected registration, stream end). Repeats stay silent;
    /// eviction only re-gaps, never re-hides a proven tracked child.
    workflow_unknown: HashSet<WorkflowChildKey>,
    workflow_unknown_order: VecDeque<WorkflowChildKey>,
    /// Active decide qualification while `pump_decide_proof` pumps frames.
    /// Owning paths (`resolve_approval`, `refresh_approval`,
    /// `process_request`) set the frame proof/diag fields below only after
    /// their own successful validation and required persistence; the pump
    /// reads them only after `handle_frame` returns. `None` outside a pump.
    active_decide: Option<DecideCapture>,
    active_decide_admission: bool,
    frame_decide_proof: bool,
    frame_decide_decisive: bool,
    frame_decide_diag: DecideViewDiag,
}

struct StashedPending {
    request: CorePending,
    capture: u64,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<NativeFrame>,
    stream: watch::Receiver<StreamState>,
    native: NativeClient,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let processor = EventProcessor::new(native, host, config);
    let actor = EventActor::spawn(
        CONTROL_QUEUE,
        EventStatus::default(),
        move |control_rx, status_tx| async move {
            run(frames, stream, processor, control_rx, status_tx).await;
        },
    );
    EventHandle { actor }
}

impl EventHandle {
    pub async fn wait_for_model_stop(&self) {
        let mut status = self.actor.watch().clone();
        loop {
            let deadline = status.borrow_and_update().model_stop_at;
            if let Some(deadline) = deadline {
                match timeout_at(deadline, status.changed()).await {
                    Err(_) if status.borrow().model_stop_at == Some(deadline) => return,
                    Err(_) | Ok(Ok(())) => continue,
                    Ok(Err(_)) => break,
                }
            } else if status.changed().await.is_err() {
                break;
            }
        }
        std::future::pending::<()>().await;
    }

    pub async fn view_readback(
        &self,
        session_id: String,
        reason: Option<NativeHistoryNoneReason>,
    ) -> Result<EventStatus> {
        self.send(|reply| Control::ViewReadback {
            session_id,
            reason,
            reply,
        })
        .await
    }

    /// Budget-bound `view_readback` (k167b): the queue wait and the outcome
    /// wait share the caller's request `deadline` instead of minting a
    /// fresh event deadline, so proof reads cannot outlive the budget
    /// reserved for the typed rejection.
    pub async fn view_readback_at(
        &self,
        session_id: String,
        reason: Option<NativeHistoryNoneReason>,
        deadline: Instant,
    ) -> Result<EventStatus> {
        self.send_at(
            |reply| Control::ViewReadback {
                session_id,
                reason,
                reply,
            },
            deadline,
        )
        .await
    }

    /// Budget-bound `reestablish_idle` (k167b): same shared-deadline
    /// contract as `view_readback_at`.
    pub async fn reestablish_idle_at(&self, session_id: String, deadline: Instant) -> Result<bool> {
        self.send_at(
            |reply| Control::ReestablishIdle { session_id, reply },
            deadline,
        )
        .await
    }

    pub async fn ready(
        &self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        self.send(|reply| Control::Ready {
            native_scope,
            session_id,
            effective,
            reply,
        })
        .await
    }

    pub async fn associate_turn(
        &self,
        command_id: String,
        native_command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
        effort: Option<String>,
        model: Option<String>,
    ) -> Result<()> {
        self.send(|reply| Control::AssociateTurn {
            command_id,
            native_command_id,
            turn_id,
            context,
            effort,
            model,
            reply,
        })
        .await
    }

    pub async fn expect_turn_start(
        &self,
        command_id: String,
        native_command_id: String,
    ) -> Result<()> {
        self.send(|reply| Control::ExpectTurnStart {
            command_id,
            native_command_id,
            reply,
        })
        .await
    }

    pub async fn reject_turn_start(&self, native_command_id: String) -> Result<()> {
        self.send(|reply| Control::RejectTurnStart {
            native_command_id,
            reply,
        })
        .await
    }

    pub async fn unknown_turn_start(&self, native_command_id: String) -> Result<()> {
        self.send(|reply| Control::UnknownTurnStart {
            native_command_id,
            reply,
        })
        .await
    }

    pub async fn reply_request(
        &self,
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
    ) -> Result<Value> {
        self.actor
            .call(|reply| Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            })
            .await?
    }

    /// Authoritative exact-turn gate for steer/interrupt: the check runs
    /// on the event task, serialized behind the terminal commit, so a
    /// control that arrives after the terminal batch always sees the
    /// finished turn even though the watch snapshot still lags it.
    pub async fn check_turn(&self, turn_id: &str) -> Result<bool> {
        self.actor
            .call(|reply| Control::CheckTurn {
                turn_id: turn_id.to_owned(),
                reply,
            })
            .await
    }

    pub async fn adopt_pulled(
        &self,
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
    ) -> Result<()> {
        self.send(|reply| Control::AdoptPulled {
            entries,
            expected,
            capture,
            snapshot_had_undecodable,
            reply,
        })
        .await
    }

    pub async fn begin_stop(&self) -> Result<()> {
        self.send(|reply| Control::BeginStop { reply }).await
    }

    pub async fn end_stop(&self) -> Result<()> {
        self.send(|reply| Control::EndStop { reply }).await
    }

    pub async fn close(&self) {
        let _ = self.actor.call(|reply| Control::Close { reply }).await;
    }

    pub async fn drain_native(&self) -> Result<()> {
        eliot_plugin::native_lifecycle::wait_for_native_tail(self.actor.watch(), |s| {
            s.native_drained
        })
        .await
    }

    /// Retain one confirmed native endpoint exit for the readback. Only the
    /// first death wins; terminality is never manufactured here. Best effort:
    /// a closing queue drops the record instead of failing the shutdown.
    pub async fn record_native_exit(&self, text: String) {
        let _ = self
            .actor
            .call(|reply| Control::RecordNativeExit { text, reply })
            .await;
    }

    pub fn status(&self) -> EventStatus {
        self.actor.watch().borrow().clone()
    }

    async fn send<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> Control,
    ) -> Result<T> {
        self.actor.call(build).await?
    }

    async fn send_at<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> Control,
        deadline: Instant,
    ) -> Result<T> {
        self.actor.call_by(deadline, build).await?
    }
}

impl EventProcessor {
    fn new(native: NativeClient, host: PeerClient, config: EventConfig) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
            native,
            host,
            sink,
            credential: config.credential,
            agent_id: config.agent_id,
            generation: config.generation,
            secrets: config.secrets,
            provider_subscription: None,
            model_hold_settings_path: config.model_hold_settings_path,
            turn_model: TurnModel::default(),
            status: EventStatus::default(),
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            pending_native_starts: HashMap::new(),
            uncertain_native_starts: HashSet::new(),
            uncertain_native_turns: HashSet::new(),
            active_native_command_id: None,
            turn_completion_usage: None,
            gap_number: 0,
            accept_frames: true,
            planned_stop: false,
            loss_reason: None,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            transport_loss: false,
            view_stale_reported: false,
            session_usage: None,
            session_usage_turn: None,
            applied_view_cursors: AppliedViewCursors::default(),
            messages: TurnMessages::default(),
            pending_requests: HashMap::new(),
            native_index: HashMap::new(),
            resolved_native_ids: HashSet::new(),
            resolved_native_order: VecDeque::new(),
            dropped_native_at: HashMap::new(),
            dropped_native_order: VecDeque::new(),
            expected_requests: HashSet::new(),
            stashed_entries: Vec::new(),
            native_scope: None,
            workflow_children: HashMap::new(),
            workflow_unknown: HashSet::new(),
            workflow_unknown_order: VecDeque::new(),
            active_decide: None,
            active_decide_admission: false,
            frame_decide_proof: false,
            frame_decide_decisive: false,
            frame_decide_diag: DecideViewDiag::default(),
        }
    }

    fn observe(&mut self) -> u64 {
        self.status.observation = self.status.observation.saturating_add(1);
        self.status.observation
    }

    async fn handle_control(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        control: Control,
    ) -> bool {
        match control {
            Control::ViewReadback {
                session_id,
                reason,
                reply,
            } => {
                let result = self.bind_view_session(&session_id);
                let result = match result {
                    Ok(()) => self.flush_buffered_health().await,
                    Err(error) => Err(error),
                };
                let result = match (result, reason) {
                    (Ok(()), Some(reason)) => self.view_unavailable(&session_id, reason).await,
                    (result, _) => result,
                };
                let result = result.and_then(|()| {
                    if self.status.session_id.as_deref() != Some(session_id.as_str())
                        || self.status.view_unavailable
                    {
                        Err(Error::new(
                            "unknown",
                            "Muse native observation is unresolved",
                        ))
                    } else {
                        Ok(self.status.clone())
                    }
                });
                let _ = reply.send(result);
                true
            }
            Control::Ready {
                native_scope,
                session_id,
                effective,
                reply,
            } => {
                let result = self.ready(native_scope, session_id, effective).await;
                let _ = reply.send(result);
                true
            }
            Control::AssociateTurn {
                command_id,
                native_command_id,
                turn_id,
                context,
                effort,
                model,
                reply,
            } => {
                let result = self
                    .associate_turn(
                        command_id,
                        native_command_id,
                        turn_id,
                        context,
                        effort,
                        model,
                    )
                    .await;
                let _ = reply.send(result);
                true
            }
            Control::ExpectTurnStart {
                command_id,
                native_command_id,
                reply,
            } => {
                let result = self.expect_turn_start(command_id, native_command_id);
                let _ = reply.send(result);
                true
            }
            Control::RejectTurnStart {
                native_command_id,
                reply,
            } => {
                let result = self.reject_turn_start(&native_command_id).await;
                let _ = reply.send(result);
                true
            }
            Control::UnknownTurnStart {
                native_command_id,
                reply,
            } => {
                let result = self.unknown_turn_start(&native_command_id).await;
                let _ = reply.send(result);
                true
            }
            Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            } => {
                let result = self
                    .reply_request(frames, &command_id, &request_id, &turn_id, &verb, &args)
                    .await;
                let _ = reply.send(result);
                true
            }
            Control::CheckTurn { turn_id, reply } => {
                let _ = reply.send(self.status.active_turn_id.as_deref() == Some(turn_id.as_str()));
                true
            }
            Control::AdoptPulled {
                entries,
                expected,
                capture,
                snapshot_had_undecodable,
                reply,
            } => {
                let result = self
                    .adopt_pulled(entries, expected, capture, snapshot_had_undecodable)
                    .await;
                let _ = reply.send(result);
                true
            }
            Control::BeginStop { reply } => {
                self.planned_stop = true;
                let _ = reply.send(Ok(()));
                true
            }
            Control::EndStop { reply } => {
                if self.frames_drained {
                    let result = self.finish_stop().await;
                    let _ = reply.send(result);
                } else if self.pending_end_stop.is_some() {
                    let _ = reply.send(Err(Error::new(
                        "bad_state",
                        "Muse stop is already waiting for native output drain",
                    )));
                } else {
                    self.pending_end_stop = Some(reply);
                }
                true
            }
            Control::RecordNativeExit { text, reply } => {
                // Endpoint-scoped only: the active turn keeps whatever
                // terminality the drained frames proved, and the snapshot
                // pairs it with this exit instead of proving nothing.
                if self.status.native_exit.is_none() {
                    self.status.native_exit = Some(text);
                }
                let _ = reply.send(());
                true
            }
            Control::ReestablishIdle { session_id, reply } => {
                let result = self.reestablish_idle(&session_id);
                let _ = reply.send(result);
                true
            }
            Control::Close { reply } => {
                if let Some(pending) = self.pending_end_stop.take() {
                    let _ = pending.send(Err(Error::new(
                        "unknown",
                        "Muse native event queue closed before stop settlement",
                    )));
                }
                let _ = reply.send(());
                false
            }
        }
    }

    async fn handle_stream_state(&mut self, current: StreamState) {
        if current == StreamState::Open {
            return;
        }
        match current {
            StreamState::Closed => {
                if !self.planned_stop {
                    self.defer_gap("Muse session host event stream closed");
                    self.transport_loss = true;
                }
            }
            StreamState::Overflow => {
                self.defer_gap("Muse session host event queue overflowed");
                self.transport_loss = true;
            }
            StreamState::Malformed(fault) => {
                self.defer_gap(format!(
                    "Muse session host stream fault ({})",
                    fault.describe()
                ));
                self.transport_loss = true;
            }
            StreamState::Open => {}
        }
    }

    fn bind_view_session(&mut self, session_id: &str) -> Result<()> {
        if session_id.trim().is_empty()
            || self
                .status
                .session_id
                .as_deref()
                .is_some_and(|owned| owned != session_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Muse view-health readback named another session",
            ));
        }
        self.status.session_id = Some(session_id.to_owned());
        Ok(())
    }

    fn reestablish_idle(&mut self, session_id: &str) -> Result<bool> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse idle readback named another session",
            ));
        }
        if self.writes_blocked {
            return Ok(false);
        }
        let had_turn = self.status.active_turn_id.take().is_some();
        self.status.active_command_id = None;
        self.active_native_command_id = None;
        self.turn_completion_usage = None;
        if had_turn {
            self.pending_requests.clear();
            self.observe();
        }
        let cleared = self.status.clear_stale_idle_latch();
        let gate_held = std::mem::replace(&mut self.status.needs_idle_proof, false);
        if cleared {
            self.gap_reported = false;
            self.view_stale_reported = false;
        }
        Ok(had_turn || cleared || gate_held)
    }

    /// Record the native projection gap (E46). Recovery, retries and turn
    /// settlement are daemon responsibilities; this translator only latches
    /// the native evidence loss and keeps subsequent snapshot reads truthful.
    async fn view_unavailable(
        &mut self,
        session_id: &str,
        reason: NativeHistoryNoneReason,
    ) -> Result<()> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse view-health readback named another session",
            ));
        }
        let new_episode = !self.status.view_unavailable;
        let reason_label = format!("{reason:?}");
        self.status.view_unavailable = true;
        self.status.view_unavailable_reason = Some(reason);
        self.status.source_unknown = true;
        if new_episode && !self.writes_blocked {
            // The projection failure is distinct from an earlier transport gap.
            self.gap_reported = false;
            self.record_gap(&format!(
                "{MUSE_VIEW_UNAVAILABLE_REASON_PREFIX} ({reason_label})"
            ))
            .await?;
        }
        self.report_view_stale().await;
        Ok(())
    }

    async fn handle_frame(&mut self, frame: Option<NativeFrame>) -> bool {
        match frame {
            Some(NativeFrame::Request { id, method, params }) => {
                if !self.status.ready || self.status.active_turn_id.is_none() {
                    self.buffer_frame(BufferedNativeFrame::Request { id, method, params });
                } else if let Err(error) = self.process_request(id, method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported
                        && !self.writes_blocked
                        && error.failure_where.as_deref() != Some("plugin.events.put")
                    {
                        let _ = self
                            .record_gap("Muse native request could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(*error.message);
                    }
                }
            }
            Some(NativeFrame::Notification { method, params }) => {
                if method == "log/record" && provider::signal(&params).is_none() {
                    return true;
                }
                // Workflow children ride the parent stream but outlive the
                // parent turn: route them before active-parent-turn buffering.
                let bypass_buffer = should_bypass_parent_buffer(
                    self.status.ready,
                    self.status.session_id.is_some(),
                    &method,
                    &params,
                );
                if !bypass_buffer && self.should_buffer(&method, &params) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported
                        && !self.writes_blocked
                        && error.failure_where.as_deref() != Some("plugin.events.put")
                    {
                        let _ = self
                            .record_gap("Muse notification could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(*error.message);
                    }
                }
            }
            None => {
                self.frames_drained = true;
                self.accept_frames = false;
                if !self.buffer.is_empty() {
                    self.buffer.clear();
                    self.buffer_bytes = 0;
                    self.defer_gap("Muse native events remained unassociated at stream end");
                }
                if !self.pending_requests.is_empty() {
                    self.defer_gap("Muse native requests remained unresolved at stream end");
                }
                if !self.planned_stop && self.loss_reason.is_none() {
                    self.defer_gap("Muse native event reader closed");
                }
                if !self.planned_stop {
                    self.transport_loss = true;
                }
                self.settle_workflow_children().await;
                self.report_gap_if_queue_empty(true).await;
                self.finish_pending_end_stop().await;
            }
        }
        true
    }

    fn defer_gap(&mut self, reason: impl Into<String>) {
        if self.loss_reason.is_none() {
            self.loss_reason = Some(reason.into());
        }
    }

    async fn report_gap_if_queue_empty(&mut self, queue_empty: bool) {
        if queue_empty
            && !self.gap_reported
            && !self.writes_blocked
            && let Some(reason) = self.loss_reason.clone()
        {
            let _ = self.stream_source().record_gap(&reason).await;
        }
        if queue_empty && self.transport_loss {
            self.report_view_stale().await;
        }
    }

    fn should_buffer(&self, method: &str, params: &Value) -> bool {
        if method == "session/viewHealthChanged" {
            return self.status.session_id.is_none();
        }
        if method == "turn/started" {
            if !self.status.ready {
                return true;
            }
            let Some(command_id) = params.get("commandId").and_then(Value::as_str) else {
                return false;
            };
            return self.pending_native_starts.contains_key(command_id);
        }
        if native_notification_turn_id(params)
            .is_some_and(|turn_id| self.uncertain_native_turns.contains(turn_id))
        {
            return false;
        }
        matches!(
            method,
            "turn/completed"
                | "log/record"
                | "item/started"
                | "item/updated"
                | "item/completed"
                | "approval/resolved"
                | "approval/updated"
                | "userInput/settled"
        ) && (!self.status.ready || self.status.active_turn_id.is_none())
    }

    fn expect_turn_start(&mut self, command_id: String, native_command_id: String) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || self.status.active_turn_id.is_some()
            || command_id.trim().is_empty()
            || native_command_id.trim().is_empty()
            || self.pending_native_starts.len() >= MAX_PENDING_NATIVE_TURN_STARTS
            || self.uncertain_native_starts.contains(&native_command_id)
        {
            return Err(Error::new(
                "unknown",
                "Muse could not fence the exact native turn/start before dispatch",
            ));
        }
        self.pending_native_starts
            .insert(native_command_id, command_id);
        Ok(())
    }

    async fn reject_turn_start(&mut self, native_command_id: &str) -> Result<()> {
        self.pending_native_starts.remove(native_command_id);
        let (found, turn_id) = self.discard_buffered_native_start(native_command_id);
        if found {
            self.uncertain_native_starts
                .insert(native_command_id.to_owned());
            if let Some(turn_id) = turn_id {
                self.uncertain_native_turns.insert(turn_id);
            }
            self.record_gap(
                "Muse emitted the exact turn/start after proving it was not dispatched",
            )
            .await?;
        }
        Ok(())
    }

    async fn unknown_turn_start(&mut self, native_command_id: &str) -> Result<()> {
        if self.active_native_command_id.as_deref() == Some(native_command_id)
            && !self.pending_native_starts.contains_key(native_command_id)
        {
            return Ok(());
        }
        let expected = self
            .pending_native_starts
            .remove(native_command_id)
            .is_some();
        if !expected && !self.uncertain_native_starts.contains(native_command_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse uncertain turn/start did not match its registered native command",
            ));
        }
        self.uncertain_native_starts
            .insert(native_command_id.to_owned());
        let (found, turn_id) = self.discard_buffered_native_start(native_command_id);
        if found {
            if let Some(turn_id) = turn_id {
                self.uncertain_native_turns.insert(turn_id);
            }
            self.record_gap(
                "Muse turn/start result is unknown; buffered native facts were discarded",
            )
            .await?;
        }
        Ok(())
    }

    fn discard_buffered_native_start(&mut self, native_command_id: &str) -> (bool, Option<String>) {
        let turn_id = self.buffer.iter().find_map(|buffered| {
            let BufferedNativeFrame::Notification { method, params } = &buffered.frame else {
                return None;
            };
            (method == "turn/started"
                && params.get("commandId").and_then(Value::as_str) == Some(native_command_id))
            .then(|| {
                params
                    .get("turnId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten()
        });
        let found_start = self.buffer.iter().any(|buffered| {
            matches!(&buffered.frame,
                BufferedNativeFrame::Notification { method, params }
                    if method == "turn/started"
                        && params.get("commandId").and_then(Value::as_str)
                            == Some(native_command_id))
        });
        if !found_start {
            return (false, None);
        }
        let mut retained = VecDeque::with_capacity(self.buffer.len());
        let mut retained_bytes = 0usize;
        while let Some(buffered) = self.buffer.pop_front() {
            let start_matches = matches!(&buffered.frame,
                BufferedNativeFrame::Notification { method, params }
                    if method == "turn/started"
                        && params.get("commandId").and_then(Value::as_str)
                            == Some(native_command_id));
            let turn_matches = turn_id
                .as_deref()
                .is_some_and(|turn_id| buffered_frame_turn_id(&buffered.frame) == Some(turn_id));
            if !start_matches && !turn_matches {
                retained_bytes = retained_bytes.saturating_add(buffered.size);
                retained.push_back(buffered);
            }
        }
        self.buffer = retained;
        self.buffer_bytes = retained_bytes;
        (true, turn_id)
    }

    fn buffer_frame(&mut self, frame: BufferedNativeFrame) {
        let encoded = match &frame {
            BufferedNativeFrame::Notification { method, params } => {
                serde_json::to_vec(&json!({"method": method, "params": params}))
            }
            BufferedNativeFrame::Request { id, method, params } => {
                serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))
            }
        };
        let size = encoded
            .map(|bytes| bytes.len())
            .unwrap_or_else(|_| EARLY_EVENT_BYTES.saturating_add(1));
        if self.buffer.len() >= EARLY_EVENT_LIMIT
            || self.buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            self.buffer.clear();
            self.buffer_bytes = 0;
            self.defer_gap("Muse early-event buffer exceeded its bound");
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedFrame { frame, size });
    }

    async fn ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        if self.status.ready || session_id.trim().is_empty() || !effective.is_object() {
            return Err(Error::new(
                "invalid",
                "Muse readiness requires one exact session and effective settings",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|observed| observed != session_id)
        {
            self.record_gap("Muse start response disagreed with session/started")
                .await?;
            return Err(Error::new(
                "source_integrity",
                "Muse session identity did not match its start notification",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.clone());
        self.native_scope = Some(native_scope.clone());
        self.flush_buffered_health().await?;
        if self.status.view_unavailable {
            return Err(Error::new(
                "unknown",
                "Muse native view is unavailable before readiness",
            ));
        }
        self.provider_subscription = Some(
            provider::subscribe(
                &self.native,
                &session_id,
                Instant::now() + Duration::from_secs(5),
            )
            .await?,
        );
        let native_id = format!("muse:{session_id}:session.ready");
        self.commit_event(
            "session.ready",
            native_id,
            json!({
                "native_scope": native_scope,
                "native_ref": session_id,
                "effective": effective,
            }),
        )
        .await?;
        self.status.ready = true;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    async fn associate_turn(
        &mut self,
        command_id: String,
        native_command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
        effort: Option<String>,
        model: Option<String>,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || native_command_id.trim().is_empty()
            || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Muse turn association requires a ready exact session and turn",
            ));
        }
        if self.status.active_turn_id.is_some() {
            return Err(Error::new(
                "busy",
                "Muse already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        if self.pending_native_starts.get(&native_command_id) != Some(&command_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse turn association did not match its registered native command",
            ));
        }
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        self.active_native_command_id = Some(native_command_id.clone());
        if self
            .turn_completion_usage
            .as_ref()
            .is_none_or(|(sample_turn_id, _)| sample_turn_id != &turn_id)
        {
            self.turn_completion_usage = None;
        }
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = event_id(session_id, &turn_id, &turn_id, "turn.started")?;
        let mut data = json!({"command_id": command_id, "turn_id": turn_id});
        // Report the effort the native turn request actually carried, so the
        // Store readback shows the resolved value instead of the overlay's
        // absent override. Omitted when unknown: nothing is invented.
        if let Some(effort) = effort.as_deref().filter(|effort| !effort.trim().is_empty()) {
            data["effort"] = json!(effort);
        }
        self.turn_model = TurnModel {
            expected: model,
            ..TurnModel::default()
        };
        self.status.provider_model_turn = None;
        self.status.model_mismatch = None;
        if !context.is_empty() {
            data["context_outcomes"] = json!(next_input_outcomes(&context, &command_id));
        }
        self.commit_event("turn.started", native_id, data).await?;
        self.pending_native_starts.remove(&native_command_id);
        self.messages = TurnMessages::default();
        self.flush_buffer().await?;
        self.apply_stashed_entries().await?;
        Ok(())
    }

    async fn start_native_turn(
        &mut self,
        notification: NativeTurnStartedNotification,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.active_turn_id.is_some()
            || notification.command_id.trim().is_empty()
            || notification.turn_id.trim().is_empty()
        {
            return self
                .record_gap("Muse native turn/start did not prove one exact idle-session turn")
                .await;
        }
        let session_id = notification.session_id;
        let turn_id = notification.turn_id;
        self.status.active_command_id = None;
        self.status.active_turn_id = Some(turn_id.clone());
        self.active_native_command_id = Some(notification.command_id);
        self.turn_completion_usage = None;
        let native_id = event_id(&session_id, &turn_id, &turn_id, "turn.started")?;
        self.commit_event(
            "turn.started",
            native_id,
            json!({
                "turn_id": turn_id,
                "native_trigger": MUSE_NATIVE_TURN_TRIGGER,
            }),
        )
        .await?;
        self.messages = TurnMessages::default();
        self.flush_buffer().await?;
        self.apply_stashed_entries().await?;
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        self.flush_buffered_health().await?;
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            let should_buffer = match &frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    !should_bypass_parent_buffer(
                        self.status.ready,
                        self.status.session_id.is_some(),
                        method,
                        params,
                    ) && self.should_buffer(method, params)
                }
                BufferedNativeFrame::Request { .. } => {
                    !self.status.ready || self.status.active_turn_id.is_none()
                }
            };
            if should_buffer {
                self.buffer.push_front(frame);
                self.buffer_bytes = self
                    .buffer_bytes
                    .saturating_add(self.buffer.front().map_or(0, |item| item.size));
                break;
            }
            // Buffered flushes already run inside frame/control processing,
            // so they must not re-enter the live queues.
            self.process_buffered_frame(frame.frame).await?;
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    /// Session health is independent of turn association. Keep every request
    /// and other deferred frame in its original order, including on errors.
    async fn flush_buffered_health(&mut self) -> Result<()> {
        if self.status.session_id.is_none() {
            return Ok(());
        }
        while let Some(index) = self.buffer.iter().position(|frame| {
            matches!(
                &frame.frame, BufferedNativeFrame::Notification { method, .. }
                    if method == "session/viewHealthChanged"
            )
        }) {
            let frame = self
                .buffer
                .remove(index)
                .ok_or_else(|| Error::new("internal", "Muse buffered health frame disappeared"))?;
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            // Buffered flushes already run inside frame/control processing,
            // so they must not re-enter the live queues.
            self.process_buffered_frame(frame.frame).await?;
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    async fn process_buffered_frame(&mut self, frame: BufferedNativeFrame) -> Result<()> {
        let result = match frame {
            BufferedNativeFrame::Notification { method, params } => {
                self.process_notification(method, params).await
            }
            BufferedNativeFrame::Request { id, method, params } => {
                self.process_request(id, method, params).await
            }
        };
        if let Err(error) = result {
            // The host settles a definite write rejection. Preserve its
            // structured error instead of inventing an unknown outcome.
            if error.failure_where.as_deref() == Some("plugin.events.put") {
                return Err(error);
            }
            self.status.source_unknown = true;
            if !self.gap_reported
                && !self.writes_blocked
                && error.failure_where.as_deref() != Some("plugin.events.put")
            {
                let _ = self
                    .record_gap("Buffered Muse native frame could not be normalized")
                    .await;
            }
            if self.status.failure.is_none() {
                self.status.failure = Some(*error.message);
            }
            return Err(Error::new(
                "unknown",
                "Buffered Muse event outcome is unresolved",
            ));
        }
        Ok(())
    }

    /// Server-initiated must-answer requests. `approval/request` and
    /// `userInput/request` are presented (transport receipt `{}`) and opened
    /// durably; the decision/answer travels later as `approval/decide`,
    /// `userInput/answer` or `userInput/cancel` on the command lane. Unknown
    /// methods are rejected with a typed error, never an invented default
    /// reply, and gap the source: the native turn may be waiting on a request
    /// this slice cannot present.
    async fn process_request(&mut self, id: Value, method: String, params: Value) -> Result<()> {
        let session_id = self
            .status
            .session_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Muse request has no owned session"))?;
        let turn_id = self
            .status
            .active_turn_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Muse request has no associated turn"))?;
        let session_id_owned = session_id.to_owned();
        let request = match describe_request(&method, &params, session_id, turn_id) {
            Ok(request) => request,
            Err(error) if error.code == "source_integrity" => {
                // A scope mismatch for a resume-expected id is a routine
                // orphan (the dead generation's turn): receipt it and drop it
                // instead of gapping the source for another generation's past.
                if let Some(native_id) = orphan_request_id(&method, &params)
                    && self.expected_requests.remove(native_id.as_str())
                {
                    // The receipt is fire-and-forget by protocol: a lost
                    // write just means the host re-issues the orphan later,
                    // which drops the same way.
                    let _ = self
                        .native
                        .reply_server_request(id, json!({}), Instant::now() + EVENT_DEADLINE)
                        .await;
                    return Ok(());
                }
                let gap_reason = native_request_gap_reason(
                    &method,
                    &error.code,
                    &params,
                    session_id,
                    turn_id,
                    self.status.active_turn_id.is_some(),
                );
                return self
                    .reject_request(id, &method, &params, error, gap_reason)
                    .await;
            }
            Err(error) => {
                let gap_reason = native_request_gap_reason(
                    &method,
                    &error.code,
                    &params,
                    session_id,
                    turn_id,
                    self.status.active_turn_id.is_some(),
                );
                return self
                    .reject_request(id, &method, &params, error, gap_reason)
                    .await;
            }
        };
        // The receipt acknowledges presentation only (SS5.3.3): it changes no
        // state and the decision travels as a command.
        if self
            .native
            .reply_server_request(id.clone(), json!({}), Instant::now() + EVENT_DEADLINE)
            .await
            .is_err()
        {
            self.defer_gap("Muse native request receipt was not written");
            return Ok(());
        }
        let request_id = request.id.clone();
        if let Some(native_id) = pending_native_id(&request)
            && self.resolved_native_ids.contains(native_id)
        {
            self.observe();
            return self
                .record_gap("Muse native request was re-issued after its resolution")
                .await;
        }
        // Re-issue of a retained open: an identical offer is idempotent, a
        // changed approval offer replaces the in-memory snapshot (the Store
        // keeps the opened snapshot; replies always decide from the refreshed
        // offer and its current requirement).
        let reissued = self.pending_requests.contains_key(&request_id);
        if reissued {
            let same = request.kind != "approval"
                || self
                    .pending_requests
                    .get(&request_id)
                    .is_some_and(|existing| {
                        same_approval_offer(&existing.request.details, &params)
                    });
            let observed = self.observe();
            let (prior, newer) = self
                .pending_requests
                .get(&request_id)
                .map(|existing| {
                    (
                        existing.reply_state,
                        !same && is_newer_approval_requirement(&existing.request.details, &params),
                    )
                })
                .unwrap_or((NativeReplyState::Available, false));
            if same || !newer {
                if let Some(existing) = self.pending_requests.get_mut(&request_id) {
                    existing.observed_at = observed;
                }
                if let Some(native_id) = orphan_request_id(&method, &params) {
                    self.expected_requests.remove(native_id.as_str());
                }
                return Ok(());
            }
            let refreshed = if let Some(existing) = self.pending_requests.get_mut(&request_id) {
                existing.observed_at = observed;
                if refresh_offer_from_raw(&mut existing.request.details, &params).is_err() {
                    None
                } else {
                    if existing.reply_state == NativeReplyState::Written {
                        existing.reply_state = NativeReplyState::Available;
                        existing.reply_command_id = None;
                        existing.reply_native_command_id = None;
                    }
                    Some(existing.request.clone())
                }
            } else {
                None
            };
            let Some(refreshed) = refreshed else {
                return self
                    .record_gap("Muse re-issued approval omitted its refreshed offer")
                    .await;
            };
            if matches!(
                prior,
                NativeReplyState::Available | NativeReplyState::Written
            ) && !self.commit_offer_update(&refreshed).await?
            {
                return Ok(());
            }
            if let (Some(seq), Some(approval_id)) = (
                refreshed.details.get("offer_seq").and_then(Value::as_i64),
                refreshed.details.get("approval_id").and_then(Value::as_str),
            ) {
                self.note_decide_offer_proof(
                    session_id_owned.as_str(),
                    refreshed.turn_id.as_str(),
                    approval_id,
                    seq,
                );
            }
            if let Some(native_id) = orphan_request_id(&method, &params) {
                self.expected_requests.remove(native_id.as_str());
            }
            return Ok(());
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.defer_gap("Muse pending native request bound was exceeded");
            return Ok(());
        }
        let command_id = self.status.active_command_id.clone();
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let Ok((public_kind, public_details)) = public_request_view(&request) else {
            self.defer_gap("Muse public request view could not be normalized");
            return Ok(());
        };
        let mut data = json!({
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if request.kind == "approval"
            && let Some(raw_args) = params.get("rawArgs").and_then(Value::as_str)
            && let Ok(input) = serde_json::from_str::<Value>(raw_args)
            && let Some(command) = input.get("command").and_then(Value::as_str)
        {
            data[eliot_core::COMMAND_GUARD_CONTEXT_FIELD] = json!({"commands": [command]});
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse normalized request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&request_id, "request.opened", "muse", "Muse") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.defer_gap("Muse native request event identity exceeded its bound");
                return Ok(());
            }
        };
        self.commit_event("request.opened", native_id, data).await?;
        let open_proof: Option<(String, i64, String)> = if request.kind == "approval" {
            match (
                request.details.get("offer_seq").and_then(Value::as_i64),
                request.details.get("approval_id").and_then(Value::as_str),
            ) {
                (Some(seq), Some(approval_id)) => {
                    Some((request.turn_id.clone(), seq, approval_id.to_owned()))
                }
                _ => None,
            }
        } else {
            None
        };
        let observed = self.observe();
        if let Some(native_id) = pending_native_id(&request) {
            self.native_index
                .insert(native_id.to_owned(), request_id.clone());
            self.expected_requests.remove(native_id);
        }
        self.pending_requests.insert(
            request_id.clone(),
            PendingNativeRequest {
                request,
                reply_command_id: None,
                reply_native_command_id: None,
                reply_state: NativeReplyState::Available,
                observed_at: observed,
            },
        );
        if let Some((open_turn, seq, approval_id)) = open_proof {
            self.note_decide_offer_proof(
                session_id_owned.as_str(),
                open_turn.as_str(),
                approval_id.as_str(),
                seq,
            );
        }
        Ok(())
    }

    async fn reject_request(
        &mut self,
        id: Value,
        method: &str,
        params: &Value,
        error: Error,
        gap_reason: String,
    ) -> Result<()> {
        let rejected = self
            .native
            .reject_unsupported_request(id, method, params, Instant::now() + EVENT_DEADLINE)
            .await;
        if error.code == "unsupported" {
            // Unknown request family: rejected with a typed error, and the
            // source gaps — the native turn may be waiting on a request this
            // slice cannot present.
            self.defer_gap("Muse native request method is not connected");
        } else if rejected.is_err() {
            self.defer_gap("Muse native request rejection was not written");
        } else {
            self.defer_gap(gap_reason);
        }
        Ok(())
    }

    /// Pump live native frames through the existing frame path while an
    /// `approval/decide` control result stays unqualified, watching for the
    /// authoritative view proof of this exact decide: a terminal resolution
    /// naming our native command id, or — only after a clean admission ack —
    /// an offer that advanced past our decided requirement. Every drained
    /// frame is processed normally in arrival order (`handle_frame` first);
    /// nothing is dropped and no pre-parse of the frame is consulted.
    /// Proof is recorded only by the owning paths (`resolve_approval`,
    /// `refresh_approval`, `process_request`) after their own successful
    /// validation and required persistence, and read only after that frame
    /// has been handled. A rejected frame, a gap, or a failed persistence
    /// therefore never counts as Applied. Returns true once the decide is
    /// proven applied. The single absolute deadline (captured before the
    /// write, never renewed) and stream EOF both end the wait unproven.
    async fn pump_decide_proof(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        capture: &DecideCapture,
        deadline: Instant,
        admission_proven: bool,
        diag: &mut DecideViewDiag,
    ) -> bool {
        self.active_decide = Some(capture.clone());
        self.active_decide_admission = admission_proven;
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        self.frame_decide_diag = DecideViewDiag::default();
        let proven = 'outer: loop {
            while Instant::now() < deadline {
                let Ok(frame) = frames.try_recv() else {
                    break;
                };
                let frame_proven = self.pump_one_decide_frame(frames, frame, diag).await;
                if frame_proven {
                    break 'outer true;
                }
                if self.frame_decide_decisive {
                    break 'outer false;
                }
            }
            if Instant::now() >= deadline {
                break false;
            }
            match timeout_at(deadline, frames.recv()).await {
                Ok(Some(frame)) => {
                    let frame_proven = self.pump_one_decide_frame(frames, frame, diag).await;
                    if frame_proven {
                        break true;
                    }
                    if self.frame_decide_decisive {
                        break false;
                    }
                }
                _ => break false,
            }
        };
        self.active_decide = None;
        self.active_decide_admission = false;
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        proven
    }

    /// Handle one pumped frame in arrival order and merge any proof/diag
    /// the owning path recorded for that exact frame. Returns true only
    /// when the owning path proved this decide applied.
    async fn pump_one_decide_frame(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        frame: NativeFrame,
        diag: &mut DecideViewDiag,
    ) -> bool {
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        self.frame_decide_diag = DecideViewDiag::default();
        // Decide pumps already run inside control processing, so the frame
        // path must not re-enter the live queues here.
        let _ = self.handle_frame(Some(frame)).await;
        self.report_gap_if_queue_empty(frames.is_empty()).await;
        if self.frame_decide_diag.resolved_seen {
            diag.resolved_seen = true;
            diag.decider_present = self.frame_decide_diag.decider_present;
            diag.decider_match = self.frame_decide_diag.decider_match;
            diag.resolved_decisive = true;
        }
        self.frame_decide_proof
    }

    /// Record non-terminal offer proof from an owning path: only with
    /// admission proven, on the exact captured session/turn/approval, and
    /// only when the owning refreshed offer is strictly past the decided
    /// requirement. Callers invoke this only after their own successful
    /// validation (and, where they persist, successful persistence).
    fn note_decide_offer_proof(
        &mut self,
        session_id: &str,
        turn_id: &str,
        approval_id: &str,
        offer_seq: i64,
    ) {
        let Some(capture) = self.active_decide.clone() else {
            return;
        };
        if !self.active_decide_admission {
            return;
        }
        if capture.session_id != session_id
            || capture.turn_id != turn_id
            || capture.approval_id != approval_id
        {
            return;
        }
        if offer_seq > capture.decided_seq {
            self.frame_decide_proof = true;
        }
    }

    /// Record terminal-resolution proof/diag from `resolve_approval`, only
    /// after its commit succeeded and the entry was removed. A resolution
    /// that does not name our native decide id is decisive the other way.
    fn note_decide_resolved(
        &mut self,
        session_id: &str,
        turn_id: &str,
        approval_id: &str,
        decided_by: Option<&str>,
        bound_to_ours: bool,
    ) {
        let Some(capture) = self.active_decide.clone() else {
            return;
        };
        if capture.session_id != session_id
            || capture.turn_id != turn_id
            || capture.approval_id != approval_id
        {
            return;
        }
        self.frame_decide_diag.resolved_seen = true;
        self.frame_decide_diag.decider_present = decided_by.is_some_and(|id| !id.trim().is_empty());
        self.frame_decide_diag.decider_match =
            resolved_decider_matches(decided_by, &capture.native_command_id);
        self.frame_decide_diag.resolved_decisive = true;
        self.frame_decide_decisive = true;
        self.frame_decide_proof = bound_to_ours && self.frame_decide_diag.decider_match;
    }

    async fn reply_request(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        command_id: &str,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
    ) -> Result<Value> {
        if command_id.trim().is_empty() || request_id.trim().is_empty() || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Muse request reply requires exact command, request, and turn ids",
            ));
        }
        let Some(entry) = self.pending_requests.get_mut(request_id) else {
            return Ok(rejected_control("Muse request is no longer pending"));
        };
        if entry.request.turn_id != turn_id {
            return Ok(rejected_control(
                "Muse request reply does not match the captured turn",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Ok(rejected_control(
                "Muse request is no longer attached to the active turn",
            ));
        }
        match entry.reply_state {
            NativeReplyState::Writing | NativeReplyState::Unknown => {
                return Ok(unknown_control(
                    "Muse request reply outcome is unresolved and will not be repeated",
                ));
            }
            NativeReplyState::Written => {
                return Ok(rejected_control(
                    "Muse request already has a native decision and awaits resolution",
                ));
            }
            NativeReplyState::Available => {}
        }
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(rejected_control(
                "Muse request session is no longer writable",
            ));
        };
        if self.status.stopped {
            return Ok(rejected_control(
                "Muse request session is no longer writable",
            ));
        }
        // The native idempotency handle is minted here, before the write: a
        // lost reply leaves the outcome unknown and the decision is never
        // written a second time under another id.
        let native_command_id = crate::host::new_command_id();
        let reply = match reply_for(&entry.request, verb, args, &native_command_id, &session_id) {
            Ok(reply) => reply,
            Err(error) => return Ok(rejected_control(&error.message)),
        };
        let params = reply.params;
        let expected_native_id = match reply.method {
            "approval/decide" => params.get("approvalId").and_then(Value::as_str),
            _ => params.get("userInputId").and_then(Value::as_str),
        }
        .unwrap_or_default()
        .to_owned();
        let decided_offer_seq = (reply.method == "approval/decide")
            .then(|| {
                params
                    .get("requirementId")
                    .and_then(|value| value.get("sourceIndex"))
                    .and_then(Value::as_i64)
            })
            .flatten();
        let is_decide = reply.method == "approval/decide";
        // View qualification needs the exact sent binding. It is read from
        // the same validated values the reply was built from, so it cannot
        // be missing; anything else leaves the request available pre-write.
        let capture: Option<DecideCapture> = if !is_decide {
            None
        } else {
            let (Some(approval_id), Some(sent_session), Some(seq)) = (
                params.get("approvalId").and_then(Value::as_str),
                params.get("sessionId").and_then(Value::as_str),
                decided_offer_seq,
            ) else {
                return Ok(rejected_control(
                    "Muse request reply binding could not be captured",
                ));
            };
            Some(DecideCapture {
                session_id: sent_session.to_owned(),
                turn_id: turn_id.to_owned(),
                approval_id: approval_id.to_owned(),
                decided_seq: seq,
                native_command_id: native_command_id.clone(),
            })
        };
        let request = entry.request.clone();
        entry.reply_state = NativeReplyState::Writing;
        entry.reply_native_command_id = Some(native_command_id.clone());
        if is_decide {
            // The view-wait below may resolve while this RPC is outstanding;
            // the outstanding Eliot command must already be recorded so the
            // resolution can bind it.
            entry.reply_command_id = Some(command_id.to_owned());
        }
        // ONE absolute deadline, captured before the write and carried
        // through the RPC and the view wait alike: never renewed.
        let deadline = Instant::now() + EVENT_DEADLINE;
        let acknowledged = match self.native.call(reply.method, params, deadline).await {
            Ok(ack) => ack,
            Err(error) if native_no_write(&error.code) || error.code == "native_rejected" => {
                // Proven not admitted (transport never wrote, or a
                // schema-documented pre-admission/validation error): the
                // request stays available for a later reply.
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
                return Ok(rejected_control("Muse request reply was not admitted"));
            }
            Err(error) => {
                let error_code = native_error_code(&error);
                if !is_decide {
                    if let Some(entry) = self.pending_requests.get_mut(request_id) {
                        entry.reply_state = NativeReplyState::Unknown;
                        entry.reply_command_id = Some(command_id.to_owned());
                    }
                    return Ok(unknown_control(
                        "Muse request reply may have been dispatched; it will not be repeated",
                    ));
                }
                let Some(capture) = capture else {
                    if let Some(entry) = self.pending_requests.get_mut(request_id) {
                        entry.reply_state = NativeReplyState::Unknown;
                        entry.reply_command_id = Some(command_id.to_owned());
                    }
                    let _ = self
                        .record_gap("Muse decide binding could not be captured")
                        .await;
                    return Ok(unknown_control(
                        "Muse request reply binding is unresolved and will not be repeated",
                    ));
                };
                return self
                    .qualify_decide(
                        frames,
                        DecideQualification {
                            command_id,
                            request_id,
                            turn_id,
                            capture: &capture,
                            deadline,
                            rpc: DecideRpcOutcome::Ambiguous {
                                error_code,
                                unproven_reason:
                                    "Muse request reply may have been dispatched; it will not be repeated",
                            },
                        },
                    )
                    .await;
            }
        };
        // Admission proves nothing by itself (the ack echoes the command
        // id, not the decision): the ack must echo the exact submitted
        // command id, the acceptance status, and the exact decided request
        // id.
        let acked = match reply.method {
            "approval/decide" => serde_json::from_value::<NativeApprovalDecideResult>(acknowledged)
                .ok()
                .is_some_and(|ack| {
                    ack.command_id == native_command_id
                        && ack.status == "accepted"
                        && ack.approval_id == expected_native_id
                }),
            "userInput/answer" => {
                serde_json::from_value::<NativeUserInputAnswerResult>(acknowledged)
                    .ok()
                    .is_some_and(|ack| {
                        ack.command_id == native_command_id
                            && ack.status == "accepted"
                            && ack.user_input_id == expected_native_id
                    })
            }
            _ => serde_json::from_value::<NativeUserInputCancelResult>(acknowledged)
                .ok()
                .is_some_and(|ack| {
                    ack.command_id == native_command_id
                        && ack.status == "accepted"
                        && ack.user_input_id == expected_native_id
                }),
        };
        if !is_decide {
            if !acked {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                return Ok(unknown_control(
                    "Muse request reply did not acknowledge the exact submitted command",
                ));
            }
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Written;
                entry.reply_command_id = Some(command_id.to_owned());
                entry.reply_native_command_id = None;
            }
            return Ok(request_reply_applied(
                command_id,
                &request.id,
                &request.turn_id,
            ));
        }
        let Some(capture) = capture else {
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
                entry.reply_command_id = Some(command_id.to_owned());
            }
            let _ = self
                .record_gap("Muse decide binding could not be captured")
                .await;
            return Ok(unknown_control(
                "Muse request reply binding is unresolved and will not be repeated",
            ));
        };
        // A decide is Applied only on authoritative view proof, never from
        // the admission ack alone; a malformed ack qualifies like an
        // ambiguous RPC error (a resolution naming our decide still proves
        // the write applied).
        if acked {
            self.qualify_decide(
                frames,
                DecideQualification {
                    command_id,
                    request_id,
                    turn_id,
                    capture: &capture,
                    deadline,
                    rpc: DecideRpcOutcome::Admitted,
                },
            )
            .await
        } else {
            self.qualify_decide(
                frames,
                DecideQualification {
                    command_id,
                    request_id,
                    turn_id,
                    capture: &capture,
                    deadline,
                    rpc: DecideRpcOutcome::Ambiguous {
                        error_code: None,
                        unproven_reason:
                            "Muse request reply did not acknowledge the exact submitted command",
                    },
                },
            )
            .await
        }
    }

    /// Settle one written decide against the authoritative view stream: pump
    /// live frames through the existing frame path until this exact decide is
    /// proven applied, proven otherwise decided, the stream ends, or the
    /// single carried deadline expires. Proven terminal proof removed the
    /// entry via the pumped resolution; proven nonterminal proof only
    /// refreshed it, so the stage is consumed here (mirroring the update
    /// reset, which cannot fire while Writing) and the consumed offer is
    /// persisted for the owner projection and the stage gate; if that
    /// required persistence fails or gaps, the result is Unknown, never
    /// Applied. Unproven outcomes stay honest and fenced: an
    /// admitted-but-unproven write stays Unknown with both the Core and
    /// native bindings retained, so only a late resolution naming this
    /// exact native decide id can bind it (otherwise it closes unbound);
    /// an ambiguous write likewise stays fenced unknown. Never resends,
    /// never rearms Unknown.
    async fn qualify_decide(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        qualification: DecideQualification<'_>,
    ) -> Result<Value> {
        let DecideQualification {
            command_id,
            request_id,
            turn_id,
            capture,
            deadline,
            rpc,
        } = qualification;
        let admission_proven = matches!(rpc, DecideRpcOutcome::Admitted);
        let mut diag = DecideViewDiag::default();
        let proven = self
            .pump_decide_proof(frames, capture, deadline, admission_proven, &mut diag)
            .await;
        if proven {
            if let Some(refreshed) = self
                .pending_requests
                .get(request_id)
                .map(|entry| entry.request.clone())
            {
                let live_turn =
                    self.status.active_turn_id.as_deref() == Some(refreshed.turn_id.as_str());
                if live_turn {
                    // The pumped update could not persist while Writing, so
                    // this is the required persistence for non-terminal
                    // proof (terminal proof already removed the entry and
                    // persisted its resolution in the owning path). A
                    // failed or gapped persistence is never Applied.
                    let persisted = matches!(self.commit_offer_update(&refreshed).await, Ok(true));
                    if !persisted {
                        if let Some(entry) = self.pending_requests.get_mut(request_id) {
                            entry.reply_state = NativeReplyState::Unknown;
                            entry.reply_command_id = Some(command_id.to_owned());
                        }
                        let error_code = match rpc {
                            DecideRpcOutcome::Admitted => None,
                            DecideRpcOutcome::Ambiguous { error_code, .. } => error_code,
                        };
                        return Ok(with_decide_diag(
                            unknown_control(
                                "Muse consumed offer update was not persisted; it will not be repeated",
                            ),
                            error_code,
                            &diag,
                        ));
                    }
                }
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
            }
            let outcome = json!({
                "state": "applied",
                "command_id": command_id,
                "request_id": request_id,
                "turn_id": turn_id,
                "decided_offer_seq": capture.decided_seq,
            });
            let error_code = match rpc {
                DecideRpcOutcome::Admitted => None,
                DecideRpcOutcome::Ambiguous { error_code, .. } => error_code,
            };
            return Ok(with_decide_diag(outcome, error_code, &diag));
        }
        match rpc {
            DecideRpcOutcome::Admitted => {
                // Unproven after admission: stay Unknown with both the Core
                // command id and the native decide id retained. Storing
                // Written and clearing the native id would let a later
                // missing/foreign `decidedByCommandId` bind this write at
                // resolution time; Unknown requires exact late proof and
                // otherwise closes unbound. Never replayed or rearmed.
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(with_decide_diag(
                    unknown_control(
                        "Muse request reply was admitted but its decision is unproven; it will not be repeated",
                    ),
                    None,
                    &diag,
                ))
            }
            DecideRpcOutcome::Ambiguous {
                error_code,
                unproven_reason,
            } => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(with_decide_diag(
                    unknown_control(unproven_reason),
                    error_code,
                    &diag,
                ))
            }
        }
    }

    /// Adopt pulled snapshot entries. Entries that bind to the live turn are
    /// opened; entries for any other turn are stashed until a turn associates
    /// (then adopted or dropped as orphans). A newer stream resolution always
    /// wins over stale snapshot data: terminally resolved ids are never
    /// re-adopted, and an id dropped after the snapshot capture stays dropped.
    async fn adopt_pulled(
        &mut self,
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
    ) -> Result<()> {
        // The pull landed but at least one entry could not be described: an
        // explicit gap records the missing presentation instead of silently
        // dropping it. Adoption runs after readiness, so this never fails the
        // resume (a gap acknowledgement failure is ignored the same way).
        if snapshot_had_undecodable {
            let _ = self
                .record_gap("Muse pulled request snapshot contained an undecodable entry")
                .await;
        }
        for id in expected.into_iter().take(MAX_PENDING_REQUESTS) {
            if !id.trim().is_empty() {
                self.expected_requests.insert(id);
            }
        }
        for entry in entries {
            if self.pending_requests.contains_key(&entry.id)
                || self.snapshot_superseded(&entry, capture)
            {
                continue;
            }
            if self
                .status
                .active_turn_id
                .as_deref()
                .is_some_and(|active| active == entry.turn_id)
            {
                self.open_pulled_entry(entry).await?;
            } else if self.stashed_entries.len() < MAX_PENDING_REQUESTS {
                self.stashed_entries.push(StashedPending {
                    request: entry,
                    capture,
                });
            }
        }
        Ok(())
    }

    /// Whether a pulled snapshot entry lost to a newer stream observation: a
    /// terminal resolution always wins, and a drop newer than the snapshot
    /// capture wins too.
    fn snapshot_superseded(&self, entry: &CorePending, capture: u64) -> bool {
        pending_native_id(entry).is_some_and(|native_id| {
            self.resolved_native_ids.contains(native_id)
                || self
                    .dropped_native_at
                    .get(native_id)
                    .is_some_and(|dropped_at| *dropped_at > capture)
        })
    }

    async fn open_pulled_entry(&mut self, entry: CorePending) -> Result<()> {
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.defer_gap("Muse pending native request bound was exceeded");
            return Ok(());
        }
        let command_id = self.status.active_command_id.clone();
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let Ok((public_kind, public_details)) = public_request_view(&entry) else {
            self.defer_gap("Muse public request view could not be normalized");
            return Ok(());
        };
        let mut data = json!({
            "request_id": entry.id,
            "turn_id": entry.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse pulled request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&entry.id, "request.opened", "muse", "Muse") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.defer_gap("Muse native request event identity exceeded its bound");
                return Ok(());
            }
        };
        let request_id = entry.id.clone();
        self.commit_event("request.opened", native_id, data).await?;
        let observed = self.observe();
        if let Some(native_id) = pending_native_id(&entry) {
            self.native_index
                .insert(native_id.to_owned(), request_id.clone());
            self.expected_requests.remove(native_id);
        }
        self.pending_requests.insert(
            request_id,
            PendingNativeRequest {
                request: entry,
                reply_command_id: None,
                reply_native_command_id: None,
                reply_state: NativeReplyState::Available,
                observed_at: observed,
            },
        );
        Ok(())
    }

    /// Apply stashed snapshot entries once a turn associates: adopt what binds
    /// to the new turn (guarded again — the stream may have adopted or settled
    /// the id meanwhile), drop the rest as orphans of dead turns.
    async fn apply_stashed_entries(&mut self) -> Result<()> {
        let stashed = std::mem::take(&mut self.stashed_entries);
        for stash in stashed {
            let entry = stash.request;
            if self.pending_requests.contains_key(&entry.id)
                || self.snapshot_superseded(&entry, stash.capture)
            {
                continue;
            }
            if self
                .status
                .active_turn_id
                .as_deref()
                .is_some_and(|active| active == entry.turn_id)
            {
                self.open_pulled_entry(entry).await?;
            }
        }
        Ok(())
    }

    async fn process_notification(&mut self, method: String, params: Value) -> Result<()> {
        let cursor = self
            .status
            .session_id
            .as_deref()
            .is_some_and(|session_id| {
                params.get("sessionId").and_then(Value::as_str) == Some(session_id)
            })
            .then(|| params.get("viewCursor").and_then(Value::as_str))
            .flatten()
            .filter(|cursor| !cursor.trim().is_empty())
            .map(str::to_owned);
        // `viewCursor` is a view-revision cursor, not a per-notification id:
        // one revision can carry several facts (the final `item/completed`
        // and `turn/completed` of a turn share it). Deduplicate the exact
        // fact (method, cursor, item/turn identity), never the cursor alone.
        let applied_key = cursor
            .as_deref()
            .map(|cursor| applied_fact_key(&method, cursor, &params));
        if let Some(key) = applied_key.as_deref()
            && self.applied_view_cursors.contains(key)
        {
            // Already-applied replayed copy (live facts queued past
            // `after` alongside their `(after, head]` replay): skip
            // normalization instead of committing the same native id a
            // second time. The first application did the exact
            // session/turn binding and conflict checks.
            return Ok(());
        }
        self.apply_notification(method, params).await?;
        if let Some(key) = applied_key {
            self.applied_view_cursors.insert(key);
        }
        Ok(())
    }

    async fn apply_notification(&mut self, method: String, params: Value) -> Result<()> {
        if native_notification_turn_id(&params)
            .is_some_and(|turn_id| self.uncertain_native_turns.contains(turn_id))
        {
            return Ok(());
        }
        if is_workflow_item_frame(&method, &params) {
            return self.observe_workflow_item(params).await;
        }
        match method.as_str() {
            "log/record" => self.observe_provider_model(&params).await?,
            "log/gap"
                if params.get("subscriptionId").and_then(Value::as_u64)
                    == self.provider_subscription =>
            {
                let reason = "Muse live provider model observation lost coverage";
                self.status.failure = Some(reason.to_owned());
                if let (Some(session), Some(turn)) = (
                    self.status.session_id.clone(),
                    self.status.active_turn_id.clone(),
                ) {
                    provider::cancel(
                        &self.native,
                        &session,
                        &turn,
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await;
                }
                self.record_gap(reason).await?;
            }
            "session/viewHealthChanged" => {
                let health_tag = params
                    .get("health")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let none_reason_tag = params
                    .get("noneReason")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let notification: NativeSessionViewHealthChangedNotification =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse view-health notification did not match its schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str()) {
                    if notification.health == NativeViewHealth::Unavailable {
                        self.view_unavailable(
                            &notification.session_id,
                            notification
                                .none_reason
                                .unwrap_or(NativeHistoryNoneReason::Other),
                        )
                        .await?;
                    }
                    if notification.health == NativeViewHealth::Other
                        && let Some(native_tag) = health_tag.as_deref()
                    {
                        self.record_unclassified_native(
                            "muse.session.viewHealthChanged.health",
                            native_tag,
                        )
                        .await?;
                    }
                    if notification.none_reason == Some(NativeHistoryNoneReason::Other)
                        && let Some(native_tag) = none_reason_tag.as_deref()
                    {
                        self.record_unclassified_native(
                            "muse.session.viewHealthChanged.noneReason",
                            native_tag,
                        )
                        .await?;
                    }
                }
            }
            "turn/started" => {
                let notification: NativeTurnStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse turn/started did not match the 1.4 schema",
                        )
                    })?;
                validate_session_and_turn(
                    &self.status,
                    &notification.session_id,
                    &notification.turn_id,
                )?;
                if self
                    .uncertain_native_starts
                    .contains(&notification.command_id)
                {
                    self.uncertain_native_turns
                        .insert(notification.turn_id.clone());
                    return self
                        .record_gap("Muse turn/start matched a command with an unknown outcome")
                        .await;
                }
                match self.status.active_turn_id.as_deref() {
                    Some(active_turn)
                        if active_turn == notification.turn_id
                            && self.active_native_command_id.as_deref()
                                == Some(notification.command_id.as_str()) =>
                    {
                        return Ok(());
                    }
                    Some(_) => {
                        return self
                            .record_gap("Muse turn/started did not match the exact active turn")
                            .await;
                    }
                    None => self.start_native_turn(notification).await?,
                }
            }
            "item/started" => {
                let notification: NativeItemStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/started did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
                self.start_tool(&notification.item).await?;
            }
            "item/updated" => {
                // Full re-emission at a higher revision; no per-delta event
                // is folded. The authoritative terminal object from
                // `item/completed` carries every committed tool fact, so an
                // update alone normalizes nothing.
                let notification: NativeItemCompletedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/updated did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
            }
            "item/completed" => {
                let notification: NativeItemCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/completed did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
                if notification.item.kind == "agentMessage" {
                    let text = notification.item.text.as_deref().ok_or_else(|| {
                        Error::new("native_protocol", "Muse agentMessage omitted its text")
                    })?;
                    if self.status.model_mismatch.is_none() {
                        let bytes = self.messages.awaiting_bytes.saturating_add(text.len());
                        if bytes > MAX_TEXT_BYTES
                            || self.messages.awaiting_model.len() >= EARLY_EVENT_LIMIT
                        {
                            return self
                                .record_gap(
                                    "Muse messages awaiting provider evidence exceeded their bound",
                                )
                                .await;
                        }
                        self.messages.awaiting_bytes = bytes;
                        self.messages
                            .awaiting_model
                            .push((notification.item.item_id, text.to_owned()));
                    }
                } else {
                    self.complete_tool(&notification.item).await?;
                }
            }
            "turn/completed" => {
                self.complete_turn(params).await?;
            }
            "usage/changed" => {
                // Optional quota sample: a malformed sample is dropped
                // without invalidating unrelated lifecycle evidence. Valid
                // over-quota measurements (usedPercent > 100) are retained.
                let usage: crate::native::types::NativeSubscriptionUsage =
                    match serde_json::from_value(params) {
                        Ok(usage) => usage,
                        Err(_) => return Ok(()),
                    };
                if usage.observed_at_ms < 0
                    || usage.window.window_duration_mins <= 0
                    || usage.window.resets_at_ms < 0
                    || usage.weekly.resets_at_ms < 0
                    || usage.window.used_percent < 0
                    || usage.weekly.used_percent < 0
                {
                    return Ok(());
                }
                let observation = eliot_core::api::harness::QuotaObservation {
                    observed_at_ms: usage.observed_at_ms,
                    windows: vec![
                        eliot_core::api::harness::QuotaWindow {
                            label: "5-hour".to_owned(),
                            used_percent: usage.window.used_percent as f64,
                            resets_at_ms: Some(usage.window.resets_at_ms),
                            window_duration_mins: Some(usage.window.window_duration_mins),
                        },
                        eliot_core::api::harness::QuotaWindow {
                            label: "Weekly".to_owned(),
                            used_percent: usage.weekly.used_percent as f64,
                            resets_at_ms: Some(usage.weekly.resets_at_ms),
                            window_duration_mins: Some(10080),
                        },
                    ],
                };
                if observation.validate().is_err() {
                    return Ok(());
                }
                self.commit_event(
                    "quota.observed",
                    ulid::Ulid::new().to_string(),
                    json!({"observation":observation}),
                )
                .await?;
            }
            "session/tokenUsage" => {
                self.observe_session_token_usage(params).await?;
            }
            "session/contextUsage" => {
                self.observe_session_context_usage(params).await?;
            }
            "approval/resolved" => {
                self.resolve_approval(params).await?;
            }
            "approval/updated" => {
                self.refresh_approval(params).await?;
            }
            "userInput/settled" => {
                self.settle_user_input(params).await?;
            }
            "session/started" => {
                self.observe_session_started(&params).await?;
            }
            "session/closed" => {
                let notification: NativeSessionClosedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse session/closed did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str())
                    && !self.planned_stop
                {
                    return self
                        .record_gap("Muse session closed outside a planned stop")
                        .await;
                }
            }
            "view/gap" => {
                let notification: crate::native::types::NativeViewGapNotification =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse view/gap did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str()) {
                    return self
                        .stream_source()
                        .record_gap_range_until(
                            "Muse view stream reported a delivery gap",
                            Some(notification.after),
                            Some(notification.next),
                            Instant::now() + EVENT_DEADLINE,
                        )
                        .await;
                }
            }
            _ => {
                let belongs_to_owned_session = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .is_none_or(|session_id| self.status.session_id.as_deref() == Some(session_id));
                if belongs_to_owned_session {
                    self.record_unclassified_native("muse.notification", &method)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Items outside the exact active turn are gaps. A turn-less item (only
    /// `userShell`) carries no Eliot turn binding and stays unmapped; a
    /// turn-bound `userShell` normalizes like `toolCall` under its kind.
    fn validate_item_turn(&self, session_id: &str, turn_id: Option<&String>) -> Result<()> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            ));
        }
        match turn_id.map(String::as_str) {
            None => Ok(()),
            Some(turn_id) if turn_id.trim().is_empty() => Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            )),
            Some(turn_id) => {
                if self.status.active_turn_id.as_deref() != Some(turn_id) {
                    return Err(Error::new(
                        "source_integrity",
                        "Muse item was outside the exact active turn",
                    ));
                }
                Ok(())
            }
        }
    }

    async fn complete_message(&mut self, turn_id: &str, item_id: &str, text: &str) -> Result<()> {
        if item_id.trim().is_empty() {
            return self
                .record_gap("Muse agentMessage omitted its item id")
                .await;
        }
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(text);
        let mut data = json!({
            "turn_id": turn_id,
            "text": text,
        });
        if let Some(command_id) = self.status.active_command_id.as_deref() {
            data["command_id"] = json!(command_id);
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            turn_id,
            item_id,
            "message.completed",
        )?;
        let (mut source, messages) = self.stream_source_and_messages();
        messages.complete(&mut source, native_id, text, data).await
    }

    async fn flush_observed_messages(&mut self, turn: &str) -> Result<()> {
        self.messages.awaiting_bytes = 0;
        for (id, text) in std::mem::take(&mut self.messages.awaiting_model) {
            self.complete_message(turn, &id, &text).await?;
        }
        Ok(())
    }

    async fn observe_provider_model(&mut self, params: &Value) -> Result<()> {
        let Some(signal) = provider::signal(params) else {
            return Ok(());
        };
        if Some(signal.subscription_id) != self.provider_subscription
            || Some(signal.session_id) != self.status.session_id.as_deref()
            || Some(signal.turn_id) != self.status.active_turn_id.as_deref()
            || self.status.model_mismatch.is_some()
        {
            return Ok(());
        }
        let actual = match signal.fact {
            provider::Fact::ForegroundTask(task) => {
                self.turn_model.tasks.insert(task.to_owned());
                return Ok(());
            }
            provider::Fact::TaskEnded(task) => {
                self.turn_model.tasks.remove(task);
                return Ok(());
            }
            provider::Fact::Response { task, model } if self.turn_model.tasks.contains(task) => {
                model
            }
            provider::Fact::Response { .. } => return Ok(()),
        };
        let native_id = event_id(
            signal.session_id,
            signal.turn_id,
            signal.record_id,
            "turn.model_observed",
        )?;
        // Muse 1.4.25 fills an omitted response model with the requested
        // selector. An exact echo carries no independent provider evidence.
        let Some(actual) =
            actual.filter(|actual| Some(*actual) != self.turn_model.expected.as_deref())
        else {
            self.status.provider_model_turn = None;
            self.commit_event(
                "turn.model_observed",
                native_id,
                json!({
                    "turn_id": signal.turn_id,
                    "model_unverified_reason": eliot_core::PROVIDER_MODEL_UNNAMED,
                }),
            )
            .await?;
            return self.flush_observed_messages(signal.turn_id).await;
        };
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let model = policy.redact_bounded(actual, 256);
        if let Some(expected) = self.turn_model.expected.as_deref()
            && provider::canonical_model(expected) != provider::canonical_model(actual)
        {
            let mismatch = eliot_core::ProviderModelMismatch {
                requested_model: policy.redact_bounded(expected, 256),
                reported_model: model,
                revision: self
                    .model_hold_settings_path
                    .as_deref()
                    .and_then(crate::install::model_hold_revision_for_path),
            };
            self.status.model_mismatch = Some((signal.turn_id.to_owned(), mismatch.reason()));
            self.status.provider_model_turn = None;
            self.messages = TurnMessages::default();

            // The hold and the cancellation are independent. Start the exact
            // cancel immediately and persist the mismatch even if it fails.
            let native = self.native.clone();
            let deadline = Instant::now() + Duration::from_secs(5);
            let (committed, accepted) = tokio::join!(
                self.commit_event(
                    "turn.model_observed",
                    native_id,
                    json!({
                        "turn_id": signal.turn_id, "model_mismatch": mismatch,
                    })
                ),
                provider::cancel(&native, signal.session_id, signal.turn_id, deadline),
            );
            self.status.model_stop_at = Some(if accepted { deadline } else { Instant::now() });
            return committed;
        }
        self.commit_event(
            "turn.model_observed",
            native_id,
            json!({
                "turn_id": signal.turn_id, "model": model,
            }),
        )
        .await?;
        self.status.provider_model_turn = Some(signal.turn_id.to_owned());
        self.flush_observed_messages(signal.turn_id).await
    }

    /// Fold session cumulative counters and estimated cost from one
    /// `session/tokenUsage` event into the session snapshot. Separately
    /// aggregate deduplicated model completions that identify this exact turn.
    async fn observe_session_token_usage(&mut self, params: Value) -> Result<()> {
        let notification: NativeSessionTokenUsageNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse session/tokenUsage did not match the 1.4 schema",
                )
            })?;
        self.check_session_signal(&notification.session_id)?;
        if let Some(turn_id) = notification.turn_id.as_deref()
            && (self.status.active_turn_id.as_deref() == Some(turn_id)
                || self.pending_native_starts.contains_key(turn_id))
            && notification
                .view_cursor
                .as_deref()
                .is_some_and(|cursor| !cursor.trim().is_empty())
        {
            if let Some(sample) = notification.usage.as_ref().and_then(turn_usage_from_native) {
                match self.turn_completion_usage.as_mut() {
                    Some((id, total)) if id == turn_id => {
                        let add = |previous: Option<i64>, next: Option<i64>| {
                            previous.zip(next).and_then(|(a, b)| a.checked_add(b))
                        };
                        total.input_tokens = add(total.input_tokens, sample.input_tokens);
                        total.output_tokens = add(total.output_tokens, sample.output_tokens);
                        total.reasoning_tokens =
                            add(total.reasoning_tokens, sample.reasoning_tokens);
                        total.cache_read_tokens =
                            add(total.cache_read_tokens, sample.cache_read_tokens);
                        total.cache_write_tokens =
                            add(total.cache_write_tokens, sample.cache_write_tokens);
                    }
                    _ => self.turn_completion_usage = Some((turn_id.to_owned(), sample)),
                }
            }
        }
        let Some(merged) =
            session_usage_from_token_usage(&notification, self.session_usage.as_ref())
        else {
            return Ok(());
        };
        self.sample_session_usage(merged).await;
        Ok(())
    }

    /// Fold one `session/contextUsage` event into the retained session
    /// snapshot: counted-once
    /// context occupancy and the effective window size, with `observed_at`
    /// captured at acceptance. Each notification replaces both context
    /// fields. Unchanged merges commit nothing, so identical adoptions never
    /// churn the readback.
    async fn observe_session_context_usage(&mut self, params: Value) -> Result<()> {
        let notification: NativeSessionContextUsageNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse session/contextUsage did not match the 1.4 schema",
                )
            })?;
        self.check_session_signal(&notification.session_id)?;
        let Some(merged) =
            session_usage_from_context_usage(&notification, self.session_usage.as_ref())
        else {
            return Ok(());
        };
        self.sample_session_usage(merged).await;
        Ok(())
    }

    /// The notification names the owned native session; anything else is a
    /// foreign session and gaps the source instead of merging its counters.
    fn check_session_signal(&self, session_id: &str) -> Result<()> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse session signal named another native session",
            ));
        }
        Ok(())
    }

    async fn sample_session_usage(&mut self, usage: SessionUsage) {
        let turn = self.status.active_turn_id.clone();
        if self.session_usage.as_ref() == Some(&usage) && self.session_usage_turn == turn {
            return;
        }
        self.session_usage = Some(usage.clone());
        self.session_usage_turn = turn.clone();
        let _ = self
            .stream_source()
            .report_session_usage_sample(usage, turn.as_deref())
            .await;
    }

    async fn commit_turn_session_usage(&mut self, turn: &str) -> Result<()> {
        if self.session_usage_turn.as_deref() != Some(turn) {
            return Ok(());
        }
        self.session_usage_turn = None;
        let Some(usage) = self.session_usage.clone() else {
            return Ok(());
        };
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = event_id(session_id, turn, "terminal", "session.usage")?;
        self.commit_event("session.usage", native_id, json!({"usage": usage}))
            .await
    }

    /// Commit `tool.started` for a turn-bound tool item, mirroring the Codex
    /// adapter's start DTO (`command_id`, `turn_id`, `item_id`, `tool`).
    /// Tool calls, command execution and edit-family calls all arrive as
    /// `toolCall`; a turn-bound `userShell` normalizes under its kind. Only
    /// the tool identity is persisted: `callId`, `args` and result payloads
    /// never leave the native frame.
    async fn start_tool(&mut self, item: &NativeItem) -> Result<()> {
        let Some(tool) = tool_label(item) else {
            if tool_item_kind(&item.kind) {
                return self
                    .record_gap("Muse tool item omitted its tool name")
                    .await;
            }
            return Ok(());
        };
        let Some((command_id, turn_id)) = self.tool_binding(item)? else {
            // Turn-less `userShell` carries no Eliot turn binding and stays
            // unmapped, as before.
            return Ok(());
        };
        if item.item_id.trim().is_empty() {
            return self
                .record_gap("Muse tool start omitted its exact item id")
                .await;
        }
        let mut data = json!({
            "turn_id": turn_id,
            "item_id": item.item_id,
            "tool": tool,
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("Muse tool start exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &turn_id,
            &item.item_id,
            "tool.started",
        )?;
        self.commit_event("tool.started", native_id, data).await
    }

    /// Commit `tool.completed` for the terminal revision of a turn-bound
    /// tool item, mirroring the Codex adapter's completion DTO (the start
    /// fields plus the verbatim native `status`). Non-terminal revisions
    /// carry no committed fact; their terminal `item/completed` follows.
    /// A completed edit-family `toolCall` normalizes as the shared
    /// completed-file-change payload (`tool` `fileChange`, verbatim
    /// `status`, bounded `changed_paths`/`changed_paths_truncated`) that
    /// native file-change hint attribution accepts, mirroring exactly what
    /// the Codex adapter emits for its `fileChange` items. The inline
    /// target path attaches when present; otherwise one bounded
    /// `item/readOutput` fetch resolves the stored `patchRef` and only the
    /// file paths from the patch headers attach. An unresolvable reference
    /// (absent, unservable, failed or timed-out read) keeps today's
    /// behavior: empty `changed_paths` with the truncation flag set.
    async fn complete_tool(&mut self, item: &NativeItem) -> Result<()> {
        let Some(tool) = tool_label(item) else {
            if tool_item_kind(&item.kind) {
                return self
                    .record_gap("Muse tool item omitted its tool name")
                    .await;
            }
            return Ok(());
        };
        if !is_terminal_item_status(&item.status) {
            return Ok(());
        }
        let Some((command_id, turn_id)) = self.tool_binding(item)? else {
            // Turn-less `userShell` carries no Eliot turn binding and stays
            // unmapped, as before.
            return Ok(());
        };
        if item.item_id.trim().is_empty() {
            return self
                .record_gap("Muse tool completion omitted its exact item id")
                .await;
        }
        let completed_file_change = completed_file_change(item);
        let mut data = json!({
            "turn_id": turn_id,
            "item_id": item.item_id,
            "tool": tool,
            "status": item.status,
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if completed_file_change {
            // Installed Codex 0.161 `fileChange` budgeting, mirrored
            // exactly: reserve the complete envelope, then admit inline
            // paths while they fit. `true` is shorter than `false`, so
            // marking truncation cannot exceed this budget.
            data["changed_paths"] = json!([]);
            data["changed_paths_truncated"] = Value::Bool(false);
            let mut metadata_bytes = serde_json::to_vec(&data)
                .map_err(|_| {
                    Error::new("internal", "Muse fileChange metadata could not be encoded")
                })?
                .len();
            let mut changed_paths = Vec::new();
            let mut truncated = false;
            let admit_hint = |path: String,
                              changed_paths: &mut Vec<String>,
                              metadata_bytes: &mut usize,
                              truncated: &mut bool|
             -> Result<bool> {
                let path_bytes = serde_json::to_vec(&path)
                    .map_err(|_| {
                        Error::new("internal", "Muse fileChange path could not be encoded")
                    })?
                    .len()
                    + usize::from(!changed_paths.is_empty());
                if path.is_empty()
                    || changed_paths.len() >= MAX_CHANGED_PATHS
                    || metadata_bytes.saturating_add(path_bytes) > MAX_EVENT_BYTES
                {
                    *truncated = true;
                    return Ok(false);
                }
                *metadata_bytes += path_bytes;
                changed_paths.push(path);
                Ok(true)
            };
            'paths: for path in edit_hint_paths(item) {
                if !admit_hint(
                    path,
                    &mut changed_paths,
                    &mut metadata_bytes,
                    &mut truncated,
                )? {
                    break 'paths;
                }
            }
            if changed_paths.is_empty() {
                // No inline target: resolve the stored patch reference once,
                // bounded by the single first-page fetch. Only header paths
                // attach; the patch body itself is never persisted. A page
                // that stops short of the stored patch still contributes the
                // paths it covered, with the truncation flag set.
                let (patch_paths, patch_complete) = self.read_patch_hint_paths(item).await;
                if !patch_complete {
                    truncated = true;
                }
                for path in patch_paths {
                    if !admit_hint(
                        path,
                        &mut changed_paths,
                        &mut metadata_bytes,
                        &mut truncated,
                    )? {
                        break;
                    }
                }
            }
            if changed_paths.is_empty() {
                // No inline target and no resolvable patch: the diff, if
                // any, stays behind `patchRef`, so downstream coverage is
                // incomplete by construction.
                truncated = true;
            }
            data["changed_paths"] = json!(changed_paths);
            data["changed_paths_truncated"] = Value::Bool(truncated);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("Muse tool completion exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &turn_id,
            &item.item_id,
            "tool.completed",
        )?;
        self.commit_event("tool.completed", native_id, data).await
    }

    /// One bounded `item/readOutput` fetch of the stored patch for a
    /// completed edit-family item with no inline target. The transport
    /// client is cloned up front and the read runs before any commit, so it
    /// holds no actor or Store lock; the tighter read deadline (not the
    /// control-plane timeout) bounds how long the sequential loop waits
    /// before serving other items. Any failure or timeout returns no paths
    /// with the page marked incomplete, and the caller keeps today's
    /// empty-paths coverage gap.
    async fn read_patch_hint_paths(&self, item: &NativeItem) -> (Vec<String>, bool) {
        let incomplete: (Vec<String>, bool) = (Vec::new(), false);
        let Some(patch_ref) = item.patch_ref.as_ref() else {
            return incomplete;
        };
        if patch_ref.id.trim().is_empty() {
            return incomplete;
        }
        if patch_ref
            .availability
            .as_deref()
            .is_some_and(|availability| availability != "available")
        {
            return incomplete;
        }
        let Some(session_id) = self.status.session_id.clone() else {
            return incomplete;
        };
        let client = self.native.clone();
        fetch_patch_page_paths(&client, &session_id, &item.item_id, patch_ref).await
    }

    /// The exact Eliot turn/command a tool item binds to, or `None` for a
    /// turn-less `userShell` (the one kind outside any turn), which stays
    /// unmapped. `validate_item_turn` already proved the session and the
    /// exact active turn; anything else here is a binding failure.
    fn tool_binding(&self, item: &NativeItem) -> Result<Option<(Option<String>, String)>> {
        let turn_id = item.turn_id.as_deref().filter(|turn| !turn.is_empty());
        let Some(turn_id) = turn_id else {
            if item.kind == "userShell" {
                return Ok(None);
            }
            return Err(Error::new(
                "source_integrity",
                "Muse toolCall omitted its turn binding",
            ));
        };
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse tool item was outside the exact active turn",
            ));
        }
        Ok(Some((
            self.status.active_command_id.clone(),
            turn_id.to_owned(),
        )))
    }

    /// Commit one natively observed turn terminal.
    async fn complete_turn(&mut self, params: Value) -> Result<()> {
        let terminal_tag = params
            .get("terminal")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let notification: NativeTurnCompletedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse turn/completed did not match the 1.4 schema",
                )
            })?;
        validate_session_and_turn(
            &self.status,
            &notification.session_id,
            &notification.turn_id,
        )?;
        if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
            return self
                .record_gap("Muse terminal notification did not match the exact active turn")
                .await;
        }
        if notification.terminal == NativeTurnTerminal::Other
            && let Some(native_tag) = terminal_tag.as_deref()
        {
            self.record_unclassified_native("muse.turn.completed.terminal", native_tag)
                .await?;
        }
        if self.status.view_unavailable
            || (notification.terminal == NativeTurnTerminal::Failed && notification.error.is_none())
        {
            self.status.needs_idle_proof = true;
            if !self.status.view_unavailable {
                self.view_unavailable(&notification.session_id, NativeHistoryNoneReason::Other)
                    .await?;
            }
            // Leave the cursor and active turn in place until the daemon has
            // command-plane evidence. A projection can synthesize this frame.
            return Err(Error::new(
                "native_readback",
                "Muse native view is unavailable; terminal requires command-plane confirmation",
            ));
        }
        let model_verified =
            self.status.provider_model_turn.as_deref() == Some(notification.turn_id.as_str());
        let model_error = if let Some((_, reason)) = self.status.model_mismatch.as_ref() {
            Some(("model_mismatch", reason.clone()))
        } else {
            None
        };
        if model_error.is_none() {
            self.flush_observed_messages(&notification.turn_id).await?;
        }
        let status_text = if model_error.is_some() {
            "failed"
        } else {
            turn_status(&notification.terminal).unwrap_or("other")
        };
        let command_id = self.status.active_command_id.clone();
        let provider_restriction = terminal_provider_restriction(&self.status, &notification);
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let mut data = json!({
            "turn_id": notification.turn_id,
            "status": status_text,
            "outcome": if model_error.is_some() {
                "failure"
            } else {
                turn_outcome(&notification.terminal).unwrap_or("unknown")
            },
            "native_status": native_terminal(&notification.terminal).unwrap_or("other"),
        });
        if model_error.is_none()
            && !model_verified
            && notification.terminal == NativeTurnTerminal::Completed
        {
            data["model_unverified_reason"] = json!(eliot_core::PROVIDER_MODEL_UNNAMED);
        }
        if let Some(command_id) = command_id.as_deref() {
            data["command_id"] = json!(command_id);
        }
        // The semantic hold signal gets first claim on the bounded terminal data.
        if notification.terminal == NativeTurnTerminal::Failed {
            let origin = if provider_restriction.is_some() {
                eliot_core::FailureOrigin::Provider
            } else if notification.error.is_some() {
                eliot_core::FailureOrigin::Harness
            } else {
                eliot_core::FailureOrigin::Unknown
            };
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "muse",
                origin,
                if origin == eliot_core::FailureOrigin::Provider {
                    "MSP authRequired, typed quota or exact provider HTTP 429 refusal"
                } else {
                    "no documented structured upstream error on native terminal"
                },
                &secrets,
            );
        }
        if let Some(restriction) = provider_restriction {
            attach_provider_restriction(&mut data, restriction, &secrets);
        }
        // Optional terminal turn-aggregate usage: attach before the final
        // text so the existing bounded terminal-text accounting includes the
        // usage bytes. Absent/malformed usage never blocks or gaps this
        // terminal; it simply leaves `data.usage` unset.
        let usage = if notification.usage.is_none() {
            self.turn_completion_usage
                .as_ref()
                .filter(|(turn_id, _)| turn_id == &notification.turn_id)
                .map(|(_, usage)| usage.clone())
        } else {
            turn_usage(&notification)
        };
        if let Some(usage) = usage {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
        }
        // The durable terminal record keeps the structured error message,
        // falling back to the terminal reason the readback used to drop.
        let native_error = model_error
            .as_ref()
            .map(|(_, reason)| reason.clone())
            .or_else(|| turn_native_error(&notification, &notification.terminal, &secrets));
        attach_native_error(&mut data, native_error.as_deref(), &secrets);
        if let Some((code, _)) = model_error {
            data["native_error"]["code"] = json!(code);
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "muse",
                eliot_core::FailureOrigin::Harness,
                "Muse raw provider response model check",
                &secrets,
            );
        }
        if let Some(text) = self.messages.final_text.as_ref() {
            attach_final_text(&mut data, text);
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &notification.turn_id,
            &notification.turn_id,
            "turn.completed",
        )?;
        let committed = match self.commit_event("turn.completed", native_id, data).await {
            Ok(()) => true,
            Err(error) if error.failure_where.as_deref() == Some("plugin.events.put") => false,
            Err(error) => return Err(error),
        };
        let dead_turn_id = notification.turn_id.clone();
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: notification.turn_id,
            status: Some(status_text.to_owned()),
            final_text: self.messages.final_text.take(),
            native_error,
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        if committed {
            self.status.model_stop_at = None;
        }
        self.active_native_command_id = None;
        self.turn_completion_usage = None;
        self.messages = TurnMessages::default();
        self.status.clear_stale_idle_latch();
        // A definite Store rejection leaves degraded coverage and requires a
        // fresh native idle proof before the next send. The host owns the gap.
        self.status.needs_idle_proof = !committed;
        self.status.source_unknown |= !committed;
        if self
            .status
            .last_turn
            .as_ref()
            .and_then(|turn| turn.native_error.as_deref())
            .is_some_and(is_mcp_startup_audit_failure)
        {
            self.status.mcp_startup_audit_failed = true;
        }
        self.gap_reported = false;
        // The turn's requests die with it: drop them locally (their Store
        // rows stay bound to the dead turn, so no later reply can address
        // them) and swallow their late resolutions. Resume orphans that never
        // materialized clear silently — they belonged to a dead turn too.
        let dropped = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| entry.request.turn_id == dead_turn_id)
            .filter_map(|(id, entry)| {
                pending_native_id(&entry.request).map(|native| (id.clone(), native.to_owned()))
            })
            .collect::<Vec<_>>();
        if !dropped.is_empty() {
            let observed = self.observe();
            for (request_id, native_id) in dropped {
                self.pending_requests.remove(&request_id);
                self.native_index.remove(&native_id);
                self.remember_dropped(native_id, observed);
            }
        }
        self.expected_requests.clear();
        // Parent completion keeps child tracking: workflow children outlive
        // the parent turn and settle on their own terminal revisions.
        if committed {
            self.commit_turn_session_usage(&dead_turn_id).await
        } else {
            Ok(())
        }
    }

    fn remember_resolved(&mut self, native_id: String) {
        self.resolved_native_ids.insert(native_id.clone());
        self.resolved_native_order.push_back(native_id);
        while self.resolved_native_order.len() > RESOLVED_REQUEST_HISTORY {
            if let Some(expired) = self.resolved_native_order.pop_front() {
                self.resolved_native_ids.remove(&expired);
            }
        }
    }

    fn remember_dropped(&mut self, native_id: String, observed: u64) {
        self.dropped_native_at.insert(native_id.clone(), observed);
        self.dropped_native_order.push_back(native_id);
        while self.dropped_native_order.len() > RESOLVED_REQUEST_HISTORY {
            if let Some(expired) = self.dropped_native_order.pop_front() {
                self.dropped_native_at.remove(&expired);
            }
        }
    }

    async fn resolve_approval(&mut self, params: Value) -> Result<()> {
        let notification: NativeApprovalResolvedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse approval/resolved did not match the 1.4 schema",
                )
            })?;
        let session_id = self.status.session_id.as_deref().ok_or_else(|| {
            Error::new("unknown", "Muse approval resolution has no owned session")
        })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse approval resolution named another native session")
                .await;
        }
        let native_id = notification.approval_id.as_str();
        if self.resolved_native_ids.contains(native_id) {
            return Ok(());
        }
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.expected_requests.remove(native_id) {
                // A resume orphan settled before its re-issued open arrived:
                // the stream won, nothing was ever opened, nothing is
                // committed.
                self.observe();
                self.remember_resolved(native_id.to_owned());
                return Ok(());
            }
            if self.dropped_native_at.contains_key(native_id) {
                return Ok(());
            }
            return self
                .record_gap("Muse approval resolution did not match a retained open")
                .await;
        };
        let Some(entry) = self.pending_requests.get(&request_id) else {
            return self
                .record_gap("Muse approval resolution lost its pending request")
                .await;
        };
        if entry.request.kind != "approval" {
            return self
                .record_gap("Muse approval resolution did not match a retained open")
                .await;
        }
        // The wire turn must name the exact retained turn before anything
        // is committed, removed, or recorded as proof: a resolution for
        // another turn never qualifies and never mutates this entry.
        if notification.turn_id != entry.request.turn_id {
            return self
                .record_gap("Muse approval resolution named another turn")
                .await;
        }
        let turn_id = entry.request.turn_id.clone();
        // `request.resolved` binds the exact dispatched Eliot reply: an
        // acked decide, or a Writing/Unknown one the resolution itself names
        // via `decidedByCommandId` (resolved-before-qualifying-ACK order,
        // and late proof for an admitted-but-unproven Unknown). Anything
        // else (policy/judge/timeout, another decider, an unnamed resolution,
        // or a reply this generation never wrote) closes without a binding.
        // Writing is reachable here only from inside the decide view-wait,
        // which pumps frames while its own RPC is outstanding; anywhere else
        // the sequential loop keeps Writing and resolution apart.
        let proven_ours = matches!(
            entry.reply_state,
            NativeReplyState::Writing | NativeReplyState::Unknown
        ) && entry.reply_command_id.is_some()
            && entry
                .reply_native_command_id
                .as_deref()
                .is_some_and(|ours| {
                    resolved_decider_matches(notification.decided_by_command_id.as_deref(), ours)
                });
        let (kind, data) = match (entry.reply_state, entry.reply_command_id.as_ref()) {
            (NativeReplyState::Written, Some(command_id)) => (
                "request.resolved",
                json!({
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
            (NativeReplyState::Writing | NativeReplyState::Unknown, Some(command_id))
                if proven_ours =>
            {
                (
                    "request.resolved",
                    json!({
                        "command_id": command_id,
                        "request_id": request_id,
                        "turn_id": turn_id,
                    }),
                )
            }
            _ => (
                "request.closed",
                json!({
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
        };
        let commit_id = request_event_id(&request_id, kind, "muse", "Muse")?;
        let bound_to_ours = kind == "request.resolved";
        self.commit_event(kind, commit_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.native_index.remove(native_id);
        self.observe();
        self.remember_resolved(native_id.to_owned());
        // Terminal proof for an active decide is recorded only here, after
        // the owning commit above succeeded. The helper additionally
        // requires an exact decider match, so a bound-but-unnamed or
        // foreign-decider resolution is decisive without being Applied.
        self.note_decide_resolved(
            notification.session_id.as_str(),
            notification.turn_id.as_str(),
            native_id,
            notification.decided_by_command_id.as_deref(),
            bound_to_ours,
        );
        Ok(())
    }

    /// `approval/updated`: one stage was satisfied while further requirements
    /// remain. A genuinely newer requirement replaces the in-memory offer and
    /// is persisted under the exact pending id so the owner projection exposes
    /// the current offer; a consumed reply resets to Available so the next
    /// stage can be decided. Duplicates and stale offers touch nothing but the
    /// observation counter; an uncertain reply is never reset nor persisted.
    async fn refresh_approval(&mut self, params: Value) -> Result<()> {
        let raw = params.clone();
        let notification: NativeApprovalUpdatedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse approval/updated did not match the 1.4 schema",
                )
            })?;
        let session_id =
            self.status.session_id.as_deref().ok_or_else(|| {
                Error::new("unknown", "Muse approval update has no owned session")
            })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse approval update named another native session")
                .await;
        }
        let native_id = notification.approval_id.as_str();
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.resolved_native_ids.contains(native_id)
                || self.expected_requests.contains(native_id)
            {
                return Ok(());
            }
            return self
                .record_gap("Muse approval update arrived without its retained open")
                .await;
        };
        let observed = self.observe();
        let (prior, same, newer) = self
            .pending_requests
            .get(&request_id)
            .map(|entry| {
                (
                    entry.reply_state,
                    same_approval_offer(&entry.request.details, &raw),
                    is_newer_approval_requirement(&entry.request.details, &raw),
                )
            })
            .unwrap_or((NativeReplyState::Available, false, false));
        if same || !newer {
            if let Some(entry) = self.pending_requests.get_mut(&request_id) {
                entry.observed_at = observed;
            }
            return Ok(());
        }
        let refreshed = if let Some(entry) = self.pending_requests.get_mut(&request_id) {
            entry.observed_at = observed;
            if refresh_offer_from_raw(&mut entry.request.details, &raw).is_err() {
                None
            } else {
                if entry.reply_state == NativeReplyState::Written {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
                Some(entry.request.clone())
            }
        } else {
            None
        };
        let Some(refreshed) = refreshed else {
            return self
                .record_gap("Muse approval update omitted its refreshed offer")
                .await;
        };
        if matches!(
            prior,
            NativeReplyState::Available | NativeReplyState::Written
        ) && !self.commit_offer_update(&refreshed).await?
        {
            return Ok(());
        }
        if let Some(seq) = refreshed.details.get("offer_seq").and_then(Value::as_i64) {
            self.note_decide_offer_proof(
                notification.session_id.as_str(),
                refreshed.turn_id.as_str(),
                native_id,
                seq,
            );
        }
        Ok(())
    }

    async fn settle_user_input(&mut self, params: Value) -> Result<()> {
        let notification: NativeUserInputSettledNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse userInput/settled did not match the 1.4 schema",
                )
            })?;
        let session_id =
            self.status.session_id.as_deref().ok_or_else(|| {
                Error::new("unknown", "Muse prompt settlement has no owned session")
            })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse prompt settlement named another native session")
                .await;
        }
        let native_id = notification.user_input_id.as_str();
        if self.resolved_native_ids.contains(native_id) {
            return Ok(());
        }
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.expected_requests.remove(native_id) {
                self.observe();
                return Ok(());
            }
            if self.dropped_native_at.contains_key(native_id) {
                return Ok(());
            }
            return self
                .record_gap("Muse prompt settlement did not match a retained open")
                .await;
        };
        let Some(entry) = self.pending_requests.get(&request_id) else {
            return self
                .record_gap("Muse prompt settlement lost its pending request")
                .await;
        };
        if entry.request.kind != "user_input" {
            return self
                .record_gap("Muse prompt settlement did not match a retained open")
                .await;
        }
        let turn_id = entry.request.turn_id.clone();
        let (kind, data) = match (entry.reply_state, entry.reply_command_id.as_ref()) {
            (NativeReplyState::Written, Some(command_id)) => (
                "request.resolved",
                json!({
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
            _ => (
                "request.closed",
                json!({
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
        };
        let commit_id = request_event_id(&request_id, kind, "muse", "Muse")?;
        self.commit_event(kind, commit_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.native_index.remove(native_id);
        self.observe();
        self.remember_resolved(native_id.to_owned());
        Ok(())
    }

    async fn observe_session_started(&mut self, params: &Value) -> Result<()> {
        let notification: NativeSessionStartedNotification = serde_json::from_value(params.clone())
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse session/started omitted its exact session id",
                )
            })?;
        if notification.session.session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Muse session/started omitted its exact session id",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|existing| existing != notification.session.session_id)
        {
            return self
                .record_gap("Muse session host announced a second session in one Agent generation")
                .await;
        }
        self.status.session_id = Some(notification.session.session_id);
        Ok(())
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse source acknowledgement is unresolved",
            ));
        }
        let native_id = format!(
            "muse:{}:session.ended",
            self.status.session_id.as_deref().unwrap_or("unresolved")
        );
        self.commit_event("session.ended", native_id, json!({}))
            .await?;
        if let Some(turn_id) = self.status.active_turn_id.clone() {
            self.status.last_turn = Some(TurnFacts {
                command_id: self.status.active_command_id.clone(),
                turn_id,
                status: None,
                final_text: None,
                native_error: None,
                terminal: false,
            });
        }
        self.status.stopped = true;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
        self.active_native_command_id = None;
        self.turn_completion_usage = None;
        self.messages = TurnMessages::default();
        Ok(())
    }

    async fn finish_pending_end_stop(&mut self) {
        if !self.frames_drained {
            return;
        }
        if let Some(reply) = self.pending_end_stop.take() {
            let result = self.finish_stop().await;
            let _ = reply.send(result);
        }
    }

    /// Fold one workflow-item revision: every `children[]` entry drives its
    /// canonical key K independently. One malformed entry gaps the parent
    /// source; proven siblings still fold.
    async fn observe_workflow_item(&mut self, params: Value) -> Result<()> {
        let notification: NativeWorkflowItemNotification =
            serde_json::from_value(params).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse workflow item did not match the installed schema",
                )
            })?;
        if self.status.session_id.as_deref() != Some(notification.session_id.as_str()) {
            return self
                .record_gap("Muse workflow update named another native session")
                .await;
        }
        for child in &notification.item.children {
            if let Err(error) = self.observe_workflow_child(&notification, child).await {
                self.status.source_unknown = true;
                if !self.gap_reported
                    && !self.writes_blocked
                    && error.failure_where.as_deref() != Some("plugin.events.put")
                {
                    let _ = self
                        .record_gap("Muse workflow child entry could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(*error.message);
                }
            }
        }
        Ok(())
    }

    /// Drive one child entry to its plan. Child-scoped failures become child
    /// gaps; only a malformed binding errors to the parent gap above.
    async fn observe_workflow_child(
        &mut self,
        notification: &NativeWorkflowItemNotification,
        child: &NativeWorkflowChild,
    ) -> Result<()> {
        let key = muse_child_key(
            &notification.session_id,
            &notification.item.workflow_run_id,
            &child.child_id,
            child.attempt,
        )?;
        if self.workflow_unknown.contains(&key) {
            return Ok(());
        }
        if !self.workflow_children.contains_key(&key) {
            if self.workflow_children.len() >= MAX_WORKFLOW_CHILDREN {
                // Detailed tracking is full, but this proven child must stay
                // visible: persist it through one idempotent registration
                // plus an honest observation-unavailable child gap. No state
                // retained, no replay, no eviction, no turn facts.
                self.persist_overflow_child(&key).await;
                self.remember_workflow_unknown(key);
                return Ok(());
            }
            self.workflow_children
                .insert(key.clone(), WorkflowChildState::new());
        }
        if let Some(state) = self.workflow_children.get_mut(&key) {
            state.observe_rev(
                notification.item.revision,
                &notification.item.status,
                notification.source_range.clone(),
            );
        }
        loop {
            let planned = self
                .workflow_children
                .get(&key)
                .map(|state| state.plan(&child.status, child.terminal.as_deref()))
                .unwrap_or(ChildAction::Ignore);
            match planned {
                ChildAction::Ignore => return Ok(()),
                ChildAction::ConflictingTerminal => {
                    let host = self.host.clone();
                    let credential = self.credential.clone();
                    let sink = &mut self.sink;
                    if let Some(state) = self.workflow_children.get_mut(&key) {
                        let _ = state
                            .record_gap(
                                sink,
                                &host,
                                &credential,
                                "Muse workflow child terminal changed after completion",
                            )
                            .await;
                    }
                    return Ok(());
                }
                ChildAction::UnknownTerminal => {
                    if let Some(native_tag) = child.terminal.as_deref()
                        && NativeTurnTerminal::parse_workflow_terminal(native_tag)
                            == NativeTurnTerminal::Other
                    {
                        self.record_unclassified_native("muse.workflow.child.terminal", native_tag)
                            .await?;
                    }
                    let host = self.host.clone();
                    let credential = self.credential.clone();
                    let sink = &mut self.sink;
                    if let Some(state) = self.workflow_children.get_mut(&key)
                        && state.lifecycle.observation_lost()
                    {
                        let _ = state
                            .record_gap(
                                sink,
                                &host,
                                &credential,
                                "Muse workflow child terminal is unknown",
                            )
                            .await;
                    }
                    return Ok(());
                }
                ChildAction::Register => {
                    if !self.register_workflow_child(&key).await {
                        return Ok(());
                    }
                }
                ChildAction::Start => {
                    self.start_workflow_child_turn(&key).await;
                    return Ok(());
                }
                ChildAction::Complete { status, native } => {
                    self.complete_workflow_child_turn(
                        &key,
                        status,
                        native,
                        notification.item.revision,
                    )
                    .await;
                    return Ok(());
                }
            }
        }
    }

    /// Persist one proven over-limit child without detailed tracking: a
    /// single idempotent registration makes the Agent row visible with
    /// Unknown state, and one child-scoped gap marks turn observation
    /// unavailable. No turn facts are committed, no state is retained, and
    /// an uncertain outcome is never replayed — the unknown marker stops
    /// later revisions, and idempotent re-registration stays safe if the
    /// bounded marker ever expires. Only registration failure falls back to
    /// a parent gap, mirroring the tracked-key rejection path.
    async fn persist_overflow_child(&mut self, key: &WorkflowChildKey) {
        let Some(native_scope) = self.native_scope.clone() else {
            let _ = self
                .record_gap("Muse workflow child registration lacks its native scope")
                .await;
            return;
        };
        let registration =
            match key.build_registration(&self.agent_id, self.generation, &native_scope) {
                Ok(registration) => registration,
                Err(_) => {
                    let _ = self
                        .record_gap("Muse workflow child registration could not be built")
                        .await;
                    return;
                }
            };
        let ack = match register_child(
            &self.host,
            &self.credential,
            registration.clone(),
            "muse",
            &WORKFLOW_CHILD_FAMILIES,
        )
        .await
        {
            Ok(ack) => ack,
            Err(error) if is_admission_rejection(&error) => {
                let _ = self
                    .record_gap("Muse workflow child registration was rejected")
                    .await;
                return;
            }
            Err(_) => {
                let _ = self
                    .record_gap("Muse workflow child registration outcome is unknown")
                    .await;
                return;
            }
        };
        let mut ephemeral = WorkflowChildState::new();
        ephemeral.binding = Some(binding_from_ack(registration, &ack));
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        // Best-effort explicit unavailability: even if this RPC is lost, the
        // visible Unknown row with no turns stays honest (nothing claimed),
        // while a parent gap here would misattribute parent-source loss.
        let _ = ephemeral
            .record_gap(
                sink,
                &host,
                &credential,
                "Muse workflow-child observation unavailable: tracking exceeded its bound",
            )
            .await;
    }

    /// Register one workflow child, retrying the retained identical
    /// registration after a lost ACK. Returns true when the caller should
    /// re-plan the same revision against the new binding.
    async fn register_workflow_child(&mut self, key: &WorkflowChildKey) -> bool {
        let Some(native_scope) = self.native_scope.clone() else {
            // Readiness always binds the scope before workflow routing; a
            // missing scope retries on the next revision, never unknowns.
            let _ = self
                .record_gap("Muse workflow child registration lacks its native scope")
                .await;
            return false;
        };
        let retained = self
            .workflow_children
            .get(key)
            .and_then(|state| state.pending_registration.clone());
        let registration = match retained {
            Some(registration) => registration,
            None => match key.build_registration(&self.agent_id, self.generation, &native_scope) {
                Ok(registration) => registration,
                Err(_) => {
                    if let Some(state) = self.workflow_children.get_mut(key) {
                        state.lifecycle.observation_lost();
                    }
                    let _ = self
                        .record_gap("Muse workflow child registration could not be built")
                        .await;
                    return false;
                }
            },
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            state.pending_registration = Some(registration.clone());
        }
        match register_child(
            &self.host,
            &self.credential,
            registration.clone(),
            "muse",
            &WORKFLOW_CHILD_FAMILIES,
        )
        .await
        {
            Ok(ack) => {
                let binding = binding_from_ack(registration, &ack);
                if let Some(state) = self.workflow_children.get_mut(key) {
                    state.pending_registration = None;
                    state.binding = Some(binding);
                }
                true
            }
            Err(error) if is_admission_rejection(&error) => {
                if let Some(state) = self.workflow_children.get_mut(key) {
                    state.pending_registration = None;
                    state.lifecycle.observation_lost();
                }
                let _ = self
                    .record_gap("Muse workflow child registration was rejected")
                    .await;
                false
            }
            // Retain the exact registration ID and payload. A later revision
            // retries that same id, never a new external registration.
            Err(_) => false,
        }
    }

    async fn start_workflow_child_turn(&mut self, key: &WorkflowChildKey) {
        let native_id = match child_event_id(key, "turn.started") {
            Ok(native_id) => native_id,
            Err(_) => {
                let host = self.host.clone();
                let credential = self.credential.clone();
                let sink = &mut self.sink;
                if let Some(state) = self.workflow_children.get_mut(key)
                    && state.lifecycle.observation_lost()
                {
                    let _ = state
                        .record_gap(
                            sink,
                            &host,
                            &credential,
                            "Muse workflow child turn identity exceeded its bound",
                        )
                        .await;
                }
                return;
            }
        };
        let data = start_event_data(key);
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let committed = match self.workflow_children.get_mut(key) {
            Some(state) => state
                .commit_event(sink, &host, &credential, "turn.started", native_id, data)
                .await
                .is_ok(),
            None => return,
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            if committed {
                state.lifecycle.started(&key.turn_id());
            } else {
                state.lifecycle.observation_lost();
            }
        }
    }

    async fn complete_workflow_child_turn(
        &mut self,
        key: &WorkflowChildKey,
        status: &'static str,
        native: &'static str,
        revision: i64,
    ) {
        if self
            .workflow_children
            .get(key)
            .is_some_and(|state| state.lifecycle.active_turn.is_none())
        {
            self.start_workflow_child_turn(key).await;
            if self
                .workflow_children
                .get(key)
                .is_some_and(|state| state.lifecycle.active_turn.is_none())
            {
                return;
            }
        }
        let native_id = match child_event_id(key, "turn.completed") {
            Ok(native_id) => native_id,
            Err(_) => {
                let host = self.host.clone();
                let credential = self.credential.clone();
                let sink = &mut self.sink;
                if let Some(state) = self.workflow_children.get_mut(key)
                    && state.lifecycle.observation_lost()
                {
                    let _ = state
                        .record_gap(
                            sink,
                            &host,
                            &credential,
                            "Muse workflow child turn identity exceeded its bound",
                        )
                        .await;
                }
                return;
            }
        };
        let data = complete_event_data(key, status, native);
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let committed = match self.workflow_children.get_mut(key) {
            Some(state) => state
                .commit_event(sink, &host, &credential, "turn.completed", native_id, data)
                .await
                .is_ok(),
            None => return,
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            if committed {
                state.lifecycle.completed(Some(status));
                state.completed_revision = Some(revision);
            } else {
                state.lifecycle.observation_lost();
            }
        }
    }

    async fn settle_workflow_children(&mut self) {
        let open: Vec<WorkflowChildKey> = self
            .workflow_children
            .iter()
            .filter(|entry| entry.1.lifecycle.terminal.is_none() && !entry.1.lifecycle.unknown)
            .map(|entry| entry.0.clone())
            .collect();
        if open.is_empty() {
            return;
        }
        let mut pending_lost = false;
        for key in &open {
            if let Some(state) = self.workflow_children.get_mut(key) {
                if state.pending_registration.is_some() {
                    state.pending_registration = None;
                    pending_lost = true;
                }
                state.lifecycle.observation_lost();
            }
        }
        for key in &open {
            let running = self.workflow_children.get(key).is_some_and(|state| {
                state.lifecycle.active_turn.is_some()
                    && state.lifecycle.terminal.is_none()
                    && state.binding.is_some()
            });
            if !running {
                continue;
            }
            let host = self.host.clone();
            let credential = self.credential.clone();
            let sink = &mut self.sink;
            if let Some(state) = self.workflow_children.get_mut(key) {
                let _ = state
                    .record_gap(
                        sink,
                        &host,
                        &credential,
                        "Muse workflow child observation ended before its terminal",
                    )
                    .await;
            }
        }
        if pending_lost {
            let _ = self
                .record_gap("Muse workflow child registration acknowledgement is unresolved")
                .await;
        }
    }

    fn remember_workflow_unknown(&mut self, key: WorkflowChildKey) {
        if self.workflow_unknown.insert(key.clone()) {
            self.workflow_unknown_order.push_back(key);
            while self.workflow_unknown_order.len() > MAX_WORKFLOW_CHILDREN {
                if let Some(expired) = self.workflow_unknown_order.pop_front() {
                    self.workflow_unknown.remove(&expired);
                }
            }
        }
    }

    async fn record_unclassified_native(
        &mut self,
        site: &'static str,
        native_tag: &str,
    ) -> Result<()> {
        let native_id = format!("muse-unclassified:{}", ulid::Ulid::new());
        self.commit_event(
            UNCLASSIFIED_NATIVE_EVENT,
            native_id,
            unclassified(site, native_tag),
        )
        .await
    }

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await
    }

    /// Persist a refreshed offer. Returns `Ok(true)` only after the actual
    /// required `request.opened` commit succeeded. `Ok(false)` means the
    /// call was handled but nothing was persisted (a gap was recorded, or
    /// an oversized envelope was deferred); `Err` carries the write failure.
    /// Callers that need persistence proof must require `Ok(true)`.
    async fn commit_offer_update(&mut self, refreshed: &CorePending) -> Result<bool> {
        let command_id = self.status.active_command_id.clone();
        let (Some(seq), Some(cursor)) = (
            refreshed.details.get("offer_seq").and_then(Value::as_i64),
            refreshed.details.get("view_cursor").and_then(Value::as_str),
        ) else {
            self.record_gap("Muse refreshed offer omitted its identity")
                .await?;
            return Ok(false);
        };
        if cursor.trim().is_empty() {
            self.record_gap("Muse refreshed offer omitted its identity")
                .await?;
            return Ok(false);
        }
        let native_id = format!("muse-o:{}:s{seq}:{cursor}", refreshed.id);
        if native_id.len() > MAX_COMMAND_ID_BYTES {
            self.record_gap("Muse refreshed offer identity exceeded its bound")
                .await?;
            return Ok(false);
        }
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let Ok((public_kind, public_details)) = public_request_view(refreshed) else {
            self.record_gap("Muse refreshed public offer could not be normalized")
                .await?;
            return Ok(false);
        };
        let mut data = json!({
            "request_id": refreshed.id,
            "turn_id": refreshed.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse refreshed offer exceeded the persisted-event bound");
            return Ok(false);
        }
        self.commit_event("request.opened", native_id, data).await?;
        Ok(true)
    }

    async fn report_view_stale(&mut self) {
        if !std::mem::replace(&mut self.view_stale_reported, true) {
            self.set_source_health("stale", Instant::now() + EVENT_DEADLINE)
                .await;
        }
    }

    async fn set_source_health(&mut self, health: &str, deadline: Instant) -> bool {
        let update =
            SourceHealthUpdate::new(self.agent_id.clone(), self.generation, "stream", health);
        let Ok(mut request) = serde_json::to_value(&update) else {
            self.status.source_unknown = true;
            return false;
        };
        if update.validate().is_err() {
            self.status.source_unknown = true;
            return false;
        }
        request["credential"] = json!(self.credential);
        let acknowledgement = self.host.call("source.health", request, deadline).await;
        if acknowledgement.as_ref().is_ok_and(|value| {
            serde_json::from_value::<SourceHealthUpdate>(value.clone())
                .is_ok_and(|ack| ack == update)
        }) {
            return true;
        }

        self.status.source_unknown = true;
        false
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.stream_source().record_gap(reason).await
    }

    /// The shared stream-commit half of this processor.
    fn stream_source(&mut self) -> StreamSource<'_> {
        self.stream_source_and_messages().0
    }

    fn stream_source_and_messages(&mut self) -> (StreamSource<'_>, &mut TurnMessages) {
        let source = StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent_id,
            generation: self.generation,
            harness: "Muse",
            id_prefix: "muse",
            families: &["session", "turn", "message", "tool", "request"],
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.status.source_unknown,
            batch_unknown: &mut self.status.batch_unknown,
            failure: &mut self.status.failure,
        };
        (source, &mut self.messages)
    }
}

async fn run(
    mut frames: mpsc::Receiver<NativeFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut controls: mpsc::Receiver<Control>,
    status_tx: watch::Sender<EventStatus>,
) {
    loop {
        let accept_frames = processor.accept_frames;
        // Frames win over controls when both are ready: a `CheckTurn`
        // arriving after the queued terminal frame must see the finished
        // turn, never a still-live one.
        tokio::select! {
            biased;
            frame = frames.recv(), if accept_frames => {
                if frame.is_none() {
                    let current = *stream.borrow_and_update();
                    processor.handle_stream_state(current).await;
                }
                if !processor.handle_frame(frame).await {
                    break;
                }
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            changed = stream.changed(), if accept_frames => {
                let current = match changed {
                    Ok(()) => *stream.borrow_and_update(),
                    Err(_) => StreamState::Closed,
                };
                processor.handle_stream_state(current).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(&mut frames, control).await {
                            break;
                        }
                        // Associate flushes buffered frames inline and request
                        // handling defers loss the same way frames do; report
                        // here too so a deferred gap never waits on a frame
                        // that never comes.
                        processor.report_gap_if_queue_empty(frames.is_empty()).await;
                    }
                    None => break,
                }
            }
        }
        processor.status.native_drained = processor.frames_drained;
        let _ = status_tx.send(processor.status.clone());
    }
}

fn validate_session_and_turn(status: &EventStatus, session_id: &str, turn_id: &str) -> Result<()> {
    if status.session_id.as_deref() != Some(session_id) || turn_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse notification did not belong to the owned native session",
        ));
    }
    Ok(())
}

/// Shared native event identity, kept under the local path for the
/// `event_id_rejects_blank_or_oversize_identity` test.
fn event_id(session_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    shared_event_id(session_id, turn_id, item_id, kind, "muse", "Muse")
}

/// Builds a gap reason from fixed classes and boolean comparisons only.
fn native_request_gap_reason(
    method: &str,
    error_code: &str,
    params: &Value,
    expected_session: &str,
    active_turn: &str,
    active_parent_turn_present: bool,
) -> String {
    let method_class = match method {
        "approval/request" => "approval",
        "userInput/request" => "userInput",
        _ => "other",
    };
    let error_class = match error_code {
        "native_protocol" => "native_protocol",
        "source_integrity" => "source_integrity",
        "unsupported" => "unsupported",
        _ => "other",
    };
    let session_id = params.get("sessionId");
    let session_value = session_id.and_then(Value::as_str);
    let turn_id = params.get("turnId");
    let turn_value = turn_id.and_then(Value::as_str);

    format!(
        "Muse native request validation: method_class={method_class} error_code={error_class} active_parent_turn_present={active_parent_turn_present} session_id_present={} session_id_nonblank={} session_id_matches_expected={} turn_id_present={} turn_id_nonblank={} turn_id_matches_active={} subagent_origin_present={}",
        session_id.is_some(),
        session_value.is_some_and(|value| !value.trim().is_empty()),
        session_value.is_some_and(|value| value == expected_session),
        turn_id.is_some(),
        turn_value.is_some_and(|value| !value.trim().is_empty()),
        turn_value.is_some_and(|value| value == active_turn),
        params.get("subagentOrigin").is_some(),
    )
}

/// The native approval/user-input id carried by raw server-request params, if
/// any. Used only to recognize resume-expected orphans; binding still goes
/// through the typed describe path.
fn orphan_request_id(method: &str, params: &Value) -> Option<String> {
    let key = match method {
        "approval/request" => "approvalId",
        "userInput/request" => "userInputId",
        _ => return None,
    };
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// The native approval/user-input id a described request answers to.
fn pending_native_id(request: &CorePending) -> Option<&str> {
    match request.kind.as_str() {
        "approval" => request.details.get("approval_id").and_then(Value::as_str),
        "user_input" => request.details.get("user_input_id").and_then(Value::as_str),
        _ => None,
    }
}

/// MSP 1.4 item kinds normalized into the shared tool events, mirroring the
/// Codex adapter's tool-item coverage: `toolCall` (tool calls, including
/// command execution and edit-family calls) and `userShell` (command
/// execution; bound only when it names the exact active turn). Every other
/// kind keeps its existing mapping (`agentMessage` to `message.completed`,
/// `workflow` to the workflow-child observer) or stays unmapped; unknown
/// future kinds never become tool facts.
fn tool_item_kind(kind: &str) -> bool {
    matches!(kind, "toolCall" | "userShell")
}

/// Display tool for a normalized item: the `toolCall` tool name, or the
/// `userShell` kind itself. An edit-family `toolCall` normalizes as
/// `fileChange`, mirroring the Codex adapter's `fileChange` item type, so
/// native file-change hint attribution accepts the completion. A
/// `userShell` `commandText` is a full shell invocation and is never
/// persisted; a blank `toolCall` tool name proves nothing and the caller
/// gaps instead.
fn tool_label(item: &NativeItem) -> Option<String> {
    match item.kind.as_str() {
        "toolCall" => item
            .tool
            .clone()
            .filter(|tool| !tool.trim().is_empty())
            .map(|tool| {
                if is_edit_tool(&tool) {
                    "fileChange".to_owned()
                } else {
                    tool
                }
            }),
        "userShell" => Some("userShell".to_owned()),
        _ => None,
    }
}

/// Muse edit-family tool names: workspace mutators whose terminal revision
/// carries the edit diff. Observed read-only in the installed host binary
/// (`muse-bin-1.4.0-R4161.1.exe` embeds
/// `DIRECT_MUTATORS = { "apply_patch", "delete_file", "edit_file",
/// "write_file" }`); shell tools (`bash`, `exec_command`, `shell`) and
/// readers stay under their raw tool name.
fn is_edit_tool(tool: &str) -> bool {
    matches!(
        tool,
        "apply_patch" | "delete_file" | "edit_file" | "write_file"
    )
}

/// A completed edit-family `toolCall` carries the shared
/// completed-file-change payload exactly when the native status is
/// `completed`, mirroring the Codex adapter's `fileChange` + `completed`
/// gate that hint attribution requires. Other terminal statuses keep the
/// `fileChange` tool name with their verbatim status but no paths.
fn completed_file_change(item: &NativeItem) -> bool {
    item.kind == "toolCall"
        && item.tool.as_deref().is_some_and(is_edit_tool)
        && item.status == "completed"
}

/// Inline file targets carried by an edit-family `toolCall`. The 1.4 schema
/// keeps `args` verbatim for clients to parse; workspace file tools address
/// their target under `path` (observed in the installed host:
/// `tools.workspace.read_file({ path: ... })`, and write tools reject a
/// target that does not resolve to an absolute path). Only those path
/// strings are returned, at most one bound over the adapter limit; `args`
/// itself, file contents, diffs and results are never persisted. An absent,
/// undecodable or almost-JSON `args` yields no paths here: the diff, if any,
/// lives behind `patchRef`, which the caller resolves with one bounded read.
fn edit_hint_paths(item: &NativeItem) -> Vec<String> {
    let Some(args) = item.args.as_deref() else {
        return Vec::new();
    };
    let Ok(parsed) = serde_json::from_str::<Value>(args) else {
        return Vec::new();
    };
    let Some(fields) = parsed.as_object() else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    if let Some(raw) = fields.get("path") {
        if let Some(path) = raw.as_str() {
            if !path.is_empty() {
                paths.push(path.to_owned());
            }
        } else if let Some(items) = raw.as_array() {
            for raw in items {
                if paths.len() > MAX_CHANGED_PATHS {
                    break;
                }
                if let Some(path) = raw.as_str().filter(|path| !path.is_empty()) {
                    paths.push(path.to_owned());
                }
            }
        }
    }
    if paths.len() > MAX_CHANGED_PATHS + 1 {
        paths.truncate(MAX_CHANGED_PATHS + 1);
    }
    paths
}

/// Single bounded page fetch behind a stored patch reference: `offset 0`,
/// `length PATCH_READ_BYTES`, decoded per the served `encoding`. Only file
/// paths from the patch headers return alongside whether the page covered
/// the whole stored patch; the page body is dropped here and never
/// persisted. Any transport, decode or shape failure returns no paths with
/// the page marked incomplete so the caller keeps today's empty-paths
/// coverage gap.
async fn fetch_patch_page_paths(
    client: &NativeClient,
    session_id: &str,
    item_id: &str,
    patch_ref: &NativePatchRef,
) -> (Vec<String>, bool) {
    let incomplete: (Vec<String>, bool) = (Vec::new(), false);
    let Ok(params) = serde_json::to_value(NativeItemReadOutputParams {
        session_id: session_id.to_owned(),
        item_id: item_id.to_owned(),
        output_ref: patch_ref.id.clone(),
        offset_bytes: 0,
        length_bytes: PATCH_READ_BYTES,
    }) else {
        return incomplete;
    };
    let page = match client
        .call(
            "item/readOutput",
            params,
            Instant::now() + PATCH_READ_DEADLINE,
        )
        .await
    {
        Ok(page) => page,
        Err(_) => return incomplete,
    };
    let Ok(page) = serde_json::from_value::<NativeItemReadOutputResult>(page) else {
        return incomplete;
    };
    patch_page_paths(&page)
}

/// File paths from one stored patch page, plus whether the page covered
/// the whole stored patch. Only the `utf8` text encoding is consumed (the
/// `tool_patch` media type is `application/json` text); any other encoding
/// keeps today's gap and counts as incomplete. Paths come only from patch
/// headers — never from bodies, contents or args — in the verbatim
/// relative-or-absolute form the Store hint normalizer accepts (it drops
/// paths outside the workspace, same as for inline hints). A page that stops
/// short of the stored patch still contributes the paths it covered; the
/// caller marks the truncation.
fn patch_page_paths(page: &NativeItemReadOutputResult) -> (Vec<String>, bool) {
    let complete = patch_page_complete(page);
    if page.encoding != "utf8" {
        return (Vec::new(), complete);
    }
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    for text in patch_candidate_texts(&page.content) {
        for path in patch_text_paths(&text) {
            if paths.len() > MAX_CHANGED_PATHS {
                return (paths, complete);
            }
            if seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }
    (paths, complete)
}

/// Whether one stored patch page covered the whole stored patch. The host
/// reports `eof` once the page reaches the stored end; a nonzero page offset
/// or a stored length running past the fetched bytes also marks the page
/// partial where that shape is present, so downstream attribution never
/// treats a cut-off read as full coverage.
fn patch_page_complete(page: &NativeItemReadOutputResult) -> bool {
    if page.encoding != "utf8" {
        return false;
    }
    if !page.eof {
        return false;
    }
    if page.offset_bytes != 0 {
        return false;
    }
    // `content` is the decoded `utf8` text, so its byte length matches the
    // stored bytes it carries; a stored length past the fetched bytes means
    // the page was cut short even when `eof` claims otherwise.
    if page.byte_len > 0 && (page.content.len() as i64) < page.byte_len {
        return false;
    }
    true
}

/// File paths from the lines of one patch text, in order. The installed
/// host's `apply_patch` patch-text vocabulary (observed read-only in
/// `muse-bin-1.4.0-R4161.1.exe`) marks files as `*** Add File: <path>`,
/// `*** Update File: <path>`, `*** Delete File: <path>` or `*** Move to:
/// <path>` (rename destination). Inside such a `*** ... File:` section every
/// `---`/`+++` line is hunk content (a removed `-- comment` reads
/// `--- comment`, an added `++ x` reads `+++ x`) and never a header.
/// Outside those sections, unified-diff headers count only as an adjacent
/// `--- <path>` / `+++ <path>` pair; both sides are taken and a lone
/// `---`/`+++` line never contributes.
fn patch_text_paths(text: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    let mut in_host_section = false;
    while index < lines.len() {
        let line = lines[index];
        if let Some(path) = host_patch_header_path(line) {
            in_host_section = true;
            paths.push(path);
            index += 1;
            continue;
        }
        if line.starts_with("***") {
            in_host_section = false;
            index += 1;
            continue;
        }
        if !in_host_section
            && let Some(rest) = line.strip_prefix("--- ")
            && let Some(next) = lines.get(index + 1)
            && let Some(plus) = next.strip_prefix("+++ ")
        {
            if let Some(path) = diff_side_path(rest) {
                paths.push(path);
            }
            if let Some(path) = diff_side_path(plus) {
                paths.push(path);
            }
            index += 2;
            continue;
        }
        index += 1;
    }
    paths
}

/// Candidate patch texts inside one stored page body. The `tool_patch`
/// document is `application/json`: a bare patch string, or an object/array
/// whose string leaves carry the patch text. Every string leaf scans — no
/// field name is assumed — and a non-JSON body scans as raw patch text.
fn patch_candidate_texts(body: &str) -> Vec<String> {
    let Ok(document) = serde_json::from_str::<Value>(body) else {
        return vec![body.to_owned()];
    };
    let mut texts = Vec::new();
    collect_patch_texts(&document, &mut texts);
    if texts.is_empty() {
        vec![body.to_owned()]
    } else {
        texts
    }
}

/// Every string leaf of a stored patch document, in document order.
fn collect_patch_texts(document: &Value, texts: &mut Vec<String>) {
    match document {
        Value::String(text) => texts.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                collect_patch_texts(item, texts);
            }
        }
        Value::Object(fields) => {
            for (_, value) in fields {
                collect_patch_texts(value, texts);
            }
        }
        _ => {}
    }
}

/// One host patch header line to its file path: `*** Add File:`,
/// `*** Update File:`, `*** Delete File:` or `*** Move to:` (rename
/// destination). Unified-diff `---`/`+++` sides never count here; they are
/// paired up in `patch_text_paths`, where hunk position decides.
fn host_patch_header_path(line: &str) -> Option<String> {
    for marker in [
        "*** Add File:",
        "*** Update File:",
        "*** Delete File:",
        "*** Move to:",
    ] {
        if let Some(path) = line.strip_prefix(marker) {
            let path = path.trim();
            if path.is_empty() {
                return None;
            }
            return Some(path.to_owned());
        }
    }
    None
}

/// One unified-diff `---`/`+++` side to its file path, with an optional
/// `a/`/`b/` tree prefix. `/dev/null` sides and blank targets are skipped.
fn diff_side_path(rest: &str) -> Option<String> {
    let rest = rest.trim();
    if rest.is_empty() || rest == "/dev/null" {
        return None;
    }
    let path = rest
        .strip_prefix("a/")
        .or_else(|| rest.strip_prefix("b/"))
        .unwrap_or(rest);
    if path.is_empty() || path == "/dev/null" {
        return None;
    }
    Some(path.to_owned())
}

/// Terminal item revisions commit tool facts. The schema folds terminal as
/// anything other than `inProgress`; unknown statuses stay
/// terminal-unknown and still commit under their verbatim status.
fn is_terminal_item_status(status: &str) -> bool {
    status != "inProgress"
}

/// Map the MSP terminal to the stored turn status. `cancelled` folds to
/// `interrupted`, matching the Codex path; an unknown wire terminal settles
/// nothing and the caller records a gap instead. Shared with the
/// workflow-child observer: child terminals use this same mapping.
pub(crate) fn turn_status(terminal: &NativeTurnTerminal) -> Option<&'static str> {
    match terminal {
        NativeTurnTerminal::Completed => Some("completed"),
        NativeTurnTerminal::Failed => Some("failed"),
        NativeTurnTerminal::Cancelled => Some("interrupted"),
        NativeTurnTerminal::Other => None,
    }
}

/// Canonical outcome for the MSP terminal vocabulary (`completed` | `failed`
/// | `cancelled`): decided here at the plugin boundary, with the raw wire
/// terminal preserved as `native_status`. `cancelled` is an interrupt.
pub(crate) fn turn_outcome(terminal: &NativeTurnTerminal) -> Option<&'static str> {
    match terminal {
        NativeTurnTerminal::Completed => Some("success"),
        NativeTurnTerminal::Failed => Some("failure"),
        NativeTurnTerminal::Cancelled => Some("interrupted"),
        NativeTurnTerminal::Other => None,
    }
}

/// Raw wire terminal verbatim, preserved as `native_status` next to the
/// canonical outcome.
pub(crate) fn native_terminal(terminal: &NativeTurnTerminal) -> Option<&'static str> {
    match terminal {
        NativeTurnTerminal::Completed => Some("completed"),
        NativeTurnTerminal::Failed => Some("failed"),
        NativeTurnTerminal::Cancelled => Some("cancelled"),
        NativeTurnTerminal::Other => None,
    }
}

/// Native failure text for one terminal `turn/completed`: the structured
/// error message first, the terminal reason when a failed turn carried no
/// error. A vendor reason on a completed or cancelled turn never surfaces
/// as a native error. Redacted and bounded for the turn readback; turns
/// with no native text (successes, silent cancels) stay `None` so the
/// readback keeps proving nothing it was not told.
pub(crate) fn turn_native_error(
    notification: &NativeTurnCompletedNotification,
    terminal: &NativeTurnTerminal,
    policy: &SecretPolicy,
) -> Option<String> {
    let message = match notification.error.as_ref() {
        Some(error) => Some(error.message.as_str()),
        None if matches!(terminal, NativeTurnTerminal::Failed) => notification.reason.as_deref(),
        None => None,
    }?;
    if message.trim().is_empty() {
        return None;
    }
    let redacted = policy.redact_text(message);
    if redacted.trim().is_empty() {
        return None;
    }
    const MAX_NATIVE_ERROR_BYTES: usize = 4 * 1024;
    if redacted.len() <= MAX_NATIVE_ERROR_BYTES {
        return Some(redacted);
    }
    let mut end = MAX_NATIVE_ERROR_BYTES;
    while !redacted.is_char_boundary(end) {
        end -= 1;
    }
    Some(redacted[..end].to_owned())
}

fn terminal_provider_restriction(
    status: &EventStatus,
    notification: &NativeTurnCompletedNotification,
) -> Option<ProviderRestriction> {
    if status.session_id.as_deref() != Some(notification.session_id.as_str())
        || status.active_turn_id.as_deref() != Some(notification.turn_id.as_str())
    {
        return None;
    }
    notification.provider_restriction()
}
