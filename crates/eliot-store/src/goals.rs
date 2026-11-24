//! Durable Eliot-mode Goals: revisioned definitions on one Agent
//! generation plus the stale-round-safe conditional round commit.

use super::*;
use eliot_core::api::{
    CHECK_SPAWN_RETRYABLE, CommandSubmitResponse, GoalContinuationStage, GoalCounters,
    GoalDefinition, GoalMarkerState, GoalMode, GoalRoundEvent, GoalRoundEventOutcome,
    GoalRoundMarker, GoalRoundOutcome, GoalRoundRequest, GoalRoundResponse, GoalStatus, GoalView,
    MAX_GOAL_OBSERVATION_BYTES,
};
use serde::{Deserialize, Serialize};

const MAX_GOAL_CHECK_HISTORY_ROWS: i64 = 8_192;
const NATIVE_FOLLOWUP_WINDOW_MS: i64 = 5 * 60 * 1_000;

/// A Swarm note: an `inject` from a Swarm principal. An inject from the Owner
/// or a manager is never one, whatever its delivery state.
macro_rules! swarm_note_command_sql {
    () => {
        "(verb = 'inject' AND (principal GLOB 'swarm_note:*' OR principal = 'project_check'))"
    };
}

/// Any lifecycle event of a Swarm note, from admission through delivery or
/// cancellation. These neither name an idle point nor make a Goal round stale.
const SWARM_NOTE_EVENT_SQL: &str = concat!(
    "(source = 'daemon' AND (kind GLOB 'command.*' OR kind = 'context.moved') AND EXISTS (
         SELECT 1 FROM commands
         WHERE commands.id = json_extract(events.data, '$.command_id') AND ",
    swarm_note_command_sql!(),
    "))"
);

/// Pre-round facts. The Router evaluates gates from this snapshot; the commit
/// re-verifies generation, revision and round freshness atomically.
#[derive(Clone, Debug)]
pub struct GoalRoundProbe {
    pub view: GoalView,
    pub project_id: String,
    pub project_default: bool,
    /// First reason the Agent is not cleanly idle, from the same gate the
    /// claim enforces.
    pub idle_refusal: Option<Error>,
    pub max_seq: i64,
    pub turn_count: i64,
    /// True when the turn budget was spent solely by this Goal's own last
    /// admitted continuation (exactly one turn since the last round commit,
    /// and it is that continuation's completed turn). Only then may a
    /// spent-budget round still run its final check.
    pub spent_by_goal_continuation: bool,
    pub workspace: Option<String>,
    pub task_base: Option<String>,
    pub unsuccessful_turn: bool,
    /// Starting point of a Goal with `scope` or `check_inputs`.
    pub scope_base: Option<core::api::GoalScopeBase>,
}

/// Definite check result. Unknown check outcomes never reach the commit.
#[derive(Clone, Debug)]
pub struct GoalCheckResult {
    pub exit_code: Option<i32>,
    pub output_complete: bool,
    pub output_digest: Option<String>,
    pub observation: Option<String>,
}

/// One successful check attempt, frozen to its project and argv identity.
#[derive(Clone, Debug)]
pub struct GoalCheckHistoryEntry {
    pub project_id: String,
    pub check_digest: String,
    pub duration_ms: i64,
}

/// A current Goal check owned by an Agent on its current project generation.
#[derive(Clone, Debug)]
pub struct GoalCheckDeadlineTarget {
    pub project_id: String,
    pub agent: String,
    pub definition: GoalDefinition,
}

/// One ordinary `send` continuation admitted atomically with the round commit.
#[derive(Clone, Debug)]
pub struct GoalContinuation {
    pub id: String,
    pub text: String,
    /// The stage the Router built the text for; the commit re-derives it
    /// from the same counters and refuses a mismatch as stale.
    pub stage: GoalContinuationStage,
    /// Fresh command id of the escalation `configure`, set only with
    /// [`GoalContinuationStage::Escalate`].
    pub escalation_id: Option<String>,
}

/// One round-key form. Idle keys name an exact idle event seq; slot keys
/// name an anchored cadence slot index (first slot is 1). The form must
/// match the Goal: cadenced Goals round on slots, the rest on idle seqs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoalRoundKey {
    Idle(i64),
    Slot(i64),
}

impl GoalRoundKey {
    pub fn value(self) -> i64 {
        match self {
            Self::Idle(value) | Self::Slot(value) => value,
        }
    }

    pub fn kind(self) -> &'static str {
        match self {
            Self::Idle(_) => "idle",
            Self::Slot(_) => "slot",
        }
    }

    pub fn text(self) -> String {
        format!("{}:{}", self.kind(), self.value())
    }
}

/// Binding check inside an open Immediate transaction. Returns Some(view)
/// when this exact write id is an exact replay; the caller commits and
/// returns it without touching Goal state. Returns None for a fresh id.
fn goal_set_digest(agent: &str, mode: &str, definition: &GoalDefinition) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "agent": agent,
        "mode": mode,
        "definition": definition,
    }))
}

fn record_goal_check_history_tx(tx: &Transaction<'_>, entry: &GoalCheckHistoryEntry) -> Result<()> {
    db!(tx.execute(
        "INSERT INTO goal_check_history(project_id, check_digest, duration_ms)
         VALUES (?1, ?2, ?3)",
        params![entry.project_id, entry.check_digest, entry.duration_ms],
    ));
    db!(tx.execute(
        "DELETE FROM goal_check_history
         WHERE id IN (
             SELECT id FROM goal_check_history
             WHERE project_id = ?1 AND check_digest = ?2
             ORDER BY id DESC LIMIT -1 OFFSET ?3
         )",
        params![
            entry.project_id,
            entry.check_digest,
            core::api::GOAL_CHECK_HISTORY_LIMIT as i64,
        ],
    ));
    db!(tx.execute(
        "DELETE FROM goal_check_history
         WHERE id <= (
             SELECT id FROM goal_check_history ORDER BY id DESC LIMIT 1 OFFSET ?1
         )",
        params![MAX_GOAL_CHECK_HISTORY_ROWS],
    ));
    Ok(())
}

fn replay_goal_write<T: serde::de::DeserializeOwned>(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
) -> Result<Option<T>> {
    if command_binding(tx, id)?.is_some() {
        return Err(Error::new(
            "id_conflict",
            format!("{method} id is already used by a command"),
        ));
    }
    crate::ensure_no_digest_id(tx, id, "write id is already used by a completion digest")?;
    let Some((old_principal, old_method, old_digest, old_result)) = write_binding(tx, id)? else {
        return Ok(None);
    };
    if old_principal != principal || old_method != method || old_digest != payload_digest {
        return Err(Error::new(
            "id_conflict",
            format!("{method} id is bound to another principal, method, or payload"),
        ));
    }
    let value: T = serde_json::from_value(decode_json(&old_result)?)
        .map_err(|error| Error::new("internal", error.to_string()))?;
    Ok(Some(value))
}

/// Persist the original result alongside the already-applied mutation,
/// still inside the caller's transaction.
fn retain_goal_result<T: Serialize>(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
    view: &T,
    now: i64,
) -> Result<()> {
    crate::ensure_no_digest_id(tx, id, "write id is already used by a completion digest")?;
    let result_json = encode_json(view)?;
    // The retained write stores the full original GoalView, whose
    // definition alone may approach 64 KiB plus counters/marker
    // overhead. Bound it by the wire frame budget, not by the
    // smaller persisted-event budget used for ordinary events.
    if result_json.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{method} result exceeds the frame bound"),
        ));
    }
    db!(tx.execute(
        "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, principal, method, payload_digest, result_json, now],
    ));
    Ok(())
}

#[derive(Clone, Debug)]
pub struct GoalRoundCommit {
    pub id: String,
    pub principal: String,
    pub agent: String,
    pub mode: String,
    pub expected_generation: i64,
    pub expected_revision: i64,
    pub round_key: GoalRoundKey,
    /// Agent high-water observed by the claim. Slot commits require it
    /// unchanged (no Agent event during the check); idle commits pin the
    /// key itself instead.
    pub claim_seq: i64,
    pub check: Option<GoalCheckResult>,
    /// Successful command timing and frozen check identity, recorded atomically.
    pub check_history: Option<GoalCheckHistoryEntry>,
    /// Deadlines actually passed to the runner for this round's attempts.
    pub check_timeout_ms: Option<i64>,
    pub retry_timeout_ms: Option<i64>,
    /// A Router decision for a round that cannot proceed. A scope block may
    /// also override a passing check.
    pub decision: Option<GoalRoundDecision>,
    /// Bounded observation for a decision without a check result.
    pub observation: Option<String>,
    /// Workspace fingerprint read just before the check, compared with the
    /// previous round's baseline; absent when that read failed.
    pub fingerprint_before: Option<String>,
    /// Workspace fingerprint read right after a failed check: the next
    /// round's baseline, so the check's own writes are never progress.
    pub fingerprint_after: Option<String>,
    /// Exact registered project and unchanged checked worktree fingerprint.
    pub project_check: Option<(String, String)>,
    pub continuation: Option<GoalContinuation>,
}

#[derive(Clone, Debug)]
pub enum GoalRoundDecision {
    Block(String),
    RetryCheck,
    RetryCompletion(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalCommitOutcome {
    Completed,
    Blocked { reason: String },
    Continued { command_id: String },
    Retryable,
}

#[derive(Clone, Debug)]
pub struct GoalRoundCommitResult {
    pub outcome: GoalCommitOutcome,
    pub view: GoalView,
    pub admission: Option<Admission>,
    pub response: GoalRoundResponse,
    /// The escalation `configure`, admitted just before the continuation so
    /// it dispatches first on the Agent's settings lane.
    pub escalation: Option<Admission>,
    /// Committed facts of the round itself (an assignment Goal's terminal
    /// `goal.changed`); the continuation's events stay on `admission`.
    pub events: Vec<Event>,
}

impl GoalRoundCommitResult {
    fn replay(response: GoalRoundResponse) -> Result<Self> {
        let outcome = match response.outcome {
            GoalRoundOutcome::Completed => GoalCommitOutcome::Completed,
            GoalRoundOutcome::Blocked => GoalCommitOutcome::Blocked {
                reason: response
                    .reason
                    .clone()
                    .ok_or_else(|| Error::new("internal", "stored blocked round has no reason"))?,
            },
            GoalRoundOutcome::Continued => GoalCommitOutcome::Continued {
                command_id: response.command_id.clone().ok_or_else(|| {
                    Error::new("internal", "stored continued round has no command id")
                })?,
            },
            GoalRoundOutcome::Retryable => GoalCommitOutcome::Retryable,
        };
        Ok(Self {
            outcome,
            view: response.view.clone(),
            admission: None,
            response,
            escalation: None,
            events: Vec::new(),
        })
    }
}

/// One active Eliot-mode Goal that could round on its retained idle
/// event. `idle_seq` carries the latest event seq only when that row
/// still names an idle event of the Goal's generation; otherwise the
/// retained evidence is pruned, superseded or never existed and the
/// Router waits for a real event instead of inventing one.
/// `key_consumed` names the same burn the probe and claim enforce: the
/// committed key matches or a non-live marker already spent this
/// generation's idle key. The Router skips consumed subjects before
/// taking a scheduler slot, so one burned key cannot starve an
/// eligible Goal past the concurrency cap.
#[derive(Clone, Debug)]
pub struct GoalReconcileCandidate {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub agent_state: AgentState,
    pub idle_seq: Option<i64>,
    pub key_consumed: bool,
}

/// A duplicate claim returns its original reply, without another check.
#[derive(Clone, Debug)]
pub enum GoalRoundClaim {
    Claimed { seq: i64, continuation_id: String },
    Replay(Box<GoalRoundResponse>),
}

/// Private reservation and final reply in the existing writes.result column.
/// Its captured marker binding survives Goal revisions and daemon restart.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct GoalRoundRecord {
    request: GoalRoundRequest,
    generation: i64,
    revision: i64,
    key: GoalRoundKey,
    holder: String,
    claim_seq: i64,
    continuation_id: String,
    outcome: StoredGoalRoundOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum StoredGoalRoundOutcome {
    Running,
    Finished(Box<GoalRoundResponse>),
    Failed(Error),
}

#[allow(
    clippy::too_many_arguments,
    reason = "The Goal round event records explicit outcome, timing, and causal evidence"
)]
fn insert_goal_round_event(
    tx: &Transaction<'_>,
    record: &GoalRoundRecord,
    outcome: GoalRoundEventOutcome,
    reason: Option<String>,
    exit_code: Option<i32>,
    check_timeout_ms: Option<i64>,
    retry_timeout_ms: Option<i64>,
    cause: Option<core::EventCause>,
    now: i64,
) -> Result<Event> {
    let data = serde_json::to_value(GoalRoundEvent {
        id: record.request.id.clone(),
        mode: record.request.mode,
        revision: record.revision,
        round_key: record.key.text(),
        outcome,
        reason,
        exit_code,
        check_timeout_ms,
        retry_timeout_ms,
    })
    .map_err(|_| Error::new("internal", "could not encode Goal round event"))?;
    let event = DaemonEventInput::new(
        now,
        "goal.round",
        "goal",
        Some(parse_agent_id(&record.request.agent)?),
        Some(record.generation),
        data,
    );
    let event = match cause {
        Some(cause) => event.with_cause(cause),
        None => event,
    };
    insert_daemon_event_input(tx, event)
}

/// Retention uses the same serialized variant as the live reservation DTO.
pub(super) fn goal_round_running_outcome_json() -> Result<String> {
    encode_json(&StoredGoalRoundOutcome::Running)
}

fn goal_round_digest(request: &GoalRoundRequest) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "agent": request.agent, "mode": request.mode.as_str(),
        "idle_seq": request.idle_seq, "slot": request.slot,
    }))
}

fn goal_request_key(request: &GoalRoundRequest) -> Result<GoalRoundKey> {
    request.validate()?;
    match (request.idle_seq, request.slot) {
        (Some(seq), None) => Ok(GoalRoundKey::Idle(seq)),
        (None, Some(slot)) => Ok(GoalRoundKey::Slot(slot)),
        _ => Err(Error::new("invalid", "goal round needs exactly one key")),
    }
}

fn update_goal_round_tx(tx: &Transaction<'_>, record: &GoalRoundRecord) -> Result<()> {
    let result = encode_json(record)?;
    if result.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "goal.round result exceeds the frame bound",
        ));
    }
    let changed = db!(tx.execute(
        "UPDATE writes SET result = ?1 WHERE id = ?2 AND method = 'goal.round'",
        params![result, record.request.id],
    ));
    if changed != 1 {
        return Err(Error::new(
            "unknown",
            "goal round reservation is no longer retained",
        ));
    }
    Ok(())
}

fn abandon_goal_marker_tx(
    tx: &Transaction<'_>,
    record: &GoalRoundRecord,
    custody_closed: bool,
    now: i64,
) -> Result<()> {
    let agent = parse_agent_id(&record.request.agent)?;
    let mode = record.request.mode.as_str();
    if custody_closed
        && load_goal_row(tx, agent, mode)?.is_some_and(|goal| {
            goal.generation == record.generation
                && goal
                    .state
                    .round_reset_revision
                    .is_some_and(|revision| revision >= record.revision)
        })
    {
        db!(tx.execute(
            "DELETE FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
               AND round_key = ?5 AND round_key_kind = ?6 AND holder = ?7",
            params![
                agent,
                mode,
                record.generation,
                record.revision,
                record.key.value(),
                record.key.kind(),
                record.holder
            ],
        ));
        return Ok(());
    }
    db!(tx.execute(
        "UPDATE goal_markers SET state = 'unknown', updated_at = ?1, custody_closed = ?2
         WHERE agent = ?3 AND mode = ?4 AND generation = ?5 AND revision = ?6
           AND round_key = ?7 AND round_key_kind = ?8 AND state = 'running' AND holder = ?9",
        params![
            now,
            custody_closed,
            agent,
            mode,
            record.generation,
            record.revision,
            record.key.value(),
            record.key.kind(),
            record.holder
        ],
    ));
    Ok(())
}

fn unknown_goal_round_error() -> Error {
    Error::new("unknown", "goal round ended without a retained result")
}

fn retain_goal_round_error_tx(
    tx: &Transaction<'_>,
    record: &mut GoalRoundRecord,
    error: &Error,
) -> Result<Error> {
    record.outcome = StoredGoalRoundOutcome::Failed(error.clone());
    if let Err(failure) = update_goal_round_tx(tx, record) {
        if failure.code != "frame_too_large" {
            return Err(failure);
        }
        // If the original error cannot fit, retain and return unknown.
        // Claim preflights this recovery record before any external check.
        let unknown = unknown_goal_round_error();
        record.outcome = StoredGoalRoundOutcome::Failed(unknown.clone());
        update_goal_round_tx(tx, record)?;
        return Ok(unknown);
    }
    Ok(error.clone())
}

// Nested Result lets callers commit recovery of an abandoned reservation
// before returning its stored error to the client.
fn replay_goal_round_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &GoalRoundRequest,
    holder: &str,
    now: i64,
) -> Result<Option<Result<GoalRoundResponse>>> {
    let digest = goal_round_digest(request)?;
    let Some(mut record) =
        replay_goal_write::<GoalRoundRecord>(tx, &request.id, principal, "goal.round", &digest)?
    else {
        validate_id_age(&request.id, now)?;
        return Ok(None);
    };
    match &record.outcome {
        StoredGoalRoundOutcome::Finished(response) => {
            request.validate_response(response)?;
            return Ok(Some(Ok(*response.clone())));
        }
        StoredGoalRoundOutcome::Failed(error) => return Ok(Some(Err(error.clone()))),
        StoredGoalRoundOutcome::Running => {}
    }
    let marker = load_goal_marker_tx(
        tx,
        parse_agent_id(&request.agent)?,
        request.mode.as_str(),
        record.generation,
        record.revision,
        record.key,
    )?;
    if record.holder == holder && marker.is_some_and(|marker| marker.is_live(holder)) {
        return Ok(Some(Err(Error::new(
            "busy",
            "goal round already in flight",
        ))));
    }
    abandon_goal_marker_tx(tx, &record, false, now)?;
    let error = retain_goal_round_error_tx(tx, &mut record, &unknown_goal_round_error())?;
    Ok(Some(Err(error)))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GoalStatePersist {
    /// Turns consumed before the current Agent baseline (task handoff).
    #[serde(default)]
    transferred_turns: i64,
    #[serde(default)]
    await_first_turn: bool,
    #[serde(default)]
    project_default: bool,
    status: GoalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paused_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocked_reason: Option<String>,
    /// Latest Goal revision explicitly cleared by the Owner, for this
    /// generation. A late custody proof may release markers through it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    round_reset_revision: Option<i64>,
    #[serde(default)]
    counters: GoalCounters,
    #[serde(default)]
    unverified_completion_rounds: i64,
    /// Slot-grid anchor captured when a cadence was set; stable across
    /// pause/clear/resume. Absent without a cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cadence_anchor_ms: Option<i64>,
    /// Last committed cadence slot. Slot keys are monotonic: any slot at
    /// or below it is refused at probe, claim and commit. Fresh goals
    /// start with none; idle rounds never touch it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_committed_slot: Option<i64>,
    /// Command id of the Goal continuation admitted by the most recent
    /// `Continued` round commit, until a later round consumes it. Private
    /// and never shown in a view: the probe resolves it to that command's
    /// completed turn so a budget spent by the Goal's own continuation
    /// does not skip the final check. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_continuation_command_id: Option<String>,
    /// The launch cause of a Goal installed by a Goal-owned folder
    /// assignment. Its terminal `goal.changed` carries this cause, so the
    /// assignment's completion rule retires exactly that Agent generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assignment_cause: Option<core::EventCause>,
    /// Starting point of a Goal with `scope` or `check_inputs`, recorded by
    /// the Router at `set` or launch preflight and kept until the next `set`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope_base: Option<core::api::GoalScopeBase>,
    /// Cost baseline of a Goal with `max_cost`, recorded at `set` or initialized
    /// when launch admission binds it to a new native session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cost_base: Option<GoalCostBase>,
    /// Set only when a confirmed Agent stop paused this Goal; the next resume of
    /// that Agent continues it. Any manual pause, set, clear or other pause clears it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    stop_paused: bool,
}

struct GoalRow {
    agent_num: i64,
    mode: String,
    generation: i64,
    revision: i64,
    definition: GoalDefinition,
    state: GoalStatePersist,
    last_round_key: Option<String>,
    updated_at: i64,
}

fn update_goal_state_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    mode: &str,
    revision: i64,
    state: &GoalStatePersist,
    now: i64,
) -> Result<()> {
    db!(tx.execute(
        "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
         WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
        params![
            revision + 1,
            encode_json(state)?,
            now,
            agent_num,
            mode,
            revision
        ],
    ));
    Ok(())
}

/// Pause an active Goal in the same transaction as restart settlement. Its
/// generation and any unresolved marker remain intact until a safe Resume.
pub(super) fn pause_for_restart_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(mut row) = load_goal_row(tx, agent_num, "eliot")? else {
        return Ok(());
    };
    if row.generation != generation || row.state.status != GoalStatus::Active {
        return Ok(());
    }
    row.state.status = GoalStatus::Paused;
    row.state.paused_reason = Some(reason.to_owned());
    update_goal_state_tx(tx, agent_num, &row.mode, row.revision, &row.state, now)?;
    events.push(insert_daemon_event(
        tx,
        now,
        "goal.changed",
        "goal",
        Some(agent_num),
        Some(generation),
        json!({"status": "paused", "reason": reason}),
    )?);
    Ok(())
}

/// Pause an active Goal after a confirmed Agent stop. The flag marks it for
/// automatic continuation on this Agent's next resume, which
/// `advance_resume_tx` applies behind its custody fence.
pub(super) fn pause_for_stop_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(mut row) = load_goal_row(tx, agent_num, "eliot")? else {
        return Ok(());
    };
    if row.generation != generation || row.state.status != GoalStatus::Active {
        return Ok(());
    }
    row.state.status = GoalStatus::Paused;
    row.state.paused_reason = Some("agent_stopped".to_owned());
    row.state.stop_paused = true;
    update_goal_state_tx(tx, agent_num, &row.mode, row.revision, &row.state, now)?;
    events.push(insert_daemon_event(
        tx,
        now,
        "goal.changed",
        "goal",
        Some(agent_num),
        Some(generation),
        json!({"status": "paused", "reason": "agent_stopped"}),
    )?);
    Ok(())
}

/// Restart reconciliation for Goals left active by a stop that predates
/// stop-pausing: each active Eliot Goal on a Stopped Agent of its own
/// generation takes the same stop pause. Manual pauses are untouched.
pub(super) fn pause_stopped_active_goals_tx(
    tx: &Transaction<'_>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let stopped: Vec<(i64, i64)> = {
        let mut statement = tx
            .prepare(
                "SELECT g.agent, g.generation FROM goals g JOIN agents a ON a.id = g.agent
                 WHERE g.mode = 'eliot' AND a.state = 'stopped'
                   AND a.generation = g.generation
                   AND json_extract(g.state, '$.status') = 'active'",
            )
            .map_err(sql_error)?;
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<(i64, i64)>>>()
            .map_err(sql_error)?
    };
    for (agent_num, generation) in stopped {
        pause_for_stop_tx(tx, agent_num, generation, now, events)?;
    }
    Ok(())
}

/// Resume dispatch rebinds the Goal. Native readiness or a later settlement
/// can continue it once the Agent and Goal have no unresolved work.
pub(super) fn continue_stop_paused_goal_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(mut row) = load_goal_row(tx, agent_num, "eliot")? else {
        return Ok(());
    };
    if row.state.status != GoalStatus::Paused || !row.state.stop_paused {
        return Ok(());
    }
    let ready = db!(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1 AND generation = ?2
                      AND state IN ('idle', 'busy', 'waiting'))",
        params![agent_num, row.generation],
        |record| record.get::<_, bool>(0)
    ));
    if !ready
        || super::has_unknown_gate(tx, agent_num)?
        || has_unresolved_goal_round(tx, agent_num, &row.mode)?
    {
        return Ok(());
    }
    row.state.status = GoalStatus::Active;
    row.state.paused_reason = None;
    row.state.stop_paused = false;
    update_goal_state_tx(tx, agent_num, &row.mode, row.revision, &row.state, now)?;
    events.push(insert_daemon_event(
        tx,
        now,
        "goal.changed",
        "goal",
        Some(agent_num),
        Some(row.generation),
        json!({"status": "active", "reason": "agent_resumed"}),
    )?);
    Ok(())
}

fn has_unresolved_goal_round(tx: &Transaction<'_>, agent_num: i64, mode: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM goal_markers
         WHERE agent = ?1 AND mode = ?2 AND state IN ('running', 'unknown'))",
        params![agent_num, mode],
        |record| record.get(0),
    )
    .map_err(sql_error)
}

const RESTART_GOAL_WORK_UNRESOLVED: &str = "prior Agent work is unresolved after restart";

/// Carry a Goal to a resumed generation without rearming an uncertain round
/// or unresolved Agent effect. A stop-paused Goal follows the generation for
/// accounting. Restart recovery also carries the Goal behind the same fence.
pub(super) fn advance_resume_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    old_generation: i64,
    next_generation: i64,
    reason: eliot_core::api::GenerationReason,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(mut row) = load_goal_row(tx, agent_num, "eliot")? else {
        return Ok(());
    };
    if row.generation != old_generation
        || !matches!(row.state.status, GoalStatus::Active | GoalStatus::Paused)
    {
        return Ok(());
    }
    let unresolved_work = has_unresolved_goal_round(tx, agent_num, &row.mode)?
        || super::has_unknown_gate(tx, agent_num)?;
    let restart = reason == eliot_core::api::GenerationReason::RestartResume;
    if unresolved_work && !row.state.stop_paused && !restart {
        if row.state.status == GoalStatus::Active {
            row.state.status = GoalStatus::Paused;
            row.state.paused_reason = Some("prior Agent work is unresolved".to_owned());
            update_goal_state_tx(tx, agent_num, &row.mode, row.revision, &row.state, now)?;
            events.push(insert_daemon_event(
                tx,
                now,
                "goal.changed",
                "goal",
                Some(agent_num),
                Some(old_generation),
                json!({"status": "paused", "reason": "prior_work_unresolved"}),
            )?);
        }
        return Ok(());
    }
    if unresolved_work {
        row.state.status = GoalStatus::Paused;
        row.state.paused_reason = Some(
            if restart {
                RESTART_GOAL_WORK_UNRESOLVED
            } else {
                "prior Agent work is unresolved"
            }
            .to_owned(),
        );
    }

    let old_turn_count = count_turns(tx, agent_num, old_generation)?;
    row.state.counters.turns_used = row.state.counters.turns_used.max(
        row.state
            .transferred_turns
            .saturating_add(old_turn_count.saturating_sub(row.state.counters.turns_at_set)),
    );
    row.state.transferred_turns = row.state.counters.turns_used;
    row.state.counters.turns_at_set = count_turns(tx, agent_num, next_generation)?;
    row.state.last_continuation_command_id = None;
    row.state.round_reset_revision = None;
    let revision = row
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "Goal revision overflow"))?;
    db!(tx.execute(
        "UPDATE goals SET generation = ?1, revision = ?2, state = ?3,
             last_round_key = NULL, updated_at = ?4
         WHERE agent = ?5 AND mode = ?6 AND generation = ?7 AND revision = ?8",
        params![
            next_generation,
            revision,
            encode_json(&row.state)?,
            now,
            agent_num,
            row.mode,
            old_generation,
            row.revision
        ],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "goal.changed",
        "goal",
        Some(agent_num),
        Some(next_generation),
        json!({
            "status": row.state.status.as_str(),
            "reason": "agent_resumed",
            "from_generation": old_generation
        }),
    )?);
    Ok(())
}

pub(super) fn eliot_goal_status(conn: &Connection, agent_num: i64) -> Result<Option<GoalStatus>> {
    Ok(load_goal_row(conn, agent_num, "eliot")?.map(|row| row.state.status))
}

pub(super) fn require_handoff_source_goal(
    tx: &Transaction<'_>,
    source: &Agent,
    policy: &core::SecretPolicy,
) -> Result<()> {
    require_handoff_round(tx, source, true)?;
    transferable_source_goal(tx, source, policy)?;
    Ok(())
}

/// An ended session cannot finish a Goal round. Its unknown round stays
/// burned and is reported; only a live round blocks the transfer.
fn require_handoff_round(tx: &Transaction<'_>, source: &Agent, continuation: bool) -> Result<()> {
    let agent = parse_agent_id(&source.id)?;
    if !(continuation && source.is_terminal()) {
        return require_settled_handoff_round(tx, agent);
    }
    let running: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM goal_markers WHERE agent=?1 AND state='running')",
            [agent],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if running {
        return Err(Error::new(
            "busy",
            format!("Agent {} has a Goal round still running", source.id),
        ));
    }
    Ok(())
}

fn require_settled_handoff_round(tx: &Transaction<'_>, agent: i64) -> Result<()> {
    let unsettled: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM goal_markers WHERE agent=?1 AND state IN ('running','unknown'))",
        [agent], |row| row.get(0),
    ).map_err(sql_error)?;
    if unsettled {
        return Err(Error::new(
            "busy",
            format!(
                "Agent {} has an unresolved Goal round",
                super::format_agent_id(agent)
            ),
        ));
    }
    Ok(())
}

fn transferable_source_goal(
    tx: &Transaction<'_>,
    from: &Agent,
    policy: &core::SecretPolicy,
) -> Result<Option<GoalRow>> {
    let from_num = parse_agent_id(&from.id)?;
    let Some(source) = load_goal_row(tx, from_num, "eliot")? else {
        return Ok(None);
    };
    if matches!(
        source.state.status,
        GoalStatus::Cleared | GoalStatus::Completed
    ) {
        return Ok(None);
    }
    if source.state.status == GoalStatus::Blocked {
        return Err(Error::new(
            "bad_state",
            "blocked source Goal requires an explicit set or clear before handoff",
        ));
    }
    if source.generation != from.generation {
        return Err(Error::new(
            "stale_generation",
            "source Goal belongs to another generation",
        ));
    }
    if let Some(id) = source.state.last_continuation_command_id.as_deref()
        && load_command(tx, id)?.is_none_or(|command| {
            matches!(
                command.state,
                CommandState::Accepted
                    | CommandState::Queued
                    | CommandState::Dispatched
                    | CommandState::Unknown
            )
        })
    {
        return Err(Error::new("busy", "source Goal continuation is unresolved"));
    }
    if policy.contains_value(
        &serde_json::to_value(&source.definition)
            .map_err(|_| Error::new("internal", "cannot encode Goal"))?,
    ) {
        return Err(Error::new(
            "secret_in_payload",
            "Goal contains a known secret",
        ));
    }
    Ok(Some(source))
}

/// Keep definition/counters, clear A, and install B paused with a fresh turn baseline.
pub(super) fn transfer_goal_tx(
    tx: &Transaction<'_>,
    from: &Agent,
    to: &Agent,
    policy: &core::SecretPolicy,
    continuation: bool,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<i64>> {
    let from_num = parse_agent_id(&from.id)?;
    let to_num = parse_agent_id(&to.id)?;
    require_handoff_round(tx, from, continuation)?;
    require_settled_handoff_round(tx, to_num)?;
    let Some(mut source) = transferable_source_goal(tx, from, policy)? else {
        return Ok(None);
    };
    let destination = load_goal_row(tx, to_num, "eliot")?;
    if let Some(row) = &destination
        && row.state.status != GoalStatus::Cleared
        && !(row.generation == to.generation
            && row.revision == 1
            && row.state.status == GoalStatus::Active
            && row.state.project_default
            && row.state.await_first_turn
            && row.state.counters.rounds == 0
            && count_turns(tx, to_num, to.generation)? == 0)
    {
        return Err(Error::new(
            "bad_state",
            "destination already has an Eliot Goal",
        ));
    }
    source.state.counters.turns_used = source.state.counters.turns_used.max(
        source.state.transferred_turns.saturating_add(
            count_turns(tx, from_num, from.generation)?
                .saturating_sub(source.state.counters.turns_at_set),
        ),
    );
    let total_used = source.state.counters.turns_used;
    let mut cleared = source.state.clone();
    cleared.status = GoalStatus::Cleared;
    cleared.paused_reason = None;
    cleared.stop_paused = false;
    cleared.last_continuation_command_id = None;
    tx.execute("UPDATE goals SET revision=?1,state=?2,last_round_key=NULL,updated_at=?3 WHERE agent=?4 AND mode='eliot'",
        params![source.revision.checked_add(1).ok_or_else(||Error::new("bad_state","Goal revision overflow"))?,encode_json(&cleared)?,now,from_num]).map_err(sql_error)?;
    let revision = destination
        .map_or(Some(1), |row| row.revision.checked_add(1))
        .ok_or_else(|| Error::new("bad_state", "Goal revision overflow"))?;
    // A continuing successor keeps the Goal running: its first round waits
    // for the brief's turn. Any other transfer needs an explicit resume.
    let keep_active = continuation && source.state.status == GoalStatus::Active;
    source.state.status = if keep_active {
        GoalStatus::Active
    } else {
        GoalStatus::Paused
    };
    source.state.await_first_turn = keep_active;
    source.state.stop_paused = false;
    // The assignment launch cause names the source Agent's launch: the
    // destination was not launched by the assignment, so its Goal is not
    // assignment-owned and never claims that Agent's retirement.
    source.state.assignment_cause = None;
    // A cost baseline belongs to the source Agent's native session: the
    // destination's spend cannot be measured against it, so its next failed
    // round reports `cost_unverified` until the Owner sets the Goal again.
    if source.state.cost_base.take().is_some() {
        source.state.counters.cost_spent_micros = None;
        source.state.counters.cost_unverified =
            Some("the Goal moved by handoff; set it again to budget this session".to_owned());
    }
    source.state.counters.last_observation = source
        .state
        .counters
        .last_observation
        .as_deref()
        .map(|text| policy.redact_text(text));
    source.state.paused_reason = (!keep_active).then(|| "handoff_requires_explicit_resume".into());
    source.state.transferred_turns = total_used;
    source.state.counters.turns_at_set = count_turns(tx, to_num, to.generation)?;
    source.state.last_committed_slot = None;
    source.state.last_continuation_command_id = None;
    source.state.round_reset_revision = None;
    source.state.cadence_anchor_ms = source.definition.cadence_ms.map(|_| now);
    tx.execute("INSERT INTO goals(agent,mode,generation,revision,definition,state,last_round_key,updated_at)
        VALUES (?1,'eliot',?2,?3,?4,?5,NULL,?6) ON CONFLICT(agent,mode) DO UPDATE SET
        generation=excluded.generation,revision=excluded.revision,definition=excluded.definition,state=excluded.state,last_round_key=NULL,updated_at=excluded.updated_at",
        params![to_num,to.generation,revision,encode_json(&source.definition)?,encode_json(&source.state)?,now]).map_err(sql_error)?;
    for (num, generation, status) in [
        (from_num, from.generation, "cleared"),
        (
            to_num,
            to.generation,
            if keep_active { "active" } else { "paused" },
        ),
    ] {
        events.push(insert_daemon_event(
            tx,
            now,
            "goal.changed",
            "goal",
            Some(num),
            Some(generation),
            json!({"status":status,"handoff_from":from.id,"handoff_to":to.id}),
        )?);
    }
    Ok(Some(revision))
}

impl Store {
    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    pub fn goal_set(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        definition: &GoalDefinition,
        scope_base: Option<core::api::GoalScopeBase>,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        definition.validate()?;
        if definition.tracks_changes() != scope_base.is_some() {
            return Err(Error::new(
                "internal",
                "a Goal tracks changes exactly when its starting point is recorded",
            ));
        }
        let result = self.goal_set_inner(agent, mode, id, principal, definition, scope_base, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// The retained result of an earlier `goal.set` with this id, so a retry
    /// is answered before the Router reads the workspace again.
    pub fn goal_set_replay(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        definition: &GoalDefinition,
    ) -> Result<Option<GoalView>> {
        let payload_digest = goal_set_digest(agent, mode, definition)?;
        let tx = self.connection.transaction().map_err(sql_error)?;
        replay_goal_write(&tx, id, principal, "goal.set", &payload_digest)
    }

    pub fn goal_get(&self, agent: &str, mode: &str) -> Result<Option<GoalView>> {
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        load_goal_row(&self.connection, agent_num, mode)?
            .map(|row| self.goal_view(row))
            .transpose()
    }

    /// Current-generation active Eliot Goals, bounded for deadline status.
    pub fn goal_check_deadline_targets(
        &self,
        limit: usize,
    ) -> Result<Vec<GoalCheckDeadlineTarget>> {
        self.ensure_committed_reads()?;
        validate_limit(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT a.id, a.project, g.definition
                 FROM goals g JOIN agents a ON a.id = g.agent AND a.generation = g.generation
                 WHERE g.mode = 'eliot' AND json_extract(g.state, '$.status') = 'active'
                 ORDER BY a.project, a.id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut targets = Vec::new();
        for row in rows {
            let (agent, project_id, encoded) = row.map_err(sql_error)?;
            let definition: GoalDefinition = serde_json::from_str(&encoded).map_err(|error| {
                Error::new(
                    "source_integrity",
                    format!("stored Goal is invalid: {error}"),
                )
            })?;
            targets.push(GoalCheckDeadlineTarget {
                project_id,
                agent: format_agent_id(agent),
                definition,
            });
        }
        Ok(targets)
    }

    /// Newest successful durations for one exact project/argv identity.
    pub fn goal_check_history(&self, project_id: &str, check_digest: &str) -> Result<Vec<i64>> {
        self.ensure_committed_reads()?;
        if project_id.is_empty()
            || check_digest.len() != 64
            || !check_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::new(
                "invalid",
                "goal check history identity is invalid",
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT duration_ms FROM goal_check_history
                 WHERE project_id = ?1 AND check_digest = ?2
                 ORDER BY id DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    check_digest,
                    core::api::GOAL_CHECK_HISTORY_LIMIT as i64,
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        rows.collect::<std::result::Result<Vec<i64>, _>>()
            .map_err(sql_error)
    }

    /// Read the Goal attached to an ordinary launch before native dispatch.
    pub fn launch_goal_scope_target(&self, agent: &str, launch: &str) -> Result<Option<GoalView>> {
        self.ensure_committed_reads()?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&self.connection, agent_num, GoalMode::Eliot.as_str())?
        else {
            return Ok(None);
        };
        if row.generation != 1
            || row
                .state
                .assignment_cause
                .as_ref()
                .is_none_or(|cause| cause.command_id != launch)
            || row.state.scope_base.is_some()
            || !row.definition.tracks_changes()
        {
            return Ok(None);
        }
        self.goal_view(row).map(Some)
    }

    /// The active generation's launch-installed definition, before any manual replacement.
    pub fn launch_goal_definition(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<GoalDefinition>> {
        self.ensure_committed_reads()?;
        let agent_num = parse_agent_id(agent)?;
        Ok(
            load_goal_row(&self.connection, agent_num, GoalMode::Eliot.as_str())?
                .filter(|row| row.generation == generation && row.state.assignment_cause.is_some())
                .map(|row| row.definition),
        )
    }

    /// Commit the checkout starting point only while the same launch is undispatched.
    pub fn set_launch_goal_scope(
        &mut self,
        launch: &str,
        agent: &str,
        workspace: &str,
        revision: i64,
        base: core::api::GoalScopeBase,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result =
            self.set_launch_goal_scope_inner(launch, agent, workspace, revision, base, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_pause(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        reason: Option<&str>,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_pause_inner(agent, mode, id, principal, reason, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_clear(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_clear_inner(agent, mode, id, principal, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_resume(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_resume_inner(agent, mode, id, principal, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Whether an active native-mode Goal owns the Agent. The automatic
    /// Eliot driver waits while one drives. Native `set` is unsupported,
    /// so no such row exists today; this stays the wait point.
    pub fn goal_native_driving(&self, agent: &str) -> Result<bool> {
        let agent_num = parse_agent_id(agent)?;
        Ok(load_goal_row(&self.connection, agent_num, "native")?
            .is_some_and(|row| row.state.status == GoalStatus::Active))
    }

    /// Release expired background-work holds on active Goals. The idle
    /// driver reoffers the retained idle subject when any hold expires.
    pub fn expire_goal_native_followups(&mut self, now: i64) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE turns SET native_followup_pending = 0
             WHERE native_followup_pending = 1 AND completed_at <= ?1
               AND rowid IN (
                   SELECT (
                       SELECT t.rowid FROM turns t INDEXED BY turns_agent_result_order
                       WHERE t.agent = a.id AND t.generation = a.generation
                       ORDER BY t.insertion_seq DESC LIMIT 1
                   )
                   FROM goals g JOIN agents a ON a.id = g.agent AND a.generation = g.generation
                   WHERE g.mode = 'eliot' AND json_extract(g.state, '$.status') = 'active'
               )",
                [now.saturating_sub(NATIVE_FOLLOWUP_WINDOW_MS)],
            )
            .map(|changed| changed > 0)
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Active non-cadenced Eliot Goals bound to their Agent's current
    /// generation. The Router applies the same enablement checks as the
    /// event path before reserving a round on the retained idle subject.
    pub fn goal_reconcile_candidates(&self) -> Result<Vec<GoalReconcileCandidate>> {
        let agents: Vec<i64> = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT g.agent
                     FROM goals g JOIN agents a ON a.id = g.agent
                     WHERE g.mode = 'eliot'
                       AND json_extract(g.state, '$.status') = 'active'
                       AND g.generation = a.generation
                       AND json_extract(g.definition, '$.cadence_ms') IS NULL
                       AND a.observe_only = 0
                     ORDER BY g.agent",
                )
                .map_err(sql_error)?;
            statement
                .query_map([], |row| row.get(0))
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<i64>>>()
                .map_err(sql_error)?
        };
        let mut candidates = Vec::with_capacity(agents.len());
        for agent_num in agents {
            let goal = load_goal_row(&self.connection, agent_num, "eliot")?
                .ok_or_else(|| Error::new("internal", "reconcile goal disappeared"))?;
            let generation = goal.generation;
            let agent_row = load_agent_by_num(&self.connection, agent_num)?
                .ok_or_else(|| Error::new("internal", "reconcile goal lost its Agent"))?;
            let (idle_seq, key_consumed) =
                match latest_idle_key(&self.connection, &goal, &self.goal_holder)? {
                    Some((seq, consumed)) => (Some(seq), consumed),
                    None => (None, false),
                };
            candidates.push(GoalReconcileCandidate {
                agent: format_agent_id(agent_num),
                generation,
                revision: goal.revision,
                agent_state: agent_row.state,
                idle_seq,
                key_consumed,
            });
        }
        Ok(candidates)
    }

    /// The latest unspent idle key of an active on-idle Goal bound to this
    /// exact Agent generation. Admission stays with the round's own gates.
    pub fn goal_latest_idle_key(&self, agent: &str, generation: i64) -> Result<Option<i64>> {
        self.ensure_committed_reads()?;
        let agent_num = parse_agent_id(agent)?;
        let Some(goal) = load_goal_row(&self.connection, agent_num, "eliot")? else {
            return Ok(None);
        };
        let bound = load_agent_by_num(&self.connection, agent_num)?
            .is_some_and(|row| row.generation == generation);
        if !bound
            || goal.generation != generation
            || goal.state.status != GoalStatus::Active
            || goal.definition.cadence_ms.is_some()
        {
            return Ok(None);
        }
        Ok(latest_idle_key(&self.connection, &goal, &self.goal_holder)?
            .and_then(|(seq, consumed)| (!consumed).then_some(seq)))
    }

    /// Whether this committed event belongs to a Swarm note's lifecycle.
    pub fn goal_event_is_swarm_note(&self, seq: i64) -> Result<bool> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE seq = ?1 AND {SWARM_NOTE_EVENT_SQL})"
                ),
                [seq],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Read round facts, pausing a generation-drifted Goal. Fails before any
    /// check runs when the Goal is missing, terminal, paused or drifted, when
    /// another round is in flight, or when the key is already consumed. Gate
    /// facts (idle state, turns, commands, sources) are only reported here;
    /// the Router evaluates them and `goal_claim_round` re-validates them
    /// atomically before anything runs.
    pub fn goal_round_probe(
        &mut self,
        agent: &str,
        mode: &str,
        key: GoalRoundKey,
        now: i64,
    ) -> Result<GoalRoundProbe> {
        self.ensure_writer_healthy()?;
        require_eliot_mode(mode)?;
        require_key_range(key)?;
        let agent_num = parse_agent_id(agent)?;
        let agent_row = load_agent_by_num(&self.connection, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        super::provider_holds::require_model_unheld(&self.connection, &agent_row)?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goal rounds",
            ));
        }
        let Some(row) = load_goal_row(&self.connection, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.generation != agent_row.generation {
            if row.state.status == GoalStatus::Active {
                self.pause_goal_tx(agent_num, &row, "Agent generation advanced", now)?;
            }
            return Err(Error::new(
                "stale_generation",
                format!(
                    "goal binds generation {} but Agent {} is at generation {}",
                    row.generation, agent, agent_row.generation
                ),
            ));
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.last_round_key.as_deref() == Some(&key.text()) {
            return Err(Error::new(
                "stale_round",
                "goal round was already committed",
            ));
        }
        let markers = load_goal_markers(&self.connection, agent_num, mode)?;
        if markers
            .iter()
            .any(|marker| marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("busy", "goal round already in flight"));
        }
        // A retained marker burns its key across revisions and separates key
        // forms. An explicit clear can remove it only after proven closure.
        if markers.iter().any(|marker| {
            marker.round_key == key.value()
                && marker.kind == key.kind()
                && marker.generation == row.generation
                && !marker.is_live(&self.goal_holder)
        }) {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
        require_key_form(&row, key)?;
        match key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_idle_event(&self.connection, &row, idle_seq)? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
            }
        }
        let turn_count = count_turns(&self.connection, agent_num, agent_row.generation)?;
        if row.state.await_first_turn && turn_count <= row.state.counters.turns_at_set {
            return Err(Error::new(
                "bad_state",
                "the default project Goal awaits the Agent's first turn",
            ));
        }
        let spent_by_goal_continuation = goal_continuation_spent_budget(
            &self.connection,
            agent_num,
            row.generation,
            row.state.counters.turns_at_set,
            row.state.counters.turns_used,
            row.state.last_continuation_command_id.as_deref(),
            turn_count.saturating_add(row.state.transferred_turns),
        )?;
        let scope_base = row.state.scope_base.clone();
        let project_default = row.state.project_default;
        let unsuccessful_turn = self.goal_last_task_turn_unsuccessful(agent, &row)?;
        let max_seq = max_agent_seq(&self.connection, &row)?;
        let view = self.goal_view(row)?;
        let probe = GoalRoundProbe {
            scope_base,
            project_default,
            project_id: agent_row.project.clone(),
            idle_refusal: idle_round_refusal(
                &self.connection,
                agent_num,
                &agent_row,
                agent_row.generation,
                now,
            )?,
            max_seq,
            turn_count,
            spent_by_goal_continuation,
            workspace: agent_row.workspace.clone(),
            task_base: agent_row.base_oid.clone(),
            unsuccessful_turn,
            view,
        };
        Ok(probe)
    }

    /// Resume can open a new generation before another task turn runs.
    /// Until then, the last turn on this Agent is still the completion evidence.
    fn goal_last_task_turn_unsuccessful(&self, agent: &str, goal: &GoalRow) -> Result<bool> {
        let latest: Option<(i64, String)> = self
            .connection
            .query_row(
                "SELECT generation, turn_id FROM turns
                 WHERE agent = ?1 AND generation <= ?2
                   AND (native_trigger IS NULL OR start_command_id IS NOT NULL)
                 ORDER BY generation DESC, COALESCE(started_at, completed_at, 0) DESC, turn_id DESC
                 LIMIT 1",
                params![goal.agent_num, goal.generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((generation, turn_id)) = latest else {
            return Ok(false);
        };
        let turn = self
            .get_turn(agent, generation, &turn_id)?
            .ok_or_else(|| Error::new("source_integrity", "Goal's latest task turn is missing"))?;
        let outcome = turn.outcome.unwrap_or(core::TurnOutcome::Unknown);
        let reported = turn
            .resolution_source
            .map_or(outcome, |source| source.reported_outcome(outcome));
        Ok(reported != core::TurnOutcome::Success)
    }

    /// Commit the durable in-flight marker for one round, re-validating the
    /// whole probe (Goal binding, single flight, idle gates, round key, and
    /// the spent-budget qualification) in the same Immediate transaction.
    /// `expected_spent_by_goal_continuation` is the probe's qualification;
    /// if the live one differs the claim is refused as stale without
    /// inserting a marker. A second claim while a marker is running is
    /// refused without running anything. Every claim resolves exactly once:
    /// the final commit consumes its marker, anything else (unknown check,
    /// interference, crash) leaves it `unknown` and burns the key.
    pub fn goal_claim_round(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        expected_revision: i64,
        expected_spent_by_goal_continuation: bool,
        now: i64,
    ) -> Result<GoalRoundClaim> {
        self.ensure_writer_healthy()?;
        let result = self.goal_claim_round_inner(
            principal,
            request,
            expected_revision,
            expected_spent_by_goal_continuation,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    /// Resolve retained requests before probing any newer Goal state.
    pub fn goal_round_replay(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        now: i64,
    ) -> Result<Option<GoalRoundResponse>> {
        self.ensure_writer_healthy()?;
        require_nonempty(principal, "principal")?;
        request.validate()?;
        require_eliot_mode(request.mode.as_str())?;
        validate_ulid(&request.id)?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let replay = replay_goal_round_tx(&tx, principal, request, &self.goal_holder, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(replay)
        })();
        self.fault_on_persistence_error(&result);
        result?.transpose()
    }

    /// Burn the marker and retain the original sanitized error atomically.
    /// If persistence itself fails, the reservation stays unresolved; it
    /// never authorizes a rerun, and a new holder reads it as unknown.
    pub fn goal_abandon_round(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        check_timeout_ms: Option<i64>,
        retry_timeout_ms: Option<i64>,
        now: i64,
    ) -> Result<Change<Error>> {
        self.goal_finish_round_error(
            principal,
            request,
            error,
            check_timeout_ms,
            retry_timeout_ms,
            now,
            true,
        )
    }

    /// Retain the request error without releasing process single-flight.
    /// A failed request and an unproven process-family closure are independent.
    pub fn goal_hold_round_error(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        check_timeout_ms: Option<i64>,
        retry_timeout_ms: Option<i64>,
        now: i64,
    ) -> Result<Change<Error>> {
        self.goal_finish_round_error(
            principal,
            request,
            error,
            check_timeout_ms,
            retry_timeout_ms,
            now,
            false,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "The failure settlement carries the exact request, observed deadlines, and ownership state"
    )]
    fn goal_finish_round_error(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        check_timeout_ms: Option<i64>,
        retry_timeout_ms: Option<i64>,
        now: i64,
        release_marker: bool,
    ) -> Result<Change<Error>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut record = replay_goal_write::<GoalRoundRecord>(
                &tx,
                &request.id,
                principal,
                "goal.round",
                &goal_round_digest(request)?,
            )?
            .ok_or_else(|| Error::new("unknown", "goal round reservation is no longer retained"))?;
            if release_marker && record.holder == self.goal_holder {
                // A later proven closure may release the marker without
                // replacing an already retained original request error.
                abandon_goal_marker_tx(&tx, &record, true, now)?;
            }
            let mut events = Vec::new();
            let retained = if matches!(record.outcome, StoredGoalRoundOutcome::Running)
                && record.holder == self.goal_holder
            {
                let retained = retain_goal_round_error_tx(&tx, &mut record, error)?;
                events.push(insert_goal_round_event(
                    &tx,
                    &record,
                    GoalRoundEventOutcome::Unknown,
                    Some(retained.code.clone()),
                    None,
                    check_timeout_ms,
                    retry_timeout_ms,
                    None,
                    now,
                )?);
                retained
            } else if let StoredGoalRoundOutcome::Failed(retained) = record.outcome {
                retained
            } else {
                error.clone()
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: retained,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Apply one round outcome only if generation, revision and round still
    /// match. An optional `send` continuation is admitted in the same
    /// transaction; any mismatch rolls everything back.
    pub fn goal_commit_round(
        &mut self,
        commit: &GoalRoundCommit,
        now: i64,
    ) -> Result<GoalRoundCommitResult> {
        self.ensure_writer_healthy()?;
        let result = self.goal_commit_round_inner(commit, now);
        self.fault_on_persistence_error(&result);
        result
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    fn goal_set_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        definition: &GoalDefinition,
        scope_base: Option<core::api::GoalScopeBase>,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        definition.require_auto_bound()?;
        let payload_digest = goal_set_digest(agent, mode, definition)?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.set", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goals",
            ));
        }
        // The one escalation must be a configure this Agent's harness can
        // apply; refuse it now rather than at the round that needs it.
        if let Some(escalation) = &definition.escalation {
            admission::resolve_configure(
                &escalation.configure_args(),
                &agent_row.harness,
                &agent_row.profile_snapshot,
            )?;
        }
        let encoded = encode_json(definition)?;
        if encoded.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal definition exceeds 64 KiB",
            ));
        }
        let stop_paused = agent_row.state == AgentState::Stopped;
        let status = if stop_paused {
            GoalStatus::Paused
        } else {
            GoalStatus::Active
        };
        let paused_reason = stop_paused.then(|| "agent_stopped".to_owned());
        let turn_count = count_turns(&tx, agent_num, agent_row.generation)?;
        if let Some(row) = load_goal_row(&tx, agent_num, mode)? {
            if row.generation == agent_row.generation
                && row.state.status == status
                && row.state.stop_paused == stop_paused
                && !row.state.project_default
                && encode_json(&row.definition)? == encoded
            {
                let view = goal_view_on(&tx, &goal_holder, row)?;
                retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(view);
            }
            let cost_base =
                goal_cost_base_tx(&tx, agent_num, agent_row.generation, definition, turn_count)?;
            let revision = row.revision + 1;
            let state = GoalStatePersist {
                transferred_turns: 0,
                unverified_completion_rounds: 0,
                await_first_turn: false,
                project_default: false,
                status,
                paused_reason,
                blocked_reason: None,
                round_reset_revision: row
                    .state
                    .round_reset_revision
                    .filter(|_| row.generation == agent_row.generation),
                counters: GoalCounters {
                    turns_at_set: turn_count,
                    cost_spent_micros: definition.max_cost.as_ref().map(|_| 0),
                    ..GoalCounters::default()
                },
                cadence_anchor_ms: definition.cadence_ms.map(|_| now),
                last_committed_slot: None,
                last_continuation_command_id: None,
                assignment_cause: row.state.assignment_cause.clone(),
                scope_base: scope_base.clone(),
                cost_base: cost_base.clone(),
                stop_paused,
            };
            db!(tx.execute(
                "UPDATE goals SET generation = ?1, revision = ?2, definition = ?3,
                     state = ?4, last_round_key = NULL, updated_at = ?5
                 WHERE agent = ?6 AND mode = ?7",
                params![
                    agent_row.generation,
                    revision,
                    encoded,
                    encode_json(&state)?,
                    now,
                    agent_num,
                    mode,
                ],
            ));
            let updated = load_goal_row(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
            let view = goal_view_on(&tx, &goal_holder, updated)?;
            retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        let cost_base =
            goal_cost_base_tx(&tx, agent_num, agent_row.generation, definition, turn_count)?;
        let state = GoalStatePersist {
            transferred_turns: 0,
            unverified_completion_rounds: 0,
            await_first_turn: false,
            project_default: false,
            status,
            paused_reason,
            blocked_reason: None,
            round_reset_revision: None,
            counters: GoalCounters {
                turns_at_set: turn_count,
                cost_spent_micros: definition.max_cost.as_ref().map(|_| 0),
                ..GoalCounters::default()
            },
            cadence_anchor_ms: definition.cadence_ms.map(|_| now),
            last_committed_slot: None,
            last_continuation_command_id: None,
            assignment_cause: None,
            scope_base: scope_base.clone(),
            cost_base: cost_base.clone(),
            stop_paused,
        };
        db!(tx.execute(
            "INSERT INTO goals(agent, mode, generation, revision, definition,
                 state, last_round_key, updated_at)
             VALUES (?1, ?2, ?3, 1, ?4, ?5, NULL, ?6)",
            params![
                agent_num,
                mode,
                agent_row.generation,
                encoded,
                encode_json(&state)?,
                now,
            ],
        ));
        let inserted = load_goal_row(&tx, agent_num, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
        let view = goal_view_on(&tx, &goal_holder, inserted)?;
        retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn set_launch_goal_scope_inner(
        &mut self,
        launch: &str,
        agent: &str,
        workspace: &str,
        revision: i64,
        base: core::api::GoalScopeBase,
        now: i64,
    ) -> Result<()> {
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_command(&tx, launch)?;
        let current_agent = load_agent_by_num(&tx, agent_num)?;
        if current.is_none_or(|command| {
            command.verb != "launch"
                || command.target.as_deref() != Some(agent)
                || command.generation != Some(0)
                || !matches!(command.state, CommandState::Accepted | CommandState::Queued)
        }) || current_agent.is_none_or(|current| {
            current.generation != 0 || current.workspace.as_deref() != Some(workspace)
        }) {
            return Err(Error::new(
                "stale_generation",
                "launch or workspace changed before the Goal starting point was committed",
            ));
        }
        let Some(mut row) = load_goal_row(&tx, agent_num, GoalMode::Eliot.as_str())? else {
            return commit_transaction(tx, &self.write_faulted);
        };
        if row.generation != 1
            || row
                .state
                .assignment_cause
                .as_ref()
                .is_none_or(|cause| cause.command_id != launch)
            || row.state.scope_base.is_some()
            || !row.definition.tracks_changes()
        {
            return commit_transaction(tx, &self.write_faulted);
        }
        if row.revision != revision {
            return Err(Error::new(
                "stale_goal",
                "launch Goal changed before its starting point was committed",
            ));
        }
        row.state.scope_base = Some(base);
        db!(tx.execute(
            "UPDATE goals SET state = ?1, updated_at = ?2 WHERE agent = ?3 AND mode = 'eliot'",
            params![encode_json(&row.state)?, now, agent_num],
        ));
        commit_transaction(tx, &self.write_faulted)
    }

    fn goal_pause_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        reason: Option<&str>,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        if let Some(reason) = reason
            && (reason.trim().is_empty() || reason.len() > MAX_GOAL_OBSERVATION_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "goal pause reason must be nonempty and at most 4 KiB",
            ));
        }
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
            "reason": reason,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.pause", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status != GoalStatus::Paused && row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and cannot pause", row.state.status.as_str()),
            ));
        }
        // A stop-paused Goal becomes a manual pause, so it stops auto-continuing.
        let view = if row.state.status == GoalStatus::Paused && !row.state.stop_paused {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            let mut state = row.state;
            state.status = GoalStatus::Paused;
            state.paused_reason = reason.map(ToOwned::to_owned);
            state.stop_paused = false;
            update_goal_state_tx(&tx, agent_num, mode, row.revision, &state, now)?;
            let updated = load_goal_row(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after pause"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.pause",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn goal_clear_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.clear", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        db!(tx.execute(
            "DELETE FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND custody_closed = 1",
            params![agent_num, mode, row.generation],
        ));
        let view = if row.state.status == GoalStatus::Cleared
            && row.state.round_reset_revision.is_some()
        {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            let state = GoalStatePersist {
                transferred_turns: row.state.transferred_turns,
                unverified_completion_rounds: 0,
                await_first_turn: row.state.await_first_turn,
                project_default: row.state.project_default,
                status: GoalStatus::Cleared,
                paused_reason: None,
                blocked_reason: None,
                round_reset_revision: Some(row.revision),
                counters: row.state.counters,
                cadence_anchor_ms: row.state.cadence_anchor_ms,
                last_committed_slot: row.state.last_committed_slot,
                last_continuation_command_id: None,
                assignment_cause: row.state.assignment_cause.clone(),
                scope_base: row.state.scope_base.clone(),
                cost_base: row.state.cost_base.clone(),
                stop_paused: false,
            };
            update_goal_state_tx(&tx, agent_num, mode, row.revision, &state, now)?;
            let updated = load_goal_row(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after clear"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.clear",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn goal_resume_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.resume", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        let view = if row.state.status == GoalStatus::Active {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            if row.state.status != GoalStatus::Paused {
                return Err(Error::new(
                    "bad_state",
                    format!(
                        "goal is {} and resumes only via set",
                        row.state.status.as_str()
                    ),
                ));
            }
            let agent_row = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
            if agent_row.observe_only {
                return Err(Error::new(
                    "not_allowed",
                    "observe-only Agents take no Goals",
                ));
            }
            if agent_row.state == AgentState::Stopped {
                return Err(Error::new(
                    "bad_state",
                    "Agent is stopped; resume the Agent, which continues its Goal",
                ));
            }
            if row.generation != agent_row.generation {
                return Err(Error::new(
                    "stale_generation",
                    "goal binds a past Agent generation; set a new goal",
                ));
            }
            if row.state.paused_reason.as_deref() == Some(RESTART_GOAL_WORK_UNRESOLVED)
                && (super::has_unknown_gate(&tx, agent_num)?
                    || has_unresolved_goal_round(&tx, agent_num, &row.mode)?)
            {
                return Err(Error::new(
                    "bad_state",
                    "settle prior Agent work before resuming this Goal",
                ));
            }
            let mut state = row.state;
            state.status = GoalStatus::Active;
            state.unverified_completion_rounds = 0;
            state.paused_reason = None;
            state.stop_paused = false;
            update_goal_state_tx(&tx, agent_num, mode, row.revision, &state, now)?;
            let updated = load_goal_row(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after resume"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.resume",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn pause_goal_tx(
        &mut self,
        agent_num: i64,
        row: &GoalRow,
        reason: &str,
        now: i64,
    ) -> Result<GoalView> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let state = GoalStatePersist {
            transferred_turns: row.state.transferred_turns,
            unverified_completion_rounds: row.state.unverified_completion_rounds,
            await_first_turn: row.state.await_first_turn,
            project_default: row.state.project_default,
            status: GoalStatus::Paused,
            paused_reason: Some(reason.to_owned()),
            blocked_reason: row.state.blocked_reason.clone(),
            round_reset_revision: row.state.round_reset_revision,
            counters: row.state.counters.clone(),
            cadence_anchor_ms: row.state.cadence_anchor_ms,
            last_committed_slot: row.state.last_committed_slot,
            last_continuation_command_id: row.state.last_continuation_command_id.clone(),
            assignment_cause: row.state.assignment_cause.clone(),
            scope_base: row.state.scope_base.clone(),
            cost_base: row.state.cost_base.clone(),
            stop_paused: false,
        };
        update_goal_state_tx(&tx, agent_num, &row.mode, row.revision, &state, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        let row = load_goal_row(&self.connection, agent_num, &row.mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after pause"))?;
        self.goal_view(row)
    }

    fn goal_claim_round_inner(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        expected_revision: i64,
        expected_spent: bool,
        now: i64,
    ) -> Result<GoalRoundClaim> {
        let agent = request.agent.as_str();
        let mode = request.mode.as_str();
        let key = goal_request_key(request)?;
        require_eliot_mode(mode)?;
        require_nonempty(principal, "principal")?;
        validate_ulid(&request.id)?;
        require_key_range(key)?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(replay) = replay_goal_round_tx(&tx, principal, request, &self.goal_holder, now)?
        {
            commit_transaction(tx, &self.write_faulted)?;
            return replay.map(|response| GoalRoundClaim::Replay(Box::new(response)));
        }
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        super::provider_holds::require_model_unheld(&tx, &agent_row)?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goal rounds",
            ));
        }
        project_transfer::check_automation_target(&tx, &agent_row)?;
        let Some(row) = load_goal_row(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.generation != agent_row.generation {
            if row.state.status == GoalStatus::Active {
                let state = GoalStatePersist {
                    transferred_turns: row.state.transferred_turns,
                    unverified_completion_rounds: row.state.unverified_completion_rounds,
                    await_first_turn: row.state.await_first_turn,
                    project_default: row.state.project_default,
                    status: GoalStatus::Paused,
                    paused_reason: Some("Agent generation advanced".to_owned()),
                    blocked_reason: row.state.blocked_reason.clone(),
                    round_reset_revision: row.state.round_reset_revision,
                    counters: row.state.counters.clone(),
                    cadence_anchor_ms: row.state.cadence_anchor_ms,
                    last_committed_slot: row.state.last_committed_slot,
                    last_continuation_command_id: row.state.last_continuation_command_id.clone(),
                    assignment_cause: row.state.assignment_cause.clone(),
                    scope_base: row.state.scope_base.clone(),
                    cost_base: row.state.cost_base.clone(),
                    stop_paused: false,
                };
                update_goal_state_tx(&tx, agent_num, mode, row.revision, &state, now)?;
                commit_transaction(tx, &self.write_faulted)?;
            }
            return Err(Error::new(
                "stale_generation",
                format!(
                    "goal binds generation {} but Agent {} is at generation {}",
                    row.generation, agent, agent_row.generation
                ),
            ));
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.revision != expected_revision {
            return Err(Error::new(
                "stale_round",
                "goal revision changed since the probe",
            ));
        }
        // A crashed holder's `running` rows read as `unknown`; rewrite them
        // durably so later readers need no holder comparison.
        db!(tx.execute(
            "UPDATE goal_markers SET state = 'unknown', updated_at = ?1
             WHERE agent = ?2 AND mode = ?3 AND state = 'running' AND holder <> ?4",
            params![now, agent_num, mode, self.goal_holder],
        ));
        let markers = load_goal_markers(&tx, agent_num, mode)?;
        if markers
            .iter()
            .any(|marker| marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("busy", "goal round already in flight"));
        }
        // A marker burns its key across revisions until an explicit clear
        // and a custody proof have both occurred.
        if row.last_round_key.as_deref() == Some(&key.text())
            || markers.iter().any(|marker| {
                marker.round_key == key.value()
                    && marker.kind == key.kind()
                    && marker.generation == row.generation
            })
        {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
        require_key_form(&row, key)?;
        if let Some(refusal) = idle_round_refusal(&tx, agent_num, &agent_row, row.generation, now)?
        {
            return Err(refusal);
        }
        // Re-validate the exact spent-budget binding in this transaction:
        // a probe whose qualification went stale before the claim (a turn
        // ran after the probe) must not act. Refusing inserts no marker,
        // so the round key stays usable with a fresh probe.
        let live_spent = goal_continuation_spent_budget(
            &tx,
            agent_num,
            row.generation,
            row.state.counters.turns_at_set,
            row.state.counters.turns_used,
            row.state.last_continuation_command_id.as_deref(),
            count_turns(&tx, agent_num, row.generation)?
                .saturating_add(row.state.transferred_turns),
        )?;
        if live_spent != expected_spent {
            return Err(Error::new(
                "stale_round",
                "goal turn budget state changed since the probe",
            ));
        }
        let claim_seq = max_agent_seq(&tx, &row)?;
        match key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_idle_event(&tx, &row, idle_seq)? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
                if claim_seq != idle_seq {
                    return Err(Error::new(
                        "stale_round",
                        "round key is not the latest Agent event",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
            }
        }
        db!(tx.execute(
            "INSERT INTO goal_markers(agent, mode, generation, revision, round_key,
                 round_key_kind, holder, state, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8)",
            params![
                agent_num,
                mode,
                row.generation,
                row.revision,
                key.value(),
                key.kind(),
                self.goal_holder,
                now,
            ],
        ));
        let continuation_id = Ulid::new().to_string();
        if command_binding(&tx, &continuation_id)?.is_some() || write_exists(&tx, &continuation_id)?
        {
            return Err(Error::new(
                "id_conflict",
                "goal continuation id is already used",
            ));
        }
        let record = GoalRoundRecord {
            request: request.clone(),
            generation: row.generation,
            revision: row.revision,
            key,
            holder: self.goal_holder.clone(),
            claim_seq,
            continuation_id: continuation_id.clone(),
            outcome: StoredGoalRoundOutcome::Running,
        };
        let mut recovery = record.clone();
        recovery.outcome = StoredGoalRoundOutcome::Failed(unknown_goal_round_error());
        if encode_json(&recovery)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal.round recovery exceeds the frame bound",
            ));
        }
        retain_goal_result(
            &tx,
            &request.id,
            principal,
            "goal.round",
            &goal_round_digest(request)?,
            &record,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(GoalRoundClaim::Claimed {
            seq: claim_seq,
            continuation_id,
        })
    }

    fn goal_commit_round_inner(
        &mut self,
        commit: &GoalRoundCommit,
        now: i64,
    ) -> Result<GoalRoundCommitResult> {
        let request = GoalRoundRequest {
            id: commit.id.clone(),
            agent: commit.agent.clone(),
            mode: GoalMode::parse(&commit.mode)?,
            idle_seq: match commit.round_key {
                GoalRoundKey::Idle(seq) => Some(seq),
                _ => None,
            },
            slot: match commit.round_key {
                GoalRoundKey::Slot(slot) => Some(slot),
                _ => None,
            },
        };
        require_eliot_mode(&commit.mode)?;
        let agent_num = parse_agent_id(&commit.agent)?;
        require_key_range(commit.round_key)?;
        if commit.claim_seq < 0 {
            return Err(Error::new(
                "invalid",
                "goal round claim seq must be nonnegative",
            ));
        }
        if let Some(continuation) = &commit.continuation {
            validate_ulid(&continuation.id)?;
            if let Some(id) = &continuation.escalation_id {
                validate_ulid(id)?;
            }
            require_nonempty(&continuation.text, "continuation text")?;
            check_input(&continuation.text)?;
        }
        if commit.check.is_none() && commit.continuation.is_some() {
            return Err(Error::new(
                "internal",
                "goal continuation requires a check result",
            ));
        }
        if commit.check_history.is_some()
            && commit
                .check
                .as_ref()
                .is_none_or(|check| check.exit_code != Some(0))
        {
            return Err(Error::new(
                "internal",
                "only a successful check may add Goal check history",
            ));
        }
        // A checked round decides its own block, except the Router's scope
        // finding on a passing check, which never carries a continuation.
        let blocked_reason = match &commit.decision {
            Some(GoalRoundDecision::Block(reason)) => Some(reason),
            _ => None,
        };
        let retry_check = matches!(commit.decision, Some(GoalRoundDecision::RetryCheck));
        let retry_completion = match commit.decision {
            Some(GoalRoundDecision::RetryCompletion(reason)) => Some(reason),
            _ => None,
        };
        if retry_completion.is_some()
            && (commit.continuation.is_some()
                || commit.check_history.is_some()
                || commit.project_check.is_some()
                || commit
                    .check
                    .as_ref()
                    .is_none_or(|check| check.exit_code != Some(0)))
        {
            return Err(Error::new(
                "internal",
                "deferred Goal completion requires a check pass without success effects",
            ));
        }
        if retry_check && commit.check.is_some() {
            return Err(Error::new(
                "internal",
                "a retryable check has no direct result",
            ));
        }
        if blocked_reason.is_some()
            && (commit.continuation.is_some()
                || commit
                    .check
                    .as_ref()
                    .is_some_and(|check| check.exit_code != Some(0)))
        {
            return Err(Error::new(
                "internal",
                "only a passing check may carry the Router's block, and no continuation",
            ));
        }
        if commit
            .check
            .as_ref()
            .is_some_and(|check| check.exit_code.is_none())
        {
            return Err(Error::new(
                "unknown",
                "goal check without an exit code has no committable outcome",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut record = replay_goal_write::<GoalRoundRecord>(
            &tx,
            &request.id,
            &commit.principal,
            "goal.round",
            &goal_round_digest(&request)?,
        )?
        .ok_or_else(|| Error::new("unknown", "goal round reservation is no longer retained"))?;
        if !matches!(record.outcome, StoredGoalRoundOutcome::Running)
            || record.holder != self.goal_holder
        {
            let replay =
                replay_goal_round_tx(&tx, &commit.principal, &request, &self.goal_holder, now)?
                    .ok_or_else(|| {
                        Error::new("unknown", "goal round reservation is no longer retained")
                    })?;
            commit_transaction(tx, &self.write_faulted)?;
            return replay.and_then(GoalRoundCommitResult::replay);
        }
        if record.generation != commit.expected_generation
            || record.revision != commit.expected_revision
            || record.claim_seq != commit.claim_seq
            || record.key != commit.round_key
            || commit
                .continuation
                .as_ref()
                .is_some_and(|c| c.id != record.continuation_id)
        {
            return Err(Error::new(
                "stale_round",
                "goal round commit does not match its reservation",
            ));
        }
        let Some(row) = load_goal_row(&tx, agent_num, &commit.mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.generation != commit.expected_generation {
            return Err(Error::new(
                "stale_generation",
                "goal generation no longer matches",
            ));
        }
        if row.revision != commit.expected_revision {
            return Err(Error::new(
                "stale_round",
                "goal revision changed during the round",
            ));
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.last_round_key.as_deref() == Some(&commit.round_key.text()) {
            return Err(Error::new(
                "stale_round",
                "goal round was already committed",
            ));
        }
        let marker = load_goal_marker_tx(
            &tx,
            agent_num,
            &commit.mode,
            commit.expected_generation,
            commit.expected_revision,
            commit.round_key,
        )?;
        if marker
            .as_ref()
            .is_none_or(|marker| !marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("stale_round", "goal round is not in flight"));
        }
        match commit.round_key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_idle_event(&tx, &row, idle_seq)? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
                if max_agent_seq(&tx, &row)? != idle_seq {
                    return Err(Error::new(
                        "stale_round",
                        "Agent events advanced past the round key",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
                if max_agent_seq(&tx, &row)? != commit.claim_seq {
                    return Err(Error::new(
                        "stale_round",
                        "Agent events advanced during the round",
                    ));
                }
            }
        }
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if let Some(history) = &commit.check_history
            && (history.duration_ms < 0
                || history.project_id != agent_row.project
                || history.check_digest
                    != core::api::goal_check_digest(
                        &row.definition.check,
                        row.definition.check_cwd.as_deref(),
                    )?)
        {
            return Err(Error::new(
                "stale_round",
                "Goal check history does not match the frozen project and argv",
            ));
        }
        if let Some(refusal) = idle_round_refusal(&tx, agent_num, &agent_row, row.generation, now)?
        {
            return Err(Error::new("stale_round", *refusal.message));
        }
        let turn_count = count_turns(&tx, agent_num, row.generation)?;
        let mut counters = row.state.counters.clone();
        counters.last_retry_reason = retry_completion.map(str::to_owned);
        let unverified_completion_rounds =
            if retry_completion == Some(core::api::UNSUCCESSFUL_TURN_WORK_UNVERIFIED) {
                row.state.unverified_completion_rounds.saturating_add(1)
            } else {
                0
            };
        counters.turns_used = counters.turns_used.max(
            row.state
                .transferred_turns
                .saturating_add(turn_count.saturating_sub(counters.turns_at_set)),
        );
        let mut admission: Option<Admission> = None;
        let mut escalation_admission: Option<Admission> = None;
        let mut learned_cost_source = None;
        let outcome = if let Some(check) = &commit.check {
            let (same_failure, stage) = row.state.counters.failure_stage(
                &row.definition,
                check.output_complete,
                check.output_digest.as_deref(),
            );
            counters.consecutive_same_failure = same_failure;
            counters.rounds += 1;
            counters.last_exit_code = check.exit_code;
            counters.last_check_at = Some(now);
            counters.last_observation = check.observation.clone();
            if check.output_complete
                && let Some(digest) = &check.output_digest
            {
                let same_output = counters.last_output_digest.as_deref() == Some(digest.as_str());
                match goal_fingerprint_tx(
                    &tx,
                    agent_num,
                    row.generation,
                    commit.fingerprint_before.as_deref(),
                )? {
                    GoalFingerprint::None => {
                        // No Git observation exists for this generation;
                        // check output alone decides the streak.
                        if same_output {
                            counters.consecutive_same_output += 1;
                        } else {
                            counters.consecutive_same_output = 0;
                        }
                    }
                    GoalFingerprint::Incomplete => {
                        // An incomplete fingerprint never proves no progress.
                        counters.consecutive_same_output = 0;
                        counters.last_fingerprint = commit.fingerprint_after.clone();
                    }
                    GoalFingerprint::Complete(fingerprint) => {
                        let same_fingerprint =
                            counters.last_fingerprint.as_deref() == Some(fingerprint.as_str());
                        if same_output && same_fingerprint {
                            counters.consecutive_same_output += 1;
                        } else {
                            counters.consecutive_same_output = 0;
                        }
                        counters.last_fingerprint = commit.fingerprint_after.clone();
                    }
                }
                counters.last_output_digest = Some(digest.clone());
            } else {
                // A partly captured output proves no repeat: no streak may
                // span it, so the next round compares against nothing.
                counters.last_output_digest = None;
                counters.consecutive_same_output = 0;
            }
            let cost = goal_cost_reading_tx(
                &tx,
                agent_num,
                row.generation,
                &row.definition,
                row.state.cost_base.as_ref(),
                counters.turns_used,
            )?;
            match &cost {
                Some(GoalCostReading::Spent { micros, source }) => {
                    counters.cost_spent_micros = Some(*micros);
                    counters.cost_unverified = None;
                    learned_cost_source = source.clone();
                }
                Some(GoalCostReading::Unverified(reason)) => {
                    counters.cost_spent_micros = None;
                    counters.cost_unverified = Some((*reason).to_owned());
                }
                None => {}
            }
            if let Some(reason) = blocked_reason {
                GoalCommitOutcome::Blocked {
                    reason: reason.clone(),
                }
            } else if unverified_completion_rounds > 0
                && (goal_postcheck_block(&row.definition, &counters).is_some()
                    || row
                        .definition
                        .max_without_progress
                        .or(row.definition.max_rounds.is_none().then_some(3))
                        .is_some_and(|limit| unverified_completion_rounds >= limit))
            {
                GoalCommitOutcome::Blocked {
                    reason: COMPLETION_DEFERRAL_LIMIT_REASON.to_owned(),
                }
            } else if retry_completion.is_some() {
                match goal_postcheck_block(&row.definition, &counters) {
                    Some(reason) => GoalCommitOutcome::Blocked { reason },
                    None => GoalCommitOutcome::Retryable,
                }
            } else if check.exit_code == Some(0) {
                GoalCommitOutcome::Completed
            } else if let Some(reason) = goal_postcheck_block(&row.definition, &counters) {
                GoalCommitOutcome::Blocked { reason }
            } else if let Some(reason) = cost
                .as_ref()
                .and_then(|reading| reading.block(&row.definition))
            {
                GoalCommitOutcome::Blocked { reason }
            } else if stage == GoalContinuationStage::Escalate
                && !escalation_admissible(&commit.principal, &row.definition, &agent_row)
            {
                // Committed, so the round and its budgets still advance.
                GoalCommitOutcome::Blocked {
                    reason: "escalation_refused".to_owned(),
                }
            } else if let Some(continuation) = &commit.continuation {
                if continuation.stage != stage {
                    return Err(Error::new(
                        "stale_round",
                        "goal continuation stage no longer matches the round",
                    ));
                }
                let limits = self.hierarchy_limits;
                match stage {
                    GoalContinuationStage::Repair => {}
                    GoalContinuationStage::Diagnose => {
                        counters.diagnosis_round = Some(counters.rounds)
                    }
                    GoalContinuationStage::Escalate => {
                        let escalation = row.definition.escalation.as_ref().ok_or_else(|| {
                            Error::new("stale_round", "goal escalation is no longer defined")
                        })?;
                        let id = continuation.escalation_id.clone().ok_or_else(|| {
                            Error::new("invalid", "goal escalation needs a configure command id")
                        })?;
                        if command_binding(&tx, &id)?.is_some() || write_exists(&tx, &id)? {
                            return Err(Error::new(
                                "id_conflict",
                                "goal escalation id is already used",
                            ));
                        }
                        let configure = SubmitCommand {
                            id,
                            target: Some(commit.agent.clone()),
                            selector: None,
                            verb: "configure".to_owned(),
                            args: escalation.configure_args(),
                            expected_generation: Some(row.generation),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                            trusted_review_labels: None,
                        };
                        escalation_admission = Some(admission::submit_tx(
                            &tx,
                            &commit.principal,
                            configure,
                            None,
                            now,
                            false,
                            limits,
                            None,
                        )?);
                        counters.escalation_round = Some(counters.rounds);
                    }
                }
                let request = SubmitCommand {
                    id: continuation.id.clone(),
                    target: Some(commit.agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text": continuation.text}),
                    expected_generation: Some(row.generation),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                    trusted_review_labels: None,
                };
                if command_binding(&tx, &request.id)?.is_some() || write_exists(&tx, &request.id)? {
                    return Err(Error::new(
                        "id_conflict",
                        "goal continuation id is already used",
                    ));
                }
                let send = admission::submit_tx(
                    &tx,
                    &commit.principal,
                    request,
                    None,
                    now,
                    false,
                    limits,
                    None,
                )?;
                let outcome = GoalCommitOutcome::Continued {
                    command_id: send.command.id.clone(),
                };
                admission = Some(send);
                outcome
            } else {
                return Err(Error::new(
                    "internal",
                    "failed goal check needs a block reason or a continuation",
                ));
            }
        } else if retry_check {
            counters.rounds = counters.rounds.saturating_add(1);
            counters.last_exit_code = None;
            counters.last_check_at = Some(now);
            counters.last_observation = commit.observation.clone();
            match goal_postcheck_block(&row.definition, &counters) {
                Some(reason) => GoalCommitOutcome::Blocked { reason },
                None => GoalCommitOutcome::Retryable,
            }
        } else if let Some(reason) = blocked_reason {
            if let Some(observation) = &commit.observation {
                counters.last_observation = Some(observation.clone());
            }
            GoalCommitOutcome::Blocked {
                reason: reason.clone(),
            }
        } else {
            return Err(Error::new(
                "internal",
                "goal round without a check requires a block or retry decision",
            ));
        };
        if let Some(history) = &commit.check_history {
            record_goal_check_history_tx(&tx, history)?;
        }
        let mut state = row.state;
        state.unverified_completion_rounds = unverified_completion_rounds;
        state.await_first_turn = false;
        // A zero baseline (no session yet at set) binds the first session seen.
        if let (Some(base), Some(source)) = (state.cost_base.as_mut(), learned_cost_source)
            && base.source.is_none()
        {
            base.source = Some(source);
        }
        match &outcome {
            GoalCommitOutcome::Completed => {
                state.status = GoalStatus::Completed;
                state.last_continuation_command_id = None;
            }
            GoalCommitOutcome::Blocked { reason } if reason == COMPLETION_DEFERRAL_LIMIT_REASON => {
                state.status = GoalStatus::Paused;
                state.paused_reason = Some(reason.clone());
                state.last_continuation_command_id = None;
            }
            GoalCommitOutcome::Blocked { reason } => {
                state.status = GoalStatus::Blocked;
                state.blocked_reason = Some(reason.clone());
                state.last_continuation_command_id = None;
            }
            // Only a newly admitted continuation binds: an idempotent
            // readback of an older command (same round id at a fresh key)
            // must never grant the owned-continuation proof.
            GoalCommitOutcome::Continued { command_id } => {
                state.last_continuation_command_id =
                    if admission.as_ref().is_some_and(|admission| admission.is_new) {
                        Some(command_id.clone())
                    } else {
                        None
                    };
            }
            GoalCommitOutcome::Retryable => {}
        }
        state.counters = counters;
        if let GoalRoundKey::Slot(slot) = commit.round_key {
            state.last_committed_slot = Some(slot);
        }
        let state_text = encode_json(&state)?;
        db!(tx.execute(
            "DELETE FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
               AND round_key = ?5 AND round_key_kind = ?6",
            params![
                agent_num,
                commit.mode,
                commit.expected_generation,
                commit.expected_revision,
                commit.round_key.value(),
                commit.round_key.kind(),
            ],
        ));
        db!(tx.execute(
            "UPDATE goals SET revision = ?1, state = ?2, last_round_key = ?3,
                 updated_at = ?4
             WHERE agent = ?5 AND mode = ?6 AND revision = ?7",
            params![
                row.revision + 1,
                state_text,
                commit.round_key.text(),
                now,
                agent_num,
                commit.mode,
                row.revision,
            ],
        ));
        let mut events = Vec::new();
        if state.project_default
            && matches!(outcome, GoalCommitOutcome::Completed)
            && let Some((project, fingerprint)) = &commit.project_check
            && let Some(event) = project_check_passed_event(
                &tx,
                agent_num,
                &agent_row,
                project,
                fingerprint,
                commit.retry_timeout_ms.or(commit.check_timeout_ms),
                now,
            )?
        {
            events.push(event);
        }
        if let (Some(cause), Some((status, reason))) = (
            state.assignment_cause.clone(),
            match &outcome {
                GoalCommitOutcome::Completed => Some(("completed", None)),
                GoalCommitOutcome::Blocked { reason }
                    if reason == COMPLETION_DEFERRAL_LIMIT_REASON =>
                {
                    Some(("paused", Some(reason.clone())))
                }
                GoalCommitOutcome::Blocked { reason } => Some(("blocked", Some(reason.clone()))),
                GoalCommitOutcome::Continued { .. } | GoalCommitOutcome::Retryable => None,
            },
        ) {
            events.push(insert_daemon_event_input(
                &tx,
                DaemonEventInput::new(
                    now,
                    "goal.changed",
                    "goal",
                    Some(agent_num),
                    Some(commit.expected_generation),
                    json!({"status": status, "reason": reason}),
                )
                .with_cause(cause),
            )?);
        }
        let row = load_goal_row(&tx, agent_num, &commit.mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after round"))?;
        let view = goal_view_on(&tx, &self.goal_holder, row)?;
        let (wire_outcome, reason, command_id) = match &outcome {
            GoalCommitOutcome::Completed => (GoalRoundOutcome::Completed, None, None),
            GoalCommitOutcome::Blocked { reason } => {
                (GoalRoundOutcome::Blocked, Some(reason.clone()), None)
            }
            GoalCommitOutcome::Continued { command_id } => {
                (GoalRoundOutcome::Continued, None, Some(command_id.clone()))
            }
            GoalCommitOutcome::Retryable => (
                GoalRoundOutcome::Retryable,
                Some(retry_completion.unwrap_or(CHECK_SPAWN_RETRYABLE).to_owned()),
                None,
            ),
        };
        let response = GoalRoundResponse {
            id: request.id.clone(),
            outcome: wire_outcome,
            reason,
            command_id,
            view: view.clone(),
            continuation: admission.as_ref().map(|a| CommandSubmitResponse {
                command_id: a.command.id.clone(),
                agent: a.command.target.clone(),
                generation: a.command.generation,
                state: a.command.state,
                outcome: a.command.outcome.clone(),
                linked_command: None,
            }),
        };
        request.validate_response(&response)?;
        events.push(insert_goal_round_event(
            &tx,
            &record,
            response.outcome.into(),
            response.reason.clone(),
            commit.check.as_ref().and_then(|check| check.exit_code),
            commit.check_timeout_ms,
            commit.retry_timeout_ms,
            state.assignment_cause.clone(),
            now,
        )?);
        record.outcome = StoredGoalRoundOutcome::Finished(Box::new(response.clone()));
        update_goal_round_tx(&tx, &record)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(GoalRoundCommitResult {
            outcome,
            view,
            admission,
            response,
            escalation: escalation_admission,
            events,
        })
    }
}

fn project_check_passed_event(
    tx: &Transaction<'_>,
    agent_num: i64,
    agent: &Agent,
    project: &str,
    fingerprint: &str,
    check_deadline_ms: Option<i64>,
    now: i64,
) -> Result<Option<Event>> {
    if agent.project != project
        || agent.branch.is_none()
        || agent.parent.is_some()
        || projects::project_registration_tx(tx, project)?.is_none_or(|registration| {
            !registration
                .actions
                .iter()
                .any(|action| action == "git.publish")
        })
    {
        return Ok(None);
    }
    let turn_id: Option<String> = tx
        .query_row(
            "SELECT turn_id FROM turns WHERE agent = ?1 AND generation = ?2
             ORDER BY COALESCE(completed_at, started_at, 0) DESC, rowid DESC LIMIT 1",
            params![agent_num, agent.generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(turn_id) = turn_id else {
        return Ok(None);
    };
    let Some(turn) = load_turn(tx, agent_num, agent.generation, &turn_id)? else {
        return Ok(None);
    };
    let Some(subject) = turn
        .final_text
        .as_deref()
        .and_then(|text| text.lines().next())
    else {
        return Ok(None);
    };
    if turn.state != TurnState::Completed || turn.outcome != Some(core::TurnOutcome::Success) {
        return Ok(None);
    }
    let receipt = core::api::ProjectCheckPassed {
        project: project.to_owned(),
        turn_id,
        fingerprint: fingerprint.to_owned(),
        check_deadline_ms,
        subject: subject.to_owned(),
    };
    if receipt.validate().is_err() {
        return Ok(None);
    }
    insert_daemon_event(
        tx,
        now,
        "project.check.passed",
        "goal",
        Some(agent_num),
        Some(agent.generation),
        serde_json::to_value(receipt)
            .map_err(|_| Error::new("internal", "project check receipt could not be encoded"))?,
    )
    .map(Some)
}

/// One continuation command row as the probe reads it. Only the fields
/// that bind it to this Goal's Agent, generation and completed turn.
struct ContinuationCommand {
    target: Option<i64>,
    generation: Option<i64>,
    turn_id: Option<String>,
    state: String,
    verb: String,
}

/// Whether the goal's turn budget was spent solely by its own last
/// admitted continuation. Qualifies only when exactly one turn ran since
/// the last round commit (the stored `turns_used` is that commit's live
/// count) and that turn is the pending continuation's completed turn,
/// resolved through its durable command id — two bounded point lookups,
/// never an event scan. Anything else (no binding, another turn after
/// the continuation, an unfinished or unknown turn) does not qualify.
fn goal_continuation_spent_budget(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    turns_at_set: i64,
    stored_turns_used: i64,
    continuation_command_id: Option<&str>,
    turn_count: i64,
) -> Result<bool> {
    let Some(command_id) = continuation_command_id else {
        return Ok(false);
    };
    if turn_count.saturating_sub(turns_at_set) != stored_turns_used.saturating_add(1) {
        return Ok(false);
    }
    let command: Option<ContinuationCommand> = connection
        .query_row(
            "SELECT target, generation, turn_id, state, verb FROM commands WHERE id = ?1",
            params![command_id],
            |row| {
                Ok(ContinuationCommand {
                    target: row.get(0)?,
                    generation: row.get(1)?,
                    turn_id: row.get(2)?,
                    state: row.get(3)?,
                    verb: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some(command) = command else {
        return Ok(false);
    };
    if command.target != Some(agent_num) || command.state != "applied" || command.verb != "send" {
        return Ok(false);
    }
    // A missing command generation is not an exact binding.
    if command.generation != Some(generation) {
        return Ok(false);
    }
    let Some(turn_id) = command.turn_id else {
        return Ok(false);
    };
    let turn_state: Option<String> = connection
        .query_row(
            "SELECT state FROM turns WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
            params![agent_num, generation, turn_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(turn_state.as_deref() == Some("completed"))
}

/// Decide a failed check against the frozen limits. Success always
/// completes; otherwise the round budget, turn budget and no-progress streak
/// block in that order. Counters are already updated for this check.
/// Cost baseline of a Goal with `max_cost`: the native session's cumulative
/// cost at `set` in the budget's unit, and the session it came from. A zero
/// baseline without a source binds the first session the Goal sees.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct GoalCostBase {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    amount_micros: i64,
}

/// The cost spent since `set`, or why it cannot be trusted.
enum GoalCostReading {
    Spent { micros: i64, source: Option<String> },
    Unverified(&'static str),
}

impl GoalCostReading {
    fn block(&self, definition: &GoalDefinition) -> Option<String> {
        match self {
            Self::Unverified(_) => Some("cost_unverified".to_owned()),
            Self::Spent { micros, .. } => definition
                .max_cost
                .as_ref()
                .filter(|limit| *micros >= limit.amount_micros)
                .map(|_| "cost_limit".to_owned()),
        }
    }
}

fn cost_micros(amount: f64) -> i64 {
    (amount * 1_000_000.0).round().clamp(0.0, i64::MAX as f64) as i64
}

/// The latest native session snapshot of an Agent generation and when it was
/// observed.
fn latest_session_usage(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<Option<(core::SessionUsage, i64)>> {
    let row: Option<(String, i64)> = tx
        .query_row(
            "SELECT snapshot, observed_at FROM session_usage WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(snapshot, observed_at)| {
        serde_json::from_value(decode_json(&snapshot)?)
            .map(|usage| (usage, observed_at))
            .map_err(|_| Error::new("internal", "stored session usage is malformed"))
    })
    .transpose()
}

/// The verbatim cumulative cost of a snapshot, when it has amount and unit.
fn snapshot_cost(usage: &core::SessionUsage) -> Option<(f64, &str)> {
    let cost = usage.cumulative_cost.as_ref()?;
    Some((cost.amount?, cost.unit.as_deref()?))
}

/// Baseline at `set` or launch preflight. A budget needs the Agent's native
/// session cost in its unit: the latest snapshot, or zero before its first turn.
fn goal_cost_base_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    definition: &GoalDefinition,
    turn_count: i64,
) -> Result<Option<GoalCostBase>> {
    let Some(limit) = &definition.max_cost else {
        return Ok(None);
    };
    match latest_session_usage(tx, agent_num, generation)? {
        Some((usage, _)) => match snapshot_cost(&usage) {
            Some((amount, unit)) if unit == limit.unit => Ok(Some(GoalCostBase {
                source: usage.source.clone(),
                amount_micros: cost_micros(amount),
            })),
            Some(_) => Err(Error::new(
                "unsupported",
                "the Agent reports its session cost in another unit than max_cost",
            )),
            None => Err(Error::new(
                "unsupported",
                "the Agent's harness reports no session cost",
            )),
        },
        None if turn_count == 0 => Ok(Some(GoalCostBase::default())),
        None => Err(Error::new(
            "unsupported",
            "no session cost was observed for this Agent's turns",
        )),
    }
}

/// The cost spent since `set`, read after a check. Missing, stale, mixed,
/// reset or decreasing data is unverified, never zero.
fn goal_cost_reading_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    definition: &GoalDefinition,
    base: Option<&GoalCostBase>,
    turns_used: i64,
) -> Result<Option<GoalCostReading>> {
    let Some(limit) = &definition.max_cost else {
        return Ok(None);
    };
    let Some(base) = base else {
        return Ok(Some(GoalCostReading::Unverified(
            "no cost baseline is recorded for this session",
        )));
    };
    let Some((usage, observed_at)) = latest_session_usage(tx, agent_num, generation)? else {
        return Ok(Some(if turns_used == 0 {
            GoalCostReading::Spent {
                micros: 0,
                source: base.source.clone(),
            }
        } else {
            GoalCostReading::Unverified("no session cost was observed")
        }));
    };
    let last_completed: Option<i64> = tx
        .query_row(
            "SELECT MAX(completed_at) FROM turns WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let reading = match snapshot_cost(&usage) {
        None => GoalCostReading::Unverified("the latest session snapshot has no cost"),
        Some((_, unit)) if unit != limit.unit => {
            GoalCostReading::Unverified("the session cost unit changed")
        }
        Some(_) if usage.source.is_none() && base.amount_micros > 0 => {
            GoalCostReading::Unverified("the session snapshot names no source")
        }
        Some(_) if base.source.is_some() && usage.source != base.source => {
            GoalCostReading::Unverified("the native session changed")
        }
        Some(_) if last_completed.is_some_and(|done| done > observed_at) => {
            GoalCostReading::Unverified("no session cost after the last turn")
        }
        Some((amount, _)) => match cost_micros(amount).checked_sub(base.amount_micros) {
            Some(micros) if micros >= 0 => GoalCostReading::Spent {
                micros,
                source: usage.source.clone(),
            },
            _ => GoalCostReading::Unverified("the cumulative session cost went down"),
        },
    };
    Ok(Some(reading))
}

/// `untracked` without a budget; otherwise spent and budgeted amounts, or why
/// the spent cost is unverified.
fn goal_cost_status(definition: &GoalDefinition, counters: &GoalCounters) -> String {
    let Some(limit) = &definition.max_cost else {
        return "untracked".to_owned();
    };
    if let Some(reason) = &counters.cost_unverified {
        return format!("unverified: {reason}");
    }
    let amount = |micros: i64| format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000);
    match counters.cost_spent_micros {
        Some(spent) => format!(
            "{} of {} {}",
            amount(spent),
            amount(limit.amount_micros),
            limit.unit
        ),
        None => format!(
            "not observed yet, budget {} {}",
            amount(limit.amount_micros),
            limit.unit
        ),
    }
}

/// Whether the Goal's escalation `configure` can be admitted now: the Owner
/// principal and a model or effort this Agent's harness can apply.
fn escalation_admissible(principal: &str, definition: &GoalDefinition, agent: &Agent) -> bool {
    principal == "owner"
        && definition.escalation.as_ref().is_some_and(|escalation| {
            admission::resolve_configure(
                &escalation.configure_args(),
                &agent.harness,
                &agent.profile_snapshot,
            )
            .is_ok()
        })
}

pub(crate) const COMPLETION_DEFERRAL_LIMIT_REASON: &str = "unsuccessful_turn_deferral_limit";

fn goal_postcheck_block(definition: &GoalDefinition, counters: &GoalCounters) -> Option<String> {
    if definition
        .max_rounds
        .is_some_and(|max| counters.rounds >= max)
    {
        return Some("round_budget".to_owned());
    }
    if definition
        .max_turns
        .is_some_and(|max| counters.turns_used >= max)
    {
        return Some("turn_limit".to_owned());
    }
    if definition
        .max_without_progress
        .is_some_and(|max| counters.consecutive_same_output >= max)
    {
        return Some("no_progress".to_owned());
    }
    None
}

fn require_eliot_mode(mode: &str) -> Result<()> {
    if mode != "eliot" {
        return Err(Error::new(
            "unsupported",
            "native Goal mode is not supported by the installed harness plugins",
        ));
    }
    Ok(())
}

fn require_key_range(key: GoalRoundKey) -> Result<()> {
    match key {
        GoalRoundKey::Idle(seq) => {
            if seq < 0 {
                return Err(Error::new(
                    "invalid",
                    "goal round idle_seq must be nonnegative",
                ));
            }
        }
        GoalRoundKey::Slot(slot) => {
            if slot < 1 {
                return Err(Error::new("invalid", "goal round slot must be at least 1"));
            }
        }
    }
    Ok(())
}

/// The key form must match the Goal: cadenced Goals round on slots, the
/// rest on idle seqs. A mismatch is a discipline refusal, not a stale key.
fn require_key_form(row: &GoalRow, key: GoalRoundKey) -> Result<()> {
    match (row.definition.cadence_ms, key) {
        (Some(_), GoalRoundKey::Idle(_)) => Err(Error::new(
            "bad_state",
            "cadenced goal rounds name cadence slots, not idle events",
        )),
        (None, GoalRoundKey::Slot(_)) => Err(Error::new(
            "bad_state",
            "goal without a cadence rounds on idle events, not slots",
        )),
        _ => Ok(()),
    }
}

/// Start of slot `slot` (1-based) on the Goal's anchored grid. Like timer
/// intervals, the first slot starts one period after the anchor.
fn slot_start_ms(anchor_ms: i64, period_ms: i64, slot: i64) -> Result<i64> {
    let start = i128::from(anchor_ms) + i128::from(period_ms) * i128::from(slot);
    i64::try_from(start)
        .map_err(|_| Error::new("invalid", "goal cadence slot overflows epoch millis"))
}

/// A slot round names a due slot newer than the last committed one.
/// Due slots never un-due, so the commit recheck only guards clock
/// rollback. Called at probe, claim and commit alike.
fn require_slot_due(row: &GoalRow, slot: i64, now: i64) -> Result<()> {
    if row
        .state
        .last_committed_slot
        .is_some_and(|committed| slot <= committed)
    {
        return Err(Error::new(
            "stale_round",
            "goal cadence slot is not newer than the last committed slot",
        ));
    }
    let (Some(period_ms), Some(anchor_ms)) =
        (row.definition.cadence_ms, row.state.cadence_anchor_ms)
    else {
        return Err(Error::new("internal", "cadenced goal lost its anchor"));
    };
    if slot_start_ms(anchor_ms, period_ms, slot)? > now {
        return Err(Error::new(
            "stale_round",
            "goal cadence slot is not due yet",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct GoalMarkerRow {
    generation: i64,
    round_key: i64,
    kind: String,
    holder: String,
    state: String,
}

impl GoalMarkerRow {
    /// Live only for the holder that claimed it. A `running` row from a
    /// crashed holder reads as `unknown` everywhere.
    fn is_live(&self, holder: &str) -> bool {
        self.state == "running" && self.holder == holder
    }
}

impl Store {
    /// Render one Goal row with a live turn count and its unresolved round
    /// marker (the live in-flight round if any, else this generation's
    /// latest unknown marker).
    fn goal_view(&self, row: GoalRow) -> Result<GoalView> {
        goal_view_on(&self.connection, &self.goal_holder, row)
    }
}

fn goal_view_on(connection: &Connection, goal_holder: &str, row: GoalRow) -> Result<GoalView> {
    let turn_count = count_turns(connection, row.agent_num, row.generation)?;
    let mut counters = row.state.counters.clone();
    counters.turns_used = counters.turns_used.max(
        row.state
            .transferred_turns
            .saturating_add(turn_count.saturating_sub(counters.turns_at_set)),
    );
    let round_marker = display_goal_marker(
        connection,
        goal_holder,
        row.agent_num,
        &row.mode,
        row.generation,
    )?;
    let cost_status = goal_cost_status(&row.definition, &counters);
    Ok(GoalView {
        agent: format!("a{}", row.agent_num),
        mode: if row.mode == "eliot" {
            GoalMode::Eliot
        } else {
            GoalMode::Native
        },
        generation: row.generation,
        revision: row.revision,
        cadence_anchor_ms: row.state.cadence_anchor_ms,
        status: row.state.status,
        paused_reason: row.state.paused_reason,
        blocked_reason: row.state.blocked_reason,
        definition: row.definition,
        counters,
        scope_start: row
            .state
            .scope_base
            .as_ref()
            .map(core::api::GoalScopeBase::start),
        cost_status,
        last_round_key: row.last_round_key,
        round_marker,
        updated_at: row.updated_at,
    })
}

/// The live in-flight marker if one exists, else this generation's
/// latest unknown marker (crashed `running` rows read as `unknown`).
/// Revision bumps never hide it.
fn display_goal_marker(
    connection: &Connection,
    holder: &str,
    agent_num: i64,
    mode: &str,
    generation: i64,
) -> Result<Option<GoalRoundMarker>> {
    let live: Option<(i64, String, i64, i64, i64)> = connection
        .query_row(
            "SELECT round_key, round_key_kind, generation, revision, updated_at
             FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND state = 'running' AND holder = ?3
             LIMIT 1",
            params![agent_num, mode, holder],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    if let Some((round_key, kind, generation, revision, updated_at)) = live {
        return Ok(Some(GoalRoundMarker {
            round_key: format!("{kind}:{round_key}"),
            generation,
            revision,
            state: GoalMarkerState::Running,
            updated_at,
        }));
    }
    let burned: Option<(i64, String, i64, i64)> = connection
        .query_row(
            "SELECT round_key, round_key_kind, revision, updated_at FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3
               AND (state = 'unknown' OR holder <> ?4)
             ORDER BY updated_at DESC, round_key DESC LIMIT 1",
            params![agent_num, mode, generation, holder],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(
        burned.map(|(round_key, kind, revision, updated_at)| GoalRoundMarker {
            round_key: format!("{kind}:{round_key}"),
            generation,
            revision,
            state: GoalMarkerState::Unknown,
            updated_at,
        }),
    )
}

fn load_goal_markers(
    connection: &Connection,
    agent_num: i64,
    mode: &str,
) -> Result<Vec<GoalMarkerRow>> {
    let mut statement = connection
        .prepare(
            "SELECT generation, round_key, round_key_kind, holder, state
             FROM goal_markers WHERE agent = ?1 AND mode = ?2",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, mode], |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                kind: row.get(2)?,
                holder: row.get(3)?,
                state: row.get(4)?,
            })
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<GoalMarkerRow>>>()
        .map_err(sql_error)
}

fn load_goal_marker_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    mode: &str,
    generation: i64,
    revision: i64,
    key: GoalRoundKey,
) -> Result<Option<GoalMarkerRow>> {
    tx.query_row(
        "SELECT generation, round_key, round_key_kind, holder, state
         FROM goal_markers
         WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
           AND round_key = ?5 AND round_key_kind = ?6",
        params![
            agent_num,
            mode,
            generation,
            revision,
            key.value(),
            key.kind()
        ],
        |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                kind: row.get(2)?,
                holder: row.get(3)?,
                state: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(sql_error)
}

/// The Goal's latest automatic idle key and whether it is already spent, the
/// burn the probe and claim enforce. A live marker does not spend its key.
fn latest_idle_key(
    connection: &Connection,
    goal: &GoalRow,
    holder: &str,
) -> Result<Option<(i64, bool)>> {
    let seq = max_agent_seq(connection, goal)?;
    if seq <= 0 || !round_key_names_automatic_idle_event(connection, goal, seq)? {
        return Ok(None);
    }
    let key = GoalRoundKey::Idle(seq);
    let consumed = goal.last_round_key.as_deref() == Some(&key.text())
        || load_goal_markers(connection, goal.agent_num, "eliot")?
            .iter()
            .any(|marker| {
                marker.round_key == key.value()
                    && marker.kind == key.kind()
                    && marker.generation == goal.generation
                    && !marker.is_live(holder)
            });
    Ok(Some((seq, consumed)))
}

/// The first reason the Agent is not cleanly idle for a Goal round.
fn idle_round_refusal(
    connection: &Connection,
    agent_num: i64,
    agent: &Agent,
    generation: i64,
    now: i64,
) -> Result<Option<Error>> {
    if agent.state != AgentState::Idle {
        return Ok(Some(Error::new(
            "bad_state",
            "goal round needs an idle Agent",
        )));
    }
    if agent.active_turn.is_some() {
        return Ok(Some(Error::new(
            "bad_state",
            "goal round needs no active turn",
        )));
    }
    if !agent.pending.is_empty() {
        return Ok(Some(Error::new(
            "bad_state",
            "goal round needs no pending requests",
        )));
    }
    if let Some(source) = first_unhealthy_open_source(connection, agent_num, generation)? {
        return Ok(Some(Error::new(
            "bad_state",
            format!("goal round needs healthy harness sources; {source} is not healthy"),
        )));
    }
    if count_agent_commands(connection, agent_num, generation, true)? > 0 {
        return Ok(Some(Error::new(
            "bad_state",
            "resolve unknown commands before the goal round",
        )));
    }
    if count_agent_commands(connection, agent_num, generation, false)? > 0 {
        return Ok(Some(Error::new("busy", "Agent has open commands")));
    }
    if !send_outcome_gate_open(connection, agent_num, generation)? {
        return Ok(Some(Error::new(
            "bad_state",
            "resolve unsettled turns before the goal round",
        )));
    }
    let native_followup_pending: bool = connection
        .query_row(
            "SELECT COALESCE((
                 SELECT native_followup_pending = 1 AND completed_at > ?3
                 FROM turns INDEXED BY turns_agent_result_order
                 WHERE agent = ?1 AND generation = ?2
                 ORDER BY insertion_seq DESC LIMIT 1
             ), 0)",
            params![
                agent_num,
                generation,
                now.saturating_sub(NATIVE_FOLLOWUP_WINDOW_MS)
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if native_followup_pending {
        return Ok(Some(Error::new(
            "busy",
            "the harness reported pending native follow-up work",
        )));
    }
    Ok(None)
}

/// The round key must name an idle event of this Agent generation: the
/// event-time subject snapshot shows the Agent idle. A missing row, a row
/// of another Agent/generation, a Swarm note event, or a non-idle subject
/// is a stale key.
fn round_key_names_idle_event(connection: &Connection, goal: &GoalRow, seq: i64) -> Result<bool> {
    round_key_names_idle_event_with(connection, goal, seq, true)
}

fn round_key_names_automatic_idle_event(
    connection: &Connection,
    goal: &GoalRow,
    seq: i64,
) -> Result<bool> {
    round_key_names_idle_event_with(connection, goal, seq, false)
}

fn round_key_names_idle_event_with(
    connection: &Connection,
    goal: &GoalRow,
    seq: i64,
    allow_unknown_goal_round: bool,
) -> Result<bool> {
    // A rejected continuation leaves the Agent idle without native progress.
    // Its rejection and the rejection of its claimed context cannot rearm
    // the same Goal. Use the retained event so this also holds after restart.
    let sql = format!(
        "SELECT agent, generation, subject FROM events
         WHERE seq = ?1 AND NOT {SWARM_NOTE_EVENT_SQL} AND (
             ?2 IS NULL OR source <> 'daemon' OR kind <> 'command.rejected'
             OR CASE
                 WHEN json_extract(data, '$.outcome.code') = 'native_not_dispatched'
                     THEN json_extract(data, '$.command_id')
                 WHEN json_extract(data, '$.outcome.reason') = 'consumer_input_unconfirmed'
                     THEN json_extract(data, '$.outcome.claim.consumer_id')
             END IS NOT ?2
         ) AND (kind <> 'goal.round' OR (
             json_extract(data, '$.outcome') IN ('blocked', 'completed')
             AND json_extract(data, '$.revision') + 1 < ?3
         ) OR (
             json_extract(data, '$.outcome') = 'retryable'
             AND json_extract(data, '$.reason') = 'check_spawn_retryable'
             AND json_extract(data, '$.revision') + 1 = ?3
         ) OR (
             ?4 = 1
             AND json_extract(data, '$.outcome') = 'unknown'
             AND json_extract(data, '$.reason') IS NOT 'stale_round'
             AND json_extract(data, '$.revision') = ?3
         ))"
    );
    connection
        .query_row(
            &sql,
            params![
                seq,
                goal.state.last_continuation_command_id,
                goal.revision,
                i64::from(allow_unknown_goal_round),
            ],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)
        .map(|found| {
            found.is_some_and(|(agent, row_generation, subject)| {
                agent == Some(goal.agent_num)
                    && row_generation == Some(goal.generation)
                    && subject_is_idle(subject.as_deref())
            })
        })
}

/// An event-time subject snapshot proves the Agent was idle at that
/// event. A missing or unreadable subject proves nothing.
fn subject_is_idle(subject: Option<&str>) -> bool {
    subject.is_some_and(|text| {
        serde_json::from_str::<EventSubject>(text)
            .map(|subject| subject.state == AgentState::Idle)
            .unwrap_or(false)
    })
}

/// No-progress fingerprint of the round's workspace. The Store decides only
/// whether Git applies (a Git binding of this generation); the fingerprint
/// itself is the Router's fresh read, so a shell edit of an already dirty
/// file changes it and a stale or partial observation never stands in.
enum GoalFingerprint {
    /// No Git binding exists for this generation: the streak falls back to
    /// check output alone.
    None,
    /// The fresh read failed or exceeded its bounds. Never proves no progress.
    Incomplete,
    /// Digest of HEAD and the content `dirty_digest`, read before the check.
    /// It is compared with the read after the previous failed check.
    Complete(String),
}

fn goal_fingerprint_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    fresh: Option<&str>,
) -> Result<GoalFingerprint> {
    let owners: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM source_owners WHERE agent = ?1 AND generation = ?2
             AND family IN ('git', 'file') AND source = 'git' AND mechanism = 'git'",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if owners == 0 {
        return Ok(GoalFingerprint::None);
    }
    Ok(match fresh {
        Some(fingerprint) => GoalFingerprint::Complete(fingerprint.to_owned()),
        None => GoalFingerprint::Incomplete,
    })
}

fn load_goal_row(connection: &Connection, agent_num: i64, mode: &str) -> Result<Option<GoalRow>> {
    connection
        .query_row(
            "SELECT agent, mode, generation, revision, definition, state,
                    last_round_key, updated_at
             FROM goals WHERE agent = ?1 AND mode = ?2",
            params![agent_num, mode],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .map(
            |(agent, mode, generation, revision, definition, state, last_round_key, updated_at)| {
                let definition: GoalDefinition =
                    serde_json::from_str(&definition).map_err(|error| {
                        Error::new("internal", format!("invalid stored goal: {error}"))
                    })?;
                let state: GoalStatePersist = serde_json::from_str(&state).map_err(|error| {
                    Error::new("internal", format!("invalid stored goal state: {error}"))
                })?;
                Ok(GoalRow {
                    agent_num: agent,
                    mode,
                    generation,
                    revision,
                    definition,
                    state,
                    last_round_key,
                    updated_at,
                })
            },
        )
        .transpose()
}

fn count_turns(connection: &Connection, agent_num: i64, generation: i64) -> Result<i64> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn max_agent_seq(connection: &Connection, goal: &GoalRow) -> Result<i64> {
    let sql = format!(
        "SELECT COALESCE((
             SELECT seq FROM events INDEXED BY events_agent
             WHERE agent = ?1 AND generation = ?2
               AND NOT {SWARM_NOTE_EVENT_SQL}
               AND NOT (kind = 'goal.round'
                        AND json_extract(data, '$.outcome') = 'unknown'
                        AND (
                            json_extract(data, '$.reason') = 'stale_round'
                            OR (json_extract(data, '$.mode') = ?3 AND
                                json_extract(data, '$.revision') <= COALESCE(?4, 0))
                        ))
             ORDER BY seq DESC LIMIT 1
         ), 0)"
    );
    connection
        .query_row(
            &sql,
            params![
                goal.agent_num,
                goal.generation,
                goal.mode,
                goal.state.round_reset_revision
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn first_unhealthy_open_source(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT source FROM source_state
             WHERE agent = ?1 AND generation = ?2 AND state = 'open' AND health <> 'ok'
               AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
             ORDER BY source LIMIT 1",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)
}

fn count_agent_commands(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    unknown_only: bool,
) -> Result<i64> {
    let states = if unknown_only {
        "state = 'unknown'"
    } else {
        concat!(
            "state IN ('accepted', 'queued', 'dispatched')
             AND NOT (state IN ('accepted', 'queued')
                      AND json_extract(args, '$.point') IN ('next_input', 'turn_safe_point')
                      AND ",
            swarm_note_command_sql!(),
            ")"
        )
    };
    let sql = format!(
        "SELECT COUNT(*) FROM commands WHERE target = ?1 AND {states}
         AND (generation IS NULL OR generation = ?2)"
    );
    connection
        .query_row(&sql, params![agent_num, generation], |row| row.get(0))
        .map_err(sql_error)
}

/// Build ready `goal.round` arguments for a stuck active Goal when its Agent
/// is cleanly idle with an unconsumed round key. Returns `None` when a round
/// is not currently admissible so the inbox falls back to inspection.
pub(crate) fn stuck_goal_round_args(
    connection: &Connection,
    agent_num: i64,
    mode: &str,
    now: i64,
) -> Result<Option<Value>> {
    if mode != "eliot" {
        return Ok(None);
    }
    let Some(row) = load_goal_row(connection, agent_num, mode)? else {
        return Ok(None);
    };
    let Some(agent_row) = load_agent_by_num(connection, agent_num)? else {
        return Ok(None);
    };
    if row.state.status != GoalStatus::Active
        || agent_row.observe_only
        || row.generation != agent_row.generation
        || idle_round_refusal(connection, agent_num, &agent_row, row.generation, now)?.is_some()
    {
        return Ok(None);
    }
    if row.state.await_first_turn
        && count_turns(connection, agent_num, row.generation)? <= row.state.counters.turns_at_set
    {
        return Ok(None);
    }
    let markers = load_goal_markers(connection, agent_num, mode)?;
    if markers.iter().any(|marker| marker.state == "running") {
        return Ok(None);
    }
    let agent_id = format_agent_id(agent_num);
    if row.definition.cadence_ms.is_some() {
        let committed = row.state.last_committed_slot.unwrap_or(0);
        let burned = markers
            .iter()
            .filter(|marker| marker.generation == row.generation && marker.kind == "slot")
            .map(|marker| marker.round_key)
            .max()
            .unwrap_or(0);
        let slot = committed.max(burned).saturating_add(1).max(1);
        if require_slot_due(&row, slot, now).is_err() {
            return Ok(None);
        }
        return Ok(Some(json!({
            "agent": agent_id,
            "mode": mode,
            "slot": slot,
        })));
    }
    let seq = max_agent_seq(connection, &row)?;
    if seq <= 0 || !round_key_names_idle_event(connection, &row, seq)? {
        return Ok(None);
    }
    let key = GoalRoundKey::Idle(seq);
    if row.last_round_key.as_deref() == Some(&key.text())
        || markers.iter().any(|marker| {
            marker.generation == row.generation
                && marker.round_key == key.value()
                && marker.kind == key.kind()
        })
    {
        return Ok(None);
    }
    Ok(Some(json!({
        "agent": agent_id,
        "mode": mode,
        "idle_seq": seq,
    })))
}

/// Install the Goal frozen into a Goal-owned assignment launch, bound to the
/// generation that launch starts, in the admission transaction and so before
/// any native dispatch.
pub(super) fn install_assignment_goal_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    definition: &GoalDefinition,
    cause: core::EventCause,
    project_default: bool,
    now: i64,
) -> Result<()> {
    definition.validate()?;
    definition.require_auto_bound()?;
    let state = GoalStatePersist {
        transferred_turns: 0,
        unverified_completion_rounds: 0,
        await_first_turn: true,
        project_default,
        status: GoalStatus::Active,
        paused_reason: None,
        blocked_reason: None,
        round_reset_revision: None,
        counters: GoalCounters::default(),
        cadence_anchor_ms: definition.cadence_ms.map(|_| now),
        last_committed_slot: None,
        last_continuation_command_id: None,
        assignment_cause: Some(cause),
        scope_base: None,
        cost_base: definition
            .max_cost
            .as_ref()
            .map(|_| GoalCostBase::default()),
        stop_paused: false,
    };
    db!(tx.execute(
        "INSERT INTO goals(agent, mode, generation, revision, definition,
             state, last_round_key, updated_at)
         VALUES (?1, ?2, ?3, 1, ?4, ?5, NULL, ?6)",
        params![
            agent_num,
            GoalMode::Eliot.as_str(),
            generation,
            encode_json(definition)?,
            encode_json(&state)?,
            now,
        ],
    ));
    Ok(())
}

/// Whether this Agent generation's Goal was installed by a Goal-owned
/// assignment and has not completed, blocked or been cleared: its work is
/// still open, so a completed turn must not retire it.
pub(super) fn assignment_goal_open(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<bool> {
    let Some(row) = load_goal_row(connection, agent_num, GoalMode::Eliot.as_str())? else {
        return Ok(false);
    };
    Ok(row.generation == generation
        && row.state.assignment_cause.is_some()
        && !matches!(
            row.state.status,
            GoalStatus::Completed | GoalStatus::Blocked | GoalStatus::Cleared
        ))
}
