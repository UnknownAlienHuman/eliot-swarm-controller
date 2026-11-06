#![forbid(unsafe_code)]

//! Durable L1 state for Agents, commands, native events, turns, and source acks.

mod canonical;
mod capacity;
mod projects;
mod provider_holds;
mod rules;
mod scripts;

pub use attribution::WhoWorkerRow;
pub use capacity::{RecordedProcess, RetainedGeneration};
pub use command_children::{CommandChildrenPage, history_expired};
pub use coordination::{NoticeClaim, NoticeKind, NoticePending, NoticeState};
pub use goals::{
    GoalCheckResult, GoalCommitOutcome, GoalContinuation, GoalRoundCommit, GoalRoundCommitResult,
    GoalRoundKey, GoalRoundProbe,
};
pub use projects::ProjectRegistrationInput;
pub use resolution::TurnInactiveProof;
pub use retention::RetentionReport;
pub use rules::{
    RuleCatchUpAction, RuleCatchUpActionOutcome, RuleCatchUpAdmission, RuleCursorOutcome,
    RuleDefinitionInput, RuleEventAdmission, RuleProcessingState, TimerDefinitionInput,
    TimerSlotAdmission, TimerSlotCommit,
};
pub use scripts::ScriptRunAdmission;

use eliot_core::{
    self as core, Agent, AgentControl, AgentEdit, AgentState, BatchAck, Command, CommandState,
    Effect, Event, EventSubject, FileChangeHint, FrozenSetupFile, GitBaseline, GitHintPage,
    GitSnapshot, NativeEvent, NativeStoreBinding, PendingRequest, ProcessIdentity, ProcessRecord,
    ProcessSnapshot, ProcessState, SourceBatch, SourceGap, SourceGapAck, SubmitCommand, Turn,
    TurnState, TurnUsage, WorkspaceCancelOutcome, WorkspaceCreationOutcome, WorkspacePhase,
    WorkspacePreparation, WorkspacePreparationInput, WorkspaceRemovalOutcome, WorkspaceSetupArg,
    WorkspaceSetupOutcome, parse_agent_id,
};
pub use eliot_core::{Error, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{cell::Cell, collections::HashSet, path::Path, time::Duration};
use ulid::Ulid;
use workspace::recover_workspaces;

const MAX_PAGE_ENTRIES: usize = 1_000;
const MAX_PAGE_BYTES: usize = 900 * 1024;
const MAX_NATIVE_FAMILIES: i64 = 9;
const MAX_EVENT_BYTES: usize = 64 * 1024;
const MAX_INPUT_BYTES: usize = 512 * 1024;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RETRY_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
/// Allowed generator-clock skew for ULID age checks. A mutation id timestamp
/// may run ahead of the Store commit clock by up to this margin; admission
/// refuses anything further ahead, and retention keeps rows long enough that
/// such an id still reads `id_expired` after pruning. See
/// docs/spec/configuration.md (daemon retention).
const RETRY_CLOCK_SKEW_MS: i64 = 5 * 60 * 1_000;
const COMMAND_METHOD: &str = "command.submit";
const INITIAL_SOURCE: &str = "stream";
const PROCESS_SOURCE: &str = "os";
const WORKSPACE_PROCESS_SOURCE: &str = "workspace_process";
const GIT_SOURCE: &str = "git";
const LIST_UNSETTLED_COMMANDS_SQL: &str = "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                        created_at, updated_at, probe, selector
                 FROM commands
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown')
                   AND NOT (state = 'unknown' AND verb IN ('attach','detach')
                     AND EXISTS(SELECT 1 FROM agents a WHERE a.id = commands.target AND a.observe_only = 1)
                     AND NOT EXISTS(SELECT 1 FROM source_state s WHERE s.agent = commands.target
                         AND s.source NOT IN ('daemon','os','git','workspace_process') AND s.state = 'open'))
                 ORDER BY created_at, rowid LIMIT ?1";
const LIST_PROCESSES_FIRST_PAGE_SQL: &str =
    "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
             FROM processes WHERE owner = ?1
             ORDER BY host_boot_id, pid, start_time LIMIT ?2";
const LIST_PROCESSES_CURSOR_PAGE_SQL: &str =
    "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
             FROM processes WHERE owner = ?1
               AND (host_boot_id, pid, start_time) > (?2, ?3, ?4)
             ORDER BY host_boot_id, pid, start_time LIMIT ?5";

macro_rules! db {
    ($expression:expr) => {
        $expression.map_err(sql_error)?
    };
}

mod admission;
mod attribution;
mod command_children;
mod coordination;
mod dispatch;
mod git;
mod goals;
mod hierarchy;
mod lifecycle;
mod native_children;
mod native_events;
mod observer;
mod observer_live;
mod processes;
mod resolution;
mod retention;
mod schema;
mod source_batches;
mod source_handoff;
mod workspace;

fn sql_error(error: rusqlite::Error) -> Error {
    Error::new("internal", error.to_string())
}

fn commit_transaction(tx: Transaction<'_>, write_faulted: &Cell<bool>) -> Result<()> {
    match tx.commit() {
        Ok(()) => Ok(()),
        Err(error) => {
            write_faulted.set(true);
            Err(sql_error(error))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Admission {
    pub command: Command,
    pub linked_command: Option<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Change<T> {
    pub value: T,
    pub events: Vec<Event>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceGitReservation {
    Reserved,
    Deferred,
    MayHaveBegun,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchCommit {
    pub ack: BatchAck,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteAdmission {
    pub result: Value,
    pub is_new: bool,
}

/// A single SQLite connection is the Store's writer. All mutations use an
/// IMMEDIATE transaction and return events only after commit succeeds.
pub struct Store {
    connection: Connection,
    write_faulted: Cell<bool>,
    /// Opaque owner of this handle's Goal round markers. A `running` marker
    /// from another holder is a crashed round: it reads as `unknown`.
    goal_holder: String,
    hierarchy_limits: core::api::HierarchyLimits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOpenPhase {
    ConnectionOpened,
    PragmasApplied,
    Migrated,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_observer(path, |_| {})
    }

    pub fn open_with_observer<F>(path: &Path, mut on_phase: F) -> Result<Self>
    where
        F: FnMut(StoreOpenPhase),
    {
        let connection = Connection::open(path).map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(sql_error)?;
        on_phase(StoreOpenPhase::ConnectionOpened);
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;",
            )
            .map_err(sql_error)?;
        on_phase(StoreOpenPhase::PragmasApplied);

        let mut store = Self {
            connection,
            write_faulted: Cell::new(false),
            goal_holder: Ulid::new().to_string(),
            hierarchy_limits: core::api::HierarchyLimits::default(),
        };
        store.migrate()?;
        on_phase(StoreOpenPhase::Migrated);
        Ok(store)
    }

    /// Install the owner `[daemon.limits]` fanout bounds loaded once at daemon
    /// startup. Until then (and in tests) the coordination-contract defaults
    /// apply. Limits gate launch/selector admission only, never reads.
    pub fn set_hierarchy_limits(&mut self, limits: core::api::HierarchyLimits) -> Result<()> {
        limits.validate()?;
        self.hierarchy_limits = limits;
        Ok(())
    }

    fn ensure_writer_healthy(&self) -> Result<()> {
        if self.write_faulted.get() {
            return Err(Error::new(
                "unavailable",
                "Store writer is faulted; reopen the Store before writing or dispatching",
            ));
        }
        Ok(())
    }

    fn fault_on_persistence_error<T>(&self, result: &Result<T>) {
        if let Err(error) = result
            && error.code == "internal"
        {
            self.write_faulted.set(true);
        }
    }

    /// Whether a previous SQLite commit failure fenced this Store instance.
    pub fn is_write_faulted(&self) -> bool {
        self.write_faulted.get()
    }

    /// Admit an external launch/send/stop command. Launch allocates its stable
    /// Agent row here; generation 1 is installed only by `dispatch`.
    pub fn submit(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = self.submit_inner(principal, request, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn submit_with_workspace(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        preparation: WorkspacePreparationInput,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = self.submit_inner(principal, request, Some(preparation), now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Resolve a targetless Git fetch retry using only its caller-authored
    /// selectors and normalized timeout. Frozen remote/OID snapshots are not
    /// consulted until a genuinely new command is prepared.
    pub fn replay_git_fetch_submission(
        &self,
        principal: &str,
        request: &SubmitCommand,
        timeout_ms: i64,
    ) -> Result<Option<Admission>> {
        require_nonempty(principal, "principal")?;
        request.validate()?;
        validate_ulid(&request.id)?;
        if request.verb != "git.fetch"
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "Git fetch replay lookup requires the exact targetless command shape",
            ));
        }
        let definition =
            serde_json::from_value::<core::api::GitFetchDefinitionArgs>(request.args.clone())
                .map_err(|_| Error::new("invalid", "git.fetch definition args are malformed"))?;
        definition.validate()?;
        if encode_json(request)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "command request exceeds 1 MiB",
            ));
        }
        let semantic_args = definition.binding_args(timeout_ms)?;
        let semantic_payload = admission::command_semantic_payload(request, semantic_args)?;
        let payload_digest = canonical::digest(&semantic_payload)?;
        if let Some((old_principal, old_method, old_digest)) =
            command_binding(&self.connection, &request.id)?
        {
            if old_principal != principal
                || old_method != COMMAND_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "command id is already bound to another principal, method, or payload",
                ));
            }
            let command = load_command(&self.connection, &request.id)?.ok_or_else(|| {
                Error::new("internal", "command binding exists without its command row")
            })?;
            if command.verb != "git.fetch" || command.target.is_some() {
                return Err(Error::new(
                    "source_integrity",
                    "retained Git fetch binding has another command shape",
                ));
            }
            let linked_command = load_linked_command(&self.connection, &request.id)?;
            return Ok(Some(Admission {
                command,
                linked_command,
                events: Vec::new(),
                is_new: false,
            }));
        }
        if write_exists(&self.connection, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "command id is already bound to another write",
            ));
        }
        Ok(None)
    }

    fn submit_inner(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let limits = self.hierarchy_limits;
        let admission =
            admission::submit_tx(&tx, principal, request, preparation, now, false, limits)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(admission)
    }
}

fn has_pending_resume_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    except_id: &str,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'resume'
               AND id <> ?3
               AND state IN ('accepted', 'queued', 'dispatched', 'unknown')
         )",
        params![agent, generation, except_id],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn has_in_flight_stop_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<bool> {
    for state in ["dispatched", "unknown"] {
        for id in command_ids_in_state(tx, agent, generation, state)? {
            let command = load_command(tx, &id)?
                .ok_or_else(|| Error::new("source_integrity", "pending command disappeared"))?;
            if command.verb == "stop" {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

impl Store {
    /// Persist an idempotent non-command write. L1 uses this for daemon shutdown;
    /// command admission remains in the commands table.
    pub fn record_write(
        &mut self,
        id: &str,
        principal: &str,
        method: &str,
        semantic_payload: &Value,
        result: &Value,
        now: i64,
    ) -> Result<WriteAdmission> {
        self.ensure_writer_healthy()?;
        let outcome = self.record_write_inner(id, principal, method, semantic_payload, result, now);
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    fn record_write_inner(
        &mut self,
        id: &str,
        principal: &str,
        method: &str,
        semantic_payload: &Value,
        result: &Value,
        now: i64,
    ) -> Result<WriteAdmission> {
        require_nonempty(principal, "principal")?;
        require_nonempty(method, "method")?;
        validate_ulid(id)?;
        let digest = canonical::digest(semantic_payload)?;
        let result_json = encode_json(result)?;
        if result_json.len() > MAX_EVENT_BYTES {
            return Err(Error::new("frame_too_large", "write result exceeds 64 KiB"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "mutation id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) = write_binding(&tx, id)? {
            if old_principal != principal || old_method != method || old_digest != digest {
                return Err(Error::new(
                    "id_conflict",
                    "write id is already bound to another principal, method, or payload",
                ));
            }
            let result = decode_json(&old_result)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(WriteAdmission {
                result,
                is_new: false,
            });
        }
        validate_id_age(id, now)?;
        db!(tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, principal, method, digest, result_json, now],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(WriteAdmission {
            result: result.clone(),
            is_new: true,
        })
    }

    /// Record a dispatch decision before the caller performs native I/O. A
    /// dispatched or unknown command is never dispatched again by this method.
    pub fn dispatch(&mut self, id: &str, now: i64) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        if let Some(command) = self.get_command(id)?
            && command.verb == "stop"
            && let Some(target) = command.target.as_deref()
            && let Some(agent) = self.get_agent(target)?
            && let Some(workspace) =
                workspace::pending_workspace_for_stop(&self.connection, &agent)?
        {
            let change = self.request_workspace_cancel(
                id,
                &workspace.start_command_id,
                workspace.target_generation,
                now,
            )?;
            let value = self
                .get_command(id)?
                .ok_or_else(|| Error::new("internal", "workspace stop command disappeared"))?;
            return Ok(Change {
                value,
                events: change.events,
            });
        }
        let result = self.dispatch_inner(id, now, None);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Append the verified store to an unchanged launch target in the same
    /// transaction that commits its generation, credential and dispatch decision.
    pub fn dispatch_with_native_store_binding(
        &mut self,
        id: &str,
        now: i64,
        expected_agent: &Agent,
        binding: &NativeStoreBinding,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = self.dispatch_inner(id, now, Some((expected_agent, binding)));
        self.fault_on_persistence_error(&result);
        result
    }

    /// Settle a known command result. `unknown` is durable and does not authorize
    /// replay; later exact readback may resolve it to a terminal state.
    pub fn settle(
        &mut self,
        id: &str,
        state: CommandState,
        outcome: Option<Value>,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        if state == CommandState::Applied
            && self
                .get_command(id)?
                .is_some_and(|command| command.verb == "run")
        {
            return Err(Error::new(
                "bad_state",
                "a Run is applied only with its atomic fixed child-set commit",
            ));
        }
        let result = self.settle_inner(id, state, outcome, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<Agent>> {
        let agent_num = parse_agent_id(id)?;
        load_agent_by_num(&self.connection, agent_num)
    }

    /// Load the current-generation token for native environment installation.
    pub fn get_agent_token(&self, agent: &str, generation: i64) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        let token: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT token FROM agents WHERE id = ?1 AND generation = ?2 AND observe_only = 0",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(token.flatten())
    }

    pub fn list_agents(&self, after: Option<&str>, limit: usize) -> Result<Vec<Agent>> {
        validate_limit(limit)?;
        let after_num = after.map(parse_agent_id).transpose()?.unwrap_or(0);
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0),
                        parent, root, depth, control
                 FROM agents WHERE id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after_num, limit as i64], agent_record_from_row)
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(agent_from_record(
                &self.connection,
                row.map_err(sql_error)?,
            )?);
        }
        enforce_page_bytes(&mut agents)?;
        Ok(agents)
    }

    /// Return live Agent projections independently of historical-row volume.
    pub fn list_nonterminal_agents(&self, limit: usize) -> Result<Vec<Agent>> {
        validate_limit(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0),
                        parent, root, depth, control
                 FROM agents
                 WHERE observe_only = 0 AND state IN ('created', 'starting', 'idle', 'busy', 'waiting')
                 ORDER BY id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([limit as i64], agent_record_from_row)
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(agent_from_record(
                &self.connection,
                row.map_err(sql_error)?,
            )?);
        }
        enforce_page_bytes(&mut agents)?;
        Ok(agents)
    }

    /// Return unsettled commands without letting terminal history consume the limit.
    pub fn list_unsettled_commands(&self, limit: usize) -> Result<Vec<Command>> {
        validate_limit(limit)?;
        let mut statement = self
            .connection
            .prepare(LIST_UNSETTLED_COMMANDS_SQL)
            .map_err(sql_error)?;
        let rows = statement
            .query_map([limit as i64], command_record_from_row)
            .map_err(sql_error)?;
        let mut commands = Vec::new();
        for row in rows {
            commands.push(command_from_raw(row.map_err(sql_error)?)?.command);
        }
        enforce_page_bytes(&mut commands)?;
        Ok(commands)
    }

    /// Count all Agents without truncating at a page limit.
    pub fn agent_count(&self) -> Result<i64> {
        self.connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
            .map_err(sql_error)
    }

    pub fn get_command(&self, id: &str) -> Result<Option<Command>> {
        Ok(load_command_record(&self.connection, id)?.map(|record| record.command))
    }

    pub fn get_turn(&self, agent: &str, generation: i64, turn_id: &str) -> Result<Option<Turn>> {
        let agent_num = parse_agent_id(agent)?;
        load_turn(&self.connection, agent_num, generation, turn_id)
    }

    pub fn list_turns(&self, agent: &str, generation: i64, limit: usize) -> Result<Vec<Turn>> {
        validate_limit(limit)?;
        let agent_num = parse_agent_id(agent)?;
        if load_agent_by_num(&self.connection, agent_num)?.is_none() {
            return Err(Error::new(
                "not_found",
                format!("Agent {agent} was not found"),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT agent, generation, turn_id, state, status, final_text,
                        started_at, completed_at, usage
                 FROM turns WHERE agent = ?1 AND generation = ?2
                 ORDER BY COALESCE(started_at, completed_at, 0) DESC, turn_id DESC
                 LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![agent_num, generation, limit as i64],
                turn_record_from_row,
            )
            .map_err(sql_error)?;
        let mut turns = Vec::new();
        for row in rows {
            turns.push(turn_from_record(row.map_err(sql_error)?)?);
        }
        enforce_page_bytes(&mut turns)?;
        Ok(turns)
    }

    pub fn read_events(
        &self,
        after: i64,
        limit: usize,
        through: Option<i64>,
    ) -> Result<Vec<Event>> {
        validate_limit(limit)?;
        if after < 0 || through.is_some_and(|bound| bound < after) {
            return Err(Error::new("invalid", "invalid event sequence bounds"));
        }
        // Replay must report pruned history instead of silently omitting it.
        // Live rule cursors always stay above this floor, so the rule worker
        // never trips here; client-supplied replay cursors do.
        let retained: i64 = self
            .connection
            .query_row(
                "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("source_integrity", "rule history coverage is absent"))?;
        if after < retained {
            return Err(Error::new(
                "history_gap",
                "requested event history is no longer fully retained",
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events
                 WHERE seq > ?1 AND (?2 IS NULL OR seq <= ?2)
                 ORDER BY seq LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, through, limit as i64], event_record_from_row)
            .map_err(sql_error)?;
        let mut events = Vec::new();
        for row in rows {
            events.push(event_from_record(row.map_err(sql_error)?)?);
        }
        enforce_page_bytes(&mut events)?;
        Ok(events)
    }

    pub fn high_water(&self) -> Result<i64> {
        self.connection
            .query_row(
                "SELECT MAX(
                     COALESCE((SELECT MAX(seq) FROM events), 0),
                     COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// On daemon restart, fence every live generation as lost, retain active
    /// turns as unknown, and mark dispatched commands unknown without replay.
    pub fn recover_on_restart(&mut self, now: i64) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.recover_on_restart_inner(now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn recover_on_restart_inner(&mut self, now: i64) -> Result<Vec<Event>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        db!(tx.execute("UPDATE agents SET token = NULL WHERE token IS NOT NULL", []));
        db!(tx.execute("UPDATE agents SET state = 'unknown', active_turn = NULL, pending = '[]', updated_at = ?1 WHERE observe_only = 1", [now]));
        db!(tx.execute(
            "UPDATE source_state SET health = 'stale', updated_at = ?1 WHERE state = 'open'
             AND EXISTS(SELECT 1 FROM agents a WHERE a.id = source_state.agent AND a.observe_only = 1)", [now],
        ));
        db!(tx.execute(
            "UPDATE processes SET state = 'unknown', exit_code = NULL, updated_at = ?1
             WHERE state = 'running'",
            [now],
        ));
        db!(tx.execute(
            "UPDATE source_state SET health = 'stale', updated_at = ?1
             WHERE source IN ('os', 'git', 'workspace_process') AND state = 'open'",
            [now],
        ));
        let mut dispatched_statement = tx
            .prepare(
                "SELECT id, target, generation FROM commands
                 WHERE state = 'dispatched' ORDER BY created_at, rowid",
            )
            .map_err(sql_error)?;
        let dispatched_rows = dispatched_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut dispatched_commands = Vec::new();
        for row in dispatched_rows {
            dispatched_commands.push(row.map_err(sql_error)?);
        }
        drop(dispatched_statement);

        let mut events = Vec::new();
        recover_workspaces(&tx, now, &mut events)?;
        Self::recover_lost_native_children_tx(&tx, now, &mut events)?;
        for (command_id, target, generation) in dispatched_commands {
            let outcome = json!({"reason": "daemon_restart"});
            update_command_state(
                &tx,
                &command_id,
                CommandState::Unknown,
                Some(&outcome),
                None,
                now,
            )?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                "command.unknown",
                target,
                generation,
                &command_id,
                json!({"command_id": command_id, "reason": "daemon_restart"}),
            )?);
        }

        let mut statement = tx
            .prepare(
                "SELECT id, generation FROM agents
                 WHERE observe_only = 0 AND state IN ('starting', 'idle', 'busy', 'waiting') ORDER BY id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(row.map_err(sql_error)?);
        }
        drop(statement);

        for (agent_num, generation) in agents {
            lifecycle::transition_lost_tx(
                &tx,
                agent_num,
                generation,
                now,
                "daemon_restart",
                &mut events,
            )?;
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    /// Record the Host's proof of owned-process exit and reader drain, then
    /// release the generation if the OS source has already closed; otherwise
    /// the OS-source closure releases it later. Closing sources or marking an
    /// Agent lost is not sufficient by itself, and missing facts never fail
    /// Host settlement.
    pub fn record_owned_control_exit(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let agent_num = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "owned-exit Agent was not found"))?;
            observer::require_controlled(&current)?;
            if generation <= 0 || generation > current.generation {
                return Err(Error::new(
                    "stale_generation",
                    "owned-exit generation does not exist",
                ));
            }
            capacity::record_owned_exit_proof_tx(&tx, agent_num, generation, now)?;
            let events = capacity::release_owned_exit_event_tx(
                &tx,
                agent_num,
                generation,
                "owned_control_exit",
                now,
            )?
            .into_iter()
            .collect();
            commit_transaction(tx, &self.write_faulted).map(|()| events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn mark_lost(&mut self, agent: &str, generation: i64, now: i64) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.mark_lost_inner(agent, generation, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn mark_lost_inner(&mut self, agent: &str, generation: i64, now: i64) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
        if current.generation != generation {
            return Err(Error::new(
                "stale_generation",
                format!("Agent {agent} is at generation {}", current.generation),
            ));
        }
        let mut events = Vec::new();
        lifecycle::transition_lost_tx(
            &tx,
            agent_num,
            generation,
            now,
            "owner_marked_lost",
            &mut events,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }
}

#[derive(Clone, Copy)]
enum ProcessSourceKind {
    Agent,
    Workspace,
}

impl ProcessSourceKind {
    fn source(self) -> &'static str {
        match self {
            Self::Agent => PROCESS_SOURCE,
            Self::Workspace => WORKSPACE_PROCESS_SOURCE,
        }
    }
    fn family(self) -> &'static str {
        match self {
            Self::Agent => "process",
            Self::Workspace => "workspace_process",
        }
    }
    fn owner(self, agent: &str, generation: i64) -> String {
        match self {
            Self::Agent => format!("agent:{agent}#{generation}"),
            Self::Workspace => format!("workspace:{agent}#{generation}"),
        }
    }
}

impl Store {
    pub fn ensure_workspace_process_source(
        &mut self,
        agent: &str,
        target_generation: i64,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.ensure_process_source_inner(
            agent,
            target_generation,
            now,
            ProcessSourceKind::Workspace,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn observe_workspace_processes(
        &mut self,
        agent: &str,
        target_generation: i64,
        snapshot: &ProcessSnapshot,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        if encode_json(snapshot)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "workspace process snapshot exceeds frame bound",
            ));
        }
        let records: Vec<ProcessRecord> = snapshot
            .samples
            .iter()
            .map(|sample| sample.record.clone())
            .collect();
        // Completeness describes only this custody snapshot. It never proves
        // departure of a missing row from a different/prior runner's Job.
        let result = self.observe_processes_inner(
            agent,
            target_generation,
            &records,
            now,
            ProcessSourceKind::Workspace,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn set_workspace_process_source_status(
        &mut self,
        agent: &str,
        target_generation: i64,
        health: &str,
        closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.set_process_source_status_inner(
            agent,
            target_generation,
            health,
            closed,
            now,
            ProcessSourceKind::Workspace,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    /// Install OS process ownership before the caller spawns this generation.
    pub fn ensure_process_source(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result =
            self.ensure_process_source_inner(agent, generation, now, ProcessSourceKind::Agent);
        self.fault_on_persistence_error(&result);
        result
    }

    fn ensure_process_source_inner(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
        scope: ProcessSourceKind,
    ) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "process source Agent was not found"))?;
        validate_process_generation(&tx, &current, generation, scope, true)?;
        let owner: Option<(String, String)> = tx
            .query_row(
                "SELECT source, mechanism FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 AND family = ?3",
                params![agent_num, generation, scope.family()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let source = load_source_state(&tx, agent_num, generation, scope.source())?;
        let mut events = Vec::new();
        match (owner, source) {
            (Some((source, mechanism)), Some(_))
                if source == scope.source() && mechanism == "os" => {}
            (None, None)
                if matches!(scope, ProcessSourceKind::Workspace) || !current.is_terminal() =>
            {
                db!(tx.execute(
                    "INSERT INTO source_owners(agent, generation, family, source, mechanism)
                     VALUES (?1, ?2, ?3, ?4, 'os')",
                    params![agent_num, generation, scope.family(), scope.source()],
                ));
                db!(tx.execute(
                    "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
                     VALUES (?1, ?2, ?3, 'open', 'ok', ?4)",
                    params![agent_num, generation, scope.source(), now],
                ));
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "source.opened",
                    "source",
                    Some(agent_num),
                    Some(generation),
                    json!({"source": scope.source(), "families": [scope.family()], "mechanism": "os", "owner": scope.owner(agent, generation)}),
                )?);
            }
            _ => {
                return Err(Error::new(
                    "source_integrity",
                    "process source ownership is missing, conflicting, or terminal",
                ));
            }
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    /// Update OS health independently of lifecycle, and close only after exact drain.
    pub fn set_process_source_status(
        &mut self,
        agent: &str,
        generation: i64,
        health: &str,
        closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.set_process_source_status_inner(
            agent,
            generation,
            health,
            closed,
            now,
            ProcessSourceKind::Agent,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn set_process_source_status_inner(
        &mut self,
        agent: &str,
        generation: i64,
        health: &str,
        closed: bool,
        now: i64,
        scope: ProcessSourceKind,
    ) -> Result<Vec<Event>> {
        if !matches!(health, "ok" | "slow" | "stale" | "error") {
            return Err(Error::new(
                "invalid",
                "process source health must be ok, slow, stale, or error",
            ));
        }
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "process source Agent was not found"))?;
        validate_process_generation(&tx, &current, generation, scope, false)?;
        let source = require_process_source(&tx, agent_num, generation, scope)?;
        if closed {
            if matches!(scope, ProcessSourceKind::Workspace) {
                let workspace = workspace::load_workspace(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "managed preparation was not found"))?;
                if workspace.target_generation == generation
                    && workspace.phase != WorkspacePhase::Removed
                {
                    return Err(Error::new(
                        "bad_state",
                        "workspace process source remains open for later preparation or removal work",
                    ));
                }
            }
            let retained: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM processes
                 WHERE owner = ?1 AND state IN ('running', 'unknown'))",
                    [scope.owner(agent, generation)],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if retained {
                return Err(Error::new(
                    "bad_state",
                    "OS source cannot close while a process is running or unknown",
                ));
            }
        }
        let state = if closed {
            "closed"
        } else {
            source.state.as_str()
        };
        let mut events = Vec::new();
        if state != source.state || health != source.health {
            db!(tx.execute(
                "UPDATE source_state SET state = ?1, health = ?2, updated_at = ?3
                 WHERE agent = ?4 AND generation = ?5 AND source = ?6",
                params![state, health, now, agent_num, generation, scope.source()],
            ));
            events.push(insert_daemon_event(
                &tx,
                now,
                if state != source.state {
                    "source.closed"
                } else {
                    "source.health"
                },
                "source",
                Some(agent_num),
                Some(generation),
                json!({"source": scope.source(), "state": state, "health": health}),
            )?);
        }
        if closed && matches!(scope, ProcessSourceKind::Agent) {
            events.extend(capacity::release_owned_exit_event_tx(
                &tx,
                agent_num,
                generation,
                "owned_exit_os_closed",
                now,
            )?);
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    /// Accept only host-authoritative OS observations, including historical exits.
    /// Missing records are not evidence of departure; this never reduces an Agent.
    pub fn observe_processes(
        &mut self,
        agent: &str,
        generation: i64,
        records: &[ProcessRecord],
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result =
            self.observe_processes_inner(agent, generation, records, now, ProcessSourceKind::Agent);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn list_processes(
        &self,
        owner: &str,
        after: Option<&ProcessIdentity>,
        limit: usize,
    ) -> Result<Vec<ProcessRecord>> {
        require_nonempty(owner, "process owner")?;
        validate_limit(limit)?;
        let cursor = after
            .map(|identity| {
                validate_process_identity(identity).map(|start_time| (identity, start_time))
            })
            .transpose()?;
        let mut statement = self
            .connection
            .prepare(match cursor {
                None => LIST_PROCESSES_FIRST_PAGE_SQL,
                Some(_) => LIST_PROCESSES_CURSOR_PAGE_SQL,
            })
            .map_err(sql_error)?;
        let rows = match cursor {
            None => statement
                .query_map(params![owner, limit as i64], process_record_from_row)
                .map_err(sql_error)?,
            Some((identity, start_time)) => statement
                .query_map(
                    params![
                        owner,
                        identity.host_boot_id.as_str(),
                        identity.pid,
                        start_time,
                        limit as i64
                    ],
                    process_record_from_row,
                )
                .map_err(sql_error)?,
        };
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(sql_error)?);
        }
        enforce_page_bytes(&mut records)?;
        Ok(records)
    }

    /// Native model facts and validated file-change causes count at their original timestamps.
    /// Ownership was verified on commit; later handoff cannot erase those facts.
    pub fn last_model_progress(&self, agent: &str, generation: i64) -> Result<Option<i64>> {
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT MAX(e.ts) FROM events e
                 WHERE e.agent = ?1 AND e.generation = ?2
                   AND EXISTS(SELECT 1 FROM agents a WHERE a.id = e.agent)
                   AND ((e.source NOT IN ('os', 'daemon', 'git', 'workspace_process') AND e.native_id IS NOT NULL
                   AND e.kind IN ('turn.started', 'turn.completed', 'message.completed',
                                  'tool.started', 'tool.completed',
                                  'request.opened', 'request.resolved', 'request.closed'))
                     OR (e.source = 'git' AND e.kind = 'file.changed'))",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }
}

impl Store {
    pub fn ensure_git_source(
        &mut self,
        agent: &str,
        generation: i64,
        workspace: &str,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = current_git_agent(&tx, agent, generation, Some(workspace))?;
            let agent_num = parse_agent_id(agent)?;
            let mut statement = tx
                .prepare(
                    "SELECT source, mechanism FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 AND family IN ('git', 'file')",
                )
                .map_err(sql_error)?;
            let rows = statement
                .query_map(params![agent_num, generation], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(sql_error)?;
            let owners = rows
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?;
            drop(statement);
            let source = load_source_state(&tx, agent_num, generation, GIT_SOURCE)?;
            let mut events = Vec::new();
            if owners.is_empty() && source.is_none() && !current.is_terminal() {
                for family in ["git", "file"] {
                    db!(tx.execute(
                        "INSERT INTO source_owners(agent, generation, family, source, mechanism)
                         VALUES (?1, ?2, ?3, 'git', 'git')",
                        params![agent_num, generation, family],
                    ));
                }
                db!(tx.execute(
                    "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
                     VALUES (?1, ?2, 'git', 'open', 'ok', ?3)",
                    params![agent_num, generation, now],
                ));
                events.push(insert_daemon_event(
                    &tx, now, "source.opened", "source", Some(agent_num), Some(generation),
                    json!({"source": GIT_SOURCE, "families": ["git", "file"], "workspace": workspace}),
                )?);
            } else if owners.len() != 2
                || source.is_none()
                || owners
                    .iter()
                    .any(|(source, mechanism)| source != GIT_SOURCE || mechanism != "git")
            {
                return Err(Error::new(
                    "source_integrity",
                    "Git source ownership is missing, conflicting, or terminal",
                ));
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn record_git_observation(
        &mut self,
        agent: &str,
        generation: i64,
        snapshot: &GitSnapshot,
        hints: &[FileChangeHint],
        through: u64,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result =
            self.record_git_observation_inner(agent, generation, snapshot, hints, through, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn git_baseline(&self, workspace: &str) -> Result<Option<GitBaseline>> {
        require_nonempty(workspace, "Git workspace")?;
        load_git_baseline(&self.connection, workspace)
    }

    pub fn git_cursor(&self, agent: &str, generation: i64) -> Result<u64> {
        retained_git_workspace(&self.connection, agent, generation)?;
        let source = require_git_source(&self.connection, parse_agent_id(agent)?, generation)?;
        git_source_cursor(&source)
    }

    pub fn git_hint_page(
        &self,
        agent: &str,
        generation: i64,
        after: u64,
        limit: usize,
    ) -> Result<GitHintPage> {
        retained_git_workspace(&self.connection, agent, generation)?;
        require_git_source(&self.connection, parse_agent_id(agent)?, generation)?;
        validate_limit(limit)?;
        scan_git_hints(
            &self.connection,
            agent,
            generation,
            after,
            self.high_water()?,
            limit,
        )
    }

    /// Cleanup candidates include retained provenance from every Agent generation.
    pub fn git_edit_paths(&self, workspace: &str, limit: usize) -> Result<(Vec<String>, bool)> {
        require_nonempty(workspace, "Git workspace")?;
        validate_limit(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT path FROM agent_edits WHERE workspace = ?1
             ORDER BY path LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![workspace, limit as i64 + 1], |row| row.get(0))
            .map_err(sql_error)?;
        let mut paths = rows
            .collect::<rusqlite::Result<Vec<String>>>()
            .map_err(sql_error)?;
        let complete = paths.len() <= limit;
        paths.truncate(limit);
        Ok((paths, complete))
    }

    pub fn list_agent_edits(
        &self,
        agent: &str,
        generation: i64,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AgentEdit>> {
        let agent_num = parse_agent_id(agent)?;
        validate_limit(limit)?;
        if let Some(path) = after_path {
            validate_git_path(path)?;
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT workspace, path, updated_at FROM agent_edits
             WHERE agent = ?1 AND generation = ?2 AND (?3 IS NULL OR path > ?3)
             ORDER BY path LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![agent_num, generation, after_path, limit as i64],
                |row| {
                    Ok(AgentEdit {
                        agent: agent.to_owned(),
                        generation,
                        workspace: row.get(0)?,
                        path: row.get(1)?,
                        updated_at: row.get(2)?,
                    })
                },
            )
            .map_err(sql_error)?;
        let mut edits = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        enforce_page_bytes(&mut edits)?;
        Ok(edits)
    }

    pub fn set_git_source_status(
        &mut self,
        agent: &str,
        generation: i64,
        health: &str,
        closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            if !matches!(health, "ok" | "slow" | "stale" | "error") {
                return Err(Error::new(
                    "invalid",
                    "Git source health must be ok, slow, stale, or error",
                ));
            }
            let agent_num = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "Git source Agent was not found"))?;
            if generation <= 0 || generation > current.generation {
                return Err(Error::new(
                    "stale_generation",
                    "Git source generation is not committed",
                ));
            }
            let source = require_git_source(&tx, agent_num, generation)?;
            if closed && pending_git_hints(&tx, agent_num, generation, git_source_cursor(&source)?)?
            {
                return Err(Error::new(
                    "bad_state",
                    "Git source still has unconsumed native file-change hints",
                ));
            }
            let state = if closed {
                "closed"
            } else {
                source.state.as_str()
            };
            let mut events = Vec::new();
            if state != source.state || health != source.health {
                db!(tx.execute(
                    "UPDATE source_state SET state = ?1, health = ?2, updated_at = ?3
                     WHERE agent = ?4 AND generation = ?5 AND source = 'git'",
                    params![state, health, now, agent_num, generation],
                ));
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    if state != source.state {
                        "source.closed"
                    } else {
                        "source.health"
                    },
                    "source",
                    Some(agent_num),
                    Some(generation),
                    json!({"source": GIT_SOURCE, "state": state, "health": health}),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn retained_git_workspace(connection: &Connection, agent: &str, generation: i64) -> Result<String> {
    let agent_num = parse_agent_id(agent)?;
    let current = load_agent_by_num(connection, agent_num)?
        .ok_or_else(|| Error::new("not_found", "Git observation Agent was not found"))?;
    if generation <= 0 || generation > current.generation {
        return Err(Error::new(
            "stale_generation",
            "Git generation is not committed",
        ));
    }
    let opened: Option<String> = connection.query_row(
        "SELECT data FROM events WHERE agent = ?1 AND generation = ?2 AND kind = 'source.opened'
         AND json_extract(data, '$.source') = 'git' ORDER BY seq DESC LIMIT 1",
        params![agent_num, generation], |row| row.get(0),
    ).optional().map_err(sql_error)?;
    if let Some(opened) = opened {
        return decode_json(&opened)?
            .get("workspace")
            .and_then(Value::as_str)
            .filter(|workspace| !workspace.trim().is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::new("source_integrity", "Git source has no retained workspace"));
    }
    if generation == current.generation {
        return current
            .workspace
            .ok_or_else(|| Error::new("invalid", "Git workspace is missing"));
    }
    Err(Error::new(
        "source_integrity",
        "historical Git workspace evidence is unavailable",
    ))
}

fn git_interval_gap_covers(
    connection: &Connection,
    agent: i64,
    generation: i64,
    source: &SourceStateRecord,
    cursor: u64,
    through: u64,
    family: &str,
) -> Result<bool> {
    if source.gap_from.as_deref() != Some(cursor.to_string().as_str())
        || source.gap_to.as_deref() != Some(through.to_string().as_str())
    {
        return Ok(false);
    }
    let Some(seq) = source.gap_seq else {
        return Ok(false);
    };
    let data: Option<String> = connection.query_row(
        "SELECT data FROM events WHERE seq = ?1 AND agent = ?2 AND generation = ?3 AND kind = 'source.gap'",
        params![seq, agent, generation], |row| row.get(0),
    ).optional().map_err(sql_error)?;
    let Some(data) = data else {
        return Ok(false);
    };
    let data = decode_json(&data)?;
    Ok(
        data.get("source").and_then(Value::as_str) == Some(GIT_SOURCE)
            && data.get("gap_id").and_then(Value::as_str) == source.gap_id.as_deref()
            && data
                .get("families")
                .and_then(Value::as_array)
                .is_some_and(|families| {
                    families.iter().any(|value| value.as_str() == Some(family))
                })
            && source.gap_digest.as_deref() == Some(canonical::digest(&data)?.as_slice()),
    )
}

fn pending_git_hints(
    connection: &Connection,
    agent: i64,
    generation: i64,
    cursor: u64,
) -> Result<bool> {
    // Persisted native hints remain authoritative after their family changes owner.
    connection
        .query_row(
            "SELECT EXISTS(
            SELECT 1 FROM events e
            WHERE e.agent = ?1 AND e.generation = ?2 AND e.seq > ?3 AND e.family = 'tool'
              AND e.kind = 'tool.completed' AND e.native_id IS NOT NULL
              AND e.source NOT IN ('git', 'os', 'daemon')
              AND json_extract(e.data, '$.tool') = 'fileChange'
              AND json_extract(e.data, '$.status') = 'completed'
              AND (json_array_length(e.data, '$.changed_paths') > 0
                   OR json_extract(e.data, '$.changed_paths_truncated') = 1
                   OR json_extract(e.data, '$.redacted') = 1))",
            params![agent, generation, git_sequence(cursor)?],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn current_git_agent(
    connection: &Connection,
    agent: &str,
    generation: i64,
    workspace: Option<&str>,
) -> Result<Agent> {
    let current = load_agent_by_num(connection, parse_agent_id(agent)?)?
        .ok_or_else(|| Error::new("not_found", "Git observation Agent was not found"))?;
    observer::require_controlled(&current)?;
    if generation <= 0 || generation != current.generation {
        return Err(Error::new(
            "stale_generation",
            "Git observation generation is not current",
        ));
    }
    let admitted = current
        .workspace
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "Git observation requires an admitted workspace"))?;
    require_nonempty(admitted, "Git workspace")?;
    if workspace.is_some_and(|value| value != admitted) {
        return Err(Error::new(
            "source_integrity",
            "Git workspace does not match the Agent",
        ));
    }
    Ok(current)
}

fn require_git_source(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<SourceStateRecord> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM source_owners WHERE agent = ?1 AND generation = ?2
         AND family IN ('git', 'file') AND source = 'git' AND mechanism = 'git'",
            params![agent, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if count != 2 {
        return Err(Error::new(
            "source_integrity",
            "Git/file ownership was not committed before observation",
        ));
    }
    load_source_state(connection, agent, generation, GIT_SOURCE)?
        .ok_or_else(|| Error::new("source_integrity", "Git source state is missing"))
}

fn git_source_cursor(source: &SourceStateRecord) -> Result<u64> {
    source
        .cursor
        .as_deref()
        .unwrap_or("0")
        .parse::<u64>()
        .map_err(|_| Error::new("internal", "invalid stored Git hint cursor"))
}

fn load_git_baseline(connection: &Connection, workspace: &str) -> Result<Option<GitBaseline>> {
    connection
        .query_row(
            "SELECT head, updated_at FROM git_workspaces WHERE workspace = ?1",
            [workspace],
            |row| {
                Ok(GitBaseline {
                    head: row.get(0)?,
                    updated_at: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

fn git_sequence(seq: u64) -> Result<i64> {
    i64::try_from(seq)
        .map_err(|_| Error::new("invalid", "Git cursor exceeds signed 64-bit event sequence"))
}

fn is_native_file_hint(event: &Event) -> bool {
    event.kind == "tool.completed"
        && event.native_id.is_some()
        && event.data.get("tool").and_then(Value::as_str) == Some("fileChange")
        && event.data.get("status").and_then(Value::as_str) == Some("completed")
        && (event
            .data
            .get("changed_paths")
            .and_then(Value::as_array)
            .is_some_and(|paths| !paths.is_empty())
            || event
                .data
                .get("changed_paths_truncated")
                .and_then(Value::as_bool)
                == Some(true)
            || event.data.get("redacted").and_then(Value::as_bool) == Some(true))
}

fn scan_git_hints(
    connection: &Connection,
    agent: &str,
    generation: i64,
    after: u64,
    through: i64,
    limit: usize,
) -> Result<GitHintPage> {
    let after = git_sequence(after)?;
    if after > through {
        return Err(Error::new(
            "invalid",
            "Git cursor exceeds committed high-water",
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
         FROM events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![after, through, limit as i64], event_record_from_row)
        .map_err(sql_error)?;
    let mut scanned = Vec::new();
    for row in rows {
        scanned.push(event_from_record(row.map_err(sql_error)?)?);
    }
    enforce_page_bytes(&mut scanned)?;
    let boundary = scanned.last().map_or(through, |event| event.seq);
    scanned.retain(|event| {
        event.agent.as_deref() == Some(agent)
            && event.generation == Some(generation)
            && event.source != GIT_SOURCE
            && event.source != PROCESS_SOURCE
            && event.source != "daemon"
            && is_native_file_hint(event)
    });
    Ok(GitHintPage {
        events: scanned,
        through: boundary as u64,
    })
}

fn load_native_git_hint(
    connection: &Connection,
    agent: i64,
    generation: i64,
    seq: u64,
) -> Result<Event> {
    let record = connection
        .query_row(
            "SELECT e.seq, e.ts, e.kind, e.agent, e.generation, e.source, e.native_id, e.data, e.subject, e.cause
         FROM events e
         WHERE e.seq = ?1 AND e.agent = ?2 AND e.generation = ?3 AND e.family = 'tool'
           AND e.source NOT IN ('git', 'os', 'daemon') AND e.native_id IS NOT NULL",
            params![git_sequence(seq)?, agent, generation],
            event_record_from_row,
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "file hint has no exact authoritative native event",
            )
        })?;
    let event = event_from_record(record)?;
    if !is_native_file_hint(&event)
        || event.data.get("redacted").and_then(Value::as_bool) == Some(true)
    {
        return Err(Error::new(
            "source_integrity",
            "file hint is not a completed native file change",
        ));
    }
    Ok(event)
}

fn validate_git_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.contains(':')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(Error::new(
            "invalid",
            "Git path must be normalized and workspace-relative",
        ));
    }
    Ok(())
}

fn validate_git_oid(oid: &str) -> Result<()> {
    if !matches!(oid.len(), 40 | 64) || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::new(
            "invalid",
            "Git object id must be a full SHA-1 or SHA-256 hex id",
        ));
    }
    Ok(())
}

fn validate_process_identity(identity: &ProcessIdentity) -> Result<i64> {
    require_nonempty(&identity.host_boot_id, "host_boot_id")?;
    if identity.pid == 0 {
        return Err(Error::new("invalid", "process PID must be positive"));
    }
    i64::try_from(identity.start_time).map_err(|_| {
        Error::new(
            "invalid",
            "process start_time exceeds a signed 64-bit integer",
        )
    })
}

fn process_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProcessRecord> {
    let state = serde_json::from_value(Value::String(row.get(5)?)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(ProcessRecord {
        identity: ProcessIdentity {
            host_boot_id: row.get(0)?,
            pid: row.get(1)?,
            start_time: u64::try_from(row.get::<_, i64>(2)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?,
        },
        owner: row.get(3)?,
        role: row.get(4)?,
        state,
        exit_code: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn require_process_source(
    connection: &Connection,
    agent: i64,
    generation: i64,
    scope: ProcessSourceKind,
) -> Result<SourceStateRecord> {
    let owned: bool = connection
        .query_row(
            "SELECT EXISTS(
             SELECT 1 FROM source_owners o JOIN source_state s
               ON s.agent = o.agent AND s.generation = o.generation AND s.source = o.source
             WHERE o.agent = ?1 AND o.generation = ?2 AND o.family = ?3
               AND o.source = ?4 AND o.mechanism = 'os'
         )",
            params![agent, generation, scope.family(), scope.source()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if !owned {
        return Err(Error::new(
            "source_integrity",
            "OS process ownership was not committed before observation",
        ));
    }
    load_source_state(connection, agent, generation, scope.source())?
        .ok_or_else(|| Error::new("source_integrity", "OS source state is missing"))
}

fn close_finished_workspace_process_source(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let workspace = workspace::load_workspace(tx, agent)?
        .ok_or_else(|| Error::new("not_found", "workspace process lifetime has no preparation"))?;
    if workspace.target_generation < generation
        || workspace.target_generation == generation && workspace.phase != WorkspacePhase::Removed
    {
        return Ok(());
    }
    let Some(source) = load_source_state(tx, agent, generation, WORKSPACE_PROCESS_SOURCE)? else {
        return Ok(());
    };
    if source.state != "open" {
        return Ok(());
    }
    let retained: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM processes WHERE owner = ?1 AND state IN ('running','unknown'))",
        [format!("workspace:{}#{generation}", format_agent_id(agent))], |row| row.get(0),
    ).map_err(sql_error)?;
    let state = if retained { "open" } else { "closed" };
    let health = if retained {
        "stale"
    } else {
        source.health.as_str()
    };
    if state == source.state && health == source.health {
        return Ok(());
    }
    db!(tx.execute(
        "UPDATE source_state SET state = ?1, health = ?2, updated_at = ?3
         WHERE agent = ?4 AND generation = ?5 AND source = 'workspace_process' AND state = 'open'",
        params![state, health, now, agent, generation],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        if retained {
            "source.health"
        } else {
            "source.closed"
        },
        "source",
        Some(agent),
        Some(generation),
        json!({"source": WORKSPACE_PROCESS_SOURCE, "state": state, "health": health}),
    )?);
    Ok(())
}

fn validate_process_generation(
    connection: &Connection,
    agent: &Agent,
    generation: i64,
    scope: ProcessSourceKind,
    installing: bool,
) -> Result<()> {
    if agent.observe_only && installing {
        observer::require_reader_source(connection, agent, generation)?;
    }
    if generation <= 0 {
        return Err(Error::new(
            "stale_generation",
            "process generation must be positive",
        ));
    }
    match scope {
        ProcessSourceKind::Agent
            if generation > agent.generation || installing && generation != agent.generation =>
        {
            return Err(Error::new(
                "stale_generation",
                "process generation is not the admitted Agent generation",
            ));
        }
        ProcessSourceKind::Workspace => {
            let workspace = workspace::load_workspace(connection, parse_agent_id(&agent.id)?)?
                .ok_or_else(|| {
                    Error::new(
                        "not_found",
                        "workspace process source requires a managed preparation",
                    )
                })?;
            if installing
                && (workspace.target_generation != generation
                    || workspace.phase == WorkspacePhase::Removed)
                || !installing
                    && workspace.target_generation != generation
                    && generation > agent.generation
            {
                return Err(Error::new(
                    "stale_generation",
                    "process generation is not the exact managed preparation target",
                ));
            }
            // Late exits/status for a retired target also require its already
            // committed workspace_process binding; this never creates a past owner.
        }
        ProcessSourceKind::Agent => {}
    }
    Ok(())
}

struct AgentRecord {
    id: i64,
    generation: i64,
    name: Option<String>,
    project: String,
    harness: String,
    profile: String,
    profile_snapshot: String,
    native_scope: Option<String>,
    native_ref: Option<String>,
    workspace: Option<String>,
    effective: Option<String>,
    pending: String,
    state: String,
    active_turn: Option<String>,
    created_at: i64,
    updated_at: i64,
    origin: String,
    observe_only: bool,
    own_identity: bool,
    parent: Option<i64>,
    root: Option<i64>,
    depth: i64,
    control: String,
}

struct CommandRecord {
    command: Command,
    state: CommandState,
    target: Option<i64>,
    expected_turn_id: Option<String>,
}

struct TurnRecord {
    agent: i64,
    generation: i64,
    turn_id: String,
    state: String,
    status: Option<String>,
    final_text: Option<String>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    usage: Option<String>,
}

struct SourceStateRecord {
    cursor: Option<String>,
    last_store_seq: Option<i64>,
    last_batch_id: Option<String>,
    last_batch_digest: Option<Vec<u8>>,
    last_batch_first_seq: Option<i64>,
    last_batch_last_seq: Option<i64>,
    state: String,
    health: String,
    gap_id: Option<String>,
    gap_digest: Option<Vec<u8>>,
    gap_from: Option<String>,
    gap_to: Option<String>,
    gap_seq: Option<i64>,
    last_handoff_digest: Option<Vec<u8>>,
    last_handoff: Option<String>,
}

struct CommandInsert<'a> {
    id: &'a str,
    principal: &'a str,
    method: &'a str,
    payload_digest: &'a [u8],
    target: Option<i64>,
    selector: Option<&'a str>,
    expected_generation: Option<i64>,
    generation: Option<i64>,
    expected_turn_id: Option<&'a str>,
    parent_command: Option<&'a str>,
    link_kind: Option<&'a str>,
    ordinal: Option<i64>,
    turn_id: Option<&'a str>,
    request_id: Option<&'a str>,
    probe: bool,
    verb: &'a str,
    args: &'a Value,
    state: CommandState,
    outcome: Option<&'a Value>,
    created_at: i64,
    updated_at: i64,
}

impl<'a> CommandInsert<'a> {
    fn admitted(
        principal: &'a str,
        request: &'a SubmitCommand,
        payload_digest: &'a [u8],
        target: (i64, i64),
        now: i64,
    ) -> Self {
        Self {
            id: &request.id,
            principal,
            method: COMMAND_METHOD,
            payload_digest,
            target: Some(target.0),
            selector: None,
            expected_generation: request.expected_generation,
            generation: Some(target.1),
            expected_turn_id: request.expected_turn_id.as_deref(),
            parent_command: None,
            link_kind: None,
            ordinal: None,
            turn_id: None,
            request_id: None,
            probe: request.probe,
            verb: &request.verb,
            args: &request.args,
            state: CommandState::Accepted,
            outcome: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn targetless_admitted(
        principal: &'a str,
        request: &'a SubmitCommand,
        payload_digest: &'a [u8],
        now: i64,
    ) -> Self {
        Self {
            id: &request.id,
            principal,
            method: COMMAND_METHOD,
            payload_digest,
            target: None,
            selector: None,
            expected_generation: None,
            generation: None,
            expected_turn_id: None,
            parent_command: None,
            link_kind: None,
            ordinal: None,
            turn_id: None,
            request_id: None,
            probe: false,
            verb: &request.verb,
            args: &request.args,
            state: CommandState::Accepted,
            outcome: None,
            created_at: now,
            updated_at: now,
        }
    }
}

struct EventRecord {
    seq: i64,
    ts: i64,
    kind: String,
    agent: Option<i64>,
    generation: Option<i64>,
    source: String,
    native_id: Option<String>,
    data: String,
    subject: Option<String>,
    cause: Option<String>,
}

struct EventInsert<'a> {
    ts: i64,
    kind: &'a str,
    family: Option<&'a str>,
    agent: Option<i64>,
    generation: Option<i64>,
    source: &'a str,
    native_id: Option<&'a str>,
    cursor: Option<&'a str>,
    payload_digest: Option<&'a [u8]>,
    subject: Option<EventSubject>,
    cause: Option<core::EventCause>,
    data: Value,
}

struct DaemonEventInput<'a> {
    ts: i64,
    kind: &'a str,
    family: &'a str,
    agent: Option<i64>,
    generation: Option<i64>,
    data: Value,
    cause: Option<core::EventCause>,
}

impl<'a> DaemonEventInput<'a> {
    fn new(
        ts: i64,
        kind: &'a str,
        family: &'a str,
        agent: Option<i64>,
        generation: Option<i64>,
        data: Value,
    ) -> Self {
        Self {
            ts,
            kind,
            family,
            agent,
            generation,
            data,
            cause: None,
        }
    }

    fn with_cause(mut self, cause: core::EventCause) -> Self {
        self.cause = Some(cause);
        self
    }
}

struct NativeEventInsert<'a> {
    ts: i64,
    native_event: &'a NativeEvent,
    family: &'a str,
    agent: i64,
    generation: i64,
    source: &'a str,
    cursor: Option<&'a str>,
    payload_digest: &'a [u8],
    subject: Option<EventSubject>,
    cause: Option<core::EventCause>,
}

struct TurnInsert<'a> {
    agent: i64,
    generation: i64,
    turn_id: &'a str,
    state: TurnState,
    status: Option<&'a str>,
    final_text: Option<&'a str>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    usage: Option<&'a str>,
}

fn agent_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRecord> {
    Ok(AgentRecord {
        id: row.get(0)?,
        generation: row.get(1)?,
        name: row.get(2)?,
        project: row.get(3)?,
        harness: row.get(4)?,
        profile: row.get(5)?,
        profile_snapshot: row.get(6)?,
        native_scope: row.get(7)?,
        native_ref: row.get(8)?,
        workspace: row.get(9)?,
        effective: row.get(10)?,
        pending: row.get(11)?,
        state: row.get(12)?,
        active_turn: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
        origin: row.get(16)?,
        observe_only: row.get(17)?,
        own_identity: row.get(18)?,
        parent: row.get(19)?,
        root: row.get(20)?,
        depth: row.get(21)?,
        control: row.get(22)?,
    })
}

fn agent_from_record(connection: &Connection, record: AgentRecord) -> Result<Agent> {
    let pending = decode_pending_requests(&record.pending)?;
    let preparation = match workspace::load_workspace(connection, record.id)? {
        Some(workspace) => Some(workspace),
        None => record
            .workspace
            .as_deref()
            .map(|path| workspace::load_workspace_by_path(connection, path))
            .transpose()?
            .flatten(),
    };
    Ok(Agent {
        id: format_agent_id(record.id),
        generation: record.generation,
        origin: parse_enum(&record.origin, "Agent origin")?,
        observe_only: record.observe_only,
        own_identity: record.own_identity,
        parent: record.parent.map(format_agent_id),
        // Production launch/attach always write root; a NULL root only arises
        // from raw test rows without hierarchy metadata and reads self-rooted.
        root: format_agent_id(record.root.unwrap_or(record.id)),
        depth: record.depth,
        control: AgentControl::parse(&record.control)?,
        name: record.name,
        project: record.project,
        harness: record.harness,
        profile: record.profile,
        profile_snapshot: decode_json(&record.profile_snapshot)?,
        native_scope: record.native_scope,
        native_ref: record.native_ref,
        workspace: record.workspace,
        workspace_phase: preparation.as_ref().map(|workspace| workspace.phase),
        branch: preparation
            .as_ref()
            .map(|workspace| workspace.requested_branch.clone()),
        base_ref: preparation
            .as_ref()
            .and_then(|workspace| workspace.base_ref.clone()),
        base_oid: preparation
            .as_ref()
            .map(|workspace| workspace.base_oid.clone()),
        effective: record
            .effective
            .map(|value| decode_json(&value))
            .transpose()?,
        state: parse_enum(&record.state, "Agent state")?,
        active_turn: record.active_turn,
        pending,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn load_agent_by_num(connection: &Connection, id: i64) -> Result<Option<Agent>> {
    let record = connection
        .query_row(
            "SELECT id, generation, name, project, harness, profile,
                    profile_snapshot, native_scope, native_ref, workspace, effective,
                    pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0),
                    parent, root, depth, control
             FROM agents WHERE id = ?1",
            [id],
            agent_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    record
        .map(|record| agent_from_record(connection, record))
        .transpose()
}

fn command_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CommandRecordRaw> {
    Ok(CommandRecordRaw {
        id: row.get(0)?,
        principal: row.get(1)?,
        method: row.get(2)?,
        target: row.get(3)?,
        expected_generation: row.get(4)?,
        generation: row.get(5)?,
        expected_turn_id: row.get(6)?,
        turn_id: row.get(7)?,
        request_id: row.get(8)?,
        verb: row.get(9)?,
        args: row.get(10)?,
        state: row.get(11)?,
        outcome: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
        probe: row.get(15)?,
        selector: row.get(16)?,
    })
}

struct CommandRecordRaw {
    id: String,
    principal: String,
    method: String,
    target: Option<i64>,
    expected_generation: Option<i64>,
    generation: Option<i64>,
    expected_turn_id: Option<String>,
    turn_id: Option<String>,
    request_id: Option<String>,
    verb: String,
    args: String,
    state: String,
    outcome: Option<String>,
    created_at: i64,
    updated_at: i64,
    probe: bool,
    selector: Option<String>,
}

fn command_from_raw(raw: CommandRecordRaw) -> Result<CommandRecord> {
    let target = raw.target;
    let state = parse_enum(&raw.state, "command state")?;
    Ok(CommandRecord {
        command: Command {
            id: raw.id,
            principal: raw.principal,
            method: raw.method,
            verb: raw.verb,
            target: target.map(format_agent_id),
            selector: raw.selector,
            expected_generation: raw.expected_generation,
            generation: raw.generation,
            args: decode_json(&raw.args)?,
            state,
            turn_id: raw.turn_id,
            request_id: raw.request_id,
            outcome: raw.outcome.as_deref().map(decode_json).transpose()?,
            created_at: raw.created_at,
            updated_at: raw.updated_at,
            probe: raw.probe,
        },
        state,
        target,
        expected_turn_id: raw.expected_turn_id,
    })
}

fn load_command_record(connection: &Connection, id: &str) -> Result<Option<CommandRecord>> {
    let raw = connection
        .query_row(
            "SELECT id, principal, method, target, expected_generation,
                    generation, expected_turn_id, turn_id, request_id, verb, args,
                    state, outcome, created_at, updated_at, probe, selector
             FROM commands WHERE id = ?1",
            [id],
            command_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    raw.map(command_from_raw).transpose()
}

fn load_command(connection: &Connection, id: &str) -> Result<Option<Command>> {
    Ok(load_command_record(connection, id)?.map(|record| record.command))
}

fn command_binding(connection: &Connection, id: &str) -> Result<Option<(String, String, Vec<u8>)>> {
    connection
        .query_row(
            "SELECT principal, method, payload_digest FROM commands WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)
}

fn repo_git_write_blocked_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    common_dir: &str,
) -> Result<bool> {
    let current_rowid: i64 = tx
        .query_row(
            "SELECT rowid FROM commands WHERE id = ?1",
            [command_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let fetch_pending = tx
        .query_row(
            "SELECT EXISTS(
             SELECT 1
             FROM commands prior
             WHERE prior.verb = 'git.fetch'
               AND prior.id <> ?1
               AND prior.state IN ('accepted','queued','dispatched','unknown')
               AND json_extract(prior.args, '$.common_dir') = ?2
               AND (prior.state IN ('dispatched','unknown') OR prior.rowid < ?3)
         )",
            params![command_id, common_dir, current_rowid],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if fetch_pending {
        return Ok(true);
    }
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM workspaces workspace
             JOIN commands prior ON (
                 (prior.verb = 'launch' AND prior.id = workspace.start_command_id)
                 OR (prior.verb = 'worktree.remove' AND prior.target = workspace.agent)
             )
             WHERE prior.id <> ?1
               AND workspace.repo_common_dir = ?2
               AND (prior.verb <> 'worktree.remove'
                    OR json_extract(prior.args, '$.target_generation') = workspace.target_generation)
               AND (
                   (json_extract(workspace.outcome, '$.native_git_write_dispatched') = 1
                    AND workspace.phase IN ('creating','removing','unknown')
                    AND ((prior.verb = 'launch'
                          AND json_extract(workspace.outcome, '$.stage') = 'creation'
                          AND NOT (COALESCE(json_extract(workspace.outcome, '$.reuse'), 0) = 1
                                   AND json_type(workspace.outcome, '$.resume_previous') IS NOT NULL))
                         OR (prior.verb = 'worktree.remove'
                             AND json_extract(workspace.outcome, '$.stage') = 'removal'
                             AND json_extract(workspace.outcome, '$.command_id') = prior.id)))
                   OR (json_type(workspace.outcome, '$.native_git_write_dispatched') IS NULL
                       AND workspace.phase IN ('creating','removing','unknown')
                       AND ((prior.verb = 'launch'
                             AND json_extract(workspace.outcome, '$.stage') = 'creation'
                             AND NOT (COALESCE(json_extract(workspace.outcome, '$.reuse'), 0) = 1
                                      AND json_type(workspace.outcome, '$.resume_previous') IS NOT NULL))
                            OR (prior.verb = 'worktree.remove'
                                AND workspace.phase IN ('removing','unknown')
                                AND json_extract(workspace.outcome, '$.stage') = 'removal'
                                AND json_extract(workspace.outcome, '$.command_id') = prior.id)))
                   OR (prior.rowid < ?3
                       AND prior.state IN ('accepted','queued','dispatched')
                       AND ((prior.verb = 'launch'
                             AND json_extract(workspace.outcome, '$.native_git_write_dispatched') = 0
                             AND workspace.phase = 'creating'
                             AND json_extract(workspace.outcome, '$.stage') = 'creation'
                             AND json_extract(workspace.outcome, '$.reuse') IS NOT 1)
                            OR (prior.verb = 'worktree.remove'
                                AND ((prior.state IN ('accepted','queued')
                                      AND workspace.phase IN ('ready','failed'))
                                     OR (prior.state = 'dispatched'
                                         AND json_extract(workspace.outcome, '$.native_git_write_dispatched') = 0
                                         AND workspace.phase = 'removing'
                                         AND json_extract(workspace.outcome, '$.stage') = 'removal'
                                         AND json_extract(workspace.outcome, '$.command_id') = prior.id)))))
               )
         )",
        params![command_id, common_dir, current_rowid],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

pub(crate) fn resume_parent_payload_matches(
    connection: &Connection,
    parent: &Command,
    args: &Value,
) -> Result<bool> {
    if parent.verb != "resume"
        || parent.request_id.is_some()
        || parent.probe
        || parent.turn_id.is_some()
    {
        return Ok(false);
    }
    let digest = canonical::digest(&json!({
        "target": parent.target.as_deref(),
        "verb": parent.verb.as_str(),
        "args": args,
        "expected_generation": parent.expected_generation,
        "expected_turn_id": Value::Null,
    }))?;
    let Some((principal, method, stored_digest)) = command_binding(connection, &parent.id)? else {
        return Err(Error::new(
            "source_integrity",
            "resume parent has no retained command binding",
        ));
    };
    Ok(principal == parent.principal && method == parent.method && stored_digest == digest)
}

fn command_cause_tx(connection: &Connection, command_id: &str) -> Result<Option<core::EventCause>> {
    let encoded: Option<String> = connection
        .query_row(
            "SELECT cause FROM commands WHERE id = ?1",
            [command_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    Ok(decode_event_cause(encoded.as_deref(), Some(command_id)))
}

fn has_open_request_reply(
    connection: &Connection,
    agent: i64,
    generation: i64,
    request_id: &str,
    except_command: Option<&str>,
) -> Result<bool> {
    admission::request_reply_blocked(connection, agent, generation, request_id, except_command)
}

type StoredWriteBinding = (String, String, Vec<u8>, String);

fn write_binding(connection: &Connection, id: &str) -> Result<Option<StoredWriteBinding>> {
    connection
        .query_row(
            "SELECT principal, method, payload_digest, result FROM writes WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)
}

fn write_exists(connection: &Connection, id: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM writes WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn load_linked_command(connection: &Connection, parent: &str) -> Result<Option<Command>> {
    let child_id: Option<String> = connection
        .query_row(
            "SELECT id FROM commands
             WHERE parent_command = ?1 AND link_kind = 'send' AND ordinal = 0",
            [parent],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    match child_id {
        Some(id) => load_command(connection, &id),
        None => Ok(None),
    }
}

fn admit_command(
    tx: &Transaction<'_>,
    command: &CommandInsert<'_>,
    event_data: Value,
) -> Result<Event> {
    if event_data.get("command_id").and_then(Value::as_str) != Some(command.id) {
        return Err(Error::new(
            "source_integrity",
            "command admission event does not match its command row",
        ));
    }
    let cause = ordinary_command_cause(command.id);
    insert_command(tx, command, &cause)?;
    insert_daemon_event_input(
        tx,
        DaemonEventInput::new(
            command.created_at,
            &format!("command.{}", command_state_name(command.state)),
            "command",
            command.target,
            command.generation,
            event_data,
        )
        .with_cause(cause),
    )
}

fn ordinary_command_cause(command_id: &str) -> core::EventCause {
    core::EventCause {
        command_id: command_id.to_owned(),
        rule: None,
        depth: 0,
    }
}

fn annotate_new_command_cause_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    command_event: &mut Event,
    cause: core::EventCause,
) -> Result<()> {
    validate_event_cause_shape(&cause, Some(command_id))?;
    if cause.depth > 5 {
        return Err(Error::new(
            "bad_state",
            "rule cause depth exceeds the firing limit",
        ));
    }
    if command_event.source != "daemon"
        || !command_event.kind.starts_with("command.")
        || command_event.data.get("command_id").and_then(Value::as_str) != Some(command_id)
    {
        return Err(Error::new(
            "source_integrity",
            "command cause annotation does not match its new command event",
        ));
    }

    let default_cause = core::EventCause {
        command_id: command_id.to_owned(),
        rule: None,
        depth: 0,
    };
    let default_json = encode_json(&default_cause)?;
    let cause_json = encode_json(&cause)?;
    let changed = tx
        .execute(
            "UPDATE commands SET cause = ?1 WHERE id = ?2 AND cause = ?3",
            params![cause_json, command_id, default_json],
        )
        .map_err(sql_error)?;
    if changed != 1 && command_cause_tx(tx, command_id)?.as_ref() != Some(&cause) {
        return Err(Error::new(
            "source_integrity",
            "new command cause could not be bound to its exact command",
        ));
    }

    let changed = tx
        .execute(
            "UPDATE events SET cause = ?1
             WHERE seq = ?2 AND source = 'daemon' AND kind = ?3 AND cause = ?4
               AND json_extract(data, '$.command_id') = ?5",
            params![
                cause_json,
                command_event.seq,
                command_event.kind,
                default_json,
                command_id
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        let stored: Option<String> = tx
            .query_row(
                "SELECT cause FROM events WHERE seq = ?1",
                [command_event.seq],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .flatten();
        if decode_event_cause(stored.as_deref(), Some(command_id)).as_ref() != Some(&cause) {
            return Err(Error::new(
                "source_integrity",
                "new command event cause could not be bound to its exact command",
            ));
        }
    }
    command_event.cause = Some(cause);
    Ok(())
}

fn annotate_new_event_cause_tx(
    tx: &Transaction<'_>,
    event: &mut Event,
    cause: core::EventCause,
) -> Result<()> {
    validate_event_cause_shape(&cause, None)?;
    if event.source != "daemon" {
        return Err(Error::new(
            "source_integrity",
            "only a newly committed daemon event can inherit a controller cause",
        ));
    }
    let encoded = encode_json(&cause)?;
    let changed = tx
        .execute(
            "UPDATE events SET cause = ?1 WHERE seq = ?2 AND source = 'daemon' AND cause IS NULL",
            params![encoded, event.seq],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        let stored: Option<String> = tx
            .query_row(
                "SELECT cause FROM events WHERE seq = ?1",
                [event.seq],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .flatten();
        if decode_event_cause(stored.as_deref(), None).as_ref() != Some(&cause) {
            return Err(Error::new(
                "source_integrity",
                "event cause could not be bound to its newly committed event",
            ));
        }
    }
    event.cause = Some(cause);
    Ok(())
}

fn admit_linked_send(
    tx: &Transaction<'_>,
    principal: &str,
    parent: &str,
    agent: i64,
    generation: i64,
    text: &str,
    now: i64,
) -> Result<(String, Event)> {
    let agent_id = format_agent_id(agent);
    let mut request = SubmitCommand::new(format!("{parent}/send"), "send");
    request.args = json!({"text": text});
    request.expected_generation = Some(generation);
    check_rpc_frame(
        "agent.command",
        &json!({
            "agent_id": agent_id,
            "generation": generation,
            "command_id": request.id,
            "verb": "send",
            "args": request.args,
        }),
    )?;
    let digest = canonical::digest(&json!({
        "target": agent_id,
        "verb": "send",
        "args": request.args,
        "expected_generation": generation,
        "expected_turn_id": Value::Null,
    }))?;
    let command = CommandInsert {
        parent_command: Some(parent),
        link_kind: Some("send"),
        ordinal: Some(0),
        state: CommandState::Queued,
        ..CommandInsert::admitted(principal, &request, &digest, (agent, generation), now)
    };
    let event = admit_command(
        tx,
        &command,
        json!({"command_id": request.id, "parent_command": parent, "verb": "send"}),
    )?;
    Ok((request.id, event))
}

fn insert_command(
    tx: &Transaction<'_>,
    command: &CommandInsert<'_>,
    cause: &core::EventCause,
) -> Result<()> {
    validate_event_cause_shape(cause, Some(command.id))?;
    if cause.depth != 0 || cause.rule.is_some() {
        return Err(Error::new(
            "invalid",
            "ordinary command admission must start at cause depth zero",
        ));
    }
    let project_binding = projects::capture_project_command_binding_tx(tx, command)?;
    let args = encode_json(command.args)?;
    let outcome = command.outcome.map(encode_json).transpose()?;
    let cause = encode_json(cause)?;
    db!(tx.execute(
        "INSERT INTO commands(
             id, principal, method, payload_digest, target, expected_generation,
             generation, expected_turn_id, parent_command, link_kind, ordinal,
             turn_id, request_id, verb, args, state, outcome, created_at, updated_at, probe, cause,
             project_binding, selector
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23
         )",
        params![
            command.id,
            command.principal,
            command.method,
            command.payload_digest,
            command.target,
            command.expected_generation,
            command.generation,
            command.expected_turn_id,
            command.parent_command,
            command.link_kind,
            command.ordinal,
            command.turn_id,
            command.request_id,
            command.verb,
            args,
            command_state_name(command.state),
            outcome,
            command.created_at,
            command.updated_at,
            command.probe,
            cause,
            project_binding,
            command.selector,
        ],
    ));
    Ok(())
}

fn update_command_state(
    tx: &Transaction<'_>,
    id: &str,
    state: CommandState,
    outcome: Option<&Value>,
    generation: Option<i64>,
    now: i64,
) -> Result<()> {
    let previous: String = tx
        .query_row("SELECT state FROM commands WHERE id = ?1", [id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("not_found", "command state row was not found"))?;
    let previous: CommandState = parse_enum(&previous, "command state")?;
    if !core::command_transition_allowed(previous, state) {
        return Err(Error::new(
            "bad_state",
            "command state transition is not allowed",
        ));
    }
    write_command_state(tx, id, state, outcome, generation, now)?;
    provider_holds::command_terminal_tx(tx, id, previous, state, now)
}

fn write_command_state(
    tx: &Transaction<'_>,
    id: &str,
    state: CommandState,
    outcome: Option<&Value>,
    generation: Option<i64>,
    now: i64,
) -> Result<()> {
    let outcome = outcome.map(encode_json).transpose()?;
    db!(tx.execute(
        "UPDATE commands
         SET state = ?1, outcome = ?2, generation = COALESCE(?3, generation), updated_at = ?4
         WHERE id = ?5",
        params![command_state_name(state), outcome, generation, now, id],
    ));
    Ok(())
}

fn insert_daemon_event(
    tx: &Transaction<'_>,
    ts: i64,
    kind: &str,
    family: &str,
    agent: Option<i64>,
    generation: Option<i64>,
    data: Value,
) -> Result<Event> {
    insert_daemon_event_input(
        tx,
        DaemonEventInput::new(ts, kind, family, agent, generation, data),
    )
}

fn insert_daemon_event_input(tx: &Transaction<'_>, event: DaemonEventInput<'_>) -> Result<Event> {
    let subject = match (event.agent, event.generation) {
        (Some(agent), Some(generation)) => event_subject_for_generation(tx, agent, generation)?,
        _ => None,
    };
    insert_event(
        tx,
        EventInsert {
            ts: event.ts,
            kind: event.kind,
            family: Some(event.family),
            agent: event.agent,
            generation: event.generation,
            source: "daemon",
            native_id: None,
            cursor: None,
            payload_digest: None,
            subject,
            cause: event.cause,
            data: event.data,
        },
    )
}

fn insert_daemon_command_event(
    tx: &Transaction<'_>,
    ts: i64,
    kind: &str,
    agent: Option<i64>,
    generation: Option<i64>,
    command_id: &str,
    data: Value,
) -> Result<Event> {
    insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(ts, kind, "command", agent, generation, data),
    )
}

fn insert_daemon_event_for_command(
    tx: &Transaction<'_>,
    command_id: &str,
    mut event: DaemonEventInput<'_>,
) -> Result<Event> {
    if event
        .data
        .get("command_id")
        .and_then(Value::as_str)
        .is_some_and(|event_command_id| event_command_id != command_id)
    {
        return Err(Error::new(
            "source_integrity",
            "daemon event command id differs from its verified producer",
        ));
    }
    let command = load_command_record(tx, command_id)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "daemon command event references a missing command",
        )
    })?;
    let bound = !event
        .agent
        .is_some_and(|agent| command.target != Some(agent))
        && !event
            .generation
            .is_some_and(|generation| command.command.generation != Some(generation));
    event.cause = if bound {
        command_cause_tx(tx, command_id)?
    } else {
        None
    };
    insert_daemon_event_input(tx, event)
}

fn event_subject_for_generation(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<Option<EventSubject>> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Ok(None);
    };
    if agent.generation != generation {
        return Ok(None);
    }
    Ok(Some(EventSubject::from(&agent)))
}

fn insert_native_event(tx: &Transaction<'_>, event: NativeEventInsert<'_>) -> Result<Event> {
    insert_event(
        tx,
        EventInsert {
            ts: event.ts,
            kind: &event.native_event.kind,
            family: Some(event.family),
            agent: Some(event.agent),
            generation: Some(event.generation),
            source: event.source,
            native_id: Some(&event.native_event.native_id),
            cursor: event.cursor,
            payload_digest: Some(event.payload_digest),
            subject: event.subject,
            cause: event.cause,
            data: event.native_event.data.clone(),
        },
    )
}

fn insert_event(tx: &Transaction<'_>, event: EventInsert<'_>) -> Result<Event> {
    let data_json = encode_json(&event.data)?;
    if data_json.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "persisted event data exceeds 64 KiB",
        ));
    }
    let subject_json = event.subject.as_ref().map(encode_json).transpose()?;
    if let Some(cause) = &event.cause {
        validate_event_cause_shape(cause, None)?;
    }
    let cause_json = event.cause.as_ref().map(encode_json).transpose()?;
    db!(tx.execute(
        "INSERT INTO events(ts, kind, family, agent, generation, source, native_id,
                            cursor, payload_digest, data, subject, cause)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            event.ts,
            event.kind,
            event.family,
            event.agent,
            event.generation,
            event.source,
            event.native_id,
            event.cursor,
            event.payload_digest,
            data_json,
            subject_json,
            cause_json,
        ],
    ));
    let seq = tx.last_insert_rowid();
    Ok(Event {
        seq,
        ts: event.ts,
        kind: event.kind.to_owned(),
        agent: event.agent.map(format_agent_id),
        generation: event.generation,
        source: event.source.to_owned(),
        native_id: event.native_id.map(ToOwned::to_owned),
        data: event.data,
        subject: event.subject,
        cause: event.cause,
    })
}

fn event_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRecord> {
    Ok(EventRecord {
        seq: row.get(0)?,
        ts: row.get(1)?,
        kind: row.get(2)?,
        agent: row.get(3)?,
        generation: row.get(4)?,
        source: row.get(5)?,
        native_id: row.get(6)?,
        data: row.get(7)?,
        subject: row.get(8)?,
        cause: row.get(9)?,
    })
}

fn event_from_record(record: EventRecord) -> Result<Event> {
    let subject = record
        .subject
        .as_deref()
        .map(decode_json)
        .transpose()?
        .map(|value| {
            serde_json::from_value::<EventSubject>(value)
                .map_err(|error| Error::new("internal", error.to_string()))
        })
        .transpose()?;
    let cause = decode_event_cause(record.cause.as_deref(), None);
    Ok(Event {
        seq: record.seq,
        ts: record.ts,
        kind: record.kind,
        agent: record.agent.map(format_agent_id),
        generation: record.generation,
        source: record.source,
        native_id: record.native_id,
        data: decode_json(&record.data)?,
        subject,
        cause,
    })
}

fn validate_event_cause_shape(
    cause: &core::EventCause,
    expected_command_id: Option<&str>,
) -> Result<()> {
    cause.validate()?;
    if expected_command_id.is_some_and(|expected| expected != cause.command_id) {
        return Err(Error::new(
            "source_integrity",
            "event cause binding is invalid",
        ));
    }
    Ok(())
}

fn decode_event_cause(
    encoded: Option<&str>,
    expected_command_id: Option<&str>,
) -> Option<core::EventCause> {
    let cause = serde_json::from_str::<core::EventCause>(encoded?).ok()?;
    validate_event_cause_shape(&cause, expected_command_id).ok()?;
    Some(cause)
}

fn turn_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRecord> {
    Ok(TurnRecord {
        agent: row.get(0)?,
        generation: row.get(1)?,
        turn_id: row.get(2)?,
        state: row.get(3)?,
        status: row.get(4)?,
        final_text: row.get(5)?,
        started_at: row.get(6)?,
        completed_at: row.get(7)?,
        usage: row.get(8)?,
    })
}

fn turn_from_record(record: TurnRecord) -> Result<Turn> {
    let usage = record.usage.as_deref().map(decode_turn_usage).transpose()?;
    Ok(Turn {
        agent: format_agent_id(record.agent),
        generation: record.generation,
        turn_id: record.turn_id,
        state: parse_enum(&record.state, "turn state")?,
        status: record.status,
        final_text: record.final_text,
        started_at: record.started_at,
        completed_at: record.completed_at,
        usage,
    })
}

fn decode_turn_usage(value: &str) -> Result<TurnUsage> {
    serde_json::from_str(value)
        .map_err(|error| Error::new("internal", format!("invalid stored turn usage: {error}")))
}

fn load_turn(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Option<Turn>> {
    let record = connection
        .query_row(
            "SELECT agent, generation, turn_id, state, status, final_text,
                    started_at, completed_at, usage
             FROM turns WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
            params![agent, generation, turn_id],
            turn_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    record.map(turn_from_record).transpose()
}

fn insert_turn(tx: &Transaction<'_>, turn: TurnInsert<'_>) -> Result<()> {
    db!(tx.execute(
        "INSERT INTO turns(agent, generation, turn_id, state, status, final_text,
                           started_at, completed_at, usage)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            turn.agent,
            turn.generation,
            turn.turn_id,
            turn_state_name(turn.state),
            turn.status,
            turn.final_text,
            turn.started_at,
            turn.completed_at,
            turn.usage,
        ],
    ));
    Ok(())
}

fn load_source_state(
    connection: &Connection,
    agent: i64,
    generation: i64,
    source: &str,
) -> Result<Option<SourceStateRecord>> {
    connection
        .query_row(
            "SELECT cursor, last_store_seq, last_batch_id, last_batch_digest,
                    last_batch_first_seq, last_batch_last_seq, state, health,
                    gap_id, gap_digest, gap_from, gap_to, gap_seq, last_handoff_digest, last_handoff
             FROM source_state WHERE agent = ?1 AND generation = ?2 AND source = ?3",
            params![agent, generation, source],
            |row| {
                Ok(SourceStateRecord {
                    cursor: row.get(0)?,
                    last_store_seq: row.get(1)?,
                    last_batch_id: row.get(2)?,
                    last_batch_digest: row.get(3)?,
                    last_batch_first_seq: row.get(4)?,
                    last_batch_last_seq: row.get(5)?,
                    state: row.get(6)?,
                    health: row.get(7)?,
                    gap_id: row.get(8)?,
                    gap_digest: row.get(9)?,
                    gap_from: row.get(10)?,
                    gap_to: row.get(11)?,
                    gap_seq: row.get(12)?,
                    last_handoff_digest: row.get(13)?,
                    last_handoff: row.get(14)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

fn install_initial_source_plan(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    harness: &str,
    now: i64,
) -> Result<()> {
    install_stream_source_plan(
        tx,
        agent,
        generation,
        source_families_for_harness(harness)?,
        now,
    )
}

fn install_stream_source_plan(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    families: &[&str],
    now: i64,
) -> Result<()> {
    for family in families {
        db!(tx.execute(
            "INSERT INTO source_owners(agent, generation, family, source, mechanism)
             VALUES (?1, ?2, ?3, ?4, 'stream')",
            params![agent, generation, family, INITIAL_SOURCE],
        ));
    }
    db!(tx.execute(
        "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
         VALUES (?1, ?2, ?3, 'open', 'ok', ?4)",
        params![agent, generation, INITIAL_SOURCE, now],
    ));
    Ok(())
}

fn source_families_for_harness(harness: &str) -> Result<&'static [&'static str]> {
    match harness {
        "fake" => Ok(&["session", "turn", "message", "request"]),
        "codex" => Ok(&["session", "turn", "message", "tool", "request"]),
        "muse" => Ok(&["session", "turn", "message", "request"]),
        "claude" => Ok(&["session", "turn", "message", "request"]),
        "opencode" => Ok(&["session", "turn", "message", "request"]),
        "antigravity" => Ok(&["session", "turn", "message", "tool"]),
        other => Err(Error::new(
            "unsupported",
            format!("unsupported launch harness {other}"),
        )),
    }
}

fn family_for_kind(kind: &str) -> Result<&'static str> {
    match kind {
        "session.ready" | "session.ended" => Ok("session"),
        "turn.started" | "turn.completed" => Ok("turn"),
        "message.completed" => Ok("message"),
        "tool.started" | "tool.completed" => Ok("tool"),
        "request.opened" | "request.resolved" | "request.closed" => Ok("request"),
        other => Err(Error::new(
            "unsupported",
            format!("L1 Store does not persist native event {other}"),
        )),
    }
}

fn send_gate_open(connection: &Connection, agent_num: i64, generation: i64) -> Result<bool> {
    let Some(agent) = load_agent_by_num(connection, agent_num)? else {
        return Err(Error::new("not_found", "send target Agent was not found"));
    };
    if agent.generation != generation
        || agent.state != AgentState::Idle
        || agent.active_turn.is_some()
    {
        return Ok(false);
    }
    let turn_block: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND state IN ('running', 'unknown')
             )",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if turn_block {
        return Ok(false);
    }
    let input_block: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands
                 WHERE target = ?1 AND generation = ?2 AND verb = 'send'
                   AND state IN ('dispatched', 'unknown') AND turn_id IS NULL
             )",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(!input_block)
}

fn earliest_pending_send(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT id FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'send'
               AND state IN ('accepted', 'queued')
             ORDER BY created_at, rowid LIMIT 1",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)
}

fn decode_pending_request(
    id: String,
    turn_id: String,
    kind: String,
    details: String,
) -> Result<PendingRequest> {
    Ok(PendingRequest {
        id,
        turn_id,
        kind,
        details: decode_json(&details)?,
    })
}

fn load_pending_request(
    connection: &Connection,
    agent: i64,
    generation: i64,
    request_id: &str,
) -> Result<Option<(PendingRequest, String)>> {
    let row = connection
        .query_row(
            "SELECT turn_id, kind, details, state FROM pending_requests
             WHERE agent = ?1 AND generation = ?2 AND request_id = ?3",
            params![agent, generation, request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(turn_id, kind, details, state)| {
        Ok((
            decode_pending_request(request_id.to_owned(), turn_id, kind, details)?,
            state,
        ))
    })
    .transpose()
}

fn capture_single_pending_request(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<PendingRequest> {
    let mut statement = connection
        .prepare(
            "SELECT request_id, turn_id, kind, details FROM pending_requests
             WHERE agent = ?1 AND generation = ?2 AND state = 'pending'
             ORDER BY request_id LIMIT 2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(sql_error)?;
    let mut pending = Vec::with_capacity(2);
    for row in rows {
        pending.push(row.map_err(sql_error)?);
    }
    match pending.len() {
        0 => Err(Error::new(
            "not_found",
            "Agent has no pending native request",
        )),
        1 => {
            let (id, turn_id, kind, details) = pending
                .pop()
                .ok_or_else(|| Error::new("internal", "captured pending request disappeared"))?;
            decode_pending_request(id, turn_id, kind, details)
        }
        _ => Err(Error::new(
            "invalid",
            "request_id is required when multiple native requests are pending",
        )),
    }
}

fn validate_resume_ready_subject_tx(
    tx: &Transaction<'_>,
    projection: &Agent,
    agent_num: i64,
    generation: i64,
    native_event: &NativeEvent,
) -> Result<()> {
    if native_event.kind != "session.ready" {
        return Ok(());
    }
    let (count, command_id): (i64, Option<String>) = tx
        .query_row(
            "SELECT COUNT(*), MIN(id) FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'resume'
               AND state IN ('dispatched','unknown','applied')",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    if count == 0 {
        return Ok(());
    }
    if count != 1 {
        return Err(Error::new(
            "source_integrity",
            "resumed generation has multiple retained start commands",
        ));
    }
    let command_id = command_id.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "resumed generation has no retained start command id",
        )
    })?;
    let command = load_command_record(tx, &command_id)?
        .ok_or_else(|| Error::new("source_integrity", "resume command row is missing"))?;
    let old_generation = command
        .command
        .expected_generation
        .or_else(|| {
            command
                .command
                .generation
                .and_then(|value| value.checked_sub(1))
        })
        .ok_or_else(|| Error::new("source_integrity", "resume command has no old generation"))?;
    let command_id_matches = match native_event.data.get("command_id") {
        None | Some(Value::Null) => true,
        Some(Value::String(reported)) => reported == &command_id,
        Some(_) => false,
    };
    if command.target != Some(agent_num)
        || command.command.verb != "resume"
        || command.command.generation != Some(generation)
        || old_generation.checked_add(1) != Some(generation)
        || !command_id_matches
    {
        return Err(Error::new(
            "source_integrity",
            "session readiness does not match the exact resumed generation",
        ));
    }
    let retained_scope = projection
        .native_scope
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("source_integrity", "resume lost its retained native scope"))?;
    let retained_ref = projection
        .native_ref
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "resume lost its retained native reference",
            )
        })?;
    if native_event
        .data
        .get("native_scope")
        .and_then(Value::as_str)
        != Some(retained_scope)
        || native_event.data.get("native_ref").and_then(Value::as_str) != Some(retained_ref)
    {
        return Err(Error::new(
            "source_integrity",
            "resumed session readiness changed its retained native subject",
        ));
    }
    Ok(())
}

fn command_cause_for_turn_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Option<core::EventCause>> {
    if load_turn(tx, agent_num, generation, turn_id)?.is_none() {
        return Ok(None);
    }
    let mut statement = tx
        .prepare(
            "SELECT id FROM commands
             WHERE target = ?1 AND generation = ?2 AND turn_id = ?3
               AND verb = 'send' AND state = 'applied'
             ORDER BY id LIMIT 2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent_num, generation, turn_id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    let mut ids = Vec::with_capacity(2);
    for row in rows {
        ids.push(row.map_err(sql_error)?);
    }
    match ids.as_slice() {
        [command_id] => command_cause_tx(tx, command_id),
        _ => Ok(None),
    }
}

fn supports_control_verb(harness: &str, verb: &str) -> bool {
    harness == "codex"
        || (matches!(harness, "fake" | "muse")
            && matches!(verb, "interrupt" | "steer" | "approve" | "deny" | "answer"))
        || (harness == "opencode" && matches!(verb, "approve" | "deny" | "answer" | "interrupt"))
        || (harness == "claude" && matches!(verb, "interrupt" | "approve" | "deny"))
}

fn contains_secret_marker(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            (matches!(key.as_str(), "is_secret" | "isSecret") && child.as_bool() == Some(true))
                || contains_secret_marker(child)
        }),
        Value::Array(items) => items.iter().any(contains_secret_marker),
        _ => false,
    }
}

fn update_agent_projection(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    observer::require_controlled(agent)?;
    let agent_num = parse_agent_id(&agent.id)?;
    let effective = agent.effective.as_ref().map(encode_json).transpose()?;
    let pending = encode_json(&agent.pending)?;
    if agent.pending.len() > MAX_PAGE_ENTRIES || pending.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "Agent pending request projection exceeds its bound",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE agents SET generation = ?1, state = ?2, active_turn = ?3,
                               native_scope = ?4, native_ref = ?5, effective = ?6,
                               pending = ?7, updated_at = ?8,
                               token = CASE WHEN ?9 THEN NULL ELSE token END
             WHERE id = ?10",
            params![
                agent.generation,
                agent_state_name(agent.state),
                agent.active_turn,
                agent.native_scope,
                agent.native_ref,
                effective,
                pending,
                agent.updated_at,
                agent.is_terminal(),
                agent_num,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "not_found",
            format!("Agent {} was not found", agent.id),
        ));
    }
    Ok(())
}

fn cancel_pending_command_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let outcome = json!({"reason": reason});
    update_command_state(
        tx,
        command_id,
        CommandState::Cancelled,
        Some(&outcome),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.cancelled",
        Some(agent),
        Some(generation),
        command_id,
        json!({"command_id": command_id, "reason": reason}),
    )?);
    Ok(())
}

fn fail_unstarted_launch_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let command = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("not_found", "initial launch command was not found"))?;
    if command.target != Some(agent_num)
        || command.command.verb != "launch"
        || command.command.generation != Some(0)
        || !matches!(
            command.state,
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
        )
    {
        return Err(Error::new(
            "stale_generation",
            "initial launch no longer has its exact pre-dispatch binding",
        ));
    }
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "initial launch Agent was not found"))?;
    if agent.generation != 0 || agent.state != AgentState::Created {
        return Err(Error::new(
            "stale_generation",
            "initial launch Agent changed before its pre-dispatch failure was committed",
        ));
    }

    cancel_unstarted_launch_input(tx, command_id, agent_num, now, events)?;
    let changed = tx
        .execute(
            "UPDATE agents SET state = 'failed', active_turn = NULL, pending = '[]',
                               token = NULL, updated_at = ?1
             WHERE id = ?2 AND generation = 0 AND state = 'created'",
            params![now, agent_num],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "stale_generation",
            "initial launch Agent changed before failure settlement",
        ));
    }
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "agent.failed",
            "agent",
            Some(agent_num),
            Some(0),
            json!({
                "agent_id": agent.id,
                "command_id": command_id,
                "reason": "launch_not_dispatched"
            }),
        ),
    )?);
    Ok(())
}

fn cancel_unstarted_launch_input(
    tx: &Transaction<'_>,
    command_id: &str,
    agent_num: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(linked) = load_linked_command(tx, command_id)? else {
        return Ok(());
    };
    let linked = load_command_record(tx, &linked.id)?
        .ok_or_else(|| Error::new("source_integrity", "linked launch input disappeared"))?;
    if linked.target != Some(agent_num) {
        return Err(Error::new(
            "source_integrity",
            "linked launch input no longer matches its exact parent",
        ));
    }
    if !matches!(linked.state, CommandState::Accepted | CommandState::Queued) {
        return Ok(());
    }
    let outcome = json!({"reason": "launch_not_dispatched", "native_dispatched": false});
    update_command_state(
        tx,
        &linked.command.id,
        CommandState::Cancelled,
        Some(&outcome),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.cancelled",
        Some(agent_num),
        linked.command.generation,
        &linked.command.id,
        json!({"command_id": linked.command.id, "outcome": outcome}),
    )?);
    Ok(())
}

fn command_ids_in_state(
    connection: &Connection,
    agent: i64,
    generation: i64,
    state: &str,
) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare(
            "SELECT id FROM commands
             WHERE target = ?1 AND generation = ?2 AND state = ?3
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation, state], |row| row.get(0))
        .map_err(sql_error)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(sql_error)?);
    }
    Ok(ids)
}

fn pending_command_ids(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare(
            "SELECT id FROM commands
             WHERE target = ?1 AND generation = ?2 AND state IN ('accepted', 'queued')
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation], |row| row.get(0))
        .map_err(sql_error)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(sql_error)?);
    }
    Ok(ids)
}

fn terminalize_generation_commands(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    reason: &str,
    preserve_dispatched_stop: bool,
) -> Result<()> {
    let mut statement = tx
        .prepare(
            "SELECT id, verb FROM commands
             WHERE target = ?1 AND generation = ?2 AND state = 'dispatched'
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?;
    let mut dispatched = Vec::new();
    for row in rows {
        dispatched.push(row.map_err(sql_error)?);
    }
    drop(statement);
    for (id, verb) in dispatched {
        // A committed session.ended boundary proves the session stopped, but it
        // does not bind that end to an in-flight stop RPC. Keep a dispatched
        // stop pending for its exact agent.stop reply. Generic loss/restart
        // paths do not set this flag and continue to settle dispatched stops
        // as unknown.
        if preserve_dispatched_stop && verb == "stop" {
            continue;
        }
        let outcome = json!({"reason": reason});
        update_command_state(tx, &id, CommandState::Unknown, Some(&outcome), None, now)?;
    }
    for id in pending_command_ids(tx, agent, generation)? {
        let outcome = json!({"reason": reason});
        update_command_state(tx, &id, CommandState::Cancelled, Some(&outcome), None, now)?;
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", format!("{field} must not be empty")));
    }
    Ok(())
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::new("invalid", format!("{field} must be a non-empty string")))
}

fn optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(Error::new("invalid", format!("{field} must be a string"))),
    }
}

fn check_input(text: &str) -> Result<()> {
    if text.len() > MAX_INPUT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "inline input exceeds 512 KiB",
        ));
    }
    Ok(())
}

fn check_rpc_frame(method: &str, params: &impl serde::Serialize) -> Result<()> {
    let frame = json!({
        "jsonrpc": "2.0",
        "id": "preflight",
        "method": method,
        "params": params,
    });
    if encode_json(&frame)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "encoded plugin request exceeds 1 MiB",
        ));
    }
    Ok(())
}

fn validate_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_PAGE_ENTRIES).contains(&limit) {
        return Err(Error::new(
            "invalid",
            "page limit must be between 1 and 1,000",
        ));
    }
    Ok(())
}

fn enforce_page_bytes<T: serde::Serialize>(items: &mut Vec<T>) -> Result<()> {
    let had_items = !items.is_empty();
    while encode_json(items)?.len() > MAX_PAGE_BYTES {
        items.pop();
        if items.is_empty() {
            break;
        }
    }
    if had_items && items.is_empty() {
        return Err(Error::new(
            "frame_too_large",
            "one page entry exceeds the 900 KiB response bound",
        ));
    }
    Ok(())
}

fn encode_json(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::new("invalid", error.to_string()))
}

fn decode_json(value: &str) -> Result<Value> {
    serde_json::from_str(value).map_err(|error| Error::new("internal", error.to_string()))
}

fn decode_pending_requests(value: &str) -> Result<Vec<PendingRequest>> {
    serde_json::from_str(value).map_err(|error| {
        Error::new(
            "internal",
            format!("invalid stored pending requests: {error}"),
        )
    })
}

fn parse_enum<T: DeserializeOwned>(value: &str, what: &str) -> Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|error| Error::new("internal", format!("invalid stored {what}: {error}")))
}

fn format_agent_id(id: i64) -> String {
    format!("a{id}")
}

fn validate_ulid(id: &str) -> Result<Ulid> {
    Ulid::from_string(id).map_err(|_| Error::new("invalid", "mutation id must be a ULID"))
}

/// ULID timestamp of `id` in commit-clock milliseconds, or `None` when the
/// id is not a ULID. Shared by the admission `id_expired` check and the
/// `command.children` pruned-history classification so both read one clock.
fn ulid_timestamp_ms(id: &str) -> Option<i64> {
    let parsed = Ulid::from_string(id).ok()?;
    Some(i64::try_from(parsed.timestamp_ms()).unwrap_or(i64::MAX))
}

fn validate_id_age(id: &str, now: i64) -> Result<()> {
    validate_ulid(id)?;
    // `validate_ulid` just accepted this id, so the timestamp parse cannot fail.
    let created_at = ulid_timestamp_ms(id).unwrap_or(i64::MAX);
    if now.saturating_sub(created_at) > RETRY_WINDOW_MS {
        return Err(Error::new(
            "id_expired",
            "an absent mutation id is older than the 24-hour retry window",
        ));
    }
    // A ULID further ahead than the allowed generator-clock skew is a client
    // clock error, not a retry: refusing it at admission keeps every retained
    // id skew-bounded, so the retention floors can promise `id_expired` (and
    // mail `history_gap`) after pruning instead of a re-admitted-as-new id.
    if created_at.saturating_sub(now) > RETRY_CLOCK_SKEW_MS {
        return Err(Error::new(
            "invalid",
            "mutation id is more than the allowed clock skew in the future",
        ));
    }
    Ok(())
}

fn agent_state_name(state: AgentState) -> &'static str {
    match state {
        AgentState::Unknown => "unknown",
        AgentState::Created => "created",
        AgentState::Starting => "starting",
        AgentState::Idle => "idle",
        AgentState::Busy => "busy",
        AgentState::Waiting => "waiting",
        AgentState::Stopped => "stopped",
        AgentState::Failed => "failed",
        AgentState::Lost => "lost",
    }
}

fn command_state_name(state: CommandState) -> &'static str {
    match state {
        CommandState::Accepted => "accepted",
        CommandState::Queued => "queued",
        CommandState::Dispatched => "dispatched",
        CommandState::Applied => "applied",
        CommandState::Rejected => "rejected",
        CommandState::Failed => "failed",
        CommandState::Unknown => "unknown",
        CommandState::Cancelled => "cancelled",
    }
}

fn turn_state_name(state: TurnState) -> &'static str {
    match state {
        TurnState::Running => "running",
        TurnState::Completed => "completed",
        TurnState::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod timer_tests;

#[cfg(test)]
mod git_fetch_tests;

#[cfg(test)]
mod git_workspace_fence_tests;
#[cfg(test)]
mod resolution_tests;

#[cfg(test)]
mod goals_tests;
