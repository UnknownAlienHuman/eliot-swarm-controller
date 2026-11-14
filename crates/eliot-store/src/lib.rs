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
pub use context::ContextDispatch;
pub use coordination::{
    MailSendOutcome, MailSendResult, NoticeClaim, NoticeKind, NoticePending, NoticeState,
};
pub use goals::{
    GoalCheckResult, GoalCommitOutcome, GoalContinuation, GoalRoundClaim, GoalRoundCommit,
    GoalRoundCommitResult, GoalRoundKey, GoalRoundProbe,
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
pub use usage::ResolvedUsageScope;

use eliot_core::api::{AgentConfigView, AgentSummary};
use eliot_core::{
    self as core, Agent, AgentControl, AgentEdit, AgentState, BatchAck, Command, CommandState,
    Effect, Event, EventSubject, FileChangeHint, FrozenSetupFile, GitBaseline, GitHintPage,
    GitSnapshot, InjectPoint, NativeEvent, NativeStoreBinding, PendingRequest, ProcessIdentity,
    ProcessRecord, ProcessSnapshot, ProcessState, SessionUsage, SourceBatch, SourceGap,
    SourceGapAck, SubmitCommand, Turn, TurnOutcome, TurnState, TurnUsage, WorkspaceCancelOutcome,
    WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation, WorkspacePreparationInput,
    WorkspaceRemovalOutcome, WorkspaceSetupArg, WorkspaceSetupOutcome, parse_agent_id,
};
pub use eliot_core::{Error, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use std::{
    cell::Cell,
    collections::{BTreeMap, HashSet},
    path::Path,
    time::Duration,
};
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
/// Rows that still block drain: neither settled nor pruned observer
/// attach/detach noise. Shared by the bounded list and its total so the
/// drain report can mark truncation honestly for any omission (the list
/// bound or the page byte bound trimming full command rows).
const UNSETTLED_COMMANDS_WHERE: &str = "state IN ('accepted', 'queued', 'dispatched', 'unknown')
                  AND NOT (state = 'unknown' AND verb IN ('attach','detach')
                    AND EXISTS(SELECT 1 FROM agents a WHERE a.id = commands.target AND a.observe_only = 1)
                    AND NOT EXISTS(SELECT 1 FROM source_state s WHERE s.agent = commands.target
                        AND s.source NOT IN ('daemon','os','git','workspace_process') AND s.state = 'open'))";
const LIST_UNSETTLED_COMMANDS_COLUMNS: &str =
    "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                        created_at, updated_at, probe, selector, config_revision
                 FROM commands";
/// Live Agent rows that can still produce work. Shared by the bounded drain
/// list and its total for the same honest-truncation reason as unsettled
/// commands.
const NONTERMINAL_AGENTS_WHERE: &str =
    "observe_only = 0 AND state IN ('created', 'starting', 'idle', 'busy', 'waiting')";
const LIST_NONTERMINAL_AGENTS_COLUMNS: &str = "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0),
                        parent, root, depth, control, profile_inherited
                 FROM agents";
const RECOVER_ACCEPTED_COMMAND_FILTER: &str =
    "state IN ('accepted', 'queued', 'dispatched', 'unknown')
       AND state = 'accepted' AND principal = 'owner' AND parent_command IS NULL
       AND selector IS NULL
       AND NOT EXISTS (SELECT 1 FROM workspaces w
           WHERE w.agent = commands.target AND w.start_command_id = commands.id)";
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
mod context;
mod coordination;
mod dispatch;
mod git;
mod goals;
mod hierarchy;
mod lifecycle;
mod native_children;
pub use native_children::{
    NATIVE_CHILD_ATTENTION_PAGE_SIZE, NativeChildrenAttention, NativeChildrenAttentionPage,
};
mod native_events;
mod observer;
mod observer_live;
mod processes;
mod resolution;
mod retention;
mod schema;
mod source_batches;
mod source_handoff;
pub(crate) mod usage;
mod workspace;

fn sql_error(error: rusqlite::Error) -> Error {
    Error::new("internal", error.to_string())
}

fn validate_remote_command_context(
    principal: &str,
    remote: Option<&core::RemoteCallContext>,
) -> Result<()> {
    if let Some(remote) = remote {
        remote.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "remote provenance requires the authenticated owner bridge",
            ));
        }
    }
    Ok(())
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

/// Bounded evidence for tools in one exact, retained turn. Unknown includes
/// missing turn history, a tool-family source gap, or a truncated event tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveToolCoverage {
    Unknown,
    Clear,
    Active { count: usize },
}

/// One retained loss row for attention startup reconciliation. `event_seq`
/// is `None` when the exact `agent.lost` evidence was pruned or never
/// retained; the row must then be skipped, never synthesized.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct StartupLoss {
    pub agent: String,
    pub agent_num: i64,
    pub generation: i64,
    pub event_seq: Option<i64>,
    pub updated_at: i64,
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

    /// Refuse retained-command reads if a failed transaction left the
    /// connection outside autocommit. In that state the same connection could
    /// still observe transaction-local rows.
    pub fn ensure_committed_reads(&self) -> Result<()> {
        if !self.connection.is_autocommit() {
            return Err(Error::new(
                "unavailable",
                "Store transaction state is uncertain; retained commands cannot be read",
            ));
        }
        Ok(())
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

    /// Preserve authenticated HTTP call provenance in the existing command
    /// cause and admission events, in the original admission transaction.
    /// A retained retry keeps its original provenance and never rewrites history.
    pub fn submit_with_remote_context(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        remote: Option<&core::RemoteCallContext>,
        now: i64,
    ) -> Result<Admission> {
        let Some(remote) = remote else {
            return self.submit_inner_checked(principal, request, preparation, now);
        };
        validate_remote_command_context(principal, Some(remote))?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let previous_command_rowid: i64 = tx
                .query_row("SELECT COALESCE(MAX(rowid), 0) FROM commands", [], |row| {
                    row.get(0)
                })
                .map_err(sql_error)?;
            let mut admission = admission::submit_tx(
                &tx,
                principal,
                request,
                preparation,
                now,
                false,
                self.hierarchy_limits,
            )?;
            if admission.is_new {
                let root_cause =
                    command_cause_tx(&tx, &admission.command.id)?.ok_or_else(|| {
                        Error::new("source_integrity", "new admission has no command cause")
                    })?;
                for event in &mut admission.events {
                    annotate_remote_admission_event_tx(
                        &tx,
                        event,
                        &root_cause,
                        remote,
                        previous_command_rowid,
                    )?;
                }
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admission)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read a retained command binding without entering the writer path.
    /// This remains available after a persistence fault because it performs
    /// no transaction or write. Store write transactions are local to their
    /// call; this refuses reads if commit/rollback leaves one active.
    pub fn replay_command_submission(
        &self,
        principal: &str,
        request: &SubmitCommand,
        remote: Option<&core::RemoteCallContext>,
    ) -> Result<Option<Admission>> {
        require_nonempty(principal, "principal")?;
        validate_remote_command_context(principal, remote)?;
        self.ensure_committed_reads()?;
        let payload_digest = admission::command_payload_digest(request)?;
        if let Some(admission) = admission::replay_command_binding(
            &self.connection,
            principal,
            request,
            &payload_digest,
        )? {
            return Ok(Some(admission));
        }
        if write_exists(&self.connection, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "mutation id is already used by another write",
            ));
        }
        Ok(None)
    }

    fn submit_inner_checked(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = self.submit_inner(principal, request, preparation, now);
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
        self.ensure_committed_reads()?;
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
        if let Some(admission) = admission::replay_command_binding(
            &self.connection,
            principal,
            request,
            &payload_digest,
        )? {
            let command = &admission.command;
            if command.verb != "git.fetch" || command.target.is_some() {
                return Err(Error::new(
                    "source_integrity",
                    "retained Git fetch binding has another command shape",
                ));
            }
            return Ok(Some(admission));
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
        let result = self.dispatch_inner(id, now, None, None);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Dispatch a normal send and claim its FIFO next_input context in the
    /// same transaction. The returned command retains its original args.
    pub fn dispatch_with_context(&mut self, id: &str, now: i64) -> Result<Change<ContextDispatch>> {
        self.ensure_writer_healthy()?;
        let command = self
            .get_command(id)?
            .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        if command.verb != "send" {
            return Err(Error::new(
                "bad_state",
                "context can be claimed only with a send dispatch",
            ));
        }
        let mut context_items = Vec::new();
        let result = self.dispatch_inner(id, now, None, Some(&mut context_items));
        self.fault_on_persistence_error(&result);
        let change = result?;
        Ok(Change {
            value: ContextDispatch {
                command: change.value,
                context_items,
            },
            events: change.events,
        })
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
        let result = self.dispatch_inner(id, now, Some((expected_agent, binding)), None);
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
                        parent, root, depth, control, profile_inherited
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

    /// Compact fleet projection for `agent.list`: scope-checked, filtered,
    /// ordered by Agent ID. Every equality filter rides one `agents_list_*`
    /// composite index (forced with `INDEXED BY`, so `EXPLAIN QUERY PLAN`
    /// shows the index); the cursor rides the primary key. The response
    /// carries the matching total ignoring the cursor, honest truncation, and
    /// the continuation cursor at the last returned row.
    pub fn list_agent_summaries(
        &self,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
    ) -> Result<AgentListPage> {
        self.list_agent_summaries_inner(None, filter, after, limit, now_ms)
    }

    /// Scope-checked fleet projection for an Agent caller: itself for `self`
    /// control, its subtree for `tree`, its project for `project`. The caller
    /// row itself is always in scope.
    pub fn list_agent_summaries_scoped(
        &self,
        caller: &Agent,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
    ) -> Result<AgentListPage> {
        self.list_agent_summaries_inner(Some(caller), filter, after, limit, now_ms)
    }

    fn list_agent_summaries_inner(
        &self,
        caller: Option<&Agent>,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
    ) -> Result<AgentListPage> {
        validate_limit(limit)?;
        filter.validate()?;
        let after_num = after.map(parse_agent_id).transpose()?.unwrap_or(0);
        // One extra row probes continuation; the byte bound may trim further.
        let probe = i64::try_from(limit)
            .map_err(|_| Error::new("invalid", "page limit must fit the index bound"))?
            + 1;
        let page = agent_list_select(filter, caller, Some(after_num), Some(probe), false)?;
        let mut statement = self.connection.prepare(&page.sql).map_err(sql_error)?;
        let rows = statement
            .query_map(
                rusqlite::params_from_iter(page.params.iter()),
                agent_record_from_row,
            )
            .map_err(sql_error)?;
        let mut summaries = Vec::new();
        for row in rows {
            summaries.push(self.summary_from_record(row.map_err(sql_error)?, now_ms)?);
        }
        let total_page = agent_list_select(filter, caller, None, None, true)?;
        let total: i64 = self
            .connection
            .query_row(
                &total_page.sql,
                rusqlite::params_from_iter(total_page.params.iter()),
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let mut truncated = summaries.len() > limit;
        summaries.truncate(limit);
        let kept = summaries.len();
        enforce_page_bytes(&mut summaries)?;
        truncated = truncated || summaries.len() < kept;
        let next = summaries.last().map(|summary| summary.id.clone());
        Ok(AgentListPage {
            summaries,
            total: usize::try_from(total.max(0)).unwrap_or(0),
            truncated,
            next,
        })
    }

    fn summary_from_record(&self, record: AgentRecord, now_ms: i64) -> Result<AgentSummary> {
        let state: AgentState = parse_enum(&record.state, "Agent state")?;
        let id = format_agent_id(record.id);
        let terminal = self.last_terminal_turn(record.id, record.generation, &record.harness)?;
        let last_progress_at =
            native_children::native_progress(&self.connection, record.id, record.generation)?;
        Ok(AgentSummary {
            id,
            generation: record.generation,
            project: record.project,
            harness: record.harness,
            state,
            parent: record.parent.map(format_agent_id),
            // A NULL root only arises from raw test rows without hierarchy
            // metadata and reads self-rooted, matching `agent_from_record`.
            root: format_agent_id(record.root.unwrap_or(record.id)),
            active_turn: record.active_turn,
            last_progress_at,
            progress_age_ms: last_progress_at
                .and_then(|at| now_ms.checked_sub(at))
                .filter(|age| *age >= 0),
            last_turn_id: terminal.as_ref().map(|turn| turn.0.clone()),
            last_outcome: terminal.as_ref().and_then(|turn| turn.1),
            activity: if record.origin == "native" && record.observe_only {
                Some(native_children::activity(
                    &self.connection,
                    record.id,
                    record.generation,
                )?)
            } else {
                None
            },
            usage: terminal.and_then(|turn| turn.2),
            updated_at: record.updated_at,
        })
    }

    /// Newest terminal turn for one Agent generation: its canonical outcome
    /// (legacy rows classify explicitly, never defaulting to success) with
    /// the retained usage where the Store kept one. No terminal turn reads
    /// back as no outcome, never a fabricated one.
    fn last_terminal_turn(
        &self,
        agent: i64,
        generation: i64,
        harness: &str,
    ) -> Result<Option<LastTerminalTurn>> {
        let row: Option<StoredTerminalTurn> = self
            .connection
            .query_row(
                "SELECT turn_id, outcome, status, usage FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND state = 'completed'
                 ORDER BY COALESCE(completed_at, started_at, 0) DESC, rowid DESC LIMIT 1",
                params![agent, generation],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|(turn_id, outcome, status, usage)| {
            let canonical = match outcome {
                Some(value) => Some(TurnOutcome::parse(&value).ok_or_else(|| {
                    Error::new("internal", format!("invalid stored turn outcome: {value}"))
                })?),
                None => Some(TurnOutcome::classify_legacy_status_for_harness(
                    harness,
                    status.as_deref(),
                )),
            };
            let usage = usage.map(|value| decode_turn_usage(&value)).transpose()?;
            Ok((turn_id, canonical, usage))
        })
        .transpose()
    }

    /// Page routed attention candidates. Registration existence is an indexed
    /// candidate check; child activity is inspected in separate bounded pages.
    pub fn list_attention_agents(&self, after: Option<&str>, limit: usize) -> Result<Vec<Agent>> {
        validate_limit(limit)?;
        let after_num = after.map(parse_agent_id).transpose()?.unwrap_or(0);
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0),
                        parent, root, depth, control, profile_inherited
                 FROM agents
                 WHERE id > ?1 AND observe_only = 0 AND (state IN ('busy', 'waiting')
                   OR EXISTS(SELECT 1 FROM native_child_registrations r
                     WHERE r.producer = agents.id AND r.producer_generation = agents.generation))
                 ORDER BY id LIMIT ?2",
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

    /// One bounded page of retained loss evidence for attention startup:
    /// currently-`lost` Agents with their latest `agent.lost` sequence, most
    /// recently updated first. Callers drain every page through the `after`
    /// cursor (the last row's `(updated_at, id)`), so eligible evidence is
    /// never permanently omitted; each step stays bounded. A `None` sequence
    /// means the exact `agent.lost` evidence was pruned or never retained:
    /// callers skip that row rather than synthesizing an event identity.
    /// Callers deduplicate reported incidents through the existing
    /// `AgentLost` handling per incident.
    pub fn startup_lost_agents(
        &self,
        limit: usize,
        after: Option<(i64, i64)>,
    ) -> Result<Vec<StartupLoss>> {
        validate_limit(limit)?;
        let (updated_at, id) = after.unwrap_or((i64::MAX, i64::MAX));
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation, updated_at FROM agents
                 WHERE observe_only = 0 AND state = 'lost'
                   AND (updated_at < ?1 OR (updated_at = ?1 AND id < ?2))
                 ORDER BY updated_at DESC, id DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![updated_at, id, limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(row.map_err(sql_error)?);
        }
        let mut losses = Vec::with_capacity(agents.len());
        for (num, generation, row_updated_at) in agents {
            let seq: Option<i64> = self
                .connection
                .query_row(
                    "SELECT seq FROM events
                     WHERE kind = 'agent.lost' AND agent = ?1 AND generation = ?2
                     ORDER BY seq DESC LIMIT 1",
                    params![num, generation],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            losses.push(StartupLoss {
                agent: format_agent_id(num),
                agent_num: num,
                generation,
                event_seq: seq,
                updated_at: row_updated_at,
            });
        }
        enforce_page_bytes(&mut losses)?;
        Ok(losses)
    }

    /// Timestamp of one retained event, for deterministic loss-report
    /// identity. `None` when the evidence was pruned.
    pub fn daemon_event_timestamp(&self, seq: i64) -> Result<Option<i64>> {
        self.connection
            .query_row(
                "SELECT ts FROM events WHERE seq = ?1",
                params![seq],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Whether an attention incident event with this incident id is retained.
    /// Startup reconciliation uses it to make a post-restart re-report of the
    /// same loss evidence a no-op without a second incident ledger.
    pub fn attention_incident_exists(&self, incident_id: &str) -> Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM events
                     WHERE kind = 'x.user.agent.attention'
                       AND json_extract(data, '$.data.incident_id') = ?1
                 )",
                params![incident_id],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Whether any message (read or unread) on this thread was mailed to this
    /// recipient. Reconciling a retained incident resends only missing mail.
    pub fn attention_thread_mailed_to(&self, thread: &str, recipient: &str) -> Result<bool> {
        let num = parse_agent_id(recipient)?;
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM messages WHERE thread = ?1 AND recipient = ?2
                 )",
                params![thread, num],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Whether an owner notice naming this attention incident was committed.
    /// The notice text names the incident id, which is unique per incident.
    pub fn attention_owner_notice_exists(&self, incident_id: &str) -> Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM events
                     WHERE kind = 'x.user.notify'
                       AND json_extract(data, '$.message') LIKE '%' || ?1 || '%'
                 )",
                params![incident_id],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Return live Agent projections independently of historical-row volume.
    pub fn list_nonterminal_agents(&self, limit: usize) -> Result<Vec<Agent>> {
        validate_limit(limit)?;
        let sql = format!(
            "{LIST_NONTERMINAL_AGENTS_COLUMNS} WHERE {NONTERMINAL_AGENTS_WHERE} ORDER BY id LIMIT ?1"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
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

    /// Total nonterminal Agents ignoring the drain list bound and the page
    /// byte bound. Compare with the bounded list to mark truncation honestly
    /// for any omission reason.
    pub fn count_nonterminal_agents(&self) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM agents WHERE {NONTERMINAL_AGENTS_WHERE}");
        self.connection
            .query_row(&sql, [], |row| row.get(0))
            .map_err(sql_error)
    }

    /// Total unsettled commands ignoring the drain list bound and the page
    /// byte bound. Compare with the bounded list to mark truncation honestly
    /// for any omission reason.
    pub fn count_unsettled_commands(&self) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM commands WHERE {UNSETTLED_COMMANDS_WHERE}");
        self.connection
            .query_row(&sql, [], |row| row.get(0))
            .map_err(sql_error)
    }

    /// Live custody snapshot from the Store source of truth: distinct process
    /// owners with at least one non-exited (`running`/`unknown`) process
    /// record, ordered by owner. Returns the bounded prefix and the total
    /// distinct owner count; the caller reports truncation as
    /// `owners.len() < total`, so no omission can read back as completeness.
    /// A held workspace operation lock alone writes no process record and
    /// never appears here; only observed live children do.
    pub fn live_process_owners(&self, limit: usize) -> Result<(Vec<LiveProcessOwner>, usize)> {
        validate_limit(limit)?;
        let total: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM (SELECT DISTINCT owner FROM processes WHERE state IN ('running','unknown'))",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT owner FROM processes WHERE state IN ('running','unknown')
                 ORDER BY owner LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([limit as i64], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut owners = Vec::new();
        for row in rows {
            if let Some(owner) = parse_live_process_owner(&row.map_err(sql_error)?) {
                owners.push(owner);
            }
        }
        Ok((owners, usize::try_from(total.max(0)).unwrap_or(0)))
    }

    /// Return unsettled commands without letting terminal history consume the limit.
    pub fn list_unsettled_commands(&self, limit: usize) -> Result<Vec<Command>> {
        validate_limit(limit)?;
        let sql = format!(
            "{LIST_UNSETTLED_COMMANDS_COLUMNS} WHERE {UNSETTLED_COMMANDS_WHERE} ORDER BY created_at, rowid LIMIT ?1"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
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

    /// Page only keys for unattempted ordinary Owner admissions in index order.
    /// Rule actions, linked inputs and managed starts retain their own recovery.
    /// The rowid cursor remains usable if an earlier page settles or is pruned.
    pub fn recover_accepted_command_keys(
        &self,
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> Result<Vec<(String, i64, i64)>> {
        self.ensure_writer_healthy()?;
        validate_limit(limit)?;
        let (created_at, rowid) = after.unwrap_or((i64::MIN, i64::MIN));
        let sql = format!(
            "SELECT id, created_at, rowid FROM commands
             WHERE {RECOVER_ACCEPTED_COMMAND_FILTER}
               AND (created_at, rowid) > (?1, ?2)
             ORDER BY created_at, rowid LIMIT ?3"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
        let rows = statement
            .query_map(params![created_at, rowid, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// Recheck one key under the Store lock before handing it to the scheduler.
    /// Full retained commands are loaded individually, not as a wire-sized page.
    pub fn recover_accepted_admission(&self, id: &str) -> Result<Option<Admission>> {
        self.ensure_writer_healthy()?;
        let eligible = self
            .connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM commands
                     WHERE id = ?1 AND {RECOVER_ACCEPTED_COMMAND_FILTER})"
                ),
                [id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?;
        if !eligible {
            return Ok(None);
        }
        let command = load_command(&self.connection, id)?
            .ok_or_else(|| Error::new("internal", "recovery command disappeared"))?;
        let linked_command = load_linked_command(&self.connection, id)?
            .filter(|linked| matches!(linked.state, CommandState::Accepted | CommandState::Queued));
        Ok(Some(Admission {
            command,
            linked_command,
            events: Vec::new(),
            is_new: false,
        }))
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

    /// Return the committed command binding for an exact generation/turn.
    /// Missing or pruned turn-start evidence stays absent.
    pub fn turn_command_id(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT json_extract(data, '$.command_id') FROM events
                 WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.started'
                   AND json_extract(data, '$.turn_id') = ?3
                 ORDER BY seq DESC LIMIT 1",
                params![agent_num, generation, turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Stable sequence for the current unhealthy source episode. The caller
    /// first checks the current projected state; this reads only committed
    /// source transitions and does not infer health from silence.
    pub fn latest_unhealthy_source_event_seq(
        &self,
        agent: &str,
        generation: i64,
        source: &str,
    ) -> Result<Option<i64>> {
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT seq FROM events
                 WHERE agent = ?1 AND generation = ?2
                   AND kind IN ('source.health', 'source.closed')
                   AND json_extract(data, '$.source') = ?3
                   AND (json_extract(data, '$.state') = 'closed'
                        OR json_extract(data, '$.health') IN ('slow', 'stale', 'error'))
                 ORDER BY seq DESC LIMIT 1",
                params![agent_num, generation, source],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Summarize exact-turn tool events only when the retained history proves
    /// coverage. No tool names, arguments, or outputs leave Store.
    pub fn active_tool_coverage(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<ActiveToolCoverage> {
        const MAX_TOOL_EVENTS: usize = 512;
        const MAX_GAP_EVENTS: usize = 128;

        let agent_num = parse_agent_id(agent)?;
        let start_seq: Option<i64> = self
            .connection
            .query_row(
                "SELECT seq FROM events
                 WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.started'
                   AND json_extract(data, '$.turn_id') = ?3
                 ORDER BY seq DESC LIMIT 1",
                params![agent_num, generation, turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(start_seq) = start_seq else {
            return Ok(ActiveToolCoverage::Unknown);
        };

        let (tool_owners, healthy_tool_owners): (i64, i64) = self
            .connection
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(CASE
                     WHEN s.state = 'open' AND s.health = 'ok' THEN 1 ELSE 0 END), 0)
                 FROM source_owners o JOIN source_state s
                   ON s.agent = o.agent AND s.generation = o.generation AND s.source = o.source
                 WHERE o.agent = ?1 AND o.generation = ?2 AND o.family = 'tool'",
                params![agent_num, generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sql_error)?;
        if tool_owners == 0 || tool_owners != healthy_tool_owners {
            return Ok(ActiveToolCoverage::Unknown);
        }

        let mut gaps = self
            .connection
            .prepare(
                "SELECT data FROM events
                 WHERE agent = ?1 AND generation = ?2 AND seq > ?3 AND kind = 'source.gap'
                 ORDER BY seq LIMIT ?4",
            )
            .map_err(sql_error)?;
        let gap_rows = gaps
            .query_map(
                params![
                    agent_num,
                    generation,
                    start_seq,
                    (MAX_GAP_EVENTS + 1) as i64
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut gap_count = 0_usize;
        for row in gap_rows {
            gap_count += 1;
            if gap_count > MAX_GAP_EVENTS {
                return Ok(ActiveToolCoverage::Unknown);
            }
            let data = decode_json(&row.map_err(sql_error)?)?;
            if data
                .get("families")
                .and_then(Value::as_array)
                .is_some_and(|families| {
                    families
                        .iter()
                        .any(|family| family.as_str() == Some("tool"))
                })
            {
                return Ok(ActiveToolCoverage::Unknown);
            }
        }

        let mut statement = self
            .connection
            .prepare(
                "SELECT kind, data FROM events
                 WHERE agent = ?1 AND generation = ?2 AND seq > ?3
                   AND kind IN ('tool.started', 'tool.completed')
                   AND json_extract(data, '$.turn_id') = ?4
                 ORDER BY seq LIMIT ?5",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    agent_num,
                    generation,
                    start_seq,
                    turn_id,
                    (MAX_TOOL_EVENTS + 1) as i64
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(sql_error)?;
        let mut active = HashSet::new();
        let mut event_count = 0_usize;
        for row in rows {
            event_count += 1;
            if event_count > MAX_TOOL_EVENTS {
                return Ok(ActiveToolCoverage::Unknown);
            }
            let (kind, raw_data) = row.map_err(sql_error)?;
            let data = decode_json(&raw_data)?;
            let Some(key) = data
                .get("item_id")
                .or_else(|| data.get("tool_call_id"))
                .or_else(|| data.get("tool_id"))
                .or_else(|| data.get("call_id"))
                .or_else(|| data.get("tool_use_id"))
                .or_else(|| data.get("toolUseId"))
                .or_else(|| data.get("itemId"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            else {
                return Ok(ActiveToolCoverage::Unknown);
            };
            match kind.as_str() {
                "tool.started" => {
                    active.insert(key.to_owned());
                }
                "tool.completed" if active.remove(key) => {}
                "tool.completed" => return Ok(ActiveToolCoverage::Unknown),
                _ => return Ok(ActiveToolCoverage::Unknown),
            }
        }
        if event_count == 0 {
            return Ok(ActiveToolCoverage::Unknown);
        }
        Ok(if active.is_empty() {
            ActiveToolCoverage::Clear
        } else {
            ActiveToolCoverage::Active {
                count: active.len(),
            }
        })
    }

    pub fn list_turns(&self, agent: &str, generation: i64, limit: usize) -> Result<Vec<Turn>> {
        validate_limit(limit)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(agent_row) = load_agent_by_num(&self.connection, agent_num)? else {
            return Err(Error::new(
                "not_found",
                format!("Agent {agent} was not found"),
            ));
        };
        let harness = agent_row.harness;
        let mut statement = self
            .connection
            .prepare(
                "SELECT agent, generation, turn_id, state, status, outcome, native_status,
                        final_text, started_at, completed_at, usage, config_revision
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
            turns.push(turn_from_record(row.map_err(sql_error)?, &harness)?);
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
        let retained = retention::coverage_pruned_through(&self.connection)?;
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

    pub fn event_bounds(&self) -> Result<core::api::EventsBoundsResponse> {
        Ok(core::api::EventsBoundsResponse {
            pruned_through: retention::coverage_pruned_through(&self.connection)?,
            hi: self.high_water()?,
        })
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
        // Clear only commandless notice reservations (crash between claim
        // commit and submit); notices with a durable command are untouched.
        db!(tx.execute(
            "UPDATE mail_notices SET open_notice_command = NULL
             WHERE open_notice_command IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM commands WHERE commands.id = mail_notices.open_notice_command)",
            [],
        ));
        let mut dispatched_statement = tx
            .prepare(
                "SELECT id, target, generation, verb, outcome FROM commands
                 WHERE state = 'dispatched' ORDER BY created_at, rowid",
            )
            .map_err(sql_error)?;
        let dispatched_rows = dispatched_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
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
        for (command_id, target, generation, verb, encoded_outcome) in dispatched_commands {
            let previous_outcome = encoded_outcome.as_deref().map(decode_json).transpose()?;
            let outcome = if verb == "inject" {
                context::unknown_claim_outcome(previous_outcome.as_ref(), "daemon_restart")
            } else {
                json!({"reason": "daemon_restart"})
            };
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
        let exists: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1)",
                [agent_num],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !exists {
            return Ok(None);
        }
        let native = native_children::native_progress(&self.connection, agent_num, generation)?;
        let git: Option<i64> = self
            .connection
            .query_row(GIT_PROGRESS_SQL, params![agent_num, generation], |row| {
                row.get(0)
            })
            .optional()
            .map_err(sql_error)?;
        Ok(native.into_iter().chain(git).max())
    }
}

const GIT_PROGRESS_SQL: &str = "SELECT ts FROM events WHERE agent = ?1 AND generation = ?2
    AND source = 'git' AND kind = 'file.changed' ORDER BY ts DESC LIMIT 1";

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
    profile_inherited: bool,
}

struct CommandRecord {
    command: Command,
    state: CommandState,
    target: Option<i64>,
    expected_turn_id: Option<String>,
    /// Configuration revision captured at admission: the latest retained
    /// `agent_config` revision for the target Agent generation, or 0 when
    /// none existed (or the command is targetless). Never rewritten: a
    /// retry or redispatch keeps the original stamp.
    config_revision: i64,
}

struct TurnRecord {
    agent: i64,
    generation: i64,
    turn_id: String,
    state: String,
    status: Option<String>,
    outcome: Option<String>,
    native_status: Option<String>,
    final_text: Option<String>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    usage: Option<String>,
    /// Configuration revision the turn ran under, copied from its bound
    /// send command at turn creation. 0 when no overlay applied.
    config_revision: i64,
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
    outcome: Option<&'a str>,
    native_status: Option<&'a str>,
    final_text: Option<&'a str>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    usage: Option<&'a str>,
    config_revision: i64,
}

/// Indexed equality filters for the compact `agent.list` fleet projection.
/// Unknown shapes reject at the Core DTO boundary, never as empty matches.
#[derive(Clone, Debug, Default)]
pub struct AgentListFilter {
    pub state: Option<AgentState>,
    pub project: Option<String>,
    pub harness: Option<String>,
    pub parent: Option<String>,
    pub root: Option<String>,
}

impl AgentListFilter {
    fn validate(&self) -> Result<()> {
        if let Some(project) = &self.project {
            require_filter_text(project)?;
        }
        if let Some(harness) = &self.harness {
            require_filter_text(harness)?;
        }
        if let Some(parent) = &self.parent {
            require_filter_text(parent)?;
            parse_agent_id(parent)?;
        }
        if let Some(root) = &self.root {
            require_filter_text(root)?;
            parse_agent_id(root)?;
        }
        Ok(())
    }
}

/// One live custody owner: an Agent generation with at least one
/// non-exited process record. `workspace` distinguishes live workspace
/// children (`workspace:` owners) from controlled native children
/// (`agent:` owners).
pub struct LiveProcessOwner {
    pub agent: String,
    pub generation: i64,
    pub workspace: bool,
}

/// Parse a stored process owner (`agent:<id>#<generation>` or
/// `workspace:<id>#<generation>`). Anything else is a corrupt row the drain
/// report must not fabricate custody from.
fn parse_live_process_owner(owner: &str) -> Option<LiveProcessOwner> {
    let (scope, rest) = owner.split_once(':')?;
    let workspace = match scope {
        "agent" => false,
        "workspace" => true,
        _ => return None,
    };
    let (agent, generation) = rest.rsplit_once('#')?;
    let generation = generation.parse::<i64>().ok()?;
    if agent.trim().is_empty() || generation < 0 {
        return None;
    }
    Some(LiveProcessOwner {
        agent: agent.to_owned(),
        generation,
        workspace,
    })
}

/// One filtered, scope-checked fleet page: ordered summaries, the matching
/// total ignoring the cursor, honest truncation, and the continuation cursor.
pub struct AgentListPage {
    pub summaries: Vec<AgentSummary>,
    pub total: usize,
    pub truncated: bool,
    pub next: Option<String>,
}

/// Newest terminal turn behind one summary row: turn id, canonical outcome,
/// and retained usage where the Store kept one.
type LastTerminalTurn = (String, Option<TurnOutcome>, Option<TurnUsage>);

/// Stored terminal-turn columns before canonical classification.
type StoredTerminalTurn = (String, Option<String>, Option<String>, Option<String>);

struct AgentListSelect {
    sql: String,
    params: Vec<rusqlite::types::Value>,
}

fn require_filter_text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", "Agent list filter must be nonblank"));
    }
    Ok(())
}

/// Assemble the fleet query for one scope, filter set, and window. Equality
/// filters ride their `agents_list_*` composite (forced with `INDEXED BY`,
/// so `EXPLAIN QUERY PLAN` reports the index, preferring the selective
/// structural/scope index when several filters are present); the cursor rides
/// the primary key; tree scope walks descendants through the bounded subtree
/// CTE shared with direct reads. The count variant drops the cursor and limit
/// but keeps scope and filters.
fn agent_list_select(
    filter: &AgentListFilter,
    caller: Option<&Agent>,
    after_num: Option<i64>,
    limit: Option<i64>,
    count: bool,
) -> Result<AgentListSelect> {
    let columns = if count {
        "COUNT(*)".to_owned()
    } else {
        "agents.id, agents.generation, agents.name, agents.project, agents.harness, agents.profile,
                agents.profile_snapshot, agents.native_scope, agents.native_ref, agents.workspace, agents.effective,
                agents.pending, agents.state, agents.active_turn, agents.created_at, agents.updated_at, agents.origin, agents.observe_only, (agents.token IS NOT NULL AND agents.observe_only = 0),
                agents.parent, agents.root, agents.depth, agents.control, agents.profile_inherited"
            .to_owned()
    };
    let mut predicates = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    // Prefer the selective structural/scope index (parent/root/project) when
    // present; state/harness composites only lead when no structural filter
    // does. Project scope without a user filter rides the project composite
    // the same way.
    let mut equality = |column: &str, value: rusqlite::types::Value| {
        predicates.push(format!("agents.{column} = ?"));
        params.push(value);
    };
    let mut structural: Option<&str> = None;
    let mut fallback: Option<&str> = None;
    let mut note_index = |index: &'static str, structural_filter: bool| {
        if structural_filter {
            if structural.is_none() {
                structural = Some(index);
            }
        } else if fallback.is_none() {
            fallback = Some(index);
        }
    };
    if let Some(parent) = &filter.parent {
        equality(
            "parent",
            rusqlite::types::Value::from(parse_agent_id(parent)?),
        );
        note_index("agents_list_parent", true);
    }
    if let Some(root) = &filter.root {
        equality("root", rusqlite::types::Value::from(parse_agent_id(root)?));
        note_index("agents_list_root", true);
    }
    if let Some(project) = &filter.project {
        equality("project", rusqlite::types::Value::from(project.clone()));
        note_index("agents_list_project", true);
    }
    if let Some(state) = filter.state {
        equality(
            "state",
            rusqlite::types::Value::from(agent_state_name(state).to_owned()),
        );
        note_index("agents_list_state", false);
    }
    if let Some(harness) = &filter.harness {
        equality("harness", rusqlite::types::Value::from(harness.clone()));
        note_index("agents_list_harness", false);
    }
    let mut forced_index = structural.or(fallback);
    let mut prefix = String::new();
    if let Some(caller) = caller {
        let caller_num = parse_agent_id(&caller.id)?;
        match caller.control {
            AgentControl::Slf => {
                predicates.push("agents.id = ?".to_owned());
                params.push(rusqlite::types::Value::from(caller_num));
            }
            AgentControl::Project => {
                predicates.push("agents.project = ?".to_owned());
                params.push(rusqlite::types::Value::from(caller.project.clone()));
                if forced_index.is_none() {
                    forced_index = Some("agents_list_project");
                }
            }
            AgentControl::Tree => {
                // Same depth bound as direct reads (`hierarchy::is_in_subtree_of`):
                // descendants past the walk cap stay outside tree scope.
                prefix = format!(
                    "WITH RECURSIVE subtree(id, depth) AS \
                     (SELECT {caller_num}, 0 UNION ALL \
                     SELECT a.id, s.depth + 1 FROM agents a JOIN subtree s ON a.parent = s.id \
                     WHERE s.depth + 1 < {} ) ",
                    hierarchy::ANCESTOR_WALK_CAP
                );
                predicates.push("agents.id IN (SELECT id FROM subtree)".to_owned());
            }
        }
    }
    if let Some(after) = after_num {
        predicates.push("agents.id > ?".to_owned());
        params.push(rusqlite::types::Value::from(after));
    }
    let table = match forced_index {
        Some(index) => format!("agents INDEXED BY {index}"),
        None => "agents".to_owned(),
    };
    let mut sql = format!("{prefix}SELECT {columns} FROM {table}");
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    if !count {
        sql.push_str(" ORDER BY agents.id");
    }
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        params.push(rusqlite::types::Value::from(limit));
    }
    Ok(AgentListSelect { sql, params })
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
        profile_inherited: row.get(23)?,
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
        profile_inherited: record.profile_inherited,
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
        activity: if record.origin == "native" && record.observe_only {
            Some(native_children::activity(
                connection,
                record.id,
                record.generation,
            )?)
        } else {
            None
        },
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
                    parent, root, depth, control, profile_inherited
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
        config_revision: row.get(17)?,
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
    config_revision: i64,
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
            config_revision: raw.config_revision,
            config: None,
        },
        state,
        target,
        expected_turn_id: raw.expected_turn_id,
        config_revision: raw.config_revision,
    })
}

fn load_command_record(connection: &Connection, id: &str) -> Result<Option<CommandRecord>> {
    let raw = connection
        .query_row(
            "SELECT id, principal, method, target, expected_generation,
                    generation, expected_turn_id, turn_id, request_id, verb, args,
                    state, outcome, created_at, updated_at, probe, selector, config_revision
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
        remote_call: None,
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
        remote_call: None,
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

    annotate_admission_event_cause_tx(tx, command_event, &cause)
}

/// Common direct/rule/script admission event boundary. The caller first binds
/// the new command row; every returned event associated with that command uses
/// this same compare-and-swap, including events without data.command_id.
fn annotate_admission_event_cause_tx(
    tx: &Transaction<'_>,
    event: &mut Event,
    cause: &core::EventCause,
) -> Result<()> {
    validate_event_cause_shape(cause, Some(&cause.command_id))?;
    if event.source != "daemon"
        || event
            .cause
            .as_ref()
            .is_some_and(|old| old.command_id != cause.command_id)
    {
        return Err(Error::new(
            "source_integrity",
            "admission event is not bound to its causal command",
        ));
    }
    let original = event.cause.as_ref().map(encode_json).transpose()?;
    let encoded = encode_json(cause)?;
    let changed = tx
        .execute(
            "UPDATE events SET cause = ?1 WHERE seq = ?2 AND source = 'daemon' AND cause IS ?3",
            params![encoded, event.seq, original],
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
        if decode_event_cause(stored.as_deref(), Some(&cause.command_id)).as_ref() != Some(cause) {
            return Err(Error::new(
                "source_integrity",
                "admission event cause changed before annotation",
            ));
        }
    }
    event.cause = Some(cause.clone());
    Ok(())
}

fn annotate_admission_command_cause_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    events: &mut [Event],
    cause: core::EventCause,
) -> Result<()> {
    let index = events
        .iter()
        .position(|event| {
            event.kind.starts_with("command.")
                && event.data.get("command_id").and_then(Value::as_str) == Some(command_id)
        })
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "new admission has no matching command event",
            )
        })?;
    annotate_new_command_cause_tx(tx, command_id, &mut events[index], cause.clone())?;
    for (i, event) in events.iter_mut().enumerate() {
        if i != index
            && (event
                .cause
                .as_ref()
                .is_some_and(|old| old.command_id == command_id)
                || event.data.get("command_id").and_then(Value::as_str) == Some(command_id))
        {
            annotate_admission_event_cause_tx(tx, event, &cause)?;
        }
    }
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

/// Associate every event returned by this fresh admission, including
/// agent.created, before its original transaction commits. Existing causal
/// command/rule/depth fields are preserved. The rowid fence prevents this
/// request from rewriting a command that predates the admission transaction.
fn annotate_remote_admission_event_tx(
    tx: &Transaction<'_>,
    event: &mut Event,
    root_cause: &core::EventCause,
    remote: &core::RemoteCallContext,
    previous_command_rowid: i64,
) -> Result<()> {
    if event.source != "daemon" {
        return Err(Error::new(
            "source_integrity",
            "admission provenance requires a daemon event",
        ));
    }
    let mut cause = event.cause.clone().unwrap_or_else(|| root_cause.clone());
    let stored = command_cause_tx(tx, &cause.command_id)?
        .ok_or_else(|| Error::new("source_integrity", "admission event has no bound command"))?;
    if stored.command_id != cause.command_id
        || stored.rule != cause.rule
        || stored.depth != cause.depth
        || stored
            .remote_call
            .as_ref()
            .is_some_and(|known| known != remote)
    {
        return Err(Error::new(
            "source_integrity",
            "admission event differs from its command cause",
        ));
    }
    cause.remote_call = Some(remote.clone());
    validate_event_cause_shape(&cause, Some(&cause.command_id))?;
    let encoded = encode_json(&cause)?;
    let changed = tx
        .execute(
            "UPDATE commands SET cause = ?1 WHERE id = ?2 AND rowid > ?3 AND cause = ?4",
            params![
                encoded,
                cause.command_id,
                previous_command_rowid,
                encode_json(&stored)?
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "remote cause does not name a newly admitted command",
        ));
    }
    annotate_admission_event_cause_tx(tx, event, &cause)
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
             project_binding, selector, config_revision
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, 0
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

/// Latest retained `configure` revision for one Agent generation, or 0 when
/// no `configure` ever applied. Reads inside the caller's transaction so an
/// admission stamp and a revision bump stay serialized.
fn current_config_revision_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<i64> {
    tx.query_row(
        "SELECT COALESCE(MAX(revision), 0) FROM agent_config WHERE agent = ?1 AND generation = ?2",
        params![agent, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
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
        outcome: row.get(5)?,
        native_status: row.get(6)?,
        final_text: row.get(7)?,
        started_at: row.get(8)?,
        completed_at: row.get(9)?,
        usage: row.get(10)?,
        config_revision: row.get(11)?,
    })
}

fn turn_from_record(record: TurnRecord, harness: &str) -> Result<Turn> {
    let usage = record.usage.as_deref().map(decode_turn_usage).transpose()?;
    let state: TurnState = parse_enum(&record.state, "turn state")?;
    // Rows written before canonical outcomes existed carry no stored outcome:
    // classify them explicitly from the legacy status with the stored
    // harness (Claude rows carry the raw terminal reason), never as success.
    let outcome = match record.outcome.as_deref() {
        Some(value) => Some(
            TurnOutcome::parse(value)
                .ok_or_else(|| Error::new("internal", "invalid stored turn outcome"))?,
        ),
        None if state == TurnState::Completed => Some(
            TurnOutcome::classify_legacy_status_for_harness(harness, record.status.as_deref()),
        ),
        None => None,
    };
    let native_status = record.native_status.filter(|value| !value.is_empty());
    Ok(Turn {
        agent: format_agent_id(record.agent),
        generation: record.generation,
        turn_id: record.turn_id,
        state,
        status: record.status,
        outcome,
        native_status,
        final_text: record.final_text,
        started_at: record.started_at,
        completed_at: record.completed_at,
        usage,
        config_revision: record.config_revision,
        config: None,
    })
}

fn decode_turn_usage(value: &str) -> Result<TurnUsage> {
    serde_json::from_str(value)
        .map_err(|error| Error::new("internal", format!("invalid stored turn usage: {error}")))
}

fn decode_session_usage(value: &str) -> Result<SessionUsage> {
    serde_json::from_str(value)
        .map_err(|error| Error::new("internal", format!("invalid stored session usage: {error}")))
}

impl Store {
    /// Latest retained `session.usage` snapshot for one Agent generation, or
    /// `None` when no session fact was ever committed. Only the latest
    /// snapshot is retained: older and duplicate snapshots never replace it.
    pub fn get_session_usage(&self, agent: &str, generation: i64) -> Result<Option<SessionUsage>> {
        let agent_num = parse_agent_id(agent)?;
        let snapshot: Option<String> = self
            .connection
            .query_row(
                "SELECT snapshot FROM session_usage WHERE agent = ?1 AND generation = ?2",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        snapshot
            .map(|value| decode_session_usage(&value))
            .transpose()
    }
}

/// Retained model/effort overlay for one Agent generation: the requested
/// values from one `configure` command plus the effective values the next
/// native turn carries. The original profile snapshot is never rewritten;
/// it stays on the Agent row while these rows accumulate per generation.
/// A `resume` starts its new generation by inheriting the previous
/// generation's latest overlay as revision 1 with `provenance` set to
/// `inherited_from`; directly configured rows carry `configure`.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentConfig {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub requested_model: Option<String>,
    pub requested_effort: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub application: String,
    pub provenance: String,
    pub command_id: String,
    pub created_at: i64,
}

fn agent_config_from_row(
    agent: i64,
    generation: i64,
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AgentConfig> {
    let requested: String = row.get(1)?;
    let effective: String = row.get(2)?;
    let requested: Value = serde_json::from_str(&requested).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(ConfigDecodeError),
        )
    })?;
    let effective: Value = serde_json::from_str(&effective).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(ConfigDecodeError),
        )
    })?;
    Ok(AgentConfig {
        agent: format_agent_id(agent),
        generation,
        revision: row.get(0)?,
        requested_model: requested
            .get("model")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        requested_effort: requested
            .get("effort")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        model: effective
            .get("model")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        effort: effective
            .get("effort")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        application: row.get(3)?,
        provenance: row.get(4)?,
        command_id: row.get(5)?,
        created_at: row.get(6)?,
    })
}

#[derive(Debug)]
struct ConfigDecodeError;

impl std::fmt::Display for ConfigDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid stored agent_config JSON")
    }
}

impl std::error::Error for ConfigDecodeError {}

fn get_agent_config_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    revision: i64,
) -> Result<Option<AgentConfig>> {
    tx.query_row(
        "SELECT revision, requested, effective, application, provenance, command_id, created_at
         FROM agent_config WHERE agent = ?1 AND generation = ?2 AND revision = ?3",
        params![agent, generation, revision],
        |row| agent_config_from_row(agent, generation, row),
    )
    .optional()
    .map_err(sql_error)
}

impl Store {
    /// Retained `configure` overlay for one exact revision, or `None` when
    /// the command ran under the frozen snapshot (revision 0). A nonzero
    /// revision without a retained row is corruption, never a silent
    /// snapshot fallback.
    pub fn get_agent_config(
        &self,
        agent: &str,
        generation: i64,
        revision: i64,
    ) -> Result<Option<AgentConfig>> {
        if revision == 0 {
            return Ok(None);
        }
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT revision, requested, effective, application, provenance, command_id, created_at
                 FROM agent_config WHERE agent = ?1 AND generation = ?2 AND revision = ?3",
                params![agent_num, generation, revision],
                |row| agent_config_from_row(agent_num, generation, row),
            )
            .optional()
            .map_err(sql_error)?
            .map(|config| {
                if config.revision != revision {
                    Err(Error::new(
                        "source_integrity",
                        "retained agent_config revision does not match its row",
                    ))
                } else {
                    Ok(config)
                }
            })
            .transpose()
    }

    /// Latest retained `configure` overlay for one Agent generation, or
    /// `None` when no `configure` ever applied to it.
    pub fn get_agent_config_current(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<AgentConfig>> {
        let agent_num = parse_agent_id(agent)?;
        let revision: Option<i64> = self
            .connection
            .query_row(
                "SELECT MAX(revision) FROM agent_config WHERE agent = ?1 AND generation = ?2",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        revision
            .map(|revision| {
                self.get_agent_config(agent, generation, revision)?
                    .ok_or_else(|| {
                        Error::new("internal", "newest agent_config revision disappeared")
                    })
            })
            .transpose()
    }

    /// Effective model/effort for one send command's retained revision, or
    /// `None` when the send runs under the frozen snapshot. Used by the
    /// plugin host before native turn construction and by settlement when
    /// it records what the native request carried. Reads only the retained
    /// revision stamped at dispatch, so a later config-file reload can never
    /// rewrite a pending or unknown command's meaning.
    pub fn send_effective_config(&self, command_id: &str) -> Result<Option<AgentConfig>> {
        self.command_effective_config(command_id, &["send"])
    }

    /// Effective configuration for one retained command's stamped revision,
    /// or `None` when it runs under the frozen snapshot (revision 0). The
    /// revision is read from the retained command row and resolved against
    /// the retained `agent_config` rows only: config-file reloads never
    /// rewrite a retained generation, a pending command, or an
    /// unknown-outcome resolution. `allowed` lists the verbs that may carry
    /// a native configuration (launch, resume, send, configure).
    pub fn command_effective_config(
        &self,
        command_id: &str,
        allowed: &[&str],
    ) -> Result<Option<AgentConfig>> {
        let record = load_command_record(&self.connection, command_id)?.ok_or_else(|| {
            Error::new("not_found", format!("command {command_id} was not found"))
        })?;
        if !allowed.contains(&record.command.verb.as_str()) {
            return Err(Error::new(
                "invalid",
                "effective configuration resolves only for native commands",
            ));
        }
        let Some(target) = record.target else {
            return Err(Error::new(
                "source_integrity",
                "command has no retained Agent target",
            ));
        };
        let generation = record
            .command
            .generation
            .ok_or_else(|| Error::new("source_integrity", "command has no retained generation"))?;
        let agent = format_agent_id(target);
        self.get_agent_config(&agent, generation, record.config_revision)
    }

    /// Effective configuration for one retained turn's stamped revision, or
    /// `None` when the turn ran under the frozen snapshot. Reads only
    /// retained rows, so reloads never rewrite a retained turn's meaning.
    pub fn turn_effective_config(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<AgentConfig>> {
        let turn = self
            .get_turn(agent, generation, turn_id)?
            .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
        self.get_agent_config(agent, generation, turn.config_revision)
    }

    /// Retained configuration view for one command's exact stamped
    /// generation and revision: the requested and effective model/effort,
    /// the revision, the provenance, and the profile name. `None` when the
    /// command runs under the frozen snapshot, has no retained target, or
    /// carries no native configuration. Reads only retained rows, so a
    /// config-file reload never rewrites a retained generation, a pending
    /// command, or an unknown-outcome resolution.
    pub fn command_config_view(&self, command: &Command) -> Result<Option<AgentConfigView>> {
        if !matches!(
            command.verb.as_str(),
            "launch" | "resume" | "send" | "configure"
        ) {
            return Ok(None);
        }
        if command.config_revision == 0 {
            return Ok(None);
        }
        let Some(target) = command.target.as_deref() else {
            return Ok(None);
        };
        let Some(generation) = command.generation else {
            return Ok(None);
        };
        let agent = self.get_agent(target)?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                format!("command target {target} was not found"),
            )
        })?;
        self.retained_config_view(&agent, generation, command.config_revision)
    }

    /// Retained configuration view for one turn's exact stamped generation
    /// and revision. `None` when the turn ran under the frozen snapshot.
    /// Reads only retained rows, so later revisions never rewrite it.
    pub fn turn_config_view(&self, agent: &Agent, turn: &Turn) -> Result<Option<AgentConfigView>> {
        if turn.config_revision == 0 {
            return Ok(None);
        }
        self.retained_config_view(agent, turn.generation, turn.config_revision)
    }

    /// Build the readback view from the exact retained overlay row. The
    /// profile name, snapshot originals, and launch inheritance come from
    /// the retained Agent row; the revision, provenance, and requested and
    /// effective values come from the retained `agent_config` row. A nonzero
    /// revision without a retained row is corruption, never a silent
    /// snapshot fallback.
    fn retained_config_view(
        &self,
        agent: &Agent,
        generation: i64,
        revision: i64,
    ) -> Result<Option<AgentConfigView>> {
        let Some(overlay) = self.get_agent_config(&agent.id, generation, revision)? else {
            return Err(Error::new(
                "source_integrity",
                format!(
                    "retained agent_config revision {revision} for {} generation {generation} is missing",
                    agent.id
                ),
            ));
        };
        Ok(Some(AgentConfigView {
            revision: overlay.revision,
            application: overlay.application.clone(),
            provenance: overlay.provenance.clone(),
            profile: agent.profile.clone(),
            profile_inherited: agent.profile_inherited,
            requested_model: overlay.requested_model.clone(),
            requested_effort: overlay.requested_effort.clone(),
            model: overlay.model.clone(),
            effort: overlay.effort.clone(),
            original_model: agent
                .profile_snapshot
                .get("model")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            original_effort: agent
                .profile_snapshot
                .get("effort")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }))
    }

    /// Commit one dispatched `configure` command's overlay and stamp the
    /// command with its assigned revision. The revision is `max + 1` within
    /// the Agent generation, assigned here (not at admission) so concurrent
    /// admissions stay monotonic; `command_id` is unique so a reprocessed
    /// dispatch returns the retained row instead of bumping again.
    /// Settlement to `applied` happens after this returns. A pre-commit
    /// persistence error latches the Store like every other mutation, so
    /// the Router cannot settle the command `rejected` past the fault.
    pub fn apply_agent_config(&mut self, command_id: &str, now: i64) -> Result<AgentConfig> {
        self.ensure_writer_healthy()?;
        let result = self.apply_agent_config_inner(command_id, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn apply_agent_config_inner(&mut self, command_id: &str, now: i64) -> Result<AgentConfig> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let record = load_command_record(&tx, command_id)?.ok_or_else(|| {
            Error::new("not_found", format!("command {command_id} was not found"))
        })?;
        if record.command.verb != "configure" {
            return Err(Error::new(
                "invalid",
                "agent configuration applies only to configure commands",
            ));
        }
        if record.state != CommandState::Dispatched {
            return Err(Error::new(
                "bad_state",
                "configure applies only after a committed dispatch",
            ));
        }
        if let Some(existing) = tx
            .query_row(
                "SELECT agent, generation, revision FROM agent_config WHERE command_id = ?1",
                [command_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
        {
            let (agent, generation, revision) = existing;
            let config = get_agent_config_tx(&tx, agent, generation, revision)?
                .ok_or_else(|| Error::new("internal", "retained agent_config row disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(config);
        }
        let agent_num = record.target.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "configure command has no retained Agent target",
            )
        })?;
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "configure Agent was not found"))?;
        let generation = record.command.generation.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "configure command has no retained generation",
            )
        })?;
        if generation != agent.generation {
            return Err(Error::new(
                "stale_generation",
                format!(
                    "configure targets generation {generation}, current is {}",
                    agent.generation
                ),
            ));
        }
        if agent.is_terminal()
            || !matches!(
                agent.state,
                AgentState::Idle | AgentState::Busy | AgentState::Waiting
            )
        {
            return Err(Error::new(
                "bad_state",
                "configure requires a live Agent generation with a native session",
            ));
        }
        let (requested, effective, application) = admission::resolve_configure(
            &record.command.args,
            &agent.harness,
            &agent.profile_snapshot,
        )?;
        let revision = current_config_revision_tx(&tx, agent_num, generation)? + 1;
        let requested_json = encode_json(&requested)?;
        let effective_json = encode_json(&effective)?;
        db!(tx.execute(
            "INSERT INTO agent_config(agent, generation, revision, requested, effective, application, provenance, command_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'configure', ?7, ?8)",
            params![
                agent_num,
                generation,
                revision,
                requested_json,
                effective_json,
                application,
                command_id,
                now,
            ],
        ));
        db!(tx.execute(
            "UPDATE commands SET config_revision = ?1 WHERE id = ?2",
            params![revision, command_id],
        ));
        let config = get_agent_config_tx(&tx, agent_num, generation, revision)?
            .ok_or_else(|| Error::new("internal", "new agent_config row was not visible"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(config)
    }
}

fn load_turn(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Option<Turn>> {
    let record = connection
        .query_row(
            "SELECT agent, generation, turn_id, state, status, outcome, native_status,
                    final_text, started_at, completed_at, usage, config_revision
             FROM turns WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
            params![agent, generation, turn_id],
            turn_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    record
        .map(|record| {
            let harness = load_harness_by_num(connection, agent)?;
            turn_from_record(record, &harness)
        })
        .transpose()
}

/// Stored harness for one agent row, used to classify legacy turn rows that
/// carry no canonical outcome. Missing agent rows classify generically.
fn load_harness_by_num(connection: &Connection, agent: i64) -> Result<String> {
    connection
        .query_row("SELECT harness FROM agents WHERE id = ?1", [agent], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(sql_error)
        .map(|harness| harness.unwrap_or_default())
}

fn insert_turn(tx: &Transaction<'_>, turn: TurnInsert<'_>) -> Result<()> {
    db!(tx.execute(
        "INSERT INTO turns(agent, generation, turn_id, state, status, outcome, native_status,
                           final_text, started_at, completed_at, usage, config_revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            turn.agent,
            turn.generation,
            turn.turn_id,
            turn_state_name(turn.state),
            turn.status,
            turn.outcome,
            turn.native_status,
            turn.final_text,
            turn.started_at,
            turn.completed_at,
            turn.usage,
            turn.config_revision,
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
    now: i64,
) -> Result<()> {
    // The initial install is an empty open stream source. The validated
    // manifest plan is installed by `install_plugin_sources` after PluginHost
    // manifest validation and before `agent.start`, so launch/resume dispatch
    // must not choose families per harness here.
    db!(tx.execute(
        "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
         VALUES (?1, ?2, ?3, 'open', 'ok', ?4)",
        params![agent, generation, INITIAL_SOURCE, now],
    ));
    Ok(())
}

impl Store {
    /// Install the PluginHost-validated manifest source plan for a Starting
    /// generation, before its native start effect. The plan maps each common
    /// native family to its `stream` mechanism; ownership is written in one
    /// transaction that commits before the caller proceeds. An identical
    /// already-installed plan is a no-op; any conflicting installed ownership
    /// or already-committed native fact is refused. Only this
    /// `(agent, generation)` stream plan is considered, so confirmed plans of
    /// native children (other Agent rows) are never overwritten.
    pub fn install_plugin_sources(
        &mut self,
        agent: &str,
        generation: i64,
        start_command_id: &str,
        plan: &BTreeMap<String, String>,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result =
            self.install_plugin_sources_inner(agent, generation, start_command_id, plan, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn install_plugin_sources_inner(
        &mut self,
        agent: &str,
        generation: i64,
        start_command_id: &str,
        plan: &BTreeMap<String, String>,
        now: i64,
    ) -> Result<()> {
        if generation <= 0 {
            return Err(Error::new(
                "stale_generation",
                "install_plugin_sources requires an admitted generation",
            ));
        }
        if plan.len() > MAX_NATIVE_FAMILIES as usize {
            return Err(Error::new(
                "invalid",
                "plugin source plan exceeds its bound",
            ));
        }
        for (family, mechanism) in plan {
            if !source_handoff::is_native_family(family) {
                return Err(Error::new(
                    "unsupported",
                    format!("plugin source family {family} is not a common native family"),
                ));
            }
            if mechanism != INITIAL_SOURCE {
                return Err(Error::new(
                    "unsupported",
                    "plugin source plan mechanism must be stream",
                ));
            }
        }
        validate_ulid(start_command_id)?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
        observer::require_controlled(&current)?;
        if current.generation != generation || current.state != AgentState::Starting {
            return Err(Error::new(
                "stale_generation",
                "install_plugin_sources requires the current Starting generation",
            ));
        }
        let record = load_command_record(&tx, start_command_id)?
            .ok_or_else(|| Error::new("not_found", "plugin start command was not found"))?;
        if record.state != CommandState::Dispatched
            || record.target != Some(agent_num)
            || !matches!(record.command.verb.as_str(), "launch" | "resume")
            || record.command.generation != Some(generation)
        {
            return Err(Error::new(
                "stale_generation",
                "plugin source plan does not match the dispatched start command generation",
            ));
        }
        let mut statement = tx
            .prepare(
                "SELECT family, source, mechanism FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 ORDER BY family",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![agent_num, generation], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut existing = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        drop(statement);
        // Compare only registered native-family ownership: Monitoring may
        // already have installed OS/Git/daemon rows for this generation, and
        // those must be preserved untouched. A native family owned by any
        // other source or mechanism still conflicts below.
        existing.retain(|(family, _, _)| source_handoff::is_native_family(family));
        let source = load_source_state(&tx, agent_num, generation, INITIAL_SOURCE)?;
        let identical = existing.len() == plan.len()
            && existing.iter().all(|(family, owner, mechanism)| {
                owner == INITIAL_SOURCE && plan.get(family) == Some(mechanism)
            });
        if identical {
            match &source {
                Some(state) if state.state == "open" => {
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(());
                }
                Some(_) => {
                    return Err(Error::new(
                        "source_integrity",
                        "plugin source stream is already closed",
                    ));
                }
                None => {}
            }
        }
        if !existing.is_empty() {
            return Err(Error::new(
                "source_integrity",
                "plugin source plan conflicts with already installed ownership",
            ));
        }
        if let Some(state) = &source {
            if state.state != "open" {
                return Err(Error::new(
                    "source_integrity",
                    "plugin source stream is already closed",
                ));
            }
            if state.cursor.is_some()
                || state.last_batch_id.is_some()
                || state.last_batch_digest.is_some()
                || state.last_store_seq.is_some()
            {
                return Err(Error::new(
                    "source_integrity",
                    "native facts are already committed for this generation",
                ));
            }
        }
        if source.is_none() {
            db!(tx.execute(
                "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
                 VALUES (?1, ?2, ?3, 'open', 'ok', ?4)",
                params![agent_num, generation, INITIAL_SOURCE, now],
            ));
        }
        for (family, mechanism) in plan {
            db!(tx.execute(
                "INSERT INTO source_owners(agent, generation, family, source, mechanism)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_num, generation, family, INITIAL_SOURCE, mechanism],
            ));
        }
        commit_transaction(tx, &self.write_faulted)
    }
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

fn family_for_kind(kind: &str) -> Result<&'static str> {
    match kind {
        "session.ready" | "session.ended" | "session.usage" => Ok("session"),
        "turn.started" | "turn.completed" | "turn.unknown" => Ok("turn"),
        "message.completed" => Ok("message"),
        "tool.started" | "tool.completed" | "hook.before_tool" => Ok("tool"),
        "hook.before_model" => Ok("turn"),
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

/// Admission order of one command as `(created_at, rowid)`: the shared
/// settings-lane order for sends and configures. An earlier-admitted command
/// passes a later one, and a later command waits for an earlier one.
fn command_admission_key(connection: &Connection, id: &str) -> Result<(i64, i64)> {
    connection
        .query_row(
            "SELECT created_at, rowid FROM commands WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("internal", "queued command disappeared"))
}

/// Earliest not-yet-terminal `configure` for one Agent generation, with its
/// admission key. A dispatched or unknown configure holds the settings
/// lane, but only for later-admitted sends: an earlier send still dispatches
/// first so it never runs under newer configuration.
fn earliest_pending_configure_key(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<Option<(String, i64, i64)>> {
    connection
        .query_row(
            "SELECT id, created_at, rowid FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'configure'
               AND state IN ('accepted', 'queued', 'dispatched', 'unknown')
             ORDER BY created_at, rowid LIMIT 1",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)
}

/// Earliest send input that has not frozen its revision or association yet
/// (accepted, queued, or dispatched/unknown with no turn), with its
/// admission key. An earlier one holds the settings lane for a later
/// `configure` until it dispatches or its turn association lands.
fn earliest_unresolved_send_key(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<Option<(String, i64, i64)>> {
    connection
        .query_row(
            "SELECT id, created_at, rowid FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'send'
               AND (state IN ('accepted', 'queued')
                    OR (state IN ('dispatched', 'unknown') AND turn_id IS NULL))
             ORDER BY created_at, rowid LIMIT 1",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)
}

/// Current-generation `send` input awaiting `turn.started` (accepted,
/// queued, dispatched or unknown, no turn). Shared send-lane proof for
/// the notice claim and admission gates.
fn has_pending_send_input(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands
                 WHERE target = ?1 AND generation = ?2 AND verb = 'send'
                   AND state IN ('accepted', 'queued', 'dispatched', 'unknown')
                   AND turn_id IS NULL
             )",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

/// Unknown command or turn fencing notice presentation; shared by the
/// notice claim gate and mail admission revalidation.
fn has_unknown_gate(connection: &Connection, agent_num: i64) -> Result<bool> {
    let unknown_command: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands WHERE target = ?1 AND state = 'unknown'
             )",
            params![agent_num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if unknown_command {
        return Ok(true);
    }
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns WHERE agent = ?1 AND state = ?2
             )",
            params![agent_num, turn_state_name(TurnState::Unknown)],
            |row| row.get(0),
        )
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
            "SELECT id, verb, outcome FROM commands
             WHERE target = ?1 AND generation = ?2 AND state = 'dispatched'
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(sql_error)?;
    let mut dispatched = Vec::new();
    for row in rows {
        dispatched.push(row.map_err(sql_error)?);
    }
    drop(statement);
    for (id, verb, encoded_outcome) in dispatched {
        // A committed session.ended boundary proves the session stopped, but it
        // does not bind that end to an in-flight stop RPC. Keep a dispatched
        // stop pending for its exact agent.stop reply. Generic loss/restart
        // paths do not set this flag and continue to settle dispatched stops
        // as unknown.
        if preserve_dispatched_stop && verb == "stop" {
            continue;
        }
        let previous = encoded_outcome.as_deref().map(decode_json).transpose()?;
        let outcome = context::unknown_claim_outcome(previous.as_ref(), reason);
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
    if items.is_empty() {
        return Ok(());
    }
    // Linear sizing: serialize each candidate entry once (escaped JSON bytes)
    // and account for the enclosing brackets plus one comma per gap, matching
    // exactly how `serde_json` encodes `Vec<T>` (`[` entries `,` `]`).
    // The result is the longest fitting prefix, identical to reserializing
    // each shrinking prefix but without the quadratic work under the Store
    // mutex. `MAX_PAGE_BYTES` (900 KiB) leaves headroom for the 1 MiB
    // response frame envelope.
    let mut total = 2_usize;
    let mut keep = 0_usize;
    for (index, item) in items.iter().enumerate() {
        let len = encode_json(item)?.len();
        let next = total
            .saturating_add(len)
            .saturating_add(if index == 0 { 0 } else { 1 });
        if next > MAX_PAGE_BYTES {
            break;
        }
        total = next;
        keep = index + 1;
    }
    if keep == 0 {
        items.clear();
        return Err(Error::new(
            "frame_too_large",
            "one page entry exceeds the 900 KiB response bound",
        ));
    }
    items.truncate(keep);
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
