#![forbid(unsafe_code)]

//! Durable L1 state for Agents, commands, native events, turns, and source acks.

mod canonical;
pub mod error;

pub use crate::error::{Error, Result};
use eliot_core::{
    self as core, Agent, AgentState, BatchAck, Command, CommandState, Effect, Event, NativeEvent,
    SourceBatch, SubmitCommand, Turn, TurnState,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{cell::Cell, collections::HashSet, path::Path, time::Duration};
use ulid::Ulid;

const MAX_PAGE_ENTRIES: usize = 1_000;
const MAX_PAGE_BYTES: usize = 900 * 1024;
const MAX_EVENT_BYTES: usize = 64 * 1024;
const MAX_INPUT_BYTES: usize = 512 * 1024;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RETRY_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const COMMAND_METHOD: &str = "command.submit";
const INITIAL_SOURCE: &str = "stream";

macro_rules! db {
    ($expression:expr) => {
        $expression.map_err(sql_error)?
    };
}

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
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(sql_error)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;",
            )
            .map_err(sql_error)?;

        let mut store = Self {
            connection,
            write_faulted: Cell::new(false),
        };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&mut self) -> Result<()> {
        self.connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                     version INTEGER PRIMARY KEY
                 );",
            )
            .map_err(sql_error)?;
        let current: i64 = self
            .connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if current > 1 {
            return Err(Error::new(
                "unsupported",
                format!("Store schema version {current} is newer than this binary"),
            ));
        }
        if current == 1 {
            return Ok(());
        }

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        tx.execute_batch(
            "CREATE TABLE agents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 generation INTEGER NOT NULL DEFAULT 0,
                 name TEXT UNIQUE,
                 project TEXT NOT NULL,
                 harness TEXT NOT NULL,
                 profile TEXT NOT NULL,
                 workspace TEXT,
                 token TEXT UNIQUE,
                 state TEXT NOT NULL,
                 active_turn TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX agents_state ON agents(state);

             CREATE TABLE events (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 kind TEXT NOT NULL,
                 family TEXT,
                 agent INTEGER,
                 generation INTEGER,
                 source TEXT NOT NULL,
                 native_id TEXT,
                 cursor TEXT,
                 payload_digest BLOB,
                 data TEXT NOT NULL,
                 CHECK(payload_digest IS NULL OR length(payload_digest) = 32)
             );
             CREATE INDEX events_agent ON events(agent, seq);
             CREATE INDEX events_kind ON events(kind, seq);
             CREATE UNIQUE INDEX events_native
                 ON events(agent, generation, source, native_id)
                 WHERE native_id IS NOT NULL;

             CREATE TABLE commands (
                 id TEXT PRIMARY KEY,
                 principal TEXT NOT NULL,
                 method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL,
                 target INTEGER REFERENCES agents(id),
                 expected_generation INTEGER,
                 generation INTEGER,
                 expected_turn_id TEXT,
                 parent_command TEXT REFERENCES commands(id),
                 link_kind TEXT,
                 ordinal INTEGER,
                 turn_id TEXT,
                 verb TEXT NOT NULL,
                 args TEXT NOT NULL,
                 state TEXT NOT NULL,
                 outcome TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL)
                    OR (parent_command IS NOT NULL AND link_kind IS NOT NULL
                        AND ordinal IS NOT NULL AND ordinal >= 0))
             );
             CREATE INDEX commands_target ON commands(target, created_at);
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             CREATE UNIQUE INDEX commands_children
                 ON commands(parent_command, ordinal)
                 WHERE parent_command IS NOT NULL;

             CREATE TABLE turns (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 turn_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 status TEXT,
                 final_text TEXT,
                 started_at INTEGER,
                 completed_at INTEGER,
                 PRIMARY KEY(agent, generation, turn_id)
             );

             CREATE TABLE source_owners (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 family TEXT NOT NULL,
                 source TEXT NOT NULL,
                 mechanism TEXT NOT NULL,
                 PRIMARY KEY(agent, generation, family)
             );
             CREATE TABLE source_state (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 source TEXT NOT NULL,
                 cursor TEXT,
                 last_store_seq INTEGER,
                 last_batch_id TEXT,
                 last_batch_digest BLOB,
                 last_batch_first_seq INTEGER,
                 last_batch_last_seq INTEGER,
                 state TEXT NOT NULL CHECK(state IN ('open', 'closed')),
                 health TEXT NOT NULL CHECK(health IN ('ok', 'slow', 'stale', 'error')),
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY(agent, generation, source),
                 CHECK(last_batch_digest IS NULL OR length(last_batch_digest) = 32)
             );

             CREATE TABLE writes (
                 id TEXT PRIMARY KEY,
                 principal TEXT NOT NULL,
                 method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL,
                 result TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             INSERT INTO schema_migrations(version) VALUES (1);",
        )
        .map_err(sql_error)?;
        tx.commit().map_err(sql_error)
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
        let result = self.submit_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn submit_inner(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        now: i64,
    ) -> Result<Admission> {
        require_nonempty(principal, "principal")?;
        if !request.args.is_object() {
            return Err(Error::new("invalid", "command args must be a JSON object"));
        }
        if encode_json(&request)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "command request exceeds 1 MiB",
            ));
        }
        validate_ulid(&request.id)?;
        let semantic_payload = json!({
            "target": request.target,
            "verb": request.verb,
            "args": request.args,
            "expected_generation": request.expected_generation,
            "expected_turn_id": request.expected_turn_id,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        if let Some((old_principal, old_method, old_digest)) = command_binding(&tx, &request.id)? {
            if old_principal != principal
                || old_method != COMMAND_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "command id is already bound to another principal, method, or payload",
                ));
            }
            let command = load_command(&tx, &request.id)?.ok_or_else(|| {
                Error::new("internal", "command binding exists without its command row")
            })?;
            let linked_command = load_linked_command(&tx, &request.id)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Admission {
                command,
                linked_command,
                events: Vec::new(),
                is_new: false,
            });
        }
        if write_exists(&tx, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "mutation id is already used by another write",
            ));
        }
        validate_id_age(&request.id, now)?;

        let mut events = Vec::new();
        let linked = match request.verb.as_str() {
            "launch" => {
                if request.target.is_some() {
                    return Err(Error::new("invalid", "launch does not take a target"));
                }
                if request.expected_turn_id.is_some()
                    || request
                        .expected_generation
                        .is_some_and(|expected| expected != 0)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "a new launch starts from generation 0",
                    ));
                }
                let project = required_string(&request.args, "project")?;
                let profile = required_string(&request.args, "profile")?;
                let workspace = required_string(&request.args, "workspace")?;
                let harness = request
                    .args
                    .get("harness")
                    .and_then(Value::as_str)
                    .unwrap_or("fake");
                if profile != "fake" || harness != "fake" {
                    return Err(Error::new(
                        "unsupported",
                        "L1 launch currently supports the fake profile and harness",
                    ));
                }
                if request.args.get("prompt").is_some() {
                    return Err(Error::new(
                        "invalid",
                        "use args.text for the linked launch input",
                    ));
                }
                let text = optional_string(&request.args, "text")?;
                if let Some(text) = &text {
                    require_nonempty(text, "args.text")?;
                    check_input(text)?;
                }
                check_rpc_frame(
                    "agent.start",
                    &json!({
                        "agent_id": "a1",
                        "generation": 1,
                        "command_id": request.id,
                        "profile": profile,
                        "workspace": workspace,
                        "source_plan": {"session": "stream", "turn": "stream", "message": "stream"}
                    }),
                )?;

                db!(tx.execute(
                    "INSERT INTO agents(generation, project, harness, profile, workspace,
                                            state, created_at, updated_at)
                         VALUES (0, ?1, 'fake', 'fake', ?2, 'created', ?3, ?3)",
                    params![project, workspace, now],
                ));
                let agent_num = tx.last_insert_rowid();
                let agent_id = format_agent_id(agent_num);
                events.push(insert_daemon_event(
                        &tx,
                        now,
                        "agent.created",
                        "agent",
                        Some(agent_num),
                        Some(0),
                        json!({"agent_id": agent_id, "project": project, "harness": "fake", "profile": "fake"}),
                    )?);

                let state = CommandState::Accepted;
                insert_command(
                    &tx,
                    &CommandInsert {
                        id: &request.id,
                        principal,
                        method: COMMAND_METHOD,
                        payload_digest: &payload_digest,
                        target: Some(agent_num),
                        expected_generation: request.expected_generation,
                        generation: Some(0),
                        expected_turn_id: None,
                        parent_command: None,
                        link_kind: None,
                        ordinal: None,
                        turn_id: None,
                        verb: "launch",
                        args: &request.args,
                        state,
                        outcome: None,
                        created_at: now,
                        updated_at: now,
                    },
                )?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.accepted",
                    "command",
                    Some(agent_num),
                    Some(0),
                    json!({"command_id": request.id, "verb": "launch"}),
                )?);

                if let Some(text) = text {
                    let linked_id = format!("{}/send", request.id);
                    let linked_args = json!({"text": text});
                    check_rpc_frame(
                        "agent.command",
                        &json!({
                            "agent_id": agent_id,
                            "generation": 1,
                            "command_id": linked_id,
                            "verb": "send",
                            "args": linked_args,
                        }),
                    )?;
                    let linked_semantic = json!({
                        "target": agent_id,
                        "verb": "send",
                        "args": linked_args,
                        "expected_generation": 1,
                        "expected_turn_id": Value::Null,
                    });
                    let linked_digest = canonical::digest(&linked_semantic)?;
                    insert_command(
                        &tx,
                        &CommandInsert {
                            id: &linked_id,
                            principal,
                            method: COMMAND_METHOD,
                            payload_digest: &linked_digest,
                            target: Some(agent_num),
                            expected_generation: Some(1),
                            generation: Some(1),
                            expected_turn_id: None,
                            parent_command: Some(&request.id),
                            link_kind: Some("send"),
                            ordinal: Some(0),
                            turn_id: None,
                            verb: "send",
                            args: &linked_args,
                            state: CommandState::Queued,
                            outcome: None,
                            created_at: now,
                            updated_at: now,
                        },
                    )?;
                    events.push(insert_daemon_event(
                            &tx,
                            now,
                            "command.queued",
                            "command",
                            Some(agent_num),
                            Some(1),
                            json!({"command_id": linked_id, "parent_command": request.id, "verb": "send"}),
                        )?);
                    Some(linked_id)
                } else {
                    None
                }
            }
            "send" | "stop" => {
                let target = request.target.as_deref().ok_or_else(|| {
                    Error::new("invalid", "send and stop require an Agent target")
                })?;
                let agent_num = parse_agent_id(target)?;
                let agent = load_agent_by_num(&tx, agent_num)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {target} was not found"))
                })?;
                if let Some(expected) = request.expected_generation
                    && expected != agent.generation
                {
                    return Err(Error::new(
                        "stale_generation",
                        format!(
                            "expected generation {expected}, current is {}",
                            agent.generation
                        ),
                    ));
                }
                if let Some(expected_turn) = request.expected_turn_id.as_deref()
                    && agent.active_turn.as_deref() != Some(expected_turn)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "expected turn does not match the current Agent turn",
                    ));
                }
                if request.verb == "send" {
                    if agent.generation == 0 || agent.state == AgentState::Created {
                        return Err(Error::new(
                            "bad_state",
                            "dispatch launch before submitting Agent input",
                        ));
                    }
                    if agent.is_terminal() {
                        return Err(Error::new("bad_state", "cannot send to a terminal Agent"));
                    }
                    let text = required_string(&request.args, "text")?;
                    check_input(&text)?;
                    check_rpc_frame(
                        "agent.command",
                        &json!({
                            "agent_id": agent.id,
                            "generation": agent.generation,
                            "command_id": request.id,
                            "verb": "send",
                            "args": request.args,
                        }),
                    )?;
                    let ready = send_gate_open(&tx, agent_num, agent.generation)?
                        && earliest_pending_send(&tx, agent_num, agent.generation)?.is_none();
                    let state = if ready {
                        CommandState::Accepted
                    } else {
                        CommandState::Queued
                    };
                    insert_command(
                        &tx,
                        &CommandInsert {
                            id: &request.id,
                            principal,
                            method: COMMAND_METHOD,
                            payload_digest: &payload_digest,
                            target: Some(agent_num),
                            expected_generation: request.expected_generation,
                            generation: Some(agent.generation),
                            expected_turn_id: request.expected_turn_id.as_deref(),
                            parent_command: None,
                            link_kind: None,
                            ordinal: None,
                            turn_id: None,
                            verb: "send",
                            args: &request.args,
                            state,
                            outcome: None,
                            created_at: now,
                            updated_at: now,
                        },
                    )?;
                    events.push(insert_daemon_event(
                        &tx,
                        now,
                        if state == CommandState::Accepted {
                            "command.accepted"
                        } else {
                            "command.queued"
                        },
                        "command",
                        Some(agent_num),
                        Some(agent.generation),
                        json!({"command_id": request.id, "verb": "send"}),
                    )?);
                    None
                } else {
                    if request
                        .args
                        .get("force")
                        .is_some_and(|value| !value.is_boolean())
                    {
                        return Err(Error::new("invalid", "stop force must be a boolean"));
                    }
                    check_rpc_frame(
                        "agent.stop",
                        &json!({
                            "agent_id": agent.id,
                            "generation": agent.generation,
                            "command_id": request.id,
                            "force": request.args.get("force").and_then(Value::as_bool).unwrap_or(false),
                        }),
                    )?;
                    let state = CommandState::Accepted;
                    insert_command(
                        &tx,
                        &CommandInsert {
                            id: &request.id,
                            principal,
                            method: COMMAND_METHOD,
                            payload_digest: &payload_digest,
                            target: Some(agent_num),
                            expected_generation: request.expected_generation,
                            generation: Some(agent.generation),
                            expected_turn_id: request.expected_turn_id.as_deref(),
                            parent_command: None,
                            link_kind: None,
                            ordinal: None,
                            turn_id: request.expected_turn_id.as_deref(),
                            verb: "stop",
                            args: &request.args,
                            state,
                            outcome: None,
                            created_at: now,
                            updated_at: now,
                        },
                    )?;
                    events.push(insert_daemon_event(
                        &tx,
                        now,
                        "command.accepted",
                        "command",
                        Some(agent_num),
                        Some(agent.generation),
                        json!({"command_id": request.id, "verb": "stop"}),
                    )?);
                    None
                }
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("L1 Store does not admit the {verb} verb"),
                ));
            }
        };

        let command = load_command(&tx, &request.id)?.ok_or_else(|| {
            Error::new("internal", "new command was not visible in its transaction")
        })?;
        let linked_command = match linked.as_deref() {
            Some(linked_id) => load_command(&tx, linked_id)?,
            None => None,
        };
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Admission {
            command,
            linked_command,
            events,
            is_new: true,
        })
    }

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
        let result = self.dispatch_inner(id, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn dispatch_inner(&mut self, id: &str, now: i64) -> Result<Change<Command>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut command = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        if !matches!(command.state, CommandState::Accepted | CommandState::Queued) {
            return Err(Error::new(
                "bad_state",
                format!(
                    "command {} in state {:?} cannot be dispatched",
                    command.command.id, command.state
                ),
            ));
        }
        let agent_num = command.target.ok_or_else(|| {
            Error::new("bad_state", "L1 dispatch requires a concrete Agent target")
        })?;
        let mut events = Vec::new();
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "command target Agent was not found"))?;
        let command_generation = command.command.generation.unwrap_or(agent.generation);

        match command.command.verb.as_str() {
            "launch" => {
                if command.state != CommandState::Accepted
                    || agent.generation != 0
                    || command_generation != 0
                    || agent.state != AgentState::Created
                {
                    return Err(Error::new(
                        "stale_generation",
                        "launch no longer targets the initial created Agent generation",
                    ));
                }
                check_rpc_frame(
                    "agent.start",
                    &json!({
                        "agent_id": agent.id,
                        "generation": 1,
                            "command_id": id,
                        "profile": agent.profile,
                        "workspace": agent.workspace,
                        "source_plan": {"session": "stream", "turn": "stream", "message": "stream"}
                    }),
                )?;
                let token = format!("{}{}", Ulid::new(), Ulid::new());
                let changed = db!(tx.execute(
                    "UPDATE agents SET generation = 1, state = 'starting', token = ?1,
                                         updated_at = ?2
                     WHERE id = ?3 AND generation = 0 AND state = 'created'",
                    params![token, now, agent_num],
                ));
                if changed != 1 {
                    return Err(Error::new(
                        "stale_generation",
                        "launch target changed before its generation token was committed",
                    ));
                }
                install_initial_source_plan(&tx, agent_num, 1, now)?;
                update_command_state(&tx, id, CommandState::Dispatched, None, Some(1), now)?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "agent.starting",
                    "agent",
                    Some(agent_num),
                    Some(1),
                    json!({"agent_id": agent.id, "command_id": id}),
                )?);
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(1),
                    json!({"command_id": id, "verb": "launch"}),
                )?);
            }
            "send" => {
                if command_generation != agent.generation {
                    return Err(Error::new(
                        "stale_generation",
                        format!(
                            "command targets generation {command_generation}, current is {}",
                            agent.generation
                        ),
                    ));
                }
                if command
                    .expected_turn_id
                    .as_deref()
                    .is_some_and(|expected| agent.active_turn.as_deref() != Some(expected))
                {
                    return Err(Error::new(
                        "stale_generation",
                        "send command's captured turn is no longer current",
                    ));
                }
                if agent.is_terminal() {
                    return Err(Error::new(
                        "bad_state",
                        "cannot dispatch input to a terminal Agent",
                    ));
                }
                let ready = send_gate_open(&tx, agent_num, agent.generation)?;
                let earliest = earliest_pending_send(&tx, agent_num, agent.generation)?;
                if !ready || earliest.as_deref().is_some_and(|first| first != id) {
                    if command.state == CommandState::Accepted {
                        update_command_state(&tx, id, CommandState::Queued, None, None, now)?;
                        events.push(insert_daemon_event(
                            &tx,
                            now,
                            "command.queued",
                            "command",
                            Some(agent_num),
                            Some(agent.generation),
                            json!({"command_id": id, "verb": "send"}),
                        )?);
                    }
                    command = load_command_record(&tx, id)?
                        .ok_or_else(|| Error::new("internal", "queued command disappeared"))?;
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: command.command,
                        events,
                    });
                }
                check_rpc_frame(
                    "agent.command",
                    &json!({
                        "agent_id": agent.id,
                        "generation": agent.generation,
                        "command_id": id,
                        "verb": "send",
                        "args": command.command.args,
                    }),
                )?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(agent.generation),
                    json!({"command_id": id, "verb": "send"}),
                )?);
            }
            "stop" => {
                if command_generation != agent.generation {
                    return Err(Error::new(
                        "stale_generation",
                        format!(
                            "command targets generation {command_generation}, current is {}",
                            agent.generation
                        ),
                    ));
                }
                if command
                    .expected_turn_id
                    .as_deref()
                    .is_some_and(|expected| agent.active_turn.as_deref() != Some(expected))
                {
                    return Err(Error::new(
                        "stale_generation",
                        "stop command's captured turn is no longer current",
                    ));
                }
                check_rpc_frame(
                    "agent.stop",
                    &json!({
                        "agent_id": agent.id,
                        "generation": agent.generation,
                        "command_id": id,
                        "force": command.command.args.get("force").and_then(Value::as_bool).unwrap_or(false),
                    }),
                )?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(agent.generation),
                    json!({"command_id": id, "verb": "stop"}),
                )?);
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("L1 Store does not dispatch the {verb} verb"),
                ));
            }
        }

        command = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: command.command,
            events,
        })
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
        let result = self.settle_inner(id, state, outcome, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn settle_inner(
        &mut self,
        id: &str,
        state: CommandState,
        outcome: Option<Value>,
        now: i64,
    ) -> Result<Change<Command>> {
        if !matches!(
            state,
            CommandState::Applied
                | CommandState::Rejected
                | CommandState::Failed
                | CommandState::Unknown
        ) {
            return Err(Error::new(
                "invalid",
                "settlement state is not a result state",
            ));
        }
        if let Some(outcome) = &outcome
            && encode_json(outcome)?.len() > MAX_EVENT_BYTES
        {
            return Err(Error::new(
                "frame_too_large",
                "command outcome exceeds 64 KiB",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        if matches!(
            record.state,
            CommandState::Applied
                | CommandState::Rejected
                | CommandState::Failed
                | CommandState::Cancelled
        ) {
            if record.state == state {
                if state == CommandState::Applied && record.command.verb == "send" {
                    if outcome
                        .as_ref()
                        .and_then(|value| value.get("turn_id"))
                        .and_then(Value::as_str)
                        .is_some_and(|turn_id| record.command.turn_id.as_deref() != Some(turn_id))
                    {
                        return Err(Error::new(
                            "source_integrity",
                            "settlement turn id conflicts with the committed send association",
                        ));
                    }
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: record.command,
                        events: Vec::new(),
                    });
                }
                if outcome.is_none() || record.command.outcome == outcome {
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: record.command,
                        events: Vec::new(),
                    });
                }
            }
            if matches!(
                record.state,
                CommandState::Applied
                    | CommandState::Rejected
                    | CommandState::Failed
                    | CommandState::Cancelled
            ) {
                return Err(Error::new(
                    "bad_state",
                    format!(
                        "command {} already has terminal state {:?}",
                        id, record.state
                    ),
                ));
            }
        }
        if record.state == CommandState::Unknown
            && state == CommandState::Unknown
            && (outcome.is_none() || record.command.outcome == outcome)
        {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: record.command,
                events: Vec::new(),
            });
        }
        if matches!(record.state, CommandState::Accepted | CommandState::Queued)
            && state != CommandState::Rejected
        {
            return Err(Error::new(
                "bad_state",
                "an undispatched command can only be rejected before native I/O",
            ));
        }
        if state == CommandState::Applied
            && record.command.verb == "send"
            && record.command.turn_id.is_none()
        {
            return Err(Error::new(
                "bad_state",
                "send is applied only after exact turn association is committed",
            ));
        }

        let target = record.target;
        let generation = record.command.generation;
        update_command_state(&tx, id, state, outcome.as_ref(), None, now)?;
        let mut events = Vec::new();
        let state_name = command_state_name(state);
        events.push(insert_daemon_event(
            &tx,
            now,
            &format!("command.{state_name}"),
            "command",
            target,
            generation,
            json!({"command_id": id, "state": state_name, "outcome": outcome}),
        )?);

        if record.command.verb == "launch"
            && matches!(state, CommandState::Rejected | CommandState::Failed)
        {
            if let (Some(agent_num), Some(generation)) = (target, generation) {
                fail_start_tx(&tx, agent_num, generation, id, now, &mut events)?;
            }
        } else if record.command.verb == "stop"
            && state == CommandState::Applied
            && let (Some(agent_num), Some(generation)) = (target, generation)
        {
            stop_agent_tx(&tx, agent_num, generation, id, now, &mut events)?;
        }

        record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: record.command,
            events,
        })
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
                "SELECT token FROM agents WHERE id = ?1 AND generation = ?2",
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
                "SELECT id, generation, name, project, harness, profile, workspace,
                        state, active_turn, created_at, updated_at
                 FROM agents WHERE id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after_num, limit as i64], agent_record_from_row)
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(agent_from_record(row.map_err(sql_error)?)?);
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
                "SELECT id, generation, name, project, harness, profile, workspace,
                        state, active_turn, created_at, updated_at
                 FROM agents
                 WHERE state IN ('created', 'starting', 'idle', 'busy', 'waiting')
                 ORDER BY id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([limit as i64], agent_record_from_row)
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(agent_from_record(row.map_err(sql_error)?)?);
        }
        enforce_page_bytes(&mut agents)?;
        Ok(agents)
    }

    /// Return unsettled commands without letting terminal history consume the limit.
    pub fn list_unsettled_commands(&self, limit: usize) -> Result<Vec<Command>> {
        validate_limit(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, verb, args, state, outcome,
                        created_at, updated_at
                 FROM commands
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown')
                 ORDER BY created_at, rowid LIMIT ?1",
            )
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
                        started_at, completed_at
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
        let mut statement = self
            .connection
            .prepare(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data
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
            events.push(insert_daemon_event(
                &tx,
                now,
                "command.unknown",
                "command",
                target,
                generation,
                json!({"command_id": command_id, "reason": "daemon_restart"}),
            )?);
        }

        let mut statement = tx
            .prepare(
                "SELECT id, generation FROM agents
                 WHERE state IN ('starting', 'idle', 'busy', 'waiting') ORDER BY id",
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
            transition_lost_tx(
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
        transition_lost_tx(
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

struct AgentRecord {
    id: i64,
    generation: i64,
    name: Option<String>,
    project: String,
    harness: String,
    profile: String,
    workspace: Option<String>,
    state: String,
    active_turn: Option<String>,
    created_at: i64,
    updated_at: i64,
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
}

struct SourceStateRecord {
    cursor: Option<String>,
    last_batch_id: Option<String>,
    last_batch_digest: Option<Vec<u8>>,
    last_batch_first_seq: Option<i64>,
    last_batch_last_seq: Option<i64>,
    state: String,
}

struct CommandInsert<'a> {
    id: &'a str,
    principal: &'a str,
    method: &'a str,
    payload_digest: &'a [u8],
    target: Option<i64>,
    expected_generation: Option<i64>,
    generation: Option<i64>,
    expected_turn_id: Option<&'a str>,
    parent_command: Option<&'a str>,
    link_kind: Option<&'a str>,
    ordinal: Option<i64>,
    turn_id: Option<&'a str>,
    verb: &'a str,
    args: &'a Value,
    state: CommandState,
    outcome: Option<&'a Value>,
    created_at: i64,
    updated_at: i64,
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
    data: Value,
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
}

fn agent_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRecord> {
    Ok(AgentRecord {
        id: row.get(0)?,
        generation: row.get(1)?,
        name: row.get(2)?,
        project: row.get(3)?,
        harness: row.get(4)?,
        profile: row.get(5)?,
        workspace: row.get(6)?,
        state: row.get(7)?,
        active_turn: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn agent_from_record(record: AgentRecord) -> Result<Agent> {
    Ok(Agent {
        id: format_agent_id(record.id),
        generation: record.generation,
        name: record.name,
        project: record.project,
        harness: record.harness,
        profile: record.profile,
        workspace: record.workspace,
        state: parse_enum(&record.state, "Agent state")?,
        active_turn: record.active_turn,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn load_agent_by_num(connection: &Connection, id: i64) -> Result<Option<Agent>> {
    let record = connection
        .query_row(
            "SELECT id, generation, name, project, harness, profile, workspace,
                    state, active_turn, created_at, updated_at
             FROM agents WHERE id = ?1",
            [id],
            agent_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    record.map(agent_from_record).transpose()
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
        verb: row.get(8)?,
        args: row.get(9)?,
        state: row.get(10)?,
        outcome: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
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
    verb: String,
    args: String,
    state: String,
    outcome: Option<String>,
    created_at: i64,
    updated_at: i64,
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
            expected_generation: raw.expected_generation,
            generation: raw.generation,
            args: decode_json(&raw.args)?,
            state,
            turn_id: raw.turn_id,
            outcome: raw.outcome.as_deref().map(decode_json).transpose()?,
            created_at: raw.created_at,
            updated_at: raw.updated_at,
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
                    generation, expected_turn_id, turn_id, verb, args,
                    state, outcome, created_at, updated_at
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
            "SELECT id FROM commands WHERE parent_command = ?1 ORDER BY ordinal LIMIT 1",
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

fn insert_command(tx: &Transaction<'_>, command: &CommandInsert<'_>) -> Result<()> {
    let args = encode_json(command.args)?;
    let outcome = command.outcome.map(encode_json).transpose()?;
    db!(tx.execute(
        "INSERT INTO commands(
             id, principal, method, payload_digest, target, expected_generation,
             generation, expected_turn_id, parent_command, link_kind, ordinal,
             turn_id, verb, args, state, outcome, created_at, updated_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18
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
            command.verb,
            args,
            command_state_name(command.state),
            outcome,
            command.created_at,
            command.updated_at,
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
    insert_event(
        tx,
        EventInsert {
            ts,
            kind,
            family: Some(family),
            agent,
            generation,
            source: "daemon",
            native_id: None,
            cursor: None,
            payload_digest: None,
            data,
        },
    )
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
    db!(tx.execute(
        "INSERT INTO events(ts, kind, family, agent, generation, source, native_id,
                            cursor, payload_digest, data)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
    })
}

fn event_from_record(record: EventRecord) -> Result<Event> {
    Ok(Event {
        seq: record.seq,
        ts: record.ts,
        kind: record.kind,
        agent: record.agent.map(format_agent_id),
        generation: record.generation,
        source: record.source,
        native_id: record.native_id,
        data: decode_json(&record.data)?,
    })
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
    })
}

fn turn_from_record(record: TurnRecord) -> Result<Turn> {
    Ok(Turn {
        agent: format_agent_id(record.agent),
        generation: record.generation,
        turn_id: record.turn_id,
        state: parse_enum(&record.state, "turn state")?,
        status: record.status,
        final_text: record.final_text,
        started_at: record.started_at,
        completed_at: record.completed_at,
    })
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
                    started_at, completed_at
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
                           started_at, completed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            turn.agent,
            turn.generation,
            turn.turn_id,
            turn_state_name(turn.state),
            turn.status,
            turn.final_text,
            turn.started_at,
            turn.completed_at,
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
            "SELECT cursor, last_batch_id, last_batch_digest,
                    last_batch_first_seq, last_batch_last_seq, state
             FROM source_state WHERE agent = ?1 AND generation = ?2 AND source = ?3",
            params![agent, generation, source],
            |row| {
                Ok(SourceStateRecord {
                    cursor: row.get(0)?,
                    last_batch_id: row.get(1)?,
                    last_batch_digest: row.get(2)?,
                    last_batch_first_seq: row.get(3)?,
                    last_batch_last_seq: row.get(4)?,
                    state: row.get(5)?,
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
    for family in ["session", "turn", "message"] {
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
        "session.ready" | "session.ended" => Ok("session"),
        "turn.started" | "turn.completed" => Ok("turn"),
        "message.completed" => Ok("message"),
        "request.opened" | "request.resolved" => Ok("request"),
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

fn pending_request_ids(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn_id: &str,
) -> Result<HashSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT kind, data FROM events
             WHERE agent = ?1 AND generation = ?2
               AND kind IN ('request.opened', 'request.resolved')
             ORDER BY seq",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent, generation], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?;
    let mut pending = HashSet::new();
    for row in rows {
        let (kind, data) = row.map_err(sql_error)?;
        let data = decode_json(&data)?;
        if data.get("turn_id").and_then(Value::as_str) != Some(turn_id) {
            continue;
        }
        if let Some(request_id) = data.get("request_id").and_then(Value::as_str) {
            if kind == "request.opened" {
                pending.insert(request_id.to_owned());
            } else {
                pending.remove(request_id);
            }
        }
    }
    Ok(pending)
}

fn persist_effects(
    tx: &Transaction<'_>,
    effects: &[Effect],
    agent_num: i64,
    generation: i64,
    now: i64,
) -> Result<()> {
    for effect in effects {
        match effect {
            Effect::CommandTurnAssociated {
                command_id,
                turn_id,
            } => {
                let command = load_command_record(tx, command_id)?.ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "turn.started references an unknown command",
                    )
                })?;
                require_effect_command(&command, agent_num, generation, "send", command_id)?;
                if !matches!(
                    command.command.state,
                    CommandState::Dispatched | CommandState::Unknown
                ) || command.command.turn_id.is_some()
                    || command
                        .expected_turn_id
                        .as_deref()
                        .is_some_and(|expected| expected != turn_id)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "turn.started does not match an unresolved send command and expected turn",
                    ));
                }
                if load_turn(tx, agent_num, generation, turn_id)?.is_some() {
                    return Err(Error::new(
                        "source_integrity",
                        format!("turn {turn_id} already exists in this Agent generation"),
                    ));
                }
                insert_turn(
                    tx,
                    TurnInsert {
                        agent: agent_num,
                        generation,
                        turn_id,
                        state: TurnState::Running,
                        status: None,
                        final_text: None,
                        started_at: Some(now),
                        completed_at: None,
                    },
                )?;
                let outcome = json!({"turn_id": turn_id});
                db!(tx.execute(
                    "UPDATE commands SET state = 'applied', turn_id = ?1, outcome = ?2,
                                         updated_at = ?3 WHERE id = ?4",
                    params![turn_id, encode_json(&outcome)?, now, command_id],
                ));
            }
            Effect::RequestPending { .. } => {
                // Request state is represented by durable native events. L1 has no
                // request-reply caller or separate request table.
            }
            Effect::MessageCompleted {
                command_id,
                turn_id,
                text,
            } => {
                if text.len() > MAX_EVENT_BYTES {
                    return Err(Error::new(
                        "frame_too_large",
                        "final message exceeds 64 KiB",
                    ));
                }
                let command = load_command_record(tx, command_id)?.ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "message.completed references an unknown command",
                    )
                })?;
                require_effect_command(&command, agent_num, generation, "send", command_id)?;
                if command.command.turn_id.as_deref() != Some(turn_id)
                    || command.command.state != CommandState::Applied
                {
                    return Err(Error::new(
                        "source_integrity",
                        "message.completed is not bound to the command's exact active turn",
                    ));
                }
                let changed = tx
                    .execute(
                        "UPDATE turns SET final_text = ?1
                         WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4
                           AND state = 'running'",
                        params![text, agent_num, generation, turn_id],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(Error::new(
                        "source_integrity",
                        "message.completed does not target the exact running turn",
                    ));
                }
            }
            Effect::TurnTerminal {
                command_id,
                turn_id,
                state,
                status,
                final_text,
                completed_at,
            } => {
                if let Some(text) = final_text
                    && text.len() > MAX_EVENT_BYTES
                {
                    return Err(Error::new(
                        "frame_too_large",
                        "turn final text exceeds 64 KiB",
                    ));
                }
                if let Some(command_id) = command_id {
                    let command = load_command_record(tx, command_id)?.ok_or_else(|| {
                        Error::new(
                            "source_integrity",
                            "turn terminal event references an unknown command",
                        )
                    })?;
                    require_effect_command(&command, agent_num, generation, "send", command_id)?;
                    if command.command.turn_id.as_deref() != Some(turn_id) {
                        return Err(Error::new(
                            "source_integrity",
                            "turn terminal command is not associated with this exact turn",
                        ));
                    }
                }
                let changed = tx
                    .execute(
                        "UPDATE turns SET state = ?1, status = COALESCE(?2, status),
                                          final_text = COALESCE(?3, final_text), completed_at = ?4
                         WHERE agent = ?5 AND generation = ?6 AND turn_id = ?7",
                        params![
                            turn_state_name(*state),
                            status,
                            final_text,
                            completed_at,
                            agent_num,
                            generation,
                            turn_id,
                        ],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(Error::new(
                        "source_integrity",
                        format!("terminal event references missing turn {turn_id}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn require_effect_command(
    command: &CommandRecord,
    agent: i64,
    generation: i64,
    verb: &str,
    command_id: &str,
) -> Result<()> {
    if command.target != Some(agent)
        || command.command.generation != Some(generation)
        || command.command.verb != verb
    {
        return Err(Error::new(
            "source_integrity",
            format!("native event command {command_id} is bound to another Agent or generation"),
        ));
    }
    Ok(())
}

fn update_agent_projection(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    let agent_num = parse_agent_id(&agent.id)?;
    let changed = tx
        .execute(
            "UPDATE agents SET generation = ?1, state = ?2, active_turn = ?3,
                               updated_at = ?4,
                               token = CASE WHEN ?5 THEN NULL ELSE token END
             WHERE id = ?6",
            params![
                agent.generation,
                agent_state_name(agent.state),
                agent.active_turn,
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

fn transition_lost_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    reason: &str,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "Agent was not found"));
    };
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "loss transition targets a stale generation",
        ));
    }
    if agent.is_terminal() {
        return Ok(());
    }

    let dispatched = command_ids_in_state(tx, agent_num, generation, "dispatched")?;
    for command_id in dispatched {
        let outcome = json!({"reason": reason});
        update_command_state(
            tx,
            &command_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.unknown",
            "command",
            Some(agent_num),
            Some(generation),
            json!({"command_id": command_id, "reason": reason}),
        )?);
    }
    let pending = pending_command_ids(tx, agent_num, generation)?;
    for command_id in pending {
        let outcome = json!({"reason": "generation_lost"});
        update_command_state(
            tx,
            &command_id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.cancelled",
            "command",
            Some(agent_num),
            Some(generation),
            json!({"command_id": command_id, "reason": "generation_lost"}),
        )?);
    }

    if let Some(turn_id) = agent.active_turn.as_deref() {
        let exists = load_turn(tx, agent_num, generation, turn_id)?.is_some();
        if exists {
            db!(tx.execute(
                "UPDATE turns SET state = 'unknown', completed_at = ?1
                 WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4",
                params![now, agent_num, generation, turn_id],
            ));
        } else {
            insert_turn(
                tx,
                TurnInsert {
                    agent: agent_num,
                    generation,
                    turn_id,
                    state: TurnState::Unknown,
                    status: None,
                    final_text: None,
                    started_at: None,
                    completed_at: Some(now),
                },
            )?;
        }
        events.push(insert_daemon_event(
            tx,
            now,
            "turn.unknown",
            "turn",
            Some(agent_num),
            Some(generation),
            json!({"turn_id": turn_id, "reason": reason}),
        )?);
    }

    db!(tx.execute(
        "UPDATE agents SET state = 'lost', active_turn = NULL, token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'stale', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.lost",
        "agent",
        Some(agent_num),
        Some(generation),
        json!({"agent_id": agent.id, "reason": reason}),
    )?);
    Ok(())
}

fn fail_start_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "launch target Agent was not found"));
    };
    if agent.generation != generation || agent.state != AgentState::Starting {
        return Ok(());
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        let outcome = json!({"reason": "launch_failed"});
        update_command_state(
            tx,
            &pending_id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.cancelled",
            "command",
            Some(agent_num),
            Some(generation),
            json!({"command_id": pending_id, "reason": "launch_failed"}),
        )?);
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'failed', active_turn = NULL, token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'error', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.failed",
        "agent",
        Some(agent_num),
        Some(generation),
        json!({"agent_id": agent.id, "command_id": command_id, "reason": "launch_failed"}),
    )?);
    Ok(())
}

fn stop_agent_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "stop target Agent was not found"));
    };
    if agent.generation != generation || agent.is_terminal() {
        return Ok(());
    }
    for dispatched_id in command_ids_in_state(tx, agent_num, generation, "dispatched")? {
        if dispatched_id == command_id {
            continue;
        }
        let outcome = json!({"reason": "generation_stopped"});
        update_command_state(
            tx,
            &dispatched_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.unknown",
            "command",
            Some(agent_num),
            Some(generation),
            json!({"command_id": dispatched_id, "reason": "generation_stopped"}),
        )?);
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        if pending_id == command_id {
            continue;
        }
        let outcome = json!({"reason": "generation_stopped"});
        update_command_state(
            tx,
            &pending_id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.cancelled",
            "command",
            Some(agent_num),
            Some(generation),
            json!({"command_id": pending_id, "reason": "generation_stopped"}),
        )?);
    }
    if let Some(turn_id) = agent.active_turn.as_deref() {
        db!(tx.execute(
            "UPDATE turns SET state = 'unknown', completed_at = ?1
             WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4",
            params![now, agent_num, generation, turn_id],
        ));
        events.push(insert_daemon_event(
            tx,
            now,
            "turn.unknown",
            "turn",
            Some(agent_num),
            Some(generation),
            json!({"turn_id": turn_id, "reason": "stopped_without_terminal_result"}),
        )?);
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'stopped', active_turn = NULL, token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.stopped",
        "agent",
        Some(agent_num),
        Some(generation),
        json!({"agent_id": agent.id, "command_id": command_id}),
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
) -> Result<()> {
    for id in command_ids_in_state(tx, agent, generation, "dispatched")? {
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

fn check_rpc_frame(method: &str, params: &Value) -> Result<()> {
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

fn parse_enum<T: DeserializeOwned>(value: &str, what: &str) -> Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|error| Error::new("internal", format!("invalid stored {what}: {error}")))
}

fn format_agent_id(id: i64) -> String {
    format!("a{id}")
}

fn parse_agent_id(id: &str) -> Result<i64> {
    let digits = id
        .strip_prefix('a')
        .ok_or_else(|| Error::new("invalid", "Agent id must use the a<positive integer> form"))?;
    let number = digits
        .parse::<i64>()
        .map_err(|_| Error::new("invalid", "Agent id is not a signed 64-bit integer"))?;
    if number <= 0 || format_agent_id(number) != id {
        return Err(Error::new(
            "invalid",
            "Agent id must be canonical and positive",
        ));
    }
    Ok(number)
}

fn validate_ulid(id: &str) -> Result<Ulid> {
    Ulid::from_string(id).map_err(|_| Error::new("invalid", "mutation id must be a ULID"))
}

fn validate_id_age(id: &str, now: i64) -> Result<()> {
    let parsed = validate_ulid(id)?;
    let created_at = i64::try_from(parsed.timestamp_ms()).unwrap_or(i64::MAX);
    if now.saturating_sub(created_at) > RETRY_WINDOW_MS {
        return Err(Error::new(
            "id_expired",
            "an absent mutation id is older than the 24-hour retry window",
        ));
    }
    Ok(())
}

fn agent_state_name(state: AgentState) -> &'static str {
    match state {
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

impl Store {
    /// Commit one complete native batch, its reducer projections, derived
    /// command/turn facts, source cursor, and last-batch acknowledgement.
    pub fn put_batch(&mut self, batch: SourceBatch, now: i64) -> Result<BatchCommit> {
        self.ensure_writer_healthy()?;
        let result = self.put_batch_inner(batch, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn put_batch_inner(&mut self, batch: SourceBatch, now: i64) -> Result<BatchCommit> {
        require_nonempty(&batch.batch_id, "batch_id")?;
        require_nonempty(&batch.source, "source")?;
        if batch.events.is_empty() {
            return Err(Error::new("invalid", "native batch must contain an event"));
        }
        if batch.events.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "native batch exceeds 1,000 events",
            ));
        }
        if encode_json(&batch)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "native batch exceeds 900 KiB",
            ));
        }
        let agent_num = parse_agent_id(&batch.agent_id)?;
        let mut native_ids = HashSet::with_capacity(batch.events.len());
        let mut semantic_events = Vec::with_capacity(batch.events.len());
        for event in &batch.events {
            require_nonempty(&event.kind, "event kind")?;
            require_nonempty(&event.native_id, "native_id")?;
            if !native_ids.insert(event.native_id.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    format!(
                        "native id {} occurs more than once in the batch",
                        event.native_id
                    ),
                ));
            }
            if encode_json(&event.data)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    format!("event {} data exceeds 64 KiB", event.native_id),
                ));
            }
            semantic_events.push(json!({
                "kind": event.kind,
                "native_id": event.native_id,
                "data": event.data,
            }));
        }
        let batch_digest = canonical::digest(&Value::Array(semantic_events))?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut projection = load_agent_by_num(&tx, agent_num)?.ok_or_else(|| {
            Error::new(
                "not_found",
                format!("Agent {} was not found", batch.agent_id),
            )
        })?;
        if projection.generation != batch.generation {
            return Err(Error::new(
                "stale_generation",
                format!(
                    "batch generation {} does not match Agent generation {}",
                    batch.generation, projection.generation
                ),
            ));
        }
        let source = load_source_state(&tx, agent_num, batch.generation, &batch.source)?
            .ok_or_else(|| Error::new("source_integrity", "source has no committed state"))?;

        if source.last_batch_id.as_deref() == Some(batch.batch_id.as_str()) {
            if source.last_batch_digest.as_deref() == Some(batch_digest.as_slice())
                && source.cursor == batch.cursor
            {
                let ack = BatchAck {
                    first_seq: source.last_batch_first_seq.ok_or_else(|| {
                        Error::new("internal", "source batch ack is missing its first sequence")
                    })?,
                    last_seq: source.last_batch_last_seq.ok_or_else(|| {
                        Error::new("internal", "source batch ack is missing its last sequence")
                    })?,
                    cursor: source.cursor,
                };
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(BatchCommit {
                    ack,
                    events: Vec::new(),
                });
            }
            return Err(Error::new(
                "source_integrity",
                "the last batch id was reused with different content or cursor",
            ));
        }
        if source.state != "open" {
            return Err(Error::new("source_integrity", "source is closed"));
        }
        if projection.is_terminal() {
            return Err(Error::new(
                "bad_state",
                "native events target a terminal Agent",
            ));
        }

        for event in &batch.events {
            let family = family_for_kind(&event.kind)?;
            let owner: Option<String> = tx
                .query_row(
                    "SELECT source FROM source_owners
                     WHERE agent = ?1 AND generation = ?2 AND family = ?3",
                    params![agent_num, batch.generation, family],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if owner.as_deref() != Some(batch.source.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    format!("source {} does not own family {family}", batch.source),
                ));
            }
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM events
                         WHERE agent = ?1 AND generation = ?2 AND source = ?3 AND native_id = ?4
                     )",
                    params![agent_num, batch.generation, batch.source, event.native_id],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if exists {
                return Err(Error::new(
                    "source_integrity",
                    "batch mixes previously committed native ids with new events",
                ));
            }
        }

        let mut committed_events = Vec::with_capacity(batch.events.len());
        for (index, raw_event) in batch.events.iter().enumerate() {
            let mut native_event = raw_event.clone();
            if native_event.kind == "request.resolved" {
                let turn_id = required_string(&native_event.data, "turn_id")?;
                let request_id = required_string(&native_event.data, "request_id")?;
                let mut pending = pending_request_ids(&tx, agent_num, batch.generation, &turn_id)?;
                pending.remove(&request_id);
                let object = native_event.data.as_object_mut().ok_or_else(|| {
                    Error::new("invalid", "request.resolved data must be an object")
                })?;
                object.insert("last_pending".to_owned(), Value::Bool(pending.is_empty()));
            }
            let event_data = encode_json(&native_event.data)?;
            if event_data.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    format!(
                        "event {} data exceeds 64 KiB after normalization",
                        native_event.native_id
                    ),
                ));
            }
            let (next, effects) = core::apply(&projection, &native_event, now)?;
            let family = family_for_kind(&native_event.kind)?;
            let event_digest = canonical::digest(&json!({
                "kind": native_event.kind,
                "native_id": native_event.native_id,
                "data": native_event.data,
            }))?;
            let cursor = if index + 1 == batch.events.len() {
                batch.cursor.as_deref()
            } else {
                None
            };
            let stored = insert_native_event(
                &tx,
                NativeEventInsert {
                    ts: now,
                    native_event: &native_event,
                    family,
                    agent: agent_num,
                    generation: batch.generation,
                    source: &batch.source,
                    cursor,
                    payload_digest: &event_digest,
                },
            )?;
            persist_effects(&tx, &effects, agent_num, batch.generation, now)?;
            if next.is_terminal() {
                terminalize_generation_commands(
                    &tx,
                    agent_num,
                    batch.generation,
                    now,
                    "session_ended",
                )?;
            }
            update_agent_projection(&tx, &next)?;
            projection = next;
            committed_events.push(stored);
        }

        let first_seq = committed_events
            .first()
            .map(|event| event.seq)
            .ok_or_else(|| Error::new("internal", "non-empty batch stored no events"))?;
        let last_seq = committed_events
            .last()
            .map(|event| event.seq)
            .ok_or_else(|| Error::new("internal", "non-empty batch stored no events"))?;
        let source_state = if projection.is_terminal() {
            "closed"
        } else {
            "open"
        };
        db!(tx.execute(
            "UPDATE source_state
             SET cursor = ?1, last_store_seq = ?2, last_batch_id = ?3,
                 last_batch_digest = ?4, last_batch_first_seq = ?5,
                 last_batch_last_seq = ?6, state = ?7, updated_at = ?8
             WHERE agent = ?9 AND generation = ?10 AND source = ?11",
            params![
                batch.cursor,
                last_seq,
                batch.batch_id,
                batch_digest,
                first_seq,
                last_seq,
                source_state,
                now,
                agent_num,
                batch.generation,
                batch.source,
            ],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(BatchCommit {
            ack: BatchAck {
                first_seq,
                last_seq,
                cursor: batch.cursor,
            },
            events: committed_events,
        })
    }
}
