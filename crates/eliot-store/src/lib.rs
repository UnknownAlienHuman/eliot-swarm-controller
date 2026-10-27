#![forbid(unsafe_code)]

//! Durable L1 state for Agents, commands, native events, turns, and source acks.

mod canonical;
pub mod error;

pub use crate::error::{Error, Result};
use eliot_core::{
    self as core, Agent, AgentState, BatchAck, Command, CommandState, Effect, Event, NativeEvent,
    PendingRequest, SourceBatch, SourceGap, SourceGapAck, SubmitCommand, Turn, TurnState,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{cell::Cell, collections::HashSet, path::Path, time::Duration};
use ulid::Ulid;

const MAX_PAGE_ENTRIES: usize = 1_000;
const MAX_PAGE_BYTES: usize = 900 * 1024;
const MAX_NATIVE_FAMILIES: i64 = 9;
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
        if current > 3 {
            return Err(Error::new(
                "unsupported",
                format!("Store schema version {current} is newer than this binary"),
            ));
        }
        if current == 0 {
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
            tx.commit().map_err(sql_error)?;
        }
        if current < 2 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN profile_snapshot TEXT NOT NULL DEFAULT '{}';
                 ALTER TABLE agents ADD COLUMN native_scope TEXT;
                 ALTER TABLE agents ADD COLUMN native_ref TEXT;
                 ALTER TABLE agents ADD COLUMN effective TEXT;
                 CREATE UNIQUE INDEX agents_native
                     ON agents(native_scope, native_ref)
                     WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
                 ALTER TABLE source_state ADD COLUMN gap_id TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_digest BLOB
                     CHECK(gap_digest IS NULL OR length(gap_digest) = 32);
                 ALTER TABLE source_state ADD COLUMN gap_from TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_to TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_seq INTEGER;
                 INSERT INTO schema_migrations(version) VALUES (2);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 3 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN pending TEXT NOT NULL DEFAULT '[]';
                 ALTER TABLE commands ADD COLUMN request_id TEXT;
                 CREATE INDEX commands_request_open
                     ON commands(target, generation, request_id, state)
                     WHERE request_id IS NOT NULL;
                 CREATE TABLE pending_requests (
                     agent INTEGER NOT NULL REFERENCES agents(id),
                     generation INTEGER NOT NULL,
                     request_id TEXT NOT NULL,
                     turn_id TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     details TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN ('pending', 'resolved', 'stale')),
                     PRIMARY KEY(agent, generation, request_id)
                 );
                 CREATE INDEX pending_requests_open
                     ON pending_requests(agent, generation, turn_id, request_id)
                     WHERE state = 'pending';
                 INSERT INTO schema_migrations(version) VALUES (3);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
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
        let mut semantic_payload = json!({
            "target": request.target,
            "verb": request.verb,
            "args": request.args,
            "expected_generation": request.expected_generation,
            "expected_turn_id": request.expected_turn_id,
        });
        if let Some(request_id) = request.request_id.as_deref() {
            let object = semantic_payload.as_object_mut().ok_or_else(|| {
                Error::new("internal", "command semantic payload is not an object")
            })?;
            object.insert(
                "request_id".to_owned(),
                Value::String(request_id.to_owned()),
            );
        }
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
                if request.request_id.is_some() {
                    return Err(Error::new(
                        "invalid",
                        "launch does not take a native request id",
                    ));
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
                let harness = if request.args.get("harness").is_none() {
                    "fake".to_owned()
                } else {
                    required_string(&request.args, "harness")?
                };
                if !matches!(harness.as_str(), "fake" | "codex") {
                    return Err(Error::new(
                        "unsupported",
                        format!("unsupported launch harness {harness}"),
                    ));
                }
                let profile = if request.args.get("profile").is_none() && harness == "fake" {
                    "fake".to_owned()
                } else {
                    required_string(&request.args, "profile")?
                };
                if harness == "fake" && profile != "fake" {
                    return Err(Error::new(
                        "unsupported",
                        "the fake harness requires the fake profile",
                    ));
                }
                let profile_snapshot = match request.args.get("profile_snapshot") {
                    Some(Value::Object(_)) => request.args["profile_snapshot"].clone(),
                    Some(_) => {
                        return Err(Error::new(
                            "invalid",
                            "args.profile_snapshot must be a JSON object",
                        ));
                    }
                    None if harness == "fake" => json!({}),
                    None => {
                        return Err(Error::new(
                            "invalid",
                            "the codex harness requires a frozen profile_snapshot",
                        ));
                    }
                };
                let encoded_profile_snapshot = encode_json(&profile_snapshot)?;
                let workspace = required_string(&request.args, "workspace")?;
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
                        "source_plan": source_plan_for_harness(&harness)?
                    }),
                )?;

                db!(tx.execute(
                    "INSERT INTO agents(generation, project, harness, profile,
                                            profile_snapshot, workspace, state,
                                            created_at, updated_at)
                         VALUES (0, ?1, ?2, ?3, ?4, ?5, 'created', ?6, ?6)",
                    params![
                        project,
                        harness,
                        profile,
                        encoded_profile_snapshot,
                        workspace,
                        now
                    ],
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
                        json!({"agent_id": agent_id, "project": project, "harness": harness, "profile": profile}),
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
                        request_id: None,
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
                            request_id: None,
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
                if request.request_id.is_some() {
                    return Err(Error::new(
                        "invalid",
                        "send and stop do not take a native request id",
                    ));
                }
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
                            request_id: None,
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
                            request_id: None,
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
            "resume" => {
                let target = request
                    .target
                    .as_deref()
                    .ok_or_else(|| Error::new("invalid", "resume requires an Agent target"))?;
                let agent_num = parse_agent_id(target)?;
                let agent = load_agent_by_num(&tx, agent_num)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {target} was not found"))
                })?;
                if agent.harness != "codex" {
                    return Err(Error::new(
                        "unsupported",
                        "resume is only supported for a Codex Agent with a retained native subject",
                    ));
                }
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
                if request.expected_turn_id.is_some() {
                    return Err(Error::new(
                        "stale_generation",
                        "resume cannot target an active turn",
                    ));
                }
                if request.request_id.is_some() {
                    return Err(Error::new(
                        "invalid",
                        "resume does not take a native request id",
                    ));
                }
                if !matches!(
                    agent.state,
                    AgentState::Stopped | AgentState::Failed | AgentState::Lost
                ) || agent.active_turn.is_some()
                {
                    return Err(Error::new(
                        "bad_state",
                        "resume requires a stopped, failed, or lost Agent with no active turn",
                    ));
                }
                let native_scope = agent
                    .native_scope
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "unsupported",
                            "resume requires a retained native_scope and native_ref",
                        )
                    })?;
                let native_ref = agent
                    .native_ref
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "unsupported",
                            "resume requires a retained native_scope and native_ref",
                        )
                    })?;
                if request
                    .args
                    .as_object()
                    .is_some_and(|args| args.keys().any(|key| key != "text"))
                {
                    return Err(Error::new(
                        "unsupported",
                        "resume accepts only new linked input text; profile and subject are retained",
                    ));
                }
                let text = optional_string(&request.args, "text")?;
                if let Some(text) = &text {
                    require_nonempty(text, "args.text")?;
                    check_input(text)?;
                }
                let next_generation = agent.generation.checked_add(1).ok_or_else(|| {
                    Error::new("bad_state", "Agent generation cannot be incremented")
                })?;
                check_rpc_frame(
                    "agent.start",
                    &json!({
                        "agent_id": agent.id,
                        "generation": next_generation,
                        "command_id": request.id,
                        "profile": agent.profile,
                        "workspace": agent.workspace,
                        "resume_from": {
                            "native_scope": native_scope,
                            "native_ref": native_ref
                        },
                        "source_plan": source_plan_for_harness(&agent.harness)?
                    }),
                )?;
                let resume_args = json!({});
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
                        request_id: None,
                        verb: "resume",
                        args: &resume_args,
                        state: CommandState::Accepted,
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
                    json!({"command_id": request.id, "verb": "resume"}),
                )?);

                if let Some(text) = text {
                    let linked_id = format!("{}/send", request.id);
                    let linked_args = json!({"text": text});
                    check_rpc_frame(
                        "agent.command",
                        &json!({
                            "agent_id": agent.id,
                            "generation": next_generation,
                            "command_id": linked_id,
                            "verb": "send",
                            "args": linked_args,
                        }),
                    )?;
                    let linked_semantic = json!({
                        "target": agent.id,
                        "verb": "send",
                        "args": linked_args,
                        "expected_generation": next_generation,
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
                            expected_generation: Some(next_generation),
                            generation: Some(next_generation),
                            expected_turn_id: None,
                            parent_command: Some(&request.id),
                            link_kind: Some("send"),
                            ordinal: Some(0),
                            turn_id: None,
                            request_id: None,
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
                        Some(next_generation),
                        json!({
                            "command_id": linked_id,
                            "parent_command": request.id,
                            "verb": "send"
                        }),
                    )?);
                    Some(linked_id)
                } else {
                    None
                }
            }
            "steer" | "interrupt" | "approve" | "deny" | "answer" => {
                let target = request.target.as_deref().ok_or_else(|| {
                    Error::new("invalid", "control command requires an Agent target")
                })?;
                let agent_num = parse_agent_id(target)?;
                let agent = load_agent_by_num(&tx, agent_num)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {target} was not found"))
                })?;
                if agent.harness != "codex" {
                    return Err(Error::new(
                        "unsupported",
                        format!(
                            "{} is not supported by the {} harness",
                            request.verb, agent.harness
                        ),
                    ));
                }
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
                if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                    return Err(Error::new(
                        "bad_state",
                        "turn controls require a busy or waiting Agent",
                    ));
                }
                let turn_id = agent.active_turn.clone().ok_or_else(|| {
                    Error::new("bad_state", "turn control requires an exact active turn")
                })?;
                if request
                    .expected_turn_id
                    .as_deref()
                    .is_some_and(|expected| expected != turn_id.as_str())
                {
                    return Err(Error::new(
                        "stale_generation",
                        "expected turn does not match the current Agent turn",
                    ));
                }

                let request_id = match request.verb.as_str() {
                    "steer" | "interrupt" => {
                        if request.request_id.is_some() {
                            return Err(Error::new(
                                "invalid",
                                "steer and interrupt do not take a native request id",
                            ));
                        }
                        None
                    }
                    _ => {
                        let pending = if let Some(request_id) = request.request_id.as_deref() {
                            require_nonempty(request_id, "request_id")?;
                            let (pending, state) = load_pending_request(
                                &tx,
                                agent_num,
                                agent.generation,
                                request_id,
                            )?
                            .ok_or_else(|| {
                                Error::new(
                                    "not_found",
                                    "pending request was not found in this Agent generation",
                                )
                            })?;
                            if state != "pending" {
                                return Err(Error::new(
                                    "source_integrity",
                                    "native request is no longer pending",
                                ));
                            }
                            pending
                        } else {
                            capture_single_pending_request(&tx, agent_num, agent.generation)?
                        };
                        if pending.turn_id.as_str() != turn_id.as_str()
                            || request
                                .expected_turn_id
                                .as_deref()
                                .is_some_and(|expected| expected != pending.turn_id.as_str())
                        {
                            return Err(Error::new(
                                "stale_generation",
                                "pending request does not belong to the captured active turn",
                            ));
                        }
                        if !agent.pending.contains(&pending) {
                            return Err(Error::new(
                                "source_integrity",
                                "pending request row does not match the Agent projection",
                            ));
                        }
                        if request.verb == "answer" && contains_secret_marker(&pending.details) {
                            return Err(Error::new(
                                "secret_in_payload",
                                "answer to a secret question is not permitted",
                            ));
                        }
                        if has_open_request_reply(
                            &tx,
                            agent_num,
                            agent.generation,
                            &pending.id,
                            None,
                        )? {
                            return Err(Error::new(
                                "bad_state",
                                "a reply command is already unresolved for this native request",
                            ));
                        }
                        Some(pending.id)
                    }
                };
                if request.verb == "steer" {
                    let text = required_string(&request.args, "text")?;
                    check_input(&text)?;
                }
                let mut params = json!({
                    "agent_id": agent.id,
                    "generation": agent.generation,
                    "command_id": request.id,
                    "verb": request.verb,
                    "turn_id": turn_id,
                    "args": request.args,
                });
                if let Some(request_id) = request_id.as_deref() {
                    params["request_id"] = Value::String(request_id.to_owned());
                }
                check_rpc_frame("agent.command", &params)?;
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
                        turn_id: Some(&turn_id),
                        request_id: request_id.as_deref(),
                        verb: &request.verb,
                        args: &request.args,
                        state: CommandState::Accepted,
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
                    json!({
                        "command_id": request.id,
                        "verb": request.verb,
                        "turn_id": turn_id,
                        "request_id": request_id
                    }),
                )?);
                None
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("Store does not admit the {verb} verb"),
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
                        "source_plan": source_plan_for_harness(&agent.harness)?
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
                install_initial_source_plan(&tx, agent_num, 1, &agent.harness, now)?;
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
            "resume" => {
                if command.state != CommandState::Accepted
                    || command_generation != agent.generation
                    || command
                        .command
                        .expected_generation
                        .is_some_and(|expected| expected != agent.generation)
                    || !matches!(
                        agent.state,
                        AgentState::Stopped | AgentState::Failed | AgentState::Lost
                    )
                    || agent.active_turn.is_some()
                {
                    return Err(Error::new(
                        "stale_generation",
                        "resume no longer targets the captured terminal Agent generation",
                    ));
                }
                if agent.harness != "codex" {
                    return Err(Error::new(
                        "unsupported",
                        "resume is only supported for a Codex Agent",
                    ));
                }
                let native_scope = agent
                    .native_scope
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new("unsupported", "resume has no retained native subject")
                    })?;
                let native_ref = agent
                    .native_ref
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new("unsupported", "resume has no retained native subject")
                    })?;
                let next_generation = agent.generation.checked_add(1).ok_or_else(|| {
                    Error::new("bad_state", "Agent generation cannot be incremented")
                })?;
                check_rpc_frame(
                    "agent.start",
                    &json!({
                        "agent_id": agent.id,
                        "generation": next_generation,
                        "command_id": id,
                        "profile": agent.profile,
                        "workspace": agent.workspace,
                        "resume_from": {
                            "native_scope": native_scope,
                            "native_ref": native_ref
                        },
                        "source_plan": source_plan_for_harness(&agent.harness)?
                    }),
                )?;

                for old_id in pending_command_ids(&tx, agent_num, agent.generation)? {
                    if old_id == id {
                        continue;
                    }
                    let outcome = json!({"reason": "generation_resumed"});
                    update_command_state(
                        &tx,
                        &old_id,
                        CommandState::Cancelled,
                        Some(&outcome),
                        None,
                        now,
                    )?;
                    events.push(insert_daemon_event(
                        &tx,
                        now,
                        "command.cancelled",
                        "command",
                        Some(agent_num),
                        Some(agent.generation),
                        json!({"command_id": old_id, "reason": "generation_resumed"}),
                    )?);
                }
                db!(tx.execute(
                    "UPDATE source_state SET state = 'closed', updated_at = ?1
                     WHERE agent = ?2 AND generation = ?3 AND state = 'open'",
                    params![now, agent_num, agent.generation],
                ));
                db!(tx.execute(
                    "UPDATE pending_requests SET state = 'stale'
                     WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
                    params![agent_num, agent.generation],
                ));
                let token = format!("{}{}", Ulid::new(), Ulid::new());
                let changed = db!(tx.execute(
                    "UPDATE agents
                     SET generation = ?1, state = 'starting', active_turn = NULL,
                         pending = '[]', token = ?2, updated_at = ?3
                     WHERE id = ?4 AND generation = ?5
                       AND state IN ('stopped', 'failed', 'lost')
                       AND active_turn IS NULL AND native_scope = ?6 AND native_ref = ?7",
                    params![
                        next_generation,
                        token,
                        now,
                        agent_num,
                        agent.generation,
                        native_scope,
                        native_ref
                    ],
                ));
                if changed != 1 {
                    return Err(Error::new(
                        "stale_generation",
                        "resume target changed before its new generation was committed",
                    ));
                }
                install_initial_source_plan(&tx, agent_num, next_generation, &agent.harness, now)?;
                update_command_state(
                    &tx,
                    id,
                    CommandState::Dispatched,
                    None,
                    Some(next_generation),
                    now,
                )?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "agent.starting",
                    "agent",
                    Some(agent_num),
                    Some(next_generation),
                    json!({
                        "agent_id": agent.id,
                        "command_id": id,
                        "resume_from": {
                            "native_scope": native_scope,
                            "native_ref": native_ref
                        }
                    }),
                )?);
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(next_generation),
                    json!({"command_id": id, "verb": "resume"}),
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
            "steer" | "interrupt" | "approve" | "deny" | "answer" => {
                if command_generation != agent.generation
                    || command
                        .command
                        .expected_generation
                        .is_some_and(|expected| expected != agent.generation)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "control command targets a stale Agent generation",
                    ));
                }
                if agent.harness != "codex" {
                    return Err(Error::new(
                        "unsupported",
                        "control command is not supported by this harness",
                    ));
                }
                if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                    return Err(Error::new(
                        "bad_state",
                        "control command requires a busy or waiting Agent",
                    ));
                }
                let turn_id = command.command.turn_id.as_deref().ok_or_else(|| {
                    Error::new("source_integrity", "control command has no captured turn")
                })?;
                if command
                    .expected_turn_id
                    .as_deref()
                    .is_some_and(|expected| expected != turn_id)
                    || agent.active_turn.as_deref() != Some(turn_id)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "control command no longer targets the exact active turn",
                    ));
                }
                if matches!(command.command.verb.as_str(), "approve" | "deny" | "answer") {
                    let request_id = command.command.request_id.as_deref().ok_or_else(|| {
                        Error::new(
                            "source_integrity",
                            "request reply has no captured request id",
                        )
                    })?;
                    let Some((pending, state)) =
                        load_pending_request(&tx, agent_num, agent.generation, request_id)?
                    else {
                        return Err(Error::new(
                            "stale_generation",
                            "captured native request is no longer present",
                        ));
                    };
                    if state != "pending"
                        || pending.turn_id.as_str() != turn_id
                        || !agent.pending.contains(&pending)
                    {
                        return Err(Error::new(
                            "stale_generation",
                            "request reply no longer matches its exact pending request",
                        ));
                    }
                    if has_open_request_reply(
                        &tx,
                        agent_num,
                        agent.generation,
                        request_id,
                        Some(id),
                    )? {
                        return Err(Error::new(
                            "bad_state",
                            "another reply command is unresolved for this native request",
                        ));
                    }
                } else if command.command.request_id.is_some() {
                    return Err(Error::new(
                        "source_integrity",
                        "turn control unexpectedly has a native request id",
                    ));
                }
                let mut params = json!({
                    "agent_id": agent.id,
                    "generation": agent.generation,
                    "command_id": id,
                    "verb": command.command.verb,
                    "turn_id": turn_id,
                    "args": command.command.args,
                });
                if let Some(request_id) = command.command.request_id.as_deref() {
                    params["request_id"] = Value::String(request_id.to_owned());
                }
                check_rpc_frame("agent.command", &params)?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(agent.generation),
                    json!({
                        "command_id": id,
                        "verb": command.command.verb,
                        "turn_id": turn_id,
                        "request_id": command.command.request_id
                    }),
                )?);
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("Store does not dispatch the {verb} verb"),
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
        if state == CommandState::Applied && is_turn_control(&record.command.verb) {
            validate_control_applied_outcome(&record.command, id, outcome.as_ref())?;
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

        if matches!(record.command.verb.as_str(), "launch" | "resume")
            && matches!(state, CommandState::Rejected | CommandState::Failed)
        {
            if let (Some(agent_num), Some(generation)) = (target, generation) {
                let reason = if record.command.verb == "resume" {
                    "resume_failed"
                } else {
                    "launch_failed"
                };
                fail_start_tx(&tx, agent_num, generation, id, reason, now, &mut events)?;
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
                "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at
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
                "SELECT id, generation, name, project, harness, profile,
                        profile_snapshot, native_scope, native_ref, workspace, effective,
                        pending, state, active_turn, created_at, updated_at
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
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
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

#[cfg(test)]
mod tests {
    use super::{Store, contains_secret_marker};
    use eliot_core::{
        AgentState, CommandState, NativeEvent, SourceBatch, SourceGap, SubmitCommand,
    };
    use serde_json::json;
    use std::{
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };
    use ulid::Ulid;

    #[test]
    fn secret_question_answer_is_rejected_before_command_persistence() {
        assert!(contains_secret_marker(
            &json!({"questions": [{"is_secret": true}]})
        ));
        assert!(contains_secret_marker(
            &json!({"native": {"question": {"isSecret": true}}})
        ));
        assert!(!contains_secret_marker(
            &json!({"questions": [{"is_secret": false}]})
        ));

        let launch_id = Ulid::new().to_string();
        let send_id = Ulid::new().to_string();
        let answer_id = Ulid::new().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after the Unix epoch")
            .as_millis()
            .try_into()
            .expect("system time in milliseconds fits in i64");
        let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({
                        "project": "secret-answer-regression",
                        "workspace": "synthetic-workspace",
                        "harness": "codex",
                        "profile": "synthetic-codex",
                        "profile_snapshot": {}
                    }),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                },
                now,
            )
            .expect("admit Codex launch");
        let started = store
            .dispatch(&launch_id, now)
            .expect("dispatch Codex launch");
        let agent_id = admission
            .command
            .target
            .expect("launch admission returns its Agent");
        assert_eq!(started.value.generation, Some(1));
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ready".to_owned(),
                        native_id: "ready-1".to_owned(),
                        data: json!({
                            "native_scope": "synthetic-codex-home",
                            "native_ref": "synthetic-thread",
                            "effective": {}
                        }),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit synthetic session readiness");
        store
            .settle(
                &launch_id,
                CommandState::Applied,
                Some(json!({"state": "ready"})),
                now,
            )
            .expect("settle launch");
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent_id.clone()),
                    verb: "send".to_owned(),
                    args: json!({"text": "trigger synthetic request"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                },
                now,
            )
            .expect("admit synthetic input");
        store
            .dispatch(&send_id, now)
            .expect("dispatch synthetic input");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "turn.started".to_owned(),
                        native_id: "turn-started-1".to_owned(),
                        data: json!({"command_id": send_id, "turn_id": "turn-1"}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit synthetic turn start");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "request.opened".to_owned(),
                        native_id: "request-opened-1".to_owned(),
                        data: json!({
                            "command_id": send_id,
                            "request_id": "public-request-1",
                            "turn_id": "turn-1",
                            "kind": "question",
                            "details": {
                                "questions": [{"id": "q1", "is_secret": true}]
                            }
                        }),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit synthetic secret question");

        let marker = "synthetic-answer-secret-marker";
        let error = store
            .submit(
                "owner",
                SubmitCommand {
                    id: answer_id.clone(),
                    target: Some(agent_id),
                    verb: "answer".to_owned(),
                    args: json!({"text": marker}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                },
                now,
            )
            .expect_err("secret question answers must be rejected");
        assert_eq!(error.code, "secret_in_payload");
        assert!(
            store
                .get_command(&answer_id)
                .expect("read rejected answer command")
                .is_none(),
            "rejected answer args must not be persisted"
        );
    }

    #[test]
    fn native_request_cleanup_does_not_acknowledge_reply() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after the Unix epoch")
            .as_millis()
            .try_into()
            .expect("system time in milliseconds fits in i64");
        let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
        let command = |verb: &str, target, args, request_id| SubmitCommand {
            id: Ulid::new().to_string(),
            target,
            verb: verb.to_owned(),
            args,
            expected_generation: None,
            expected_turn_id: None,
            request_id,
        };
        let launch = command(
            "launch",
            None,
            json!({
                "project": "native-request-cleanup",
                "workspace": "synthetic-workspace",
                "harness": "codex",
                "profile": "synthetic-codex",
                "profile_snapshot": {}
            }),
            None,
        );
        let launch_id = launch.id.clone();
        let agent_id = store
            .submit("owner", launch, now)
            .expect("admit launch")
            .command
            .target
            .expect("launch returns an Agent");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        let batch = |kind: &str, data| SourceBatch {
            batch_id: Ulid::new().to_string(),
            agent_id: agent_id.clone(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                kind: kind.to_owned(),
                native_id: Ulid::new().to_string(),
                data,
            }],
            cursor: None,
        };
        store
            .put_batch(batch("session.ready", json!({})), now)
            .expect("commit session readiness");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        let send = command(
            "send",
            Some(agent_id.clone()),
            json!({"text": "open native requests"}),
            None,
        );
        let send_id = send.id.clone();
        store.submit("owner", send, now).expect("admit input");
        store.dispatch(&send_id, now).expect("dispatch input");
        store
            .put_batch(
                batch(
                    "turn.started",
                    json!({"command_id": send_id, "turn_id": "turn-1"}),
                ),
                now,
            )
            .expect("commit exact turn association");
        for request_id in ["cleanup-only", "uncertain-reply"] {
            store
                .put_batch(
                    batch(
                        "request.opened",
                        json!({
                            "command_id": send_id,
                            "request_id": request_id,
                            "turn_id": "turn-1",
                            "kind": "approval",
                            "details": {}
                        }),
                    ),
                    now,
                )
                .expect("commit pending request");
        }
        let reply = command(
            "approve",
            Some(agent_id.clone()),
            json!({}),
            Some("uncertain-reply".to_owned()),
        );
        let reply_id = reply.id.clone();
        store.submit("owner", reply, now).expect("admit reply");
        store.dispatch(&reply_id, now).expect("dispatch reply");
        let unknown_reply = store
            .settle(
                &reply_id,
                CommandState::Unknown,
                Some(json!({"reason": "native reply outcome unknown"})),
                now,
            )
            .expect("retain unknown reply outcome")
            .value;

        let close_data = json!({"request_id": "cleanup-only", "turn_id": "turn-1"});
        let mut wrong_source = batch("request.closed", close_data.clone());
        wrong_source.source = "unowned".to_owned();
        let mut wrong_generation = batch("request.closed", close_data.clone());
        wrong_generation.generation = 2;
        let before_rejections = store.high_water().expect("read event high water");
        for (invalid, code) in [
            (wrong_source, "source_integrity"),
            (wrong_generation, "stale_generation"),
            (
                batch(
                    "request.closed",
                    json!({"request_id": "missing", "turn_id": "turn-1"}),
                ),
                "source_integrity",
            ),
            (
                batch(
                    "request.closed",
                    json!({"request_id": "cleanup-only", "turn_id": "other-turn"}),
                ),
                "source_integrity",
            ),
            (batch("request.resolved", close_data.clone()), "invalid"),
            (
                batch(
                    "request.resolved",
                    json!({
                        "request_id": "cleanup-only", "turn_id": "turn-1",
                        "command_id": send_id
                    }),
                ),
                "source_integrity",
            ),
            (
                batch(
                    "request.closed",
                    json!({
                        "request_id": "cleanup-only", "turn_id": "turn-1",
                        "command_id": reply_id
                    }),
                ),
                "invalid",
            ),
        ] {
            let error = store
                .put_batch(invalid, now)
                .expect_err("reject wrong binding");
            assert_eq!(error.code, code);
        }
        assert_eq!(
            store.high_water().expect("read event high water"),
            before_rejections
        );

        store
            .record_source_gap(
                SourceGap {
                    gap_id: "independent-gap".to_owned(),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    families: vec!["request".to_owned()],
                    reason: "synthetic source gap".to_owned(),
                    from: None,
                    to: None,
                },
                now,
            )
            .expect("record independent source gap");
        let source_before = store
            .read_source_state(&agent_id, 1, "stream")
            .expect("read source gap before cleanup");
        for (request_id, expected_state, remaining) in [
            ("cleanup-only", AgentState::Waiting, 1),
            ("uncertain-reply", AgentState::Busy, 0),
        ] {
            let cleanup = store
                .put_batch(
                    batch(
                        "request.closed",
                        json!({"request_id": request_id, "turn_id": "turn-1"}),
                    ),
                    now,
                )
                .expect("commit native request cleanup without a reply acknowledgement");
            assert_eq!(cleanup.events.len(), 1);
            assert_eq!(cleanup.events[0].kind, "request.closed");
            assert!(cleanup.events[0].data.get("command_id").is_none());
            let agent = store
                .get_agent(&agent_id)
                .expect("read Agent")
                .expect("Agent exists");
            assert_eq!(agent.state, expected_state);
            assert_eq!(agent.pending.len(), remaining);
            assert!(!agent.pending.iter().any(|pending| pending.id == request_id));
            assert_eq!(agent.active_turn.as_deref(), Some("turn-1"));
            assert_eq!(
                store.get_command(&reply_id).expect("read reply"),
                Some(unknown_reply.clone())
            );

            let late_reply = command(
                "approve",
                Some(agent_id.clone()),
                json!({}),
                Some(request_id.to_owned()),
            );
            let late_reply_id = late_reply.id.clone();
            let error = store
                .submit("owner", late_reply, now)
                .expect_err("durable pending row must already be resolved");
            assert_eq!(error.code, "source_integrity");
            assert!(
                store
                    .get_command(&late_reply_id)
                    .expect("read late reply")
                    .is_none()
            );
        }
        let source_after = store
            .read_source_state(&agent_id, 1, "stream")
            .expect("read source gap after cleanup");
        for field in ["gap_id", "gap_digest", "gap_seq", "health"] {
            assert_eq!(source_after[field], source_before[field]);
        }
    }
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
    request_id: Option<&'a str>,
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
    })
}

fn agent_from_record(record: AgentRecord) -> Result<Agent> {
    let pending = decode_pending_requests(&record.pending)?;
    Ok(Agent {
        id: format_agent_id(record.id),
        generation: record.generation,
        name: record.name,
        project: record.project,
        harness: record.harness,
        profile: record.profile,
        profile_snapshot: decode_json(&record.profile_snapshot)?,
        native_scope: record.native_scope,
        native_ref: record.native_ref,
        workspace: record.workspace,
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
                    pending, state, active_turn, created_at, updated_at
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
        request_id: row.get(8)?,
        verb: row.get(9)?,
        args: row.get(10)?,
        state: row.get(11)?,
        outcome: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
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
            request_id: raw.request_id,
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
                    generation, expected_turn_id, turn_id, request_id, verb, args,
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

fn has_open_request_reply(
    connection: &Connection,
    agent: i64,
    generation: i64,
    request_id: &str,
    except_command: Option<&str>,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands
                 WHERE target = ?1 AND generation = ?2 AND request_id = ?3
                   AND state IN ('accepted', 'queued', 'dispatched', 'unknown', 'applied')
                   AND (?4 IS NULL OR id != ?4)
             )",
            params![agent, generation, request_id, except_command],
            |row| row.get(0),
        )
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
             turn_id, request_id, verb, args, state, outcome, created_at, updated_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19
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
            "SELECT cursor, last_store_seq, last_batch_id, last_batch_digest,
                    last_batch_first_seq, last_batch_last_seq, state, health,
                    gap_id, gap_digest, gap_from, gap_to, gap_seq
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
    for family in source_families_for_harness(harness)? {
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
        "fake" => Ok(&["session", "turn", "message"]),
        "codex" => Ok(&["session", "turn", "message", "tool", "request"]),
        other => Err(Error::new(
            "unsupported",
            format!("unsupported launch harness {other}"),
        )),
    }
}

fn source_plan_for_harness(harness: &str) -> Result<Value> {
    let mut plan = serde_json::Map::new();
    for family in source_families_for_harness(harness)? {
        plan.insert((*family).to_owned(), json!("stream"));
    }
    Ok(Value::Object(plan))
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

fn persist_effects(
    tx: &Transaction<'_>,
    effects: &[Effect],
    agent_num: i64,
    generation: i64,
    now: i64,
    native_event: &NativeEvent,
    next_agent: &Agent,
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
                        .is_some_and(|expected| expected != turn_id.as_str())
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
            Effect::RequestPending {
                request_id,
                turn_id,
                pending,
                kind,
                details,
            } => {
                let expected_kind = if *pending {
                    "request.opened"
                } else {
                    "request.resolved"
                };
                if native_event.kind != expected_kind
                    || required_string(&native_event.data, "request_id")?.as_str()
                        != request_id.as_str()
                    || required_string(&native_event.data, "turn_id")?.as_str() != turn_id.as_str()
                {
                    return Err(Error::new(
                        "source_integrity",
                        "request effect is not bound to its exact native request and turn",
                    ));
                }
                let command_id = required_string(&native_event.data, "command_id")?;
                let command = load_command_record(tx, &command_id)?.ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "request event references an unknown command",
                    )
                })?;
                if command.target != Some(agent_num)
                    || command.command.generation != Some(generation)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "request event command is bound to another Agent or generation",
                    ));
                }
                if *pending {
                    require_effect_command(&command, agent_num, generation, "send", &command_id)?;
                    let kind = kind.as_deref().ok_or_else(|| {
                        Error::new("source_integrity", "request.opened is missing its kind")
                    })?;
                    let details = details.as_ref().ok_or_else(|| {
                        Error::new("source_integrity", "request.opened is missing its details")
                    })?;
                    require_nonempty(kind, "request kind")?;
                    if encode_json(details)?.len() > MAX_EVENT_BYTES {
                        return Err(Error::new(
                            "frame_too_large",
                            "pending request details exceed 64 KiB",
                        ));
                    }
                    if command.command.state != CommandState::Applied
                        || command.command.turn_id.as_deref() != Some(turn_id.as_str())
                    {
                        return Err(Error::new(
                            "source_integrity",
                            "request.opened is not bound to its exact applied send turn",
                        ));
                    }
                    let projected = PendingRequest {
                        id: request_id.clone(),
                        turn_id: turn_id.clone(),
                        kind: kind.to_owned(),
                        details: details.clone(),
                    };
                    if !next_agent.pending.contains(&projected) {
                        return Err(Error::new(
                            "source_integrity",
                            "request.opened is missing from the next Agent projection",
                        ));
                    }
                    match load_pending_request(tx, agent_num, generation, request_id)? {
                        Some((old, state)) if state == "pending" && old == projected => {}
                        Some(_) => {
                            return Err(Error::new(
                                "source_integrity",
                                "request id was reused with another pending binding",
                            ));
                        }
                        None => {
                            let details = encode_json(details)?;
                            db!(tx.execute(
                                "INSERT INTO pending_requests(
                                     agent, generation, request_id, turn_id, kind, details, state
                                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                                params![agent_num, generation, request_id, turn_id, kind, details],
                            ));
                        }
                    }
                } else {
                    if kind.is_some() || details.is_some() {
                        return Err(Error::new(
                            "source_integrity",
                            "request.resolved unexpectedly carries open-request details",
                        ));
                    }
                    if !matches!(command.command.verb.as_str(), "approve" | "deny" | "answer")
                        || command.command.request_id.as_deref() != Some(request_id.as_str())
                        || command.command.turn_id.as_deref() != Some(turn_id.as_str())
                        || !matches!(
                            command.state,
                            CommandState::Dispatched
                                | CommandState::Unknown
                                | CommandState::Applied
                                | CommandState::Failed
                        )
                    {
                        return Err(Error::new(
                            "source_integrity",
                            "request.resolved is not bound to its exact dispatched reply",
                        ));
                    }
                    if next_agent.pending.iter().any(|request| {
                        request.id.as_str() == request_id.as_str()
                            && request.turn_id.as_str() == turn_id.as_str()
                    }) {
                        return Err(Error::new(
                            "source_integrity",
                            "request.resolved remains in the next Agent projection",
                        ));
                    }
                    let Some((existing, state)) =
                        load_pending_request(tx, agent_num, generation, request_id)?
                    else {
                        return Err(Error::new(
                            "source_integrity",
                            "request.resolved references no durable pending request",
                        ));
                    };
                    if state != "pending" || existing.turn_id.as_str() != turn_id.as_str() {
                        return Err(Error::new(
                            "source_integrity",
                            "request.resolved does not match its durable pending binding",
                        ));
                    }
                    let changed = tx
                        .execute(
                            "UPDATE pending_requests SET state = 'resolved'
                             WHERE agent = ?1 AND generation = ?2 AND request_id = ?3
                               AND turn_id = ?4 AND state = 'pending'",
                            params![agent_num, generation, request_id, turn_id],
                        )
                        .map_err(sql_error)?;
                    if changed != 1 {
                        return Err(Error::new(
                            "source_integrity",
                            "pending request changed before resolution was stored",
                        ));
                    }
                }
            }
            Effect::RequestClosed {
                request_id,
                turn_id,
            } => {
                if native_event.kind != "request.closed"
                    || native_event.data.get("command_id").is_some()
                    || required_string(&native_event.data, "request_id")?.as_str()
                        != request_id.as_str()
                    || required_string(&native_event.data, "turn_id")?.as_str() != turn_id.as_str()
                    || next_agent
                        .pending
                        .iter()
                        .any(|request| request.id == *request_id)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "request.closed is not an exact pending-request cleanup",
                    ));
                }
                let Some((existing, state)) =
                    load_pending_request(tx, agent_num, generation, request_id)?
                else {
                    return Err(Error::new(
                        "source_integrity",
                        "request.closed references no durable pending request",
                    ));
                };
                if state != "pending" || existing.turn_id.as_str() != turn_id.as_str() {
                    return Err(Error::new(
                        "source_integrity",
                        "request.closed does not match its durable pending binding",
                    ));
                }
                let changed = tx
                    .execute(
                        "UPDATE pending_requests SET state = 'resolved'
                         WHERE agent = ?1 AND generation = ?2 AND request_id = ?3
                           AND turn_id = ?4 AND state = 'pending'",
                        params![agent_num, generation, request_id, turn_id],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(Error::new(
                        "source_integrity",
                        "pending request changed before native cleanup was stored",
                    ));
                }
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
                db!(tx.execute(
                    "UPDATE pending_requests SET state = 'stale'
                     WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
                       AND state = 'pending'",
                    params![agent_num, generation, turn_id],
                ));
                if next_agent
                    .pending
                    .iter()
                    .any(|request| request.turn_id == *turn_id)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "terminal turn retains pending requests in the Agent projection",
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

fn is_turn_control(verb: &str) -> bool {
    matches!(verb, "steer" | "interrupt" | "approve" | "deny" | "answer")
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

fn validate_control_applied_outcome(
    command: &Command,
    command_id: &str,
    outcome: Option<&Value>,
) -> Result<()> {
    let Some(outcome) = outcome else {
        return Err(Error::new(
            "source_integrity",
            "control command cannot be applied without its exact native result",
        ));
    };
    let reported_command = outcome.get("command_id").and_then(Value::as_str);
    let reported_turn = outcome.get("turn_id").and_then(Value::as_str);
    let reported_request = outcome.get("request_id").and_then(Value::as_str);
    if reported_command != Some(command_id)
        || command
            .turn_id
            .as_deref()
            .is_none_or(|turn_id| reported_turn != Some(turn_id))
        || reported_request != command.request_id.as_deref()
    {
        return Err(Error::new(
            "source_integrity",
            "control result does not match its exact command, turn, and request binding",
        ));
    }
    Ok(())
}

fn update_agent_projection(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
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
        "UPDATE agents SET state = 'lost', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
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
    reason: &str,
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
        let outcome = json!({"reason": reason});
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
            json!({"command_id": pending_id, "reason": reason}),
        )?);
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'failed', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
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
        json!({"agent_id": agent.id, "command_id": command_id, "reason": reason}),
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
        "UPDATE agents SET state = 'stopped', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
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
    /// Read the bounded source projection and its latest durable acknowledgements.
    pub fn read_source_state(
        &self,
        agent: &str,
        generation: i64,
        source: &str,
    ) -> Result<serde_json::Value> {
        require_nonempty(source, "source")?;
        let agent_num = parse_agent_id(agent)?;
        let current_generation: i64 = self
            .connection
            .query_row(
                "SELECT generation FROM agents WHERE id = ?1",
                [agent_num],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
        if current_generation != generation {
            return Err(Error::new(
                "stale_generation",
                format!("Agent {agent} is at generation {current_generation}"),
            ));
        }
        let state = load_source_state(&self.connection, agent_num, generation, source)?
            .ok_or_else(|| Error::new("source_integrity", "source has no committed state"))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT family FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 AND source = ?3
                 ORDER BY family LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![agent_num, generation, source, MAX_NATIVE_FAMILIES],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut families = Vec::new();
        for row in rows {
            families.push(row.map_err(sql_error)?);
        }
        let state = json!({
            "agent_id": agent,
            "generation": generation,
            "source": source,
            "families": families,
            "state": state.state,
            "health": state.health,
            "cursor": state.cursor,
            "last_store_seq": state.last_store_seq,
            "last_batch_id": state.last_batch_id,
            "last_batch_digest": state.last_batch_digest,
            "last_batch_first_seq": state.last_batch_first_seq,
            "last_batch_last_seq": state.last_batch_last_seq,
            "gap_id": state.gap_id,
            "gap_digest": state.gap_digest,
            "gap_from": state.gap_from,
            "gap_to": state.gap_to,
            "gap_seq": state.gap_seq,
        });
        if encode_json(&state)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "source state exceeds 900 KiB",
            ));
        }
        Ok(state)
    }

    /// Commit one source-loss boundary and its retained acknowledgement.
    pub fn record_source_gap(&mut self, gap: SourceGap, now: i64) -> Result<Change<SourceGapAck>> {
        self.ensure_writer_healthy()?;
        let result = self.record_source_gap_inner(gap, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_gap_inner(
        &mut self,
        gap: SourceGap,
        now: i64,
    ) -> Result<Change<SourceGapAck>> {
        require_nonempty(&gap.gap_id, "gap_id")?;
        require_nonempty(&gap.agent_id, "agent_id")?;
        require_nonempty(&gap.source, "source")?;
        require_nonempty(&gap.reason, "reason")?;
        if gap.families.is_empty() {
            return Err(Error::new("invalid", "source gap must name a family"));
        }
        if gap.families.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "source gap names more than 1,000 families",
            ));
        }
        if encode_json(&gap)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "source gap request exceeds 1 MiB",
            ));
        }

        let agent_num = parse_agent_id(&gap.agent_id)?;
        let mut families = gap.families.clone();
        for family in &families {
            require_nonempty(family, "family")?;
        }
        families.sort_unstable();
        if families.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::new("invalid", "source gap families must be unique"));
        }
        let gap_data = json!({
            "gap_id": gap.gap_id,
            "agent_id": gap.agent_id,
            "generation": gap.generation,
            "source": gap.source,
            "families": families,
            "reason": gap.reason,
            "from": gap.from,
            "to": gap.to,
        });
        if encode_json(&gap_data)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "source gap event data exceeds 64 KiB",
            ));
        }
        let gap_digest = canonical::digest(&gap_data)?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "source gap Agent was not found"))?;
        if agent.generation != gap.generation {
            return Err(Error::new(
                "stale_generation",
                format!(
                    "source gap generation {} does not match Agent generation {}",
                    gap.generation, agent.generation
                ),
            ));
        }
        for family in &families {
            let owner: Option<String> = tx
                .query_row(
                    "SELECT source FROM source_owners
                     WHERE agent = ?1 AND generation = ?2 AND family = ?3",
                    params![agent_num, gap.generation, family],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if owner.as_deref() != Some(gap.source.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    format!("source {} does not own family {family}", gap.source),
                ));
            }
        }
        let source_state = load_source_state(&tx, agent_num, gap.generation, &gap.source)?
            .ok_or_else(|| Error::new("source_integrity", "source has no committed state"))?;
        if source_state.gap_id.as_deref() == Some(gap.gap_id.as_str()) {
            if source_state.gap_digest.as_deref() == Some(gap_digest.as_slice()) {
                let seq = source_state.gap_seq.ok_or_else(|| {
                    Error::new(
                        "internal",
                        "source gap acknowledgement is missing its sequence",
                    )
                })?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: SourceGapAck {
                        gap_id: gap.gap_id,
                        seq,
                    },
                    events: Vec::new(),
                });
            }
            return Err(Error::new(
                "source_integrity",
                "the latest gap id was reused with different content",
            ));
        }
        if source_state.state != "open" {
            return Err(Error::new("source_integrity", "source is closed"));
        }

        let event = insert_daemon_event(
            &tx,
            now,
            "source.gap",
            "source",
            Some(agent_num),
            Some(gap.generation),
            gap_data,
        )?;
        let changed = tx
            .execute(
                "UPDATE source_state
                 SET gap_id = ?1, gap_digest = ?2, gap_from = ?3, gap_to = ?4,
                     gap_seq = ?5, updated_at = ?6
                 WHERE agent = ?7 AND generation = ?8 AND source = ?9",
                params![
                    gap.gap_id,
                    gap_digest,
                    gap.from,
                    gap.to,
                    event.seq,
                    now,
                    agent_num,
                    gap.generation,
                    gap.source,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "source_integrity",
                "source state changed before the gap was stored",
            ));
        }
        let ack = SourceGapAck {
            gap_id: gap.gap_id,
            seq: event.seq,
        };
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: ack,
            events: vec![event],
        })
    }

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
            let native_event = raw_event.clone();
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
            persist_effects(
                &tx,
                &effects,
                agent_num,
                batch.generation,
                now,
                &native_event,
                &next,
            )?;
            if next.is_terminal() {
                terminalize_generation_commands(
                    &tx,
                    agent_num,
                    batch.generation,
                    now,
                    "session_ended",
                )?;
                db!(tx.execute(
                    "UPDATE pending_requests SET state = 'stale'
                     WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
                    params![agent_num, batch.generation],
                ));
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
