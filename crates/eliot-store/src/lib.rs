#![forbid(unsafe_code)]

//! Durable L1 state for Agents, commands, native events, turns, and source acks.

mod canonical;

use eliot_core::{
    self as core, Agent, AgentEdit, AgentState, BatchAck, Command, CommandState, Effect, Event,
    FileChangeHint, FrozenSetupFile, GitBaseline, GitHintPage, GitSnapshot, NativeEvent,
    PendingRequest, ProcessIdentity, ProcessRecord, ProcessSnapshot, ProcessState, SourceBatch,
    SourceGap, SourceGapAck, SubmitCommand, Turn, TurnState, WorkspaceCancelOutcome,
    WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation, WorkspacePreparationInput,
    WorkspaceRemovalOutcome, WorkspaceSetupArg, WorkspaceSetupOutcome, parse_agent_id,
};
pub use eliot_core::{Error, Result};
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
const PROCESS_SOURCE: &str = "os";
const WORKSPACE_PROCESS_SOURCE: &str = "workspace_process";
const GIT_SOURCE: &str = "git";

macro_rules! db {
    ($expression:expr) => {
        $expression.map_err(sql_error)?
    };
}

mod observer;
mod observer_live;
mod source_handoff;

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
        if current > 8 {
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
        if current < 4 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE processes (
                     host_boot_id TEXT NOT NULL CHECK(length(host_boot_id) > 0),
                     pid INTEGER NOT NULL CHECK(pid > 0 AND pid <= 4294967295),
                     start_time INTEGER NOT NULL CHECK(start_time >= 0),
                     owner TEXT NOT NULL,
                     role TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN ('running', 'exited', 'unknown')),
                     exit_code INTEGER,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(host_boot_id, pid, start_time),
                     CHECK(exit_code IS NULL OR state = 'exited')
                 );
                 CREATE INDEX processes_owner ON processes(owner, state);
                 INSERT INTO schema_migrations(version) VALUES (4);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 5 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE agent_edits (
                     agent INTEGER NOT NULL,
                     generation INTEGER NOT NULL,
                     workspace TEXT NOT NULL,
                     path TEXT NOT NULL,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, generation, path)
                 );
                 CREATE INDEX agent_edits_workspace ON agent_edits(workspace, path);
                 CREATE TABLE git_workspaces (
                     workspace TEXT PRIMARY KEY,
                     head TEXT,
                     updated_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations(version) VALUES (5);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 6 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE workspaces (
                     agent INTEGER PRIMARY KEY REFERENCES agents(id),
                     target_generation INTEGER NOT NULL CHECK(target_generation > 0),
                     start_command_id TEXT NOT NULL REFERENCES commands(id),
                     project TEXT NOT NULL, repo_path TEXT NOT NULL, repo_common_dir TEXT NOT NULL,
                     requested_branch TEXT NOT NULL, base_ref TEXT, base_oid TEXT NOT NULL,
                     worktree_path TEXT NOT NULL UNIQUE, frozen_setup TEXT NOT NULL,
                     phase TEXT NOT NULL CHECK(phase IN ('creating','setup_ready','setup_running','ready','unknown','failed','removing','removed')),
                     setup_index INTEGER NOT NULL DEFAULT 0 CHECK(setup_index >= 0),
                     outcome TEXT, updated_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations(version) VALUES (6);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 7 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN origin TEXT NOT NULL DEFAULT 'eliot' CHECK(origin IN ('eliot','native','external'));
                 ALTER TABLE agents ADD COLUMN observe_only INTEGER NOT NULL DEFAULT 0
                     CHECK(observe_only IN (0,1) AND (state <> 'unknown' OR observe_only = 1) AND (observe_only = 0 OR token IS NULL));
                 INSERT INTO schema_migrations(version) VALUES (7);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 8 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE source_state ADD COLUMN last_handoff_digest BLOB
                     CHECK(last_handoff_digest IS NULL OR length(last_handoff_digest) = 32);
                 ALTER TABLE source_state ADD COLUMN last_handoff TEXT
                     CHECK((last_handoff IS NULL) = (last_handoff_digest IS NULL));
                 INSERT INTO schema_migrations(version) VALUES (8);",
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

    fn submit_inner(
        &mut self,
        principal: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        require_nonempty(principal, "principal")?;
        if !request.args.is_object() {
            return Err(Error::new("invalid", "command args must be a JSON object"));
        }
        if request.verb != "attach" && encode_json(&request)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "command request exceeds 1 MiB",
            ));
        }
        validate_ulid(&request.id)?;
        let mut semantic_args = request.args.clone();
        if request.verb == "attach" {
            semantic_args
                .as_object_mut()
                .ok_or_else(|| Error::new("invalid", "attach args must be an object"))?
                .remove("profile_snapshot");
        }
        let mut semantic_payload = json!({
            "target": request.target,
            "verb": request.verb,
            "args": semantic_args,
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
        if encode_json(&semantic_payload)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "command semantic request exceeds its bound",
            ));
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
        if !matches!(request.verb.as_str(), "attach" | "detach")
            && let Some(target) = request.target.as_deref()
            && let Some(agent) = load_agent_by_num(&tx, parse_agent_id(target)?)?
        {
            observer::require_controlled(&agent)?;
        }
        if let Some(input) = &preparation {
            if !matches!(request.verb.as_str(), "launch" | "resume") {
                return Err(Error::new(
                    "invalid",
                    "workspace preparation requires launch or resume",
                ));
            }
            validate_workspace_input(input)?;
        } else if request.verb == "launch"
            && request
                .args
                .get("worktree")
                .is_some_and(|value| !value.is_null())
        {
            return Err(Error::new(
                "invalid",
                "worktree launch requires frozen preparation",
            ));
        }

        let mut events = Vec::new();
        let linked = match request.verb.as_str() {
            "attach" | "detach" => {
                observer::admit(&tx, principal, &request, &payload_digest, now, &mut events)?;
                None
            }
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
                let workspace = match &preparation {
                    Some(input) => {
                        if input.project != project {
                            return Err(Error::new(
                                "invalid",
                                "workspace project differs from launch",
                            ));
                        }
                        input.worktree_path.clone()
                    }
                    None => required_string(&request.args, "workspace")?,
                };
                if preparation.is_some() && load_workspace_by_path(&tx, &workspace)?.is_some() {
                    return Err(Error::new(
                        "id_conflict",
                        "worktree path is already managed",
                    ));
                }
                require_managed_path_ready(&tx, &workspace)?;
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
            "worktree.remove" => {
                let target = request
                    .target
                    .as_deref()
                    .ok_or_else(|| Error::new("invalid", "worktree.remove requires an Agent"))?;
                let agent_num = parse_agent_id(target)?;
                let agent = load_agent_by_num(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
                if request.request_id.is_some()
                    || request.expected_turn_id.is_some()
                    || request
                        .expected_generation
                        .is_some_and(|expected| expected != agent.generation)
                    || request.args.as_object().is_none_or(|args| !args.is_empty())
                {
                    return Err(Error::new(
                        "invalid",
                        "invalid worktree.remove binding or arguments",
                    ));
                }
                let workspace = load_workspace(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "Agent has no managed worktree"))?;
                require_workspace_removable(&tx, &agent, &workspace)?;
                let args = json!({"target_generation": workspace.target_generation});
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
                        expected_turn_id: None,
                        parent_command: None,
                        link_kind: None,
                        ordinal: None,
                        turn_id: None,
                        request_id: None,
                        verb: "worktree.remove",
                        args: &args,
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
                    json!({"command_id": request.id, "verb": "worktree.remove"}),
                )?);
                None
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
                if let Some(path) = &agent.workspace {
                    require_managed_path_ready(&tx, path)?;
                }
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

        if matches!(request.verb.as_str(), "launch" | "resume") {
            admit_workspace_preparation(&tx, &request, preparation.as_ref(), now, &mut events)?;
        }
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
        if let Some(command) = self.get_command(id)?
            && command.verb == "stop"
            && let Some(target) = command.target.as_deref()
            && let Some(agent) = self.get_agent(target)?
            && let Some(workspace) = pending_workspace_for_stop(&self.connection, &agent)?
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
        if !matches!(command.command.verb.as_str(), "attach" | "detach") {
            observer::require_controlled(&agent)?;
        }

        match command.command.verb.as_str() {
            "attach" | "detach" => observer::dispatch(&tx, &command, &agent, now, &mut events)?,
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
                gate_workspace_start(&tx, &agent, &command.command, 1)?;
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
                gate_workspace_start(&tx, &agent, &command.command, next_generation)?;
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
                     WHERE agent = ?2 AND generation = ?3 AND state = 'open' AND source NOT IN ('os', 'git', 'workspace_process')",
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
            "worktree.remove" => {
                let mut workspace = load_workspace(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "managed worktree was not found"))?;
                require_workspace_removable(&tx, &agent, &workspace)?;
                if command_generation != agent.generation
                    || command
                        .command
                        .args
                        .get("target_generation")
                        .and_then(Value::as_i64)
                        != Some(workspace.target_generation)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "worktree removal binding changed",
                    ));
                }
                let old = workspace.clone();
                workspace.phase = WorkspacePhase::Removing;
                workspace.outcome = Some(
                    json!({"stage": "removal", "command_id": id, "previous_phase": old.phase}),
                );
                workspace.updated_at = now;
                persist_workspace(&tx, &old, &workspace)?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(workspace_event(&tx, &workspace, now)?);
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "command.dispatched",
                    "command",
                    Some(agent_num),
                    Some(agent.generation),
                    json!({"command_id": id, "verb": "worktree.remove"}),
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
        if matches!(record.command.verb.as_str(), "attach" | "detach")
            && state == CommandState::Applied
        {
            return Err(Error::new(
                "not_allowed",
                "observer commands require exact observer settlement",
            ));
        }
        if record.command.verb == "worktree.remove" {
            return Err(Error::new(
                "bad_state",
                "use typed workspace removal settlement",
            ));
        }
        if let Some(agent_num) = record.target
            && let Some(workspace) = load_workspace(&tx, agent_num)?
            && ((workspace.start_command_id == id
                && record
                    .command
                    .generation
                    .is_some_and(|generation| generation < workspace.target_generation)
                && matches!(
                    record.state,
                    CommandState::Accepted | CommandState::Queued | CommandState::Unknown
                ))
                || (record.command.verb == "stop"
                    && workspace
                        .outcome
                        .as_ref()
                        .and_then(|outcome| outcome.get("stop_command_id"))
                        .and_then(Value::as_str)
                        == Some(id)))
        {
            return Err(Error::new(
                "bad_state",
                "use workspace-bound preparation/cancellation settlement",
            ));
        }
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
        if !core::command_transition_allowed(record.state, state) {
            return Err(Error::new(
                "bad_state",
                "command state transition is not allowed for ordinary settlement",
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

        if matches!(record.command.verb.as_str(), "attach" | "detach") {
            observer::settle_failure(&tx, &record.command, now, &mut events)?;
        }

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
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0)
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
                        pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0)
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
            .prepare(
                "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                        created_at, updated_at
                 FROM commands
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown')
                   AND NOT (state = 'unknown' AND verb IN ('attach','detach')
                     AND EXISTS(SELECT 1 FROM agents a WHERE a.id = commands.target AND a.observe_only = 1)
                     AND NOT EXISTS(SELECT 1 FROM source_state s WHERE s.agent = commands.target
                         AND s.source NOT IN ('daemon','os','git','workspace_process') AND s.state = 'open'))
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
                let workspace = load_workspace(&tx, agent_num)?
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

    fn observe_processes_inner(
        &mut self,
        agent: &str,
        generation: i64,
        records: &[ProcessRecord],
        now: i64,
        scope: ProcessSourceKind,
    ) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(agent)?;
        let expected_owner = scope.owner(agent, generation);
        if records.len() > MAX_PAGE_ENTRIES || encode_json(&records)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "process observation exceeds its bound",
            ));
        }
        let mut identities = HashSet::with_capacity(records.len());
        for record in records {
            validate_process_identity(&record.identity)?;
            require_nonempty(&record.role, "process role")?;
            if record.owner != expected_owner || !identities.insert(&record.identity) {
                return Err(Error::new(
                    "source_integrity",
                    "process observation has another owner or a repeated identity",
                ));
            }
            if record.state != ProcessState::Exited && record.exit_code.is_some() {
                return Err(Error::new(
                    "invalid",
                    "only an exited process can have an exit code",
                ));
            }
            if encode_json(record)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "process record exceeds 64 KiB",
                ));
            }
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "process observation Agent was not found"))?;
        validate_process_generation(&tx, &current, generation, scope, false)?;
        let source = require_process_source(&tx, agent_num, generation, scope)?;
        let mut events = Vec::new();
        for record in records {
            let start_time = validate_process_identity(&record.identity)?;
            let old = tx
                .query_row(
                    "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
                     FROM processes WHERE host_boot_id = ?1 AND pid = ?2 AND start_time = ?3",
                    params![record.identity.host_boot_id, record.identity.pid, start_time],
                    process_record_from_row,
                )
                .optional()
                .map_err(sql_error)?;
            let mut next = record.clone();
            next.updated_at = now;
            if let Some(old) = &old {
                if old.owner != expected_owner {
                    return Err(Error::new(
                        "source_integrity",
                        "process identity already has another owner",
                    ));
                }
                if next.role != old.role {
                    match (old.role.as_str(), next.role.as_str()) {
                        ("child", "harness") => {}
                        ("child", _) if matches!(scope, ProcessSourceKind::Workspace) => {}
                        (_, "child") => next.role = old.role.clone(),
                        _ => {
                            return Err(Error::new(
                                "source_integrity",
                                "process role can only be promoted from child to harness",
                            ));
                        }
                    }
                }
                // An uncertain later query cannot revoke a known departure or exit code.
                if old.state == ProcessState::Exited {
                    if next.state == ProcessState::Running {
                        return Err(Error::new(
                            "source_integrity",
                            "an exited process identity cannot run again",
                        ));
                    }
                    if old.exit_code.is_some()
                        && next.exit_code.is_some()
                        && old.exit_code != next.exit_code
                    {
                        return Err(Error::new("source_integrity", "process exit code changed"));
                    }
                    next.state = ProcessState::Exited;
                    next.exit_code = next.exit_code.or(old.exit_code);
                }
            }
            if source.state == "closed" && next.state != ProcessState::Exited {
                return Err(Error::new(
                    "bad_state",
                    "a closed OS source cannot admit a non-exited process",
                ));
            }
            db!(tx.execute(
                "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(host_boot_id, pid, start_time) DO UPDATE
                 SET role = excluded.role, state = excluded.state,
                     exit_code = excluded.exit_code, updated_at = excluded.updated_at",
                params![
                    next.identity.host_boot_id, next.identity.pid, start_time, next.owner,
                    next.role, process_state_name(next.state), next.exit_code, now,
                ],
            ));
            if old.is_none() {
                events.push(insert_process_event(
                    &tx,
                    agent_num,
                    generation,
                    now,
                    "process.started",
                    &next,
                    scope,
                )?);
            }
            let kind = match next.state {
                ProcessState::Exited
                    if old
                        .as_ref()
                        .is_none_or(|old| old.state != ProcessState::Exited) =>
                {
                    Some("process.exited")
                }
                ProcessState::Unknown
                    if old
                        .as_ref()
                        .is_some_and(|old| old.state != ProcessState::Unknown) =>
                {
                    Some("process.unknown")
                }
                _ => None,
            };
            if let Some(kind) = kind {
                events.push(insert_process_event(
                    &tx, agent_num, generation, now, kind, &next, scope,
                )?);
            }
        }
        db!(tx.execute(
            "UPDATE source_state SET last_store_seq = COALESCE(?1, last_store_seq), updated_at = ?2
             WHERE agent = ?3 AND generation = ?4 AND source = ?5",
            params![
                events.last().map(|event| event.seq),
                now,
                agent_num,
                generation,
                scope.source()
            ],
        ));
        if matches!(scope, ProcessSourceKind::Workspace) {
            close_finished_workspace_process_source(&tx, agent_num, generation, now, &mut events)?;
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    pub fn list_processes(
        &self,
        owner: &str,
        after: Option<&ProcessIdentity>,
        limit: usize,
    ) -> Result<Vec<ProcessRecord>> {
        require_nonempty(owner, "process owner")?;
        validate_limit(limit)?;
        let after_start = after.map(validate_process_identity).transpose()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
             FROM processes WHERE owner = ?1
               AND (?2 IS NULL OR (host_boot_id, pid, start_time) > (?2, ?3, ?4))
             ORDER BY host_boot_id, pid, start_time LIMIT ?5",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    owner,
                    after.map(|id| id.host_boot_id.as_str()),
                    after.map(|id| id.pid),
                    after_start,
                    limit as i64
                ],
                process_record_from_row,
            )
            .map_err(sql_error)?;
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

    fn record_git_observation_inner(
        &mut self,
        agent: &str,
        generation: i64,
        snapshot: &GitSnapshot,
        hints: &[FileChangeHint],
        through: u64,
        now: i64,
    ) -> Result<Vec<Event>> {
        validate_git_snapshot(snapshot)?;
        if hints.len() > MAX_PAGE_ENTRIES || encode_json(&hints)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "Git hint observation exceeds its bound",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if retained_git_workspace(&tx, agent, generation)? != snapshot.workspace {
            return Err(Error::new(
                "source_integrity",
                "Git workspace does not match its retained generation",
            ));
        }
        let agent_num = parse_agent_id(agent)?;
        let source = require_git_source(&tx, agent_num, generation)?;
        if source.state != "open" {
            return Err(Error::new("bad_state", "Git source is closed"));
        }
        let cursor = git_source_cursor(&source)?;
        let boundary = git_sequence(through)?;
        let high_water: i64 = tx
            .query_row(
                "SELECT MAX(COALESCE((SELECT MAX(seq) FROM events), 0),
                 COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if through < cursor || boundary > high_water {
            return Err(Error::new(
                "invalid",
                "Git hint cursor is outside the committed interval",
            ));
        }
        let scanned = scan_git_hints(&tx, agent, generation, cursor, boundary, MAX_PAGE_ENTRIES)?;
        if scanned.through != through {
            return Err(Error::new(
                "invalid",
                "Git hint cursor skips beyond a bounded scanned page",
            ));
        }
        let file_gap =
            git_interval_gap_covers(&tx, agent_num, generation, &source, cursor, through, "file")?;
        let mut metadata_uncovered = false;
        let mut expected = HashSet::new();
        for event in &scanned.events {
            metadata_uncovered |= event
                .data
                .get("changed_paths_truncated")
                .and_then(Value::as_bool)
                == Some(true)
                || event.data.get("redacted").and_then(Value::as_bool) == Some(true);
            let Some(paths) = event.data.get("changed_paths").and_then(Value::as_array) else {
                metadata_uncovered = true;
                continue;
            };
            for path in paths {
                let Some(path) = path.as_str() else {
                    metadata_uncovered = true;
                    continue;
                };
                if let Ok(path) = normalized_native_git_path(&snapshot.workspace, path) {
                    expected.insert((event.seq as u64, git_path_key(&snapshot.workspace, &path)));
                } else {
                    metadata_uncovered = true;
                }
            }
        }
        let baseline = load_git_baseline(&tx, &snapshot.workspace)?;
        if let Some(base) = &snapshot.base_oid {
            let prepared = load_workspace_by_path(&tx, &snapshot.workspace)?;
            if prepared
                .as_ref()
                .is_none_or(|workspace| workspace.base_oid != *base)
            {
                return Err(Error::new(
                    "source_integrity",
                    "Git base is not the workspace's frozen base OID",
                ));
            }
        }
        if baseline
            .as_ref()
            .is_some_and(|old| old.updated_at > snapshot.observed_at)
        {
            return Err(Error::new(
                "bad_state",
                "Git snapshot predates the stored HEAD baseline",
            ));
        }
        let mut seen = HashSet::with_capacity(hints.len());
        let mut covered = HashSet::with_capacity(hints.len());
        let mut attributed = Vec::new();
        for hint in hints {
            validate_git_path(&hint.path)?;
            if hint.event_seq == 0
                || hint.event_seq > through
                || !seen.insert((
                    hint.event_seq,
                    git_path_key(&snapshot.workspace, &hint.path),
                ))
            {
                return Err(Error::new(
                    "source_integrity",
                    "Git hint has an invalid or repeated cause",
                ));
            }
            let event = load_native_git_hint(&tx, agent_num, generation, hint.event_seq)?;
            if hint.ts != event.ts {
                return Err(Error::new(
                    "source_integrity",
                    "Git hint timestamp does not match its native cause",
                ));
            }
            let paths = event
                .data
                .get("changed_paths")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::new("source_integrity", "native file change has no paths"))?;
            let mut belongs = false;
            for path in paths {
                let path = path.as_str().ok_or_else(|| {
                    Error::new("source_integrity", "native changed path is not text")
                })?;
                if let Ok(path) = normalized_native_git_path(&snapshot.workspace, path) {
                    belongs |= git_path_key(&snapshot.workspace, &path)
                        == git_path_key(&snapshot.workspace, &hint.path);
                }
            }
            if !belongs {
                return Err(Error::new(
                    "source_integrity",
                    "claimed path is absent from its native file change",
                ));
            }
            // Lost-ack retry: old causes stay historical and never recreate a cleaned row.
            if hint.event_seq <= cursor {
                continue;
            }
            covered.insert((
                hint.event_seq,
                git_path_key(&snapshot.workspace, &hint.path),
            ));
            attributed.push(hint);
        }
        if (covered != expected || metadata_uncovered) && !file_gap {
            return Err(Error::new(
                "source_integrity",
                "Git cursor would skip an unconsumed in-scope native file-change path",
            ));
        }
        let mut events = Vec::new();
        if !git_interval_gap_covers(&tx, agent_num, generation, &source, cursor, through, "git")?
            && let Some(event) = git_history_gap(
                &tx,
                agent_num,
                generation,
                snapshot,
                baseline.as_ref(),
                &source,
            )?
        {
            events.push(event);
        }
        let mut dirty = snapshot.dirty.clone();
        dirty.sort_by(|a, b| {
            (&a.path, &a.status, &a.original_path).cmp(&(&b.path, &b.status, &b.original_path))
        });
        let mut committed_paths = snapshot.committed_paths.clone();
        committed_paths.sort();
        committed_paths.dedup();
        let data = json!({
            "workspace": snapshot.workspace, "head": snapshot.head, "branch": snapshot.branch,
            "dirty": dirty, "complete": snapshot.complete,
            "history_complete": snapshot.history_complete, "history_gap": snapshot.history_gap,
            "base_oid": snapshot.base_oid, "committed_paths": committed_paths, "base_complete": snapshot.base_complete,
        });
        let previous: Option<String> = tx
            .query_row(
                "SELECT data FROM events WHERE agent = ?1 AND generation = ?2
             AND source = 'git' AND kind = 'git.changed' ORDER BY seq DESC LIMIT 1",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        let previous = previous.as_deref().map(decode_json).transpose()?;
        // Delivery belongs to this binding. A partial view is not a commit receipt.
        let delivered = if previous
            .as_ref()
            .is_some_and(|view| view.get("complete").and_then(Value::as_bool) == Some(true))
        {
            previous.clone()
        } else {
            let complete: Option<String> = tx
                .query_row(
                    "SELECT data FROM events WHERE agent = ?1 AND generation = ?2
                 AND source = 'git' AND kind = 'git.changed'
                 AND json_extract(data, '$.complete') = 1 ORDER BY seq DESC LIMIT 1",
                    params![agent_num, generation],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            complete.as_deref().map(decode_json).transpose()?
        };
        let deliver_commits = delivered
            .as_ref()
            .is_none_or(|view| view.get("head") != data.get("head"));
        let changed = match previous {
            Some(previous) => canonical::digest(&previous)? != canonical::digest(&data)?,
            None => true,
        };
        if changed {
            events.push(insert_git_event(
                &tx,
                agent_num,
                generation,
                snapshot.observed_at,
                "git.changed",
                "git",
                data,
            )?);
        }
        if snapshot.complete && deliver_commits {
            for commit in &snapshot.commits {
                events.push(insert_git_event(&tx, agent_num, generation, snapshot.observed_at,
                    "git.committed", "git", json!({"workspace": snapshot.workspace, "oid": commit.oid, "paths": commit.paths}))?);
            }
        }
        for hint in attributed {
            db!(tx.execute(
                "INSERT INTO agent_edits(agent, generation, workspace, path, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(agent, generation, path) DO UPDATE
                 SET updated_at = MAX(agent_edits.updated_at, excluded.updated_at)",
                params![
                    agent_num,
                    generation,
                    snapshot.workspace,
                    hint.path,
                    hint.ts
                ],
            ));
            events.push(insert_git_event(&tx, agent_num, generation, hint.ts, "file.changed", "file",
                json!({"workspace": snapshot.workspace, "path": hint.path, "cause_seq": hint.event_seq}))?);
        }
        if snapshot.complete {
            // Only explicit clean proofs clear attribution, including older generations.
            for path in &snapshot.clean_paths {
                db!(tx.execute(
                    "DELETE FROM agent_edits WHERE workspace = ?1 AND path = ?2 AND updated_at <= ?3",
                    params![snapshot.workspace, path, snapshot.observed_at],
                ));
            }
            if baseline.as_ref().is_none_or(|old| {
                old.head != snapshot.head || old.updated_at != snapshot.observed_at
            }) {
                db!(tx.execute(
                    "INSERT INTO git_workspaces(workspace, head, updated_at) VALUES (?1, ?2, ?3)
                     ON CONFLICT(workspace) DO UPDATE SET head = excluded.head, updated_at = excluded.updated_at",
                    params![snapshot.workspace, snapshot.head, snapshot.observed_at],
                ));
            }
        }
        db!(tx.execute(
            "UPDATE source_state SET cursor = ?1, last_store_seq = COALESCE(?2, last_store_seq), updated_at = ?3
             WHERE agent = ?4 AND generation = ?5 AND source = 'git'",
            params![through.to_string(), events.last().map(|event| event.seq), now, agent_num, generation],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
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
            "SELECT seq, ts, kind, agent, generation, source, native_id, data
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
            "SELECT e.seq, e.ts, e.kind, e.agent, e.generation, e.source, e.native_id, e.data
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

// Lexical only: filesystem containment and symlink checks belong to the daemon.
fn normalized_native_git_path(workspace: &str, path: &str) -> Result<String> {
    fn portable(value: &str) -> String {
        let value = value.replace('\\', "/");
        if let Some(rest) = value.strip_prefix("//?/UNC/") {
            format!("//{rest}")
        } else {
            value.strip_prefix("//?/").unwrap_or(&value).to_owned()
        }
    }
    fn parts(value: &str) -> Result<Vec<&str>> {
        let mut parts = Vec::new();
        for part in value
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
        {
            if part == ".." {
                if parts.pop().is_none() {
                    return Err(Error::new("invalid", "file hint escapes its workspace"));
                }
            } else {
                parts.push(part);
            }
        }
        Ok(parts)
    }
    let workspace = portable(workspace);
    let path = portable(path);
    let windows = workspace.starts_with("//") || workspace.as_bytes().get(1) == Some(&b':');
    let absolute = path.starts_with('/') || path.as_bytes().get(1) == Some(&b':');
    let base = parts(&workspace)?;
    let candidate = parts(&path)?;
    let relative = if absolute {
        if candidate.len() <= base.len()
            || !base.iter().zip(&candidate).all(|(base, candidate)| {
                if windows {
                    base.eq_ignore_ascii_case(candidate)
                } else {
                    base == candidate
                }
            })
        {
            return Err(Error::new(
                "invalid",
                "native file hint is outside its workspace",
            ));
        }
        candidate[base.len()..].join("/")
    } else {
        candidate.join("/")
    };
    validate_git_path(&relative)?;
    Ok(relative)
}

fn git_path_key(workspace: &str, path: &str) -> String {
    if workspace.starts_with("\\\\")
        || workspace.starts_with("//")
        || workspace.as_bytes().get(1) == Some(&b':')
    {
        path.to_ascii_lowercase()
    } else {
        path.to_owned()
    }
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

fn validate_git_snapshot(snapshot: &GitSnapshot) -> Result<()> {
    require_nonempty(&snapshot.workspace, "Git workspace")?;
    if snapshot.dirty.len() > MAX_PAGE_ENTRIES
        || snapshot.commits.len() > MAX_PAGE_ENTRIES
        || snapshot.clean_paths.len() > MAX_PAGE_ENTRIES
        || snapshot.committed_paths.len() > MAX_PAGE_ENTRIES
        || encode_json(snapshot)?.len() > MAX_PAGE_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "Git snapshot exceeds its bound",
        ));
    }
    if let Some(head) = &snapshot.head {
        validate_git_oid(head)?;
    }
    if let Some(base) = &snapshot.base_oid {
        validate_git_oid(base)?;
    } else if snapshot.base_complete || !snapshot.committed_paths.is_empty() {
        return Err(Error::new(
            "invalid",
            "unfrozen Git base cannot report complete base coverage or committed paths",
        ));
    }
    for path in &snapshot.committed_paths {
        validate_git_path(path)?;
    }
    if let Some(branch) = &snapshot.branch {
        require_nonempty(branch, "Git branch")?;
    }
    let mut dirty = HashSet::new();
    for status in &snapshot.dirty {
        validate_git_path(&status.path)?;
        require_nonempty(&status.status, "Git path status")?;
        dirty.insert(git_path_key(&snapshot.workspace, &status.path));
        if let Some(path) = &status.original_path {
            validate_git_path(path)?;
            dirty.insert(git_path_key(&snapshot.workspace, path));
        }
    }
    for commit in &snapshot.commits {
        validate_git_oid(&commit.oid)?;
        for path in &commit.paths {
            validate_git_path(path)?;
        }
    }
    for path in &snapshot.clean_paths {
        validate_git_path(path)?;
        if snapshot.complete && dirty.contains(&git_path_key(&snapshot.workspace, path)) {
            return Err(Error::new(
                "source_integrity",
                "Git path is reported both dirty and clean",
            ));
        }
    }
    Ok(())
}

fn git_history_gap(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    snapshot: &GitSnapshot,
    baseline: Option<&GitBaseline>,
    source: &SourceStateRecord,
) -> Result<Option<Event>> {
    let moved = baseline.is_none_or(|old| old.head != snapshot.head);
    if baseline.is_some()
        && (snapshot.history_complete && !snapshot.history_gap || !moved && source.gap_id.is_some())
    {
        return Ok(None);
    }
    let from = baseline.and_then(|old| old.head.as_deref());
    let anchor = baseline
        .map(|old| old.updated_at.to_string())
        .unwrap_or_else(|| "missing".to_owned());
    let gap_id = format!(
        "git-history:a{agent}#{generation}:{anchor}:{}:{}",
        from.unwrap_or("unborn"),
        snapshot.head.as_deref().unwrap_or("unborn")
    );
    if source.gap_id.as_deref() == Some(gap_id.as_str()) {
        return Ok(None);
    }
    let data = json!({
        "gap_id": gap_id, "agent_id": format_agent_id(agent), "generation": generation,
        "source": GIT_SOURCE, "families": ["git"],
        "reason": if baseline.is_none() { "initial_baseline" } else { "history_gap" },
        "from": from, "to": snapshot.head,
    });
    let digest = canonical::digest(&data)?;
    let event = insert_git_event(
        tx,
        agent,
        generation,
        snapshot.observed_at,
        "source.gap",
        "git",
        data,
    )?;
    db!(tx.execute(
        "UPDATE source_state SET gap_id = ?1, gap_digest = ?2, gap_from = ?3,
             gap_to = ?4, gap_seq = ?5 WHERE agent = ?6 AND generation = ?7 AND source = 'git'",
        params![
            gap_id,
            digest,
            from,
            snapshot.head,
            event.seq,
            agent,
            generation
        ],
    ));
    Ok(Some(event))
}

fn insert_git_event(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    ts: i64,
    kind: &str,
    family: &str,
    data: Value,
) -> Result<Event> {
    insert_event(
        tx,
        EventInsert {
            ts,
            kind,
            family: Some(family),
            agent: Some(agent),
            generation: Some(generation),
            source: GIT_SOURCE,
            native_id: None,
            cursor: None,
            payload_digest: None,
            data,
        },
    )
}

const WORKSPACE_COLUMNS: &str = "agent, target_generation, start_command_id, project, repo_path,
    repo_common_dir, requested_branch, base_ref, base_oid, worktree_path, frozen_setup,
    phase, setup_index, outcome, updated_at";

fn workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspacePreparation> {
    let frozen_setup: Vec<core::WorkspaceSetup> = serde_json::from_str(&row.get::<_, String>(10)?)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let phase = parse_enum(&row.get::<_, String>(11)?, "workspace phase").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let outcome = row
        .get::<_, Option<String>>(13)?
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                13,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let setup_index = usize::try_from(row.get::<_, i64>(12)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    if setup_index > frozen_setup.len() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(Error::new(
                "invalid",
                "stored setup index exceeds the frozen setup length",
            )),
        ));
    }
    Ok(WorkspacePreparation {
        agent: format_agent_id(row.get(0)?),
        target_generation: row.get(1)?,
        start_command_id: row.get(2)?,
        project: row.get(3)?,
        repo_path: row.get(4)?,
        repo_common_dir: row.get(5)?,
        requested_branch: row.get(6)?,
        base_ref: row.get(7)?,
        base_oid: row.get(8)?,
        worktree_path: row.get(9)?,
        frozen_setup,
        phase,
        setup_index,
        outcome,
        updated_at: row.get(14)?,
    })
}

fn load_workspace(connection: &Connection, agent: i64) -> Result<Option<WorkspacePreparation>> {
    connection
        .query_row(
            &format!("SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE agent = ?1"),
            [agent],
            workspace_from_row,
        )
        .optional()
        .map_err(sql_error)
}

fn load_workspace_by_path(
    connection: &Connection,
    path: &str,
) -> Result<Option<WorkspacePreparation>> {
    connection
        .query_row(
            &format!("SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE worktree_path = ?1"),
            [path],
            workspace_from_row,
        )
        .optional()
        .map_err(sql_error)
}

fn workspace_phase_name(phase: WorkspacePhase) -> &'static str {
    match phase {
        WorkspacePhase::Creating => "creating",
        WorkspacePhase::SetupReady => "setup_ready",
        WorkspacePhase::SetupRunning => "setup_running",
        WorkspacePhase::Ready => "ready",
        WorkspacePhase::Unknown => "unknown",
        WorkspacePhase::Failed => "failed",
        WorkspacePhase::Removing => "removing",
        WorkspacePhase::Removed => "removed",
    }
}

fn workspace_cancel_requested(workspace: &WorkspacePreparation) -> bool {
    workspace
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("cancel_requested"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn require_absolute_workspace_path(path: &str) -> Result<()> {
    let bytes = path.as_bytes();
    let absolute = path.starts_with('/')
        || path.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'));
    if !absolute
        || path.contains('\0')
        || path.starts_with("\\\\.\\")
        || path.replace('\\', "/").split('/').any(|part| part == "..")
    {
        return Err(Error::new(
            "invalid",
            "workspace reference must be an absolute non-traversing path",
        ));
    }
    Ok(())
}

fn validate_setup_file(file: &FrozenSetupFile) -> Result<()> {
    require_absolute_workspace_path(&file.canonical_path)?;
    if file.size > i64::MAX as u64
        || file.sha256.len() != 64
        || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "invalid",
            "invalid frozen setup file size or SHA-256",
        ));
    }
    Ok(())
}

fn validate_workspace_input(input: &WorkspacePreparationInput) -> Result<()> {
    if encode_json(input)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "workspace preparation exceeds its bound",
        ));
    }
    require_nonempty(&input.project, "workspace project")?;
    require_nonempty(&input.requested_branch, "workspace branch")?;
    if input.requested_branch.starts_with('-') || input.requested_branch.contains('\0') {
        return Err(Error::new("invalid", "invalid requested workspace branch"));
    }
    if let Some(base) = &input.base_ref {
        require_nonempty(base, "workspace base ref")?;
    }
    validate_git_oid(&input.base_oid)?;
    for path in [
        &input.repo_path,
        &input.repo_common_dir,
        &input.worktree_path,
    ] {
        require_absolute_workspace_path(path)?;
    }
    if input.repo_path == input.worktree_path {
        return Err(Error::new(
            "invalid",
            "prepared worktree must differ from owner repository",
        ));
    }
    for setup in &input.frozen_setup {
        validate_setup_file(&setup.program)?;
        if setup.timeout_ms == 0 || setup.timeout_ms > i64::MAX as u64 {
            return Err(Error::new("invalid", "invalid setup timeout"));
        }
        for arg in &setup.args {
            match arg {
                WorkspaceSetupArg::Literal(value) if value.contains('\0') => {
                    return Err(Error::new("invalid", "setup argument contains NUL"));
                }
                WorkspaceSetupArg::Literal(_) => {}
                WorkspaceSetupArg::File(file) => validate_setup_file(file)?,
            }
        }
    }
    Ok(())
}

fn managed_path_contains(root: &str, candidate: &str) -> bool {
    fn normalized(path: &str) -> String {
        #[cfg(windows)]
        {
            let path = path.replace('\\', "/");
            let path = match path.strip_prefix("//?/UNC/") {
                Some(rest) => format!("//{rest}"),
                None => path.strip_prefix("//?/").unwrap_or(&path).to_owned(),
            };
            path.to_ascii_lowercase()
        }
        #[cfg(not(windows))]
        {
            path.to_owned()
        }
    }
    let root = normalized(root);
    let candidate = normalized(candidate);
    let mut candidate = Path::new(&candidate).components();
    Path::new(&root)
        .components()
        .all(|component| candidate.next() == Some(component))
}

fn require_managed_path_ready(connection: &Connection, path: &str) -> Result<()> {
    // Check every containing root, including a non-ready parent of a nested checkout.
    // Stream only path/status candidates; never load frozen setup payloads here.
    let mut statement = connection
        .prepare(
            "SELECT worktree_path FROM workspaces
         WHERE phase <> 'ready' OR json_extract(outcome, '$.cancel_requested') = 1",
        )
        .map_err(sql_error)?;
    let roots = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for root in roots {
        if managed_path_contains(&root.map_err(sql_error)?, path) {
            return Err(Error::new(
                "bad_state",
                "containing managed workspace is not ready or was removed",
            ));
        }
    }
    Ok(())
}

fn gate_workspace_start(
    connection: &Connection,
    agent: &Agent,
    command: &Command,
    target: i64,
) -> Result<()> {
    if let Some(path) = &agent.workspace {
        require_managed_path_ready(connection, path)?;
    }
    if let Some(workspace) = load_workspace(connection, parse_agent_id(&agent.id)?)?
        && (workspace.phase != WorkspacePhase::Ready
            || workspace.target_generation != target
            || workspace.start_command_id != command.id
            || workspace_cancel_requested(&workspace)
            || agent.workspace.as_deref() != Some(workspace.worktree_path.as_str()))
    {
        return Err(Error::new(
            "bad_state",
            "native start requires its exact ready workspace preparation",
        ));
    }
    Ok(())
}

fn require_workspace_removable(
    connection: &Connection,
    agent: &Agent,
    workspace: &WorkspacePreparation,
) -> Result<()> {
    if !agent.is_terminal()
        || agent.active_turn.is_some()
        || matches!(
            workspace.phase,
            WorkspacePhase::Creating
                | WorkspacePhase::SetupReady
                | WorkspacePhase::SetupRunning
                | WorkspacePhase::Removing
                | WorkspacePhase::Removed
        )
        || workspace
            .outcome
            .as_ref()
            .and_then(|value| value.get("stage"))
            .and_then(Value::as_str)
            == Some("removal")
            && workspace.phase == WorkspacePhase::Unknown
            && workspace
                .outcome
                .as_ref()
                .and_then(|value| value.get("result"))
                .and_then(|value| value.get("state"))
                .and_then(Value::as_str)
                != Some("failed")
    {
        return Err(Error::new(
            "bad_state",
            "worktree removal requires a terminal owner and a settled preparation",
        ));
    }
    let start = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("source_integrity", "workspace start command is missing"))?;
    if matches!(start.state, CommandState::Accepted | CommandState::Queued) {
        return Err(Error::new(
            "busy",
            "cancel the pending start before removing its workspace",
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT workspace FROM agents WHERE id <> ?1 AND workspace IS NOT NULL
         AND state IN ('created','starting','idle','busy','waiting','unknown')",
        )
        .map_err(sql_error)?;
    let borrowers = statement
        .query_map([parse_agent_id(&agent.id)?], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for borrower in borrowers {
        if managed_path_contains(&workspace.worktree_path, &borrower.map_err(sql_error)?) {
            return Err(Error::new(
                "busy",
                "another nonterminal Agent references this worktree or a descendant",
            ));
        }
    }
    let mut sources = connection
        .prepare("SELECT repo_path FROM workspaces WHERE agent <> ?1 AND phase <> 'removed'")
        .map_err(sql_error)?;
    let sources = sources
        .query_map([parse_agent_id(&agent.id)?], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for source in sources {
        if managed_path_contains(&workspace.worktree_path, &source.map_err(sql_error)?) {
            return Err(Error::new(
                "busy",
                "another retained worktree depends on this source repository",
            ));
        }
    }
    let mut inputs = connection
        .prepare(
            "SELECT w.frozen_setup, w.setup_index FROM workspaces w
         JOIN commands c ON c.id = w.start_command_id
         WHERE w.phase <> 'removed' AND w.setup_index < json_array_length(w.frozen_setup)
           AND c.state IN ('accepted','queued','dispatched','unknown')",
        )
        .map_err(sql_error)?;
    let inputs = inputs
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    for input in inputs {
        let (setup, index) = input.map_err(sql_error)?;
        let setup: Vec<core::WorkspaceSetup> = serde_json::from_str(&setup).map_err(|error| {
            Error::new("internal", format!("invalid retained setup input: {error}"))
        })?;
        let index = usize::try_from(index)
            .map_err(|_| Error::new("internal", "invalid retained setup index"))?;
        if index > setup.len() {
            return Err(Error::new(
                "internal",
                "retained setup index exceeds input length",
            ));
        }
        for entry in setup.iter().skip(index) {
            let referenced =
                managed_path_contains(&workspace.worktree_path, &entry.program.canonical_path)
                    || entry.args.iter().any(|arg| match arg {
                        WorkspaceSetupArg::File(file) => {
                            managed_path_contains(&workspace.worktree_path, &file.canonical_path)
                        }
                        WorkspaceSetupArg::Literal(_) => false,
                    });
            if referenced {
                return Err(Error::new(
                    "busy",
                    "pending setup retains a declared input inside this worktree",
                ));
            }
        }
    }
    Ok(())
}

fn require_workspace_sources_ready(
    connection: &Connection,
    repo_path: &str,
    setup: &[core::WorkspaceSetup],
    index: usize,
) -> Result<()> {
    require_managed_path_ready(connection, repo_path)?;
    for entry in setup.iter().skip(index) {
        require_managed_path_ready(connection, &entry.program.canonical_path)?;
        for arg in &entry.args {
            if let WorkspaceSetupArg::File(file) = arg {
                require_managed_path_ready(connection, &file.canonical_path)?;
            }
        }
    }
    Ok(())
}

fn persist_workspace(
    tx: &Transaction<'_>,
    old: &WorkspacePreparation,
    next: &WorkspacePreparation,
) -> Result<()> {
    if encode_json(next)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "workspace row exceeds readback bound",
        ));
    }
    let changed = tx.execute(
        "UPDATE workspaces SET target_generation = ?1, start_command_id = ?2, phase = ?3, setup_index = ?4, outcome = ?5, updated_at = ?6
         WHERE agent = ?7 AND start_command_id = ?8 AND target_generation = ?9 AND phase = ?10 AND setup_index = ?11 AND outcome IS ?12",
        params![next.target_generation, next.start_command_id, workspace_phase_name(next.phase), next.setup_index as i64,
            next.outcome.as_ref().map(encode_json).transpose()?, next.updated_at, parse_agent_id(&old.agent)?,
            old.start_command_id, old.target_generation, workspace_phase_name(old.phase), old.setup_index as i64,
            old.outcome.as_ref().map(encode_json).transpose()?],
    ).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "stale_generation",
            "workspace preparation marker changed",
        ));
    }
    Ok(())
}

fn workspace_event(
    tx: &Transaction<'_>,
    workspace: &WorkspacePreparation,
    now: i64,
) -> Result<Event> {
    insert_daemon_event(
        tx,
        now,
        &format!("workspace.{}", workspace_phase_name(workspace.phase)),
        "workspace",
        Some(parse_agent_id(&workspace.agent)?),
        Some(workspace.target_generation),
        json!({"start_command_id": workspace.start_command_id, "phase": workspace.phase,
            "setup_index": workspace.setup_index, "target_generation": workspace.target_generation, "outcome": workspace.outcome}),
    )
}

fn admit_workspace_preparation(
    tx: &Transaction<'_>,
    request: &SubmitCommand,
    input: Option<&WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let command = load_command_record(tx, &request.id)?
        .ok_or_else(|| Error::new("internal", "start command disappeared"))?;
    let agent_num = command
        .target
        .ok_or_else(|| Error::new("internal", "start has no Agent"))?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("internal", "start Agent disappeared"))?;
    let target = agent
        .generation
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "generation overflow"))?;
    if request.verb == "launch" {
        let Some(input) = input else {
            return Ok(());
        };
        require_workspace_sources_ready(tx, &input.repo_path, &input.frozen_setup, 0)?;
        let workspace = WorkspacePreparation {
            agent: agent.id,
            target_generation: target,
            start_command_id: request.id.clone(),
            project: input.project.clone(),
            repo_path: input.repo_path.clone(),
            repo_common_dir: input.repo_common_dir.clone(),
            requested_branch: input.requested_branch.clone(),
            base_ref: input.base_ref.clone(),
            base_oid: input.base_oid.clone(),
            worktree_path: input.worktree_path.clone(),
            frozen_setup: input.frozen_setup.clone(),
            phase: WorkspacePhase::Creating,
            setup_index: 0,
            outcome: Some(json!({"stage": "creation", "reuse": false})),
            updated_at: now,
        };
        if encode_json(&workspace)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "workspace row exceeds readback bound",
            ));
        }
        db!(tx.execute(
            "INSERT INTO workspaces(agent,target_generation,start_command_id,project,repo_path,repo_common_dir,requested_branch,base_ref,base_oid,
             worktree_path,frozen_setup,phase,setup_index,outcome,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'creating',0,?12,?13)",
            params![agent_num, target, workspace.start_command_id, workspace.project, workspace.repo_path, workspace.repo_common_dir,
                workspace.requested_branch, workspace.base_ref, workspace.base_oid, workspace.worktree_path, encode_json(&workspace.frozen_setup)?,
                workspace.outcome.as_ref().map(encode_json).transpose()?, now],
        ));
        events.push(workspace_event(tx, &workspace, now)?);
    } else if let Some(mut workspace) = load_workspace(tx, agent_num)? {
        if workspace.phase != WorkspacePhase::Ready
            || workspace_cancel_requested(&workspace)
            || workspace.target_generation != agent.generation
            || workspace.setup_index != workspace.frozen_setup.len()
        {
            return Err(Error::new(
                "bad_state",
                "resume requires a previously ready retained worktree",
            ));
        }
        if let Some(input) = input {
            let retained: WorkspacePreparationInput = serde_json::from_value(json!(&workspace))
                .map_err(|_| Error::new("internal", "invalid retained workspace"))?;
            if *input != retained {
                return Err(Error::new(
                    "id_conflict",
                    "resume cannot replace frozen workspace inputs",
                ));
            }
        }
        require_workspace_sources_ready(
            tx,
            &workspace.repo_path,
            &workspace.frozen_setup,
            workspace.setup_index,
        )?;
        let old = workspace.clone();
        workspace.start_command_id = request.id.clone();
        workspace.target_generation = target;
        workspace.phase = WorkspacePhase::Creating;
        workspace.outcome = Some(json!({"stage": "creation", "reuse": true}));
        workspace.updated_at = now;
        persist_workspace(tx, &old, &workspace)?;
        events.push(workspace_event(tx, &workspace, now)?);
        close_finished_workspace_process_source(tx, agent_num, old.target_generation, now, events)?;
    } else if input.is_some() {
        return Err(Error::new(
            "unsupported",
            "resume does not create a replacement for an unmanaged workspace",
        ));
    }
    Ok(())
}

fn preparation_command(
    connection: &Connection,
    workspace: &WorkspacePreparation,
) -> Result<(CommandRecord, Agent)> {
    let command = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("not_found", "workspace start command was not found"))?;
    let agent = load_agent_by_num(connection, parse_agent_id(&workspace.agent)?)?
        .ok_or_else(|| Error::new("not_found", "workspace Agent was not found"))?;
    if command.target != Some(parse_agent_id(&workspace.agent)?)
        || !matches!(command.command.verb.as_str(), "launch" | "resume")
        || agent.generation.checked_add(1) != Some(workspace.target_generation)
        || command.command.generation != Some(agent.generation)
        || agent.active_turn.is_some()
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
        || (command.command.verb == "launch"
            && (agent.generation != 0 || agent.state != AgentState::Created))
        || (command.command.verb == "resume" && !agent.is_terminal())
    {
        return Err(Error::new(
            "stale_generation",
            "preparation no longer precedes its exact native dispatch",
        ));
    }
    Ok((command, agent))
}

fn cancel_workspace_input(
    tx: &Transaction<'_>,
    command: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if let Some(linked) = load_linked_command(tx, command)?
        && matches!(linked.state, CommandState::Accepted | CommandState::Queued)
    {
        let outcome = json!({"stage": "workspace", "reason": "start_not_dispatched"});
        update_command_state(
            tx,
            &linked.id,
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
            linked.target.as_deref().map(parse_agent_id).transpose()?,
            linked.generation,
            json!({"command_id": linked.id, "reason": "workspace_preparation"}),
        )?);
    }
    Ok(())
}

fn settle_preparation_command(
    tx: &Transaction<'_>,
    workspace: &WorkspacePreparation,
    state: CommandState,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let (command, agent) = preparation_command(tx, workspace)?;
    if !matches!(
        command.state,
        CommandState::Accepted | CommandState::Queued | CommandState::Unknown
    ) || !core::workspace_preparation_transition_allowed(command.state, state)
    {
        return Err(Error::new(
            "bad_state",
            "workspace start is already settled or dispatched",
        ));
    }
    let outcome = json!({"stage": "workspace", "native_dispatched": false, "target_generation": workspace.target_generation, "detail": workspace.outcome});
    // preparation_command plus the typed workspace caller prove the pre-native
    // marker/verb/generation exception; ordinary settlement cannot use it.
    write_command_state(tx, &command.command.id, state, Some(&outcome), None, now)?;
    if command.command.verb == "launch" {
        let next = match state {
            CommandState::Failed => AgentState::Failed,
            CommandState::Cancelled => AgentState::Stopped,
            _ => AgentState::Created,
        };
        db!(tx.execute("UPDATE agents SET state = ?1, updated_at = ?2 WHERE id = ?3 AND generation = 0 AND state = 'created'",
            params![agent_state_name(next), now, parse_agent_id(&agent.id)?]));
    }
    if matches!(state, CommandState::Failed | CommandState::Cancelled) {
        cancel_workspace_input(tx, &command.command.id, now, events)?;
    }
    events.push(insert_daemon_event(
        tx,
        now,
        &format!("command.{}", command_state_name(state)),
        "command",
        Some(parse_agent_id(&agent.id)?),
        Some(agent.generation),
        json!({"command_id": command.command.id, "outcome": outcome}),
    )?);
    Ok(())
}

enum WorkspaceAction {
    Creation(WorkspaceCreationOutcome),
    Begin(usize),
    Setup(usize, WorkspaceSetupOutcome),
    CancelRequest(String),
    Cancel(String, WorkspaceCancelOutcome),
    Removal(WorkspaceRemovalOutcome),
}

impl Store {
    pub fn get_workspace(&self, agent: &str) -> Result<Option<WorkspacePreparation>> {
        load_workspace(&self.connection, parse_agent_id(agent)?)
    }

    pub fn get_workspace_by_path(&self, path: &str) -> Result<Option<WorkspacePreparation>> {
        require_nonempty(path, "managed workspace path")?;
        load_workspace_by_path(&self.connection, path)
    }

    /// Physical cleanup must wait for every containing workspace's Git source to drain.
    pub fn has_open_git_sources_within(&self, worktree_path: &str) -> Result<bool> {
        require_absolute_workspace_path(worktree_path)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT COALESCE(
                 (SELECT json_extract(e.data, '$.workspace') FROM events e
                  WHERE e.agent = s.agent AND e.generation = s.generation
                    AND e.kind = 'source.opened' AND json_extract(e.data, '$.source') = 'git'
                  ORDER BY e.seq DESC LIMIT 1), a.workspace)
             FROM source_state s LEFT JOIN agents a ON a.id = s.agent
             WHERE s.source = 'git' AND s.state = 'open'",
            )
            .map_err(sql_error)?;
        let paths = statement
            .query_map([], |row| row.get::<_, Option<String>>(0))
            .map_err(sql_error)?;
        for path in paths {
            let path = path
                .map_err(sql_error)?
                .filter(|path| !path.trim().is_empty())
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "open Git source has no retained workspace identity",
                    )
                })?;
            if managed_path_contains(worktree_path, &path) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn list_workspace_preparations(
        &self,
        after_agent: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WorkspacePreparation>> {
        validate_limit(limit)?;
        let after = after_agent.map(parse_agent_id).transpose()?.unwrap_or(0);
        let mut statement = self.connection.prepare(&format!(
            "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE agent > ?1 ORDER BY agent LIMIT ?2"
        )).map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, limit as i64], workspace_from_row)
            .map_err(sql_error)?;
        let mut workspaces = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        enforce_page_bytes(&mut workspaces)?;
        Ok(workspaces)
    }

    pub fn settle_workspace_creation(
        &mut self,
        command: &str,
        target_generation: i64,
        outcome: WorkspaceCreationOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Creation(outcome),
            now,
        )
    }

    pub fn begin_workspace_setup(
        &mut self,
        command: &str,
        target_generation: i64,
        index: usize,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Begin(index),
            now,
        )
    }

    pub fn settle_workspace_setup(
        &mut self,
        command: &str,
        target_generation: i64,
        index: usize,
        outcome: WorkspaceSetupOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Setup(index, outcome),
            now,
        )
    }

    pub fn request_workspace_cancel(
        &mut self,
        stop_command: &str,
        start_command: &str,
        target_generation: i64,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            start_command,
            target_generation,
            WorkspaceAction::CancelRequest(stop_command.to_owned()),
            now,
        )
    }

    pub fn settle_workspace_cancel(
        &mut self,
        stop_command: &str,
        start_command: &str,
        target_generation: i64,
        outcome: WorkspaceCancelOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            start_command,
            target_generation,
            WorkspaceAction::Cancel(stop_command.to_owned(), outcome),
            now,
        )
    }

    pub fn settle_workspace_removal(
        &mut self,
        command: &str,
        target_generation: i64,
        outcome: WorkspaceRemovalOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Removal(outcome),
            now,
        )
    }

    fn workspace_action(
        &mut self,
        command: &str,
        target: i64,
        action: WorkspaceAction,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.ensure_writer_healthy()?;
        let result = self.workspace_action_inner(command, target, action, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn workspace_action_inner(
        &mut self,
        command_id: &str,
        target: i64,
        action: WorkspaceAction,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let command = load_command_record(&tx, command_id)?
            .ok_or_else(|| Error::new("not_found", "workspace command was not found"))?;
        let agent_num = command
            .target
            .ok_or_else(|| Error::new("invalid", "workspace command has no Agent"))?;
        let mut workspace = load_workspace(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "workspace preparation was not found"))?;
        let old = workspace.clone();
        let removal = matches!(&action, WorkspaceAction::Removal(_));
        if workspace.target_generation != target
            || (!removal && workspace.start_command_id != command_id)
        {
            return Err(Error::new(
                "stale_generation",
                "workspace command or target generation changed",
            ));
        }
        let reuse = workspace
            .outcome
            .as_ref()
            .and_then(|value| value.get("reuse"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let marker = match &action {
            WorkspaceAction::Creation(outcome) => {
                json!({"stage": "creation", "reuse": reuse, "result": outcome})
            }
            WorkspaceAction::Begin(index) => json!({"stage": "setup", "index": index}),
            WorkspaceAction::Setup(index, outcome) => {
                json!({"stage": "setup", "index": index, "result": outcome})
            }
            WorkspaceAction::CancelRequest(stop) => {
                json!({"stage": "cancel", "stop_command_id": stop, "cancel_requested": true})
            }
            WorkspaceAction::Cancel(stop, outcome) => {
                json!({"stage": "cancel", "stop_command_id": stop, "cancel_requested": true, "result": outcome})
            }
            WorkspaceAction::Removal(outcome) => {
                json!({"stage": "removal", "command_id": command_id, "result": outcome,
                    "previous_phase": workspace.outcome.as_ref().and_then(|value| value.get("previous_phase"))})
            }
        };
        if encode_json(&marker)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "workspace outcome exceeds its bound",
            ));
        }
        if workspace.outcome.as_ref() == Some(&marker)
            && !matches!(
                &action,
                WorkspaceAction::Begin(_) | WorkspaceAction::CancelRequest(_)
            )
        {
            let mut events = Vec::new();
            if removal && workspace.phase == WorkspacePhase::Removed {
                close_finished_workspace_process_source(&tx, agent_num, target, now, &mut events)?;
            }
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: workspace,
                events,
            });
        }
        let mut events = Vec::new();
        if let WorkspaceAction::CancelRequest(stop) = &action {
            request_workspace_cancel_tx(&tx, &mut workspace, stop, now, &mut events)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: workspace,
                events,
            });
        }
        let mut result_state = None;
        let mut stop_result = None;
        if removal {
            let agent = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
            if command.command.verb != "worktree.remove"
                || command.command.generation != Some(agent.generation)
                || !agent.is_terminal()
                || !matches!(
                    command.state,
                    CommandState::Dispatched | CommandState::Unknown
                )
                || !matches!(
                    workspace.phase,
                    WorkspacePhase::Removing | WorkspacePhase::Unknown
                )
                || workspace
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("stage"))
                    .and_then(Value::as_str)
                    != Some("removal")
                || workspace
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("command_id"))
                    .and_then(Value::as_str)
                    != Some(command_id)
            {
                return Err(Error::new(
                    "bad_state",
                    "removal settlement requires its exact removing/unknown marker",
                ));
            }
        } else {
            preparation_command(&tx, &workspace)?;
            if !matches!(
                command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            ) {
                return Err(Error::new(
                    "bad_state",
                    "native start was already dispatched or settled",
                ));
            }
            if workspace_cancel_requested(&workspace)
                && !matches!(&action, WorkspaceAction::Cancel(_, _))
            {
                return Err(Error::new("bad_state", "workspace cancellation is pending"));
            }
        }
        match action {
            WorkspaceAction::Creation(outcome) => {
                if !matches!(
                    workspace.phase,
                    WorkspacePhase::Creating | WorkspacePhase::Unknown
                ) || workspace
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("stage"))
                    .and_then(Value::as_str)
                    != Some("creation")
                    || workspace.setup_index
                        != if reuse {
                            workspace.frozen_setup.len()
                        } else {
                            0
                        }
                {
                    return Err(Error::new(
                        "bad_state",
                        "creation readback cannot resolve an unknown setup/removal",
                    ));
                }
                match outcome {
                    WorkspaceCreationOutcome::Verified {
                        repo_path,
                        repo_common_dir,
                        worktree_path,
                        branch,
                        head,
                    } => {
                        validate_git_oid(&head)?;
                        if repo_path != workspace.repo_path
                            || repo_common_dir != workspace.repo_common_dir
                            || worktree_path != workspace.worktree_path
                            || branch != workspace.requested_branch
                            || (!reuse && head != workspace.base_oid)
                        {
                            return Err(Error::new(
                                "source_integrity",
                                "worktree readback does not match its frozen identity",
                            ));
                        }
                        workspace.phase = if workspace.setup_index == workspace.frozen_setup.len() {
                            WorkspacePhase::Ready
                        } else {
                            WorkspacePhase::SetupReady
                        };
                        if command.state == CommandState::Unknown {
                            if !core::workspace_preparation_transition_allowed(
                                command.state,
                                CommandState::Accepted,
                            ) {
                                return Err(Error::new(
                                    "bad_state",
                                    "workspace creation readback transition is not allowed",
                                ));
                            }
                            write_command_state(
                                &tx,
                                command_id,
                                CommandState::Accepted,
                                None,
                                None,
                                now,
                            )?;
                            events.push(insert_daemon_event(&tx, now, "command.accepted", "command", Some(agent_num), command.command.generation,
                                json!({"command_id": command_id, "reason": "exact_workspace_creation_readback"}))?);
                        }
                    }
                    WorkspaceCreationOutcome::Failed { .. } => {
                        if old.phase == WorkspacePhase::Unknown {
                            return Err(Error::new(
                                "unknown",
                                "absent or partial creation readback remains unknown",
                            ));
                        }
                        workspace.phase = WorkspacePhase::Failed;
                        result_state = Some(CommandState::Failed);
                    }
                    WorkspaceCreationOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        result_state = Some(CommandState::Unknown);
                    }
                }
            }
            WorkspaceAction::Begin(index) => {
                if workspace.phase == WorkspacePhase::SetupRunning {
                    return Err(Error::new(
                        "unknown",
                        "setup entry already has a running marker; never spawn it again",
                    ));
                }
                if workspace.phase != WorkspacePhase::SetupReady
                    || index != workspace.setup_index
                    || index >= workspace.frozen_setup.len()
                    || !matches!(command.state, CommandState::Accepted | CommandState::Queued)
                {
                    return Err(Error::new(
                        "bad_state",
                        "setup requires the exact next unperformed entry",
                    ));
                }
                workspace.phase = WorkspacePhase::SetupRunning;
            }
            WorkspaceAction::Setup(index, outcome) => {
                let not_started = matches!(&outcome, WorkspaceSetupOutcome::NotStarted { .. });
                if !(workspace.phase == WorkspacePhase::SetupRunning
                    || not_started && workspace.phase == WorkspacePhase::SetupReady)
                    || index != workspace.setup_index
                    || index >= workspace.frozen_setup.len()
                    || !matches!(command.state, CommandState::Accepted | CommandState::Queued)
                {
                    return Err(Error::new(
                        "unknown",
                        "setup result does not match its live running marker",
                    ));
                }
                match outcome {
                    WorkspaceSetupOutcome::Succeeded => {
                        workspace.setup_index += 1;
                        workspace.phase = if workspace.setup_index == workspace.frozen_setup.len() {
                            WorkspacePhase::Ready
                        } else {
                            WorkspacePhase::SetupReady
                        };
                    }
                    WorkspaceSetupOutcome::Failed { exit_code: 0 } => {
                        return Err(Error::new("invalid", "zero exit code must use succeeded"));
                    }
                    WorkspaceSetupOutcome::Failed { .. }
                    | WorkspaceSetupOutcome::NotStarted { .. } => {
                        workspace.phase = WorkspacePhase::Failed;
                        result_state = Some(CommandState::Failed);
                    }
                    WorkspaceSetupOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        result_state = Some(CommandState::Unknown);
                    }
                }
            }
            WorkspaceAction::Cancel(stop_id, outcome) => {
                let stop = load_command_record(&tx, &stop_id)?
                    .ok_or_else(|| Error::new("not_found", "stop command was not found"))?;
                if !workspace_cancel_requested(&workspace)
                    || workspace
                        .outcome
                        .as_ref()
                        .and_then(|value| value.get("stop_command_id"))
                        .and_then(Value::as_str)
                        != Some(stop_id.as_str())
                    || stop.command.verb != "stop"
                    || stop.target != Some(agent_num)
                    || stop.command.generation != command.command.generation
                    || !matches!(stop.state, CommandState::Dispatched | CommandState::Unknown)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "cancellation is not bound to its dispatched stop command",
                    ));
                }
                let stopped = matches!(outcome, WorkspaceCancelOutcome::Stopped);
                workspace.phase = if stopped {
                    WorkspacePhase::Failed
                } else {
                    WorkspacePhase::Unknown
                };
                result_state = Some(if stopped {
                    CommandState::Cancelled
                } else {
                    CommandState::Unknown
                });
                stop_result = Some((
                    stop_id,
                    if stopped {
                        CommandState::Applied
                    } else {
                        CommandState::Unknown
                    },
                ));
            }
            WorkspaceAction::Removal(outcome) => {
                let state = match outcome {
                    WorkspaceRemovalOutcome::Removed {
                        repo_common_dir,
                        worktree_path,
                        branch,
                        branch_retained,
                    } => {
                        if repo_common_dir != workspace.repo_common_dir
                            || worktree_path != workspace.worktree_path
                            || branch != workspace.requested_branch
                            || !branch_retained
                        {
                            return Err(Error::new(
                                "source_integrity",
                                "removal readback must prove exact absence and retain the branch",
                            ));
                        }
                        workspace.phase = WorkspacePhase::Removed;
                        db!(tx.execute(
                            "DELETE FROM agent_edits WHERE workspace = ?1",
                            [&workspace.worktree_path],
                        ));
                        CommandState::Applied
                    }
                    WorkspaceRemovalOutcome::Failed { .. } => {
                        let previous = workspace
                            .outcome
                            .as_ref()
                            .and_then(|value| value.get("previous_phase"))
                            .cloned()
                            .ok_or_else(|| {
                                Error::new(
                                    "source_integrity",
                                    "removal has no previous preparation phase",
                                )
                            })?;
                        let previous: WorkspacePhase =
                            serde_json::from_value(previous).map_err(|_| {
                                Error::new("source_integrity", "invalid previous removal phase")
                            })?;
                        if !matches!(
                            previous,
                            WorkspacePhase::Ready
                                | WorkspacePhase::Failed
                                | WorkspacePhase::Unknown
                        ) {
                            return Err(Error::new(
                                "source_integrity",
                                "removal cannot restore an active or removed preparation",
                            ));
                        }
                        workspace.phase = previous;
                        CommandState::Failed
                    }
                    WorkspaceRemovalOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        CommandState::Unknown
                    }
                };
                update_command_state(&tx, command_id, state, Some(&marker), None, now)?;
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    &format!("command.{}", command_state_name(state)),
                    "command",
                    Some(agent_num),
                    command.command.generation,
                    json!({"command_id": command_id, "outcome": marker}),
                )?);
            }
            WorkspaceAction::CancelRequest(_) => unreachable!(),
        }
        workspace.outcome = Some(marker);
        workspace.updated_at = now;
        if let Some(state) = result_state {
            settle_preparation_command(&tx, &workspace, state, now, &mut events)?;
        }
        if let Some((stop_id, state)) = stop_result {
            let outcome = json!({"stage": "workspace_cancel", "start_command_id": workspace.start_command_id, "target_generation": target, "detail": workspace.outcome});
            update_command_state(&tx, &stop_id, state, Some(&outcome), None, now)?;
            events.push(insert_daemon_event(
                &tx,
                now,
                &format!("command.{}", command_state_name(state)),
                "command",
                Some(agent_num),
                command.command.generation,
                json!({"command_id": stop_id, "outcome": outcome}),
            )?);
        }
        persist_workspace(&tx, &old, &workspace)?;
        events.push(workspace_event(&tx, &workspace, now)?);
        if removal && workspace.phase == WorkspacePhase::Removed {
            close_finished_workspace_process_source(&tx, agent_num, target, now, &mut events)?;
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: workspace,
            events,
        })
    }
}

fn pending_workspace_for_stop(
    connection: &Connection,
    agent: &Agent,
) -> Result<Option<WorkspacePreparation>> {
    let Some(workspace) = load_workspace(connection, parse_agent_id(&agent.id)?)? else {
        return Ok(None);
    };
    if agent.generation.checked_add(1) != Some(workspace.target_generation)
        || matches!(
            workspace.phase,
            WorkspacePhase::Removing | WorkspacePhase::Removed | WorkspacePhase::Failed
        )
    {
        return Ok(None);
    }
    let start = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("source_integrity", "workspace start disappeared"))?;
    Ok(matches!(
        start.state,
        CommandState::Accepted | CommandState::Queued | CommandState::Unknown
    )
    .then_some(workspace))
}

fn request_workspace_cancel_tx(
    tx: &Transaction<'_>,
    workspace: &mut WorkspacePreparation,
    stop_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if matches!(
        workspace.phase,
        WorkspacePhase::Failed | WorkspacePhase::Removing | WorkspacePhase::Removed
    ) || workspace
        .outcome
        .as_ref()
        .and_then(|value| value.get("stage"))
        .and_then(Value::as_str)
        == Some("removal")
    {
        return Err(Error::new(
            "bad_state",
            "workspace cancellation cannot replace removal or terminal preparation",
        ));
    }
    let (start, agent) = preparation_command(tx, workspace)?;
    let stop = load_command_record(tx, stop_id)?
        .ok_or_else(|| Error::new("not_found", "stop command was not found"))?;
    if stop.command.verb != "stop"
        || stop.target != start.target
        || stop.command.generation != Some(agent.generation)
        || stop.expected_turn_id.is_some()
        || !matches!(
            start.state,
            CommandState::Accepted | CommandState::Queued | CommandState::Unknown
        )
        || !matches!(stop.state, CommandState::Accepted | CommandState::Queued)
    {
        return Err(Error::new(
            "bad_state",
            "preparation cancellation requires its exact undispatched stop",
        ));
    }
    if workspace_cancel_requested(workspace) {
        return Err(Error::new(
            "unknown",
            "cancellation was already requested; read back instead of replaying stop",
        ));
    }
    let old = workspace.clone();
    let outcome = workspace.outcome.get_or_insert_with(|| json!({}));
    outcome["cancel_requested"] = json!(true);
    outcome["stop_command_id"] = json!(stop_id);
    workspace.updated_at = now;
    persist_workspace(tx, &old, workspace)?;
    let result = json!({"stage": "workspace_cancel", "start_command_id": workspace.start_command_id, "target_generation": workspace.target_generation});
    update_command_state(
        tx,
        stop_id,
        CommandState::Dispatched,
        Some(&result),
        None,
        now,
    )?;
    events.push(workspace_event(tx, workspace, now)?);
    events.push(insert_daemon_event(
        tx,
        now,
        "command.dispatched",
        "command",
        start.target,
        Some(agent.generation),
        json!({"command_id": stop_id, "outcome": result}),
    )?);
    Ok(())
}

fn recover_workspaces(tx: &Transaction<'_>, now: i64, events: &mut Vec<Event>) -> Result<()> {
    let mut statement = tx.prepare(&format!(
        "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE phase IN ('creating','setup_running','removing')
         OR (json_extract(outcome, '$.cancel_requested') = 1 AND phase IN ('setup_ready','ready'))"
    )).map_err(sql_error)?;
    let rows = statement
        .query_map([], workspace_from_row)
        .map_err(sql_error)?;
    let workspaces = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    for mut workspace in workspaces {
        let old = workspace.clone();
        let removing = workspace.phase == WorkspacePhase::Removing;
        workspace.phase = WorkspacePhase::Unknown;
        let outcome = workspace.outcome.get_or_insert_with(|| json!({}));
        outcome["reason"] = json!("daemon_restart");
        workspace.updated_at = now;
        if !removing {
            settle_preparation_command(tx, &workspace, CommandState::Unknown, now, events)?;
        }
        persist_workspace(tx, &old, &workspace)?;
        events.push(workspace_event(tx, &workspace, now)?);
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

fn process_state_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Running => "running",
        ProcessState::Exited => "exited",
        ProcessState::Unknown => "unknown",
    }
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

fn insert_process_event(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    kind: &str,
    record: &ProcessRecord,
    scope: ProcessSourceKind,
) -> Result<Event> {
    insert_event(
        tx,
        EventInsert {
            ts: now,
            kind,
            family: Some(scope.family()),
            agent: Some(agent),
            generation: Some(generation),
            source: scope.source(),
            native_id: None,
            cursor: None,
            payload_digest: None,
            data: json!(record),
        },
    )
}

fn close_finished_workspace_process_source(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let workspace = load_workspace(tx, agent)?
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
            let workspace =
                load_workspace(connection, parse_agent_id(&agent.id)?)?.ok_or_else(|| {
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
    last_handoff_digest: Option<Vec<u8>>,
    last_handoff: Option<String>,
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
        origin: row.get(16)?,
        observe_only: row.get(17)?,
        own_identity: row.get(18)?,
    })
}

fn agent_from_record(connection: &Connection, record: AgentRecord) -> Result<Agent> {
    let pending = decode_pending_requests(&record.pending)?;
    let preparation = match load_workspace(connection, record.id)? {
        Some(workspace) => Some(workspace),
        None => record
            .workspace
            .as_deref()
            .map(|path| load_workspace_by_path(connection, path))
            .transpose()?
            .flatten(),
    };
    Ok(Agent {
        id: format_agent_id(record.id),
        generation: record.generation,
        origin: parse_enum(&record.origin, "Agent origin")?,
        observe_only: record.observe_only,
        own_identity: record.own_identity,
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
                    pending, state, active_turn, created_at, updated_at, origin, observe_only, (token IS NOT NULL AND observe_only = 0)
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
    write_command_state(tx, id, state, outcome, generation, now)
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
) -> Result<Vec<Value>> {
    let mut command_events = Vec::new();
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
                ) || !core::command_transition_allowed(command.state, CommandState::Applied)
                    || command.command.turn_id.is_some()
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
                command_events.push(json!({
                    "command_id": command_id, "state": "applied", "outcome": outcome,
                }));
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
    Ok(command_events)
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
    observer::require_controlled(&agent)?;
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
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
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
    observer::require_controlled(&agent)?;
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
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
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
    observer::require_controlled(&agent)?;
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
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
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

impl Store {
    /// Safe diagnostic source views; controlled plugin acknowledgements retain
    /// their separate current-generation authorization in read_source_state.
    pub fn list_source_states(&self, agent: &str, generation: i64) -> Result<Vec<Value>> {
        let agent_num = parse_agent_id(agent)?;
        let current = load_agent_by_num(&self.connection, agent_num)?
            .ok_or_else(|| Error::new("not_found", "source-state Agent was not found"))?;
        let pending = generation > current.generation;
        if generation < 0
            || pending
                && load_workspace(&self.connection, agent_num)?
                    .is_none_or(|workspace| workspace.target_generation != generation)
        {
            return Err(Error::new(
                "stale_generation",
                "source-state generation is neither retained nor the exact preparation target",
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT source FROM source_state WHERE agent = ?1 AND generation = ?2
             AND (?3 = 0 OR source = 'workspace_process') ORDER BY source LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![agent_num, generation, pending, MAX_PAGE_ENTRIES as i64 + 1],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let names = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        if names.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "source-state view exceeds row bound",
            ));
        }
        let mut views = Vec::with_capacity(names.len());
        for source in names {
            let state = load_source_state(&self.connection, agent_num, generation, &source)?
                .ok_or_else(|| Error::new("source_integrity", "source state disappeared"))?;
            let mut owners = self
                .connection
                .prepare(
                    "SELECT family, mechanism FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 AND source = ?3 ORDER BY family LIMIT ?4",
                )
                .map_err(sql_error)?;
            let rows = owners
                .query_map(
                    params![agent_num, generation, source, MAX_PAGE_ENTRIES as i64 + 1],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(sql_error)?;
            let owners = rows
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?;
            if owners.len() > MAX_PAGE_ENTRIES {
                return Err(Error::new(
                    "frame_too_large",
                    "source owner view exceeds row bound",
                ));
            }
            let families: Vec<&str> = owners.iter().map(|(family, _)| family.as_str()).collect();
            let owners: Vec<Value> = owners.iter().map(|(family, mechanism)|
                json!({"family": family, "source": source, "mechanism": mechanism})).collect();
            views.push(json!({
                "agent_id": agent, "generation": generation, "source": source,
                "families": families, "owners": owners, "state": state.state, "health": state.health,
                "cursor": state.cursor, "last_store_seq": state.last_store_seq,
                "last_batch_id": state.last_batch_id, "last_batch_first_seq": state.last_batch_first_seq,
                "last_batch_last_seq": state.last_batch_last_seq,
                "gap_id": state.gap_id, "gap_from": state.gap_from, "gap_to": state.gap_to, "gap_seq": state.gap_seq,
            }));
        }
        if encode_json(&views)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "source-state view exceeds byte bound",
            ));
        }
        Ok(views)
    }

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
        let observer_ready = match load_agent_by_num(&self.connection, agent_num)? {
            Some(agent) => observer::live_ready(&self.connection, &agent, generation)?,
            None => false,
        };
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
            "last_handoff_digest": state.last_handoff_digest,
            "last_handoff": state.last_handoff.as_deref().map(decode_json).transpose()?,
            "owners": source_handoff::owner_bindings(&self.connection, agent_num, generation)?,
            "observer_ready": observer_ready,
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
        let result = self.record_source_gap_inner(gap, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn record_observed_source_gap(
        &mut self,
        gap: SourceGap,
        expected_subject: (&str, &str),
        now: i64,
    ) -> Result<Change<SourceGapAck>> {
        self.ensure_writer_healthy()?;
        let result = self.record_source_gap_inner(gap, Some(expected_subject), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_gap_inner(
        &mut self,
        gap: SourceGap,
        expected_subject: Option<(&str, &str)>,
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
        if let Some(subject) = expected_subject {
            observer::require_live_subject(&tx, &agent, gap.generation, subject)?;
            observer_live::require_transcript_families(&tx, &agent, &gap.source, &families)?;
        } else if !matches!(
            gap.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) {
            observer::require_controlled(&agent)?;
        }
        let historical_git = gap.source == GIT_SOURCE
            && gap.generation > 0
            && gap.generation < agent.generation
            && families
                .iter()
                .all(|family| matches!(family.as_str(), "git" | "file"));
        if agent.generation != gap.generation && !historical_git {
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
        let result = self.put_batch_inner(batch, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn put_observed_batch(
        &mut self,
        batch: SourceBatch,
        expected_subject: (&str, &str),
        now: i64,
    ) -> Result<BatchCommit> {
        self.ensure_writer_healthy()?;
        let result = self.put_batch_inner(batch, Some(expected_subject), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn put_batch_inner(
        &mut self,
        batch: SourceBatch,
        expected_subject: Option<(&str, &str)>,
        now: i64,
    ) -> Result<BatchCommit> {
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
        }
        let batch_digest = batch
            .semantic_digest()
            .map_err(|error| Error::new("invalid", error.to_string()))?
            .to_vec();

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
        if let Some(subject) = expected_subject {
            observer::require_live_subject(&tx, &projection, batch.generation, subject)?;
            let families = batch
                .events
                .iter()
                .map(|event| family_for_kind(&event.kind).map(str::to_owned))
                .collect::<Result<Vec<_>>>()?;
            observer_live::require_transcript_families(&tx, &projection, &batch.source, &families)?;
            for event in &batch.events {
                if !event.data.is_object()
                    || ["command_id", "cause", "request_id"]
                        .iter()
                        .any(|field| event.data.get(*field).is_some())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "observed facts cannot carry local command or request settlement",
                    ));
                }
            }
        } else {
            observer::require_controlled(&projection)?;
        }
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
        let mut command_events = Vec::new();
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
            let (next, effects) = if expected_subject.is_some() {
                observer_live::persist_turn_fact(&tx, &projection, &native_event)?;
                (projection.clone(), Vec::new())
            } else {
                core::apply(&projection, &native_event, now)?
            };
            let family = family_for_kind(&native_event.kind)?;
            let event_digest = canonical::digest(&native_event.semantic_value())?;
            let cursor = if index + 1 == batch.events.len() {
                batch.cursor.as_deref()
            } else {
                None
            };
            let stored = insert_native_event(
                &tx,
                NativeEventInsert {
                    ts: native_event.ts.unwrap_or(now),
                    native_event: &native_event,
                    family,
                    agent: agent_num,
                    generation: batch.generation,
                    source: &batch.source,
                    cursor,
                    payload_digest: &event_digest,
                },
            )?;
            command_events.extend(persist_effects(
                &tx,
                &effects,
                agent_num,
                batch.generation,
                now,
                &native_event,
                &next,
            )?);
            if next.is_terminal() {
                terminalize_generation_commands(
                    &tx,
                    agent_num,
                    batch.generation,
                    now,
                    "session_ended",
                    native_event.kind == "session.ended",
                )?;
                db!(tx.execute(
                    "UPDATE pending_requests SET state = 'stale'
                     WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
                    params![agent_num, batch.generation],
                ));
            }
            if expected_subject.is_none() {
                update_agent_projection(&tx, &next)?;
            }
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
        // Keep native batch ACKs scoped to native rows; the associated command
        // event follows those rows but commits in the same transaction.
        for data in command_events {
            committed_events.push(insert_daemon_event(
                &tx,
                now,
                "command.applied",
                "command",
                Some(agent_num),
                Some(batch.generation),
                data,
            )?);
        }
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

#[cfg(test)]
mod tests;
