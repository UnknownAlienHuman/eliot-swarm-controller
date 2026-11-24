//! Where an Agent's work is recorded in Git. The daemon writes the Git objects;
//! Store keeps the latest fact per generation and the event history.
use super::*;
use eliot_core::api::{
    AgentHandoffView, AgentWorkTruthView, CheckpointRestoreRequest, CheckpointRestoreResponse,
    GitCheckpointState, GitCheckpointView, GitCheckpointWithheld, HandoffReason, SendTransfer,
};
use serde::{Deserialize, Serialize};

pub const GIT_CHECKPOINT: &str = "git.checkpoint";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GitCheckpointInput {
    pub state: GitCheckpointState,
    pub trigger: String,
    pub turn_id: Option<String>,
    pub head: Option<String>,
    pub checkpoint_ref: Option<String>,
    pub oid: Option<String>,
    /// Exact task selected when this capture was reserved.
    #[serde(default)]
    pub task_ref: Option<String>,
    pub fingerprint: Option<String>,
    pub paths: Vec<String>,
    pub paths_truncated: bool,
    pub withheld: Vec<GitCheckpointWithheld>,
    pub withheld_truncated: bool,
    /// Bytes this checkpoint newly wrote to the object store.
    pub captured_bytes: u64,
    pub error: Option<String>,
    pub journaled_through: Option<i64>,
}

impl GitCheckpointInput {
    pub fn failed(trigger: &str, turn_id: Option<String>, error: String) -> Self {
        Self {
            state: GitCheckpointState::Failed,
            trigger: trigger.to_owned(),
            turn_id,
            head: None,
            checkpoint_ref: None,
            oid: None,
            task_ref: None,
            fingerprint: None,
            paths: Vec::new(),
            paths_truncated: false,
            withheld: Vec::new(),
            withheld_truncated: false,
            captured_bytes: 0,
            error: Some(error),
            journaled_through: None,
        }
    }
}

#[derive(Debug)]
pub struct GitCheckpointIntent {
    pub common_dir: String,
    pub agent: String,
    pub generation: i64,
    pub workspace: String,
    /// Initially the conservative charge for an interrupted capture, then
    /// the exact candidate that must be reconciled after ref publication.
    pub input: GitCheckpointInput,
}

#[derive(Clone, Debug)]
pub struct CheckpointRetentionGeneration {
    pub agent_id: i64,
    pub generation: i64,
}

pub enum GitCheckpointReservation {
    Reserved,
    Deferred,
    Skipped,
}

const CHECKPOINT_RESTORE: &str = "checkpoint.restore";

pub enum CheckpointRestoreAdmission {
    Start { oid: String },
    Replay(Result<CheckpointRestoreResponse>),
}

pub struct CheckpointRestoreIntent {
    pub id: String,
    pub writer_key: String,
    pub writer_custody_bound: bool,
    pub writer_boot: Option<String>,
    pub writer_identity: Option<core::ProcessIdentity>,
}

#[derive(Deserialize, Serialize)]
pub struct CheckpointRestoreWriterObservation {
    pub state: core::ProcessState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<core::ProcessIdentity>,
}

#[derive(Deserialize)]
struct CheckpointRestoreFailure {
    #[serde(flatten)]
    error: Error,
    #[serde(default)]
    writer_at_abandonment: Option<CheckpointRestoreWriterObservation>,
}

impl CheckpointRestoreFailure {
    fn into_error(mut self) -> Error {
        if let Some(writer) = self.writer_at_abandonment {
            let state = match writer.state {
                core::ProcessState::Running => "running",
                core::ProcessState::Exited => "exited",
                core::ProcessState::Unknown => "unverified",
            };
            let mut message = format!("restore reservation abandoned with writer state {state}");
            if let Some(identity) = writer.identity {
                message.push_str(&format!(
                    " (PID {}, start time {})",
                    identity.pid, identity.start_time,
                ));
            }
            message.push_str(". ");
            message.push_str(&self.error.message);
            self.error.message = Box::new(message);
        }
        self.error
    }
}

pub(super) fn require_no_checkpoint_restore(conn: &Connection, workspace: &str) -> Result<()> {
    if checkpoint_restore_overlaps(conn, workspace)? {
        return Err(Error::new(
            "busy",
            "workspace is reserved for checkpoint restore",
        ));
    }
    Ok(())
}

pub(super) fn checkpoint_restore_overlaps(conn: &Connection, workspace: &str) -> Result<bool> {
    let mut statement = conn
        .prepare("SELECT workspace FROM checkpoint_restore_intents")
        .map_err(sql_error)?;
    let paths = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for path in paths {
        if workspace::workspace_paths_overlap(workspace, &path.map_err(sql_error)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn require_restore_workspace_idle(tx: &Transaction<'_>, workspace: &str) -> Result<()> {
    let mut statement = tx
        .prepare("SELECT id, workspace, state FROM agents WHERE workspace IS NOT NULL")
        .map_err(sql_error)?;
    let agents = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?;
    for agent in agents {
        let (id, path, state) = agent.map_err(sql_error)?;
        if !workspace::workspace_paths_overlap(workspace, &path) {
            continue;
        }
        if !matches!(state.as_str(), "stopped" | "failed" | "lost") {
            return Err(Error::new(
                "busy",
                format!(
                    "restore workspace overlaps active Agent {}",
                    format_agent_id(id)
                ),
            ));
        }
        let agent = load_agent_by_num(tx, id)?
            .ok_or_else(|| Error::new("source_integrity", "restore workspace Agent is missing"))?;
        if let Some(blocker) = handoff::session_exit_blocker(tx, &agent)? {
            return Err(Error::new(
                "busy",
                format!(
                    "restore workspace Agent {} has unproven exit: {blocker}",
                    agent.id
                ),
            ));
        }
        if has_pending_resume_tx(tx, id, agent.generation, "")? {
            return Err(Error::new(
                "busy",
                "restore workspace has a pending Agent Resume",
            ));
        }
    }
    Ok(())
}

fn restore_digest(request: &CheckpointRestoreRequest) -> Result<Vec<u8>> {
    let args = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "cannot encode checkpoint restore"))?;
    canonical::digest(&args)
}

fn checkpoint_restore_receipt(
    conn: &Connection,
    id: &str,
    digest: &[u8],
) -> Result<Option<Result<CheckpointRestoreResponse>>> {
    let Some((principal, method, saved_digest, saved)) = write_binding(conn, id)? else {
        return Ok(None);
    };
    if principal != "owner" || method != CHECKPOINT_RESTORE || saved_digest != digest {
        return Err(Error::new(
            "id_conflict",
            "checkpoint restore id is already bound",
        ));
    }
    let outcome: Option<std::result::Result<CheckpointRestoreResponse, CheckpointRestoreFailure>> =
        serde_json::from_str(&saved)
            .map_err(|_| Error::new("source_integrity", "checkpoint restore receipt is invalid"))?;
    Ok(Some(outcome.map_or_else(
        || {
            Err(Error::new(
                "unknown",
                "checkpoint restore effect has no retained outcome",
            ))
        },
        |outcome| outcome.map_err(CheckpointRestoreFailure::into_error),
    )))
}

fn require_restore_intent_update(changed: rusqlite::Result<usize>) -> Result<()> {
    if changed.map_err(sql_error)? != 1 {
        return Err(Error::new(
            "source_integrity",
            "checkpoint restore intent changed",
        ));
    }
    Ok(())
}

fn settle_restore_receipt(
    conn: &Connection,
    id: &str,
    outcome: &Result<CheckpointRestoreResponse>,
) -> Result<()> {
    conn.execute(
        "UPDATE writes SET result = ?1
         WHERE id = ?2 AND method = ?3 AND principal = 'owner' AND result = 'null'",
        params![encode_json(outcome)?, id, CHECKPOINT_RESTORE],
    )
    .map_err(sql_error)?;
    Ok(())
}

/// What the next checkpoint of a generation starts from.
#[derive(Clone, Debug, Default)]
pub struct GitCheckpointBasis {
    /// Content identity of the last successful checkpoint.
    pub fingerprint: Option<String>,
    /// The last snapshot commit, whose blobs need no new storage.
    pub snapshot: Option<String>,
    /// New object bytes this generation has written so far.
    pub captured_bytes: u64,
    /// When the last checkpoint ran, if one has.
    pub updated_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct JournalTurn {
    pub turn_id: String,
    pub outcome: String,
    pub completed_at: i64,
    pub final_text: Option<String>,
}

fn state_name(state: GitCheckpointState) -> &'static str {
    match state {
        GitCheckpointState::Snapshot => "snapshot",
        GitCheckpointState::Clean => "clean",
        GitCheckpointState::Unchanged => "unchanged",
        GitCheckpointState::Failed => "failed",
    }
}

/// Copy the already-bound Git task through a lifecycle change in the same
/// transaction. Command and event retention cannot change this binding.
pub(super) fn inherit_work_task_tx(
    tx: &Transaction<'_>,
    from: &Agent,
    to: &Agent,
    transfers: &[SendTransfer],
    now: i64,
) -> Result<()> {
    let from = (parse_agent_id(&from.id)?, from.generation);
    let to = (parse_agent_id(&to.id)?, to.generation);
    inherit_work_task_generation_tx(tx, from, to, now)?;
    for transfer in transfers {
        inherit_work_task_binding_tx(tx, from, to, &transfer.original, &transfer.successor, now)?;
    }
    Ok(())
}

fn inherit_work_task_binding_tx(
    tx: &Transaction<'_>,
    from: (i64, i64),
    to: (i64, i64),
    original: &str,
    successor: &str,
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO git_work_tasks(agent, generation, task_command, task_ref, principal, updated_at)
         SELECT ?3, ?4, ?7, task_ref, principal, ?5
         FROM git_work_tasks
         WHERE agent = ?1 AND generation = ?2 AND task_command = ?6
         ON CONFLICT(agent, generation, task_command) DO UPDATE SET
             task_ref = excluded.task_ref,
             principal = COALESCE(excluded.principal, git_work_tasks.principal),
             updated_at = excluded.updated_at",
        params![from.0, from.1, to.0, to.1, now, original, successor],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn inherit_work_task_generation_tx(
    tx: &Transaction<'_>,
    from: (i64, i64),
    to: (i64, i64),
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO git_work_tasks(agent, generation, task_command, task_ref, principal, updated_at)
         SELECT ?3, ?4, checkpoint.task_command, checkpoint.task_ref,
                COALESCE(task.principal, original.principal), ?5
         FROM git_checkpoints checkpoint
         LEFT JOIN git_work_tasks task
           ON task.agent = checkpoint.agent AND task.generation = checkpoint.generation
          AND task.task_command = checkpoint.task_command
         LEFT JOIN commands original ON original.id = checkpoint.task_command
         WHERE checkpoint.agent = ?1 AND checkpoint.generation = ?2
           AND checkpoint.task_ref IS NOT NULL AND checkpoint.task_command IS NOT NULL
         ON CONFLICT(agent, generation, task_command) DO UPDATE SET
             task_ref = excluded.task_ref,
             principal = COALESCE(excluded.principal, git_work_tasks.principal),
             updated_at = excluded.updated_at",
        params![from.0, from.1, to.0, to.1, now],
    )
    .map_err(sql_error)?;
    if from.0 == to.0 {
        tx.execute(
            "INSERT INTO git_work_tasks(agent, generation, task_command, task_ref, principal, updated_at)
             SELECT ?3, ?4, task_command, task_ref, principal, ?5
             FROM git_work_tasks
             WHERE agent = ?1 AND generation = ?2
             ON CONFLICT(agent, generation, task_command) DO UPDATE SET
                 task_ref = excluded.task_ref,
                 principal = COALESCE(excluded.principal, git_work_tasks.principal),
                 updated_at = excluded.updated_at",
            params![from.0, from.1, to.0, to.1, now],
        )
        .map_err(sql_error)?;
    }
    tx.execute(
        "INSERT INTO git_checkpoints(agent, generation, state, task_ref, task_command, updated_at)
         SELECT ?3, ?4, 'none', task_ref, task_command, ?5
         FROM git_checkpoints WHERE agent = ?1 AND generation = ?2 AND task_ref IS NOT NULL
         ON CONFLICT(agent, generation) DO UPDATE SET task_ref = excluded.task_ref,
             task_command = excluded.task_command",
        params![from.0, from.1, to.0, to.1, now],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn require_no_checkpoint_intent_tx(tx: &Transaction<'_>, source: &Agent) -> Result<()> {
    let pending: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM git_checkpoint_intents
             WHERE agent = ?1 AND generation = ?2)",
            params![parse_agent_id(&source.id)?, source.generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if pending {
        return Err(Error::new("busy", "source checkpoint has not finished"));
    }
    Ok(())
}

fn checkpoint_switch_gate(
    tx: &Transaction<'_>,
    intent: &GitCheckpointIntent,
    agent: i64,
    source: &Agent,
) -> Result<Option<GitCheckpointReservation>> {
    use GitCheckpointReservation::{Deferred, Skipped};
    let retired: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_successors WHERE agent = ?1)",
            [agent],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if retired {
        return Ok(Some(Skipped));
    }
    let switch: Option<(String, bool)> = tx
        .query_row(
            "SELECT p.state, EXISTS(SELECT 1 FROM commands c
                 WHERE c.parent_command = p.id AND c.link_kind = 'switch' AND c.ordinal = 0)
             FROM commands p INDEXED BY commands_unsettled_created
             WHERE p.verb = 'switch' AND p.method = 'swarm.switch'
               AND p.state IN ('accepted','queued','dispatched','unknown')
               AND json_extract(p.args, '$.from') = ?1
               AND json_extract(p.args, '$.from_generation') = ?2
             LIMIT 1",
            params![intent.agent, intent.generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(match switch {
        Some((_, true)) => Some(Deferred),
        Some((state, false))
            if state == "dispatched"
                && intent.input.trigger == "handoff"
                && source.is_terminal()
                && handoff::session_exit_blocker(tx, source)?.is_none() =>
        {
            None
        }
        Some(_) => Some(Deferred),
        None => None,
    })
}

impl Store {
    pub fn retained_checkpoint_restore(
        &self,
        request: &CheckpointRestoreRequest,
    ) -> Result<Option<Result<CheckpointRestoreResponse>>> {
        self.ensure_committed_reads()?;
        checkpoint_restore_receipt(&self.connection, &request.id, &restore_digest(request)?)
    }

    pub fn reserve_checkpoint_restore(
        &mut self,
        request: &CheckpointRestoreRequest,
        workspace: &str,
        common_dir: &str,
        now: i64,
    ) -> Result<CheckpointRestoreAdmission> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let digest = restore_digest(request)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(outcome) = checkpoint_restore_receipt(&tx, &request.id, &digest)? {
                return Ok(CheckpointRestoreAdmission::Replay(outcome));
            }
            if command_binding(&tx, &request.id)?.is_some() {
                return Err(Error::new(
                    "id_conflict",
                    "restore id is already used by a command",
                ));
            }
            validate_id_age(&request.id, now)?;
            let oid: String = tx
                .query_row(
                    "SELECT oid FROM git_checkpoints
                     WHERE agent = ?1 AND generation = ?2 AND checkpoint_ref = ?3
                       AND oid IS NOT NULL
                     UNION SELECT oid FROM git_checkpoint_restore_points
                     WHERE agent = ?1 AND generation = ?2 AND checkpoint_ref = ?3",
                    params![
                        parse_agent_id(&request.agent)?,
                        request.generation,
                        request.checkpoint_ref
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    Error::new(
                        "not_found",
                        "checkpoint reference is not retained for restore",
                    )
                })?;
            require_no_checkpoint_restore(&tx, workspace)?;
            workspace::require_managed_path_ready(&tx, workspace)?;
            require_restore_workspace_idle(&tx, workspace)?;
            if repo_git_write_conflicts_tx(&tx, "", common_dir)? {
                return Err(Error::new(
                    "busy",
                    "restore repository has a pending Git write",
                ));
            }
            coordination::retain_write_tx(
                &tx,
                &request.id,
                "owner",
                CHECKPOINT_RESTORE,
                &digest,
                &Value::Null,
                now,
            )?;
            tx.execute(
                "INSERT INTO checkpoint_restore_intents(id, workspace, common_dir) VALUES (?1, ?2, ?3)",
                params![request.id, workspace, common_dir],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(CheckpointRestoreAdmission::Start { oid })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn settle_checkpoint_restore(
        &mut self,
        id: &str,
        outcome: Result<CheckpointRestoreResponse>,
    ) -> Result<Result<CheckpointRestoreResponse>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let (armed, applying): (bool, bool) = tx
                .query_row(
                    "SELECT writer_boot IS NOT NULL, applying
                     FROM checkpoint_restore_intents WHERE id = ?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    Error::new("source_integrity", "checkpoint restore intent is missing")
                })?;
            let outcome = if armed || applying {
                outcome.map_err(|error| {
                    Error::new(
                        "unknown",
                        format!(
                            "checkpoint restore did not settle; inspect the worktree: {}",
                            error.message
                        ),
                    )
                })
            } else {
                outcome
            };
            if !armed && (!applying || outcome.is_ok()) {
                require_restore_intent_update(
                    tx.execute("DELETE FROM checkpoint_restore_intents WHERE id = ?1", [id]),
                )?;
            }
            settle_restore_receipt(&tx, id, &outcome)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(outcome)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn mark_checkpoint_restore_applying(&mut self, id: &str) -> Result<()> {
        self.update_restore_writer(
            "UPDATE checkpoint_restore_intents SET applying = 1
             WHERE id = ?1 AND applying = 0 AND writer_boot IS NULL",
            [id],
        )
    }

    pub fn arm_checkpoint_restore_writer(&mut self, id: &str, boot: &str) -> Result<()> {
        self.update_restore_writer(
            "UPDATE checkpoint_restore_intents SET writer_boot = ?2,
               writer_namespace = (SELECT namespace FROM custody_binding WHERE singleton = 1)
             WHERE id = ?1 AND writer_boot IS NULL",
            params![id, boot],
        )
    }

    pub fn record_checkpoint_restore_writer(
        &mut self,
        id: &str,
        identity: &core::ProcessIdentity,
    ) -> Result<()> {
        self.update_restore_writer(
            "UPDATE checkpoint_restore_intents SET writer_identity = ?2
             WHERE id = ?1 AND writer_boot IS NOT NULL AND writer_identity IS NULL",
            params![id, encode_json(identity)?],
        )
    }

    pub fn close_checkpoint_restore_writer(&mut self, id: &str) -> Result<()> {
        self.update_restore_writer(
            "UPDATE checkpoint_restore_intents
             SET writer_boot = NULL, writer_identity = NULL, writer_namespace = NULL, custody_error = NULL
             WHERE id = ?1 AND writer_boot IS NOT NULL",
            [id],
        )
    }

    fn update_restore_writer(&mut self, sql: &str, params: impl rusqlite::Params) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = require_restore_intent_update(self.connection.execute(sql, params));
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn checkpoint_restore_intents(
        &self,
        settled_id: Option<&str>,
    ) -> Result<Vec<CheckpointRestoreIntent>> {
        self.read_checkpoint_restore_intents(
            "WHERE ?1 IS NULL OR (r.id = ?1 AND w.result <> 'null')",
            [settled_id],
        )
    }

    pub fn pending_checkpoint_restore_writers(
        &self,
        after: &str,
    ) -> Result<Vec<CheckpointRestoreIntent>> {
        self.read_checkpoint_restore_intents(
            "WHERE r.id > ?1 AND r.writer_boot IS NOT NULL AND w.result <> 'null'
             ORDER BY r.id LIMIT 16",
            [after],
        )
    }

    fn read_checkpoint_restore_intents(
        &self,
        filter: &str,
        bindings: impl rusqlite::Params,
    ) -> Result<Vec<CheckpointRestoreIntent>> {
        let mut statement = self
            .connection
            .prepare_cached(&format!(
                "SELECT r.id, r.writer_boot, r.writer_identity, r.writer_namespace,
                    (SELECT namespace FROM custody_binding WHERE singleton = 1)
             FROM checkpoint_restore_intents r JOIN writes w ON w.id = r.id {filter}"
            ))
            .map_err(sql_error)?;
        statement
            .query_map(bindings, |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(sql_error)?
            .map(|row| {
                let (id, writer_boot, identity, captured, namespace) = row.map_err(sql_error)?;
                Ok(CheckpointRestoreIntent {
                    writer_key: format!("{namespace}-{id}"),
                    writer_custody_bound: captured.as_deref() == Some(namespace.as_str()),
                    id,
                    writer_boot,
                    writer_identity: identity
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|_| {
                            Error::new("source_integrity", "restore writer identity is invalid")
                        })?,
                })
            })
            .collect()
    }

    /// A stopped Git process proves custody closure. Only a restore that never
    /// began applying worktree or index changes can release automatically.
    pub fn recover_checkpoint_restore(
        &mut self,
        id: &str,
        writer_closed: bool,
        custody_error: Option<&str>,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if writer_closed {
                tx.execute(
                    "UPDATE checkpoint_restore_intents SET writer_boot = NULL, writer_identity = NULL,
                        writer_namespace = NULL, custody_error = NULL
                     WHERE id = ?1 AND applying = 1 AND writer_boot IS NOT NULL",
                    [id],
                ).map_err(sql_error)?;
                tx.execute(
                    "DELETE FROM checkpoint_restore_intents WHERE id = ?1 AND applying = 0",
                    [id],
                )
                .map_err(sql_error)?;
            } else {
                tx.execute(
                    "UPDATE checkpoint_restore_intents SET custody_error = ?2
                            WHERE id = ?1 AND writer_boot IS NOT NULL AND custody_error IS NOT ?2",
                    params![id, custody_error],
                )
                .map_err(sql_error)?;
            }
            settle_restore_receipt(
                &tx,
                id,
                &Err(Error::new(
                    "unknown",
                    "checkpoint restore was interrupted; inspect the worktree",
                )),
            )?;
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn release_checkpoint_restore(
        &mut self,
        action_id: &str,
        restore_id: &str,
        writer_closed: bool,
        abandon: Option<(&str, CheckpointRestoreWriterObservation)>,
        now: i64,
    ) -> Result<Value> {
        let method = if abandon.is_some() {
            "checkpoint.restore.abandon"
        } else {
            "checkpoint.restore.release"
        };
        self.ensure_writer_healthy()?;
        let result = (|| {
            let mut payload = json!({"restore_id": restore_id});
            if let Some((reason, _)) = &abandon {
                require_nonempty(reason, "restore abandonment reason")?;
                if reason.len() > 1024 {
                    return Err(Error::new(
                        "invalid",
                        "restore abandonment reason exceeds 1024 bytes",
                    ));
                }
                payload["reason"] = json!(reason);
            }
            let digest = canonical::digest(&payload)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some((principal, saved_method, saved_digest, saved)) =
                write_binding(&tx, action_id)?
            {
                if principal != "owner" || saved_method != method || saved_digest != digest {
                    return Err(Error::new(
                        "id_conflict",
                        "restore release id is already bound",
                    ));
                }
                return decode_json(&saved);
            }
            validate_id_age(action_id, now)?;
            if command_binding(&tx, action_id)?.is_some() || digest_id_bound(&tx, action_id)? {
                return Err(Error::new(
                    "id_conflict",
                    "restore release id is already bound",
                ));
            }
            if !writer_closed && abandon.is_none() {
                return Err(Error::new(
                    "busy",
                    "checkpoint restore writer exit is unproven",
                ));
            }
            let released = tx
                .execute(
                    "DELETE FROM checkpoint_restore_intents WHERE id = ?1
                 AND EXISTS (SELECT 1 FROM writes WHERE id = ?1 AND result <> 'null')",
                    [restore_id],
                )
                .map_err(sql_error)?;
            if released != 1 {
                return Err(Error::new(
                    "stale_item",
                    "checkpoint restore no longer needs release",
                ));
            }
            let mut response = json!({"restore_id": restore_id, "released": true});
            if let Some((reason, writer)) = abandon {
                let writer = serde_json::to_value(writer).map_err(|_| {
                    Error::new("internal", "cannot encode restore writer observation")
                })?;
                let updated = tx.execute(
                    "UPDATE writes SET result = json_set(result, '$.Err.writer_at_abandonment', json(?2))
                     WHERE id = ?1 AND principal = 'owner' AND method = ?3
                        AND json_type(result, '$.Err') = 'object'",
                    params![restore_id, encode_json(&writer)?, CHECKPOINT_RESTORE],
                ).map_err(sql_error)?;
                if updated != 1 {
                    return Err(Error::new(
                        "source_integrity",
                        "checkpoint restore has no retained failure",
                    ));
                }
                response["abandoned"] = json!(true);
                response["reason"] = json!(reason);
                response["writer_at_abandonment"] = writer;
            }
            coordination::retain_write_tx(
                &tx, action_id, "owner", method, &digest, &response, now,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(response)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn settle_checkpoint_restore_stop(
        &mut self,
        id: &str,
        outcome: &Result<Value>,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = require_restore_intent_update(self.connection.execute(
            "UPDATE writes SET result = ?2
             WHERE id = ?1 AND method = 'checkpoint.restore.stop' AND principal = 'owner' AND result = 'null'",
            params![id, encode_json(outcome)?],
        ));
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn work_truth_cursor(&self) -> Result<i64> {
        self.connection
            .query_row("SELECT seq FROM work_truth_cursor WHERE id=1", [], |row| {
                row.get(0)
            })
            .map_err(sql_error)
    }

    pub fn advance_work_truth_cursor(&mut self, seq: i64) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE work_truth_cursor SET seq=MAX(seq, ?1) WHERE id=1",
                [seq],
            )
            .map(|_| ())
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn git_checkpoint_basis(&self, agent: &str, generation: i64) -> Result<GitCheckpointBasis> {
        self.ensure_committed_reads()?;
        let basis = self
            .connection
            .query_row(
                "SELECT CASE WHEN state IN ('snapshot','clean') THEN fingerprint END, oid,
                        captured_bytes, CASE WHEN state <> 'none' THEN updated_at END
                 FROM git_checkpoints WHERE agent = ?1 AND generation = ?2",
                params![parse_agent_id(agent)?, generation],
                |row| {
                    Ok(GitCheckpointBasis {
                        fingerprint: row.get(0)?,
                        snapshot: row.get(1)?,
                        captured_bytes: row.get::<_, i64>(2)?.max(0) as u64,
                        updated_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)?;
        Ok(basis.unwrap_or_default())
    }

    pub fn reserve_git_checkpoint(
        &mut self,
        intent: &GitCheckpointIntent,
        removal_command: Option<&str>,
    ) -> Result<GitCheckpointReservation> {
        use GitCheckpointReservation::{Deferred, Reserved, Skipped};
        self.ensure_writer_healthy()?;
        let result = (|| {
            let agent = parse_agent_id(&intent.agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let Some(row) = load_agent_by_num(&tx, agent)?.filter(|row| {
                row.generation == intent.generation
                    && row.workspace.as_deref() == Some(intent.workspace.as_str())
            }) else {
                return Ok(Skipped);
            };
            if let Some(outcome) = checkpoint_switch_gate(&tx, intent, agent, &row)? {
                return Ok(outcome);
            }
            if repo_git_write_blocked_tx(
                &tx,
                removal_command.unwrap_or_default(),
                &intent.common_dir,
            )? {
                return Ok(Deferred);
            }
            tx.execute(
                "INSERT INTO git_checkpoint_intents(common_dir, agent, generation, workspace, input)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![
                    intent.common_dir,
                    agent,
                    intent.generation,
                    intent.workspace,
                    encode_json(&intent.input)?,
                ],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Reserved)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn stage_git_checkpoint(
        &mut self,
        agent: &str,
        generation: i64,
        input: &GitCheckpointInput,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let changed = self
                .connection
                .execute(
                    "UPDATE git_checkpoint_intents SET input = ?1
                 WHERE agent = ?2 AND generation = ?3",
                    params![encode_json(input)?, parse_agent_id(agent)?, generation],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new("source_integrity", "checkpoint intent was lost"));
            }
            Ok(())
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn git_checkpoint_intents(&self) -> Result<Vec<GitCheckpointIntent>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT common_dir, agent, generation, workspace, input
             FROM git_checkpoint_intents ORDER BY common_dir",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get::<_, i64>(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (common_dir, agent, generation, workspace, input) = row.map_err(sql_error)?;
            Ok(GitCheckpointIntent {
                common_dir,
                agent: format_agent_id(agent),
                generation,
                workspace,
                input: serde_json::from_str(&input).map_err(|_| {
                    Error::new("source_integrity", "invalid stored checkpoint intent")
                })?,
            })
        })
        .collect()
    }

    pub fn has_git_checkpoint_intents(&self) -> Result<bool> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM git_checkpoint_intents)",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Old or terminal generations eligible for bounded private-ref cleanup.
    pub fn checkpoint_retention_generations(
        &self,
        after: Option<(i64, i64)>,
        limit: usize,
        terminal_cutoff: i64,
    ) -> Result<Vec<CheckpointRetentionGeneration>> {
        self.ensure_committed_reads()?;
        let (after_agent, after_generation) = after.unwrap_or((0, 0));
        let mut statement = self
            .connection
            .prepare(
                "SELECT checkpoint.agent, checkpoint.generation
                 FROM git_checkpoints checkpoint
                 LEFT JOIN agents agent ON agent.id = checkpoint.agent
                 WHERE checkpoint.session_end_pending = 0
                   AND NOT EXISTS (
                       SELECT 1 FROM git_checkpoint_intents intent
                       WHERE intent.agent = checkpoint.agent
                         AND intent.generation = checkpoint.generation
                   )
                   AND checkpoint.generation <= agent.generation
                   AND agent.state IN ('stopped','failed','lost')
                   AND agent.updated_at <= ?3
                   AND (checkpoint.agent > ?1
                        OR (checkpoint.agent = ?1 AND checkpoint.generation > ?2))
                 ORDER BY checkpoint.agent, checkpoint.generation
                 LIMIT ?4",
            )
            .map_err(sql_error)?;
        statement
            .query_map(
                params![
                    after_agent,
                    after_generation,
                    terminal_cutoff,
                    limit.clamp(1, 32) as i64
                ],
                |row| {
                    Ok(CheckpointRetentionGeneration {
                        agent_id: row.get(0)?,
                        generation: row.get(1)?,
                    })
                },
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// The exact generation and per-task snapshots retention must keep.
    pub fn checkpoint_restore_refs(&self, agent: i64, generation: i64) -> Result<HashSet<String>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT checkpoint_ref FROM git_checkpoints
             WHERE agent = ?1 AND generation = ?2 AND checkpoint_ref IS NOT NULL
             UNION SELECT checkpoint_ref FROM git_checkpoint_restore_points
             WHERE agent = ?1 AND generation = ?2",
            )
            .map_err(sql_error)?;
        statement
            .query_map(params![agent, generation], |row| row.get(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)
    }

    /// Whether this ended generation's whole process tree is proven gone.
    pub fn session_exit_proven(&self, agent: &str, generation: i64) -> Result<bool> {
        self.ensure_committed_reads()?;
        let Some(row) = load_agent_by_num(&self.connection, parse_agent_id(agent)?)? else {
            return Ok(false);
        };
        Ok(row.generation == generation
            && row.is_terminal()
            && handoff::session_exit_blocker(&self.connection, &row)?.is_none())
    }

    /// The final checkpoint runs once the process tree is proven gone.
    pub fn defer_session_checkpoint(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            self.connection
                .execute(
                    "INSERT INTO git_checkpoints(agent, generation, state, session_end_pending,
                         updated_at)
                     VALUES (?1, ?2, 'none', 1, ?3)
                     ON CONFLICT(agent, generation) DO UPDATE SET session_end_pending = 1",
                    params![parse_agent_id(agent)?, generation, now],
                )
                .map_err(sql_error)?;
            Ok(())
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn deferred_session_checkpoints(&self, limit: usize) -> Result<Vec<(String, i64)>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT agent, generation FROM git_checkpoints
                 INDEXED BY git_checkpoints_session_end
                 WHERE session_end_pending = 1 ORDER BY agent, generation LIMIT ?1",
            )
            .map_err(sql_error)?;
        statement
            .query_map([limit as i64], |row| {
                Ok((format_agent_id(row.get::<_, i64>(0)?), row.get(1)?))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    pub fn clear_session_checkpoint(&mut self, agent: &str, generation: i64) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            self.connection
                .execute(
                    "UPDATE git_checkpoints SET session_end_pending = 0
                     WHERE agent = ?1 AND generation = ?2",
                    params![parse_agent_id(agent)?, generation],
                )
                .map_err(sql_error)?;
            Ok(())
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn work_task_ref(&self, agent: &str, generation: i64) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        let value: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT task_ref FROM git_checkpoints WHERE agent = ?1 AND generation = ?2",
                params![parse_agent_id(agent)?, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(value.flatten())
    }

    pub fn work_task_command(&self, agent: &str, generation: i64) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        let value: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT task_command FROM git_checkpoints WHERE agent = ?1 AND generation = ?2",
                params![parse_agent_id(agent)?, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(value.flatten())
    }

    pub(super) fn work_task_binding(
        connection: &Connection,
        agent: &str,
        generation: i64,
        command: &str,
    ) -> Result<Option<(String, Option<String>)>> {
        connection
            .query_row(
                "SELECT task_ref, principal FROM git_work_tasks
                 WHERE agent = ?1 AND generation = ?2 AND task_command = ?3",
                params![parse_agent_id(agent)?, generation, command],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)
    }

    pub(super) fn selected_work_task_command(
        connection: &Connection,
        agent: &str,
        generation: i64,
    ) -> Result<Option<String>> {
        connection
            .query_row(
                "SELECT task_command FROM git_checkpoints
                 WHERE agent = ?1 AND generation = ?2",
                params![parse_agent_id(agent)?, generation],
                |row| row.get(0),
            )
            .optional()
            .map(|value: Option<Option<String>>| value.flatten())
            .map_err(sql_error)
    }

    /// Select an already-recorded original SEND, even after command retention.
    pub fn select_work_task(
        &mut self,
        agent: &str,
        generation: i64,
        command: &str,
        now: i64,
    ) -> Result<Option<String>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let Some((task_ref, _)) = Self::work_task_binding(&tx, agent, generation, command)?
            else {
                return Ok(None);
            };
            tx.execute(
                "INSERT INTO git_checkpoints(agent, generation, state, task_ref,
                         task_command, updated_at)
                 VALUES (?1, ?2, 'none', ?3, ?4, ?5)
                 ON CONFLICT(agent, generation) DO UPDATE SET
                     task_ref = excluded.task_ref, task_command = excluded.task_command",
                params![parse_agent_id(agent)?, generation, task_ref, command, now],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Some(task_ref))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Find an original SEND for an Agent launched without a prompt. A
    /// queued input is eligible for transfer as its own command.
    pub fn first_work_task_send(&self, agent: &str) -> Result<Option<Command>> {
        self.ensure_committed_reads()?;
        let id = self
            .connection
            .query_row(
                "SELECT id FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND verb = 'send'
                   AND state IN ('dispatched','applied','unknown')
                   AND (link_kind IS NOT 'handoff' OR ordinal > 0)
                   AND principal NOT LIKE 'swarm_note:%'
                   AND json_type(args, '$.text') = 'text'
                 ORDER BY created_at, rowid LIMIT 1",
                [parse_agent_id(agent)?],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        match id {
            Some(id) => load_command(&self.connection, &id),
            None => Ok(None),
        }
    }

    pub fn work_task_send(&self, agent: &str, id: &str) -> Result<Option<Command>> {
        self.ensure_committed_reads()?;
        let Some(command) = load_command(&self.connection, id)? else {
            return Ok(None);
        };
        if command.verb != "send"
            || command.target.as_deref() != Some(agent)
            || command.principal.starts_with("swarm_note:")
            || handoff::is_brief_tx(&self.connection, id)?
            || !matches!(
                command.state,
                CommandState::Dispatched | CommandState::Applied | CommandState::Unknown
            )
        {
            return Ok(None);
        }
        Ok(Some(command))
    }

    pub fn record_work_task(
        &mut self,
        agent: &str,
        generation: i64,
        task_ref: &str,
        command: &str,
        principal: &str,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let agent = parse_agent_id(agent)?;
            tx.execute(
                "INSERT INTO git_work_tasks(agent, generation, task_command, task_ref,
                         principal, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(agent, generation, task_command) DO UPDATE SET
                     task_ref = excluded.task_ref,
                     principal = COALESCE(excluded.principal, git_work_tasks.principal),
                     updated_at = excluded.updated_at",
                params![agent, generation, command, task_ref, principal, now],
            )
            .map_err(sql_error)?;
            tx.execute(
                "INSERT INTO git_checkpoints(agent, generation, state, task_ref,
                         task_command, updated_at)
                 VALUES (?1, ?2, 'none', ?3, ?4, ?5)
                 ON CONFLICT(agent, generation) DO UPDATE SET
                     task_ref = excluded.task_ref, task_command = excluded.task_command",
                params![agent, generation, task_ref, command, now],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(())
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// The managed branch and base of a worktree, shared by its successors.
    pub fn worktree_branch_base(&self, workspace: &str) -> Result<Option<(String, String)>> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT requested_branch, base_oid FROM workspaces WHERE worktree_path = ?1",
                [workspace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Whether the Owner wrote this task directly. Text produced by a rule
    /// or other automation can quote outside input, so it never names files
    /// for the daemon to capture.
    pub fn command_is_owner_authored(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let row: Option<(String, Option<String>, Option<String>)> = self
            .connection
            .query_row(
                "SELECT principal, link_kind, parent_command FROM commands WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((principal, link, parent)) = row else {
            return Ok(false);
        };
        if principal != "owner" || link.as_deref() == Some("rule_action") {
            return Ok(false);
        }
        for command in std::iter::once(id).chain(parent.as_deref()) {
            if command_cause_tx(&self.connection, command)?
                .is_some_and(|cause| cause.rule.is_some())
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Finished turns not yet written to the task journal.
    pub fn journal_turns(
        &self,
        agent: &str,
        generation: i64,
        limit: usize,
    ) -> Result<Vec<JournalTurn>> {
        self.ensure_committed_reads()?;
        let num = parse_agent_id(agent)?;
        let through: i64 = self
            .connection
            .query_row(
                "SELECT journaled_through FROM git_checkpoints
                 WHERE agent = ?1 AND generation = ?2",
                params![num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .unwrap_or(0);
        let mut statement = self
            .connection
            .prepare(
                "SELECT turn_id, COALESCE(outcome, state), COALESCE(completed_at, 0), final_text
                 FROM turns WHERE agent = ?1 AND generation = ?2 AND state <> 'running'
                   AND COALESCE(completed_at, 0) > ?3
                 ORDER BY completed_at, rowid LIMIT ?4",
            )
            .map_err(sql_error)?;
        statement
            .query_map(params![num, generation, through, limit as i64], |row| {
                Ok(JournalTurn {
                    turn_id: row.get(0)?,
                    outcome: row.get(1)?,
                    completed_at: row.get(2)?,
                    final_text: row.get(3)?,
                })
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// Commit the latest checkpoint fact and its event. An unchanged worktree
    /// keeps the previous reference and writes no Git object.
    pub fn record_git_checkpoint(
        &mut self,
        agent: &str,
        generation: i64,
        input: &GitCheckpointInput,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let num = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let removed = tx
                .execute(
                    "DELETE FROM git_checkpoint_intents WHERE agent = ?1 AND generation = ?2",
                    params![num, generation],
                )
                .map_err(sql_error)?;
            if removed != 1 {
                return Err(Error::new("source_integrity", "checkpoint intent was lost"));
            }
            if load_agent_by_num(&tx, num)?.is_none() {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Vec::new());
            }
            let journaled = input.journaled_through.unwrap_or(0);
            if input.state == GitCheckpointState::Unchanged {
                tx.execute(
                    "UPDATE git_checkpoints SET trigger = ?1, turn_id = ?2,
                         journaled_through = MAX(journaled_through, ?3), updated_at = ?4,
                         session_end_pending = CASE ?1 WHEN 'session_end' THEN 0
                             ELSE session_end_pending END
                     WHERE agent = ?5 AND generation = ?6",
                    params![
                        input.trigger,
                        input.turn_id,
                        journaled,
                        now,
                        num,
                        generation
                    ],
                )
                .map_err(sql_error)?;
            } else {
                tx.execute(
                    "INSERT INTO git_checkpoints(agent, generation, state, trigger, turn_id, head,
                         checkpoint_ref, oid, fingerprint, paths, paths_truncated, error,
                         journaled_through, updated_at, withheld, withheld_truncated,
                         captured_bytes)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)
                     ON CONFLICT(agent, generation) DO UPDATE SET state = excluded.state,
                         trigger = excluded.trigger, turn_id = excluded.turn_id,
                         head = CASE excluded.state WHEN 'failed' THEN head ELSE excluded.head END,
                         checkpoint_ref = CASE excluded.state WHEN 'snapshot' THEN excluded.checkpoint_ref ELSE checkpoint_ref END,
                         oid = CASE excluded.state WHEN 'snapshot' THEN excluded.oid ELSE oid END,
                         fingerprint = excluded.fingerprint,
                         paths = CASE excluded.state WHEN 'failed' THEN paths ELSE excluded.paths END, paths_truncated = CASE excluded.state WHEN 'failed' THEN paths_truncated ELSE excluded.paths_truncated END,
                         withheld = CASE WHEN excluded.state = 'failed'
                             AND excluded.withheld = '[]' THEN withheld ELSE excluded.withheld END,
                         withheld_truncated = CASE WHEN excluded.state = 'failed'
                             AND excluded.withheld = '[]' THEN withheld_truncated
                             ELSE excluded.withheld_truncated END,
                         captured_bytes = captured_bytes + excluded.captured_bytes,
                         session_end_pending = CASE excluded.trigger WHEN 'session_end' THEN 0
                             ELSE session_end_pending END,
                         error = excluded.error,
                         journaled_through = MAX(journaled_through, excluded.journaled_through),
                         updated_at = excluded.updated_at",
                    params![
                        num,
                        generation,
                        state_name(input.state),
                        input.trigger,
                        input.turn_id,
                        input.head,
                        input.checkpoint_ref,
                        input.oid,
                        input.fingerprint,
                        encode_json(&input.paths)?,
                        input.paths_truncated,
                        input.error,
                        journaled,
                        now,
                        encode_json(&input.withheld)?,
                        input.withheld_truncated,
                        i64::try_from(input.captured_bytes).unwrap_or(i64::MAX),
                    ],
                )
                .map_err(sql_error)?;
            }
            if let (GitCheckpointState::Snapshot, Some(reference), Some(oid)) =
                (input.state, &input.checkpoint_ref, &input.oid)
            {
                tx.execute(
                    "INSERT INTO git_checkpoint_restore_points(agent, generation, task_ref, checkpoint_ref, oid)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(agent, generation, task_ref) DO UPDATE SET
                         checkpoint_ref = excluded.checkpoint_ref, oid = excluded.oid",
                    params![num, generation, input.task_ref.as_deref().unwrap_or_default(), reference, oid],
                ).map_err(sql_error)?;
            }
            let (reference, oid): (Option<String>, Option<String>) = tx.query_row(
                "SELECT checkpoint_ref, oid FROM git_checkpoints WHERE agent = ?1 AND generation = ?2",
                params![num, generation], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional().map_err(sql_error)?.unwrap_or_default();
            let event = insert_daemon_event(
                &tx,
                now,
                GIT_CHECKPOINT,
                "git_checkpoint",
                Some(num),
                Some(generation),
                json!({
                    "state": state_name(input.state),
                    "trigger": input.trigger,
                    "turn_id": input.turn_id,
                    "head": input.head,
                    "ref": reference,
                    "oid": oid,
                    "files": input.paths.len(),
                    "paths_truncated": input.paths_truncated,
                    "withheld": input.withheld.len(),
                    "captured_bytes": input.captured_bytes,
                    "error": input.error,
                }),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(vec![event])
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn agent_work_truth(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<AgentWorkTruthView>> {
        self.ensure_committed_reads()?;
        type Row = (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            bool,
            Option<String>,
            Option<String>,
            i64,
            String,
            bool,
            bool,
        );
        let row: Option<Row> = self
            .connection
            .query_row(
                "SELECT state, trigger, turn_id, head, checkpoint_ref, oid, paths,
                        paths_truncated, error, task_ref, updated_at, withheld,
                        withheld_truncated, session_end_pending
                 FROM git_checkpoints WHERE agent = ?1 AND generation = ?2",
                params![parse_agent_id(agent)?, generation],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let (checkpoint, task_ref, awaiting_exit) = match row {
            None => (None, None, false),
            Some((
                state,
                trigger,
                turn,
                head,
                reference,
                oid,
                paths,
                truncated,
                error,
                task,
                at,
                withheld,
                withheld_truncated,
                awaiting_exit,
            )) => {
                let state = match state.as_str() {
                    "snapshot" => Some(GitCheckpointState::Snapshot),
                    "clean" => Some(GitCheckpointState::Clean),
                    "failed" => Some(GitCheckpointState::Failed),
                    _ => None,
                };
                let view = match state {
                    Some(state) => Some(GitCheckpointView {
                        state,
                        trigger: trigger.unwrap_or_default(),
                        turn_id: turn,
                        head,
                        checkpoint_ref: reference,
                        oid,
                        paths: serde_json::from_str(&paths).map_err(|_| {
                            Error::new("source_integrity", "checkpoint paths are invalid")
                        })?,
                        paths_truncated: truncated,
                        withheld: serde_json::from_str(&withheld).map_err(|_| {
                            Error::new("source_integrity", "checkpoint withheld list is invalid")
                        })?,
                        withheld_truncated,
                        error,
                        at,
                    }),
                    None => None,
                };
                (view, task, awaiting_exit)
            }
        };
        let handoff = self
            .latest_handoff_event(agent, generation)?
            .map(|event| handoff_view(&event));
        if checkpoint.is_none() && task_ref.is_none() && handoff.is_none() && !awaiting_exit {
            return Ok(None);
        }
        Ok(Some(AgentWorkTruthView {
            task_ref,
            checkpoint,
            awaiting_exit,
            handoff,
        }))
    }
}

fn handoff_view(event: &Event) -> AgentHandoffView {
    let text = |key: &str| event.data[key].as_str().map(str::to_owned);
    let state = match event.kind.as_str() {
        super::HANDOFF_QUEUED => "queued".to_owned(),
        super::HANDOFF_WAITING => "waiting".to_owned(),
        _ => text("state").unwrap_or_else(|| "unknown".to_owned()),
    };
    AgentHandoffView {
        state,
        reason: serde_json::from_value::<HandoffReason>(event.data["reason"].clone()).ok(),
        kind: serde_json::from_value(event.data["kind"].clone()).ok(),
        continued_to: text("continued_to"),
        profile: text("profile"),
        uncertain: event.data["uncertain"].as_bool().unwrap_or(false),
        message: text("message").or_else(|| text("error")),
        at: event.ts,
    }
}
