//! Where an Agent's work is recorded in Git. The daemon writes the Git objects;
//! Store keeps the latest fact per generation and the event history.
use super::*;
use eliot_core::api::{
    AgentHandoffView, AgentWorkTruthView, CheckpointRestoreRequest, CheckpointRestoreResponse,
    GitCheckpointState, GitCheckpointView, GitCheckpointWithheld, HandoffReason,
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
    now: i64,
) -> Result<()> {
    inherit_work_task_generation_tx(
        tx,
        (parse_agent_id(&from.id)?, from.generation),
        (parse_agent_id(&to.id)?, to.generation),
        now,
    )
}

pub(super) fn inherit_work_task_generation_tx(
    tx: &Transaction<'_>,
    from: (i64, i64),
    to: (i64, i64),
    now: i64,
) -> Result<()> {
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
    pub fn reserve_checkpoint_restore(
        &mut self,
        request: &CheckpointRestoreRequest,
        workspace: &str,
        now: i64,
    ) -> Result<CheckpointRestoreAdmission> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let args = serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode checkpoint restore"))?;
            let digest = canonical::digest(&args)?;
            if let Some((principal, method, saved_digest, saved)) =
                write_binding(&self.connection, &request.id)?
            {
                if principal != "owner" || method != CHECKPOINT_RESTORE || saved_digest != digest {
                    return Err(Error::new(
                        "id_conflict",
                        "checkpoint restore id is already bound",
                    ));
                }
                let outcome: Option<Result<CheckpointRestoreResponse>> =
                    serde_json::from_str(&saved).map_err(|_| {
                        Error::new("source_integrity", "checkpoint restore receipt is invalid")
                    })?;
                return Ok(CheckpointRestoreAdmission::Replay(outcome.unwrap_or_else(
                    || {
                        Err(Error::new(
                            "unknown",
                            "checkpoint restore effect has no retained outcome",
                        ))
                    },
                )));
            }
            if command_binding(&self.connection, &request.id)?.is_some() {
                return Err(Error::new(
                    "id_conflict",
                    "restore id is already used by a command",
                ));
            }
            validate_id_age(&request.id, now)?;
            let oid = self
                .agent_work_truth(&request.agent, request.generation)?
                .and_then(|truth| truth.checkpoint)
                .filter(|checkpoint| {
                    checkpoint.state == GitCheckpointState::Snapshot
                        && checkpoint.checkpoint_ref.as_deref() == Some(&request.checkpoint_ref)
                })
                .and_then(|checkpoint| checkpoint.oid)
                .ok_or_else(|| Error::new("not_found", "checkpoint reference is not current"))?;
            if self.workspace_has_active_agents(workspace)? {
                return Err(Error::new("busy", "restore workspace has an active Agent"));
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            coordination::retain_write_tx(
                &tx,
                &request.id,
                "owner",
                CHECKPOINT_RESTORE,
                &digest,
                &Value::Null,
                now,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(CheckpointRestoreAdmission::Start { oid })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn settle_checkpoint_restore(
        &mut self,
        id: &str,
        outcome: &Result<CheckpointRestoreResponse>,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let changed = self
                .connection
                .execute(
                    "UPDATE writes SET result = ?1
                 WHERE id = ?2 AND method = ?3 AND principal = 'owner' AND result = 'null'",
                    params![encode_json(outcome)?, id, CHECKPOINT_RESTORE],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "checkpoint restore intent was lost",
                ));
            }
            Ok(())
        })();
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
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            self.connection
                .execute(
                    "INSERT INTO git_checkpoints(agent, generation, state, task_ref,
                             task_command, updated_at)
                     VALUES (?1, ?2, 'none', ?3, ?4, ?5)
                     ON CONFLICT(agent, generation) DO UPDATE SET task_ref = excluded.task_ref,
                         task_command = excluded.task_command",
                    params![parse_agent_id(agent)?, generation, task_ref, command, now],
                )
                .map_err(sql_error)?;
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
                         head = CASE excluded.state WHEN 'failed' THEN head ELSE excluded.head END, checkpoint_ref = CASE excluded.state WHEN 'failed' THEN checkpoint_ref ELSE excluded.checkpoint_ref END,
                         oid = CASE excluded.state WHEN 'failed' THEN oid ELSE excluded.oid END, fingerprint = excluded.fingerprint,
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
                    "ref": input.checkpoint_ref,
                    "oid": input.oid,
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
