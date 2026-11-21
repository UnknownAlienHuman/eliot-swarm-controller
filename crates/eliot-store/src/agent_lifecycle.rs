//! Explicit owner tree operations, sharing retention's command deletion.
use super::*;
use eliot_core::api::AgentTreeResponse;

fn delete_sql_error(error: rusqlite::Error) -> Error {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
        return Error::new(
            "source_integrity",
            "Agent deletion violates a Store constraint",
        );
    }
    sql_error(error)
}

macro_rules! delete_db {
    ($expression:expr) => {
        $expression.map_err(delete_sql_error)?
    };
}

const AGENT_DELETE_RECEIPT_KEY: &str = "__eliot_agent_delete";

enum AgentDeleteReceipt {
    Pending,
    Completed(Value),
    Refused(Error),
}

fn agent_delete_payload(agent: &str) -> Value {
    json!({"agent":agent,"archived":null})
}

fn agent_delete_pending_value() -> Value {
    json!({"__eliot_agent_delete":{"state":"pending"}})
}

fn agent_delete_error_value(error: &Error) -> Value {
    json!({
        "__eliot_agent_delete": {
            "state": "refused",
            "error": {
                "code": &error.code,
                "message": error.message.as_str()
            }
        }
    })
}

fn parse_agent_delete_receipt(value: Value) -> Result<AgentDeleteReceipt> {
    let Some(receipt) = value.get(AGENT_DELETE_RECEIPT_KEY) else {
        return Ok(AgentDeleteReceipt::Completed(value));
    };
    match receipt.get("state").and_then(Value::as_str) {
        Some("pending") => Ok(AgentDeleteReceipt::Pending),
        Some("refused") => {
            let error = receipt.get("error").ok_or_else(|| {
                Error::new("source_integrity", "retained Agent delete error is absent")
            })?;
            let code = error.get("code").and_then(Value::as_str).ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "retained Agent delete error code is invalid",
                )
            })?;
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "retained Agent delete error message is invalid",
                    )
                })?;
            Ok(AgentDeleteReceipt::Refused(Error::new(code, message)))
        }
        _ => Err(Error::new(
            "source_integrity",
            "retained Agent delete receipt is invalid",
        )),
    }
}

fn agent_delete_replay_result(value: Value) -> Result<Value> {
    match parse_agent_delete_receipt(value)? {
        AgentDeleteReceipt::Pending => Err(Error::new(
            "unknown",
            "Agent deletion has an unsettled external cleanup receipt",
        )),
        AgentDeleteReceipt::Completed(value) => Ok(value),
        AgentDeleteReceipt::Refused(error) => Err(error),
    }
}

fn agent_delete_receipt_tx(
    tx: &Transaction<'_>,
    id: &str,
    digest: &[u8],
) -> Result<Option<AgentDeleteReceipt>> {
    if let Some((principal, method, old_digest, result)) = write_binding(tx, id)? {
        if principal != "owner" || method != "agent.delete" || old_digest.as_slice() != digest {
            return Err(Error::new(
                "id_conflict",
                "cleanup write id has another binding",
            ));
        }
        return parse_agent_delete_receipt(decode_json(&result)?).map(Some);
    }
    if command_binding(tx, id)?.is_some() || digest_id_bound(tx, id)? {
        return Err(Error::new("id_conflict", "cleanup id is already in use"));
    }
    Ok(None)
}

fn current_agent_tree_tx(
    tx: &Transaction<'_>,
    root: i64,
    archived: Option<bool>,
    before_cleanup: bool,
) -> Result<(Vec<i64>, Vec<Agent>)> {
    if load_agent_by_num(tx, root)?.is_none() {
        return Err(Error::new("not_found", "Agent was not found"));
    }
    let members = hierarchy::subtree_member_nums(tx, root, MAX_PAGE_ENTRIES as i64)?;
    if members.len() > MAX_PAGE_ENTRIES {
        return Err(Error::new(
            "unavailable",
            "Agent tree exceeds operation bound",
        ));
    }
    let method = if archived.is_some() {
        "agent.archive"
    } else {
        "agent.delete"
    };
    let mut agents = Vec::with_capacity(members.len());
    for num in &members {
        let row = load_agent_by_num(tx, *num)?
            .ok_or_else(|| Error::new("source_integrity", "Agent tree changed"))?;
        // Archive hides a tree that is not working: idle, ended
        // (stopped, failed or lost), or detached external observation.
        // Delete needs a settled end or a detached observer. A detached
        // observer is an observe-only Unknown row with no live reader:
        // the custody check below still refuses an attached observer
        // with an open source, and the turn/command checks still apply.
        // Native observed children keep their own rule: live ones are
        // Unknown and stay blocked here; ended ones are stopped.
        let detached_observer = row.observe_only
            && row.origin == core::AgentOrigin::External
            && row.state == AgentState::Unknown;
        let allowed = if archived.is_some() {
            matches!(
                row.state,
                AgentState::Idle | AgentState::Stopped | AgentState::Failed | AgentState::Lost
            ) || detached_observer
        } else {
            matches!(row.state, AgentState::Stopped | AgentState::Failed) || detached_observer
        };
        if !allowed {
            return Err(Error::new(
                "busy",
                format!(
                    "{} is {:?}; {} requires {}",
                    row.id,
                    row.state,
                    method,
                    if archived.is_some() {
                        "idle or ended (stopped, failed or lost)"
                    } else {
                        "stopped or failed"
                    },
                ),
            ));
        }
        if archived.is_none() {
            project_transfer::require_deletable(tx, &row)?;
            workspace::require_workspace_owner_deletable(tx, &row)?;
        }
        if let Some(turn) = row.active_turn.as_deref() {
            return Err(Error::new(
                "busy",
                format!(
                    "{} generation {} has active turn {turn}",
                    row.id, row.generation
                ),
            ));
        }
        if !row.pending.is_empty() {
            return Err(Error::new(
                "busy",
                format!(
                    "{} generation {} has {} pending requests",
                    row.id,
                    row.generation,
                    row.pending.len()
                ),
            ));
        }
        let keep_drained_unknowns = archived.is_some()
            && !row.observe_only
            && row.state.is_terminal()
            && unknown_generations_drained(tx, *num)?;
        let command: Option<(String, String, Option<i64>)> = tx
            .query_row(
                "SELECT id,state,generation FROM commands WHERE target=?1
               AND (state IN ('accepted','queued','dispatched') OR (state='unknown' AND NOT ?2))
             ORDER BY created_at,id LIMIT 1",
                params![num, keep_drained_unknowns],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some((command, state, generation)) = command {
            let generation =
                generation.map_or_else(|| "unassigned".to_owned(), |value| value.to_string());
            return Err(Error::new(
                "busy",
                format!(
                    "{} has {state} command {command} (generation {generation})",
                    row.id
                ),
            ));
        }
        let turn: Option<(String, String, i64)> = tx
            .query_row(
                "SELECT turn_id,state,generation FROM turns WHERE agent=?1
               AND (state='running' OR (state='unknown' AND NOT ?2))
             ORDER BY generation,turn_id LIMIT 1",
                params![num, keep_drained_unknowns],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some((turn, state, generation)) = turn {
            // Ended native observation keeps its unknown turn as retained
            // evidence, not a live gate: the child reaches stopped only
            // through the ended-with-parent sweep, whose producer check
            // proves the granting generation ended, so no live source
            // remains to complete it. The other guards (no active turn,
            // no pending or live commands, drained custody) still apply,
            // and running turns still block everywhere.
            let ended_observation = state == "unknown"
                && row.observe_only
                && row.origin == core::AgentOrigin::Native
                && row.state == AgentState::Stopped;
            if !ended_observation {
                return Err(Error::new(
                    "busy",
                    format!(
                        "{} generation {generation} has {state} turn {turn}; stopped lifecycle does not resolve an uncertain turn",
                        row.id
                    ),
                ));
            }
        }
        // Delete needs drained custody. Archive of an ended member needs
        // resolved custody, so it never hides a process, artifact or
        // native source the owner must still resolve; Eliot's own os, git
        // and workspace-process observers of a resumable Agent may stay
        // open. An idle member keeps its live custody (archive never
        // stops a generation); unarchive only reveals.
        let guard = match archived {
            None => Some(true),
            Some(true) if row.state != AgentState::Idle => Some(false),
            Some(_) => None,
        };
        let workspace_cleanup_generation = if before_cleanup {
            workspace::load_workspace(tx, *num)?
                .filter(|workspace| {
                    workspace.target_generation == row.generation
                        && matches!(
                            workspace.phase,
                            core::WorkspacePhase::Ready | core::WorkspacePhase::Failed
                        )
                })
                .map(|workspace| workspace.target_generation)
        } else {
            None
        };
        if let Some(observers) = guard
            && let Some(blocker) =
                custody_blocker_tx(tx, *num, observers, workspace_cleanup_generation)?
        {
            return Err(Error::new(
                "busy",
                format!(
                    "{} still has {blocker}; {method} requires {} custody",
                    row.id,
                    if observers { "drained" } else { "resolved" }
                ),
            ));
        }
        agents.push(row);
    }
    Ok((members, agents))
}

/// Archive may preserve unknown history of an ended Agent whose exact
/// generations have committed process exit and reader drain. Deletion still
/// uses its own stricter guards.
fn unknown_generations_drained(tx: &Transaction<'_>, agent: i64) -> Result<bool> {
    let mut statement = tx
        .prepare(
            "SELECT generation FROM commands WHERE target=?1 AND state='unknown'
             UNION SELECT generation FROM turns WHERE agent=?1 AND state='unknown'
             LIMIT ?2",
        )
        .map_err(sql_error)?;
    let generations = statement
        .query_map(params![agent, MAX_PAGE_ENTRIES as i64 + 1], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(sql_error)?;
    for (index, generation) in generations.enumerate() {
        let Some(generation) = generation.map_err(sql_error)? else {
            return Ok(false);
        };
        if index == MAX_PAGE_ENTRIES
            || !super::capacity::has_owned_exit_and_reader_drain_tx(tx, agent, generation)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn verify_agent_delete_snapshot(expected: &[Agent], current: &[Agent]) -> Result<()> {
    let mut expected_ids = expected
        .iter()
        .map(|agent| Ok((parse_agent_id(&agent.id)?, agent.generation)))
        .collect::<Result<Vec<_>>>()?;
    let mut current_ids = current
        .iter()
        .map(|agent| Ok((parse_agent_id(&agent.id)?, agent.generation)))
        .collect::<Result<Vec<_>>>()?;
    expected_ids.sort_unstable();
    current_ids.sort_unstable();
    if expected_ids.windows(2).any(|pair| pair[0].0 == pair[1].0) || expected_ids != current_ids {
        return Err(Error::new(
            "stale_generation",
            "Agent tree membership or generation changed since delete inspection",
        ));
    }
    Ok(())
}

impl Store {
    /// Read and preflight a tree before the manager plans external cleanup.
    /// An exact completed write replays; a reserved write stays unknown until
    /// the manager settles it.
    pub fn inspect_agent_delete(
        &mut self,
        id: &str,
        agent: &str,
        now: i64,
    ) -> Result<(Option<Value>, Vec<Agent>)> {
        self.ensure_committed_reads()?;
        let payload = agent_delete_payload(agent);
        if let Some(result) = self.cleanup_replay("owner", id, "agent.delete", &payload)? {
            return Ok((Some(agent_delete_replay_result(result)?), Vec::new()));
        }
        self.ensure_writer_healthy()?;

        let digest = canonical::digest(&payload)?;
        let outcome = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(receipt) = agent_delete_receipt_tx(&tx, id, &digest)? {
                return match receipt {
                    AgentDeleteReceipt::Pending => Err(Error::new(
                        "unknown",
                        "Agent deletion has an unsettled external cleanup receipt",
                    )),
                    AgentDeleteReceipt::Completed(result) => Ok((Some(result), Vec::new())),
                    AgentDeleteReceipt::Refused(error) => Err(error),
                };
            }
            validate_id_age(id, now)?;
            let root = parse_agent_id(agent)?;
            let (members, current) = current_agent_tree_tx(&tx, root, None, true)?;
            prepare_delete_tree_tx(&tx, &members)?;
            tx.rollback().map_err(sql_error)?;
            Ok((None, current))
        })();
        if outcome.is_err() && !self.connection.is_autocommit() {
            self.write_faulted.set(true);
            return Err(Error::new(
                "unknown",
                "Agent delete inspection rollback could not be verified",
            ));
        }
        outcome
    }

    /// Recheck the inspected tree and reserve its agent.delete write before
    /// the manager dispatches native or worktree cleanup.
    pub fn reserve_agent_delete(
        &mut self,
        id: &str,
        agent: &str,
        expected: &[Agent],
        now: i64,
    ) -> Result<()> {
        self.ensure_committed_reads()?;
        let payload = agent_delete_payload(agent);
        if let Some(result) = self.cleanup_replay("owner", id, "agent.delete", &payload)? {
            return match parse_agent_delete_receipt(result)? {
                AgentDeleteReceipt::Pending => Err(Error::new(
                    "unknown",
                    "Agent deletion already has an unsettled external cleanup receipt",
                )),
                AgentDeleteReceipt::Completed(_) => Err(Error::new(
                    "bad_state",
                    "Agent deletion is already complete; inspect its retained result",
                )),
                AgentDeleteReceipt::Refused(error) => Err(error),
            };
        }
        self.ensure_writer_healthy()?;
        let outcome = self.reserve_agent_delete_inner(id, agent, expected, now, &payload);
        if outcome.is_err() && !self.connection.is_autocommit() {
            self.write_faulted.set(true);
            return Err(Error::new(
                "unknown",
                "Agent delete reservation rollback could not be verified",
            ));
        }
        outcome
    }

    fn reserve_agent_delete_inner(
        &mut self,
        id: &str,
        agent: &str,
        expected: &[Agent],
        now: i64,
        payload: &Value,
    ) -> Result<()> {
        let digest = canonical::digest(payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(receipt) = agent_delete_receipt_tx(&tx, id, &digest)? {
            return match receipt {
                AgentDeleteReceipt::Pending => Err(Error::new(
                    "unknown",
                    "Agent deletion already has an unsettled external cleanup receipt",
                )),
                AgentDeleteReceipt::Completed(_) => Err(Error::new(
                    "bad_state",
                    "Agent deletion is already complete; inspect its retained result",
                )),
                AgentDeleteReceipt::Refused(error) => Err(error),
            };
        }
        validate_id_age(id, now)?;
        let root = parse_agent_id(agent)?;
        let (members, current) = current_agent_tree_tx(&tx, root, None, true)?;
        verify_agent_delete_snapshot(expected, &current)?;
        prepare_delete_tree_tx(&tx, &members)?;
        tx.execute_batch("DELETE FROM deleted_agents; DELETE FROM deleted_commands;")
            .map_err(sql_error)?;
        tx.execute(
            "INSERT INTO writes(id,principal,method,payload_digest,result,created_at)
             VALUES (?1,'owner','agent.delete',?2,?3,?4)",
            params![id, digest, encode_json(&agent_delete_pending_value())?, now],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)
    }

    /// Finish a reserved delete after external cleanup. The retained write,
    /// generation snapshot, lifecycle guards and tree command preflight are
    /// all rechecked in the deletion transaction.
    pub fn finish_agent_delete(
        &mut self,
        id: &str,
        agent: &str,
        expected: &[Agent],
        now: i64,
    ) -> Result<Value> {
        self.ensure_committed_reads()?;
        let payload = agent_delete_payload(agent);
        if let Some(result) = self.cleanup_replay("owner", id, "agent.delete", &payload)? {
            match parse_agent_delete_receipt(result)? {
                AgentDeleteReceipt::Pending => {}
                AgentDeleteReceipt::Completed(result) => return Ok(result),
                AgentDeleteReceipt::Refused(error) => return Err(error),
            }
        }
        self.ensure_writer_healthy()?;
        let outcome = self.finish_agent_delete_inner(id, agent, expected, now, &payload);
        if outcome.is_err() && !self.connection.is_autocommit() {
            self.write_faulted.set(true);
            return Err(Error::new(
                "unknown",
                "Agent deletion rollback could not be verified",
            ));
        }
        outcome
    }

    fn finish_agent_delete_inner(
        &mut self,
        id: &str,
        agent: &str,
        expected: &[Agent],
        now: i64,
        payload: &Value,
    ) -> Result<Value> {
        let digest = canonical::digest(payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        match agent_delete_receipt_tx(&tx, id, &digest)? {
            Some(AgentDeleteReceipt::Pending) => {}
            Some(AgentDeleteReceipt::Completed(result)) => return Ok(result),
            Some(AgentDeleteReceipt::Refused(error)) => return Err(error),
            None => {
                return Err(Error::new(
                    "bad_state",
                    "Agent deletion has no cleanup reservation",
                ));
            }
        }
        let root = parse_agent_id(agent)?;
        let (members, current) = current_agent_tree_tx(&tx, root, None, false)?;
        verify_agent_delete_snapshot(expected, &current)?;
        delete_tree_tx(&tx, &members, now)?;
        let result = serde_json::to_value(AgentTreeResponse {
            agent: agent.to_owned(),
            affected: members.len(),
            archived: None,
            deleted: true,
        })
        .map_err(|_| Error::new("internal", "Agent tree result encoding failed"))?;
        let changed = tx
            .execute(
                "UPDATE writes SET result=?1
                 WHERE id=?2 AND principal='owner' AND method='agent.delete'
                   AND payload_digest=?3 AND result=?4",
                params![
                    encode_json(&result)?,
                    id,
                    digest,
                    encode_json(&agent_delete_pending_value())?
                ],
            )
            .map_err(delete_sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "Agent delete reservation changed before settlement",
            ));
        }
        if let Err(error) = commit_sql(&tx) {
            if tx.is_autocommit()
                && error.sqlite_error_code() != Some(rusqlite::ErrorCode::ConstraintViolation)
            {
                self.write_faulted.set(true);
                return Err(Error::new(
                    "unknown",
                    "Agent deletion commit outcome is unknown",
                ));
            }
            return Err(delete_sql_error(error));
        }
        Ok(result)
    }

    /// Retain a definite cleanup refusal while leaving the Agent tree intact.
    /// An unknown outcome deliberately leaves the reservation unsettled.
    pub fn fail_agent_delete(&mut self, id: &str, agent: &str, error: &Error) -> Result<()> {
        self.ensure_committed_reads()?;
        let payload = agent_delete_payload(agent);
        let Some(result) = self.cleanup_replay("owner", id, "agent.delete", &payload)? else {
            return Err(Error::new(
                "bad_state",
                "Agent deletion has no cleanup reservation to fail",
            ));
        };
        match parse_agent_delete_receipt(result)? {
            AgentDeleteReceipt::Pending if error.code == "unknown" => return Ok(()),
            AgentDeleteReceipt::Pending => {}
            AgentDeleteReceipt::Completed(_) | AgentDeleteReceipt::Refused(_) => return Ok(()),
        }
        self.ensure_writer_healthy()?;
        let outcome = self.fail_agent_delete_inner(id, agent, error, &payload);
        if outcome.is_err() && !self.connection.is_autocommit() {
            self.write_faulted.set(true);
            return Err(Error::new(
                "unknown",
                "Agent delete refusal rollback could not be verified",
            ));
        }
        outcome
    }

    fn fail_agent_delete_inner(
        &mut self,
        id: &str,
        agent: &str,
        error: &Error,
        payload: &Value,
    ) -> Result<()> {
        let digest = canonical::digest(payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        match agent_delete_receipt_tx(&tx, id, &digest)? {
            Some(AgentDeleteReceipt::Pending) => {}
            Some(AgentDeleteReceipt::Completed(_) | AgentDeleteReceipt::Refused(_)) => {
                return Ok(());
            }
            None => {
                return Err(Error::new(
                    "bad_state",
                    "Agent deletion has no cleanup reservation to fail",
                ));
            }
        }
        let root = parse_agent_id(agent)?;
        if load_agent_by_num(&tx, root)?.is_none() {
            return Err(Error::new(
                "unknown",
                "Agent deletion is reserved but its Agent row is absent",
            ));
        }
        let changed = tx
            .execute(
                "UPDATE writes SET result=?1
                 WHERE id=?2 AND principal='owner' AND method='agent.delete'
                   AND payload_digest=?3 AND result=?4",
                params![
                    encode_json(&agent_delete_error_value(error))?,
                    id,
                    digest,
                    encode_json(&agent_delete_pending_value())?
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "Agent delete reservation changed before refusal settlement",
            ));
        }
        commit_transaction(tx, &self.write_faulted)
    }

    /// First retained unknown of each kind across all generations. Lifecycle
    /// deletion checks historical work too; diagnosis must use the same scope,
    /// independent of its bounded recent-turn page.
    pub fn agent_unknowns(&self, agent: &str) -> Result<(Option<Command>, Option<Turn>)> {
        let num = parse_agent_id(agent)?;
        let command: Option<String> = self.connection.query_row(
            "SELECT id FROM commands WHERE target=?1 AND state='unknown' ORDER BY created_at,id LIMIT 1",
            [num], |row| row.get(0),
        ).optional().map_err(sql_error)?;
        let turn: Option<(i64, String)> = self.connection.query_row(
            "SELECT generation,turn_id FROM turns WHERE agent=?1 AND state='unknown' ORDER BY generation,turn_id LIMIT 1",
            [num], |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(sql_error)?;
        Ok((
            command
                .map(|id| self.get_command(&id))
                .transpose()?
                .flatten(),
            turn.map(|(generation, id)| self.get_turn(agent, generation, &id))
                .transpose()?
                .flatten(),
        ))
    }

    pub fn change_agent_tree(
        &mut self,
        id: &str,
        agent: &str,
        archived: Option<bool>,
        now: i64,
    ) -> Result<Value> {
        let method = if archived.is_some() {
            "agent.archive"
        } else {
            "agent.delete"
        };
        self.ensure_committed_reads()?;
        let payload = json!({"agent":agent,"archived":archived});
        if let Some(result) = self.cleanup_replay("owner", id, method, &payload)? {
            return if archived.is_none() {
                agent_delete_replay_result(result)
            } else {
                Ok(result)
            };
        }
        self.ensure_writer_healthy()?;
        let result = self.change_agent_tree_inner(id, agent, archived, now);
        if archived.is_some() {
            self.fault_on_persistence_error(&result);
        } else if result.is_err() && !self.connection.is_autocommit() {
            // A failed automatic rollback leaves the writer connection unsafe to reuse.
            self.write_faulted.set(true);
            return Err(Error::new(
                "unknown",
                "Agent deletion rollback could not be verified",
            ));
        }
        result
    }
    fn change_agent_tree_inner(
        &mut self,
        id: &str,
        agent: &str,
        archived: Option<bool>,
        now: i64,
    ) -> Result<Value> {
        validate_ulid(id)?;
        let root = parse_agent_id(agent)?;
        let method = if archived.is_some() {
            "agent.archive"
        } else {
            "agent.delete"
        };
        let payload = json!({"agent":agent,"archived":archived});
        let digest = canonical::digest(&payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, id)?.is_some() {
            return Err(Error::new("id_conflict", "id belongs to a command"));
        }
        crate::ensure_no_digest_id(&tx, id, "id is already used by a completion digest")?;
        if let Some((principal, old_method, old_digest, result)) = write_binding(&tx, id)? {
            if principal != "owner" || old_method != method || old_digest != digest {
                return Err(Error::new("id_conflict", "id belongs to another mutation"));
            }
            let result = decode_json(&result)?;
            return if archived.is_none() {
                agent_delete_replay_result(result)
            } else {
                Ok(result)
            };
        }
        validate_id_age(id, now)?;
        let (members, _) = current_agent_tree_tx(&tx, root, archived, false)?;
        if let Some(value) = archived {
            for num in &members {
                db!(tx.execute(
                    "UPDATE agents SET archived=?1,updated_at=?2 WHERE id=?3",
                    params![value, now, num]
                ));
            }
        } else {
            delete_tree_tx(&tx, &members, now)?;
        }
        let result = serde_json::to_value(AgentTreeResponse {
            agent: agent.to_owned(),
            affected: members.len(),
            archived,
            deleted: archived.is_none(),
        })
        .map_err(|_| Error::new("internal", "Agent tree result encoding failed"))?;
        let receipt = tx.execute(
            "INSERT INTO writes(id,principal,method,payload_digest,result,created_at)
             VALUES (?1,'owner',?2,?3,?4,?5)",
            params![id, method, digest, encode_json(&result)?, now],
        );
        if archived.is_some() {
            db!(receipt);
            commit_transaction(tx, &self.write_faulted)?;
        } else {
            delete_db!(receipt);
            if let Err(error) = commit_sql(&tx) {
                // An open transaction can still roll back. A constraint refusal
                // also proves SQLite did not commit, including a commit-hook veto.
                if tx.is_autocommit()
                    && error.sqlite_error_code() != Some(rusqlite::ErrorCode::ConstraintViolation)
                {
                    self.write_faulted.set(true);
                    return Err(Error::new(
                        "unknown",
                        "Agent deletion commit outcome is unknown",
                    ));
                }
                return Err(delete_sql_error(error));
            }
        }
        Ok(result)
    }
}

/// The ended states in which a custody-held archived Agent stays visible
/// under the default archived exclusion. `agent.list` rows and counts and
/// the organization projection's members and totals all apply this list
/// with [`default_list_custody_agents`], so a row lists exactly where the
/// projection counts it.
pub(crate) const CUSTODY_VISIBLE_STATES: &str = "'stopped','failed','lost'";

/// Distinct Agent numbers holding unresolved custody for the default
/// `agent.list` exclusion (the same blockers as [`custody_blocker_tx`]
/// with observers disabled): an unexited owned process, an `unknown`
/// artifact, or an open non-daemon, non-observer source. Each leg seeks its
/// own table instead of scanning archived history per row, so the archived
/// branch and its counts stay bounded by live custody rather than by the
/// retained fleet size.
pub(crate) fn default_list_custody_agents(connection: &Connection) -> Result<Vec<i64>> {
    use std::collections::BTreeSet;
    let mut agents = BTreeSet::new();
    let mut statement = connection
        .prepare("SELECT DISTINCT owner FROM processes WHERE state IN ('running','unknown')")
        .map_err(sql_error)?;
    let owners = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for owner in owners {
        let owner = owner.map_err(sql_error)?;
        let num = owner
            .strip_prefix("agent:a")
            .or_else(|| owner.strip_prefix("workspace:a"))
            .and_then(|rest| rest.split('#').next())
            .and_then(|num| num.parse::<i64>().ok());
        if let Some(num) = num {
            agents.insert(num);
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT DISTINCT agent FROM artifacts WHERE json_extract(body,'$.state')='unknown'",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(sql_error)?;
    for row in rows {
        agents.insert(row.map_err(sql_error)?);
    }
    let mut statement = connection
        .prepare(
            "SELECT DISTINCT agent FROM source_state
             WHERE state='open' AND source!='daemon'
               AND source NOT IN ('os','git','workspace_process')",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(sql_error)?;
    for row in rows {
        agents.insert(row.map_err(sql_error)?);
    }
    Ok(agents.into_iter().collect())
}

/// A settled managed workspace keeps its process source open until removal.
/// Only delete preflight can defer that exact generation's source. Every
/// live process remains a blocker, and record deletion requires closure.
fn custody_blocker_tx(
    tx: &Transaction<'_>,
    agent: i64,
    observers: bool,
    workspace_cleanup_generation: Option<i64>,
) -> Result<Option<String>> {
    tx.query_row(
        "SELECT state||' '||role||' process '||pid||' ('||owner||')' FROM processes
             WHERE state IN ('running','unknown')
               AND (owner GLOB 'agent:a'||?1||'#*' OR owner GLOB 'workspace:a'||?1||'#*')
         UNION ALL SELECT 'unknown artifact '||id FROM artifacts WHERE agent=?1 AND json_extract(body,'$.state')='unknown'
         UNION ALL SELECT 'open '||source||' source (generation '||generation||')' FROM source_state
             WHERE agent=?1 AND state='open' AND source!='daemon'
               AND (?2 OR source NOT IN ('os','git','workspace_process'))
               AND NOT (source='workspace_process' AND generation IS ?3)
         LIMIT 1",
        params![agent, observers, workspace_cleanup_generation],
        |row| row.get(0),
    )
    .optional()
    .map_err(sql_error)
}

fn prepare_delete_tree_tx(tx: &Transaction<'_>, members: &[i64]) -> Result<Vec<String>> {
    const MAX_DELETE_COMMANDS: usize = 10_000;
    // Temporary selection is transaction-local and is never a second deletion ledger.
    delete_db!(tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS deleted_agents(id INTEGER PRIMARY KEY);
         CREATE TEMP TABLE IF NOT EXISTS deleted_commands(id TEXT PRIMARY KEY NOT NULL);
         DELETE FROM deleted_agents;
         DELETE FROM deleted_commands;"
    ));
    for num in members {
        delete_db!(tx.execute("INSERT INTO deleted_agents VALUES (?1)", [num]));
    }
    let retained_root: bool = delete_db!(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM deleted_agents d JOIN agents a ON a.root=d.id
                       WHERE a.observe_only=1 AND a.id NOT IN (SELECT id FROM deleted_agents))",
        [],
        |row| row.get(0),
    ));
    if retained_root {
        return Err(Error::new(
            "busy",
            "a retained native observation still refers to this root",
        ));
    }
    let mut command_count = delete_db!(tx.execute(
        "INSERT INTO deleted_commands
         SELECT id FROM commands WHERE target IN (SELECT id FROM deleted_agents) LIMIT ?1",
        [MAX_DELETE_COMMANDS as i64 + 1],
    ));
    if command_count <= MAX_DELETE_COMMANDS {
        // Selector and automation admission parents have no Agent target. The
        // link check below still requires every member of each included set.
        command_count += delete_db!(tx.execute(
            "INSERT INTO deleted_commands
             SELECT DISTINCT p.id FROM deleted_commands d
             JOIN commands c ON c.id=d.id JOIN commands p ON p.id=c.parent_command
             WHERE p.target IS NULL AND p.parent_command IS NULL AND p.state='applied'
               AND ((c.link_kind=?1 AND p.selector IS NOT NULL)
                 OR (c.link_kind='rule_action' AND p.verb IN ('rule','timer.run','rule.catch_up')))
             LIMIT ?2",
            params![
                hierarchy::SELECTOR_LINK_KIND,
                (MAX_DELETE_COMMANDS - command_count) as i64 + 1
            ],
        ));
    }
    if command_count > MAX_DELETE_COMMANDS {
        return Err(Error::new(
            "unavailable",
            "Agent command tree exceeds deletion bound",
        ));
    }
    let crossing: bool = delete_db!(tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM deleted_commands d JOIN commands c ON c.id=d.id
             WHERE c.parent_command IS NOT NULL
               AND c.parent_command NOT IN (SELECT id FROM deleted_commands)
         ) OR EXISTS(
             SELECT 1 FROM deleted_commands d JOIN commands c ON c.parent_command=d.id
             WHERE c.id NOT IN (SELECT id FROM deleted_commands)
         )",
        [],
        |r| r.get(0),
    ));
    if crossing {
        return Err(Error::new(
            "busy",
            "command tree crosses the selected Agent tree",
        ));
    }
    let latest: i64=tx.query_row("SELECT COALESCE(MAX(seq),0) FROM events WHERE agent IN (SELECT id FROM deleted_agents)",[],|r|r.get(0)).map_err(delete_sql_error)?;
    // Rules traverse the global log. Per-Agent sources consume only their own
    // generation's history; the stopped-and-drained guard already excludes
    // every live source in the selected tree.
    let pinned: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM rule_state WHERE enabled=1 AND (current_cursor<?1 OR retiring_cursor<?1))",[latest],|r|r.get(0)).map_err(delete_sql_error)?;
    if pinned {
        return Err(Error::new(
            "busy",
            "retained history is pinned by a live rule cursor",
        ));
    }
    let ids: Vec<String> = tx
        .prepare(
            "WITH RECURSIVE tree(id,depth) AS (
                 SELECT c.id,0 FROM deleted_commands d JOIN commands c ON c.id=d.id
                 WHERE c.parent_command IS NULL
                 UNION ALL
                 SELECT c.id,t.depth+1 FROM commands c JOIN tree t ON c.parent_command=t.id
                 WHERE t.depth<1000
             ) SELECT id FROM tree ORDER BY depth DESC",
        )
        .map_err(delete_sql_error)?
        .query_map([], |r| r.get(0))
        .map_err(delete_sql_error)?
        .collect::<rusqlite::Result<_>>()
        .map_err(delete_sql_error)?;
    if ids.len() != command_count {
        return Err(Error::new(
            "source_integrity",
            "command tree is incomplete or exceeds its depth bound",
        ));
    }
    Ok(ids)
}

fn delete_tree_tx(tx: &Transaction<'_>, members: &[i64], now: i64) -> Result<()> {
    let ids = prepare_delete_tree_tx(tx, members)?;
    delete_db!(tx.execute("DELETE FROM processes WHERE EXISTS(SELECT 1 FROM deleted_agents d WHERE processes.owner GLOB 'agent:a'||d.id||'#*' OR processes.owner GLOB 'workspace:a'||d.id||'#*')",[]));
    // Preserve existing mutation receipts. Create retry tombstones only
    // for commands whose identities would otherwise disappear.
    delete_db!(tx.execute(
        "INSERT INTO writes(id,principal,method,payload_digest,result,created_at)
         SELECT c.id,c.principal,c.method,c.payload_digest,'{}',?1
         FROM deleted_commands d JOIN commands c ON c.id=d.id
         WHERE NOT EXISTS (SELECT 1 FROM writes w WHERE w.id=c.id)",
        [now],
    ));
    // A deleted probe binding is proven settled (the tree guard above admits
    // no live or unknown command), so its claim falls back to the
    // conservative held restriction instead of violating the held/probing
    // CHECK or silently dropping the restriction. Genuinely unknown ongoing
    // probes never reach this statement: they block the delete above.
    delete_db!(tx.execute("UPDATE provider_holds SET probe_command=NULL, state='held', updated_at=?1 WHERE probe_command IN (SELECT id FROM deleted_commands)",[now]));
    // Active cleanup attempts already block. Deleting the Agent supersedes
    // plan items that never acquired an attempt fence.
    for (table, column) in [
        ("agent_config", "agent"),
        ("agent_edits", "agent"),
        ("workspaces", "agent"),
        ("turns", "agent"),
        ("pending_requests", "agent"),
        ("source_owners", "agent"),
        ("source_state", "agent"),
        ("session_usage", "agent"),
        ("goals", "agent"),
        ("goal_markers", "agent"),
        ("claims", "agent"),
        ("owned_exits", "agent"),
        ("mail_notices", "recipient"),
        ("attention_wakes", "recipient"),
        ("completion_digests", "parent"),
        ("artifacts", "agent"),
        ("chat_organization", "agent"),
    ] {
        delete_db!(tx.execute(
            &format!("DELETE FROM {table} WHERE {column} IN (SELECT id FROM deleted_agents)"),
            []
        ));
    }
    delete_db!(tx.execute("DELETE FROM native_child_registrations WHERE producer IN (SELECT id FROM deleted_agents) OR parent IN (SELECT id FROM deleted_agents) OR child IN (SELECT id FROM deleted_agents)",[]));
    delete_db!(tx.execute("DELETE FROM messages WHERE recipient IN (SELECT id FROM deleted_agents) OR sender IN (SELECT 'a'||id FROM deleted_agents)",[]));
    // Mark a conservative history floor before removing individual retained events.
    delete_db!(tx.execute("UPDATE rule_history_coverage SET pruned_through=MAX(pruned_through,COALESCE((SELECT MAX(seq) FROM events WHERE agent IN (SELECT id FROM deleted_agents)),0)) WHERE singleton=1",[]));
    delete_db!(tx.execute(
        "DELETE FROM events WHERE agent IN (SELECT id FROM deleted_agents)",
        []
    ));
    delete_db!(retention::delete_commands_tx(
        tx,
        ids.iter().map(String::as_str)
    ));
    delete_db!(tx.execute(
        "UPDATE agents SET parent=NULL,root=NULL WHERE id IN (SELECT id FROM deleted_agents)",
        []
    ));
    delete_db!(tx.execute(
        "DELETE FROM agents WHERE id IN (SELECT id FROM deleted_agents)",
        []
    ));
    delete_db!(tx.execute_batch("DELETE FROM deleted_commands; DELETE FROM deleted_agents;"));
    Ok(())
}

impl Store {
    pub fn latest_quota_observation(
        &self,
        snapshot: &Value,
    ) -> Result<Option<core::api::harness::QuotaObservation>> {
        let row: Option<String>=self.connection.query_row("SELECT e.data FROM events e JOIN agents a ON a.id=e.agent WHERE e.kind='quota.observed' AND a.harness='muse' AND a.profile_snapshot=?1 ORDER BY json_extract(e.data,'$.observation.observedAtMs') DESC,e.seq DESC LIMIT 1",[encode_json(snapshot)?],|r|r.get(0)).optional().map_err(sql_error)?;
        row.map(|data| {
            let value: Value = decode_json(&data)?;
            let observation: core::api::harness::QuotaObservation =
                serde_json::from_value(value.get("observation").cloned().unwrap_or(Value::Null))
                    .map_err(|_| {
                        Error::new("source_integrity", "retained quota observation invalid")
                    })?;
            observation.validate()?;
            Ok(observation)
        })
        .transpose()
    }
}
