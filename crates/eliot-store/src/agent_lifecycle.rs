//! Explicit owner tree operations, sharing retention's command deletion.
use super::*;
use eliot_core::api::AgentTreeResponse;

impl Store {
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
        if let Some(result) = self.cleanup_replay(
            "owner",
            id,
            method,
            &json!({"agent":agent,"archived":archived}),
        )? {
            return Ok(result);
        }
        self.ensure_writer_healthy()?;
        let result = self.change_agent_tree_inner(id, agent, archived, now);
        self.fault_on_persistence_error(&result);
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
        if let Some((principal, old_method, old_digest, result)) = write_binding(&tx, id)? {
            if principal != "owner" || old_method != method || old_digest != digest {
                return Err(Error::new("id_conflict", "id belongs to another mutation"));
            }
            return decode_json(&result);
        }
        validate_id_age(id, now)?;
        if load_agent_by_num(&tx, root)?.is_none() {
            return Err(Error::new("not_found", "Agent was not found"));
        }
        let members = hierarchy::subtree_member_nums(&tx, root, MAX_PAGE_ENTRIES as i64)?;
        if members.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "unavailable",
                "Agent tree exceeds operation bound",
            ));
        }
        for num in &members {
            let row = load_agent_by_num(&tx, *num)?
                .ok_or_else(|| Error::new("source_integrity", "Agent tree changed"))?;
            let allowed = if archived.is_some() {
                matches!(row.state, AgentState::Idle | AgentState::Stopped)
            } else {
                row.state == AgentState::Stopped
            };
            let open: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND state IN ('accepted','queued','dispatched','unknown')) OR EXISTS(SELECT 1 FROM turns WHERE agent=?1 AND state IN ('running','unknown'))",[num],|r|r.get(0)).map_err(sql_error)?;
            if !allowed || open || row.active_turn.is_some() || !row.pending.is_empty() {
                return Err(Error::new(
                    "busy",
                    "Agent tree must be idle or stopped to archive, and stopped to delete",
                ));
            }
            if archived.is_none() {
                let live = generation_liveness_tx(&tx, *num)?;
                if live {
                    return Err(Error::new("busy", "Agent tree still has live custody"));
                }
            }
        }
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
        db!(tx.execute("INSERT INTO writes(id,principal,method,payload_digest,result,created_at) VALUES (?1,'owner',?2,?3,?4,?5)",params![id,method,digest,encode_json(&result)?,now]));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(result)
    }
}

fn generation_liveness_tx(tx: &Transaction<'_>, agent: i64) -> Result<bool> {
    tx.query_row("SELECT EXISTS(SELECT 1 FROM artifacts WHERE agent=?1 AND json_extract(body,'$.state')='unknown') OR EXISTS(SELECT 1 FROM source_state WHERE agent=?1 AND state='open' AND source!='daemon') OR EXISTS(SELECT 1 FROM processes WHERE owner LIKE ?2 AND state!='exited') OR EXISTS(SELECT 1 FROM processes WHERE owner LIKE ?3 AND state!='exited')",params![agent,format!("agent:{}#%",format_agent_id(agent)),format!("workspace:{}#%",format_agent_id(agent))],|r|r.get(0)).map_err(sql_error)
}

fn delete_tree_tx(tx: &Transaction<'_>, members: &[i64], now: i64) -> Result<()> {
    // Temporary selection is transaction-local and is never a second deletion ledger.
    db!(tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS deleted_agents(id INTEGER PRIMARY KEY); DELETE FROM deleted_agents;"));
    for num in members {
        db!(tx.execute("INSERT INTO deleted_agents VALUES (?1)", [num]));
    }
    let crossing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM commands c JOIN commands p ON p.id=c.parent_command WHERE (c.target IN (SELECT id FROM deleted_agents)) != (p.target IN (SELECT id FROM deleted_agents)))",[],|r|r.get(0)).map_err(sql_error)?;
    if crossing {
        return Err(Error::new(
            "busy",
            "command tree crosses the selected Agent tree",
        ));
    }
    let command_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE target IN (SELECT id FROM deleted_agents)",
            [],
            |r| r.get(0),
        )
        .map_err(sql_error)?;
    if command_count > 10000 {
        return Err(Error::new(
            "unavailable",
            "Agent command tree exceeds deletion bound",
        ));
    }
    let latest: i64=tx.query_row("SELECT COALESCE(MAX(seq),0) FROM events WHERE agent IN (SELECT id FROM deleted_agents)",[],|r|r.get(0)).map_err(sql_error)?;
    let pinned: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM rule_state WHERE enabled=1 AND (current_cursor<?1 OR retiring_cursor<?1)) OR EXISTS(SELECT 1 FROM source_state WHERE source='git' AND state='open' AND agent NOT IN (SELECT id FROM deleted_agents) AND COALESCE(CAST(cursor AS INTEGER),0)<?1)",[latest],|r|r.get(0)).map_err(sql_error)?;
    if pinned {
        return Err(Error::new(
            "busy",
            "retained history is pinned by a live rule or Git cursor",
        ));
    }
    let ids: Vec<String> = tx.prepare("WITH RECURSIVE tree(id,depth) AS (SELECT id,0 FROM commands WHERE target IN (SELECT id FROM deleted_agents) AND parent_command IS NULL UNION ALL SELECT c.id,t.depth+1 FROM commands c JOIN tree t ON c.parent_command=t.id WHERE t.depth<1000) SELECT id FROM tree ORDER BY depth DESC").map_err(sql_error)?.query_map([],|r|r.get(0)).map_err(sql_error)?.collect::<rusqlite::Result<_>>().map_err(sql_error)?;
    if ids.len() != command_count as usize {
        return Err(Error::new(
            "source_integrity",
            "command tree is incomplete or exceeds its depth bound",
        ));
    }
    db!(tx.execute("DELETE FROM processes WHERE owner IN (SELECT 'agent:a'||id||'#'||generation FROM agents WHERE id IN (SELECT id FROM deleted_agents)) OR owner IN (SELECT 'workspace:a'||id||'#'||generation FROM agents WHERE id IN (SELECT id FROM deleted_agents))",[]));
    // Keep retry identities after removing payloads: a deleted native effect can never be admitted again.
    db!(tx.execute("INSERT INTO writes(id,principal,method,payload_digest,result,created_at) SELECT id,principal,method,payload_digest,'{}',?1 FROM commands WHERE target IN (SELECT id FROM deleted_agents)",[now]));
    db!(tx.execute("UPDATE provider_holds SET probe_command=NULL WHERE probe_command IN (SELECT id FROM commands WHERE target IN (SELECT id FROM deleted_agents))",[]));
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
    ] {
        db!(tx.execute(
            &format!("DELETE FROM {table} WHERE {column} IN (SELECT id FROM deleted_agents)"),
            []
        ));
    }
    db!(tx.execute("DELETE FROM native_child_registrations WHERE producer IN (SELECT id FROM deleted_agents) OR parent IN (SELECT id FROM deleted_agents) OR child IN (SELECT id FROM deleted_agents)",[]));
    db!(tx.execute("DELETE FROM messages WHERE recipient IN (SELECT id FROM deleted_agents) OR sender IN (SELECT 'a'||id FROM deleted_agents)",[]));
    // Mark a conservative history floor before removing individual retained events.
    db!(tx.execute("UPDATE rule_history_coverage SET pruned_through=MAX(pruned_through,COALESCE((SELECT MAX(seq) FROM events WHERE agent IN (SELECT id FROM deleted_agents)),0)) WHERE singleton=1",[]));
    db!(tx.execute(
        "DELETE FROM events WHERE agent IN (SELECT id FROM deleted_agents)",
        []
    ));
    retention::delete_commands_tx(tx, ids.iter().map(String::as_str))?;
    db!(tx.execute(
        "UPDATE agents SET parent=NULL,root=NULL WHERE id IN (SELECT id FROM deleted_agents)",
        []
    ));
    db!(tx.execute(
        "DELETE FROM agents WHERE id IN (SELECT id FROM deleted_agents)",
        []
    ));
    db!(tx.execute("DELETE FROM deleted_agents", []));
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
