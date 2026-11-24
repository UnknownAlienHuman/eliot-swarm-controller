use super::*;
use eliot_core::api::{FailedCustodyResolveRequest, FailedCustodyResolveResponse};

const RESOLVE_METHOD: &str = "agent.custody.resolve";
pub(super) const OWNER_ASSERTED: &str = "owner_asserted_exit";
pub(super) const JOB_VERIFIED: &str = "verified_generation_job_exit";
pub(super) const NEVER_SPAWNED: &str = "start_never_spawned";

impl Store {
    pub(super) fn migrate_failed_custody(&mut self, version: i64) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        tx.execute_batch(
            "CREATE TABLE failed_generation_exits (
                 agent INTEGER NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                 generation INTEGER NOT NULL,
                 basis TEXT NOT NULL CHECK(basis IN ('owner_asserted_exit','verified_generation_job_exit','start_never_spawned')),
                 reason TEXT NOT NULL,
                 resolved_at INTEGER NOT NULL,
                 PRIMARY KEY(agent, generation)
             );",
        ).map_err(sql_error)?;
        tx.execute(
            "INSERT INTO schema_migrations(version) VALUES (?1)",
            [version],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)
    }

    pub fn failed_generation_exit_basis(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        failed_generation_exit_basis_tx(&self.connection, parse_agent_id(agent)?, generation)
    }

    pub fn failed_generation_exit_snapshot(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<RetainedGeneration> {
        self.ensure_committed_reads()?;
        failed_generation_exit_snapshot(&self.connection, agent, generation)
    }

    pub fn commit_failed_generation_job_exit(
        &mut self,
        snapshot: &RetainedGeneration,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = failed_generation_exit_snapshot(&tx, &snapshot.agent, snapshot.generation)?;
        if snapshot.processes_truncated || current != *snapshot {
            return Err(Error::new(
                "stale_generation",
                "failed generation custody changed during verification",
            ));
        }
        let namespace: String = tx
            .query_row(
                "SELECT namespace FROM custody_binding WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if snapshot.custody_namespace.as_deref() != Some(namespace.as_str()) {
            return Err(Error::new(
                "process_custody_foreign",
                "failed generation custody does not belong to this daemon",
            ));
        }
        let events = record_failed_exit(
            &tx,
            parse_agent_id(&snapshot.agent)?,
            snapshot.generation,
            JOB_VERIFIED,
            "Exact generation Job and retained process identities verified gone",
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    pub fn replay_failed_custody_resolution(
        &self,
        request: &FailedCustodyResolveRequest,
    ) -> Result<Option<FailedCustodyResolveResponse>> {
        self.ensure_committed_reads()?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("invalid", "invalid custody resolution"))?;
        self.cleanup_replay("owner", &request.id, RESOLVE_METHOD, &payload)?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| Error::new("source_integrity", "invalid custody resolution receipt"))
    }

    pub fn resolve_failed_custody(
        &mut self,
        request: &FailedCustodyResolveRequest,
        now: i64,
    ) -> Result<Change<FailedCustodyResolveResponse>> {
        self.ensure_writer_healthy()?;
        request.validate()?;
        if let Some(value) = self.replay_failed_custody_resolution(request)? {
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("invalid", "invalid custody resolution"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = require_failed_generation(&tx, &request.agent, request.generation)?;
        let events = record_failed_exit(
            &tx,
            agent,
            request.generation,
            OWNER_ASSERTED,
            &request.reason,
            now,
        )?;
        let basis =
            failed_generation_exit_basis_tx(&tx, agent, request.generation)?.ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "failed custody resolution was not retained",
                )
            })?;
        let value = FailedCustodyResolveResponse {
            id: request.id.clone(),
            agent: request.agent.clone(),
            generation: request.generation,
            basis,
        };
        coordination::retain_write_tx(
            &tx,
            &request.id,
            "owner",
            RESOLVE_METHOD,
            &canonical::digest(&payload)?,
            &serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "invalid custody receipt"))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

fn require_failed_generation(connection: &Connection, agent: &str, generation: i64) -> Result<i64> {
    let num = parse_agent_id(agent)?;
    let current = load_agent_by_num(connection, num)?
        .ok_or_else(|| Error::new("not_found", "failed custody Agent was not found"))?;
    observer::require_controlled(&current)?;
    if generation != current.generation {
        return Err(Error::new(
            "stale_generation",
            "failed custody generation moved",
        ));
    }
    let open_native: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_state WHERE agent=?1 AND generation=?2 AND state='open'
          AND source NOT IN ('daemon','os','git','workspace_process'))",
        params![num, generation], |row| row.get(0),
    ).map_err(sql_error)?;
    if current.state != AgentState::Failed || current.active_turn.is_some() || open_native {
        return Err(Error::new(
            "bad_state",
            "custody resolution requires a Failed generation with closed native sources",
        ));
    }
    Ok(num)
}

fn failed_generation_exit_snapshot(
    connection: &Connection,
    agent: &str,
    generation: i64,
) -> Result<RetainedGeneration> {
    let num = require_failed_generation(connection, agent, generation)?;
    let custody_namespace = connection
        .query_row(
            "SELECT custody_namespace FROM agents WHERE id=?1",
            [num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let owner = format!("agent:{agent}#{generation}");
    let mut statement = connection.prepare(
        "SELECT host_boot_id,pid,start_time,role FROM processes WHERE owner=?1 AND state!='exited'
         ORDER BY host_boot_id,pid,start_time LIMIT ?2",
    ).map_err(sql_error)?;
    let mut processes = statement
        .query_map(params![owner, MAX_PAGE_ENTRIES as i64 + 1], |row| {
            Ok(RecordedProcess {
                identity: ProcessIdentity {
                    host_boot_id: row.get(0)?,
                    pid: row.get(1)?,
                    start_time: row.get::<_, i64>(2)? as u64,
                },
                role: row.get(3)?,
                exited: false,
            })
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let processes_truncated = processes.len() > MAX_PAGE_ENTRIES;
    processes.truncate(MAX_PAGE_ENTRIES);
    Ok(RetainedGeneration {
        agent: agent.to_owned(),
        generation,
        custody_namespace,
        processes,
        processes_truncated,
    })
}

pub(super) fn failed_generation_exit_basis_tx(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT e.basis FROM failed_generation_exits e JOIN agents a ON a.id=e.agent
         WHERE e.agent=?1 AND e.generation=?2 AND a.generation=e.generation AND a.state='failed'",
            params![agent, generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)
}

pub(super) fn record_failed_exit(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    basis: &str,
    reason: &str,
    now: i64,
) -> Result<Vec<Event>> {
    let inserted = tx
        .execute(
            "INSERT INTO failed_generation_exits(agent,generation,basis,reason,resolved_at)
         VALUES (?1,?2,?3,?4,?5) ON CONFLICT(agent,generation) DO NOTHING",
            params![agent, generation, basis, reason, now],
        )
        .map_err(sql_error)?;
    if inserted == 0 {
        return Ok(Vec::new());
    }
    tx.execute(
        "UPDATE processes SET state='exited',exit_code=NULL,updated_at=?1
         WHERE owner=?2 AND state IN ('running','unknown')",
        params![
            now,
            format!("agent:{}#{generation}", format_agent_id(agent))
        ],
    )
    .map_err(sql_error)?;
    tx.execute(
        "UPDATE source_state SET state='closed',updated_at=?1
         WHERE agent=?2 AND generation=?3 AND source='os' AND state='open'",
        params![now, agent, generation],
    )
    .map_err(sql_error)?;
    let mut events = vec![insert_daemon_event(
        tx,
        now,
        "agent.custody_resolved",
        "source",
        Some(agent),
        Some(generation),
        json!({"basis":basis,"reason":reason}),
    )?];
    if capacity::release_proven_generation_tx(tx, agent, generation)? {
        events.push(insert_daemon_event(
            tx,
            now,
            "capacity.released",
            "source",
            Some(agent),
            Some(generation),
            json!({"reason":"failed_custody_resolved","basis":basis}),
        )?);
    }
    Ok(events)
}
