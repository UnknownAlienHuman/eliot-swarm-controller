use super::*;

impl Store {
    pub(super) fn observe_processes_inner(
        &mut self,
        agent: &str,
        generation: i64,
        records: &[ProcessRecord],
        now: i64,
        scope: ProcessSourceKind,
    ) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(agent)?;
        let expected_owner = scope.owner(agent, generation);
        validate_process_observation(records, &expected_owner)?;
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
            merge_process_record(old.as_ref(), &mut next, scope, &expected_owner)?;
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
}

fn validate_process_observation(records: &[ProcessRecord], expected_owner: &str) -> Result<()> {
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
    Ok(())
}

fn merge_process_record(
    old: Option<&ProcessRecord>,
    next: &mut ProcessRecord,
    scope: ProcessSourceKind,
    expected_owner: &str,
) -> Result<()> {
    if let Some(old) = old {
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
    Ok(())
}

fn process_state_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Running => "running",
        ProcessState::Exited => "exited",
        ProcessState::Unknown => "unknown",
    }
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
            subject: event_subject_for_generation(tx, agent, generation)?,
            cause: None,
            data: json!(record),
        },
    )
}
