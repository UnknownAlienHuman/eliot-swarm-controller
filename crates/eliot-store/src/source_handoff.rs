//! Atomic native-family transfer. Paseo timeline-sync at abc23d77 is a pattern
//! only: coverage and checkpoints survive independently of visible native facts.

use super::*;
use eliot_core::{SourceHandoff, SourceHandoffAck, api::SourceOwner};

pub(super) fn owner_bindings(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<Vec<SourceOwner>> {
    let mut statement = connection
        .prepare(
            "SELECT family, source, mechanism FROM source_owners
             WHERE agent = ?1 AND generation = ?2 ORDER BY family LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![agent, generation, MAX_PAGE_ENTRIES as i64 + 1],
            |row| {
                Ok(SourceOwner {
                    family: row.get(0)?,
                    source: row.get(1)?,
                    mechanism: row.get(2)?,
                })
            },
        )
        .map_err(sql_error)?;
    let owners = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    if owners.len() > MAX_PAGE_ENTRIES || encode_json(&owners)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "source ownership readback exceeds its bound",
        ));
    }
    Ok(owners)
}

fn reserved_source(source: &str) -> bool {
    matches!(source, "daemon" | "os" | "git" | "workspace_process")
}

impl Store {
    /// Commit the boundary, named-family owners and retained ACK before the caller
    /// publishes events. No native effect is performed or replayed by this method.
    pub fn record_source_handoff(
        &mut self,
        handoff: SourceHandoff,
        now: i64,
    ) -> Result<Change<SourceHandoffAck>> {
        self.ensure_writer_healthy()?;
        let result = self.record_source_handoff_inner(handoff, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_handoff_inner(
        &mut self,
        mut handoff: SourceHandoff,
        now: i64,
    ) -> Result<Change<SourceHandoffAck>> {
        if reserved_source(&handoff.source) || reserved_source(&handoff.successor) {
            return Err(Error::new(
                "not_allowed",
                "native handoff cannot claim daemon monitor sources",
            ));
        }
        for family in &handoff.families {
            match family.as_str() {
                "session" | "turn" | "message" | "tool" | "request" => {}
                "child" | "goal" | "usage" => {
                    return Err(Error::new(
                        "unsupported",
                        "handoff family is not implemented",
                    ));
                }
                _ => {
                    return Err(Error::new(
                        "not_allowed",
                        "native handoff cannot claim daemon families",
                    ));
                }
            }
        }
        handoff.validate()?;
        if handoff.families.len() > MAX_NATIVE_FAMILIES as usize {
            return Err(Error::new(
                "invalid",
                "handoff family set exceeds its bound",
            ));
        }
        handoff.families.sort_unstable();
        let data = serde_json::to_value(&handoff)
            .map_err(|error| Error::new("invalid", error.to_string()))?;
        if encode_json(&data)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "source handoff exceeds its event bound",
            ));
        }
        let digest = canonical::digest(&data)?;
        let gap_data = handoff.gap.as_ref().map(|gap| json!({
            "gap_id": gap.gap_id, "agent_id": handoff.agent_id, "generation": handoff.generation,
            "source": handoff.source, "families": handoff.families, "reason": gap.reason,
            "from": gap.from, "to": gap.to,
        }));
        let gap_digest = gap_data.as_ref().map(canonical::digest).transpose()?;
        let agent_num = parse_agent_id(&handoff.agent_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "handoff Agent was not found"))?;
        if handoff.generation <= 0 || handoff.generation != agent.generation {
            return Err(Error::new(
                "stale_generation",
                "handoff requires the current admitted generation",
            ));
        }
        // Stage A transfers controlled facts only. Private observation history
        // remains read-only; observer live recovery belongs to its next caller.
        observer::require_controlled(&agent)?;
        if agent.is_terminal() {
            return Err(Error::new(
                "bad_state",
                "handoff cannot revive a terminal Agent",
            ));
        }
        let supported = source_families_for_harness(&agent.harness)?;
        if handoff
            .families
            .iter()
            .any(|family| !supported.contains(&family.as_str()))
        {
            return Err(Error::new(
                "unsupported",
                "handoff family is not implemented by this harness",
            ));
        }
        let predecessor = load_source_state(&tx, agent_num, handoff.generation, &handoff.source)?
            .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "handoff predecessor has no committed state",
            )
        })?;
        let successor = load_source_state(&tx, agent_num, handoff.generation, &handoff.successor)?;
        if predecessor.state != "open"
            || successor
                .as_ref()
                .is_some_and(|source| source.state != "open")
        {
            return Err(Error::new(
                "source_integrity",
                "handoff source is closed; reopening is forbidden",
            ));
        }
        let owners = owner_bindings(&tx, agent_num, handoff.generation)?;
        let all_predecessor = handoff.families.iter().all(|family| {
            owners
                .iter()
                .any(|owner| owner.family == *family && owner.source == handoff.source)
        });
        let all_successor = handoff.families.iter().all(|family| {
            owners.iter().any(|owner| {
                owner.family == *family
                    && owner.source == handoff.successor
                    && owner.mechanism == handoff.mechanism
            })
        });
        if predecessor.last_handoff_digest.as_deref() == Some(digest.as_slice()) {
            let ack: SourceHandoffAck = predecessor
                .last_handoff
                .as_deref()
                .map(|encoded| {
                    serde_json::from_str(encoded).map_err(|_| {
                        Error::new("internal", "stored handoff acknowledgement is malformed")
                    })
                })
                .transpose()?
                .ok_or_else(|| {
                    Error::new("source_integrity", "handoff acknowledgement is missing")
                })?;
            handoff
                .validate_ack(&ack)
                .map_err(|_| retained_mismatch())?;
            let boundary_matches = match (&handoff.cursor, &handoff.gap) {
                (Some(cursor), None) => predecessor.cursor.as_ref() == Some(cursor),
                (None, Some(gap)) => {
                    predecessor.gap_id.as_deref() == Some(gap.gap_id.as_str())
                        && predecessor.gap_digest.as_deref() == gap_digest.as_deref()
                        && predecessor.gap_from == gap.from
                        && predecessor.gap_to == gap.to
                        && predecessor
                            .gap_seq
                            .is_some_and(|seq| seq > 0 && seq < ack.seq)
                }
                _ => false,
            };
            if !all_successor || !boundary_matches || successor.is_none() {
                return Err(retained_mismatch());
            }
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: ack,
                events: Vec::new(),
            });
        }
        if !all_predecessor {
            return Err(Error::new(
                "source_integrity",
                "named families do not all have the expected predecessor",
            ));
        }
        if handoff
            .cursor
            .as_ref()
            .is_some_and(|cursor| predecessor.cursor.as_ref() != Some(cursor))
        {
            return Err(Error::new(
                "source_integrity",
                "handoff cursor is not the committed exclusive checkpoint; an explicit gap is required",
            ));
        }
        let mut events = Vec::new();
        let mut gap_seq = None;
        if let Some(gap) = &handoff.gap {
            if predecessor.gap_id.as_deref() == Some(gap.gap_id.as_str()) {
                if predecessor.gap_digest.as_deref() != gap_digest.as_deref() {
                    return Err(Error::new(
                        "source_integrity",
                        "handoff gap_id conflicts with its retained boundary",
                    ));
                }
                gap_seq = predecessor.gap_seq;
                if gap_seq.is_none_or(|seq| seq <= 0) {
                    return Err(Error::new(
                        "source_integrity",
                        "retained gap acknowledgement is missing",
                    ));
                }
            } else {
                let event = insert_daemon_event(
                    &tx,
                    now,
                    "source.gap",
                    "source",
                    Some(agent_num),
                    Some(handoff.generation),
                    gap_data
                        .ok_or_else(|| Error::new("internal", "handoff gap data is missing"))?,
                )?;
                db!(tx.execute(
                    "UPDATE source_state SET gap_id = ?1, gap_digest = ?2, gap_from = ?3, gap_to = ?4,
                     gap_seq = ?5, updated_at = ?6 WHERE agent = ?7 AND generation = ?8 AND source = ?9",
                    params![gap.gap_id, gap_digest, gap.from, gap.to, event.seq, now, agent_num, handoff.generation, handoff.source],
                ));
                gap_seq = Some(event.seq);
                events.push(event);
            }
        }
        if successor.is_none() {
            db!(tx.execute(
                "INSERT INTO source_state(agent,generation,source,state,health,updated_at)
                 VALUES (?1,?2,?3,'open','ok',?4)",
                params![agent_num, handoff.generation, handoff.successor, now],
            ));
            events.push(insert_daemon_event(&tx, now, "source.opened", "source", Some(agent_num),
                Some(handoff.generation), json!({"source":handoff.successor,"mechanism":handoff.mechanism,"families":handoff.families}))?);
        }
        for family in &handoff.families {
            let changed = tx
                .execute(
                    "UPDATE source_owners SET source = ?1, mechanism = ?2
                 WHERE agent = ?3 AND generation = ?4 AND family = ?5 AND source = ?6",
                    params![
                        handoff.successor,
                        handoff.mechanism,
                        agent_num,
                        handoff.generation,
                        family,
                        handoff.source
                    ],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "handoff predecessor changed before transfer",
                ));
            }
        }
        let event = insert_daemon_event(
            &tx,
            now,
            "source.handoff",
            "source",
            Some(agent_num),
            Some(handoff.generation),
            data,
        )?;
        if gap_seq.is_some_and(|seq| seq >= event.seq) {
            return Err(Error::new(
                "source_integrity",
                "handoff gap does not precede its acknowledgement",
            ));
        }
        let ack = SourceHandoffAck {
            agent_id: handoff.agent_id,
            generation: handoff.generation,
            source: handoff.source,
            families: handoff.families,
            successor: handoff.successor,
            mechanism: handoff.mechanism,
            cursor: handoff.cursor,
            gap_id: handoff.gap.map(|gap| gap.gap_id),
            seq: event.seq,
        };
        let changed = tx.execute(
            "UPDATE source_state SET last_handoff_digest = ?1, last_handoff = ?2, updated_at = ?3
             WHERE agent = ?4 AND generation = ?5 AND source = ?6 AND state = 'open'",
            params![digest, encode_json(&ack)?, now, agent_num, ack.generation, ack.source],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "source_integrity",
                "handoff predecessor changed before acknowledgement",
            ));
        }
        events.push(event);
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value: ack, events })
    }

    /// Record an exiting plugin's uncovered native tail using current owners,
    /// never a hardcoded predecessor. The Host calls after authoritative drain;
    /// all source gaps commit together before it publishes any returned event.
    pub fn record_native_exit_gaps(
        &mut self,
        agent: &str,
        generation: i64,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.record_native_exit_gaps_inner(agent, generation, reason, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_native_exit_gaps_inner(
        &mut self,
        agent: &str,
        generation: i64,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Event>> {
        require_nonempty(reason, "native exit gap reason")?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "exited plugin Agent was not found"))?;
        if generation <= 0 || current.generation != generation {
            return Err(Error::new(
                "stale_generation",
                "exited plugin generation is no longer current",
            ));
        }
        observer::require_controlled(&current)?;
        // A terminal native fact committed during drainage wins over loss inference.
        if current.is_terminal() {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Vec::new());
        }
        let events = native_exit_gaps_tx(&tx, &current, reason, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    /// The Host has confirmed reader unavailability, independently of custody.
    /// A closed reader is a separate fact: false retains open/stale sources;
    /// true closes their transport lifetime. Neither case settles native work.
    pub fn record_native_reader_unavailable(
        &mut self,
        agent: &str,
        generation: i64,
        reason: &str,
        reader_closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.record_native_reader_unavailable_inner(
            agent,
            generation,
            reason,
            reader_closed,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_native_reader_unavailable_inner(
        &mut self,
        agent: &str,
        generation: i64,
        reason: &str,
        reader_closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        require_nonempty(reason, "native reader unavailable reason")?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "unavailable reader Agent was not found"))?;
        if generation <= 0 || generation > current.generation {
            return Err(Error::new(
                "stale_generation",
                "native reader generation does not exist",
            ));
        }
        observer::require_controlled(&current)?;
        let mut events = if generation == current.generation && !current.is_terminal() {
            native_exit_gaps_tx(&tx, &current, reason, now)?
        } else {
            Vec::new()
        };
        let mut statement = tx.prepare(
            "SELECT source, state, health FROM source_state WHERE agent = ?1 AND generation = ?2
             AND source NOT IN ('daemon','os','git','workspace_process') ORDER BY source LIMIT ?3",
        ).map_err(sql_error)?;
        let sources = statement
            .query_map(
                params![agent_num, generation, MAX_PAGE_ENTRIES as i64 + 1],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        drop(statement);
        if sources.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "native reader unavailability exceeds its row bound",
            ));
        }
        for (source, state, health) in sources {
            let close = reader_closed && state == "open";
            if health == "stale" && !close {
                continue;
            }
            let next_state = if close { "closed" } else { state.as_str() };
            let changed = tx
                .execute(
                    "UPDATE source_state SET health = 'stale', state = ?1, updated_at = ?2
                 WHERE agent = ?3 AND generation = ?4 AND source = ?5",
                    params![next_state, now, agent_num, generation, source],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "native source changed before unavailable commit",
                ));
            }
            if health != "stale" {
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "source.health",
                    "source",
                    Some(agent_num),
                    Some(generation),
                    json!({"source":source,"state":next_state,"health":"stale"}),
                )?);
            }
            if close {
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "source.closed",
                    "source",
                    Some(agent_num),
                    Some(generation),
                    json!({"source":source}),
                )?);
            }
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }

    /// The Host calls this only after authoritative EOF/drain or confirmed owned
    /// plugin cleanup. Closing a retiring generation never changes the current
    /// Agent, credentials, family owners, boundaries or command outcomes.
    pub fn close_native_sources(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.close_native_sources_inner(agent, generation, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn close_native_sources_inner(
        &mut self,
        agent: &str,
        generation: i64,
        now: i64,
    ) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "native source Agent was not found"))?;
        if generation <= 0 || generation > current.generation {
            return Err(Error::new(
                "stale_generation",
                "native source generation does not exist",
            ));
        }
        let mut statement = tx
            .prepare(
                "SELECT source FROM source_state WHERE agent = ?1 AND generation = ?2
             AND source NOT IN ('daemon','os','git','workspace_process') AND state = 'open'
             ORDER BY source LIMIT ?3",
            )
            .map_err(sql_error)?;
        let sources = statement
            .query_map(
                params![agent_num, generation, MAX_PAGE_ENTRIES as i64 + 1],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        drop(statement);
        if sources.len() > MAX_PAGE_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "native source closure exceeds its row bound",
            ));
        }
        let mut events = Vec::new();
        for source in sources {
            db!(tx.execute(
                "UPDATE source_state SET state = 'closed', updated_at = ?1
                 WHERE agent = ?2 AND generation = ?3 AND source = ?4 AND state = 'open'",
                params![now, agent_num, generation, source],
            ));
            events.push(insert_daemon_event(
                &tx,
                now,
                "source.closed",
                "source",
                Some(agent_num),
                Some(generation),
                json!({"source":source}),
            )?);
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }
}

fn native_exit_gaps_tx(
    tx: &Transaction<'_>,
    current: &Agent,
    reason: &str,
    now: i64,
) -> Result<Vec<Event>> {
    let agent = current.id.as_str();
    let agent_num = parse_agent_id(agent)?;
    let generation = current.generation;
    let supported = source_families_for_harness(&current.harness)?;
    let mut groups = std::collections::BTreeMap::<String, Vec<String>>::new();
    for owner in owner_bindings(tx, agent_num, generation)? {
        if supported.contains(&owner.family.as_str()) {
            if reserved_source(&owner.source) {
                return Err(Error::new(
                    "source_integrity",
                    "native family has a daemon source owner",
                ));
            }
            groups.entry(owner.source).or_default().push(owner.family);
        }
    }
    let mut events = Vec::new();
    for (source, families) in groups {
        let state = load_source_state(tx, agent_num, generation, &source)?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "native owner has no committed source state",
            )
        })?;
        let gap_id = format!("host-root-exit:{agent}:{generation}:{source}");
        let data = json!({
            "gap_id":gap_id, "agent_id":agent, "generation":generation,
            "source":source, "families":families, "reason":reason,
            "from":state.cursor, "to":null,
        });
        if encode_json(&data)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "native exit gap exceeds its event bound",
            ));
        }
        let digest = canonical::digest(&data)?;
        if state.gap_id.as_deref() == Some(gap_id.as_str()) {
            if state.gap_digest.as_deref() != Some(digest.as_slice())
                || state.gap_seq.is_none_or(|seq| seq <= 0)
            {
                return Err(Error::new(
                    "source_integrity",
                    "native exit gap conflicts with its retained boundary",
                ));
            }
            continue;
        }
        if state.state != "open" {
            return Err(Error::new(
                "source_integrity",
                "native owner source is already closed",
            ));
        }
        let event = insert_daemon_event(
            tx,
            now,
            "source.gap",
            "source",
            Some(agent_num),
            Some(generation),
            data,
        )?;
        let changed = tx
            .execute(
                "UPDATE source_state SET gap_id = ?1, gap_digest = ?2, gap_from = ?3,
             gap_to = NULL, gap_seq = ?4, updated_at = ?5
             WHERE agent = ?6 AND generation = ?7 AND source = ?8 AND state = 'open'",
                params![
                    gap_id,
                    digest,
                    state.cursor,
                    event.seq,
                    now,
                    agent_num,
                    generation,
                    source
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "source_integrity",
                "native source changed before exit gap commit",
            ));
        }
        events.push(event);
    }
    Ok(events)
}

fn retained_mismatch() -> Error {
    Error::new(
        "source_integrity",
        "retained handoff no longer matches current owners and boundary",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::SourceHandoffGap;

    fn fixture(harness: &str) -> (Store, String, i64) {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let command_id = Ulid::new().to_string();
        let profile = if harness == "fake" {
            "fake"
        } else {
            "synthetic-codex"
        };
        let admitted = store.submit("owner", SubmitCommand {
            id: command_id.clone(), target: None, verb: "launch".to_owned(),
            args: json!({"project":"synthetic-handoff", "harness":harness, "profile":profile,
                "profile_snapshot":{}, "workspace":"synthetic-handoff-workspace"}),
            expected_generation: Some(0), expected_turn_id: None, request_id: None,
        }, now).unwrap();
        let agent = admitted.command.target.unwrap();
        store.dispatch(&command_id, now).unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: "ready".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ready".to_owned(),
                        native_id: "ready".to_owned(),
                        data: json!({"command_id":command_id,"native_scope":"synthetic:scope","native_ref":"synthetic:subject"}),
                    }],
                    cursor: Some("native:ready".to_owned()),
                },
                now,
            )
            .unwrap();
        (store, agent, now)
    }

    fn handoff(agent: &str, families: &[&str]) -> SourceHandoff {
        SourceHandoff {
            agent_id: agent.to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            families: families.iter().map(|family| (*family).to_owned()).collect(),
            successor: "transcript".to_owned(),
            mechanism: "fake_recovery".to_owned(),
            cursor: Some("native:ready".to_owned()),
            gap: None,
        }
    }

    fn with_gap(mut request: SourceHandoff) -> SourceHandoff {
        request.cursor = None;
        request.gap = Some(SourceHandoffGap {
            gap_id: "tail-gap".to_owned(),
            reason: "uncovered_tail".to_owned(),
            from: Some("native:ready".to_owned()),
            to: None,
        });
        request
    }

    fn admit_send(store: &mut Store, agent: &str, now: i64) -> String {
        let id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: Some(agent.to_owned()),
                    verb: "send".to_owned(),
                    args: json!({"text":"synthetic"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                },
                now,
            )
            .unwrap();
        store.dispatch(&id, now).unwrap();
        id
    }

    fn turn_batch(agent: &str, source: &str, command: &str) -> SourceBatch {
        SourceBatch {
            batch_id: "turn".to_owned(),
            agent_id: agent.to_owned(),
            generation: 1,
            source: source.to_owned(),
            events: vec![NativeEvent {
                kind: "turn.started".to_owned(),
                native_id: "turn".to_owned(),
                data: json!({"command_id":command,"turn_id":"turn-1"}),
            }],
            cursor: Some("native:turn".to_owned()),
        }
    }

    #[test]
    fn cursor_handoff_keeps_predecessor_open_and_readback_settles_lost_reply() {
        let (mut store, agent, now) = fixture("fake");
        store.ensure_process_source(&agent, 1, now).unwrap();
        let before = store.get_agent(&agent).unwrap().unwrap();
        let request = handoff(&agent, &["message"]);
        let committed = store
            .record_source_handoff(request.clone(), now + 1)
            .unwrap();
        assert_eq!(
            committed
                .events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["source.opened", "source.handoff"]
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        let state = store.read_source_state(&agent, 1, "stream").unwrap();
        assert_eq!(state["state"], "open");
        assert_eq!(state["cursor"], "native:ready");
        assert_eq!(
            state["last_handoff"],
            serde_json::to_value(&committed.value).unwrap()
        );
        assert_eq!(state["last_handoff_digest"].as_array().unwrap().len(), 32);
        assert_eq!(request.ack_from_state(&state).unwrap(), committed.value);
        assert!(
            state["owners"]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| owner["family"] == "message"
                    && owner["source"] == "transcript"
                    && owner["mechanism"] == "fake_recovery")
        );
        assert!(
            state["owners"]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| owner["source"] == "os")
        );
        let views = store.list_source_states(&agent, 1).unwrap();
        for view in &views {
            assert!(view.get("last_handoff").is_none());
            assert!(view.get("last_handoff_digest").is_none());
            assert!(
                view["owners"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|owner| owner["source"] == view["source"])
            );
        }
        let high_water = store.high_water().unwrap();
        let duplicate = store.record_source_handoff(request, now + 2).unwrap();
        assert_eq!(duplicate.value, committed.value);
        assert!(duplicate.events.is_empty());
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn gap_and_named_owners_commit_before_the_handoff_ack() {
        let (mut store, agent, now) = fixture("fake");
        let request = with_gap(handoff(&agent, &["turn", "message"]));
        let committed = store
            .record_source_handoff(request.clone(), now + 1)
            .unwrap();
        assert_eq!(committed.value.families, ["message", "turn"]);
        assert_eq!(
            committed
                .events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["source.gap", "source.opened", "source.handoff"]
        );
        assert!(committed.events[0].seq < committed.value.seq);
        let state = store.read_source_state(&agent, 1, "stream").unwrap();
        assert_eq!(state["gap_id"], "tail-gap");
        assert_eq!(state["gap_seq"], committed.events[0].seq);
        assert_eq!(state["state"], "open");
        assert_eq!(request.ack_from_state(&state).unwrap(), committed.value);
        let high_water = store.high_water().unwrap();
        assert!(
            store
                .record_source_handoff(request, now + 2)
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn cursor_advance_does_not_reauthorize_a_retained_handoff() {
        let (mut store, agent, now) = fixture("fake");
        let request = handoff(&agent, &["message"]);
        let ack = store
            .record_source_handoff(request.clone(), now + 1)
            .unwrap()
            .value;
        let command = admit_send(&mut store, &agent, now + 2);
        store
            .put_batch(turn_batch(&agent, "stream", &command), now + 3)
            .unwrap();
        let high_water = store.high_water().unwrap();
        let before = store.read_source_state(&agent, 1, "stream").unwrap();
        assert_eq!(before["cursor"], "native:turn");
        assert_eq!(before["last_handoff"], serde_json::to_value(ack).unwrap());
        assert_eq!(
            store
                .record_source_handoff(request.clone(), now + 4)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(request.ack_from_state(&before).unwrap_err().code, "unknown");
        assert_eq!(
            store.read_source_state(&agent, 1, "stream").unwrap(),
            before
        );
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn invalid_or_mixed_handoffs_leave_events_sources_and_owners_unchanged() {
        let (mut store, agent, now) = fixture("fake");
        let original = handoff(&agent, &["message"]);
        let mut requests = Vec::new();
        let mut bad = original.clone();
        bad.generation = 2;
        requests.push(bad);
        let mut bad = original.clone();
        bad.source = "missing".to_owned();
        requests.push(bad);
        let mut bad = original.clone();
        bad.source = "os".to_owned();
        requests.push(bad);
        let mut bad = original.clone();
        bad.successor = "git".to_owned();
        requests.push(bad);
        let mut bad = original.clone();
        bad.families = vec!["message".to_owned(), "message".to_owned()];
        requests.push(bad);
        let mut bad = original.clone();
        bad.families = vec!["tool".to_owned()];
        requests.push(bad);
        let mut bad = original.clone();
        bad.families.clear();
        requests.push(bad);
        let mut bad = original.clone();
        bad.cursor = Some("uncommitted".to_owned());
        requests.push(bad);
        let mut bad = original.clone();
        bad.cursor = None;
        requests.push(bad);
        let mut bad = with_gap(original.clone());
        bad.cursor = Some("native:ready".to_owned());
        requests.push(bad);
        for request in requests {
            let before = store.read_source_state(&agent, 1, "stream").unwrap();
            let sources = store.list_source_states(&agent, 1).unwrap();
            let high_water = store.high_water().unwrap();
            assert!(store.record_source_handoff(request, now + 1).is_err());
            assert_eq!(
                store.read_source_state(&agent, 1, "stream").unwrap(),
                before
            );
            assert_eq!(store.list_source_states(&agent, 1).unwrap(), sources);
            assert_eq!(store.high_water().unwrap(), high_water);
        }
        store.record_source_handoff(original, now + 1).unwrap();
        let before = store.read_source_state(&agent, 1, "stream").unwrap();
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_source_handoff(handoff(&agent, &["session", "message"]), now + 2)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(
            store.read_source_state(&agent, 1, "stream").unwrap(),
            before
        );
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn successor_cannot_publish_before_transfer_and_eof_closes_native_sources_only() {
        let (mut store, agent, now) = fixture("fake");
        store.ensure_process_source(&agent, 1, now).unwrap();
        let command = admit_send(&mut store, &agent, now + 1);
        let batch = turn_batch(&agent, "transcript", &command);
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store.put_batch(batch.clone(), now + 2).unwrap_err().code,
            "source_integrity"
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        let request = handoff(&agent, &["session", "turn", "message"]);
        let ack = store
            .record_source_handoff(request.clone(), now + 2)
            .unwrap()
            .value;
        assert!(
            store.read_source_state(&agent, 1, "stream").unwrap()["families"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let committed = store.put_batch(batch, now + 3).unwrap();
        assert!(committed.ack.first_seq > ack.seq);
        let before = store.get_agent(&agent).unwrap().unwrap();
        let closed = store.close_native_sources(&agent, 1, now + 4).unwrap();
        assert_eq!(closed.len(), 2);
        assert!(closed.iter().all(|event| event.kind == "source.closed"));
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        for source in ["stream", "transcript", "os"] {
            assert_eq!(
                store.read_source_state(&agent, 1, source).unwrap()["state"],
                if source == "os" { "open" } else { "closed" }
            );
        }
        assert_eq!(
            request
                .ack_from_state(&store.read_source_state(&agent, 1, "stream").unwrap())
                .unwrap(),
            ack
        );
        assert!(
            store
                .close_native_sources(&agent, 1, now + 5)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .record_source_handoff(request, now + 5)
                .unwrap_err()
                .code,
            "source_integrity"
        );
    }

    #[test]
    fn existing_closed_successor_and_terminal_agent_are_not_reopened() {
        let (mut store, agent, now) = fixture("fake");
        store.connection.execute(
            "INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (?1,1,'transcript','closed','ok',?2)",
            params![parse_agent_id(&agent).unwrap(), now],
        ).unwrap();
        let before = store.list_source_states(&agent, 1).unwrap();
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_source_handoff(handoff(&agent, &["message"]), now + 1)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(store.list_source_states(&agent, 1).unwrap(), before);
        assert_eq!(store.high_water().unwrap(), high_water);
        store.mark_lost(&agent, 1, now + 1).unwrap();
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_source_handoff(handoff(&agent, &["message"]), now + 2)
                .unwrap_err()
                .code,
            "bad_state"
        );
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn persistence_failure_rolls_back_gap_successor_owners_and_ack() {
        let (mut store, agent, now) = fixture("fake");
        let before = store.read_source_state(&agent, 1, "stream").unwrap();
        let high_water = store.high_water().unwrap();
        store.connection.execute_batch(
            "CREATE TRIGGER reject_handoff BEFORE INSERT ON events WHEN NEW.kind = 'source.handoff'
             BEGIN SELECT RAISE(ABORT, 'synthetic handoff failure'); END;",
        ).unwrap();
        assert_eq!(
            store
                .record_source_handoff(with_gap(handoff(&agent, &["message"])), now + 1)
                .unwrap_err()
                .code,
            "internal"
        );
        assert!(store.is_write_faulted());
        assert_eq!(
            store.read_source_state(&agent, 1, "stream").unwrap(),
            before
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        assert_eq!(store.list_source_states(&agent, 1).unwrap().len(), 1);
        assert_eq!(
            store
                .record_source_handoff(handoff(&agent, &["message"]), now + 2)
                .unwrap_err()
                .code,
            "unavailable"
        );
    }

    #[test]
    fn handoff_preserves_committed_predecessor_progress_and_file_hints() {
        let (mut store, agent, now) = fixture("codex");
        store
            .ensure_git_source(&agent, 1, "synthetic-handoff-workspace", now)
            .unwrap();
        let command = admit_send(&mut store, &agent, now + 1);
        store
            .put_batch(turn_batch(&agent, "stream", &command), now + 1)
            .unwrap();
        store.put_batch(SourceBatch {
            batch_id:"edit".to_owned(), agent_id:agent.clone(), generation:1, source:"stream".to_owned(),
            events:vec![NativeEvent { kind:"tool.completed".to_owned(), native_id:"edit".to_owned(),
                data:json!({"turn_id":"turn-1","tool":"fileChange","status":"completed","changed_paths":["note.txt"]}) }],
            cursor:Some("native:edit".to_owned()),
        }, now + 2).unwrap();
        let before = store.git_hint_page(&agent, 1, 0, 100).unwrap();
        assert_eq!(before.events.len(), 1);
        assert_eq!(store.last_model_progress(&agent, 1).unwrap(), Some(now + 2));
        let mut request = handoff(&agent, &["tool"]);
        request.cursor = Some("native:edit".to_owned());
        request.mechanism = "rollout_tail".to_owned();
        store.record_source_handoff(request, now + 3).unwrap();
        assert_eq!(store.last_model_progress(&agent, 1).unwrap(), Some(now + 2));
        assert_eq!(
            store.git_hint_page(&agent, 1, 0, 100).unwrap().events,
            before.events
        );
        let id = parse_agent_id(&agent).unwrap();
        assert!(pending_git_hints(&store.connection, id, 1, 0).unwrap());
        assert_eq!(
            load_native_git_hint(&store.connection, id, 1, before.events[0].seq as u64)
                .unwrap()
                .native_id,
            before.events[0].native_id
        );
    }

    #[test]
    fn native_exit_gaps_group_current_owners_and_never_gap_a_zero_family_predecessor() {
        for families in [vec!["message"], vec!["session", "turn", "message"]] {
            let (mut store, agent, now) = fixture("fake");
            store
                .record_source_handoff(handoff(&agent, &families), now + 1)
                .unwrap();
            let before = store.get_agent(&agent).unwrap().unwrap();
            let events = store
                .record_native_exit_gaps(&agent, 1, "native_tail_uncovered", now + 2)
                .unwrap();
            let expected = if families.len() == 1 { 2 } else { 1 };
            assert_eq!(events.len(), expected);
            assert!(events.iter().all(|event| event.kind == "source.gap"));
            let successor_gap = events
                .iter()
                .find(|event| event.data["source"] == "transcript")
                .unwrap();
            let mut expected_families = families.clone();
            expected_families.sort_unstable();
            assert_eq!(successor_gap.data["families"], json!(expected_families));
            if families.len() == 1 {
                let predecessor_gap = events
                    .iter()
                    .find(|event| event.data["source"] == "stream")
                    .unwrap();
                assert_eq!(predecessor_gap.data["families"], json!(["session", "turn"]));
            } else {
                assert!(store.read_source_state(&agent, 1, "stream").unwrap()["gap_id"].is_null());
            }
            let high_water = store.high_water().unwrap();
            assert!(
                store
                    .record_native_exit_gaps(&agent, 1, "native_tail_uncovered", now + 3)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(store.high_water().unwrap(), high_water);
            assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
            assert_eq!(
                store
                    .close_native_sources(&agent, 1, now + 3)
                    .unwrap()
                    .len(),
                2
            );
        }
    }

    #[test]
    fn all_native_exit_gaps_roll_back_if_one_source_cannot_commit() {
        let (mut store, agent, now) = fixture("fake");
        store
            .record_source_handoff(handoff(&agent, &["message"]), now + 1)
            .unwrap();
        let before = store.list_source_states(&agent, 1).unwrap();
        let high_water = store.high_water().unwrap();
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER reject_successor_gap BEFORE INSERT ON events
             WHEN NEW.kind = 'source.gap' AND json_extract(NEW.data, '$.source') = 'transcript'
             BEGIN SELECT RAISE(ABORT, 'synthetic successor gap failure'); END;",
            )
            .unwrap();
        assert_eq!(
            store
                .record_native_exit_gaps(&agent, 1, "native_tail_uncovered", now + 2)
                .unwrap_err()
                .code,
            "internal"
        );
        assert!(store.is_write_faulted());
        assert_eq!(store.list_source_states(&agent, 1).unwrap(), before);
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn native_exit_gaps_preserve_current_generation_and_terminal_fact_fences() {
        let (mut store, agent, now) = fixture("fake");
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_native_exit_gaps(&agent, 2, "native_tail_uncovered", now + 1)
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        store.mark_lost(&agent, 1, now + 1).unwrap();
        let before = store.get_agent(&agent).unwrap().unwrap();
        let high_water = store.high_water().unwrap();
        assert!(
            store
                .record_native_exit_gaps(&agent, 1, "native_tail_uncovered", now + 2)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
    }

    #[test]
    fn existing_open_successor_is_reused_and_only_latest_handoff_is_retained() {
        let (mut store, agent, now) = fixture("fake");
        let first = handoff(&agent, &["message"]);
        store.record_source_handoff(first.clone(), now + 1).unwrap();
        let second = handoff(&agent, &["turn"]);
        let committed = store
            .record_source_handoff(second.clone(), now + 2)
            .unwrap();
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].kind, "source.handoff");
        let state = store.read_source_state(&agent, 1, "stream").unwrap();
        assert_eq!(second.ack_from_state(&state).unwrap(), committed.value);
        assert_eq!(first.ack_from_state(&state).unwrap_err().code, "unknown");
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_source_handoff(first, now + 3)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn unimplemented_and_daemon_families_keep_distinct_admission_errors() {
        let (mut store, agent, now) = fixture("fake");
        for (family, code) in [
            ("child", "unsupported"),
            ("goal", "unsupported"),
            ("usage", "unsupported"),
            ("tool", "unsupported"),
            ("process", "not_allowed"),
            ("file", "not_allowed"),
            ("git", "not_allowed"),
            ("hook", "not_allowed"),
        ] {
            let before = store.high_water().unwrap();
            assert_eq!(
                store
                    .record_source_handoff(handoff(&agent, &[family]), now + 1)
                    .unwrap_err()
                    .code,
                code
            );
            assert_eq!(store.high_water().unwrap(), before);
        }
    }

    #[test]
    fn reader_unavailability_gaps_and_stales_without_claiming_reader_or_session_exit() {
        let (mut store, agent, now) = fixture("fake");
        store.ensure_process_source(&agent, 1, now).unwrap();
        store
            .record_source_handoff(
                with_gap(handoff(&agent, &["session", "turn", "message"])),
                now + 1,
            )
            .unwrap();
        let command = admit_send(&mut store, &agent, now + 2);
        store
            .put_batch(turn_batch(&agent, "transcript", &command), now + 3)
            .unwrap();
        let before = store.get_agent(&agent).unwrap().unwrap();
        let command_before = store.get_command(&command).unwrap();
        let token_before: Option<String> = store
            .connection
            .query_row(
                "SELECT token FROM agents WHERE id = ?1",
                [parse_agent_id(&agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        let predecessor = store.read_source_state(&agent, 1, "stream").unwrap();
        let progress_before = store.last_model_progress(&agent, 1).unwrap();
        let events = store
            .record_native_reader_unavailable(&agent, 1, "reader_lost", false, now + 4)
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "source.gap")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "source.health")
                .count(),
            2
        );
        assert!(!events.iter().any(|event| event.kind == "source.closed"
            || event.kind.starts_with("command.")
            || event.kind.starts_with("agent.")
            || event.kind.starts_with("session.")));
        for source in ["stream", "transcript"] {
            let state = store.read_source_state(&agent, 1, source).unwrap();
            assert_eq!(state["state"], "open");
            assert_eq!(state["health"], "stale");
        }
        let after = store.read_source_state(&agent, 1, "stream").unwrap();
        for field in [
            "cursor",
            "gap_id",
            "gap_from",
            "gap_to",
            "gap_seq",
            "gap_digest",
            "last_handoff",
            "last_handoff_digest",
        ] {
            assert_eq!(after[field], predecessor[field]);
        }
        assert_eq!(
            store.read_source_state(&agent, 1, "os").unwrap()["health"],
            "ok"
        );
        assert_eq!(
            store.read_source_state(&agent, 1, "os").unwrap()["state"],
            "open"
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        assert_eq!(store.get_command(&command).unwrap(), command_before);
        let token_after: Option<String> = store
            .connection
            .query_row(
                "SELECT token FROM agents WHERE id = ?1",
                [parse_agent_id(&agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(token_after, token_before);
        assert_eq!(
            store.last_model_progress(&agent, 1).unwrap(),
            progress_before
        );
        let high_water = store.high_water().unwrap();
        assert!(
            store
                .record_native_reader_unavailable(&agent, 1, "reader_lost", false, now + 5)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        let closed = store
            .record_native_reader_unavailable(&agent, 1, "reader_lost", true, now + 6)
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert!(closed.iter().all(|event| event.kind == "source.closed"));
        for source in ["stream", "transcript"] {
            let state = store.read_source_state(&agent, 1, source).unwrap();
            assert_eq!(state["state"], "closed");
            assert_eq!(state["health"], "stale");
        }
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        assert_eq!(store.get_command(&command).unwrap(), command_before);
    }

    #[test]
    fn confirmed_terminal_fact_suppresses_gap_but_not_unavailable_source_metadata() {
        let (mut store, agent, now) = fixture("fake");
        store
            .record_source_handoff(handoff(&agent, &["session", "turn", "message"]), now + 1)
            .unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: "ended".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "transcript".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ended".to_owned(),
                        native_id: "ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: Some("native:ended".to_owned()),
                },
                now + 2,
            )
            .unwrap();
        let before = store.get_agent(&agent).unwrap().unwrap();
        assert_eq!(before.state, AgentState::Stopped);
        let events = store
            .record_native_reader_unavailable(&agent, 1, "reader_lost", true, now + 3)
            .unwrap();
        assert!(
            events
                .iter()
                .all(|event| matches!(event.kind.as_str(), "source.health" | "source.closed"))
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        for source in ["stream", "transcript"] {
            let row = store.read_source_state(&agent, 1, source).unwrap();
            assert_eq!(row["state"], "closed");
            assert_eq!(row["health"], "stale");
            assert!(row["gap_id"].is_null());
        }
    }

    #[test]
    fn retiring_reader_metadata_never_changes_the_new_agent_or_creates_a_gap() {
        let (mut store, agent, now) = fixture("codex");
        store
            .record_source_handoff(handoff(&agent, &["session", "turn", "message"]), now + 1)
            .unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: "ended".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "transcript".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ended".to_owned(),
                        native_id: "ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: Some("native:ended".to_owned()),
                },
                now + 2,
            )
            .unwrap();
        let resume_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: resume_id.clone(),
                    target: Some(agent.clone()),
                    verb: "resume".to_owned(),
                    args: json!({}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                },
                now + 3,
            )
            .unwrap();
        store.dispatch(&resume_id, now + 3).unwrap();
        let before = store.get_agent(&agent).unwrap().unwrap();
        assert_eq!(before.generation, 2);
        let current_sources = store.list_source_states(&agent, 2).unwrap();
        let command_before = store.get_command(&resume_id).unwrap();
        let events = store
            .record_native_reader_unavailable(&agent, 1, "reader_lost", true, now + 4)
            .unwrap();
        assert!(events.iter().all(|event| event.generation == Some(1)
            && matches!(event.kind.as_str(), "source.health" | "source.closed")));
        assert!(events.iter().any(|event| event.kind == "source.health"));
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        assert_eq!(store.get_command(&resume_id).unwrap(), command_before);
        assert_eq!(
            store.list_source_states(&agent, 2).unwrap(),
            current_sources
        );
        for row in store.list_source_states(&agent, 1).unwrap() {
            assert_eq!(row["state"], "closed");
            assert_eq!(row["health"], "stale");
            assert!(row["gap_id"].is_null());
        }
    }

    #[test]
    fn unavailable_metadata_failure_rolls_back_gap_health_and_lifecycle_together() {
        let (mut store, agent, now) = fixture("fake");
        store
            .record_source_handoff(handoff(&agent, &["message"]), now + 1)
            .unwrap();
        let before = store.list_source_states(&agent, 1).unwrap();
        let high_water = store.high_water().unwrap();
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER reject_reader_health BEFORE INSERT ON events
             WHEN NEW.kind = 'source.health' AND json_extract(NEW.data, '$.source') = 'transcript'
             BEGIN SELECT RAISE(ABORT, 'synthetic reader metadata failure'); END;",
            )
            .unwrap();
        assert_eq!(
            store
                .record_native_reader_unavailable(&agent, 1, "reader_lost", true, now + 2)
                .unwrap_err()
                .code,
            "internal"
        );
        assert!(store.is_write_faulted());
        assert_eq!(store.list_source_states(&agent, 1).unwrap(), before);
        assert_eq!(store.high_water().unwrap(), high_water);
    }
}
