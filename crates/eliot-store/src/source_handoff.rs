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

/// The generic native stream family vocabulary, independent of any harness.
pub(super) fn is_native_family(family: &str) -> bool {
    matches!(family, "session" | "turn" | "message" | "tool" | "request")
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
        let result = self.record_source_handoff_inner(handoff, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn record_observed_source_handoff(
        &mut self,
        handoff: SourceHandoff,
        expected_subject: (&str, &str),
        now: i64,
    ) -> Result<Change<SourceHandoffAck>> {
        self.ensure_writer_healthy()?;
        let result = self.record_source_handoff_inner(handoff, Some(expected_subject), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_handoff_inner(
        &mut self,
        mut handoff: SourceHandoff,
        expected_subject: Option<(&str, &str)>,
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
        if let Some(subject) = expected_subject {
            observer::require_live_subject(&tx, &agent, handoff.generation, subject)?;
            if handoff.source != "observe"
                || handoff.successor != "transcript"
                || handoff.mechanism != "rollout_tail"
                || handoff.families.iter().any(|family| {
                    !observer::live_families(&agent.harness)
                        .is_ok_and(|families| families.contains(&family.as_str()))
                })
            {
                return Err(Error::new(
                    "not_allowed",
                    "observer handoff is outside its live-fact grant",
                ));
            }
        } else {
            observer::require_controlled(&agent)?;
        }
        if agent.is_terminal() {
            return Err(Error::new(
                "bad_state",
                "handoff cannot revive a terminal Agent",
            ));
        }
        // Family vocabulary is validated generically above; Committed
        // generation ownership below (predecessor/successor bindings) is the
        // authority for which families this handoff may move.
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
        events.extend(native_unavailable_tx(
            &tx,
            agent_num,
            generation,
            reader_closed,
            now,
        )?);
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

pub(super) fn native_unavailable_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    reader_closed: bool,
    now: i64,
) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    let mut statement = tx
        .prepare(
            "SELECT source, state, health FROM source_state WHERE agent = ?1 AND generation = ?2
             AND source NOT IN ('daemon','os','git','workspace_process') ORDER BY source LIMIT ?3",
        )
        .map_err(sql_error)?;
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
                tx,
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
                tx,
                now,
                "source.closed",
                "source",
                Some(agent_num),
                Some(generation),
                json!({"source":source}),
            )?);
        }
    }
    Ok(events)
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
    let mut groups = std::collections::BTreeMap::<String, Vec<String>>::new();
    for owner in owner_bindings(tx, agent_num, generation)? {
        // Group actual registered native owners by their committed source.
        // Daemon-monitor family owners (process, git, ...) are not native
        // facts and stay out of these gaps.
        if is_native_family(&owner.family) {
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
