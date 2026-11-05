//! Native-child registration on an already-authenticated producer connection.
//! The one binding retains retry identity and the producer/child association;
//! Agent and source rows remain authoritative for identity and publication.

use super::*;
use eliot_core::{ChildRegistration, ChildRegistrationAck, NATIVE_CHILD_FAMILIES};
use std::collections::BTreeMap;

impl Store {
    /// The host supplies the producer from its private connection, not the wire.
    pub fn register_native_child(
        &mut self,
        producer: &str,
        producer_generation: i64,
        request: ChildRegistration,
        now: i64,
    ) -> Result<Change<ChildRegistrationAck>> {
        self.ensure_writer_healthy()?;
        let result = self.register_native_child_inner(producer, producer_generation, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn register_native_child_inner(
        &mut self,
        producer: &str,
        producer_generation: i64,
        request: ChildRegistration,
        now: i64,
    ) -> Result<Change<ChildRegistrationAck>> {
        request.validate()?;
        let request_value = serde_json::to_value(&request)
            .map_err(|_| Error::new("invalid", "child registration could not be encoded"))?;
        let request_json = encode_json(&request_value)?;
        if request_json.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "child registration exceeds 64 KiB",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let producer_row = require_producer(&tx, producer, producer_generation)?;
        let parent = require_parent(&tx, &producer_row, &request)?;
        observer::check_nonsecret(&tx, &request_value)?;
        let producer_num = parse_agent_id(producer)?;
        let parent_num = parse_agent_id(&parent.id)?;
        let retained: Option<(String, String)> = tx
            .query_row(
                "SELECT request, ack FROM native_child_registrations
             WHERE parent = ?1 AND parent_generation = ?2 AND registration_id = ?3
                 AND producer = ?4 AND producer_generation = ?5",
                params![
                    parent_num,
                    request.parent_generation,
                    request.registration_id,
                    producer_num,
                    producer_generation
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some((retained_request, retained_ack)) = retained {
            if decode_child_registration(&retained_request)? != request {
                return Err(Error::new(
                    "id_conflict",
                    "child registration id changed its binding",
                ));
            }
            let ack: ChildRegistrationAck = decode_child_registration_ack(&retained_ack)?;
            let child = load_agent_by_num(&tx, parse_agent_id(&ack.agent.id)?)?
                .ok_or_else(|| Error::new("source_integrity", "registered child disappeared"))?;
            require_grant(
                &tx,
                producer,
                producer_generation,
                &child,
                child.generation,
                INITIAL_SOURCE,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: ack,
                events: Vec::new(),
            });
        }
        let existing: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE native_scope = ?1 AND native_ref = ?2)",
                params![request.native_scope, request.native_ref],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if existing {
            return Err(Error::new(
                "id_conflict",
                "native child subject is already registered",
            ));
        }
        let root_num = parse_agent_id(&parent.root)?;
        let depth = parent
            .depth
            .checked_add(1)
            .ok_or_else(|| Error::new("invalid", "native child depth overflowed"))?;
        // Existing native work is recorded without controller launch/fanout or
        // capacity admission. Shared process custody stays with the producer.
        db!(tx.execute(
            "INSERT INTO agents(generation, project, harness, profile, profile_snapshot,
                 native_scope, native_ref, state, origin, observe_only, token,
                 parent, root, depth, control, created_at, updated_at)
             VALUES (1, ?1, 'codex', ?2, ?3, ?4, ?5, 'unknown', 'native', 1, NULL,
                 ?6, ?7, ?8, 'self', ?9, ?9)",
            params![
                parent.project,
                parent.profile,
                encode_json(&parent.profile_snapshot)?,
                request.native_scope,
                request.native_ref,
                parent_num,
                root_num,
                depth,
                now
            ],
        ));
        let child_num = tx.last_insert_rowid();
        install_stream_source_plan(&tx, child_num, 1, &NATIVE_CHILD_FAMILIES, now)?;
        let child = load_agent_by_num(&tx, child_num)?
            .ok_or_else(|| Error::new("internal", "new native child disappeared"))?;
        let ack = ChildRegistrationAck {
            registration_id: request.registration_id.clone(),
            agent: child,
            source_plan: NATIVE_CHILD_FAMILIES
                .iter()
                .map(|family| ((*family).to_owned(), INITIAL_SOURCE.to_owned()))
                .collect::<BTreeMap<_, _>>(),
        };
        request.validate_ack(&ack)?;
        let ack_json = encode_json(&ack)?;
        if ack_json.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "child acknowledgement exceeds 900 KiB",
            ));
        }
        db!(tx.execute(
            "INSERT INTO native_child_registrations
                 (producer, producer_generation, parent, parent_generation,
                  registration_id, child, request, ack)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                producer_num,
                producer_generation,
                parent_num,
                request.parent_generation,
                request.registration_id,
                child_num,
                request_json,
                ack_json
            ],
        ));
        let event = insert_daemon_event(
            &tx,
            now,
            "agent.created",
            "agent",
            Some(child_num),
            Some(1),
            json!({"agent_id": ack.agent.id, "origin": "native", "observe_only": true,
                "own_identity": false, "parent": ack.agent.parent,
                "root": ack.agent.root, "depth": ack.agent.depth}),
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: ack,
            events: vec![event],
        })
    }

    pub fn read_native_child_source_state(
        &self,
        producer: &str,
        producer_generation: i64,
        child: &str,
        generation: i64,
        source: &str,
    ) -> Result<Value> {
        let child_row = self
            .get_agent(child)?
            .ok_or_else(|| Error::new("not_found", "native child was not found"))?;
        require_grant(
            &self.connection,
            producer,
            producer_generation,
            &child_row,
            generation,
            source,
        )?;
        self.read_source_state(child, generation, source)
    }

    /// Retire observation supplied by one exact private producer connection.
    /// The host calls this after draining on producer loss/stop, never merely
    /// because the parent's turn completed. A retained generation can clean up
    /// after its producer became terminal or advanced; this grants no publication.
    pub fn mark_native_children_unavailable(
        &mut self,
        producer: &str,
        producer_generation: i64,
        reason: &str,
        reader_closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_nonempty(reason, "native child unavailable reason")?;
            let producer_num = parse_agent_id(producer)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = load_agent_by_num(&tx, producer_num)?
                .ok_or_else(|| Error::new("not_found", "native child producer was not found"))?;
            if producer_generation <= 0 || producer_generation > current.generation {
                return Err(Error::new(
                    "stale_generation",
                    "native child cleanup producer generation does not exist",
                ));
            }
            if current.harness != "codex" {
                return Err(Error::new(
                    "not_allowed",
                    "native child producer is not Codex",
                ));
            }
            observer::check_nonsecret(&tx, &json!(reason))?;
            let registrations = {
                let mut statement = tx
                    .prepare(
                        "SELECT child, ack FROM native_child_registrations
                     WHERE producer = ?1 AND producer_generation = ?2 ORDER BY child",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(params![producer_num, producer_generation], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(sql_error)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            let mut events = Vec::new();
            for (child_num, ack) in registrations {
                mark_one_native_child_unavailable_tx(&tx, (producer, producer_generation), child_num, &ack, reason, reader_closed, now, &mut events)?;
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub(crate) fn recover_lost_native_children_tx(tx: &Transaction<'_>, now: i64, events: &mut Vec<Event>) -> Result<()> {
        let keys = {
            let mut statement = tx.prepare("SELECT DISTINCT producer, producer_generation FROM native_child_registrations ORDER BY producer, producer_generation").map_err(sql_error)?;
            let rows = statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).map_err(sql_error)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_error)?
        };
        for (producer_num, producer_generation) in keys {
            let producer = format_agent_id(producer_num);
            let registrations = {
                let mut statement = tx.prepare("SELECT child, ack FROM native_child_registrations WHERE producer = ?1 AND producer_generation = ?2 ORDER BY child").map_err(sql_error)?;
                let rows = statement.query_map(params![producer_num, producer_generation], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))).map_err(sql_error)?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_error)?
            };
            for (child_num, ack) in registrations {
                mark_one_native_child_unavailable_tx(tx, (&producer, producer_generation), child_num, &ack, "daemon_restart", false, now, events)?;
            }
        }
        Ok(())
    }
}

fn mark_one_native_child_unavailable_tx(tx: &Transaction<'_>, producer: (&str, i64), child_num: i64, ack_json: &str, reason: &str, reader_closed: bool, now: i64, events: &mut Vec<Event>) -> Result<()> {
    let (producer, producer_generation) = producer;
    let ack: ChildRegistrationAck = decode_child_registration_ack(ack_json)?;
    let child = load_agent_by_num(tx, child_num)?.ok_or_else(|| Error::new("source_integrity", "registered native child disappeared"))?;
    if parse_agent_id(&ack.agent.id)? != child_num || ack.agent.generation <= 0 || ack.agent.generation > child.generation || child.origin != core::AgentOrigin::Native || !child.observe_only || child.own_identity {
        return Err(Error::new("source_integrity", "native child cleanup binding changed"));
    }
    let generation = ack.agent.generation;
    record_child_unavailable_gap_tx(tx, (producer, producer_generation), (child_num, generation), reason, now, events)?;
    let running = {
        let mut statement = tx.prepare("SELECT turn_id FROM turns WHERE agent = ?1 AND generation = ?2 AND state = 'running' ORDER BY turn_id").map_err(sql_error)?;
        let rows = statement.query_map(params![child_num, generation], |row| row.get::<_, String>(0)).map_err(sql_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_error)?
    };
    for turn_id in running {
        let changed = db!(tx.execute("UPDATE turns SET state = 'unknown' WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3 AND state = 'running'", params![child_num, generation, turn_id]));
        if changed != 1 {
            return Err(Error::new("source_integrity", "native child running turn changed during unavailable commit"));
        }
        events.push(insert_daemon_event(tx, now, "turn.unknown", "turn", Some(child_num), Some(generation), json!({"turn_id":turn_id,"reason":reason}))?);
    }
    events.extend(source_handoff::native_unavailable_tx(tx, child_num, generation, reader_closed, now)?);
    Ok(())
}

fn record_child_unavailable_gap_tx(
    tx: &Transaction<'_>,
    producer: (&str, i64),
    child: (i64, i64),
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let (producer, producer_generation) = producer;
    let (child, generation) = child;
    let state = load_source_state(tx, child, generation, INITIAL_SOURCE)?
        .ok_or_else(|| Error::new("source_integrity", "registered child stream disappeared"))?;
    let gap_id = format!(
        "native-child-producer-unavailable:{producer}:{producer_generation}:{child}:{generation}"
    );
    if state.gap_id.as_deref() == Some(gap_id.as_str()) {
        if state.gap_seq.is_none_or(|seq| seq <= 0) || state.gap_digest.is_none() {
            return Err(Error::new(
                "source_integrity",
                "native child unavailable gap acknowledgement is missing",
            ));
        }
        // The first loss reason/from boundary stays canonical when an unknown
        // reader later becomes confirmed closed, even if its diagnostic changes.
        return Ok(());
    }
    let mut families = NATIVE_CHILD_FAMILIES;
    families.sort_unstable();
    let data = json!({
        "gap_id":gap_id, "agent_id":format_agent_id(child), "generation":generation,
        "source":INITIAL_SOURCE, "families":families, "reason":reason,
        "from":state.cursor, "to":null,
    });
    if encode_json(&data)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "native child unavailable gap exceeds 64 KiB",
        ));
    }
    let digest = canonical::digest(&data)?;
    let event = insert_daemon_event(
        tx,
        now,
        "source.gap",
        "source",
        Some(child),
        Some(generation),
        data,
    )?;
    let changed = db!(tx.execute(
        "UPDATE source_state SET gap_id = ?1, gap_digest = ?2, gap_from = ?3,
             gap_to = NULL, gap_seq = ?4, updated_at = ?5
         WHERE agent = ?6 AND generation = ?7 AND source = ?8",
        params![
            gap_id,
            digest,
            state.cursor,
            event.seq,
            now,
            child,
            generation,
            INITIAL_SOURCE
        ],
    ));
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "native child stream changed during unavailable commit",
        ));
    }
    events.push(event);
    Ok(())
}

fn require_producer(connection: &Connection, producer: &str, generation: i64) -> Result<Agent> {
    let row = load_agent_by_num(connection, parse_agent_id(producer)?)?
        .ok_or_else(|| Error::new("not_found", "native child producer was not found"))?;
    if generation <= 0 || row.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "native child producer generation changed",
        ));
    }
    observer::require_controlled(&row)?;
    if row.harness != "codex"
        || !row.own_identity
        || row.is_terminal()
        || row.native_scope.is_none()
        || row.native_ref.is_none()
        || load_source_state(
            connection,
            parse_agent_id(producer)?,
            generation,
            INITIAL_SOURCE,
        )?
        .is_none_or(|state| state.state != "open")
    {
        return Err(Error::new(
            "not_allowed",
            "native child requires a live bound Codex producer",
        ));
    }
    Ok(row)
}

fn require_parent(
    connection: &Connection,
    producer: &Agent,
    request: &ChildRegistration,
) -> Result<Agent> {
    let parent = load_agent_by_num(connection, parse_agent_id(&request.parent_id)?)?
        .ok_or_else(|| Error::new("not_found", "native child's immediate parent was not found"))?;
    if parent.generation != request.parent_generation {
        return Err(Error::new(
            "stale_generation",
            "native child's immediate parent generation changed",
        ));
    }
    if parent.native_scope.as_deref() != Some(request.native_scope.as_str())
        || producer.native_scope.as_deref() != Some(request.native_scope.as_str())
        || parent.native_ref.as_deref() != Some(request.parent_native_ref.as_str())
        || parent.harness != "codex"
        || parent.root != producer.root
        || request.native_ref == producer.native_ref.as_deref().unwrap_or_default()
    {
        return Err(Error::new(
            "source_integrity",
            "native child immediate-parent provenance differs",
        ));
    }
    if parent.id != producer.id {
        let binding = registration_for_child(connection, producer, &parent)?;
        require_child_identity(connection, &parent, &binding)?;
    }
    Ok(parent)
}

fn registration_for_child(
    connection: &Connection,
    producer: &Agent,
    child: &Agent,
) -> Result<ChildRegistration> {
    let request: Option<String> = connection
        .query_row(
            "SELECT request FROM native_child_registrations
         WHERE producer = ?1 AND producer_generation = ?2 AND child = ?3",
            params![
                parse_agent_id(&producer.id)?,
                producer.generation,
                parse_agent_id(&child.id)?
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let request = request.ok_or_else(|| {
        Error::new(
            "not_allowed",
            "child is not bound to this producer generation",
        )
    })?;
    decode_child_registration(&request)
}

fn require_child_identity(
    connection: &Connection,
    child: &Agent,
    request: &ChildRegistration,
) -> Result<()> {
    let parent = load_agent_by_num(connection, parse_agent_id(&request.parent_id)?)?
        .ok_or_else(|| Error::new("source_integrity", "registered native parent disappeared"))?;
    if parent.generation != request.parent_generation {
        return Err(Error::new(
            "stale_generation",
            "registered native parent generation changed",
        ));
    }
    if child.origin != core::AgentOrigin::Native
        || !child.observe_only
        || child.own_identity
        || child.generation != 1
        || child.state != AgentState::Unknown
        || child.control != AgentControl::Slf
        || child.harness != "codex"
        || child.native_scope.as_deref() != Some(request.native_scope.as_str())
        || child.native_ref.as_deref() != Some(request.native_ref.as_str())
        || child.parent.as_deref() != Some(parent.id.as_str())
        || child.root != parent.root
        || parent.depth.checked_add(1) != Some(child.depth)
        || parent.native_scope.as_deref() != Some(request.native_scope.as_str())
        || parent.native_ref.as_deref() != Some(request.parent_native_ref.as_str())
    {
        return Err(Error::new(
            "source_integrity",
            "registered native child identity changed",
        ));
    }
    Ok(())
}

pub(super) fn require_grant(
    connection: &Connection,
    producer: &str,
    producer_generation: i64,
    child: &Agent,
    generation: i64,
    source: &str,
) -> Result<()> {
    let producer = require_producer(connection, producer, producer_generation)?;
    if child.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "native child source generation changed",
        ));
    }
    let request = registration_for_child(connection, &producer, child)?;
    require_parent(connection, &producer, &request)?;
    require_child_identity(connection, child, &request)?;
    if source != INITIAL_SOURCE {
        return Err(Error::new(
            "not_allowed",
            "native child only publishes its registered stream",
        ));
    }
    let mut owners = connection.prepare(
        "SELECT family, source, mechanism FROM source_owners WHERE agent = ?1 AND generation = ?2",
    ).map_err(sql_error)?;
    let owners = owners
        .query_map(params![parse_agent_id(&child.id)?, generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    if owners.len() != NATIVE_CHILD_FAMILIES.len()
        || owners.iter().any(|(family, owner, mechanism)| {
            !NATIVE_CHILD_FAMILIES.contains(&family.as_str())
                || owner != INITIAL_SOURCE
                || mechanism != "stream"
        })
    {
        return Err(Error::new(
            "source_integrity",
            "native child source plan changed",
        ));
    }
    Ok(())
}

pub(super) fn require_families(families: &[String]) -> Result<()> {
    if families
        .iter()
        .any(|family| !NATIVE_CHILD_FAMILIES.contains(&family.as_str()))
    {
        return Err(Error::new(
            "not_allowed",
            "family is outside the native child observation grant",
        ));
    }
    Ok(())
}

fn decode_child_registration(value: &str) -> Result<ChildRegistration> {
    serde_json::from_str(value).map_err(|error| Error::new("internal", error.to_string()))
}

fn decode_child_registration_ack(value: &str) -> Result<ChildRegistrationAck> {
    serde_json::from_str(value).map_err(|error| Error::new("internal", error.to_string()))
}

#[cfg(test)]
mod tests;
