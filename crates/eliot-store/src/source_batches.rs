use super::native_events::{persist_effects, verified_native_event_cause_tx};
use super::*;

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
                && workspace::load_workspace(&self.connection, agent_num)?
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
        let result = self.record_source_gap_inner(gap, None, None, now);
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
        let result = self.record_source_gap_inner(gap, Some(expected_subject), None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn record_native_child_source_gap(
        &mut self,
        producer: &str,
        producer_generation: i64,
        gap: SourceGap,
        now: i64,
    ) -> Result<Change<SourceGapAck>> {
        self.ensure_writer_healthy()?;
        let result =
            self.record_source_gap_inner(gap, None, Some((producer, producer_generation)), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_gap_inner(
        &mut self,
        gap: SourceGap,
        expected_subject: Option<(&str, &str)>,
        native_producer: Option<(&str, i64)>,
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
        if let Some((producer, generation)) = native_producer {
            native_children::require_grant(
                &tx,
                producer,
                generation,
                &agent,
                gap.generation,
                &gap.source,
            )?;
            native_children::require_families(&families)?;
        } else if let Some(subject) = expected_subject {
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
        let result = self.put_batch_inner(batch, None, None, now);
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
        let result = self.put_batch_inner(batch, Some(expected_subject), None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn put_native_child_batch(
        &mut self,
        producer: &str,
        producer_generation: i64,
        batch: SourceBatch,
        now: i64,
    ) -> Result<BatchCommit> {
        self.ensure_writer_healthy()?;
        let result = self.put_batch_inner(batch, None, Some((producer, producer_generation)), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn put_batch_inner(
        &mut self,
        batch: SourceBatch,
        expected_subject: Option<(&str, &str)>,
        native_producer: Option<(&str, i64)>,
        now: i64,
    ) -> Result<BatchCommit> {
        let observed = expected_subject.is_some() || native_producer.is_some();
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
        if let Some((producer, generation)) = native_producer {
            native_children::require_grant(
                &tx,
                producer,
                generation,
                &projection,
                batch.generation,
                &batch.source,
            )?;
            let families = batch
                .events
                .iter()
                .map(|event| family_for_kind(&event.kind).map(str::to_owned))
                .collect::<Result<Vec<_>>>()?;
            native_children::require_families(&families)?;
            for event in &batch.events {
                if event.kind == "session.ready"
                    && (event.data.get("native_scope").and_then(Value::as_str)
                        != projection.native_scope.as_deref()
                        || event.data.get("native_ref").and_then(Value::as_str)
                            != projection.native_ref.as_deref())
                {
                    return Err(Error::new(
                        "source_integrity",
                        "native child readiness named another subject",
                    ));
                }
            }
        } else if let Some(subject) = expected_subject {
            observer::require_live_subject(&tx, &projection, batch.generation, subject)?;
            let families = batch
                .events
                .iter()
                .map(|event| family_for_kind(&event.kind).map(str::to_owned))
                .collect::<Result<Vec<_>>>()?;
            observer_live::require_transcript_families(&tx, &projection, &batch.source, &families)?;
        } else {
            observer::require_controlled(&projection)?;
        }
        if observed {
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
        let mut provider_events = Vec::new();
        for (index, native_event) in batch.events.iter().enumerate() {
            if !observed {
                validate_resume_ready_subject_tx(
                    &tx,
                    &projection,
                    agent_num,
                    batch.generation,
                    native_event,
                )?;
            }
            let (next, effects) = if observed {
                observer_live::persist_turn_fact(&tx, &projection, native_event)?;
                (projection.clone(), Vec::new())
            } else {
                core::apply(&projection, native_event, now)?
            };
            let family = family_for_kind(&native_event.kind)?;
            let event_digest = canonical::digest(&native_event.semantic_value())?;
            let cause = if !observed {
                verified_native_event_cause_tx(
                    &tx,
                    &projection,
                    agent_num,
                    batch.generation,
                    native_event,
                    &effects,
                )?
            } else {
                None
            };
            let cursor = if index + 1 == batch.events.len() {
                batch.cursor.as_deref()
            } else {
                None
            };
            let stored = insert_native_event(
                &tx,
                NativeEventInsert {
                    ts: native_event.ts.unwrap_or(now),
                    native_event,
                    family,
                    agent: agent_num,
                    generation: batch.generation,
                    source: &batch.source,
                    cursor,
                    payload_digest: &event_digest,
                    subject: Some(EventSubject::from(&next)),
                    cause: cause.clone(),
                },
            )?;
            let (effects, hold_events) = persist_effects(
                &tx,
                &effects,
                agent_num,
                batch.generation,
                now,
                native_event,
                &next,
            )?;
            command_events.extend(effects);
            if let Some(cause) = cause.as_ref() {
                for mut event in hold_events {
                    annotate_new_event_cause_tx(&tx, &mut event, cause.clone())?;
                    provider_events.push(event);
                }
            } else {
                provider_events.extend(hold_events);
            }
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
            if !observed {
                update_agent_projection(&tx, &next)?;
                if native_event.kind == "session.ended" && next.is_terminal() {
                    capacity::release_proven_generation_tx(&tx, agent_num, batch.generation)?;
                }
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
        for command_event in command_events {
            committed_events.push(insert_daemon_command_event(
                &tx,
                now,
                "command.applied",
                Some(agent_num),
                Some(batch.generation),
                &command_event.command_id,
                command_event.data,
            )?);
        }
        committed_events.extend(provider_events);
        committed_events.sort_by_key(|event| event.seq);
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
