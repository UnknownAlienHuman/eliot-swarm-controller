use super::native_events::{persist_effects, verified_native_event_cause_tx};
use super::*;
use std::borrow::Cow;
/// Daemon-derived worktree facts attached only after the native source batch
/// digest has been checked and the exact active turn is identified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionWorktreeCapture {
    pub base: String,
    pub fingerprint: String,
    pub files: u64,
    pub lines: u64,
}

impl CompletionWorktreeCapture {
    fn validate(&self) -> Result<()> {
        if !is_git_oid(&self.base)
            || self.fingerprint.len() != 64
            || !self
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::new(
                "invalid",
                "completion worktree capture is invalid",
            ));
        }
        Ok(())
    }
}

impl Store {
    /// Validate an exact stream sample. Active-turn samples remain in the
    /// Monitor cache; only an idle sample can refresh the retained projection.
    pub fn put_session_usage_sample(
        &mut self,
        agent: &str,
        generation: i64,
        turn_id: Option<&str>,
        mut usage: core::SessionUsage,
        now: i64,
    ) -> Result<core::SessionUsage> {
        self.ensure_committed_reads()?;
        if generation < 1 || now < 0 {
            return Err(Error::new(
                "invalid",
                "invalid session usage sample binding",
            ));
        }
        usage.validate()?;
        usage.retain_valid_context();
        usage.observed_at.get_or_insert(now);
        let agent_num = parse_agent_id(agent)?;
        let previous = load_agent_by_num(&self.connection, agent_num)?
            .ok_or_else(|| Error::new("not_found", "session usage Agent was not found"))?;
        if previous.generation != generation || previous.is_terminal() {
            return Err(Error::new(
                "stale_generation",
                "session usage generation is not live",
            ));
        }
        if previous.active_turn.as_deref() != turn_id {
            return Err(Error::new(
                "stale_turn",
                "session usage sample does not match the active turn",
            ));
        }
        let stream_owns_session: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(
                SELECT 1 FROM source_state s
                JOIN source_owners o ON o.agent = s.agent AND o.generation = s.generation
                WHERE s.agent = ?1 AND s.generation = ?2 AND s.source = 'stream'
                  AND s.state = 'open' AND o.family = 'session' AND o.source = 'stream'
             )",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !stream_owns_session {
            return Err(Error::new(
                "source_integrity",
                "session usage sample has no open stream binding",
            ));
        }
        if turn_id.is_some() {
            return Ok(usage);
        }

        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            native_events::persist_session_usage_snapshot(&tx, agent_num, generation, now, &usage)?;
            commit_transaction(tx, &self.write_faulted)?;
            self.get_session_usage(agent, generation)?
                .ok_or_else(|| Error::new("internal", "session usage sample was not retained"))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn latest_stream_gap(&self, agent: &str, generation: i64) -> Result<Option<Event>> {
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT e.seq, e.ts, e.kind, e.agent, e.generation, e.source, e.native_id,
                        e.data, e.subject, e.cause
                 FROM source_state s JOIN events e ON e.seq = s.gap_seq
                 WHERE s.agent = ?1 AND s.generation = ?2 AND s.source = 'stream'
                   AND e.agent = s.agent AND e.generation = s.generation AND e.kind = 'source.gap'",
                params![agent_num, generation],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?
            .map(event_from_record)
            .transpose()
    }

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
        let diagnostic_counts = if generation == current.generation {
            load_native_diagnostic_counts(&self.connection, &current)?
        } else {
            None
        };
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
                "native_diagnostics": if matches!(source.as_str(), "stream" | "observe" | "transcript") {
                    diagnostic_counts.as_ref()
                } else { None },
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
        let (observer_ready, diagnostic_counts) =
            match load_agent_by_num(&self.connection, agent_num)? {
                Some(agent) => (
                    observer::live_ready(&self.connection, &agent, generation)?,
                    load_native_diagnostic_counts(&self.connection, &agent)?,
                ),
                None => (false, None),
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
            "native_diagnostics": diagnostic_counts,
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
        let result = self.record_source_gap_inner(gap, None, None, false, now);
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
        let result = self.record_source_gap_inner(gap, Some(expected_subject), None, false, now);
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
        let result = self.record_source_gap_inner(
            gap,
            None,
            Some((producer, producer_generation)),
            false,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    /// Record the coverage lost by a rejected event write and mark the source stale.
    /// If the rejected batch identifies no owned family, mark the whole source.
    pub fn record_rejected_batch_gap(
        &mut self,
        gap: SourceGap,
        producer: (&str, i64),
        expected_subject: Option<(&str, &str)>,
        now: i64,
    ) -> Result<Change<SourceGapAck>> {
        self.ensure_writer_healthy()?;
        let native_producer =
            (producer != (gap.agent_id.as_str(), gap.generation)).then_some(producer);
        if native_producer.is_some() && expected_subject.is_some() {
            return Err(Error::new(
                "not_allowed",
                "observer cannot record native child gaps",
            ));
        }
        let result =
            self.record_source_gap_inner(gap, expected_subject, native_producer, true, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_source_gap_inner(
        &mut self,
        gap: SourceGap,
        expected_subject: Option<(&str, &str)>,
        native_producer: Option<(&str, i64)>,
        mark_stale: bool,
        now: i64,
    ) -> Result<Change<SourceGapAck>> {
        require_nonempty(&gap.gap_id, "gap_id")?;
        require_nonempty(&gap.agent_id, "agent_id")?;
        require_nonempty(&gap.source, "source")?;
        require_nonempty(&gap.reason, "reason")?;
        if gap.families.is_empty() && !mark_stale {
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
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if mark_stale {
            let owned: Vec<_> = source_handoff::owner_bindings(&tx, agent_num, gap.generation)?
                .into_iter()
                .filter(|owner| owner.source == gap.source)
                .map(|owner| owner.family)
                .collect();
            families.retain(|family| owned.contains(family));
            if families.is_empty() {
                if owned.is_empty() {
                    return Err(Error::new("source_integrity", "source owns no families"));
                }
                families = owned;
            }
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
        let mut events = vec![event];
        if mark_stale && source_state.health != "stale" {
            events.push(observer_live::update_open_source_health(
                &tx,
                agent_num,
                gap.generation,
                &gap.source,
                "stale",
                now,
            )?);
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value: ack, events })
    }

    /// Commit one complete native batch, its reducer projections, derived
    /// command/turn facts, source cursor, and last-batch acknowledgement.
    pub fn put_batch(&mut self, batch: SourceBatch, now: i64) -> Result<BatchCommit> {
        self.ensure_writer_healthy()?;
        let result = self.put_batch_inner(batch, None, None, None, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn put_batch_with_completion_capture(
        &mut self,
        batch: SourceBatch,
        worktree: Option<CompletionWorktreeCapture>,
        project_check: Option<bool>,
        now: i64,
    ) -> Result<BatchCommit> {
        self.ensure_writer_healthy()?;
        let result = self.put_batch_inner(batch, None, None, worktree, project_check, now);
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
        let result = self.put_batch_inner(batch, Some(expected_subject), None, None, None, now);
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
        let result = self.put_batch_inner(
            batch,
            None,
            Some((producer, producer_generation)),
            None,
            None,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn put_batch_inner(
        &mut self,
        mut batch: SourceBatch,
        expected_subject: Option<(&str, &str)>,
        native_producer: Option<(&str, i64)>,
        completion_worktree: Option<CompletionWorktreeCapture>,
        project_check: Option<bool>,
        now: i64,
    ) -> Result<BatchCommit> {
        let observed = expected_subject.is_some() || native_producer.is_some();
        if let Some(worktree) = completion_worktree.as_ref() {
            worktree.validate()?;
        }
        if (completion_worktree.is_some() || project_check.is_some())
            && (observed || batch.source != "stream")
        {
            return Err(Error::new(
                "not_allowed",
                "completion worktree capture requires the native stream source",
            ));
        }
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
        let event_payload_digests = batch
            .events
            .iter()
            .map(|event| canonical::digest(&event.semantic_value()))
            .collect::<Result<Vec<_>>>()?;
        let batch_digest = batch
            .semantic_digest()
            .map_err(|error| Error::new("invalid", error.to_string()))?
            .to_vec();
        for event in batch
            .events
            .iter_mut()
            .filter(|event| event.kind == "turn.completed")
        {
            if let Some(data) = event.data.as_object_mut() {
                data.remove("worktree");
                data.remove("project_check");
            }
        }
        let agent_num = parse_agent_id(&batch.agent_id)?;
        let mut native_ids = HashSet::with_capacity(batch.events.len());
        let mut publication = Vec::with_capacity(batch.events.len());
        for event in &batch.events {
            require_nonempty(&event.kind, "event kind")?;
            require_nonempty(&event.native_id, "native_id")?;
            if event.kind == core::UNCLASSIFIED_NATIVE_EVENT {
                core::validate_unclassified_native_data(&event.data)?;
            }
            if !native_ids.insert(event.native_id.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    format!(
                        "native id {} occurs more than once in the batch",
                        event.native_id
                    ),
                ));
            }
            let mut published = Cow::Borrowed(event);
            if encode_json(&event.data)?.len() > MAX_EVENT_BYTES {
                if event.kind == "turn.completed" && event.data["final_text"].is_string() {
                    core::limits::bound_terminal_text(
                        &mut published.to_mut().data,
                        MAX_EVENT_BYTES,
                    );
                }
                if matches!(published, Cow::Borrowed(_))
                    || encode_json(&published.data)?.len() > MAX_EVENT_BYTES
                {
                    return Err(Error::new(
                        "frame_too_large",
                        format!("event {} data exceeds 64 KiB", event.native_id),
                    ));
                }
            }
            publication.push(published);
        }

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
                .map(|event| {
                    let family = family_for_kind(&event.kind)?;
                    if family == "session" && event.kind != core::UNCLASSIFIED_NATIVE_EVENT {
                        return Err(Error::new(
                            "not_allowed",
                            "observer session facts are limited to unclassified diagnostics",
                        ));
                    }
                    Ok(family.to_owned())
                })
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
                let mut native_wake_cancel = read_native_wake_cancellation(
                    &tx,
                    agent_num,
                    batch.generation,
                    source.last_batch_first_seq.unwrap_or_default(),
                    source.last_batch_last_seq.unwrap_or_default(),
                )?;
                let mut events = Vec::new();
                if native_wake_cancel.as_ref().is_some_and(|cancel| {
                    projection.active_turn.as_deref() != Some(cancel.turn_id.as_str())
                }) && let Some(cancel) = native_wake_cancel.take()
                {
                    let already_recorded: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM events INDEXED BY events_agent
                         WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.native_wake_cancel_dropped'
                           AND json_extract(data, '$.turn_id') = ?3)",
                        params![agent_num, batch.generation, cancel.turn_id],
                        |row| row.get(0),
                    ).map_err(sql_error)?;
                    if !already_recorded {
                        events.push(insert_daemon_event(
                            &tx,
                            now,
                            "turn.native_wake_cancel_dropped",
                            "turn",
                            Some(agent_num),
                            Some(batch.generation),
                            json!({"turn_id": cancel.turn_id, "reason": "wake_no_longer_active",
                                "cancel_reason": cancel.reason}),
                        )?);
                    }
                }
                let ack = BatchAck {
                    first_seq: source.last_batch_first_seq.ok_or_else(|| {
                        Error::new("internal", "source batch ack is missing its first sequence")
                    })?,
                    last_seq: source.last_batch_last_seq.ok_or_else(|| {
                        Error::new("internal", "source batch ack is missing its last sequence")
                    })?,
                    cursor: source.cursor,
                    dropped_unclassified: source.last_batch_dropped,
                    permission_refusal: None,
                    native_wake_cancel,
                };
                commit_source_batch(tx, &self.write_faulted)?;
                return Ok(BatchCommit { ack, events });
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

        {
            let mut source_owner = tx
                .prepare_cached(
                    "SELECT source FROM source_owners
                     WHERE agent = ?1 AND generation = ?2 AND family = ?3",
                )
                .map_err(sql_error)?;
            let mut native_id_exists = tx
                .prepare_cached(
                    "SELECT EXISTS(
                         SELECT 1 FROM events
                         WHERE agent = ?1 AND generation = ?2 AND source = ?3 AND native_id = ?4
                     )",
                )
                .map_err(sql_error)?;
            for event in &batch.events {
                if event.data.get("native_wake_cancel").is_some() {
                    return Err(Error::new(
                        "invalid",
                        "native wake cancellation is assigned by Store",
                    ));
                }
                let family = family_for_kind(&event.kind)?;
                let owner: Option<String> = source_owner
                    .query_row(params![agent_num, batch.generation, family], |row| {
                        row.get(0)
                    })
                    .optional()
                    .map_err(sql_error)?;
                if owner.as_deref() != Some(batch.source.as_str()) {
                    return Err(Error::new(
                        "source_integrity",
                        format!("source {} does not own family {family}", batch.source),
                    ));
                }
                let exists: bool = native_id_exists
                    .query_row(
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
        }

        let mut committed_events = Vec::with_capacity(batch.events.len());
        let mut command_events = Vec::new();
        let mut provider_events = Vec::new();
        let mut dropped_unclassified = 0;
        for (index, (source_event, published)) in batch.events.iter().zip(&publication).enumerate()
        {
            if source_event.kind == core::UNCLASSIFIED_NATIVE_EVENT
                && !admit_native_diagnostic(&tx, &projection)?
            {
                dropped_unclassified += 1;
                continue;
            }
            let mut enriched = None;
            if !observed
                && batch.source == "stream"
                && source_event.kind == "turn.started"
                && source_event.data["native_trigger"].is_string()
                && source_event.data["command_id"].is_null()
                && let Some(turn_id) = source_event.data["turn_id"].as_str()
                && let Some(cancellation) =
                    native_events::native_wake_cancellation(&tx, agent_num, &projection, turn_id)?
            {
                let mut event = published.as_ref().clone();
                event.data["native_wake_cancel"] = json!(cancellation);
                enriched = Some(event);
            }
            if !observed
                && batch.source == "stream"
                && source_event.kind == "turn.completed"
                && projection.active_turn.as_deref()
                    == source_event.data.get("turn_id").and_then(Value::as_str)
                && (completion_worktree.is_some() || project_check.is_some())
            {
                let mut event = published.as_ref().clone();
                if let Some(data) = event.data.as_object_mut() {
                    if let Some(worktree) = completion_worktree.as_ref() {
                        data.insert(
                            "worktree".to_owned(),
                            json!({
                                "base": &worktree.base,
                                "fingerprint": &worktree.fingerprint,
                                "files": worktree.files,
                                "lines": worktree.lines,
                            }),
                        );
                    }
                    if let Some(required) = project_check {
                        data.insert(
                            "project_check".to_owned(),
                            json!(if required { "required" } else { "off" }),
                        );
                    }

                    core::limits::bound_terminal_text(&mut event.data, MAX_EVENT_BYTES);
                }
                let mut data_bytes = encode_json(&event.data)?.len();
                if data_bytes > MAX_EVENT_BYTES {
                    if let Some(data) = event.data.as_object_mut() {
                        data.remove("worktree");
                    }
                    core::limits::bound_terminal_text(&mut event.data, MAX_EVENT_BYTES);
                    data_bytes = encode_json(&event.data)?.len();
                }
                if data_bytes > MAX_EVENT_BYTES
                    && let Some(data) = event.data.as_object_mut()
                {
                    data.remove("project_check");
                }
                enriched = Some(event);
            }
            let native_event = enriched.as_ref().unwrap_or_else(|| published.as_ref());
            if !observed {
                validate_resume_ready_subject_tx(
                    &tx,
                    &projection,
                    agent_num,
                    batch.generation,
                    native_event,
                )?;
            }
            // The adapter's own terminal for a turn recovery settled
            // `unknown` is evidence: it resolves that turn instead of being
            // refused as inactive.
            let resolves_recovered = !observed
                && turn_recovery::resolves_recovered_turn_tx(
                    &tx,
                    &projection,
                    agent_num,
                    batch.generation,
                    native_event,
                )?;
            let (mut next, effects) = if observed {
                observer_live::persist_turn_fact(&tx, &projection, native_event)?;
                (projection.clone(), Vec::new())
            } else if resolves_recovered
                && projection.active_turn.as_deref() != native_event.data["turn_id"].as_str()
            {
                (
                    projection.clone(),
                    vec![core::resolve_recovered_turn(
                        &projection,
                        native_event,
                        now,
                    )?],
                )
            } else {
                core::apply(&projection, native_event, now)?
            };
            let family = family_for_kind(&native_event.kind)?;
            // Deduplication still binds the exact redacted source payload.
            let event_digest = &event_payload_digests[index];
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
                    payload_digest: event_digest,
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
            // The inserted turn binds the exact config revision that Muse
            // admitted. Use it even if a newer overlay exists on the Agent.
            if !observed
                && next.harness == "muse"
                && native_event.kind == "turn.started"
                && let Some(turn) = next.active_turn.as_deref()
            {
                let selected_model: Option<String> = tx
                    .query_row(
                        "SELECT json_extract(config.effective, '$.model')
                         FROM turns AS turn JOIN agent_config AS config ON
                           config.agent = turn.agent AND config.generation = turn.generation
                           AND config.revision = turn.config_revision
                         WHERE turn.agent = ?1 AND turn.generation = ?2 AND turn.turn_id = ?3",
                        params![agent_num, batch.generation, turn],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?
                    .flatten();
                if let Some(model) = selected_model {
                    next.set_admitted_model(model);
                }
            }
            command_events.extend(effects);
            if resolves_recovered {
                provider_events.push(turn_recovery::insert_native_resolution_tx(
                    &tx,
                    agent_num,
                    batch.generation,
                    native_event,
                    stored.seq,
                    cause.clone(),
                    now,
                )?);
            }
            if let Some(cause) = cause.as_ref() {
                for mut event in hold_events {
                    annotate_new_event_cause_tx(&tx, &mut event, cause.clone())?;
                    provider_events.push(event);
                }
            } else {
                provider_events.extend(hold_events);
            }
            if next.is_terminal() {
                command_events.extend(terminalize_generation_commands(
                    &tx,
                    agent_num,
                    batch.generation,
                    now,
                    "session_ended",
                    native_event.kind == "session.ended",
                )?);
                db!(tx.execute(
                    "UPDATE pending_requests SET state = 'stale'
                     WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
                    params![agent_num, batch.generation],
                ));
            }
            if !observed {
                update_agent_projection(&tx, &next)?;
                if !resolves_recovered && batch.source == "stream" {
                    switch::reserve_provider_switch_tx(
                        &tx,
                        &next,
                        native_event,
                        now,
                        &mut provider_events,
                    )?;
                }
            }
            projection = next;
            committed_events.push(stored);
        }

        let first_seq = committed_events.first().map_or(0, |event| event.seq);
        let last_seq = committed_events.last().map_or(0, |event| event.seq);
        // Keep native batch ACKs scoped to native rows; the associated command
        // event follows those rows but commits in the same transaction.
        for command_event in command_events {
            committed_events.push(insert_daemon_command_event(
                &tx,
                now,
                &command_event.kind(),
                Some(agent_num),
                Some(batch.generation),
                &command_event.command_id,
                command_event.data,
            )?);
        }
        committed_events.extend(provider_events);
        if !observed {
            if projection.state == AgentState::Stopped {
                goals::pause_for_stop_tx(
                    &tx,
                    agent_num,
                    batch.generation,
                    now,
                    &mut committed_events,
                )?;
            }
            if projection.is_terminal() {
                native_children::end_native_children_with_parent_tx(
                    &tx,
                    Some((agent_num, batch.generation)),
                    now,
                    &mut committed_events,
                )?;
            } else if batch
                .events
                .iter()
                .any(|event| matches!(event.kind.as_str(), "session.ready" | "turn.completed"))
            {
                goals::continue_stop_paused_goal_tx(&tx, agent_num, now, &mut committed_events)?;
            }
        }
        committed_events.sort_by_key(|event| event.seq);
        let source_state = if projection.is_terminal() {
            "closed"
        } else {
            "open"
        };
        db!(tx.execute(
            "UPDATE source_state
             SET cursor = ?1, last_store_seq = COALESCE(NULLIF(?2, 0), last_store_seq), last_batch_id = ?3,
                 last_batch_digest = ?4, last_batch_first_seq = ?5,
                 last_batch_last_seq = ?6, state = ?7, updated_at = ?8,
                 last_batch_dropped = ?12
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
                dropped_unclassified,
            ],
        ));
        let native_wake_cancel =
            read_native_wake_cancellation(&tx, agent_num, batch.generation, first_seq, last_seq)?;
        commit_source_batch(tx, &self.write_faulted)?;
        Ok(BatchCommit {
            ack: BatchAck {
                first_seq,
                last_seq,
                cursor: batch.cursor,
                dropped_unclassified,
                permission_refusal: None,
                native_wake_cancel,
            },
            events: committed_events,
        })
    }
}

fn read_native_wake_cancellation(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    first_seq: i64,
    last_seq: i64,
) -> Result<Option<core::NativeWakeCancellation>> {
    tx.query_row(
        "SELECT json_extract(data, '$.native_wake_cancel') FROM events
         WHERE seq BETWEEN ?1 AND ?2 AND agent = ?3 AND generation = ?4
           AND source = 'stream' AND kind = 'turn.started'
           AND json_type(data, '$.native_wake_cancel') = 'object'
         ORDER BY seq DESC LIMIT 1",
        params![first_seq, last_seq, agent_num, generation],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(sql_error)?
    .map(|raw| {
        serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "source_integrity",
                "retained native wake cancellation is invalid",
            )
        })
    })
    .transpose()
}

fn admit_native_diagnostic(tx: &Transaction<'_>, agent: &Agent) -> Result<bool> {
    let (Some(scope), Some(native_ref)) =
        (agent.native_scope.as_deref(), agent.native_ref.as_deref())
    else {
        // No subject binding exists yet. A diagnostic cannot establish one.
        return Ok(false);
    };
    let admitted = tx.execute(
        "INSERT INTO native_session_unclassified(harness, native_scope, native_ref, admitted, dropped)
         VALUES (?1, ?2, ?3, 1, 0)
         ON CONFLICT(harness, native_scope, native_ref) DO UPDATE SET admitted = admitted + 1
         WHERE admitted < ?4",
        params![agent.harness, scope, native_ref, core::limits::MAX_UNCLASSIFIED_NATIVE_FACTS_PER_SESSION],
    ).map_err(sql_error)?;
    if admitted == 0 {
        tx.execute(
            "UPDATE native_session_unclassified SET dropped = dropped + 1
             WHERE harness = ?1 AND native_scope = ?2 AND native_ref = ?3 AND dropped < ?4",
            params![agent.harness, scope, native_ref, i64::MAX],
        )
        .map_err(sql_error)?;
    }
    Ok(admitted != 0)
}

fn load_native_diagnostic_counts(
    connection: &Connection,
    agent: &Agent,
) -> Result<Option<core::api::NativeDiagnosticCounts>> {
    let (Some(scope), Some(native_ref)) =
        (agent.native_scope.as_deref(), agent.native_ref.as_deref())
    else {
        return Ok(None);
    };
    connection
        .query_row(
            "SELECT admitted, dropped FROM native_session_unclassified
         WHERE harness = ?1 AND native_scope = ?2 AND native_ref = ?3",
            params![agent.harness, scope, native_ref],
            |row| {
                Ok(core::api::NativeDiagnosticCounts {
                    unclassified_admitted: row.get(0)?,
                    unclassified_dropped: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

// Statement errors return before COMMIT. If COMMIT itself fails, a
// successful explicit rollback proves no write. Otherwise retain uncertainty.
fn commit_source_batch(tx: Transaction<'_>, write_faulted: &Cell<bool>) -> Result<()> {
    let Err(error) = commit_sql(&tx) else {
        return Ok(());
    };
    if !tx.is_autocommit() && tx.rollback().is_ok() {
        return Err(sql_error(error));
    }
    write_faulted.set(true);
    Err(Error::new(
        "unknown",
        "native batch commit could not be settled",
    ))
}

impl Store {
    /// Read the exact bounded command and failure text for one committed native
    /// completion, plus the number of matching failures in this Agent,
    /// generation and turn (capped at the reaction threshold).
    ///
    /// The command and output must both be complete redacted values. Older
    /// adapter rows without explicit truncation flags and any truncated
    /// payload are deliberately ineligible, so prefixes can never masquerade
    /// as equal full failures.
    pub fn repeated_command_failure_evidence(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
        event_seq: i64,
    ) -> Result<Option<(String, String, u8)>> {
        if generation < 1 || turn_id.trim().is_empty() || turn_id.len() > 256 || event_seq < 1 {
            return Err(Error::new(
                "invalid",
                "invalid repeated-command failure binding",
            ));
        }
        let agent_num = parse_agent_id(agent)?;
        let current: Option<(String, String, String)> = self
            .connection
            .query_row(
                "SELECT s.source, json_extract(s.data, '$.command'),
                        json_extract(c.data, '$.native_error.message')
                 FROM events AS c
                 JOIN events AS s
                   ON s.agent = c.agent AND s.generation = c.generation
                  AND s.source = c.source AND s.kind = 'tool.started'
                  AND s.native_id IS NOT NULL
                  AND s.seq < c.seq
                  AND json_extract(s.data, '$.turn_id') = ?4
                  AND json_extract(s.data, '$.item_id') =
                      json_extract(c.data, '$.item_id')
                 WHERE c.seq = ?1 AND c.agent = ?2 AND c.generation = ?3
                   AND c.native_id IS NOT NULL
                   AND c.kind = 'tool.completed'
                   AND json_extract(c.data, '$.turn_id') = ?4
                   AND json_extract(c.data, '$.status') = 'error'
                   AND json_type(c.data, '$.native_error.message') = 'text'
                   AND json_extract(c.data, '$.native_error.truncated') = 0
                   AND json_type(s.data, '$.command') = 'text'
                   AND json_extract(s.data, '$.command_truncated') = 0
                 LIMIT 1",
                params![event_seq, agent_num, generation, turn_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((source, command, output)) = current else {
            return Ok(None);
        };
        if command.is_empty()
            || output.is_empty()
            || command.len() > 1024
            || output.len() > 4 * 1024
        {
            return Ok(None);
        }
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM (
                    SELECT DISTINCT c.seq
                    FROM events AS c
                    JOIN events AS s
                      ON s.agent = c.agent AND s.generation = c.generation
                     AND s.source = c.source AND s.kind = 'tool.started'
                     AND s.native_id IS NOT NULL
                     AND s.seq < c.seq
                     AND json_extract(s.data, '$.turn_id') = ?4
                     AND json_extract(s.data, '$.item_id') =
                         json_extract(c.data, '$.item_id')
                    WHERE c.agent = ?1 AND c.generation = ?2 AND c.source = ?3
                      AND c.kind = 'tool.completed' AND c.native_id IS NOT NULL AND c.seq <= ?7
                      AND json_extract(c.data, '$.turn_id') = ?4
                      AND json_extract(c.data, '$.status') = 'error'
                      AND json_extract(c.data, '$.native_error.message') = ?6
                      AND json_extract(c.data, '$.native_error.truncated') = 0
                      AND json_extract(s.data, '$.command') = ?5
                      AND json_extract(s.data, '$.command_truncated') = 0
                    ORDER BY c.seq DESC
                    LIMIT 3
                 )",
                params![
                    agent_num, generation, source, turn_id, command, output, event_seq
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let count = u8::try_from(count)
            .map_err(|_| Error::new("internal", "failure count exceeded its bound"))?;
        Ok(Some((command, output, count)))
    }
}

impl Store {
    /// Read a bounded recent tail of committed native tool completions.
    /// Results are returned in event order and contain only native terminal
    /// rows, so recovery never treats a daemon or user event as tool evidence.
    pub fn recent_native_tool_completions(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> Result<Vec<Event>> {
        validate_limit(limit)?;
        if after < 0 || through < after {
            return Err(Error::new("invalid", "invalid tool event sequence bounds"));
        }
        self.require_retained(after)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events INDEXED BY events_kind
                 WHERE kind = 'tool.completed' AND native_id IS NOT NULL
                   AND seq > ?1 AND seq <= ?2
                 ORDER BY seq DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, through, limit as i64], event_record_from_row)
            .map_err(sql_error)?;
        let mut events = Vec::new();
        for row in rows {
            events.push(event_from_record(row.map_err(sql_error)?)?);
        }
        events.reverse();
        enforce_page_bytes(&mut events)?;
        Ok(events)
    }
}
