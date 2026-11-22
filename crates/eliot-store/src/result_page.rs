//! Bounded cross-generation result pages and durable generation causes.

use super::*;
use eliot_core::api::{
    AgentResultRequest, AgentResultResponse, GenerationBoundary, GenerationReason, ResultCursor,
};

impl Store {
    /// Read one newest-first page across retained generations, or one exact
    /// generation when `generation` is supplied.
    pub fn agent_result_page(
        &self,
        agent: &Agent,
        generation_filter: Option<i64>,
        request: &AgentResultRequest,
    ) -> Result<AgentResultResponse> {
        let limit = request.limit;
        validate_limit(limit)?;
        let agent_num = parse_agent_id(&agent.id)?;
        let generation = generation_filter.unwrap_or(agent.generation);
        let mut response = AgentResultResponse {
            agent: request.id.clone(),
            generation,
            current_generation: agent.generation,
            turns: Vec::new(),
            agent_metadata: request.metadata.then(|| agent.clone()),
            next: None,
            generation_boundaries: Vec::new(),
        };
        if let Some(turn_id) = request.turn_id.as_deref() {
            let mut turn = self
                .get_turn(&response.agent, generation, turn_id)?
                .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
            self.fill_turn_readback(agent, &mut turn)?;
            let mask_growth = unknown_status_growth(&turn);
            response.turns.push(turn);
            response
                .generation_boundaries
                .push(self.generation_boundary(agent_num, generation)?);
            ensure_result_budget(encode_json(&response)?.len() + mask_growth)?;
            return Ok(response);
        }

        let before = request.before.as_ref();
        if before.is_none() && generation > 0 && generation == agent.generation {
            response
                .generation_boundaries
                .push(self.generation_boundary(agent_num, generation)?);
        }
        // Includes the response fields, optional metadata, empty arrays and null cursor.
        let mut bytes = encode_json(&response)?.len();
        ensure_result_budget(bytes)?;

        // Separate predicates let SQLite seek directly to the full index key.
        let filter = match (generation_filter, before) {
            (None, None) => "",
            (Some(_), None) => "AND t.generation = ?2",
            (None, Some(_)) => "AND (t.generation, t.insertion_seq) < (?2, ?3)",
            (Some(_), Some(_)) => "AND t.generation = ?2 AND t.insertion_seq < ?3",
        };
        let sql = format!(
            "SELECT t.agent, t.generation, t.turn_id, t.state, t.status, t.outcome, t.native_status,
                    t.final_text, t.started_at, t.completed_at, t.usage, t.config_revision, t.preceded_by,
                    t.resolution_source, t.effective_model, t.native_error_message, t.insertion_seq, g.reason
             FROM turns t
             LEFT JOIN agent_generation_reasons g ON g.agent = t.agent AND g.generation = t.generation
             WHERE t.agent = ?1 {filter}
             ORDER BY t.generation DESC, t.insertion_seq DESC LIMIT ?4"
        );
        let mut statement = self.connection.prepare_cached(&sql).map_err(sql_error)?;
        let mut rows = statement
            .query_map(
                params![
                    agent_num,
                    generation_filter.or_else(|| before.map(|cursor| cursor.generation)),
                    before.map(|cursor| cursor.insertion_seq),
                    (limit + 1) as i64,
                ],
                |row| {
                    Ok((
                        turn_record_from_row(row)?,
                        row.get::<_, i64>(16)?,
                        row.get::<_, Option<String>>(17)?,
                    ))
                },
            )
            .map_err(sql_error)?
            .peekable();
        while response.turns.len() < limit {
            let Some(row) = rows.next() else { break };
            let (record, insertion_seq, reason) = row.map_err(sql_error)?;
            let mut turn = turn_from_record(record, &agent.harness)?;
            self.fill_turn_readback(agent, &mut turn)?;
            let boundary = response
                .generation_boundaries
                .last()
                .is_none_or(|boundary| boundary.generation != turn.generation)
                .then(|| boundary_with_reason(turn.generation, reason.as_deref()));
            let next = rows.peek().is_some().then(|| ResultCursor {
                generation: turn.generation,
                insertion_seq,
                turn_id: turn.turn_id.clone(),
            });
            let mut payload = bytes
                + encode_json(&turn)?.len()
                + usize::from(!response.turns.is_empty())
                + unknown_status_growth(&turn);
            if let Some(boundary) = &boundary {
                payload += encode_json(boundary)?.len()
                    + usize::from(!response.generation_boundaries.is_empty());
            }
            // A continuation replaces the four bytes of null already counted.
            let cursor_bytes = next
                .as_ref()
                .map(|cursor| encode_json(cursor).map(|json| json.len() - 4))
                .transpose()?
                .unwrap_or(0);
            if payload + cursor_bytes > MAX_PAGE_BYTES {
                if response.turns.is_empty() {
                    ensure_result_budget(payload + cursor_bytes)?;
                }
                break;
            }
            bytes = payload;
            response.turns.push(turn);
            response.generation_boundaries.extend(boundary);
            response.next = next;
        }
        Ok(response)
    }

    fn generation_boundary(&self, agent: i64, generation: i64) -> Result<GenerationBoundary> {
        let reason: Option<String> = self
            .connection
            .query_row(
                "SELECT reason FROM agent_generation_reasons WHERE agent = ?1 AND generation = ?2",
                params![agent, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(boundary_with_reason(generation, reason.as_deref()))
    }

    /// Set-based unknown-turn mask for a result page spanning generations.
    /// Generation stays part of each join key so reused turn IDs cannot mask
    /// a different generation's result.
    pub fn turns_with_unknown_commands_for_page(
        &self,
        agent: &str,
        turns: &[(i64, String)],
        memory_unknown: &[String],
    ) -> Result<HashSet<(i64, String)>> {
        if turns.is_empty() {
            return Ok(HashSet::new());
        }
        let agent_num = parse_agent_id(agent)?;
        let turn_keys = serde_json::to_string(turns)
            .map_err(|_| Error::new("internal", "cannot encode unknown turn keys"))?;
        let memory_unknown = serde_json::to_string(memory_unknown)
            .map_err(|_| Error::new("internal", "cannot encode unknown command keys"))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT commands.generation, commands.turn_id
                 FROM json_each(?2) AS requested
                 CROSS JOIN commands INDEXED BY commands_turn_unknown
                 WHERE commands.target = ?1
                   AND commands.generation = json_extract(requested.value, '$[0]')
                   AND commands.turn_id = json_extract(requested.value, '$[1]')
                   AND commands.state IN ('accepted','queued','dispatched','unknown')
                   AND (commands.state = 'unknown'
                        OR commands.id IN (SELECT value FROM json_each(?3)))",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![agent_num, turn_keys, memory_unknown], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?;
        let mut unknown = HashSet::new();
        for row in rows {
            unknown.insert(row.map_err(sql_error)?);
        }
        Ok(unknown)
    }
}

pub(super) fn record_generation_reason_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    reason: GenerationReason,
) -> Result<()> {
    tx.execute(
        "INSERT INTO agent_generation_reasons(agent, generation, reason)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(agent, generation) DO UPDATE SET reason = excluded.reason",
        params![agent, generation, generation_reason_name(reason)],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn record_restart_resume_intent_tx(
    tx: &Transaction<'_>,
    command_id: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO restart_resume_intents(command_id) VALUES (?1)
         ON CONFLICT(command_id) DO NOTHING",
        [command_id],
    )
    .map_err(sql_error)?;
    Ok(())
}

/// Handoff may supply the first conversation of an already prepared Agent.
/// Preserve any earlier conversation or the generation's recorded transition.
pub(super) fn record_handoff_origin_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO agent_generation_reasons(agent, generation, reason)
         SELECT ?1, ?2, 'handoff'
         WHERE NOT EXISTS(SELECT 1 FROM turns WHERE agent = ?1 AND generation = ?2)
         ON CONFLICT(agent, generation) DO UPDATE SET reason = excluded.reason
         WHERE agent_generation_reasons.reason IN ('launch', 'unknown')",
        params![agent, generation],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn is_restart_resume_intent_tx(tx: &Transaction<'_>, command_id: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM restart_resume_intents WHERE command_id = ?1)",
        [command_id],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

pub(super) fn generation_reason_name(reason: GenerationReason) -> &'static str {
    match reason {
        GenerationReason::Launch => "launch",
        GenerationReason::RestartResume => "restart_resume",
        GenerationReason::OwnerResume => "owner_resume",
        GenerationReason::AgentResume => "agent_resume",
        GenerationReason::Switch => "switch",
        GenerationReason::Handoff => "handoff",
        GenerationReason::Attach => "attach",
        GenerationReason::Detach => "detach",
        GenerationReason::Unknown => "unknown",
    }
}

fn parse_generation_reason(reason: &str) -> GenerationReason {
    match reason {
        "launch" => GenerationReason::Launch,
        "restart_resume" => GenerationReason::RestartResume,
        "owner_resume" => GenerationReason::OwnerResume,
        "agent_resume" => GenerationReason::AgentResume,
        "switch" => GenerationReason::Switch,
        "handoff" => GenerationReason::Handoff,
        "attach" => GenerationReason::Attach,
        "detach" => GenerationReason::Detach,
        _ => GenerationReason::Unknown,
    }
}

fn boundary_with_reason(generation: i64, reason: Option<&str>) -> GenerationBoundary {
    GenerationBoundary {
        generation,
        reason: reason.map_or(GenerationReason::Unknown, parse_generation_reason),
    }
}

fn ensure_result_budget(bytes: usize) -> Result<()> {
    if bytes > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "Agent result exceeds the page byte bound",
        ));
    }
    Ok(())
}

fn unknown_status_growth(turn: &Turn) -> usize {
    // Unknown masking can replace a short quoted status with a four-byte null.
    turn.status
        .as_ref()
        .map_or(0, |status| 2_usize.saturating_sub(status.len()))
}
