use crate::{Store, event_from_record, event_record_from_row, sql_error};
use eliot_core::{Event, Result, parse_agent_id};
use rusqlite::{Connection, OptionalExtension, params};

/// Only direct Owner input to this exact turn can veto an automatic interrupt.
/// The accepted event orders input against the question even within one millisecond.
pub(super) fn owner_input_after_question(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn: &str,
    after: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT (SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1) > ?4 OR EXISTS(
                 SELECT 1 FROM events AS e INDEXED BY events_agent
                 JOIN commands AS c ON c.id = json_extract(e.data, '$.command_id')
                 WHERE e.agent = ?1 AND e.generation = ?2 AND e.seq > ?4
                   AND e.kind = 'command.accepted'
                   AND c.target = ?1 AND c.generation = ?2
                   AND c.principal = 'owner' AND c.parent_command IS NULL
                   AND c.verb IN ('send', 'steer')
                   AND c.state NOT IN ('rejected', 'cancelled')
                   AND COALESCE(c.expected_turn_id, c.turn_id) = ?3
             )",
            params![agent, generation, turn, after],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

pub(super) fn guard_attention_interrupt(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn: &str,
    command_id: &str,
) -> Result<()> {
    let after: Option<i64> = connection
        .query_row(
            "SELECT after_question_seq FROM turn_attention_steps
             WHERE command_id = ?1 AND agent = ?2 AND generation = ?3 AND turn_id = ?4
               AND step = 'interrupt'",
            params![command_id, agent, generation, turn],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(after) = after
        && owner_input_after_question(connection, agent, generation, turn, after)?
    {
        return Err(eliot_core::Error::new(
            "owner_input",
            "newer Owner input superseded the automatic interrupt",
        ));
    }
    Ok(())
}

impl Store {
    /// A timed step remains reserved for the lifetime of its exact turn,
    /// independent of diagnostic journal retention.
    pub fn attention_turn_step(
        &self,
        agent: &str,
        generation: i64,
        turn: &str,
        step: &str,
        reason: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        self.connection
            .query_row(
                "SELECT event_seq, claimed_at FROM turn_attention_steps
             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
               AND step = ?4 AND (incident_kind = ?5 OR step = 'question')",
                params![
                    parse_agent_id(agent)?,
                    generation,
                    turn,
                    step,
                    reason.unwrap_or_default()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn attention_owner_input_after(
        &self,
        agent: &str,
        generation: i64,
        turn: &str,
        after: i64,
    ) -> Result<bool> {
        owner_input_after_question(
            &self.connection,
            parse_agent_id(agent)?,
            generation,
            turn,
            after,
        )
    }

    /// Read the retained attention report for one deterministic incident key.
    pub fn attention_incident_event(&self, incident_id: &str) -> Result<Option<Event>> {
        self.attention_report_event(incident_id, "incident")
    }

    pub fn attention_report_event(&self, incident_id: &str, phase: &str) -> Result<Option<Event>> {
        let record = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events
                 WHERE kind = 'x.user.agent.attention'
                   AND json_extract(data, '$.data.incident_id') = ?1
                   AND json_extract(data, '$.data.phase') = ?2
                 ORDER BY seq
                 LIMIT 1",
                params![incident_id, phase],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        record.map(event_from_record).transpose()
    }

    /// Read one persisted reaction step, including its commit timestamp and
    /// command binding, so a restarted worker resumes from the recorded step.
    pub fn attention_step_event(&self, incident_id: &str, step: &str) -> Result<Option<Event>> {
        let record = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events
                 WHERE kind = 'x.user.agent.attention_step'
                   AND json_extract(data, '$.incident_id') = ?1
                   AND json_extract(data, '$.step') = ?2
                 ORDER BY seq DESC
                 LIMIT 1",
                params![incident_id, step],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        record.map(event_from_record).transpose()
    }

    /// The immediately preceding turn-unknown fact is written in the same
    /// loss transaction and can preserve an exact turn after the Agent
    /// projection has already cleared its active-turn field.
    pub fn attention_loss_turn_id(
        &self,
        agent: &str,
        generation: i64,
        loss_event_seq: i64,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        let exact: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT json_extract(data, '$.turn_id') FROM events
                 WHERE seq = ?1 AND agent = ?2 AND generation = ?3 AND kind = 'agent.lost'",
                params![loss_event_seq, agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(turn) = exact.flatten() {
            return Ok(Some(turn));
        }
        let Some(loss_ts) = self.daemon_event_timestamp(loss_event_seq)? else {
            return Ok(None);
        };
        self.connection
            .query_row(
                "SELECT json_extract(data, '$.turn_id')
                 FROM events
                 WHERE kind = 'turn.unknown'
                   AND agent = ?1
                   AND generation = ?2
                   AND seq < ?3
                   AND ts = ?4
                 ORDER BY seq DESC
                 LIMIT 1",
                params![agent_num, generation, loss_event_seq, loss_ts],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }
}
