use super::*;
use eliot_core::api::{
    MAX_SWARM_MCP_TURNS, SWARM_MCP_EVENT_ORIGIN, SWARM_MCP_EVENT_TOOL, SwarmMcpToolEventData,
    SwarmMcpToolStatus, SwarmMcpTurnUsage,
};
use rusqlite::OptionalExtension;
const SWARM_MCP_SOURCE: &str = "daemon";

fn call_native_id(call_id: &str, phase: &str) -> String {
    format!("swarm-mcp:{call_id}:{phase}")
}

fn validate_call_id(call_id: &str) -> Result<()> {
    if ulid::Ulid::from_string(call_id).is_err() {
        return Err(Error::new("invalid", "Swarm MCP call id is invalid"));
    }
    Ok(())
}

fn read_swarm_mcp_event(
    tx: &Transaction<'_>,
    agent: Option<i64>,
    generation: Option<i64>,
    native_id: &str,
) -> Result<Option<(String, SwarmMcpToolEventData)>> {
    let row = tx
        .query_row(
            "SELECT kind, data FROM events
             WHERE agent IS ?1 AND generation IS ?2 AND source = ?3 AND native_id = ?4",
            params![agent, generation, SWARM_MCP_SOURCE, native_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((kind, raw)) = row else {
        return Ok(None);
    };
    let value = decode_json(&raw)?;
    let data: SwarmMcpToolEventData = serde_json::from_value(value)
        .map_err(|_| Error::new("source_integrity", "retained Swarm MCP event is invalid"))?;
    data.validate_for_kind(&kind)
        .map_err(|_| Error::new("source_integrity", "retained Swarm MCP event is invalid"))?;
    Ok(Some((kind, data)))
}

fn insert_swarm_mcp_event(
    tx: &Transaction<'_>,
    agent: Option<i64>,
    generation: Option<i64>,
    kind: &str,
    data: &SwarmMcpToolEventData,
    now: i64,
) -> Result<Event> {
    data.validate_for_kind(kind)?;
    let phase = match kind {
        "tool.started" => "started",
        "tool.completed" => "completed",
        _ => return Err(Error::new("invalid", "Swarm MCP event kind is invalid")),
    };
    let native_id = call_native_id(&data.call_id, phase);
    let subject = match (agent, generation) {
        (Some(agent), Some(generation)) => event_subject_for_generation(tx, agent, generation)?,
        (None, None) => None,
        _ => {
            return Err(Error::new(
                "source_integrity",
                "Swarm MCP event binding is incomplete",
            ));
        }
    };
    let data = serde_json::to_value(data)
        .map_err(|_| Error::new("internal", "Swarm MCP event could not be encoded"))?;
    insert_event(
        tx,
        EventInsert {
            ts: now,
            kind,
            family: Some("tool"),
            agent,
            generation,
            source: SWARM_MCP_SOURCE,
            native_id: Some(&native_id),
            cursor: None,
            payload_digest: None,
            subject,
            cause: None,
            data,
        },
    )
}

impl Store {
    /// Persist a start event for an authenticated Agent call or a verified
    /// remote MCP call. Agent calls bind to the exact active turn in Store.
    pub fn record_swarm_mcp_tool_started(
        &mut self,
        agent: Option<(&str, i64)>,
        call_id: &str,
        method: &str,
        now: i64,
    ) -> Result<(bool, Option<String>, Vec<Event>)> {
        self.ensure_writer_healthy()?;
        validate_call_id(call_id)?;
        let result = (|| {
            let (agent_num, generation) = if let Some((agent, generation)) = agent {
                if generation <= 0 {
                    return Err(Error::new("invalid", "Agent generation must be positive"));
                }
                (Some(parse_agent_id(agent)?), Some(generation))
            } else {
                (None, None)
            };
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let (Some(agent_num), Some(generation)) = (agent_num, generation) {
                let current = load_agent_by_num(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "Swarm MCP Agent was not found"))?;
                if current.generation != generation || current.is_terminal() {
                    return Err(Error::new(
                        "stale_generation",
                        "Swarm MCP call does not match a live Agent generation",
                    ));
                }
            }

            let start_id = call_native_id(call_id, "started");
            if let Some((kind, existing)) =
                read_swarm_mcp_event(&tx, agent_num, generation, &start_id)?
            {
                if kind != "tool.started"
                    || existing.call_id != call_id
                    || existing.method != method
                    || existing.turn_id.is_none() != agent.is_none()
                {
                    return Err(Error::new(
                        "source_integrity",
                        "Swarm MCP call id was reused with different binding",
                    ));
                }
                return Ok((true, existing.turn_id, Vec::new()));
            }

            let turn_id = if let (Some(agent_num), Some(generation)) = (agent_num, generation) {
                let current = load_agent_by_num(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "Swarm MCP Agent was not found"))?;
                let Some(turn_id) = current.active_turn else {
                    return Ok((false, None, Vec::new()));
                };
                let state: Option<String> = tx
                    .query_row(
                        "SELECT state FROM turns
                         WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
                        params![agent_num, generation, turn_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?;
                if state.as_deref() != Some("running") {
                    return Ok((false, None, Vec::new()));
                }
                Some(turn_id)
            } else {
                None
            };

            let data =
                SwarmMcpToolEventData::new(call_id.to_owned(), method.to_owned(), turn_id.clone());
            data.validate_for_kind("tool.started")?;
            let event =
                insert_swarm_mcp_event(&tx, agent_num, generation, "tool.started", &data, now)?;
            commit_transaction(tx, &self.write_faulted).map(|()| (true, turn_id, vec![event]))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Complete only a call with a retained start event. The Store reuses the
    /// original exact turn/method binding and accepts no call parameters.
    pub fn record_swarm_mcp_tool_completed(
        &mut self,
        agent: Option<(&str, i64)>,
        call_id: &str,
        status: SwarmMcpToolStatus,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        validate_call_id(call_id)?;
        let result = (|| {
            let (agent_num, generation) = if let Some((agent, generation)) = agent {
                if generation <= 0 {
                    return Err(Error::new("invalid", "Agent generation must be positive"));
                }
                (Some(parse_agent_id(agent)?), Some(generation))
            } else {
                (None, None)
            };
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let (Some(agent_num), Some(generation)) = (agent_num, generation) {
                let current = load_agent_by_num(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "Swarm MCP Agent was not found"))?;
                if current.generation != generation || current.is_terminal() {
                    return Err(Error::new(
                        "stale_generation",
                        "Swarm MCP completion does not match a live Agent generation",
                    ));
                }
            }

            let start_id = call_native_id(call_id, "started");
            let Some((kind, started)) =
                read_swarm_mcp_event(&tx, agent_num, generation, &start_id)?
            else {
                return Ok(Vec::new());
            };
            if kind != "tool.started"
                || started.call_id != call_id
                || started.turn_id.is_none() != agent.is_none()
            {
                return Err(Error::new(
                    "source_integrity",
                    "Swarm MCP completion has no matching start binding",
                ));
            }
            let completed = started.clone().completed(status);
            completed
                .validate_for_kind("tool.completed")
                .map_err(|_| Error::new("source_integrity", "Swarm MCP start event is invalid"))?;
            let completed_id = call_native_id(call_id, "completed");
            if let Some((kind, existing)) =
                read_swarm_mcp_event(&tx, agent_num, generation, &completed_id)?
            {
                if kind == "tool.completed" && existing == completed {
                    return Ok(Vec::new());
                }
                return Err(Error::new(
                    "source_integrity",
                    "Swarm MCP completion id was reused with different content",
                ));
            }
            if let (Some(agent_num), Some(generation), Some(turn_id)) =
                (agent_num, generation, started.turn_id.as_deref())
            {
                let retained: bool = tx
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM turns
                             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
                         )",
                        params![agent_num, generation, turn_id],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                if !retained {
                    return Ok(Vec::new());
                }
            }
            let event = insert_swarm_mcp_event(
                &tx,
                agent_num,
                generation,
                "tool.completed",
                &completed,
                now,
            )?;
            commit_transaction(tx, &self.write_faulted).map(|()| vec![event])
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read recent exact-turn call/refusal counts with a fixed response bound.
    pub fn swarm_mcp_turn_usage(
        &self,
        pairs: &[(i64, i64)],
    ) -> Result<(Vec<SwarmMcpTurnUsage>, bool)> {
        if pairs.is_empty() {
            return Ok((Vec::new(), false));
        }
        let pair_json = serde_json::to_string(pairs)
            .map_err(|_| Error::new("internal", "usage scope could not be encoded"))?;
        let sql = "
            WITH wanted AS (
                SELECT CAST(json_extract(value, '$[0]') AS INTEGER) AS agent,
                       CAST(json_extract(value, '$[1]') AS INTEGER) AS generation
                FROM json_each(?1)
            ),
            per_turn AS (
                SELECT e.agent,
                       e.generation,
                       json_extract(e.data, '$.turn_id') AS turn_id,
                       COUNT(DISTINCT CASE WHEN e.kind = 'tool.started'
                           THEN json_extract(e.data, '$.call_id') END) AS calls,
                       COUNT(DISTINCT CASE
                           WHEN e.kind = 'tool.completed'
                            AND json_extract(e.data, '$.status') = 'refused'
                            AND EXISTS (
                                SELECT 1 FROM events AS started
                                WHERE started.agent = e.agent
                                  AND started.generation = e.generation
                                  AND started.source = 'daemon'
                                  AND started.kind = 'tool.started'
                                  AND json_extract(started.data, '$.origin') = ?2
                                  AND json_extract(started.data, '$.tool') = ?3
                                  AND json_extract(started.data, '$.call_id') =
                                      json_extract(e.data, '$.call_id')
                                  AND json_extract(started.data, '$.turn_id') =
                                      json_extract(e.data, '$.turn_id')
                            )
                           THEN json_extract(e.data, '$.call_id') END) AS refusals,
                       MAX(e.seq) AS latest
                FROM events AS e INDEXED BY events_agent
                JOIN wanted AS w
                  ON w.agent = e.agent AND w.generation = e.generation
                WHERE e.source = 'daemon'
                  AND e.kind IN ('tool.started', 'tool.completed')
                  AND json_extract(e.data, '$.origin') = ?2
                  AND json_extract(e.data, '$.tool') = ?3
                  AND json_type(e.data, '$.turn_id') = 'text'
                GROUP BY e.agent, e.generation, json_extract(e.data, '$.turn_id')
                HAVING COUNT(DISTINCT CASE WHEN e.kind = 'tool.started'
                    THEN json_extract(e.data, '$.call_id') END) > 0
            )
            SELECT agent, generation, turn_id, calls, refusals
            FROM per_turn
            ORDER BY latest DESC
            LIMIT ?4";
        let mut statement = self.connection.prepare(sql).map_err(sql_error)?;
        let rows = statement
            .query_map(
                rusqlite::params![
                    pair_json,
                    SWARM_MCP_EVENT_ORIGIN,
                    SWARM_MCP_EVENT_TOOL,
                    (MAX_SWARM_MCP_TURNS + 1) as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut usage = Vec::with_capacity(MAX_SWARM_MCP_TURNS);
        let mut truncated = false;
        for row in rows {
            let (agent, generation, turn_id, calls, refusals) = row.map_err(sql_error)?;
            if usage.len() == MAX_SWARM_MCP_TURNS {
                truncated = true;
                break;
            }
            let calls = usize::try_from(calls)
                .map_err(|_| Error::new("source_integrity", "Swarm MCP call count is invalid"))?;
            let refusals = usize::try_from(refusals).map_err(|_| {
                Error::new("source_integrity", "Swarm MCP refusal count is invalid")
            })?;
            if calls == 0 || refusals > calls {
                return Err(Error::new(
                    "source_integrity",
                    "Swarm MCP usage counts are inconsistent",
                ));
            }
            usage.push(SwarmMcpTurnUsage {
                agent: format_agent_id(agent),
                generation,
                turn_id,
                calls,
                refusals,
            });
        }
        Ok((usage, truncated))
    }
}
