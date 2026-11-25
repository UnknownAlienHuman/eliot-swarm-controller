use super::*;
use eliot_core::api::TurnResolveStatus;

const PAGE: usize = 4;

pub type HistoricalTurnCursor = (i64, i64, String);

/// A probe may read only the native subject recorded by this generation.
#[derive(Clone, Debug)]
pub struct HistoricalTurn {
    pub agent: Agent,
    pub generation: i64,
    pub turn_id: String,
    pub native_scope: String,
    pub native_ref: String,
    pub profile: Value,
    pub command_id: Option<String>,
    pub retained_terminal: Option<(i64, TurnResolveStatus)>,
}

impl Store {
    pub fn historical_unknown_turns(
        &self,
        after: Option<&HistoricalTurnCursor>,
    ) -> Result<(Vec<HistoricalTurn>, Option<HistoricalTurnCursor>)> {
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT agent,generation,turn_id FROM turns
             WHERE state='unknown' AND (agent,generation,turn_id) > (?1,?2,?3)
             ORDER BY agent,generation,turn_id LIMIT ?4",
            )
            .map_err(sql_error)?;
        let after = after.cloned().unwrap_or_default();
        let keys = statement
            .query_map(params![after.0, after.1, after.2, PAGE as i64 + 1], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        let next = (keys.len() > PAGE).then(|| keys[PAGE - 1].clone());
        let mut turns = Vec::new();
        for (agent, generation, turn) in keys.into_iter().take(PAGE) {
            if let Some(candidate) = binding(&self.connection, agent, generation, &turn)? {
                turns.push(candidate);
            }
        }
        Ok((turns, next))
    }

    /// Settle only the captured historical turn. The current agent, command
    /// state and custody are independent of the native history outcome.
    pub fn settle_historical_turn(
        &mut self,
        captured: &HistoricalTurn,
        status: TurnResolveStatus,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let num = parse_agent_id(&captured.agent.id)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let Some(current) = binding(&tx, num, captured.generation, &captured.turn_id)? else {
                return Ok(Vec::new());
            };
            if current.native_ref != captured.native_ref
                || current.native_scope != captured.native_scope
                || current.profile != captured.profile
                || current.command_id != captured.command_id
                || current
                    .retained_terminal
                    .is_some_and(|(_, value)| value != status)
                || captured.retained_terminal.is_some()
                    && current.retained_terminal != captured.retained_terminal
            {
                return Ok(Vec::new());
            }
            let outcome = match status {
                TurnResolveStatus::Completed => TurnOutcome::Success,
                TurnResolveStatus::Failed => TurnOutcome::Failure,
                TurnResolveStatus::Interrupted => TurnOutcome::Interrupted,
            };
            let updated = tx
                .execute(
                    "UPDATE turns SET state='completed',status=?1,outcome=?2,
                 completed_at=COALESCE(completed_at,?3),resolution_source='native'
                 WHERE agent=?4 AND generation=?5 AND turn_id=?6 AND state='unknown'",
                    params![
                        status.as_str(),
                        outcome.as_str(),
                        now,
                        num,
                        captured.generation,
                        captured.turn_id
                    ],
                )
                .map_err(sql_error)?;
            if updated == 0 {
                return Ok(Vec::new());
            }
            let mut data = json!({
                "turn_id":captured.turn_id, "command_id":captured.command_id,
                "status":status.as_str(), "outcome":outcome.as_str(), "resolution_source":"native",
                "evidence":"native_history",
            });
            if let Some((seq, _)) = current.retained_terminal {
                data["native_event_seq"] = json!(seq);
                data["evidence"] = json!("retained_native_terminal");
            }
            let event = insert_daemon_event(
                &tx,
                now,
                "turn.resolved",
                "turn",
                Some(num),
                Some(captured.generation),
                data,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(vec![event])
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn binding(
    db: &Connection,
    num: i64,
    generation: i64,
    turn: &str,
) -> Result<Option<HistoricalTurn>> {
    let Some(agent) = load_agent_by_num(db, num)? else {
        return Ok(None);
    };
    if agent.observe_only
        || generation > agent.generation
        || (generation == agent.generation && agent.active_turn.as_deref() == Some(turn))
        || load_turn(db, num, generation, turn)?.is_none_or(|turn| turn.state != TurnState::Unknown)
    {
        return Ok(None);
    }
    let mut statement = db
        .prepare_cached(
            "SELECT DISTINCT json_extract(data,'$.native_scope'),json_extract(data,'$.native_ref')
         FROM events INDEXED BY events_agent
         WHERE agent=?1 AND generation=?2 AND kind='session.ready' AND source='stream'
           AND native_id IS NOT NULL
           AND json_type(data,'$.native_scope')='text' AND json_type(data,'$.native_ref')='text'
         LIMIT 2",
        )
        .map_err(sql_error)?;
    let subjects = statement
        .query_map(params![num, generation], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let [(scope, reference)] = subjects.as_slice() else {
        return Ok(None);
    };
    let mut starts = db
        .prepare_cached(
            "SELECT DISTINCT json_extract(data,'$.command_id') FROM events INDEXED BY events_agent
         WHERE agent=?1 AND generation=?2 AND kind='turn.started'
           AND source='stream' AND native_id IS NOT NULL AND json_extract(data,'$.turn_id')=?3
         LIMIT 2",
        )
        .map_err(sql_error)?;
    let start = starts
        .query_map(params![num, generation, turn], |r| {
            r.get::<_, Option<String>>(0)
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let [command_id] = start.as_slice() else {
        return Ok(None);
    };
    let mut terminals = db
        .prepare_cached(
            "SELECT seq,json_extract(data,'$.status'),json_extract(data,'$.outcome')
         FROM events INDEXED BY events_agent
         WHERE agent=?1 AND generation=?2 AND kind='turn.completed'
           AND source='stream' AND native_id IS NOT NULL AND json_extract(data,'$.turn_id')=?3
           AND json_extract(data,'$.command_id') IS ?4
         LIMIT 2",
        )
        .map_err(sql_error)?;
    let rows = terminals
        .query_map(params![num, generation, turn, command_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let retained_terminal = match rows.as_slice() {
        [] => None,
        [(seq, status, outcome)] => {
            let status = match (status.as_deref(), outcome.as_deref()) {
                (Some("completed"), Some("success")) => TurnResolveStatus::Completed,
                (Some("failed"), Some("failure")) => TurnResolveStatus::Failed,
                (Some("interrupted" | "interrupted_by_owner"), Some("interrupted")) => {
                    TurnResolveStatus::Interrupted
                }
                _ => return Ok(None),
            };
            Some((*seq, status))
        }
        _ => return Ok(None),
    };
    let profile = if agent.native_scope.as_ref() == Some(scope)
        && agent.native_ref.as_ref() == Some(reference)
    {
        agent.profile_snapshot.clone()
    } else {
        let snapshot = db
            .query_row(
                "SELECT json_extract(args,'$.profile_snapshot') FROM commands
             WHERE target=?1 AND generation=?2 AND verb IN ('launch','resume') AND state='applied'
               AND json_type(args,'$.profile_snapshot')='object'
             ORDER BY rowid DESC LIMIT 1",
                params![num, generation],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        match snapshot {
            Some(snapshot) => decode_json(&snapshot)?,
            None if retained_terminal.is_some() => Value::Null,
            None => return Ok(None),
        }
    };
    Ok(Some(HistoricalTurn {
        agent,
        generation,
        turn_id: turn.to_owned(),
        native_scope: scope.clone(),
        native_ref: reference.clone(),
        profile,
        command_id: command_id.clone(),
        retained_terminal,
    }))
}
