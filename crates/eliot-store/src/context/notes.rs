use crate::{Result, sql_error};
use eliot_core::ContextNoteCounters;
use rusqlite::{Connection, params};

const PROBE_LIMIT: i64 = 10_001;

pub(crate) const MOVE_EVENTS: &str = "kind = 'context.moved' OR (
    kind = 'command.accepted' AND json_extract(data, '$.outcome.delivery') = 'moved'
    AND json_type(data, '$.note_kind') = 'text')";

/// Count each outcome in the reporting window. A note that moves and is later
/// delivered contributes once to each outcome at its own timestamp.
pub(crate) fn read_counters(
    connection: &Connection,
    since: i64,
    until: i64,
) -> Result<(Vec<ContextNoteCounters>, bool)> {
    let mut statement = connection.prepare(&format!(
        "WITH settled AS (
            SELECT id, state = 'applied' AS delivered, 0 AS moved,
                   state <> 'applied' AS cancelled
            FROM commands INDEXED BY commands_fact_note_settlements
            WHERE verb = 'inject' AND principal GLOB 'swarm_note:*'
              AND updated_at BETWEEN ?1 AND ?2
              AND state IN ('applied', 'cancelled', 'rejected', 'failed')
            ORDER BY updated_at, id LIMIT ?3
         ), moved AS (
            SELECT note.id, 0 delivered, 1 moved, 0 cancelled
            FROM events INDEXED BY events_context_moves
            JOIN commands AS note ON note.id = json_extract(data, '$.command_id')
            WHERE ({MOVE_EVENTS}) AND ts BETWEEN ?1 AND ?2
              AND note.verb = 'inject' AND note.principal GLOB 'swarm_note:*'
            ORDER BY ts, seq LIMIT ?3
         ), outcomes AS (
            SELECT * FROM settled UNION ALL SELECT * FROM moved
         )
         SELECT substr(c.principal, 12) AS kind,
                sum(outcome.delivered), sum(outcome.moved), sum(outcome.cancelled),
                    sum(CASE WHEN outcome.delivered AND (
                        (c.principal = 'swarm_note:no_check_before_end' AND EXISTS (
                            SELECT 1 FROM events AS checked INDEXED BY events_agent
                            WHERE checked.agent = c.target AND checked.generation = c.generation
                            AND checked.ts > c.updated_at AND checked.ts <= ?2
                            AND checked.kind IN ('goal.round', 'project.check.finished')
                            AND json_type(checked.data, '$.exit_code') = 'integer'
                        )) OR (c.principal = 'swarm_note:outside_scope' AND EXISTS (
                            SELECT 1 FROM events AS restored INDEXED BY events_agent
                            WHERE restored.agent = c.target AND restored.generation = c.generation
                            AND restored.ts > c.updated_at AND restored.ts <= ?2 AND restored.kind = 'git.changed'
                            AND json_extract(restored.data, '$.complete') = 1
                            AND json_extract(restored.data, '$.base_complete') = 1
                            AND json_array_length(restored.data, '$.dirty') = 0
                            AND json_array_length(restored.data, '$.committed_paths') = 0
                        )) OR (c.principal = 'swarm_note:repeated_command_failure' AND EXISTS (
                            SELECT 1 FROM events AS admitted INDEXED BY events_agent
                            JOIN events AS failed ON failed.seq = json_extract(admitted.data, '$.trigger_seq')
                            JOIN events AS original INDEXED BY events_agent
                              ON original.agent = c.target AND original.generation = c.generation
                              AND original.source = failed.source AND original.kind = 'tool.started'
                              AND original.seq < failed.seq AND original.native_id IS NOT NULL
                              AND json_extract(original.data, '$.item_id') = json_extract(failed.data, '$.item_id')
                              AND json_extract(original.data, '$.turn_id') = json_extract(failed.data, '$.turn_id')
                            JOIN events AS succeeded INDEXED BY events_agent
                              ON succeeded.agent = c.target AND succeeded.generation = c.generation
                              AND succeeded.ts > c.updated_at AND succeeded.ts <= ?2 AND succeeded.kind = 'tool.completed'
                              AND succeeded.native_id IS NOT NULL
                              AND json_extract(succeeded.data, '$.status') = 'completed'
                              AND json_extract(succeeded.data, '$.exit_code') = 0
                            JOIN events AS retried INDEXED BY events_agent
                              ON retried.agent = c.target AND retried.generation = c.generation
                              AND retried.kind = 'tool.started' AND retried.native_id IS NOT NULL
                              AND retried.source = succeeded.source AND retried.seq < succeeded.seq
                              AND json_extract(retried.data, '$.turn_id') = json_extract(succeeded.data, '$.turn_id')
                              AND json_extract(retried.data, '$.item_id') = json_extract(succeeded.data, '$.item_id')
                              AND json_extract(retried.data, '$.command') = json_extract(original.data, '$.command')
                              AND json_extract(retried.data, '$.command_truncated') = 0
                            WHERE admitted.agent = c.target AND admitted.generation = c.generation
                              AND admitted.kind = 'command.accepted'
                              AND json_extract(admitted.data, '$.command_id') = c.id
                        ))
                    ) THEN 1 ELSE 0 END)
         FROM outcomes AS outcome JOIN commands AS c ON c.id = outcome.id
         GROUP BY kind ORDER BY kind"
    )).map_err(sql_error)?;
    let rows = statement
        .query_map(params![since, until, PROBE_LIMIT], |row| {
            Ok(ContextNoteCounters {
                kind: row.get(0)?,
                delivered: u64::from(row.get::<_, u32>(1)?),
                moved: u64::from(row.get::<_, u32>(2)?),
                cancelled: u64::from(row.get::<_, u32>(3)?),
                resolved: u64::from(row.get::<_, u32>(4)?),
            })
        })
        .map_err(sql_error)?;
    let counts = rows
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let settled = counts
        .iter()
        .map(|count| count.delivered + count.cancelled)
        .sum::<u64>();
    let moved = counts.iter().map(|count| count.moved).sum::<u64>();
    let truncated = settled >= PROBE_LIMIT as u64 || moved >= PROBE_LIMIT as u64;
    Ok((counts, truncated))
}
