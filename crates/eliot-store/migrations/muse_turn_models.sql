-- Event retention deletes a prefix. A retained exact start establishes that
-- any later mismatch observation for that turn is still in the journal.
-- Turns without that evidence keep their existing readback.
WITH mismatches AS MATERIALIZED (
    SELECT agent, generation, json_extract(data, '$.turn_id') AS turn_id
    FROM events
    WHERE kind = 'turn.model_observed' AND source = 'stream'
      AND json_type(data, '$.model_mismatch') = 'object'
), readiness AS MATERIALIZED (
    SELECT agent, generation, json_extract(data, '$.effective.model') AS model
    FROM events
    WHERE kind = 'session.ready' AND source = 'stream'
), selectors AS MATERIALIZED (
    SELECT t.rowid AS turn_row, COALESCE(
        (SELECT json_extract(c.effective, '$.model')
         FROM agent_config AS c
         WHERE c.agent = t.agent AND c.generation = t.generation
           AND c.revision <= t.config_revision AND json_type(c.effective, '$.model') = 'text'
         ORDER BY c.revision DESC LIMIT 1),
        CASE WHEN t.generation = a.generation
             THEN json_extract(a.effective, '$.session_admitted_model') END,
        r.model
    ) AS model
    FROM events AS started
    JOIN turns AS t ON t.agent = started.agent AND t.generation = started.generation
        AND t.turn_id = json_extract(started.data, '$.turn_id')
    JOIN agents AS a ON a.id = t.agent
    LEFT JOIN readiness AS r ON r.agent = t.agent AND r.generation = t.generation
    LEFT JOIN mismatches AS m ON m.agent = t.agent AND m.generation = t.generation
        AND m.turn_id = t.turn_id
    WHERE started.kind = 'turn.started' AND started.source = 'stream'
      AND a.harness = 'muse' AND a.observe_only = 0
      AND t.effective_model IS NULL AND m.turn_id IS NULL
)
UPDATE turns SET effective_model = selectors.model
FROM selectors
WHERE turns.rowid = selectors.turn_row AND selectors.model IS NOT NULL;
