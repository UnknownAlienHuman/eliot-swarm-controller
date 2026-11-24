-- Match the turns.turn_id TEXT comparison. Keep the raw key before seq so
-- legacy values with the same text still walk in their previous order.
DROP INDEX attention_turn_endpoints;
CREATE INDEX attention_turn_endpoints
    ON events(agent, generation, kind, CAST(json_extract(data, '$.turn_id') AS TEXT),
              json_extract(data, '$.turn_id'), seq)
    WHERE kind = 'turn.started' OR kind = 'turn.completed';

CREATE INDEX events_generation_custody ON events(agent, generation, kind)
    WHERE kind = 'capacity.released' OR kind = 'agent.handed_off';
