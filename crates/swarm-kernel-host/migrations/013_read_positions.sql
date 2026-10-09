-- Read cursors use the existing immutable Observation sequence. Historical
-- objects get a stable import position, not a claim about their commit order.
-- Private markers have no public payload and are excluded from report.delta.
INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms)
SELECT 'controller:read-position:operation',operation_id,'read.position','{}',created_at_ms
FROM operations ORDER BY created_at_ms,operation_id;
INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms)
SELECT 'controller:read-position:task',task_id,'read.position','{}',created_at_ms
FROM tasks ORDER BY created_at_ms,task_id;

-- Triggers cover every Store producer, including queued/rejected/coalesced
-- receipts. The position commits or rolls back with the admitted object.
CREATE TRIGGER operation_read_position AFTER INSERT ON operations BEGIN
    INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms)
    VALUES('controller:read-position:operation',NEW.operation_id,'read.position','{}',NEW.created_at_ms);
END;
CREATE TRIGGER task_read_position AFTER INSERT ON tasks BEGIN
    INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms)
    VALUES('controller:read-position:task',NEW.task_id,'read.position','{}',NEW.created_at_ms);
END;
CREATE INDEX operation_read_positions ON observations(observation_id,source_event_key)
    WHERE source_stream_id='controller:read-position:operation';
CREATE INDEX task_read_positions ON observations(observation_id,source_event_key)
    WHERE source_stream_id='controller:read-position:task';
