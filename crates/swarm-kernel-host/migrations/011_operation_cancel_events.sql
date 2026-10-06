-- Normalize each newly committed Operation cancellation into one safe,
-- durable observation. The Operation result retains the private reason and
-- actor; the event carries only closed status metadata for generic routing.
-- Existing cancellations are not backfilled: appending old occurrences could
-- cross active ScriptRun cursors and replay work predating this event contract.

CREATE TRIGGER operation_cancel_event_after_insert
AFTER INSERT ON operations
WHEN NEW.state = 'cancelled'
BEGIN
    INSERT OR IGNORE INTO observations(
        source_stream_id, source_event_key, operation_id, kind, payload_json,
        recorded_at_ms
    )
    VALUES(
        'controller:operations',
        'operation:' || NEW.operation_id || ':operation_cancelled',
        NEW.operation_id,
        'operation.cancelled',
        json_object(
            'schema_version', 1,
            'phase', 'operation_cancelled',
            'status', 'cancelled',
            'occurrence_id', 'operation:' || NEW.operation_id || ':operation_cancelled',
            'error_code', 'OPERATION_CANCELLED'
        ),
        CAST(unixepoch('subsec') * 1000 AS INTEGER)
    );
END;

CREATE TRIGGER operation_cancel_event_after_state_update
AFTER UPDATE OF state ON operations
WHEN OLD.state IS NOT NEW.state
    AND NEW.state = 'cancelled'
BEGIN
    INSERT OR IGNORE INTO observations(
        source_stream_id, source_event_key, operation_id, kind, payload_json,
        recorded_at_ms
    )
    VALUES(
        'controller:operations',
        'operation:' || NEW.operation_id || ':operation_cancelled',
        NEW.operation_id,
        'operation.cancelled',
        json_object(
            'schema_version', 1,
            'phase', 'operation_cancelled',
            'status', 'cancelled',
            'occurrence_id', 'operation:' || NEW.operation_id || ':operation_cancelled',
            'error_code', 'OPERATION_CANCELLED'
        ),
        CAST(unixepoch('subsec') * 1000 AS INTEGER)
    );
END;
