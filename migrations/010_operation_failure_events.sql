-- Normalize committed failed/unknown Operation states into closed event facts.
--
-- These triggers run in the same SQLite transaction as the Operation state
-- transition. They intentionally retain only fixed status metadata; request,
-- result, error, provider, and credential material stay on their existing
-- private/readback paths. There is no historical backfill.
CREATE TRIGGER operation_failure_event_after_insert
AFTER INSERT ON operations
WHEN NEW.state IN ('rejected', 'outcome_unknown')
BEGIN
    INSERT OR IGNORE INTO observations(
        source_stream_id, source_event_key, operation_id, kind, payload_json,
        recorded_at_ms
    )
    VALUES(
        'controller:operations',
        'operation:' || NEW.operation_id || ':' ||
            CASE NEW.state
                WHEN 'rejected' THEN 'operation_rejected'
                ELSE 'operation_outcome_unknown'
            END,
        NEW.operation_id,
        CASE NEW.state
            WHEN 'rejected' THEN 'operation.rejected'
            ELSE 'operation.outcome_unknown'
        END,
        json_object(
            'schema_version', 1,
            'phase', CASE NEW.state
                WHEN 'rejected' THEN 'operation_rejected'
                ELSE 'operation_outcome_unknown'
            END,
            'status', CASE NEW.state
                WHEN 'rejected' THEN 'rejected'
                ELSE 'unknown'
            END,
            'occurrence_id', 'operation:' || NEW.operation_id || ':' ||
                CASE NEW.state
                    WHEN 'rejected' THEN 'operation_rejected'
                    ELSE 'operation_outcome_unknown'
                END,
            'error_code', CASE NEW.state
                WHEN 'rejected' THEN 'OPERATION_REJECTED'
                ELSE 'OUTCOME_UNKNOWN'
            END
        ),
        CAST(unixepoch('subsec') * 1000 AS INTEGER)
    );
END;

CREATE TRIGGER operation_failure_event_after_state_update
AFTER UPDATE OF state ON operations
WHEN OLD.state IS NOT NEW.state
    AND NEW.state IN ('rejected', 'outcome_unknown')
BEGIN
    INSERT OR IGNORE INTO observations(
        source_stream_id, source_event_key, operation_id, kind, payload_json,
        recorded_at_ms
    )
    VALUES(
        'controller:operations',
        'operation:' || NEW.operation_id || ':' ||
            CASE NEW.state
                WHEN 'rejected' THEN 'operation_rejected'
                ELSE 'operation_outcome_unknown'
            END,
        NEW.operation_id,
        CASE NEW.state
            WHEN 'rejected' THEN 'operation.rejected'
            ELSE 'operation.outcome_unknown'
        END,
        json_object(
            'schema_version', 1,
            'phase', CASE NEW.state
                WHEN 'rejected' THEN 'operation_rejected'
                ELSE 'operation_outcome_unknown'
            END,
            'status', CASE NEW.state
                WHEN 'rejected' THEN 'rejected'
                ELSE 'unknown'
            END,
            'occurrence_id', 'operation:' || NEW.operation_id || ':' ||
                CASE NEW.state
                    WHEN 'rejected' THEN 'operation_rejected'
                    ELSE 'operation_outcome_unknown'
                END,
            'error_code', CASE NEW.state
                WHEN 'rejected' THEN 'OPERATION_REJECTED'
                ELSE 'OUTCOME_UNKNOWN'
            END
        ),
        CAST(unixepoch('subsec') * 1000 AS INTEGER)
    );
END;
