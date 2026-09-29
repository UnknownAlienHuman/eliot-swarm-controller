-- A narrow SQL part of Store::begin_send for a canonical initial task.dispatch.
-- Not a complete Store or a generic rule for reply/configure/native.* commands.
-- Caller begins a transaction and checks authority, relevant settings, dependencies,
-- and successful prerequisite outcome before this statement in that transaction.
-- Caller finishes the statement and COMMITs before returning a DispatchTicket.
UPDATE operations AS op
SET state = 'sending', sent_at_ms = :now_ms, updated_at_ms = :now_ms
WHERE op.operation_id = :operation_id
  AND op.method = 'task.dispatch'
  AND op.state = 'queued'
  AND op.due_at_ms <= :now_ms
  AND COALESCE((SELECT json_extract(value_json, '$.new_work')
                FROM meta WHERE key = 'execution_mode'), 'disabled') = 'enabled'
  AND EXISTS (
    SELECT 1 FROM attempts AS a
    JOIN tasks AS t ON t.task_id = a.task_id
    WHERE a.attempt_id = op.attempt_id
      AND a.task_id = op.task_id
      AND a.start_operation_id = op.operation_id
      AND a.released_at_ms IS NULL
      AND a.state = 'reserved'
      AND t.state = 'open'
      AND t.revision = a.task_revision
      AND a.binding_id = op.binding_id
      AND a.binding_generation = op.binding_generation
  )
  AND EXISTS (
    SELECT 1 FROM bindings AS b
    WHERE b.binding_id = op.binding_id
      AND b.generation = op.binding_generation
      AND b.state = 'ready'
      AND b.released_at_ms IS NULL
  )
RETURNING operation_id;
