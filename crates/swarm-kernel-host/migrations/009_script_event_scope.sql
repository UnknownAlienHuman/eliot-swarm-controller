-- O6 event-only script invocation support.
--
-- Migration 004 remains immutable.  SQLite cannot alter a CHECK constraint or
-- remove NOT NULL from an existing column, so the installer rebuilds this
-- table inside its already-open Store transaction.  No deployed table has an
-- inbound foreign key to script_runs; all existing outbound references and
-- indexes are recreated below.
CREATE TABLE script_runs_event_scope (
    run_id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    script_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    bundle_ref TEXT NOT NULL REFERENCES artifacts(artifact_id),
    task_id TEXT REFERENCES tasks(task_id),
    task_revision INTEGER CHECK (task_revision IS NULL OR task_revision > 0),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    work_digest TEXT NOT NULL CHECK (length(work_digest) = 64),
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    state TEXT NOT NULL CHECK (state IN (
        'queued', 'sending', 'running', 'reconciling', 'completed', 'failed', 'incomplete', 'outcome_unknown'
    )),
    process_identity_json TEXT CHECK (process_identity_json IS NULL OR json_valid(process_identity_json)),
    result_ref TEXT REFERENCES artifacts(artifact_id),
    stdout_ref TEXT REFERENCES artifacts(artifact_id),
    stderr_ref TEXT REFERENCES artifacts(artifact_id),
    exit_code INTEGER,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY (script_id, revision) REFERENCES script_revisions(script_id, revision),
    CHECK ((task_id IS NULL AND task_revision IS NULL AND attempt_id IS NULL)
        OR (task_id IS NOT NULL AND task_revision IS NOT NULL AND attempt_id IS NOT NULL)),
    CHECK (state NOT IN ('completed', 'failed', 'incomplete') OR finished_at_ms IS NOT NULL),
    CHECK (state <> 'completed' OR (exit_code = 0 AND result_ref IS NOT NULL)),
    CHECK (state <> 'running' OR (started_at_ms IS NOT NULL AND process_identity_json IS NOT NULL))
) STRICT;

INSERT INTO script_runs_event_scope(
    run_id, operation_id, script_id, revision, bundle_ref,
    task_id, task_revision, attempt_id, work_digest, spec_json, state,
    process_identity_json, result_ref, stdout_ref, stderr_ref, exit_code,
    started_at_ms, finished_at_ms, created_at_ms
)
SELECT
    run_id, operation_id, script_id, revision, bundle_ref,
    task_id, task_revision, attempt_id, work_digest, spec_json, state,
    process_identity_json, result_ref, stdout_ref, stderr_ref, exit_code,
    started_at_ms, finished_at_ms, created_at_ms
FROM script_runs;

DROP TABLE script_runs;
ALTER TABLE script_runs_event_scope RENAME TO script_runs;

CREATE INDEX script_runs_pending ON script_runs(state, created_at_ms, run_id)
    WHERE state IN ('queued', 'sending', 'running', 'reconciling', 'outcome_unknown');
CREATE INDEX script_runs_task ON script_runs(task_id, attempt_id, created_at_ms, run_id);
