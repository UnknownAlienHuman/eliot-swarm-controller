-- O6 immutable named script revisions and direct invocation receipts.
-- The Store installs this as a digest-pinned additive extension.
CREATE TABLE scripts (
    script_id TEXT PRIMARY KEY NOT NULL,
    owner_id TEXT NOT NULL,
    active_revision INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (script_id, active_revision)
        REFERENCES script_revisions(script_id, revision)
) STRICT;

CREATE TABLE script_revisions (
    script_id TEXT NOT NULL REFERENCES scripts(script_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    bundle_ref TEXT NOT NULL REFERENCES artifacts(artifact_id),
    bundle_sha256 TEXT NOT NULL CHECK (length(bundle_sha256) = 64),
    interpreter_json TEXT NOT NULL CHECK (json_valid(interpreter_json)),
    validated_at_ms INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (script_id, revision),
    UNIQUE (bundle_ref)
) STRICT;

CREATE TABLE script_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    script_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    bundle_ref TEXT NOT NULL REFERENCES artifacts(artifact_id),
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    task_revision INTEGER NOT NULL CHECK (task_revision > 0),
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
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
    CHECK (state NOT IN ('completed', 'failed', 'incomplete') OR finished_at_ms IS NOT NULL),
    CHECK (state <> 'completed' OR (exit_code = 0 AND result_ref IS NOT NULL)),
    CHECK (state <> 'running' OR (started_at_ms IS NOT NULL AND process_identity_json IS NOT NULL))
) STRICT;

CREATE INDEX script_runs_pending ON script_runs(state, created_at_ms, run_id)
    WHERE state IN ('queued', 'sending', 'running', 'reconciling', 'outcome_unknown');
CREATE INDEX script_runs_task ON script_runs(task_id, attempt_id, created_at_ms, run_id);
