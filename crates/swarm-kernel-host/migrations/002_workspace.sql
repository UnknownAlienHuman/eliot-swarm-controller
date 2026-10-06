-- Additive extension. The version-1 core and its digest remain unchanged.
CREATE TABLE workspace_registrations (
    registration_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL UNIQUE,
    trusted_repository TEXT NOT NULL,
    repository_path TEXT NOT NULL,
    allowed_roots_json TEXT NOT NULL CHECK(json_valid(allowed_roots_json)),
    registration_digest TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    authorized_by TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','revoked')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE workspace_leases (
    lease_id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL REFERENCES workspace_registrations(registration_id),
    registration_generation INTEGER NOT NULL CHECK(registration_generation > 0),
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    task_revision INTEGER NOT NULL CHECK(task_revision > 0),
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    plan_digest TEXT NOT NULL,
    owner_client_id TEXT NOT NULL,
    attempt_id TEXT REFERENCES attempts(attempt_id),
    allowed_paths_json TEXT NOT NULL CHECK(json_valid(allowed_paths_json)),
    allowed_symbols_json TEXT NOT NULL CHECK(json_valid(allowed_symbols_json)),
    baseline_commit TEXT NOT NULL,
    branch_ref TEXT NOT NULL,
    worktree_handle TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    clean_state_json TEXT NOT NULL CHECK(json_valid(clean_state_json)),
    generation INTEGER NOT NULL CHECK(generation > 0),
    binding_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('preparing','held','outcome_unknown','released','stale')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX workspace_leases_one_task
    ON workspace_leases(task_id)
    WHERE state IN ('preparing','held','outcome_unknown');
CREATE INDEX workspace_leases_pending ON workspace_leases(state, updated_at_ms, lease_id);

-- Lease lifecycle probes use exact Task/Attempt and effect state, without
-- scanning the unrelated operation history.
CREATE INDEX workspace_task_operation_state
    ON operations(task_id, state, attempt_id, binding_id, binding_generation);
CREATE INDEX workspace_attempt_operation_state
    ON operations(attempt_id, state, binding_id, binding_generation);

CREATE INDEX controller_observation_stream_kind_cursor
    ON observations(source_stream_id, kind, observation_id);
