-- ELIOT Swarm Prototype: NEW reference schema for C01, 2026-09-29.
-- v18: canonical Task origin, Attempt-scoped active check reuse, explicit resource release.
-- Not recovered from implementation-v1.zip. No Rust Store exists yet.
-- Apply only to an empty, prototype-owned DB after the host singleton is held.
-- Production startup verifies the bundled SQLite version and foreign_keys/WAL/FULL.
-- Lifecycle guards, JSON shapes, authority and artifact validation belong to Store.
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
BEGIN IMMEDIATE;

CREATE TABLE meta (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL CHECK (json_valid(value_json))
) STRICT;

CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
    content_digest TEXT,
    created_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json))
) STRICT;

CREATE TABLE tasks (
    task_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    -- Imported root work identity, resolved by the forge adapter, not a display URL.
    origin_key TEXT CHECK (origin_key IS NULL OR length(origin_key) > 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    state TEXT NOT NULL CHECK (state IN ('open', 'accepted', 'archived')),
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    accepted_attempt_id TEXT REFERENCES attempts(attempt_id),
    accepted_revision INTEGER,
    accepted_phase TEXT,
    accepted_candidate_ref TEXT REFERENCES artifacts(artifact_id),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (
      (accepted_attempt_id IS NULL AND accepted_revision IS NULL
       AND accepted_phase IS NULL AND accepted_candidate_ref IS NULL)
      OR
      (accepted_attempt_id IS NOT NULL AND accepted_revision IS NOT NULL
       AND accepted_phase IS NOT NULL AND accepted_candidate_ref IS NOT NULL
       AND accepted_revision = revision)
    ),
    CHECK (state <> 'accepted' OR accepted_attempt_id IS NOT NULL),
    CHECK (state <> 'open' OR accepted_attempt_id IS NULL)
) STRICT;

CREATE UNIQUE INDEX one_task_per_origin ON tasks(origin_key) WHERE origin_key IS NOT NULL;

-- A row is one native generation. Reconnecting IPC does NOT insert a generation.
CREATE TABLE bindings (
    binding_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    lane_id TEXT NOT NULL,
    module_instance_id TEXT NOT NULL,
    module_artifact_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('opening', 'ready', 'reconciling', 'draining', 'closed')),
    native_scope_key TEXT,
    native_root_id TEXT,
    route_json TEXT NOT NULL CHECK (json_valid(route_json)),
    state_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(state_json)),
    released_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (binding_id, generation),
    CHECK ((native_scope_key IS NULL AND native_root_id IS NULL)
        OR (native_scope_key IS NOT NULL AND native_root_id IS NOT NULL
            AND length(native_scope_key) > 0 AND length(native_root_id) > 0)),
    CHECK (state <> 'closed' OR released_at_ms IS NOT NULL),
    CHECK (released_at_ms IS NULL OR state = 'closed')
) STRICT;
CREATE UNIQUE INDEX one_live_root_per_lane
    ON bindings(lane_id) WHERE released_at_ms IS NULL;
-- Key resolved from native namespace, not route/model/port/bridge aliases.
CREATE UNIQUE INDEX one_control_owner_per_native_root
    ON bindings(native_scope_key, native_root_id)
    WHERE released_at_ms IS NULL AND native_root_id IS NOT NULL;

CREATE TABLE attempts (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    task_revision INTEGER NOT NULL CHECK (task_revision > 0),
    task_snapshot_json TEXT NOT NULL CHECK (json_valid(task_snapshot_json)),
    owner_id TEXT NOT NULL,
    -- One logical initial delivery; subsequent corrections are separate messages.
    start_operation_id TEXT UNIQUE REFERENCES operations(operation_id),
    binding_id TEXT,
    binding_generation INTEGER,
    state TEXT NOT NULL CHECK (state IN (
      'reserved', 'running', 'submitted', 'needs_correction',
      'recovery_pending', 'accepted', 'failed', 'cancelled', 'superseded'
    )),
    producers_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(producers_json)),
    submission_ref TEXT REFERENCES artifacts(artifact_id),
    candidate_ref TEXT REFERENCES artifacts(artifact_id),
    released_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (binding_id, binding_generation)
        REFERENCES bindings(binding_id, generation),
    CHECK ((binding_id IS NULL AND binding_generation IS NULL)
        OR (binding_id IS NOT NULL AND binding_generation IS NOT NULL)),
    CHECK (state NOT IN ('submitted', 'accepted')
        OR (submission_ref IS NOT NULL AND candidate_ref IS NOT NULL)),
    CHECK (released_at_ms IS NULL
        OR state IN ('accepted', 'failed', 'cancelled', 'superseded'))
) STRICT;
CREATE UNIQUE INDEX one_owner_per_task
    ON attempts(task_id) WHERE released_at_ms IS NULL;

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    caller_id TEXT NOT NULL,
    client_request_id TEXT NOT NULL,
    method TEXT NOT NULL,
    original_request_json TEXT NOT NULL CHECK (json_valid(original_request_json)),
    effective_request_json TEXT NOT NULL CHECK (json_valid(effective_request_json)),
    task_id TEXT REFERENCES tasks(task_id),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    binding_id TEXT,
    binding_generation INTEGER,
    prerequisite_operation_id TEXT REFERENCES operations(operation_id),
    state TEXT NOT NULL CHECK (state IN (
      'queued', 'sending', 'native_accepted', 'settled',
      'rejected', 'cancelled', 'outcome_unknown'
    )),
    native_refs_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(native_refs_json)),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    due_at_ms INTEGER NOT NULL,
    sent_at_ms INTEGER,
    settled_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (caller_id, client_request_id),
    FOREIGN KEY (binding_id, binding_generation)
        REFERENCES bindings(binding_id, generation),
    CHECK ((binding_id IS NULL AND binding_generation IS NULL)
        OR (binding_id IS NOT NULL AND binding_generation IS NOT NULL)),
    CHECK ((state IN ('settled', 'rejected', 'cancelled') AND settled_at_ms IS NOT NULL)
        OR (state NOT IN ('settled', 'rejected', 'cancelled') AND settled_at_ms IS NULL)),
    CHECK (state NOT IN ('settled', 'rejected', 'cancelled') OR result_json IS NOT NULL)
) STRICT;
CREATE INDEX due_operations ON operations(due_at_ms, operation_id) WHERE state = 'queued';
CREATE INDEX unresolved_target_operations ON operations(binding_id, binding_generation, state);

CREATE TABLE observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_stream_id TEXT NOT NULL,
    source_event_key TEXT,
    binding_id TEXT,
    binding_generation INTEGER,
    operation_id TEXT REFERENCES operations(operation_id),
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    recorded_at_ms INTEGER NOT NULL,
    FOREIGN KEY (binding_id, binding_generation)
        REFERENCES bindings(binding_id, generation),
    CHECK ((binding_id IS NULL AND binding_generation IS NULL)
        OR (binding_id IS NOT NULL AND binding_generation IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX observation_source_dedupe
    ON observations(source_stream_id, source_event_key) WHERE source_event_key IS NOT NULL;
CREATE INDEX binding_observations ON observations(binding_id, binding_generation, observation_id);

CREATE TABLE check_runs (
    check_id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    candidate_ref TEXT NOT NULL REFERENCES artifacts(artifact_id),
    cache_key TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    -- Process result is not resource release; an incomplete result may retain its claim.
    resource_claimed_at_ms INTEGER,
    resource_released_at_ms INTEGER,
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    state TEXT NOT NULL CHECK (state IN (
      'queued', 'running', 'reconciling', 'passed', 'failed', 'error', 'incomplete', 'cancelled'
    )),
    process_identity_json TEXT CHECK (process_identity_json IS NULL OR json_valid(process_identity_json)),
    exit_code INTEGER,
    coverage_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(coverage_json)),
    result_ref TEXT REFERENCES artifacts(artifact_id),
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    CHECK (state NOT IN ('passed', 'failed', 'error', 'incomplete', 'cancelled')
        OR (finished_at_ms IS NOT NULL AND result_ref IS NOT NULL)),
    CHECK (resource_released_at_ms IS NULL OR resource_claimed_at_ms IS NOT NULL),
    CHECK (state <> 'queued' OR resource_claimed_at_ms IS NULL),
    CHECK (state NOT IN ('running', 'reconciling')
        OR (resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL)),
    CHECK (resource_released_at_ms IS NULL
        OR state IN ('passed', 'failed', 'error', 'incomplete', 'cancelled')),
    CHECK (state <> 'passed' OR (exit_code IS NOT NULL AND exit_code = 0
        AND resource_released_at_ms IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX one_active_check ON check_runs(attempt_id, cache_key)
    WHERE state IN ('queued', 'running', 'reconciling')
       OR (resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL);
CREATE UNIQUE INDEX one_check_writer_per_resource ON check_runs(resource_key)
    WHERE resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL;

CREATE TABLE incidents (
    incident_id TEXT PRIMARY KEY NOT NULL,
    dedup_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'resolved')),
    occurrences INTEGER NOT NULL CHECK (occurrences > 0),
    evidence_ref TEXT REFERENCES artifacts(artifact_id),
    action_operation_id TEXT REFERENCES operations(operation_id),
    details_json TEXT NOT NULL CHECK (json_valid(details_json)),
    opened_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL
) STRICT;
CREATE UNIQUE INDEX one_open_incident ON incidents(dedup_key) WHERE state = 'open';

PRAGMA user_version = 1;
COMMIT;
