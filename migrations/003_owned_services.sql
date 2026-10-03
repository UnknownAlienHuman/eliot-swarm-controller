-- A separate authority for one explicitly admitted, fresh foreground service.
-- Core/workspace schema digests remain unchanged. Paths, auth, commands and
-- configuration bytes live only in private helper files, never this readback.
CREATE TABLE owned_service_starts (
    launch_operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
    open_operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    binding_id TEXT NOT NULL,
    binding_generation INTEGER NOT NULL CHECK(binding_generation > 0),
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    task_revision INTEGER NOT NULL CHECK(task_revision > 0),
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    lease_id TEXT NOT NULL REFERENCES workspace_leases(lease_id),
    lease_generation INTEGER NOT NULL CHECK(lease_generation > 0),
    technical_requester_id TEXT NOT NULL,
    effective_manager_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    service_version TEXT NOT NULL,
    route_digest TEXT NOT NULL CHECK(length(route_digest) = 64),
    binding_digest TEXT NOT NULL CHECK(length(binding_digest) = 64),
    intent_nonce TEXT NOT NULL UNIQUE,
    intent_digest TEXT NOT NULL CHECK(length(intent_digest) = 64),
    state TEXT NOT NULL CHECK(state IN
        ('reserved','outcome_unknown','service_observed','service_departed','failed_no_effect')),
    process_id INTEGER CHECK(process_id IS NULL OR process_id > 0),
    process_birth_token TEXT,
    executable_sha256 TEXT CHECK(executable_sha256 IS NULL OR length(executable_sha256) = 64),
    proof_json TEXT NOT NULL DEFAULT '{}'
        CHECK(json_valid(proof_json) AND length(proof_json) <= 8192),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(binding_id, binding_generation),
    FOREIGN KEY(binding_id, binding_generation) REFERENCES bindings(binding_id, generation)
) STRICT;

CREATE INDEX owned_service_starts_reconcile
    ON owned_service_starts(state, updated_at_ms, launch_operation_id);
