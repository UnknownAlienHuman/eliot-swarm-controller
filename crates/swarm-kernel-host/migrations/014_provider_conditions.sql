CREATE TABLE provider_conditions (
    scope_key TEXT NOT NULL CHECK (length(scope_key) > 0),
    bucket_id TEXT NOT NULL DEFAULT '',
    source_model_slug TEXT CHECK (source_model_slug IS NULL OR length(source_model_slug) > 0),
    route_alias TEXT NOT NULL CHECK (length(route_alias) > 0),
    runtime_id TEXT NOT NULL CHECK (length(runtime_id) > 0),
    service_id TEXT CHECK (service_id IS NULL OR length(service_id) > 0),
    binding_id TEXT NOT NULL CHECK (length(binding_id) > 0),
    binding_generation INTEGER NOT NULL CHECK (binding_generation > 0),
    native_scope_key TEXT NOT NULL CHECK (length(native_scope_key) > 0),
    provider_id TEXT CHECK (provider_id IS NULL OR length(provider_id) > 0),
    account_id TEXT CHECK (account_id IS NULL OR length(account_id) > 0),
    model_id TEXT CHECK (model_id IS NULL OR length(model_id) > 0),
    condition_kind TEXT NOT NULL CHECK (condition_kind IN (
        'available', 'rate_limited', 'quota_exhausted', 'model_gone',
        'data_policy_required', 'auth_required', 'overloaded', 'unknown'
    )),
    retry_at_ms INTEGER,
    reset_at_ms INTEGER,
    unknown_native_class TEXT,
    observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
    sequence_kind TEXT NOT NULL CHECK (sequence_kind IN ('native', 'local_collection_revision')),
    native_sequence TEXT NOT NULL CHECK (length(native_sequence) > 0),
    source_connection_id TEXT,
    evidence_method TEXT,
    evidence_revision INTEGER,
    auth_context_ref TEXT CHECK (
        auth_context_ref IS NULL OR
        (length(auth_context_ref) = 64 AND auth_context_ref NOT GLOB '*[^0-9a-fA-F]*')
    ),
    source_module_sequence INTEGER NOT NULL CHECK (source_module_sequence >= 0),
    source_epoch TEXT NOT NULL CHECK (length(source_epoch) > 0),
    source_observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
    details_digest TEXT NOT NULL CHECK (
        length(details_digest) = 64 AND details_digest NOT GLOB '*[^0-9a-fA-F]*'
    ),
    PRIMARY KEY (scope_key, binding_id, binding_generation, bucket_id),
    CHECK ((bucket_id = '' AND source_model_slug IS NULL)
        OR (bucket_id <> '')),
    CHECK ((condition_kind IN ('rate_limited', 'overloaded') AND reset_at_ms IS NULL)
        OR (condition_kind NOT IN ('rate_limited', 'overloaded') AND retry_at_ms IS NULL)),
    CHECK ((condition_kind = 'quota_exhausted' AND retry_at_ms IS NULL)
        OR (condition_kind <> 'quota_exhausted' AND reset_at_ms IS NULL)),
    CHECK ((condition_kind = 'unknown' AND unknown_native_class IS NOT NULL
            AND length(unknown_native_class) BETWEEN 1 AND 128)
        OR (condition_kind <> 'unknown' AND unknown_native_class IS NULL)),
    CHECK (sequence_kind <> 'local_collection_revision' OR
        (source_connection_id IS NOT NULL AND evidence_revision > 0
         AND native_sequence = CAST(evidence_revision AS TEXT))),
    CHECK (condition_kind <> 'available' OR
        (sequence_kind = 'local_collection_revision'
         AND evidence_method = 'account/rateLimits/read'
         AND evidence_revision > 0
         AND source_connection_id IS NOT NULL
         AND auth_context_ref IS NOT NULL))
) STRICT;

CREATE INDEX provider_conditions_binding
    ON provider_conditions(scope_key, binding_id, binding_generation, source_observation_id);
