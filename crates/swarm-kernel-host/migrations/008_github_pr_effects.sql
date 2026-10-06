CREATE TABLE github_pr_effect_slots (
    repository_id INTEGER NOT NULL CHECK (repository_id > 0),
    pull_request_id INTEGER NOT NULL CHECK (pull_request_id > 0),
    head_sha TEXT NOT NULL CHECK (length(head_sha) IN (40,64)),
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    updated_at_ms INTEGER NOT NULL,
    -- The description is a property of the PR resource, not a particular
    -- commit. Keep the target head as evidence, but fence unknown writes for
    -- this PR across later head changes.
    PRIMARY KEY(repository_id,pull_request_id)
) WITHOUT ROWID;
