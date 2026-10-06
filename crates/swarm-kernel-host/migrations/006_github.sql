-- Optional, read-only GitHub Issue source and manager-selected work pool.
-- Writes from this module are local Store state and Task Operations only;
-- there is intentionally no GitHub mutation credential or endpoint here.
CREATE TABLE github_sources (
    source_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    host TEXT NOT NULL,
    owner TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    repository_id INTEGER NOT NULL CHECK (repository_id > 0),
    next_page INTEGER NOT NULL DEFAULT 1 CHECK (next_page > 0),
    poll_generation INTEGER NOT NULL DEFAULT 0 CHECK (poll_generation >= 0),
    last_poll_status TEXT NOT NULL DEFAULT 'never'
        CHECK (last_poll_status IN ('never', 'polling', 'complete', 'partial', 'failed')),
    last_poll_error_json TEXT CHECK (last_poll_error_json IS NULL OR json_valid(last_poll_error_json)),
    last_coverage_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(last_coverage_json)),
    last_poll_operation_id TEXT REFERENCES operations(operation_id),
    last_poll_started_at_ms INTEGER,
    last_poll_finished_at_ms INTEGER,
    created_by TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (host, repository_id)
) STRICT;

CREATE TABLE github_issue_items (
    source_id TEXT NOT NULL REFERENCES github_sources(source_id),
    issue_id INTEGER NOT NULL CHECK (issue_id > 0),
    issue_number INTEGER NOT NULL CHECK (issue_number > 0),
    current_fact_digest TEXT NOT NULL,
    current_event_key TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    task_id TEXT REFERENCES tasks(task_id),
    mapping_status TEXT NOT NULL
        CHECK (mapping_status IN ('pending', 'mapped', 'task_spec_conflict', 'task_not_revisable', 'task_creation_rejected')),
    mapping_error_json TEXT CHECK (mapping_error_json IS NULL OR json_valid(mapping_error_json)),
    applied_task_spec_digest TEXT,
    observed_generation INTEGER NOT NULL CHECK (observed_generation > 0),
    last_seen_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, issue_id),
    UNIQUE (source_id, task_id)
) STRICT;

CREATE TABLE github_issue_facts (
    source_id TEXT NOT NULL REFERENCES github_sources(source_id),
    issue_id INTEGER NOT NULL CHECK (issue_id > 0),
    event_key TEXT NOT NULL,
    previous_event_key TEXT,
    fact_digest TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    observed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, event_key)
) STRICT;
CREATE INDEX github_issue_facts_by_item
    ON github_issue_facts(source_id, issue_id, observed_at_ms, event_key);

CREATE TABLE github_work_pool_members (
    source_id TEXT NOT NULL REFERENCES github_sources(source_id),
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    issue_id INTEGER NOT NULL CHECK (issue_id > 0),
    selected INTEGER NOT NULL DEFAULT 0 CHECK (selected IN (0, 1)),
    selection_order INTEGER CHECK (selection_order IS NULL OR selection_order >= 0),
    discovered_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, task_id),
    UNIQUE (source_id, issue_id),
    CHECK ((selected = 0 AND selection_order IS NULL) OR (selected = 1 AND selection_order IS NOT NULL))
) STRICT;
CREATE INDEX github_work_pool_order
    ON github_work_pool_members(source_id, selected DESC, selection_order, task_id);

-- A GitHub poll read can be resumed only with its original Operation/request
-- ID. This prevents another request from silently stealing an interrupted
-- read; sending/outcome_unknown polls are safe to repeat because they are GETs.
CREATE TABLE github_poll_leases (
    source_id TEXT PRIMARY KEY NOT NULL REFERENCES github_sources(source_id),
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    started_at_ms INTEGER NOT NULL
) STRICT;
