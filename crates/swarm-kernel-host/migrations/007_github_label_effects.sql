-- Durable semantic slot for one exact source-mapped Issue/managed-label
-- desired state. The ordinary Operation remains the history and receipt.
CREATE TABLE github_label_effect_slots (
    source_id TEXT NOT NULL,
    issue_id INTEGER NOT NULL CHECK (issue_id > 0),
    label TEXT NOT NULL CHECK (length(label) BETWEEN 10 AND 50),
    desired_present INTEGER NOT NULL CHECK (desired_present IN (0, 1)),
    operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, issue_id, label),
    FOREIGN KEY (source_id, issue_id)
        REFERENCES github_issue_items(source_id, issue_id)
) STRICT;
