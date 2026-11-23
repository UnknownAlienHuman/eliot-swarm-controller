-- Store-private continuation lineage and bounded automatic handoff state.
-- Deliberately no foreign keys: retention of Agent/event rows must not erase
-- the lineage or reset its budget.
CREATE TABLE handoff_task_lineages (
    agent INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    lineage_agent INTEGER NOT NULL CHECK (lineage_agent > 0),
    successor_agent INTEGER CHECK (successor_agent IS NULL OR successor_agent > 0),
    PRIMARY KEY (agent, generation)
);
CREATE INDEX handoff_task_lineages_lineage
    ON handoff_task_lineages(lineage_agent, agent, generation);

CREATE TABLE automatic_handoff_budgets (
    lineage_agent INTEGER PRIMARY KEY CHECK (lineage_agent > 0),
    automatic_attempts INTEGER NOT NULL DEFAULT 0 CHECK (automatic_attempts >= 0),
    last_agent INTEGER,
    last_generation INTEGER,
    last_switch_command TEXT,
    last_state TEXT CHECK (last_state IS NULL OR last_state IN
        ('reserved', 'rejected', 'refused', 'failed', 'unknown', 'applied')),
    last_updated_at INTEGER,
    CHECK (
        (last_state IS NULL AND last_agent IS NULL AND last_generation IS NULL
            AND last_updated_at IS NULL)
        OR
        (last_state IS NOT NULL AND last_agent IS NOT NULL
            AND last_generation IS NOT NULL AND last_updated_at IS NOT NULL)
    )
);

INSERT INTO schema_migrations(version) VALUES (109);
