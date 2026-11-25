-- Existing scopes and task lists begin at revision zero. New writes change
-- the corresponding revision atomically; upgrade does not scan either list.
CREATE TABLE var_scope_revisions (
    scope TEXT PRIMARY KEY,
    revision INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE git_work_task_revisions (
    agent INTEGER NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY(agent, generation)
) WITHOUT ROWID;

CREATE TRIGGER git_work_task_revision_insert AFTER INSERT ON git_work_tasks
BEGIN
    INSERT INTO git_work_task_revisions VALUES (NEW.agent, NEW.generation, 1)
    ON CONFLICT(agent,generation) DO UPDATE SET revision=revision+1;
END;

CREATE TRIGGER git_work_task_revision_binding
AFTER UPDATE OF task_ref, principal ON git_work_tasks
WHEN NEW.task_ref IS NOT OLD.task_ref OR NEW.principal IS NOT OLD.principal
BEGIN
    INSERT INTO git_work_task_revisions VALUES (NEW.agent, NEW.generation, 1)
    ON CONFLICT(agent,generation) DO UPDATE SET revision=revision+1;
END;

CREATE TRIGGER git_work_task_revision_delete AFTER DELETE ON git_work_tasks
WHEN EXISTS (SELECT 1 FROM agents WHERE id=OLD.agent)
BEGIN
    INSERT INTO git_work_task_revisions VALUES (OLD.agent, OLD.generation, 1)
    ON CONFLICT(agent,generation) DO UPDATE SET revision=revision+1;
END;
