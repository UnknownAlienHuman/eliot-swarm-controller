-- Keep every named task binding after the selected task changes or its SEND is pruned.
-- No foreign key: command retention must not delete the original SEND identity.
CREATE TABLE git_work_tasks (
    agent INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    task_command TEXT NOT NULL,
    task_ref TEXT NOT NULL,
    principal TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (agent, generation, task_command)
);

CREATE INDEX git_work_tasks_ref ON git_work_tasks(task_ref, agent, generation);
CREATE INDEX git_checkpoints_ref ON git_checkpoints(checkpoint_ref) WHERE checkpoint_ref IS NOT NULL;

INSERT OR IGNORE INTO git_work_tasks(
    agent, generation, task_command, task_ref, principal, updated_at
)
SELECT checkpoint.agent, checkpoint.generation, checkpoint.task_command,
       checkpoint.task_ref, command.principal, checkpoint.updated_at
FROM git_checkpoints checkpoint
LEFT JOIN commands command ON command.id = checkpoint.task_command
WHERE checkpoint.task_command IS NOT NULL AND checkpoint.task_ref IS NOT NULL;

CREATE INDEX switch_checkout_owners_switch_command ON switch_checkout_owners(switch_command)
WHERE switch_command IS NOT NULL;
