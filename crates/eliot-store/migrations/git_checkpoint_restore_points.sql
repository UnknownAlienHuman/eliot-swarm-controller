CREATE TABLE git_checkpoint_restore_points (
    agent INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    task_ref TEXT NOT NULL,
    checkpoint_ref TEXT NOT NULL,
    oid TEXT NOT NULL,
    PRIMARY KEY (agent, generation, task_ref)
) WITHOUT ROWID;

-- Older builds could clear a generation's pointer when it became clean.
-- Recover the exact last publication when its committed event is still retained.
WITH latest AS (
    SELECT agent, generation, MAX(seq) AS seq
    FROM events INDEXED BY work_truth_events_window
    WHERE kind = 'git.checkpoint' AND source = 'daemon'
      AND json_extract(data, '$.state') = 'snapshot'
      AND json_type(data, '$.ref') = 'text' AND json_type(data, '$.oid') = 'text'
    GROUP BY agent, generation
)
UPDATE git_checkpoints AS checkpoint
SET (checkpoint_ref, oid) = (
    SELECT json_extract(event.data, '$.ref'), json_extract(event.data, '$.oid')
    FROM latest JOIN events event USING (seq)
    WHERE latest.agent = checkpoint.agent AND latest.generation = checkpoint.generation
)
WHERE checkpoint.checkpoint_ref IS NULL;

-- The older schema did not bind a snapshot to the task selected at capture.
INSERT INTO git_checkpoint_restore_points(agent, generation, task_ref, checkpoint_ref, oid)
SELECT agent, generation, '', checkpoint_ref, oid FROM git_checkpoints
WHERE checkpoint_ref IS NOT NULL AND oid IS NOT NULL;
