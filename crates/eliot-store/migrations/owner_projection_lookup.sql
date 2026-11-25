CREATE INDEX commands_retained_unknown_page ON commands(created_at)
  WHERE state = 'unknown' AND target IS NOT NULL AND generation IS NOT NULL;
CREATE INDEX owner_inbox_resume_tip ON commands(target, generation, created_at)
  WHERE verb = 'resume' AND principal = 'owner' AND state <> 'cancelled';
CREATE INDEX owner_inbox_unknown_receipt
  ON events(CAST(json_extract(data, '$.command_id') AS TEXT), seq)
  WHERE kind = 'command.unknown';
