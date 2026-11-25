CREATE INDEX commands_retained_unknown_page ON commands(created_at)
  WHERE state = 'unknown' AND target IS NOT NULL AND generation IS NOT NULL;
CREATE INDEX owner_inbox_resume_tip ON commands(target, generation, created_at)
  WHERE verb = 'resume' AND principal = 'owner' AND state <> 'cancelled';
CREATE INDEX owner_inbox_unknown_receipt
  ON events(CAST(json_extract(data, '$.command_id') AS TEXT), seq)
  WHERE kind = 'command.unknown';
CREATE INDEX artifacts_unknown_agent ON artifacts(agent)
  WHERE json_extract(body, '$.state') = 'unknown';
CREATE INDEX agents_list_visible ON agents(archived, id)
  WHERE json_type(profile_snapshot, '$._harness_check') IS NULL;
CREATE INDEX artifacts_ledger_state ON artifacts(json_extract(body, '$.state'));
CREATE INDEX commands_pending_send_page ON commands(target, generation, created_at, id)
  WHERE verb = 'send' AND state IN ('accepted', 'queued');
