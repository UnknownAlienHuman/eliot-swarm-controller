-- 015: NON-UNIQUE lookup index for settled mailbox delivery/message identity.
-- Duplicate and coalesced historical receipt refs stay valid: this index only
-- accelerates existence seeks and never constrains admission.
-- CASE+json_valid keeps the index build total on rows whose result_json is not
-- well-formed JSON; retained historical rows must never fail migration.
CREATE INDEX IF NOT EXISTS mailbox_settled_delivery_identity ON operations(
  method,
  state,
  CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END,
  CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.message_id') END
) WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state = 'settled';

