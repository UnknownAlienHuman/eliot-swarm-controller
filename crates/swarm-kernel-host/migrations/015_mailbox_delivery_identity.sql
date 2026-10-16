-- 015: NON-UNIQUE lookup indexes for settled mailbox delivery/message identity.
-- Duplicate and coalesced historical receipt refs stay valid: these indexes only
-- accelerate existence seeks and never constrain admission. Each identity gets
-- its own expression index: one composite (method,state,delivery,message) index
-- cannot bound a message_id seek while delivery_id is unconstrained.
-- The invalid-row sentinel preserves the legacy error semantics: the pre-index
-- OR query evaluated json_extract over every in-scope settled receipt, so
-- malformed result_json in scope THREW instead of answering absent. The reader
-- probes the sentinel first and re-raises that authentic error. NULL
-- result_json is not malformed and keeps its absent behavior.
-- CASE+json_valid keeps every index build total on rows whose result_json is
-- not well-formed JSON; retained historical rows must never fail migration.
DROP INDEX IF EXISTS mailbox_settled_delivery_identity;
CREATE INDEX IF NOT EXISTS mailbox_settled_delivery_identity ON operations(
  method,
  state,
  CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END
) WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state = 'settled';
CREATE INDEX IF NOT EXISTS mailbox_settled_message_identity ON operations(
  method,
  state,
  CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.message_id') END
) WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state = 'settled';
CREATE INDEX IF NOT EXISTS mailbox_settled_invalid_result ON operations(
  method,
  state
) WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state = 'settled' AND result_json IS NOT NULL AND NOT json_valid(result_json);
