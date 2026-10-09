-- The immutable Operation ledger remains the sole history of issued epochs.
-- Keep its narrow aggregate independent of unrelated retained operations.
CREATE INDEX gm_handover_history ON operations(method, state)
    WHERE method = 'gm.handover' AND state = 'settled';
