-- Canonical checkout owner and in-flight switch fence. The manager registers
-- this migration after the current 106/107 train lands.
CREATE TABLE switch_checkout_owners (
    checkout_key TEXT PRIMARY KEY NOT NULL,
    checkout_root TEXT NOT NULL,
    agent INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    switch_command TEXT,
    updated_at INTEGER NOT NULL
);
CREATE INDEX switch_checkout_owners_agent
    ON switch_checkout_owners(agent, generation);
