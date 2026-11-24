-- A commit affecting the deferred mail candidate predicates invalidates the cache.
CREATE INDEX mail_restart_events ON events(agent, generation, kind)
    WHERE kind IN ('agent.lost', 'agent.stopped');

CREATE TABLE mail_notice_revision (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL
);
INSERT INTO mail_notice_revision(singleton, revision) VALUES (1, 0);

CREATE TRIGGER mail_notice_revision_messages_insert AFTER INSERT ON messages
WHEN NEW.read_at IS NULL AND NEW.wake = 1
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_messages_delete AFTER DELETE ON messages
WHEN OLD.read_at IS NULL AND OLD.wake = 1
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_messages_update
AFTER UPDATE OF recipient, recipient_generation, read_at, wake, inline_wake_retries, thread, sender, delivery_id ON messages
WHEN ((OLD.read_at IS NULL AND OLD.wake = 1) OR (NEW.read_at IS NULL AND NEW.wake = 1))
 AND (OLD.recipient IS NOT NEW.recipient
  OR OLD.recipient_generation IS NOT NEW.recipient_generation
  OR OLD.read_at IS NOT NEW.read_at OR OLD.wake IS NOT NEW.wake
  OR OLD.inline_wake_retries IS NOT NEW.inline_wake_retries
  OR OLD.thread IS NOT NEW.thread OR OLD.sender IS NOT NEW.sender
  OR OLD.delivery_id IS NOT NEW.delivery_id)
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_agents_insert AFTER INSERT ON agents
WHEN EXISTS (SELECT 1 FROM messages m WHERE m.recipient = NEW.id AND m.read_at IS NULL AND m.wake = 1)
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_agents_delete AFTER DELETE ON agents
WHEN EXISTS (SELECT 1 FROM messages m WHERE m.recipient = OLD.id AND m.read_at IS NULL AND m.wake = 1)
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_agents_update
AFTER UPDATE OF state, archived, observe_only, generation, capacity_generation ON agents
WHEN (OLD.state IS NOT NEW.state OR OLD.archived IS NOT NEW.archived
  OR OLD.observe_only IS NOT NEW.observe_only OR OLD.generation IS NOT NEW.generation
  OR OLD.capacity_generation IS NOT NEW.capacity_generation)
 AND EXISTS (SELECT 1 FROM messages m WHERE m.recipient = NEW.id AND m.read_at IS NULL AND m.wake = 1)
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_notices_insert AFTER INSERT ON mail_notices
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_notices_delete AFTER DELETE ON mail_notices
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_notices_update
AFTER UPDATE OF recipient, last_notice_at ON mail_notices
WHEN OLD.recipient IS NOT NEW.recipient OR OLD.last_notice_at IS NOT NEW.last_notice_at
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_attention_wakes_insert AFTER INSERT ON attention_wakes
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_attention_wakes_delete AFTER DELETE ON attention_wakes
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_attention_wakes_update
AFTER UPDATE OF recipient, thread ON attention_wakes
WHEN OLD.recipient IS NOT NEW.recipient OR OLD.thread IS NOT NEW.thread
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_events_insert AFTER INSERT ON events
WHEN NEW.kind IN ('agent.lost', 'agent.stopped', 'x.user.agent.attention')
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_events_delete AFTER DELETE ON events
WHEN OLD.kind IN ('agent.lost', 'agent.stopped', 'x.user.agent.attention')
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_events_update
AFTER UPDATE OF ts, kind, agent, generation, data ON events
WHEN (OLD.kind IN ('agent.lost', 'agent.stopped', 'x.user.agent.attention')
   OR NEW.kind IN ('agent.lost', 'agent.stopped', 'x.user.agent.attention'))
 AND (OLD.ts IS NOT NEW.ts OR OLD.kind IS NOT NEW.kind OR OLD.agent IS NOT NEW.agent
   OR OLD.generation IS NOT NEW.generation OR OLD.data IS NOT NEW.data)
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_commands_insert AFTER INSERT ON commands
WHEN EXISTS (SELECT 1 FROM messages m WHERE m.recipient = NEW.target AND m.read_at IS NULL AND m.wake = 1 AND m.sender = 'swarm')
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_commands_delete AFTER DELETE ON commands
WHEN EXISTS (SELECT 1 FROM messages m WHERE m.recipient = OLD.target AND m.read_at IS NULL AND m.wake = 1 AND m.sender = 'swarm')
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_commands_update
AFTER UPDATE OF target, generation, verb, principal, state, created_at, outcome ON commands
WHEN (OLD.target IS NOT NEW.target OR OLD.generation IS NOT NEW.generation
  OR OLD.verb IS NOT NEW.verb OR OLD.principal IS NOT NEW.principal
  OR OLD.state IS NOT NEW.state OR OLD.created_at IS NOT NEW.created_at
  OR OLD.outcome IS NOT NEW.outcome)
 AND EXISTS (SELECT 1 FROM messages m WHERE m.recipient IN (OLD.target, NEW.target)
             AND m.read_at IS NULL AND m.wake = 1 AND m.sender = 'swarm')
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;

CREATE TRIGGER mail_notice_revision_digests_insert AFTER INSERT ON completion_digests
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_digests_delete AFTER DELETE ON completion_digests
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER mail_notice_revision_digests_update
AFTER UPDATE OF parent, requester, delivered_mail_id ON completion_digests
WHEN OLD.parent IS NOT NEW.parent OR OLD.requester IS NOT NEW.requester
  OR OLD.delivered_mail_id IS NOT NEW.delivered_mail_id
BEGIN UPDATE mail_notice_revision SET revision = revision + 1 WHERE singleton = 1; END;
