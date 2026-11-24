CREATE TABLE repeat_failure_alerts (
    cause TEXT PRIMARY KEY NOT NULL,
    occurrence TEXT NOT NULL,
    opened_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    message TEXT NOT NULL,
    goal_agent INTEGER,
    goal_generation INTEGER,
    goal_mode TEXT
);
CREATE INDEX repeat_failure_alerts_goal ON repeat_failure_alerts(goal_agent, goal_mode);

-- Fence queued observations across a Goal replacement or closure. Ordinary
-- rounds advance revision without changing this boundary.
ALTER TABLE goals ADD COLUMN repeat_alert_epoch INTEGER NOT NULL DEFAULT 0;

CREATE TRIGGER repeat_failure_goal_replaced AFTER UPDATE OF definition, generation ON goals
BEGIN
    UPDATE goals SET repeat_alert_epoch = NEW.revision WHERE agent = NEW.agent AND mode = NEW.mode;
    DELETE FROM repeat_failure_alerts WHERE goal_agent = NEW.agent AND goal_mode = NEW.mode;
END;

CREATE TRIGGER repeat_failure_goal_closed AFTER UPDATE OF state ON goals
WHEN json_extract(NEW.state, '$.status') NOT IN ('active','blocked')
 AND json_extract(OLD.state, '$.status') IN ('active','blocked')
BEGIN
    UPDATE goals SET repeat_alert_epoch = NEW.revision WHERE agent = NEW.agent AND mode = NEW.mode;
    DELETE FROM repeat_failure_alerts WHERE goal_agent = NEW.agent AND goal_mode = NEW.mode;
END;

CREATE TRIGGER repeat_failure_agent_ended AFTER UPDATE OF state, generation ON agents
WHEN NEW.generation <> OLD.generation
 OR (NEW.state IN ('stopped','failed','lost') AND OLD.state NOT IN ('stopped','failed','lost'))
BEGIN
    DELETE FROM repeat_failure_alerts WHERE goal_agent = NEW.id;
END;
