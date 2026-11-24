use crate::{Store, commit_transaction, insert_daemon_event, sql_error};
use eliot_core::{Event, Result, api::GoalMode};
use rusqlite::{TransactionBehavior, params};
use serde_json::json;

pub const MAX_REPEAT_FAILURE_ALERTS: usize = 1024;

#[derive(Clone, Copy)]
pub struct RepeatAlertGoal {
    pub agent: i64,
    pub generation: i64,
    pub mode: GoalMode,
    pub revision: i64,
}

pub struct RepeatAlertUpdate {
    pub cause: String,
    pub observed_at: i64,
    pub expires_at: i64,
    pub goal: Option<RepeatAlertGoal>,
    /// Present when the threshold was met. An observation below it can only renew an occurrence.
    pub message: Option<String>,
}

impl Store {
    /// Open or renew each cause and commit its first Owner notification atomically.
    pub fn update_repeat_alerts(
        &mut self,
        updates: &[RepeatAlertUpdate],
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut events = Vec::new();
            for update in updates {
                if update.expires_at <= now {
                    continue;
                }
                if let Some(goal) = update.goal {
                    let current: bool = tx.query_row(
                        "SELECT EXISTS (SELECT 1 FROM goals g JOIN agents a ON a.id = g.agent
                         WHERE g.agent = ?1 AND g.generation = ?2 AND g.mode = ?3
                           AND g.repeat_alert_epoch <= ?4 AND g.revision >= ?4
                           AND a.generation = g.generation AND a.state NOT IN ('stopped','failed','lost')
                           AND json_extract(g.state, '$.status') IN ('active','blocked'))",
                        params![goal.agent, goal.generation, goal.mode.as_str(), goal.revision],
                        |row| row.get(0),
                    ).map_err(sql_error)?;
                    if !current {
                        continue;
                    }
                }
                let renewed = tx
                    .execute(
                        "UPDATE repeat_failure_alerts SET expires_at = MAX(expires_at, ?2),
                         message = COALESCE(?3, message)
                     WHERE cause = ?1 AND expires_at > ?4",
                        params![
                            update.cause,
                            update.expires_at,
                            update.message,
                            update.observed_at
                        ],
                    )
                    .map_err(sql_error)?;
                let Some(message) = update.message.as_deref().filter(|_| renewed == 0) else {
                    continue;
                };
                let inserted = tx.execute(
                    "INSERT INTO repeat_failure_alerts
                       (cause, occurrence, opened_at, expires_at, message, goal_agent, goal_generation, goal_mode)
                     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
                     WHERE (SELECT COUNT(*) FROM repeat_failure_alerts WHERE expires_at > ?9) < ?10
                     ON CONFLICT(cause) DO UPDATE SET occurrence = excluded.occurrence,
                       opened_at = excluded.opened_at, expires_at = excluded.expires_at,
                       message = excluded.message WHERE repeat_failure_alerts.expires_at <= ?3",
                    params![update.cause, ulid::Ulid::new().to_string(), update.observed_at,
                        update.expires_at, message, update.goal.map(|goal| goal.agent),
                        update.goal.map(|goal| goal.generation),
                        update.goal.map(|goal| goal.mode.as_str()), now, MAX_REPEAT_FAILURE_ALERTS],
                ).map_err(sql_error)?;
                if inserted > 0 {
                    events.push(insert_daemon_event(
                        &tx,
                        now,
                        "x.user.notify",
                        "daemon",
                        None,
                        None,
                        json!({"title": "Daemon attention", "message": message}),
                    )?);
                }
            }
            tx.execute(
                "DELETE FROM repeat_failure_alerts WHERE expires_at <= ?1",
                [now],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}
