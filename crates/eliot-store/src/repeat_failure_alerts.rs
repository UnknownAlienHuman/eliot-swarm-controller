use crate::{Store, commit_transaction, insert_daemon_event, sql_error};
use eliot_core::{Event, Result, api::GoalMode};
use rusqlite::{Transaction, TransactionBehavior, params};
use serde_json::json;

pub const MAX_REPEAT_FAILURE_ALERTS: usize = 1024;
pub const REPEAT_ALERT_OVERFLOW_MESSAGE: &str =
    "Repeat-failure tracking reached capacity. Some new causes could not be tracked.";
// Ordinary causes are SHA-256 hex strings. Sort the overflow before them in the inbox.
const OVERFLOW_CAUSE: &str = "!overflow";
const OVERFLOW_QUIET_MS: i64 = 60_000;

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

pub struct RepeatAlertBatch {
    pub events: Vec<Event>,
    pub opened_overflow: bool,
}

impl Store {
    /// Open or renew each cause and commit its first Owner notification atomically.
    pub fn update_repeat_alerts(
        &mut self,
        updates: &[RepeatAlertUpdate],
        overflow_at: Option<i64>,
        now: i64,
    ) -> Result<RepeatAlertBatch> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut openings = Vec::new();
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
                if let Some(message) = update.message.as_deref().filter(|_| renewed == 0) {
                    openings.push((update, message));
                }
            }
            // Keep the slot for an existing occurrence when a pending observation
            // renews its deadline. Count capacity only after applying those renewals.
            tx.execute(
                "DELETE FROM repeat_failure_alerts WHERE expires_at <= ?1 AND cause <> ?2",
                params![now, OVERFLOW_CAUSE],
            )
            .map_err(sql_error)?;
            let mut occupied = if openings.is_empty() {
                0
            } else {
                tx.query_row(
                    "SELECT COUNT(*) FROM repeat_failure_alerts WHERE cause <> ?1",
                    [OVERFLOW_CAUSE],
                    |row| row.get::<_, usize>(0),
                )
                .map_err(sql_error)?
            };
            let mut overflow_at =
                overflow_at.filter(|at| at.saturating_add(OVERFLOW_QUIET_MS) > now);
            let mut events = Vec::new();
            for (update, message) in openings {
                if occupied >= MAX_REPEAT_FAILURE_ALERTS {
                    overflow_at.get_or_insert(now);
                    break;
                }
                let inserted = tx.execute(
                    "INSERT INTO repeat_failure_alerts
                       (cause, occurrence, opened_at, expires_at, message, goal_agent, goal_generation, goal_mode)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(cause) DO NOTHING",
                    params![update.cause, ulid::Ulid::new().to_string(), update.observed_at,
                        update.expires_at, message, update.goal.map(|goal| goal.agent),
                        update.goal.map(|goal| goal.generation), update.goal.map(|goal| goal.mode.as_str())],
                ).map_err(sql_error)?;
                if inserted > 0 {
                    occupied += 1;
                    events.push(notify(&tx, now, message)?);
                }
            }
            let opened_overflow = update_overflow(&tx, overflow_at, now)?;
            if opened_overflow {
                events.push(notify(&tx, now, REPEAT_ALERT_OVERFLOW_MESSAGE)?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(RepeatAlertBatch {
                events,
                opened_overflow,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn update_overflow(tx: &Transaction<'_>, overflow_at: Option<i64>, now: i64) -> Result<bool> {
    let Some(observed_at) = overflow_at else {
        tx.execute(
            "DELETE FROM repeat_failure_alerts WHERE cause = ?1 AND expires_at <= ?2",
            params![OVERFLOW_CAUSE, now],
        )
        .map_err(sql_error)?;
        return Ok(false);
    };
    let occurrence = ulid::Ulid::new().to_string();
    let current: String = tx
        .query_row(
            "INSERT INTO repeat_failure_alerts (cause, occurrence, opened_at, expires_at, message)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(cause) DO UPDATE SET
               occurrence = CASE WHEN expires_at <= ?3 THEN excluded.occurrence ELSE occurrence END,
               opened_at = CASE WHEN expires_at <= ?3 THEN excluded.opened_at ELSE opened_at END,
               expires_at = MAX(expires_at, excluded.expires_at)
             RETURNING occurrence",
            params![
                OVERFLOW_CAUSE,
                occurrence,
                observed_at,
                now.saturating_add(OVERFLOW_QUIET_MS),
                REPEAT_ALERT_OVERFLOW_MESSAGE
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(current == occurrence)
}

fn notify(tx: &Transaction<'_>, now: i64, message: &str) -> Result<Event> {
    insert_daemon_event(
        tx,
        now,
        "x.user.notify",
        "daemon",
        None,
        None,
        json!({"title": "Daemon attention", "message": message}),
    )
}
