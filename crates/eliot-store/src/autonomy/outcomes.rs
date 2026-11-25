use eliot_core::{
    Error, Result, TurnUsage,
    api::{AutonomyAcceptedChangeCost, AutonomyIdleQueue, AutonomyRow, AutonomyUnknownTurns},
};
use rusqlite::{Connection, params};

use super::super::sql_error;

const MAX_ROWS: i64 = 10_000;
const PROBE_ROWS: i64 = MAX_ROWS + 1;

pub(crate) const INDEXES: &str = "
CREATE INDEX IF NOT EXISTS autonomy_unknown_turns_window
    ON events(ts, seq) WHERE kind = 'turn.unknown';
CREATE INDEX IF NOT EXISTS autonomy_launch_intents_window
    ON writes(created_at, id) WHERE method = 'launch.enqueue';
CREATE INDEX IF NOT EXISTS autonomy_integrations_window
    ON commands(updated_at, id) WHERE verb = 'git.integrate' AND state = 'applied';";

pub(super) fn window_changes(tx: &Connection, from: i64, through: i64) -> Result<bool> {
    tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM events INDEXED BY autonomy_unknown_turns_window
                       WHERE kind = 'turn.unknown' AND ts BETWEEN ?1 AND ?2)
             OR EXISTS(SELECT 1 FROM commands INDEXED BY autonomy_integrations_window
                       WHERE verb = 'git.integrate' AND state = 'applied'
                         AND updated_at BETWEEN ?1 AND ?2)",
    )
    .map_err(sql_error)?
    .query_row(params![from, through], |row| row.get(0))
    .map_err(sql_error)
}

pub(super) fn unknown_turns(
    tx: &Connection,
    since: i64,
    until: i64,
) -> Result<AutonomyRow<AutonomyUnknownTurns>> {
    let observed: i64 = tx
        .prepare_cached(
            "SELECT COUNT(*) FROM (
                 SELECT 1 FROM events INDEXED BY autonomy_unknown_turns_window
                 WHERE kind = 'turn.unknown' AND ts BETWEEN ?1 AND ?2
                 ORDER BY ts, seq LIMIT ?3
             )",
        )
        .map_err(sql_error)?
        .query_row(params![since, until, PROBE_ROWS], |row| row.get(0))
        .map_err(sql_error)?;
    let mut row = AutonomyRow::complete(
        "retained turn.unknown events",
        AutonomyUnknownTurns {
            unknown: observed.min(MAX_ROWS) as u64,
        },
    );
    if observed > MAX_ROWS {
        row.limit_coverage("the unknown turn scan reached its 10,000-row cap");
    }
    Ok(row)
}

pub(super) fn idle_queue(
    tx: &Connection,
    since: i64,
    until: i64,
) -> Result<AutonomyRow<AutonomyIdleQueue>> {
    let mut statement = tx
        .prepare_cached(
            "SELECT w.created_at, json_extract(w.result, '$.state'), c.created_at
             FROM writes w INDEXED BY autonomy_launch_intents_window
             LEFT JOIN commands c ON c.id = json_extract(w.result, '$.command_id')
                                 AND c.verb = 'launch'
             WHERE w.method = 'launch.enqueue' AND w.created_at BETWEEN ?1 AND ?2
             ORDER BY w.created_at DESC, w.id DESC LIMIT ?3",
        )
        .map_err(sql_error)?;
    let mut rows = statement
        .query(params![since, until, PROBE_ROWS])
        .map_err(sql_error)?;
    let mut counts = AutonomyIdleQueue {
        admitted: 0,
        admitted_wait_ms: 0,
        pending: 0,
        pending_wait_ms: 0,
    };
    let mut missing_admission = false;
    for _ in 0..MAX_ROWS {
        let Some(row) = rows.next().map_err(sql_error)? else {
            break;
        };
        let enqueued: i64 = row.get(0).map_err(sql_error)?;
        let state: String = row.get(1).map_err(sql_error)?;
        match state.as_str() {
            "admitted" => {
                let admitted: Option<i64> = row.get(2).map_err(sql_error)?;
                match admitted {
                    Some(at) if (enqueued..=until).contains(&at) => {
                        counts.admitted += 1;
                        counts.admitted_wait_ms += at.abs_diff(enqueued);
                    }
                    _ => missing_admission = true,
                }
            }
            "queued" => {
                counts.pending += 1;
                counts.pending_wait_ms += until.abs_diff(enqueued);
            }
            _ => {}
        }
    }
    let mut row = AutonomyRow::partial(
        "retained interactive launch receipts and command admission",
        "receipt or command retention can omit waits. Rejected intents have no retained end time",
        counts,
    );
    if missing_admission {
        row.limit_coverage("some admitted intents lack a launch timestamp within the window");
    }
    if rows.next().map_err(sql_error)?.is_some() {
        row.limit_coverage("the launch intent scan reached its 10,000-row cap");
    }
    Ok(row)
}

#[derive(Default)]
pub(super) struct WindowTokens {
    observed: Option<u64>,
    pub(super) incomplete: bool,
}

impl WindowTokens {
    pub(super) fn observe(&mut self, usage: &TurnUsage) -> Result<()> {
        self.incomplete |= usage.total_tokens.is_none();
        if let Some(total) = usage.reported_total() {
            let total = u64::try_from(total)
                .map_err(|_| Error::new("source_integrity", "negative autonomy token total"))?;
            self.observed = Some(
                self.observed
                    .unwrap_or_default()
                    .checked_add(total)
                    .ok_or_else(|| {
                        Error::new("source_integrity", "autonomy token total overflow")
                    })?,
            );
        }
        Ok(())
    }
}

pub(super) fn accepted_change_cost(
    tx: &Connection,
    since: i64,
    until: i64,
    tokens: WindowTokens,
) -> Result<AutonomyRow<AutonomyAcceptedChangeCost>> {
    let mut statement = tx
        .prepare_cached(
            "SELECT json_extract(args, '$.fast_forward.expected_oid')
                         <> json_extract(outcome, '$.fast_forward.oid') AS advanced
             FROM commands INDEXED BY autonomy_integrations_window
             WHERE verb = 'git.integrate' AND state = 'applied'
               AND updated_at BETWEEN ?1 AND ?2
             ORDER BY updated_at DESC, id DESC LIMIT ?3",
        )
        .map_err(sql_error)?;
    let mut rows = statement
        .query(params![since, until, PROBE_ROWS])
        .map_err(sql_error)?;
    let mut accepted = 0;
    for _ in 0..MAX_ROWS {
        let Some(row) = rows.next().map_err(sql_error)? else {
            break;
        };
        accepted += u64::from(row.get::<_, Option<bool>>(0).map_err(sql_error)? == Some(true));
    }
    let capped = rows.next().map_err(sql_error)?.is_some();
    let mut row = AutonomyRow::partial(
        "retained normalized turn usage and applied git.integrate branch advances",
        "window-wide token ratio. Command retention can omit acceptances. Changes accepted outside Swarm have no receipt",
        AutonomyAcceptedChangeCost {
            accepted_changes: accepted,
            observed_tokens: tokens.observed,
            observed_tokens_per_accepted_change: tokens
                .observed
                .filter(|_| accepted > 0 && !capped)
                .map(|total| total as f64 / accepted as f64),
        },
    );
    if tokens.incomplete {
        row.limit_coverage("some turn usage is missing or outside the turn scan cap");
    }
    if capped {
        row.limit_coverage("the integration scan reached its 10,000-row cap");
    }
    Ok(row)
}
