//! A bounded snapshot on a separate read-only connection to the WAL Store.

mod activity;
mod fleet;
mod shell;

pub(super) use activity::SHELL_COVERING_INDEX;

use super::{Result, Store, sql_error};
use eliot_core::api::AutonomyResponse;
use rusqlite::{Connection, OptionalExtension};

pub(super) struct AutonomyCache {
    revision: (i64, u64),
    response: AutonomyResponse,
}

fn window_changes(
    connection: &Connection,
    previous: &AutonomyResponse,
    since: i64,
    until: i64,
) -> Result<bool> {
    // Lower bounds are inclusive. Upper bounds exclude only later rows.
    let lower = (since.min(previous.since_ms), since.max(previous.since_ms));
    let upper = (until.min(previous.until_ms), until.max(previous.until_ms));
    for (from, through) in [
        (lower.0, lower.1.saturating_sub(1)),
        (upper.0.saturating_add(1), upper.1),
    ] {
        if from <= through
            && (activity::window_changes(connection, from, through)?
                || fleet::window_changes(connection, from, through)?)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn revision(connection: &Connection) -> Result<(i64, u64)> {
    let version = connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .map_err(sql_error)?;
    Ok((version, connection.total_changes()))
}

pub(super) const PROVIDER_HOLD_AGE_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS autonomy_fleet_provider_hold_age
    ON provider_holds(held_since, scope) WHERE state IN ('held', 'probing');";
pub(super) const INDEXES: &[&str] = &[activity::INDEXES, fleet::INDEXES, PROVIDER_HOLD_AGE_INDEX];

impl Store {
    pub fn autonomy_from_snapshot(
        &self,
        since: i64,
        until: i64,
        confirmed: &super::StuckStates,
    ) -> Result<AutonomyResponse> {
        let connection = &self.connection;
        let observed = revision(connection)?;
        let cached = self.autonomy_cache.borrow();
        let reusable = if let Some(cache) = cached.as_ref()
            && cache.revision == observed
            && !window_changes(connection, &cache.response, since, until)?
        {
            // Notes can resolve at a later timestamp. Owner waiting also
            // depends on age, snoozes, and the live confirmed-stuck set.
            let mut response = cache.response.clone();
            response.since_ms = since;
            response.until_ms = until;
            response.notes = activity::read_notes(connection, since, until)?;
            response.owner_waiting = fleet::read_owner_waiting(connection, until, confirmed)?;
            (revision(connection)? == observed).then_some(response)
        } else {
            None
        };
        drop(cached);
        let mut report = match reusable {
            Some(response) => response,
            None => {
                let activity = activity::read(connection, since, until)?;
                let fleet = fleet::read(connection, since, until, confirmed)?;
                let response = AutonomyResponse {
                    since_ms: since,
                    until_ms: until,
                    project_policy: fleet.project_policy,
                    steer: activity.steer,
                    checks: activity.checks,
                    waits: activity.waits,
                    notes: activity.notes,
                    owner_waiting: fleet.owner_waiting,
                    usage: fleet.usage,
                    executor_swarm: activity.executor_swarm,
                    work_in_git: activity.work_in_git,
                };
                if revision(connection)? == observed {
                    *self.autonomy_cache.borrow_mut() = Some(AutonomyCache {
                        revision: observed,
                        response: response.clone(),
                    });
                }
                response
            }
        };
        if event_window_pruned(connection, since)? {
            let reason = "the event retention boundary overlaps the requested window";
            report.checks.limit_coverage(reason);
            report.waits.limit_coverage(reason);
            report.notes.limit_coverage(reason);
            report.executor_swarm.limit_coverage(reason);
            report.work_in_git.limit_coverage(reason);
        }
        Ok(report)
    }
}

fn event_window_pruned(connection: &Connection, since: i64) -> Result<bool> {
    if crate::retention::coverage_pruned_through(connection)? == 0 {
        return Ok(false);
    }
    let first: Option<i64> = connection
        .query_row("SELECT ts FROM events ORDER BY seq LIMIT 1", [], |row| {
            row.get(0)
        })
        .optional()
        .map_err(sql_error)?;
    Ok(first.is_none_or(|ts| ts >= since))
}
