//! A bounded snapshot on a separate read-only connection to the WAL Store.

mod activity;
mod fleet;
mod shell;

pub(super) use activity::SHELL_COVERING_INDEX;

use super::{Result, Store, sql_error};
use eliot_core::api::AutonomyResponse;
use rusqlite::{Connection, OptionalExtension};

pub(super) struct AutonomyCache {
    data_version: i64,
    since: i64,
    until: i64,
    confirmed: super::StuckStates,
    response: AutonomyResponse,
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
        let data_version = connection
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(sql_error)?;
        // Reuse is deliberately limited to the exact fixed window and the
        // exact confirmed-stuck predicate set. Shifting windows recompute;
        // writes observed by this connection invalidate without a TTL.
        if let Some(cache) = self.autonomy_cache.borrow().as_ref()
            && cache.data_version == data_version
            && cache.since == since
            && cache.until == until
            && cache.confirmed == *confirmed
        {
            return Ok(cache.response.clone());
        }

        let activity = activity::read(connection, since, until)?;
        let fleet = fleet::read(connection, since, until, confirmed)?;
        let mut report = AutonomyResponse {
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
        if event_window_pruned(connection, since)? {
            let reason = "the event retention boundary overlaps the requested window";
            report.checks.limit_coverage(reason);
            report.waits.limit_coverage(reason);
            report.notes.limit_coverage(reason);
            report.executor_swarm.limit_coverage(reason);
            report.work_in_git.limit_coverage(reason);
        }
        *self.autonomy_cache.borrow_mut() = Some(AutonomyCache {
            data_version,
            since,
            until,
            confirmed: confirmed.clone(),
            response: report.clone(),
        });
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
