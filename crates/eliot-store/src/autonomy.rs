//! A bounded snapshot on a separate read-only connection to the WAL Store.

mod activity;
mod fleet;
mod shell;

use super::{Result, Store, sql_error};
use eliot_core::api::AutonomyResponse;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::{path::Path, time::Duration};

pub(super) const INDEXES: &[&str] = &[activity::INDEXES, fleet::INDEXES];

impl Store {
    pub fn read_autonomy(path: &Path, since: i64, until: i64) -> Result<AutonomyResponse> {
        #[cfg(windows)]
        let path = &eliot_core::verbatim_path(path);
        let mut connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_secs(1))
            .map_err(sql_error)?;
        let tx = connection.transaction().map_err(sql_error)?;
        let activity = activity::read(&tx, since, until)?;
        let fleet = fleet::read(&tx, since, until)?;
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
        };
        if event_window_pruned(&tx, since)? {
            let reason = "the event retention boundary overlaps the requested window";
            report.checks.limit_coverage(reason);
            report.waits.limit_coverage(reason);
            report.notes.limit_coverage(reason);
            report.executor_swarm.limit_coverage(reason);
        }
        tx.commit().map_err(sql_error)?;
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
