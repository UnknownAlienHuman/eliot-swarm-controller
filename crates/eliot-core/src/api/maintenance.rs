//! Owner-triggered retention maintenance pass values.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Empty params for `maintenance.run`: the daemon reloads its retained
/// retention windows and runs the same bounded epochs as the hourly tick.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceRunRequest {}

impl MaintenanceRunRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate_response(&self, response: &MaintenanceRunResponse) -> Result<()> {
        response.validate()
    }
}

/// Pruned counts for one on-demand maintenance pass. Field names match the
/// tick's `daemon.retention_pruned` log counts; `pruned_through` is the
/// retained-event watermark after the pass.
#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceRunResponse {
    pub events_deleted: i64,
    pub pruned_through: i64,
    pub commands_deleted: i64,
    pub writes_deleted: i64,
    pub mail_deleted: i64,
    pub digests_deleted: i64,
    pub processes_deleted: i64,
}

impl MaintenanceRunResponse {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("events_deleted", self.events_deleted),
            ("pruned_through", self.pruned_through),
            ("commands_deleted", self.commands_deleted),
            ("writes_deleted", self.writes_deleted),
            ("mail_deleted", self.mail_deleted),
            ("digests_deleted", self.digests_deleted),
            ("processes_deleted", self.processes_deleted),
        ] {
            if value < 0 {
                return Err(Error::new(
                    "unknown",
                    format!("maintenance.run response {name} is negative"),
                ));
            }
        }
        Ok(())
    }
}
