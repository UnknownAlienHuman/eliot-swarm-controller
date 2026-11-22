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

/// Preview owner-sent legacy advisory-attention mail by default. Supplying the
/// exact count from a prior preview explicitly applies the purge.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePurgeLegacyAttentionRequest {
    pub expected_message_count: Option<i64>,
}

impl MaintenancePurgeLegacyAttentionRequest {
    pub fn validate(&self) -> Result<()> {
        if self.expected_message_count.is_some_and(|count| count < 0) {
            return Err(Error::new(
                "invalid",
                "expected_message_count must be non-negative",
            ));
        }
        Ok(())
    }

    pub fn validate_response(
        &self,
        response: &MaintenancePurgeLegacyAttentionResponse,
    ) -> Result<()> {
        response.validate()?;
        let valid = match self.expected_message_count {
            Some(expected) => {
                response.applied
                    && response.matching_count == expected
                    && response.deleted_count == expected
            }
            None => !response.applied && response.deleted_count == 0,
        };
        if !valid {
            return Err(Error::new(
                "unknown",
                "maintenance purge response does not match its request",
            ));
        }
        Ok(())
    }
}

/// Exact legacy-message match count and the outcome of an explicit purge.
#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePurgeLegacyAttentionResponse {
    pub matching_count: i64,
    pub deleted_count: i64,
    pub applied: bool,
}

impl MaintenancePurgeLegacyAttentionResponse {
    pub fn validate(&self) -> Result<()> {
        if self.matching_count < 0 || self.deleted_count < 0 {
            return Err(Error::new(
                "unknown",
                "maintenance purge response contains a negative count",
            ));
        }
        if self.deleted_count > self.matching_count
            || (self.applied && self.deleted_count != self.matching_count)
        {
            return Err(Error::new(
                "unknown",
                "maintenance purge response has inconsistent counts",
            ));
        }
        Ok(())
    }
}
