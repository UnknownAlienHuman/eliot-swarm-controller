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
}

impl MaintenanceRunResponse {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("events_deleted", self.events_deleted),
            ("pruned_through", self.pruned_through),
            ("commands_deleted", self.commands_deleted),
            ("writes_deleted", self.writes_deleted),
            ("mail_deleted", self.mail_deleted),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maintenance_run_shapes_are_strict_and_counts_are_nonnegative() {
        let request = MaintenanceRunRequest::default();
        request.validate().unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), json!({}));
        assert_eq!(
            serde_json::from_value::<MaintenanceRunRequest>(json!({})).unwrap(),
            request
        );
        assert!(serde_json::from_value::<MaintenanceRunRequest>(json!({"extra": true})).is_err());

        let response = MaintenanceRunResponse {
            events_deleted: 12,
            pruned_through: 120,
            commands_deleted: 3,
            writes_deleted: 4,
            mail_deleted: 0,
        };
        response.validate().unwrap();
        request.validate_response(&response).unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "events_deleted": 12,
                "pruned_through": 120,
                "commands_deleted": 3,
                "writes_deleted": 4,
                "mail_deleted": 0,
            })
        );
        assert_eq!(
            serde_json::from_value::<MaintenanceRunResponse>(
                serde_json::to_value(response).unwrap()
            )
            .unwrap(),
            response
        );
        let mut negative = response;
        negative.commands_deleted = -1;
        assert!(negative.validate().is_err());
        assert!(request.validate_response(&negative).is_err());
        assert!(
            serde_json::from_value::<MaintenanceRunResponse>(json!({
                "events_deleted": 12,
                "pruned_through": 120,
                "commands_deleted": 3,
                "writes_deleted": 4,
            }))
            .is_err()
        );
    }
}
