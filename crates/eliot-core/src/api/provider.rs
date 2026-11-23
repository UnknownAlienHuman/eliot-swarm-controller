//! Owner provider-hold reads and corrective writes.

use super::{DEFAULT_LIMIT, MAX_LIMIT, mutations::nullable};
use crate::limits::MAX_COMMAND_ID_BYTES;
use crate::routing::validate_provider_scope;
use crate::{Error, ProviderHold, Result};
use serde::{Deserialize, Serialize};

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHoldsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl ProviderHoldsRequest {
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_LIMIT)
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .after
            .as_deref()
            .is_some_and(|scope| validate_provider_scope(scope).is_err())
            || !(1..=MAX_LIMIT).contains(&self.effective_limit())
        {
            return Err(Error::new("invalid", "invalid provider hold page"));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &ProviderHoldsResponse) -> Result<()> {
        if self.validate().is_err() || response.holds.len() > self.effective_limit() {
            return Err(unproven());
        }

        let mut previous = self.after.as_deref();
        for hold in &response.holds {
            hold.validate().map_err(|_| unproven())?;
            if previous.is_some_and(|scope| scope >= hold.scope.as_str()) {
                return Err(unproven());
            }
            previous = Some(&hold.scope);
        }

        let expected_next = response.holds.last().map(|hold| hold.scope.as_str());
        if response.next.as_deref() != expected_next {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHoldsResponse {
    pub holds: Vec<ProviderHold>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCorrectiveEvidence {
    CredentialsUpdated,
    ConfigurationUpdated,
    OwnerVerified,
}

impl ProviderCorrectiveEvidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CredentialsUpdated => "credentials_updated",
            Self::ConfigurationUpdated => "configuration_updated",
            Self::OwnerVerified => "owner_verified",
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderClearHoldRequest {
    pub id: String,
    pub scope: String,
    pub corrective_evidence: ProviderCorrectiveEvidence,
    /// Required on the wire; `null` means no probe claim is expected.
    #[serde(deserialize_with = "nullable")]
    #[schemars(with = "super::discovery::RequiredNullable<String>")]
    pub expected_probe_command: Option<String>,
}

impl ProviderClearHoldRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        validate_provider_scope(&self.scope)?;
        if self
            .expected_probe_command
            .as_deref()
            .is_some_and(|command| validate_probe_command(command).is_err())
        {
            return Err(Error::new(
                "invalid",
                "invalid expected provider probe command",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &ProviderClearHoldResponse) -> Result<()> {
        if self.validate().is_err() || response.id != self.id || response.scope != self.scope {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderClearHoldResponse {
    pub id: String,
    pub scope: String,
    pub cleared: bool,
}

fn validate_write_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new("invalid", "invalid provider hold write id"));
    }
    Ok(())
}

fn validate_probe_command(command: &str) -> Result<()> {
    validate_write_id(command)?;
    if command.len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::new(
            "invalid",
            "expected provider probe command exceeds its byte limit",
        ));
    }
    Ok(())
}

fn unproven() -> Error {
    Error::new(
        "unknown",
        "provider response does not match its requested binding",
    )
}
