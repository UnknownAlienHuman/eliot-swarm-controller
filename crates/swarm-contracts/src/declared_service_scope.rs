//! Data-only identity for explicitly declared, non-binding service scopes.
//!
//! A purpose classifies the owner lifecycle; it is not a Principal, grant, or
//! credential. The service identifier remains opaque and must be hashed or
//! encoded before being used as a filesystem component.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

const MAX_SERVICE_ID_BYTES: usize = 128;

/// Closed purposes supported by the current host-owned service lifecycle.
/// Adding a variant requires a Store registration/credential policy as well as
/// process-owner support; it cannot be selected from an untrusted string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclaredServicePurpose {
    BusConsumer,
    AutomationScheduler,
}

impl DeclaredServicePurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BusConsumer => "bus_consumer",
            Self::AutomationScheduler => "automation_scheduler",
        }
    }

    /// Stable suffix for the OS ownership object name. This is deliberately
    /// separate from the user-controlled service identifier.
    pub const fn owner_name_component(self) -> &'static str {
        match self {
            Self::BusConsumer => "BusConsumer",
            Self::AutomationScheduler => "AutomationScheduler",
        }
    }
}

/// Exact non-binding scope for one declared service incarnation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredServiceScope {
    pub purpose: DeclaredServicePurpose,
    pub service_id: String,
    pub generation: u64,
}

impl DeclaredServiceScope {
    pub fn new(
        purpose: DeclaredServicePurpose,
        service_id: impl Into<String>,
        generation: u64,
    ) -> Result<Self> {
        let scope = Self {
            purpose,
            service_id: service_id.into(),
            generation,
        };
        scope.validate()?;
        Ok(scope)
    }

    pub fn validate_service_id(service_id: &str) -> Result<()> {
        if service_id.is_empty()
            || service_id.len() > MAX_SERVICE_ID_BYTES
            || !service_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
        {
            return Err(Error::invalid(
                "declared service identifier is invalid or too long",
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        Self::validate_service_id(&self.service_id)?;
        if self.generation == 0 {
            return Err(Error::invalid(
                "declared service generation must be positive",
            ));
        }
        Ok(())
    }
}
