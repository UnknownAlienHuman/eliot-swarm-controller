//! Owner provider-hold reads and corrective writes.

use super::{DEFAULT_LIMIT, MAX_LIMIT, mutations::nullable};
use crate::limits::MAX_COMMAND_ID_BYTES;
use crate::{CapacityScope, Error, ProviderHold, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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
            .is_some_and(|scope| validate_scope(scope).is_err())
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHoldsResponse {
    pub holds: Vec<ProviderHold>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderClearHoldRequest {
    pub id: String,
    pub scope: String,
    pub corrective_evidence: ProviderCorrectiveEvidence,
    /// Required on the wire; `null` means no probe claim is expected.
    #[serde(deserialize_with = "nullable")]
    pub expected_probe_command: Option<String>,
}

impl ProviderClearHoldRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        validate_scope(&self.scope)?;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderClearHoldResponse {
    pub id: String,
    pub scope: String,
    pub cleared: bool,
}

fn validate_scope(scope: &str) -> Result<()> {
    CapacityScope {
        scope: scope.to_owned(),
        max_live: None,
    }
    .validate()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderHoldState, ProviderRestrictionKind};
    use serde_json::{Value, json};

    fn hold(scope: &str) -> ProviderHold {
        ProviderHold {
            scope: scope.to_owned(),
            kind: ProviderRestrictionKind::RateLimit,
            retry_after: Some(123),
            probe_command: None,
            state: ProviderHoldState::Held,
            updated_at: 456,
        }
    }

    #[test]
    fn provider_hold_pages_validate_cursor_order_and_nullable_next() {
        let request = ProviderHoldsRequest {
            after: Some("account:main".to_owned()),
            limit: Some(2),
        };
        request.validate().unwrap();
        let response = ProviderHoldsResponse {
            holds: vec![hold("profile:codex-luna"), hold("proxy:opencodex")],
            next: Some("proxy:opencodex".to_owned()),
        };
        request.validate_response(&response).unwrap();

        assert!(
            ProviderHoldsRequest {
                after: Some("vendor-account:unknown".to_owned()),
                limit: None,
            }
            .validate()
            .is_err()
        );
        assert!(
            ProviderHoldsRequest {
                after: None,
                limit: Some(0),
            }
            .validate()
            .is_err()
        );
        assert!(
            request
                .validate_response(&ProviderHoldsResponse {
                    holds: vec![hold("profile:codex-luna")],
                    next: Some("proxy:opencodex".to_owned()),
                })
                .is_err()
        );
        assert!(
            request
                .validate_response(&ProviderHoldsResponse {
                    holds: vec![hold("profile:codex-luna")],
                    next: None,
                })
                .is_err()
        );
        let empty: ProviderHoldsResponse =
            serde_json::from_value(json!({"holds":[],"next":null})).unwrap();
        ProviderHoldsRequest::default()
            .validate_response(&empty)
            .unwrap();
        assert!(serde_json::from_value::<ProviderHoldsResponse>(json!({"holds":[]})).is_err());
        assert!(
            ProviderHoldsRequest::default()
                .validate_response(&ProviderHoldsResponse {
                    holds: vec![hold("profile:codex-luna")],
                    next: None,
                })
                .is_err()
        );
        assert!(serde_json::from_value::<ProviderHoldsRequest>(json!({"unexpected":1})).is_err());
        assert!(
            serde_json::from_value::<ProviderHoldsResponse>(json!({
            "holds":[{"scope":"profile:p","kind":"billing_disabled","retry_after":null,
                    "probe_command":null,"state":"held","updated_at":1}],
                "next":null
            }))
            .is_err()
        );
    }

    #[test]
    fn clear_hold_requires_nullable_probe_binding_and_closed_evidence() {
        let request: ProviderClearHoldRequest = serde_json::from_value(json!({
            "id":"clear-1",
            "scope":"proxy:opencodex",
            "corrective_evidence":"credentials_updated",
            "expected_probe_command":null
        }))
        .unwrap();
        request.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&request).unwrap()["expected_probe_command"],
            Value::Null
        );

        let mut missing_probe = serde_json::to_value(&request).unwrap();
        missing_probe
            .as_object_mut()
            .unwrap()
            .remove("expected_probe_command");
        assert!(serde_json::from_value::<ProviderClearHoldRequest>(missing_probe).is_err());
        let mut wrong_probe_type = serde_json::to_value(&request).unwrap();
        wrong_probe_type["expected_probe_command"] = json!(false);
        assert!(serde_json::from_value::<ProviderClearHoldRequest>(wrong_probe_type).is_err());
        let mut long_probe = request.clone();
        long_probe.expected_probe_command = Some("p".repeat(MAX_COMMAND_ID_BYTES + 1));
        assert!(long_probe.validate().is_err());
        assert!(
            serde_json::from_value::<ProviderClearHoldRequest>(json!({
                "id":"clear-1",
                "scope":"proxy:opencodex",
                "corrective_evidence":"the key is now valid",
                "expected_probe_command":null
            }))
            .is_err()
        );

        request
            .validate_response(&ProviderClearHoldResponse {
                id: "clear-1".to_owned(),
                scope: "proxy:opencodex".to_owned(),
                cleared: false,
            })
            .unwrap();
        assert!(
            request
                .validate_response(&ProviderClearHoldResponse {
                    id: "other".to_owned(),
                    scope: "proxy:opencodex".to_owned(),
                    cleared: false,
                })
                .is_err()
        );
        assert!(
            request
                .validate_response(&ProviderClearHoldResponse {
                    id: "clear-1".to_owned(),
                    scope: "account:opencodex".to_owned(),
                    cleared: false,
                })
                .is_err()
        );
    }
}
