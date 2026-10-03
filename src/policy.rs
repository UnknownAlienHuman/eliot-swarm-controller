//! Stable, explicit owner-policy editions recorded with each new Attempt.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const OWNER_POLICY_V1_ID: &str = "owner-policy-v1";
pub const OWNER_POLICY_V1_EDITION: u32 = 1;
pub const OWNER_POLICY_V1_DOCUMENT_PATH: &str = "docs/owner-decisions.md";
pub const OWNER_POLICY_V1_DOCUMENT_SECTION: &str = "1. Owner policy v1 — resolves issue #14";
pub const OWNER_POLICY_V1_DOCUMENT_SHA256: &str =
    "786df3cddc329ceac270442e1e825e2af37122b4902ecf07410d3a85a4d470dc";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OwnerPolicyEdition {
    pub status: String,
    pub policy_id: String,
    pub edition: u32,
    pub document_path: String,
    pub document_section: String,
    pub document_sha256: String,
}

pub fn current_edition() -> OwnerPolicyEdition {
    OwnerPolicyEdition {
        status: "accepted".to_owned(),
        policy_id: OWNER_POLICY_V1_ID.to_owned(),
        edition: OWNER_POLICY_V1_EDITION,
        document_path: OWNER_POLICY_V1_DOCUMENT_PATH.to_owned(),
        document_section: OWNER_POLICY_V1_DOCUMENT_SECTION.to_owned(),
        document_sha256: OWNER_POLICY_V1_DOCUMENT_SHA256.to_owned(),
    }
}

pub fn accepted_edition(policy_id: Option<&str>) -> Result<OwnerPolicyEdition> {
    match policy_id {
        None => Err(Error::new(
            "OWNER_POLICY_REQUIRED",
            "a new Attempt requires an explicit accepted owner_policy_id",
        )),
        Some(OWNER_POLICY_V1_ID) => Ok(current_edition()),
        Some(policy_id) => Err(Error::new(
            "OWNER_POLICY_UNKNOWN",
            format!("owner_policy_id '{policy_id}' is not a known accepted edition"),
        )),
    }
}

/// Project the policy fact for the attempt.get surface without changing the
/// persisted snapshot. Missing historical policy stays explicitly unknown;
/// an unrecognized recorded object is retained as evidence, not accepted.
pub fn attempt_projection(snapshot: &Value) -> Value {
    let Some(recorded) = snapshot.get("owner_policy") else {
        return json!({"status":"legacy_unknown"});
    };
    match serde_json::from_value::<OwnerPolicyEdition>(recorded.clone()) {
        Ok(edition) if edition == current_edition() => json!(edition),
        _ => json!({"status":"unrecognized","recorded":recorded}),
    }
}

pub fn is_accepted_snapshot(snapshot: &Value) -> bool {
    attempt_projection(snapshot)["status"] == "accepted"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edition_requires_explicit_known_identity_and_projects_legacy_unknown() {
        let edition = accepted_edition(Some(OWNER_POLICY_V1_ID)).unwrap();
        assert_eq!(edition.edition, 1);
        assert_eq!(edition.document_sha256, OWNER_POLICY_V1_DOCUMENT_SHA256);
        assert_eq!(
            accepted_edition(None).unwrap_err().code,
            "OWNER_POLICY_REQUIRED"
        );
        assert_eq!(
            accepted_edition(Some("owner-policy-v2")).unwrap_err().code,
            "OWNER_POLICY_UNKNOWN"
        );
        assert_eq!(
            attempt_projection(&json!({})),
            json!({"status":"legacy_unknown"})
        );
        assert!(is_accepted_snapshot(&json!({"owner_policy":edition})));
    }

    #[test]
    fn v1_digest_pins_the_named_owner_decision_section() {
        let document = include_str!("../docs/owner-decisions.md").replace("\r\n", "\n");
        let heading = format!("## {OWNER_POLICY_V1_DOCUMENT_SECTION}");
        let start = document.find(&heading).expect("policy heading exists");
        let end = document[start + heading.len()..]
            .find("## 2. Module update and retention")
            .map(|offset| start + heading.len() + offset)
            .expect("following section heading exists");
        assert_eq!(
            crate::model::digest(document[start..end].as_bytes()),
            OWNER_POLICY_V1_DOCUMENT_SHA256
        );
    }
}
