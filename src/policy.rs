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
pub const OWNER_POLICY_V2_ID: &str = "owner-policy-v2";
pub const OWNER_POLICY_V2_EDITION: u32 = 2;
pub const OWNER_POLICY_V2_DOCUMENT_PATH: &str = "docs/owner-policy-v2.md";
pub const OWNER_POLICY_V2_DOCUMENT_SECTION: &str =
    "Owner policy v2 — scoped manager review disposition";
pub const OWNER_POLICY_V2_DOCUMENT_SHA256: &str =
    "a3490caa5ee7afa435dd0a4a317917a88b99c746f477a304de9458d45553b5db";

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

/// Frozen v1 identity, kept under its original function name for callers that
/// need to recognize Attempts created before v2.
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

pub fn v2_edition() -> OwnerPolicyEdition {
    OwnerPolicyEdition {
        status: "accepted".to_owned(),
        policy_id: OWNER_POLICY_V2_ID.to_owned(),
        edition: OWNER_POLICY_V2_EDITION,
        document_path: OWNER_POLICY_V2_DOCUMENT_PATH.to_owned(),
        document_section: OWNER_POLICY_V2_DOCUMENT_SECTION.to_owned(),
        document_sha256: OWNER_POLICY_V2_DOCUMENT_SHA256.to_owned(),
    }
}

pub fn accepted_editions() -> [OwnerPolicyEdition; 2] {
    [current_edition(), v2_edition()]
}

pub fn accepted_edition(policy_id: Option<&str>) -> Result<OwnerPolicyEdition> {
    match policy_id {
        None => Err(Error::new(
            "OWNER_POLICY_REQUIRED",
            "a new Attempt requires an explicit accepted owner_policy_id",
        )),
        Some(OWNER_POLICY_V1_ID) => Ok(current_edition()),
        Some(OWNER_POLICY_V2_ID) => Ok(v2_edition()),
        Some(policy_id) => Err(Error::new(
            "OWNER_POLICY_UNKNOWN",
            format!("owner_policy_id '{policy_id}' is not a known accepted edition"),
        )),
    }
}

/// Return a policy edition only when every persisted field matches a frozen
/// accepted identity. A policy ID alone cannot grant rights.
pub fn recorded_edition(snapshot: &Value) -> Option<OwnerPolicyEdition> {
    let recorded =
        serde_json::from_value::<OwnerPolicyEdition>(snapshot.get("owner_policy")?.clone()).ok()?;
    accepted_editions()
        .into_iter()
        .find(|edition| edition == &recorded)
}

pub fn allows_scoped_manager_feedback(snapshot: &Value) -> bool {
    recorded_edition(snapshot).is_some_and(|edition| edition.policy_id == OWNER_POLICY_V2_ID)
}

/// Project the policy fact for the attempt.get surface without changing the
/// persisted snapshot. Missing historical policy stays explicitly unknown;
/// an unrecognized recorded object is retained as evidence, not accepted.
pub fn attempt_projection(snapshot: &Value) -> Value {
    let Some(recorded) = snapshot.get("owner_policy") else {
        return json!({"status":"legacy_unknown"});
    };
    match recorded_edition(snapshot) {
        Some(edition) => json!(edition),
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
            accepted_edition(Some("owner-policy-v999"))
                .unwrap_err()
                .code,
            "OWNER_POLICY_UNKNOWN"
        );
        assert_eq!(
            attempt_projection(&json!({})),
            json!({"status":"legacy_unknown"})
        );
        assert!(is_accepted_snapshot(&json!({"owner_policy":edition})));
        assert!(is_accepted_snapshot(&json!({"owner_policy":v2_edition()})));
        assert!(allows_scoped_manager_feedback(
            &json!({"owner_policy":v2_edition()})
        ));
        assert!(!allows_scoped_manager_feedback(
            &json!({"owner_policy":current_edition()})
        ));
        assert_eq!(
            accepted_edition(Some(OWNER_POLICY_V2_ID)).unwrap(),
            v2_edition()
        );
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

    #[test]
    fn v2_digest_pins_scoped_manager_feedback_policy() {
        let document = include_str!("../docs/owner-policy-v2.md").replace("\r\n", "\n");
        let heading = format!("## {OWNER_POLICY_V2_DOCUMENT_SECTION}");
        let start = document.find(&heading).expect("policy heading exists");
        let end = document[start..]
            .find("\n\nThe complete section above")
            .map(|offset| start + offset)
            .expect("policy section terminator exists");
        let section = document[start..end].trim_end_matches('\n');
        assert_eq!(
            crate::model::digest(section.as_bytes()),
            OWNER_POLICY_V2_DOCUMENT_SHA256
        );
    }
}
