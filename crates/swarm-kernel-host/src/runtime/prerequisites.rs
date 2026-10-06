//! Adapter-owned validation for persisted configure-to-input prerequisites.
//!
//! The Store owns only the generic receipt/barrier: Operation ownership,
//! earlier-Operation ordering, digest/revision equality over validated
//! evidence, later-conflicting-evidence checks, and observation freshness.
//! Every vendor interpretation of configuration evidence — kinds, contract
//! revisions, native snapshot shape — lives behind this registry, keyed by
//! runtime kind, with the validator implementation on the runtime side.
//! Evidence produced by one runtime's validator is never accepted by
//! another's: the dispatch enums below make a foreign payload a typed
//! mismatch, not a value that can accidentally validate.

use super::opencode_v2;
use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};

/// Hard cap on the number of conditions in one bounded setup snapshot.
pub const MAX_SETUP_CONDITIONS: usize = 160;
/// Hard cap on the canonical byte size of one bounded setup snapshot.
pub const MAX_SETUP_SNAPSHOT_BYTES: usize = 64 * 1024;

/// One generic setup condition. The scope string is adapter vocabulary
/// (for example `session:model` or `instruction:<key>`); the Store treats
/// it as an opaque equality key and never parses it. A condition carries
/// either an effective `revision` (session-scoped state) or a
/// `desired_digest` (one owned entry; `None` with the key present in JSON
/// means "must be absent").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetupCondition {
    pub scope: String,
    pub revision: Option<String>,
    pub desired_digest: Option<String>,
}

impl SetupCondition {
    pub fn to_json(&self) -> Value {
        let mut condition = json!({"scope": self.scope});
        if let Some(revision) = &self.revision {
            condition["revision"] = json!(revision);
        } else {
            condition["desired_digest"] = match &self.desired_digest {
                Some(digest) => json!(digest),
                None => Value::Null,
            };
        }
        condition
    }

    pub fn from_json(value: &Value) -> Result<Self> {
        let scope = model::text(value, "scope")?.to_owned();
        if scope.len() > 512 {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup condition has an invalid scope",
            ));
        }
        let digest_field = |field: &str| -> Result<Option<String>> {
            match value.get(field) {
                None | Some(Value::Null) => Ok(None),
                Some(digest) => {
                    let digest = digest.as_str().ok_or_else(|| {
                        Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            format!("setup condition {field} is not a digest reference"),
                        )
                    })?;
                    let hex = digest
                        .strip_prefix("sha256:")
                        .filter(|hex| {
                            hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                        .ok_or_else(|| {
                            Error::new(
                                "PREREQUISITE_EVIDENCE_INVALID",
                                format!("setup condition {field} is not a SHA-256 reference"),
                            )
                        })?;
                    Ok(Some(format!("sha256:{}", hex.to_ascii_lowercase())))
                }
            }
        };
        let revision = digest_field("revision")?;
        let has_desired = value
            .as_object()
            .is_some_and(|object| object.contains_key("desired_digest"));
        if revision.is_some() == has_desired {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup condition must carry exactly one of revision or desired_digest",
            ));
        }
        let desired_digest = if has_desired {
            digest_field("desired_digest")?
        } else {
            None
        };
        Ok(Self {
            scope,
            revision,
            desired_digest,
        })
    }
}

/// A bounded snapshot of the whole setup proven at a prerequisite's
/// settlement: the prerequisite's own condition plus every other
/// effective condition the adapter could prove from recorded state at
/// that moment. One `prerequisite_operation_id` alone orders a single
/// step; this snapshot is what binds the dependent Operation to the
/// whole setup that step was part of. It is a flat condition list —
/// deliberately not a DAG or workflow language. `setup_digest` is the
/// SHA-256 of the canonical condition list, so a stored snapshot cannot
/// be edited without changing its digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetupSnapshot {
    pub setup_digest: String,
    pub conditions: Vec<SetupCondition>,
}

impl SetupSnapshot {
    pub fn new(conditions: Vec<SetupCondition>) -> Result<Self> {
        if conditions.len() > MAX_SETUP_CONDITIONS {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot exceeds the condition-count bound",
            ));
        }
        let snapshot = Self {
            setup_digest: Self::digest(&conditions)?,
            conditions,
        };
        if model::canonical(&snapshot.to_json())?.len() > MAX_SETUP_SNAPSHOT_BYTES {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot exceeds the byte bound",
            ));
        }
        Ok(snapshot)
    }

    pub fn digest(conditions: &[SetupCondition]) -> Result<String> {
        let list: Vec<Value> = conditions.iter().map(SetupCondition::to_json).collect();
        let canonical = model::canonical(&json!({"conditions": list}))?;
        Ok(format!("sha256:{}", model::digest(canonical.as_bytes())))
    }

    pub fn to_json(&self) -> Value {
        json!({
            "setup_digest": self.setup_digest,
            "conditions": self.conditions.iter().map(SetupCondition::to_json).collect::<Vec<_>>(),
        })
    }

    pub fn from_json(value: &Value) -> Result<Self> {
        if model::canonical(value)?.len() > MAX_SETUP_SNAPSHOT_BYTES {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot exceeds the byte bound",
            ));
        }
        let raw_conditions = value["conditions"].as_array().ok_or_else(|| {
            Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot has no condition list",
            )
        })?;
        if raw_conditions.len() > MAX_SETUP_CONDITIONS {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot exceeds the condition-count bound",
            ));
        }
        let mut conditions = Vec::with_capacity(raw_conditions.len());
        for raw in raw_conditions {
            let condition = SetupCondition::from_json(raw)?;
            if conditions
                .iter()
                .any(|existing: &SetupCondition| existing.scope == condition.scope)
            {
                return Err(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "setup snapshot contains a duplicate condition scope",
                ));
            }
            conditions.push(condition);
        }
        let recorded = model::text(value, "setup_digest")?;
        if Self::digest(&conditions)? != recorded {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "setup snapshot digest does not match its conditions",
            ));
        }
        Ok(Self {
            setup_digest: recorded.to_owned(),
            conditions,
        })
    }
}

/// Which binding-state slot an adapter-produced effective-configuration
/// record folds into. The slot vocabulary is generic controller state;
/// the record contents remain adapter-owned data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectiveSlot {
    Settings,
    Agent,
    Model,
}

/// An adapter-produced effective-configuration record plus its slot.
#[derive(Clone, Debug)]
pub struct EffectiveRecord {
    pub slot: EffectiveSlot,
    pub value: Value,
}

/// An adapter-parsed expectation for one saved configure request. Opaque
/// to the Store: it is only ever handed back to the producing validator.
#[derive(Debug)]
pub enum Expectation {
    OpenCodeV2(opencode_v2::ConfigurationExpectation),
}

/// Adapter-validated evidence for one settled configure result. Opaque to
/// the Store for the same reason as [`Expectation`].
#[derive(Debug)]
pub enum ValidatedEvidence {
    OpenCodeV2(opencode_v2::ValidatedConfiguration),
}

/// The prerequisite validator registered for one runtime kind.
#[derive(Debug)]
pub enum Validator {
    OpenCodeV2(opencode_v2::PrerequisiteValidator),
}

/// The validator registry, keyed by runtime kind. A runtime with no
/// registered validator does not implement persisted configure-to-input
/// prerequisites at all.
pub fn validator_for(runtime: &str) -> Option<Validator> {
    match runtime {
        opencode_v2::RUNTIME => Some(Validator::OpenCodeV2(opencode_v2::PrerequisiteValidator)),
        _ => None,
    }
}

// Dispatch is by exhaustive destructuring: a payload can only ever reach
// the validator variant that produced it. Registering a second runtime
// makes every method below fail to compile until its dispatch is extended,
// so foreign evidence can never silently validate.
impl Validator {
    /// Parse the saved configure request into an opaque expectation.
    pub fn parse_expectation(&self, original_request: &Value) -> Result<Expectation> {
        let Self::OpenCodeV2(validator) = self;
        validator
            .parse_expectation(original_request)
            .map(Expectation::OpenCodeV2)
    }

    /// The adapter contract revision a dependent Operation must require.
    pub fn contract_revision(&self, expectation: &Expectation) -> Result<&'static str> {
        let (Self::OpenCodeV2(validator), Expectation::OpenCodeV2(expectation)) =
            (self, expectation);
        Ok(validator.contract_revision(expectation))
    }

    /// The generic condition scope of an expectation, as an opaque
    /// equality key (`session:agent`, `instruction:<key>`, ...). Two
    /// expectations share a scope exactly when their scope keys are equal;
    /// the Store compares the keys and never parses them.
    pub fn scope(&self, expectation: &Expectation) -> Result<String> {
        let (Self::OpenCodeV2(validator), Expectation::OpenCodeV2(expectation)) =
            (self, expectation);
        Ok(validator.scope(expectation))
    }

    /// Validate the vendor-owned fields of a recorded operation contract
    /// against the expectation. Order-scope ownership (binding/generation)
    /// is generic and stays with the Store.
    pub fn validate_contract(&self, expectation: &Expectation, contract: &Value) -> Result<()> {
        let (Self::OpenCodeV2(validator), Expectation::OpenCodeV2(expectation)) =
            (self, expectation);
        validator.validate_contract(expectation, contract)
    }

    /// Validate a settled configure result's adapter-owned details and
    /// produce validated evidence. The Store checks the generic result
    /// envelope (operation identity, outcome, native root/scope
    /// ownership) before calling this.
    pub fn validate_applied(
        &self,
        expectation: &Expectation,
        contract: &Value,
        details: &Value,
    ) -> Result<ValidatedEvidence> {
        let (Self::OpenCodeV2(validator), Expectation::OpenCodeV2(expectation)) =
            (self, expectation);
        validator
            .validate_applied(expectation, contract, details)
            .map(ValidatedEvidence::OpenCodeV2)
    }

    /// The adapter contract revision carried by validated evidence.
    pub fn evidence_contract_revision(&self, evidence: &ValidatedEvidence) -> Result<&'static str> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        Ok(validator.contract_revision(&evidence.expectation))
    }

    /// Whether two evidences describe the same effective state (exact
    /// requested target, and for session-scoped state the exact applied
    /// revision).
    pub fn same_effective(
        &self,
        expected: &ValidatedEvidence,
        later: &ValidatedEvidence,
    ) -> Result<bool> {
        let (
            Self::OpenCodeV2(validator),
            ValidatedEvidence::OpenCodeV2(expected),
            ValidatedEvidence::OpenCodeV2(later),
        ) = (self, expected, later);
        Ok(validator.same_effective(expected, later))
    }

    /// Whether two evidences were proven on the same condition scope.
    pub fn evidence_same_scope(
        &self,
        expected: &ValidatedEvidence,
        later: &ValidatedEvidence,
    ) -> Result<bool> {
        let (
            Self::OpenCodeV2(validator),
            ValidatedEvidence::OpenCodeV2(expected),
            ValidatedEvidence::OpenCodeV2(later),
        ) = (self, expected, later);
        Ok(validator.same_scope(expected, later))
    }

    /// Whether the current native observation satisfies the validated
    /// condition at full adapter fidelity. `Ok(None)` means the
    /// observation is incomplete and proves nothing either way.
    pub fn snapshot_matches(
        &self,
        evidence: &ValidatedEvidence,
        native_observation: &Value,
    ) -> Result<Option<bool>> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        validator.snapshot_matches(evidence, native_observation)
    }

    /// The generic condition proven by validated evidence: its scope
    /// plus the revision or desired digest a later check compares
    /// against. This is the form persisted in setup snapshots.
    pub fn evidence_condition(&self, evidence: &ValidatedEvidence) -> Result<SetupCondition> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        Ok(validator.condition(evidence))
    }

    /// Whether a persisted generic condition still holds against a later
    /// Operation's validated evidence on the same scope.
    pub fn condition_holds(
        &self,
        condition: &SetupCondition,
        later: &ValidatedEvidence,
    ) -> Result<bool> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(later)) = (self, later);
        Ok(validator.condition_holds(condition, later))
    }

    /// Whether the current native observation satisfies a persisted
    /// generic condition. `Ok(None)` means the relevant part of the
    /// observation is incomplete and proves nothing either way.
    pub fn condition_matches(
        &self,
        condition: &SetupCondition,
        native_observation: &Value,
    ) -> Result<Option<bool>> {
        let Self::OpenCodeV2(validator) = self;
        validator.condition_matches(condition, native_observation)
    }

    /// Emit the bounded setup snapshot frozen at this prerequisite's
    /// settlement: the just-validated own condition plus every wider
    /// effective condition the adapter can prove from the binding's
    /// recorded observation (effective records and native subtrees).
    /// Emission is fail-soft — `Ok(None)` leaves the barrier on its
    /// single-condition form rather than failing a proven result.
    pub fn setup_snapshot(
        &self,
        evidence: &ValidatedEvidence,
        binding_observation: &Value,
    ) -> Result<Option<SetupSnapshot>> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        validator.setup_snapshot(evidence, binding_observation)
    }

    /// Build the effective-configuration record folded into binding
    /// state for an applied configure result.
    pub fn applied_record(
        &self,
        evidence: &ValidatedEvidence,
        operation_id: &str,
        result: &Value,
        observed_at_ms: i64,
    ) -> Result<EffectiveRecord> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        let (slot, value) =
            validator.applied_record(evidence, operation_id, result, observed_at_ms);
        Ok(EffectiveRecord { slot, value })
    }

    /// Fold a native observation into effective-configuration records.
    /// `binding_observation` is the binding's currently recorded
    /// observation object (it carries the prior effective records, whose
    /// originating Operation identity is preserved when the revision is
    /// unchanged).
    pub fn observed_records(
        &self,
        binding_observation: &Value,
        native_state: &Value,
        observation_id: i64,
        observed_at_ms: i64,
    ) -> Result<Vec<EffectiveRecord>> {
        let Self::OpenCodeV2(validator) = self;
        validator
            .observed_records(
                binding_observation,
                native_state,
                observation_id,
                observed_at_ms,
            )
            .map(|records| {
                records
                    .into_iter()
                    .map(|(slot, value)| EffectiveRecord { slot, value })
                    .collect()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sha(pair: &str) -> String {
        format!("sha256:{}", pair.repeat(32))
    }

    fn instruction_contract() -> Value {
        json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":"binding","generation":1},
            "completion_condition":"native_configuration_applied",
            "application_boundary":"next_step_boundary",
            "replay_policy":"readback_only_no_mutation_replay",
            "fallback_used":false,
            "contract_revision":"opencode-instruction-entry-v1",
            "configuration_kind":"instruction_entry"
        })
    }

    fn instruction_details(desired_digest: &str) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"instruction_entry",
            "action":"put",
            "key":"eliot.policy",
            "application_scope":"session",
            "application_boundary":"next_step_boundary",
            "native_applied":true,
            "model_work_started":false,
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":"opencode-instruction-entry-v1",
            "settings_revision_kind":"eliot_owned_instruction_entries_v1",
            "read_method":"experimental.session.instructions.entry.list",
            "desired_digest":desired_digest,
            "evidence":"post_mutation_exact_readback",
            "mutation_sent":true,
            "settings_revision":sha("aa"),
            "entries_revision":sha("bb")
        })
    }

    #[test]
    fn foreign_runtime_kind_has_no_validator() {
        // A runtime kind with no registered validator cannot satisfy a
        // persisted prerequisite at all; there is no fallback validation.
        assert!(validator_for("claude_code").is_none());
        assert!(validator_for("zed").is_none());
        assert!(validator_for("").is_none());
        assert!(validator_for(opencode_v2::RUNTIME).is_some());
    }

    #[test]
    fn foreign_adapter_evidence_does_not_pass_the_opencode_validator() {
        let validator = validator_for(opencode_v2::RUNTIME).unwrap();
        let original = json!({"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":true}}}});
        let expectation = validator.parse_expectation(&original).unwrap();
        let Expectation::OpenCodeV2(parsed) = &expectation;
        let desired = parsed.desired_digest().unwrap().to_owned();

        // A contract stamped under a foreign adapter's revision is a
        // contract mismatch, never a satisfied prerequisite.
        let mut foreign_contract = instruction_contract();
        foreign_contract["contract_revision"] = json!("foreign-config-v9");
        assert_eq!(
            validator
                .validate_contract(&expectation, &foreign_contract)
                .unwrap_err()
                .code,
            "PREREQUISITE_CONTRACT_MISMATCH"
        );

        // Result details stamped under a foreign adapter's revision kind
        // are invalid evidence even when every digest is well-formed.
        let mut foreign_details = instruction_details(&desired);
        foreign_details["settings_revision_kind"] = json!("foreign_settings_v9");
        assert_eq!(
            validator
                .validate_applied(&expectation, &instruction_contract(), &foreign_details)
                .unwrap_err()
                .code,
            "PREREQUISITE_EVIDENCE_INVALID"
        );

        // The same evidence under the adapter's own revisions validates.
        assert!(
            validator
                .validate_applied(
                    &expectation,
                    &instruction_contract(),
                    &instruction_details(&desired)
                )
                .is_ok()
        );
    }

    fn agent_contract() -> Value {
        json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":"binding","generation":1},
            "completion_condition":"native_configuration_applied",
            "application_boundary":"subsequent_provider_turn",
            "replay_policy":"readback_only_no_mutation_replay",
            "fallback_used":false,
            "contract_revision":"opencode-session-agent-v1",
            "configuration_kind":"session_agent"
        })
    }

    fn agent_details(desired_digest: &str, settings_revision: &str) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"session_agent",
            "action":"switch",
            "application_scope":"session",
            "application_boundary":"subsequent_provider_turn",
            "native_applied":true,
            "model_work_started":false,
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":"opencode-session-agent-v1",
            "settings_revision_kind":"opencode_session_agent_v1",
            "read_method":"session.get+agent.list",
            "desired_digest":desired_digest,
            "evidence":"post_mutation_exact_readback",
            "mutation_sent":true,
            "settings_revision":settings_revision,
            "agent_id":"build",
            "catalog_verified":true,
            "agent_mode":"primary",
            "agent_hidden":false,
            "agent_model_override":false,
            "agent_catalog_revision":sha("cc"),
            "agent_definition_digest":sha("dd")
        })
    }

    fn agent_evidence(validator: &Validator, settings_revision: &str) -> ValidatedEvidence {
        let original = json!({"settings":{"agent":{"id":"build"}}});
        let expectation = validator.parse_expectation(&original).unwrap();
        let Expectation::OpenCodeV2(parsed) = &expectation;
        let desired = parsed.desired_digest().unwrap().to_owned();
        validator
            .validate_applied(
                &expectation,
                &agent_contract(),
                &agent_details(&desired, settings_revision),
            )
            .unwrap()
    }

    fn instruction_evidence(validator: &Validator, value: Value) -> ValidatedEvidence {
        let original = json!({"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":value}}});
        let expectation = validator.parse_expectation(&original).unwrap();
        let Expectation::OpenCodeV2(parsed) = &expectation;
        let desired = parsed.desired_digest().unwrap().to_owned();
        validator
            .validate_applied(
                &expectation,
                &instruction_contract(),
                &instruction_details(&desired),
            )
            .unwrap()
    }

    #[test]
    fn setup_snapshot_bounds_and_digest_are_enforced() {
        let condition = |scope: &str| SetupCondition {
            scope: scope.to_owned(),
            revision: Some(sha("aa")),
            desired_digest: None,
        };
        // The condition-count cap is a hard bound, not a truncation.
        let too_many: Vec<_> = (0..=MAX_SETUP_CONDITIONS)
            .map(|index| condition(&format!("instruction:key{index}")))
            .collect();
        assert!(SetupSnapshot::new(too_many).is_err());
        // The byte cap binds even under the count cap.
        let wide: Vec<_> = (0..MAX_SETUP_CONDITIONS)
            .map(|index| {
                condition(&format!(
                    "instruction:{}",
                    "k".repeat(400) + &index.to_string()
                ))
            })
            .collect();
        assert!(SetupSnapshot::new(wide).is_err());

        let snapshot = SetupSnapshot::new(vec![
            condition("session:agent"),
            SetupCondition {
                scope: "instruction:eliot.policy".to_owned(),
                revision: None,
                desired_digest: Some(sha("bb")),
            },
        ])
        .unwrap();
        let encoded = snapshot.to_json();
        assert_eq!(SetupSnapshot::from_json(&encoded).unwrap(), snapshot);
        // A tampered condition no longer matches the recorded digest.
        let mut tampered = encoded.clone();
        tampered["conditions"][0]["revision"] = json!(sha("ff"));
        assert_eq!(
            SetupSnapshot::from_json(&tampered).unwrap_err().code,
            "PREREQUISITE_EVIDENCE_INVALID"
        );
        // Duplicate scopes are rejected: one condition per scope.
        let mut duplicated = encoded;
        duplicated["conditions"]
            .as_array_mut()
            .unwrap()
            .push(json!({"scope":"session:agent","revision":sha("ee")}));
        assert!(SetupSnapshot::from_json(&duplicated).is_err());
    }

    #[test]
    fn setup_snapshot_emission_covers_the_whole_proven_setup() {
        let validator = validator_for(opencode_v2::RUNTIME).unwrap();
        let own_revision = sha("11");
        let evidence = agent_evidence(&validator, &own_revision);
        let policy_digest = sha("22");
        let style_digest = sha("33");
        let model_revision = sha("44");
        let binding_observation = json!({
            "observed_at_ms": 5000,
            "native": {
                "configuration": {
                    "complete": true,
                    "revision": sha("55"),
                    "owned_entries": [
                        {"key": "eliot.policy", "value_digest": policy_digest},
                        {"key": "eliot.style", "value_digest": style_digest}
                    ]
                },
                "agent_configuration": {
                    "complete": true,
                    "settings_revision": sha("66"),
                    "agent_id": "build",
                    "definition_digest": sha("dd")
                },
                "model_configuration": {
                    "complete": true,
                    "settings_revision": model_revision,
                    "catalog_revision": sha("77"),
                    "model": {"id": "m", "providerID": "p", "variant": "v"},
                    "definition_digest": sha("88"),
                    "variant_digest": sha("99"),
                    "enabled": true,
                    "status": "active"
                }
            }
        });
        let snapshot = validator
            .setup_snapshot(&evidence, &binding_observation)
            .unwrap()
            .unwrap();
        // Every proven condition is present exactly once, sorted by
        // scope; the prerequisite's own condition carries its just-
        // validated revision, not the older native read of the same axis.
        let scopes: Vec<&str> = snapshot
            .conditions
            .iter()
            .map(|condition| condition.scope.as_str())
            .collect();
        assert_eq!(
            scopes,
            [
                "instruction:eliot.policy",
                "instruction:eliot.style",
                "session:agent",
                "session:model"
            ]
        );
        assert_eq!(
            snapshot.conditions[0].desired_digest.as_deref(),
            Some(policy_digest.as_str())
        );
        assert_eq!(
            snapshot.conditions[2].revision.as_deref(),
            Some(own_revision.as_str())
        );
        assert_eq!(
            snapshot.conditions[3].revision.as_deref(),
            Some(model_revision.as_str())
        );
        assert_eq!(
            snapshot.setup_digest,
            SetupSnapshot::digest(&snapshot.conditions).unwrap()
        );

        // The model condition blocks when a catalog reload changed the
        // selected definition (new settings revision) or disabled the
        // selection, and holds while the revision is unchanged.
        let model_condition = &snapshot.conditions[3];
        let mut reloaded = binding_observation["native"].clone();
        reloaded["model_configuration"]["settings_revision"] = json!(sha("ab"));
        assert_eq!(
            validator
                .condition_matches(model_condition, &reloaded)
                .unwrap(),
            Some(false)
        );
        let mut disabled = binding_observation["native"].clone();
        disabled["model_configuration"]["enabled"] = json!(false);
        assert_eq!(
            validator
                .condition_matches(model_condition, &disabled)
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            validator
                .condition_matches(model_condition, &binding_observation["native"])
                .unwrap(),
            Some(true)
        );

        // A later instruction Operation on a snapshot scope holds only
        // while it proves the exact recorded desired digest.
        let same = instruction_evidence(&validator, json!({"review_before_submit": true}));
        let recorded = validator.evidence_condition(&same).unwrap();
        assert_eq!(recorded.scope, "instruction:eliot.policy");
        assert!(validator.condition_holds(&recorded, &same).unwrap());
        let changed = instruction_evidence(&validator, json!({"review_before_submit": false}));
        assert!(!validator.condition_holds(&recorded, &changed).unwrap());
    }
}
