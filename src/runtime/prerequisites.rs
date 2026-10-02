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
use crate::error::Result;
use serde_json::Value;

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

    /// The generic condition scope of validated evidence, as an opaque
    /// equality key (see [`Validator::scope`]).
    pub fn evidence_scope(&self, evidence: &ValidatedEvidence) -> Result<String> {
        let (Self::OpenCodeV2(validator), ValidatedEvidence::OpenCodeV2(evidence)) =
            (self, evidence);
        Ok(validator.scope(&evidence.expectation))
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
}
