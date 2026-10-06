//! OpenCode's prerequisite validator: the adapter-side half of the
//! configure-to-input barrier. Every OpenCode-specific kind, contract
//! revision, and native-evidence rule for prerequisites lives here, behind
//! the runtime-side registry in `crate::runtime::prerequisites`. The Store
//! sees only opaque expectations and validated evidence.
use super::configuration::{
    AGENT_SETTINGS_REVISION_KIND, ConfigurationExpectation, INSTRUCTION_SETTINGS_REVISION_KIND,
    MODEL_SETTINGS_REVISION_KIND, ValidatedConfiguration,
};
use crate::{
    error::{Error, Result},
    model,
    runtime::prerequisites::{EffectiveSlot, SetupCondition, SetupSnapshot},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const INSTRUCTION_STATE_CONTRACT_REVISION: &str = "opencode-configure-prerequisite-v1";
const AGENT_STATE_CONTRACT_REVISION: &str = "opencode-session-agent-state-v1";
const MODEL_STATE_CONTRACT_REVISION: &str = "opencode-session-model-state-v1";

/// Stateless OpenCode implementation of the prerequisite validator
/// contract. Registered for the `opencode_v2` runtime kind only.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Validator;

fn sha256(value: &Value, field: &str) -> Result<String> {
    let digest = model::text(value, field)?;
    let hex = digest
        .strip_prefix("sha256:")
        .filter(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                format!("{field} is not a SHA-256 reference"),
            )
        })?;
    Ok(format!("sha256:{}", hex.to_ascii_lowercase()))
}

fn compact_model_ref(value: &Value) -> Result<Value> {
    model::fields(value, &["id", "providerID", "variant"]).map_err(|_| {
        Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "native model snapshot has an invalid model reference",
        )
    })?;
    let invalid = || {
        Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "native model snapshot has an invalid model reference",
        )
    };
    let id = model::text(value, "id").map_err(|_| invalid())?;
    let provider = model::text(value, "providerID").map_err(|_| invalid())?;
    let variant = match value.get("variant") {
        None | Some(Value::Null) => None,
        Some(_) => Some(
            model::text(value, "variant")
                .map(str::to_owned)
                .map_err(|_| invalid())?,
        ),
    };
    if [id, provider]
        .into_iter()
        .chain(variant.as_deref())
        .any(|part| {
            part.is_empty() || part.len() > 256 || part.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "native model snapshot has an invalid model reference",
        ));
    }
    Ok(json!({"id":id,"providerID":provider,"variant":variant}))
}

fn validate_evidence(details: &Value) -> Result<()> {
    let evidence = model::text(details, "evidence")?;
    let mutation_sent = details["mutation_sent"]
        .as_bool()
        .ok_or_else(|| Error::new("PREREQUISITE_EVIDENCE_INVALID", "missing mutation evidence"))?;
    if !matches!(
        (evidence, mutation_sent),
        ("preexisting_exact_readback", false)
            | ("post_mutation_exact_readback", true)
            | ("exact_state_reconciliation", false)
    ) {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "configure result has an unsupported readback proof",
        ));
    }
    Ok(())
}

impl Validator {
    pub(crate) fn parse_expectation(&self, original: &Value) -> Result<ConfigurationExpectation> {
        super::configuration_expectation(&original["settings"]).map_err(|_| {
            Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "saved configure request is not a supported OpenCode configuration change",
            )
        })
    }

    pub(crate) fn contract_revision(&self, expectation: &ConfigurationExpectation) -> &'static str {
        expectation.contract_revision()
    }

    /// Generic condition scope key for an expectation: session-scoped
    /// state collapses to one key per axis, instruction entries key by
    /// their owned entry name. Equality of these keys is exactly the
    /// same-scope relation the barrier needs.
    pub(crate) fn scope(&self, expectation: &ConfigurationExpectation) -> String {
        match expectation {
            ConfigurationExpectation::InstructionEntry { key, .. } => {
                format!("instruction:{key}")
            }
            ConfigurationExpectation::SessionAgent { .. } => "session:agent".to_owned(),
            ConfigurationExpectation::SessionModel { .. } => "session:model".to_owned(),
        }
    }

    /// The OpenCode-owned contract fields. The generic order-scope
    /// ownership check (binding/generation) stays with the Store and is
    /// deliberately not repeated here.
    pub(crate) fn validate_contract(
        &self,
        expectation: &ConfigurationExpectation,
        contract: &Value,
    ) -> Result<()> {
        if contract["effect_scope"] != "native_session"
            || contract["completion_condition"] != "native_configuration_applied"
            || contract["application_boundary"] != expectation.application_boundary()
            || contract["replay_policy"] != "readback_only_no_mutation_replay"
            || contract["fallback_used"] != false
            || contract["contract_revision"] != expectation.contract_revision()
            || contract
                .get("configuration_kind")
                .is_some_and(|kind| kind != expectation.kind())
        {
            return Err(Error::new(
                "PREREQUISITE_CONTRACT_MISMATCH",
                "prerequisite does not carry the required native-configuration contract",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_applied(
        &self,
        expectation: &ConfigurationExpectation,
        contract: &Value,
        details: &Value,
    ) -> Result<ValidatedConfiguration> {
        self.validate_contract(expectation, contract)?;
        if details["completion_condition"] != "native_configuration_applied"
            || details["configuration_kind"] != expectation.kind()
            || details["action"] != expectation.action()
            || details["application_scope"] != "session"
            || details["application_boundary"] != expectation.application_boundary()
            || details["native_applied"] != true
            || details["model_work_started"] != false
            || details["replay_policy"] != "readback_only_no_mutation_replay"
            || details["contract_revision"] != expectation.contract_revision()
            || details["settings_revision_kind"] != expectation.settings_revision_kind()
            || details["read_method"] != expectation.read_method()
        {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "configure result does not prove the required typed native application",
            ));
        }
        match expectation.desired_digest() {
            Some(expected) if details["desired_digest"].as_str() == Some(expected) => {}
            None if details["desired_digest"].is_null() => {}
            _ => {
                return Err(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "configure result does not match the saved requested value",
                ));
            }
        }
        validate_evidence(details)?;
        let settings_revision = sha256(details, "settings_revision")?;
        let (
            entries_revision,
            agent_definition_digest,
            model_definition_digest,
            model_variant_digest,
        ) = match expectation {
            ConfigurationExpectation::InstructionEntry { action, key, .. } => {
                if details["action"].as_str() != Some(action.as_str())
                    || details["key"].as_str() != Some(key.as_str())
                {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "instruction-entry result does not match the saved target",
                    ));
                }
                (Some(sha256(details, "entries_revision")?), None, None, None)
            }
            ConfigurationExpectation::SessionAgent { agent_id, .. } => {
                if details["agent_id"].as_str() != Some(agent_id.as_str())
                    || details["catalog_verified"] != true
                    || !matches!(
                        details["agent_mode"].as_str(),
                        Some("subagent" | "primary" | "all")
                    )
                    || details["agent_hidden"].as_bool().is_none()
                    || details["agent_model_override"].as_bool().is_none()
                {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "session-agent result does not match the saved target",
                    ));
                }
                sha256(details, "agent_catalog_revision")?;
                (
                    None,
                    Some(sha256(details, "agent_definition_digest")?),
                    None,
                    None,
                )
            }
            ConfigurationExpectation::SessionModel { model, .. } => {
                if details["model"] != json!(model)
                    || details["catalog_verified"] != true
                    || details["model_enabled"] != true
                    || !matches!(
                        details["model_status"].as_str(),
                        Some("alpha" | "beta" | "deprecated" | "active")
                    )
                {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "session-model result does not match the saved route target",
                    ));
                }
                sha256(details, "model_catalog_revision")?;
                (
                    None,
                    None,
                    Some(sha256(details, "model_definition_digest")?),
                    Some(sha256(details, "model_variant_digest")?),
                )
            }
        };
        Ok(ValidatedConfiguration {
            expectation: expectation.clone(),
            settings_revision,
            entries_revision,
            agent_definition_digest,
            model_definition_digest,
            model_variant_digest,
        })
    }

    pub(crate) fn same_scope(
        &self,
        expected: &ValidatedConfiguration,
        later: &ValidatedConfiguration,
    ) -> bool {
        expected.expectation.same_scope(&later.expectation)
    }

    pub(crate) fn same_effective(
        &self,
        expected: &ValidatedConfiguration,
        later: &ValidatedConfiguration,
    ) -> bool {
        if expected.expectation != later.expectation {
            return false;
        }
        match &expected.expectation {
            ConfigurationExpectation::InstructionEntry { .. } => true,
            ConfigurationExpectation::SessionAgent { .. }
            | ConfigurationExpectation::SessionModel { .. } => {
                expected.settings_revision == later.settings_revision
            }
        }
    }

    pub(crate) fn snapshot_matches(
        &self,
        validated: &ValidatedConfiguration,
        native: &Value,
    ) -> Result<Option<bool>> {
        match &validated.expectation {
            ConfigurationExpectation::InstructionEntry {
                action,
                key,
                desired_digest,
            } => {
                let configuration = &native["configuration"];
                if configuration["complete"] != true {
                    return Ok(None);
                }
                sha256(configuration, "revision")?;
                let entries = configuration["owned_entries"].as_array().ok_or_else(|| {
                    Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "invalid native configuration snapshot",
                    )
                })?;
                let mut keys = BTreeSet::new();
                let mut found = None;
                for entry in entries {
                    let observed_key = model::text(entry, "key")?;
                    if !keys.insert(observed_key.to_owned()) {
                        return Err(Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            "native configuration snapshot contains a duplicate key",
                        ));
                    }
                    let digest = sha256(entry, "value_digest")?;
                    if observed_key == key {
                        found = Some(digest);
                    }
                }
                Ok(Some(match action.as_str() {
                    "put" => found.as_deref() == desired_digest.as_deref(),
                    "remove" => found.is_none(),
                    _ => false,
                }))
            }
            ConfigurationExpectation::SessionAgent { agent_id, .. } => {
                let agent = &native["agent_configuration"];
                if agent["complete"] != true {
                    return Ok(None);
                }
                let revision = sha256(agent, "settings_revision")?;
                let definition = match agent["definition_digest"].as_str() {
                    Some(_) => Some(sha256(agent, "definition_digest")?),
                    None if agent["definition_digest"].is_null() => None,
                    None => {
                        return Err(Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            "native agent snapshot has an invalid definition digest",
                        ));
                    }
                };
                Ok(Some(
                    agent["agent_id"].as_str() == Some(agent_id.as_str())
                        && revision == validated.settings_revision
                        && definition.as_deref() == validated.agent_definition_digest.as_deref(),
                ))
            }
            ConfigurationExpectation::SessionModel {
                model: expected, ..
            } => {
                let observed = &native["model_configuration"];
                if observed["complete"] != true {
                    return Ok(None);
                }
                let revision = sha256(observed, "settings_revision")?;
                sha256(observed, "catalog_revision")?;
                let definition = match observed["definition_digest"].as_str() {
                    Some(_) => Some(sha256(observed, "definition_digest")?),
                    None if observed["definition_digest"].is_null() => None,
                    None => {
                        return Err(Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            "native model snapshot has an invalid definition digest",
                        ));
                    }
                };
                let variant = match observed["variant_digest"].as_str() {
                    Some(_) => Some(sha256(observed, "variant_digest")?),
                    None if observed["variant_digest"].is_null() => None,
                    None => {
                        return Err(Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            "native model snapshot has an invalid variant digest",
                        ));
                    }
                };
                let model_matches = observed
                    .get("model")
                    .filter(|model| model.is_object())
                    .map(compact_model_ref)
                    .transpose()?
                    .is_some_and(|model| model == json!(expected));
                Ok(Some(
                    model_matches
                        && observed["enabled"] == true
                        && matches!(
                            observed["status"].as_str(),
                            Some("alpha" | "beta" | "deprecated" | "active")
                        )
                        && revision == validated.settings_revision
                        && definition.as_deref() == validated.model_definition_digest.as_deref()
                        && variant.as_deref() == validated.model_variant_digest.as_deref(),
                ))
            }
        }
    }

    /// The generic condition one validated result proves: session axes
    /// by their effective settings revision, an instruction entry by
    /// its desired value digest (`None` = the entry must be absent).
    pub(crate) fn condition(&self, validated: &ValidatedConfiguration) -> SetupCondition {
        match &validated.expectation {
            ConfigurationExpectation::InstructionEntry {
                key,
                desired_digest,
                ..
            } => SetupCondition {
                scope: format!("instruction:{key}"),
                revision: None,
                desired_digest: desired_digest.clone(),
            },
            ConfigurationExpectation::SessionAgent { .. } => SetupCondition {
                scope: "session:agent".to_owned(),
                revision: Some(validated.settings_revision.clone()),
                desired_digest: None,
            },
            ConfigurationExpectation::SessionModel { .. } => SetupCondition {
                scope: "session:model".to_owned(),
                revision: Some(validated.settings_revision.clone()),
                desired_digest: None,
            },
        }
    }

    pub(crate) fn condition_holds(
        &self,
        condition: &SetupCondition,
        later: &ValidatedConfiguration,
    ) -> bool {
        let later = self.condition(later);
        if later.scope != condition.scope {
            return false;
        }
        if condition.revision.is_some() || later.revision.is_some() {
            return condition.revision == later.revision;
        }
        condition.desired_digest == later.desired_digest
    }

    /// Generic-condition form of the native observation check: the same
    /// native subtrees and digest strictness as `snapshot_matches`, but
    /// driven by the persisted condition instead of a live expectation.
    /// For session axes the settings revision is a digest over the
    /// selected identity and definition, so revision equality binds
    /// both; the model check additionally requires the selection to
    /// still be enabled with a live status.
    pub(crate) fn condition_matches(
        &self,
        condition: &SetupCondition,
        native: &Value,
    ) -> Result<Option<bool>> {
        if condition.scope == "session:agent" {
            let agent = &native["agent_configuration"];
            if agent["complete"] != true {
                return Ok(None);
            }
            let revision = sha256(agent, "settings_revision")?;
            match agent["definition_digest"].as_str() {
                Some(_) => {
                    sha256(agent, "definition_digest")?;
                }
                None if agent["definition_digest"].is_null() => {}
                None => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native agent snapshot has an invalid definition digest",
                    ));
                }
            }
            return Ok(Some(
                condition.revision.as_deref() == Some(revision.as_str()),
            ));
        }
        if condition.scope == "session:model" {
            let observed = &native["model_configuration"];
            if observed["complete"] != true {
                return Ok(None);
            }
            let revision = sha256(observed, "settings_revision")?;
            sha256(observed, "catalog_revision")?;
            for field in ["definition_digest", "variant_digest"] {
                if observed[field].is_string() {
                    sha256(observed, field)?;
                } else if !observed[field].is_null() {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid digest",
                    ));
                }
            }
            return Ok(Some(
                condition.revision.as_deref() == Some(revision.as_str())
                    && observed["enabled"] == true
                    && matches!(
                        observed["status"].as_str(),
                        Some("alpha" | "beta" | "deprecated" | "active")
                    ),
            ));
        }
        if let Some(key) = condition.scope.strip_prefix("instruction:") {
            let configuration = &native["configuration"];
            if configuration["complete"] != true {
                return Ok(None);
            }
            sha256(configuration, "revision")?;
            let entries = configuration["owned_entries"].as_array().ok_or_else(|| {
                Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "invalid native configuration snapshot",
                )
            })?;
            let mut keys = BTreeSet::new();
            let mut found = None;
            for entry in entries {
                let observed_key = model::text(entry, "key")?;
                if !keys.insert(observed_key.to_owned()) {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native configuration snapshot contains a duplicate key",
                    ));
                }
                let digest = sha256(entry, "value_digest")?;
                if observed_key == key {
                    found = Some(digest);
                }
            }
            return Ok(Some(found == condition.desired_digest));
        }
        Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "setup condition names a scope this adapter does not prove",
        ))
    }

    /// Emit the bounded setup snapshot for a just-settled prerequisite
    /// from the binding's recorded observation. Candidates come from the
    /// native snapshot subtrees (a complete native read proves every
    /// owned entry plus both session axes) and from the recorded
    /// effective records (exact applied results); per scope the freshest
    /// fact wins, and the prerequisite's own condition — being proven
    /// right now — always wins its scope. Any inconsistency degrades to
    /// `None` (no snapshot; the barrier keeps its single-condition
    /// form) instead of failing the proven configure result.
    pub(crate) fn setup_snapshot(
        &self,
        validated: &ValidatedConfiguration,
        binding_observation: &Value,
    ) -> Result<Option<SetupSnapshot>> {
        match self.build_setup_snapshot(validated, binding_observation) {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(_) => Ok(None),
        }
    }

    fn build_setup_snapshot(
        &self,
        validated: &ValidatedConfiguration,
        binding_observation: &Value,
    ) -> Result<SetupSnapshot> {
        let mut by_scope: BTreeMap<String, (SetupCondition, i64)> = BTreeMap::new();
        let mut offer = |condition: SetupCondition, observed_at_ms: i64| {
            let replace = by_scope
                .get(&condition.scope)
                .is_none_or(|(_, at)| observed_at_ms >= *at);
            if replace {
                by_scope.insert(condition.scope.clone(), (condition, observed_at_ms));
            }
        };

        let native = &binding_observation["native"];
        let observed_at = binding_observation["observed_at_ms"].as_i64().unwrap_or(0);
        if native.is_object() {
            let configuration = &native["configuration"];
            if configuration["complete"] == true {
                let entries = configuration["owned_entries"].as_array().ok_or_else(|| {
                    Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "invalid native configuration snapshot",
                    )
                })?;
                let mut keys = BTreeSet::new();
                for entry in entries {
                    let key = model::text(entry, "key")?;
                    if !keys.insert(key.to_owned()) {
                        return Err(Error::new(
                            "PREREQUISITE_EVIDENCE_INVALID",
                            "native configuration snapshot contains a duplicate key",
                        ));
                    }
                    offer(
                        SetupCondition {
                            scope: format!("instruction:{key}"),
                            revision: None,
                            desired_digest: Some(sha256(entry, "value_digest")?),
                        },
                        observed_at,
                    );
                }
            }
            let agent = &native["agent_configuration"];
            if agent["complete"] == true {
                offer(
                    SetupCondition {
                        scope: "session:agent".to_owned(),
                        revision: Some(sha256(agent, "settings_revision")?),
                        desired_digest: None,
                    },
                    observed_at,
                );
            }
            let model_state = &native["model_configuration"];
            if model_state["complete"] == true {
                offer(
                    SetupCondition {
                        scope: "session:model".to_owned(),
                        revision: Some(sha256(model_state, "settings_revision")?),
                        desired_digest: None,
                    },
                    observed_at,
                );
            }
        }

        let settings = &binding_observation["effective_settings"];
        if settings["complete"] == true
            && settings["kind"] == INSTRUCTION_SETTINGS_REVISION_KIND
            && settings["contract_revision"] == INSTRUCTION_STATE_CONTRACT_REVISION
            && settings["source"] == "exact_configuration_result"
            && let Some(key) = settings["key"].as_str()
        {
            let desired_digest = match settings["action"].as_str() {
                Some("put") => Some(Some(sha256(settings, "desired_digest")?)),
                Some("remove") => Some(None),
                _ => None,
            };
            if let Some(desired_digest) = desired_digest {
                offer(
                    SetupCondition {
                        scope: format!("instruction:{key}"),
                        revision: None,
                        desired_digest,
                    },
                    settings["observed_at_ms"].as_i64().unwrap_or(0),
                );
            }
        }
        let agent_slot = &binding_observation["effective_agent"];
        if agent_slot["complete"] == true
            && agent_slot["kind"] == AGENT_SETTINGS_REVISION_KIND
            && agent_slot["contract_revision"] == AGENT_STATE_CONTRACT_REVISION
        {
            offer(
                SetupCondition {
                    scope: "session:agent".to_owned(),
                    revision: Some(sha256(agent_slot, "revision")?),
                    desired_digest: None,
                },
                agent_slot["observed_at_ms"].as_i64().unwrap_or(0),
            );
        }
        let model_slot = &binding_observation["effective_model"];
        if model_slot["complete"] == true
            && model_slot["kind"] == MODEL_SETTINGS_REVISION_KIND
            && model_slot["contract_revision"] == MODEL_STATE_CONTRACT_REVISION
        {
            offer(
                SetupCondition {
                    scope: "session:model".to_owned(),
                    revision: Some(sha256(model_slot, "revision")?),
                    desired_digest: None,
                },
                model_slot["observed_at_ms"].as_i64().unwrap_or(0),
            );
        }

        offer(self.condition(validated), i64::MAX);
        let conditions = by_scope
            .into_values()
            .map(|(condition, _)| condition)
            .collect();
        SetupSnapshot::new(conditions)
    }

    pub(crate) fn applied_record(
        &self,
        validated: &ValidatedConfiguration,
        operation_id: &str,
        result: &Value,
        observed_at_ms: i64,
    ) -> (EffectiveSlot, Value) {
        match &validated.expectation {
            ConfigurationExpectation::InstructionEntry { .. } => (
                EffectiveSlot::Settings,
                json!({
                    "kind":INSTRUCTION_SETTINGS_REVISION_KIND,
                    "revision":validated.settings_revision,
                    "entries_revision":validated.entries_revision,
                    "operation_id":operation_id,
                    "key":result["details"]["key"],
                    "action":result["details"]["action"],
                    "desired_digest":result["details"]["desired_digest"],
                    "complete":true,
                    "source":"exact_configuration_result",
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":INSTRUCTION_STATE_CONTRACT_REVISION
                }),
            ),
            ConfigurationExpectation::SessionAgent { agent_id, .. } => (
                EffectiveSlot::Agent,
                json!({
                    "kind":AGENT_SETTINGS_REVISION_KIND,
                    "revision":validated.settings_revision,
                    "operation_id":operation_id,
                    "agent_id":agent_id,
                    "definition_digest":validated.agent_definition_digest,
                    "desired_digest":result["details"]["desired_digest"],
                    "complete":true,
                    "source":"exact_configuration_result",
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":AGENT_STATE_CONTRACT_REVISION
                }),
            ),
            ConfigurationExpectation::SessionModel { model, .. } => (
                EffectiveSlot::Model,
                json!({
                    "kind":MODEL_SETTINGS_REVISION_KIND,
                    "revision":validated.settings_revision,
                    "operation_id":operation_id,
                    "model":model,
                    "definition_digest":validated.model_definition_digest,
                    "variant_digest":validated.model_variant_digest,
                    "desired_digest":result["details"]["desired_digest"],
                    "complete":true,
                    "source":"exact_configuration_result",
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":MODEL_STATE_CONTRACT_REVISION
                }),
            ),
        }
    }

    pub(crate) fn observed_records(
        &self,
        binding_observation: &Value,
        native_state: &Value,
        observation_id: i64,
        observed_at_ms: i64,
    ) -> Result<Vec<(EffectiveSlot, Value)>> {
        let mut updates = Vec::new();
        let configuration = &native_state["configuration"];
        if configuration["complete"] == true {
            let revision = sha256(configuration, "revision")?;
            let prior = &binding_observation["effective_settings"];
            let operation_id = if prior["kind"] == INSTRUCTION_SETTINGS_REVISION_KIND
                && prior["contract_revision"] == INSTRUCTION_STATE_CONTRACT_REVISION
                && prior["complete"] == true
                && prior["revision"] == revision
            {
                prior["operation_id"].clone()
            } else {
                Value::Null
            };
            updates.push((
                EffectiveSlot::Settings,
                json!({
                    "kind":INSTRUCTION_SETTINGS_REVISION_KIND,
                    "revision":revision,
                    "operation_id":operation_id,
                    "complete":true,
                    "source":"native_snapshot",
                    "observation_id":observation_id,
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":INSTRUCTION_STATE_CONTRACT_REVISION
                }),
            ));
        }

        let agent = &native_state["agent_configuration"];
        if agent["complete"] == true {
            let revision = sha256(agent, "settings_revision")?;
            let definition_digest = match agent["definition_digest"].as_str() {
                Some(_) => Some(sha256(agent, "definition_digest")?),
                None if agent["definition_digest"].is_null() => None,
                None => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native agent snapshot has an invalid definition digest",
                    ));
                }
            };
            let prior = &binding_observation["effective_agent"];
            let operation_id = if prior["kind"] == AGENT_SETTINGS_REVISION_KIND
                && prior["contract_revision"] == AGENT_STATE_CONTRACT_REVISION
                && prior["complete"] == true
                && prior["revision"] == revision
            {
                prior["operation_id"].clone()
            } else {
                Value::Null
            };
            updates.push((
                EffectiveSlot::Agent,
                json!({
                    "kind":AGENT_SETTINGS_REVISION_KIND,
                    "revision":revision,
                    "operation_id":operation_id,
                    "agent_id":agent["agent_id"],
                    "definition_digest":definition_digest,
                    "complete":true,
                    "source":"native_snapshot",
                    "observation_id":observation_id,
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":AGENT_STATE_CONTRACT_REVISION
                }),
            ));
        }

        let model_state = &native_state["model_configuration"];
        if model_state["complete"] == true {
            let revision = sha256(model_state, "settings_revision")?;
            sha256(model_state, "catalog_revision")?;
            let selected = match model_state.get("model") {
                Some(value) if value.is_object() => compact_model_ref(value)?,
                Some(value) if value.is_null() => Value::Null,
                _ => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid model reference",
                    ));
                }
            };
            let definition_digest = match model_state["definition_digest"].as_str() {
                Some(_) => Some(sha256(model_state, "definition_digest")?),
                None if model_state["definition_digest"].is_null() => None,
                None => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid definition digest",
                    ));
                }
            };
            let variant_digest = match model_state["variant_digest"].as_str() {
                Some(_) => Some(sha256(model_state, "variant_digest")?),
                None if model_state["variant_digest"].is_null() => None,
                None => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid variant digest",
                    ));
                }
            };
            let enabled = match model_state.get("enabled") {
                Some(value) if value.is_boolean() || value.is_null() => value.clone(),
                _ => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid enabled flag",
                    ));
                }
            };
            let status = match model_state.get("status") {
                Some(value) if value.is_null() => Value::Null,
                Some(value)
                    if matches!(
                        value.as_str(),
                        Some("alpha" | "beta" | "deprecated" | "active")
                    ) =>
                {
                    value.clone()
                }
                _ => {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "native model snapshot has an invalid status",
                    ));
                }
            };
            let inconsistent = if selected.is_null() {
                definition_digest.is_some()
                    || variant_digest.is_some()
                    || !enabled.is_null()
                    || !status.is_null()
            } else {
                definition_digest.is_none() || enabled.is_null() || status.is_null()
            };
            if inconsistent {
                return Err(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "native model snapshot contains inconsistent selected-model evidence",
                ));
            }
            let prior = &binding_observation["effective_model"];
            let operation_id = if prior["kind"] == MODEL_SETTINGS_REVISION_KIND
                && prior["contract_revision"] == MODEL_STATE_CONTRACT_REVISION
                && prior["complete"] == true
                && prior["revision"] == revision
            {
                prior["operation_id"].clone()
            } else {
                Value::Null
            };
            updates.push((
                EffectiveSlot::Model,
                json!({
                    "kind":MODEL_SETTINGS_REVISION_KIND,
                    "revision":revision,
                    "operation_id":operation_id,
                    "model":selected,
                    "definition_digest":definition_digest,
                    "variant_digest":variant_digest,
                    "enabled":enabled,
                    "status":status,
                    "complete":true,
                    "source":"native_snapshot",
                    "observation_id":observation_id,
                    "observed_at_ms":observed_at_ms,
                    "contract_revision":MODEL_STATE_CONTRACT_REVISION
                }),
            ));
        }
        Ok(updates)
    }
}
