//! Durable session configuration delegates to OpenCode's native state machines.
//! ELIOT keeps only scoped evidence and never creates a parallel writable store.
use super::{
    ModelRef, Options, Service,
    effects::{failed, outcome, verify_directory},
    http::{Data, decode},
};
use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

const MAX_VALUE_BYTES: usize = 256 * 1024;
const MAX_OWNED_KEY_BYTES: usize = 128;
const MAX_OBSERVED_ENTRIES: usize = 128;
const MAX_AGENT_ID_BYTES: usize = 256;
const MAX_AGENT_NAME_BYTES: usize = 512;
const MAX_MODEL_NAME_BYTES: usize = 512;
const MAX_MODEL_VARIANTS: usize = 256;
const MAX_MODEL_COST_TIERS: usize = 64;
const OWNED_KEY_PREFIX: &str = "eliot.";

pub(crate) const INSTRUCTION_CONTRACT_REVISION: &str = "opencode-instruction-entry-v1";
pub(crate) const AGENT_CONTRACT_REVISION: &str = "opencode-session-agent-v1";
pub(crate) const MODEL_CONTRACT_REVISION: &str = "opencode-session-model-v1";
pub(crate) const INSTRUCTION_SETTINGS_REVISION_KIND: &str = "eliot_owned_instruction_entries_v1";
pub(crate) const AGENT_SETTINGS_REVISION_KIND: &str = "opencode_session_agent_v1";
pub(crate) const MODEL_SETTINGS_REVISION_KIND: &str = "opencode_session_model_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConfigurationExpectation {
    InstructionEntry {
        action: String,
        key: String,
        desired_digest: Option<String>,
    },
    SessionAgent {
        agent_id: String,
        desired_digest: String,
    },
    SessionModel {
        model: ModelRef,
        desired_digest: String,
    },
}

impl ConfigurationExpectation {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "instruction_entry",
            Self::SessionAgent { .. } => "session_agent",
            Self::SessionModel { .. } => "session_model",
        }
    }

    pub(crate) fn action(&self) -> &str {
        match self {
            Self::InstructionEntry { action, .. } => action,
            Self::SessionAgent { .. } | Self::SessionModel { .. } => "switch",
        }
    }

    pub(crate) fn desired_digest(&self) -> Option<&str> {
        match self {
            Self::InstructionEntry { desired_digest, .. } => desired_digest.as_deref(),
            Self::SessionAgent { desired_digest, .. }
            | Self::SessionModel { desired_digest, .. } => Some(desired_digest),
        }
    }

    pub(crate) fn contract_revision(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => INSTRUCTION_CONTRACT_REVISION,
            Self::SessionAgent { .. } => AGENT_CONTRACT_REVISION,
            Self::SessionModel { .. } => MODEL_CONTRACT_REVISION,
        }
    }

    pub(crate) fn settings_revision_kind(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => INSTRUCTION_SETTINGS_REVISION_KIND,
            Self::SessionAgent { .. } => AGENT_SETTINGS_REVISION_KIND,
            Self::SessionModel { .. } => MODEL_SETTINGS_REVISION_KIND,
        }
    }

    pub(crate) fn application_boundary(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "next_step_boundary",
            Self::SessionAgent { .. } | Self::SessionModel { .. } => "subsequent_provider_turn",
        }
    }

    pub(crate) fn read_method(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "experimental.session.instructions.entry.list",
            Self::SessionAgent { .. } => "session.get+agent.list",
            Self::SessionModel { .. } => "session.get+model.list",
        }
    }

    pub(crate) fn ensure_route(&self, options: &Options) -> Result<()> {
        if let Self::SessionModel { model, .. } = self
            && model != &options.model
        {
            return Err(Error::new(
                "NATIVE_MODEL_ROUTE_MISMATCH",
                "session model control must select the route's exact provider/model/variant",
            ));
        }
        Ok(())
    }

    pub(crate) fn same_scope(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::InstructionEntry { key: left, .. },
                Self::InstructionEntry { key: right, .. },
            ) => left == right,
            (Self::SessionAgent { .. }, Self::SessionAgent { .. })
            | (Self::SessionModel { .. }, Self::SessionModel { .. }) => true,
            _ => false,
        }
    }
}

/// Evidence proven by validating one settled configure result against its
/// saved expectation: the effective revision that now holds natively, plus
/// the per-kind digests the observation check compares against.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedConfiguration {
    pub(crate) expectation: ConfigurationExpectation,
    pub(crate) settings_revision: String,
    pub(crate) entries_revision: Option<String>,
    pub(crate) agent_definition_digest: Option<String>,
    pub(crate) model_definition_digest: Option<String>,
    pub(crate) model_variant_digest: Option<String>,
}

#[derive(Clone)]
enum ChangeAction {
    Put(Value),
    Remove,
}

#[derive(Clone)]
struct InstructionChange {
    key: String,
    action: ChangeAction,
    desired_digest: Option<String>,
}

#[derive(Clone)]
struct AgentChange {
    agent_id: String,
    desired_digest: String,
}

#[derive(Clone)]
struct ModelChange {
    model: ModelRef,
    desired_digest: String,
}

#[derive(Clone)]
enum Change {
    InstructionEntry(InstructionChange),
    SessionAgent(AgentChange),
    SessionModel(ModelChange),
}

struct InstructionReadback {
    matches: bool,
    entries_revision: String,
    settings_revision: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AgentDefinition {
    definition_digest: String,
    mode: String,
    hidden: bool,
    model: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AgentCatalog {
    revision: String,
    definitions: BTreeMap<String, AgentDefinition>,
}

struct AgentReadback {
    matches: bool,
    settings_revision: String,
    catalog_revision: String,
    definition: AgentDefinition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelDefinition {
    definition_digest: String,
    enabled: bool,
    status: String,
    variants: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelCatalog {
    revision: String,
    definitions: BTreeMap<String, ModelDefinition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelState {
    selected: Option<Value>,
    catalog: ModelCatalog,
    definition: ModelDefinition,
    variant_digest: String,
}

struct ModelReadback {
    matches: bool,
    settings_revision: String,
    catalog_revision: String,
    definition: ModelDefinition,
    variant_digest: String,
}

pub(super) fn digest_json(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(value)?.as_bytes())
    ))
}

fn native_key(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
        })
}

fn bounded_agent_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_AGENT_ID_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn agent_settings_revision(
    agent_id: Option<&str>,
    definition_digest: Option<&str>,
) -> Result<String> {
    digest_json(&json!({
        "agent_id":agent_id,
        "definition_digest":definition_digest
    }))
}

fn model_ref_value(model: &ModelRef) -> Value {
    json!({"id":model.id,"providerID":model.provider_id,"variant":model.variant})
}

fn parse_session_model(value: &Value) -> Result<Value> {
    model::fields(value, &["id", "providerID", "variant"])
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native session model is invalid"))?;
    let id = model::text(value, "id")
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native session model is invalid"))?;
    let provider = model::text(value, "providerID")
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native session model is invalid"))?;
    let variant = value
        .get("variant")
        .map(|_| {
            model::text(value, "variant")
                .map(str::to_owned)
                .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native session model is invalid"))
        })
        .transpose()?;
    if [id, provider]
        .into_iter()
        .chain(variant.as_deref())
        .any(|value| {
            value.is_empty()
                || value.len() > 256
                || value.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native session model is invalid",
        ));
    }
    Ok(json!({"id":id,"providerID":provider,"variant":variant}))
}

fn model_settings_revision(
    model_ref: Option<&Value>,
    definition_digest: Option<&str>,
    variant_digest: Option<&str>,
) -> Result<String> {
    digest_json(&json!({
        "model":model_ref,
        "definition_digest":definition_digest,
        "variant_digest":variant_digest
    }))
}

fn model_catalog_key(provider_id: &str, model_id: &str) -> String {
    format!("{provider_id}\u{0}{model_id}")
}

impl InstructionChange {
    fn expectation(&self) -> ConfigurationExpectation {
        ConfigurationExpectation::InstructionEntry {
            action: self.action_name().to_owned(),
            key: self.key.clone(),
            desired_digest: self.desired_digest.clone(),
        }
    }

    fn action_name(&self) -> &'static str {
        match &self.action {
            ChangeAction::Put(_) => "put",
            ChangeAction::Remove => "remove",
        }
    }

    fn matches(&self, entries: &[Value]) -> bool {
        let found = entries.iter().find(|entry| entry["key"] == self.key);
        match (&self.action, found) {
            (ChangeAction::Put(value), Some(entry)) => entry.get("value") == Some(value),
            (ChangeAction::Remove, None) => true,
            _ => false,
        }
    }

    fn details(
        &self,
        readback: &InstructionReadback,
        evidence: &str,
        mutation_sent: bool,
    ) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"instruction_entry",
            "action":self.action_name(),
            "key":self.key,
            "desired_digest":self.desired_digest,
            "entries_revision":readback.entries_revision,
            "settings_revision":readback.settings_revision,
            "settings_revision_kind":INSTRUCTION_SETTINGS_REVISION_KIND,
            "application_scope":"session",
            "application_boundary":"next_step_boundary",
            "native_applied":true,
            "model_work_started":false,
            "mutation_sent":mutation_sent,
            "evidence":evidence,
            "read_method":"experimental.session.instructions.entry.list",
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":INSTRUCTION_CONTRACT_REVISION
        })
    }
}

impl AgentChange {
    fn expectation(&self) -> ConfigurationExpectation {
        ConfigurationExpectation::SessionAgent {
            agent_id: self.agent_id.clone(),
            desired_digest: self.desired_digest.clone(),
        }
    }

    fn details(&self, readback: &AgentReadback, evidence: &str, mutation_sent: bool) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"session_agent",
            "action":"switch",
            "agent_id":self.agent_id,
            "desired_digest":self.desired_digest,
            "agent_definition_digest":readback.definition.definition_digest,
            "agent_catalog_revision":readback.catalog_revision,
            "agent_mode":readback.definition.mode,
            "agent_hidden":readback.definition.hidden,
            "agent_model_override":readback.definition.model.is_some(),
            "settings_revision":readback.settings_revision,
            "settings_revision_kind":AGENT_SETTINGS_REVISION_KIND,
            "application_scope":"session",
            "application_boundary":"subsequent_provider_turn",
            "native_applied":true,
            "model_work_started":false,
            "mutation_sent":mutation_sent,
            "evidence":evidence,
            "catalog_verified":true,
            "read_method":"session.get+agent.list",
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":AGENT_CONTRACT_REVISION
        })
    }
}

impl ModelChange {
    fn expectation(&self) -> ConfigurationExpectation {
        ConfigurationExpectation::SessionModel {
            model: self.model.clone(),
            desired_digest: self.desired_digest.clone(),
        }
    }

    fn details(&self, readback: &ModelReadback, evidence: &str, mutation_sent: bool) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"session_model",
            "action":"switch",
            "model":model_ref_value(&self.model),
            "desired_digest":self.desired_digest,
            "model_definition_digest":readback.definition.definition_digest,
            "model_variant_digest":readback.variant_digest,
            "model_catalog_revision":readback.catalog_revision,
            "model_enabled":readback.definition.enabled,
            "model_status":readback.definition.status,
            "settings_revision":readback.settings_revision,
            "settings_revision_kind":MODEL_SETTINGS_REVISION_KIND,
            "application_scope":"session",
            "application_boundary":"subsequent_provider_turn",
            "native_applied":true,
            "model_work_started":false,
            "mutation_sent":mutation_sent,
            "evidence":evidence,
            "catalog_verified":true,
            "read_method":"session.get+model.list",
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":MODEL_CONTRACT_REVISION
        })
    }
}

impl Change {
    fn parse(settings: &Value) -> Result<Self> {
        model::fields(settings, &["instruction_entry", "agent", "model"])?;
        match (
            settings.get("instruction_entry"),
            settings.get("agent"),
            settings.get("model"),
        ) {
            (Some(entry), None, None) => {
                model::fields(entry, &["action", "key", "value"])?;
                let key = model::text(entry, "key")?;
                if key.len() > MAX_OWNED_KEY_BYTES
                    || !key.starts_with(OWNED_KEY_PREFIX)
                    || key.len() == OWNED_KEY_PREFIX.len()
                    || !native_key(key)
                {
                    return Err(Error::invalid(
                        "instruction entry key must be eliot.<lowercase alphanumeric, dot, underscore or hyphen>",
                    ));
                }
                let action = match model::text(entry, "action")? {
                    "put" => {
                        let value = entry.get("value").cloned().ok_or_else(|| {
                            Error::invalid("put requires instruction entry value")
                        })?;
                        if model::canonical(&value)?.len() > MAX_VALUE_BYTES {
                            return Err(Error::invalid(
                                "instruction entry value exceeds the native 256 KiB boundary",
                            ));
                        }
                        ChangeAction::Put(value)
                    }
                    "remove" => {
                        if entry.get("value").is_some() {
                            return Err(Error::invalid(
                                "remove does not accept an instruction entry value",
                            ));
                        }
                        ChangeAction::Remove
                    }
                    _ => {
                        return Err(Error::invalid(
                            "instruction entry action must be put or remove",
                        ));
                    }
                };
                let desired_digest = match &action {
                    ChangeAction::Put(value) => Some(digest_json(value)?),
                    ChangeAction::Remove => None,
                };
                Ok(Self::InstructionEntry(InstructionChange {
                    key: key.to_owned(),
                    action,
                    desired_digest,
                }))
            }
            (None, Some(agent), None) => {
                model::fields(agent, &["id"])?;
                let agent_id = model::text(agent, "id")?;
                if !bounded_agent_id(agent_id) {
                    return Err(Error::invalid(
                        "agent id is empty, contains controls, or exceeds 256 bytes",
                    ));
                }
                let desired_digest = digest_json(&json!({"agent":agent_id}))?;
                Ok(Self::SessionAgent(AgentChange {
                    agent_id: agent_id.to_owned(),
                    desired_digest,
                }))
            }
            (None, None, Some(value)) => {
                let requested: ModelRef = serde_json::from_value(value.clone()).map_err(|_| {
                    Error::invalid("model must contain exact id, providerID and variant")
                })?;
                if !requested.valid() {
                    return Err(Error::invalid(
                        "model id, providerID and variant must be bounded nonempty strings",
                    ));
                }
                let desired_digest = digest_json(&json!({"model":model_ref_value(&requested)}))?;
                Ok(Self::SessionModel(ModelChange {
                    model: requested,
                    desired_digest,
                }))
            }
            _ => Err(Error::invalid(
                "settings must select exactly one of instruction_entry, agent or model",
            )),
        }
    }

    fn expectation(&self) -> ConfigurationExpectation {
        match self {
            Self::InstructionEntry(change) => change.expectation(),
            Self::SessionAgent(change) => change.expectation(),
            Self::SessionModel(change) => change.expectation(),
        }
    }

    fn ensure_route(&self, options: &Options) -> Result<()> {
        self.expectation().ensure_route(options)
    }
}

pub(crate) fn configuration_expectation(settings: &Value) -> Result<ConfigurationExpectation> {
    Ok(Change::parse(settings)?.expectation())
}

pub(crate) fn configuration_contract(
    settings: &Value,
    binding_id: &str,
    generation: i64,
    native_options: &Value,
) -> Result<Value> {
    let expectation = configuration_expectation(settings)?;
    expectation.ensure_route(&Options::parse(native_options)?)?;
    Ok(json!({
        "effect_scope":"native_session",
        "order_scope":{"binding_id":binding_id,"generation":generation},
        "completion_condition":"native_configuration_applied",
        "application_boundary":expectation.application_boundary(),
        "replay_policy":"readback_only_no_mutation_replay",
        "fallback_used":false,
        "configuration_kind":expectation.kind(),
        "contract_revision":expectation.contract_revision()
    }))
}

fn validate_entries(entries: Vec<Value>) -> Result<Vec<Value>> {
    let mut keys = BTreeSet::new();
    for entry in &entries {
        model::fields(entry, &["key", "value"])?;
        let key = model::text(entry, "key")?;
        if !native_key(key) || entry.get("value").is_none() || !keys.insert(key.to_owned()) {
            return Err(Error::new(
                "NATIVE_CONFIGURATION_SCHEMA",
                "native instruction entry list is invalid or ambiguous",
            ));
        }
    }
    Ok(entries)
}

pub(super) fn owned_projection(entries: &[Value]) -> Result<Vec<Value>> {
    let mut owned = Vec::new();
    for entry in entries.iter().filter(|entry| {
        entry["key"]
            .as_str()
            .is_some_and(|key| key.starts_with(OWNED_KEY_PREFIX))
    }) {
        let key = model::text(entry, "key")?;
        let value = model::canonical(&entry["value"])?;
        owned.push(json!({
            "key":key,
            "value_digest":format!("sha256:{}",model::digest(value.as_bytes()))
        }));
    }
    owned.sort_by(|left, right| {
        left["key"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["key"].as_str().unwrap_or_default())
    });
    Ok(owned)
}

pub(super) fn projection_revision(projection: &[Value]) -> Result<String> {
    digest_json(&Value::Array(projection.to_vec()))
}

fn validate_agent_definition(agent: &Value) -> Result<(String, AgentDefinition)> {
    model::fields(
        agent,
        &[
            "id",
            "name",
            "model",
            "request",
            "system",
            "description",
            "mode",
            "hidden",
            "color",
            "steps",
            "permissions",
        ],
    )
    .map_err(|_| {
        Error::new(
            "NATIVE_AGENT_SCHEMA",
            "native agent definition contains unsupported fields",
        )
    })?;
    let id = model::text(agent, "id")
        .map_err(|_| Error::new("NATIVE_AGENT_SCHEMA", "native agent id is missing"))?;
    let name = model::text(agent, "name")
        .map_err(|_| Error::new("NATIVE_AGENT_SCHEMA", "native agent name is missing"))?;
    if !bounded_agent_id(id)
        || name.len() > MAX_AGENT_NAME_BYTES
        || name.bytes().any(|byte| byte.is_ascii_control())
        || !matches!(agent["mode"].as_str(), Some("subagent" | "primary" | "all"))
        || agent["hidden"].as_bool().is_none()
        || !agent["request"].is_object()
        || !agent["permissions"].is_array()
    {
        return Err(Error::new(
            "NATIVE_AGENT_SCHEMA",
            "native agent definition is invalid",
        ));
    }
    let model = match agent.get("model") {
        None => None,
        Some(value) if value.is_object() => {
            model::fields(value, &["id", "providerID", "variant"])
                .map_err(|_| Error::new("NATIVE_AGENT_SCHEMA", "native agent model is invalid"))?;
            for field in ["id", "providerID"] {
                let value = model::text(value, field).map_err(|_| {
                    Error::new("NATIVE_AGENT_SCHEMA", "native agent model is invalid")
                })?;
                if value.len() > 256 {
                    return Err(Error::new(
                        "NATIVE_AGENT_SCHEMA",
                        "native agent model is invalid",
                    ));
                }
            }
            if value.get("variant").is_some() {
                let variant = model::text(value, "variant").map_err(|_| {
                    Error::new("NATIVE_AGENT_SCHEMA", "native agent model is invalid")
                })?;
                if variant.len() > 256 {
                    return Err(Error::new(
                        "NATIVE_AGENT_SCHEMA",
                        "native agent model is invalid",
                    ));
                }
            }
            Some(value.clone())
        }
        Some(_) => {
            return Err(Error::new(
                "NATIVE_AGENT_SCHEMA",
                "native agent model is invalid",
            ));
        }
    };
    let definition_digest = digest_json(agent)?;
    Ok((
        id.to_owned(),
        AgentDefinition {
            definition_digest,
            mode: agent["mode"].as_str().unwrap_or_default().to_owned(),
            hidden: agent["hidden"].as_bool().unwrap_or(false),
            model,
        },
    ))
}

fn bounded_model_text(value: &Value, field: &str, max: usize) -> Result<String> {
    let text = model::text(value, field)
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model definition is invalid"))?;
    if text.is_empty() || text.len() > max || text.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model definition is invalid",
        ));
    }
    Ok(text.to_owned())
}

fn validate_string_map(value: &Value) -> bool {
    value.as_object().is_some_and(|map| {
        map.iter().all(|(key, value)| {
            !key.is_empty()
                && key.len() <= 256
                && !key.bytes().any(|byte| byte.is_ascii_control())
                && value.as_str().is_some_and(|value| value.len() <= 8192)
        })
    })
}

fn validate_model_compatibility(value: &Value) -> Result<()> {
    model::fields(
        value,
        &[
            "reasoningField",
            "requireReasoning",
            "maxTokensField",
            "requireFinishReason",
            "requireAssistantAfterTool",
            "supportsPromptCacheKey",
        ],
    )
    .map_err(|_| {
        Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model compatibility is invalid",
        )
    })?;
    if value.get("reasoningField").is_some_and(|field| {
        field.as_str().is_none_or(|field| {
            field.is_empty()
                || field.len() > 128
                || field.bytes().any(|byte| byte.is_ascii_control())
        })
    }) || value.get("maxTokensField").is_some_and(|field| {
        !matches!(field.as_str(), Some("max_completion_tokens" | "max_tokens"))
    }) || [
        "requireReasoning",
        "requireFinishReason",
        "requireAssistantAfterTool",
        "supportsPromptCacheKey",
    ]
    .iter()
    .any(|field| value.get(*field).is_some_and(|flag| !flag.is_boolean()))
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model compatibility is invalid",
        ));
    }
    Ok(())
}

fn finite(value: &Value) -> bool {
    value.as_f64().is_some_and(f64::is_finite)
}

fn validate_model_costs(value: &Value) -> Result<()> {
    let costs = value
        .as_array()
        .filter(|costs| costs.len() <= MAX_MODEL_COST_TIERS)
        .ok_or_else(|| Error::new("NATIVE_MODEL_SCHEMA", "native model costs are invalid"))?;
    for cost in costs {
        model::fields(cost, &["tier", "input", "output", "cache"])
            .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model costs are invalid"))?;
        model::fields(&cost["cache"], &["read", "write"])
            .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model costs are invalid"))?;
        if !finite(&cost["input"])
            || !finite(&cost["output"])
            || !finite(&cost["cache"]["read"])
            || !finite(&cost["cache"]["write"])
        {
            return Err(Error::new(
                "NATIVE_MODEL_SCHEMA",
                "native model costs are invalid",
            ));
        }
        if let Some(tier) = cost.get("tier") {
            model::fields(tier, &["type", "size"]).map_err(|_| {
                Error::new("NATIVE_MODEL_SCHEMA", "native model cost tier is invalid")
            })?;
            if tier["type"] != "context" || tier["size"].as_i64().is_none() {
                return Err(Error::new(
                    "NATIVE_MODEL_SCHEMA",
                    "native model cost tier is invalid",
                ));
            }
        }
    }
    Ok(())
}

fn validate_model_variant(variant: &Value) -> Result<(String, String)> {
    model::fields(variant, &["id", "settings", "headers", "body"])
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model variant is invalid"))?;
    let id = bounded_model_text(variant, "id", 256)?;
    if variant
        .get("settings")
        .is_some_and(|value| !value.is_object())
        || variant.get("body").is_some_and(|value| !value.is_object())
        || variant
            .get("headers")
            .is_some_and(|value| !validate_string_map(value))
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model variant is invalid",
        ));
    }
    Ok((id, digest_json(variant)?))
}

fn validate_model_definition(value: &Value) -> Result<(String, ModelDefinition)> {
    model::fields(
        value,
        &[
            "id",
            "modelID",
            "providerID",
            "canonical",
            "family",
            "name",
            "compatibility",
            "package",
            "settings",
            "headers",
            "body",
            "capabilities",
            "variants",
            "time",
            "cost",
            "status",
            "enabled",
            "limit",
        ],
    )
    .map_err(|_| {
        Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model definition contains unsupported fields",
        )
    })?;
    let id = bounded_model_text(value, "id", 256)?;
    bounded_model_text(value, "modelID", 256)?;
    let provider = bounded_model_text(value, "providerID", 256)?;
    bounded_model_text(value, "name", MAX_MODEL_NAME_BYTES)?;
    for field in ["canonical", "family"] {
        if value.get(field).is_some() {
            bounded_model_text(value, field, 256)?;
        }
    }
    if value.get("package").is_some() {
        bounded_model_text(value, "package", 512)?;
    }
    if value
        .get("compatibility")
        .is_some_and(|value| !value.is_object())
        || value
            .get("settings")
            .is_some_and(|value| !value.is_object())
        || value.get("body").is_some_and(|value| !value.is_object())
        || value
            .get("headers")
            .is_some_and(|value| !validate_string_map(value))
        || !value["capabilities"].is_object()
        || !value["time"].is_object()
        || !value["cost"].is_array()
        || !value["limit"].is_object()
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model definition is invalid",
        ));
    }
    if let Some(compatibility) = value.get("compatibility") {
        validate_model_compatibility(compatibility)?;
    }
    validate_model_costs(&value["cost"])?;
    model::fields(&value["capabilities"], &["tools", "input", "output"]).map_err(|_| {
        Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model capabilities are invalid",
        )
    })?;
    if value["capabilities"]["tools"].as_bool().is_none()
        || ["input", "output"].iter().any(|field| {
            value["capabilities"][*field]
                .as_array()
                .is_none_or(|items| {
                    items.len() > 64
                        || items.iter().any(|item| {
                            item.as_str().is_none_or(|text| {
                                text.is_empty()
                                    || text.len() > 128
                                    || text.bytes().any(|byte| byte.is_ascii_control())
                            })
                        })
                })
        })
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model capabilities are invalid",
        ));
    }
    model::fields(&value["time"], &["released"])
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model time is invalid"))?;
    if value["time"]["released"]
        .as_f64()
        .is_none_or(|released| !released.is_finite())
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model time is invalid",
        ));
    }
    model::fields(&value["limit"], &["context", "input", "output"])
        .map_err(|_| Error::new("NATIVE_MODEL_SCHEMA", "native model limits are invalid"))?;
    if value["limit"]["context"].as_i64().is_none()
        || value["limit"]["output"].as_i64().is_none()
        || value["limit"]
            .get("input")
            .is_some_and(|input| input.as_i64().is_none())
    {
        return Err(Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model limits are invalid",
        ));
    }
    let enabled = value["enabled"].as_bool().ok_or_else(|| {
        Error::new(
            "NATIVE_MODEL_SCHEMA",
            "native model enabled flag is invalid",
        )
    })?;
    let status = value["status"]
        .as_str()
        .filter(|status| matches!(*status, "alpha" | "beta" | "deprecated" | "active"))
        .ok_or_else(|| Error::new("NATIVE_MODEL_SCHEMA", "native model status is invalid"))?
        .to_owned();
    let variants = value["variants"]
        .as_array()
        .filter(|variants| variants.len() <= MAX_MODEL_VARIANTS)
        .ok_or_else(|| Error::new("NATIVE_MODEL_SCHEMA", "native model variants are invalid"))?;
    let mut projected_variants = BTreeMap::new();
    for variant in variants {
        let (variant_id, digest) = validate_model_variant(variant)?;
        if projected_variants.insert(variant_id, digest).is_some() {
            return Err(Error::new(
                "NATIVE_MODEL_SCHEMA",
                "native model definition contains a duplicate variant",
            ));
        }
    }
    Ok((
        model_catalog_key(&provider, &id),
        ModelDefinition {
            definition_digest: digest_json(value)?,
            enabled,
            status,
            variants: projected_variants,
        },
    ))
}

impl Service {
    pub(super) async fn instruction_entries(&self, root: &str) -> Result<Vec<Value>> {
        let response: Data<Vec<Value>> = decode(
            self.get(
                &format!("/api/experimental/session/{root}/instructions/entries"),
                &[],
            )
            .await?,
        )?;
        validate_entries(response.data)
    }

    async fn instruction_readback(
        &self,
        root: &str,
        options: &Options,
        command: &RuntimeCommand,
        change: &InstructionChange,
    ) -> Result<InstructionReadback> {
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let entries = self.instruction_entries(root).await?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let canonical = model::canonical(&Value::Array(entries.clone()))?;
        let owned = owned_projection(&entries)?;
        Ok(InstructionReadback {
            matches: change.matches(&entries),
            entries_revision: format!("sha256:{}", model::digest(canonical.as_bytes())),
            settings_revision: projection_revision(&owned)?,
        })
    }

    async fn agent_catalog(&self, directory: &Path) -> Result<AgentCatalog> {
        let catalog = self
            .get(
                "/api/agent",
                &[(
                    "location[directory]",
                    directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(directory, &catalog["location"]).await?;
        let agents = catalog["data"]
            .as_array()
            .ok_or_else(|| Error::new("NATIVE_AGENT_SCHEMA", "missing native agent catalog"))?;
        let mut definitions = BTreeMap::new();
        for agent in agents {
            let (id, definition) = validate_agent_definition(agent)?;
            if definitions.insert(id, definition).is_some() {
                return Err(Error::new(
                    "NATIVE_AGENT_SCHEMA",
                    "native agent catalog contains a duplicate id",
                ));
            }
        }
        let projection = definitions
            .iter()
            .map(|(id, definition)| {
                json!({"id":id,"definition_digest":definition.definition_digest})
            })
            .collect::<Vec<_>>();
        Ok(AgentCatalog {
            revision: projection_revision(&projection)?,
            definitions,
        })
    }

    async fn model_catalog(&self, directory: &Path) -> Result<ModelCatalog> {
        let catalog = self
            .get(
                "/api/model",
                &[(
                    "location[directory]",
                    directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(directory, &catalog["location"]).await?;
        let models = catalog["data"]
            .as_array()
            .ok_or_else(|| Error::new("NATIVE_MODEL_SCHEMA", "missing native model catalog"))?;
        let mut definitions = BTreeMap::new();
        for model in models {
            let (key, definition) = validate_model_definition(model)?;
            if definitions.insert(key, definition).is_some() {
                return Err(Error::new(
                    "NATIVE_MODEL_SCHEMA",
                    "native model catalog contains a duplicate provider/model id",
                ));
            }
        }
        let projection = definitions
            .iter()
            .map(|(key, definition)| {
                json!({"key":key,"definition_digest":definition.definition_digest})
            })
            .collect::<Vec<_>>();
        Ok(ModelCatalog {
            revision: projection_revision(&projection)?,
            definitions,
        })
    }

    fn checked_model<'a>(
        catalog: &'a ModelCatalog,
        requested: &ModelRef,
    ) -> Result<(&'a ModelDefinition, &'a str)> {
        let key = model_catalog_key(&requested.provider_id, &requested.id);
        let definition = catalog.definitions.get(&key).ok_or_else(|| {
            Error::new(
                "NATIVE_MODEL_UNAVAILABLE",
                "the exact route provider/model is absent from the current catalog",
            )
        })?;
        if !definition.enabled {
            return Err(Error::new(
                "NATIVE_MODEL_UNAVAILABLE",
                "the exact route provider/model is disabled",
            ));
        }
        let variant_digest = definition
            .variants
            .get(&requested.variant)
            .map(String::as_str)
            .ok_or_else(|| {
                Error::new(
                    "NATIVE_MODEL_UNAVAILABLE",
                    "the exact route model variant is absent from the current catalog",
                )
            })?;
        Ok((definition, variant_digest))
    }

    pub(super) async fn check_route_model_available(&self, options: &Options) -> Result<()> {
        let catalog = self.model_catalog(&options.directory).await?;
        Self::checked_model(&catalog, &options.model)?;
        Ok(())
    }

    fn checked_agent<'a>(
        catalog: &'a AgentCatalog,
        agent_id: &str,
        options: &Options,
    ) -> Result<&'a AgentDefinition> {
        let definition = catalog.definitions.get(agent_id).ok_or_else(|| {
            Error::new(
                "NATIVE_AGENT_UNAVAILABLE",
                "the exact requested agent is not registered in this location",
            )
        })?;
        if definition
            .model
            .as_ref()
            .is_some_and(|model| model != &json!(options.model))
        {
            return Err(Error::new(
                "NATIVE_AGENT_MODEL_MISMATCH",
                "the requested agent overrides the route's exact provider/model/variant",
            ));
        }
        Ok(definition)
    }

    pub(super) async fn verify_session_agent_route(
        &self,
        session: &Value,
        options: &Options,
    ) -> Result<()> {
        let Some(agent_id) = session.get("agent").and_then(Value::as_str) else {
            return Ok(());
        };
        if !bounded_agent_id(agent_id) {
            return Err(Error::new(
                "NATIVE_AGENT_SCHEMA",
                "native session agent is invalid",
            ));
        }
        let catalog = self.agent_catalog(&options.directory).await?;
        Self::checked_agent(&catalog, agent_id, options)?;
        Ok(())
    }

    async fn agent_state_once(
        &self,
        root: &str,
        options: &Options,
        change: &AgentChange,
    ) -> Result<(Option<String>, AgentCatalog, AgentDefinition)> {
        let catalog = self.agent_catalog(&options.directory).await?;
        let definition = Self::checked_agent(&catalog, &change.agent_id, options)?.clone();
        let session = self.session(root).await?;
        let observed = session
            .get("agent")
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| bounded_agent_id(value))
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::new("NATIVE_AGENT_SCHEMA", "native session agent is invalid")
                    })
            })
            .transpose()?;
        Ok((observed, catalog, definition))
    }

    async fn agent_readback(
        &self,
        root: &str,
        options: &Options,
        command: &RuntimeCommand,
        change: &AgentChange,
    ) -> Result<AgentReadback> {
        self.verify_binding_model(root, options, &command.binding_id, command.generation)
            .await?;
        let first = self.agent_state_once(root, options, change).await?;
        let second = self.agent_state_once(root, options, change).await?;
        self.verify_binding_model(root, options, &command.binding_id, command.generation)
            .await?;
        if first != second {
            return Err(Error::new(
                "NATIVE_CONFIGURATION_CHANGED",
                "native session agent or registered definition changed during readback",
            ));
        }
        let (observed, catalog, definition) = first;
        Ok(AgentReadback {
            matches: observed.as_deref() == Some(change.agent_id.as_str()),
            settings_revision: agent_settings_revision(
                Some(&change.agent_id),
                Some(&definition.definition_digest),
            )?,
            catalog_revision: catalog.revision,
            definition,
        })
    }

    async fn model_state_once(
        &self,
        root: &str,
        options: &Options,
        change: &ModelChange,
    ) -> Result<ModelState> {
        let catalog = self.model_catalog(&options.directory).await?;
        let (definition, variant_digest) = Self::checked_model(&catalog, &change.model)?;
        let definition = definition.clone();
        let variant_digest = variant_digest.to_owned();
        let session = self.session(root).await?;
        let selected = session.get("model").map(parse_session_model).transpose()?;
        Ok(ModelState {
            selected,
            catalog,
            definition,
            variant_digest,
        })
    }

    async fn model_readback(
        &self,
        root: &str,
        options: &Options,
        command: &RuntimeCommand,
        change: &ModelChange,
    ) -> Result<ModelReadback> {
        self.verify_binding_identity(root, options, &command.binding_id, command.generation)
            .await?;
        let first = self.model_state_once(root, options, change).await?;
        let second = self.model_state_once(root, options, change).await?;
        self.verify_binding_identity(root, options, &command.binding_id, command.generation)
            .await?;
        if first != second {
            return Err(Error::new(
                "NATIVE_CONFIGURATION_CHANGED",
                "native session model or model catalog changed during readback",
            ));
        }
        let requested = model_ref_value(&change.model);
        Ok(ModelReadback {
            matches: first.selected.as_ref() == Some(&requested),
            settings_revision: model_settings_revision(
                Some(&requested),
                Some(&first.definition.definition_digest),
                Some(&first.variant_digest),
            )?,
            catalog_revision: first.catalog.revision,
            definition: first.definition,
            variant_digest: first.variant_digest,
        })
    }

    pub(super) async fn configure(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let root = match command.native_root_id.as_deref() {
            Some(root) => root.to_owned(),
            None => {
                return failed(
                    command,
                    options,
                    &Error::invalid("native root is missing"),
                    false,
                );
            }
        };
        let change = match Change::parse(&command.input["settings"]) {
            Ok(change) => change,
            Err(error) => return failed(command, options, &error, false),
        };
        if let Err(error) = change.ensure_route(options) {
            return failed(command, options, &error, false);
        }
        match change {
            Change::InstructionEntry(change) => {
                let readback = match self
                    .instruction_readback(&root, options, command, &change)
                    .await
                {
                    Ok(readback) => readback,
                    Err(error) => return failed(command, options, &error, false),
                };
                if readback.matches {
                    return outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "preexisting_exact_readback", false),
                    );
                }
                let path = format!(
                    "/api/experimental/session/{root}/instructions/entries/{}",
                    change.key
                );
                let written = match &change.action {
                    ChangeAction::Put(value) => self.put(&path, json!({"value":value})).await,
                    ChangeAction::Remove => self.delete(&path).await,
                };
                match written {
                    Ok(Value::Null) => {}
                    Ok(_) => {
                        return failed(
                            command,
                            options,
                            &Error::new(
                                "NATIVE_CONFIGURATION_SCHEMA",
                                "native configuration mutation returned an unexpected body",
                            ),
                            true,
                        );
                    }
                    Err(error) => return failed(command, options, &error, true),
                }
                match self
                    .instruction_readback(&root, options, command, &change)
                    .await
                {
                    Ok(readback) if readback.matches => outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "post_mutation_exact_readback", true),
                    ),
                    Ok(_) => failed(
                        command,
                        options,
                        &Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "native configuration did not match after mutation acknowledgement",
                        ),
                        true,
                    ),
                    Err(error) => failed(command, options, &error, true),
                }
            }
            Change::SessionAgent(change) => {
                let readback = match self.agent_readback(&root, options, command, &change).await {
                    Ok(readback) => readback,
                    Err(error) => return failed(command, options, &error, false),
                };
                if readback.matches {
                    return outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "preexisting_exact_readback", false),
                    );
                }
                let written = self
                    .post(
                        &format!("/api/session/{root}/agent"),
                        json!({"agent":change.agent_id}),
                    )
                    .await;
                match written {
                    Ok(Value::Null) => {}
                    Ok(_) => {
                        return failed(
                            command,
                            options,
                            &Error::new(
                                "NATIVE_CONFIGURATION_SCHEMA",
                                "native agent switch returned an unexpected body",
                            ),
                            true,
                        );
                    }
                    Err(error) => return failed(command, options, &error, true),
                }
                match self.agent_readback(&root, options, command, &change).await {
                    Ok(readback) if readback.matches => outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "post_mutation_exact_readback", true),
                    ),
                    Ok(_) => failed(
                        command,
                        options,
                        &Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "native session agent did not match after mutation acknowledgement",
                        ),
                        true,
                    ),
                    Err(error) => failed(command, options, &error, true),
                }
            }
            Change::SessionModel(change) => {
                let readback = match self.model_readback(&root, options, command, &change).await {
                    Ok(readback) => readback,
                    Err(error) => return failed(command, options, &error, false),
                };
                if readback.matches {
                    return outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "preexisting_exact_readback", false),
                    );
                }
                let written = self
                    .post(
                        &format!("/api/session/{root}/model"),
                        json!({"model":model_ref_value(&change.model)}),
                    )
                    .await;
                match written {
                    Ok(Value::Null) => {}
                    Ok(_) => {
                        return failed(
                            command,
                            options,
                            &Error::new(
                                "NATIVE_CONFIGURATION_SCHEMA",
                                "native model switch returned an unexpected body",
                            ),
                            true,
                        );
                    }
                    Err(error) => return failed(command, options, &error, true),
                }
                match self.model_readback(&root, options, command, &change).await {
                    Ok(readback) if readback.matches => outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "post_mutation_exact_readback", true),
                    ),
                    Ok(_) => failed(
                        command,
                        options,
                        &Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "native session model did not match after mutation acknowledgement",
                        ),
                        true,
                    ),
                    Err(error) => failed(command, options, &error, true),
                }
            }
        }
    }

    pub(super) async fn reconcile_configuration(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let readback = async {
            self.verify().await?;
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let change = Change::parse(&command.input["settings"])?;
            change.ensure_route(options)?;
            match change {
                Change::InstructionEntry(change) => {
                    let readback = self
                        .instruction_readback(root, options, command, &change)
                        .await?;
                    if !readback.matches {
                        return Err(Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "saved configuration is not the current native state",
                        ));
                    }
                    Ok(outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "exact_state_reconciliation", false),
                    ))
                }
                Change::SessionAgent(change) => {
                    let readback = self.agent_readback(root, options, command, &change).await?;
                    if !readback.matches {
                        return Err(Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "saved session agent is not the current native state",
                        ));
                    }
                    Ok(outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "exact_state_reconciliation", false),
                    ))
                }
                Change::SessionModel(change) => {
                    let readback = self.model_readback(root, options, command, &change).await?;
                    if !readback.matches {
                        return Err(Error::new(
                            "NATIVE_CONFIGURATION_UNRESOLVED",
                            "saved session model is not the current native state",
                        ));
                    }
                    Ok(outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        change.details(&readback, "exact_state_reconciliation", false),
                    ))
                }
            }
        }
        .await;
        match readback {
            Ok(outcome) => outcome,
            Err(error) => outcome(
                command,
                EffectOutcome::Unknown,
                options,
                super::diagnostic(&error),
            ),
        }
    }

    pub(super) fn instruction_observation_from_entries(entries: &[Value]) -> Result<Value> {
        let projection = owned_projection(entries)?;
        let complete = projection.len() <= MAX_OBSERVED_ENTRIES;
        let owned = projection
            .iter()
            .take(MAX_OBSERVED_ENTRIES)
            .cloned()
            .collect::<Vec<_>>();
        let revision = complete
            .then(|| projection_revision(&projection))
            .transpose()?;
        Ok(json!({
            "complete":complete,
            "owned_entries":owned,
            "revision":revision,
            "limit":MAX_OBSERVED_ENTRIES,
            "limit_reached":!complete,
            "value_content_persisted":false,
            "source":"experimental.session.instructions.entry.list"
        }))
    }

    pub(super) async fn agent_observation(&self, session: &Value) -> Result<Value> {
        let directory = Path::new(model::text(&session["location"], "directory")?);
        if !directory.is_absolute() {
            return Err(Error::new(
                "NATIVE_LOCATION_MISMATCH",
                "native session location is not absolute",
            ));
        }
        let catalog = self.agent_catalog(directory).await?;
        let agent_id = session
            .get("agent")
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| bounded_agent_id(value))
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::new("NATIVE_AGENT_SCHEMA", "native session agent is invalid")
                    })
            })
            .transpose()?;
        let definition = agent_id
            .as_ref()
            .map(|agent_id| {
                catalog.definitions.get(agent_id).cloned().ok_or_else(|| {
                    Error::new(
                        "NATIVE_AGENT_UNAVAILABLE",
                        "the session-selected agent is absent from the current catalog",
                    )
                })
            })
            .transpose()?;
        let definition_digest = definition
            .as_ref()
            .map(|definition| definition.definition_digest.as_str());
        Ok(json!({
            "complete":true,
            "agent_id":agent_id,
            "definition_digest":definition_digest,
            "settings_revision":agent_settings_revision(agent_id.as_deref(),definition_digest)?,
            "catalog_revision":catalog.revision,
            "mode":definition.as_ref().map(|definition|definition.mode.as_str()),
            "hidden":definition.as_ref().map(|definition|definition.hidden),
            "raw_definition_persisted":false,
            "source":"session.get+agent.list"
        }))
    }

    pub(super) async fn model_observation(&self, session: &Value) -> Result<Value> {
        let directory = Path::new(model::text(&session["location"], "directory")?);
        if !directory.is_absolute() {
            return Err(Error::new(
                "NATIVE_LOCATION_MISMATCH",
                "native session location is not absolute",
            ));
        }
        let catalog = self.model_catalog(directory).await?;
        let selected = session.get("model").map(parse_session_model).transpose()?;
        let (definition, variant_digest) = if let Some(selected) = &selected {
            let id = model::text(selected, "id")?;
            let provider = model::text(selected, "providerID")?;
            let definition = catalog
                .definitions
                .get(&model_catalog_key(provider, id))
                .cloned()
                .ok_or_else(|| {
                    Error::new(
                        "NATIVE_MODEL_UNAVAILABLE",
                        "the session-selected model is absent from the current catalog",
                    )
                })?;
            let variant_digest = match selected["variant"].as_str() {
                Some(variant) => {
                    Some(definition.variants.get(variant).cloned().ok_or_else(|| {
                        Error::new(
                            "NATIVE_MODEL_UNAVAILABLE",
                            "the session-selected variant is absent from the current catalog",
                        )
                    })?)
                }
                None => None,
            };
            (Some(definition), variant_digest)
        } else {
            (None, None)
        };
        let definition_digest = definition
            .as_ref()
            .map(|definition| definition.definition_digest.as_str());
        let settings_revision = model_settings_revision(
            selected.as_ref(),
            definition_digest,
            variant_digest.as_deref(),
        )?;
        Ok(json!({
            "complete":true,
            "model":selected,
            "definition_digest":definition_digest,
            "variant_digest":variant_digest,
            "settings_revision":settings_revision,
            "catalog_revision":catalog.revision,
            "enabled":definition.as_ref().map(|definition|definition.enabled),
            "status":definition.as_ref().map(|definition|definition.status.as_str()),
            "raw_definition_persisted":false,
            "source":"session.get+model.list"
        }))
    }
}
