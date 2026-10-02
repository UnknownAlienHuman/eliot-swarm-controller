//! Durable session configuration delegates to OpenCode's native state machines.
//! ELIOT keeps only scoped evidence and never creates a parallel writable store.
use super::{
    Options, Service,
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
const OWNED_KEY_PREFIX: &str = "eliot.";

pub(crate) const INSTRUCTION_CONTRACT_REVISION: &str = "opencode-instruction-entry-v1";
pub(crate) const AGENT_CONTRACT_REVISION: &str = "opencode-session-agent-v1";
pub(crate) const INSTRUCTION_SETTINGS_REVISION_KIND: &str = "eliot_owned_instruction_entries_v1";
pub(crate) const AGENT_SETTINGS_REVISION_KIND: &str = "opencode_session_agent_v1";

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
}

impl ConfigurationExpectation {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "instruction_entry",
            Self::SessionAgent { .. } => "session_agent",
        }
    }

    pub(crate) fn action(&self) -> &str {
        match self {
            Self::InstructionEntry { action, .. } => action,
            Self::SessionAgent { .. } => "switch",
        }
    }

    pub(crate) fn desired_digest(&self) -> Option<&str> {
        match self {
            Self::InstructionEntry { desired_digest, .. } => desired_digest.as_deref(),
            Self::SessionAgent { desired_digest, .. } => Some(desired_digest),
        }
    }

    pub(crate) fn contract_revision(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => INSTRUCTION_CONTRACT_REVISION,
            Self::SessionAgent { .. } => AGENT_CONTRACT_REVISION,
        }
    }

    pub(crate) fn settings_revision_kind(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => INSTRUCTION_SETTINGS_REVISION_KIND,
            Self::SessionAgent { .. } => AGENT_SETTINGS_REVISION_KIND,
        }
    }

    pub(crate) fn application_boundary(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "next_step_boundary",
            Self::SessionAgent { .. } => "subsequent_provider_turn",
        }
    }

    pub(crate) fn read_method(&self) -> &'static str {
        match self {
            Self::InstructionEntry { .. } => "experimental.session.instructions.entry.list",
            Self::SessionAgent { .. } => "session.get+agent.list",
        }
    }

    pub(crate) fn same_scope(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::InstructionEntry { key: left, .. },
                Self::InstructionEntry { key: right, .. },
            ) => left == right,
            (Self::SessionAgent { .. }, Self::SessionAgent { .. }) => true,
            _ => false,
        }
    }
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
enum Change {
    InstructionEntry(InstructionChange),
    SessionAgent(AgentChange),
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

fn digest_json(value: &Value) -> Result<String> {
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

impl Change {
    fn parse(settings: &Value) -> Result<Self> {
        model::fields(settings, &["instruction_entry", "agent"])?;
        match (settings.get("instruction_entry"), settings.get("agent")) {
            (Some(entry), None) => {
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
            (None, Some(agent)) => {
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
            _ => Err(Error::invalid(
                "settings must select exactly one of instruction_entry or agent",
            )),
        }
    }

    fn expectation(&self) -> ConfigurationExpectation {
        match self {
            Self::InstructionEntry(change) => change.expectation(),
            Self::SessionAgent(change) => change.expectation(),
        }
    }
}

pub(crate) fn configuration_expectation(settings: &Value) -> Result<ConfigurationExpectation> {
    Ok(Change::parse(settings)?.expectation())
}

pub(crate) fn configuration_contract(
    settings: &Value,
    binding_id: &str,
    generation: i64,
) -> Result<Value> {
    let expectation = configuration_expectation(settings)?;
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

fn owned_projection(entries: &[Value]) -> Result<Vec<Value>> {
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

fn projection_revision(projection: &[Value]) -> Result<String> {
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

impl Service {
    async fn instruction_entries(&self, root: &str) -> Result<Vec<Value>> {
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
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let first = self.agent_state_once(root, options, change).await?;
        let second = self.agent_state_once(root, options, change).await?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
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
            match Change::parse(&command.input["settings"])? {
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

    pub(super) async fn instruction_observation(&self, root: &str) -> Result<Value> {
        let entries = self.instruction_entries(root).await?;
        let projection = owned_projection(&entries)?;
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
}
