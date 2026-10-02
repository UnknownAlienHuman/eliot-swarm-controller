//! Durable session configuration delegates to OpenCode's instruction-entry
//! backend. ELIOT does not maintain a second writable configuration store.
use super::{
    Options, Service,
    effects::{failed, outcome},
    http::{Data, decode},
};
use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const MAX_VALUE_BYTES: usize = 256 * 1024;
const MAX_OWNED_KEY_BYTES: usize = 128;
const MAX_OBSERVED_ENTRIES: usize = 128;
const OWNED_KEY_PREFIX: &str = "eliot.";

#[derive(Clone)]
enum ChangeAction {
    Put(Value),
    Remove,
}

#[derive(Clone)]
struct Change {
    key: String,
    action: ChangeAction,
    desired_digest: Option<String>,
}

struct Readback {
    matches: bool,
    entries_revision: String,
    settings_revision: String,
}

fn native_key(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
        })
}

impl Change {
    fn parse(command: &RuntimeCommand) -> Result<Self> {
        let settings = &command.input["settings"];
        model::fields(settings, &["instruction_entry"])?;
        let entry = settings
            .get("instruction_entry")
            .ok_or_else(|| Error::invalid("settings.instruction_entry is required"))?;
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
                let value = entry
                    .get("value")
                    .cloned()
                    .ok_or_else(|| Error::invalid("put requires instruction entry value"))?;
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
            ChangeAction::Put(value) => {
                let value = model::canonical(value)?;
                Some(format!("sha256:{}", model::digest(value.as_bytes())))
            }
            ChangeAction::Remove => None,
        };
        Ok(Self {
            key: key.to_owned(),
            action,
            desired_digest,
        })
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

    fn details(&self, readback: &Readback, evidence: &str, mutation_sent: bool) -> Value {
        json!({
            "completion_condition":"native_configuration_applied",
            "configuration_kind":"instruction_entry",
            "action":self.action_name(),
            "key":self.key,
            "desired_digest":self.desired_digest,
            "entries_revision":readback.entries_revision,
            "settings_revision":readback.settings_revision,
            "settings_revision_kind":"eliot_owned_instruction_entries_v1",
            "application_scope":"session",
            "application_boundary":"next_step_boundary",
            "native_applied":true,
            "model_work_started":false,
            "mutation_sent":mutation_sent,
            "evidence":evidence,
            "read_method":"experimental.session.instructions.entry.list",
            "replay_policy":"readback_only_no_mutation_replay",
            "contract_revision":"opencode-instruction-entry-v1"
        })
    }
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
    let canonical = model::canonical(&Value::Array(projection.to_vec()))?;
    Ok(format!("sha256:{}", model::digest(canonical.as_bytes())))
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

    async fn configuration_readback(
        &self,
        root: &str,
        options: &Options,
        command: &RuntimeCommand,
        change: &Change,
    ) -> Result<Readback> {
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let entries = self.instruction_entries(root).await?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let canonical = model::canonical(&Value::Array(entries.clone()))?;
        let owned = owned_projection(&entries)?;
        Ok(Readback {
            matches: change.matches(&entries),
            entries_revision: format!("sha256:{}", model::digest(canonical.as_bytes())),
            settings_revision: projection_revision(&owned)?,
        })
    }

    pub(super) async fn configure(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let prepared = async {
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let change = Change::parse(command)?;
            let readback = self
                .configuration_readback(root, options, command, &change)
                .await?;
            Ok::<_, Error>((root.to_owned(), change, readback))
        }
        .await;
        let (root, change, readback) = match prepared {
            Ok(prepared) => prepared,
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
            .configuration_readback(&root, options, command, &change)
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
            let change = Change::parse(command)?;
            let readback = self
                .configuration_readback(root, options, command, &change)
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
}
