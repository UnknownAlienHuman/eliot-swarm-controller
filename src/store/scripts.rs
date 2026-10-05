//! Store-side registry and direct-run admission for trusted-local scripts.
//!
//! Script execution is deliberately a closed consumer: no script receives a
//! Store handle, Manager credential, or controller API capability. Filesystem
//! work runs outside SQLite; the database records the immutable admitted
//! bundle, Task/Attempt, work receipt digest and Operation lifecycle.
use super::{Store, automation_dispatch, current_principal, gm, meta, results, submissions, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    automation::{authorization, config as automation_config},
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
    scripts::{manifest, protocol, registry, runner},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::watch;

const RECONCILE_INTERVAL: Duration = Duration::from_millis(200);
const STARTING_WORKER_TIMEOUT_MS: i64 = 120_000;

type ScriptCompletionRow = (
    String,
    String,
    i64,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<String>,
);

#[derive(Debug, Clone)]
struct RevisionSnapshot {
    record: ArtifactRecord,
    interpreter: manifest::InterpreterIdentity,
    validated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct PendingRun {
    run_id: String,
    operation_id: String,
    operation_state: String,
    work_digest: String,
    sent_at_ms: Option<i64>,
    started_at_ms: Option<i64>,
    worker_identity_json: Option<String>,
}

const SCRIPT_WORKER_OBSERVATION_TIMEOUT_MS: i64 =
    manifest::MAX_SCRIPT_DURATION_MS as i64 + STARTING_WORKER_TIMEOUT_MS;

struct OperationRecord<'a> {
    method: &'a str,
    request_id: &'a str,
    original_request: &'a str,
    operation_id: &'a str,
    result: &'a Value,
    context: Value,
    settled: bool,
    now_ms: i64,
}

struct ScriptRunArtifactLinks {
    run_id: String,
    operation_id: String,
    state: String,
    result_ref: Option<String>,
    stdout_ref: Option<String>,
    stderr_ref: Option<String>,
    method: String,
}

struct RunnerObservation {
    completion: Option<runner::Completion>,
    process_control_failure: Option<swarm_scripts::process::ProcessControlFailure>,
    ready: Option<Value>,
    launch: Option<Value>,
    launch_departed: bool,
    has_go: bool,
}

struct ScriptEffectParentOperationRow {
    caller_id: String,
    method: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
}

struct ScriptEffectRunRow {
    script_id: String,
    revision: i64,
    bundle_ref: String,
    task_id: Option<String>,
    task_revision: Option<i64>,
    attempt_id: Option<String>,
    spec_json: String,
}

struct ScriptEffectOperationRow {
    method: String,
    caller_id: String,
    original_request_json: String,
    effective_request_json: String,
}

struct ScriptTriggerStart<'a> {
    operation_id: &'a str,
    script_id: &'a str,
    script_revision: i64,
    task_id: Option<&'a str>,
    task_revision: Option<i64>,
    attempt_id: Option<&'a str>,
    app_config: &'a Config,
}

#[derive(Debug, Clone)]
struct ScriptRunTriggerGrant {
    entry: automation_config::AutomationEntry,
    cause: Value,
    context: automation_dispatch::script_trigger_authority::ScriptRunConsumerContext,
}

/// Store-derived permission to commit the one controller effect currently
/// declared by the immutable script manifest. This is intentionally not a
/// Principal: the technical caller remains the automation service, while the
/// registered Manager, current entry, source and exact event or Task/Attempt
/// scope are revalidated at each Store boundary.
#[derive(Debug, Clone)]
pub(super) struct ScriptEffectAdmission {
    context: automation_dispatch::script_trigger_authority::ScriptRunConsumerContext,
    source_cause: Value,
    parent_operation_id: String,
    run_id: String,
    script_id: String,
    script_revision: i64,
    effect: manifest::ScriptControllerEffect,
    task_id: Option<String>,
    task_revision: Option<i64>,
    attempt_id: Option<String>,
    request_id: String,
    recipient: Option<String>,
    text: Option<String>,
    task_spec: Option<Value>,
}

struct ScriptEffectRunInput<'a> {
    db: &'a Connection,
    config: &'a Config,
    caller_id: &'a str,
    parent_operation_id: &'a str,
    run_id: &'a str,
    script_id: &'a str,
    script_revision: i64,
    task_id: Option<&'a str>,
    task_revision: Option<i64>,
    attempt_id: Option<&'a str>,
    request_id: &'a str,
    effect: &'a protocol::ScriptEffectRequest,
    expected_manager_id: &'a str,
}

struct ScriptEffectLinkInput<'a> {
    caller_id: &'a str,
    effective_manager_id: &'a str,
    request_id: &'a str,
    effect_operation_id: &'a str,
    request: &'a Value,
    cause: &'a Value,
    effect: manifest::ScriptControllerEffect,
    automatic_admission: Option<&'a ScriptEffectAdmission>,
    now_ms: i64,
}

impl ScriptEffectAdmission {
    fn from_run(input: ScriptEffectRunInput<'_>) -> Result<Self> {
        let ScriptEffectRunInput {
            db,
            config,
            caller_id,
            parent_operation_id,
            run_id,
            script_id,
            script_revision,
            task_id,
            task_revision,
            attempt_id,
            request_id,
            effect,
            expected_manager_id,
        } = input;
        if caller_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
            return Err(Error::new(
                "SCRIPT_EFFECT_AUTHORITY_CHANGED",
                "automatic effect did not retain the automation technical requester",
            ));
        }
        let link = authorization::operation_link(db, parent_operation_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "automatic ScriptRun effect has no retained owner attribution",
            )
        })?;
        let context =
            automation_dispatch::script_trigger_authority::ScriptRunConsumerContext::from_cause(
                &link.cause,
            )?;
        if link.action != "script.run"
            || link.technical_requester_id != caller_id
            || link.effective_manager_id != expected_manager_id
            || link.effective_manager_id != context.owner_manager_id()
            || link.project_id != context.project_id()
            || link.automation_id != context.automation_id()
            || link.automation_revision != context.automation_revision()
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "automatic ScriptRun attribution differs from its typed consumer context",
            ));
        }
        let entry = context.require_current_entry(db)?;
        let source = context.require_current_source(db, config, &entry, &link.cause)?;
        let (task, attempt) = context.require_live_subject(db, &source)?;
        effect.validate()?;
        let (recipient, text, task_spec) = match effect.effect {
            manifest::ScriptControllerEffect::TaskOwnerMessage => {
                if source.task_id.as_deref() != task_id
                    || source.task_revision != task_revision
                    || source.attempt_id.as_deref() != attempt_id
                    || task.as_ref().and_then(|value| value["task_id"].as_str()) != task_id
                    || task.as_ref().and_then(|value| value["revision"].as_i64()) != task_revision
                    || attempt
                        .as_ref()
                        .and_then(|value| value["attempt_id"].as_str())
                        != attempt_id
                    || task_id.is_none()
                    || task_revision.is_none()
                    || attempt_id.is_none()
                {
                    return Err(Error::new(
                        "SCRIPT_SCOPE_CHANGED",
                        "Task-owner effect no longer has the exact current Manager-owned Task/Attempt",
                    ));
                }
                let recipient = attempt
                    .as_ref()
                    .and_then(|value| value["owner_id"].as_str())
                    .ok_or_else(|| {
                        Error::new(
                            "SCRIPT_SCOPE_CHANGED",
                            "current effect Attempt has no registered Task owner",
                        )
                    })?
                    .to_owned();
                (Some(recipient), Some(effect.text.clone()), None)
            }
            manifest::ScriptControllerEffect::ManagerNotification => {
                if link.cause["kind"] != "system_event"
                    || task_id.is_some()
                    || task_revision.is_some()
                    || attempt_id.is_some()
                    || source.task_id.is_some()
                    || source.task_revision.is_some()
                    || source.attempt_id.is_some()
                    || task.is_some()
                    || attempt.is_some()
                {
                    return Err(Error::new(
                        "SCRIPT_SCOPE_CHANGED",
                        "Manager notification requires this exact taskless system-event invocation",
                    ));
                }
                (
                    Some(context.owner_manager_id().to_owned()),
                    Some(effect.text.clone()),
                    None,
                )
            }
            manifest::ScriptControllerEffect::TaskCreate => {
                if link.cause["kind"] != "system_event"
                    || task_id.is_some()
                    || task_revision.is_some()
                    || attempt_id.is_some()
                    || source.task_id.is_some()
                    || source.task_revision.is_some()
                    || source.attempt_id.is_some()
                    || task.is_some()
                    || attempt.is_some()
                {
                    return Err(Error::new(
                        "SCRIPT_SCOPE_CHANGED",
                        "task_create requires this exact taskless system-event invocation",
                    ));
                }
                let spec = effect
                    .spec
                    .clone()
                    .ok_or_else(|| Error::invalid("task_create requires one strict TaskSpec"))?;
                let parsed: model::TaskSpec = serde_json::from_value(spec.clone())?;
                parsed.validate()?;
                (None, None, Some(spec))
            }
        };
        let admission = Self {
            context,
            source_cause: link.cause,
            parent_operation_id: parent_operation_id.to_owned(),
            run_id: run_id.to_owned(),
            script_id: script_id.to_owned(),
            script_revision,
            effect: effect.effect,
            task_id: task_id.map(str::to_owned),
            task_revision,
            attempt_id: attempt_id.map(str::to_owned),
            request_id: request_id.to_owned(),
            recipient,
            text,
            task_spec,
        };
        let request = admission.request_value();
        admission.require_current(db, config, admission.method(), &request)?;
        Ok(admission)
    }

    pub(super) fn technical_requester_id(&self) -> &'static str {
        authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    }

    pub(super) fn effective_manager_id(&self) -> &str {
        self.context.owner_manager_id()
    }

    pub(super) fn automation_id(&self) -> &str {
        self.context.automation_id()
    }

    pub(super) fn automation_revision(&self) -> i64 {
        self.context.automation_revision()
    }

    pub(super) fn project_id(&self) -> &str {
        self.context.project_id()
    }

    fn method(&self) -> &'static str {
        match self.effect {
            manifest::ScriptControllerEffect::TaskCreate => "task.create",
            manifest::ScriptControllerEffect::TaskOwnerMessage
            | manifest::ScriptControllerEffect::ManagerNotification => "message.send",
        }
    }

    fn request_value(&self) -> Value {
        match self.effect {
            manifest::ScriptControllerEffect::TaskOwnerMessage
            | manifest::ScriptControllerEffect::ManagerNotification => json!({
                "client_request_id":self.request_id,
                "recipient":self.recipient,
                "text":self.text,
            }),
            manifest::ScriptControllerEffect::TaskCreate => json!({
                "client_request_id":self.request_id,
                "project_id":self.project_id(),
                "spec":self.task_spec,
            }),
        }
    }

    pub(super) fn require_current(
        &self,
        db: &Connection,
        config: &Config,
        method: &str,
        params: &Value,
    ) -> Result<()> {
        if method != self.method() || params != &self.request_value() {
            return Err(Error::new(
                "SCRIPT_EFFECT_AUTHORITY_CHANGED",
                "automatic script effect differs from its closed admitted request",
            ));
        }
        validate_script_effect_authority(
            db,
            config,
            &self.context,
            &self.source_cause,
            &self.parent_operation_id,
            &self.run_id,
            &self.script_id,
            self.script_revision,
            self.effect,
            self.task_id.as_deref(),
            self.task_revision,
            self.attempt_id.as_deref(),
        )
    }

    fn link_cause(&self, request: &Value) -> Result<Value> {
        let mut cause = json!({
            "kind":"script_controller_effect",
            "id":self.request_id,
            "script_run_operation_id":self.parent_operation_id,
            "script_run_id":self.run_id,
            "script_id":self.script_id,
            "script_revision":self.script_revision,
            "effect":self.effect,
            "request_sha256":model::digest(model::canonical(request)?.as_bytes()),
            "effective_manager_id":self.effective_manager_id(),
        });
        if let Some(recipient) = self.recipient.as_deref() {
            cause["recipient"] = json!(recipient);
        }
        if self.effect == manifest::ScriptControllerEffect::TaskOwnerMessage {
            cause["task_id"] = json!(self.task_id);
            cause["task_revision"] = json!(self.task_revision);
            cause["attempt_id"] = json!(self.attempt_id);
        }
        Ok(cause)
    }

    fn retain_operation_link(
        &self,
        db: &Connection,
        effect_operation_id: &str,
        request: &Value,
        now_ms: i64,
    ) -> Result<Value> {
        let cause = self.link_cause(request)?;
        let link = authorization::OnBehalfOperationLink {
            schema_version: 1,
            operation_id: effect_operation_id.to_owned(),
            technical_requester_id: self.technical_requester_id().to_owned(),
            effective_manager_id: self.effective_manager_id().to_owned(),
            automation_id: self.automation_id().to_owned(),
            automation_revision: self.automation_revision(),
            project_id: self.project_id().to_owned(),
            action: self.method().to_owned(),
            cause,
            linked_at_ms: now_ms,
        };
        let value = link.value()?;
        let operation_key = automation_config::operation_link_key(effect_operation_id)?;
        let entry_key = automation_config::entry_operation_key(
            self.effective_manager_id(),
            self.project_id(),
            self.automation_id(),
            effect_operation_id,
        )?;
        for (key, label) in [
            (operation_key, "script effect operation link"),
            (entry_key, "script effect entry index"),
        ] {
            match automation_config::read_record(db, &key, label)? {
                Some(existing) if existing != value => {
                    return Err(Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "script effect link cannot replace another retained attribution",
                    ));
                }
                Some(_) => {}
                None => automation_config::write_record(db, &key, &value)?,
            }
        }
        Ok(value)
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_script_effect_authority(
    db: &Connection,
    config: &Config,
    context: &automation_dispatch::script_trigger_authority::ScriptRunConsumerContext,
    source_cause: &Value,
    parent_operation_id: &str,
    run_id: &str,
    script_id: &str,
    script_revision: i64,
    effect: manifest::ScriptControllerEffect,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
) -> Result<()> {
    let link = authorization::operation_link(db, parent_operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "automatic ScriptRun effect has no retained owner attribution",
        )
    })?;
    if link.operation_id != parent_operation_id
        || link.action != "script.run"
        || link.technical_requester_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.effective_manager_id != context.owner_manager_id()
        || link.project_id != context.project_id()
        || link.automation_id != context.automation_id()
        || link.automation_revision != context.automation_revision()
        || &link.cause != source_cause
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun link differs from its typed effect authority",
        ));
    }
    context.require_matches_cause(&link.cause)?;
    let entry = context.require_current_entry(db)?;
    let source = context.require_current_source(db, config, &entry, &link.cause)?;
    let (task, attempt) = context.require_live_subject(db, &source)?;
    let scope_matches = match effect {
        manifest::ScriptControllerEffect::TaskOwnerMessage => {
            task_id.is_some()
                && task_revision.is_some()
                && attempt_id.is_some()
                && source.task_id.as_deref() == task_id
                && source.task_revision == task_revision
                && source.attempt_id.as_deref() == attempt_id
                && task.as_ref().and_then(|value| value["task_id"].as_str()) == task_id
                && task.as_ref().and_then(|value| value["revision"].as_i64()) == task_revision
                && attempt
                    .as_ref()
                    .and_then(|value| value["attempt_id"].as_str())
                    == attempt_id
        }
        manifest::ScriptControllerEffect::ManagerNotification
        | manifest::ScriptControllerEffect::TaskCreate => {
            link.cause["kind"] == "system_event"
                && task_id.is_none()
                && task_revision.is_none()
                && attempt_id.is_none()
                && source.task_id.is_none()
                && source.task_revision.is_none()
                && source.attempt_id.is_none()
                && task.is_none()
                && attempt.is_none()
        }
    };
    if entry.owner_manager_id != context.owner_manager_id()
        || entry.project_id != context.project_id()
        || !scope_matches
    {
        return Err(Error::new(
            "SCRIPT_SCOPE_CHANGED",
            "automatic effect no longer has its exact current Manager-owned scope",
        ));
    }
    let parent: Option<ScriptEffectParentOperationRow> = db
        .query_row(
            "SELECT o.caller_id,o.method,o.task_id,o.attempt_id,o.original_request_json \
             FROM operations AS o WHERE o.operation_id=?1",
            [parent_operation_id],
            |row| {
                Ok(ScriptEffectParentOperationRow {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    original_request_json: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(ScriptEffectParentOperationRow {
        caller_id: caller,
        method,
        task_id: operation_task,
        attempt_id: operation_attempt,
        original_request_json: original,
    }) = parent
    else {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "automatic effect parent Operation is missing",
        ));
    };
    let original: Value = serde_json::from_str(&original).map_err(|_| {
        Error::new(
            "SCRIPT_RUN_DAMAGED",
            "automatic effect parent request is not readable",
        )
    })?;
    if caller != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "script.run"
        || operation_task.as_deref() != task_id
        || operation_attempt.as_deref() != attempt_id
        || original["script_id"] != script_id
        || original["expected_script_revision"] != script_revision
        || original["attempt_id"].as_str() != attempt_id
        || original["expected_task_revision"].as_i64() != task_revision
    {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "automatic effect parent Operation differs from its exact script and Task scope",
        ));
    }
    let run: Option<ScriptEffectRunRow> = db
        .query_row(
            "SELECT script_id,revision,bundle_ref,task_id,task_revision,attempt_id,spec_json \
             FROM script_runs WHERE operation_id=?1 AND run_id=?2",
            params![parent_operation_id, run_id],
            |row| {
                Ok(ScriptEffectRunRow {
                    script_id: row.get(0)?,
                    revision: row.get(1)?,
                    bundle_ref: row.get(2)?,
                    task_id: row.get(3)?,
                    task_revision: row.get(4)?,
                    attempt_id: row.get(5)?,
                    spec_json: row.get(6)?,
                })
            },
        )
        .optional()?;
    let Some(ScriptEffectRunRow {
        script_id: run_script,
        revision: run_revision,
        bundle_ref,
        task_id: run_task,
        task_revision: run_task_revision,
        attempt_id: run_attempt,
        spec_json,
    }) = run
    else {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "automatic effect has no exact retained ScriptRun row",
        ));
    };
    if run_script != script_id
        || run_revision != script_revision
        || run_task.as_deref() != task_id
        || run_task_revision != task_revision
        || run_attempt.as_deref() != attempt_id
    {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "retained ScriptRun row differs from its exact effect scope",
        ));
    }
    let spec: Value = serde_json::from_str(&spec_json).map_err(|_| {
        Error::new(
            "SCRIPT_RUN_DAMAGED",
            "retained ScriptRun grant is unreadable",
        )
    })?;
    let capabilities: Vec<manifest::ScriptControllerEffect> = serde_json::from_value(
        spec.get("capabilities")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| Error::new("SCRIPT_RUN_DAMAGED", "retained ScriptRun grant is invalid"))?;
    let bundle_record = registry::bundle_record(db, script_id, script_revision)?;
    if bundle_record.artifact_id != bundle_ref
        || artifact_controller_effects(&bundle_record.metadata)? != capabilities
        || !capabilities.contains(&effect)
    {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "automatic effect is outside the exact immutable script manifest grant",
        ));
    }
    let owner: String = db.query_row(
        "SELECT owner_id FROM scripts WHERE script_id=?1",
        [script_id],
        |row| row.get(0),
    )?;
    let (active_revision, _) = script_head(db, script_id)?;
    if owner != context.owner_manager_id() || active_revision != Some(script_revision) {
        return Err(Error::new(
            "SCRIPT_REVISION_NOT_ACTIVE",
            "the current Manager must still own and actively grant this script revision",
        ));
    }
    Ok(())
}

struct ScriptTriggerAdmissionFailure {
    error: Error,
    failed_script_revision: Option<i64>,
}

impl From<Error> for ScriptTriggerAdmissionFailure {
    fn from(error: Error) -> Self {
        Self {
            error,
            failed_script_revision: None,
        }
    }
}

type ScriptRunScopeRow = (String, i64, Option<String>, Option<i64>, Option<String>);

impl Store {
    pub(super) async fn script_call(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        match method.as_str() {
            "script.register" => self.register_script(principal, params).await,
            "script.revise" => self.revise_script(principal, params).await,
            "script.validate" => self.validate_script(principal, params).await,
            "script.activate" => self.activate_script(principal, params).await,
            "script.run" => self.run_script(principal, params).await,
            "script.get" => self.get_script(principal, params).await,
            "script.list" => self.list_scripts(principal, params).await,
            _ => Err(Error::new("METHOD_NOT_FOUND", method)),
        }
    }

    async fn register_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::RegisterRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.register", &params).await? {
            return Ok(value);
        }
        let caller = principal.client_id.clone();
        let bundle_request = request.bundle;
        let bundle = tokio::task::spawn_blocking(move || bundle_request.capture())
            .await
            .map_err(|error| Error::new("SCRIPT_PREPARATION", error.to_string()))??;
        let bundle_bytes = model::canonical(&json!(bundle))?.into_bytes();
        if bundle_bytes.len() > manifest::MAX_BUNDLE_REQUEST_BYTES + manifest::MAX_SCHEMA_BYTES {
            return Err(Error::invalid(
                "captured script bundle exceeds the 932 KiB stored manifest limit",
            ));
        }
        let bundle_sha256 = model::digest(&bundle_bytes);
        let revision = 1_i64;
        let artifact_id = bundle_artifact_id(
            &bundle.script_id,
            revision,
            &caller,
            &request.client_request_id,
            &bundle_sha256,
        )?;
        let metadata = json!({
            "script_id":bundle.script_id,
            "revision":revision,
            "bundle_sha256":bundle_sha256,
            "interpreter_kind":bundle.interpreter.kind,
            "interpreter_sha256":bundle.interpreter.sha256,
            "controller_effects":bundle.controller_effects,
        });
        let (record, bytes) = ArtifactFiles::document(
            registry::BUNDLE_KIND,
            &artifact_id,
            &json!(bundle),
            metadata,
        )?;
        let publish_record = record.clone();
        self.file_io(move |files| files.publish(&publish_record, &bytes))
            .await?;
        let request_json = model::canonical(&params)?;
        let owner = principal.clone();
        let caller = principal.client_id.clone();
        let request_id = request.client_request_id;
        let bundle_for_tx = bundle.clone();
        let record_for_tx = record.clone();
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &owner)?;
                if let Some(receipt) = replay_tx(
                    &tx,
                    &caller,
                    "script.register",
                    &request_id,
                    &request_json,
                )? {
                    tx.commit()?;
                    return Ok(receipt);
                }
                if script_exists(&tx, &bundle_for_tx.script_id)? {
                    return Err(Error::conflict("script ID is already registered"));
                }
                let now = model::now_ms()?;
                registry::register_artifact(&tx, &record_for_tx, now)?;
                tx.execute(
                    "INSERT INTO scripts(script_id,owner_id,active_revision,created_at_ms,updated_at_ms) VALUES(?1,?2,NULL,?3,?3)",
                    params![bundle_for_tx.script_id, current.client_id, now],
                )?;
                insert_revision(&tx, &bundle_for_tx, &record_for_tx, &current.client_id, 1, now)?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":bundle_for_tx.script_id,
                    "revision":1,
                    "bundle_ref":record_for_tx.artifact_id,
                    "state":"registered",
                    "validated":true,
                    "activation":"not_selected",
                    "controller_effects":bundle_for_tx.controller_effects,
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.register",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":bundle_for_tx.script_id,"revision":1,"bundle_ref":record_for_tx.artifact_id}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    async fn revise_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ReviseRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.revise", &params).await? {
            return Ok(value);
        }
        let request_json = model::canonical(&params)?;
        let caller = principal.client_id.clone();
        let principal_for_head = principal.clone();
        let id = request.script_id.clone();
        let expected_revision = request.expected_revision;
        let head = self
            .run(move |db| {
                let current = require_script_authority(db, &principal_for_head)?;
                let owner: Option<String> = db
                    .query_row(
                        "SELECT owner_id FROM scripts WHERE script_id=?1",
                        [&id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let owner =
                    owner.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
                if current.role == Role::Manager && owner != current.client_id {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "only the registered script owner may revise it",
                    ));
                }
                let latest: i64 = db.query_row(
                    "SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=?1",
                    [&id],
                    |row| row.get(0),
                )?;
                if latest != expected_revision {
                    return Err(Error::new(
                        "STALE_REVISION",
                        "script revision changed before revise",
                    ));
                }
                latest
                    .checked_add(1)
                    .ok_or_else(|| Error::invalid("script revision overflow"))
            })
            .await?;
        let bundle_request = request.bundle;
        let bundle = tokio::task::spawn_blocking(move || bundle_request.capture())
            .await
            .map_err(|error| Error::new("SCRIPT_PREPARATION", error.to_string()))??;
        let bundle_bytes = model::canonical(&json!(bundle))?.into_bytes();
        if bundle_bytes.len() > manifest::MAX_BUNDLE_REQUEST_BYTES + manifest::MAX_SCHEMA_BYTES {
            return Err(Error::invalid(
                "captured script bundle exceeds the 932 KiB stored manifest limit",
            ));
        }
        let bundle_sha256 = model::digest(&bundle_bytes);
        let artifact_id = bundle_artifact_id(
            &bundle.script_id,
            head,
            &caller,
            &request.client_request_id,
            &bundle_sha256,
        )?;
        let metadata = json!({
            "script_id":bundle.script_id,
            "revision":head,
            "bundle_sha256":bundle_sha256,
            "interpreter_kind":bundle.interpreter.kind,
            "interpreter_sha256":bundle.interpreter.sha256,
            "controller_effects":bundle.controller_effects,
        });
        let (record, bytes) = ArtifactFiles::document(
            registry::BUNDLE_KIND,
            &artifact_id,
            &json!(bundle),
            metadata,
        )?;
        let publish_record = record.clone();
        self.file_io(move |files| files.publish(&publish_record, &bytes))
            .await?;
        let principal_for_tx = principal.clone();
        let request_id = request.client_request_id;
        let bundle_for_tx = bundle.clone();
        let record_for_tx = record.clone();
        let script_id = request.script_id;
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &principal_for_tx)?;
                if let Some(receipt) = replay_tx(
                    &tx,
                    &caller,
                    "script.revise",
                    &request_id,
                    &request_json,
                )? {
                    tx.commit()?;
                    return Ok(receipt);
                }
                let owner: Option<String> = tx
                    .query_row("SELECT owner_id FROM scripts WHERE script_id=?1", [&script_id], |row| row.get(0))
                    .optional()?;
                let owner = owner.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
                if current.role == Role::Manager && owner != current.client_id {
                    return Err(Error::new("FORBIDDEN", "only the registered script owner may revise it"));
                }
                let latest: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=?1",
                    [&script_id],
                    |row| row.get(0),
                )?;
                if latest != expected_revision || head != expected_revision + 1 {
                    return Err(Error::new("STALE_REVISION", "script revision changed before revise"));
                }
                let now = model::now_ms()?;
                registry::register_artifact(&tx, &record_for_tx, now)?;
                insert_revision(&tx, &bundle_for_tx, &record_for_tx, &current.client_id, head, now)?;
                tx.execute("UPDATE scripts SET updated_at_ms=?2 WHERE script_id=?1", params![script_id, now])?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":script_id,
                    "revision":head,
                    "previous_revision":expected_revision,
                    "bundle_ref":record_for_tx.artifact_id,
                    "state":"registered",
                    "validated":true,
                    "activation":"not_selected",
                    "controller_effects":bundle_for_tx.controller_effects,
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.revise",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":script_id,"revision":head,"bundle_ref":record_for_tx.artifact_id}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    async fn validate_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ValidateRequest::parse(&params)?;
        let id = request.script_id.clone();
        let revision = request.revision;
        let snapshot = self
            .run(move |db| {
                require_script_scope(db, &principal, &id)?;
                revision_snapshot(db, &id, revision)
            })
            .await?;
        let record = snapshot.record.clone();
        let interpreter = snapshot.interpreter.clone();
        let checked = self
            .file_io(move |files| verify_revision_files(&files, &record, &interpreter))
            .await?;
        Ok(json!({
            "script_id":request.script_id,
            "revision":request.revision,
            "valid":true,
            "bundle_ref":snapshot.record.artifact_id,
            "bundle_sha256":snapshot.record.content_digest,
            "interpreter":checked.interpreter,
            "validated_at_ms":snapshot.validated_at_ms,
            "checked_at_ms":model::now_ms()?,
            "execution_started":false,
            "controller_effects":checked.controller_effects,
        }))
    }

    async fn activate_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ActivateRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.activate", &params).await? {
            return Ok(value);
        }
        let id = request.script_id.clone();
        let revision = request.revision;
        let principal_for_snapshot = principal.clone();
        let snapshot = self
            .run(move |db| {
                require_script_scope(db, &principal_for_snapshot, &id)?;
                revision_snapshot(db, &id, revision)
            })
            .await?;
        let record = snapshot.record.clone();
        let interpreter = snapshot.interpreter.clone();
        self.file_io(move |files| verify_revision_files(&files, &record, &interpreter))
            .await?;
        let request_json = model::canonical(&params)?;
        let caller = principal.client_id.clone();
        let p = principal.clone();
        let request_id = request.client_request_id;
        let script_id = request.script_id;
        let revision = request.revision;
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_scope(&tx, &p, &script_id)?;
                if let Some(receipt) =
                    replay_tx(&tx, &caller, "script.activate", &request_id, &request_json)?
                {
                    tx.commit()?;
                    return Ok(receipt);
                }
                let current_snapshot = revision_snapshot(&tx, &script_id, revision)?;
                if current_snapshot.record.artifact_id != snapshot.record.artifact_id
                    || current_snapshot.record.content_digest != snapshot.record.content_digest
                    || current_snapshot.interpreter != snapshot.interpreter
                    || current_snapshot.validated_at_ms <= 0
                {
                    return Err(Error::new(
                        "SCRIPT_REVISION_CHANGED",
                        "script revision changed before activation",
                    ));
                }
                let now = model::now_ms()?;
                tx.execute(
                    "UPDATE scripts SET active_revision=?2,updated_at_ms=?3 WHERE script_id=?1",
                    params![script_id, revision, now],
                )?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":script_id,
                    "active_revision":revision,
                    "bundle_ref":current_snapshot.record.artifact_id,
                    "state":"activated",
                    "execution_started":false,
                    "controller_effects":artifact_controller_effects(&current_snapshot.record.metadata)?,
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.activate",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":script_id,"revision":revision}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    pub(crate) async fn reconcile_script_triggers_once(&self, limit: usize) -> Result<Value> {
        let intents = self
            .run(move |db| automation_dispatch::pending_script_triggers(db, limit))
            .await?;
        let mut outcomes = Vec::new();
        for intent in intents {
            match self.admit_script_trigger(intent.clone()).await {
                Ok(value) => {
                    let details = value.clone();
                    let done = intent.clone();
                    self.run(move |db| {
                        automation_dispatch::finish_script_trigger(
                            db,
                            &done,
                            "admitted",
                            details,
                            model::now_ms()?,
                        )?;
                        Ok(())
                    })
                    .await?;
                    outcomes.push(json!({"submission_ref":intent.cause["id"],"script_id":intent.script_id,"state":"admitted","operation_id":value["operation_id"]}));
                }
                Err(failure) if permanent_script_trigger_error(&failure.error) => {
                    let details =
                        json!({"code":failure.error.code,"message":failure.error.message});
                    let done = intent.clone();
                    let projection = details.clone();
                    self.run(move |db| {
                        automation_dispatch::finish_script_trigger(
                            db,
                            &done,
                            "rejected",
                            projection,
                            model::now_ms()?,
                        )?;
                        Ok(())
                    })
                    .await?;
                    outcomes.push(json!({"submission_ref":intent.cause["id"],"script_id":intent.script_id,"state":"rejected","error":details}));
                }
                Err(failure)
                    if blocked_script_trigger_error(&failure.error)
                        && (failure.error.code != "SCRIPT_REGISTRY_DAMAGED"
                            || failure.failed_script_revision.is_some()) =>
                {
                    let reason = failure.error.code.clone();
                    let failed_script_revision = failure.failed_script_revision;
                    let mut details =
                        json!({"code":failure.error.code,"message":failure.error.message});
                    if let Some(revision) = failed_script_revision {
                        details["failed_script_revision"] = json!(revision);
                    }
                    let blocked = intent.clone();
                    let projection = details.clone();
                    self.run(move |db| {
                        automation_dispatch::hold_script_trigger(
                            db,
                            &blocked,
                            &reason,
                            failed_script_revision,
                            projection,
                            model::now_ms()?,
                        )?;
                        Ok(())
                    })
                    .await?;
                    outcomes.push(json!({"submission_ref":intent.cause["id"],"script_id":intent.script_id,"state":"blocked_pending_revalidation","error":details}));
                }
                Err(failure) => return Err(failure.error),
            }
        }
        Ok(json!({"considered":outcomes.len(),"outcomes":outcomes}))
    }

    async fn admit_script_trigger(
        &self,
        intent: automation_dispatch::ScriptTriggerIntent,
    ) -> std::result::Result<Value, ScriptTriggerAdmissionFailure> {
        let intent_for_prepare = intent.clone();
        let app_config = self.config.clone();
        let (params, grant) = self
            .run(move |db| {
                let entry = automation_config::load_entry(
                    db,
                    &intent_for_prepare.owner_manager_id,
                    &intent_for_prepare.project_id,
                    &intent_for_prepare.automation_id,
                )?
                .ok_or_else(|| {
                    Error::new("AUTOMATION_ACTION_CHANGED", "ScriptRun automation disappeared")
                })?;
                automation_config::validate_entry(&entry)?;
                if entry.revision != intent_for_prepare.automation_revision
                    || !entry.script_run_ready()
                    || entry
                        .script_run
                        .as_ref()
                        .is_none_or(|settings| settings.script_id != intent_for_prepare.script_id)
                {
                    return Err(Error::new(
                        "AUTOMATION_ACTION_CHANGED",
                        "current automation no longer admits the staged ScriptRun intent",
                    ));
                }
                let context = intent_for_prepare
                    .consumer_context
                    .clone()
                    .ok_or_else(|| Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "staged ScriptRun has no persisted owner context",
                    ))?;
                if !context.matches_pending_identity(
                    &intent_for_prepare.owner_manager_id,
                    &intent_for_prepare.project_id,
                    &intent_for_prepare.automation_id,
                    intent_for_prepare.automation_revision,
                    &intent_for_prepare.script_id,
                ) {
                    return Err(Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "staged ScriptRun owner context differs from its journal identity",
                    ));
                }
                let current_entry = context.require_current_entry(db)?;
                let source = context.require_current_source(
                    db,
                    &app_config,
                    &current_entry,
                    &intent_for_prepare.cause,
                )?;
                let (task, attempt) = context.require_live_subject(db, &source)?;
                let script_owner: String = db.query_row(
                    "SELECT owner_id FROM scripts WHERE script_id=?1",
                    [&intent_for_prepare.script_id],
                    |row| row.get(0),
                ).optional()?.ok_or_else(|| Error::new("NOT_FOUND", "selected script was not found"))?;
                if script_owner != entry.owner_manager_id {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "only the current automation Manager who owns the script may enable its trigger",
                    ));
                }
                let (active_revision, _) = script_head(db, &intent_for_prepare.script_id)?;
                let expected_script_revision = active_revision.ok_or_else(|| {
                    Error::new(
                        "SCRIPT_REVISION_NOT_ACTIVE",
                        "selected script has no active immutable revision",
                    )
                })?;
                let request_id = match intent_for_prepare.cause["kind"].as_str() {
                    Some("applied_submission") => {
                        let (Some(task_id), Some(task_revision), Some(attempt_id)) = (
                            source.task_id.as_deref(),
                            source.task_revision,
                            source.attempt_id.as_deref(),
                        ) else {
                            return Err(Error::new(
                                "SCRIPT_SCOPE_CHANGED",
                                "applied submission lacks its exact Task/Attempt scope",
                            ));
                        };
                        automatic_script_request_id(
                            &entry.automation_id,
                            task_id,
                            task_revision,
                            attempt_id,
                            model::text(&intent_for_prepare.cause, "id")?,
                            &intent_for_prepare.script_id,
                        )?
                    }
                    Some("system_event") => authorization::script_run_event_request_id(
                        &entry.automation_id,
                        model::text(&intent_for_prepare.cause, "id")?,
                        &intent_for_prepare.script_id,
                    )?,
                    _ => {
                        return Err(Error::new(
                            "AUTOMATION_ACTION_CHANGED",
                            "staged ScriptRun cause is unsupported",
                        ));
                    }
                };
                let params = json!({
                    "client_request_id":request_id,
                    "script_id":intent_for_prepare.script_id,
                    "expected_script_revision":expected_script_revision,
                    "attempt_id":source.attempt_id,
                    "expected_task_revision":source.task_revision,
                    "input":source.input
                });
                let mut cause = intent_for_prepare.cause.clone();
                cause["script_id"] = json!(intent_for_prepare.script_id);
                cause["script_revision"] = json!(expected_script_revision);
                if let (Some(task), Some(attempt)) = (task.as_ref(), attempt.as_ref()) {
                    cause["task_id"] = task["task_id"].clone();
                    cause["task_revision"] = task["revision"].clone();
                    cause["attempt_id"] = attempt["attempt_id"].clone();
                    if let Some(submission_ref) = cause["id"].as_str() {
                        let document = submissions::document(db, submission_ref)?;
                        cause["candidate_ref"] = document["candidate_ref"].clone();
                    }
                }
                let cause = context.with_cause(&cause)?;
                Ok((params, ScriptRunTriggerGrant { entry, cause, context }))
            })
            .await?;
        let failed_script_revision = grant.cause["script_revision"].as_i64();
        self.run_script_with_trigger(None, params, Some(grant))
            .await
            .map_err(|error| ScriptTriggerAdmissionFailure {
                failed_script_revision: if error.code == "SCRIPT_REGISTRY_DAMAGED" {
                    failed_script_revision
                } else {
                    None
                },
                error,
            })
    }

    async fn run_script(&self, principal: Principal, params: Value) -> Result<Value> {
        self.run_script_with_trigger(Some(principal), params, None)
            .await
    }

    async fn run_script_with_trigger(
        &self,
        principal: Option<Principal>,
        params: Value,
        trigger: Option<ScriptRunTriggerGrant>,
    ) -> Result<Value> {
        let request = protocol::RunRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if trigger.is_none() && request.attempt_id.is_none() {
            return Err(Error::invalid(
                "manual script.run requires an exact Task Attempt and revision",
            ));
        }
        if trigger.is_none() {
            let direct_principal = principal.as_ref().ok_or_else(|| {
                Error::new(
                    "UNAUTHORIZED",
                    "manual ScriptRun requires its direct caller",
                )
            })?;
            if let Some(value) = self.replay(direct_principal, "script.run", &params).await? {
                return Ok(value);
            }
        }
        let script_id = request.script_id.clone();
        let expected_revision = request.expected_script_revision;
        let attempt_id = request.attempt_id.clone();
        let expected_task_revision = request.expected_task_revision;
        let principal_for_snapshot = principal.clone();
        let trigger_for_snapshot = trigger.clone();
        let config_for_snapshot = self.config.clone();
        let snapshot = self
            .run(move |db| {
                let (task, attempt) = if let Some(grant) = trigger_for_snapshot.as_ref() {
                    validate_script_trigger_current(
                        db,
                        &config_for_snapshot,
                        grant,
                        &script_id,
                        expected_revision,
                    )?
                } else {
                    let direct_principal = principal_for_snapshot.as_ref().ok_or_else(|| {
                        Error::new("UNAUTHORIZED", "manual ScriptRun has no direct caller")
                    })?;
                    let current = require_script_scope(db, direct_principal, &script_id)?;
                    require_optional_run_scope(
                        db,
                        &current,
                        attempt_id.as_deref(),
                        expected_task_revision,
                    )?
                };
                let (active, _) = script_head(db, &script_id)?;
                if active != Some(expected_revision) {
                    return Err(Error::new(
                        "SCRIPT_REVISION_NOT_ACTIVE",
                        "requested script revision is not active",
                    ));
                }
                let revision = revision_snapshot(db, &script_id, expected_revision)?;
                Ok((task, attempt, revision))
            })
            .await?;
        let task = snapshot.0;
        let attempt = snapshot.1;
        let revision = snapshot.2;
        let record = revision.record.clone();
        let interpreter = revision.interpreter.clone();
        let input = request.input.clone();
        let (bundle, environment) = self
            .file_io(move |files| {
                let bundle = verify_revision_files(&files, &record, &interpreter)?;
                protocol::validate_input(&bundle.input_schema, &input)?;
                let environment = runner::capture_environment(&bundle)?;
                Ok((bundle, environment))
            })
            .await?;
        let operation_id = model::new_id();
        let run_id = model::new_id();
        let token = model::new_id();
        let invocation_effects = if task.is_some() && attempt.is_some() {
            bundle
                .controller_effects
                .iter()
                .copied()
                .filter(|effect| *effect == manifest::ScriptControllerEffect::TaskOwnerMessage)
                .collect()
        } else if task.is_none()
            && attempt.is_none()
            && trigger
                .as_ref()
                .is_some_and(|grant| grant.cause["kind"] == "system_event")
        {
            bundle
                .controller_effects
                .iter()
                .copied()
                .filter(|effect| {
                    matches!(
                        effect,
                        manifest::ScriptControllerEffect::ManagerNotification
                            | manifest::ScriptControllerEffect::TaskCreate
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        let invocation = protocol::ScriptInvocation {
            protocol_version: 1,
            operation_id: operation_id.clone(),
            run_id: run_id.clone(),
            script_id: request.script_id.clone(),
            script_revision: request.expected_script_revision,
            task_id: task
                .as_ref()
                .map(|task| model::text(task, "task_id").map(ToOwned::to_owned))
                .transpose()?,
            task_revision: request.expected_task_revision,
            attempt_id: request.attempt_id.clone(),
            input: request.input.clone(),
            controller_effects: invocation_effects,
        };
        let work = runner::Work {
            run_id: run_id.clone(),
            operation_id: operation_id.clone(),
            token,
            data_dir: self.data_dir.clone(),
            bundle_record: revision.record.clone(),
            interpreter: bundle.interpreter.clone(),
            bundle: bundle.clone(),
            environment_sha256: runner::environment_sha256(&environment)?,
            environment,
            invocation,
            executor: self.config.scripts.executor.clone(),
        };
        let work_for_write = work.clone();
        let (receipt_path, work_digest) = self
            .file_io(move |_| runner::write_work(&work_for_write))
            .await?;
        if receipt_path != runner::directory(&self.data_dir, &run_id)?.join("receipt.json") {
            return Err(Error::new(
                "SCRIPT_WORK_DAMAGED",
                "stored script receipt path differs from its run",
            ));
        }
        let request_json = model::canonical(&params)?;
        let caller = principal
            .as_ref()
            .map(|principal| principal.client_id.clone());
        let operation_caller = if trigger.is_some() {
            authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned()
        } else {
            caller.clone().ok_or_else(|| {
                Error::new("UNAUTHORIZED", "manual ScriptRun has no direct caller")
            })?
        };
        let trigger_for_tx = trigger;
        let p = principal;
        let config_for_tx = self.config.clone();
        let request_id = request.client_request_id;
        let script_id = request.script_id;
        let requested_revision = request.expected_script_revision;
        let expected_task_revision = request.expected_task_revision;
        let attempt_id = request.attempt_id;
        let stored_task = task.clone();
        let stored_attempt = attempt.clone();
        let stored_revision = revision.clone();
        let digest_for_tx = work_digest.clone();
        let work_for_tx = work.clone();
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = p
                    .as_ref()
                    .map(|principal| require_script_scope(&tx, principal, &script_id))
                    .transpose()?;
                if let Some(receipt) = replay_tx(&tx, &operation_caller, "script.run", &request_id, &request_json)? {
                    if let Some(grant) = trigger_for_tx.as_ref() {
                        let old_operation_id: String = tx.query_row(
                            "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                            params![operation_caller, request_id],
                            |row| row.get(0),
                        )?;
                        let link = authorization::operation_link(&tx, &old_operation_id)?
                            .ok_or_else(|| Error::new("AUTOMATION_LINK_CORRUPT", "replayed triggered ScriptRun has no on-behalf link"))?;
                        if link.action != "script.run"
                            || link.effective_manager_id != grant.entry.owner_manager_id
                            || link.project_id != grant.entry.project_id
                            || link.automation_id != grant.entry.automation_id
                            || automation_dispatch::script_trigger_authority::ScriptRunConsumerContext::from_cause(&link.cause)?
                                != grant.context
                            || !authorization::script_run_causes_semantically_match(
                                &link.cause,
                                &grant.cause,
                            )
                        {
                            return Err(Error::new("REQUEST_ID_CONFLICT", "semantic ScriptRun request is retained under another automation cause"));
                        }
                    }
                    tx.commit()?;
                    return Ok(receipt);
                }
                let (current_task, current_attempt) = if let Some(grant) = trigger_for_tx.as_ref() {
                    validate_script_trigger_current(
                        &tx,
                        &config_for_tx,
                        grant,
                        &script_id,
                        requested_revision,
                    )?
                } else {
                    let current = current.as_ref().ok_or_else(|| {
                        Error::new("UNAUTHORIZED", "manual ScriptRun has no current caller")
                    })?;
                    require_optional_run_scope(
                        &tx,
                        current,
                        attempt_id.as_deref(),
                        expected_task_revision,
                    )?
                };
                let scope_unchanged = match (
                    current_task.as_ref(),
                    current_attempt.as_ref(),
                    stored_task.as_ref(),
                    stored_attempt.as_ref(),
                ) {
                    (Some(current_task), Some(current_attempt), Some(stored_task), Some(stored_attempt)) => {
                        current_task["task_id"] == stored_task["task_id"]
                            && current_task["revision"] == stored_task["revision"]
                            && current_attempt["attempt_id"] == stored_attempt["attempt_id"]
                            && current_attempt["task_revision"] == stored_attempt["task_revision"]
                    }
                    (None, None, None, None) => true,
                    _ => false,
                };
                if !scope_unchanged {
                    return Err(Error::new("SCRIPT_SCOPE_CHANGED", "Task or Attempt changed during run preparation"));
                }
                let (active, _) = script_head(&tx, &script_id)?;
                if active != Some(requested_revision) {
                    return Err(Error::new("SCRIPT_REVISION_NOT_ACTIVE", "requested script revision is no longer active"));
                }
                let current_revision = revision_snapshot(&tx, &script_id, requested_revision)?;
                if current_revision.record.artifact_id != stored_revision.record.artifact_id
                    || current_revision.record.content_digest != stored_revision.record.content_digest
                    || current_revision.interpreter != stored_revision.interpreter
                {
                    return Err(Error::new("SCRIPT_REVISION_CHANGED", "retained script revision changed during run preparation"));
                }
                if work_for_tx.invocation.task_id.as_deref()
                    != current_task.as_ref().and_then(|task| task["task_id"].as_str())
                    || work_for_tx.invocation.task_revision
                        != current_task.as_ref().and_then(|task| task["revision"].as_i64())
                    || work_for_tx.invocation.attempt_id.as_deref()
                        != current_attempt
                            .as_ref()
                            .and_then(|attempt| attempt["attempt_id"].as_str())
                {
                    return Err(Error::new("SCRIPT_SCOPE_CHANGED", "script invocation Task/Attempt identity differs from the current scope"));
                }
                let now = model::now_ms()?;
                if !work_for_tx.invocation.controller_effects.is_empty() {
                    let effect_manager_id = trigger_for_tx
                        .as_ref()
                        .map(|grant| grant.context.owner_manager_id())
                        .or_else(|| current.as_ref().map(|principal| principal.client_id.as_str()))
                        .ok_or_else(|| {
                            Error::new(
                                "UNAUTHORIZED",
                                "script effect has no registered effective Manager",
                            )
                        })?;
                    if trigger_for_tx.is_none()
                        && current.as_ref().is_none_or(|manager| manager.role != Role::Manager)
                    {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "script controller effects require a Manager-owned script",
                        ));
                    }
                    let script_owner: String = tx.query_row(
                        "SELECT owner_id FROM scripts WHERE script_id=?1",
                        [&script_id],
                        |row| row.get(0),
                    )?;
                    if script_owner != effect_manager_id {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "only the Manager who owns this script revision may admit its controller effect grant",
                        ));
                    }
                }
                let effective_manager_id = trigger_for_tx
                    .as_ref()
                    .map(|grant| grant.context.owner_manager_id())
                    .or_else(|| current.as_ref().map(|principal| principal.client_id.as_str()));
                let admitted = json!({
                    "operation_id":operation_id,
                    "run_id":run_id,
                    "script_id":script_id,
                    "revision":requested_revision,
                    "bundle_ref":current_revision.record.artifact_id,
                    "task_id":current_task.as_ref().and_then(|task| task.get("task_id")),
                    "task_revision":current_task.as_ref().and_then(|task| task.get("revision")),
                    "attempt_id":current_attempt.as_ref().and_then(|attempt| attempt.get("attempt_id")),
                    "state":"queued",
                    "admission":"durable_local",
                    "controller_effects":work_for_tx.invocation.controller_effects,
                });
                let mut effective = json!({
                    "script_run":{"run_id":run_id,"bundle_ref":current_revision.record.artifact_id,"work_digest":digest_for_tx,"environment_sha256":work_for_tx.environment_sha256,"controller_effects":work_for_tx.invocation.controller_effects},
                    "receipt":{"ok":true,"value":admitted},
                });
                let linkage = if let Some(grant) = trigger_for_tx.as_ref() {
                    let linkage = authorization::save_script_run_operation_link(
                        &tx,
                        &operation_id,
                        &grant.entry,
                        &grant.cause,
                        now,
                    )?;
                    effective["automation_on_behalf"] = linkage.clone();
                    Some(linkage)
                } else {
                    None
                };
                tx.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'script.run',?4,?5,?6,?7,'queued',?8,?9,NULL,?9,?9)",
                    params![
                        operation_id,
                        operation_caller,
                        request_id,
                        request_json,
                        model::canonical(&effective)?,
                        current_task
                            .as_ref()
                            .and_then(|task| task["task_id"].as_str()),
                        current_attempt
                            .as_ref()
                            .and_then(|attempt| attempt["attempt_id"].as_str()),
                        model::canonical(&admitted)?,
                        now,
                    ],
                )?;
                let mut run_spec = json!({
                    "environment_sha256":work_for_tx.environment_sha256,
                    "input_sha256":model::digest(model::canonical(&work_for_tx.invocation.input)?.as_bytes()),
                    "capabilities":work_for_tx.invocation.controller_effects,
                    "invocation":{
                        "operation_id":operation_id,
                        "run_id":run_id,
                        "script_id":script_id,
                        "script_revision":requested_revision,
                        "task_id":current_task.as_ref().and_then(|task| task.get("task_id")),
                        "task_revision":current_task.as_ref().and_then(|task| task.get("revision")),
                        "attempt_id":current_attempt.as_ref().and_then(|attempt| attempt.get("attempt_id")),
                    "effective_manager_id":effective_manager_id,
                        "cause":{"kind":"script.run","operation_id":operation_id,"run_id":run_id},
                    },
                    "trust":"trusted_local",
                });
                if let Some(linkage) = linkage {
                    run_spec["automation_on_behalf"] = linkage;
                }
                tx.execute(
                    "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'queued',?11)",
                    params![
                        run_id,
                        operation_id,
                        script_id,
                        requested_revision,
                        current_revision.record.artifact_id,
                        current_task
                            .as_ref()
                            .and_then(|task| task["task_id"].as_str()),
                        current_task
                            .as_ref()
                            .and_then(|task| task["revision"].as_i64()),
                        current_attempt
                            .as_ref()
                            .and_then(|attempt| attempt["attempt_id"].as_str()),
                        digest_for_tx,
                        model::canonical(&run_spec)?,
                        now,
                    ],
                )?;
                tx.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.run',?3,?4)",
                    params![format!("admitted:{operation_id}"), operation_id, model::canonical(&admitted)?, now],
                )?;
                tx.commit()?;
                Ok(admitted)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    async fn get_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::GetRequest::parse(&params)?;
        self.run(move |db| {
            require_script_scope(db, &principal, &request.script_id)?;
            registry::describe(db, &request.script_id, request.revision)
        })
        .await
    }

    async fn list_scripts(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ListRequest::parse(&params)?;
        let after = request.after.unwrap_or(0);
        let limit = request.limit.unwrap_or(50);
        self.run(move |db| {
            let current = require_script_authority(db, &principal)?;
            match gm::require_authority(db, &current) {
                Ok(()) => registry::list(db, after, limit),
                Err(error) if error.code == "FORBIDDEN" => {
                    registry::list_owned(db, &current.client_id, after, limit)
                }
                Err(error) => Err(error),
            }
        })
        .await
    }

    async fn replay(
        &self,
        principal: &Principal,
        method: &'static str,
        params: &Value,
    ) -> Result<Option<Value>> {
        let current = principal.clone();
        let caller = principal.client_id.clone();
        let request_id = model::text(params, "client_request_id")?.to_owned();
        let original = model::canonical(params)?;
        let script_id = if method == "script.register" {
            None
        } else {
            params
                .get("script_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        self.run(move |db| {
            let current = require_script_authority(db, &current)?;
            if let Some(script_id) = script_id.as_deref() {
                require_script_scope(db, &current, script_id)?;
            }
            replay_tx(db, &caller, method, &request_id, &original)
        })
        .await
    }

    pub async fn supervise_scripts(&self, mut stop: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            self.reconcile_scripts_once().await?;
            tokio::select! {
                _ = tokio::time::sleep(RECONCILE_INTERVAL) => {},
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { return Ok(()); }
                }
            }
        }
    }

    pub(crate) async fn reconcile_scripts_once(&self) -> Result<()> {
        let rows = self.run(|db| pending_runs(db)).await?;
        for pending in rows {
            let run_id = pending.run_id.clone();
            let digest = pending.work_digest.clone();
            let data_dir = self.data_dir.clone();
            let loaded = self
                .file_io(move |_| {
                    let receipt = runner::directory(&data_dir, &run_id)?.join("receipt.json");
                    let (work, actual_digest) = runner::read_work_record(&receipt)?;
                    if actual_digest != digest {
                        return Err(Error::new(
                            "SCRIPT_WORK_DAMAGED",
                            "retained work digest differs from SQLite",
                        ));
                    }
                    Ok(work)
                })
                .await;
            let work = match loaded {
                Ok(work) => work,
                Err(error) => {
                    self.record_run_error(&pending, error.clone()).await?;
                    if pending.operation_state == "queued" {
                        self.fail_before_start(&pending, &error.code).await?;
                    } else if pending.operation_state == "sending" {
                        let data_dir = self.data_dir.clone();
                        let run_id = pending.run_id.clone();
                        let operation_id = pending.operation_id.clone();
                        let code = error.code.clone();
                        let denied = self
                            .file_io(move |_| {
                                runner::deny_unstarted(&data_dir, &run_id, &operation_id, &code)
                            })
                            .await
                            .unwrap_or(false);
                        if denied {
                            self.settle_prestart_failure_if_family_departed(&pending, &error.code)
                                .await?;
                        } else {
                            self.mark_unknown(&pending, &error.code).await?;
                        }
                    } else if matches!(
                        pending.operation_state.as_str(),
                        "native_accepted" | "outcome_unknown"
                    ) {
                        self.settle_incomplete_if_worker_departed(&pending, &error.code)
                            .await?;
                    }
                    continue;
                }
            };
            let observe_work = work.clone();
            let observed = self
                .file_io(move |files| observe(&observe_work, &files))
                .await;
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    self.record_run_error(&pending, error.clone()).await?;
                    if pending.operation_state == "queued" {
                        self.fail_before_start(&pending, &error.code).await?;
                    } else if pending.operation_state == "sending"
                        && !self.start_gate_exists(&work).await
                    {
                        self.deny_work(&work, &error.code).await?;
                        self.settle_prestart_failure_if_family_departed(&pending, &error.code)
                            .await?;
                    } else if matches!(
                        pending.operation_state.as_str(),
                        "native_accepted" | "outcome_unknown"
                    ) {
                        self.settle_incomplete_if_worker_departed(&pending, &error.code)
                            .await?;
                    } else {
                        self.mark_unknown(&pending, &error.code).await?;
                    }
                    continue;
                }
            };
            if let Some(failure) = observed.process_control_failure.as_ref() {
                self.record_process_control_failure(&pending, failure)
                    .await?;
            }
            if let Some(completion) = observed.completion {
                if !completion_matches_retained_worker(&pending, &completion) {
                    let code = "SCRIPT_COMPLETION_DAMAGED";
                    self.record_run_error(
                        &pending,
                        Error::new(
                            code,
                            "completion identity differs from the Store-retained worker identity",
                        ),
                    )
                    .await?;
                    self.settle_incomplete_if_worker_departed(&pending, code)
                        .await?;
                    continue;
                }
                if !observed.launch_departed {
                    let code = "SCRIPT_COMPLETION_FAMILY_ACTIVE";
                    self.record_run_error(
                        &pending,
                        Error::new(
                            code,
                            "validated ScriptRun completion is waiting for its exact worker Group to depart",
                        ),
                    )
                    .await?;
                    self.mark_unknown(&pending, code).await?;
                    continue;
                }
                let execution_may_have_started =
                    observed.has_go || completion.started_at_ms.is_some();
                let id = pending.run_id.clone();
                let config = self.config.clone();
                let result = self
                    .run(move |db| finish(db, &id, completion, &config))
                    .await;
                if let Err(error) = result {
                    if completion_failure_is_terminal(&error) {
                        self.record_run_error(&pending, error.clone()).await?;
                        self.settle_incomplete(&pending, &error.code, execution_may_have_started)
                            .await?;
                    } else {
                        return Err(error);
                    }
                } else {
                    self.changed
                        .send_modify(|revision| *revision = revision.wrapping_add(1));
                }
                continue;
            }
            match pending.operation_state.as_str() {
                "queued" => {
                    let id = pending.run_id.clone();
                    let operation_id = pending.operation_id.clone();
                    let start_work = work.clone();
                    let config = self.config.clone();
                    let started = self
                        .run(move |db| begin_run(db, &id, &operation_id, &config))
                        .await;
                    match started {
                        Ok(true) => {
                            let spawn_work = start_work.clone();
                            let digest = pending.work_digest.clone();
                            let launch = self
                                .file_io(move |_| runner::prepare_and_spawn(&spawn_work, &digest))
                                .await;
                            if let Err(error) = launch {
                                self.record_run_error(&pending, error.clone()).await?;
                                self.deny_work(&start_work, &error.code).await?;
                                self.mark_unknown(&pending, &error.code).await?;
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            self.record_run_error(&pending, error).await?;
                        }
                    }
                }
                "sending" => {
                    if let Some(identity) = observed.ready {
                        let id = pending.run_id.clone();
                        let config = self.config.clone();
                        let accepted = self
                            .run(move |db| acknowledge_worker(db, &id, &identity, &config))
                            .await;
                        match accepted {
                            Ok(true) => {
                                let allow_work = work.clone();
                                let result =
                                    self.file_io(move |_| runner::allow(&allow_work)).await;
                                if let Err(error) = result {
                                    self.record_run_error(&pending, error.clone()).await?;
                                    if !self.start_gate_exists(&work).await {
                                        self.deny_work(&work, &error.code).await?;
                                    }
                                    self.mark_unknown(&pending, &error.code).await?;
                                } else {
                                    self.changed.send_modify(|revision| {
                                        *revision = revision.wrapping_add(1)
                                    });
                                }
                            }
                            Ok(false) => {}
                            Err(error) => {
                                self.record_run_error(&pending, error.clone()).await?;
                                self.deny_work(&work, &error.code).await?;
                                self.mark_unknown(&pending, &error.code).await?;
                            }
                        }
                    } else if observed.launch_departed {
                        let code = "SCRIPT_WORKER_LOST_BEFORE_START";
                        self.record_run_error(
                            &pending,
                            Error::new(code, "worker exited before its start gate"),
                        )
                        .await?;
                        if observed.has_go {
                            self.mark_unknown(&pending, code).await?;
                        } else {
                            self.deny_work(&work, code).await?;
                            self.settle_prestart_failure_if_family_departed(&pending, code)
                                .await?;
                        }
                    } else if pending.sent_at_ms.is_some_and(|sent| {
                        model::now_ms()
                            .is_ok_and(|now| now.saturating_sub(sent) > STARTING_WORKER_TIMEOUT_MS)
                    }) {
                        let code = "SCRIPT_WORKER_START_TIMEOUT";
                        self.record_run_error(
                            &pending,
                            Error::new(code, "worker did not publish its ready receipt"),
                        )
                        .await?;
                        self.deny_work(&work, code).await?;
                        self.settle_prestart_failure_if_family_departed(&pending, code)
                            .await?;
                    }
                }
                "native_accepted" | "outcome_unknown"
                    if observed.launch_departed
                        || (observed.ready.is_none() && observed.launch.is_none())
                        || script_worker_observation_expired(&pending) =>
                {
                    let code = "SCRIPT_OUTCOME_UNOBSERVED";
                    let detail = if observed.launch_departed {
                        "started worker departed without a completion receipt"
                    } else if script_worker_observation_expired(&pending) {
                        "ScriptRun exceeded its existing start-gate and execution observation bounds"
                    } else {
                        "started worker identity and launch record are both unavailable"
                    };
                    self.record_run_error(&pending, Error::new(code, detail))
                        .await?;
                    if pending.worker_identity_json.is_none() && !observed.has_go {
                        self.settle_prestart_failure_if_family_departed(&pending, code)
                            .await?;
                    } else if observed.launch_departed {
                        self.settle_incomplete_if_worker_departed(&pending, code)
                            .await?;
                    } else {
                        self.mark_unknown(&pending, code).await?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn deny_work(&self, work: &runner::Work, code: &str) -> Result<()> {
        let work = work.clone();
        let code = code.to_owned();
        // A missing denial file cannot authorize execution: the worker still
        // requires the separate go receipt, and will time out if it never
        // arrives. Keep this optional run failure local to its Operation.
        let _ = self.file_io(move |_| runner::deny(&work, &code)).await;
        Ok(())
    }

    async fn start_gate_exists(&self, work: &runner::Work) -> bool {
        let work = work.clone();
        self.file_io(move |_| runner::has_start_gate(&work))
            .await
            .unwrap_or(true)
    }

    async fn fail_before_start(&self, pending: &PendingRun, code: &str) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| fail_before_start(db, &run_id, &code))
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn settle_incomplete(
        &self,
        pending: &PendingRun,
        code: &str,
        execution_may_have_started: bool,
    ) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| settle_incomplete(db, &run_id, &code, execution_may_have_started))
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn mark_unknown(&self, pending: &PendingRun, code: &str) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| mark_unknown(db, &run_id, &code)).await?;
        if pending.operation_state != "outcome_unknown" {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    async fn record_process_control_failure(
        &self,
        pending: &PendingRun,
        failure: &swarm_scripts::process::ProcessControlFailure,
    ) -> Result<()> {
        let pending = pending.clone();
        let failure = failure.clone();
        let changed = self
            .run(move |db| record_process_control_failure(db, &pending, &failure))
            .await?;
        if changed {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    async fn settle_prestart_failure_if_family_departed(
        &self,
        pending: &PendingRun,
        code: &str,
    ) -> Result<()> {
        let data_dir = self.data_dir.clone();
        let run_id = pending.run_id.clone();
        let operation_id = pending.operation_id.clone();
        let departed = self
            .file_io(move |_| {
                runner::prestart_worker_family_departed(&data_dir, &run_id, &operation_id)
            })
            .await;
        match departed {
            Ok(true) if pending.operation_state == "sending" => {
                self.fail_before_start(pending, code).await
            }
            Ok(true) => self.settle_incomplete(pending, code, false).await,
            Ok(false) => self.mark_unknown(pending, code).await,
            Err(error) => {
                self.record_run_error(pending, error.clone()).await?;
                self.mark_unknown(pending, &error.code).await
            }
        }
    }

    async fn settle_incomplete_if_worker_departed(
        &self,
        pending: &PendingRun,
        code: &str,
    ) -> Result<()> {
        let Some(identity_json) = pending.worker_identity_json.clone() else {
            return self
                .settle_prestart_failure_if_family_departed(pending, code)
                .await;
        };
        let identity: Value = match serde_json::from_str(&identity_json) {
            Ok(identity) => identity,
            Err(_) => {
                let error = Error::new(
                    "SCRIPT_WORKER_IDENTITY_DAMAGED",
                    "retained ScriptRun process identity cannot be parsed",
                );
                self.record_run_error(pending, error.clone()).await?;
                self.settle_prestart_failure_if_family_departed(pending, &error.code)
                    .await?;
                return Ok(());
            }
        };
        let data_dir = self.data_dir.clone();
        let run_id = pending.run_id.clone();
        let operation_id = pending.operation_id.clone();
        let departed = self
            .file_io(move |_| {
                runner::worker_family_departed_from_identity(
                    &data_dir,
                    &run_id,
                    &operation_id,
                    &identity,
                )
            })
            .await;
        match departed {
            Ok(true) => {
                self.settle_incomplete(pending, code, true).await?;
                Ok(())
            }
            Ok(false) => {
                self.mark_unknown(pending, code).await?;
                Ok(())
            }
            Err(error) => {
                self.record_run_error(pending, error.clone()).await?;
                self.mark_unknown(pending, &error.code).await?;
                Ok(())
            }
        }
    }

    async fn record_run_error(&self, pending: &PendingRun, error: Error) -> Result<()> {
        let run_id = pending.run_id.clone();
        let operation_id = pending.operation_id.clone();
        self.run(move |db| record_incident(db, &run_id, &operation_id, error))
            .await
    }
}

fn require_script_authority(db: &Connection, principal: &Principal) -> Result<Principal> {
    let current = current_principal(db, principal.clone())?;
    if !matches!(current.role, Role::Manager | Role::Operator) {
        return Err(Error::new(
            "FORBIDDEN",
            "script methods require Manager or local Operator authority",
        ));
    }
    Ok(current)
}

/// Managers manage their own immutable script catalog independently of the
/// current-GM designation. Cross-owner access remains a real GM/Operator
/// capability, checked against the current principal on every call.
fn require_script_scope(
    db: &Connection,
    principal: &Principal,
    script_id: &str,
) -> Result<Principal> {
    let current = require_script_authority(db, principal)?;
    let owner: Option<String> = db
        .query_row(
            "SELECT owner_id FROM scripts WHERE script_id=?1",
            [script_id],
            |row| row.get(0),
        )
        .optional()?;
    if current.role == Role::Manager {
        if owner.as_deref() == Some(current.client_id.as_str()) {
            return Ok(current);
        }
        // Unknown and foreign script IDs are indistinguishable to an
        // ordinary Manager. A current GM retains the existing shared-catalog
        // read path without making existence a side channel to other owners.
        gm::require_authority(db, &current)?;
    } else {
        gm::require_authority(db, &current)?;
    }
    owner.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
    Ok(current)
}

fn validate_client_request_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(
            "client_request_id must be 1..=128 bytes without whitespace",
        ));
    }
    Ok(())
}

fn automatic_script_request_id(
    automation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    submission_ref: &str,
    script_id: &str,
) -> Result<String> {
    authorization::script_run_request_id(
        automation_id,
        task_id,
        task_revision,
        attempt_id,
        submission_ref,
        script_id,
    )
}

fn permanent_script_trigger_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "SCRIPT_SCHEMA_MISMATCH" | "INVALID_PARAMS"
    ) || error.code.starts_with("SCRIPT_BUNDLE")
        || error.code.starts_with("SCRIPT_INPUT")
}

fn blocked_script_trigger_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "AUTOMATION_ACTION_CHANGED"
            | "AUTOMATION_LINK_CORRUPT"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
            | "NOT_FOUND"
            | "SCRIPT_EVENT_SOURCE_UNAUTHORIZED"
            | "SCRIPT_EVENT_SOURCE_REVOKED"
            | "SCRIPT_REVISION_NOT_ACTIVE"
            | "SCRIPT_REVISION_CHANGED"
            | "STALE_ATTEMPT"
            | "ATTEMPT_SCOPE_STALE"
            | "SCRIPT_SCOPE_CHANGED"
            | "SUBMISSION_DAMAGED"
            | "SCRIPT_REGISTRY_DAMAGED"
    )
}

fn validate_script_trigger_current(
    db: &Connection,
    app_config: &Config,
    grant: &ScriptRunTriggerGrant,
    script_id: &str,
    script_revision: i64,
) -> Result<(Option<Value>, Option<Value>)> {
    grant.context.require_matches_cause(&grant.cause)?;
    if !grant.context.matches_pending_identity(
        &grant.entry.owner_manager_id,
        &grant.entry.project_id,
        &grant.entry.automation_id,
        grant.entry.revision,
        script_id,
    ) || grant.cause["script_id"] != script_id
        || grant.cause["script_revision"] != script_revision
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "retained ScriptRun owner context or script selection is inconsistent",
        ));
    }
    let entry = grant.context.require_current_entry(db)?;
    if entry.revision != grant.entry.revision {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current ScriptRun entry changed after its typed consumer was retained",
        ));
    }
    let source = grant
        .context
        .require_current_source(db, app_config, &entry, &grant.cause)?;
    let (task, attempt) = grant.context.require_live_subject(db, &source)?;
    if source.task_id.as_deref() != task.as_ref().and_then(|value| value["task_id"].as_str())
        || source.task_revision != task.as_ref().and_then(|value| value["revision"].as_i64())
        || source.attempt_id.as_deref()
            != attempt
                .as_ref()
                .and_then(|value| value["attempt_id"].as_str())
    {
        return Err(Error::new(
            "SCRIPT_SCOPE_CHANGED",
            "current ScriptRun source no longer matches its exact Task/Attempt",
        ));
    }
    if let Some(task) = task.as_ref() {
        let current_attempt = attempt.as_ref().ok_or_else(|| {
            Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "current Task has no matching Attempt",
            )
        })?;
        if grant
            .cause
            .get("task_id")
            .is_some_and(|value| value != &task["task_id"])
            || grant
                .cause
                .get("task_revision")
                .is_some_and(|value| value != &task["revision"])
            || grant
                .cause
                .get("attempt_id")
                .is_some_and(|value| value != &current_attempt["attempt_id"])
        {
            return Err(Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "retained ScriptRun cause differs from current Task/Attempt facts",
            ));
        }
    }
    let script_owner: Option<String> = db
        .query_row(
            "SELECT owner_id FROM scripts WHERE script_id=?1",
            [script_id],
            |row| row.get(0),
        )
        .optional()?;
    if script_owner.as_deref() != Some(grant.context.owner_manager_id()) {
        return Err(Error::new(
            "FORBIDDEN",
            "selected script is no longer owned by the configured Manager",
        ));
    }
    let (active_revision, _) = script_head(db, script_id)?;
    if active_revision != Some(script_revision) {
        return Err(Error::new(
            "SCRIPT_REVISION_NOT_ACTIVE",
            "the exact script revision is no longer active",
        ));
    }
    Ok((task, attempt))
}

fn replay_tx(
    db: &Connection,
    caller: &str,
    method: &str,
    request_id: &str,
    original: &str,
) -> Result<Option<Value>> {
    let row: Option<(String, String, String)> = db
        .query_row(
            "SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((old_method, old_original, effective)) = row else {
        return Ok(None);
    };
    if old_method != method || old_original != original {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "script request ID was used with another method or payload",
        ));
    }
    let effective: Value = serde_json::from_str(&effective)?;
    let receipt = effective
        .get("receipt")
        .ok_or_else(|| Error::new("INVALID_RECEIPT", "script Operation has no receipt"))?;
    super::receipt_result(receipt).map(Some)
}

fn record_operation(
    tx: &Transaction<'_>,
    principal: &Principal,
    record: OperationRecord<'_>,
) -> Result<()> {
    let state = if record.settled { "settled" } else { "queued" };
    let effective = json!({"script":record.context,"receipt":{"ok":true,"value":record.result}});
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?9,?9)",
        params![
            record.operation_id,
            principal.client_id,
            record.request_id,
            record.method,
            record.original_request,
            model::canonical(&effective)?,
            state,
            model::canonical(record.result)?,
            record.now_ms,
            if record.settled { Some(record.now_ms) } else { None },
        ],
    )?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,?3,?4,?5)",
        params![format!("admitted:{}", record.operation_id), record.operation_id, record.method, model::canonical(record.result)?, record.now_ms],
    )?;
    Ok(())
}

fn script_exists(db: &Connection, script_id: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM scripts WHERE script_id=?1)",
        [script_id],
        |row| row.get(0),
    )?)
}

fn script_head(db: &Connection, script_id: &str) -> Result<(Option<i64>, i64)> {
    db.query_row(
        "SELECT active_revision,(SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=s.script_id) FROM scripts s WHERE script_id=?1",
        [script_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))
}

/// Called only while decoding a retained revision, never for caller input.
fn script_revision_integrity_error(error: Error) -> Error {
    if matches!(
        error.code.as_str(),
        "ARTIFACT_DAMAGED" | "SCRIPT_BUNDLE_DAMAGED" | "INVALID_PARAMS"
    ) {
        Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "retained script bundle integrity or metadata is invalid",
        )
    } else {
        error
    }
}

fn revision_snapshot(db: &Connection, script_id: &str, revision: i64) -> Result<RevisionSnapshot> {
    let record = registry::bundle_record(db, script_id, revision)
        .map_err(script_revision_integrity_error)?;
    let (bundle_sha256, interpreter_json, validated_at_ms): (String, String, i64) = db.query_row(
        "SELECT bundle_sha256,interpreter_json,validated_at_ms FROM script_revisions WHERE script_id=?1 AND revision=?2",
        params![script_id, revision],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if bundle_sha256 != record.content_digest || validated_at_ms <= 0 {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "script revision digest or validation receipt is invalid",
        ));
    }
    let interpreter: manifest::InterpreterIdentity = serde_json::from_str(&interpreter_json)
        .map_err(|_| {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "captured interpreter identity cannot be parsed",
            )
        })?;
    if record.metadata["script_id"] != script_id
        || record.metadata["revision"] != revision
        || record.metadata["bundle_sha256"] != bundle_sha256
        || record.metadata["interpreter_sha256"] != interpreter.sha256
    {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "bundle artifact metadata differs from its revision",
        ));
    }
    Ok(RevisionSnapshot {
        record,
        interpreter,
        validated_at_ms,
    })
}

/// Check retained revision metadata for trigger revalidation without reading
/// bundle files while the dispatcher owns its SQLite transaction.
pub(super) fn script_trigger_revision_snapshot(
    db: &Connection,
    script_id: &str,
    revision: i64,
) -> Result<()> {
    match revision_snapshot(db, script_id, revision) {
        Ok(_) => Ok(()),
        Err(error) if error.code == "NOT_FOUND" => Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "active script revision has no retained bundle",
        )),
        Err(error) => Err(error),
    }
}

fn insert_revision(
    tx: &Transaction<'_>,
    bundle: &manifest::ScriptBundle,
    record: &ArtifactRecord,
    created_by: &str,
    revision: i64,
    now: i64,
) -> Result<()> {
    if bundle.script_id != record.metadata["script_id"]
        || record.metadata["revision"] != revision
        || record.content_digest != record.metadata["bundle_sha256"].as_str().unwrap_or("")
        || bundle.controller_effects != artifact_controller_effects(&record.metadata)?
    {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "bundle artifact differs from its revision metadata",
        ));
    }
    tx.execute(
        "INSERT INTO script_revisions(script_id,revision,bundle_ref,bundle_sha256,interpreter_json,validated_at_ms,created_by,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?6)",
        params![
            bundle.script_id,
            revision,
            record.artifact_id,
            record.content_digest,
            model::canonical(&json!(bundle.interpreter))?,
            now,
            created_by,
        ],
    )?;
    Ok(())
}

fn bundle_artifact_id(
    script_id: &str,
    revision: i64,
    caller: &str,
    request_id: &str,
    sha256: &str,
) -> Result<String> {
    let identity = json!({"script_id":script_id,"revision":revision,"caller":caller,"request_id":request_id,"bundle_sha256":sha256});
    Ok(format!(
        "script-{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn verify_revision_files(
    files: &ArtifactFiles,
    record: &ArtifactRecord,
    expected_interpreter: &manifest::InterpreterIdentity,
) -> Result<manifest::ScriptBundle> {
    let bytes = files
        .document_bytes(record)
        .map_err(script_revision_integrity_error)?;
    let bundle = registry::parse_bundle(&bytes, record).map_err(script_revision_integrity_error)?;
    if bundle.controller_effects
        != artifact_controller_effects(&record.metadata).map_err(script_revision_integrity_error)?
    {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "bundle controller effects differ from the retained revision metadata",
        ));
    }
    if bundle.interpreter != *expected_interpreter {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "bundle interpreter differs from the revision's captured identity",
        ));
    }
    let actual = manifest::capture_interpreter(
        &expected_interpreter.canonical_path,
        expected_interpreter.kind,
    )?;
    if actual != *expected_interpreter {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "installed interpreter differs from the captured revision",
        ));
    }
    Ok(bundle)
}

fn artifact_controller_effects(metadata: &Value) -> Result<Vec<manifest::ScriptControllerEffect>> {
    serde_json::from_value(
        metadata
            .get("controller_effects")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| {
        Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "retained script controller effect metadata is invalid",
        )
    })
}

pub(super) fn require_run_scope(
    db: &Connection,
    principal: &Principal,
    attempt_id: &str,
    expected_task_revision: i64,
) -> Result<(Value, Value)> {
    let current = require_script_authority(db, principal)?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let task = tasks::get_task(db, model::text(&attempt, "task_id")?)?;
    if attempt["released_at_ms"] != Value::Null
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
        || task["revision"] != expected_task_revision
        || attempt["task_revision"] != expected_task_revision
    {
        return Err(Error::new(
            "STALE_ATTEMPT",
            "script run requires the exact current unreleased Attempt and Task revision",
        ));
    }
    gm::require_attempt_control(db, &current, &attempt)?;
    Ok((task, attempt))
}

fn require_optional_run_scope(
    db: &Connection,
    principal: &Principal,
    attempt_id: Option<&str>,
    expected_task_revision: Option<i64>,
) -> Result<(Option<Value>, Option<Value>)> {
    match (attempt_id, expected_task_revision) {
        (Some(attempt_id), Some(task_revision)) => {
            let (task, attempt) = require_run_scope(db, principal, attempt_id, task_revision)?;
            Ok((Some(task), Some(attempt)))
        }
        (None, None) => {
            require_script_authority(db, principal)?;
            Ok((None, None))
        }
        _ => Err(Error::invalid(
            "script Task scope must provide both an Attempt ID and Task revision",
        )),
    }
}

fn pending_runs(db: &Connection) -> Result<Vec<PendingRun>> {
    let mut statement = db.prepare(
        "SELECT r.run_id,r.operation_id,o.state,r.work_digest,o.sent_at_ms,r.started_at_ms,r.process_identity_json \
         FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id \
         WHERE r.state IN ('queued','sending','running','reconciling','outcome_unknown') \
         ORDER BY r.created_at_ms,r.run_id LIMIT 64",
    )?;
    statement
        .query_map([], |row| {
            Ok(PendingRun {
                run_id: row.get(0)?,
                operation_id: row.get(1)?,
                operation_state: row.get(2)?,
                work_digest: row.get(3)?,
                sent_at_ms: row.get(4)?,
                started_at_ms: row.get(5)?,
                worker_identity_json: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn script_worker_observation_expired(pending: &PendingRun) -> bool {
    pending.started_at_ms.is_some_and(|started| {
        model::now_ms()
            .is_ok_and(|now| now.saturating_sub(started) > SCRIPT_WORKER_OBSERVATION_TIMEOUT_MS)
    })
}

fn completion_matches_retained_worker(
    pending: &PendingRun,
    completion: &runner::Completion,
) -> bool {
    let Some(identity_json) = pending.worker_identity_json.as_deref() else {
        return completion.state == "failed"
            && completion.started_at_ms.is_none()
            && completion.controller_effects.is_empty()
            && completion.run_id == pending.run_id
            && completion.operation_id == pending.operation_id
            && completion.process["purpose"] == "script";
    };
    serde_json::from_str::<Value>(identity_json).is_ok_and(|identity| {
        identity["run_id"] == pending.run_id
            && identity["operation_id"] == pending.operation_id
            && identity["token"] == completion.token
            && identity["process"] == completion.process
    })
}

fn observe(work: &runner::Work, files: &ArtifactFiles) -> Result<RunnerObservation> {
    // A damaged optional diagnostic must not block a valid terminal receipt.
    let process_control_failure = runner::process_control_failure(work).ok().flatten();
    if let Some(completion) = runner::completion(work, files)? {
        let launch_departed = runner::completion_family_departed(work, &completion)?;
        return Ok(RunnerObservation {
            completion: Some(completion),
            process_control_failure,
            ready: None,
            launch: None,
            launch_departed,
            has_go: runner::has_start_gate(work)?,
        });
    }
    let ready = runner::ready(work)?;
    let launch = runner::launch_record(work)?;
    let launch_departed = match launch.as_ref() {
        Some(launch) => runner::worker_departed(work, launch)?,
        None => false,
    };
    Ok(RunnerObservation {
        completion: None,
        process_control_failure,
        ready,
        launch,
        launch_departed,
        has_go: runner::has_start_gate(work)?,
    })
}

fn record_process_control_failure(
    db: &mut Connection,
    pending: &PendingRun,
    failure: &swarm_scripts::process::ProcessControlFailure,
) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        String,
    )> = tx
        .query_row(
            "SELECT r.operation_id,r.state,r.process_identity_json,o.state,o.result_json,o.method \
             FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id \
             WHERE r.run_id=?1",
            [&pending.run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((
        operation_id,
        run_state,
        process_identity_json,
        operation_state,
        previous_result_json,
        operation_method,
    )) = row
    else {
        tx.commit()?;
        return Ok(false);
    };
    if operation_id != pending.operation_id
        || operation_method != "script.run"
        || !matches!(
            run_state.as_str(),
            "running" | "reconciling" | "outcome_unknown"
        )
        || !matches!(
            operation_state.as_str(),
            "native_accepted" | "outcome_unknown"
        )
    {
        tx.commit()?;
        return Ok(false);
    }
    let (Some(process_identity_json), Some(pending_identity_json)) = (
        process_identity_json,
        pending.worker_identity_json.as_deref(),
    ) else {
        // The receipt is retained and will be reconsidered after the exact
        // ready identity has been durably acknowledged.
        tx.commit()?;
        return Ok(false);
    };
    if process_identity_json != pending_identity_json {
        return Err(Error::new(
            "SCRIPT_WORKER_IDENTITY_DAMAGED",
            "pending and current ScriptRun worker identities differ",
        ));
    }
    let identity: Value = serde_json::from_str(&process_identity_json).map_err(|_| {
        Error::new(
            "SCRIPT_WORKER_IDENTITY_DAMAGED",
            "retained ScriptRun process identity cannot be parsed",
        )
    })?;
    let token = identity["token"].as_str().ok_or_else(|| {
        Error::new(
            "SCRIPT_WORKER_IDENTITY_DAMAGED",
            "retained ScriptRun token is missing",
        )
    })?;
    if identity["run_id"] != pending.run_id
        || identity["operation_id"] != pending.operation_id
        || failure
            .validate_for(
                &pending.run_id,
                &pending.operation_id,
                token,
                &identity["process"],
            )
            .is_err()
    {
        return Err(Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "cancellation diagnostic differs from the Store-retained worker identity",
        ));
    }
    let diagnostic = failure.manager_projection().map_err(|_| {
        Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "cancellation diagnostic cannot be safely projected",
        )
    })?;
    let mut result: Value = previous_result_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| {
            Error::new(
                "SCRIPT_RESULT_DAMAGED",
                "script Operation result is invalid",
            )
        })?
        .unwrap_or_else(|| json!({}));
    let object = result.as_object_mut().ok_or_else(|| {
        Error::new(
            "SCRIPT_RESULT_DAMAGED",
            "script Operation result is not an object",
        )
    })?;
    if let Some(previous) = object.get("process_control_diagnostic") {
        if previous == &diagnostic {
            tx.commit()?;
            return Ok(false);
        }
        return Err(Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_CONFLICT",
            "ScriptRun already retains a different cancellation diagnostic",
        ));
    }
    object.insert("process_control_diagnostic".into(), diagnostic);
    let now = model::now_ms()?;
    let changed = tx.execute(
        "UPDATE operations SET result_json=?2,updated_at_ms=?3 WHERE operation_id=?1 AND state=?4",
        params![
            operation_id,
            model::canonical(&result)?,
            now,
            operation_state
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "ScriptRun Operation changed while recording process control evidence",
        ));
    }
    tx.commit()?;
    Ok(true)
}

fn retain_process_control_diagnostic(
    source_result_json: Option<&str>,
    target: &mut Value,
) -> Result<()> {
    let Some(source_result_json) = source_result_json else {
        return Ok(());
    };
    let source: Value = serde_json::from_str(source_result_json).map_err(|_| {
        Error::new(
            "SCRIPT_RESULT_DAMAGED",
            "script Operation result is invalid",
        )
    })?;
    let Some(diagnostic) = source.get("process_control_diagnostic") else {
        return Ok(());
    };
    let object = diagnostic.as_object().ok_or_else(|| {
        Error::new(
            "SCRIPT_RESULT_DAMAGED",
            "script process-control diagnostic is not an object",
        )
    })?;
    let digest = diagnostic["process_identity_sha256"]
        .as_str()
        .unwrap_or_default();
    if object.len() != 7
        || diagnostic["schema_version"] != 1
        || diagnostic["code"] != "SCRIPT_PROCESS_CANCEL_FAILED"
        || diagnostic["phase"] != "cancel_children"
        || diagnostic["status"] != "control_error_observed"
        || !matches!(
            diagnostic["failure_class"].as_str(),
            Some("os_control" | "process_inventory" | "process_identity" | "other")
        )
        || !matches!(
            diagnostic["process_scope"].as_str(),
            Some("windows_job" | "linux_process_group")
        )
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "SCRIPT_RESULT_DAMAGED",
            "script process-control diagnostic has an invalid shape",
        ));
    }
    let target = target.as_object_mut().ok_or_else(|| {
        Error::new(
            "SCRIPT_RESULT_DAMAGED",
            "script Operation result is not an object",
        )
    })?;
    target.insert("process_control_diagnostic".into(), diagnostic.clone());
    Ok(())
}

fn begin_run(
    db: &mut Connection,
    run_id: &str,
    operation_id: &str,
    app_config: &Config,
) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let state: Option<(String, String)> = tx
        .query_row(
            "SELECT o.state,r.state FROM operations o JOIN script_runs r ON r.operation_id=o.operation_id WHERE r.run_id=?1 AND o.operation_id=?2",
            params![run_id, operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((operation_state, run_state)) = state else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    if operation_state != "queued" || run_state != "queued" {
        tx.commit()?;
        return Ok(false);
    }
    let caller: String = tx.query_row(
        "SELECT caller_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let script_scope: Option<ScriptRunScopeRow> = tx
        .query_row(
            "SELECT r.script_id,r.revision,r.task_id,r.task_revision,o.attempt_id FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    let (script_id, revision, task_id, task_revision, attempt_id) =
        script_scope.ok_or_else(|| Error::new("NOT_FOUND", "script run is not registered"))?;
    let preflight = (|| -> Result<()> {
        if caller == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
            require_script_trigger_start(
                &tx,
                ScriptTriggerStart {
                    operation_id,
                    script_id: &script_id,
                    script_revision: revision,
                    task_id: task_id.as_deref(),
                    task_revision,
                    attempt_id: attempt_id.as_deref(),
                    app_config,
                },
            )?;
        } else {
            let actor = registered_actor(&tx, &caller)?;
            let current = require_script_scope(&tx, &actor, &script_id)?;
            let (task, attempt) =
                require_optional_run_scope(&tx, &current, attempt_id.as_deref(), task_revision)?;
            let scope_matches = match (task.as_ref(), attempt.as_ref()) {
                (Some(task), Some(attempt)) => {
                    task_id.as_deref() == task["task_id"].as_str()
                        && task_revision == task["revision"].as_i64()
                        && attempt_id.as_deref() == attempt["attempt_id"].as_str()
                }
                (None, None) => {
                    task_id.is_none() && task_revision.is_none() && attempt_id.is_none()
                }
                _ => false,
            };
            if !scope_matches {
                return Err(Error::new(
                    "SCRIPT_SCOPE_CHANGED",
                    "queued script Task/Attempt scope changed",
                ));
            }
        }
        let (active, _) = script_head(&tx, &script_id)?;
        if active != Some(revision) {
            return Err(Error::new(
                "SCRIPT_REVISION_NOT_ACTIVE",
                "queued script revision is no longer active",
            ));
        }
        Ok(())
    })();
    if let Err(error) = preflight {
        fail_before_start_tx(&tx, run_id, &error.code, model::now_ms()?)?;
        tx.commit()?;
        return Ok(false);
    }
    let now = model::now_ms()?;
    tx.execute(
        "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
        params![operation_id, now],
    )?;
    tx.execute(
        "UPDATE script_runs SET state='sending' WHERE run_id=?1 AND state='queued'",
        [run_id],
    )?;
    tx.commit()?;
    Ok(true)
}

fn registered_actor(db: &Connection, caller: &str) -> Result<Principal> {
    let registration = meta(db, &format!("client:{caller}"))?.ok_or_else(|| {
        Error::new(
            "UNAUTHORIZED",
            "script Operation caller is no longer registered",
        )
    })?;
    let role: Role = serde_json::from_value(registration["role"].clone())?;
    Ok(Principal {
        client_id: caller.to_owned(),
        role,
        link_id: String::new(),
    })
}

fn require_script_trigger_start(db: &Connection, start: ScriptTriggerStart<'_>) -> Result<()> {
    let ScriptTriggerStart {
        operation_id,
        script_id,
        script_revision,
        task_id,
        task_revision,
        attempt_id,
        app_config,
    } = start;
    let link = authorization::operation_link(db, operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "queued ScriptRun has no validated trigger attribution",
        )
    })?;
    let context =
        automation_dispatch::script_trigger_authority::ScriptRunConsumerContext::from_cause(
            &link.cause,
        )?;
    if link.operation_id != operation_id
        || link.action != "script.run"
        || link.technical_requester_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.effective_manager_id != context.owner_manager_id()
        || link.project_id != context.project_id()
        || link.automation_id != context.automation_id()
        || link.automation_revision != context.automation_revision()
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "queued ScriptRun link differs from its typed owner context",
        ));
    }
    let entry = context.require_current_entry(db)?;
    let grant = ScriptRunTriggerGrant {
        entry,
        cause: link.cause,
        context,
    };
    let (task, attempt) =
        validate_script_trigger_current(db, app_config, &grant, script_id, script_revision)?;
    if task.as_ref().and_then(|value| value["task_id"].as_str()) != task_id
        || task.as_ref().and_then(|value| value["revision"].as_i64()) != task_revision
        || attempt
            .as_ref()
            .and_then(|value| value["attempt_id"].as_str())
            != attempt_id
    {
        return Err(Error::new(
            "SCRIPT_SCOPE_CHANGED",
            "queued ScriptRun Operation differs from its current owner-scoped source",
        ));
    }
    Ok(())
}

fn acknowledge_worker(
    db: &mut Connection,
    run_id: &str,
    identity: &Value,
    app_config: &Config,
) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, Option<String>, String)> = tx
        .query_row(
            "SELECT o.operation_id,o.state,r.process_identity_json,o.caller_id FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, operation_state, prior_identity, caller)) = row else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    if operation_state != "sending" {
        tx.commit()?;
        return Ok(false);
    }
    if identity["run_id"] != run_id
        || identity["operation_id"] != operation_id
        || identity["token"].as_str().is_none_or(str::is_empty)
        || identity["process"]["purpose"] != "script"
    {
        return Err(Error::new(
            "SCRIPT_WORKER_DAMAGED",
            "worker readiness identity differs from its Operation",
        ));
    }
    if let Some(prior) = prior_identity {
        if serde_json::from_str::<Value>(&prior)? != *identity {
            return Err(Error::new(
                "SCRIPT_WORKER_DAMAGED",
                "script worker identity cannot be replaced",
            ));
        }
        tx.commit()?;
        return Ok(true);
    }
    let scope: (Option<String>, Option<i64>, Option<String>) = tx.query_row(
        "SELECT task_id,task_revision,attempt_id FROM script_runs WHERE run_id=?1",
        [run_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if caller == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        let script_scope: (String, i64) = tx.query_row(
            "SELECT script_id,revision FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if let Err(error) = require_script_trigger_start(
            &tx,
            ScriptTriggerStart {
                operation_id: &operation_id,
                script_id: &script_scope.0,
                script_revision: script_scope.1,
                task_id: scope.0.as_deref(),
                task_revision: scope.1,
                attempt_id: scope.2.as_deref(),
                app_config,
            },
        ) {
            fail_before_start_tx(&tx, run_id, &error.code, model::now_ms()?)?;
            tx.commit()?;
            return Ok(false);
        }
    } else {
        let actor = registered_actor(&tx, &caller)?;
        let script_id: String = tx.query_row(
            "SELECT script_id FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        let current = require_script_scope(&tx, &actor, &script_id)?;
        let (task, attempt) =
            require_optional_run_scope(&tx, &current, scope.2.as_deref(), scope.1)?;
        if task.as_ref().and_then(|task| task["task_id"].as_str()) != scope.0.as_deref()
            || task.as_ref().and_then(|task| task["revision"].as_i64()) != scope.1
            || attempt
                .as_ref()
                .and_then(|attempt| attempt["attempt_id"].as_str())
                != scope.2.as_deref()
        {
            return Err(Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "script Task/Attempt changed before the start gate",
            ));
        }
    }
    let now = model::now_ms()?;
    let run_changed = tx.execute(
        "UPDATE script_runs SET state='running',started_at_ms=?2,process_identity_json=?3 WHERE run_id=?1 AND state='sending'",
        params![run_id, now, model::canonical(identity)?],
    )?;
    let operation_changed = tx.execute(
        "UPDATE operations SET state='native_accepted',updated_at_ms=?2 WHERE operation_id=?1 AND state='sending'",
        params![operation_id, now],
    )?;
    if run_changed != 1 || operation_changed != 1 {
        return Err(Error::conflict(
            "ScriptRun changed before its start fact could be committed",
        ));
    }
    let occurrence_id = format!("operation:{operation_id}:script_run_started");
    let started_event = json!({
        "schema_version":1,
        "phase":"script_run_started",
        "occurrence_id":occurrence_id,
        "operation_id":operation_id,
        "run_id":run_id,
    });
    record_script_lifecycle_event_tx(
        &tx,
        &format!("operation:{operation_id}:script_run_started"),
        &operation_id,
        "script.started",
        &started_event,
        now,
    )?;
    tx.commit()?;
    Ok(true)
}

/// Record the exact queued ScriptRun cancellation in the transaction that
/// cancels its Operation. The general Operation cancellation fact remains the
/// source-wide view; this alias adds the retained run identity without copying
/// cancellation text or granting a second execution path.
pub(super) fn record_operation_cancelled_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    now: i64,
) -> Result<()> {
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT r.run_id,r.state,o.state FROM script_runs r \
             JOIN operations o ON o.operation_id=r.operation_id \
             WHERE r.operation_id=?1 AND o.method='script.run'",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((run_id, run_state, operation_state)) = row else {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "cancelled script.run Operation has no exact retained ScriptRun",
        ));
    };
    if operation_state != "cancelled" || run_state != "queued" {
        return Err(Error::conflict(
            "ScriptRun cancellation no longer matches the queued Operation",
        ));
    }
    let changed = tx.execute(
        "UPDATE script_runs SET state='cancelled',finished_at_ms=?3 \
         WHERE operation_id=?1 AND run_id=?2 AND state='queued'",
        params![operation_id, run_id, now],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "ScriptRun changed before its cancellation fact could be committed",
        ));
    }
    let occurrence_id = format!("operation:{operation_id}:operation_cancelled");
    let event = json!({
        "schema_version":1,
        "phase":"operation_cancelled",
        "occurrence_id":occurrence_id,
        "status":"cancelled",
        "error_code":"OPERATION_CANCELLED",
        "operation_id":operation_id,
        "run_id":run_id,
    });
    record_script_lifecycle_event_tx(
        tx,
        &format!("operation:{operation_id}:operation_cancelled"),
        operation_id,
        "script.cancelled",
        &event,
        now,
    )?;
    Ok(())
}

fn record_script_lifecycle_event_tx(
    tx: &Transaction<'_>,
    event_key: &str,
    operation_id: &str,
    event_kind: &str,
    payload: &Value,
    now: i64,
) -> Result<()> {
    let payload_json = model::canonical(payload)?;
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,?3,?4,?5)",
        params![event_key, operation_id, event_kind, payload_json, now],
    )?;
    if inserted == 1 {
        return Ok(());
    }
    let retained: Option<(Option<String>, String, String, i64)> = tx
        .query_row(
            "SELECT operation_id,kind,payload_json,recorded_at_ms FROM observations \
             WHERE source_stream_id='controller:scripts' AND source_event_key=?1",
            [event_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if !retained.is_some_and(
        |(retained_operation, retained_kind, retained_payload, recorded_at)| {
            retained_operation.as_deref() == Some(operation_id)
                && retained_kind == event_kind
                && retained_payload == payload_json
                && recorded_at == now
        },
    ) {
        return Err(Error::new(
            "SCRIPT_EVENT_ID_CONFLICT",
            "ScriptRun lifecycle identity already names another immutable observation",
        ));
    }
    Ok(())
}

fn fail_before_start(db: &mut Connection, run_id: &str, code: &str) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    fail_before_start_tx(&tx, run_id, code, model::now_ms()?)?;
    tx.commit()?;
    Ok(())
}

fn fail_before_start_tx(tx: &Transaction<'_>, run_id: &str, code: &str, now: i64) -> Result<()> {
    let operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(operation_id) = operation_id else {
        return Ok(());
    };
    let op_state: String = tx.query_row(
        "SELECT state FROM operations WHERE operation_id=?1",
        [&operation_id],
        |row| row.get(0),
    )?;
    if matches!(op_state.as_str(), "settled" | "rejected" | "cancelled") {
        return Ok(());
    }
    if matches!(op_state.as_str(), "native_accepted" | "outcome_unknown") {
        return mark_unknown_tx(tx, run_id, code, now);
    }
    let result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"failed",
        "state":"failed",
        "error_code":code,
        "execution_started":false,
        "controller_effects":[],
    });
    tx.execute(
        "UPDATE script_runs SET state='failed',finished_at_ms=?2 WHERE run_id=?1 AND state IN ('queued','sending','reconciling')",
        params![run_id, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('queued','sending')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.failed',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn settle_incomplete(
    db: &mut Connection,
    run_id: &str,
    code: &str,
    execution_may_have_started: bool,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, String, Option<String>)> = tx
        .query_row(
            "SELECT r.operation_id,r.state,o.state,o.result_json FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, run_state, operation_state, previous_result_json)) = row else {
        tx.commit()?;
        return Ok(());
    };
    if matches!(
        run_state.as_str(),
        "completed" | "failed" | "incomplete" | "cancelled"
    ) {
        tx.commit()?;
        return Ok(());
    }
    let now = model::now_ms()?;
    let mut result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"incomplete",
        "state":"incomplete",
        "diagnostic_code":code,
        "execution_may_have_started":execution_may_have_started,
        "result_read_method":"operation.get",
        "controller_effects":[],
    });
    retain_process_control_diagnostic(previous_result_json.as_deref(), &mut result)?;
    tx.execute(
        "UPDATE script_runs SET state='incomplete',finished_at_ms=?2 WHERE run_id=?1 AND state IN ('queued','sending','running','reconciling','outcome_unknown')",
        params![run_id, now],
    )?;
    if !matches!(
        operation_state.as_str(),
        "settled" | "rejected" | "cancelled"
    ) {
        tx.execute(
            "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('queued','sending','native_accepted','outcome_unknown')",
            params![operation_id, model::canonical(&result)?, now],
        )?;
    }
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.incomplete',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

fn mark_unknown(db: &mut Connection, run_id: &str, code: &str) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    mark_unknown_tx(&tx, run_id, code, model::now_ms()?)?;
    tx.commit()?;
    Ok(())
}

fn mark_unknown_tx(tx: &Transaction<'_>, run_id: &str, code: &str, now: i64) -> Result<()> {
    let operation: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT r.operation_id,o.state,o.result_json FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((operation_id, operation_state, previous_result)) = operation else {
        return Ok(());
    };
    if operation_state == "outcome_unknown"
        && previous_result
            .as_deref()
            .and_then(|result| serde_json::from_str::<Value>(result).ok())
            .is_some_and(|result| {
                result["state"] == "outcome_unknown" && result["diagnostic_code"] == code
            })
    {
        return Ok(());
    }
    let mut result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"unknown",
        "state":"outcome_unknown",
        "diagnostic_code":code,
        "execution_may_have_started":true,
        "controller_effects":[],
    });
    retain_process_control_diagnostic(previous_result.as_deref(), &mut result)?;
    tx.execute(
        "UPDATE script_runs SET state='outcome_unknown' WHERE run_id=?1 AND state IN ('sending','running','reconciling','outcome_unknown')",
        [run_id],
    )?;
    tx.execute(
        "UPDATE operations SET state='outcome_unknown',result_json=?2,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn completion_failure_is_terminal(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "SCRIPT_COMPLETION_DAMAGED"
            | "SCRIPT_RUN_DAMAGED"
            | "SCRIPT_EFFECT_DAMAGED"
            | "SCRIPT_REGISTRY_DAMAGED"
            | "ARTIFACT_DAMAGED"
            | "NOT_FOUND"
            | "CONFLICT"
    )
}

fn finish(
    db: &mut Connection,
    run_id: &str,
    completion: runner::Completion,
    config: &Config,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<ScriptCompletionRow> = tx
        .query_row(
            "SELECT r.operation_id,r.script_id,r.revision,r.task_id,r.task_revision,r.attempt_id,r.state,o.state,o.method,o.task_id,o.attempt_id,o.caller_id,r.spec_json,r.process_identity_json FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                ))
            },
        )
        .optional()?;
    let Some((
        operation_id,
        script_id,
        revision,
        task_id,
        task_revision,
        attempt_id,
        run_state,
        operation_state,
        operation_method,
        operation_task_id,
        operation_attempt_id,
        caller_id,
        spec_json,
        process_identity_json,
    )) = row
    else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    let has_task_scope = match (&task_id, task_revision, &attempt_id) {
        (Some(task_id), Some(task_revision), Some(attempt_id))
            if !task_id.is_empty() && task_revision > 0 && !attempt_id.is_empty() =>
        {
            true
        }
        (None, None, None) => false,
        _ => {
            return Err(Error::new(
                "SCRIPT_RUN_DAMAGED",
                "script run Task/Attempt scope is partially populated",
            ));
        }
    };
    if operation_method != "script.run"
        || operation_task_id != task_id
        || operation_attempt_id != attempt_id
    {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "script run Operation no longer identifies its exact Task and Attempt",
        ));
    }
    if completion.run_id != run_id || completion.operation_id != operation_id {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script completion identifies another Operation",
        ));
    }
    if run_state == "cancelled" && operation_state == "cancelled" {
        tx.commit()?;
        return Ok(());
    }
    if completion.process["purpose"] != "script" {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script completion process identity has another purpose",
        ));
    }
    match process_identity_json
        .as_deref()
        .map(|identity| {
            serde_json::from_str::<Value>(identity).map_err(|_| {
                Error::new(
                    "SCRIPT_WORKER_IDENTITY_DAMAGED",
                    "retained ScriptRun process identity cannot be parsed",
                )
            })
        })
        .transpose()?
    {
        Some(identity)
            if identity["run_id"] == run_id
                && identity["operation_id"] == operation_id
                && identity["token"] == completion.token
                && identity["process"] == completion.process => {}
        Some(_) => {
            return Err(Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "script completion differs from its Store-retained worker identity",
            ));
        }
        None if completion.state == "failed"
            && completion.started_at_ms.is_none()
            && completion.controller_effects.is_empty() => {}
        None => {
            return Err(Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "started ScriptRun completion has no Store-retained worker identity",
            ));
        }
    }
    if matches!(run_state.as_str(), "completed" | "failed" | "incomplete") {
        let result_ref: Option<String> = tx.query_row(
            "SELECT result_ref FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        if result_ref.as_deref() == Some(completion.result.artifact_id.as_str()) {
            tx.commit()?;
            return Ok(());
        }
        return Err(Error::conflict("completed script run cannot be replaced"));
    }
    if !matches!(
        operation_state.as_str(),
        "sending" | "native_accepted" | "outcome_unknown"
    ) {
        return Err(Error::conflict(
            "script completion arrived outside its retained run",
        ));
    }
    let revision_info = revision_snapshot(&tx, &script_id, revision).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "retained script revision metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    let work_record = registry::bundle_record(&tx, &script_id, revision).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "retained script bundle metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    if work_record.artifact_id != revision_info.record.artifact_id {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "completion revision no longer names its retained bundle",
        ));
    }
    let now = model::now_ms()?;
    let spec: Value = serde_json::from_str(&spec_json).map_err(|_| {
        Error::new(
            "SCRIPT_RUN_DAMAGED",
            "retained script invocation grant cannot be parsed",
        )
    })?;
    let declared_effects: Vec<manifest::ScriptControllerEffect> = serde_json::from_value(
        spec.get("capabilities")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| {
        Error::new(
            "SCRIPT_RUN_DAMAGED",
            "retained script invocation grant is invalid",
        )
    })?;
    let taskless_event_run = !has_task_scope
        && caller_id == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        && authorization::operation_link(&tx, &operation_id)?.is_some_and(|link| {
            link.action == "script.run" && link.cause["kind"] == "system_event"
        });
    let bundle_effects = artifact_controller_effects(&work_record.metadata)?;
    let expected_effects = if has_task_scope {
        bundle_effects
            .iter()
            .copied()
            .filter(|effect| *effect == manifest::ScriptControllerEffect::TaskOwnerMessage)
            .collect::<Vec<_>>()
    } else if taskless_event_run {
        bundle_effects
            .iter()
            .copied()
            .filter(|effect| {
                matches!(
                    effect,
                    manifest::ScriptControllerEffect::ManagerNotification
                        | manifest::ScriptControllerEffect::TaskCreate
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if expected_effects != declared_effects {
        return Err(Error::new(
            "SCRIPT_RUN_DAMAGED",
            "invocation effect grant differs from its immutable revision or Task scope",
        ));
    }
    if completion.controller_effects.len() > manifest::MAX_CONTROLLER_EFFECTS
        || completion
            .controller_effects
            .iter()
            .any(|effect| !declared_effects.contains(&effect.effect) || effect.validate().is_err())
        || (completion.state != "completed" && !completion.controller_effects.is_empty())
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "completion requests effects outside its retained invocation grant",
        ));
    }
    if !completion.controller_effects.is_empty()
        && (spec["invocation"]["operation_id"] != operation_id
            || spec["invocation"]["run_id"] != run_id
            || spec["invocation"]["script_id"] != script_id
            || spec["invocation"]["script_revision"] != revision
            || spec["invocation"]["task_id"].as_str() != task_id.as_deref()
            || spec["invocation"]["task_revision"].as_i64() != task_revision
            || spec["invocation"]["attempt_id"].as_str() != attempt_id.as_deref()
            || spec["invocation"]["cause"]
                != json!({"kind":"script.run","operation_id":operation_id,"run_id":run_id}))
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "effect invocation does not match its exact retained Manager/cause scope",
        ));
    }
    let effective_manager_id = if !completion.controller_effects.is_empty()
        && caller_id == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    {
        authorization::operation_link(&tx, &operation_id)?
            .filter(|link| link.action == "script.run")
            .map(|link| link.effective_manager_id)
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "automatic ScriptRun effect has no validated owner attribution",
                )
            })?
    } else if !completion.controller_effects.is_empty() {
        registered_actor(&tx, &caller_id)?.client_id
    } else {
        caller_id.clone()
    };
    if !completion.controller_effects.is_empty()
        && spec["invocation"]["effective_manager_id"] != effective_manager_id
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "effect invocation Manager differs from the retained effective Manager",
        ));
    }
    let artifacts = [&completion.result, &completion.stdout, &completion.stderr];
    for artifact in artifacts {
        register_output_artifact(&tx, artifact, now)?;
    }
    let controller_effects = if completion.state == "completed" {
        completion
            .controller_effects
            .iter()
            .map(|effect| {
                apply_controller_effect(
                    &tx,
                    &caller_id,
                    &effective_manager_id,
                    &operation_id,
                    run_id,
                    &script_id,
                    revision,
                    task_id.as_deref(),
                    task_revision,
                    attempt_id.as_deref(),
                    effect,
                    config,
                    now,
                )
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let effects_complete = controller_effects
        .iter()
        .all(|effect| effect["status"] == "applied");
    let outcome = match completion.state.as_str() {
        "completed" if effects_complete => "applied",
        "completed" => "effects_incomplete",
        "incomplete" => "incomplete",
        _ => "failed",
    };
    let previous_result_json: Option<String> = tx.query_row(
        "SELECT result_json FROM operations WHERE operation_id=?1",
        [&operation_id],
        |row| row.get(0),
    )?;
    let mut result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":outcome,
        "state":completion.state,
        "started_at_ms":completion.started_at_ms,
        "exit_code":completion.exit_code,
        "result_ref":completion.result.artifact_id,
        "stdout_ref":completion.stdout.artifact_id,
        "stderr_ref":completion.stderr.artifact_id,
        "result":completion.result_value,
        "error_code":completion.error_code,
        "task_revision":task_revision,
        "controller_effects":controller_effects,
    });
    retain_process_control_diagnostic(previous_result_json.as_deref(), &mut result)?;
    tx.execute(
        "UPDATE script_runs SET state=?2,result_ref=?3,stdout_ref=?4,stderr_ref=?5,exit_code=?6,started_at_ms=COALESCE(?7,started_at_ms),finished_at_ms=?8 WHERE run_id=?1",
        params![run_id, completion.state, completion.result.artifact_id, completion.stdout.artifact_id, completion.stderr.artifact_id, completion.exit_code, completion.started_at_ms, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.completed',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_controller_effect(
    tx: &Transaction<'_>,
    caller_id: &str,
    effective_manager_id: &str,
    operation_id: &str,
    run_id: &str,
    script_id: &str,
    script_revision: i64,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
    effect: &protocol::ScriptEffectRequest,
    config: &Config,
    now: i64,
) -> Result<Value> {
    effect.validate()?;
    let request_id = script_effect_request_id(operation_id, run_id, effect)?;
    let automatic_admission = if caller_id == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        match ScriptEffectAdmission::from_run(ScriptEffectRunInput {
            db: tx,
            config,
            caller_id,
            parent_operation_id: operation_id,
            run_id,
            script_id,
            script_revision,
            task_id,
            task_revision,
            attempt_id,
            request_id: &request_id,
            effect,
            expected_manager_id: effective_manager_id,
        }) {
            Ok(admission) => Some(admission),
            Err(error) if error.code != "STORE_ERROR" => {
                return Ok(controller_effect_rejection(
                    effect,
                    effective_manager_id,
                    operation_id,
                    run_id,
                    script_id,
                    script_revision,
                    effect.effect,
                    task_id,
                    task_revision,
                    attempt_id,
                    &error,
                ));
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    let direct_actor = if automatic_admission.is_none() {
        match (|| -> Result<(Principal, Value)> {
            if effect.effect != manifest::ScriptControllerEffect::TaskOwnerMessage {
                return Err(Error::new(
                    "FORBIDDEN",
                    "Manager notification is limited to an admitted taskless event ScriptRun",
                ));
            }
            let (Some(task_id), Some(task_revision), Some(attempt_id)) =
                (task_id, task_revision, attempt_id)
            else {
                return Err(Error::new(
                    "SCRIPT_SCOPE_CHANGED",
                    "Task-owner effect requires the exact Task and Attempt scope",
                ));
            };
            let actor = registered_actor(tx, caller_id)?;
            if actor.role != Role::Manager {
                return Err(Error::new(
                    "FORBIDDEN",
                    "script controller effects require the admitting Manager",
                ));
            }
            let current = require_script_authority(tx, &actor)?;
            let (task, attempt) = require_run_scope(tx, &current, attempt_id, task_revision)?;
            if model::text(&task, "task_id")? != task_id
                || task["revision"] != task_revision
                || attempt["attempt_id"] != attempt_id
            {
                return Err(Error::new(
                    "SCRIPT_SCOPE_CHANGED",
                    "script invocation no longer identifies the same Task and Attempt",
                ));
            }
            let owner_id: String = tx.query_row(
                "SELECT owner_id FROM scripts WHERE script_id=?1",
                [script_id],
                |row| row.get(0),
            )?;
            if owner_id != current.client_id {
                return Err(Error::new(
                    "FORBIDDEN",
                    "only the Manager who owns this script revision may use its effect grant",
                ));
            }
            let (active_revision, _) = script_head(tx, script_id)?;
            if active_revision != Some(script_revision) {
                return Err(Error::new(
                    "SCRIPT_REVISION_NOT_ACTIVE",
                    "script effect requires the same revision to remain active",
                ));
            }
            Ok((actor, attempt))
        })() {
            Ok(actor) => Some(actor),
            Err(error) if error.code != "STORE_ERROR" => {
                return Ok(controller_effect_rejection(
                    effect,
                    effective_manager_id,
                    operation_id,
                    run_id,
                    script_id,
                    script_revision,
                    effect.effect,
                    task_id,
                    task_revision,
                    attempt_id,
                    &error,
                ));
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    let recipient = automatic_admission
        .as_ref()
        .and_then(|admission| admission.recipient.clone())
        .or_else(|| {
            direct_actor
                .as_ref()
                .and_then(|(_, attempt)| attempt["owner_id"].as_str().map(str::to_owned))
        });
    let request_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE caller_id=?1 AND client_request_id=?3) \
         OR EXISTS(SELECT 1 FROM operations WHERE caller_id=?2 AND client_request_id=?3)",
        params![caller_id, effective_manager_id, request_id],
        |row| row.get(0),
    )?;
    if request_exists {
        return Ok(controller_effect_rejection(
            effect,
            effective_manager_id,
            operation_id,
            run_id,
            script_id,
            script_revision,
            effect.effect,
            task_id,
            task_revision,
            attempt_id,
            &Error::new(
                "SCRIPT_EFFECT_REQUEST_CONFLICT",
                "script effect request identity is already used by an existing Operation",
            ),
        ));
    }
    let action_method = match effect.effect {
        manifest::ScriptControllerEffect::TaskCreate => "task.create",
        manifest::ScriptControllerEffect::TaskOwnerMessage
        | manifest::ScriptControllerEffect::ManagerNotification => "message.send",
    };
    let request = if effect.effect == manifest::ScriptControllerEffect::TaskCreate {
        let admission = automatic_admission.as_ref().ok_or_else(|| {
            Error::new(
                "SCRIPT_EFFECT_DAMAGED",
                "task_create lost its taskless event admission",
            )
        })?;
        json!({
            "client_request_id":request_id,
            "project_id":admission.project_id(),
            "spec":effect.spec,
        })
    } else {
        let recipient = recipient.as_deref().ok_or_else(|| {
            Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "message effect has no Store-derived recipient",
            )
        })?;
        json!({
            "client_request_id":request_id,
            "recipient":recipient,
            "text":effect.text,
        })
    };
    let action = match automatic_admission.as_ref() {
        Some(admission) => super::mutate_script_effect_in_transaction(
            tx,
            admission,
            action_method,
            &request,
            config,
            now,
        ),
        None => {
            let (actor, _) = direct_actor.as_ref().ok_or_else(|| {
                Error::new(
                    "SCRIPT_EFFECT_DAMAGED",
                    "direct script effect lost its authenticated Manager",
                )
            })?;
            super::mutate_in_transaction(tx, actor, action_method, &request, config, now)
        }
    };
    let (action_value, action_error) = match action {
        Ok(Ok(value)) => (Some(value), None),
        Ok(Err(error)) => (None, Some(error)),
        Err(error) if error.code != "STORE_ERROR" => (None, Some(error)),
        Err(error) => return Err(error),
    };
    let action_caller_id = automatic_admission
        .as_ref()
        .map(ScriptEffectAdmission::technical_requester_id)
        .unwrap_or(caller_id);
    let action_operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2 AND method=?3",
            params![action_caller_id, request_id, action_method],
            |row| row.get(0),
        )
        .optional()?;
    let cause = script_effect_cause(
        effective_manager_id,
        operation_id,
        run_id,
        script_id,
        script_revision,
        effect.effect,
        task_id,
        task_revision,
        attempt_id,
    );
    if let Some(effect_operation_id) = action_operation_id.as_deref() {
        retain_script_effect_link(
            tx,
            ScriptEffectLinkInput {
                caller_id: action_caller_id,
                effective_manager_id,
                request_id: &request_id,
                effect_operation_id,
                request: &request,
                cause: &cause,
                effect: effect.effect,
                automatic_admission: automatic_admission.as_ref(),
                now_ms: now,
            },
        )?;
    }
    match (action_value, action_error) {
        (Some(value), None) => {
            if effect.effect == manifest::ScriptControllerEffect::TaskCreate {
                Ok(json!({
                    "effect":effect.effect,
                    "status":"applied",
                    "operation_id":action_operation_id,
                    "task_id":value["task_id"],
                    "task_revision":value["revision"],
                    "created":value["created"],
                    "cause":cause,
                }))
            } else {
                let recipient = recipient.as_deref().ok_or_else(|| {
                    Error::new(
                        "SCRIPT_EFFECT_DAMAGED",
                        "message effect lost its Store-derived recipient",
                    )
                })?;
                Ok(json!({
                    "effect":effect.effect,
                    "status":"applied",
                    "operation_id":action_operation_id,
                    "message_id":value["message_id"],
                    "recipient":recipient,
                    "cause":cause,
                }))
            }
        }
        (_, Some(error)) => Ok(json!({
            "effect":effect.effect,
            "status":"rejected",
            "operation_id":action_operation_id,
            "error":{"code":error.code,"message":error.message},
            "cause":cause,
        })),
        _ => Err(Error::new(
            "SCRIPT_EFFECT_DAMAGED",
            "script effect mutation returned neither a result nor a rejection",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn controller_effect_rejection(
    effect: &protocol::ScriptEffectRequest,
    effective_manager_id: &str,
    operation_id: &str,
    run_id: &str,
    script_id: &str,
    script_revision: i64,
    effect_kind: manifest::ScriptControllerEffect,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
    error: &Error,
) -> Value {
    json!({
        "effect":effect.effect,
        "status":"rejected",
        "operation_id":Value::Null,
        "error":{"code":error.code,"message":error.message},
        "cause":script_effect_cause(effective_manager_id,operation_id,run_id,script_id,script_revision,effect_kind,task_id,task_revision,attempt_id),
    })
}

#[allow(clippy::too_many_arguments)]
fn script_effect_cause(
    manager_id: &str,
    operation_id: &str,
    run_id: &str,
    script_id: &str,
    script_revision: i64,
    effect: manifest::ScriptControllerEffect,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
) -> Value {
    let mut cause = json!({
        "kind":"script_invocation",
        "id":operation_id,
        "script_run_operation_id":operation_id,
        "script_run_id":run_id,
        "identity":{
            "script_id":script_id,
            "script_revision":script_revision
        },
        "effective_manager_id":manager_id,
    });
    if effect == manifest::ScriptControllerEffect::TaskOwnerMessage {
        cause["identity"]["task_id"] = json!(task_id);
        cause["identity"]["task_revision"] = json!(task_revision);
        cause["identity"]["attempt_id"] = json!(attempt_id);
    }
    cause
}

fn script_effect_request_id(
    operation_id: &str,
    run_id: &str,
    effect: &protocol::ScriptEffectRequest,
) -> Result<String> {
    let identity = json!(["script-effect-v1", operation_id, run_id, effect.effect]);
    Ok(format!(
        "script-effect-{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn retain_script_effect_link(tx: &Transaction<'_>, input: ScriptEffectLinkInput<'_>) -> Result<()> {
    let ScriptEffectLinkInput {
        caller_id,
        effective_manager_id,
        request_id,
        effect_operation_id,
        request,
        cause,
        effect,
        automatic_admission,
        now_ms,
    } = input;
    let row: Option<ScriptEffectOperationRow> = tx
        .query_row(
            "SELECT method,caller_id,original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
            [effect_operation_id],
            |row| {
                Ok(ScriptEffectOperationRow {
                    method: row.get(0)?,
                    caller_id: row.get(1)?,
                    original_request_json: row.get(2)?,
                    effective_request_json: row.get(3)?,
                })
            },
        )
        .optional()?;
    let Some(ScriptEffectOperationRow {
        method,
        caller_id: caller,
        original_request_json: original,
        effective_request_json: effective,
    }) = row
    else {
        return Err(Error::new(
            "SCRIPT_EFFECT_DAMAGED",
            "effect Operation disappeared before its script link was retained",
        ));
    };
    let action_method = match effect {
        manifest::ScriptControllerEffect::TaskCreate => "task.create",
        manifest::ScriptControllerEffect::TaskOwnerMessage
        | manifest::ScriptControllerEffect::ManagerNotification => "message.send",
    };
    if method != action_method
        || caller != caller_id
        || model::canonical(&serde_json::from_str::<Value>(&original)?)?
            != model::canonical(request)?
    {
        return Err(Error::new(
            "SCRIPT_EFFECT_DAMAGED",
            "effect Operation differs from the exact Manager request",
        ));
    }
    let mut effective: Value = serde_json::from_str(&effective)?;
    let link = json!({
        "schema_version":1,
        "operation_id":effect_operation_id,
        "technical_requester_id":caller_id,
        "effective_manager_id":effective_manager_id,
        "action":action_method,
        "grant":effect,
        "cause":cause,
    });
    if let Some(admission) = automatic_admission {
        if caller_id != admission.technical_requester_id()
            || effective_manager_id != admission.effective_manager_id()
            || effect != admission.effect
        {
            return Err(Error::new(
                "SCRIPT_EFFECT_DAMAGED",
                "automatic effect linkage differs from its typed caller identity",
            ));
        }
        let automation_link =
            admission.retain_operation_link(tx, effect_operation_id, request, now_ms)?;
        if effective
            .get("automation_on_behalf")
            .is_some_and(|prior| prior != &automation_link)
        {
            return Err(Error::new(
                "SCRIPT_EFFECT_DAMAGED",
                "effect Operation already has different on-behalf attribution",
            ));
        }
        effective["automation_on_behalf"] = automation_link;
    }
    if let Some(prior) = effective.get("script_invocation") {
        if prior != &link {
            return Err(Error::new(
                "SCRIPT_EFFECT_DAMAGED",
                "effect Operation is already linked to another invocation",
            ));
        }
    } else {
        effective["script_invocation"] = link.clone();
    }
    // Automatic effects add the automation link even if a compatible exact
    // invocation link was retained by an earlier recovery step. Persist both
    // fields together so readers never observe half of the child provenance.
    if automatic_admission.is_some() || effective.get("script_invocation") == Some(&link) {
        tx.execute(
            "UPDATE operations SET effective_request_json=?2 WHERE operation_id=?1 AND caller_id=?3 AND client_request_id=?4",
            params![effect_operation_id, model::canonical(&effective)?, caller_id, request_id],
        )?;
    }
    Ok(())
}

fn register_output_artifact(
    tx: &Transaction<'_>,
    artifact: &ArtifactRecord,
    now: i64,
) -> Result<()> {
    let limit = match artifact.kind.as_str() {
        "script_result" => manifest::MAX_RESULT_BYTES as u64 + 16 * 1024,
        "script_output" if artifact.metadata["stream"] == "stdout" => {
            manifest::MAX_RESULT_BYTES as u64
        }
        "script_output" if artifact.metadata["stream"] == "stderr" => {
            manifest::MAX_STDERR_BYTES as u64
        }
        _ => {
            return Err(Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "script completion names an unsupported artifact",
            ));
        }
    };
    if artifact.byte_length > limit
        || artifact.content_digest.len() != 64
        || !artifact
            .content_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || artifact.relative_path != format!("artifacts/{}.bin", artifact.artifact_id)
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script artifact exceeds its recorded bound",
        ));
    }
    let byte_length = i64::try_from(artifact.byte_length)
        .map_err(|_| Error::invalid("script artifact length exceeds SQLite range"))?;
    tx.execute(
        "INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,metadata_json,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![artifact.artifact_id, artifact.relative_path, artifact.kind, byte_length, artifact.content_digest, model::canonical(&artifact.metadata)?, now],
    )?;
    let existing = results::get(tx, &artifact.artifact_id).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "registered script artifact metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    if existing.kind != artifact.kind
        || existing.relative_path != artifact.relative_path
        || existing.byte_length != artifact.byte_length
        || existing.content_digest != artifact.content_digest
        || existing.metadata != artifact.metadata
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "registered script artifact differs from its immutable completion",
        ));
    }
    Ok(())
}

fn record_incident(db: &Connection, run_id: &str, operation_id: &str, error: Error) -> Result<()> {
    let now = model::now_ms()?;
    let key = format!("script:{run_id}:{}", error.code);
    let evidence = json!({"run_id":run_id,"operation_id":operation_id,"code":error.code});
    db.execute(
        "INSERT INTO incidents(incident_id,dedup_key,state,occurrences,action_operation_id,details_json,opened_at_ms,last_seen_at_ms) VALUES(?1,?2,'open',1,?3,?4,?5,?5) ON CONFLICT(dedup_key) WHERE state='open' DO NOTHING",
        params![model::new_id(), key, operation_id, model::canonical(&evidence)?, now],
    )?;
    Ok(())
}

#[cfg(test)]
mod controller_effect_tests {
    use super::*;

    const MANAGER_ID: &str = "script-effect-manager";
    const NEXT_MANAGER_ID: &str = "script-effect-next-manager";
    const TASK_OWNER_ID: &str = "script-effect-task-owner";
    const TASK_ID: &str = "script-effect-task";
    const ATTEMPT_ID: &str = "script-effect-attempt";
    const SCRIPT_ID: &str = "script_effect_fixture";
    const RUN_ID: &str = "script-effect-run";
    const OPERATION_ID: &str = "script-effect-run-operation";

    struct Fixture {
        db: Connection,
        config: Config,
    }

    fn fixture(grants: Vec<manifest::ScriptControllerEffect>) -> Fixture {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        let schema_tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        super::super::install_schema_extension(
            &schema_tx,
            "schema_extension:scripts:v1",
            super::super::SCRIPT_SCHEMA,
            &["scripts", "script_revisions", "script_runs"],
        )
        .unwrap();
        super::super::script_event_schema::install(&schema_tx).unwrap();
        super::super::install_schema_extension(
            &schema_tx,
            "schema_extension:github:v1",
            super::super::GITHUB_SCHEMA,
            &[
                "github_sources",
                "github_issue_items",
                "github_issue_facts",
                "github_work_pool_members",
                "github_poll_leases",
            ],
        )
        .unwrap();
        schema_tx.commit().unwrap();

        for manager in [MANAGER_ID, NEXT_MANAGER_ID, TASK_OWNER_ID] {
            super::super::set_meta(
                &db,
                &format!("client:{manager}"),
                &json!({"role":"manager","disabled":false}),
            )
            .unwrap();
        }
        super::super::set_meta(
            &db,
            "gm",
            &json!({"client_id":MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":1}),
        )
        .unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'script-effect-project',1,'open','{}',1,1)",
            [TASK_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,created_at_ms,updated_at_ms) VALUES(?1,?2,1,'{}',?3,'controller','running','[]',1,1)",
            params![ATTEMPT_ID, TASK_ID, TASK_OWNER_ID],
        )
        .unwrap();

        let bundle_ref = format!("script-{}", "d".repeat(64));
        let bundle_digest = "b".repeat(64);
        let interpreter = manifest::InterpreterIdentity {
            kind: manifest::InterpreterKind::Powershell,
            canonical_path: std::env::temp_dir().join("script-effect-fixture-interpreter"),
            sha256: "a".repeat(64),
            byte_length: 512,
        };
        let effects = json!(grants);
        let metadata = json!({
            "script_id":SCRIPT_ID,
            "revision":1,
            "bundle_sha256":bundle_digest,
            "interpreter_kind":"powershell",
            "interpreter_sha256":interpreter.sha256,
            "controller_effects":effects,
        });
        db.execute(
            "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'script_bundle',1,?3,1,?4)",
            params![bundle_ref, format!("artifacts/{bundle_ref}.bin"), bundle_digest, model::canonical(&metadata).unwrap()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO scripts(script_id,owner_id,active_revision,created_at_ms,updated_at_ms) VALUES(?1,?2,NULL,1,1)",
            params![SCRIPT_ID, MANAGER_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO script_revisions(script_id,revision,bundle_ref,bundle_sha256,interpreter_json,validated_at_ms,created_by,created_at_ms) VALUES(?1,1,?2,?3,?4,1,?5,1)",
            params![SCRIPT_ID, bundle_ref, bundle_digest, model::canonical(&json!(interpreter)).unwrap(), MANAGER_ID],
        )
        .unwrap();
        db.execute(
            "UPDATE scripts SET active_revision=1 WHERE script_id=?1",
            [SCRIPT_ID],
        )
        .unwrap();

        let invocation = json!({
            "operation_id":OPERATION_ID,
            "run_id":RUN_ID,
            "script_id":SCRIPT_ID,
            "script_revision":1,
            "task_id":TASK_ID,
            "task_revision":1,
            "attempt_id":ATTEMPT_ID,
            "effective_manager_id":MANAGER_ID,
            "cause":{"kind":"script.run","operation_id":OPERATION_ID,"run_id":RUN_ID},
        });
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,'fixture-script-run','script.run','{}','{}',?3,?4,'native_accepted',1,1,1)",
            params![OPERATION_ID, MANAGER_ID, TASK_ID, ATTEMPT_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,process_identity_json,started_at_ms,created_at_ms) VALUES(?1,?2,?3,1,?4,?5,1,?6,?7,?8,'running','{}',1,1)",
            params![
                RUN_ID,
                OPERATION_ID,
                SCRIPT_ID,
                bundle_ref,
                TASK_ID,
                ATTEMPT_ID,
                "c".repeat(64),
                model::canonical(&json!({"capabilities":grants,"invocation":invocation})).unwrap(),
            ],
        )
        .unwrap();

        Fixture {
            db,
            config: Config::default(),
        }
    }

    #[test]
    fn owning_manager_keeps_script_and_attempt_scope_after_gm_handover() {
        let mut fixture = fixture(Vec::new());
        fixture
            .db
            .execute(
                "UPDATE attempts SET owner_id=?1 WHERE attempt_id=?2",
                params![MANAGER_ID, ATTEMPT_ID],
            )
            .unwrap();
        super::super::set_meta(
            &fixture.db,
            "gm",
            &json!({"client_id":NEXT_MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":2}),
        )
        .unwrap();
        let manager = Principal {
            link_id: "fixture-link".to_owned(),
            client_id: MANAGER_ID.to_owned(),
            role: Role::Manager,
        };

        super::require_script_scope(&fixture.db, &manager, SCRIPT_ID)
            .expect("the registered owner manages its script without the GM designation");
        let (task, attempt) = super::require_run_scope(&fixture.db, &manager, ATTEMPT_ID, 1)
            .expect("the exact unreleased Attempt remains controllable by its owner");
        assert_eq!(task["task_id"], TASK_ID);
        assert_eq!(attempt["attempt_id"], ATTEMPT_ID);

        let foreign_manager = Principal {
            link_id: "foreign-fixture-link".to_owned(),
            client_id: TASK_OWNER_ID.to_owned(),
            role: Role::Manager,
        };
        let error = super::require_script_scope(&fixture.db, &foreign_manager, SCRIPT_ID)
            .expect_err("a non-owner Manager without GM authority cannot manage the script");
        assert_eq!(error.code, "FORBIDDEN");
    }

    fn effect_completion(effect: protocol::ScriptEffectRequest) -> runner::Completion {
        let run_id = RUN_ID.to_owned();
        let operation_id = OPERATION_ID.to_owned();
        let make_artifact = |kind: &str, artifact_id: String, metadata: Value| ArtifactRecord {
            kind: kind.to_owned(),
            relative_path: format!("artifacts/{artifact_id}.bin"),
            artifact_id,
            byte_length: 1,
            content_digest: "e".repeat(64),
            metadata,
        };
        runner::Completion {
            run_id,
            operation_id,
            token: "fixture-token".to_owned(),
            state: "completed".to_owned(),
            started_at_ms: Some(1),
            exit_code: Some(0),
            process: json!({"fixture":"store-only"}),
            result: make_artifact(
                "script_result",
                format!("scriptresult-{}", "1".repeat(64)),
                json!({"run_id":RUN_ID,"operation_id":OPERATION_ID}),
            ),
            stdout: make_artifact(
                "script_output",
                format!("scriptlog-{}", "2".repeat(64)),
                json!({"run_id":RUN_ID,"stream":"stdout"}),
            ),
            stderr: make_artifact(
                "script_output",
                format!("scriptlog-{}", "3".repeat(64)),
                json!({"run_id":RUN_ID,"stream":"stderr"}),
            ),
            result_value: Some(json!({"done":true})),
            controller_effects: vec![effect],
            error_code: None,
        }
    }

    fn task_owner_message() -> protocol::ScriptEffectRequest {
        protocol::ScriptEffectRequest {
            effect: manifest::ScriptControllerEffect::TaskOwnerMessage,
            text: "The bounded fixture finished.".to_owned(),
            spec: None,
        }
    }

    fn operation_result(db: &Connection) -> Value {
        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE operation_id=?1",
                [OPERATION_ID],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn declared_effect_uses_message_send_and_retains_exact_invocation_link() {
        let mut fixture = fixture(vec![manifest::ScriptControllerEffect::TaskOwnerMessage]);
        finish(
            &mut fixture.db,
            RUN_ID,
            effect_completion(task_owner_message()),
            &fixture.config,
        )
        .unwrap();

        let result = operation_result(&fixture.db);
        assert_eq!(result["outcome"], "applied");
        assert_eq!(result["controller_effects"][0]["status"], "applied");
        assert_eq!(result["controller_effects"][0]["recipient"], TASK_OWNER_ID);
        let child_operation_id = result["controller_effects"][0]["operation_id"]
            .as_str()
            .unwrap();
        let (method, raw): (String, String) = fixture
            .db
            .query_row(
                "SELECT method,effective_request_json FROM operations WHERE operation_id=?1",
                [child_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(method, "message.send");
        let effective: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            effective["script_invocation"]["operation_id"],
            child_operation_id
        );
        assert_eq!(
            effective["script_invocation"]["effective_manager_id"],
            MANAGER_ID
        );
        assert_eq!(effective["script_invocation"]["action"], "message.send");
        assert_eq!(
            effective["script_invocation"]["grant"],
            "task_owner_message"
        );
        assert_eq!(
            effective["script_invocation"]["cause"],
            json!({
                "kind":"script_invocation",
                "id":OPERATION_ID,
                "script_run_operation_id":OPERATION_ID,
                "script_run_id":RUN_ID,
                "identity":{
                    "script_id":SCRIPT_ID,
                    "script_revision":1,
                    "task_id":TASK_ID,
                    "task_revision":1,
                    "attempt_id":ATTEMPT_ID,
                },
                "effective_manager_id":MANAGER_ID,
            })
        );
        let child_original: Value = serde_json::from_str(
            &fixture
                .db
                .query_row(
                    "SELECT original_request_json FROM operations WHERE operation_id=?1",
                    [child_operation_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(child_original["recipient"], TASK_OWNER_ID);
        assert_eq!(child_original["text"], "The bounded fixture finished.");
    }

    #[test]
    fn ungranted_effect_is_rejected_before_any_message_operation_is_created() {
        let mut fixture = fixture(Vec::new());
        let error = finish(
            &mut fixture.db,
            RUN_ID,
            effect_completion(task_owner_message()),
            &fixture.config,
        )
        .unwrap_err();

        assert_eq!(error.code, "SCRIPT_COMPLETION_DAMAGED");
        let messages: i64 = fixture
            .db
            .query_row(
                "SELECT count(*) FROM operations WHERE method='message.send'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(messages, 0);
    }

    #[test]
    fn manager_handover_revokes_effect_before_current_message_admission() {
        let mut fixture = fixture(vec![manifest::ScriptControllerEffect::TaskOwnerMessage]);
        super::super::set_meta(
            &fixture.db,
            "gm",
            &json!({"client_id":NEXT_MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":2}),
        )
        .unwrap();

        finish(
            &mut fixture.db,
            RUN_ID,
            effect_completion(task_owner_message()),
            &fixture.config,
        )
        .unwrap();

        let result = operation_result(&fixture.db);
        assert_eq!(result["outcome"], "effects_incomplete");
        assert_eq!(result["controller_effects"][0]["status"], "rejected");
        assert_eq!(
            result["controller_effects"][0]["error"]["code"],
            "FORBIDDEN"
        );
        let messages: i64 = fixture
            .db
            .query_row(
                "SELECT count(*) FROM operations WHERE method='message.send'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(messages, 0);
    }

    #[test]
    fn deactivating_the_exact_revision_revokes_its_effect_grant() {
        let mut fixture = fixture(vec![manifest::ScriptControllerEffect::TaskOwnerMessage]);
        fixture
            .db
            .execute(
                "UPDATE scripts SET active_revision=NULL WHERE script_id=?1",
                [SCRIPT_ID],
            )
            .unwrap();

        finish(
            &mut fixture.db,
            RUN_ID,
            effect_completion(task_owner_message()),
            &fixture.config,
        )
        .unwrap();

        let result = operation_result(&fixture.db);
        assert_eq!(result["outcome"], "effects_incomplete");
        assert_eq!(result["controller_effects"][0]["status"], "rejected");
        assert_eq!(
            result["controller_effects"][0]["error"]["code"],
            "SCRIPT_REVISION_NOT_ACTIVE"
        );
        let messages: i64 = fixture
            .db
            .query_row(
                "SELECT count(*) FROM operations WHERE method='message.send'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(messages, 0);
    }
    // Append this test inside `#[cfg(test)] mod controller_effect_tests` in
    // `src/store/scripts.rs`. It deliberately reuses that module's real in-memory
    // Store schema and helpers; it does not launch a script or process.

    #[test]
    fn deterministic_effect_request_collision_preserves_manual_message_without_script_link() {
        let mut fixture = fixture(vec![manifest::ScriptControllerEffect::TaskOwnerMessage]);
        let effect = task_owner_message();
        let request_id = script_effect_request_id(OPERATION_ID, RUN_ID, &effect).unwrap();
        let manual_request = json!({
            "client_request_id":request_id.clone(),
            "recipient":TASK_OWNER_ID,
            "text":effect.text.clone(),
        });
        let manual_request_json = crate::model::canonical(&manual_request).unwrap();

        let manual_receipt = {
            let tx = fixture.db.transaction().unwrap();
            let actor = registered_actor(&tx, MANAGER_ID).unwrap();
            let receipt = super::super::mutate_in_transaction(
                &tx,
                &actor,
                "message.send",
                &manual_request,
                &fixture.config,
                2,
            )
            .unwrap()
            .unwrap();
            tx.commit().unwrap();
            receipt
        };
        let manual_operation_id = manual_receipt["operation_id"].as_str().unwrap().to_owned();
        assert_eq!(
            manual_receipt["message_id"].as_str(),
            Some(manual_operation_id.as_str())
        );

        finish(
            &mut fixture.db,
            RUN_ID,
            effect_completion(effect),
            &fixture.config,
        )
        .unwrap();

        let run_result = operation_result(&fixture.db);
        assert_eq!(run_result["outcome"], "effects_incomplete");
        assert_eq!(run_result["controller_effects"][0]["status"], "rejected");
        assert_eq!(
            run_result["controller_effects"][0]["error"]["code"],
            "SCRIPT_EFFECT_REQUEST_CONFLICT"
        );
        assert!(run_result["controller_effects"][0]["operation_id"].is_null());

        let message_count: i64 = fixture
            .db
            .query_row(
                "SELECT count(*) FROM operations WHERE method='message.send'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            message_count, 1,
            "collision must not create a second message"
        );

        let retained_operation = fixture
        .db
        .query_row(
            "SELECT operation_id,method,original_request_json,effective_request_json,result_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![MANAGER_ID, request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .unwrap();
        let (operation_id, method, original, effective, result) = retained_operation;
        assert_eq!(operation_id, manual_operation_id);
        assert_eq!(method, "message.send");
        assert_eq!(original, manual_request_json);

        let effective: Value = serde_json::from_str(&effective).unwrap();
        let result: Value = serde_json::from_str(&result).unwrap();
        assert!(effective.get("script_invocation").is_none());
        assert_eq!(effective["receipt"]["ok"], true);
        assert_eq!(result["text"], manual_request["text"]);
        assert_eq!(result["recipient"], TASK_OWNER_ID);
        assert_eq!(result["message_id"], manual_operation_id);
    }

    fn insert_terminal_run(
        fixture: &mut Fixture,
        operation_id: &str,
        run_id: &str,
        operation_state: &str,
        run_state: &str,
        task_scope: Option<(&str, i64, &str)>,
    ) {
        let (task_id, task_revision, attempt_id) = match task_scope {
            Some((task_id, task_revision, attempt_id)) => {
                (Some(task_id), Some(task_revision), Some(attempt_id))
            }
            None => (None, None, None),
        };
        let request_id = format!("{operation_id}-request");
        let bundle_ref = format!("script-{}", "d".repeat(64));
        fixture.db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,?3,'script.run','{}','{}',?4,?5,?6,1,1,1)",
            params![operation_id, MANAGER_ID, request_id, task_id, attempt_id, operation_state],
        ).unwrap();
        fixture.db.execute(
            "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,process_identity_json,started_at_ms,created_at_ms) \
             VALUES(?1,?2,?3,1,?4,?5,?6,?7,?8,'{}',?9,CASE WHEN ?9='running' THEN '{}' ELSE NULL END,CASE WHEN ?9='running' THEN 1 ELSE NULL END,1)",
            params![
                run_id,
                operation_id,
                SCRIPT_ID,
                bundle_ref,
                task_id,
                task_revision,
                attempt_id,
                "f".repeat(64),
                run_state,
            ],
        ).unwrap();
    }

    fn terminal_event(
        db: &Connection,
        operation_id: &str,
        event_kind: &str,
    ) -> crate::automation::intake::ObservedEvent {
        db.query_row(
            "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
             FROM observations WHERE source_stream_id='controller:scripts' \
               AND operation_id=?1 AND kind=?2",
            params![operation_id, event_kind],
            |row| {
                Ok(crate::automation::intake::ObservedEvent {
                    observation_id: row.get(0)?,
                    source_id: row.get(1)?,
                    event_kind: row.get(2)?,
                    operation_id: row.get(3)?,
                    recorded_at_ms: row.get(4)?,
                })
            },
        )
        .unwrap()
    }

    fn assert_terminal_projection(
        db: &Connection,
        operation_id: &str,
        event_kind: &str,
        expected_status: crate::automation::event_rules::EventStatus,
        expected_phase: &str,
    ) -> crate::automation::intake::SafeEventProjection {
        let event = terminal_event(db, operation_id, event_kind);
        let projection =
            super::super::automation_intake::safe_event_projection(db, &event).unwrap();
        assert_eq!(projection.status, Some(expected_status));
        assert_eq!(projection.error_code, None);
        assert_eq!(projection.occurrence_phase.as_deref(), Some(expected_phase));
        projection
    }

    fn terminal_callback(
        run_id: &str,
        operation_id: &str,
        state: &str,
        private_marker: &str,
    ) -> runner::Completion {
        let make_artifact = |kind: &str, artifact_id: String, metadata: Value| ArtifactRecord {
            kind: kind.to_owned(),
            relative_path: format!("artifacts/{artifact_id}.bin"),
            artifact_id,
            byte_length: 1,
            content_digest: "9".repeat(64),
            metadata,
        };
        let mut stdout_identity = run_id.as_bytes().to_vec();
        stdout_identity.extend_from_slice(b":stdout");
        let mut stderr_identity = run_id.as_bytes().to_vec();
        stderr_identity.extend_from_slice(b":stderr");
        let completed = state == "completed";
        runner::Completion {
            run_id: run_id.to_owned(),
            operation_id: operation_id.to_owned(),
            token: "fixture-terminal-token".to_owned(),
            state: state.to_owned(),
            started_at_ms: Some(1),
            exit_code: if completed { Some(0) } else { Some(1) },
            process: json!({"fixture":"actual_store_writer"}),
            result: make_artifact(
                "script_result",
                format!("scriptresult-{}", model::digest(operation_id.as_bytes())),
                json!({"run_id":run_id,"operation_id":operation_id,"script_id":SCRIPT_ID,"state":state}),
            ),
            stdout: make_artifact(
                "script_output",
                format!("scriptlog-{}", model::digest(&stdout_identity)),
                json!({"run_id":run_id,"operation_id":operation_id,"stream":"stdout"}),
            ),
            stderr: make_artifact(
                "script_output",
                format!("scriptlog-{}", model::digest(&stderr_identity)),
                json!({"run_id":run_id,"operation_id":operation_id,"stream":"stderr"}),
            ),
            result_value: completed.then(|| json!({"private":private_marker})),
            controller_effects: Vec::new(),
            error_code: match state {
                "completed" => None,
                "failed" => Some("SCRIPT_EXIT_NONZERO".to_owned()),
                _ => Some("SCRIPT_CHILD_STARTED".to_owned()),
            },
        }
    }

    #[test]
    fn terminal_script_writers_project_only_exact_bounded_state() {
        use crate::automation::event_rules::EventStatus;

        let mut fixture = fixture(Vec::new());
        insert_terminal_run(
            &mut fixture,
            "script-terminal-taskless-failed-op",
            "script-terminal-taskless-failed-run",
            "queued",
            "queued",
            None,
        );
        insert_terminal_run(
            &mut fixture,
            "script-terminal-scoped-incomplete-op",
            "script-terminal-scoped-incomplete-run",
            "sending",
            "sending",
            Some((TASK_ID, 1, ATTEMPT_ID)),
        );
        insert_terminal_run(
            &mut fixture,
            "script-terminal-taskless-callback-failed-op",
            "script-terminal-taskless-callback-failed-run",
            "native_accepted",
            "running",
            None,
        );
        insert_terminal_run(
            &mut fixture,
            "script-terminal-scoped-callback-incomplete-op",
            "script-terminal-scoped-callback-incomplete-run",
            "native_accepted",
            "running",
            Some((TASK_ID, 1, ATTEMPT_ID)),
        );

        fail_before_start(
            &mut fixture.db,
            "script-terminal-taskless-failed-run",
            "PRIVATE_FAILURE_DETAIL",
        )
        .unwrap();
        settle_incomplete(
            &mut fixture.db,
            "script-terminal-scoped-incomplete-run",
            "PRIVATE_INCOMPLETE_DETAIL",
            true,
        )
        .unwrap();
        finish(
            &mut fixture.db,
            RUN_ID,
            terminal_callback(
                RUN_ID,
                OPERATION_ID,
                "completed",
                "script-terminal-result-marker",
            ),
            &fixture.config,
        )
        .unwrap();
        finish(
            &mut fixture.db,
            "script-terminal-taskless-callback-failed-run",
            terminal_callback(
                "script-terminal-taskless-callback-failed-run",
                "script-terminal-taskless-callback-failed-op",
                "failed",
                "script-terminal-failed-callback-marker",
            ),
            &fixture.config,
        )
        .unwrap();
        finish(
            &mut fixture.db,
            "script-terminal-scoped-callback-incomplete-run",
            terminal_callback(
                "script-terminal-scoped-callback-incomplete-run",
                "script-terminal-scoped-callback-incomplete-op",
                "incomplete",
                "script-terminal-incomplete-callback-marker",
            ),
            &fixture.config,
        )
        .unwrap();

        let failure_projection = assert_terminal_projection(
            &fixture.db,
            "script-terminal-taskless-failed-op",
            "script.failed",
            EventStatus::Failed,
            "script_run_failed",
        );
        let incomplete_projection = assert_terminal_projection(
            &fixture.db,
            "script-terminal-scoped-incomplete-op",
            "script.incomplete",
            EventStatus::Incomplete,
            "script_run_incomplete",
        );
        let callback_failed_projection = assert_terminal_projection(
            &fixture.db,
            "script-terminal-taskless-callback-failed-op",
            "script.completed",
            EventStatus::Failed,
            "script_run_failed",
        );
        let callback_incomplete_projection = assert_terminal_projection(
            &fixture.db,
            "script-terminal-scoped-callback-incomplete-op",
            "script.completed",
            EventStatus::Incomplete,
            "script_run_incomplete",
        );

        let completed = terminal_event(&fixture.db, OPERATION_ID, "script.completed");
        let completed_projection =
            super::super::automation_intake::safe_event_projection(&fixture.db, &completed)
                .unwrap();
        assert_eq!(completed_projection.status, Some(EventStatus::Completed));
        assert_eq!(completed_projection.error_code, None);
        assert_eq!(
            completed_projection.occurrence_id.as_deref(),
            Some("operation:script-effect-run-operation:script_run_completed")
        );
        let projected = format!(
            "{failure_projection:?}{incomplete_projection:?}{completed_projection:?}{callback_failed_projection:?}{callback_incomplete_projection:?}"
        );
        for private_value in [
            "PRIVATE_FAILURE_DETAIL",
            "PRIVATE_INCOMPLETE_DETAIL",
            "script-terminal-result-marker",
            "script-terminal-failed-callback-marker",
            "script-terminal-incomplete-callback-marker",
            "SCRIPT_EXIT_NONZERO",
            "SCRIPT_CHILD_STARTED",
        ] {
            assert!(!projected.contains(private_value));
        }

        let wrong_event_link = crate::automation::intake::ObservedEvent {
            operation_id: Some("script-terminal-taskless-failed-op".to_owned()),
            ..completed.clone()
        };
        assert_eq!(
            super::super::automation_intake::safe_event_projection(&fixture.db, &wrong_event_link)
                .unwrap(),
            Default::default()
        );

        fixture
            .db
            .execute(
                "UPDATE observations SET operation_id=?1 WHERE observation_id=?2",
                params![
                    "script-terminal-taskless-failed-op",
                    completed.observation_id
                ],
            )
            .unwrap();
        assert_eq!(
            super::super::automation_intake::safe_event_projection(&fixture.db, &completed)
                .unwrap(),
            Default::default()
        );
        fixture
            .db
            .execute(
                "UPDATE observations SET operation_id=?1 WHERE observation_id=?2",
                params![OPERATION_ID, completed.observation_id],
            )
            .unwrap();
        for wrong_outcome in [json!(true), json!(17)] {
            let mut altered_result = operation_result(&fixture.db);
            altered_result["outcome"] = wrong_outcome;
            let altered_json = model::canonical(&altered_result).unwrap();
            fixture
                .db
                .execute(
                    "UPDATE operations SET result_json=?1 WHERE operation_id=?2",
                    params![altered_json, OPERATION_ID],
                )
                .unwrap();
            fixture
                .db
                .execute(
                    "UPDATE observations SET payload_json=?1 WHERE observation_id=?2",
                    params![altered_json, completed.observation_id],
                )
                .unwrap();
            assert_eq!(
                super::super::automation_intake::safe_event_projection(&fixture.db, &completed)
                    .unwrap(),
                Default::default(),
                "a non-text outcome must fail closed without breaking intake"
            );
        }
        fixture
            .db
            .execute(
                "UPDATE operations SET result_json='{}' WHERE operation_id=?1",
                [OPERATION_ID],
            )
            .unwrap();
        assert_eq!(
            super::super::automation_intake::safe_event_projection(&fixture.db, &completed)
                .unwrap(),
            Default::default()
        );
    }
}

pub(super) fn authorize_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact: &ArtifactRecord,
) -> Result<()> {
    require_script_authority(db, principal)?;
    match artifact.kind.as_str() {
        "script_bundle" => {
            let (script_id, revision): (String, i64) = db
                .query_row(
                    "SELECT script_id,revision FROM script_revisions WHERE bundle_ref=?1",
                    [&artifact.artifact_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| {
                    Error::new(
                        "NOT_FOUND",
                        "script bundle is not linked to a retained revision",
                    )
                })?;
            require_script_scope(db, principal, &script_id)?;
            let linked = registry::bundle_record(db, &script_id, revision)?;
            if linked.kind != artifact.kind
                || linked.artifact_id != artifact.artifact_id
                || linked.content_digest != artifact.content_digest
                || linked.byte_length != artifact.byte_length
                || linked.relative_path != artifact.relative_path
                || linked.metadata != artifact.metadata
            {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "script bundle artifact differs from its retained revision",
                ));
            }
        }
        "script_result" | "script_output" => {
            let run_id = model::text(&artifact.metadata, "run_id")?;
            let operation_id = model::text(&artifact.metadata, "operation_id")?;
            let row: Option<ScriptRunArtifactLinks> = db
                .query_row(
                    "SELECT r.run_id,r.operation_id,r.state,r.result_ref,r.stdout_ref,r.stderr_ref,o.method \
                     FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1 AND r.operation_id=?2",
                    params![run_id, operation_id],
                    |row| {
                        Ok(ScriptRunArtifactLinks {
                            run_id: row.get(0)?,
                            operation_id: row.get(1)?,
                            state: row.get(2)?,
                            result_ref: row.get(3)?,
                            stdout_ref: row.get(4)?,
                            stderr_ref: row.get(5)?,
                            method: row.get(6)?,
                        })
                    },
                )
                .optional()?;
            let Some(stored) = row else {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact has no retained run",
                ));
            };
            let script_id: String = db.query_row(
                "SELECT script_id FROM script_runs WHERE run_id=?1",
                [run_id],
                |row| row.get(0),
            )?;
            require_script_scope(db, principal, &script_id)?;
            if stored.run_id != run_id
                || stored.operation_id != operation_id
                || stored.method != "script.run"
                || !matches!(
                    stored.state.as_str(),
                    "completed" | "failed" | "incomplete" | "outcome_unknown"
                )
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact is outside its retained run",
                ));
            }
            let expected_ref = match artifact.kind.as_str() {
                "script_result" => stored.result_ref,
                "script_output" if artifact.metadata["stream"] == "stdout" => stored.stdout_ref,
                "script_output" if artifact.metadata["stream"] == "stderr" => stored.stderr_ref,
                _ => None,
            };
            if expected_ref.as_deref() != Some(artifact.artifact_id.as_str()) {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact is not referenced by its run",
                ));
            }
            let registered = results::get(db, &artifact.artifact_id)?;
            if registered.kind != artifact.kind
                || registered.relative_path != artifact.relative_path
                || registered.byte_length != artifact.byte_length
                || registered.content_digest != artifact.content_digest
                || registered.metadata != artifact.metadata
            {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "script artifact identity changed",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "script artifact read requires a script artifact",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod script_trigger_admission_isolation_tests {
    use crate::{
        config::Config,
        model::{self, Credential, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use rusqlite::{TransactionBehavior, params};
    use serde_json::{Value, json};
    use std::{path::PathBuf, sync::Arc};

    const OWNER_ID: &str = "script-trigger-isolation-owner";
    const PROJECT_ID: &str = "script-trigger-isolation-project";
    const DAMAGED_AUTOMATION_ID: &str = "a-script-trigger-isolation-damaged";
    const HEALTHY_AUTOMATION_ID: &str = "b-script-trigger-isolation-healthy";
    const DAMAGED_SCRIPT_ID: &str = "a_script_trigger_isolation_damaged";
    const HEALTHY_SCRIPT_ID: &str = "b_script_trigger_isolation_healthy";

    async fn start_store() -> (StoreOwner, PathBuf, Principal) {
        let directory = std::env::temp_dir().join(format!(
            "swarm-script-trigger-isolation-{}",
            model::new_id()
        ));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let root = DataRoot::acquire(&directory).expect("acquire temporary Store root");
        let credential = bootstrap_credential(&root.path).expect("create Operator credential");
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .expect("start Store");
        let operator = owner
            .store
            .authenticate(credential)
            .await
            .expect("authenticate Operator");
        (owner, directory, operator)
    }

    async fn register_manager(store: &Store, operator: &Principal) -> Principal {
        let token = format!("script-trigger-isolation-{}", model::new_id());
        store
            .call(
                operator.clone(),
                "client.register".into(),
                json!({
                    "client_request_id":"script-trigger-isolation-register-manager",
                    "client_id":OWNER_ID,
                    "role":"manager",
                    "token_hash":model::digest(token.as_bytes()),
                }),
            )
            .await
            .expect("register Manager");
        store
            .authenticate(Credential {
                client_id: OWNER_ID.to_owned(),
                token,
            })
            .await
            .expect("authenticate Manager")
    }

    async fn register_named_manager(
        store: &Store,
        operator: &Principal,
        client_id: &str,
    ) -> Principal {
        let token = format!("script-trigger-isolation-{}", model::new_id());
        store
            .call(
                operator.clone(),
                "client.register".into(),
                json!({
                    "client_request_id":format!("register-{client_id}"),
                    "client_id":client_id,
                    "role":"manager",
                    "token_hash":model::digest(token.as_bytes()),
                }),
            )
            .await
            .expect("register scoped Manager");
        store
            .authenticate(Credential {
                client_id: client_id.to_owned(),
                token,
            })
            .await
            .expect("authenticate scoped Manager")
    }

    fn powershell_path() -> PathBuf {
        let executable = if cfg!(windows) { "pwsh.exe" } else { "pwsh" };
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .map(|directory| directory.join(executable))
            .filter_map(|path| std::fs::canonicalize(path).ok())
            .find(|path| {
                std::fs::symlink_metadata(path)
                    .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
            })
            .expect("pwsh is needed only for retained bundle identity checks")
    }

    fn script_bundle(script_id: &str) -> Value {
        json!({
            "script_id":script_id,
            "interpreter_kind":"powershell",
            "interpreter_path":powershell_path(),
            "entrypoint":"main.ps1",
            "argv":[],
            "trust":"trusted_local",
            "inherit_environment":[],
            "controller_effects":[],
            "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
            "result_schema":{"type":"null"},
            "files":[{"path":"main.ps1","content_base64":STANDARD.encode(b"Write-Output {}")}],
        })
    }

    async fn register_active_script(store: &Store, manager: &Principal, script_id: &str) {
        let bundle = script_bundle(script_id);
        store
            .call(
                manager.clone(),
                "script.register".into(),
                json!({
                    "client_request_id":format!("register-{script_id}"),
                    "bundle":bundle,
                }),
            )
            .await
            .expect("register bundle without executing it");
        store
            .call(
                manager.clone(),
                "script.activate".into(),
                json!({
                    "client_request_id":format!("activate-{script_id}"),
                    "script_id":script_id,
                    "revision":1,
                }),
            )
            .await
            .expect("activate immutable revision");
    }

    async fn configure_trigger(
        store: &Store,
        manager: &Principal,
        automation_id: &str,
        script_id: &str,
    ) {
        let changes = json!([{
            "automation_id":automation_id,
            "expected_revision":0,
            "include_existing":false,
            "patch":{
                "enabled":true,
                "steps":["script_run"],
                "script_run":{"script_id":script_id},
                "event_rules":[{
                    "source_id":"controller:host-lifecycle",
                    "event_kind":"host.interrupted",
                    "status":"unknown",
                    "action":"script_run",
                }],
            },
        }]);
        let preview = store
            .call(
                manager.clone(),
                "automation.config.preview".into(),
                json!({"project_id":PROJECT_ID,"changes":changes}),
            )
            .await
            .expect("preview ScriptRun automation");
        assert_eq!(preview["valid"], true, "{preview}");
        store
            .call(
                manager.clone(),
                "automation.config.apply".into(),
                json!({
                    "client_request_id":format!("apply-{automation_id}"),
                    "project_id":PROJECT_ID,
                    "changes":changes,
                    "preview_digest":preview["plan_sha256"],
                }),
            )
            .await
            .expect("apply ScriptRun automation");
    }

    async fn record_interruption(store: &Store) {
        store
            .run(|db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                crate::store::set_meta(&tx, "host_epoch", &json!(2))?;
                crate::store::set_meta(
                    &tx,
                    "host:lifecycle:v1",
                    &json!({
                        "schema_version":1,
                        "host_epoch":1,
                        "state":"running",
                        "started_at_ms":1,
                        "updated_at_ms":1,
                    }),
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("seed prior running host receipt");
        store
            .record_host_start()
            .await
            .expect("record host restart");
    }

    async fn explain_trigger(store: &Store, manager: &Principal, automation_id: &str) -> Value {
        store
            .call(
                manager.clone(),
                "automation.config.explain".into(),
                json!({
                    "project_id":PROJECT_ID,
                    "automation_id":automation_id,
                }),
            )
            .await
            .expect("read manager-scoped ScriptRun explanation")
    }

    #[tokio::test]
    async fn ordinary_manager_script_api_is_owner_scoped_without_gm() {
        let (owner, directory, operator) = start_store().await;
        let manager = register_manager(&owner.store, &operator).await;
        let foreign = register_named_manager(
            &owner.store,
            &operator,
            "script-trigger-isolation-foreign-owner",
        )
        .await;
        register_active_script(&owner.store, &manager, "manager_owned_script").await;
        register_active_script(&owner.store, &foreign, "foreign_owned_script").await;

        let authorization = owner
            .store
            .call(manager.clone(), "mcp.authorization".into(), json!({}))
            .await
            .expect("ordinary Manager receives Store-authorized MCP discovery");
        let allowed = authorization["allowed_methods"]
            .as_array()
            .expect("MCP authorization returns a method list");
        for method in [
            "script.register",
            "script.validate",
            "script.activate",
            "script.run",
            "script.get",
            "script.list",
        ] {
            assert!(
                allowed
                    .iter()
                    .any(|candidate| candidate.as_str() == Some(method)),
                "owner Manager should discover {method} without current-GM authority: {authorization}"
            );
        }

        let own_script = owner
            .store
            .call(
                manager.clone(),
                "script.get".into(),
                json!({"script_id":"manager_owned_script"}),
            )
            .await
            .expect("own script remains readable without the GM designation");
        assert_eq!(own_script["owner_id"], manager.client_id);

        let own_list = owner
            .store
            .call(manager.clone(), "script.list".into(), json!({}))
            .await
            .expect("Manager receives its filtered catalog");
        assert_eq!(own_list["items"].as_array().unwrap().len(), 1);
        assert_eq!(own_list["items"][0]["script_id"], "manager_owned_script");

        let foreign_get = owner
            .store
            .call(
                manager.clone(),
                "script.get".into(),
                json!({"script_id":"foreign_owned_script"}),
            )
            .await
            .expect_err("a Manager cannot read another owner's script");
        assert_eq!(foreign_get.code, "FORBIDDEN");
        let unknown_get = owner
            .store
            .call(
                manager.clone(),
                "script.get".into(),
                json!({"script_id":"not_registered_for_this_manager"}),
            )
            .await
            .expect_err("an ordinary Manager cannot probe unknown script IDs");
        assert_eq!(unknown_get.code, "FORBIDDEN");
        let foreign_activate = owner
            .store
            .call(
                manager.clone(),
                "script.activate".into(),
                json!({
                    "client_request_id":"manager-cannot-activate-foreign-script",
                    "script_id":"foreign_owned_script",
                    "revision":1,
                }),
            )
            .await
            .expect_err("a Manager cannot activate another owner's script");
        assert_eq!(foreign_activate.code, "FORBIDDEN");
        let foreign_run = owner
            .store
            .call(
                manager,
                "script.run".into(),
                json!({
                    "client_request_id":"manager-cannot-run-foreign-script",
                    "script_id":"foreign_owned_script",
                    "expected_script_revision":1,
                    "attempt_id":"nonexistent-attempt",
                    "expected_task_revision":1,
                    "input":{},
                }),
            )
            .await
            .expect_err("a Manager cannot run another owner's script");
        assert_eq!(foreign_run.code, "FORBIDDEN");

        owner.close().await.expect("close temporary Store");
        std::fs::remove_dir_all(directory).expect("remove owned temporary Store directory");
    }

    enum Damage {
        RevisionReceipt,
        BundleBytes,
        BundleJson,
        ArtifactMetadata,
    }

    #[tokio::test]
    async fn damaged_script_trigger_is_held_and_healthy_neighbor_is_admitted() {
        assert_damaged_script_trigger_isolated(Damage::RevisionReceipt).await;
    }

    #[tokio::test]
    async fn corrupt_retained_event_link_is_held_while_healthy_neighbor_is_admitted() {
        let (owner, directory, operator) = start_store().await;
        let manager = register_manager(&owner.store, &operator).await;
        owner
            .store
            .call(
                operator,
                "gm.handover".into(),
                json!({
                    "client_request_id":"script-trigger-link-handover",
                    "client_id":OWNER_ID,
                }),
            )
            .await
            .expect("designate fixture Manager");
        register_active_script(&owner.store, &manager, DAMAGED_SCRIPT_ID).await;
        register_active_script(&owner.store, &manager, HEALTHY_SCRIPT_ID).await;
        configure_trigger(
            &owner.store,
            &manager,
            DAMAGED_AUTOMATION_ID,
            DAMAGED_SCRIPT_ID,
        )
        .await;
        configure_trigger(
            &owner.store,
            &manager,
            HEALTHY_AUTOMATION_ID,
            HEALTHY_SCRIPT_ID,
        )
        .await;
        owner
            .store
            .record_host_start()
            .await
            .expect("record host start");
        owner
            .store
            .record_host_ready()
            .await
            .expect("record host ready");
        record_interruption(&owner.store).await;

        let config = Config::default();
        owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let staged = crate::store::automation_dispatch::reconcile(
                    &tx,
                    &config,
                    16,
                    64,
                    model::now_ms()?,
                )?;
                tx.commit()?;
                Ok(staged)
            })
            .await
            .expect("commit both event causes before admission");

        let pending = owner
            .store
            .run(|db| crate::store::automation_dispatch::pending_script_triggers(db, 16))
            .await
            .expect("read durable ScriptRun causes");
        let damaged_intent = pending
            .into_iter()
            .find(|intent| intent.script_id == DAMAGED_SCRIPT_ID)
            .expect("damaged event cause");
        let admitted = owner
            .store
            .admit_script_trigger(damaged_intent)
            .await
            .unwrap_or_else(|failure| {
                panic!(
                    "seed admitted Operation before journal completion: {}",
                    failure.error.code
                )
            });
        let damaged_operation_id = admitted["operation_id"]
            .as_str()
            .expect("retained ScriptRun Operation ID")
            .to_owned();
        let link_key = crate::automation::config::operation_link_key(&damaged_operation_id)
            .expect("build exact Operation-link key");
        owner
            .store
            .run(move |db| {
                let removed = db.execute("DELETE FROM meta WHERE key=?1", [&link_key])?;
                assert_eq!(removed, 1);
                Ok(())
            })
            .await
            .expect("simulate link loss after Operation commit and before journal completion");

        let pass = owner
            .store
            .reconcile_script_triggers_once(16)
            .await
            .expect("entry-local link corruption must not stop trigger reconciliation");
        assert_eq!(pass["considered"], 2, "{pass}");
        let outcomes = pass["outcomes"].as_array().expect("trigger outcomes");
        let damaged = outcomes
            .iter()
            .find(|outcome| outcome["script_id"] == DAMAGED_SCRIPT_ID)
            .expect("damaged trigger outcome");
        assert_eq!(damaged["state"], "blocked_pending_revalidation");
        assert_eq!(damaged["error"]["code"], "AUTOMATION_LINK_CORRUPT");
        let healthy = outcomes
            .iter()
            .find(|outcome| outcome["script_id"] == HEALTHY_SCRIPT_ID)
            .expect("healthy trigger outcome");
        assert_eq!(healthy["state"], "admitted");
        let healthy_operation_id = healthy["operation_id"]
            .as_str()
            .expect("healthy admitted Operation ID")
            .to_owned();

        let damaged_state =
            explain_trigger(&owner.store, &manager, DAMAGED_AUTOMATION_ID).await["script_run"]
                .clone();
        assert_eq!(damaged_state["pending"].as_array().unwrap().len(), 1);
        assert_eq!(damaged_state["pending"][0]["held"], true);
        assert_eq!(
            damaged_state["pending"][0]["held_reason"],
            "admission_revalidation:automation_link_corrupt"
        );
        assert!(
            damaged_state["recent"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item["script_id"] == DAMAGED_SCRIPT_ID
                        && item["details"]["code"] == "AUTOMATION_LINK_CORRUPT"
                })
        );
        let healthy_state =
            explain_trigger(&owner.store, &manager, HEALTHY_AUTOMATION_ID).await["script_run"]
                .clone();
        assert!(healthy_state["pending"].as_array().unwrap().is_empty());

        let damaged_explanation =
            explain_trigger(&owner.store, &manager, DAMAGED_AUTOMATION_ID).await;
        for projection in ["linked_operations", "linked_operation_history"] {
            assert_eq!(
                damaged_explanation[projection]["closed"]["status"],
                "degraded"
            );
            assert_eq!(
                damaged_explanation[projection]["closed"]["error_code"],
                "AUTOMATION_LINK_CORRUPT"
            );
            assert_eq!(
                damaged_explanation[projection]["closed"]["category"],
                "automation_link_corrupt"
            );
            assert_eq!(damaged_explanation[projection]["truncated"], true);
            assert_eq!(
                damaged_explanation[projection]["closed"]
                    .as_object()
                    .unwrap()
                    .len(),
                3
            );
        }
        assert!(
            damaged_explanation["linked_operations"]["unstarted"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(
            damaged_explanation["linked_operation_history"]["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let explanation_json = serde_json::to_string(&damaged_explanation).unwrap();
        assert!(!explanation_json.contains(&damaged_operation_id));

        let (damaged_operation_ids, healthy_operation_ids, started_runs) = owner
            .store
            .run(|db| {
                let mut damaged_statement = db.prepare(
                    "SELECT operation_id FROM script_runs WHERE script_id=?1 ORDER BY run_id",
                )?;
                let damaged = damaged_statement
                    .query_map([DAMAGED_SCRIPT_ID], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut healthy_statement = db.prepare(
                    "SELECT operation_id FROM script_runs WHERE script_id=?1 ORDER BY run_id",
                )?;
                let healthy = healthy_statement
                    .query_map([HEALTHY_SCRIPT_ID], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let started: i64 = db.query_row(
                    "SELECT COUNT(*) FROM script_runs WHERE started_at_ms IS NOT NULL",
                    [],
                    |row| row.get(0),
                )?;
                Ok((damaged, healthy, started))
            })
            .await
            .expect("read admitted runs without starting the script supervisor");
        assert_eq!(damaged_operation_ids, vec![damaged_operation_id]);
        assert_eq!(healthy_operation_ids, vec![healthy_operation_id]);
        assert_eq!(started_runs, 0);
        owner.close().await.expect("close temporary Store");
        std::fs::remove_dir_all(directory).expect("remove owned temporary Store directory");
    }

    #[tokio::test]
    async fn damaged_bundle_bytes_are_isolated_and_recover_on_new_revision() {
        assert_damaged_script_trigger_isolated(Damage::BundleBytes).await;
    }

    #[tokio::test]
    async fn malformed_retained_bundle_is_isolated_and_recoverable() {
        assert_damaged_script_trigger_isolated(Damage::BundleJson).await;
    }

    #[tokio::test]
    async fn invalid_artifact_metadata_is_isolated_and_recoverable() {
        assert_damaged_script_trigger_isolated(Damage::ArtifactMetadata).await;
    }

    async fn assert_damaged_script_trigger_isolated(damage: Damage) {
        // This fixture exercises Store admission only. The host's separate
        // `supervise_scripts` worker is not started, so no interpreter launches.
        let (owner, directory, operator) = start_store().await;
        let manager = register_manager(&owner.store, &operator).await;
        owner
            .store
            .call(
                operator,
                "gm.handover".into(),
                json!({
                    "client_request_id":"script-trigger-isolation-handover",
                    "client_id":OWNER_ID,
                }),
            )
            .await
            .expect("designate fixture Manager");

        register_active_script(&owner.store, &manager, DAMAGED_SCRIPT_ID).await;
        register_active_script(&owner.store, &manager, HEALTHY_SCRIPT_ID).await;
        configure_trigger(
            &owner.store,
            &manager,
            DAMAGED_AUTOMATION_ID,
            DAMAGED_SCRIPT_ID,
        )
        .await;
        configure_trigger(
            &owner.store,
            &manager,
            HEALTHY_AUTOMATION_ID,
            HEALTHY_SCRIPT_ID,
        )
        .await;
        owner
            .store
            .record_host_start()
            .await
            .expect("record host start");
        owner
            .store
            .record_host_ready()
            .await
            .expect("record host ready");

        let corrupt_script = DAMAGED_SCRIPT_ID.to_owned();
        match damage {
            Damage::RevisionReceipt | Damage::ArtifactMetadata => {
                let metadata = matches!(damage, Damage::ArtifactMetadata);
                let rows = owner.store.run(move |db| {
                    if metadata {
                        Ok(db.execute(
                            // The schema rejects malformed JSON. A valid JSON
                            // value with the wrong shape still represents a
                            // reachable retained-metadata integrity failure.
                            "UPDATE artifacts SET metadata_json='[]' WHERE artifact_id=(SELECT bundle_ref FROM script_revisions WHERE script_id=?1 AND revision=1)",
                            [corrupt_script],
                        )?)
                    } else {
                        Ok(db.execute(
                            "UPDATE script_revisions SET bundle_sha256=?1 WHERE script_id=?2 AND revision=1",
                            params!["0".repeat(64), corrupt_script],
                        )?)
                    }
                }).await.expect("corrupt only the selected retained revision");
                assert_eq!(rows, 1);
            }
            Damage::BundleBytes | Damage::BundleJson => {
                let change_digest = matches!(damage, Damage::BundleJson);
                let relative_path: String = owner.store.run(move |db| {
                    let path = db.query_row(
                        "SELECT a.relative_path FROM artifacts a JOIN script_revisions r ON r.bundle_ref=a.artifact_id WHERE r.script_id=?1 AND r.revision=1",
                        [&corrupt_script], |row| row.get(0),
                    )?;
                    if change_digest {
                        let digest = model::digest(b"{");
                        db.execute("UPDATE artifacts SET byte_length=1,content_digest=?1,metadata_json=json_set(metadata_json,'$.bundle_sha256',?1) WHERE artifact_id=(SELECT bundle_ref FROM script_revisions WHERE script_id=?2 AND revision=1)", params![digest,corrupt_script])?;
                        db.execute("UPDATE script_revisions SET bundle_sha256=?1 WHERE script_id=?2 AND revision=1",params![digest,corrupt_script])?;
                    }
                    Ok(path)
                }).await.expect("read exact owned bundle identity");
                let path = std::fs::canonicalize(directory.join(relative_path)).unwrap();
                assert!(path.starts_with(std::fs::canonicalize(&directory).unwrap()));
                std::fs::write(path, b"{").expect("corrupt only the owned fixture bundle");
            }
        }
        record_interruption(&owner.store).await;

        let pass = owner
            .store
            .reconcile_automations_once()
            .await
            .expect("one damaged trigger must not fail host reconciliation");
        assert_eq!(pass["script_run"]["considered"], 2, "{pass}");
        let outcomes = pass["script_run"]["outcomes"].as_array().unwrap();
        let damaged_outcome = outcomes
            .iter()
            .find(|outcome| outcome["script_id"] == DAMAGED_SCRIPT_ID)
            .expect("damaged trigger outcome");
        assert_eq!(damaged_outcome["state"], "blocked_pending_revalidation");
        assert_eq!(damaged_outcome["error"]["code"], "SCRIPT_REGISTRY_DAMAGED");
        let healthy_outcome = outcomes
            .iter()
            .find(|outcome| outcome["script_id"] == HEALTHY_SCRIPT_ID)
            .expect("unrelated healthy trigger outcome");
        assert_eq!(healthy_outcome["state"], "admitted");
        let healthy_operation_id = healthy_outcome["operation_id"].clone();

        let damaged_state =
            explain_trigger(&owner.store, &manager, DAMAGED_AUTOMATION_ID).await["script_run"]
                .clone();
        let pending = damaged_state["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0]["held"], true);
        assert_eq!(
            pending[0]["held_reason"],
            "admission_revalidation:script_registry_damaged:revision:1"
        );
        let damaged_history = damaged_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["script_id"] == DAMAGED_SCRIPT_ID)
            .expect("persisted held history");
        assert_eq!(
            damaged_history["disposition"],
            "blocked_pending_current_authorization"
        );
        assert_eq!(
            damaged_history["details"]["code"],
            "SCRIPT_REGISTRY_DAMAGED"
        );

        let healthy_state =
            explain_trigger(&owner.store, &manager, HEALTHY_AUTOMATION_ID).await["script_run"]
                .clone();
        assert_eq!(healthy_state["pending"].as_array().unwrap().len(), 0);
        let healthy_history = healthy_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["script_id"] == HEALTHY_SCRIPT_ID)
            .expect("persisted admitted history");
        assert_eq!(healthy_history["disposition"], "admitted");
        assert_eq!(
            healthy_history["details"]["operation_id"],
            healthy_operation_id
        );
        assert_eq!(damaged_history["details"]["failed_script_revision"], 1);

        let damaged_id = DAMAGED_SCRIPT_ID.to_owned();
        let healthy_id = HEALTHY_SCRIPT_ID.to_owned();
        let (damaged_runs, healthy_runs) = owner
            .store
            .run(move |db| {
                let damaged: i64 = db.query_row(
                    "SELECT COUNT(*) FROM script_runs WHERE script_id=?1",
                    [&damaged_id],
                    |row| row.get(0),
                )?;
                let healthy: i64 = db.query_row(
                    "SELECT COUNT(*) FROM script_runs WHERE script_id=?1",
                    [&healthy_id],
                    |row| row.get(0),
                )?;
                Ok((damaged, healthy))
            })
            .await
            .expect("read admitted script runs");
        assert_eq!(damaged_runs, 0);
        assert_eq!(healthy_runs, 1);

        let replay = owner
            .store
            .reconcile_automations_once()
            .await
            .expect("held trigger remains durably withheld");
        assert_eq!(replay["script_run"]["considered"], 0, "{replay}");

        owner
            .store
            .call(
                manager.clone(),
                "script.revise".into(),
                json!({
                    "client_request_id":"script-trigger-isolation-repair-revision",
                    "script_id":DAMAGED_SCRIPT_ID,
                    "expected_revision":1,
                    "bundle":script_bundle(DAMAGED_SCRIPT_ID),
                }),
            )
            .await
            .expect("publish a replacement immutable revision");
        owner
            .store
            .call(
                manager.clone(),
                "script.activate".into(),
                json!({
                    "client_request_id":"script-trigger-isolation-activate-repair-revision",
                    "script_id":DAMAGED_SCRIPT_ID,
                    "revision":2,
                }),
            )
            .await
            .expect("activate the repaired immutable revision");
        let recovered = owner
            .store
            .reconcile_automations_once()
            .await
            .expect("a valid replacement revision releases the held trigger");
        assert_eq!(recovered["script_run"]["considered"], 1, "{recovered}");
        assert_eq!(recovered["script_run"]["outcomes"][0]["state"], "admitted");
        assert_eq!(
            recovered["script_run"]["outcomes"][0]["script_id"],
            DAMAGED_SCRIPT_ID
        );
        let recovered_state = explain_trigger(&owner.store, &manager, DAMAGED_AUTOMATION_ID).await
            ["script_run"]
            .clone();
        assert!(recovered_state["pending"].as_array().unwrap().is_empty());
        assert!(
            recovered_state["recent"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| {
                    event["script_id"] == DAMAGED_SCRIPT_ID && event["disposition"] == "admitted"
                })
        );
        let (admitted_runs, started_runs) = owner
            .store
            .run(|db| {
                let counts = db.query_row(
                    "SELECT COUNT(*),COUNT(started_at_ms) FROM script_runs",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )?;
                Ok(counts)
            })
            .await
            .expect("read queued runs without starting the script supervisor");
        assert_eq!(admitted_runs, 2);
        assert_eq!(started_runs, 0);
        owner.close().await.expect("close temporary Store");
        std::fs::remove_dir_all(directory).expect("remove owned temporary Store directory");
    }
}
