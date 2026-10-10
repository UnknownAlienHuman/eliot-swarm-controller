use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("CLOCK_ERROR", "system clock precedes Unix epoch"))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| Error::new("CLOCK_ERROR", "timestamp overflow"))
}
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Equality is over the original request, before defaults or route resolution.
/// serde_json's default map is sorted; reconstruct recursively to keep that explicit.
pub fn canonical(value: &Value) -> Result<String> {
    fn ordered(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> =
                    map.iter().map(|(k, v)| (k.clone(), ordered(v))).collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(a) => Value::Array(a.iter().map(ordered).collect()),
            v => v.clone(),
        }
    }
    Ok(serde_json::to_string(&ordered(value))?)
}

/// The payload a mailbox delivery commits to: the two parties and the text.
/// The digest is SHA-256 over the canonical JSON of exactly this object, so
/// key order in any reconstruction cannot change the digest. Deadlines,
/// scopes and generations are delivery facts recorded next to the digest;
/// they are not part of the payload the digest binds.
pub fn message_payload_digest(sender: &str, recipient: &str, text: &str) -> Result<String> {
    let payload = json!({"recipient": recipient, "sender": sender, "text": text});
    Ok(digest(canonical(&payload)?.as_bytes()))
}

/// Actor identity for a mailbox record, taken from the durable client
/// registration. The generation is the module binding generation where one
/// is tracked; a client without a binding has no generation (explicit null),
/// never an invented one.
pub fn message_actor(registration: &Value, client_id: &str) -> Value {
    json!({"client_id":client_id,"role":registration.get("role").cloned().unwrap_or(Value::Null),"generation":registration.get("binding_generation").cloned().unwrap_or(Value::Null)})
}

/// Source/target scope of a delivery: the registered scope the client acts
/// under, read from the same durable registration as the actor identity.
/// Binding fields are explicit nulls for clients without a module binding.
pub fn message_scope(registration: &Value, client_id: &str) -> Value {
    json!({"client_id":client_id,"role":registration.get("role").cloned().unwrap_or(Value::Null),"binding_id":registration.get("binding_id").cloned().unwrap_or(Value::Null),"binding_generation":registration.get("binding_generation").cloned().unwrap_or(Value::Null)})
}

/// Structured reference to an original delivery, projected from its recorded
/// result. Records written before delivery identity existed carry neither a
/// delivery_id nor a payload digest; the reference projects explicit nulls
/// for them — nothing is reconstructed or fabricated.
pub fn message_reply_reference(original_result: &Value) -> Value {
    json!({"delivery_id":original_result.get("delivery_id").cloned().unwrap_or(Value::Null),"payload_digest":original_result.get("payload_digest").cloned().unwrap_or(Value::Null)})
}

/// A caller-supplied digest claim about an original delivery must match the
/// recorded digest. A claim against a record that predates digest recording
/// cannot be verified and is rejected rather than silently dropped.
pub fn verify_payload_digest_claim(recorded: Option<&str>, claimed: Option<&str>) -> Result<()> {
    match (recorded, claimed) {
        (_, None) => Ok(()),
        (Some(recorded), Some(claimed)) if recorded == claimed => Ok(()),
        (Some(_), Some(_)) => Err(Error::new(
            "DIGEST_MISMATCH",
            "payload digest does not match the original delivery",
        )),
        (None, Some(_)) => Err(Error::new(
            "DIGEST_UNVERIFIABLE",
            "the original delivery has no recorded payload digest; the claimed digest cannot be verified",
        )),
    }
}

/// An optional epoch-ms deadline supplied by a call contract: absent or null
/// stays an explicit null, a supplied value must be a positive integer.
/// Deadlines are recorded facts only — none is ever invented here, and
/// nothing in the mailbox acts on them.
pub fn deadline(value: &Value, field: &str) -> Result<Value> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(Value::Null),
        Some(_) => Ok(json!(positive(value, field)?)),
    }
}
pub fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::invalid(format!("{field} must be a nonempty string")))
}
pub fn positive(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .filter(|n| *n > 0)
        .ok_or_else(|| Error::invalid(format!("{field} must be a positive integer")))
}
pub fn fields(value: &Value, allowed: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("params must be an object"))?;
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(Error::invalid(format!("unknown field: {key}")));
        }
    }
    Ok(())
}

pub(crate) const MAX_MANAGER_EVENT_NAME_BYTES: usize = 256;
pub(crate) const MAX_MANAGER_EVENT_PROJECT_BYTES: usize = 128;
pub(crate) const MAX_MANAGER_EVENT_DEDUPE_BYTES: usize = 256;
pub(crate) const MAX_MANAGER_EVENT_PAYLOAD_BYTES: usize = 16 * 1024;
pub(crate) const MAX_MANAGER_EVENT_CAUSE_BYTES: usize = 4 * 1024;

fn bounded_event_text<'a>(
    value: &'a Value,
    field: &str,
    max_bytes: usize,
    allowed: impl FnMut(u8) -> bool,
) -> Result<&'a str> {
    let text = text(value, field)?;
    if text.len() > max_bytes || !text.bytes().all(allowed) {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes with the supported event identity characters"
        )));
    }
    Ok(text)
}

fn validate_manager_event_mutation(params: &Value) -> Result<()> {
    fields(
        params,
        &[
            "client_request_id",
            "project_id",
            "name",
            "payload",
            "dedupe_key",
            "cause",
        ],
    )?;
    let request_id = text(params, "client_request_id")?;
    if request_id.len() > 128
        || request_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(
            "client_request_id must be 1..=128 bytes without whitespace",
        ));
    }
    bounded_event_text(
        params,
        "project_id",
        MAX_MANAGER_EVENT_PROJECT_BYTES,
        |byte| !byte.is_ascii_control() && !byte.is_ascii_whitespace(),
    )?;
    bounded_event_text(params, "name", MAX_MANAGER_EVENT_NAME_BYTES, |byte| {
        byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte)
    })?;
    bounded_event_text(
        params,
        "dedupe_key",
        MAX_MANAGER_EVENT_DEDUPE_BYTES,
        |byte| !byte.is_ascii_control() && !byte.is_ascii_whitespace(),
    )?;
    let payload = params
        .get("payload")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::invalid("payload must be a non-null JSON value"))?;
    if canonical(payload)?.len() > MAX_MANAGER_EVENT_PAYLOAD_BYTES {
        return Err(Error::invalid("event payload exceeds its storage bound"));
    }
    if let Some(cause) = params.get("cause").filter(|value| !value.is_null()) {
        if !cause.is_object() {
            return Err(Error::invalid(
                "cause must be an object containing an existing Operation or observation reference",
            ));
        }
        fields(cause, &["operation_id", "observation_id"])?;
        let has_operation = cause
            .get("operation_id")
            .is_some_and(|value| !value.is_null());
        let has_observation = cause
            .get("observation_id")
            .is_some_and(|value| !value.is_null());
        if !has_operation && !has_observation {
            return Err(Error::invalid(
                "cause must name an existing operation_id or observation_id",
            ));
        }
        if cause
            .get("operation_id")
            .is_some_and(|value| !value.is_null())
        {
            bounded_event_text(cause, "operation_id", 128, |byte| {
                !byte.is_ascii_control() && !byte.is_ascii_whitespace()
            })?;
        }
        if cause
            .get("observation_id")
            .is_some_and(|value| !value.is_null())
        {
            positive(cause, "observation_id")?;
        }
        if canonical(cause)?.len() > MAX_MANAGER_EVENT_CAUSE_BYTES {
            return Err(Error::invalid("event cause exceeds its storage bound"));
        }
    }
    Ok(())
}

/// Validate the closed request shapes for the two automation configuration
/// reads. `owner_manager_id` is an optional read selector; authorization for
/// selecting another owner's scope remains in the Store handler.
pub fn validate_automation_config_read(method: &str, params: &Value) -> Result<()> {
    let allowed = match method {
        "automation.config.get" => &["project_id", "owner_manager_id", "after", "limit"][..],
        "automation.config.explain" => &["project_id", "automation_id", "owner_manager_id"][..],
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    fields(params, allowed)?;
    text(params, "project_id")?;
    if method == "automation.config.explain" {
        text(params, "automation_id")?;
    }
    if params.get("owner_manager_id").is_some() {
        let owner = text(params, "owner_manager_id")?;
        if owner.len() > 128
            || owner
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(Error::invalid(
                "owner_manager_id must be 1..=128 bytes without whitespace",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Operator,
    Manager,
    /// A setup-issued, repository-scoped hook credential. Store admits only
    /// the hook ingress/readback methods for this identity.
    HookSource,
    /// Assignment-bound coordination only; never a generic writer identity.
    Participant,
    Observer,
    Module,
    /// Host-issued, local supervisor identity with only descriptor registration authority.
    ModuleSupervisor,
    /// In-process schedule admission only; never an authenticatable client.
    Scheduler,
}
pub const INTERNAL_SCHEDULER_CLIENT_ID: &str = "eliot-internal-scheduler-v1";
pub const INTERNAL_MODULE_SUPERVISOR_CLIENT_ID: &str = "eliot-module-supervisor-v1";
pub use swarm_contracts::credential::Credential;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    /// Ephemeral authenticated transport identity, never a durable client ID.
    pub link_id: String,
    pub client_id: String,
    pub role: Role,
}
impl Principal {
    pub fn require_operator(&self) -> Result<()> {
        if self.role != Role::Operator {
            return Err(Error::new("FORBIDDEN", "operator authority required"));
        }
        Ok(())
    }
    pub fn require_writer(&self) -> Result<()> {
        if matches!(
            self.role,
            Role::HookSource | Role::Observer | Role::Participant | Role::ModuleSupervisor
        ) {
            return Err(Error::new(
                "FORBIDDEN",
                "this role has no generic writer authority",
            ));
        }
        Ok(())
    }
    pub fn require_participant(&self) -> Result<()> {
        if self.role != Role::Participant {
            return Err(Error::new(
                "FORBIDDEN",
                "assignment-bound participant authority required",
            ));
        }
        Ok(())
    }
    pub fn owns(&self, owner: &str) -> Result<()> {
        if self.role == Role::Operator
            || (self.role == Role::Scheduler && self.client_id == INTERNAL_SCHEDULER_CLIENT_ID)
            || (self.role == Role::Manager && self.client_id == owner)
        {
            Ok(())
        } else {
            Err(Error::new(
                "FORBIDDEN",
                "this attempt belongs to another client",
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub id: String,
    pub statement: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub task_id: String,
    pub required_revision: i64,
    pub required_phase: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    #[serde(default)]
    pub initial_paths: Vec<String>,
    #[serde(default)]
    pub forbidden_paths: Vec<String>,
    #[serde(default)]
    pub prerequisite_policy: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceIndexStatus {
    Selected,
    Gap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSourceIndexEntry {
    pub source_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub status: SourceIndexStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_reason: Option<String>,
}

impl TaskSourceIndexEntry {
    pub fn validate(&self) -> Result<()> {
        let value = serde_json::to_value(self)?;
        swarm_kernel::tasks::validate_source_index_entry(&value).map_err(task_validation_error)
    }

    fn legacy_gap(source_ref: &str) -> Self {
        Self {
            source_ref: source_ref.to_owned(),
            revision: None,
            content_sha256: None,
            text: None,
            status: SourceIndexStatus::Gap,
            gap_reason: Some("legacy_source_ref_without_pinned_revision_or_content".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskBrief {
    pub objective: String,
    pub phase: String,
    pub requirements: Vec<Requirement>,
    pub dependencies: Vec<Dependency>,
    pub scope: Option<Scope>,
    pub acceptance: Option<crate::acceptance::AcceptancePolicy>,
    pub owner_policy_id: Option<String>,
    pub source_index: Vec<TaskSourceIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    /// Absent policy does not block writing/submitting, but cannot imply acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance: Option<crate::acceptance::AcceptancePolicy>,
    pub objective: String,
    pub phase: String,
    pub requirements: Vec<Requirement>,
    #[serde(default)]
    pub dependencies: Vec<Dependency>,
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_index: Vec<TaskSourceIndexEntry>,
    /// Exact accepted source candidate to use as a CheckRunner reverse-scope baseline.
    /// Claim freezes the verified acceptance identity; absent or unproven input widens scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_candidate_ref: Option<String>,
}
impl TaskSpec {
    pub fn brief(&self) -> TaskBrief {
        let mut source_index = self.source_index.clone();
        for entry in &mut source_index {
            if entry.status == SourceIndexStatus::Gap
                && entry.content_sha256.is_none()
                && let Some(text) = entry.text.as_deref()
            {
                entry.content_sha256 = Some(digest(text.as_bytes()));
            }
        }
        let mut indexed_refs: BTreeSet<String> = source_index
            .iter()
            .map(|entry| entry.source_ref.clone())
            .collect();
        for source_ref in &self.source_refs {
            if indexed_refs.insert(source_ref.clone()) {
                source_index.push(TaskSourceIndexEntry::legacy_gap(source_ref));
            }
        }
        TaskBrief {
            objective: self.objective.clone(),
            phase: self.phase.clone(),
            requirements: self.requirements.clone(),
            dependencies: self.dependencies.clone(),
            scope: self.scope.clone(),
            acceptance: self.acceptance.clone(),
            owner_policy_id: self.owner_policy_id.clone(),
            source_index,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(policy) = &self.acceptance {
            policy.validate()?;
        }
        let value = serde_json::to_value(self)?;
        swarm_kernel::tasks::validate_spec(&value).map_err(task_validation_error)
    }
}

fn task_validation_error(error: swarm_kernel::tasks::ValidationError) -> Error {
    Error::new(error.code(), error.message())
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartOwner {
    Controller,
    NativeManager,
}
impl StartOwner {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Controller => "controller",
            Self::NativeManager => "native_manager",
        }
    }
}

pub use swarm_contracts::rpc::Request;
pub fn response(id: Value, result: Result<Value>) -> Value {
    match result {
        Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
        Err(e) => {
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":e.message,"data":{"code":e.code}}})
        }
    }
}

/// Reject malformed/unknown envelopes before storing the original request. In
/// particular, an accidental client.hello/token must never become a receipt.
pub fn validate_mutation(method: &str, params: &Value) -> Result<()> {
    // Store, direct CLI, and automation admissions share the positive class
    // table. Read/facade/unknown names never acquire a writer default.
    if !matches!(
        swarm_contracts::method_policy::class(method),
        Some(
            swarm_contracts::method_policy::MethodClass::Mutation
                | swarm_contracts::method_policy::MethodClass::Internal
        )
    ) {
        return Err(Error::new("METHOD_NOT_FOUND", method));
    }
    let allowed: &[&str] = match method {
        "concilium.propose"
        | "concilium.open"
        | "concilium.position.submit"
        | "concilium.round.advance"
        | "concilium.close" => {
            crate::coordination::concilium::validate(method, params)?;
            return Ok(());
        }
        "swarm.launch" => {
            crate::launcher::LaunchRequest::parse(params)?;
            return Ok(());
        }
        "coordination.sync_integration" => {
            crate::coordination::integration::SyncRequest::parse(params)?;
            return Ok(());
        }
        "coordination.integration.ack" => {
            crate::coordination::integration::AckRequest::parse(params)?;
            return Ok(());
        }
        "coordination.watch.create" | "coordination.watch.cancel" => {
            crate::coordination::watch::validate_mutation(method, params)?;
            return Ok(());
        }
        "coordination.thread.open"
        | "coordination.message.send"
        | "coordination.thread.resolve"
        | "coordination.thread.withdraw"
        | "coordination.thread.supersede" => {
            crate::coordination::thread::validate_mutation(method, params)?;
            return Ok(());
        }
        "code.scope.propose" | "code.scope.accept" | "code.scope.release" => {
            crate::coordination::code_scope::validate_mutation(method, params)?;
            return Ok(());
        }
        "bus.consumer.register" | "bus.consumer.revoke" | "bus.consumer.admit" => {
            crate::store::bus_kernel::validate_mutation(method, params)?;
            return Ok(());
        }
        "coordination.participant.register"
        | "coordination.participant.disable"
        | "coordination.work_card.publish"
        | "coordination.work_card.withdraw"
        | "coordination.contract_card.publish"
        | "coordination.contract_card.withdraw"
        | "coordination.contract.propose"
        | "coordination.contract.respond"
        | "coordination.contract.ratify"
        | "coordination.contract.reject"
        | "coordination.send"
        | "coordination.consult" => {
            crate::coordination::validate_mutation(method, params)?;
            return Ok(());
        }
        "review.assign" => {
            crate::review::ReviewAssignRequest::parse(params)?;
            return Ok(());
        }
        "review.submit" => {
            crate::review::ReviewSubmitRequest::parse(params)?;
            return Ok(());
        }
        "event.emit" => {
            validate_manager_event_mutation(params)?;
            return Ok(());
        }
        "automation.config.apply" => {
            crate::automation::config::parse_request(params, true)?;
            text(params, "client_request_id")?;
            return Ok(());
        }
        "automation.config.transfer" => {
            crate::automation::config::TransferRequest::parse(params)?;
            text(params, "client_request_id")?;
            return Ok(());
        }
        "script.register" => {
            crate::scripts::protocol::RegisterRequest::parse(params)?;
            return Ok(());
        }
        "script.revise" => {
            crate::scripts::protocol::ReviseRequest::parse(params)?;
            return Ok(());
        }
        "script.activate" => {
            crate::scripts::protocol::ActivateRequest::parse(params)?;
            return Ok(());
        }
        "script.run" => {
            crate::scripts::protocol::RunRequest::parse(params)?;
            return Ok(());
        }
        "hook.source.setup" => {
            crate::hooks::contract::HookSetupRequest::parse(params)?;
            return Ok(());
        }
        "github.source.setup"
        | "github.source.poll"
        | "github.work_pool.apply"
        | "github.effect.managed_label"
        | "github.effect.reconcile_managed_label"
        | "github.pull_request.update_description"
        | "github.pull_request.reconcile_description" => {
            crate::github::protocol::validate_mutation(method, params)?;
            return Ok(());
        }
        "hook.source.revoke" => &["client_request_id", "source_id", "expected_revision"],
        "goal.create" | "goal.revise" | "goal.enable" | "goal.disable" | "goal.readback" => {
            crate::goals::validate_mutation(method, params)?;
            return Ok(());
        }
        "forge.publish_ref" => {
            crate::forge::PublishRefRequest::parse(params)?;
            return Ok(());
        }
        "source.capture" => {
            crate::checks::model::CaptureRequest::parse(params)?;
            return Ok(());
        }
        "check.run" => {
            crate::checks::model::CheckRequest::parse(params)?;
            return Ok(());
        }
        "schedule.run_now" => {
            fields(
                params,
                &["client_request_id", "project_id", "automation_id"],
            )?;
            let request_id = text(params, "client_request_id")?;
            if request_id.len() > 128
                || request_id
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::invalid(
                    "client_request_id must be 1..=128 bytes without whitespace",
                ));
            }
            text(params, "project_id")?;
            text(params, "automation_id")?;
            return Ok(());
        }
        "check.cancel" => &["client_request_id", "check_id", "reason"],

        "logging.set" => &[
            "client_request_id",
            "client_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "operation_id",
            "binding_id",
            "binding_generation",
            "module_id",
            "level",
            "content",
            "ttl_seconds",
        ],

        "artifact.assemble" => &["client_request_id", "page_refs", "expected_sha256"],
        "task.submit.recover" => &["client_request_id", "operation_id"],
        "task.submit" => &[
            "client_request_id",
            "attempt_id",
            "expected_revision",
            "expected_submission_ref",
            "candidate_ref",
            "summary",
            "claims",
        ],
        "task.accept" => &[
            "client_request_id",
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "expected_feedback_observation_id",
            "reason",
            "reviews",
            "check_ids",
        ],
        "task.invalidate_acceptance" => &[
            "client_request_id",
            "acceptance_operation_id",
            "reason",
            "evidence",
        ],
        "task.request_changes" => &[
            "client_request_id",
            "schema_version",
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "review_assignment_id",
            "review_result_operation_id",
            "finding_ids",
        ],
        "task.create" => &["client_request_id", "project_id", "origin_key", "spec"],
        "task.revise" => &["client_request_id", "task_id", "expected_revision", "spec"],
        "task.claim" => &[
            "client_request_id",
            "task_id",
            "expected_revision",
            "owner_id",
            "start_owner",
            "binding_id",
            "binding_generation",
        ],
        "task.dispatch" => &[
            "client_request_id",
            "attempt_id",
            "launch_operation_id",
            "text",
            "prerequisite_operation_id",
        ],
        "attempt.bind_producer" => &[
            "client_request_id",
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
            "observation_id",
        ],
        "attempt.release" => &[
            "client_request_id",
            "attempt_id",
            "outcome",
            "reason",
            "assignment_closed",
        ],
        "agent.open" => &["client_request_id", "lane_id", "route"],
        "agent.send" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "text",
            "delivery",
            "expected_turn_id",
            "prerequisite_operation_id",
        ],
        "agent.result" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "selector",
            "offset_bytes",
            "length_bytes",
        ],
        "agent.reply" => &["client_request_id", "binding_id", "generation", "reply"],
        "native.opencode.loop_step" => {
            fields(
                params,
                &["client_request_id", "binding_id", "generation", "text"],
            )?;
            text(params, "client_request_id")?;
            text(params, "binding_id")?;
            positive(params, "generation")?;
            text(params, "text")?;
            return Ok(());
        }
        "native.command.cancel_turn" => {
            fields(
                params,
                &[
                    "client_request_id",
                    "binding_id",
                    "generation",
                    "session_id",
                    "target_operation_id",
                    "turn_id",
                ],
            )?;
            text(params, "client_request_id")?;
            text(params, "binding_id")?;
            positive(params, "generation")?;
            text(params, "session_id")?;
            text(params, "target_operation_id")?;
            text(params, "turn_id")?;
            return Ok(());
        }
        "native.command.close_session" => {
            fields(
                params,
                &[
                    "client_request_id",
                    "binding_id",
                    "generation",
                    "session_id",
                    "target_operation_id",
                ],
            )?;
            text(params, "client_request_id")?;
            text(params, "binding_id")?;
            positive(params, "generation")?;
            text(params, "session_id")?;
            text(params, "target_operation_id")?;
            return Ok(());
        }
        "agent.configure" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "settings",
            "prerequisite_operation_id",
        ],
        "agent.goal" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "action",
            "objective",
            "expected_revision",
            "prerequisite_operation_id",
        ],
        "agent.background" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "session_id",
        ],
        "agent.refresh" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "session_id",
        ],
        "agent.recover" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "expected_boot_id",
            "reason",
        ],
        "agent.reconcile" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "operation_id",
        ],
        "module.route.select" => &[
            "client_request_id",
            "route_alias",
            "module_id",
            "artifact_id",
            "version",
            "expected_catalog_revision",
        ],
        "operation.cancel" => &["client_request_id", "operation_id", "reason"],
        "host.mode" => &["client_request_id", "new_work"],
        "gm.handover" => &[
            "client_request_id",
            "client_id",
            "binding_id",
            "binding_generation",
        ],
        "client.register" => &[
            "client_request_id",
            "client_id",
            "role",
            "token_hash",
            "binding_id",
            "binding_generation",
        ],
        "hook.emit" => {
            fields(params, &["source_id", "commit_oid"])?;
            text(params, "source_id")?;
            let commit_oid = text(params, "commit_oid")?;
            if !crate::forge::valid_object_id(commit_oid) {
                return Err(Error::invalid("commit_oid must be a full Git object ID"));
            }
            return Ok(());
        }
        "message.send" => &[
            "client_request_id",
            "recipient",
            "text",
            "in_reply_to",
            "in_reply_to_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
        ],
        "message.cancel" => &[
            "client_request_id",
            "delivery_id",
            "payload_digest",
            "reason",
        ],
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    fields(params, allowed)?;
    if method == "logging.set" {
        match text(params, "content")? {
            "metadata" | "redacted_text" => {}
            "native_frames" => {
                return Err(Error::new(
                    "LOGGING_CONTENT_UNSUPPORTED",
                    "no bounded native-frame producer is available",
                ));
            }
            _ => return Err(Error::invalid("content must be metadata or redacted_text")),
        }
    }
    match method {
        "hook.source.revoke" => {
            text(params, "source_id")?;
            positive(params, "expected_revision")?;
        }
        "task.accept" => {
            crate::acceptance::AcceptRequest::parse(params)?;
        }
        "task.invalidate_acceptance" => {
            crate::acceptance::InvalidateRequest::parse(params)?;
        }
        "task.submit" => {
            crate::submission::SubmitRequest::parse(params)?;
        }
        "task.submit.recover" => {
            text(params, "operation_id")?;
        }
        "task.request_changes" => {
            crate::submission::ChangeRequestV2::parse(params)?;
        }
        "module.route.select" => {
            for field in ["route_alias", "module_id", "artifact_id", "version"] {
                text(params, field)?;
            }
            if params["expected_catalog_revision"].as_u64().is_none() {
                return Err(Error::invalid(
                    "expected_catalog_revision must be a nonnegative integer",
                ));
            }
        }
        _ => {}
    }
    text(params, "client_request_id")?;
    if method == "message.send" {
        for field in [
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
        ] {
            if let Some(value) = params.get(field)
                && !value.is_null()
            {
                positive(params, field)?;
            }
        }
        if let Some(value) = params.get("in_reply_to_digest")
            && !value.is_null()
        {
            text(params, "in_reply_to_digest")?;
        }
    }
    if method == "message.cancel" {
        text(params, "delivery_id")?;
        text(params, "payload_digest")?;
        if let Some(value) = params.get("reason")
            && !value.is_null()
        {
            text(params, "reason")?;
        }
    }
    for field in ["prerequisite_operation_id", "launch_operation_id"] {
        if params.get(field).is_some() {
            let operation_id = text(params, field)?;
            if operation_id.is_empty()
                || operation_id.len() > 128
                || operation_id
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
            {
                return Err(Error::invalid(format!("invalid {field}")));
            }
        }
    }
    if matches!(method, "task.create" | "task.revise") {
        let spec: TaskSpec = serde_json::from_value(params["spec"].clone())?;
        spec.validate()?;
    }
    if method == "artifact.assemble" {
        let request: crate::artifacts::AssemblyRequest = serde_json::from_value(params.clone())?;
        request.validate()?;
    }
    if method == "agent.result" {
        positive(params, "generation")?;
        for field in ["offset_bytes", "length_bytes"] {
            if let Some(value) = params.get(field) {
                let n = value.as_u64().ok_or_else(|| {
                    Error::invalid(format!("{field} must be a nonnegative integer"))
                })?;
                if field == "length_bytes"
                    && (n == 0 || n > crate::artifacts::MAX_PAGE_BYTES as u64)
                {
                    return Err(Error::invalid("length_bytes must be 1..65536"));
                }
            }
        }

        if params["selector"].as_object().is_none_or(|o| o.is_empty())
            || canonical(&params["selector"])?.len() > 8192
        {
            return Err(Error::invalid(
                "result selector must be a compact nonempty object",
            ));
        }
    }
    if method == "attempt.bind_producer" {
        for field in [
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
        ] {
            text(params, field)?;
        }
        positive(params, "observation_id")?;
    }
    for field in [
        "owner_id",
        "origin_key",
        "binding_id",
        "in_reply_to",
        "session_id",
    ] {
        if let Some(value) = params.get(field)
            && !value.is_null()
        {
            text(params, field)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_payload_digest_uses_the_canonical_payload() {
        // The canonical payload is exactly {recipient, sender, text} with
        // recursively sorted keys, so the digest is a plain SHA-256 of it.
        let expected = digest(b"{\"recipient\":\"bob\",\"sender\":\"alice\",\"text\":\"hello\"}");
        assert_eq!(
            message_payload_digest("alice", "bob", "hello").unwrap(),
            expected
        );
        // Both parties and the text are bound: changing any of them changes
        // the digest a reply or cancellation must cite.
        assert_ne!(
            message_payload_digest("alice", "bob", "hello").unwrap(),
            message_payload_digest("alice", "carol", "hello").unwrap()
        );
        assert_ne!(
            message_payload_digest("alice", "bob", "hello").unwrap(),
            message_payload_digest("alice", "bob", "hello!").unwrap()
        );
    }

    #[test]
    fn digest_claims_are_checked_against_recorded_evidence() {
        assert!(verify_payload_digest_claim(None, None).is_ok());
        assert!(verify_payload_digest_claim(Some("abc"), None).is_ok());
        assert!(verify_payload_digest_claim(Some("abc"), Some("abc")).is_ok());
        let mismatch = verify_payload_digest_claim(Some("abc"), Some("def")).unwrap_err();
        assert_eq!(mismatch.code, "DIGEST_MISMATCH");
        let unverifiable = verify_payload_digest_claim(None, Some("abc")).unwrap_err();
        assert_eq!(unverifiable.code, "DIGEST_UNVERIFIABLE");
    }

    #[test]
    fn reply_reference_projects_unknowns_for_legacy_records() {
        let current = json!({"delivery_id":"d-1","payload_digest":"abc"});
        assert_eq!(
            message_reply_reference(&current),
            json!({"delivery_id":"d-1","payload_digest":"abc"})
        );
        // A record written before delivery identity existed: explicit nulls,
        // never a reconstructed delivery_id or digest.
        let legacy =
            json!({"operation_id":"op-1","message_id":"op-1","sender":"alice","recipient":"bob"});
        assert_eq!(
            message_reply_reference(&legacy),
            json!({"delivery_id":Value::Null,"payload_digest":Value::Null})
        );
    }

    #[test]
    fn actor_and_scope_take_generation_from_the_registration() {
        let module = json!({"role":"module","binding_id":"b-1","binding_generation":3});
        assert_eq!(
            message_actor(&module, "mod-1"),
            json!({"client_id":"mod-1","role":"module","generation":3})
        );
        assert_eq!(
            message_scope(&module, "mod-1"),
            json!({"client_id":"mod-1","role":"module","binding_id":"b-1","binding_generation":3})
        );
        let plain = json!({"role":"manager"});
        assert_eq!(
            message_actor(&plain, "alice"),
            json!({"client_id":"alice","role":"manager","generation":Value::Null})
        );
        assert_eq!(
            message_scope(&plain, "alice"),
            json!({"client_id":"alice","role":"manager","binding_id":Value::Null,"binding_generation":Value::Null})
        );
    }

    #[test]
    fn deadlines_are_explicit_nulls_unless_the_caller_supplies_one() {
        let none = json!({"text":"x"});
        assert_eq!(deadline(&none, "reply_deadline_ms").unwrap(), Value::Null);
        let explicit_null = json!({"reply_deadline_ms":Value::Null});
        assert_eq!(
            deadline(&explicit_null, "reply_deadline_ms").unwrap(),
            Value::Null
        );
        let supplied = json!({"reply_deadline_ms":4102444800000i64});
        assert_eq!(
            deadline(&supplied, "reply_deadline_ms").unwrap(),
            json!(4102444800000i64)
        );
        assert!(deadline(&json!({"reply_deadline_ms":0}), "reply_deadline_ms").is_err());
        assert!(deadline(&json!({"reply_deadline_ms":"soon"}), "reply_deadline_ms").is_err());
    }
}
