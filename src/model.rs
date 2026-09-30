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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Operator,
    Manager,
    Observer,
    Module,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub client_id: String,
    pub token: String,
}
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
        if self.role == Role::Observer {
            return Err(Error::new("FORBIDDEN", "observer is read-only"));
        }
        Ok(())
    }
    pub fn owns(&self, owner: &str) -> Result<()> {
        if self.role == Role::Operator || (self.role == Role::Manager && self.client_id == owner) {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub objective: String,
    pub phase: String,
    pub requirements: Vec<Requirement>,
    #[serde(default)]
    pub dependencies: Vec<Dependency>,
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default)]
    pub source_refs: Vec<String>,
}
impl TaskSpec {
    pub fn validate(&self) -> Result<()> {
        if self.objective.trim().is_empty()
            || self.phase.trim().is_empty()
            || self.requirements.is_empty()
        {
            return Err(Error::invalid(
                "objective, phase and at least one requirement are required",
            ));
        }
        let mut ids = BTreeSet::new();
        for r in &self.requirements {
            if r.id.trim().is_empty() || r.statement.trim().is_empty() || !ids.insert(&r.id) {
                return Err(Error::invalid(
                    "requirement IDs must be nonempty and unique; statements cannot be empty",
                ));
            }
        }
        let mut deps = BTreeSet::new();
        for d in &self.dependencies {
            if d.task_id.trim().is_empty()
                || d.required_revision < 1
                || d.required_phase.trim().is_empty()
                || !deps.insert(&d.task_id)
            {
                return Err(Error::invalid(
                    "dependencies require unique task IDs, revision and phase",
                ));
            }
        }
        Ok(())
    }
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    #[serde(default = "empty_object")]
    pub params: Value,
}
fn empty_object() -> Value {
    json!({})
}
impl Request {
    pub fn validate(&self) -> Result<()> {
        if self.jsonrpc != "2.0"
            || self.id.is_empty()
            || self.method.is_empty()
            || !self.params.is_object()
        {
            return Err(Error::invalid(
                "expected JSON-RPC 2.0 with nonempty string id/method and object params",
            ));
        }
        Ok(())
    }
}
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
    let allowed: &[&str] = match method {
        "artifact.assemble" => &["client_request_id", "page_refs", "expected_sha256"],
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
        "task.dispatch" => &["client_request_id", "attempt_id", "text"],
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
        "agent.configure" => &["client_request_id", "binding_id", "generation", "settings"],
        "agent.goal" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "action",
            "objective",
        ],
        "agent.refresh" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "session_id",
        ],
        "agent.reconcile" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "operation_id",
        ],
        "operation.cancel" => &["client_request_id", "operation_id", "reason"],
        "host.mode" => &["client_request_id", "new_work"],
        "client.register" => &[
            "client_request_id",
            "client_id",
            "role",
            "token_hash",
            "binding_id",
            "binding_generation",
        ],
        "message.send" => &["client_request_id", "recipient", "text", "in_reply_to"],
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    fields(params, allowed)?;
    text(params, "client_request_id")?;
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
