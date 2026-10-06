//! Typed authenticated control plane used by the independent supervisor.
//!
//! The supervisor never receives a Store handle, database connection, or
//! controller callback.  It asks the kernel host for bounded retained demand,
//! credential/readiness evidence, admission state, recovery reconciliation,
//! and status delivery through the existing local IPC transport.  The host
//! remains the authorization and transaction boundary.

use crate::{AdmissionState, ModuleDescriptor, ModuleSupervisorObservation};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::path::PathBuf;
use swarm_client::IpcConfig;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_catalog::{ModuleId, ProtectedRef, ProtocolVersion},
};

const MODULE_DESCRIPTOR_REGISTER: &str = "module.descriptor.register";
const SUPERVISOR_ADMISSION: &str = "module.supervisor.admission";
const SUPERVISOR_DEMAND_PAGE: &str = "module.supervisor.demand.page";
const SUPERVISOR_SCOPE_READBACK: &str = "module.supervisor.scope.readback";
const SUPERVISOR_CREDENTIAL_ENSURE: &str = "module.supervisor.credential.ensure";
const SUPERVISOR_CREDENTIAL_READY: &str = "module.supervisor.credential.ready";
const SUPERVISOR_RECOVERY_RECONCILE: &str = "module.supervisor.recovery.reconcile";
const SUPERVISOR_OBSERVATION_RECORD: &str = "module.supervisor.observation.record";
const SUPERVISOR_HEALTH_RECORD: &str = "module.supervisor.health.record";

/// Status-only Operation evidence returned by the host.  Inputs, outputs,
/// caller identities, and native payloads never cross this boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredOperation {
    pub operation_id: String,
    pub method: String,
    pub binding_id: String,
    pub generation: u64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDemandRecord {
    pub descriptor: ModuleDescriptor,
    pub descriptor_revision: u64,
    pub module_id: String,
    pub artifact_id: String,
    pub artifact_version: String,
    pub operation_id: String,
    pub required_capability: String,
    pub binding_id: String,
    pub generation: u64,
    pub module_client_id: Option<String>,
    pub credential_ref: Option<String>,
    pub route_native_options: Value,
    pub operation_readback: Vec<StoredOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDemandBlock {
    pub binding_id: String,
    pub generation: u64,
    pub operation_id: String,
    pub error_code: String,
    pub descriptor: Option<ModuleDescriptor>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDemandPage {
    pub demands: Vec<ModuleDemandRecord>,
    pub blocked: Vec<ModuleDemandBlock>,
    pub truncated: bool,
    pub next_cursor: Option<ModuleDemandCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDemandCursor {
    pub created_at_ms: i64,
    pub operation_id: String,
}

/// Complete status-only readback for one exact binding generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleScopeReadback {
    pub operations: Vec<StoredOperation>,
    pub native_identity_retained: bool,
    /// The exact module boot accepted by Store's existing module.hello state,
    /// when this binding has one. A missing or stale value never proves that
    /// the currently launched worker completed its hello.
    #[serde(default)]
    pub module_hello_boot_id: Option<String>,
}

/// Host proof that the exact binding credential is prepared and registered.
/// Token bytes and token hashes are intentionally absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleBindingCredential {
    pub operation_id: String,
    pub binding_id: String,
    pub generation: i64,
    pub module_id: ModuleId,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub descriptor_revision: u64,
    pub protocol: ProtocolVersion,
    pub module_client_id: String,
    pub credential_ref: ProtectedRef,
    pub credential_file: PathBuf,
    pub credential_file_sha256: String,
    pub ready: bool,
}

/// The host-owned process receipt for the independently launched supervisor.
/// The platform identity objects are copied exactly from swarm-process after
/// their live PID/birth/image checks; this envelope only adds the bounded
/// authenticated health boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorChildProcessIdentity {
    pub pid: u32,
    pub birth: Value,
    pub image: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorChildExitCategory {
    Exited,
    Signaled,
    WaitError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorChildExit {
    pub category: SupervisorChildExitCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorChildStopState {
    Running,
    NotRequested,
    Confirmed,
    Uncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorChildHealth {
    pub process: SupervisorChildProcessIdentity,
    pub stop: SupervisorChildStopState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<SupervisorChildExit>,
}

impl SupervisorChildHealth {
    pub fn validate(&self) -> Result<()> {
        self.process.validate()?;
        if let Some(exit) = &self.exit {
            exit.validate()?;
        }
        match self.stop {
            SupervisorChildStopState::Running if self.exit.is_some() => {
                Err(Error::invalid("running supervisor child has an exit receipt"))
            }
            SupervisorChildStopState::Confirmed if self.exit.is_none() => {
                Err(Error::invalid("confirmed supervisor stop has no exit receipt"))
            }
            SupervisorChildStopState::NotRequested if self.exit.is_none() => {
                Err(Error::invalid("departed supervisor child has no exit receipt"))
            }
            _ => Ok(()),
        }
    }
}

impl SupervisorChildProcessIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.pid == 0
            || serde_json::to_vec(&self.birth)
                .map(|bytes| bytes.len() > 2048)
                .unwrap_or(true)
            || serde_json::to_vec(&self.image)
                .map(|bytes| bytes.len() > 4096)
                .unwrap_or(true)
            || !valid_child_birth(&self.birth, self.pid)
            || !valid_child_image(&self.image, self.pid)
        {
            return Err(Error::invalid(
                "supervisor child process identity is outside its bounded receipt shape",
            ));
        }
        Ok(())
    }
}

impl SupervisorChildExit {
    fn validate(&self) -> Result<()> {
        let valid_code = self
            .error_code
            .as_deref()
            .is_none_or(valid_health_code);
        let valid_category = match self.category {
            SupervisorChildExitCategory::Exited => self.code.is_some(),
            SupervisorChildExitCategory::Signaled | SupervisorChildExitCategory::WaitError => {
                self.code.is_none()
            }
        };
        if !valid_code || !valid_category {
            return Err(Error::invalid("supervisor child exit receipt is invalid"));
        }
        Ok(())
    }
}

fn valid_health_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn exact_child_keys(value: &Value, keys: &[&str]) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
}

fn positive_decimal(value: &Value, maximum: usize) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    if value.len() > maximum || value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let Ok(number) = value.parse::<u64>() else {
        return false;
    };
    number > 0 && number.to_string() == value
}

fn valid_child_uuid(value: &Value) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| value.as_bytes()[index] == b'-')
        && value.bytes().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit()
        })
}

fn valid_child_birth(value: &Value, pid: u32) -> bool {
    let Some(platform) = value.get("platform").and_then(Value::as_str) else {
        return false;
    };
    let pid_matches = value.get("pid").and_then(Value::as_u64) == Some(u64::from(pid));
    match platform {
        "windows" => {
            exact_child_keys(value, &["platform", "pid", "creation_filetime"])
                && pid_matches
                && positive_decimal(&value["creation_filetime"], 64)
        }
        "linux" => {
            exact_child_keys(value, &["platform", "pid", "boot_id", "start_ticks"])
                && pid_matches
                && valid_child_uuid(&value["boot_id"])
                && positive_decimal(&value["start_ticks"], 64)
        }
        _ => false,
    }
}

fn valid_child_image(value: &Value, pid: u32) -> bool {
    let pid_matches = value.get("pid").and_then(Value::as_u64) == Some(u64::from(pid));
    let path_valid = value
        .get("image_path")
        .and_then(Value::as_str)
        .is_some_and(|path| !path.is_empty() && path.len() <= 4096 && !path.chars().any(char::is_control));
    let digest_valid = value
        .get("image_sha256")
        .and_then(Value::as_str)
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    if !pid_matches || !path_valid || !digest_valid {
        return false;
    }
    if value.get("start_ticks").is_some() || value.get("boot_id").is_some() {
        exact_child_keys(
            value,
            &["pid", "boot_id", "image_path", "image_sha256", "start_ticks"],
        ) && valid_child_uuid(&value["boot_id"])
            && positive_decimal(&value["start_ticks"], 64)
    } else {
        exact_child_keys(
            value,
            &["pid", "creation_filetime", "image_path", "image_sha256"],
        ) && positive_decimal(&value["creation_filetime"], 64)
    }
}

/// Client for the host-owned supervisor scope.  Every method opens one
/// authenticated transport exchange; the underlying client never replays a
/// request after an uncertain write or reply.
#[derive(Clone)]
pub struct SupervisorControlClient {
    root: PathBuf,
    credential: Credential,
    ipc: IpcConfig,
}

impl SupervisorControlClient {
    pub fn new(root: PathBuf, credential: Credential, ipc: IpcConfig) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::invalid(
                "module supervisor IPC root must be an absolute path",
            ));
        }
        Ok(Self {
            root,
            credential,
            ipc,
        })
    }

    pub async fn register_descriptor(&self, descriptor: &ModuleDescriptor) -> Result<Value> {
        self.request_value(
            MODULE_DESCRIPTOR_REGISTER,
            json!({"descriptor": descriptor}),
        )
        .await
    }

    pub async fn admission(&self) -> Result<AdmissionState> {
        self.request(SUPERVISOR_ADMISSION, json!({})).await
    }

    pub async fn demand_page(
        &self,
        cursor: Option<&ModuleDemandCursor>,
    ) -> Result<ModuleDemandPage> {
        self.request(SUPERVISOR_DEMAND_PAGE, json!({"cursor": cursor}))
            .await
    }

    pub async fn scope_readback(
        &self,
        module_id: &str,
        binding_id: &str,
        generation: i64,
    ) -> Result<ModuleScopeReadback> {
        self.request(
            SUPERVISOR_SCOPE_READBACK,
            json!({
                "module_id": module_id,
                "binding_id": binding_id,
                "generation": generation,
            }),
        )
        .await
    }

    pub async fn ensure_binding_credential(
        &self,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
    ) -> Result<ModuleBindingCredential> {
        self.request(
            SUPERVISOR_CREDENTIAL_ENSURE,
            json!({
                "operation_id": operation_id,
                "binding_id": binding_id,
                "generation": generation,
            }),
        )
        .await
    }

    pub async fn check_binding_credential_ready(
        &self,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
    ) -> Result<ModuleBindingCredential> {
        self.request(
            SUPERVISOR_CREDENTIAL_READY,
            json!({
                "operation_id": operation_id,
                "binding_id": binding_id,
                "generation": generation,
            }),
        )
        .await
    }

    pub async fn reconcile_recovery_page(&self) -> Result<()> {
        self.request_value(SUPERVISOR_RECOVERY_RECONCILE, json!({}))
            .await
            .map(|_| ())
    }

    pub async fn record_observation(
        &self,
        observation: &ModuleSupervisorObservation,
    ) -> Result<()> {
        self.request_value(
            SUPERVISOR_OBSERVATION_RECORD,
            json!({"observation": observation}),
        )
        .await
        .map(|_| ())
    }

    pub async fn record_health(
        &self,
        state: &str,
        consecutive_failures: u32,
        error_code: Option<&str>,
        retry_in_ms: Option<u64>,
    ) -> Result<()> {
        self.request_value(
            SUPERVISOR_HEALTH_RECORD,
            json!({
                "name": "module-supervisor",
                "state": state,
                "consecutive_failures": consecutive_failures,
                "error_code": error_code,
                "retry_in_ms": retry_in_ms,
            }),
        )
        .await
        .map(|_| ())
    }

    /// Record the existing bounded actor health plus an optional verified
    /// child process receipt. The legacy method above remains unchanged for
    /// supervisor-internal health updates that have no host child process.
    pub async fn record_health_with_child(
        &self,
        state: &str,
        consecutive_failures: u32,
        error_code: Option<&str>,
        retry_in_ms: Option<u64>,
        child: Option<&SupervisorChildHealth>,
    ) -> Result<()> {
        if let Some(child) = child {
            child.validate()?;
        }
        let child = child
            .map(|value| serde_json::to_value(value))
            .transpose()
            .map_err(|_| Error::invalid("supervisor child health receipt is not serializable"))?;
        self.request_value(
            SUPERVISOR_HEALTH_RECORD,
            json!({
                "name": "module-supervisor",
                "state": state,
                "consecutive_failures": consecutive_failures,
                "error_code": error_code,
                "retry_in_ms": retry_in_ms,
                "child": child,
            }),
        )
        .await
        .map(|_| ())
    }

    async fn request<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let value = self.request_value(method, params).await?;
        serde_json::from_value(value).map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_RESPONSE_INVALID",
                "supervisor control response did not match its bounded typed shape",
            )
        })
    }

    async fn request_value(&self, method: &str, params: Value) -> Result<Value> {
        swarm_client::call(&self.root, &self.credential, method, params, &self.ipc).await
    }
}
