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

/// Client for the host-owned supervisor scope.  Every method opens one
/// authenticated transport exchange; the underlying client never replays a
/// request after an uncertain write or reply.
#[derive(Debug, Clone)]
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
