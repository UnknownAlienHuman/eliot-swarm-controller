use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    credential::Credential,
    error::Result,
    runtime::{ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome},
};

use crate::{
    contract,
    controller::{Controller, PendingObservation},
    module_receipt,
    process::{CandidateManagedOwner, VerifiedManagedOwner},
    wire::ARTIFACT_ID,
};

pub struct VerifiedModuleHello {
    pub response: Value,
    pub managed_owner: VerifiedManagedOwner,
}

pub struct ManagerLink {
    client: Client,
}

impl ManagerLink {
    pub async fn connect(
        host_data_dir: &Path,
        credential: &Credential,
        ipc: &IpcConfig,
    ) -> Result<Self> {
        let client = Client::connect(host_data_dir, credential, ipc).await?;
        Ok(Self { client })
    }

    /// One manager application exchange. This method intentionally performs
    /// no retries; callers decide whether the saved payload is safe to replay.
    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.client.request(method, params).await
    }

    /// Send the module handshake through the existing authenticated client.
    /// The manager checks that `managed_owner` names a live local module group
    /// before recording this boot. A successful typed response is the only
    /// path to a spawner capability. Worker boot identity and process-group
    /// custody token remain separate from Task identity.
    pub async fn hello(
        &mut self,
        native_ready: bool,
        native_root_id: Option<&str>,
        native_scope_key: Option<&str>,
        managed_owner: CandidateManagedOwner,
    ) -> Result<VerifiedModuleHello> {
        let boot_id = managed_owner.boot_id().to_owned();
        let owner_record = managed_owner.record().clone();
        let module_contract = contract::claim()?;
        let response = self
            .client
            .request(
                "module.hello",
                json!({
                    "module_artifact_id": ARTIFACT_ID,
                    "boot_id": &boot_id,
                    "native_ready": native_ready,
                    "native_root_id": native_root_id,
                    "native_scope_key": native_scope_key,
                    "managed_owner": &owner_record,
                    "module_contract": &module_contract,
                }),
            )
            .await?;
        let binding_id = response
            .get("binding_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let generation = response.get("generation").and_then(Value::as_i64);
        let recovery_required = response.get("recovery_required").and_then(Value::as_bool);
        let native_root_id = optional_nonempty_text(&response, "native_root_id")?;
        let native_scope_key = optional_nonempty_text(&response, "native_scope_key")?;
        if binding_id.is_none()
            || !matches!(generation, Some(value) if value > 0)
            || recovery_required.is_none()
            || native_root_id.is_some() != native_scope_key.is_some()
        {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_HELLO_ACK_INVALID",
                "manager hello response did not match the module acknowledgement contract",
            ));
        }
        validate_negotiation(&response, &module_contract)?;
        let managed_owner = VerifiedManagedOwner::accepted_module_hello(
            managed_owner,
            recovery_required.ok_or_else(|| {
                swarm_contracts::error::Error::new(
                    "MODULE_HELLO_ACK_INVALID",
                    "manager omitted recovery disposition",
                )
            })?,
        );
        Ok(VerifiedModuleHello {
            response,
            managed_owner,
        })
    }

    /// Do not ask the manager for another durably admitted operation while
    /// this bounded receipt outbox has no capacity to retain its outcome.
    pub async fn next(&mut self, controller: &Controller) -> Result<Option<RuntimeCommand>> {
        if !controller.may_request_next() {
            return Ok(None);
        }
        let value = self.client.request("module.next", json!({})).await?;
        let command = value.get("command").cloned().unwrap_or(Value::Null);
        if command.is_null() {
            return Ok(None);
        }
        serde_json::from_value(command).map(Some).map_err(|_| {
            swarm_contracts::error::Error::new(
                "MODULE_COMMAND_INVALID",
                "manager returned a command that did not match the shared command contract",
            )
        })
    }

    pub async fn observe(&mut self, observation: &PendingObservation) -> Result<Value> {
        self.client
            .request(
                "module.observe",
                json!({
                    "event_id": observation.event_id,
                    "sequence": observation.sequence,
                    "state": observation.state,
                }),
            )
            .await
    }

    pub async fn outcome(
        &mut self,
        outcome: &RuntimeOutcome,
        identity: &ModuleReceiptIdentity,
    ) -> Result<Value> {
        let params = module_receipt::serialize_outcome(outcome, identity).map_err(|_| {
            swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_INVALID",
                "adapter could not encode its bounded operation receipt",
            )
        })?;
        self.outcome_value(params).await
    }

    /// Resend an already-serialized operation receipt without changing any
    /// field. Use only for the same idempotent manager request after reconnect;
    /// never reconstruct or repeat the native prompt.
    pub async fn outcome_value(&mut self, params: Value) -> Result<Value> {
        self.client.request("module.outcome", params).await
    }
}

fn optional_nonempty_text<'a>(response: &'a Value, key: &str) -> Result<Option<&'a str>> {
    let value = response.get(key).ok_or_else(|| {
        swarm_contracts::error::Error::new(
            "MODULE_HELLO_ACK_INVALID",
            "manager hello response omitted a native identity field",
        )
    })?;
    match value {
        Value::Null => Ok(None),
        Value::String(value) if !value.trim().is_empty() => Ok(Some(value)),
        _ => Err(swarm_contracts::error::Error::new(
            "MODULE_HELLO_ACK_INVALID",
            "manager returned an invalid native identity field",
        )),
    }
}

fn validate_negotiation(
    response: &Value,
    claim: &swarm_contracts::module_contract::ModuleContractClaim,
) -> Result<()> {
    let negotiation = response
        .get("module_contract_negotiation")
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            swarm_contracts::error::Error::new(
                "MODULE_HELLO_ACK_INVALID",
                "manager omitted the Store-owned module contract negotiation",
            )
        })?;
    let expected_artifact = serde_json::to_value(&claim.artifact)?;
    let expected_module_id = serde_json::to_value(&claim.module_id)?;
    let expected_capabilities = serde_json::to_value(&claim.capabilities)?;
    let expected_config_schema = serde_json::to_value(&claim.config_schema)?;
    let expected_commands = serde_json::to_value(&claim.command_schemas)?;
    let expected_events = serde_json::to_value(&claim.event_schemas)?;
    let expected_protocol = serde_json::to_value(claim.protocol)?;
    let revision = negotiation
        .get("descriptor_revision")
        .and_then(Value::as_u64)
        .filter(|revision| *revision > 0);
    if negotiation.get("status").and_then(Value::as_str) != Some("negotiated")
        || revision.is_none()
        || negotiation.get("module_id") != Some(&expected_module_id)
        || negotiation.get("artifact") != Some(&expected_artifact)
        || negotiation.get("protocol") != Some(&expected_protocol)
        || negotiation.get("capabilities") != Some(&expected_capabilities)
        || negotiation.get("config_schema") != Some(&expected_config_schema)
        || negotiation.get("command_schemas") != Some(&expected_commands)
        || negotiation.get("event_schemas") != Some(&expected_events)
        || negotiation.get("effects_authorized_by_descriptor") != Some(&Value::Bool(false))
    {
        return Err(swarm_contracts::error::Error::new(
            "MODULE_CONTRACT_NEGOTIATION_MISMATCH",
            "Store did not negotiate this exact Antigravity 1.0 claim",
        ));
    }
    Ok(())
}

/// The authenticated client is deliberately not retried here. After an error,
/// the caller must reconnect, repeat hello, and may resend only the byte-for-
/// byte same outcome/observation already held in memory. It must never ask the
/// native child to process the prompt again.
pub async fn connect_manager(
    host_data_dir: PathBuf,
    credential: Credential,
    ipc: IpcConfig,
) -> Result<ManagerLink> {
    ManagerLink::connect(&host_data_dir, &credential, &ipc).await
}
