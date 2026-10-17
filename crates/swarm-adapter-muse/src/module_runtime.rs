//! Owned Muse worker bootstrap and host delivery loop. No provider credentials,
//! host credential generation, native installer, HOME rewrite, or second owner.

use crate::driver::Driver;
use crate::msp::{MspTransport, Notification};
use crate::native_owner::{
    NativeChildCustodian, NativeClientSpec, NativeStderrResult, spawn_owned,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use swarm_client::{HostConnectionConfig, ModuleLink};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_catalog::ProtocolVersion,
    module_contract::ModuleContractClaim,
};
use swarm_process::{
    module_owner::{VerifiedModuleWorker, verify_current_adapter_from_env},
    read_private_durable,
};

pub type NativeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
pub struct NativeAttachment {
    pub transport: Arc<MspTransport>,
    pub notifications: tokio::sync::mpsc::Receiver<Notification>,
}
/// Implemented by the external joined custodian. It retains every
/// post-spawn handle outside the consumer task, including on error/cancellation.
/// No Tokio Child spawn or blocking consumer Drop is implemented in this crate.
pub trait NativeRuntime: Send + Sync {
    fn start<'a>(
        &'a self,
        options: &'a NativeOptions,
        router: jsonrpsee_core::server::RpcModule<()>,
    ) -> NativeFuture<'a, NativeAttachment>;
    /// Called after all MSP transports have completed their submission/close
    /// barriers, while all IO tasks remain schedulable.
    fn drain_stdio(&self) -> NativeFuture<'_, ()>;
    /// Join externally owned custodians only AFTER drain_stdio. Direct exit is
    /// not family departure, and wait errors are never NoEffect evidence.
    fn join_custodians(&self) -> NativeFuture<'_, ()>;
}
/// Requires reviewed native_owner c177a1c or its qualified descendant. The
/// actual std Child is retained here before spawn, outside the consumer task.
/// No blocking Drop, detached reaper or Tokio Command spawn is introduced.
struct JoinedNativeRuntime {
    custodian: tokio::sync::Mutex<NativeChildCustodian>,
}
impl JoinedNativeRuntime {
    fn new() -> Self {
        Self {
            custodian: tokio::sync::Mutex::new(NativeChildCustodian::new()),
        }
    }
}
impl NativeRuntime for JoinedNativeRuntime {
    fn start<'a>(
        &'a self,
        options: &'a NativeOptions,
        router: jsonrpsee_core::server::RpcModule<()>,
    ) -> NativeFuture<'a, NativeAttachment> {
        Box::pin(async move {
            let mut custodian = self.custodian.lock().await;
            let spec = NativeClientSpec::serve(options.executable.clone())
                .current_dir(options.workspace.clone());
            let child = spawn_owned(&mut custodian, spec, router)
                .await
                .map_err(|_| fault("MUSE_NATIVE_START_UNRESOLVED"))?;
            // NativeDirectChild is a non-owning observer under c177a1c. Its
            // drop neither waits nor loses the externally retained std Child.
            Ok(NativeAttachment {
                transport: Arc::new(child.transport),
                notifications: child.notifications,
            })
        })
    }
    fn drain_stdio(&self) -> NativeFuture<'_, ()> {
        // Driver already awaited MSP's writer/close barrier and notification
        // consumer. The custodian joins the actual stderr drain below. MSP's
        // API does not expose an additional physical stdout-EOF receipt.
        Box::pin(async { Ok(()) })
    }
    fn join_custodians(&self) -> NativeFuture<'_, ()> {
        Box::pin(async move {
            let observed = self.custodian.lock().await.join().await;
            if let Some(observed) = observed
                && (observed.wait_failures.count > 0
                    || !observed.direct_child.success()
                    || !matches!(observed.stderr, NativeStderrResult::Closed { .. }))
            {
                return Err(fault("MUSE_NATIVE_EXIT_OBSERVATION_UNRESOLVED"));
            }
            // Direct exit plus stderr EOF does not prove family departure.
            Ok(())
        })
    }
}

pub const MODULE_ID: &str = "muse";
pub const ARTIFACT_ID: &str = "muse-rust-controller.1";
pub const ARTIFACT_VERSION: &str = "0.1.0";
pub const CAPABILITIES: [&str; 10] = [
    "agent.configure",
    "agent.goal",
    "agent.open",
    "agent.reconcile",
    "agent.recover",
    "agent.refresh",
    "agent.reply",
    "agent.result",
    "agent.send",
    "task.dispatch",
];

pub(crate) fn fault(code: &'static str) -> Error {
    Error::new(code, code)
}
fn required(name: &str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| fault("MUSE_MODULE_ENV_INVALID"))
}
fn absolute(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(fault("MUSE_MODULE_PATH_INVALID"));
    }
    Ok(path)
}
fn default_executable() -> PathBuf {
    PathBuf::from(r"C:\Tools\MuseCode\muse-bin-1.4.0-R4161.1.exe")
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOptions {
    #[serde(rename = "workspaceRoot")]
    pub workspace: PathBuf,
    #[serde(rename = "nativeExecutable", default = "default_executable")]
    pub executable: PathBuf,
    #[serde(rename = "modelId", default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(
        rename = "providerId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provider: Option<String>,
    #[serde(
        rename = "reasoningEffort",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub effort: Option<String>,
    #[serde(
        rename = "approvalMode",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub approval_mode: Option<String>,
    #[serde(
        rename = "mcpServers",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub mcp_servers: Option<Value>,
}
impl NativeOptions {
    /// Values come from the immutable Store route, as in bridge.13, rather
    /// than adding a Muse-specific supervisor config mapper/Core version pin.
    pub(crate) fn from_route(route: &Value, installed: &Path) -> Result<Self> {
        let mut value = route
            .get("native_options")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| fault("MUSE_NATIVE_OPTIONS_INVALID"))?;
        if value.get("nativeExecutable").is_none() {
            value["nativeExecutable"] = json!(installed);
        }
        let options: Self =
            serde_json::from_value(value).map_err(|_| fault("MUSE_NATIVE_OPTIONS_INVALID"))?;
        if options.executable != installed
            || !options.executable.is_absolute()
            || !options.executable.is_file()
            || !options.workspace.is_absolute()
            || !options.workspace.is_dir()
        {
            return Err(fault("MUSE_NATIVE_INSTALLATION_UNAVAILABLE"));
        }
        Ok(options)
    }
}

/// A Muse specialization of the existing protected module-owner bootstrap.
/// The reviewed base has HostConnectionConfig/ModuleHostConfigPath and owner
/// verification, but no generic ModuleOwnedBootstrap implementation to import.
pub struct ModuleOwnedBootstrap {
    pub host: HostConnectionConfig,
    pub credential: Credential,
    pub worker: VerifiedModuleWorker,
    pub contract: ModuleContractClaim,
    pub binding_id: String,
    pub generation: i64,
    pub state_dir: PathBuf,
    pub native_executable: PathBuf,
}
impl ModuleOwnedBootstrap {
    pub fn from_config_path(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(fault("MUSE_MODULE_PATH_INVALID"));
        }
        let host: HostConnectionConfig =
            serde_json::from_slice(&read_private_durable(path, 64 * 1024)?)
                .map_err(|_| fault("MUSE_HOST_CONFIG_INVALID"))?;
        host.validate()?;
        let worker = verify_current_adapter_from_env()?;
        let state_dir = absolute("ELIOT_SWARM_MODULE_STATE")?;
        if absolute("ELIOT_SWARM_MODULE_OWNER")? != state_dir.join("owner.json") {
            return Err(fault("MUSE_MODULE_OWNER_INVALID"));
        }
        let contract: ModuleContractClaim =
            serde_json::from_str(&required("ELIOT_SWARM_MODULE_CONTRACT")?)
                .map_err(|_| fault("MUSE_MODULE_CONTRACT_INVALID"))?;
        contract
            .validate()
            .map_err(|_| fault("MUSE_MODULE_CONTRACT_INVALID"))?;
        if contract.module_id.as_str() != MODULE_ID
            || contract.artifact.artifact_id.as_str() != ARTIFACT_ID
            || contract.artifact.version.as_str() != ARTIFACT_VERSION
            || contract.protocol != (ProtocolVersion { major: 1, minor: 0 })
            || required("ELIOT_SWARM_MODULE_ID")? != MODULE_ID
            || required("ELIOT_SWARM_MODULE_ARTIFACT_ID")? != ARTIFACT_ID
            || required("ELIOT_SWARM_MODULE_ARTIFACT_VERSION")? != ARTIFACT_VERSION
            || contract.artifact.build_id.as_deref()
                != env::var("ELIOT_SWARM_MODULE_BUILD_ID").ok().as_deref()
            || contract.config_schema.is_some()
            || contract.capabilities.len() != CAPABILITIES.len()
            || !CAPABILITIES.iter().all(|cap| {
                contract
                    .capabilities
                    .iter()
                    .any(|actual| actual.as_str() == *cap)
            })
        {
            return Err(fault("MUSE_MODULE_CONTRACT_INVALID"));
        }
        for required_schema in [
            "swarm.runtime_command",
            "swarm.task_prompt",
            "swarm.task_dispatch_context",
        ] {
            if !contract
                .command_schemas
                .iter()
                .any(|s| s.schema_id == required_schema && s.version == "1")
            {
                return Err(fault("MUSE_MODULE_CONTRACT_INVALID"));
            }
        }
        for required_schema in ["swarm.runtime_outcome", "swarm.task_dispatch_admission"] {
            if !contract
                .event_schemas
                .iter()
                .any(|s| s.schema_id == required_schema && s.version == "1")
            {
                return Err(fault("MUSE_MODULE_CONTRACT_INVALID"));
            }
        }
        let credential: Credential = serde_json::from_slice(&read_private_durable(
            &absolute("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")?,
            64 * 1024,
        )?)
        .map_err(|_| fault("MUSE_MODULE_CREDENTIAL_INVALID"))?;
        if credential.client_id != required("ELIOT_SWARM_MODULE_CLIENT_ID")? {
            return Err(fault("MUSE_MODULE_CREDENTIAL_INVALID"));
        }
        let generation = required("ELIOT_SWARM_MODULE_BINDING_GENERATION")?
            .parse::<i64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| fault("MUSE_MODULE_BINDING_INVALID"))?;
        let native_executable = env::var_os("ELIOT_SWARM_MUSE_NATIVE_EXECUTABLE")
            .map(PathBuf::from)
            .unwrap_or_else(default_executable);
        if !native_executable.is_absolute() || !native_executable.is_file() {
            return Err(fault("MUSE_NATIVE_INSTALLATION_UNAVAILABLE"));
        }
        Ok(Self {
            host,
            credential,
            worker,
            contract,
            generation,
            binding_id: required("ELIOT_SWARM_MODULE_BINDING_ID")?,
            state_dir,
            native_executable,
        })
    }
    pub(crate) async fn link(&self, driver: &Driver) -> Result<ModuleLink> {
        let mut link =
            ModuleLink::connect(&self.host.host_data_dir, &self.credential, &self.host.ipc).await?;
        let hello = link.hello(driver.hello(), Some(&self.contract)).await?;
        if hello["binding_id"] != self.binding_id || hello["generation"] != self.generation {
            return Err(fault("MUSE_HOST_BINDING_CHANGED"));
        }
        Ok(link)
    }
}

/// Root must provide the shared multiplexed ModuleLink before activation.
/// Multiple hello connections replace each other's Store link fence; the
/// current sequential &mut Client cannot safely emulate that API here.
pub async fn run_owned(bootstrap: ModuleOwnedBootstrap) -> Result<()> {
    // Created before any consumer/spawn; retained until join while Tokio lives.
    let native: Arc<dyn NativeRuntime> = Arc::new(JoinedNativeRuntime::new());
    let _ = bootstrap;
    let result: Result<()> = Err(fault("MUSE_HOST_MULTIPLEX_API_REQUIRED"));
    let drains = native.drain_stdio().await;
    let joined = native.join_custodians().await;
    result.and(drains).and(joined)
}

/// Synthetic Host/MSP fixtures may provide pipes through the same consumer
/// boundary; production always constructs the externally joined owner above.
pub async fn run_owned_with(
    bootstrap: ModuleOwnedBootstrap,
    native: Arc<dyn NativeRuntime>,
) -> Result<()> {
    let bootstrap = Arc::new(bootstrap);
    let driver = match Driver::new(bootstrap.clone(), native.clone()) {
        Ok(driver) => driver,
        Err(error) => {
            let _ = native.drain_stdio().await;
            let _ = native.join_custodians().await;
            return Err(error);
        }
    };
    let consumer_driver = driver.clone();
    let consumer = tokio::spawn(async move { consume(bootstrap, consumer_driver).await });
    let result = consumer
        .await
        .unwrap_or_else(|_| Err(fault("MUSE_CONSUMER_TASK_FAILED")));
    // Runtime/custodian remains owned by this outer frame, never by consumer.
    driver.request_stop();
    driver.stop_native().await;
    let drains = native.drain_stdio().await;
    let joined = native.join_custodians().await;
    result.and(drains).and(joined)
}

async fn consume(bootstrap: Arc<ModuleOwnedBootstrap>, driver: Arc<Driver>) -> Result<()> {
    // Sequential seam only, not activation qualification. Exactly ONE hello
    // owns the current link. A future shared multiplexed link must service
    // outbox/optional stream calls while retaining an outstanding next future.
    intake(bootstrap, driver).await
}

async fn intake(bootstrap: Arc<ModuleOwnedBootstrap>, driver: Arc<Driver>) -> Result<()> {
    let slots = Arc::new(tokio::sync::Semaphore::new(8));
    let mut requests = tokio::task::JoinSet::new();
    let mut stop = std::pin::pin!(tokio::signal::ctrl_c());
    let result=async { 'running: loop {
        let mut link = tokio::select! {
            _ = &mut stop => break,
            result = bootstrap.link(&driver) => match result {
                Ok(link) => link,
                Err(_) => { tokio::time::sleep(Duration::from_millis(500)).await; continue; }
            }
        };
        loop {
            if driver.flush(&mut link).await.is_err(){break;}
            let permit = tokio::select! { _ = &mut stop => break 'running, permit = slots.clone().acquire_owned() => permit.map_err(|_| fault("MUSE_DRIVER_STOPPED"))? };
            let next = tokio::select! { _ = &mut stop => break 'running, next = link.next() => match next { Ok(next) => next, Err(_) => break } };
            let Some(command) = next.get("command").filter(|command| !command.is_null()) else { drop(permit); continue; };
            let command = serde_json::from_value(command.clone()).map_err(|_| fault("MUSE_HOST_COMMAND_INVALID"))?;
            let worker = driver.clone();
            requests.spawn(async move { let _permit = permit; worker.execute(command).await });
            while let Some(completed) = requests.try_join_next() {
                match completed {Ok(Ok(()))=>{},Ok(Err(error))=>return Err(error),Err(_)=>return Err(fault("MUSE_COMMAND_TASK_FAILED"))}
            }
        }
    } Ok(()) }.await;
    driver.request_stop();
    // Do not abort accepted mutation futures or discard child custody.
    driver.stop_native().await;
    let mut result = result;
    while let Some(completed) = requests.join_next().await {
        if result.is_ok() {
            result = match completed {
                Ok(result) => result,
                Err(_) => Err(fault("MUSE_COMMAND_TASK_FAILED")),
            };
        }
    }
    result
}
