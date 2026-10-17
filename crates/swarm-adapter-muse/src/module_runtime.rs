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
pub const ARTIFACT_VERSION: &str = "0.1.2";
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
            "swarm.normalized_result_context",
        ] {
            if !contract
                .command_schemas
                .iter()
                .any(|s| s.schema_id == required_schema && s.version == "1")
            {
                return Err(fault("MUSE_MODULE_CONTRACT_INVALID"));
            }
        }
        if !contract
            .command_schemas
            .contains(&swarm_contracts::module_contract::normalized_result_context_schema())
            || !contract
                .event_schemas
                .contains(&swarm_contracts::module_contract::normalized_result_page_schema())
        {
            return Err(fault("MUSE_MODULE_CONTRACT_INVALID"));
        }
        for required_schema in [
            "swarm.runtime_outcome",
            "swarm.task_dispatch_admission",
            "swarm.normalized_result_page",
        ] {
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
    #[cfg(test)]
    pub(crate) async fn link(&self, driver: &Driver) -> Result<ModuleLink> {
        self.link_hello(driver.hello()).await
    }
    async fn link_hello(&self, params: Value) -> Result<ModuleLink> {
        let mut link =
            ModuleLink::connect(&self.host.host_data_dir, &self.credential, &self.host.ipc).await?;
        let hello = link.hello(params, Some(&self.contract)).await?;
        if hello["binding_id"] != self.binding_id || hello["generation"] != self.generation {
            return Err(fault("MUSE_HOST_BINDING_CHANGED"));
        }
        Ok(link)
    }
}

/// Normal executable entry. The external custodian outlives the consumer and
/// uses the same shared-link runtime loop as the connected qualification seam.
pub async fn run_owned(bootstrap: ModuleOwnedBootstrap) -> Result<()> {
    let native: Arc<dyn NativeRuntime> = Arc::new(JoinedNativeRuntime::new());
    run_owned_with(bootstrap, native).await
}

/// A retained outer owner, outside every abortable native consumer task.
/// Call `join(&mut self)` before runtime teardown, including after consumer
/// abort/panic. Cancelling join only borrows this owner; keep it and rejoin.
/// Dropping the owner itself unjoined violates the native custodian contract;
/// neither async Drop nor arbitrary runtime destruction is claimed safe.
#[must_use = "retain outside the consumer and join before Tokio teardown"]
pub struct OwnedModuleRun {
    owner: RunOwner<Driver>,
}

#[derive(Clone)]
pub struct ModuleRunControl {
    stop: tokio::sync::watch::Sender<bool>,
    abort: tokio::task::AbortHandle,
}
impl ModuleRunControl {
    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    /// Abort the consumer only; the external owner and command pool survive.
    /// The caller must still join OwnedModuleRun while Tokio is alive.
    pub fn abort_consumer(&self) {
        self.request_stop();
        self.abort.abort();
    }
}
impl OwnedModuleRun {
    pub fn control(&self) -> ModuleRunControl {
        self.owner.control()
    }
    pub async fn join(&mut self) -> Result<()> {
        self.owner.join().await
    }
}

/// Construct the retained owner before any consumer can start native work.
/// On constructor error no new native start was issued; an already populated
/// caller-supplied NativeRuntime remains the caller's custody obligation.
pub fn start_owned_with(
    bootstrap: ModuleOwnedBootstrap,
    native: &Arc<dyn NativeRuntime>,
) -> Result<OwnedModuleRun> {
    let bootstrap = Arc::new(bootstrap);
    let driver = Driver::new(bootstrap.clone(), native.clone())?;
    Ok(OwnedModuleRun {
        owner: RunOwner::start(bootstrap, driver, native.clone()),
    })
}

/// Convenience outer entry; do not put this whole owner in an abortable task.
/// Callers needing cancellation use start_owned_with, retain OwnedModuleRun,
/// signal/abort its CONSUMER via control(), then rejoin the retained owner.
pub async fn run_owned_with(
    bootstrap: ModuleOwnedBootstrap,
    native: Arc<dyn NativeRuntime>,
) -> Result<()> {
    let mut run = match start_owned_with(bootstrap, &native) {
        Ok(run) => run,
        Err(error) => {
            let _ = native.drain_stdio().await;
            let _ = native.join_custodians().await;
            return Err(error);
        }
    };
    let control = run.control();
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    // Retain this exact join future while waiting for a stop signal. A signal
    // never drops/restarts consumer requests or loses the outer custody owner.
    let join = run.join();
    tokio::pin!(join);
    tokio::select! {
        result = &mut join => result,
        signal_result = &mut signal => {
            control.request_stop();
            let result = join.await;
            if result.is_err() { result } else if signal_result.is_err() { Err(fault("MUSE_SIGNAL_UNAVAILABLE")) } else { Ok(()) }
        }
    }
}

// Private loop seam: production implements this only through the existing
// Driver. Connected fixtures exercise this same loop with synthetic MSP/Host
// peers; it defines no DTO, journal, credentials, scheduling or Task authority.
type LoopCall<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
trait ConsumerDriver: Send + Sync + 'static {
    fn hello(&self) -> Value;
    fn request_stop(&self);
    fn execute(
        self: Arc<Self>,
        command: swarm_contracts::RuntimeCommand,
    ) -> LoopCall<'static, Result<()>>;
    fn flush<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, Result<()>>;
    fn publish<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, bool>;
    fn stop_native(&self) -> LoopCall<'_, ()>;
}
impl ConsumerDriver for Driver {
    fn hello(&self) -> Value {
        Driver::hello(self)
    }
    fn request_stop(&self) {
        Driver::request_stop(self);
    }
    fn execute(
        self: Arc<Self>,
        command: swarm_contracts::RuntimeCommand,
    ) -> LoopCall<'static, Result<()>> {
        Box::pin(Driver::execute(self, command))
    }
    fn flush<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, Result<()>> {
        Box::pin(Driver::flush(self, link))
    }
    fn publish<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, bool> {
        Box::pin(Driver::publish_next_stream(self, link))
    }
    fn stop_native(&self) -> LoopCall<'_, ()> {
        Box::pin(Driver::stop_native(self))
    }
}

type Commands = Arc<tokio::sync::Mutex<tokio::task::JoinSet<Result<()>>>>;
type ActiveLink = Arc<tokio::sync::Mutex<Option<ModuleLink>>>;
struct RunOwner<D: ConsumerDriver> {
    driver: Arc<D>,
    native: Arc<dyn NativeRuntime>,
    commands: Commands,
    active_link: ActiveLink,
    consumer: Option<tokio::task::JoinHandle<Result<()>>>,
    control: ModuleRunControl,
    primary: Option<Result<()>>,
    finished: bool,
}
impl<D: ConsumerDriver> RunOwner<D> {
    fn start(
        bootstrap: Arc<ModuleOwnedBootstrap>,
        driver: Arc<D>,
        native: Arc<dyn NativeRuntime>,
    ) -> Self {
        let commands = Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new()));
        let active_link = Arc::new(tokio::sync::Mutex::new(None));
        let (stop, stopping) = tokio::sync::watch::channel(false);
        // NativeRuntime/custodian and commands are retained by this outer
        // frame before consumer spawn and remain retained on consumer unwind.
        let worker = driver.clone();
        let work = commands.clone();
        let transport = active_link.clone();
        let consumer =
            tokio::spawn(
                async move { consume(bootstrap, worker, work, transport, stopping).await },
            );
        let control = ModuleRunControl {
            stop,
            abort: consumer.abort_handle(),
        };
        Self {
            driver,
            native,
            commands,
            active_link,
            consumer: Some(consumer),
            control,
            primary: None,
            finished: false,
        }
    }
    fn control(&self) -> ModuleRunControl {
        self.control.clone()
    }
    fn retain(&mut self, result: Result<()>) {
        if self.primary.as_ref().is_none_or(|primary| primary.is_ok()) && result.is_err() {
            self.primary = Some(result);
        }
    }
    async fn join(&mut self) -> Result<()> {
        if self.finished {
            return self.primary.clone().unwrap_or(Ok(()));
        }
        if let Some(consumer) = self.consumer.as_mut() {
            let result = match consumer.await {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => Err(fault("MUSE_CONSUMER_TASK_ABORTED")),
                Err(_) => Err(fault("MUSE_CONSUMER_TASK_FAILED")),
            };
            self.primary = Some(result);
            self.consumer = None;
        }
        {
            let mut active = self.active_link.lock().await;
            if let Some(link) = active.as_ref() {
                link.close().await;
            }
            *active = None;
        }
        self.driver.request_stop();
        // Fence every command before the FINAL transport snapshot/close. A
        // command already inside native.start may still publish its transport
        // after request_stop; closing first could miss it and strand custody.
        let retained_commands = self.commands.clone();
        let mut commands = retained_commands.lock().await;
        commands.abort_all();
        while let Some(result) = commands.join_next().await {
            // An explicit shutdown abort is uncertain work, not NoEffect.
            // Preserve original consumer failure over shutdown consequences.
            match result {
                Ok(result) => self.retain(result),
                Err(error) if error.is_cancelled() => {
                    self.retain(Err(fault("MUSE_COMMAND_TASK_ABORTED")))
                }
                Err(_) => self.retain(Err(fault("MUSE_COMMAND_TASK_FAILED"))),
            }
        }
        drop(commands);
        // No command can now publish a late attachment. MSP close still drains
        // accepted frames; aborted commands remain uncertain, never replayed.
        self.driver.stop_native().await;
        let drains = self.native.drain_stdio().await;
        self.retain(drains);
        let joined = self.native.join_custodians().await;
        self.retain(joined);
        self.finished = true;
        self.primary.clone().unwrap_or(Ok(()))
    }
}

const COMMAND_LIMIT: usize = 8;
const LOOP_TICK: Duration = Duration::from_millis(25);
const DELIVERY_TICK: Duration = Duration::from_millis(250);
const DELIVERY_RETRY: Duration = Duration::from_secs(1);
type Slot<T> = Option<LoopCall<'static, T>>;
async fn wait_slot<T: Send + 'static>(slot: &mut Slot<T>) -> T {
    match slot {
        Some(call) => call.as_mut().await,
        None => std::future::pending().await,
    }
}
fn terminal_host(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "UNAUTHORIZED"
            | "FORBIDDEN"
            | "BINDING_CLOSED"
            | "STALE_LINK"
            | "MUSE_HOST_BINDING_CHANGED"
    ) || error.code.starts_with("MUSE_JOURNAL_")
}

async fn consume<D: ConsumerDriver>(
    bootstrap: Arc<ModuleOwnedBootstrap>,
    driver: Arc<D>,
    commands: Commands,
    active_link: ActiveLink,
    mut stopping: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    // Reserve capacity for one retained next, one durable delivery and at most
    // one optional publication. There is no unbounded task/request producer.
    if bootstrap.host.ipc.max_inflight_per_connection < 3 {
        return Err(fault("MUSE_HOST_INFLIGHT_BOUND_UNSUPPORTED"));
    }
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        let mut link = tokio::select! {
            _ = stopping.changed() => return Ok(()),
            link = bootstrap.link_hello(driver.hello()) => match link {
                Ok(link) => link,
                Err(error) if terminal_host(&error) => return Err(error),
                Err(_) => { tokio::select! { _ = stopping.changed() => return Ok(()), _ = tokio::time::sleep(Duration::from_millis(500)) => {} } continue; }
            }
        };
        *active_link.lock().await = Some(link.clone());
        let result = epoch(&mut link, driver.clone(), &commands, &mut stopping).await;
        // Poison/drain the ONE old physical session before replacing it. A
        // lost next/outcome/stream ACK never requests another native write.
        link.close().await;
        *active_link.lock().await = None;
        match result {
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(error) => return Err(error),
        }
    }
}
async fn epoch<D: ConsumerDriver>(
    link: &mut ModuleLink,
    driver: Arc<D>,
    commands: &Commands,
    stopping: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool> {
    let mut next: Slot<Result<Value>> = None;
    let mut delivery: Slot<Result<()>> = None;
    let mut publication: Slot<bool> = None;
    let mut tick = tokio::time::interval(LOOP_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut deliver_at = tokio::time::Instant::now();
    let mut publish_at = tokio::time::Instant::now();
    loop {
        if *stopping.borrow() {
            return Ok(false);
        }
        if !link.is_connected() {
            return Ok(true);
        }
        let count = {
            let mut tasks = commands.lock().await;
            while let Some(completed) = tasks.try_join_next() {
                match completed {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Err(fault("MUSE_COMMAND_TASK_FAILED")),
                }
            }
            tasks.len()
        };
        if next.is_none() && count < COMMAND_LIMIT {
            let mut handle = link.clone();
            next = Some(Box::pin(async move { handle.next().await }));
        }
        let now = tokio::time::Instant::now();
        if delivery.is_none() && now >= deliver_at {
            let worker = driver.clone();
            let mut handle = link.clone();
            delivery = Some(Box::pin(async move { worker.flush(&mut handle).await }));
        }
        if publication.is_none() && now >= publish_at {
            let worker = driver.clone();
            let mut handle = link.clone();
            publication = Some(Box::pin(async move { worker.publish(&mut handle).await }));
        }
        tokio::select! {
            biased;
            _ = stopping.changed() => return Ok(false),
            result = wait_slot(&mut next) => {
                next = None;
                match result {
                    Ok(value) => if let Some(command) = value.get("command").filter(|v| !v.is_null()) {
                        let command = serde_json::from_value(command.clone()).map_err(|_| fault("MUSE_HOST_COMMAND_INVALID"))?;
                        let worker = driver.clone(); commands.lock().await.spawn(async move { worker.execute(command).await });
                    },
                    Err(_) if !link.is_connected() => return Ok(true),
                    Err(error) if error.code == "IPC_CAPACITY" => { tick.tick().await; },
                    Err(error) => return Err(error),
                }
            },
            result = wait_slot(&mut delivery) => {
                delivery = None;
                match result {
                    Ok(()) => deliver_at = tokio::time::Instant::now() + DELIVERY_TICK,
                    Err(_) if !link.is_connected() => return Ok(true),
                    Err(error) if terminal_host(&error) => return Err(error),
                    Err(_) => deliver_at = tokio::time::Instant::now() + DELIVERY_RETRY,
                }
            },
            _ = wait_slot(&mut publication) => {
                publication = None;
                publish_at = tokio::time::Instant::now() + LOOP_TICK;
                // Optional stream rejection/capacity/loss is never a native or
                // control failure. A poisoned SESSION reconnects on the next
                // iteration, with Driver/native state and exact outbox intact.
            },
            _ = tick.tick() => {},
        }
        // Slots retain their exact in-flight futures across every other reply
        // and tick. No pending next/publication is cancelled to unlock control.
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use crate::msp::Options;
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use swarm_contracts::{
        RuntimeCommand,
        module_catalog::ModuleDescriptor,
        native_stream::{StreamPayload, StreamPublish, StreamScope, TextClass},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
        sync::Notify,
        task::JoinHandle,
    };
    const DEADLINE: Duration = Duration::from_secs(4);
    #[test]
    fn muse_normalized_result_descriptor_version_and_typed_schemas_are_coherent() {
        let descriptor: Value =
            serde_json::from_str(include_str!("../registration/descriptor.template.json")).unwrap();
        assert_eq!(descriptor["artifact"]["version"], ARTIFACT_VERSION);
        assert_eq!(descriptor["enabled"], false);
        for (field, schema) in [
            (
                "command_schemas",
                swarm_contracts::module_contract::normalized_result_context_schema(),
            ),
            (
                "event_schemas",
                swarm_contracts::module_contract::normalized_result_page_schema(),
            ),
        ] {
            let declared: Vec<swarm_contracts::module_catalog::SchemaDescriptor> =
                serde_json::from_value(descriptor[field].clone()).unwrap();
            assert!(declared.contains(&schema));
        }
    }
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("sm-loop-{}", uuid::Uuid::now_v7()));
            std::fs::create_dir(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn bootstrap(path: &Path) -> Arc<ModuleOwnedBootstrap> {
        let executable = env::current_exe().unwrap();
        let mut descriptor: Value =
            serde_json::from_str(include_str!("../registration/descriptor.template.json")).unwrap();
        descriptor["launch"]["executable"] = json!(executable);
        let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
        let contract = ModuleContractClaim::from_descriptor(
            &descriptor,
            ProtocolVersion { major: 1, minor: 0 },
        )
        .unwrap();
        Arc::new(ModuleOwnedBootstrap {
            host: HostConnectionConfig {
                schema_version: 1,
                host_data_dir: path.into(),
                ipc: swarm_client::IpcConfig::default(),
            },
            credential: Credential {
                client_id: "fixture-module".into(),
                token: "fixture-only".into(),
            },
            worker: VerifiedModuleWorker {
                boot_id: "fixture-boot".into(),
                owner_record: json!({"token":"fixture-owner","process":{"pid":1}}),
                process_image_identity: json!({}),
            },
            contract,
            binding_id: "binding".into(),
            generation: 1,
            state_dir: path.into(),
            native_executable: executable,
        })
    }
    fn command(operation: &str, method: &str) -> Value {
        serde_json::to_value(RuntimeCommand {
            operation_id: operation.into(),
            method: method.into(),
            created_at_ms: 1,
            binding_id: "binding".into(),
            generation: 1,
            native_root_id: Some("root".into()),
            route: json!({"module_artifact_id":ARTIFACT_ID}),
            input: json!({"text":"abc"}),
            input_sha256: Some("a".repeat(64)),
            target_input_sha256: None,
        })
        .unwrap()
    }
    async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) {
        writer
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }
    async fn reply<W: AsyncWrite + Unpin>(writer: &mut W, id: &Value, value: Value) {
        send(writer, json!({"jsonrpc":"2.0","id":id,"result":value})).await;
    }

    // This fixture implements only the loop's native/Host seams. It exercises
    // real MSP and ModuleLink transports, not Driver/journal/Task qualification.
    // The production ConsumerDriver impl above delegates solely to real Driver.
    struct LoopDriver {
        transport: Arc<MspTransport>,
        outbox: Mutex<BTreeMap<String, Value>>,
        events: Mutex<VecDeque<Value>>,
        turn: Mutex<Option<String>>,
        intents: AtomicUsize,
        native_writes: AtomicUsize,
        acknowledged: AtomicBool,
        stopped: AtomicBool,
        pump_done: AtomicBool,
        pump: tokio::sync::Mutex<Option<JoinHandle<()>>>,
        panic_hello: AtomicBool,
        fail_flush: AtomicBool,
    }
    impl ConsumerDriver for LoopDriver {
        fn hello(&self) -> Value {
            assert!(
                !self.panic_hello.load(Ordering::SeqCst),
                "fixture consumer panic"
            );
            json!({"boot_id":"fixture-boot","module_artifact_id":ARTIFACT_ID,"native_ready":true})
        }
        fn request_stop(&self) {
            self.stopped.store(true, Ordering::SeqCst);
        }
        fn execute(self: Arc<Self>, command: RuntimeCommand) -> LoopCall<'static, Result<()>> {
            Box::pin(async move {
                let method = match command.method.as_str() {
                    "task.dispatch" => "turn/start",
                    "agent.reply" => "userInput/answer",
                    _ => return Err(fault("FIXTURE_UNEXPECTED_COMMAND")),
                };
                // A test-only intent marker precedes the real transport write.
                // This is not a durable journal implementation or proof.
                self.intents.fetch_add(1, Ordering::SeqCst);
                let id = if method == "turn/start" {
                    "019a0000-0000-7000-8000-000000000001"
                } else {
                    "019a0000-0000-7000-8000-000000000002"
                };
                let params = if method == "turn/start" {
                    json!({"commandId":id,"sessionId":"root","input":[{"type":"text","text":"abc"}],"ifBusy":"queue"})
                } else {
                    json!({"commandId":id,"sessionId":"root","userInputId":"question","answers":[]})
                };
                let request = self
                    .transport
                    .submit_request(method, params.as_object().unwrap().clone())
                    .map_err(|_| fault("FIXTURE_NATIVE_UNKNOWN"))?;
                self.native_writes.fetch_add(1, Ordering::SeqCst);
                request
                    .submitted
                    .await
                    .map_err(|_| fault("FIXTURE_NATIVE_UNKNOWN"))?
                    .map_err(|_| fault("FIXTURE_NATIVE_UNKNOWN"))?;
                let ack = request
                    .response
                    .await
                    .map_err(|_| fault("FIXTURE_NATIVE_UNKNOWN"))?
                    .map_err(|_| fault("FIXTURE_NATIVE_UNKNOWN"))?;
                if method == "turn/start" {
                    *self.turn.lock().unwrap() = ack["turnId"].as_str().map(str::to_owned);
                }
                self.outbox.lock().unwrap().insert(command.operation_id.clone(),json!({"operation_id":command.operation_id,"outcome":"applied","native_root_id":"root","native_scope_key":"fixture-scope","turn_id":ack["turnId"],"details":{"execution_complete":false}}));
                Ok(())
            })
        }
        fn flush<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, Result<()>> {
            Box::pin(async move {
                if self.fail_flush.load(Ordering::SeqCst) {
                    return Err(fault("MUSE_JOURNAL_PERSISTENCE_UNKNOWN"));
                }
                let pending = self.outbox.lock().unwrap().clone();
                for (id, value) in pending {
                    link.outcome(value.clone()).await?;
                    let mut outbox = self.outbox.lock().unwrap();
                    if outbox.get(&id) == Some(&value) {
                        outbox.remove(&id);
                    }
                    if id == "dispatch" {
                        self.acknowledged.store(true, Ordering::SeqCst);
                    }
                }
                link.observe(json!({"event_id":"fixture-summary","state":{"boot_id":"fixture-boot","execution":"unknown"}})).await?;
                Ok(())
            })
        }
        fn publish<'a>(&'a self, link: &'a mut ModuleLink) -> LoopCall<'a, bool> {
            Box::pin(async move {
                if !self.acknowledged.load(Ordering::SeqCst) {
                    return false;
                }
                let event = self.events.lock().unwrap().pop_front();
                let Some(event) = event else {
                    return false;
                };
                let turn = self.turn.lock().unwrap().clone();
                if turn.as_deref() != event["params"]["item"]["turnId"].as_str() {
                    return false;
                }
                let revision = event["params"]["item"]["revision"].as_u64().unwrap();
                let frame = StreamPublish {
                    scope: StreamScope {
                        binding_id: "binding".into(),
                        generation: 1,
                        boot_id: "fixture-boot".into(),
                        task_id: "task".into(),
                        task_revision: 1,
                        attempt_id: "attempt".into(),
                        operation_id: "dispatch".into(),
                        native_session_id: "root".into(),
                        turn_id: turn.unwrap(),
                    },
                    source_sequence: revision,
                    native_cursor: format!("opaque:{revision}"),
                    item_id: "item".into(),
                    item_order: 0,
                    part_index: 0,
                    payload: StreamPayload::Text {
                        class: TextClass::Assistant,
                        text: "synthetic".into(),
                        final_record: true,
                        revision: Some(revision),
                    },
                };
                link.publish_stream(&frame).await.is_ok()
            })
        }
        fn stop_native(&self) -> LoopCall<'_, ()> {
            Box::pin(async move {
                self.stopped.store(true, Ordering::SeqCst);
                let _ = self.transport.close().await;
                let mut pump = self.pump.lock().await;
                if let Some(task) = pump.as_mut() {
                    let _ = task.await;
                }
                *pump = None;
            })
        }
    }
    struct Custody {
        driver: Arc<LoopDriver>,
        peer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
        drained: AtomicBool,
        joined: AtomicBool,
    }
    impl NativeRuntime for Custody {
        fn start<'a>(
            &'a self,
            _: &'a NativeOptions,
            _: jsonrpsee_core::server::RpcModule<()>,
        ) -> NativeFuture<'a, NativeAttachment> {
            Box::pin(async { Err(fault("FIXTURE_NO_NATIVE_SPAWN")) })
        }
        fn drain_stdio(&self) -> NativeFuture<'_, ()> {
            Box::pin(async move {
                let mut peer = self.peer.lock().await;
                if let Some(task) = peer.as_mut() {
                    task.await.map_err(|_| fault("FIXTURE_PEER_FAILED"))?;
                }
                *peer = None;
                self.drained.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
        fn join_custodians(&self) -> NativeFuture<'_, ()> {
            Box::pin(async move {
                assert!(self.driver.stopped.load(Ordering::SeqCst));
                assert!(self.driver.pump_done.load(Ordering::SeqCst));
                assert!(self.drained.load(Ordering::SeqCst));
                self.joined.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    fn fixture_native(hold_ack: bool) -> (Arc<LoopDriver>, Arc<Custody>, Arc<Notify>) {
        let (owned, peer) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(owned);
        let (transport, mut events) = MspTransport::attach_fixture(
            read,
            write,
            jsonrpsee_core::server::RpcModule::new(()),
            Options::default(),
        )
        .unwrap();
        let driver = Arc::new(LoopDriver {
            transport: Arc::new(transport),
            outbox: Mutex::new(BTreeMap::new()),
            events: Mutex::new(VecDeque::new()),
            turn: Mutex::new(None),
            intents: AtomicUsize::new(0),
            native_writes: AtomicUsize::new(0),
            acknowledged: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            pump_done: AtomicBool::new(false),
            pump: tokio::sync::Mutex::new(None),
            panic_hello: AtomicBool::new(false),
            fail_flush: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&driver);
        let pump = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if let Some(driver) = weak.upgrade() {
                    let mut queue = driver.events.lock().unwrap();
                    assert!(queue.len() < 8);
                    queue.push_back(event.envelope);
                }
            }
            if let Some(driver) = weak.upgrade() {
                driver.pump_done.store(true, Ordering::SeqCst);
            }
        });
        *driver.pump.try_lock().unwrap() = Some(pump);
        let received = Arc::new(Notify::new());
        let seen = received.clone();
        let weak = Arc::downgrade(&driver);
        let peer = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(peer);
            let mut lines = BufReader::new(read).lines();
            while let Some(line) = lines.next_line().await.unwrap() {
                let call: Value = serde_json::from_str(&line).unwrap();
                assert!(weak.upgrade().unwrap().intents.load(Ordering::SeqCst) > 0);
                seen.notify_one();
                if hold_ack {
                    continue;
                }
                reply(&mut write,&call["id"],json!({"commandId":call["params"]["commandId"],"status":"accepted","turnId":"native-turn"})).await;
                if call["method"] == "turn/start" {
                    for revision in [1, 2] {
                        send(&mut write,json!({"jsonrpc":"2.0","method":"item/completed","params":{"sessionId":"root","viewCursor":format!("opaque:{revision}"),"item":{"itemId":"item","turnId":"native-turn","revision":revision,"kind":"agentMessage","status":"completed","text":"synthetic"}}})).await;
                    }
                }
            }
        });
        let custody = Arc::new(Custody {
            driver: driver.clone(),
            peer: tokio::sync::Mutex::new(Some(peer)),
            drained: AtomicBool::new(false),
            joined: AtomicBool::new(false),
        });
        (driver, custody, received)
    }

    trait FixtureIo: AsyncRead + AsyncWrite + Unpin + Send {}
    impl<T: AsyncRead + AsyncWrite + Unpin + Send> FixtureIo for T {}
    #[derive(Default)]
    struct HostFacts {
        auth: usize,
        hello: usize,
        next: usize,
        outcomes: usize,
        rejected_stream: usize,
        stalled_stream: usize,
        stop_requested: bool,
    }
    async fn host(mut stream: Box<dyn FixtureIo>, facts: Arc<Mutex<HostFacts>>, done: Arc<Notify>) {
        let (read, mut write) = tokio::io::split(&mut stream);
        let mut lines = BufReader::new(read).lines();
        let mut pending = None;
        let mut reply_sent = false;
        // Consumer error/abort deliberately closes this pipe while a request
        // may already be admitted. Only expected peer-close IO is tolerated;
        // all phase/content/count assertions below remain mandatory.
        macro_rules! output {
            ($value:expr) => {{
                let bytes = format!("{}\n", $value);
                let written = async {
                    write.write_all(bytes.as_bytes()).await?;
                    write.flush().await
                }
                .await;
                if let Err(error) = written {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::NotConnected
                        ),
                        "unexpected fixture Host write failure"
                    );
                    return;
                }
            }};
        }
        macro_rules! respond {($id:expr,$value:expr)=>{output!(json!({"jsonrpc":"2.0","id":$id,"result":$value}))};}
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(error)
                    if facts.lock().unwrap().stop_requested
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::NotConnected
                        ) =>
                {
                    break;
                }
                Err(error) => panic!(
                    "fixture Host read failed before controlled peer shutdown: {:?}",
                    error.kind()
                ),
            };
            let call: Value = serde_json::from_str(&line).unwrap();
            let id = call["id"].clone();
            match call["method"].as_str().unwrap() {
                "client.hello" => {
                    assert_eq!(call["params"]["token"], "fixture-only");
                    facts.lock().unwrap().auth += 1;
                    respond!(&id, json!({}));
                }
                "module.hello" => {
                    facts.lock().unwrap().hello += 1;
                    respond!(&id, json!({"binding_id":"binding","generation":1}));
                }
                "module.next" => {
                    let count = {
                        let mut f = facts.lock().unwrap();
                        f.next += 1;
                        f.next
                    };
                    if count == 1 {
                        respond!(&id, json!({"command":command("dispatch","task.dispatch")}));
                    } else {
                        assert!(
                            pending.is_none(),
                            "pending next must never be resent on a tick or side reply"
                        );
                        pending = Some(id.clone());
                    }
                }
                "module.outcome" => {
                    facts.lock().unwrap().outcomes += 1;
                    respond!(&id, json!({"recorded":true}));
                    if call["params"]["operation_id"] == "reply" {
                        done.notify_one();
                    }
                }
                "module.observe" => respond!(&id, json!({"recorded":true})),
                "module.stream.publish" => {
                    let first = {
                        let f = facts.lock().unwrap();
                        f.rejected_stream == 0
                    };
                    if first {
                        facts.lock().unwrap().rejected_stream += 1;
                        output!(
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"fixture capacity","data":{"code":"STREAM_CAPACITY"}}})
                        );
                    } else {
                        facts.lock().unwrap().stalled_stream += 1; /* deliberately no ACK, while control remains usable */
                    }
                }
                _ => panic!("unexpected fixture Host method"),
            }
            let can_reply = {
                let f = facts.lock().unwrap();
                f.outcomes >= 1 && f.rejected_stream >= 1
            };
            if can_reply
                && !reply_sent
                && let Some(id) = pending.take()
            {
                reply_sent = true;
                respond!(&id, json!({"command":command("reply","agent.reply")}));
            }
        }
    }
    fn host_task(path: &Path, facts: Arc<Mutex<HostFacts>>, done: Arc<Notify>) -> JoinHandle<()> {
        let endpoint = swarm_client::ipc_endpoint(path).unwrap();
        #[cfg(unix)]
        {
            let listener = tokio::net::UnixListener::bind(endpoint).unwrap();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                drop(listener);
                host(Box::new(stream), facts, done).await;
            })
        }
        #[cfg(windows)]
        {
            let listener = tokio::net::windows::named_pipe::ServerOptions::new()
                .first_pipe_instance(true)
                .create(endpoint)
                .unwrap();
            tokio::spawn(async move {
                listener.connect().await.unwrap();
                host(Box::new(listener), facts, done).await;
            })
        }
    }

    #[tokio::test]
    async fn muse_runtime_same_link_pending_next_delivery_and_optional_stream_loss() {
        let directory = Directory::new();
        let bootstrap = bootstrap(&directory.0);
        let (driver, custody, _) = fixture_native(false);
        let facts = Arc::new(Mutex::new(HostFacts::default()));
        let done = Arc::new(Notify::new());
        let host = host_task(&directory.0, facts.clone(), done.clone());
        let mut owner = RunOwner::start(bootstrap, driver.clone(), custody.clone());
        tokio::time::timeout(DEADLINE, done.notified())
            .await
            .unwrap();
        // Wait for the second optional publication to become pending. Control
        // has already delivered the reply outcome through that same session.
        tokio::time::timeout(DEADLINE, async {
            loop {
                if facts.lock().unwrap().stalled_stream == 1
                    && driver.outbox.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        {
            let f = facts.lock().unwrap();
            assert_eq!(f.auth, 1);
            assert_eq!(f.hello, 1);
            assert_eq!(f.outcomes, 2);
            assert_eq!(f.rejected_stream, 1);
            assert_eq!(f.next, 3);
        }
        facts.lock().unwrap().stop_requested = true;
        owner.control().request_stop();
        tokio::time::timeout(DEADLINE, owner.join())
            .await
            .unwrap()
            .unwrap();
        host.await.unwrap();
        assert_eq!(driver.native_writes.load(Ordering::SeqCst), 2);
        assert!(custody.joined.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn muse_runtime_consumer_panic_retains_outer_custody_until_stdio_join() {
        let directory = Directory::new();
        let (driver, custody, _) = fixture_native(false);
        driver.panic_hello.store(true, Ordering::SeqCst);
        let mut owner = RunOwner::start(bootstrap(&directory.0), driver.clone(), custody.clone());
        let error = tokio::time::timeout(DEADLINE, owner.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "MUSE_CONSUMER_TASK_FAILED");
        assert!(custody.joined.load(Ordering::SeqCst));
        assert_eq!(driver.native_writes.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn muse_runtime_consumer_error_retains_primary_failure_until_custody_join() {
        let directory = Directory::new();
        let (driver, custody, _) = fixture_native(false);
        driver.fail_flush.store(true, Ordering::SeqCst);
        let host = host_task(
            &directory.0,
            Arc::new(Mutex::new(HostFacts::default())),
            Arc::new(Notify::new()),
        );
        let mut owner = RunOwner::start(bootstrap(&directory.0), driver, custody.clone());
        let error = tokio::time::timeout(DEADLINE, owner.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "MUSE_JOURNAL_PERSISTENCE_UNKNOWN");
        host.await.unwrap();
        assert!(custody.joined.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn muse_runtime_cancelled_join_then_consumer_abort_rejoins_without_native_replay() {
        let directory = Directory::new();
        let (driver, custody, received) = fixture_native(true);
        let facts = Arc::new(Mutex::new(HostFacts::default()));
        let host = host_task(&directory.0, facts.clone(), Arc::new(Notify::new()));
        let mut owner = RunOwner::start(bootstrap(&directory.0), driver.clone(), custody.clone());
        tokio::time::timeout(DEADLINE, received.notified())
            .await
            .unwrap();
        let mut joined = Box::pin(owner.join());
        assert!(futures_util::poll!(joined.as_mut()).is_pending());
        drop(joined);
        assert!(!custody.joined.load(Ordering::SeqCst));
        owner.control().abort_consumer();
        let error = tokio::time::timeout(DEADLINE, owner.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "MUSE_CONSUMER_TASK_ABORTED");
        host.await.unwrap();
        assert!(custody.joined.load(Ordering::SeqCst));
        assert_eq!(driver.native_writes.load(Ordering::SeqCst), 1);
        assert_eq!(facts.lock().unwrap().auth, 1);
    }

    #[tokio::test]
    async fn muse_runtime_public_entry_enforces_admission_before_native_spawn() {
        let directory = Directory::new();
        let mut bootstrap = Arc::try_unwrap(bootstrap(&directory.0)).ok().unwrap();
        bootstrap.host.ipc.max_inflight_per_connection = 2;
        let error = tokio::time::timeout(DEADLINE, run_owned(bootstrap))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "MUSE_HOST_INFLIGHT_BOUND_UNSUPPORTED");
        assert!(!directory.0.join("muse-rust-scope.v1.json").exists());
        assert!(!directory.0.join("muse-rust-checkpoint.v1.json").exists());
    }

    include!("module_runtime_startup_shutdown_tests.rs");
}
