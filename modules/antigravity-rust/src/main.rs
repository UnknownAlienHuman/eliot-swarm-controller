use std::{env, path::PathBuf, time::Duration};

use serde_json::{Value, json};
use swarm_client::IpcConfig;
use swarm_contracts::{
    credential::Credential,
    error::{Error, Result},
    runtime::{RuntimeCommand, RuntimeOutcome},
};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

use swarm_antigravity_adapter::{
    config::AdapterConfig,
    controller::{Controller, PromptWrite},
    ipc::{ManagerLink, VerifiedModuleHello},
    launch::build_launch_spec,
    process::{
        CandidateManagedOwner, NativeExit, NativeMembershipGuard, OwnedNativeChild,
        OwnedNativeSpawner,
    },
    result_page,
    stderr::{self, StderrSummary},
    stream::NativeLineReader,
    wire::{ARTIFACT_ID, REQUIRED_MODEL_ID},
};

const MAX_HOST_ATTEMPTS: usize = 5;
const MAX_OBSERVATION_RETRIES: usize = 4;
const NATIVE_EVENT_QUEUE: usize = 16;
const OBSERVE_INTERVAL: Duration = Duration::from_secs(1);

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        // Error messages can contain local paths or transport details. Keep
        // process diagnostics to a closed code only.
        eprintln!("{}", json!({ "error_code": error.code }));
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config_path = config_argument()?;
    let owner = CandidateManagedOwner::from_environment()?;
    let (config, credential) = AdapterConfig::read(&config_path)?;
    let boot_id = owner.token().to_owned();
    let native_scope_key = native_scope_key()?;

    let mut host = HostSession::new(config, credential, owner, boot_id, native_scope_key);
    let initial_hello = host.connect_and_hello(None, false).await?;
    let mut controller = controller_from_hello(&host, &initial_hello)?;
    let spawner = OwnedNativeSpawner::new(initial_hello.managed_owner);
    let mut native: Option<NativeSession> = None;
    let mut native_started = false;
    let mut native_live = false;
    let mut dispatch_closed = false;
    // A new process has no native stream readback yet. Do not publish an empty
    // snapshot over the Store's last observation merely because hello returned
    // the retained root identity.
    let mut dirty = false;

    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;

    loop {
        drain_ready_native(
            native.as_mut(),
            &mut controller,
            &mut dirty,
            &mut native_live,
            &mut dispatch_closed,
        )
        .await?;
        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;

        let command = match host.next(&controller, native_live).await {
            Ok(command) => command,
            Err(error) if error.code == "MODULE_NEXT_UNCERTAIN" => {
                // The Store may already have durably admitted a command, but
                // no command identity reached this process. Do not request a
                // second command or infer which Operation was returned.
                let _ = host.reconnect(&controller, native_live).await;
                let _ = flush_reports(&mut host, &mut controller, &mut dirty, native_live).await;
                if let Some(native) = native.as_mut() {
                    native.shutdown_and_drain(&mut controller, true).await?;
                }
                return Err(error);
            }
            Err(error) => {
                if let Some(native) = native.as_mut() {
                    native.shutdown_and_drain(&mut controller, true).await?;
                }
                return Err(error);
            }
        };

        let Some(command) = command else {
            continue;
        };

        if host.route.as_ref() != Some(&command.route) {
            controller.reject_command(&command, "MANAGER_ROUTE_CHANGED")?;
            dirty = true;
            flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
            continue;
        }

        match command.method.as_str() {
            "agent.open" => {
                if native_started || dispatch_closed || host.recovery_required {
                    controller.reject_command(
                        &command,
                        if host.recovery_required {
                            "RECOVERY_REQUIRED"
                        } else {
                            "NATIVE_SESSION_ALREADY_STARTED"
                        },
                    )?;
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    continue;
                }
                if controller.begin_open(&command).is_err() {
                    controller.reject_command(&command, "NATIVE_SESSION_ALREADY_OPEN")?;
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    continue;
                }

                let spec = match build_launch_spec(&host.config.native_executable, &command) {
                    Ok(spec) => spec,
                    Err(code) => {
                        controller.reject_open_before_spawn(code);
                        dirty = true;
                        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                        continue;
                    }
                };
                let spawned = match spawner.spawn_owned(spec) {
                    Ok(child) => child,
                    Err(_) => {
                        controller.reject_open_before_spawn("NATIVE_SPAWN_FAILED");
                        dirty = true;
                        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                        continue;
                    }
                };
                native_started = true;
                let membership_verified = spawned.membership_error_code().is_none();
                let mut session = NativeSession::start(spawned)?;
                native_live = membership_verified;

                if !membership_verified {
                    controller.finish_open_without_init();
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    session.shutdown_and_drain(&mut controller, false).await?;
                    native_live = false;
                    dispatch_closed = true;
                    native = Some(session);
                    continue;
                }

                let open = wait_for_open(
                    &mut session,
                    &mut controller,
                    &mut host,
                    &mut dirty,
                    &command,
                )
                .await;
                match open {
                    Ok(OpenProgress::Ready) => {
                        native_live = true;
                        dirty = true;
                    }
                    Ok(OpenProgress::Ended) => {
                        native_live = false;
                        dispatch_closed = true;
                        dirty = true;
                    }
                    Err(error) => {
                        session.shutdown_and_drain(&mut controller, true).await?;
                        return Err(error);
                    }
                }
                native = Some(session);
                flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
            }
            "task.dispatch" | "agent.send" => {
                if dispatch_closed || host.recovery_required {
                    controller.reject_command(
                        &command,
                        if host.recovery_required {
                            "RECOVERY_REQUIRED"
                        } else {
                            "NATIVE_EFFECTS_CLOSED"
                        },
                    )?;
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    continue;
                }
                let Some(session) = native.as_mut() else {
                    controller.reject_command(&command, "NATIVE_SESSION_NOT_READY")?;
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    continue;
                };
                if session.membership_guard.verify().is_err() {
                    controller.reject_command(&command, "NATIVE_OWNER_MEMBERSHIP_UNVERIFIED")?;
                    dirty = true;
                    flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    session.shutdown_and_drain(&mut controller, true).await?;
                    native_live = false;
                    dispatch_closed = true;
                    continue;
                }
                let line = match controller.begin_prompt(&command) {
                    Ok(line) => line,
                    Err(_) => {
                        controller.reject_command(&command, "NATIVE_PROMPT_REJECTED")?;
                        dirty = true;
                        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                        continue;
                    }
                };
                let write = controller.write_prompt(&mut session.stdin, &line).await;
                match write {
                    PromptWrite::Written => {
                        match wait_for_terminal(session, &mut controller, &mut host, &mut dirty)
                            .await
                        {
                            Ok(TurnProgress::Settled) => dirty = true,
                            Ok(TurnProgress::Ended) => {
                                native_live = false;
                                dispatch_closed = true;
                                dirty = true;
                            }
                            Err(error) => {
                                session.shutdown_and_drain(&mut controller, true).await?;
                                return Err(error);
                            }
                        }
                    }
                    PromptWrite::Unknown(_) => {
                        // The write may have reached Antigravity. Preserve the
                        // exact Unknown Operation and never submit this prompt
                        // or another mutating prompt again in this process.
                        dirty = true;
                        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                        session.shutdown_and_drain(&mut controller, true).await?;
                        native_live = false;
                        dispatch_closed = true;
                        dirty = true;
                    }
                    PromptWrite::MissingPendingOperation => {
                        dispatch_closed = true;
                        session.shutdown_and_drain(&mut controller, true).await?;
                        native_live = false;
                    }
                }
                flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
            }
            "agent.refresh" | "agent.reconcile" => {
                match controller.read_command(&command) {
                    Ok(_) => dirty = true,
                    Err(_) => {
                        controller.reject_command(&command, "NATIVE_READBACK_UNAVAILABLE")?;
                        dirty = true;
                    }
                }
                flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
            }
            "agent.reply" => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "CAPABILITY_REPLY_UNAVAILABLE",
                )
                .await?
            }
            "agent.configure" => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "CAPABILITY_CONFIGURE_UNAVAILABLE",
                )
                .await?
            }
            "agent.goal" => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "CAPABILITY_GOAL_UNAVAILABLE",
                )
                .await?
            }
            "agent.background" => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "CAPABILITY_BACKGROUND_UNAVAILABLE",
                )
                .await?
            }
            "agent.result" => {
                let params = if command.input["normalized_result_origin"].is_object() {
                    result_page::build_normalized(&command)
                } else {
                    result_page::build(&command)
                };
                match params {
                    Ok(params) => {
                        let response = match host
                            .request_saved("module.result", params, &controller, native_live)
                            .await
                        {
                            Ok(response) => response,
                            Err(error) => {
                                let Some(diagnostic_code) =
                                    result_ack_diagnostic(error.code.as_str())
                                else {
                                    return Err(error);
                                };
                                controller.reject_command(&command, diagnostic_code)?;
                                dirty = true;
                                flush_reports(&mut host, &mut controller, &mut dirty, native_live)
                                    .await?;
                                continue;
                            }
                        };
                        if response["recorded"] != true {
                            controller.reject_command(&command, "MODULE_RESULT_ACK_INVALID")?;
                            dirty = true;
                            flush_reports(&mut host, &mut controller, &mut dirty, native_live)
                                .await?;
                        }
                    }
                    Err(error) => {
                        controller.reject_command(
                            &command,
                            result_page_diagnostic(error.code.as_str()),
                        )?;
                        dirty = true;
                        flush_reports(&mut host, &mut controller, &mut dirty, native_live).await?;
                    }
                }
            }
            "agent.recover" => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "CROSS_BOOT_RECOVERY_UNAVAILABLE",
                )
                .await?
            }
            _ => {
                reject_and_report(
                    &mut host,
                    &mut controller,
                    &mut dirty,
                    native_live,
                    &command,
                    "UNSUPPORTED_OPERATION",
                )
                .await?
            }
        }
    }
}

async fn reject_and_report(
    host: &mut HostSession,
    controller: &mut Controller,
    dirty: &mut bool,
    native_live: bool,
    command: &RuntimeCommand,
    diagnostic_code: &'static str,
) -> Result<()> {
    controller.reject_command(command, diagnostic_code)?;
    *dirty = true;
    flush_reports(host, controller, dirty, native_live).await
}

fn result_page_diagnostic(code: &str) -> &'static str {
    match code {
        "RESULT_RANGE_INVALID" => "RESULT_RANGE_INVALID",
        "RESULT_BODY_UNAVAILABLE" => "RESULT_BODY_UNAVAILABLE",
        "RESULT_PROVENANCE_INVALID" => "RESULT_PROVENANCE_INVALID",
        "RESULT_SELECTOR_UNSUPPORTED" => "RESULT_SELECTOR_UNSUPPORTED",
        _ => "RESULT_PAGE_UNAVAILABLE",
    }
}

/// Store can reject a fully built page before it is durably registered. Keep
/// deterministic result/provenance diagnostics on the same admitted result
/// Operation; transport-uncertain failures stay unresolved because the page
/// may already have been recorded.
fn result_ack_diagnostic(code: &str) -> Option<&'static str> {
    match code {
        "MODULE_RECEIPT_INVALID" => Some("MODULE_RECEIPT_INVALID"),
        "RESULT_BODY_UNAVAILABLE" => Some("RESULT_BODY_UNAVAILABLE"),
        "RESULT_PROVENANCE_INVALID" => Some("RESULT_PROVENANCE_INVALID"),
        "RESULT_RANGE_INVALID" => Some("RESULT_RANGE_INVALID"),
        "RESULT_ORIGIN_INVALID" => Some("RESULT_ORIGIN_INVALID"),
        "RESULT_SELECTOR_UNSUPPORTED" => Some("RESULT_SELECTOR_UNSUPPORTED"),
        "RESULT_TARGET_NOT_ADMITTED" => Some("RESULT_TARGET_NOT_ADMITTED"),
        "RESULT_TARGET_NOT_TERMINAL" => Some("RESULT_TARGET_NOT_TERMINAL"),
        "RESULT_TARGET_ORIGIN_INVALID" => Some("RESULT_TARGET_ORIGIN_INVALID"),
        "RESULT_TARGET_RECEIPT_INVALID" => Some("RESULT_TARGET_RECEIPT_INVALID"),
        "RESULT_TARGET_SCOPE_INVALID" => Some("RESULT_TARGET_SCOPE_INVALID"),
        _ => None,
    }
}

fn controller_from_hello(host: &HostSession, hello: &VerifiedModuleHello) -> Result<Controller> {
    let binding_id = hello
        .response
        .get("binding_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::new("MODULE_HELLO_ACK_INVALID", "binding identity is missing"))?;
    let generation = hello
        .response
        .get("generation")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::new("MODULE_HELLO_ACK_INVALID", "binding generation is invalid"))?;
    let native_root_id = hello
        .response
        .get("native_root_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let native_scope_key = hello
        .response
        .get("native_scope_key")
        .and_then(Value::as_str);
    if native_root_id.is_some() != native_scope_key.is_some()
        || native_scope_key.is_some_and(|scope| scope != host.native_scope_key)
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "manager returned an incomplete or foreign native root identity",
        ));
    }
    let normalized_dispatch_enabled =
        swarm_antigravity_adapter::contract::normalized_dispatch_enabled(
            &swarm_antigravity_adapter::contract::claim()?,
        );
    Ok(Controller::new(
        host.boot_id.clone(),
        host.native_scope_key.clone(),
        binding_id.to_owned(),
        generation,
        native_root_id,
        normalized_dispatch_enabled,
    ))
}

fn config_argument() -> Result<PathBuf> {
    let mut args = env::args_os();
    let _program = args.next();
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::invalid("usage: swarm-antigravity <absolute-config.json>"))?;
    if args.next().is_some() || !path.is_absolute() {
        return Err(Error::invalid(
            "usage: swarm-antigravity <absolute-config.json>",
        ));
    }
    Ok(path)
}

fn native_scope_key() -> Result<String> {
    let home = if cfg!(windows) {
        env::var_os("USERPROFILE").or_else(|| env::var_os("HOME"))
    } else {
        env::var_os("HOME")
    }
    .map(PathBuf::from)
    .filter(|path| path.is_absolute())
    .ok_or_else(|| Error::new("NATIVE_SCOPE_UNAVAILABLE", "home directory is unavailable"))?;
    let scope = home.join(".gemini").join("antigravity-cli");
    let scope = scope
        .to_str()
        .filter(|value| value.len() <= 32 * 1024 && !value.chars().any(char::is_control))
        .ok_or_else(|| Error::new("NATIVE_SCOPE_UNAVAILABLE", "native scope path is invalid"))?;
    Ok(format!("antigravity:{scope}"))
}

struct HostSession {
    config: AdapterConfig,
    credential: Credential,
    owner: CandidateManagedOwner,
    boot_id: String,
    native_scope_key: String,
    link: Option<ManagerLink>,
    binding_id: Option<String>,
    generation: Option<i64>,
    route: Option<Value>,
    recovery_required: bool,
}

impl HostSession {
    fn new(
        config: AdapterConfig,
        credential: Credential,
        owner: CandidateManagedOwner,
        boot_id: String,
        native_scope_key: String,
    ) -> Self {
        Self {
            config,
            credential,
            owner,
            boot_id,
            native_scope_key,
            link: None,
            binding_id: None,
            generation: None,
            route: None,
            recovery_required: false,
        }
    }

    async fn connect_and_hello(
        &mut self,
        controller: Option<&Controller>,
        native_live: bool,
    ) -> Result<VerifiedModuleHello> {
        let root_id = controller.and_then(Controller::native_root_id);
        let native_scope = root_id.map(|_| self.native_scope_key.clone());
        let native_ready = native_live && root_id.is_some();
        let mut last_error = None;
        for attempt in 0..MAX_HOST_ATTEMPTS {
            let mut link = match ManagerLink::connect(
                &self.config.host_data_dir,
                &self.credential,
                &IpcConfig::default(),
            )
            .await
            {
                Ok(link) => link,
                Err(error) if is_transport_uncertain(&error) => {
                    last_error = Some(error);
                    if attempt + 1 < MAX_HOST_ATTEMPTS {
                        time::sleep(retry_delay(attempt)).await;
                        continue;
                    }
                    break;
                }
                Err(error) => return Err(error),
            };
            match link
                .hello(
                    native_ready,
                    root_id,
                    native_scope.as_deref(),
                    self.owner.clone_for_reconnect(),
                )
                .await
            {
                Ok(hello) => {
                    self.accept_hello(&hello.response, root_id, native_scope.as_deref())?;
                    self.recovery_required = hello
                        .response
                        .get("recovery_required")
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                    self.link = Some(link);
                    return Ok(hello);
                }
                Err(error) if is_transport_uncertain(&error) => {
                    last_error = Some(error);
                    self.link = None;
                    if attempt + 1 < MAX_HOST_ATTEMPTS {
                        time::sleep(retry_delay(attempt)).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let _ = last_error;
        Err(Error::new(
            "HOST_RETRY_EXHAUSTED",
            "controller did not accept the bounded module handshake retries",
        ))
    }

    fn accept_hello(
        &mut self,
        response: &Value,
        expected_native_root_id: Option<&str>,
        expected_native_scope_key: Option<&str>,
    ) -> Result<()> {
        let binding_id = response
            .get("binding_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("MODULE_HELLO_ACK_INVALID", "binding identity is missing"))?;
        let generation = response
            .get("generation")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::new("MODULE_HELLO_ACK_INVALID", "binding generation is invalid")
            })?;
        let route = response
            .get("route")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| Error::new("MODULE_HELLO_ACK_INVALID", "manager route is missing"))?;
        let native_root_id = response
            .get("native_root_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let native_scope_key = response
            .get("native_scope_key")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        if native_root_id.is_some() != native_scope_key.is_some()
            || native_scope_key.is_some_and(|scope| scope != self.native_scope_key)
            || expected_native_root_id.is_some_and(|expected| native_root_id != Some(expected))
            || expected_native_scope_key.is_some_and(|expected| native_scope_key != Some(expected))
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "manager handshake native identity is incomplete or differs from this adapter scope",
            ));
        }
        if route["runtime"] != "antigravity" || route["module_artifact_id"] != ARTIFACT_ID {
            return Err(Error::new(
                "ARTIFACT_MISMATCH",
                "manager route is not registered for the Rust headless artifact",
            ));
        }
        if route["native_options"]["modelId"] != REQUIRED_MODEL_ID {
            return Err(Error::new(
                "UNSUPPORTED_MODEL_ID",
                "manager route does not select the exact documented Antigravity model ID",
            ));
        }
        if self
            .binding_id
            .as_deref()
            .is_some_and(|old| old != binding_id)
            || self.generation.is_some_and(|old| old != generation)
            || self.route.as_ref().is_some_and(|old| old != &route)
        {
            return Err(Error::new(
                "BINDING_IDENTITY_MISMATCH",
                "manager handshake changed the adapter binding identity",
            ));
        }
        self.binding_id = Some(binding_id.to_owned());
        self.generation = Some(generation);
        self.route = Some(route);
        Ok(())
    }

    async fn reconnect(&mut self, controller: &Controller, native_live: bool) -> Result<()> {
        self.link = None;
        self.connect_and_hello(Some(controller), native_live)
            .await
            .map(|_| ())
    }

    async fn request_saved(
        &mut self,
        method: &str,
        params: Value,
        controller: &Controller,
        native_live: bool,
    ) -> Result<Value> {
        for attempt in 0..MAX_HOST_ATTEMPTS {
            if self.link.is_none() {
                self.reconnect(controller, native_live).await?;
            }
            let result = self
                .link
                .as_mut()
                .ok_or_else(|| Error::new("HOST_UNAVAILABLE", "controller link is unavailable"))?
                .request(method, params.clone())
                .await;
            match result {
                Ok(result) => return Ok(result),
                Err(error) if is_transport_uncertain(&error) => {
                    self.link = None;
                    if attempt + 1 < MAX_HOST_ATTEMPTS {
                        time::sleep(retry_delay(attempt)).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(Error::new(
            "HOST_RETRY_EXHAUSTED",
            "controller did not acknowledge the saved idempotent request",
        ))
    }

    async fn next(
        &mut self,
        controller: &Controller,
        native_live: bool,
    ) -> Result<Option<RuntimeCommand>> {
        for attempt in 0..MAX_HOST_ATTEMPTS {
            if self.link.is_none() {
                self.reconnect(controller, native_live).await?;
            }
            let result = self
                .link
                .as_mut()
                .ok_or_else(|| Error::new("HOST_UNAVAILABLE", "controller link is unavailable"))?
                .next(controller)
                .await;
            match result {
                Ok(command) => return Ok(command),
                Err(error) if error.code == "OUTCOME_UNKNOWN" => {
                    self.link = None;
                    return Err(Error::new(
                        "MODULE_NEXT_UNCERTAIN",
                        "module.next may have durably admitted a command; do not request another",
                    ));
                }
                Err(error)
                    if matches!(error.code.as_str(), "HOST_UNAVAILABLE" | "DISCONNECTED") =>
                {
                    self.link = None;
                    if attempt + 1 < MAX_HOST_ATTEMPTS {
                        time::sleep(retry_delay(attempt)).await;
                    }
                }
                Err(error) => {
                    self.link = None;
                    return Err(error);
                }
            }
        }
        Err(Error::new(
            "HOST_RETRY_EXHAUSTED",
            "controller did not answer bounded module.next retries",
        ))
    }
}

fn is_transport_uncertain(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "HOST_UNAVAILABLE" | "DISCONNECTED" | "OUTCOME_UNKNOWN" | "IO_ERROR"
    )
}

fn retry_delay(attempt: usize) -> Duration {
    let multiplier = 1_u32.checked_shl(attempt.min(5) as u32).unwrap_or(32);
    Duration::from_millis(150_u64.saturating_mul(u64::from(multiplier)).min(4_800))
}

async fn flush_reports(
    host: &mut HostSession,
    controller: &mut Controller,
    dirty: &mut bool,
    native_live: bool,
) -> Result<()> {
    for mut value in controller.pending_receipts() {
        let operation_id = value
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::new("MODULE_OUTCOME_INVALID", "retained Operation ID is missing")
            })?
            .to_owned();
        if Controller::receipt_needs_observation(&value) {
            let observation_id =
                record_observation_for_receipt(host, controller, native_live).await?;
            let mut outcome: RuntimeOutcome = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "MODULE_OUTCOME_INVALID",
                    "retained Operation receipt is invalid",
                )
            })?;
            controller.bind_observation_id(&mut outcome, observation_id)?;
            value = serde_json::to_value(outcome).map_err(|_| {
                Error::new(
                    "MODULE_OUTCOME_INVALID",
                    "bound Operation receipt is invalid",
                )
            })?;
        }
        let response = host
            .request_saved("module.outcome", value, controller, native_live)
            .await?;
        if response["recorded"] != true && response["superseded"] != true {
            return Err(Error::new(
                "MODULE_OUTCOME_ACK_INVALID",
                "manager did not acknowledge the exact saved Operation receipt",
            ));
        }
        controller.acknowledge_outcome(&operation_id);
        *dirty = true;
    }

    if *dirty {
        for _ in 0..MAX_OBSERVATION_RETRIES {
            let observation = controller.prepare_observation();
            let response = host
                .request_saved(
                    "module.observe",
                    observation_params(&observation),
                    controller,
                    native_live,
                )
                .await?;
            if controller.acknowledge_observation(&response).is_some() {
                controller.finish_observation();
                *dirty = false;
                return Ok(());
            }
            if response["stale"] == true {
                *dirty = true;
                continue;
            }
            if response["replayed"] == true && response["recorded"] == true {
                *dirty = false;
                return Ok(());
            }
            return Err(Error::new(
                "MODULE_OBSERVATION_ACK_INVALID",
                "manager did not acknowledge the saved observation",
            ));
        }
        return Err(Error::new(
            "MODULE_OBSERVATION_STALE",
            "manager did not accept a fresh ordered observation",
        ));
    }
    Ok(())
}

async fn record_observation_for_receipt(
    host: &mut HostSession,
    controller: &mut Controller,
    native_live: bool,
) -> Result<i64> {
    for _ in 0..MAX_OBSERVATION_RETRIES {
        let observation = controller.prepare_observation();
        let response = host
            .request_saved(
                "module.observe",
                observation_params(&observation),
                controller,
                native_live,
            )
            .await?;
        if let Some(observation_id) = controller.acknowledge_observation(&response) {
            return Ok(observation_id);
        }
        if response["stale"] == true || response["replayed"] == true {
            continue;
        }
        return Err(Error::new(
            "MODULE_OBSERVATION_ACK_INVALID",
            "manager did not record an observation for the local result receipt",
        ));
    }
    Err(Error::new(
        "MODULE_OBSERVATION_STALE",
        "manager did not return a current observation identity for the local result",
    ))
}

fn observation_params(
    observation: &swarm_antigravity_adapter::controller::PendingObservation,
) -> Value {
    json!({
        "event_id": observation.event_id,
        "sequence": observation.sequence,
        "state": observation.state,
    })
}

#[derive(Debug)]
enum NativeEvent {
    Line(String),
    FrameError,
}

enum NativeWaitResult {
    Stream(Option<NativeEvent>),
    Child(NativeExit),
}

struct NativeSession {
    stdin: tokio::process::ChildStdin,
    events: mpsc::Receiver<NativeEvent>,
    stdout_wait: Option<JoinHandle<()>>,
    child_wait: Option<JoinHandle<Result<NativeExit>>>,
    stderr_wait: Option<JoinHandle<StderrSummary>>,
    exit: Option<NativeExit>,
    stream_tail_deadline: Option<time::Instant>,
    membership_guard: NativeMembershipGuard,
}

impl NativeSession {
    fn start(mut child: OwnedNativeChild) -> Result<Self> {
        let membership_guard = child.membership_guard();
        let stdin = child
            .take_stdin()
            .ok_or_else(|| Error::new("NATIVE_PIPE_MISSING", "native stdin pipe is unavailable"))?;
        let stdout = child.take_stdout().ok_or_else(|| {
            Error::new("NATIVE_PIPE_MISSING", "native stdout pipe is unavailable")
        })?;
        let stderr = child.take_stderr().ok_or_else(|| {
            Error::new("NATIVE_PIPE_MISSING", "native stderr pipe is unavailable")
        })?;

        let (sender, events) = mpsc::channel(NATIVE_EVENT_QUEUE);
        let stdout_wait = tokio::spawn(async move {
            let mut reader = NativeLineReader::new(stdout);
            while let Some(line) = reader.next_line().await {
                let event = match line {
                    Ok(line) => NativeEvent::Line(line),
                    Err(_) => NativeEvent::FrameError,
                };
                if sender.send(event).await.is_err() {
                    return;
                }
            }
        });
        let stderr_wait = tokio::spawn(stderr::drain(stderr));
        let child_wait = tokio::spawn(async move {
            let mut child = child;
            child.wait().await
        });
        Ok(Self {
            stdin,
            events,
            stdout_wait: Some(stdout_wait),
            child_wait: Some(child_wait),
            stderr_wait: Some(stderr_wait),
            exit: None,
            stream_tail_deadline: None,
            membership_guard,
        })
    }

    async fn next_event(&mut self) -> Result<Option<NativeEvent>> {
        loop {
            if let Some(deadline) = self.stream_tail_deadline {
                tokio::select! {
                    biased;
                    event = self.events.recv() => match event {
                        Some(event) => return Ok(Some(event)),
                        None => {
                            self.finish_readers(false).await;
                            return Ok(None);
                        }
                    },
                    _ = time::sleep_until(deadline) => {
                        self.finish_readers(true).await;
                        return Ok(None);
                    }
                }
            }
            let event_or_exit = {
                let wait = self.child_wait.as_mut().ok_or_else(|| {
                    Error::new(
                        "NATIVE_WAIT_UNAVAILABLE",
                        "direct child wait handle is unavailable",
                    )
                })?;
                tokio::select! {
                    event = self.events.recv() => NativeWaitResult::Stream(event),
                    result = wait => NativeWaitResult::Child(
                        result
                            .map_err(|_| Error::new("NATIVE_WAIT_FAILED", "direct child wait task failed"))??,
                    ),
                }
            };
            match event_or_exit {
                NativeWaitResult::Stream(Some(event)) => return Ok(Some(event)),
                NativeWaitResult::Stream(None) => {
                    self.wait_for_direct_exit().await?;
                    return Ok(None);
                }
                NativeWaitResult::Child(exit) => {
                    self.exit = Some(exit);
                    self.child_wait = None;
                    self.stream_tail_deadline = Some(time::Instant::now() + Duration::from_secs(2));
                }
            }
        }
    }

    fn take_ready_event(
        &mut self,
    ) -> std::result::Result<Option<NativeEvent>, mpsc::error::TryRecvError> {
        match self.events.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(error @ mpsc::error::TryRecvError::Disconnected) => Err(error),
        }
    }

    async fn wait_for_direct_exit(&mut self) -> Result<()> {
        if self.exit.is_some() {
            return Ok(());
        }
        let wait = self.child_wait.take().ok_or_else(|| {
            Error::new(
                "NATIVE_WAIT_UNAVAILABLE",
                "direct child wait handle is unavailable",
            )
        })?;
        let exit = wait
            .await
            .map_err(|_| Error::new("NATIVE_WAIT_FAILED", "direct child wait task failed"))??;
        self.exit = Some(exit);
        self.finish_readers(false).await;
        Ok(())
    }

    async fn finish_readers(&mut self, abort_stdout: bool) {
        if let Some(stdout_wait) = self.stdout_wait.take() {
            if abort_stdout {
                stdout_wait.abort();
            } else {
                let _ = stdout_wait.await;
            }
        }
        if let Some(mut stderr_wait) = self.stderr_wait.take()
            && time::timeout(Duration::from_millis(250), &mut stderr_wait)
                .await
                .is_err()
        {
            stderr_wait.abort();
        }
    }

    fn exit_code(&self) -> Option<i32> {
        self.exit.as_ref().and_then(|exit| exit.exit_code)
    }

    fn direct_child_exit_ready(&self) -> bool {
        self.exit.is_some()
            || self
                .child_wait
                .as_ref()
                .is_some_and(|wait| wait.is_finished())
    }

    async fn shutdown_and_drain(
        &mut self,
        controller: &mut Controller,
        trust_stream: bool,
    ) -> Result<()> {
        let _ = self.stdin.shutdown().await;
        let mut event_stream_open = true;
        while self.exit.is_none() {
            if event_stream_open {
                let child_exit = {
                    let wait = self.child_wait.as_mut().ok_or_else(|| {
                        Error::new(
                            "NATIVE_WAIT_UNAVAILABLE",
                            "direct child wait handle is unavailable",
                        )
                    })?;
                    tokio::select! {
                        event = self.events.recv() => {
                            match event {
                                Some(event) => if trust_stream {
                                    consume_native_event(controller, event);
                                    let _ = controller.settle_terminal();
                                },
                                None => event_stream_open = false,
                            }
                            None
                        },
                        result = wait => Some(
                            result
                                .map_err(|_| Error::new("NATIVE_WAIT_FAILED", "direct child wait task failed"))??,
                        )
                    }
                };
                if let Some(exit) = child_exit {
                    self.exit = Some(exit);
                    self.child_wait = None;
                }
            } else {
                self.wait_for_direct_exit().await?;
            }
        }
        let mut stream_incomplete = false;
        let drain_tail = time::sleep(Duration::from_secs(2));
        tokio::pin!(drain_tail);
        loop {
            tokio::select! {
                event = self.events.recv() => match event {
                    Some(event) => if trust_stream {
                        consume_native_event(controller, event);
                        let _ = controller.settle_terminal();
                    },
                    None => break,
                },
                _ = &mut drain_tail => {
                    stream_incomplete = true;
                    break;
                }
            }
        }
        self.finish_readers(stream_incomplete).await;
        if trust_stream {
            if stream_incomplete {
                let _ = controller.stdout_failed();
            }
            controller.child_exited(self.exit_code());
        }
        Ok(())
    }
}

fn consume_native_event(controller: &mut Controller, event: NativeEvent) {
    match event {
        NativeEvent::Line(line) => controller.consume_line(line.as_bytes()),
        NativeEvent::FrameError => controller.consume_frame_error(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenProgress {
    Ready,
    Ended,
}

async fn wait_for_open(
    native: &mut NativeSession,
    controller: &mut Controller,
    host: &mut HostSession,
    dirty: &mut bool,
    command: &RuntimeCommand,
) -> Result<OpenProgress> {
    let mut timer = time::interval(OBSERVE_INTERVAL);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let _ = timer.tick().await;
    loop {
        tokio::select! {
            event = native.next_event() => match event? {
                Some(event) => {
                    consume_native_event(controller, event);
                    *dirty = true;
                    if controller.stream().init.is_some() {
                        match controller.complete_open(command) {
                            Ok(_) => return Ok(OpenProgress::Ready),
                            Err(_) => {
                                controller.finish_open_without_init();
                                return Ok(OpenProgress::Ended);
                            }
                        }
                    }
                    if controller.stream().phase == swarm_antigravity_adapter::stream::Phase::InitFailed {
                        controller.finish_open_without_init();
                        return Ok(OpenProgress::Ended);
                    }
                }
                None => {
                    controller.child_exited(native.exit_code());
                    *dirty = true;
                    return Ok(OpenProgress::Ended);
                }
            },
            _ = timer.tick() => {
                flush_reports(host, controller, dirty, false).await?;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnProgress {
    Settled,
    Ended,
}

async fn wait_for_terminal(
    native: &mut NativeSession,
    controller: &mut Controller,
    host: &mut HostSession,
    dirty: &mut bool,
) -> Result<TurnProgress> {
    let mut timer = time::interval(OBSERVE_INTERVAL);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let _ = timer.tick().await;
    loop {
        tokio::select! {
            event = native.next_event() => match event? {
                Some(event) => {
                    consume_native_event(controller, event);
                    *dirty = true;
                    if controller.settle_terminal().is_some() {
                        return Ok(TurnProgress::Settled);
                    }
                }
                None => {
                    controller.child_exited(native.exit_code());
                    *dirty = true;
                    return Ok(TurnProgress::Ended);
                }
            },
            _ = timer.tick() => {
                flush_reports(host, controller, dirty, true).await?;
            }
        }
    }
}

async fn drain_ready_native(
    native: Option<&mut NativeSession>,
    controller: &mut Controller,
    dirty: &mut bool,
    native_live: &mut bool,
    dispatch_closed: &mut bool,
) -> Result<()> {
    let Some(native) = native else { return Ok(()) };
    loop {
        match native.take_ready_event() {
            Ok(Some(event)) => {
                consume_native_event(controller, event);
                *dirty = true;
            }
            Ok(None) => {
                if !native.direct_child_exit_ready() {
                    return Ok(());
                }
                while let Some(event) = native.next_event().await? {
                    consume_native_event(controller, event);
                    let _ = controller.settle_terminal();
                    *dirty = true;
                }
                controller.child_exited(native.exit_code());
                *native_live = false;
                *dispatch_closed = true;
                *dirty = true;
                return Ok(());
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                if native.exit.is_some() {
                    return Ok(());
                }
                native.wait_for_direct_exit().await?;
                controller.child_exited(native.exit_code());
                *native_live = false;
                *dispatch_closed = true;
                *dirty = true;
                return Ok(());
            }
            Err(mpsc::error::TryRecvError::Empty) => return Ok(()),
        }
    }
}
