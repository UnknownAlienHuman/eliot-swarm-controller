use std::{collections::BTreeMap, path::PathBuf};

use eliot_core::{Error, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{ManifestFeatures, stream_sources},
    session::{
        decode, initialize_fields_valid, model_catalogue_profile, not_initialized,
        required_native_cmd, validate_binding, validate_identity,
    },
};
use serde_json::{Value, json};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

use crate::{
    events::FAMILIES,
    hooks::HookServer,
    session::{DeferredSteer, Session, SteerStart, unknown_steer},
};

pub(crate) const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Plugin {
    initialized: Option<InitializeParams>,
    session: Option<Session>,
    /// Set once the start reaches its pre-spawn latch; the daemon sends
    /// agent.start exactly once per plugin process and never replays it.
    start: Option<()>,
}

enum DispatchOutcome {
    Immediate(Value),
    Deferred(DeferredSteer),
}

struct DeferredCompletion {
    request_id: Value,
    generation: i64,
    command_id: String,
    turn_id: String,
    outcome: Result<Value>,
}

pub(crate) async fn run() -> Result<()> {
    let mut peer = Peer::stdio();
    let client = peer.client();
    let (tx, mut requests) = mpsc::channel(16);
    let reader = tokio::spawn(async move {
        while let Some(request) = peer.next_request().await {
            if tx.send(request).await.is_err() {
                break;
            }
        }
    });
    let mut plugin = Plugin::default();
    let (deferred_tx, mut deferred_rx) = mpsc::channel(16);
    let mut deferred_count = 0_usize;
    let mut shutdown_requested = false;
    let mut shutdown = false;
    let mut native_endpoint_retired = false;
    let mut native_retirement = Ok(());
    let mut transport_closed = false;
    let mut pending_request = None;
    while !shutdown_requested || deferred_count > 0 {
        enum Next {
            NativeExit,
            Request(Option<RpcRequest>),
            Deferred(Option<DeferredCompletion>),
        }
        let next = if let Some(request) = pending_request.take() {
            Next::Request(Some(request))
        } else {
            tokio::select! {
                biased;
                () = async {
                    match plugin.session.as_mut() {
                        Some(session) => session.wait_for_native_exit().await,
                        None => std::future::pending().await,
                    }
                }, if !shutdown_requested => Next::NativeExit,
                request = requests.recv(), if !shutdown_requested => Next::Request(request),
                completed = deferred_rx.recv(), if deferred_count > 0 => Next::Deferred(completed),
            }
        };
        match next {
            Next::NativeExit => {
                if let Some(session) = plugin.session.as_mut() {
                    native_retirement = session.retire_native_exit().await;
                }
                native_endpoint_retired = true;
                shutdown_requested = true;
            }
            Next::Request(Some(request)) => {
                let stopping = request.method == "shutdown";
                let dispatch = if request.method == "agent.snapshot" {
                    match eliot_plugin::preemptible_snapshot(
                        &mut requests,
                        plugin.dispatch(&request, &client),
                    )
                    .await
                    {
                        eliot_plugin::SnapshotDispatch::Ready(result) => result,
                        eliot_plugin::SnapshotDispatch::Preempted(next) => {
                            let preempted = client
                                .respond(
                                    request.id.clone(),
                                    Err(Error::new(
                                        "busy",
                                        "status snapshot was preempted by a host request",
                                    )),
                                    Instant::now() + TIMEOUT,
                                )
                                .await;
                            if preempted.is_err() {
                                transport_closed = true;
                                break;
                            }
                            let Some(next) = next else { break };
                            pending_request = Some(next);
                            continue;
                        }
                    }
                } else {
                    plugin.dispatch(&request, &client).await
                };
                match dispatch {
                    Ok(DispatchOutcome::Immediate(result)) => {
                        if client
                            .respond(request.id, Ok(result), Instant::now() + TIMEOUT)
                            .await
                            .is_err()
                        {
                            transport_closed = true;
                            break;
                        }
                        if stopping {
                            shutdown_requested = true;
                            shutdown = true;
                        }
                    }
                    Ok(DispatchOutcome::Deferred(deferred)) => {
                        let request_id = request.id;
                        let generation = deferred.generation;
                        let command_id = deferred.command_id.clone();
                        let turn_id = deferred.turn_id.clone();
                        let sender = deferred_tx.clone();
                        deferred_count += 1;
                        tokio::spawn(async move {
                            let outcome = match deferred.receiver.await {
                                Ok(outcome) => outcome,
                                Err(_) => Ok(unknown_steer(
                                    &command_id,
                                    &turn_id,
                                    "PreInvocation response outcome is unknown",
                                )),
                            };
                            let _ = sender
                                .send(DeferredCompletion {
                                    request_id,
                                    generation,
                                    command_id,
                                    turn_id,
                                    outcome,
                                })
                                .await;
                        });
                    }
                    Err(error) => {
                        if client
                            .respond(request.id, Err(error), Instant::now() + TIMEOUT)
                            .await
                            .is_err()
                        {
                            transport_closed = true;
                            break;
                        }
                        if stopping {
                            shutdown_requested = true;
                        }
                    }
                }
            }
            Next::Request(None) => break,
            Next::Deferred(Some(completion)) => {
                deferred_count = deferred_count.saturating_sub(1);
                let outcome = plugin.settle_steer(
                    completion.generation,
                    &completion.command_id,
                    &completion.turn_id,
                    completion.outcome,
                );
                if client
                    .respond(completion.request_id, outcome, Instant::now() + TIMEOUT)
                    .await
                    .is_err()
                {
                    transport_closed = true;
                    break;
                }
            }
            Next::Deferred(None) => {
                transport_closed = true;
                break;
            }
        }
    }
    if !native_endpoint_retired && let Some(session) = &mut plugin.session {
        let _ = session.terminate(true).await;
    }
    reader.abort();
    let _ = reader.await;
    if native_endpoint_retired && !transport_closed {
        native_retirement
    } else if shutdown && !transport_closed {
        Ok(())
    } else {
        Err(Error::new(
            "transport_closed",
            "Antigravity plugin input closed before shutdown completed",
        ))
    }
}

impl Plugin {
    async fn dispatch(
        &mut self,
        request: &RpcRequest,
        peer: &PeerClient,
    ) -> Result<DispatchOutcome> {
        match request.method.as_str() {
            "initialize" => self
                .initialize(&request.params)
                .map(DispatchOutcome::Immediate),
            "quota.read" => crate::quota::read(&request.params)
                .await
                .map(DispatchOutcome::Immediate),
            "profile.store" => Err(Error::new(
                "unsupported",
                "Antigravity exposes no verified metadata-only native-store readback",
            )),
            "model.list" => self
                .model_list(&request.params)
                .await
                .map(DispatchOutcome::Immediate),
            "agent.start" => self
                .start(&request.params, peer)
                .await
                .map(DispatchOutcome::Immediate),
            "agent.command" => {
                let params: AgentCommandParams =
                    decode(&request.params, "Invalid Antigravity command")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                let session = self.bound(&params.agent_id, params.generation)?;
                if params.verb == "steer" {
                    match session.begin_steer(params).await? {
                        SteerStart::Deferred(waiter) => Ok(DispatchOutcome::Deferred(waiter)),
                    }
                } else {
                    session
                        .command(params)
                        .await
                        .map(DispatchOutcome::Immediate)
                }
            }
            "agent.snapshot" => {
                let params: AgentSnapshotParams =
                    decode(&request.params, "Invalid Antigravity snapshot")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                self.bound(&params.agent_id, params.generation)?
                    .snapshot(params)
                    .await
                    .map(DispatchOutcome::Immediate)
            }
            "agent.stop" => {
                let params: AgentStopParams = decode(&request.params, "Invalid Antigravity stop")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                self.bound(&params.agent_id, params.generation)?
                    .stop(params)
                    .await
                    .map(DispatchOutcome::Immediate)
            }
            "shutdown" => {
                if let Some(session) = &mut self.session {
                    session.terminate(true).await?;
                }
                Ok(DispatchOutcome::Immediate(json!({"state": "shutdown"})))
            }
            _ => Err(Error::new(
                "unsupported",
                "Antigravity method is unconnected",
            )),
        }
    }

    fn settle_steer(
        &mut self,
        generation: i64,
        command_id: &str,
        turn_id: &str,
        outcome: Result<Value>,
    ) -> Result<Value> {
        self.session
            .as_mut()
            .ok_or_else(|| Error::new("unknown", "Antigravity session is no longer retained"))?
            .settle_steer(generation, command_id, turn_id, outcome)
    }

    /// Read-only native model catalogue for `harness.models`. A dedicated
    /// reader instance runs the frozen snapshot `cmd` with exact argv
    /// `models`, bounded by [`crate::native::MODELS_TIMEOUT`] and the output
    /// cap, with the same environment construction as the launch path. No
    /// session, no turn, no input and no inference. Ids are returned verbatim;
    /// `agy models` names no default, so `default` stays null.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        let snapshot = model_catalogue_profile(
            raw,
            self.session.is_some() || self.start.is_some(),
            "antigravity",
            "Antigravity",
            false,
        )?;
        let program = required_native_cmd(
            snapshot,
            "Antigravity model.list requires a frozen native cmd",
        )?;
        let environment = crate::profile::reader_environment(snapshot)?;
        // The catalogue errors are static typed strings (stderr is discarded,
        // like every other helper), so there is no native text to redact.
        let models = crate::native::run_models(
            std::ffi::OsStr::new(program),
            &environment.variables,
            Instant::now() + crate::native::MODELS_TIMEOUT,
        )
        .await?;
        Ok(json!({"models": models, "default": Value::Null}))
    }

    fn bound(&mut self, agent: &str, generation: i64) -> Result<&mut Session> {
        self.initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Antigravity"))?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Antigravity session is not retained"))?;
        validate_binding(
            &session.agent,
            session.generation,
            agent,
            generation,
            "Antigravity",
        )?;
        Ok(session)
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Invalid Antigravity initialization")?;
        if !initialize_fields_valid(&params) {
            return Err(Error::new(
                "invalid",
                "Antigravity requires protocol, private instance directory and credential",
            ));
        }
        let instance_dir = PathBuf::from(&params.instance_dir);
        if !instance_dir.is_absolute() || !instance_dir.is_dir() {
            return Err(Error::new(
                "invalid",
                "Antigravity requires an existing absolute private instance directory",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.instance_dir != params.instance_dir
                || existing.credential != params.credential
            {
                return Err(Error::new(
                    "bad_state",
                    "Antigravity already belongs to another instance",
                ));
            }
        } else {
            self.initialized = Some(params);
        }
        let manifest = PluginManifest::per_agent(
            "antigravity",
            env!("CARGO_PKG_VERSION"),
            &[
                "launch",
                "resume",
                "send",
                "configure",
                "steer",
                "interrupt",
                "approve",
                "deny",
                "stop",
            ],
            ManifestFeatures {
                start_requires_input: false,
                native_children: false,
                discover: false,
                observe: false,
            },
            BTreeMap::from([("controlled".into(), stream_sources(FAMILIES))]),
            "none",
        );
        serde_json::to_value(manifest)
            .map_err(|_| Error::new("internal", "Could not encode Antigravity manifest"))
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "Invalid Antigravity start")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Antigravity",
        )?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Antigravity"))?;
        let credential = initialized.credential.clone();
        let instance_dir = PathBuf::from(&initialized.instance_dir);
        // The daemon sends agent.start exactly once per freshly spawned
        // plugin process: a bound or settled plugin never starts again.
        if self.session.is_some() || self.start.is_some() {
            return Err(Error::new(
                "bad_state",
                "Antigravity plugin already received agent.start",
            ));
        }
        let hooks = HookServer::bind().await?;
        // Stage the dedicated hook-discovery root before planning: it is the
        // only `--add-dir` native root, and the private instance directory
        // is never exposed to the native CLI.
        let helper = crate::session::hook_helper_path()?;
        let workspace_path = params
            .workspace
            .as_deref()
            .map_or_else(PathBuf::new, PathBuf::from);
        // The bundled skill stages from the daemon-owned copy resolved
        // through the scoped integration environment; a missing copy fails the
        // start rather than starting an Agent without its skill.
        let data_dir = params
            .mcp_environment
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|environment| environment.get("ELIOT_HOME"))
            .and_then(serde_json::Value::as_str)
            .map(std::path::PathBuf::from)
            .filter(|path| path.is_absolute() && path.is_dir())
            .ok_or_else(|| {
                eliot_core::Error::new(
                    "not_allowed",
                    "Antigravity skill staging requires the Agent-bound Eliot data root",
                )
            })?;
        let hook_config =
            crate::session::stage_hook_config(&instance_dir, &workspace_path, &helper, &data_dir)
                .map_err(|error| error.with_code("native_not_dispatched"))?;
        let plan =
            crate::profile::Plan::new(&params, &credential, hooks.token(), hook_config.root())?;
        let mcp_config = crate::mcp::install(&plan, &instance_dir)
            .map_err(|error| error.with_code("native_not_dispatched"))?;
        // Latch before creating a native conversation: this generation
        // never starts a second process on retry.
        self.start = Some(());
        match Session::spawn(
            &params,
            &plan,
            peer,
            &credential,
            hook_config,
            hooks,
            mcp_config,
        )
        .await
        {
            Ok(session) => {
                self.session = Some(session);
                let session = self
                    .session
                    .as_mut()
                    .ok_or_else(|| Error::new("internal", "Antigravity session disappeared"))?;
                let result = session.start(&plan, peer, &credential).await;
                match result {
                    Err(error) if error.code == "native_model_unavailable" => {
                        if session.terminate(true).await.is_ok() {
                            let error = eliot_plugin::session::classify_start_failure(
                                error,
                                "antigravity",
                                "session",
                            );
                            let value = eliot_plugin::session::failed_start_value(&params, error);
                            Ok(value)
                        } else {
                            Err(Error::new(
                                "unknown",
                                "Antigravity start failed and owned cleanup is unconfirmed",
                            ))
                        }
                    }
                    Err(_) if session.terminate(true).await.is_err() => Err(Error::new(
                        "unknown",
                        "Antigravity start failed and owned cleanup is unconfirmed",
                    )),
                    other => other,
                }
            }
            Err(error) => Err(error),
        }
    }
}
