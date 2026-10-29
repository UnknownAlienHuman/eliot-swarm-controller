use crate::{
    peer::{CommitSink, Peer, PeerClient, preserve_rpc_error},
    protocol::{
        AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams,
        DiscoverCoverage, DiscoverPage, DiscoverParams, DiscoverSubject, InitializeParams,
        MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, MAX_DISCOVER_PAGE_BYTES,
        MAX_DISCOVER_PAGE_ENTRIES, MAX_EVENT_BYTES, MAX_TEXT_BYTES, ObservationPage,
        ObservationQuery, ObserverAttachParams, ObserverDetachParams, ObserverSnapshotParams,
        RpcError, RpcRequest, empty_object, fake_manifest,
    },
};
use eliot_core::{
    BatchAck, Error as CoreError, NativeEvent, ProviderRestriction, ProviderRestrictionKind,
    SecretPolicy, SourceBatch, SourceHandoff, SourceHandoffGap, SourceHealthUpdate,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::hash_map::DefaultHasher,
    error::Error,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    time::Duration,
};
use tokio::time::Instant;
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};

const HOST_REQUEST_QUEUE: usize = 16;
const EVENTS_PUT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const HOST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const DISCOVERY_SCOPE: &str = "fake:discovery";
const DISCOVERY_SUBJECTS: usize = 3;
const MALFORMED_AFTER_DISPATCH: &str = "[fake:malformed-after-dispatch]";
const NATIVE_ERROR_FIXTURE_INPUT: &str = "[fake:native-turn-error]";
const PROVIDER_RATE_LIMIT_FIXTURE_INPUT: &str = "[eliot_fake_provider_rate_limit]";
const PROVIDER_QUOTA_FIXTURE_INPUT: &str = "[eliot_fake_provider_quota]";
const PROVIDER_AUTH_FIXTURE_INPUT: &str = "[eliot_fake_provider_auth]";
const PROVIDER_MODEL_REMOVED_FIXTURE_INPUT: &str = "[eliot_fake_provider_model_removed]";
const HANDOFF_FIXTURE_INPUT: &str = "[fake:source-handoff]";
const LIVE_OBSERVER_SUBJECT: &str = "fake-discovered-2";
const LIVE_OBSERVER_TS: i64 = 1_730_000_000_000;

#[derive(Debug)]
pub struct PluginError {
    message: String,
}

impl PluginError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for PluginError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for PluginError {}

/// Serve the bounded fake plugin over stdin/stdout JSON-RPC 2.0 NDJSON.
/// Stdout is reserved for protocol frames; this function writes no log lines.
pub async fn run_fake() -> Result<(), PluginError> {
    let mut peer = Peer::stdio();
    let client = peer.client();
    let (request_tx, request_rx) = mpsc::channel(HOST_REQUEST_QUEUE);
    let (actor_done_tx, mut actor_done_rx) = oneshot::channel();
    let actor_client = client.clone();
    let actor = tokio::spawn(async move {
        let result = fake_actor(request_rx, actor_client).await;
        let _ = actor_done_tx.send(result);
    });

    loop {
        tokio::select! {
            request = peer.next_request() => {
                let Some(request) = request else {
                    actor.abort();
                    let _ = actor.await;
                    return Err(PluginError::new(
                        "plugin input closed before a completed shutdown",
                    ));
                };
                let id = request.id.clone();
                let error = match request_tx.try_send(request) {
                    Ok(()) => None,
                    Err(mpsc::error::TrySendError::Full(_)) => Some(domain_error(
                        "busy",
                        -32020,
                        "plugin request queue is full",
                    )),
                    Err(mpsc::error::TrySendError::Closed(_)) => Some(domain_error(
                        "unavailable",
                        -32021,
                        "plugin is shutting down",
                    )),
                };
                if let Some(error) = error {
                    client
                        .respond(
                            id,
                            Err(preserve_rpc_error(error)),
                            Instant::now() + HOST_RESPONSE_TIMEOUT,
                        )
                        .await
                        .map_err(|error| PluginError::new(error.message))?;
                }
            }
            result = &mut actor_done_rx => {
                let result = result.map_err(|_| PluginError::new("plugin actor stopped"))?;
                actor.await.map_err(|_| PluginError::new("plugin actor failed"))?;
                return result;
            }
        }
    }
}

async fn fake_actor(
    mut requests: mpsc::Receiver<RpcRequest>,
    client: PeerClient,
) -> Result<(), PluginError> {
    let mut state = FakeState::default();

    while let Some(request) = requests.recv().await {
        let id = request.id.clone();
        let is_shutdown = request.method == "shutdown";
        let result = dispatch(&request, &mut state, &client).await;
        let shutdown_after_reply = is_shutdown && result.is_ok();
        let live_after_reply = request.method == "agent.attach"
            && result.is_ok()
            && state.observer.as_ref().is_some_and(|observer| {
                observer.native_ref == LIVE_OBSERVER_SUBJECT && !observer.live_published
            });
        client
            .respond(
                id,
                result.map_err(preserve_rpc_error),
                Instant::now() + HOST_RESPONSE_TIMEOUT,
            )
            .await
            .map_err(|error| PluginError::new(error.message))?;

        if live_after_reply {
            exercise_observer_live(&mut state, &client)
                .await
                .map_err(|_| PluginError::new("fake observer live publication stopped"))?;
        }
        if shutdown_after_reply {
            state.credential.take();
            return Ok(());
        }
    }

    Err(PluginError::new(
        "plugin request channel closed before shutdown",
    ))
}

async fn dispatch(
    request: &RpcRequest,
    state: &mut FakeState,
    client: &PeerClient,
) -> Result<Value, RpcError> {
    match request.method.as_str() {
        "initialize" => handle_initialize(&request.params, state),
        "discover" => handle_discover(&request.params, state),
        "agent.attach" => handle_observer_attach(&request.params, state),
        "agent.detach" => handle_observer_detach(&request.params, state),
        "agent.start" => handle_start(&request.params, state, client).await,
        "agent.command" => handle_command(&request.params, state, client).await,
        "agent.stop" => handle_stop(&request.params, state, client).await,
        "agent.snapshot" if request.params.get("observation").is_some() => {
            handle_observer_snapshot(&request.params, state)
        }
        "agent.snapshot" => handle_snapshot(&request.params, state),
        "shutdown" => handle_shutdown(state),
        _ => Err(domain_error(
            "unsupported",
            -32601,
            "plugin method is not implemented",
        )),
    }
}

fn handle_discover(raw_params: &Value, state: &FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    let params: DiscoverParams = decode_params(raw_params)?;
    if !(1..=MAX_DISCOVER_PAGE_ENTRIES).contains(&params.limit) {
        return Err(invalid_params("discovery limit must be between 1 and 1000"));
    }
    if params
        .native_scope
        .as_deref()
        .is_some_and(|scope| scope != DISCOVERY_SCOPE)
    {
        return Err(domain_error(
            "not_found",
            -32034,
            "fake discovery scope was not found",
        ));
    }
    let start = match params.cursor.as_deref() {
        None => 0,
        Some(cursor) => (1..DISCOVERY_SUBJECTS)
            .find(|offset| cursor == discovery_cursor(&params, *offset))
            .ok_or_else(|| {
                invalid_params("fake discovery cursor does not match its scope and profile context")
            })?,
    };
    let end = (start + params.limit).min(DISCOVERY_SUBJECTS);
    let page = DiscoverPage {
        harness: "fake".to_owned(),
        native_scope: DISCOVERY_SCOPE.to_owned(),
        subjects: (start + 1..=end)
            .map(|index| DiscoverSubject {
                native_ref: format!("fake-discovered-{index}"),
                workspace: None,
                state: "idle".to_owned(),
                created_at: None,
                updated_at: None,
                source: Some("fake".to_owned()),
                ephemeral: Some(false),
            })
            .collect(),
        next_cursor: (end < DISCOVERY_SUBJECTS).then(|| discovery_cursor(&params, end)),
        coverage: DiscoverCoverage {
            kind: "fixture".to_owned(),
            complete: end == DISCOVERY_SUBJECTS,
        },
    };
    let encoded = serde_json::to_vec(&page).map_err(|_| {
        domain_error(
            "internal",
            -32603,
            "fake discovery page could not be encoded",
        )
    })?;
    if encoded.len() > MAX_DISCOVER_PAGE_BYTES {
        return Err(frame_too_large("fake discovery page exceeds 900 KiB"));
    }
    serde_json::to_value(page).map_err(|_| {
        domain_error(
            "internal",
            -32603,
            "fake discovery page could not be encoded",
        )
    })
}

fn discovery_cursor(params: &DiscoverParams, offset: usize) -> String {
    // Fake-only deterministic context binding, not an authorization token.
    // Value hashing ignores object-key insertion order and keeps profile/env
    // plaintext out of the cursor. No per-session cursor state is required.
    let mut binding = DefaultHasher::new();
    DISCOVERY_SCOPE.hash(&mut binding);
    params.profile.hash(&mut binding);
    params.target_environment.hash(&mut binding);
    format!("fake-discovery-v1:{:016x}:{offset}", binding.finish())
}

fn handle_observer_attach(raw: &Value, state: &mut FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    let params: ObserverAttachParams = decode_params(raw)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    if params.mode != "observe" {
        return Err(domain_error(
            "unsupported",
            -32601,
            "fake attach is observe-only",
        ));
    }
    if state.binding.is_some() {
        return Err(domain_error(
            "busy",
            -32020,
            "fake plugin already controls a session",
        ));
    }
    if params.native_scope != DISCOVERY_SCOPE
        || !(1..=DISCOVERY_SUBJECTS)
            .any(|index| params.native_ref == format!("fake-discovered-{index}"))
    {
        return Err(domain_error(
            "not_found",
            -32034,
            "fake observation subject was not found",
        ));
    }
    if params.profile.get("harness").and_then(Value::as_str) != Some("fake")
        || !params.target_environment.is_object()
    {
        return Err(invalid_params(
            "fake observation requires a fake profile and environment object",
        ));
    }
    let mut context = DefaultHasher::new();
    params.profile.hash(&mut context);
    params.target_environment.hash(&mut context);
    params.source_plan.hash(&mut context);
    params.source_state.hash(&mut context);
    let context = context.finish();
    if let Some(observer) = &state.observer {
        validate_observer_binding(
            state,
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        if observer.attach_command_id != params.command_id {
            return Err(domain_error(
                "busy",
                -32020,
                "fake observer is already bound",
            ));
        }
        if observer.context != context {
            return Err(domain_error(
                "source_integrity",
                -32025,
                "fake observation binding changed",
            ));
        }
        if observer.detach_command_id.is_some() {
            return Err(domain_error(
                "bad_state",
                -32024,
                "fake observer is detached",
            ));
        }
    } else {
        state.source_policy = source_secret_policy(
            Some(&params.profile),
            Some(&params.target_environment),
            state.credential.as_deref().ok_or_else(not_initialized)?,
        );
        state.observer = Some(FakeObserver {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            native_scope: params.native_scope.clone(),
            native_ref: params.native_ref.clone(),
            attach_command_id: params.command_id.clone(),
            context,
            detach_command_id: None,
            live_published: false,
        });
    }
    Ok(json!({
        "agent_id": params.agent_id, "generation": params.generation,
        "command_id": params.command_id,
        "native_scope": params.native_scope, "native_ref": params.native_ref,
        "state": "attached", "observation_state": "unknown",
        "metadata": {"workspace": null, "source": "fake", "ephemeral": false},
        "coverage": {"kind": "fixture", "complete": false},
    }))
}

fn handle_observer_snapshot(raw: &Value, state: &FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    let params: ObserverSnapshotParams = decode_params(raw)?;
    let observer = validate_observer_binding(
        state,
        &params.agent_id,
        params.generation,
        &params.native_scope,
        &params.native_ref,
    )?;
    if observer.detach_command_id.is_some() {
        return Err(domain_error(
            "bad_state",
            -32024,
            "fake observer is detached",
        ));
    }
    let query = params.observation;
    query.validate().map_err(core_error_to_rpc)?;
    if !matches!(query.source.as_str(), "native" | "rollout") {
        return Err(domain_error(
            "unsupported",
            -32601,
            "fake observation source is unsupported",
        ));
    }
    let entries = match query.view.as_str() {
        "subject" if query.turn_id.is_none() => vec![json!({
            "native_ref": observer.native_ref, "workspace": null,
            "state": "unknown", "source": "fake", "ephemeral": false,
        })],
        "turns" if query.turn_id.is_none() => (1..=2)
            .map(|index| {
                json!({
                    "turn_id": format!("fake-observed-turn-{index}"), "status": "completed",
                })
            })
            .collect(),
        "items" => {
            let turn = query
                .turn_id
                .as_deref()
                .filter(|turn| matches!(*turn, "fake-observed-turn-1" | "fake-observed-turn-2"))
                .ok_or_else(|| invalid_params("items requires an exact fixture turn_id"))?;
            vec![
                json!({"turn_id": turn, "kind": "message", "role": "assistant",
                "text": "fake observation history fixture"}),
            ]
        }
        _ => return Err(invalid_params("invalid observation view or turn binding")),
    };
    let start = match query.cursor.as_deref() {
        None => 0,
        Some(cursor) => (1..entries.len())
            .find(|offset| cursor == observer_cursor(observer, &query, *offset))
            .ok_or_else(|| invalid_params("fake observation cursor binding is invalid"))?,
    };
    let end = (start + query.limit).min(entries.len());
    let page = ObservationPage {
        agent_id: params.agent_id,
        generation: params.generation,
        native_scope: params.native_scope,
        native_ref: params.native_ref,
        next_cursor: (end < entries.len()).then(|| observer_cursor(observer, &query, end)),
        source: query.source,
        view: query.view,
        entries: entries[start..end].to_vec(),
        coverage: json!({"kind": "fixture", "complete": end == entries.len()}),
    };
    page.validate().map_err(core_error_to_rpc)?;
    serde_json::to_value(page)
        .map_err(|_| domain_error("internal", -32603, "cannot encode fake observation page"))
}

fn observer_cursor(observer: &FakeObserver, query: &ObservationQuery, offset: usize) -> String {
    // Fake-only context binding, never an authority token or production file ID.
    let mut binding = DefaultHasher::new();
    observer.agent_id.hash(&mut binding);
    observer.generation.hash(&mut binding);
    observer.native_scope.hash(&mut binding);
    observer.native_ref.hash(&mut binding);
    observer.context.hash(&mut binding);
    query.source.hash(&mut binding);
    query.view.hash(&mut binding);
    query.turn_id.hash(&mut binding);
    query.limit.hash(&mut binding);
    format!("fake-observation-v1:{:016x}:{offset}", binding.finish())
}

fn handle_observer_detach(raw: &Value, state: &mut FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    let params: ObserverDetachParams = decode_params(raw)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    validate_observer_binding(
        state,
        &params.agent_id,
        params.generation,
        &params.native_scope,
        &params.native_ref,
    )?;
    let observer = state
        .observer
        .as_mut()
        .ok_or_else(|| domain_error("not_found", -32034, "fake observer is not attached"))?;
    if observer
        .detach_command_id
        .as_deref()
        .is_some_and(|id| id != params.command_id)
    {
        return Err(domain_error(
            "source_integrity",
            -32025,
            "fake detach command binding changed",
        ));
    }
    observer.detach_command_id = Some(params.command_id.clone());
    Ok(json!({
        "agent_id": params.agent_id, "generation": params.generation,
        "command_id": params.command_id,
        "native_scope": params.native_scope, "native_ref": params.native_ref,
        "state": "detached", "observation_state": "unknown",
    }))
}

fn validate_observer_binding<'a>(
    state: &'a FakeState,
    agent_id: &str,
    generation: i64,
    native_scope: &str,
    native_ref: &str,
) -> Result<&'a FakeObserver, RpcError> {
    let observer = state
        .observer
        .as_ref()
        .ok_or_else(|| domain_error("not_found", -32034, "fake observer is not attached"))?;
    if observer.agent_id != agent_id || observer.generation != generation {
        return Err(domain_error(
            "stale_generation",
            -32033,
            "fake observer generation does not match",
        ));
    }
    if observer.native_scope != native_scope || observer.native_ref != native_ref {
        return Err(domain_error(
            "source_integrity",
            -32025,
            "fake observation subject does not match",
        ));
    }
    Ok(observer)
}

fn reject_observer_control(state: &FakeState) -> Result<(), RpcError> {
    if state.observer.is_some() {
        return Err(domain_error(
            "not_allowed",
            -32030,
            "observation grants no session control",
        ));
    }
    Ok(())
}

fn handle_initialize(params: &Value, state: &mut FakeState) -> Result<Value, RpcError> {
    let params: InitializeParams = decode_params(params)?;
    if params.protocol != "eliot-plugin/1" {
        return Err(domain_error(
            "unsupported",
            -32022,
            "unsupported plugin protocol version",
        ));
    }
    if params.instance_dir.trim().is_empty()
        || params.credential.trim().is_empty()
        || params.credential.len() > MAX_CREDENTIAL_BYTES
    {
        return Err(invalid_params(
            "initialize requires a private instance directory and credential",
        ));
    }

    if let Some(existing) = &state.credential {
        if existing != &params.credential {
            return Err(domain_error(
                "bad_state",
                -32023,
                "plugin is already initialized",
            ));
        }
    } else {
        state.credential = Some(params.credential);
    }

    serde_json::to_value(fake_manifest())
        .map_err(|_| domain_error("internal", -32603, "plugin manifest could not be encoded"))
}

async fn handle_start(
    raw_params: &Value,
    state: &mut FakeState,
    client: &PeerClient,
) -> Result<Value, RpcError> {
    require_initialized(state)?;
    reject_observer_control(state)?;
    let params: AgentStartParams = decode_params(raw_params)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    if params.initial.is_some() || params.resume_from.is_some() {
        return Err(domain_error(
            "unsupported",
            -32602,
            "fake plugin does not support input-created start or resume",
        ));
    }
    if let Some(binding) = &state.binding
        && (binding.agent_id != params.agent_id || binding.generation != params.generation)
    {
        return Err(domain_error(
            "stale_generation",
            -32033,
            "start does not match the bound Agent generation",
        ));
    }
    if state.batch_unknown {
        return Err(unknown_effect());
    }

    if state.phase == SessionPhase::Idle
        && state.start_command_id.as_deref() == Some(params.command_id.as_str())
    {
        return Ok(json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": "ready"
        }));
    }
    if state.phase != SessionPhase::New {
        return Err(domain_error(
            "bad_state",
            -32024,
            "fake session has already started",
        ));
    }
    if let Some(command_id) = &state.start_command_id
        && command_id != &params.command_id
    {
        return Err(domain_error(
            "source_integrity",
            -32025,
            "start command id changed for the bound generation",
        ));
    }

    let credential = state.credential.clone().ok_or_else(not_initialized)?;
    let batch = session_ready_batch(&params.agent_id, params.generation);

    state.source_policy = source_secret_policy(
        params.profile.as_ref(),
        params.target_environment.as_ref(),
        &credential,
    );
    state.binding = Some(Binding {
        agent_id: params.agent_id.clone(),
        generation: params.generation,
    });
    state.start_command_id = Some(params.command_id.clone());
    state.phase = SessionPhase::Starting;

    match commit_batch(state, batch, &credential, client).await {
        Ok(_) => {
            state.phase = SessionPhase::Idle;
            Ok(json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "state": "ready"
            }))
        }
        Err(failure) => {
            state.phase = if failure.uncertain {
                SessionPhase::Unknown
            } else {
                SessionPhase::New
            };
            Err(failure.error)
        }
    }
}

async fn handle_command(
    raw_params: &Value,
    state: &mut FakeState,
    client: &PeerClient,
) -> Result<Value, RpcError> {
    require_initialized(state)?;
    reject_observer_control(state)?;
    let params: AgentCommandParams = decode_params(raw_params)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    validate_existing_binding(state, &params.agent_id, params.generation)?;
    if params.verb != "send" {
        return Err(domain_error(
            "unsupported",
            -32602,
            "fake plugin implements only the send verb",
        ));
    }
    if state.batch_unknown {
        return Err(unknown_effect());
    }
    if state.phase != SessionPhase::Idle {
        return Err(domain_error(
            "bad_state",
            -32026,
            "send requires a ready idle session",
        ));
    }

    let text = params
        .args
        .as_object()
        .and_then(|args| args.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_params("agent.command send requires args.text"))?;
    if text.len() > MAX_TEXT_BYTES {
        return Err(frame_too_large("input text exceeds 512 KiB"));
    }
    if text.len() > MAX_EVENT_BYTES {
        return Err(frame_too_large(
            "fake echo output exceeds the 64 KiB persisted-text limit",
        ));
    }
    if let Some(last) = &state.last_turn
        && last.command_id == params.command_id
    {
        if last.text == text {
            return Ok(json!({
                "state": "applied",
                "turn_id": last.turn_id
            }));
        }
        return Err(domain_error(
            "source_integrity",
            -32027,
            "command id was reused with different input",
        ));
    }

    let credential = state.credential.clone().ok_or_else(not_initialized)?;
    if text == MALFORMED_AFTER_DISPATCH {
        // Exact C1 regression hook: validation passed, but the dispatched call
        // loses its transport without any authoritative response or echo batch.
        let mut stderr = tokio::io::stderr();
        let diagnostic = format!("fake_stderr_before_malformed={credential}\n");
        let _ = stderr.write_all(diagnostic.as_bytes()).await;
        let _ = stderr.flush().await;
        let mut stdout = tokio::io::stdout();
        let _ = stdout
            .write_all(b"fake malformed frame after dispatch\n")
            .await;
        let _ = stdout.flush().await;
        std::process::exit(1);
    }
    let turn_id = format!(
        "fake-{}-{}-{}",
        params.agent_id, params.generation, params.command_id
    );
    if text == HANDOFF_FIXTURE_INPUT {
        if state.publish_source.is_some() {
            return Err(domain_error(
                "bad_state",
                -32024,
                "fake source has already been transferred",
            ));
        }
        if let Err(error) = exercise_source_handoff(state, &params, &credential, client).await {
            state.phase = SessionPhase::Unknown;
            state.batch_unknown = true;
            return Err(error);
        }
    }
    let mut batch = send_batch(&params, &turn_id, text);
    batch.source = state
        .publish_source
        .as_deref()
        .unwrap_or("stream")
        .to_owned();

    state.phase = SessionPhase::Busy;
    state.active_turn = Some(turn_id.clone());
    state.active_command_id = Some(params.command_id.clone());
    match commit_batch(state, batch, &credential, client).await {
        Ok(_) => {
            state.phase = SessionPhase::Idle;
            state.active_turn = None;
            state.active_command_id = None;
            state.last_turn = Some(LastTurn {
                command_id: params.command_id,
                turn_id: turn_id.clone(),
                text: text.to_owned(),
            });
            Ok(json!({
                "state": "applied",
                "turn_id": turn_id
            }))
        }
        Err(failure) => {
            state.phase = if failure.uncertain {
                SessionPhase::Unknown
            } else {
                SessionPhase::Idle
            };
            if !failure.uncertain {
                state.active_turn = None;
                state.active_command_id = None;
            } else {
                state.last_turn = Some(LastTurn {
                    command_id: params.command_id,
                    turn_id,
                    text: text.to_owned(),
                });
            }
            Err(failure.error)
        }
    }
}

async fn handle_stop(
    raw_params: &Value,
    state: &mut FakeState,
    client: &PeerClient,
) -> Result<Value, RpcError> {
    require_initialized(state)?;
    reject_observer_control(state)?;
    let params: AgentStopParams = decode_params(raw_params)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    validate_existing_binding(state, &params.agent_id, params.generation)?;

    if state.phase == SessionPhase::Stopped
        && state.stop_command_id.as_deref() == Some(params.command_id.as_str())
    {
        return Ok(json!({"state": "applied"}));
    }
    // Service stop promptly, but never publish a second batch over an unknown one.
    if state.batch_unknown {
        return Err(unknown_effect());
    }
    if state.phase != SessionPhase::Idle {
        return Err(domain_error(
            "bad_state",
            -32028,
            "stop requires a ready session with no unresolved batch",
        ));
    }

    let credential = state.credential.clone().ok_or_else(not_initialized)?;
    let mut batch = session_ended_batch(&params.agent_id, params.generation);
    batch.source = state
        .publish_source
        .as_deref()
        .unwrap_or("stream")
        .to_owned();
    state.phase = SessionPhase::Stopping;
    state.stop_command_id = Some(params.command_id);

    match commit_batch(state, batch, &credential, client).await {
        Ok(_) => {
            state.phase = SessionPhase::Stopped;
            state.active_turn = None;
            state.active_command_id = None;
            Ok(json!({"state": "applied"}))
        }
        Err(failure) => {
            state.phase = if failure.uncertain {
                SessionPhase::Unknown
            } else {
                SessionPhase::Idle
            };
            Err(failure.error)
        }
    }
}

fn handle_snapshot(raw_params: &Value, state: &FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    let params: AgentSnapshotParams = decode_params(raw_params)?;
    validate_agent_identity(&params.agent_id, params.generation, &params.command_id)?;
    validate_existing_binding(state, &params.agent_id, params.generation)?;

    let command_known = state.start_command_id.as_deref() == Some(params.command_id.as_str())
        || state.active_command_id.as_deref() == Some(params.command_id.as_str())
        || state
            .last_turn
            .as_ref()
            .is_some_and(|turn| turn.command_id == params.command_id)
        || state.stop_command_id.as_deref() == Some(params.command_id.as_str());
    if !command_known {
        return Err(domain_error(
            "not_found",
            -32029,
            "snapshot command is not known to this fake session",
        ));
    }
    if let Some(turn_id) = &params.turn_id {
        let turn_known = state.active_turn.as_deref() == Some(turn_id.as_str())
            || state
                .last_turn
                .as_ref()
                .is_some_and(|turn| turn.turn_id == *turn_id);
        if !turn_known {
            return Err(domain_error(
                "not_found",
                -32030,
                "snapshot turn is not known to this fake session",
            ));
        }
    }

    let last_turn = state.last_turn.as_ref().map(|turn| {
        let failed = fake_turn_failed(&turn.text);
        json!({
            "command_id": turn.command_id,
            "turn_id": turn.turn_id,
            "status": if failed { "failed" } else { "completed" },
            "final_text": (!failed).then_some(&turn.text)
        })
    });
    Ok(json!({
        "agent_id": params.agent_id,
        "generation": params.generation,
        "command_id": params.command_id,
        "state": state.phase.as_str(),
        "active_turn": state.active_turn,
        "last_turn": last_turn,
        "batch_unknown": state.batch_unknown,
        "last_batch_ack": state.last_batch_ack
    }))
}

fn handle_shutdown(state: &FakeState) -> Result<Value, RpcError> {
    require_initialized(state)?;
    Ok(json!({"state": "shutdown"}))
}

fn session_ready_batch(agent_id: &str, generation: i64) -> SourceBatch {
    let event_id = format!("fake:{agent_id}:{generation}:session.ready");
    SourceBatch {
        batch_id: format!("fake:{agent_id}:{generation}:session-ready"),
        agent_id: agent_id.to_owned(),
        generation,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            kind: "session.ready".to_owned(),
            native_id: event_id,
            ts: None,
            data: empty_object(),
        }],
        cursor: None,
    }
}

fn send_batch(params: &AgentCommandParams, turn_id: &str, text: &str) -> SourceBatch {
    let prefix = format!(
        "fake:{}:{}:{}",
        params.agent_id, params.generation, params.command_id
    );
    let restriction = fake_provider_restriction(text);
    let failed = text == NATIVE_ERROR_FIXTURE_INPUT || restriction.is_some();
    let mut completion = json!({
        "command_id": params.command_id,
        "turn_id": turn_id,
        "status": if failed { "failed" } else { "completed" }
    });
    if text == NATIVE_ERROR_FIXTURE_INPUT {
        completion["native_error"] = json!({
            "message": "fake native request failed (HTTP 401)",
            "truncated": false
        });
    }
    if let Some(restriction) = restriction {
        completion["provider_restriction"] = json!({
            "kind": restriction.kind.as_str(),
            "retry_after": restriction.retry_after
        });
    }
    SourceBatch {
        batch_id: format!("{prefix}:turn"),
        agent_id: params.agent_id.clone(),
        generation: params.generation,
        source: "stream".to_owned(),
        events: vec![
            NativeEvent {
                kind: "turn.started".to_owned(),
                native_id: format!("{prefix}:turn.started"),
                ts: None,
                data: json!({"command_id": params.command_id, "turn_id": turn_id}),
            },
            NativeEvent {
                kind: "message.completed".to_owned(),
                native_id: format!("{prefix}:message.completed"),
                ts: None,
                data: json!({
                    "command_id": params.command_id,
                    "turn_id": turn_id,
                    "text": text
                }),
            },
            NativeEvent {
                kind: "turn.completed".to_owned(),
                native_id: format!("{prefix}:turn.completed"),
                ts: None,
                data: completion,
            },
        ]
        .into_iter()
        .filter(|event| !failed || event.kind != "message.completed")
        .collect(),
        cursor: None,
    }
}

fn fake_provider_restriction(text: &str) -> Option<ProviderRestriction> {
    let kind = match text {
        PROVIDER_RATE_LIMIT_FIXTURE_INPUT => ProviderRestrictionKind::RateLimit,
        PROVIDER_QUOTA_FIXTURE_INPUT => ProviderRestrictionKind::Quota,
        PROVIDER_AUTH_FIXTURE_INPUT => ProviderRestrictionKind::Auth,
        PROVIDER_MODEL_REMOVED_FIXTURE_INPUT => ProviderRestrictionKind::ModelRemoved,
        _ => return None,
    };
    Some(ProviderRestriction {
        kind,
        retry_after: None,
    })
}

fn fake_turn_failed(text: &str) -> bool {
    text == NATIVE_ERROR_FIXTURE_INPUT || fake_provider_restriction(text).is_some()
}

fn session_ended_batch(agent_id: &str, generation: i64) -> SourceBatch {
    SourceBatch {
        batch_id: format!("fake:{agent_id}:{generation}:session-ended"),
        agent_id: agent_id.to_owned(),
        generation,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            kind: "session.ended".to_owned(),
            native_id: format!("fake:{agent_id}:{generation}:session.ended"),
            ts: None,
            data: empty_object(),
        }],
        cursor: None,
    }
}

struct BatchFailure {
    error: RpcError,
    uncertain: bool,
}

fn source_secret_policy(
    profile: Option<&Value>,
    environment: Option<&Value>,
    credential: &str,
) -> SecretPolicy {
    let mut known = vec![credential.to_owned()];
    if let Some(environment) = environment.and_then(Value::as_object) {
        let names = profile
            .and_then(|profile| profile.get("pass_env"))
            .and_then(Value::as_array);
        for (name, value) in environment {
            let matches = |expected: &str| {
                if cfg!(windows) {
                    name.eq_ignore_ascii_case(expected)
                } else {
                    name == expected
                }
            };
            if (matches("ELIOT_AGENT_TOKEN")
                || names.is_some_and(|names| names.iter().filter_map(Value::as_str).any(matches)))
                && let Some(value) = value.as_str()
            {
                known.push(value.to_owned());
            }
        }
    }
    SecretPolicy::new(known)
}

async fn exercise_source_handoff(
    state: &mut FakeState,
    command: &AgentCommandParams,
    credential: &str,
    client: &PeerClient,
) -> Result<(), RpcError> {
    let mut handoff = SourceHandoff {
        agent_id: command.agent_id.clone(),
        generation: command.generation,
        source: "stream".to_owned(),
        families: vec![
            "message".to_owned(),
            "session".to_owned(),
            "turn".to_owned(),
        ],
        successor: "transcript".to_owned(),
        mechanism: "fake_recovery".to_owned(),
        cursor: None,
        gap: Some(SourceHandoffGap {
            gap_id: format!(
                "fake:{}:{}:{}:handoff-gap",
                command.agent_id, command.generation, command.command_id
            ),
            reason: format!("fake_source_handoff_fixture {credential}"),
            from: None,
            to: None,
        }),
    };
    handoff
        .prepare(&state.source_policy)
        .map_err(core_error_to_rpc)?;
    state.batch_unknown = true;
    commit_handoff(&handoff, credential, client).await?;
    state.publish_source = Some(handoff.successor);
    state.batch_unknown = false;
    Ok(())
}

async fn commit_handoff(
    handoff: &SourceHandoff,
    credential: &str,
    client: &PeerClient,
) -> Result<(), RpcError> {
    // Retain this exact prepared request for both the wire and full-digest proof.
    let mut request = serde_json::to_value(handoff)
        .map_err(|_| domain_error("internal", -32603, "cannot encode fake handoff"))?;
    request["credential"] = json!(credential);

    // The actor has acknowledged all preceding batches and processes no later
    // request while this operation is unresolved. Never replay the operation.
    let _discarded_reply = client
        .call(
            "source.handoff",
            request,
            Instant::now() + HOST_RESPONSE_TIMEOUT,
        )
        .await;
    // Deliberately choose readback instead of the direct ACK. This exercises
    // proof selection, without claiming that the transport actually lost a reply.
    let readback = client
        .call(
            "source.state",
            json!({
                "credential": credential,
                "agent_id": handoff.agent_id,
                "generation": handoff.generation,
                "source": handoff.source,
            }),
            Instant::now() + HOST_RESPONSE_TIMEOUT,
        )
        .await
        .map_err(|_| unknown_effect())?;
    handoff
        .ack_from_state(&readback)
        .map_err(|_| unknown_effect())?;
    Ok(())
}

async fn exercise_observer_live(
    state: &mut FakeState,
    client: &PeerClient,
) -> Result<(), RpcError> {
    let observer = state.observer.as_ref().ok_or_else(not_initialized)?;
    let credential = state.credential.clone().ok_or_else(not_initialized)?;
    let until = Instant::now() + HOST_RESPONSE_TIMEOUT;
    loop {
        let ready = client
            .call(
                "source.state",
                json!({
                    "credential": credential, "agent_id": observer.agent_id,
                    "generation": observer.generation, "source": "observe",
                }),
                until,
            )
            .await
            .map_err(core_error_to_rpc)?;
        if ready["agent_id"] != observer.agent_id
            || ready["generation"].as_i64() != Some(observer.generation)
            || ready["source"] != "observe"
            || ready["state"] != "open"
        {
            return Err(unknown_effect());
        }
        match ready["observer_ready"].as_bool() {
            Some(true) => break,
            Some(false) if Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            _ => return Err(unknown_effect()),
        }
    }
    let mut handoff = SourceHandoff {
        agent_id: observer.agent_id.clone(),
        generation: observer.generation,
        source: "observe".to_owned(),
        families: vec!["message".to_owned(), "turn".to_owned()],
        successor: "transcript".to_owned(),
        mechanism: "rollout_tail".to_owned(),
        cursor: None,
        gap: Some(SourceHandoffGap {
            gap_id: format!(
                "fake:{}:{}:observer-live-edge",
                observer.agent_id, observer.generation
            ),
            reason: "fake observer starts at the live edge".to_owned(),
            from: None,
            to: Some("fake-observer-edge:0".to_owned()),
        }),
    };
    handoff
        .prepare(&state.source_policy)
        .map_err(core_error_to_rpc)?;
    state.batch_unknown = true;
    commit_handoff(&handoff, &credential, client).await?;
    for health in ["stale", "stale", "ok"] {
        let update = SourceHealthUpdate {
            agent_id: handoff.agent_id.clone(),
            generation: handoff.generation,
            source: handoff.source.clone(),
            health: health.to_owned(),
        };
        let expected = serde_json::to_value(&update).map_err(|_| unknown_effect())?;
        let mut request = expected.clone();
        request["credential"] = json!(credential);
        let ack = client
            .call(
                "source.health",
                request,
                Instant::now() + HOST_RESPONSE_TIMEOUT,
            )
            .await;
        if ack.as_ref().is_ok_and(|ack| ack != &expected) {
            return Err(unknown_effect());
        }
        let readback = client
            .call(
                "source.state",
                json!({
                    "credential": credential, "agent_id": handoff.agent_id,
                    "generation": handoff.generation, "source": handoff.source,
                }),
                Instant::now() + HOST_RESPONSE_TIMEOUT,
            )
            .await
            .map_err(core_error_to_rpc)?;
        handoff
            .ack_from_state(&readback)
            .map_err(|_| unknown_effect())?;
        if readback["state"] != "open" || readback["health"] != health {
            return Err(unknown_effect());
        }
    }
    let turn_id = "fake-observed-live-turn";
    let prefix = format!(
        "fake:{}:{}:observer-live",
        handoff.agent_id, handoff.generation
    );
    let batch = SourceBatch {
        batch_id: format!("{prefix}:batch"),
        agent_id: handoff.agent_id,
        generation: handoff.generation,
        source: handoff.successor,
        cursor: Some("fake-observer-edge:1".to_owned()),
        events: vec![
            NativeEvent {
                kind: "turn.started".to_owned(),
                native_id: format!("{prefix}:started"),
                ts: Some(LIVE_OBSERVER_TS + 1),
                data: json!({"turn_id": turn_id, "started_at": LIVE_OBSERVER_TS - 1_000}),
            },
            NativeEvent {
                kind: "message.completed".to_owned(),
                native_id: format!("{prefix}:message"),
                ts: Some(LIVE_OBSERVER_TS + 2),
                data: json!({"turn_id": turn_id, "phase": "commentary",
                    "text": "fake observed commentary"}),
            },
            NativeEvent {
                kind: "turn.completed".to_owned(),
                native_id: format!("{prefix}:completed"),
                ts: Some(LIVE_OBSERVER_TS + 3),
                data: json!({"turn_id": turn_id, "status": "completed",
                    "completed_at": LIVE_OBSERVER_TS, "duration_ms": 1_000,
                    "final_text": "fake observed live reply"}),
            },
        ],
    };
    commit_batch(state, batch, &credential, client)
        .await
        .map_err(|failure| failure.error)?;
    state
        .observer
        .as_mut()
        .ok_or_else(not_initialized)?
        .live_published = true;
    Ok(())
}

async fn commit_batch(
    state: &mut FakeState,
    batch: SourceBatch,
    credential: &str,
    client: &PeerClient,
) -> Result<BatchAck, BatchFailure> {
    let mut sink = CommitSink::new(client.clone(), credential.to_owned());
    match sink
        .commit(batch, Instant::now() + EVENTS_PUT_ACK_TIMEOUT)
        .await
    {
        Ok(ack) => {
            state.batch_unknown = false;
            state.last_batch_ack = Some(ack.clone());
            Ok(ack)
        }
        Err(error) => {
            let uncertain = sink.is_unknown();
            state.batch_unknown = uncertain;
            let error = if uncertain {
                unknown_effect()
            } else {
                core_error_to_rpc(error)
            };
            Err(BatchFailure { error, uncertain })
        }
    }
}

fn core_error_to_rpc(error: CoreError) -> RpcError {
    let rpc_code = match error.code.as_str() {
        "invalid" | "frame_too_large" => -32602,
        "unsupported" => -32601,
        "internal" => -32603,
        _ => -32000,
    };
    domain_error(&error.code, rpc_code, &error.message)
}

fn decode_params<T: DeserializeOwned>(params: &Value) -> Result<T, RpcError> {
    serde_json::from_value(params.clone()).map_err(|_| invalid_params("invalid method parameters"))
}

fn validate_agent_identity(
    agent_id: &str,
    generation: i64,
    command_id: &str,
) -> Result<(), RpcError> {
    let valid_agent = agent_id
        .strip_prefix('a')
        .and_then(|digits| digits.parse::<i64>().ok())
        .is_some_and(|number| number > 0);
    if !valid_agent
        || generation <= 0
        || command_id.trim().is_empty()
        || command_id.len() > MAX_COMMAND_ID_BYTES
    {
        return Err(invalid_params(
            "agent_id, generation, or command_id is invalid",
        ));
    }
    Ok(())
}

fn validate_existing_binding(
    state: &FakeState,
    agent_id: &str,
    generation: i64,
) -> Result<(), RpcError> {
    match &state.binding {
        Some(binding) if binding.agent_id == agent_id && binding.generation == generation => Ok(()),
        Some(_) => Err(domain_error(
            "stale_generation",
            -32033,
            "request does not match the bound Agent generation",
        )),
        None => Err(domain_error(
            "not_found",
            -32034,
            "no Agent generation is bound to this plugin",
        )),
    }
}

fn require_initialized(state: &FakeState) -> Result<(), RpcError> {
    if state.credential.is_some() {
        Ok(())
    } else {
        Err(not_initialized())
    }
}

fn not_initialized() -> RpcError {
    domain_error("bad_state", -32035, "plugin must be initialized first")
}

fn invalid_params(message: &str) -> RpcError {
    domain_error("invalid", -32602, message)
}

fn frame_too_large(message: &str) -> RpcError {
    domain_error("frame_too_large", -32602, message)
}

fn unknown_effect() -> RpcError {
    domain_error(
        "unknown",
        -32036,
        "event batch outcome is unknown; do not replay it",
    )
}

fn domain_error(code: &str, rpc_code: i64, message: &str) -> RpcError {
    RpcError {
        code: rpc_code,
        message: message.to_owned(),
        data: Some(json!({"code": code})),
    }
}

#[derive(Default)]
struct FakeState {
    credential: Option<String>,
    source_policy: SecretPolicy,
    binding: Option<Binding>,
    observer: Option<FakeObserver>,
    publish_source: Option<String>,
    start_command_id: Option<String>,
    stop_command_id: Option<String>,
    phase: SessionPhase,
    active_turn: Option<String>,
    active_command_id: Option<String>,
    last_turn: Option<LastTurn>,
    last_batch_ack: Option<BatchAck>,
    batch_unknown: bool,
}

struct FakeObserver {
    agent_id: String,
    generation: i64,
    native_scope: String,
    native_ref: String,
    attach_command_id: String,
    context: u64,
    detach_command_id: Option<String>,
    live_published: bool,
}

struct Binding {
    agent_id: String,
    generation: i64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum SessionPhase {
    #[default]
    New,
    Starting,
    Idle,
    Busy,
    Stopping,
    Unknown,
    Stopped,
}

impl SessionPhase {
    fn as_str(&self) -> &'static str {
        match self {
            Self::New => "created",
            Self::Starting => "starting",
            Self::Idle => "idle",
            Self::Busy => "busy",
            Self::Stopping => "stopping",
            Self::Unknown => "unknown",
            Self::Stopped => "stopped",
        }
    }
}

struct LastTurn {
    command_id: String,
    turn_id: String,
    text: String,
}

#[cfg(test)]
mod provider_hold_tests {
    use super::*;

    fn command(text: &str) -> AgentCommandParams {
        AgentCommandParams {
            agent_id: "fake-agent".to_owned(),
            generation: 1,
            command_id: "send-1".to_owned(),
            verb: "send".to_owned(),
            args: json!({"text": text}),
            turn_id: None,
            request_id: None,
        }
    }

    fn completion(text: &str) -> (Value, Vec<NativeEvent>) {
        let batch = send_batch(&command(text), "turn-1", text);
        let completed = batch
            .events
            .iter()
            .find(|event| event.kind == "turn.completed")
            .expect("terminal turn event")
            .data
            .clone();
        (completed, batch.events)
    }

    #[test]
    fn fake_provider_markers_emit_only_structured_failed_terminal_restrictions() {
        for (marker, expected_kind) in [
            (PROVIDER_RATE_LIMIT_FIXTURE_INPUT, "rate_limit"),
            (PROVIDER_QUOTA_FIXTURE_INPUT, "quota"),
            (PROVIDER_AUTH_FIXTURE_INPUT, "auth"),
            (PROVIDER_MODEL_REMOVED_FIXTURE_INPUT, "model_removed"),
        ] {
            let (completed, events) = completion(marker);
            assert_eq!(completed["status"], "failed");
            assert_eq!(
                completed["provider_restriction"],
                json!({"kind": expected_kind, "retry_after": null})
            );
            assert!(completed.get("native_error").is_none());
            assert!(!events.iter().any(|event| event.kind == "message.completed"));
        }
    }

    #[test]
    fn fake_http_401_stays_failure_only_and_normal_probe_send_completes() {
        let (unauthorized, _) = completion(NATIVE_ERROR_FIXTURE_INPUT);
        assert_eq!(unauthorized["status"], "failed");
        assert!(unauthorized.get("native_error").is_some());
        assert!(unauthorized.get("provider_restriction").is_none());

        let (probe, events) = completion("ordinary probe send");
        assert_eq!(probe["status"], "completed");
        assert!(probe.get("provider_restriction").is_none());
        assert!(events.iter().any(|event| event.kind == "message.completed"));

        let (non_exact, _) = completion("[eliot_fake_provider_rate_limit] extra text");
        assert_eq!(non_exact["status"], "completed");
        assert!(non_exact.get("provider_restriction").is_none());
    }
}
