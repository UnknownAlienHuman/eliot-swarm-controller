use crate::{
    peer::{CommitSink, Peer, PeerClient, preserve_rpc_error},
    protocol::{
        AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams,
        InitializeParams, MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, MAX_EVENT_BYTES,
        MAX_TEXT_BYTES, RpcError, RpcRequest, empty_object, fake_manifest,
    },
};
use eliot_core::{BatchAck, Error as CoreError, NativeEvent, SourceBatch};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

const HOST_REQUEST_QUEUE: usize = 16;
const EVENTS_PUT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const HOST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

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
        client
            .respond(
                id,
                result.map_err(preserve_rpc_error),
                Instant::now() + HOST_RESPONSE_TIMEOUT,
            )
            .await
            .map_err(|error| PluginError::new(error.message))?;

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
        "agent.start" => handle_start(&request.params, state, client).await,
        "agent.command" => handle_command(&request.params, state, client).await,
        "agent.stop" => handle_stop(&request.params, state, client).await,
        "agent.snapshot" => handle_snapshot(&request.params, state),
        "shutdown" => handle_shutdown(state),
        _ => Err(domain_error(
            "unsupported",
            -32601,
            "plugin method is not implemented",
        )),
    }
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
    let turn_id = format!(
        "fake-{}-{}-{}",
        params.agent_id, params.generation, params.command_id
    );
    let batch = send_batch(&params, &turn_id, text);

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
    let batch = session_ended_batch(&params.agent_id, params.generation);
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
        json!({
            "command_id": turn.command_id,
            "turn_id": turn.turn_id,
            "status": "completed",
            "final_text": turn.text
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
    SourceBatch {
        batch_id: format!("{prefix}:turn"),
        agent_id: params.agent_id.clone(),
        generation: params.generation,
        source: "stream".to_owned(),
        events: vec![
            NativeEvent {
                kind: "turn.started".to_owned(),
                native_id: format!("{prefix}:turn.started"),
                data: json!({"command_id": params.command_id, "turn_id": turn_id}),
            },
            NativeEvent {
                kind: "message.completed".to_owned(),
                native_id: format!("{prefix}:message.completed"),
                data: json!({
                    "command_id": params.command_id,
                    "turn_id": turn_id,
                    "text": text
                }),
            },
            NativeEvent {
                kind: "turn.completed".to_owned(),
                native_id: format!("{prefix}:turn.completed"),
                data: json!({
                    "command_id": params.command_id,
                    "turn_id": turn_id,
                    "status": "completed"
                }),
            },
        ],
        cursor: None,
    }
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
            data: empty_object(),
        }],
        cursor: None,
    }
}

struct BatchFailure {
    error: RpcError,
    uncertain: bool,
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
    binding: Option<Binding>,
    start_command_id: Option<String>,
    stop_command_id: Option<String>,
    phase: SessionPhase,
    active_turn: Option<String>,
    active_command_id: Option<String>,
    last_turn: Option<LastTurn>,
    last_batch_ack: Option<BatchAck>,
    batch_unknown: bool,
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
