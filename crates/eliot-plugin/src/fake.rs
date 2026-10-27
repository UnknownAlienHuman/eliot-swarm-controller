use crate::protocol::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_TEXT_BYTES,
    RpcError, RpcRequest, RpcResponse, empty_object, fake_manifest,
};
use eliot_core::{BatchAck, NativeEvent, SourceBatch};
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    io,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{stdin, stdout},
    sync::{Mutex, mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

type Writer = Arc<Mutex<FramedWrite<tokio::io::Stdout, LinesCodec>>>;
type PendingAcks = Arc<Mutex<HashMap<String, oneshot::Sender<RpcResponse>>>>;

const HOST_REQUEST_QUEUE: usize = 16;
const EVENTS_PUT_ACK_TIMEOUT: Duration = Duration::from_secs(30);

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

impl From<io::Error> for PluginError {
    fn from(error: io::Error) -> Self {
        Self::new(format!("plugin I/O failed: {error}"))
    }
}

impl From<serde_json::Error> for PluginError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(format!("plugin JSON encoding failed: {error}"))
    }
}

/// Serve the bounded fake plugin over stdin/stdout JSON-RPC 2.0 NDJSON.
/// Stdout is reserved for protocol frames; this function writes no log lines.
pub async fn run_fake() -> Result<(), PluginError> {
    let reader = FramedRead::new(
        stdin(),
        LinesCodec::new_with_max_length(MAX_FRAME_BYTES.saturating_sub(1)),
    );
    let mut reader = reader;
    let writer = Arc::new(Mutex::new(FramedWrite::new(stdout(), LinesCodec::new())));
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let (request_tx, request_rx) = mpsc::channel(HOST_REQUEST_QUEUE);
    let (actor_done_tx, mut actor_done_rx) = oneshot::channel();
    let actor_writer = Arc::clone(&writer);
    let actor_pending = Arc::clone(&pending);
    let actor = tokio::spawn(async move {
        let result = fake_actor(request_rx, actor_writer, actor_pending).await;
        let _ = actor_done_tx.send(result);
    });

    loop {
        tokio::select! {
            frame = reader.next() => {
                match frame {
                    Some(Ok(line)) => {
                        if line.len().saturating_add(1) > MAX_FRAME_BYTES {
                            actor.abort();
                            return Err(PluginError::new("incoming frame exceeds 1 MiB"));
                        }
                        match decode_frame(&line) {
                            Ok(Incoming::Response(response)) => {
                                route_ack(response, &pending).await;
                            }
                            Ok(Incoming::Request(request)) => {
                                let id = request.id.clone();
                                match request_tx.try_send(request) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        write_message(
                                            &writer,
                                            &RpcResponse::failure(id, domain_error(
                                                "busy",
                                                -32020,
                                                "plugin request queue is full",
                                            )),
                                        ).await?;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        write_message(
                                            &writer,
                                            &RpcResponse::failure(id, domain_error(
                                                "unavailable",
                                                -32021,
                                                "plugin is shutting down",
                                            )),
                                        ).await?;
                                    }
                                }
                            }
                            Err(error) => {
                                write_message(
                                    &writer,
                                    &RpcResponse::failure(Value::Null, error),
                                ).await?;
                            }
                        }
                    }
                    Some(Err(_)) => {
                        actor.abort();
                        return Err(PluginError::new("invalid or oversized NDJSON frame"));
                    }
                    None => {
                        actor.abort();
                        let _ = actor.await;
                        return Err(PluginError::new(
                            "plugin input closed before a completed shutdown",
                        ));
                    }
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
    writer: Writer,
    pending: PendingAcks,
) -> Result<(), PluginError> {
    let mut state = FakeState::default();

    while let Some(request) = requests.recv().await {
        let id = request.id.clone();
        let is_shutdown = request.method == "shutdown";
        let result = dispatch(&request, &mut state, &writer, &pending).await;
        let shutdown_after_reply = is_shutdown && result.is_ok();
        let response = match result {
            Ok(value) => RpcResponse::success(id, value),
            Err(error) => RpcResponse::failure(id, error),
        };
        write_message(&writer, &response).await?;

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
    writer: &Writer,
    pending: &PendingAcks,
) -> Result<Value, RpcError> {
    match request.method.as_str() {
        "initialize" => handle_initialize(&request.params, state),
        "agent.start" => handle_start(&request.params, state, writer, pending).await,
        "agent.command" => handle_command(&request.params, state, writer, pending).await,
        "agent.stop" => handle_stop(&request.params, state, writer, pending).await,
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
    writer: &Writer,
    pending: &PendingAcks,
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
    ensure_batch_fits(&batch, &credential, state.next_rpc_id)?;

    state.binding = Some(Binding {
        agent_id: params.agent_id.clone(),
        generation: params.generation,
    });
    state.start_command_id = Some(params.command_id.clone());
    state.phase = SessionPhase::Starting;

    match commit_batch(state, &batch, &credential, writer, pending).await {
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
    writer: &Writer,
    pending: &PendingAcks,
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
    ensure_batch_fits(&batch, &credential, state.next_rpc_id)?;

    state.phase = SessionPhase::Busy;
    state.active_turn = Some(turn_id.clone());
    state.active_command_id = Some(params.command_id.clone());
    match commit_batch(state, &batch, &credential, writer, pending).await {
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
    writer: &Writer,
    pending: &PendingAcks,
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
    ensure_batch_fits(&batch, &credential, state.next_rpc_id)?;
    state.phase = SessionPhase::Stopping;
    state.stop_command_id = Some(params.command_id);

    match commit_batch(state, &batch, &credential, writer, pending).await {
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
    batch: &SourceBatch,
    credential: &str,
    writer: &Writer,
    pending: &PendingAcks,
) -> Result<BatchAck, BatchFailure> {
    let rpc_id = format!("fake-events-put-{}", state.next_rpc_id);
    let Some(next_rpc_id) = state.next_rpc_id.checked_add(1) else {
        return Err(BatchFailure {
            error: domain_error("internal", -32603, "plugin request id space exhausted"),
            uncertain: false,
        });
    };
    state.next_rpc_id = next_rpc_id;

    let mut params = serde_json::to_value(batch).map_err(|_| BatchFailure {
        error: domain_error("internal", -32603, "event batch could not be encoded"),
        uncertain: false,
    })?;
    let Some(params) = params.as_object_mut() else {
        return Err(BatchFailure {
            error: domain_error("internal", -32603, "event batch encoding was not an object"),
            uncertain: false,
        });
    };
    params.insert(
        "credential".to_owned(),
        Value::String(credential.to_owned()),
    );

    let request = RpcRequest {
        jsonrpc: "2.0".to_owned(),
        id: Value::String(rpc_id.clone()),
        method: "events.put".to_owned(),
        params: Value::Object(params.clone()),
    };
    let encoded = encode_frame(&request).map_err(|error| BatchFailure {
        error,
        uncertain: false,
    })?;

    let deadline = Instant::now() + EVENTS_PUT_ACK_TIMEOUT;
    let (ack_tx, ack_rx) = oneshot::channel();
    pending.lock().await.insert(rpc_id.clone(), ack_tx);
    state.batch_unknown = true;
    if write_encoded(writer, encoded).await.is_err() {
        pending.lock().await.remove(&rpc_id);
        return Err(BatchFailure {
            error: unknown_effect(),
            uncertain: true,
        });
    }

    let response = match timeout_at(deadline, ack_rx).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => {
            return Err(BatchFailure {
                error: unknown_effect(),
                uncertain: true,
            });
        }
    };
    if response.error.is_some() {
        // The fake effect was already recorded before events.put. A host rejection
        // does not prove the effect was absent or that the batch can be replayed.
        return Err(BatchFailure {
            error: unknown_effect(),
            uncertain: true,
        });
    }
    let Some(result) = response.result else {
        return Err(BatchFailure {
            error: unknown_effect(),
            uncertain: true,
        });
    };
    let ack: BatchAck = serde_json::from_value(result).map_err(|_| BatchFailure {
        error: unknown_effect(),
        uncertain: true,
    })?;
    let expected_count = i64::try_from(batch.events.len()).unwrap_or(i64::MAX);
    let actual_count = ack
        .last_seq
        .checked_sub(ack.first_seq)
        .and_then(|count| count.checked_add(1));
    if ack.first_seq <= 0 || actual_count != Some(expected_count) {
        return Err(BatchFailure {
            error: unknown_effect(),
            uncertain: true,
        });
    }

    state.batch_unknown = false;
    state.last_batch_ack = Some(ack.clone());
    Ok(ack)
}

fn ensure_batch_fits(
    batch: &SourceBatch,
    credential: &str,
    rpc_sequence: u64,
) -> Result<(), RpcError> {
    for event in &batch.events {
        let encoded_data = serde_json::to_vec(&event.data)
            .map_err(|_| domain_error("invalid", -32602, "event data could not be encoded"))?;
        if encoded_data.len() > MAX_EVENT_BYTES {
            return Err(frame_too_large(
                "event data exceeds the 64 KiB persisted-event limit",
            ));
        }
    }
    let mut params = serde_json::to_value(batch)
        .map_err(|_| domain_error("internal", -32603, "event batch could not be encoded"))?;
    let Some(params) = params.as_object_mut() else {
        return Err(domain_error(
            "internal",
            -32603,
            "event batch encoding was not an object",
        ));
    };
    params.insert(
        "credential".to_owned(),
        Value::String(credential.to_owned()),
    );
    let request = RpcRequest {
        jsonrpc: "2.0".to_owned(),
        id: Value::String(format!("fake-events-put-{rpc_sequence}")),
        method: "events.put".to_owned(),
        params: Value::Object(params.clone()),
    };
    encode_frame(&request).map(|_| ())
}

async fn route_ack(response: RpcResponse, pending: &PendingAcks) {
    let Some(id) = response.id.as_str().map(str::to_owned) else {
        return;
    };
    if let Some(sender) = pending.lock().await.remove(&id) {
        let _ = sender.send(response);
    }
}

async fn write_message<T: Serialize>(writer: &Writer, message: &T) -> Result<(), PluginError> {
    let encoded = encode_frame(message).map_err(|error| PluginError::new(error.message))?;
    write_encoded(writer, encoded).await
}

fn encode_frame<T: Serialize>(message: &T) -> Result<String, RpcError> {
    let encoded = serde_json::to_string(message)
        .map_err(|_| domain_error("internal", -32603, "JSON-RPC frame could not be encoded"))?;
    if encoded.len().saturating_add(1) > MAX_FRAME_BYTES {
        return Err(frame_too_large("encoded JSON-RPC frame exceeds 1 MiB"));
    }
    Ok(encoded)
}

async fn write_encoded(writer: &Writer, encoded: String) -> Result<(), PluginError> {
    writer
        .lock()
        .await
        .send(encoded)
        .await
        .map_err(|_| PluginError::new("plugin protocol write failed"))
}

enum Incoming {
    Request(RpcRequest),
    Response(RpcResponse),
}

fn decode_frame(line: &str) -> Result<Incoming, RpcError> {
    let value: Value = serde_json::from_str(line)
        .map_err(|_| domain_error("invalid", -32700, "invalid JSON-RPC frame"))?;
    let Some(object) = value.as_object() else {
        return Err(domain_error(
            "invalid",
            -32600,
            "JSON-RPC frame must be an object",
        ));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(domain_error(
            "invalid",
            -32600,
            "JSON-RPC version must be 2.0",
        ));
    }

    if object.contains_key("method") {
        let request: RpcRequest = serde_json::from_value(value)
            .map_err(|_| domain_error("invalid", -32600, "invalid JSON-RPC request"))?;
        if !valid_id(&request.id) || !request.params.is_object() {
            return Err(invalid_params(
                "JSON-RPC request requires an id and object params",
            ));
        }
        return Ok(Incoming::Request(request));
    }

    let response: RpcResponse = serde_json::from_value(value)
        .map_err(|_| domain_error("invalid", -32600, "invalid JSON-RPC response"))?;
    if !valid_id(&response.id) || response.error.is_some() == response.result.is_some() {
        return Err(domain_error(
            "invalid",
            -32600,
            "JSON-RPC response must contain exactly one result or error",
        ));
    }
    Ok(Incoming::Response(response))
}

fn valid_id(id: &Value) -> bool {
    id.is_null() || id.is_string() || id.is_number()
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
    next_rpc_id: u64,
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
