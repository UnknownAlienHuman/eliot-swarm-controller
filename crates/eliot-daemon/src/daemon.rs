use crate::{
    bus::Bus,
    platform::{DataDir, IpcListener, IpcStream},
    router::Router,
};
use eliot_core::{Error, Result};
use eliot_plugin::{RpcError, RpcRequest, RpcResponse};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinSet,
    time::{Instant, timeout_at},
};
use tokio_util::codec::{Framed, LinesCodec};
use ulid::Ulid;

const MAX_RPC_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_CLIENTS: usize = 128;
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(35);

/// Open the private local daemon, recover durable state without replaying any
/// external effects, and serve concurrent local RPC connections.
pub async fn run(path: PathBuf) -> Result<()> {
    let data_dir = DataDir::open(&path)?;
    let daemon_lock = data_dir.acquire_daemon_lock()?;
    let store_path = data_dir.path().join("eliot.db");
    let store = eliot_store::Store::open(&store_path)?;
    let store = std::sync::Arc::new(Mutex::new(store));
    let bus = Bus::new(1_024);
    let router = Router::new(
        data_dir.path().to_path_buf(),
        data_dir.owner_token().to_owned(),
        std::sync::Arc::clone(&store),
        bus.clone(),
    );

    let recovered = {
        let mut store = store.lock().await;
        let events = store.recover_on_restart(now_ms())?;
        bus.publish(&events);
        events
    };
    router.note_events(&recovered);

    let mut listener = IpcListener::bind(&data_dir, &daemon_lock).await?;
    data_dir.publish_daemon(&daemon_lock, listener.endpoint(), env!("CARGO_PKG_VERSION"))?;

    let (monitor_stop, monitor_shutdown) = watch::channel(false);
    let mut background = JoinSet::new();
    let monitoring = router.monitoring();
    background.spawn(monitoring.clone().run(monitor_shutdown));
    let mut progress = router.progress_receiver();
    let mut connections = JoinSet::new();
    let outcome = loop {
        match router.should_exit().await {
            Ok(true) => break Ok(()),
            Ok(false) => {}
            Err(error) => break Err(error),
        }
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) if connections.len() < MAX_CLIENTS => {
                        let router = router.clone();
                        connections.spawn(async move {
                            serve_connection(stream, router).await;
                        });
                    }
                    Ok(_stream) => {}
                    Err(error) => {
                        if router.is_force_requested() {
                            break Ok(());
                        }
                        break Err(error);
                    }
                }
            }
            changed = progress.changed() => {
                if changed.is_err() {
                    break Err(Error::new("internal", "daemon progress channel closed"));
                }
            }
            joined = background.join_next(), if !background.is_empty() => {
                match joined {
                    Some(Ok(Err(error))) => monitoring.mark_unavailable(&error.code),
                    Some(Err(_)) => monitoring.mark_unavailable("monitor_task_failed"),
                    _ => monitoring.mark_unavailable("monitor_task_stopped"),
                }
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                let _ = joined;
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    break Err(Error::new(
                        "platform_io",
                        format!("listen for shutdown signal: {error}"),
                    ));
                }
                router.request_force();
            }
        }
    };

    if outcome.is_err() {
        router.request_force();
    }
    if router.is_force_requested() {
        router.force_owned_processes().await;
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    monitor_stop.send_replace(true);
    while let Some(joined) = background.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => monitoring.mark_unavailable(&error.code),
            Err(_) => monitoring.mark_unavailable("monitor_task_failed"),
        }
    }
    outcome
}

/// Make one authenticated local JSON-RPC call. Clients open only an existing
/// private root; reads never initialize credentials, a database, or a daemon.
pub async fn client_call(path: &Path, method: &str, params: Value) -> Result<Value> {
    if method.trim().is_empty() || !params.is_object() {
        return Err(Error::new(
            "invalid",
            "daemon RPC requires a method and object params",
        ));
    }
    let identity = caller_identity()?;
    let data_dir = DataDir::open_existing(path)?;
    let info = data_dir
        .read_daemon_info()?
        .ok_or_else(|| Error::new("unavailable", "daemon is not running"))?;
    let auth = match identity {
        ClientIdentity::Owner => json!({
            "kind": "owner",
            "credential": data_dir.owner_token(),
        }),
        ClientIdentity::Agent {
            id,
            generation,
            token,
        } => json!({
            "kind": "agent",
            "agent_id": id,
            "generation": generation,
            "credential": token,
        }),
    };
    let mut envelope = serde_json::Map::new();
    envelope.insert("_auth".to_owned(), auth);
    envelope.insert("_payload".to_owned(), params.clone());

    let id = Value::String(Ulid::new().to_string());
    let request = RpcRequest {
        jsonrpc: "2.0".to_owned(),
        id: id.clone(),
        method: method.to_owned(),
        params: Value::Object(envelope),
    };
    let frame = serde_json::to_string(&request)
        .map_err(|error| Error::new("internal", format!("encode daemon RPC: {error}")))?;
    check_frame_size(&frame)?;

    let timeout = call_timeout(method, &params);
    let deadline = Instant::now() + timeout;
    timeout_at(deadline, async move {
        let stream = IpcStream::connect(&info.endpoint).await?;
        let mut framed = Framed::new(
            stream,
            LinesCodec::new_with_max_length(MAX_RPC_FRAME_BYTES.saturating_sub(1)),
        );
        framed
            .send(frame)
            .await
            .map_err(|_| Error::new("unavailable", "daemon RPC write failed"))?;
        let line = framed
            .next()
            .await
            .ok_or_else(|| Error::new("unavailable", "daemon closed the RPC connection"))?
            .map_err(|_| Error::new("frame_too_large", "daemon response frame is invalid"))?;
        if line.len().saturating_add(1) > MAX_RPC_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "daemon response exceeds the 4 MiB frame limit",
            ));
        }
        let response: RpcResponse = serde_json::from_str(&line)
            .map_err(|_| Error::new("invalid", "daemon response is malformed"))?;
        if response.jsonrpc != "2.0" || response.id != id {
            return Err(Error::new(
                "invalid",
                "daemon response does not match the request",
            ));
        }
        match (response.result, response.error) {
            (Some(value), None) => Ok(value),
            (None, Some(error)) => Err(core_error(error)),
            _ => Err(Error::new(
                "invalid",
                "daemon response must contain exactly one result or error",
            )),
        }
    })
    .await
    .map_err(|_| Error::new("unknown", "daemon RPC exceeded its absolute deadline"))?
}

async fn serve_connection(stream: IpcStream, router: Router) {
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_RPC_FRAME_BYTES.saturating_sub(1)),
    );
    let mut arm_shutdown = false;
    let incoming = timeout_at(Instant::now() + DEFAULT_CALL_TIMEOUT, framed.next()).await;
    let response = match incoming {
        Ok(Some(Ok(line))) if line.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => {
            match decode_request(&line) {
                Ok(request) => {
                    let is_shutdown = request.method == "daemon.shutdown";
                    match router.handle(&request.method, request.params).await {
                        Ok(result) => {
                            arm_shutdown = is_shutdown;
                            RpcResponse {
                                jsonrpc: "2.0".to_owned(),
                                id: request.id,
                                result: Some(result),
                                error: None,
                            }
                        }
                        Err(error) => RpcResponse {
                            jsonrpc: "2.0".to_owned(),
                            id: request.id,
                            result: None,
                            error: Some(rpc_error(error)),
                        },
                    }
                }
                Err((id, error)) => RpcResponse {
                    jsonrpc: "2.0".to_owned(),
                    id,
                    result: None,
                    error: Some(error),
                },
            }
        }
        Ok(Some(Ok(_))) | Ok(Some(Err(_))) => RpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Value::Null,
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "request frame exceeds the 4 MiB limit".to_owned(),
                data: Some(json!({"code": "frame_too_large"})),
            }),
        },
        Ok(None) | Err(_) => return,
    };

    let send_result = match serde_json::to_string(&response) {
        Ok(frame) if frame.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => {
            framed.send(frame).await
        }
        _ => {
            let fallback = RpcResponse {
                jsonrpc: "2.0".to_owned(),
                id: response.id.clone(),
                result: None,
                error: Some(RpcError {
                    code: -32603,
                    message: "daemon response exceeded the frame limit".to_owned(),
                    data: Some(json!({"code": "frame_too_large"})),
                }),
            };
            match serde_json::to_string(&fallback) {
                Ok(frame) => framed.send(frame).await,
                Err(_) => return,
            }
        }
    };
    let _ = send_result;
    router.response_finished(arm_shutdown);
}

fn decode_request(line: &str) -> std::result::Result<RpcRequest, (Value, RpcError)> {
    let value: Value = serde_json::from_str(line).map_err(|_| {
        (
            Value::Null,
            RpcError {
                code: -32700,
                message: "request is not valid JSON".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        )
    })?;
    let Some(object) = value.as_object() else {
        return Err((
            Value::Null,
            RpcError {
                code: -32600,
                message: "JSON-RPC request must be an object".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    };
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err((
            id,
            RpcError {
                code: -32600,
                message: "JSON-RPC version must be 2.0".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    }
    let request: RpcRequest = serde_json::from_value(value).map_err(|_| {
        (
            id.clone(),
            RpcError {
                code: -32600,
                message: "JSON-RPC request is malformed".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        )
    })?;
    if !valid_id(&request.id) || !request.params.is_object() {
        return Err((
            request.id,
            RpcError {
                code: -32600,
                message: "JSON-RPC request requires a scalar id and object params".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    }
    Ok(request)
}

fn rpc_error(error: Error) -> RpcError {
    let code = match error.code.as_str() {
        "unsupported" => -32601,
        "invalid" | "frame_too_large" => -32602,
        _ => -32000,
    };
    RpcError {
        code,
        message: error.message.chars().take(1_024).collect(),
        data: Some(json!({"code": error.code})),
    }
}

fn core_error(error: RpcError) -> Error {
    let code = error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("internal");
    Error::new(code, error.message)
}

enum ClientIdentity {
    Owner,
    Agent {
        id: String,
        generation: i64,
        token: String,
    },
}

fn caller_identity() -> Result<ClientIdentity> {
    let names = [
        "ELIOT_AGENT_ID",
        "ELIOT_AGENT_GENERATION",
        "ELIOT_AGENT_TOKEN",
    ];
    let values: Vec<_> = names.iter().map(std::env::var_os).collect();
    let any = values.iter().any(Option::is_some);
    if any {
        if values.iter().any(Option::is_none) {
            return Err(Error::new(
                "not_allowed",
                "managed Agent credentials are incomplete",
            ));
        }
        let id = values[0]
            .as_ref()
            .and_then(|value| value.to_str())
            .ok_or_else(|| Error::new("not_allowed", "managed Agent id is invalid"))?;
        let generation = values[1]
            .as_ref()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| Error::new("not_allowed", "managed Agent generation is invalid"))?;
        let token = values[2]
            .as_ref()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("not_allowed", "managed Agent token is invalid"))?;
        if !valid_agent_id(id) {
            return Err(Error::new("not_allowed", "managed Agent id is invalid"));
        }
        return Ok(ClientIdentity::Agent {
            id: id.to_owned(),
            generation,
            token: token.to_owned(),
        });
    }
    Ok(ClientIdentity::Owner)
}

fn valid_agent_id(id: &str) -> bool {
    id.strip_prefix('a')
        .and_then(|digits| digits.parse::<i64>().ok())
        .is_some_and(|number| number > 0)
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

fn call_timeout(method: &str, params: &Value) -> Duration {
    if method == "events.subscribe" {
        let wait_ms = params
            .get("wait_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(30_000);
        Duration::from_millis(wait_ms).saturating_add(Duration::from_secs(5))
    } else {
        DEFAULT_CALL_TIMEOUT
    }
}

fn check_frame_size(frame: &str) -> Result<()> {
    if frame.len().saturating_add(1) > MAX_RPC_FRAME_BYTES {
        Err(Error::new(
            "frame_too_large",
            "daemon RPC frame exceeds 4 MiB",
        ))
    } else {
        Ok(())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
