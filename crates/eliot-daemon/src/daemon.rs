use crate::{
    automation,
    bus::Bus,
    common::now_ms,
    logging,
    platform::{DataDir, IpcListener, IpcStream, data_dir::DaemonLock},
    router::Router,
};
use eliot_core::{
    Error, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SecretPolicy, bound_stderr_diagnostics,
};
use eliot_plugin::{RpcError, RpcRequest, RpcResponse};
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
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
const RULE_CHILD_RECOVERY_PAGE_SIZE: usize = 1_000;
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(35);

/// Open the private local daemon, recover durable state without replaying any
/// external effects, and serve concurrent local RPC connections.
pub async fn run(path: PathBuf) -> Result<()> {
    let data_dir = DataDir::open(&path)?;
    let daemon_lock = data_dir.acquire_daemon_lock()?;
    let _log = logging::init(&data_dir, &daemon_lock)?;
    let policy = Arc::new(SecretPolicy::new([data_dir.owner_token()]));
    logging::record(&policy, "daemon.starting", &[], None);
    let outcome = run_owned(&data_dir, &daemon_lock, Arc::clone(&policy)).await;
    logging::record(
        &policy,
        if outcome.is_ok() {
            "daemon.stopped"
        } else {
            "daemon.failed"
        },
        &[],
        outcome.as_ref().err(),
    );
    outcome
}

async fn run_owned(
    data_dir: &DataDir,
    daemon_lock: &DaemonLock,
    policy: Arc<SecretPolicy>,
) -> Result<()> {
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
    // Parse global definitions and seed only new rules before publishing ready.
    let (global_rules, initial_deferred_notice) =
        automation::initialize_global_rules(&router, data_dir.path(), &policy).await?;

    let mut listener = IpcListener::bind(data_dir, daemon_lock).await?;
    let info =
        data_dir.publish_daemon(daemon_lock, listener.endpoint(), env!("CARGO_PKG_VERSION"))?;
    logging::record(&policy, "daemon.ready", &[("boot_id", &info.boot_id)], None);

    let (monitor_stop, monitor_shutdown) = watch::channel(false);
    let mut background = JoinSet::new();
    let monitoring = router.monitoring();
    background.spawn(monitoring.clone().run(monitor_shutdown));
    let mut recovery = JoinSet::new();
    let recovery_router = router.clone();
    recovery.spawn(async move { recovery_router.recover_workspaces().await });
    let (automation_stop, automation_shutdown) = watch::channel(false);
    let mut automation_tasks = JoinSet::new();
    let rule_recovery_router = router.clone();
    automation_tasks.spawn(async move {
        rule_recovery_router
            .recover_rule_children(RULE_CHILD_RECOVERY_PAGE_SIZE)
            .await
    });
    let rule_router = router.clone();
    let rule_data_root = data_dir.path().to_path_buf();
    let rule_policy = Arc::clone(&policy);
    automation_tasks.spawn(async move {
        automation::run_global_rules(
            rule_router,
            global_rules,
            rule_data_root,
            rule_policy,
            initial_deferred_notice,
            automation_shutdown,
        )
        .await
    });
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
                        let policy = Arc::clone(&policy);
                        connections.spawn(async move {
                            serve_connection(stream, router, policy).await;
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
                    Some(Ok(Err(error))) => {
                        logging::record(&policy, "daemon.monitor_failed", &[], Some(&error));
                        monitoring.mark_unavailable(&error.code);
                    }
                    Some(Err(_)) => {
                        logging::record(&policy, "daemon.monitor_task_failed", &[], None);
                        monitoring.mark_unavailable("monitor_task_failed");
                    }
                    _ => {
                        logging::record(&policy, "daemon.monitor_task_stopped", &[], None);
                        monitoring.mark_unavailable("monitor_task_stopped");
                    }
                }
            }
            joined = recovery.join_next(), if !recovery.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => {
                        logging::record(&policy, "daemon.workspace_recovery_failed", &[], Some(&error));
                        router.note_workspace_recovery_failure(&error.code);
                    }
                    _ => {
                        logging::record(&policy, "daemon.workspace_recovery_task_failed", &[], None);
                        router.note_workspace_recovery_failure("workspace_recovery_task_failed");
                    }
                }
            }
            joined = automation_tasks.join_next(), if !automation_tasks.is_empty() => {
                match joined {
                    Some(Ok(Err(error))) => {
                        logging::record(&policy, "daemon.rule_worker_failed", &[], Some(&error));
                    }
                    Some(Err(_)) => {
                        logging::record(&policy, "daemon.rule_worker_task_failed", &[], None);
                    }
                    _ => {}
                }
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if matches!(joined, Some(Err(_))) {
                    logging::record(&policy, "daemon.connection_task_failed", &[], None);
                }
                let _ = joined;
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    break Err(Error::new(
                        "platform_io",
                        format!("listen for shutdown signal: {error}"),
                    ));
                }
                logging::record(&policy, "daemon.force_requested", &[], None);
                router.request_force().await;
            }
        }
    };

    if outcome.is_err() {
        router.request_force().await;
    }
    automation_stop.send_replace(true);
    while let Some(joined) = automation_tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                logging::record(&policy, "daemon.rule_worker_failed", &[], Some(&error));
            }
            Err(_) => logging::record(&policy, "daemon.rule_worker_task_failed", &[], None),
        }
    }
    if router.is_force_requested() {
        router.force_owned_processes().await;
    }
    recovery.abort_all();
    while recovery.join_next().await.is_some() {}
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
    let policy = SecretPolicy::new([
        data_dir.owner_token(),
        auth["credential"].as_str().unwrap_or(""),
    ]);
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
        let stream = IpcStream::connect(&info.endpoint, deadline).await?;
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
            (None, Some(error)) => Err(core_error(error, &policy)),
            _ => Err(Error::new(
                "invalid",
                "daemon response must contain exactly one result or error",
            )),
        }
    })
    .await
    .map_err(|_| Error::new("unknown", "daemon RPC exceeded its absolute deadline"))?
}

/// Typed caller boundary over the existing authenticated client. Params are
/// serialized once; transport/domain errors retain their original diagnostics.
/// Current public RPC results are objects, never positional arrays or scalars.
pub async fn client_call_typed<P: Serialize + ?Sized, R: DeserializeOwned>(
    data_dir: &Path,
    method: &str,
    params: &P,
) -> Result<R> {
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("invalid", "daemon RPC parameters could not be serialized"))?;
    decode_client_result(method, client_call(data_dir, method, params).await)
}

fn decode_client_result<R: DeserializeOwned>(method: &str, result: Result<Value>) -> Result<R> {
    let result = result?;
    let context = match method {
        "events.read" => "events.read returned a malformed typed result",
        "events.subscribe" => "events.subscribe returned a malformed typed result",
        _ => "daemon returned a malformed typed result",
    };
    if !result.is_object() {
        return Err(Error::new("unknown", context));
    }
    serde_json::from_value(result).map_err(|_| Error::new("unknown", context))
}

async fn serve_connection(stream: IpcStream, router: Router, policy: Arc<SecretPolicy>) {
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
                        Err(error) => {
                            logging::record(&policy, "rpc.error", &[], Some(&error));
                            RpcResponse {
                                jsonrpc: "2.0".to_owned(),
                                id: request.id,
                                result: None,
                                error: Some(rpc_error(error, &policy)),
                            }
                        }
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
    if send_result.is_err() {
        logging::record(&policy, "rpc.response_write_failed", &[], None);
    }
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

fn rpc_error(mut error: Error, policy: &SecretPolicy) -> RpcError {
    error.message = policy
        .redact_text(&error.message)
        .chars()
        .take(1_024)
        .collect();
    for diagnostic in &mut error.diagnostics {
        diagnostic.snapshot.text = policy.redact_text(&diagnostic.snapshot.text);
    }
    bound_stderr_diagnostics(&mut error.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    let code = match error.code.as_str() {
        "unsupported" => -32601,
        "invalid" | "frame_too_large" => -32602,
        _ => -32000,
    };
    let mut data = serde_json::to_value(&error).unwrap_or_else(|_| json!({"code": error.code}));
    if let Some(object) = data.as_object_mut() {
        object.remove("message");
    }
    RpcError {
        code,
        message: error.message,
        data: Some(data),
    }
}

fn core_error(error: RpcError, policy: &SecretPolicy) -> Error {
    let mut data = error.data.unwrap_or_else(|| json!({}));
    let code = data
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal")
        .to_owned();
    let message = policy.redact_text(&error.message);
    if let Some(values) = data.get_mut("diagnostics").and_then(Value::as_array_mut) {
        for diagnostic in values.iter_mut().take(2) {
            if let Some(Value::String(text)) = diagnostic.pointer_mut("/snapshot/text") {
                *text = policy.redact_text(text);
            }
        }
    }
    let Some(object) = data.as_object_mut() else {
        return Error::new(code, message);
    };
    object.insert("code".to_owned(), json!(code));
    object.insert("message".to_owned(), json!(message));
    let mut result =
        serde_json::from_value::<Error>(data).unwrap_or_else(|_| Error::new(code, message));
    bound_stderr_diagnostics(&mut result.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    result
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
    eliot_core::parse_agent_id(id).is_ok()
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

fn call_timeout(method: &str, params: &Value) -> Duration {
    if method == "agent.discover" {
        // Includes bounded reader shutdown/reaping after the read deadline.
        Duration::from_secs(45)
    } else if method == "events.subscribe" {
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{MAX_STDERR_BYTES, StderrDiagnostic, StderrSnapshot, StderrSource};

    #[test]
    fn shared_helpers_managed_auth_requires_canonical_agent_ids() {
        for id in ["a1", "a10", "a9223372036854775807"] {
            assert!(valid_agent_id(id));
        }
        for id in [
            "",
            "a",
            "A1",
            "a0",
            "a-1",
            "a01",
            "a+1",
            "a0001",
            " a1",
            "a1 ",
            "a1\n",
            "a١",
            "a１",
            "a9223372036854775808",
            "01MUTATIONID",
        ] {
            assert!(!valid_agent_id(id), "{id:?}");
        }
    }

    struct UnserializableParams(std::cell::Cell<usize>);

    impl Serialize for UnserializableParams {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            self.0.set(self.0.get() + 1);
            Err(serde::ser::Error::custom(
                "dummy-private-serialization-detail",
            ))
        }
    }

    #[tokio::test]
    async fn typed_client_rejects_serialization_before_opening_the_daemon() {
        let params = UnserializableParams(std::cell::Cell::new(0));
        let result = client_call_typed::<_, Value>(
            Path::new("unused-typed-client-data-dir"),
            "events.read",
            &params,
        )
        .await;
        let error = result.expect_err("invalid params before client I/O");
        assert_eq!(error.code, "invalid");
        assert_eq!(
            error.message,
            "daemon RPC parameters could not be serialized"
        );
        assert!(!error.message.contains("dummy-private"));
        assert_eq!(params.0.get(), 1);
    }

    #[test]
    fn typed_client_result_mismatch_is_unknown_without_raw_details() {
        for method in ["events.read", "events.subscribe", "dummy-private-method"] {
            let error = decode_client_result::<u64>(method, Ok(json!("dummy-private-response")))
                .expect_err("wrong successful result shape");
            assert_eq!(error.code, "unknown");
            assert!(!error.message.contains("dummy-private"));
            assert!(error.diagnostics.is_empty());
        }
    }

    #[test]
    fn typed_client_event_pages_require_objects_not_positional_arrays() {
        use eliot_core::api::{EventsReadResponse, EventsSubscribeResponse};

        let read =
            decode_client_result::<EventsReadResponse>("events.read", Ok(json!([[], 0, 0, false])));
        assert_eq!(read.expect_err("positional read result").code, "unknown");
        let subscribe = decode_client_result::<EventsSubscribeResponse>(
            "events.subscribe",
            Ok(json!([[], 0, 0, false, false])),
        );
        assert_eq!(
            subscribe.expect_err("positional subscribe result").code,
            "unknown"
        );

        let read = decode_client_result::<EventsReadResponse>(
            "events.read",
            Ok(json!({
                "events": [], "next": 0, "high_water": 0, "truncated": false,
            })),
        )
        .expect("object read result");
        assert!(read.events.is_empty());
        let subscribe = decode_client_result::<EventsSubscribeResponse>(
            "events.subscribe",
            Ok(json!({
                "events": [], "next": 0, "high_water": 0, "truncated": false, "lagged": false,
            })),
        )
        .expect("object subscribe result");
        assert!(subscribe.events.is_empty());
    }

    #[test]
    fn typed_client_preserves_the_original_core_error_and_diagnostics() {
        let original = Error::new("not_allowed", "original rejection").with_stderr(
            StderrSource::Plugin,
            StderrSnapshot {
                text: "dummy sanitized diagnostic".to_owned(),
                truncated: false,
                eof: true,
                read_error: false,
            },
        );
        let actual = decode_client_result::<u64>("events.read", Err(original.clone()))
            .expect_err("underlying Core error");
        assert_eq!(actual, original);
    }

    #[test]
    fn rpc_error_preserves_both_sources_and_primary_code_with_expanding_redaction() {
        let policy = SecretPolicy::new(["!"]);
        let mut original = Error::new("unknown", "same primary message");
        original.diagnostics = [StderrSource::Plugin, StderrSource::Harness]
            .into_iter()
            .map(|source| StderrDiagnostic {
                source,
                snapshot: StderrSnapshot {
                    text: "!\0".repeat(MAX_STDERR_BYTES / 2),
                    truncated: false,
                    eof: true,
                    read_error: false,
                },
            })
            .collect();
        let wire = rpc_error(original, &policy);
        assert!(serde_json::to_vec(&wire).unwrap().len() <= 64 * 1024);
        let restored = core_error(wire, &policy);
        assert_eq!(restored.code, "unknown");
        assert_eq!(restored.message, "same primary message");
        assert_eq!(restored.diagnostics.len(), 2);
        assert!(
            restored
                .diagnostics
                .iter()
                .any(|tail| tail.source == StderrSource::Plugin)
        );
        assert!(
            restored
                .diagnostics
                .iter()
                .any(|tail| tail.source == StderrSource::Harness)
        );
        for diagnostic in &restored.diagnostics {
            assert!(diagnostic.snapshot.truncated);
            assert!(!diagnostic.snapshot.text.contains('!'));
        }
    }
}
