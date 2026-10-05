use crate::{
    config::{NativeOptions, is_loopback_endpoint},
    journal::{OperationIntent, digest_json},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, Method, Url, header};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    net::SocketAddr,
    path::Path,
    time::Duration,
};
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::RuntimeCommand,
};

const MAX_NATIVE_BODY: usize = 4 * 1024 * 1024;
const MAX_NATIVE_REQUEST: usize = 4 * 1024 * 1024;
const MAX_CONNECTION_FILE: usize = 64 * 1024;
const MAX_LOG_BYTES: usize = 8 * 1024 * 1024;
const MAX_LOG_EVENTS: usize = 8192;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionRecord {
    schema_version: u32,
    endpoint: String,
    pid: u32,
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct ServerInfo {
    version: String,
    pid: u32,
    urls: Vec<String>,
    paths: ServerPaths,
}

#[derive(Deserialize)]
struct ServerPaths {
    tmp: String,
}

#[derive(Debug, Clone)]
pub struct NativeObservation {
    pub version: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum InputEvidence {
    InboxReadback,
    ProjectedMessageReadback,
}

pub struct NativeClient {
    client: Client,
    endpoint: Url,
    pid: u32,
    version: String,
}

impl NativeClient {
    pub async fn connect(options: &NativeOptions) -> Result<(Self, NativeObservation)> {
        let path = options.connection_file.clone();
        let record = tokio::task::spawn_blocking(move || read_connection_record(&path))
            .await
            .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection reader stopped"))??;
        let endpoint = checked_endpoint(&record.endpoint)?;
        let mut auth = header::HeaderValue::from_str(&format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", record.username, record.password))
        ))
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "invalid authentication value"))?;
        auth.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth);
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        let mut builder = Client::builder()
            .default_headers(headers)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(3))
            .http1_only();
        if endpoint.host_str() == Some("localhost") {
            builder = builder.resolve(
                "localhost",
                SocketAddr::from((
                    [127, 0, 0, 1],
                    endpoint.port_or_known_default().unwrap_or(80),
                )),
            );
        }
        let client = builder
            .build()
            .map_err(|_| Error::new("NATIVE_TRANSPORT", "HTTP client initialization failed"))?;
        let service = Self {
            client,
            endpoint,
            pid: record.pid,
            version: options.expected_version.clone(),
        };
        let info: ServerInfo = serde_json::from_value(service.get("/api/info", &[]).await?)
            .map_err(|_| Error::new("NATIVE_SCHEMA_ERROR", "native info response is invalid"))?;
        if info.pid != service.pid
            || info.version != service.version
            || info.urls.is_empty()
            || info.paths.tmp.is_empty()
        {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service identity/version differs from its explicit connection record",
            ));
        }
        Ok((
            service,
            NativeObservation {
                version: info.version,
                pid: info.pid,
            },
        ))
    }

    pub async fn probe_route(&self, options: &NativeOptions) -> Result<()> {
        self.location(options).await?;
        self.check_route_model_available(options).await
    }

    pub async fn preflight_open(&self, options: &NativeOptions) -> Result<Value> {
        self.check_route_model_available(options).await?;
        self.location(options).await
    }

    pub async fn preflight_send(
        &self,
        command: &RuntimeCommand,
        options: &NativeOptions,
        root: &str,
    ) -> Result<String> {
        if command.method == "agent.send" && command.input["delivery"] != "next_turn" {
            return Err(Error::new(
                "UNSUPPORTED_EXACT_TURN_STEER",
                "OpenCode V2 queue has no atomic expected-turn guard; exact-turn steering is unsupported",
            ));
        }
        if command.method == "task.dispatch"
            && let Some(packet) = command.input.get("launch_dispatch_packet")
            && (packet["selection"]["provider"] != options.model.provider_id
                || packet["selection"]["model"] != options.model.id
                || packet["selection"]["variant"] != options.model.variant)
        {
            return Err(Error::new(
                "NATIVE_MODEL_MISMATCH",
                "immutable dispatch selection differs from the pinned native provider/model/variant",
            ));
        }
        self.verify_binding_model(root, options, command).await?;
        self.require_durable_root_creation(root, command, options)
            .await?;
        let text = prompt(command)?;
        let request = json!({
            "id":input_id(&command.operation_id),
            "text":text,
            "metadata":{"eliot":marker(command)},
            "delivery":"queue",
            "resume":true
        });
        if serde_json::to_vec(&request)?.len() > MAX_NATIVE_REQUEST {
            return Err(Error::new(
                "NATIVE_REQUEST_LIMIT",
                "native prompt request exceeds its protocol body bound",
            ));
        }
        Ok(text)
    }

    pub async fn create_root(
        &self,
        command: &RuntimeCommand,
        options: &NativeOptions,
        root: &str,
        location: Value,
    ) -> Result<()> {
        let body = json!({
            "id":root,
            "location":location,
            "model":options.model,
            "metadata":{"eliot":marker(command)}
        });
        let value = self.post("/api/session", body).await?;
        let data = value.get("data").ok_or_else(|| {
            Error::new(
                "NATIVE_SCHEMA_ERROR",
                "session creation response lacks data",
            )
        })?;
        if !root_session_matches(data, root, command, options, &location) {
            return Err(Error::new(
                "NATIVE_OUTCOME_UNKNOWN",
                "session creation response did not prove exact identity",
            ));
        }
        self.require_durable_root_creation(root, command, options)
            .await
    }

    pub async fn admit_input(
        &self,
        command: &RuntimeCommand,
        options: &NativeOptions,
        root: &str,
        input_id: &str,
        prompt_text: &str,
    ) -> Result<()> {
        let body = json!({
            "id":input_id,
            "text":prompt_text,
            "metadata":{"eliot":marker(command)},
            "delivery":"queue",
            "resume":true
        });
        let value = self
            .post(&format!("/api/session/{root}/prompt"), body)
            .await?;
        let data = value
            .get("data")
            .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "prompt response lacks data"))?;
        if !inbox_matches(data, root, input_id, prompt_text, command) {
            return Err(Error::new(
                "NATIVE_OUTCOME_UNKNOWN",
                "prompt response did not prove exact inbox admission",
            ));
        }
        Ok(())
    }

    pub async fn reconcile_open(
        &self,
        intent: &OperationIntent,
        options: &NativeOptions,
    ) -> Result<()> {
        let root = intent.native_root_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved open intent has no native root ID",
            )
        })?;
        let session = self.session(root).await?;
        let location = self.location(options).await?;
        if !session_identity_matches(
            &session,
            root,
            options,
            &location,
            &intent.binding_id,
            intent.generation,
        ) || session["metadata"]["eliot"] != intent.marker
        {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact saved root identity was not observed",
            ));
        }
        self.require_durable_root_creation_for_intent(root, intent, options)
            .await
    }

    pub async fn reconcile_input(
        &self,
        intent: &OperationIntent,
        options: &NativeOptions,
    ) -> Result<InputEvidence> {
        let root = intent.native_root_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved input intent has no root ID",
            )
        })?;
        let input = intent.native_input_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved input intent has no input ID",
            )
        })?;
        if input != input_id(&intent.operation_id) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved input ID does not match its immutable operation identity",
            ));
        }
        valid_id(input, "msg_swarm_")?;
        let marker = &intent.marker;
        let session = self.session(root).await?;
        let location = self.location(options).await?;
        if !session_identity_matches(
            &session,
            root,
            options,
            &location,
            &intent.binding_id,
            intent.generation,
        ) || session["metadata"]["eliot"]["binding"] != intent.binding_id
            || session["metadata"]["eliot"]["generation"] != intent.generation
        {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact saved session identity was not observed",
            ));
        }
        self.check_route_model_available(options).await?;
        self.require_durable_root_creation_for_intent(root, intent, options)
            .await?;
        let inbox = self.get(&format!("/api/session/{root}/inbox"), &[]).await?;
        let items = inbox["data"]
            .as_array()
            .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "native inbox response is invalid"))?;
        if items
            .iter()
            .any(|item| saved_inbox_matches(item, root, input, marker, intent))
        {
            return Ok(InputEvidence::InboxReadback);
        }
        let message = self
            .get(&format!("/api/session/{root}/message/{input}"), &[])
            .await?;
        let message = message.get("data").ok_or_else(|| {
            Error::new("NATIVE_SCHEMA_ERROR", "native message response lacks data")
        })?;
        if saved_message_matches(message, root, input, marker, intent) {
            return Ok(InputEvidence::ProjectedMessageReadback);
        }
        Err(Error::new(
            "NATIVE_EVIDENCE_UNAVAILABLE",
            "exact saved input was not observed",
        ))
    }

    async fn verify_binding_model(
        &self,
        root: &str,
        options: &NativeOptions,
        command: &RuntimeCommand,
    ) -> Result<()> {
        let session = self.session(root).await?;
        let location = self.location(options).await?;
        if !session_identity_matches(
            &session,
            root,
            options,
            &location,
            &command.binding_id,
            command.generation,
        ) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "session identity, location or pinned model differs from this binding",
            ));
        }
        self.check_route_model_available(options).await
    }

    async fn session(&self, root: &str) -> Result<Value> {
        valid_id(root, "ses")?;
        let value = self.get(&format!("/api/session/{root}"), &[]).await?;
        let data = value.get("data").cloned().ok_or_else(|| {
            Error::new("NATIVE_SCHEMA_ERROR", "native session response lacks data")
        })?;
        if data["id"] != root || !data.is_object() {
            return Err(Error::new(
                "NATIVE_SCHEMA_ERROR",
                "native session response has the wrong identity",
            ));
        }
        Ok(data)
    }

    async fn location(&self, options: &NativeOptions) -> Result<Value> {
        let value = self
            .get(
                "/api/location",
                &[(
                    "location[directory]",
                    options.directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(&options.directory, &value)?;
        if value["project"]["id"].as_str().is_none_or(str::is_empty) {
            return Err(Error::new(
                "NATIVE_LOCATION_UNAVAILABLE",
                "native workspace project identity is missing",
            ));
        }
        Ok(json!({"directory":value["directory"]}))
    }

    async fn check_route_model_available(&self, options: &NativeOptions) -> Result<()> {
        let value = self
            .get(
                "/api/model",
                &[(
                    "location[directory]",
                    options.directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(&options.directory, &value["location"])?;
        let models = value["data"]
            .as_array()
            .ok_or_else(|| Error::new("NATIVE_MODEL_SCHEMA", "native model catalog is invalid"))?;
        let mut matching_models = 0usize;
        let mut model_enabled = false;
        let mut matching_variants = 0usize;
        for model in models {
            if model["id"] == options.model.id && model["providerID"] == options.model.provider_id {
                matching_models += 1;
                model_enabled = model["enabled"].as_bool() == Some(true);
                let variants = model["variants"].as_array().ok_or_else(|| {
                    Error::new("NATIVE_MODEL_SCHEMA", "native model variants are invalid")
                })?;
                if variants.len() > 256 {
                    return Err(Error::new(
                        "NATIVE_MODEL_SCHEMA",
                        "native model variants exceed their protocol bound",
                    ));
                }
                let mut seen = BTreeSet::new();
                for variant in variants {
                    let id = variant["id"]
                        .as_str()
                        .filter(|id| {
                            !id.trim().is_empty()
                                && id.len() <= 256
                                && !id.bytes().any(|b| b.is_ascii_control())
                        })
                        .ok_or_else(|| {
                            Error::new(
                                "NATIVE_MODEL_SCHEMA",
                                "native model variant identity is invalid",
                            )
                        })?;
                    if !seen.insert(id.to_owned()) {
                        return Err(Error::new(
                            "NATIVE_MODEL_SCHEMA",
                            "native model contains duplicate variants",
                        ));
                    }
                    if id == options.model.variant {
                        matching_variants += 1;
                    }
                }
            }
        }
        if matching_models != 1 || !model_enabled || matching_variants != 1 {
            return Err(Error::new(
                "NATIVE_MODEL_UNAVAILABLE",
                "exact route provider/model/variant is absent or disabled",
            ));
        }
        Ok(())
    }

    async fn require_durable_root_creation(
        &self,
        root: &str,
        command: &RuntimeCommand,
        options: &NativeOptions,
    ) -> Result<()> {
        let model = serde_json::to_value(&options.model)?;
        self.require_durable_root_creation_inner(
            root,
            &command.binding_id,
            command.generation,
            &model,
        )
        .await
    }

    async fn require_durable_root_creation_for_intent(
        &self,
        root: &str,
        intent: &OperationIntent,
        options: &NativeOptions,
    ) -> Result<()> {
        let model = serde_json::to_value(&options.model)?;
        self.require_durable_root_creation_inner(
            root,
            &intent.binding_id,
            intent.generation,
            &model,
        )
        .await
    }

    async fn require_durable_root_creation_inner(
        &self,
        root: &str,
        binding: &str,
        generation: i64,
        model: &Value,
    ) -> Result<()> {
        let expected = format!(
            "ses_swarm_{}",
            sha256(format!("{binding}/{generation}").as_bytes())
        );
        if root != expected {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native root does not match the exact binding generation",
            ));
        }
        let mut url = self
            .endpoint
            .join(&format!("/api/experimental/session/{root}/log"))
            .map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid native log route"))?;
        url.query_pairs_mut().append_pair("follow", "false");
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT, "text/event-stream")
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|_| {
                Error::new(
                    "NATIVE_LOG_UNAVAILABLE",
                    "native durable log request failed",
                )
            })?;
        if !response.status().is_success()
            || response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(';').next())
                != Some("text/event-stream")
        {
            return Err(Error::new(
                "NATIVE_LOG_UNAVAILABLE",
                "native log response is not the documented SSE stream",
            ));
        }
        let mut total = 0usize;
        let bytes = response
            .bytes_stream()
            .map(move |chunk| {
                let chunk = chunk.map_err(|_| std::io::Error::other("native log transport"))?;
                total = total.saturating_add(chunk.len());
                if total > MAX_LOG_BYTES {
                    return Err(std::io::Error::other("native log bound"));
                }
                Ok(chunk)
            })
            .eventsource();
        futures_util::pin_mut!(bytes);
        let mut synced = false;
        let mut created = false;
        let mut last_sequence: Option<u64> = None;
        let mut event_ids = BTreeSet::new();
        let mut count = 0usize;
        while let Some(item) = bytes.next().await {
            let event = item.map_err(|_| {
                Error::new(
                    "NATIVE_LOG_STREAM_INCOMPLETE",
                    "native durable log stream ended with a gap",
                )
            })?;
            if event.event == "effect/httpapi/stream/failure" {
                return Err(Error::new(
                    "NATIVE_LOG_NATIVE_FAILURE",
                    "native log reported a stream failure",
                ));
            }
            if synced {
                return Err(Error::new(
                    "NATIVE_LOG_AFTER_WATERMARK",
                    "non-following native log continued after sync",
                ));
            }
            let value: Value = serde_json::from_str(&event.data).map_err(|_| {
                Error::new("NATIVE_LOG_SCHEMA", "native durable log event is invalid")
            })?;
            if value["type"] == "log.synced" {
                if value["aggregateID"] != root {
                    return Err(Error::new(
                        "NATIVE_LOG_WATERMARK_SCOPE",
                        "native log watermark names another session",
                    ));
                }
                let watermark = value
                    .get("seq")
                    .and_then(Value::as_u64)
                    .filter(|n| *n <= 9_007_199_254_740_991)
                    .ok_or_else(|| {
                        Error::new("NATIVE_LOG_SEQUENCE", "native log watermark is invalid")
                    })?;
                if last_sequence.is_none_or(|n| watermark < n) {
                    return Err(Error::new(
                        "NATIVE_LOG_WATERMARK_REGRESSED",
                        "native log watermark does not cover root creation",
                    ));
                }
                synced = true;
                continue;
            }
            count += 1;
            if count > MAX_LOG_EVENTS {
                return Err(Error::new(
                    "NATIVE_LOG_EVENT_LIMIT",
                    "native root log exceeds its event bound",
                ));
            }
            let event_id = value["id"]
                .as_str()
                .filter(|id| valid_id(id, "evt_").is_ok())
                .ok_or_else(|| {
                    Error::new("NATIVE_LOG_SCHEMA", "native durable event ID is invalid")
                })?;
            if !event_ids.insert(event_id.to_owned()) {
                return Err(Error::new(
                    "NATIVE_LOG_DUPLICATE",
                    "native durable log repeated an event ID",
                ));
            }
            if value["durable"]["aggregateID"] != root
                || value["data"]["sessionID"] != root
                || value["created"]
                    .as_f64()
                    .is_none_or(|n| !n.is_finite() || n < 0.0)
                || value["durable"]["version"].as_u64().is_none_or(|n| n == 0)
            {
                return Err(Error::new(
                    "NATIVE_LOG_SCOPE",
                    "native durable event has an invalid aggregate envelope",
                ));
            }
            let sequence = value["durable"]["seq"]
                .as_u64()
                .filter(|n| *n <= 9_007_199_254_740_991)
                .ok_or_else(|| {
                    Error::new(
                        "NATIVE_LOG_SEQUENCE",
                        "native durable event sequence is invalid",
                    )
                })?;
            if last_sequence.is_some_and(|last| sequence <= last) {
                return Err(Error::new(
                    "NATIVE_LOG_ORDER",
                    "native durable event order regressed",
                ));
            }
            last_sequence = Some(sequence);
            if !created {
                if value["type"] != "session.created"
                    || value["durable"]["version"] != 1
                    || value["data"]
                        .get("parentID")
                        .is_some_and(|parent| !parent.is_null())
                    || value["data"]["metadata"]["eliot"]["binding"] != binding
                    || value["data"]["metadata"]["eliot"]["generation"] != generation
                    || value["data"]["model"] != *model
                {
                    return Err(Error::new(
                        "NATIVE_LOG_ORIGIN",
                        "durable session.created does not prove this binding's root",
                    ));
                }
                created = true;
            } else if value["type"] == "session.created" {
                return Err(Error::new(
                    "NATIVE_LOG_ORIGIN_CHANGED",
                    "native log contains another root creation",
                ));
            }
        }
        if !synced {
            return Err(Error::new(
                "NATIVE_LOG_NOT_SYNCED",
                "native durable log did not reach a sync watermark",
            ));
        }
        if !created {
            return Err(Error::new(
                "DURABLE_EVENT_PERSISTENCE_UNAVAILABLE",
                "exact native root has no retained session.created event",
            ));
        }
        Ok(())
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Value> {
        let mut url = self
            .endpoint
            .join(path)
            .map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid native route"))?;
        if url.origin() != self.endpoint.origin() {
            return Err(Error::new(
                "NATIVE_ENDPOINT",
                "cross-origin native request refused",
            ));
        }
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())));
        }
        let effect = method != Method::GET;
        let mut request = self.client.request(method, url).timeout(REQUEST_TIMEOUT);
        if let Some(body) = body {
            let bytes = serde_json::to_vec(body)?;
            if bytes.len() > MAX_NATIVE_REQUEST {
                return Err(Error::new(
                    "NATIVE_REQUEST_LIMIT",
                    "native request exceeds its protocol body bound",
                ));
            }
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(bytes);
        }
        let mut response = request.send().await.map_err(|_| {
            Error::new(
                if effect {
                    "NATIVE_OUTCOME_UNKNOWN"
                } else {
                    "NATIVE_READ_FAILED"
                },
                "native HTTP request failed",
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let code = if effect
                && matches!(
                    status.as_u16(),
                    400 | 401 | 403 | 404 | 405 | 409 | 413 | 422
                ) {
                "NATIVE_REJECTED"
            } else if effect {
                "NATIVE_OUTCOME_UNKNOWN"
            } else {
                "NATIVE_READ_FAILED"
            };
            return Err(Error::new(code, format!("HTTP {}", status.as_u16())));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_NATIVE_BODY as u64)
        {
            return Err(Error::new(
                "NATIVE_RESPONSE_LIMIT",
                "native response exceeds its body limit",
            ));
        }
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            Error::new(
                if effect {
                    "NATIVE_OUTCOME_UNKNOWN"
                } else {
                    "NATIVE_READ_FAILED"
                },
                "native response stream failed",
            )
        })? {
            if bytes.len().saturating_add(chunk.len()) > MAX_NATIVE_BODY {
                return Err(Error::new(
                    "NATIVE_RESPONSE_LIMIT",
                    "native response exceeds its body limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                if effect {
                    "NATIVE_OUTCOME_UNKNOWN"
                } else {
                    "NATIVE_SCHEMA_ERROR"
                },
                "native JSON response is invalid",
            )
        })
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        self.request(Method::GET, path, query, None).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, &[], Some(&body)).await
    }
}

pub fn intent_for(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    options: &NativeOptions,
    input_id: Option<String>,
    text: Option<&str>,
) -> Result<OperationIntent> {
    let root = if command.method == "agent.open" {
        Some(root_id(&command.binding_id, command.generation))
    } else {
        command.native_root_id.clone()
    };
    let route = serde_json::to_value(options)?;
    let route_sha256 = digest_json(&route)?;
    let module_receipt = crate::module_receipt::for_command(claim, command)?;
    let prompt_sha256 = text.map(|text| sha256(text.as_bytes()));
    let reconcile_target_operation_id = if command.method == "agent.reconcile" {
        Some(required_text(&command.input, "operation_id")?.to_owned())
    } else {
        None
    };
    Ok(OperationIntent {
        version: 2,
        operation_id: command.operation_id.clone(),
        module_receipt,
        method: command.method.clone(),
        binding_id: command.binding_id.clone(),
        generation: command.generation,
        native_scope_key: options.scope_key(),
        native_root_id: root,
        native_input_id: input_id,
        prompt_sha256,
        prompt_bytes: text.map(|text| text.len() as u64),
        reconcile_target_operation_id,
        route_sha256,
        model: serde_json::to_value(&options.model)?,
        marker: marker(command),
    })
}

pub fn prompt(command: &RuntimeCommand) -> Result<String> {
    let text = command.input["text"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("input text must be nonempty"))?;
    if command.method != "task.dispatch" {
        return Ok(text.to_owned());
    }
    if !command.input["task_snapshot"].is_object() {
        return Err(Error::invalid("immutable task snapshot is required"));
    }
    let snapshot = canonical_json(&command.input["task_snapshot"])?;
    let base = format!("{text}\n\nELIOT immutable task snapshot:\n{snapshot}");
    let Some(packet) = command.input.get("launch_dispatch_packet") else {
        return Ok(base);
    };
    validate_launch_dispatch_packet(packet)?;
    let packet = canonical_json(packet)?;
    Ok(format!(
        "{base}\n\nELIOT immutable launch dispatch packet v1:\n<<<ELIOT-LAUNCH-DISPATCH-PACKET-V1-BEGIN>>>\n{packet}\n<<<ELIOT-LAUNCH-DISPATCH-PACKET-V1-END>>>"
    ))
}

fn canonical_json(value: &Value) -> Result<String> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted: BTreeMap<_, _> = object
                    .iter()
                    .map(|(key, value)| (key.clone(), ordered(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            value => value.clone(),
        }
    }
    Ok(serde_json::to_string(&ordered(value))?)
}

fn validate_launch_dispatch_packet(packet: &Value) -> Result<()> {
    strict_fields(
        packet,
        &[
            "schema_version",
            "launch_operation_id",
            "plan_digest",
            "task",
            "selection",
            "purpose",
            "capability",
        ],
    )?;
    if packet["schema_version"] != 1 {
        return Err(Error::new(
            "INVALID_LAUNCH_DISPATCH_PACKET",
            "dispatch packet version must be 1",
        ));
    }
    for field in ["launch_operation_id", "plan_digest", "purpose"] {
        required_text(packet, field)?;
    }
    strict_fields(
        &packet["task"],
        &["task_id", "revision", "attempt_id", "snapshot_digest"],
    )?;
    for field in ["task_id", "attempt_id", "snapshot_digest"] {
        required_text(&packet["task"], field)?;
    }
    if packet["task"]["revision"].as_u64().is_none_or(|n| n == 0) {
        return Err(Error::invalid("dispatch task revision is invalid"));
    }
    strict_fields(
        &packet["selection"],
        &["route", "provider", "model", "variant"],
    )?;
    for field in ["route", "provider", "model", "variant"] {
        required_text(&packet["selection"], field)?;
    }
    let capability = &packet["capability"];
    strict_fields(
        capability,
        &[
            "identity_digest",
            "evidence_digest",
            "native_discovered_digest",
            "service_id",
            "service_version",
            "plugin_id",
            "module_sha256",
            "required_core_schemas",
        ],
    )?;
    for field in [
        "identity_digest",
        "evidence_digest",
        "native_discovered_digest",
        "service_id",
        "service_version",
        "plugin_id",
        "module_sha256",
    ] {
        required_text(capability, field)?;
    }
    let schemas = capability["required_core_schemas"]
        .as_array()
        .filter(|items| !items.is_empty())
        .ok_or_else(|| Error::invalid("required core schemas must be a nonempty string array"))?;
    if schemas
        .iter()
        .any(|v| v.as_str().is_none_or(|s| s.trim().is_empty()))
    {
        return Err(Error::invalid(
            "required core schemas must be a nonempty string array",
        ));
    }
    Ok(())
}

fn strict_fields(value: &Value, fields: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("expected object"))?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(Error::invalid("object contains unsupported fields"));
    }
    Ok(())
}

fn required_text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::invalid("required text is missing"))
}

fn marker(command: &RuntimeCommand) -> Value {
    json!({
        "binding":command.binding_id,
        "generation":command.generation,
        "operation":command.operation_id,
        "input_sha256":command.input_sha256
    })
}

fn root_session_matches(
    session: &Value,
    root: &str,
    command: &RuntimeCommand,
    options: &NativeOptions,
    location: &Value,
) -> bool {
    session_identity_matches(
        session,
        root,
        options,
        location,
        &command.binding_id,
        command.generation,
    ) && session["metadata"]["eliot"] == marker(command)
}

fn session_identity_matches(
    session: &Value,
    root: &str,
    options: &NativeOptions,
    location: &Value,
    binding: &str,
    generation: i64,
) -> bool {
    session["id"] == root
        && session["parentID"].is_null()
        && session["fork"].is_null()
        && session.get("location") == Some(location)
        && session["model"]
            == json!({"id":options.model.id,"providerID":options.model.provider_id,"variant":options.model.variant})
        && session["metadata"]["eliot"]["binding"] == binding
        && session["metadata"]["eliot"]["generation"] == generation
}

fn inbox_matches(
    item: &Value,
    root: &str,
    input_id: &str,
    text: &str,
    command: &RuntimeCommand,
) -> bool {
    item["id"] == input_id
        && item["sessionID"] == root
        && item["type"] == "user"
        && item["delivery"] == "queue"
        && item["payload"]["text"] == text
        && item["payload"]["metadata"]["eliot"] == marker(command)
        && no_attachments(&item["payload"])
}

fn saved_inbox_matches(
    item: &Value,
    root: &str,
    input_id: &str,
    marker: &Value,
    intent: &OperationIntent,
) -> bool {
    item["id"] == input_id
        && item["sessionID"] == root
        && item["type"] == "user"
        && item["delivery"] == "queue"
        && item["payload"]["metadata"]["eliot"] == *marker
        && item["payload"]["text"]
            .as_str()
            .is_some_and(|text| saved_text_matches(text, intent))
        && no_attachments(&item["payload"])
}

fn saved_message_matches(
    message: &Value,
    root: &str,
    input_id: &str,
    marker: &Value,
    intent: &OperationIntent,
) -> bool {
    message["id"] == input_id
        && message["type"] == "user"
        && message
            .get("sessionID")
            .is_none_or(|id| id.as_str() == Some(root))
        && message["metadata"]["eliot"] == *marker
        && message["text"]
            .as_str()
            .is_some_and(|text| saved_text_matches(text, intent))
        && no_attachments(message)
}

fn saved_text_matches(text: &str, intent: &OperationIntent) -> bool {
    let digest = sha256(text.as_bytes());
    intent.prompt_sha256.as_deref() == Some(digest.as_str())
        && intent.prompt_bytes == Some(text.len() as u64)
}

fn no_attachments(value: &Value) -> bool {
    ["files", "agents", "skills"].into_iter().all(|key| {
        value
            .get(key)
            .is_none_or(|items| items.as_array().is_some_and(Vec::is_empty))
    })
}

fn verify_directory(expected: &Path, observed: &Value) -> Result<()> {
    let observed = Path::new(required_text(observed, "directory")?);
    if !observed.is_absolute() {
        return Err(Error::new(
            "NATIVE_LOCATION_MISMATCH",
            "native workspace is not absolute",
        ));
    }
    if observed == expected {
        return Ok(());
    }
    let left = std::fs::canonicalize(expected).map_err(|_| {
        Error::new(
            "NATIVE_LOCATION_UNAVAILABLE",
            "selected workspace cannot be resolved",
        )
    })?;
    let right = std::fs::canonicalize(observed).map_err(|_| {
        Error::new(
            "NATIVE_LOCATION_UNAVAILABLE",
            "native workspace cannot be resolved",
        )
    })?;
    if left != right || !std::fs::metadata(left).is_ok_and(|meta| meta.is_dir()) {
        return Err(Error::new(
            "NATIVE_LOCATION_MISMATCH",
            "native workspace differs from the explicitly selected directory",
        ));
    }
    Ok(())
}

fn read_connection_record(path: &Path) -> Result<ConnectionRecord> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection record is unavailable"))?;
    if !metadata.is_file() || metadata.len() > MAX_CONNECTION_FILE as u64 {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record must be a bounded regular file",
        ));
    }
    let mut file = File::open(path)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection record cannot be read"))?;
    if !file
        .metadata()
        .is_ok_and(|meta| meta.is_file() && meta.len() <= MAX_CONNECTION_FILE as u64)
    {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record changed while opening",
        ));
    }
    let mut bytes = Vec::new();
    file.take((MAX_CONNECTION_FILE + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection record read failed"))?;
    if bytes.len() > MAX_CONNECTION_FILE {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record exceeds its limit",
        ));
    }
    let record: ConnectionRecord = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "invalid connection record schema"))?;
    if record.schema_version != 1
        || record.pid == 0
        || record.username.is_empty()
        || record.username.contains(':')
        || record.password.is_empty()
    {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "explicit service PID and Basic authentication are required",
        ));
    }
    Ok(record)
}

fn checked_endpoint(text: &str) -> Result<Url> {
    let url =
        Url::parse(text).map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid service endpoint"))?;
    if url.scheme() != "http"
        || !is_loopback_endpoint(&url)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.port_or_known_default().is_none_or(|port| port == 0)
    {
        return Err(Error::new(
            "NATIVE_ENDPOINT",
            "only credential-free loopback HTTP origins are supported",
        ));
    }
    Ok(url)
}

fn valid_id(value: &str, prefix: &str) -> Result<()> {
    if !value.starts_with(prefix)
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "native identifier is invalid",
        ));
    }
    Ok(())
}

pub fn root_id(binding: &str, generation: i64) -> String {
    format!(
        "ses_swarm_{}",
        sha256(format!("{binding}/{generation}").as_bytes())
    )
}

pub fn input_id(operation: &str) -> String {
    format!("msg_swarm_{}", sha256(operation.as_bytes()))
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}
