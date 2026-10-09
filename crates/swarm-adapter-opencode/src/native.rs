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
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    net::SocketAddr,
    path::Path,
    time::Duration,
};
use swarm_contracts::{
    error::{Error, NativeHttpFailure, NativeHttpFailureKind, Result},
    module_contract::ModuleContractClaim,
    runtime::RuntimeCommand,
};

const MAX_NATIVE_BODY: usize = 4 * 1024 * 1024;
const MAX_NATIVE_REQUEST: usize = 4 * 1024 * 1024;
const MAX_NATIVE_ERROR_BODY: usize = 8 * 1024;
const MAX_CONNECTION_FILE: usize = 64 * 1024;
const MAX_LOG_BYTES: usize = 8 * 1024 * 1024;
const MAX_LOG_EVENTS: usize = 8192;
const MAX_MESSAGE_PAGE: usize = 50;
const MAX_MESSAGE_PAGES: usize = 32;
const MAX_MESSAGE_SCAN_BYTES: usize = 8 * 1024 * 1024;
const SESSION_HISTORY_PAGE_LIMIT: usize = 100;
const SESSION_HISTORY_MAX_PAGES: usize = 32;
const SESSION_HISTORY_MAX_BYTES: usize = 8 * 1024 * 1024;
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

#[derive(Debug, Clone, Copy, Default)]
pub struct InputEvidence {
    pub admitted_sequence: Option<u64>,
    pub prompted_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct InputAdmissionEvidence {
    pub admitted_sequence: u64,
    pub promoted_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct InputStatusEvidence {
    pub input_message_sha256: String,
}

#[derive(Debug, Clone)]
pub struct AssistantResultEvidence {
    pub message_id: String,
    pub parent_id: String,
    pub message: Value,
    pub payload_sha256: String,
    pub payload_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagePage {
    data: Vec<Value>,
    cursor: MessageCursor,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionHistoryPage {
    data: Vec<Value>,
    #[serde(rename = "hasMore")]
    has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageCursor {
    next: Option<String>,
    #[serde(rename = "previous")]
    _previous: Option<String>,
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
        let mut service = Self {
            client,
            endpoint,
            pid: record.pid,
            version: String::new(),
        };
        let info: ServerInfo = serde_json::from_value(service.get("/api/info", &[]).await?)
            .map_err(|_| Error::new("NATIVE_SCHEMA_ERROR", "native info response is invalid"))?;
        if info.pid != service.pid
            || info.version.trim().is_empty()
            || info.version.len() > 256
            || info.version.chars().any(char::is_control)
            || info.urls.is_empty()
            || info.paths.tmp.is_empty()
        {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service identity or observed version is invalid",
            ));
        }
        service.version = info.version.clone();
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

    /// Read the provider integration projection for one exact workspace. The
    /// caller validates the returned shape before making any mutation.
    pub async fn provider_integration(&self, directory: &str) -> Result<Value> {
        self.get(
            "/api/integration",
            &[("location[directory]", directory.to_owned())],
        )
        .await
    }

    /// Perform the one provider API-key POST. The shared request path maps a
    /// deterministic HTTP rejection to `NATIVE_REJECTED` and every transport
    /// ambiguity to `NATIVE_OUTCOME_UNKNOWN`.
    pub async fn post_provider_key(
        &self,
        provider_id: &str,
        key: &str,
        directory: &str,
    ) -> Result<()> {
        if !valid_provider_id(provider_id) {
            return Err(Error::new(
                "NATIVE_PROVIDER_ID_INVALID",
                "selected provider ID is not one safe integration path segment",
            ));
        }
        if key.is_empty() || directory.is_empty() || directory.len() > 4096 {
            return Err(Error::invalid("provider integration input is invalid"));
        }
        let body = json!({"key":key});
        let path = format!("/api/integration/{provider_id}/connect/key");
        let value = self
            .request(
                Method::POST,
                &path,
                &[("location[directory]", directory.to_owned())],
                Some(&body),
            )
            .await?;
        if !value.is_null() {
            return Err(Error::new(
                "NATIVE_SCHEMA_ERROR",
                "provider key response was not empty",
            ));
        }
        Ok(())
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn version(&self) -> &str {
        &self.version
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
                "agent.send supports only next_turn on OpenCode's native queue",
            ));
        }
        if command.method == "native.opencode.loop_step" {
            validate_loop_step_command(command)?;
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
        let request = input_payload(command, &input_id(&command.operation_id), &text)?;
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
        root: &str,
        input_id: &str,
        prompt_text: &str,
    ) -> Result<InputAdmissionEvidence> {
        let body = input_payload(command, input_id, prompt_text)?;
        let value = self
            .post(&format!("/api/session/{root}/prompt"), body)
            .await?;
        let data = value
            .get("data")
            .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "prompt response lacks data"))?;
        admitted_input_matches(data, root, input_id, prompt_text, command)
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
        let evidence = self.read_input_history(root, input, intent, None).await?;
        if evidence.admitted_sequence.is_none() && evidence.prompted_sequence.is_none() {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact saved input was not observed in durable session history",
            ));
        }
        Ok(evidence)
    }

    /// Prove only that the exact admitted user input was projected into its
    /// saved OpenCode session. This does not locate or infer an assistant turn.
    pub async fn read_input_status(
        &self,
        intent: &OperationIntent,
        options: &NativeOptions,
    ) -> Result<InputStatusEvidence> {
        if native_delivery_for_method(&intent.method).is_none()
            || intent.native_scope_key != options.scope_key()
            || intent.route_sha256 != digest_json(&serde_json::to_value(options)?)?
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved input intent differs from the exact binding route",
            ));
        }
        let root = intent.native_root_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved input intent has no exact session ID",
            )
        })?;
        let input = intent.native_input_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved input intent has no exact message ID",
            )
        })?;
        if root != root_id(&intent.binding_id, intent.generation)
            || input != input_id(&intent.operation_id)
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved session or input ID differs from its immutable Operation identity",
            ));
        }
        valid_id(root, "ses")?;
        valid_id(input, "msg_swarm_")?;

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

        let history = self.read_input_history(root, input, intent, None).await?;
        if history.admitted_sequence.is_none() && history.prompted_sequence.is_none() {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact input admission was not observed in durable session history",
            ));
        }

        let first = self
            .get(&format!("/api/session/{root}/message/{input}"), &[])
            .await?;
        let message = first.get("data").ok_or_else(|| {
            Error::new(
                "NATIVE_SCHEMA_ERROR",
                "native input-message response lacks data",
            )
        })?;
        // V2's PublicSessionMessage omits sessionID. The GET route is scoped
        // to `root`; reject a sessionID only if the projection includes one
        // and it disagrees with that route scope.
        if !saved_message_matches(message, root, input, intent) {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact saved user input was not observed in its native session",
            ));
        }
        let second = self
            .get(&format!("/api/session/{root}/message/{input}"), &[])
            .await?;
        let second_message = second.get("data").ok_or_else(|| {
            Error::new(
                "NATIVE_SCHEMA_ERROR",
                "repeated native input-message response lacks data",
            )
        })?;
        if second_message != message {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact native input projection changed during readback",
            ));
        }
        Ok(InputStatusEvidence {
            input_message_sha256: digest_json(message)?,
        })
    }

    async fn read_input_history(
        &self,
        session: &str,
        input_id: &str,
        intent: &OperationIntent,
        after_sequence: Option<u64>,
    ) -> Result<InputEvidence> {
        let delivery = native_delivery_for_method(&intent.method).ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved input method has no exact native delivery",
            )
        })?;
        let mut after = after_sequence.unwrap_or(0);
        let mut expected = after.checked_add(1).ok_or_else(|| {
            Error::new(
                "NATIVE_HISTORY_CURSOR",
                "native history cursor is exhausted",
            )
        })?;
        let mut pages = 0usize;
        let mut scanned_bytes = 0usize;
        let mut seen = BTreeMap::<u64, (String, String)>::new();
        let mut evidence = InputEvidence::default();
        let mut complete = false;

        while pages < SESSION_HISTORY_MAX_PAGES {
            pages += 1;
            let raw = self
                .get(
                    &format!("/api/session/{session}/history"),
                    &[
                        ("after", after.to_string()),
                        ("limit", SESSION_HISTORY_PAGE_LIMIT.to_string()),
                    ],
                )
                .await?;
            scanned_bytes = scanned_bytes.saturating_add(canonical_json(&raw)?.len());
            if scanned_bytes > SESSION_HISTORY_MAX_BYTES {
                return Err(Error::new(
                    "NATIVE_HISTORY_LIMIT",
                    "durable session history exceeds its configured read bound",
                ));
            }
            let page: SessionHistoryPage = serde_json::from_value(raw).map_err(|_| {
                Error::new(
                    "NATIVE_HISTORY_SCHEMA",
                    "durable session history page is outside the selected V2 schema",
                )
            })?;
            if page.data.len() > SESSION_HISTORY_PAGE_LIMIT
                || (page.has_more && page.data.is_empty())
            {
                return Err(Error::new(
                    "NATIVE_HISTORY_SCHEMA",
                    "durable session history page is empty or exceeds its bound",
                ));
            }

            let page_start = after;
            let mut gap_after_evidence = false;
            for event in &page.data {
                let event_id = required_text(event, "id")?;
                valid_id(event_id, "evt")?;
                let event_type = required_text(event, "type")?;
                if event_type.len() > 128 {
                    return Err(Error::new(
                        "NATIVE_HISTORY_SCHEMA",
                        "durable event type exceeds its protocol bound",
                    ));
                }
                let durable = event
                    .get("durable")
                    .filter(|value| value.is_object())
                    .ok_or_else(|| {
                        Error::new(
                            "NATIVE_HISTORY_SCHEMA",
                            "durable session event lacks its aggregate identity",
                        )
                    })?;
                if required_text(durable, "aggregateID")? != session {
                    return Err(Error::new(
                        "NATIVE_HISTORY_SCOPE",
                        "durable session event belongs to another aggregate",
                    ));
                }
                let sequence = durable["seq"].as_u64().ok_or_else(|| {
                    Error::new("NATIVE_HISTORY_SCHEMA", "durable event sequence is invalid")
                })?;
                if durable["version"]
                    .as_u64()
                    .is_none_or(|version| version == 0)
                {
                    return Err(Error::new(
                        "NATIVE_HISTORY_SCHEMA",
                        "durable event schema version is invalid",
                    ));
                }
                let event_digest = digest_json(event)?;
                if let Some((previous_id, previous_digest)) = seen.get(&sequence) {
                    if previous_id != event_id || previous_digest != &event_digest {
                        return Err(Error::new(
                            "NATIVE_HISTORY_CONFLICT",
                            "conflicting durable events share one aggregate sequence",
                        ));
                    }
                    continue;
                }
                if sequence <= after {
                    return Err(Error::new(
                        "NATIVE_HISTORY_CURSOR",
                        "exclusive history cursor returned an earlier event",
                    ));
                }
                if sequence != expected {
                    // A matching durable admission already proves this input;
                    // a later history gap only prevents adding later evidence.
                    if evidence.admitted_sequence.is_some() || evidence.prompted_sequence.is_some()
                    {
                        gap_after_evidence = true;
                        break;
                    }
                    return Err(Error::new(
                        "NATIVE_HISTORY_GAP",
                        "durable session history does not cover the required sequence interval",
                    ));
                }
                seen.insert(sequence, (event_id.to_owned(), event_digest));
                let data = event
                    .get("data")
                    .filter(|value| value.is_object())
                    .ok_or_else(|| {
                        Error::new("NATIVE_HISTORY_SCHEMA", "durable event data is invalid")
                    })?;
                if matches!(
                    event_type,
                    "session.next.prompt.admitted" | "session.next.prompted"
                ) && required_text(data, "messageID")? == input_id
                {
                    validate_prompt_history_event(
                        event_type, data, session, input_id, delivery, intent,
                    )?;
                    if event_type == "session.next.prompt.admitted" {
                        if evidence.admitted_sequence.replace(sequence).is_some() {
                            return Err(Error::new(
                                "NATIVE_HISTORY_CONFLICT",
                                "the exact native input has more than one admission event",
                            ));
                        }
                    } else if evidence.prompted_sequence.replace(sequence).is_some() {
                        return Err(Error::new(
                            "NATIVE_HISTORY_CONFLICT",
                            "the exact native input has more than one promotion event",
                        ));
                    }
                }
                expected = sequence.checked_add(1).ok_or_else(|| {
                    Error::new(
                        "NATIVE_HISTORY_CURSOR",
                        "native history sequence is exhausted",
                    )
                })?;
                after = sequence;
            }
            if gap_after_evidence {
                break;
            }
            if page.has_more {
                if after <= page_start {
                    return Err(Error::new(
                        "NATIVE_HISTORY_CURSOR",
                        "history hasMore page did not advance its exclusive cursor",
                    ));
                }
                continue;
            }
            complete = true;
            break;
        }

        if !complete && evidence.admitted_sequence.is_none() && evidence.prompted_sequence.is_none()
        {
            return Err(Error::new(
                "NATIVE_HISTORY_LIMIT",
                "exact input was not found within the bounded durable history window",
            ));
        }
        if let (Some(admitted), Some(prompted)) =
            (evidence.admitted_sequence, evidence.prompted_sequence)
            && prompted <= admitted
        {
            return Err(Error::new(
                "NATIVE_HISTORY_CONFLICT",
                "native input promotion does not follow its exact admission",
            ));
        }
        Ok(evidence)
    }

    /// Read one exact assistant projection and require the native parent edge
    /// to the deterministic admitted user message. OpenCode 2.0.7's public
    /// projection currently omits that edge, so this method fails closed with
    /// `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`; it never maps by order, latest
    /// response, timestamp, or a generated sequence.
    pub async fn read_assistant_result(
        &self,
        intent: &OperationIntent,
        options: &NativeOptions,
        assistant_message_id: &str,
    ) -> Result<AssistantResultEvidence> {
        if intent.method != "task.dispatch"
            || intent.native_scope_key != options.scope_key()
            || intent.route_sha256 != digest_json(&serde_json::to_value(options)?)?
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved dispatch intent differs from the exact assistant-result route",
            ));
        }
        let root = intent.native_root_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved dispatch intent has no exact native session ID",
            )
        })?;
        let input = intent.native_input_id.as_deref().ok_or_else(|| {
            Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "saved dispatch intent has no exact native input ID",
            )
        })?;
        if root != root_id(&intent.binding_id, intent.generation)
            || input != input_id(&intent.operation_id)
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved dispatch session or input differs from its immutable Operation identity",
            ));
        }
        valid_id(root, "ses")?;
        valid_id(input, "msg_swarm_")?;
        valid_id(assistant_message_id, "msg_")?;
        if assistant_message_id == input {
            return Err(Error::new(
                "NATIVE_ASSISTANT_PARENT_MISMATCH",
                "assistant selector names the admitted user message",
            ));
        }

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
                "exact saved native session identity was not observed",
            ));
        }
        self.check_route_model_available(options).await?;
        self.require_durable_root_creation_for_intent(root, intent, options)
            .await?;

        let input_history = self.read_input_history(root, input, intent, None).await?;
        if input_history.admitted_sequence.is_none() && input_history.prompted_sequence.is_none() {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "exact dispatch input is absent from durable session history",
            ));
        }

        let first = self
            .scan_assistant_result(root, input, assistant_message_id, intent)
            .await?;
        let second = self
            .scan_assistant_result(root, input, assistant_message_id, intent)
            .await?;
        if first.message != second.message || first.parent_id != second.parent_id {
            return Err(Error::new(
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "repeated assistant projection changed during readback",
            ));
        }
        Ok(first)
    }

    async fn scan_assistant_result(
        &self,
        session: &str,
        input_id: &str,
        assistant_id: &str,
        intent: &OperationIntent,
    ) -> Result<AssistantResultEvidence> {
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut scanned_bytes = 0usize;
        let mut input_message: Option<Value> = None;
        let mut assistant_message: Option<Value> = None;

        for _ in 0..MAX_MESSAGE_PAGES {
            let mut query = vec![("limit", MAX_MESSAGE_PAGE.to_string())];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            } else {
                query.push(("order", "desc".to_owned()));
            }
            let raw = self
                .get(&format!("/api/session/{session}/message"), &query)
                .await?;
            scanned_bytes = scanned_bytes.saturating_add(canonical_json(&raw)?.len());
            if scanned_bytes > MAX_MESSAGE_SCAN_BYTES {
                return Err(Error::new(
                    "NATIVE_MESSAGE_SCAN_LIMIT",
                    "native assistant message projection exceeds its read bound",
                ));
            }
            let page: MessagePage = serde_json::from_value(raw).map_err(|_| {
                Error::new(
                    "NATIVE_MESSAGE_SCHEMA",
                    "native assistant message page is outside the pinned contract",
                )
            })?;
            if page.data.len() > MAX_MESSAGE_PAGE
                || (page.data.is_empty() && page.cursor.next.is_some())
            {
                return Err(Error::new(
                    "NATIVE_MESSAGE_SCHEMA",
                    "native assistant message page has an invalid bound",
                ));
            }
            for message in page.data {
                let id = message.get("id").and_then(Value::as_str).ok_or_else(|| {
                    Error::new(
                        "NATIVE_MESSAGE_SCHEMA",
                        "native message lacks its exact identity",
                    )
                })?;
                valid_id(id, "msg_")?;
                if !ids.insert(id.to_owned()) {
                    return Err(Error::new(
                        "NATIVE_MESSAGE_AMBIGUOUS",
                        "native message projection repeated an ID",
                    ));
                }
                if id == input_id {
                    if !saved_message_matches(&message, session, input_id, intent) {
                        return Err(Error::new(
                            "NATIVE_INPUT_MISMATCH",
                            "native user message differs from the exact admitted input",
                        ));
                    }
                    input_message = Some(message.clone());
                }
                if id == assistant_id {
                    validate_assistant_parent(&message, session, input_id)?;
                    if assistant_message.replace(message).is_some() {
                        return Err(Error::new(
                            "NATIVE_MESSAGE_AMBIGUOUS",
                            "native assistant projection contains more than one selected ID",
                        ));
                    }
                }
            }
            match page.cursor.next {
                None => {
                    // `cursor` names the page request, not unfinished work.
                    // Clear a prior-page cursor when this page proves EOF so a
                    // completed multi-page scan is not reported as limit exhaustion.
                    cursor = None;
                    break;
                }
                Some(next)
                    if !next.is_empty() && next.len() <= 4096 && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next);
                }
                Some(_) => {
                    return Err(Error::new(
                        "NATIVE_MESSAGE_CURSOR",
                        "native assistant message cursor repeated or is invalid",
                    ));
                }
            }
        }

        if cursor.is_some() {
            return Err(Error::new(
                "NATIVE_MESSAGE_SCAN_LIMIT",
                "native assistant message projection exceeded its page bound",
            ));
        }
        if input_message.is_none() {
            return Err(Error::new(
                "NATIVE_INPUT_NOT_OBSERVED",
                "exact admitted native user message was not observed",
            ));
        }
        let message = assistant_message.ok_or_else(|| {
            Error::new(
                "NATIVE_ASSISTANT_RESULT_NOT_OBSERVED",
                "selected native assistant message was not observed",
            )
        })?;
        let encoded = canonical_json(&message)?.into_bytes();
        Ok(AssistantResultEvidence {
            message_id: assistant_id.to_owned(),
            parent_id: input_id.to_owned(),
            payload_sha256: sha256(&encoded),
            payload_bytes: encoded.len() as u64,
            message,
        })
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

    /// Verify the deterministic root and prove a target is either that root
    /// or an observed descendant. Child authority comes only from the native
    /// parent chain; a caller-supplied session ID is never enough by itself.
    pub async fn verify_control_scope(
        &self,
        command: &RuntimeCommand,
        options: &NativeOptions,
        target: &str,
    ) -> Result<()> {
        let root = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| Error::new("NATIVE_ROOT_MISSING", "native root is missing"))?;
        valid_id(target, "ses")?;
        if root != root_id(&command.binding_id, command.generation) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native root differs from this binding generation",
            ));
        }
        self.verify_binding_model(root, options, command).await?;
        self.require_durable_root_creation(root, command, options)
            .await?;

        let mut current = target.to_owned();
        let mut visited = BTreeSet::new();
        for _ in 0..64 {
            if current == root {
                return Ok(());
            }
            if !visited.insert(current.clone()) {
                break;
            }
            let session = self.session(&current).await?;
            let Some(parent) = session["parentID"].as_str() else {
                break;
            };
            valid_id(parent, "ses")?;
            current = parent.to_owned();
        }
        Err(Error::new(
            "NATIVE_SCOPE_MISMATCH",
            "target session is not in the exact owned root's native parent chain",
        ))
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
            let failure = read_native_http_failure(&mut response, status).await;
            return Err(Error::new(code, format!("HTTP {}", status.as_u16()))
                .with_native_http_failure(failure));
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

    /// Native MCP command consumers use the same authenticated, bounded HTTP
    /// client as ordinary OpenCode dispatch. These wrappers expose no endpoint
    /// or credential material beyond the already-admitted local path/body.
    pub async fn mcp_get(&self, path: &str) -> Result<Value> {
        self.get(path, &[]).await
    }

    pub async fn mcp_put(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::PUT, path, &[], Some(&body)).await
    }

    pub async fn mcp_post(&self, path: &str, body: Value) -> Result<Value> {
        self.post(path, body).await
    }

    /// Internal native-control transport. Callers construct only fixed API
    /// paths after validating every path segment; this is not a runtime RPC.
    pub(crate) async fn control_get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        self.get(path, query).await
    }

    pub(crate) async fn control_post(&self, path: &str, body: Value) -> Result<Value> {
        self.post(path, body).await
    }

    pub(crate) async fn control_post_empty(&self, path: &str) -> Result<Value> {
        self.request(Method::POST, path, &[], None).await
    }

    pub async fn verify_mcp_service(&self) -> Result<()> {
        let info: ServerInfo = serde_json::from_value(self.get("/api/info", &[]).await?)
            .map_err(|_| Error::new("NATIVE_SCHEMA_ERROR", "native info response is invalid"))?;
        if info.pid != self.pid
            || info.version != self.version
            || info.urls.is_empty()
            || info.paths.tmp.is_empty()
        {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service identity/version differs from the authenticated connection",
            ));
        }
        Ok(())
    }

    pub fn process_id(&self) -> u32 {
        self.pid
    }

    pub fn process_version(&self) -> &str {
        &self.version
    }
}

async fn read_native_http_failure(
    response: &mut reqwest::Response,
    status: reqwest::StatusCode,
) -> NativeHttpFailure {
    let mut body = Vec::new();
    let mut bounded = !response
        .content_length()
        .is_some_and(|length| length > MAX_NATIVE_ERROR_BODY as u64);
    while bounded {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len().saturating_add(chunk.len()) <= MAX_NATIVE_ERROR_BODY => {
                body.extend_from_slice(&chunk);
            }
            Ok(Some(_)) | Err(_) => bounded = false,
            Ok(None) => break,
        }
    }
    NativeHttpFailure {
        status: status.as_u16(),
        kind: if bounded {
            classify_native_http_failure(status, &body)
        } else {
            NativeHttpFailureKind::Unclassified
        },
    }
}

fn classify_native_http_failure(status: reqwest::StatusCode, body: &[u8]) -> NativeHttpFailureKind {
    let tag = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("_tag").and_then(Value::as_str).map(str::to_owned));
    match (status.as_u16(), tag.as_deref()) {
        (400, Some("InvalidRequestError")) => NativeHttpFailureKind::InvalidRequest,
        (401, Some("UnauthorizedError")) => NativeHttpFailureKind::Unauthorized,
        (404, Some("SessionNotFoundError")) => NativeHttpFailureKind::SessionNotFound,
        (404, Some("MessageNotFoundError")) => NativeHttpFailureKind::MessageNotFound,
        (409, Some("ConflictError")) => NativeHttpFailureKind::Conflict,
        (500, Some("UnknownError")) => NativeHttpFailureKind::InternalServerError,
        _ => NativeHttpFailureKind::Unclassified,
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
        result_input_status: None,
        result_assistant: None,
        dispatch_admission: None,
        route_sha256,
        model: serde_json::to_value(&options.model)?,
        marker: marker(command),
    })
}

pub fn input_payload(command: &RuntimeCommand, input_id: &str, prompt_text: &str) -> Result<Value> {
    let delivery = native_delivery_for_method(&command.method).ok_or_else(|| {
        Error::new(
            "UNSUPPORTED_CAPABILITY",
            "OpenCode input method has no declared native delivery",
        )
    })?;
    Ok(json!({
        "id":input_id,
        "prompt":{"text":prompt_text},
        "delivery":delivery,
        "resume":true
    }))
}

pub fn native_delivery_for_method(method: &str) -> Option<&'static str> {
    match method {
        "task.dispatch" | "agent.send" => Some("queue"),
        "native.opencode.loop_step" => Some("steer"),
        _ => None,
    }
}

fn validate_loop_step_command(command: &RuntimeCommand) -> Result<()> {
    strict_fields(
        &command.input,
        &["client_request_id", "binding_id", "generation", "text"],
    )?;
    if command
        .input
        .as_object()
        .is_none_or(|input| input.len() != 4)
        || command.input["client_request_id"]
            .as_str()
            .is_none_or(|id| {
                id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            })
        || command.input["binding_id"].as_str() != Some(command.binding_id.as_str())
        || command.input["generation"].as_i64() != Some(command.generation)
        || command.input["text"]
            .as_str()
            .is_none_or(|text| text.trim().is_empty() || text.len() > 65_536)
    {
        return Err(Error::new(
            "INVALID_OPENCODE_LOOP_STEP",
            "loop-step input must contain a bounded caller request id, exact binding generation, and nonempty text",
        ));
    }
    Ok(())
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

pub fn canonical_json(value: &Value) -> Result<String> {
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
    let mut value = json!({
        "binding":command.binding_id,
        "generation":command.generation,
        "operation":command.operation_id,
        "input_sha256":command.input_sha256
    });
    if let Some(delivery) = native_delivery_for_method(&command.method) {
        value["native_delivery"] = json!(delivery);
    }
    value
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

fn admitted_input_matches(
    item: &Value,
    root: &str,
    input_id: &str,
    text: &str,
    command: &RuntimeCommand,
) -> Result<InputAdmissionEvidence> {
    if !exact_fields(
        item,
        &[
            "admittedSeq",
            "id",
            "sessionID",
            "prompt",
            "delivery",
            "timeCreated",
        ],
        &["promotedSeq"],
    ) {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "prompt response is outside the current SessionInput.Admitted schema",
        ));
    }
    let admitted_sequence = item["admittedSeq"]
        .as_u64()
        .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "prompt admittedSeq is invalid"))?;
    if item["timeCreated"]
        .as_f64()
        .is_none_or(|time| !time.is_finite())
    {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "prompt timeCreated is invalid",
        ));
    }
    let prompt = &item["prompt"];
    if !exact_fields(prompt, &["text"], &["files", "agents"])
        || prompt["text"].as_str() != Some(text)
        || !no_attachments(prompt)
    {
        return Err(Error::new(
            "NATIVE_OUTCOME_UNKNOWN",
            "prompt response did not prove exact admitted text without attachments",
        ));
    }
    let delivery = native_delivery_for_method(&command.method).ok_or_else(|| {
        Error::new(
            "UNSUPPORTED_CAPABILITY",
            "OpenCode input delivery is unknown",
        )
    })?;
    if item["id"] != input_id
        || item["sessionID"] != root
        || item["delivery"].as_str() != Some(delivery)
    {
        return Err(Error::new(
            "NATIVE_OUTCOME_UNKNOWN",
            "prompt response identity or delivery differs from the exact Operation",
        ));
    }
    let promoted_sequence = item
        .get("promotedSeq")
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "prompt promotedSeq is invalid"))
        })
        .transpose()?;
    if promoted_sequence.is_some_and(|sequence| sequence < admitted_sequence) {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "prompt promotedSeq precedes its admission sequence",
        ));
    }
    Ok(InputAdmissionEvidence {
        admitted_sequence,
        promoted_sequence,
    })
}

fn saved_message_matches(
    message: &Value,
    root: &str,
    input_id: &str,
    intent: &OperationIntent,
) -> bool {
    exact_fields(
        message,
        &["id", "time", "text", "type"],
        &["metadata", "files", "agents", "sessionID"],
    ) && exact_fields(&message["time"], &["created"], &[])
        && message["time"]["created"]
            .as_f64()
            .is_some_and(f64::is_finite)
        && message.get("metadata").is_none_or(Value::is_object)
        && message["id"] == input_id
        && message["type"] == "user"
        && message
            .get("sessionID")
            .is_none_or(|id| id.as_str() == Some(root))
        && message["text"]
            .as_str()
            .is_some_and(|text| saved_text_matches(text, intent))
        && no_attachments(message)
}

fn validate_assistant_parent(message: &Value, session: &str, input_id: &str) -> Result<()> {
    if message["type"] != "assistant" {
        return Err(Error::new(
            "NATIVE_ASSISTANT_RESULT_NOT_ASSISTANT",
            "selected native message is not an assistant projection",
        ));
    }
    if message
        .get("sessionID")
        .is_some_and(|value| value != session)
    {
        return Err(Error::new(
            "NATIVE_ASSISTANT_SESSION_MISMATCH",
            "selected assistant message names another native session",
        ));
    }
    let parent = message
        .get("parentID")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_ASSISTANT_PARENT_UNAVAILABLE",
                "pinned native assistant projection has no parent message identity",
            )
        })?;
    if parent != input_id {
        return Err(Error::new(
            "NATIVE_ASSISTANT_PARENT_MISMATCH",
            "assistant parent does not name the exact admitted user message",
        ));
    }
    let created = message["time"]["created"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_ASSISTANT_SCHEMA",
                "assistant creation time is invalid",
            )
        })?;
    message["time"]["completed"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= created)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_ASSISTANT_NOT_COMPLETE",
                "assistant projection has no completed timestamp",
            )
        })?;
    if message.get("truncated").is_some_and(|value| value == true) {
        return Err(Error::new(
            "NATIVE_ASSISTANT_TRUNCATED",
            "assistant projection is truncated",
        ));
    }
    let finish = message["finish"].as_str().ok_or_else(|| {
        Error::new(
            "NATIVE_ASSISTANT_NOT_COMPLETE",
            "assistant projection has no terminal finish",
        )
    })?;
    if !matches!(
        finish,
        "stop" | "length" | "tool-calls" | "content-filter" | "error" | "unknown"
    ) {
        return Err(Error::new(
            "NATIVE_ASSISTANT_SCHEMA",
            "assistant projection has an unsupported terminal finish",
        ));
    }
    let content = message["content"].as_array().ok_or_else(|| {
        Error::new(
            "NATIVE_ASSISTANT_SCHEMA",
            "assistant projection lacks its complete content body",
        )
    })?;
    if content.iter().any(|part| part["truncated"] == true) {
        return Err(Error::new(
            "NATIVE_ASSISTANT_TRUNCATED",
            "assistant content projection is truncated",
        ));
    }
    Ok(())
}

fn saved_text_matches(text: &str, intent: &OperationIntent) -> bool {
    let digest = sha256(text.as_bytes());
    intent.prompt_sha256.as_deref() == Some(digest.as_str())
        && intent.prompt_bytes == Some(text.len() as u64)
}

fn validate_prompt_history_event(
    event_type: &str,
    data: &Value,
    session: &str,
    input_id: &str,
    expected_delivery: &str,
    intent: &OperationIntent,
) -> Result<()> {
    if !matches!(
        event_type,
        "session.next.prompt.admitted" | "session.next.prompted"
    ) || !exact_fields(
        data,
        &["timestamp", "sessionID", "messageID", "prompt", "delivery"],
        &[],
    ) || data["timestamp"]
        .as_f64()
        .is_none_or(|timestamp| !timestamp.is_finite())
    {
        return Err(Error::new(
            "NATIVE_HISTORY_SCHEMA",
            "prompt history event differs from the selected V2 schema",
        ));
    }
    let prompt = &data["prompt"];
    if !exact_fields(prompt, &["text"], &["files", "agents"])
        || !prompt["text"]
            .as_str()
            .is_some_and(|text| saved_text_matches(text, intent))
        || !no_attachments(prompt)
        || data["sessionID"] != session
        || data["messageID"] != input_id
        || data["delivery"].as_str() != Some(expected_delivery)
    {
        return Err(Error::new(
            "NATIVE_INPUT_MISMATCH",
            "durable prompt event differs from the exact saved session, text or delivery",
        ));
    }
    Ok(())
}

fn exact_fields(value: &Value, required: &[&str], optional: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        required.iter().all(|field| object.contains_key(*field))
            && object.keys().all(|field| {
                required.contains(&field.as_str()) || optional.contains(&field.as_str())
            })
    })
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
    let file = File::open(path)
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

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
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
